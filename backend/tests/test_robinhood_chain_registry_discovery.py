from __future__ import annotations

import unittest
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import Session, sessionmaker

from app.models import (
    RobinhoodChainPairCapability,
    RobinhoodChainPairObjective,
    RobinhoodChainRegistryVerification,
    RobinhoodChainWalletEvent,
    TokenRegistry,
    WalletAddress,
)
from app.services.robinhood_chain_execution_discovery import RobinhoodChainExecutionDiscoveryService
from app.services.robinhood_chain_registry_discovery import (
    AMOUNT_MODE_EXACT_INPUT,
    MECHANISM_SWAP,
    MECHANISM_WRAP_UNWRAP,
    PREPARATION_STATUS,
    RobinhoodChainRegistryDiscoveryService,
    _classify_probe_result,
    _ensure_provider_scoped_capability_schema,
    _parse_probe_amount,
)


def _abi_string(value: str) -> str:
    raw = value.encode("utf-8")
    padded = raw + (b"\x00" * ((32 - (len(raw) % 32)) % 32))
    return "0x" + (
        (32).to_bytes(32, "big")
        + len(raw).to_bytes(32, "big")
        + padded
    ).hex()


def _abi_uint(value: int) -> str:
    return "0x" + int(value).to_bytes(32, "big").hex()


class _FakeRpcClient:
    def __init__(self, metadata: Dict[str, Dict[str, Any]] | None = None) -> None:
        self.metadata = {
            str(address).lower(): dict(item)
            for address, item in (metadata or {}).items()
        }
        self.calls: List[tuple[str, list]] = []

    async def verify_expected_chain(self, *, force_refresh: bool = False) -> Dict[str, Any]:
        self.calls.append(("eth_chainId", [bool(force_refresh)]))
        return {
            "ok": True,
            "actual_chain_id": 4663,
            "expected_chain_id": 4663,
        }

    async def rpc_read(
        self,
        method: str,
        params: list,
        *,
        cache_namespace: str,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        self.calls.append((method, list(params)))
        if method == "eth_getCode":
            address = str(params[0]).lower()
            item = self.metadata.get(address)
            return {
                "ok": True,
                "result": "0x60006000" if item and item.get("code", True) else "0x",
                "cached": False,
                "fetched_at": "2026-07-20T00:00:00+00:00",
            }
        if method == "eth_call":
            call = params[0]
            address = str(call.get("to") or "").lower()
            selector = str(call.get("data") or "").lower()
            item = self.metadata.get(address)
            if not item:
                return {"ok": False, "error": {"message": "missing fake metadata"}}
            if selector == "0x95d89b41":
                return {"ok": True, "result": _abi_string(str(item["symbol"]))}
            if selector == "0x06fdde03":
                return {"ok": True, "result": _abi_string(str(item.get("name") or item["symbol"]))}
            if selector == "0x313ce567":
                return {"ok": True, "result": _abi_uint(int(item["decimals"]))}
        return {"ok": False, "error": {"message": f"unsupported fake RPC method: {method}"}}

    async def get_erc20_balance(
        self,
        owner_address: str,
        contract_address: str,
        decimals: int,
        *,
        block_tag: str = "latest",
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        self.calls.append(
            (
                "erc20_balance",
                [
                    str(owner_address),
                    str(contract_address),
                    int(decimals),
                    str(block_tag),
                    bool(force_refresh),
                ],
            )
        )
        item = self.metadata.get(str(contract_address).lower())
        if not item:
            return {"ok": False, "error": "missing fake metadata"}
        atomic = int(item.get("balance_atomic") or 0)
        scale = Decimal(10) ** int(decimals)
        balance = Decimal(atomic) / scale
        balance_text = format(balance, "f")
        if "." in balance_text:
            balance_text = balance_text.rstrip("0").rstrip(".")
        if not balance_text:
            balance_text = "0"
        return {
            "ok": True,
            "owner_address": str(owner_address),
            "contract_address": str(contract_address),
            "decimals": int(decimals),
            "balance_atomic": str(atomic),
            "balance_token": balance_text,
            "read_only": True,
        }


class _FakeDiscoveryService:
    def __init__(self, results: List[Dict[str, Any]] | None = None) -> None:
        self.results = list(results or [])
        self.calls: List[Dict[str, Any]] = []

    async def probe(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(dict(kwargs))
        if self.results:
            return self.results.pop(0)
        sell = kwargs["sell_token"]["symbol"]
        buy = kwargs["buy_token"]["symbol"]
        amount = kwargs["sell_amount"]
        return {
            "ok": True,
            "liquidity_available": True,
            "sell_amount": amount,
            "buy_amount": "0.5",
            "price_buy_per_sell": "0.5",
            "price_impact_bps": "3.5",
            "route": {"fills": [{"source": f"FAKE_{sell}_{buy}"}]},
            "provider_warnings": [],
            "provider_contacted": True,
            "read_only": True,
            "execution_enabled": False,
            "signing_enabled": False,
            "transaction_calldata": None,
            "will_mutate": False,
        }


class _FakeUniswapService:
    def __init__(self, results: List[Dict[str, Any]] | None = None) -> None:
        self.results = list(results or [])
        self.calls: List[Dict[str, Any]] = []

    async def probe(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(dict(kwargs))
        if self.results:
            return self.results.pop(0)
        amount = str(kwargs.get("requested_amount") or "1")
        return {
            "ok": True,
            "provider": "uniswap_api",
            "provider_contacted": True,
            "liquidity_available": True,
            "sell_amount": amount,
            "buy_amount": "0.75",
            "price_buy_per_sell": "0.75",
            "route": {"fills": [{"source": "UNISWAP_V3"}]},
            "provider_warnings": [],
            "read_only": True,
            "execution_enabled": False,
            "transaction_calldata": None,
            "will_mutate": False,
        }


class _FakeUniswapV3Service:
    def __init__(self, results: List[Dict[str, Any]] | None = None) -> None:
        self.results = list(results or [])
        self.calls: List[Dict[str, Any]] = []

    async def probe(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(dict(kwargs))
        if self.results:
            return self.results.pop(0)
        amount = str(kwargs.get("requested_amount") or "1")
        return {
            "ok": True,
            "provider": "uniswap_v3_rpc",
            "provider_contacted": True,
            "liquidity_available": True,
            "sell_amount": amount,
            "buy_amount": "0.8",
            "price_buy_per_sell": "0.8",
            "route": {"fills": [{"source": "UNISWAP_V3_RPC_DIRECT"}]},
            "provider_warnings": [],
            "read_only": True,
            "execution_enabled": False,
            "transaction_calldata": None,
            "will_mutate": False,
        }


class _FakePlanningService:
    def __init__(self, *, ok: bool = True) -> None:
        self.ok = bool(ok)
        self.calls: List[Dict[str, Any]] = []

    async def firm_quote_plan(self, **kwargs: Any) -> Dict[str, Any]:
        self.calls.append(dict(kwargs))
        if not self.ok:
            return {"ok": False, "error": "firm_quote_provider_error"}
        spender = "0x0000000000001ff3684f28c67538d4d072c22734"
        side = str(kwargs.get("side") or "buy").strip().lower()
        base = dict(kwargs.get("base_token") or {})
        quote = dict(kwargs.get("quote_token") or {})
        input_token = base if side == "sell" else quote
        output_token = quote if side == "sell" else base
        input_amount = str(kwargs.get("requested_amount") or "")
        input_decimals = int(input_token.get("decimals") or 0)
        input_atomic = str(int(Decimal(input_amount) * (Decimal(10) ** input_decimals)))
        output_amount = "0.25" if side == "sell" else "0.0005"
        output_decimals = int(output_token.get("decimals") or 0)
        output_atomic = str(int(Decimal(output_amount) * (Decimal(10) ** output_decimals)))
        minimum_atomic = str(int(Decimal(output_atomic) * Decimal("0.99")))
        return {
            "ok": True,
            "symbol": str(kwargs.get("symbol") or "WETH-USDG"),
            "side": side,
            "amount_mode": "exact_input",
            "input_asset": str(input_token.get("symbol") or ""),
            "input_amount": input_amount,
            "input_amount_atomic": input_atomic,
            "output_asset": str(output_token.get("symbol") or ""),
            "output_amount": output_amount,
            "output_amount_atomic": output_atomic,
            "minimum_received": str(Decimal(minimum_atomic) / (Decimal(10) ** output_decimals)),
            "minimum_received_atomic": minimum_atomic,
            "quote_id": "11" * 32,
            "approval_required": True,
            "allowance": {
                "applicable": True,
                "read_method": "eth_call",
                "token": {"symbol": str(input_token.get("symbol") or "")},
                "spender": spender,
                "spender_allowlisted": True,
                "current_atomic": "0",
                "required_atomic": input_atomic,
                "shortfall_atomic": input_atomic,
                "approval_required": True,
            },
            "unsigned_transaction_plan": {
                "to": spender,
                "destination_allowlisted": True,
                "value_wei": "0",
                "native_input": False,
                "calldata_sha256": "22" * 32,
                "gas_limit": "300000",
            },
            "wallet_connection_requested": False,
            "signing_enabled": False,
            "broadcast_enabled": False,
        }


class RobinhoodChainRegistryDiscoveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite+pysqlite:///:memory:")
        for table in (
            WalletAddress.__table__,
            RobinhoodChainWalletEvent.__table__,
            TokenRegistry.__table__,
            RobinhoodChainRegistryVerification.__table__,
            RobinhoodChainPairObjective.__table__,
            RobinhoodChainPairCapability.__table__,
        ):
            table.create(self.engine)
        with self.engine.begin() as connection:
            connection.execute(text("ALTER TABLE token_registry ADD COLUMN external_price_source TEXT"))
            connection.execute(text("ALTER TABLE token_registry ADD COLUMN external_price_id TEXT"))
        self.SessionLocal = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.db: Session = self.SessionLocal()
        self.fake_rpc = _FakeRpcClient()
        self.fake_discovery = _FakeDiscoveryService()
        self.fake_planning = _FakePlanningService()
        self.fake_uniswap = _FakeUniswapService()
        self.fake_uniswap_v3 = _FakeUniswapV3Service()
        self.service = RobinhoodChainRegistryDiscoveryService(
            rpc_client=self.fake_rpc,
            discovery_service=self.fake_discovery,
            planning_service=self.fake_planning,
            uniswap_service=self.fake_uniswap,
            uniswap_v3_service=self.fake_uniswap_v3,
        )

    def tearDown(self) -> None:
        self.db.close()
        self.engine.dispose()

    def _token(
        self,
        symbol: str,
        address: str | None,
        decimals: int,
        *,
        venue: str | None = None,
        price_source: str | None = None,
        label: str | None = None,
    ) -> TokenRegistry:
        row = TokenRegistry(
            chain="robinhood_chain",
            venue=venue,
            symbol=symbol,
            address=address,
            decimals=decimals,
            label=label or symbol,
        )
        self.db.add(row)
        self.db.commit()
        self.db.refresh(row)
        if price_source is not None:
            self.db.execute(
                text(
                    "UPDATE token_registry SET external_price_source = :source WHERE id = :id"
                ),
                {"source": price_source, "id": int(row.id)},
            )
            self.db.commit()
        return row

    def _wallet(self, address: str = "0x" + "aa" * 20) -> WalletAddress:
        row = WalletAddress(
            asset="ALL",
            network="robinhood_chain",
            wallet_id="robinhood_chain",
            address=address,
            owner_scope="user",
        )
        self.db.add(row)
        self.db.commit()
        self.db.refresh(row)
        return row

    def _wallet_event(
        self,
        wallet: WalletAddress,
        *,
        contract: str,
        symbol: str,
        decimals: int,
        event_key: str,
        amount_atomic: str = "1",
        registered: bool = False,
        registry_id: int | None = None,
    ) -> RobinhoodChainWalletEvent:
        row = RobinhoodChainWalletEvent(
            wallet_address_id=str(wallet.id),
            chain_id=4663,
            event_key=event_key,
            transaction_hash="0x" + event_key[-1:] * 64,
            event_type="erc20_transfer",
            status="ok",
            classification="erc20_transfer",
            direction="in",
            asset=symbol,
            amount_atomic=str(amount_atomic),
            decimals=int(decimals),
            fee_wei="0",
            contract_address=contract.lower(),
            registry_id=registry_id,
            registered=bool(registered),
            source="blockscout_v2",
            raw={
                "provider_raw": {
                    "provider_symbol": symbol,
                    "provider_decimals": int(decimals),
                    "token_name": f"{symbol} Token",
                }
            },
        )
        self.db.add(row)
        self.db.commit()
        self.db.refresh(row)
        return row

    def _mark_verified(self, row: TokenRegistry) -> None:
        self.db.add(
            RobinhoodChainRegistryVerification(
                token_registry_id=int(row.id),
                chain_id=4663,
                asset_kind="native" if not row.address else "erc20",
                code_present=None if not row.address else True,
                onchain_symbol=row.symbol,
                onchain_name=row.label,
                onchain_decimals=int(row.decimals),
                registry_match=True,
                canonical_status="verified",
                evidence={
                    "test": True,
                    "registry_symbol": str(row.symbol or "").strip().upper(),
                    "registry_decimals": int(row.decimals),
                    "registry_contract_address": row.address,
                },
            )
        )
        self.db.commit()

    def _r5c4a_capability(self) -> tuple[TokenRegistry, TokenRegistry, RobinhoodChainPairCapability]:
        self._token("ETH", None, 18)
        weth = self._token("WETH", "0x" + "31" * 20, 18)
        usdg = self._token("USDG", "0x" + "41" * 20, 6)
        self._mark_verified(weth)
        self._mark_verified(usdg)
        objective = RobinhoodChainPairObjective(
            id="r5c4a-objective",
            base_token_registry_id=int(weth.id),
            quote_token_registry_id=int(usdg.id),
            symbol="WETH-USDG",
            mechanism="swap",
            enabled=True,
            review_only=True,
        )
        self.db.add(objective)
        self.db.flush()
        capability = RobinhoodChainPairCapability(
            id="r5c4a-capability",
            objective_id=objective.id,
            from_token_registry_id=int(usdg.id),
            to_token_registry_id=int(weth.id),
            amount_mode="exact_input",
            provider="0x",
            indicative_status="available",
            firm_plan_status="not_tested",
            execution_status="disabled",
            enabled=False,
            probe_amount="1",
            evidence={"liquidity_available": True, "read_only": True},
        )
        self.db.add(capability)
        self.db.commit()
        return weth, usdg, capability

    def _r5c4b_capability(self) -> tuple[TokenRegistry, TokenRegistry, RobinhoodChainPairCapability]:
        self._token("ETH", None, 18)
        weth = self._token("WETH", "0x" + "31" * 20, 18)
        usdg = self._token("USDG", "0x" + "41" * 20, 6)
        self._mark_verified(weth)
        self._mark_verified(usdg)
        objective = RobinhoodChainPairObjective(
            id="r5c4b-objective",
            base_token_registry_id=int(weth.id),
            quote_token_registry_id=int(usdg.id),
            symbol="WETH-USDG",
            mechanism="swap",
            enabled=True,
            review_only=True,
        )
        self.db.add(objective)
        self.db.flush()
        capability = RobinhoodChainPairCapability(
            id="r5c4b-capability",
            objective_id=objective.id,
            from_token_registry_id=int(weth.id),
            to_token_registry_id=int(usdg.id),
            amount_mode="exact_input",
            provider="0x",
            indicative_status="available",
            firm_plan_status="not_tested",
            execution_status="disabled",
            enabled=False,
            probe_amount="0.0001",
            evidence={"liquidity_available": True, "read_only": True},
        )
        self.db.add(capability)
        self.db.commit()
        return weth, usdg, capability

    def test_status_is_review_only_and_has_no_hardcoded_identity_flags(self) -> None:
        status = self.service.status(self.db)
        self.assertTrue(status["token_registry_authority"])
        self.assertFalse(status["native_identity_ready"])
        self.assertEqual(status["native_identity_error"], "robinhood_chain_native_registry_identity_not_found")
        self.assertFalse(status["hardcoded_native_symbol"])
        self.assertFalse(status["hardcoded_native_decimals"])
        self.assertFalse(status["hardcoded_token_contracts"])
        self.assertFalse(status["hardcoded_pair_contracts"])
        self.assertFalse(status["execution_enabled"])
        self.assertFalse(status["signing_enabled"])
        self.assertFalse(status["broadcast_enabled"])
        self.assertFalse(status["automatic_execution_promotion"])
        self.assertFalse(status["will_mutate_chain"])

    def test_native_identity_is_resolved_from_blank_registry_address(self) -> None:
        row = self._token("GASX", None, 9)
        identity = self.service.token_identity(self.db, row)
        self.assertTrue(identity["native"])
        self.assertEqual(identity["asset_kind"], "native")
        self.assertEqual(identity["symbol"], "GASX")
        self.assertEqual(identity["registry_id"], row.id)
        self.assertEqual(identity["decimals"], 9)
        self.assertIsNone(identity["registry_contract_address"])

    def test_erc20_identity_uses_registry_address_decimals_and_price_metadata(self) -> None:
        address = "0x" + "12" * 20
        row = self._token("ALPHA", address, 7, price_source="stable")
        identity = self.service.token_identity(self.db, row)
        self.assertFalse(identity["native"])
        self.assertEqual(identity["contract_address"].lower(), address.lower())
        self.assertEqual(identity["decimals"], 7)
        self.assertEqual(identity["external_price_source"], "stable")
        self.assertEqual(identity["identity_source"], "token_registry")

    def test_ambiguous_effective_native_registry_identity_fails_closed(self) -> None:
        self._token("GASX", None, 9)
        self._token("FUEL", None, 7, venue="robinhood_chain")
        with self.assertRaisesRegex(ValueError, "ambiguous_robinhood_chain_native_registry_identity"):
            self.service.registry_rows(self.db)

    async def test_verify_native_identity_persists_verified_record_without_contract_call(self) -> None:
        row = self._token("GASX", None, 9, label="Native Gas")
        result = await self.service.verify_asset(
            self.db,
            token_registry_id=row.id,
            force_refresh=True,
            confirm_verify=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["verification"]["canonical_status"], "verified")
        self.assertIsNone(result["verification"]["code_present"])
        self.assertEqual(result["verification"]["onchain_symbol"], "GASX")
        self.assertEqual(result["verification"]["onchain_decimals"], 9)
        self.assertFalse(any(method == "eth_getCode" for method, _ in self.fake_rpc.calls))

    async def test_verify_erc20_identity_matches_onchain_metadata(self) -> None:
        address = "0x" + "34" * 20
        row = self._token("BETA", address, 18, label="Beta Token")
        self.fake_rpc.metadata[address.lower()] = {
            "symbol": "BETA",
            "name": "Beta Token",
            "decimals": 18,
            "code": True,
        }
        result = await self.service.verify_asset(
            self.db,
            token_registry_id=row.id,
            force_refresh=True,
            confirm_verify=True,
        )
        self.assertTrue(result["ok"])
        verification = result["verification"]
        self.assertTrue(verification["code_present"])
        self.assertEqual(verification["onchain_symbol"], "BETA")
        self.assertEqual(verification["onchain_decimals"], 18)
        self.assertTrue(verification["registry_match"])

    async def test_verify_erc20_mismatch_is_persisted_and_not_accepted(self) -> None:
        address = "0x" + "56" * 20
        row = self._token("GAMMA", address, 18)
        self.fake_rpc.metadata[address.lower()] = {
            "symbol": "OTHER",
            "name": "Other",
            "decimals": 6,
            "code": True,
        }
        result = await self.service.verify_asset(
            self.db,
            token_registry_id=row.id,
            force_refresh=True,
            confirm_verify=True,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["verification"]["canonical_status"], "registry_mismatch")
        self.assertFalse(result["verification"]["registry_match"])
        self.assertFalse(result["execution_enabled"])

    def test_create_objective_requires_confirmation_and_uses_registry_ids(self) -> None:
        base = self._token("AAA", "0x" + "78" * 20, 18)
        quote = self._token("BBB", "0x" + "9a" * 20, 6)
        with self.assertRaisesRegex(ValueError, "confirm_pair_objective_create_required"):
            self.service.create_objective(
                self.db,
                base_token_registry_id=base.id,
                quote_token_registry_id=quote.id,
                mechanism=MECHANISM_SWAP,
                notes=None,
                confirm_create=False,
            )
        result = self.service.create_objective(
            self.db,
            base_token_registry_id=base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes="operator objective",
            confirm_create=True,
        )
        objective = result["objective"]
        self.assertEqual(objective["symbol"], "AAA-BBB")
        self.assertEqual(objective["base"]["registry_id"], base.id)
        self.assertEqual(objective["quote"]["registry_id"], quote.id)
        self.assertTrue(objective["review_only"])
        self.assertFalse(result["execution_enabled"])

    def test_selected_pair_objective_can_require_verified_registry_identities(self) -> None:
        base = self._token("AAA", "0x" + "7c" * 20, 18)
        quote = self._token("BBB", "0x" + "9d" * 20, 6)

        with self.assertRaisesRegex(
            ValueError,
            "pair_discovery_requires_verified_registry_identity",
        ):
            self.service.create_objective(
                self.db,
                base_token_registry_id=base.id,
                quote_token_registry_id=quote.id,
                mechanism=MECHANISM_SWAP,
                notes=None,
                confirm_create=True,
                require_verified_registry_identities=True,
            )

        self._mark_verified(base)
        self._mark_verified(quote)
        result = self.service.create_objective(
            self.db,
            base_token_registry_id=base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes=None,
            confirm_create=True,
            require_verified_registry_identities=True,
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["registry_verification_required"])
        self.assertTrue(result["registry_verified"])
        self.assertTrue(result["objective"]["review_only"])
        self.assertFalse(result["provider_contacted"])
        self.assertFalse(result["rpc_contacted"])
        self.assertFalse(result["execution_enabled"])
        self.assertEqual(self.fake_rpc.calls, [])
        self.assertEqual(self.fake_discovery.calls, [])
        self.assertEqual(self.fake_planning.calls, [])

        base.address = "0x" + "7e" * 20
        self.db.commit()
        with self.assertRaisesRegex(
            ValueError,
            "pair_discovery_requires_verified_registry_identity",
        ):
            self.service.create_objective(
                self.db,
                base_token_registry_id=base.id,
                quote_token_registry_id=quote.id,
                mechanism=MECHANISM_SWAP,
                notes=None,
                confirm_create=True,
                require_verified_registry_identities=True,
            )

        stale_asset = next(
            item for item in self.service.assets(self.db)
            if int(item["registry_id"]) == int(base.id)
        )
        self.assertEqual(
            stale_asset["verification"]["canonical_status"],
            "registry_changed_since_verification",
        )
        self.assertFalse(stale_asset["verification"]["registry_match"])

    def test_create_objective_is_idempotent_and_provider_free(self) -> None:
        base = self._token("AAA", "0x" + "79" * 20, 18)
        quote = self._token("BBB", "0x" + "9b" * 20, 6)

        first = self.service.create_objective(
            self.db,
            base_token_registry_id=base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes="operator objective",
            confirm_create=True,
        )
        second = self.service.create_objective(
            self.db,
            base_token_registry_id=base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes="operator objective",
            confirm_create=True,
        )

        self.assertTrue(first["created"])
        self.assertFalse(first["idempotent"])
        self.assertTrue(first["database_mutated"])
        self.assertFalse(second["created"])
        self.assertFalse(second["updated"])
        self.assertTrue(second["idempotent"])
        self.assertFalse(second["database_mutated"])
        self.assertEqual(first["objective"]["id"], second["objective"]["id"])
        self.assertEqual(
            self.db.query(RobinhoodChainPairObjective).count(),
            1,
        )
        for result in (first, second):
            self.assertTrue(result["selected_pair_only"])
            self.assertTrue(result["objective"]["review_only"])
            self.assertFalse(result["execution_enabled"])
            self.assertFalse(result["provider_contacted"])
            self.assertFalse(result["rpc_contacted"])
            self.assertFalse(result["wallet_connection_requested"])
            self.assertFalse(result["signing_enabled"])
            self.assertFalse(result["broadcast_enabled"])
            self.assertFalse(result["automatic_execution_promotion"])
            self.assertFalse(result["will_mutate_chain"])
        self.assertEqual(self.fake_rpc.calls, [])
        self.assertEqual(self.fake_discovery.calls, [])
        self.assertEqual(self.fake_planning.calls, [])

    def test_create_objective_requires_effective_registry_identities(self) -> None:
        global_base = self._token("AAA", "0x" + "7a" * 20, 18)
        venue_base = self._token(
            "AAA",
            "0x" + "7b" * 20,
            18,
            venue="robinhood_chain",
        )
        quote = self._token("BBB", "0x" + "9c" * 20, 6)

        with self.assertRaisesRegex(
            ValueError,
            "pair_objective_requires_effective_registry_identity",
        ):
            self.service.create_objective(
                self.db,
                base_token_registry_id=global_base.id,
                quote_token_registry_id=quote.id,
                mechanism=MECHANISM_SWAP,
                notes=None,
                confirm_create=True,
            )

        result = self.service.create_objective(
            self.db,
            base_token_registry_id=venue_base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes=None,
            confirm_create=True,
        )
        self.assertEqual(
            result["objective"]["base"]["registry_id"],
            venue_base.id,
        )
        self.assertEqual(
            result["objective"]["quote"]["registry_id"],
            quote.id,
        )

    async def test_pair_discovery_requires_verified_registry_identities(self) -> None:
        base = self._token("AAA", "0x" + "ab" * 20, 18)
        quote = self._token("BBB", "0x" + "cd" * 20, 6)
        objective = self.service.create_objective(
            self.db,
            base_token_registry_id=base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes=None,
            confirm_create=True,
        )["objective"]
        with self.assertRaisesRegex(ValueError, "pair_discovery_requires_verified_registry_identity"):
            await self.service.discover_objective(
                self.db,
                objective_id=objective["id"],
                taker_address="0x" + "ef" * 20,
                base_probe_amount="1",
                quote_probe_amount="1",
                force_refresh=True,
                confirm_discovery=True,
            )
        self.assertEqual(self.fake_discovery.calls, [])

    async def test_swap_discovery_probes_both_directions_but_stays_disabled(self) -> None:
        base = self._token("AAA", "0x" + "10" * 20, 18)
        quote = self._token("BBB", "0x" + "20" * 20, 6, price_source="stable")
        self._mark_verified(base)
        self._mark_verified(quote)
        objective = self.service.create_objective(
            self.db,
            base_token_registry_id=base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes=None,
            confirm_create=True,
        )["objective"]
        result = await self.service.discover_objective(
            self.db,
            objective_id=objective["id"],
            taker_address="0x" + "30" * 20,
            base_probe_amount="0.01",
            quote_probe_amount="10",
            force_refresh=True,
            confirm_discovery=True,
        )
        self.assertEqual(len(self.fake_discovery.calls), 2)
        self.assertEqual(self.fake_discovery.calls[0]["sell_amount"], "0.01")
        self.assertEqual(self.fake_discovery.calls[1]["sell_amount"], "10")
        self.assertTrue(all(call["require_live_verified"] is False for call in self.fake_discovery.calls))
        self.assertEqual(len(result["results"]), 2)
        for capability in result["results"]:
            self.assertEqual(capability["indicative_status"], "available")
            self.assertEqual(capability["execution_status"], "disabled")
            self.assertFalse(capability["enabled"])
            self.assertFalse(capability["execution_enabled"])
        self.assertFalse(result["automatic_execution_promotion"])

    async def test_partial_provider_failure_is_persisted_without_erasing_success(self) -> None:
        self.fake_discovery.results = [
            {
                "ok": True,
                "liquidity_available": True,
                "sell_amount": "1",
                "buy_amount": "2",
                "price_impact_bps": "1",
                "route": {"fills": [{"source": "RFQ"}]},
                "provider_contacted": True,
            },
            {
                "ok": False,
                "error": "provider_transient_error",
                "http_status": 500,
                "backoff_until": "2026-07-20T01:00:00+00:00",
                "provider_contacted": True,
            },
        ]
        base = self._token("AAA", "0x" + "40" * 20, 18)
        quote = self._token("BBB", "0x" + "50" * 20, 18)
        self._mark_verified(base)
        self._mark_verified(quote)
        objective = self.service.create_objective(
            self.db,
            base_token_registry_id=base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes=None,
            confirm_create=True,
        )["objective"]
        result = await self.service.discover_objective(
            self.db,
            objective_id=objective["id"],
            taker_address="0x" + "60" * 20,
            base_probe_amount="1",
            quote_probe_amount="1",
            force_refresh=True,
            confirm_discovery=True,
        )
        statuses = [item["indicative_status"] for item in result["results"]]
        self.assertEqual(statuses, ["available", "provider_transient_error"])
        self.assertEqual(len(self.service.route_capabilities(self.db)), 2)
        self.assertFalse(any(item["enabled"] for item in result["results"]))

    async def test_wrap_unwrap_records_two_review_only_capabilities_without_provider_call(self) -> None:
        wrapped = self._token("WRAPPED", "0x" + "70" * 20, 18)
        native = self._token("GASX", None, 9)
        self._mark_verified(wrapped)
        self._mark_verified(native)
        objective = self.service.create_objective(
            self.db,
            base_token_registry_id=wrapped.id,
            quote_token_registry_id=native.id,
            mechanism=MECHANISM_WRAP_UNWRAP,
            notes="native conversion",
            confirm_create=True,
        )["objective"]
        result = await self.service.discover_objective(
            self.db,
            objective_id=objective["id"],
            taker_address="0x" + "80" * 20,
            base_probe_amount="0.001",
            quote_probe_amount="0.001",
            force_refresh=True,
            confirm_discovery=True,
        )
        self.assertEqual(self.fake_discovery.calls, [])
        self.assertEqual(len(result["results"]), 2)
        for capability in result["results"]:
            self.assertEqual(capability["provider"], "native_wrap")
            self.assertEqual(capability["indicative_status"], "mechanism_configured")
            self.assertFalse(capability["evidence"]["provider_contacted"])
            self.assertFalse(capability["execution_enabled"])

    async def test_successful_discovery_never_automatically_promotes_execution(self) -> None:
        base = self._token("AAA", "0x" + "90" * 20, 18)
        quote = self._token("BBB", "0x" + "a0" * 20, 18)
        self._mark_verified(base)
        self._mark_verified(quote)
        objective = self.service.create_objective(
            self.db,
            base_token_registry_id=base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes=None,
            confirm_create=True,
        )["objective"]
        await self.service.discover_objective(
            self.db,
            objective_id=objective["id"],
            taker_address="0x" + "b0" * 20,
            base_probe_amount="1",
            quote_probe_amount="1",
            force_refresh=True,
            confirm_discovery=True,
        )
        for row in self.db.query(RobinhoodChainPairCapability).all():
            self.assertFalse(row.enabled)
            self.assertEqual(row.execution_status, "disabled")

    async def test_r5c4a_preparation_verification_requires_explicit_confirmation(self) -> None:
        self._r5c4a_capability()
        with self.assertRaisesRegex(ValueError, "confirm_r5c4a_preparation_verification_required"):
            await self.service.verify_preparation_authority(
                self.db, symbol="WETH-USDG", side="buy", amount_mode="exact_input",
                requested_amount="1", taker_address="0x" + "51" * 20,
                slippage_bps=100, confirm_verify=False,
            )
        self.assertEqual(self.fake_planning.calls, [])

    async def test_r5c4a_wrong_target_is_blocked_before_provider(self) -> None:
        self._r5c4a_capability()
        with self.assertRaisesRegex(ValueError, "r5c4a_preparation_target_locked"):
            await self.service.verify_preparation_authority(
                self.db, symbol="WETH-USDG", side="buy", amount_mode="exact_input",
                requested_amount="1.01", taker_address="0x" + "51" * 20,
                slippage_bps=100, confirm_verify=True,
            )
        self.assertEqual(self.fake_planning.calls, [])

    async def test_r5c4a_success_persists_bounded_preparation_not_live_execution(self) -> None:
        _, _, capability = self._r5c4a_capability()
        result = await self.service.verify_preparation_authority(
            self.db, symbol="WETH-USDG", side="buy", amount_mode="exact_input",
            requested_amount="1", taker_address="0x" + "51" * 20,
            slippage_bps=100, confirm_verify=True,
        )
        self.assertTrue(result["ok"])
        self.assertFalse(result["idempotent"])
        self.assertTrue(result["database_mutated"])
        self.assertFalse(result["broadcast_enabled"])
        self.assertFalse(result["successful_broadcast"])
        self.assertTrue(result["initial_acceptance_wallet_reject_only"])
        self.assertEqual(len(self.fake_planning.calls), 1)
        call = self.fake_planning.calls[0]
        self.assertEqual(call["symbol"], "WETH-USDG")
        self.assertEqual(call["requested_amount"], "1")
        self.assertEqual(call["base_token"]["symbol"], "WETH")
        self.assertFalse(call["base_token"]["native"])
        self.assertEqual(call["quote_token"]["symbol"], "USDG")
        self.db.refresh(capability)
        self.assertTrue(capability.enabled)
        self.assertEqual(capability.firm_plan_status, "available")
        self.assertEqual(capability.execution_status, PREPARATION_STATUS)
        self.assertEqual(capability.probe_amount, "1")
        self.assertTrue(capability.evidence["preparation_verified"])
        self.assertFalse(capability.evidence["live_accepted"])
        self.assertFalse(capability.evidence["successful_broadcast"])
        authority = result["execution_authority"]
        self.assertEqual(authority["authority_level"], PREPARATION_STATUS)
        self.assertTrue(authority["execution_permitted"])
        self.assertFalse(authority["live_execution_verified"])
        self.assertFalse(authority["successful_broadcast_authorized"])

    async def test_r5c4a_repeat_is_idempotent_without_provider_contact(self) -> None:
        self._r5c4a_capability()
        kwargs = dict(
            symbol="WETH-USDG", side="buy", amount_mode="exact_input",
            requested_amount="1", taker_address="0x" + "51" * 20,
            slippage_bps=100, confirm_verify=True,
        )
        first = await self.service.verify_preparation_authority(self.db, **kwargs)
        second = await self.service.verify_preparation_authority(self.db, **kwargs)
        self.assertFalse(first["idempotent"])
        self.assertTrue(second["idempotent"])
        self.assertFalse(second["database_mutated"])
        self.assertIsNone(second["firm_plan"])
        self.assertEqual(len(self.fake_planning.calls), 1)

    async def test_r5c5a_live_authorization_requires_explicit_confirmation(self) -> None:
        self._r5c4a_capability()
        await self.service.verify_preparation_authority(
            self.db, symbol="WETH-USDG", side="buy", amount_mode="exact_input",
            requested_amount="1", taker_address="0x" + "51" * 20,
            slippage_bps=100, confirm_verify=True,
        )
        with self.assertRaisesRegex(ValueError, "confirm_r5c5a_live_authorization_required"):
            self.service.authorize_controlled_live_buy(
                self.db, symbol="WETH-USDG", side="buy", amount_mode="exact_input",
                requested_amount="1", provider="0x", wallet_address="0x" + "51" * 20,
                confirm_authorize=False,
            )

    async def test_r5c5a_live_authorization_is_exact_wallet_bound_and_not_live_verified(self) -> None:
        _, _, capability = self._r5c4a_capability()
        await self.service.verify_preparation_authority(
            self.db, symbol="WETH-USDG", side="buy", amount_mode="exact_input",
            requested_amount="1", taker_address="0x" + "51" * 20,
            slippage_bps=100, confirm_verify=True,
        )
        calls_before = len(self.fake_planning.calls)
        result = self.service.authorize_controlled_live_buy(
            self.db, symbol="WETH-USDG", side="buy", amount_mode="exact_input",
            requested_amount="1", provider="0x", wallet_address="0x" + "51" * 20,
            confirm_authorize=True,
        )
        self.assertTrue(result["ok"])
        self.assertFalse(result["idempotent"])
        self.assertTrue(result["database_mutated"])
        self.assertTrue(result["successful_broadcast_authorized"])
        self.assertFalse(result["live_execution_verified"])
        self.assertFalse(result["broadcast_enabled"])
        self.assertFalse(result["automatic_second_transaction"])
        self.assertFalse(result["automatic_retry"])
        self.assertEqual(len(self.fake_planning.calls), calls_before)
        authority = result["execution_authority"]
        self.assertEqual(authority["authority_level"], "live_authorized_pending_confirmation")
        self.assertTrue(authority["successful_broadcast_authorized"])
        self.assertFalse(authority["live_execution_verified"])
        self.assertFalse(authority["initial_acceptance_wallet_reject_only"])
        self.assertEqual(authority["execution_ceiling"]["amount"], "1")
        self.db.refresh(capability)
        authorization = capability.evidence["live_authorization"]
        self.assertEqual(authorization["wallet_address"], ("0x" + "51" * 20).lower())
        self.assertEqual(authorization["approval_model"], "finite_exact_input")
        self.assertFalse(authorization["unlimited_approval_enabled"])
        self.assertEqual(authorization["approval_transaction_value_wei"], "0")
        self.assertEqual(authorization["swap_transaction_value_wei"], "0")
        self.assertTrue(authorization["separate_wallet_requests_required"])
        self.assertFalse(authorization["automatic_execution_promotion"])

        second = self.service.authorize_controlled_live_buy(
            self.db, symbol="WETH-USDG", side="buy", amount_mode="exact_input",
            requested_amount="1", provider="0x", wallet_address="0x" + "51" * 20,
            confirm_authorize=True,
        )
        self.assertTrue(second["idempotent"])
        self.assertFalse(second["database_mutated"])

    async def test_r5c5a_wrong_amount_and_sell_direction_remain_locked(self) -> None:
        self._r5c4a_capability()
        await self.service.verify_preparation_authority(
            self.db, symbol="WETH-USDG", side="buy", amount_mode="exact_input",
            requested_amount="1", taker_address="0x" + "51" * 20,
            slippage_bps=100, confirm_verify=True,
        )
        for side, amount in (("buy", "1.01"), ("sell", "1")):
            with self.assertRaisesRegex(ValueError, "r5c5a_live_authorization_target_locked"):
                self.service.authorize_controlled_live_buy(
                    self.db, symbol="WETH-USDG", side=side, amount_mode="exact_input",
                    requested_amount=amount, provider="0x", wallet_address="0x" + "51" * 20,
                    confirm_authorize=True,
                )

    async def test_r5c4b_preparation_verification_requires_explicit_confirmation(self) -> None:
        self._r5c4b_capability()
        with self.assertRaisesRegex(ValueError, "confirm_r5c4b_preparation_verification_required"):
            await self.service.verify_preparation_authority(
                self.db, symbol="WETH-USDG", side="sell", amount_mode="exact_input",
                requested_amount="0.0001", taker_address="0x" + "51" * 20,
                slippage_bps=100, confirm_verify=False,
            )
        self.assertEqual(self.fake_planning.calls, [])

    async def test_r5c4b_wrong_amount_is_blocked_before_provider(self) -> None:
        self._r5c4b_capability()
        with self.assertRaisesRegex(ValueError, "r5c4b_preparation_target_locked"):
            await self.service.verify_preparation_authority(
                self.db, symbol="WETH-USDG", side="sell", amount_mode="exact_input",
                requested_amount="0.0002", taker_address="0x" + "51" * 20,
                slippage_bps=100, confirm_verify=True,
            )
        self.assertEqual(self.fake_planning.calls, [])

    async def test_r5c4b_success_persists_bounded_sell_preparation_not_live_execution(self) -> None:
        _, _, capability = self._r5c4b_capability()
        result = await self.service.verify_preparation_authority(
            self.db, symbol="WETH-USDG", side="sell", amount_mode="exact_input",
            requested_amount="0.0001", taker_address="0x" + "51" * 20,
            slippage_bps=100, confirm_verify=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["tranche"], "R5C.4B")
        self.assertFalse(result["idempotent"])
        self.assertTrue(result["database_mutated"])
        self.assertFalse(result["broadcast_enabled"])
        self.assertFalse(result["successful_broadcast"])
        self.assertTrue(result["initial_acceptance_wallet_reject_only"])
        self.assertEqual(len(self.fake_planning.calls), 1)
        call = self.fake_planning.calls[0]
        self.assertEqual(call["symbol"], "WETH-USDG")
        self.assertEqual(call["side"], "sell")
        self.assertEqual(call["requested_amount"], "0.0001")
        self.assertEqual(call["base_token"]["symbol"], "WETH")
        self.assertEqual(call["quote_token"]["symbol"], "USDG")
        self.db.refresh(capability)
        self.assertTrue(capability.enabled)
        self.assertEqual(capability.firm_plan_status, "available")
        self.assertEqual(capability.execution_status, PREPARATION_STATUS)
        self.assertEqual(capability.probe_amount, "0.0001")
        self.assertEqual(capability.evidence["tranche"], "R5C.4B")
        self.assertEqual(capability.evidence["side"], "sell")
        self.assertEqual(capability.evidence["from_asset"], "WETH")
        self.assertEqual(capability.evidence["to_asset"], "USDG")
        self.assertEqual(capability.evidence["verified_input_amount"], "0.0001")
        self.assertTrue(capability.evidence["preparation_verified"])
        self.assertFalse(capability.evidence["live_accepted"])
        self.assertFalse(capability.evidence["successful_broadcast"])
        authority = result["execution_authority"]
        self.assertEqual(authority["authority_level"], PREPARATION_STATUS)
        self.assertEqual(authority["side"], "sell")
        self.assertEqual(authority["input"]["symbol"], "WETH")
        self.assertEqual(authority["output"]["symbol"], "USDG")
        self.assertEqual(authority["execution_ceiling"]["amount"], "0.0001")
        self.assertTrue(authority["execution_permitted"])
        self.assertFalse(authority["live_execution_verified"])
        self.assertFalse(authority["successful_broadcast_authorized"])

    async def test_r5c4b_repeat_is_idempotent_without_provider_contact(self) -> None:
        self._r5c4b_capability()
        kwargs = dict(
            symbol="WETH-USDG", side="sell", amount_mode="exact_input",
            requested_amount="0.0001", taker_address="0x" + "51" * 20,
            slippage_bps=100, confirm_verify=True,
        )
        first = await self.service.verify_preparation_authority(self.db, **kwargs)
        second = await self.service.verify_preparation_authority(self.db, **kwargs)
        self.assertFalse(first["idempotent"])
        self.assertTrue(second["idempotent"])
        self.assertFalse(second["database_mutated"])
        self.assertIsNone(second["firm_plan"])
        self.assertEqual(len(self.fake_planning.calls), 1)


    async def test_r5c5b_live_authorization_requires_explicit_confirmation(self) -> None:
        self._r5c4b_capability()
        await self.service.verify_preparation_authority(
            self.db,
            symbol="WETH-USDG",
            side="sell",
            amount_mode="exact_input",
            requested_amount="0.0001",
            taker_address="0x" + "51" * 20,
            slippage_bps=100,
            confirm_verify=True,
        )
        with self.assertRaisesRegex(ValueError, "confirm_r5c5b_live_authorization_required"):
            self.service.authorize_controlled_live_sell(
                self.db,
                symbol="WETH-USDG",
                side="sell",
                amount_mode="exact_input",
                requested_amount="0.0001",
                provider="0x",
                wallet_address="0x" + "51" * 20,
                confirm_authorize=False,
            )

    async def test_r5c5b_live_authorization_is_exact_wallet_bound_and_not_live_verified(self) -> None:
        _, _, capability = self._r5c4b_capability()
        await self.service.verify_preparation_authority(
            self.db,
            symbol="WETH-USDG",
            side="sell",
            amount_mode="exact_input",
            requested_amount="0.0001",
            taker_address="0x" + "51" * 20,
            slippage_bps=100,
            confirm_verify=True,
        )
        calls_before = len(self.fake_planning.calls)
        result = self.service.authorize_controlled_live_sell(
            self.db,
            symbol="WETH-USDG",
            side="sell",
            amount_mode="exact_input",
            requested_amount="0.0001",
            provider="0x",
            wallet_address="0x" + "51" * 20,
            confirm_authorize=True,
        )
        self.assertTrue(result["ok"])
        self.assertFalse(result["idempotent"])
        self.assertTrue(result["database_mutated"])
        self.assertEqual(result["tranche"], "R5C.5B")
        self.assertTrue(result["successful_broadcast_authorized"])
        self.assertFalse(result["live_execution_verified"])
        self.assertFalse(result["broadcast_enabled"])
        self.assertFalse(result["automatic_second_transaction"])
        self.assertFalse(result["automatic_retry"])
        self.assertEqual(len(self.fake_planning.calls), calls_before)

        authority = result["execution_authority"]
        self.assertEqual(authority["authority_level"], "live_authorized_pending_confirmation")
        self.assertEqual(authority["side"], "sell")
        self.assertEqual(authority["input"]["symbol"], "WETH")
        self.assertEqual(authority["output"]["symbol"], "USDG")
        self.assertEqual(authority["execution_ceiling"]["amount"], "0.0001")
        self.assertTrue(authority["successful_broadcast_authorized"])
        self.assertFalse(authority["live_execution_verified"])

        self.db.refresh(capability)
        authorization = capability.evidence["live_authorization"]
        self.assertEqual(authorization["tranche"], "R5C.5B")
        self.assertEqual(authorization["wallet_address"], ("0x" + "51" * 20).lower())
        self.assertEqual(authorization["side"], "sell")
        self.assertEqual(authorization["input_asset"], "WETH")
        self.assertEqual(authorization["output_asset"], "USDG")
        self.assertEqual(authorization["exact_input_amount"], "0.0001")
        self.assertEqual(authorization["approval_model"], "finite_exact_input")
        self.assertFalse(authorization["unlimited_approval_enabled"])
        self.assertEqual(authorization["approval_transaction_value_wei"], "0")
        self.assertEqual(authorization["swap_transaction_value_wei"], "0")
        self.assertTrue(authorization["separate_wallet_requests_required"])
        self.assertFalse(authorization["automatic_execution_promotion"])

        second = self.service.authorize_controlled_live_sell(
            self.db,
            symbol="WETH-USDG",
            side="sell",
            amount_mode="exact_input",
            requested_amount="0.0001",
            provider="0x",
            wallet_address="0x" + "51" * 20,
            confirm_authorize=True,
        )
        self.assertTrue(second["idempotent"])
        self.assertFalse(second["database_mutated"])
        self.assertEqual(
            second["execution_authority"]["live_authorization"]["authorization_id"],
            authorization["authorization_id"],
        )

    async def test_r5c5b_wrong_amount_and_buy_direction_remain_locked(self) -> None:
        self._r5c4b_capability()
        await self.service.verify_preparation_authority(
            self.db,
            symbol="WETH-USDG",
            side="sell",
            amount_mode="exact_input",
            requested_amount="0.0001",
            taker_address="0x" + "51" * 20,
            slippage_bps=100,
            confirm_verify=True,
        )
        for side, amount in (("sell", "0.0002"), ("buy", "0.0001")):
            with self.assertRaisesRegex(ValueError, "r5c5b_live_authorization_target_locked"):
                self.service.authorize_controlled_live_sell(
                    self.db,
                    symbol="WETH-USDG",
                    side=side,
                    amount_mode="exact_input",
                    requested_amount=amount,
                    provider="0x",
                    wallet_address="0x" + "51" * 20,
                    confirm_authorize=True,
                )

    async def test_r5c4a_failed_firm_plan_leaves_capability_disabled(self) -> None:
        _, _, capability = self._r5c4a_capability()
        self.service.planning_service = _FakePlanningService(ok=False)
        with self.assertRaisesRegex(ValueError, "firm_quote_provider_error"):
            await self.service.verify_preparation_authority(
                self.db, symbol="WETH-USDG", side="buy", amount_mode="exact_input",
                requested_amount="1", taker_address="0x" + "51" * 20,
                slippage_bps=100, confirm_verify=True,
            )
        self.db.refresh(capability)
        self.assertFalse(capability.enabled)
        self.assertEqual(capability.firm_plan_status, "not_tested")
        self.assertEqual(capability.execution_status, "disabled")
        self.assertFalse((capability.evidence or {}).get("preparation_verified", False))

    def test_explicit_historical_evidence_can_sync_arbitrary_registry_pair(self) -> None:
        base = self._token("OMEGA", "0x" + "c0" * 20, 18)
        quote = self._token("DELTA", "0x" + "d0" * 20, 6)
        row = self.service._upsert_historical_capability(
            self.db,
            symbol="OMEGA-DELTA",
            from_symbol="DELTA",
            to_symbol="OMEGA",
            amount_mode=AMOUNT_MODE_EXACT_INPUT,
            probe_amount="2",
            provider="0x",
            evidence={"live_accepted": True, "source_table": "test"},
        )
        self.assertIsNotNone(row)
        self.db.commit()
        capability = self.service.route_capability(
            self.db,
            from_token_registry_id=quote.id,
            to_token_registry_id=base.id,
            amount_mode=AMOUNT_MODE_EXACT_INPUT,
        )
        self.assertIsNotNone(capability)
        self.assertTrue(capability["enabled"])
        self.assertTrue(capability["execution_enabled"])
        self.assertEqual(capability["execution_status"], "live_verified")


    def test_venue_override_preserves_symbol_priority_without_token_fallback(self) -> None:
        global_row = self._token("ALPHA", "0x" + "d1" * 20, 6)
        override = self._token("ALPHA", "0x" + "d2" * 20, 7, venue="robinhood_chain")
        selected = self.service.resolve_token(self.db, "ALPHA")
        self.assertEqual(selected["registry_id"], override.id)
        self.assertNotEqual(selected["registry_id"], global_row.id)
        self.assertEqual(selected["decimals"], 7)
        self.assertEqual(selected["registry_venue"], "robinhood_chain")

    def test_provider_price_normalization_preserves_integer_trailing_zero(self) -> None:
        provider = RobinhoodChainExecutionDiscoveryService(
            api_base="https://example.invalid",
            timeout_s=2,
            cache_ttl_s=0,
            error_backoff_s=0,
            max_concurrent=1,
            max_sell_usd=25,
            credential_getter=lambda: {"api_key": "test", "source": "test", "venue": "zerox"},
            rpc_client=self.fake_rpc,
        )
        result = provider._normalize_provider_response(
            {
                "sellAmount": "1",
                "buyAmount": "10",
                "liquidityAvailable": True,
                "route": {"fills": []},
            },
            sell_token={
                "symbol": "AAA",
                "contract_address": "0x" + "f2" * 20,
                "decimals": 0,
                "native": False,
                "registry_id": 1,
            },
            buy_token={
                "symbol": "BBB",
                "contract_address": "0x" + "f3" * 20,
                "decimals": 0,
                "native": False,
                "registry_id": 2,
            },
            amount_mode="exact_input",
            requested_atomic="1",
            requested_display="1",
            credential_source="test",
            elapsed_ms=1.0,
        )
        self.assertEqual(result["price_buy_per_sell"], "10")
        self.assertEqual(result["price_sell_per_buy"], "0.1")

    def test_probe_amount_normalization_preserves_integer_trailing_zero(self) -> None:
        self.assertEqual(_parse_probe_amount("10", 6), "10")
        self.assertEqual(_parse_probe_amount("10.5000", 6), "10.5")

    async def test_unregistered_wallet_asset_positive_balance_is_register_ready(self) -> None:
        wallet = self._wallet()
        contract = "0x" + "91" * 20
        self._wallet_event(
            wallet,
            contract=contract,
            symbol="FORGE",
            decimals=18,
            event_key="event-forge-1",
            amount_atomic="123000000000000000000",
        )
        self.fake_rpc.metadata[contract.lower()] = {
            "code": True,
            "symbol": "FORGE",
            "name": "Forge Token",
            "decimals": 18,
            "balance_atomic": 123000000000000000000,
        }

        result = await self.service.unregistered_wallet_assets(
            self.db,
            wallet_address_id=str(wallet.id),
            limit=50,
            positive_only=True,
            force_refresh=True,
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["blockchain_read_only"])
        self.assertFalse(result["will_mutate"])
        self.assertEqual(len(result["items"]), 1)
        item = result["items"][0]
        self.assertEqual(item["contract_address"], contract.lower())
        self.assertEqual(item["symbol"], "FORGE")
        self.assertEqual(item["decimals"], 18)
        self.assertEqual(item["balance_atomic"], "123000000000000000000")
        self.assertEqual(item["balance_token"], "123")
        self.assertTrue(item["positive_balance"])
        self.assertTrue(item["ready_to_register"])
        self.assertEqual(item["metadata_status"], "ready")
        self.assertFalse(item["symbol_conflict"])

    async def test_unregistered_wallet_asset_excludes_current_registered_contract(self) -> None:
        wallet = self._wallet()
        contract = "0x" + "92" * 20
        self._wallet_event(
            wallet,
            contract=contract,
            symbol="KNOWN",
            decimals=6,
            event_key="event-known-2",
            amount_atomic="1000000",
            registered=False,
        )
        self._token("KNOWN", contract, 6)

        result = await self.service.unregistered_wallet_assets(
            self.db,
            wallet_address_id=str(wallet.id),
            positive_only=False,
        )

        self.assertEqual(result["items"], [])
        self.assertEqual(result["observed_unregistered_contracts"], 0)

    async def test_unregistered_wallet_asset_zero_balance_filter(self) -> None:
        wallet = self._wallet()
        contract = "0x" + "93" * 20
        self._wallet_event(
            wallet,
            contract=contract,
            symbol="ZERO",
            decimals=6,
            event_key="event-zero-3",
            amount_atomic="1000000",
        )
        self.fake_rpc.metadata[contract.lower()] = {
            "code": True,
            "symbol": "ZERO",
            "name": "Zero Token",
            "decimals": 6,
            "balance_atomic": 0,
        }

        positive = await self.service.unregistered_wallet_assets(
            self.db,
            wallet_address_id=str(wallet.id),
            positive_only=True,
        )
        all_items = await self.service.unregistered_wallet_assets(
            self.db,
            wallet_address_id=str(wallet.id),
            positive_only=False,
        )

        self.assertEqual(positive["items"], [])
        self.assertEqual(len(all_items["items"]), 1)
        self.assertFalse(all_items["items"][0]["positive_balance"])
        self.assertFalse(all_items["items"][0]["ready_to_register"])
        self.assertEqual(all_items["items"][0]["metadata_status"], "zero_balance")

    async def test_unregistered_wallet_asset_symbol_collision_is_quarantined_from_direct_add(self) -> None:
        wallet = self._wallet()
        registered = self._token("DUP", "0x" + "94" * 20, 18)
        contract = "0x" + "95" * 20
        self._wallet_event(
            wallet,
            contract=contract,
            symbol="DUP",
            decimals=18,
            event_key="event-dup-4",
            amount_atomic="5000000000000000000",
        )
        self.fake_rpc.metadata[contract.lower()] = {
            "code": True,
            "symbol": "DUP",
            "name": "Duplicate Symbol Token",
            "decimals": 18,
            "balance_atomic": 5000000000000000000,
        }

        result = await self.service.unregistered_wallet_assets(
            self.db,
            wallet_address_id=str(wallet.id),
            positive_only=True,
        )

        self.assertEqual(len(result["items"]), 1)
        item = result["items"][0]
        self.assertTrue(item["positive_balance"])
        self.assertTrue(item["symbol_conflict"])
        self.assertFalse(item["ready_to_register"])
        self.assertEqual(item["metadata_status"], "symbol_conflict")
        self.assertEqual(item["conflicting_registry_ids"], [int(registered.id)])

    def test_extra_registry_asset_is_not_automatically_added_as_objective(self) -> None:
        self._token("EXTRA", "0x" + "e0" * 20, 18)
        base = self._token("AAA", "0x" + "e1" * 20, 18)
        quote = self._token("BBB", "0x" + "e2" * 20, 18)
        self.service.create_objective(
            self.db,
            base_token_registry_id=base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes=None,
            confirm_create=True,
        )
        objectives = self.service.objectives(self.db)
        self.assertEqual(len(objectives), 1)
        self.assertEqual(objectives[0]["symbol"], "AAA-BBB")
        symbols = {item["symbol"] for item in self.service.assets(self.db)}
        self.assertIn("EXTRA", symbols)

    def test_wrap_unwrap_objective_requires_one_native_and_one_erc20_asset(self) -> None:
        base = self._token("AAA", "0x" + "f0" * 20, 18)
        quote = self._token("BBB", "0x" + "f1" * 20, 18)
        with self.assertRaisesRegex(
            ValueError,
            "wrap_unwrap_requires_one_native_and_one_erc20_asset",
        ):
            self.service.create_objective(
                self.db,
                base_token_registry_id=base.id,
                quote_token_registry_id=quote.id,
                mechanism=MECHANISM_WRAP_UNWRAP,
                notes=None,
                confirm_create=True,
            )

    def test_probe_status_taxonomy_preserves_specific_unavailable_states(self) -> None:
        cases = (
            ({"ok": True, "liquidity_available": True}, "available"),
            ({"ok": True, "liquidity_available": False}, "no_liquidity"),
            ({"ok": False, "error": "unsupported_discovery_pair"}, "unsupported"),
            ({"ok": False, "error": "execution_discovery_provider_transient_error"}, "provider_transient_error"),
            ({"ok": False, "error": "provider_authentication_failed"}, "provider_authentication_failed"),
            ({"ok": False, "error": "execution_discovery_not_configured"}, "provider_not_configured"),
            ({"ok": False, "error": "execution_discovery_backoff_active"}, "backoff_active"),
            ({"ok": False, "error": "invalid_registry_token_identity"}, "identity_invalid"),
            ({"ok": False, "error": "uniswap_v3_pool_not_found"}, "no_liquidity"),
            ({"ok": False, "error": "uniswap_v3_no_quotable_route"}, "no_liquidity"),
            ({"ok": False, "error": "uniswap_v3_rpc_requires_wrapped_native_identity"}, "identity_invalid"),
            ({"ok": False, "error": "not_yet_probed"}, "not_yet_probed"),
            (
                {
                    "ok": False,
                    "error": "execution_discovery_provider_error",
                    "provider_error": {"code": "INSUFFICIENT_ASSET_LIQUIDITY"},
                },
                "no_liquidity",
            ),
            (
                {
                    "ok": False,
                    "error": "execution_discovery_provider_error",
                    "provider_error": {"code": "TOKEN_NOT_TRADABLE"},
                },
                "unsupported",
            ),
            (
                {
                    "ok": False,
                    "error": "execution_discovery_provider_error",
                    "provider_error": {
                        "name": "BUY_TOKEN_NOT_AUTHORIZED_FOR_TRADE",
                        "message": "The buy token is not authorized for trade due to legal restrictions",
                    },
                },
                "legal_restriction",
            ),
            (
                {
                    "ok": False,
                    "error": "execution_discovery_provider_error",
                    "provider_error": {
                        "name": "SELL_TOKEN_NOT_AUTHORIZED_FOR_TRADE",
                        "message": "The sell token is not authorized for trade due to legal restrictions",
                    },
                },
                "legal_restriction",
            ),
            ({"ok": False, "error": "execution_discovery_provider_error"}, "provider_error"),
        )
        for result, expected in cases:
            with self.subTest(expected=expected):
                self.assertEqual(_classify_probe_result(result), expected)

    async def test_selected_market_refresh_uses_only_persisted_probe_amounts_and_stays_review_only(self) -> None:
        self.fake_discovery.results = [
            {
                "ok": True,
                "liquidity_available": False,
                "sell_amount": "0.01",
                "buy_amount": "0",
                "provider_contacted": True,
                "route": {"fills": []},
            },
            {
                "ok": False,
                "error": "execution_discovery_provider_error",
                "http_status": 422,
                "provider_error": {
                    "name": "SELL_TOKEN_NOT_AUTHORIZED_FOR_TRADE",
                    "message": "The sell token is not authorized for trade due to legal restrictions",
                },
                "provider_contacted": True,
            },
        ]
        base = self._token("ALPHA", "0x" + "c1" * 20, 18)
        quote = self._token("BETA", "0x" + "c2" * 20, 6, price_source="stable")
        self._mark_verified(base)
        self._mark_verified(quote)
        objective = self.service.create_objective(
            self.db,
            base_token_registry_id=base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes="selected refresh",
            confirm_create=True,
        )["objective"]
        for from_row, to_row, amount in (
            (base, quote, "0.01"),
            (quote, base, "1"),
        ):
            self.db.add(
                RobinhoodChainPairCapability(
                    objective_id=objective["id"],
                    from_token_registry_id=from_row.id,
                    to_token_registry_id=to_row.id,
                    amount_mode=AMOUNT_MODE_EXACT_INPUT,
                    provider="0x",
                    indicative_status="provider_error",
                    firm_plan_status="not_tested",
                    execution_status="disabled",
                    enabled=False,
                    probe_amount=amount,
                    route_sources={},
                    provider_error={"error": "stale"},
                    evidence={"provider_contacted": False},
                )
            )
        self.db.commit()

        result = await self.service.refresh_selected_market(
            self.db,
            symbol="ALPHA-BETA",
            taker_address="0x" + "c3" * 20,
            force_refresh=True,
            confirm_refresh=True,
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["selected_pair_only"])
        self.assertEqual(result["probe_direction_count"], 6)
        self.assertEqual(result["providers"], ["0x", "uniswap_api", "uniswap_v3_rpc"])
        self.assertTrue(result["provider_contacted"])
        self.assertEqual(len(self.fake_discovery.calls), 2)
        self.assertEqual(len(self.fake_uniswap.calls), 2)
        self.assertEqual(len(self.fake_uniswap_v3.calls), 2)
        self.assertEqual([call["sell_amount"] for call in self.fake_discovery.calls], ["0.01", "1"])
        self.assertEqual([call["max_probe_amount"] for call in self.fake_discovery.calls], ["0.01", "1"])
        self.assertEqual([call["requested_amount"] for call in self.fake_uniswap.calls], ["0.01", "1"])
        self.assertEqual([call["requested_amount"] for call in self.fake_uniswap_v3.calls], ["0.01", "1"])
        self.assertTrue(all(call["require_live_verified"] is False for call in self.fake_discovery.calls))
        self.assertEqual(
            [item["indicative_status"] for item in result["results"]],
            ["no_liquidity", "available", "available", "legal_restriction", "available", "available"],
        )
        self.assertIsNone(result["market"]["orderbook_reason"])
        self.assertEqual(result["market"]["preferred_orderbook_provider"], "uniswap_api")
        self.assertEqual(result["market"]["orderbook_providers"], ["uniswap_api", "uniswap_v3_rpc"])
        self.assertEqual(result["provider_contacted_direction_count"], 6)
        self.assertEqual(result["provider_http_statuses"], [422])
        self.assertEqual(
            result["provider_error_names"],
            ["SELL_TOKEN_NOT_AUTHORIZED_FOR_TRADE"],
        )
        self.assertEqual(
            result["provider_error_messages"],
            ["The sell token is not authorized for trade due to legal restrictions"],
        )
        self.assertTrue(result["market"]["orderbook_enabled"])
        self.assertFalse(result["execution_enabled"])
        self.assertFalse(result["automatic_execution_promotion"])
        self.assertIsNone(result["transaction_calldata"])
        for row in self.db.query(RobinhoodChainPairCapability).all():
            self.assertFalse(row.enabled)
            self.assertEqual(row.firm_plan_status, "not_tested")
            self.assertEqual(row.execution_status, "disabled")

    async def test_selected_market_refresh_never_demotes_existing_verified_authority(self) -> None:
        self.fake_discovery.results = [
            {"ok": True, "liquidity_available": False, "provider_contacted": True},
            {"ok": True, "liquidity_available": True, "provider_contacted": True},
        ]
        base = self._token("ALPHA", "0x" + "e1" * 20, 18)
        quote = self._token("BETA", "0x" + "e2" * 20, 6)
        self._mark_verified(base)
        self._mark_verified(quote)
        objective = self.service.create_objective(
            self.db,
            base_token_registry_id=base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes="preserve authority",
            confirm_create=True,
        )["objective"]
        live_row = RobinhoodChainPairCapability(
            objective_id=objective["id"],
            from_token_registry_id=base.id,
            to_token_registry_id=quote.id,
            amount_mode=AMOUNT_MODE_EXACT_INPUT,
            provider="0x",
            indicative_status="live_verified",
            firm_plan_status="live_verified",
            execution_status="live_verified",
            enabled=True,
            probe_amount="0.01",
            route_sources={},
            provider_error={},
            evidence={"provider_contacted": True},
        )
        review_row = RobinhoodChainPairCapability(
            objective_id=objective["id"],
            from_token_registry_id=quote.id,
            to_token_registry_id=base.id,
            amount_mode=AMOUNT_MODE_EXACT_INPUT,
            provider="0x",
            indicative_status="provider_error",
            firm_plan_status="not_tested",
            execution_status="disabled",
            enabled=False,
            probe_amount="1",
            route_sources={},
            provider_error={},
            evidence={"provider_contacted": False},
        )
        self.db.add_all([live_row, review_row])
        self.db.commit()

        await self.service.refresh_selected_market(
            self.db,
            symbol="ALPHA-BETA",
            taker_address="0x" + "e3" * 20,
            force_refresh=True,
            confirm_refresh=True,
        )
        self.db.refresh(live_row)
        self.assertTrue(live_row.enabled)
        self.assertEqual(live_row.firm_plan_status, "live_verified")
        self.assertEqual(live_row.execution_status, "live_verified")

    async def test_selected_market_refresh_seeds_generic_probe_amounts_for_new_pair(self) -> None:
        base = self._token("ALPHA", "0x" + "d1" * 20, 18)
        quote = self._token("BETA", "0x" + "d2" * 20, 6)
        self._mark_verified(base)
        self._mark_verified(quote)
        self.service.create_objective(
            self.db,
            base_token_registry_id=base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes="missing persisted probes",
            confirm_create=True,
        )

        result = await self.service.refresh_selected_market(
            self.db,
            symbol="ALPHA-BETA",
            taker_address="0x" + "d3" * 20,
            force_refresh=True,
            confirm_refresh=True,
        )

        self.assertEqual(len(self.fake_discovery.calls), 2)
        self.assertEqual(len(self.fake_uniswap.calls), 2)
        self.assertEqual(len(self.fake_uniswap_v3.calls), 2)
        self.assertEqual([call["sell_amount"] for call in self.fake_discovery.calls], ["1", "1"])
        self.assertEqual([call["requested_amount"] for call in self.fake_uniswap.calls], ["1", "1"])
        self.assertEqual([call["requested_amount"] for call in self.fake_uniswap_v3.calls], ["1", "1"])
        self.assertTrue(result["provider_contacted"])
        self.assertEqual(
            [item["indicative_status"] for item in result["results"]],
            ["available", "available", "available", "available", "available", "available"],
        )
        self.assertEqual(result["market"]["preferred_orderbook_provider"], "uniswap_api")
        self.assertTrue(result["market"]["orderbook_enabled"])
        self.assertFalse(result["automatic_execution_promotion"])

    def test_market_by_symbol_returns_derived_provider_orderbook_state(self) -> None:
        base = self._token("ALPHA", "0x" + "c1" * 20, 18)
        quote = self._token("USDG", "0x" + "c2" * 20, 6, price_source="stable")
        objective = self.service.create_objective(
            self.db,
            base_token_registry_id=base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes="enriched market lookup",
            confirm_create=True,
        )["objective"]
        self.db.add_all([
            RobinhoodChainPairCapability(
                objective_id=objective["id"],
                from_token_registry_id=from_row.id,
                to_token_registry_id=to_row.id,
                amount_mode=AMOUNT_MODE_EXACT_INPUT,
                provider="uniswap_api",
                indicative_status="available",
                firm_plan_status="not_tested",
                execution_status="disabled",
                enabled=False,
                probe_amount=amount,
                evidence={"provider_contacted": True},
            )
            for from_row, to_row, amount in (
                (base, quote, "1"),
                (quote, base, "1"),
            )
        ])
        self.db.commit()

        market = self.service.market_by_symbol(self.db, "alpha-usdg")

        self.assertEqual(market["symbol"], "ALPHA-USDG")
        self.assertTrue(market["orderbook_enabled"])
        self.assertEqual(market["orderbook_providers"], ["uniswap_api"])
        self.assertEqual(market["preferred_orderbook_provider"], "uniswap_api")

    def test_orderbook_route_uses_enriched_market_lookup(self) -> None:
        router_path = Path(__file__).resolve().parents[1] / "app" / "routers" / "robinhood_chain.py"
        source = router_path.read_text(encoding="utf-8")
        start = source.index('@router.get("/orderbook")')
        end = source.find("\n@router.", start + 1)
        orderbook_source = source[start:] if end < 0 else source[start:end]

        self.assertIn(
            "market = registry_service.market_by_symbol(db, symbol)",
            orderbook_source,
        )
        self.assertNotIn(
            "market = registry_service.objective_by_symbol(db, symbol)",
            orderbook_source,
        )

    def test_market_catalog_preserves_complete_live_execution_provider_preference(self) -> None:
        base = self._token("ALPHA", "0x" + "e4" * 20, 18)
        quote = self._token("USDG", "0x" + "e5" * 20, 6, price_source="stable")
        objective = self.service.create_objective(
            self.db,
            base_token_registry_id=base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes="preserve live provider",
            confirm_create=True,
        )["objective"]
        rows = []
        for provider, enabled, execution_status in (
            ("0x", True, "live_verified"),
            ("uniswap_api", False, "disabled"),
        ):
            for from_row, to_row, amount in (
                (base, quote, "1"),
                (quote, base, "1"),
            ):
                rows.append(
                    RobinhoodChainPairCapability(
                        objective_id=objective["id"],
                        from_token_registry_id=from_row.id,
                        to_token_registry_id=to_row.id,
                        amount_mode=AMOUNT_MODE_EXACT_INPUT,
                        provider=provider,
                        indicative_status="live_verified" if enabled else "available",
                        firm_plan_status="live_verified" if enabled else "not_tested",
                        execution_status=execution_status,
                        enabled=enabled,
                        probe_amount=amount,
                        route_sources={},
                        provider_error={},
                        evidence={"provider_contacted": True},
                    )
                )
        self.db.add_all(rows)
        self.db.commit()

        market = self.service.market_catalog(self.db)[0]

        self.assertEqual(market["orderbook_providers"], ["0x", "uniswap_api"])
        self.assertEqual(market["live_execution_orderbook_providers"], ["0x"])
        self.assertEqual(market["preferred_orderbook_provider"], "0x")
        self.assertTrue(market["orderbook_enabled"])

    def test_market_catalog_never_combines_directions_from_different_providers(self) -> None:
        base = self._token("ALPHA", "0x" + "f1" * 20, 18)
        quote = self._token("USDG", "0x" + "f2" * 20, 6, price_source="stable")
        objective = self.service.create_objective(
            self.db,
            base_token_registry_id=base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes="same provider book",
            confirm_create=True,
        )["objective"]
        self.db.add_all([
            RobinhoodChainPairCapability(
                objective_id=objective["id"],
                from_token_registry_id=base.id,
                to_token_registry_id=quote.id,
                amount_mode=AMOUNT_MODE_EXACT_INPUT,
                provider="0x",
                indicative_status="available",
                firm_plan_status="not_tested",
                execution_status="disabled",
                enabled=False,
                probe_amount="1",
            ),
            RobinhoodChainPairCapability(
                objective_id=objective["id"],
                from_token_registry_id=quote.id,
                to_token_registry_id=base.id,
                amount_mode=AMOUNT_MODE_EXACT_INPUT,
                provider="uniswap_api",
                indicative_status="available",
                firm_plan_status="not_tested",
                execution_status="disabled",
                enabled=False,
                probe_amount="1",
            ),
        ])
        self.db.commit()
        market = self.service.market_catalog(self.db)[0]
        self.assertFalse(market["orderbook_enabled"])
        self.assertEqual(market["orderbook_providers"], [])
        self.assertIsNone(market["preferred_orderbook_provider"])
        self.assertEqual(
            market["orderbook_reason"],
            "same_provider_both_exact_input_directions_not_available",
        )

    async def test_selected_market_refresh_isolates_provider_exception(self) -> None:
        class _ExplodingV3:
            async def probe(self, **kwargs: Any) -> Dict[str, Any]:
                raise RuntimeError("synthetic provider failure")

        self.service.uniswap_v3_service = _ExplodingV3()
        base = self._token("ALPHA", "0x" + "a7" * 20, 18)
        quote = self._token("BETA", "0x" + "a8" * 20, 6)
        self._mark_verified(base)
        self._mark_verified(quote)
        self.service.create_objective(
            self.db,
            base_token_registry_id=base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes="provider isolation",
            confirm_create=True,
        )

        result = await self.service.refresh_selected_market(
            self.db,
            symbol="ALPHA-BETA",
            taker_address="0x" + "a9" * 20,
            force_refresh=True,
            confirm_refresh=True,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["probe_direction_count"], 6)
        v3_rows = [item for item in result["results"] if item["provider"] == "uniswap_v3_rpc"]
        self.assertEqual(len(v3_rows), 2)
        self.assertTrue(all(item["indicative_status"] == "provider_error" for item in v3_rows))
        self.assertEqual(result["market"]["preferred_orderbook_provider"], "uniswap_api")
        self.assertTrue(result["market"]["orderbook_enabled"])

    def test_capability_get_or_create_uses_atomic_sqlite_conflict_handling(self) -> None:
        base = self._token("ALPHA", "0x" + "b1" * 20, 18)
        quote = self._token("BETA", "0x" + "b2" * 20, 6)
        self._mark_verified(base)
        self._mark_verified(quote)
        objective = self.service.create_objective(
            self.db,
            base_token_registry_id=base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes="atomic capability row",
            confirm_create=True,
        )

        statements: List[str] = []

        def capture_statement(
            connection: Any,
            cursor: Any,
            statement: str,
            parameters: Any,
            context: Any,
            executemany: bool,
        ) -> None:
            statements.append(str(statement))

        bind = self.db.get_bind()
        event.listen(bind, "before_cursor_execute", capture_statement)
        try:
            first = self.service._capability_row(
                self.db,
                objective_id=objective["objective"]["id"],
                from_token_registry_id=base.id,
                to_token_registry_id=quote.id,
                amount_mode=AMOUNT_MODE_EXACT_INPUT,
                provider="uniswap_api",
            )
            self.db.commit()
            first_id = first.id

            second = self.service._capability_row(
                self.db,
                objective_id=objective["objective"]["id"],
                from_token_registry_id=base.id,
                to_token_registry_id=quote.id,
                amount_mode=AMOUNT_MODE_EXACT_INPUT,
                provider="uniswap_api",
            )
            self.db.commit()
        finally:
            event.remove(bind, "before_cursor_execute", capture_statement)

        self.assertEqual(second.id, first_id)
        self.assertEqual(
            self.db.query(RobinhoodChainPairCapability)
            .filter(
                RobinhoodChainPairCapability.objective_id == objective["objective"]["id"],
                RobinhoodChainPairCapability.from_token_registry_id == base.id,
                RobinhoodChainPairCapability.to_token_registry_id == quote.id,
                RobinhoodChainPairCapability.amount_mode == AMOUNT_MODE_EXACT_INPUT,
                RobinhoodChainPairCapability.provider == "uniswap_api",
            )
            .count(),
            1,
        )
        normalized = " ".join(statements).upper()
        self.assertIn("ON CONFLICT", normalized)
        self.assertIn("DO NOTHING", normalized)

    def test_provider_scoped_schema_compatibility_is_idempotent_on_current_schema(self) -> None:
        result = _ensure_provider_scoped_capability_schema(self.db)
        self.assertTrue(result["checked"])
        self.assertFalse(result["migrated"])

    def test_provider_scoped_schema_compatibility_migrates_legacy_unique_boundary(self) -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
        with engine.begin() as connection:
            connection.execute(text("""
                CREATE TABLE token_registry (
                    id INTEGER NOT NULL PRIMARY KEY
                )
            """))
            connection.execute(text("""
                CREATE TABLE robinhood_chain_pair_objectives (
                    id VARCHAR(36) NOT NULL PRIMARY KEY
                )
            """))
            connection.execute(text("""
                CREATE TABLE robinhood_chain_pair_capabilities (
                    id VARCHAR(36) NOT NULL PRIMARY KEY,
                    objective_id VARCHAR(36) NOT NULL,
                    from_token_registry_id INTEGER NOT NULL,
                    to_token_registry_id INTEGER NOT NULL,
                    amount_mode VARCHAR(24) NOT NULL,
                    provider VARCHAR(32) NOT NULL,
                    indicative_status VARCHAR(32) NOT NULL,
                    firm_plan_status VARCHAR(32) NOT NULL,
                    execution_status VARCHAR(32) NOT NULL,
                    enabled BOOLEAN NOT NULL,
                    route_sources JSON,
                    probe_amount VARCHAR(80),
                    price_impact_bps FLOAT,
                    provider_error JSON,
                    backoff_until DATETIME,
                    evidence JSON,
                    last_verified_at DATETIME,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL,
                    UNIQUE (
                        objective_id,
                        from_token_registry_id,
                        to_token_registry_id,
                        amount_mode
                    )
                )
            """))
            connection.execute(text("""
                INSERT INTO token_registry (id) VALUES (1), (2)
            """))
            connection.execute(text("""
                INSERT INTO robinhood_chain_pair_objectives (id) VALUES ('objective-1')
            """))
            connection.execute(text("""
                INSERT INTO robinhood_chain_pair_capabilities (
                    id, objective_id, from_token_registry_id, to_token_registry_id,
                    amount_mode, provider, indicative_status, firm_plan_status,
                    execution_status, enabled, created_at, updated_at
                ) VALUES (
                    'capability-1', 'objective-1', 1, 2,
                    'exact_input', '0x', 'available', 'not_tested',
                    'disabled', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
            """))

        db = SessionLocal()
        try:
            result = _ensure_provider_scoped_capability_schema(db)
            self.assertTrue(result["checked"])
            self.assertTrue(result["migrated"])
            self.assertEqual(result["preserved_row_count"], 1)

            indexes = db.execute(
                text("PRAGMA index_list('robinhood_chain_pair_capabilities')")
            ).mappings().all()
            unique_sets = []
            for index in indexes:
                if not bool(index.get("unique")):
                    continue
                name = str(index.get("name") or "").replace("'", "''")
                columns = db.execute(
                    text(f"PRAGMA index_info('{name}')")
                ).mappings().all()
                unique_sets.append(tuple(str(item.get("name") or "") for item in columns))

            self.assertIn(
                (
                    "objective_id",
                    "from_token_registry_id",
                    "to_token_registry_id",
                    "amount_mode",
                    "provider",
                ),
                unique_sets,
            )
            preserved = db.execute(
                text("""
                    SELECT id, provider, indicative_status
                    FROM robinhood_chain_pair_capabilities
                """)
            ).mappings().one()
            self.assertEqual(preserved["id"], "capability-1")
            self.assertEqual(preserved["provider"], "0x")
            self.assertEqual(preserved["indicative_status"], "available")

            db.execute(text("""
                INSERT INTO robinhood_chain_pair_capabilities (
                    id, objective_id, from_token_registry_id, to_token_registry_id,
                    amount_mode, provider, indicative_status, firm_plan_status,
                    execution_status, enabled, created_at, updated_at
                ) VALUES (
                    'capability-2', 'objective-1', 1, 2,
                    'exact_input', 'uniswap_api', 'available', 'not_tested',
                    'disabled', 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
            """))
            db.commit()
            count = db.execute(
                text("SELECT COUNT(*) FROM robinhood_chain_pair_capabilities")
            ).scalar_one()
            self.assertEqual(count, 2)
        finally:
            db.close()
            engine.dispose()

    def test_selected_pair_registration_frontend_is_explicit_and_provider_free(self) -> None:
        test_path = Path(__file__).resolve()
        root_candidates = [
            test_path.parents[2],
            test_path.parents[1],
        ]
        root = next(
            (
                candidate
                for candidate in root_candidates
                if (candidate / "frontend" / "src" / "OrderTicketWidget.jsx").exists()
            ),
            root_candidates[0],
        )
        api_source = (root / "frontend" / "src" / "lib" / "api.js").read_text(encoding="utf-8")
        ticket_source = (root / "frontend" / "src" / "OrderTicketWidget.jsx").read_text(encoding="utf-8")
        orderbook_source = (root / "frontend" / "src" / "OrderBookWidget.jsx").read_text(encoding="utf-8")
        registry_source = (root / "frontend" / "src" / "features" / "registry" / "TokenRegistryWindow.jsx").read_text(encoding="utf-8")
        router_source = (root / "backend" / "app" / "routers" / "robinhood_chain.py").read_text(encoding="utf-8")

        helper_start = api_source.index("export async function addRobinhoodChainSelectedPair")
        helper_end = api_source.index("function requireRobinhoodChainReviewRequest", helper_start)
        helper_source = api_source[helper_start:helper_end]

        self.assertIn("getRobinhoodChainRegistryAssets", api_source)
        self.assertIn("/registry-discovery/assets", api_source)
        self.assertIn("/registry-discovery/objectives", helper_source)
        self.assertIn('mechanism: "swap"', helper_source)
        self.assertIn("confirm_create: true", helper_source)
        self.assertNotIn("/refresh", helper_source)
        self.assertNotIn("provider.request", helper_source)
        self.assertNotIn("eth_requestAccounts", helper_source)

        for source in (ticket_source, orderbook_source):
            self.assertIn("REGISTRY ASSETS FOUND · PAIR NOT CONFIGURED", source)
            self.assertIn("CANONICAL TOKEN VERIFICATION REQUIRED", source)
            self.assertIn("Add Selected Pair", source)
            self.assertIn("registry_verification_required", source)
            self.assertIn("registry_verified", source)
            self.assertIn('data-provider-contacted="false"', source)
            self.assertIn('data-execution-enabled="false"', source)
            self.assertIn("review-only", source)
            self.assertIn("separate explicit action", source)

        self.assertIn("require_verified_registry_identities=True", router_source)
        self.assertIn("Verify on-chain", registry_source)
        self.assertIn("Review vs ETH", registry_source)
        self.assertIn("Review vs USDG", registry_source)
        self.assertIn("confirm_verify: true", registry_source)
        self.assertIn("force_refresh: true", registry_source)

        self.assertIn("onClick={addSelectedRobinhoodChainPair}", ticket_source)
        self.assertIn("onClick={addSelectedRobinhoodChainPairFromBook}", orderbook_source)
        self.assertIn("robinhoodChainPairNotConfigured", ticket_source)
        self.assertIn("robinhoodChainPairNotConfigured", orderbook_source)

    def test_discovery_sources_contain_no_known_token_contract_or_pair_objectives(self) -> None:
        import inspect
        import app.services.robinhood_chain_execution_discovery as provider_module
        import app.services.robinhood_chain_registry_authority as authority_module
        import app.services.robinhood_chain_registry_discovery as registry_module

        source = (
            inspect.getsource(provider_module)
            + "\n"
            + inspect.getsource(authority_module)
            + "\n"
            + inspect.getsource(registry_module)
        ).lower()
        import re

        hex_literals = set(re.findall(r"0x[0-9a-f]{40}", source))
        self.assertEqual(hex_literals, {provider_module.ZEROX_NATIVE_TOKEN.lower()})
        self.assertNotIn("supported_output_tokens", source)
        self.assertNotIn("approval_tokens", source)
        self.assertNotIn('native_symbol = "eth"', source)
        self.assertNotIn('native_decimals = 18', source)

    def test_market_catalog_enables_only_two_direction_available_swap_books(self) -> None:
        base = self._token("ALPHA", "0x" + "a1" * 20, 18)
        quote = self._token("BETA", "0x" + "a2" * 20, 6)
        result = self.service.create_objective(
            self.db,
            base_token_registry_id=base.id,
            quote_token_registry_id=quote.id,
            mechanism=MECHANISM_SWAP,
            notes="catalog test",
            confirm_create=True,
        )
        objective_id = result["objective"]["id"]
        for from_row, to_row, amount in (
            (base, quote, "0.0005"),
            (quote, base, "1"),
        ):
            self.db.add(
                RobinhoodChainPairCapability(
                    objective_id=objective_id,
                    from_token_registry_id=from_row.id,
                    to_token_registry_id=to_row.id,
                    amount_mode=AMOUNT_MODE_EXACT_INPUT,
                    provider="0x",
                    indicative_status="available",
                    firm_plan_status="not_tested",
                    execution_status="disabled",
                    enabled=False,
                    probe_amount=amount,
                    route_sources={"sources": ["Uniswap_V3"]},
                    provider_error={},
                    evidence={"provider_contacted": True},
                )
            )
        self.db.commit()

        catalog = self.service.market_catalog(self.db)
        self.assertEqual(len(catalog), 1)
        market = catalog[0]
        self.assertEqual(market["tranche"], "RH-CHAIN.10D.2-R5C.2")
        self.assertEqual(market["symbol"], "ALPHA-BETA")
        self.assertEqual(market["indicative_state"], "available")
        self.assertTrue(market["orderbook_enabled"])
        self.assertEqual(market["available_direction_count"], 2)
        self.assertFalse(market["execution_enabled"])
        self.assertFalse(market["automatic_execution_promotion"])
        self.assertEqual(market["identity_source"], "token_registry")
        self.assertEqual(market["capability_source"], "database")
        self.assertTrue(market["refresh_supported"])
        self.assertFalse(market["explicit_refresh_required"])
        self.assertEqual(market["unavailable_direction_count"], 0)

    def test_market_catalog_classifies_wrap_and_provider_error_without_enabling_books(self) -> None:
        native = self._token("GASX", None, 9)
        wrapped = self._token("WGASX", "0x" + "b1" * 20, 9)
        blocked_base = self._token("GAMMA", "0x" + "b2" * 20, 18)
        blocked_quote = self._token("DELTA", "0x" + "b3" * 20, 6)
        legal_base = self._token("LEGALX", "0x" + "b4" * 20, 18)
        legal_quote = self._token("LEGALY", "0x" + "b5" * 20, 6)

        wrap_result = self.service.create_objective(
            self.db,
            base_token_registry_id=wrapped.id,
            quote_token_registry_id=native.id,
            mechanism=MECHANISM_WRAP_UNWRAP,
            notes=None,
            confirm_create=True,
        )
        blocked_result = self.service.create_objective(
            self.db,
            base_token_registry_id=blocked_base.id,
            quote_token_registry_id=blocked_quote.id,
            mechanism=MECHANISM_SWAP,
            notes=None,
            confirm_create=True,
        )
        legal_result = self.service.create_objective(
            self.db,
            base_token_registry_id=legal_base.id,
            quote_token_registry_id=legal_quote.id,
            mechanism=MECHANISM_SWAP,
            notes=None,
            confirm_create=True,
        )
        for objective_id, pairs, provider, status in (
            (
                wrap_result["objective"]["id"],
                ((wrapped, native), (native, wrapped)),
                "native_wrap",
                "mechanism_configured",
            ),
            (
                blocked_result["objective"]["id"],
                ((blocked_base, blocked_quote), (blocked_quote, blocked_base)),
                "0x",
                "provider_error",
            ),
            (
                legal_result["objective"]["id"],
                ((legal_base, legal_quote), (legal_quote, legal_base)),
                "0x",
                "legal_envelope",
            ),
        ):
            for from_row, to_row in pairs:
                self.db.add(
                    RobinhoodChainPairCapability(
                        objective_id=objective_id,
                        from_token_registry_id=from_row.id,
                        to_token_registry_id=to_row.id,
                        amount_mode=AMOUNT_MODE_EXACT_INPUT,
                        provider=provider,
                        indicative_status="provider_error" if status == "legal_envelope" else status,
                        firm_plan_status="not_tested",
                        execution_status="disabled",
                        enabled=False,
                        probe_amount="0.01",
                        route_sources={"sources": [provider]},
                        provider_error=(
                            {
                                "error": "execution_discovery_provider_error",
                                "http_status": 422,
                                "provider_error": {
                                    "name": "BUY_TOKEN_NOT_AUTHORIZED_FOR_TRADE",
                                    "message": "The buy token is not authorized for trade due to legal restrictions",
                                },
                                "classification": "provider_error",
                            }
                            if status == "legal_envelope"
                            else ({"message": "provider failure"} if status == "provider_error" else {})
                        ),
                        evidence={"provider_contacted": status in {"provider_error", "legal_envelope"}},
                    )
                )
        self.db.commit()

        by_symbol = {item["symbol"]: item for item in self.service.market_catalog(self.db)}
        wrap = by_symbol["WGASX-GASX"]
        self.assertEqual(wrap["indicative_state"], "mechanism_configured")
        self.assertTrue(wrap["mechanism_configured"])
        self.assertFalse(wrap["orderbook_enabled"])
        self.assertEqual(wrap["orderbook_reason"], "wrap_unwrap_uses_dedicated_mechanism_view")

        blocked = by_symbol["GAMMA-DELTA"]
        self.assertEqual(blocked["indicative_state"], "provider_error")
        self.assertEqual(blocked["provider_error_direction_count"], 2)
        self.assertFalse(blocked["orderbook_enabled"])
        self.assertEqual(blocked["orderbook_reason"], "provider_error")
        self.assertTrue(blocked["refresh_supported"])
        self.assertTrue(blocked["explicit_refresh_required"])
        self.assertEqual(blocked["unavailable_direction_count"], 2)

        legal = by_symbol["LEGALX-LEGALY"]
        self.assertEqual(legal["indicative_state"], "legal_restriction")
        self.assertEqual(legal["legal_restriction_direction_count"], 2)
        self.assertEqual(legal["provider_error_direction_count"], 0)
        self.assertEqual(legal["orderbook_reason"], "legal_restriction")
        self.assertFalse(legal["orderbook_enabled"])
        self.assertTrue(
            all(
                item["persisted_indicative_status"] == "provider_error"
                and item["indicative_status"] == "legal_restriction"
                and item["classification_source"] == "persisted_provider_error_envelope"
                for item in legal["capabilities"]
            )
        )

    def test_router_exposes_review_only_registry_discovery_routes(self) -> None:
        from pathlib import Path

        router_source = (
            Path(__file__).resolve().parents[1]
            / "app"
            / "routers"
            / "robinhood_chain.py"
        ).read_text(encoding="utf-8")
        for route in (
            "/registry-discovery/status",
            "/registry-discovery/assets",
            "/registry-discovery/objectives",
            "/registry-discovery/markets",
            "/registry-discovery/markets/{symbol}/refresh",
            "/registry-discovery/sync-execution-evidence",
            "/execution-authority/authorize-controlled-buy",
        ):
            self.assertIn(route, router_source)



if __name__ == "__main__":
    unittest.main()
