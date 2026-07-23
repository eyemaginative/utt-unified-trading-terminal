from __future__ import annotations

import unittest
from typing import Any, Dict, List

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.models import (
    RobinhoodChainPairCapability,
    RobinhoodChainPairObjective,
    RobinhoodChainRegistryVerification,
    TokenRegistry,
)
from app.services.robinhood_chain_execution_discovery import RobinhoodChainExecutionDiscoveryService
from app.services.robinhood_chain_registry_discovery import (
    AMOUNT_MODE_EXACT_INPUT,
    MECHANISM_SWAP,
    MECHANISM_WRAP_UNWRAP,
    RobinhoodChainRegistryDiscoveryService,
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


class RobinhoodChainRegistryDiscoveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite+pysqlite:///:memory:")
        for table in (
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
        self.service = RobinhoodChainRegistryDiscoveryService(
            rpc_client=self.fake_rpc,
            discovery_service=self.fake_discovery,
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
                evidence={"test": True},
            )
        )
        self.db.commit()

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
        self.assertEqual(statuses, ["available", "provider_error"])
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

    def test_market_catalog_classifies_wrap_and_provider_error_without_enabling_books(self) -> None:
        native = self._token("GASX", None, 9)
        wrapped = self._token("WGASX", "0x" + "b1" * 20, 9)
        blocked_base = self._token("GAMMA", "0x" + "b2" * 20, 18)
        blocked_quote = self._token("DELTA", "0x" + "b3" * 20, 6)

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
        ):
            for from_row, to_row in pairs:
                self.db.add(
                    RobinhoodChainPairCapability(
                        objective_id=objective_id,
                        from_token_registry_id=from_row.id,
                        to_token_registry_id=to_row.id,
                        amount_mode=AMOUNT_MODE_EXACT_INPUT,
                        provider=provider,
                        indicative_status=status,
                        firm_plan_status="not_tested",
                        execution_status="disabled",
                        enabled=False,
                        probe_amount="0.01",
                        route_sources={"sources": [provider]},
                        provider_error={"message": "provider failure"} if status == "provider_error" else {},
                        evidence={"provider_contacted": status == "provider_error"},
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
            "/registry-discovery/sync-execution-evidence",
        ):
            self.assertIn(route, router_source)



if __name__ == "__main__":
    unittest.main()
