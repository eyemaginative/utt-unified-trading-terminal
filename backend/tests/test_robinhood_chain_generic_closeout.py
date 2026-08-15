from __future__ import annotations

import asyncio
import unittest
from pathlib import Path
from datetime import datetime
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.models import BasisLot, RobinhoodChainSwapExecution, VenueOrderRow
from app.models_lot_journal import LotJournal
from app.routers import robinhood_chain as rh_router
from app.services.all_orders import _to_unified_robinhood_chain_swap_execution
from app.services.lot_sync import sync_lots_from_activity


def _fixture_address(seed: int) -> str:
    """Return a deterministic synthetic EVM address for isolated tests only."""
    return "0x" + format(int(seed), "040x")


WALLET = _fixture_address(0x101)
USDG = _fixture_address(0x102)
INDEX = _fixture_address(0x103)
SPENDER = _fixture_address(0x104)
ROUTER = _fixture_address(0x105)
SWAP_HASH = "0x" + "ab" * 32
APPROVAL_HASH = "0x" + "cd" * 32
NATIVE_SWAP_HASH = "0x" + "ef" * 32
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"


def _topic_address(address: str) -> str:
    return "0x" + ("0" * 24) + address.lower()[2:]


def _quantity(value: int) -> str:
    return hex(int(value))


def _approve_calldata(spender: str, amount: int) -> str:
    return (
        "0x095ea7b3"
        + ("0" * 24) + spender.lower()[2:]
        + int(amount).to_bytes(32, "big").hex()
    )


class _FakeRegistryService:
    def market_by_symbol(self, db, symbol: str):
        if str(symbol).upper() != "INDEX-USDG":
            raise ValueError("unexpected symbol")
        return {
            "base": {
                "symbol": "INDEX",
                "registry_id": 103,
                "registry_contract_address": INDEX,
                "decimals": 18,
                "native": False,
            },
            "quote": {
                "symbol": "USDG",
                "registry_id": 102,
                "registry_contract_address": USDG,
                "decimals": 6,
                "native": False,
            },
        }

    def resolve_verified_token(self, db, symbol: str):
        value = str(symbol).upper()
        market = self.market_by_symbol(db, "INDEX-USDG")
        if value == "INDEX":
            return dict(market["base"])
        if value == "USDG":
            return dict(market["quote"])
        raise ValueError("unexpected token")



class _FakeNativeRegistryService:
    def market_by_symbol(self, db, symbol: str):
        if str(symbol).upper() != "INDEX-ETH":
            raise ValueError("unexpected symbol")
        return {
            "base": {
                "symbol": "INDEX",
                "registry_contract_address": INDEX,
                "decimals": 18,
                "native": False,
            },
            "quote": {
                "symbol": "ETH",
                "registry_contract_address": "",
                "decimals": 18,
                "native": True,
            },
        }


class _FakeNativeRpcClient:
    def __init__(self) -> None:
        self.swap_calldata = "0x12345678" + ("11" * 64)

    async def verify_expected_chain(self, *, force_refresh: bool = False):
        return {"ok": True, "chain_id_matches": True, "actual_chain_id": 4663}

    async def rpc_read(self, method, params, *, cache_namespace=None, force_refresh=False):
        tx_hash = str(params[0]).lower() if params else ""
        if method == "eth_getTransactionByHash" and tx_hash == NATIVE_SWAP_HASH.lower():
            return {
                "ok": True,
                "result": {
                    "hash": NATIVE_SWAP_HASH,
                    "from": WALLET,
                    "to": ROUTER,
                    "value": _quantity(600_000_000_000_000),
                    "gas": _quantity(6_213_408),
                    "input": self.swap_calldata,
                },
            }
        if method == "eth_getTransactionReceipt" and tx_hash == NATIVE_SWAP_HASH.lower():
            return {
                "ok": True,
                "result": {
                    "status": "0x1",
                    "blockNumber": _quantity(31_200_000),
                    "gasUsed": _quantity(188_969),
                    "effectiveGasPrice": _quantity(28_350_000),
                    "logs": [
                        {
                            "address": INDEX,
                            "topics": [TRANSFER_TOPIC, _topic_address(ROUTER), _topic_address(WALLET)],
                            "data": _quantity(89_500_000_000_000_000_000),
                        },
                    ],
                },
            }
        if method == "eth_getBlockByNumber":
            return {"ok": True, "result": {"timestamp": _quantity(1_754_650_000)}}
        raise AssertionError(f"unexpected rpc read: {method} {params}")

    async def get_erc20_allowance(self, **kwargs):
        raise AssertionError("native ETH input must not request ERC-20 allowance")


class _FakeNativeOutputRegistryService:
    def market_by_symbol(self, db, symbol: str):
        if str(symbol).upper() != "INDEX-ETH":
            raise ValueError("unexpected symbol")
        return {
            "base": {
                "symbol": "INDEX",
                "registry_contract_address": INDEX,
                "decimals": 18,
                "native": False,
            },
            "quote": {
                "symbol": "ETH",
                "registry_contract_address": "",
                "decimals": 18,
                "native": True,
            },
        }


class _FakeRpcClient:
    def __init__(self) -> None:
        self.approval_calldata = _approve_calldata(SPENDER, 1_000_000)
        self.swap_calldata = "0x12345678" + ("00" * 64)

    async def verify_expected_chain(self, *, force_refresh: bool = False):
        return {"ok": True, "chain_id_matches": True, "actual_chain_id": 4663}

    async def rpc_read(self, method, params, *, cache_namespace=None, force_refresh=False):
        tx_hash = str(params[0]).lower() if params else ""
        if method == "eth_getTransactionByHash" and tx_hash == SWAP_HASH.lower():
            return {
                "ok": True,
                "result": {
                    "hash": SWAP_HASH,
                    "from": WALLET,
                    "to": ROUTER,
                    "value": "0x0",
                    "gas": _quantity(220_000),
                    "input": self.swap_calldata,
                },
            }
        if method == "eth_getTransactionReceipt" and tx_hash == SWAP_HASH.lower():
            return {
                "ok": True,
                "result": {
                    "status": "0x1",
                    "blockNumber": _quantity(31_116_292),
                    "gasUsed": _quantity(163_556),
                    "effectiveGasPrice": _quantity(28_350_000),
                    "logs": [
                        {
                            "address": USDG,
                            "topics": [TRANSFER_TOPIC, _topic_address(WALLET), _topic_address(ROUTER)],
                            "data": _quantity(1_000_000),
                        },
                        {
                            "address": INDEX,
                            "topics": [TRANSFER_TOPIC, _topic_address(ROUTER), _topic_address(WALLET)],
                            "data": _quantity(77_750_000_000_000_000_000),
                        },
                    ],
                },
            }
        if method == "eth_getTransactionByHash" and tx_hash == APPROVAL_HASH.lower():
            return {
                "ok": True,
                "result": {
                    "hash": APPROVAL_HASH,
                    "from": WALLET,
                    "to": USDG,
                    "value": "0x0",
                    "gas": _quantity(68_000),
                    "input": self.approval_calldata,
                },
            }
        if method == "eth_getTransactionReceipt" and tx_hash == APPROVAL_HASH.lower():
            return {
                "ok": True,
                "result": {
                    "status": "0x1",
                    "gasUsed": _quantity(58_118),
                    "effectiveGasPrice": _quantity(30_374_000),
                },
            }
        if method == "eth_getBlockByNumber":
            return {"ok": True, "result": {"timestamp": _quantity(1_754_646_400)}}
        raise AssertionError(f"unexpected rpc read: {method} {params}")

    async def get_erc20_allowance(
        self,
        *,
        owner_address: str,
        contract_address: str,
        spender_address: str,
        decimals: int,
        force_refresh: bool = False,
    ):
        self._assert_address(owner_address, WALLET)
        self._assert_address(contract_address, USDG)
        self._assert_address(spender_address, SPENDER)
        self.asserted_decimals = decimals
        return {"ok": True, "allowance_atomic": "0"}

    @staticmethod
    def _assert_address(actual: str, expected: str) -> None:
        if str(actual).lower() != str(expected).lower():
            raise AssertionError(f"address mismatch: {actual} != {expected}")


class RobinhoodChainGenericCloseoutTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        for table in (
            RobinhoodChainSwapExecution.__table__,
            VenueOrderRow.__table__,
            BasisLot.__table__,
            LotJournal.__table__,
        ):
            table.create(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False, future=True)
        self.db = self.Session()
        self.rpc = _FakeRpcClient()

    def tearDown(self) -> None:
        self.db.close()
        self.engine.dispose()

    def _prepared_lifecycle(self, *, include_capability_economics: bool = True):
        capability_token = "capability-" + ("a" * 64)
        capability = {
            "wallet_address": WALLET,
            "symbol": "INDEX-USDG",
            "side": "buy",
            "input_asset": "USDG",
            "input_registry_id": 102,
            "input_amount": "1",
            "input_amount_atomic": "1000000",
            "output_asset": "INDEX",
            "output_registry_id": 103,
            "output_amount": "77.75713332971542716",
            "output_amount_atomic": "77757133329715427160",
            "minimum_received": "76.979561996418272888",
            "minimum_received_atomic": "76979561996418272888",
            "requested_amount": "1",
            "approval_tx_hash": APPROVAL_HASH,
        }
        if not include_capability_economics:
            for key in (
                "input_amount",
                "input_amount_atomic",
                "output_amount",
                "output_amount_atomic",
                "minimum_received",
                "minimum_received_atomic",
            ):
                capability.pop(key, None)

        handoff = {
            "wallet_address": WALLET,
            "action": "swap",
            "approval_required": False,
            "provider_simulation_requested": True,
            "requires_refresh_after_approval": False,
            "input_asset": "USDG",
            "input_amount": "1",
            "output_asset": "INDEX",
            "output_amount": "77.75713332971542716",
            "minimum_received": "76.979561996418272888",
            "transaction": {
                "to": ROUTER,
                "data": self.rpc.swap_calldata,
                "value_wei": "0",
                "gas_limit": "220000",
                "gas_price": "28350000",
            },
        }
        fresh_preflight = {
            "ok": True,
            "symbol": "INDEX-USDG",
            "side": "buy",
            "requested_amount": "1",
            "prepared_at": "2026-08-15T10:00:00+00:00",
            "expires_at": "2026-08-15T10:01:00+00:00",
            "ttl_seconds": 60,
            "wallet_request": handoff,
        }
        with (
            patch.object(rh_router, "_resolve_robinhood_chain_execution_taker", return_value=WALLET),
            patch.object(rh_router, "get_robinhood_chain_registry_discovery_service", return_value=_FakeRegistryService()),
        ):
            row, created = rh_router._persist_generic_wallet_swap_prepared_lifecycle(
                self.db,
                fresh_preflight=fresh_preflight,
                handoff=handoff,
                capability_token=capability_token,
                capability=capability,
                slippage_bps=100,
                approval_tx_hash=APPROVAL_HASH,
            )
            self.db.commit()
        return row, created, capability_token, capability

    def _reconcile(self):
        with (
            patch.object(rh_router, "_resolve_robinhood_chain_execution_taker", return_value=WALLET),
            patch.object(rh_router, "get_robinhood_chain_registry_discovery_service", return_value=_FakeRegistryService()),
            patch.object(rh_router, "get_robinhood_chain_client", return_value=self.rpc),
        ):
            result = asyncio.run(
                rh_router._persist_generic_wallet_swap_reconciliation(
                    self.db,
                    tx_hash=SWAP_HASH,
                    symbol="INDEX-USDG",
                    side="buy",
                    requested_amount="1",
                    quoted_output_amount="77.75713332971542716",
                    minimum_received="76.979561996418272888",
                    approval_tx_hash=APPROVAL_HASH,
                    source="unit_test",
                )
            )
            self.db.commit()
            return result

    def test_receipt_reconciliation_persists_one_idempotent_all_orders_source(self) -> None:
        first = self._reconcile()
        row_after_first = self.db.query(RobinhoodChainSwapExecution).one()
        updated_after_first = row_after_first.updated_at
        reconciled_at_after_first = row_after_first.route["execution_reconciliation"]["reconciled_at"]
        second = self._reconcile()
        self.assertTrue(first["created"])
        self.assertTrue(second["idempotent"])
        self.assertTrue(second["already_reconciled"])
        self.assertFalse(second["database_mutation"])
        self.assertEqual(self.db.query(RobinhoodChainSwapExecution).count(), 1)

        row = self.db.query(RobinhoodChainSwapExecution).one()
        self.assertEqual(row.updated_at, updated_after_first)
        self.assertEqual(row.route["execution_reconciliation"]["reconciled_at"], reconciled_at_after_first)
        reconciliation = row.route["execution_reconciliation"]
        self.assertEqual(reconciliation["input_amount"], "1")
        self.assertEqual(reconciliation["output_amount"], "77.75")
        self.assertEqual(reconciliation["residual_allowance_atomic"], "0")
        self.assertTrue(reconciliation["minimum_received_satisfied"])
        self.assertEqual(row.approval_tx_hash.lower(), APPROVAL_HASH.lower())
        self.assertEqual(row.swap_tx_hash.lower(), SWAP_HASH.lower())

        unified = _to_unified_robinhood_chain_swap_execution(row)
        self.assertEqual(unified["venue"], "robinhood_chain")
        self.assertEqual(unified["type"], "swap")
        self.assertEqual(unified["status"], "confirmed")
        self.assertAlmostEqual(unified["filled_qty"], 77.75)
        self.assertEqual(unified["transaction_id"].lower(), SWAP_HASH.lower())
        self.assertTrue(unified["execution_reconciled"])

    def test_receipt_reconciliation_captures_actual_fees_and_approval_lifecycle(self) -> None:
        result = self._reconcile()
        row = self.db.query(RobinhoodChainSwapExecution).one()
        reconciliation = row.route["execution_reconciliation"]
        lifecycle = row.route["execution_lifecycle"]
        self.assertEqual(result["swap_network_fee_wei"], str(163_556 * 28_350_000))
        self.assertEqual(result["approval_network_fee_wei"], str(58_118 * 30_374_000))
        self.assertEqual(
            result["total_network_fee_wei"],
            str((163_556 * 28_350_000) + (58_118 * 30_374_000)),
        )
        self.assertEqual(lifecycle["approval"]["gas_used"], "58118")
        self.assertEqual(lifecycle["swap"]["gas_used"], "163556")
        self.assertEqual(reconciliation["fee_asset"], "ETH")

    def test_durable_generic_lifecycle_exists_before_wallet_and_receipt_reuses_same_owner(self) -> None:
        row, created, capability_token, capability = self._prepared_lifecycle()
        execution_id = str(row.id)
        self.assertTrue(created)
        self.assertEqual(self.db.query(RobinhoodChainSwapExecution).count(), 1)
        self.assertEqual(row.status, "swap_prepared")
        self.assertEqual(row.swap_status, "prepared")
        self.assertIsNone(row.swap_tx_hash)
        self.assertTrue(row.route["generic_wallet_lifecycle"]["durable_before_wallet_request"])
        self.assertTrue(row.route["generic_wallet_lifecycle"]["historical_preflight_available"])

        bound, changed = rh_router._record_generic_wallet_swap_submission(
            self.db,
            execution_id=execution_id,
            tx_hash=SWAP_HASH,
            capability_token=capability_token,
            capability=capability,
        )
        self.db.commit()
        self.assertTrue(changed)
        self.assertEqual(str(bound.id), execution_id)
        self.assertEqual(bound.swap_tx_hash.lower(), SWAP_HASH.lower())
        self.assertEqual(bound.status, "swap_pending")
        self.assertEqual(self.db.query(RobinhoodChainSwapExecution).count(), 1)

        result = self._reconcile()
        self.assertFalse(result["created"])
        self.assertFalse(result["idempotent"])
        self.assertEqual(result["execution_id"], execution_id)
        self.assertEqual(self.db.query(RobinhoodChainSwapExecution).count(), 1)
        final = self.db.get(RobinhoodChainSwapExecution, execution_id)
        self.assertEqual(final.status, "confirmed")
        self.assertEqual(final.swap_status, "confirmed")
        self.assertTrue(final.route["generic_wallet_lifecycle"]["historical_preflight_available"])
        self.assertEqual(final.expected_output_amount, "77.75713332971542716")
        self.assertEqual(final.minimum_output_amount, "76.979561996418272888")

    def test_prepared_lifecycle_uses_validated_handoff_economics_when_capability_omits_duplicates(self) -> None:
        row, created, _capability_token, capability = self._prepared_lifecycle(
            include_capability_economics=False
        )
        self.assertTrue(created)
        self.assertNotIn("output_amount", capability)
        self.assertNotIn("minimum_received", capability)
        self.assertEqual(row.status, "swap_prepared")
        self.assertEqual(row.exact_input_amount, "1")
        self.assertEqual(row.exact_input_amount_atomic, "1000000")
        self.assertEqual(row.expected_output_amount, "77.75713332971542716")
        self.assertEqual(row.expected_output_amount_atomic, "77757133329715427160")
        self.assertEqual(row.minimum_output_amount, "76.979561996418272888")
        self.assertEqual(row.minimum_output_amount_atomic, "76979561996418272888")
        self.assertTrue(row.route["generic_wallet_lifecycle"]["historical_preflight_available"])

    def test_generic_wallet_receipt_confirmation_refreshes_order_ticket_balances(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        ticket_path = repo_root / "frontend" / "src" / "OrderTicketWidget.jsx"
        ticket_text = ticket_path.read_text(encoding="utf-8")
        start = ticket_text.index("async function refreshRobinhoodChainSuccessfulSwapReceipt()")
        end = ticket_text.index("async function sendRobinhoodChainSuccessfulSwapRequest()", start)
        receipt_block = ticket_text[start:end]

        self.assertIn(
            "await refreshRobinhoodChainTicketBalancesAfterConfirmation({",
            receipt_block,
        )
        self.assertIn('source: "generic_wallet_swap_receipt"', receipt_block)
        self.assertIn("order_ticket_balance_refresh: ticketBalanceRefresh", receipt_block)
        self.assertIn(
            "Order Ticket balances were refreshed automatically",
            receipt_block,
        )

    def test_generic_submission_record_is_idempotent_and_never_rebinds_hash(self) -> None:
        row, _, capability_token, capability = self._prepared_lifecycle()
        first, first_changed = rh_router._record_generic_wallet_swap_submission(
            self.db,
            execution_id=row.id,
            tx_hash=SWAP_HASH,
            capability_token=capability_token,
            capability=capability,
        )
        self.db.commit()
        second, second_changed = rh_router._record_generic_wallet_swap_submission(
            self.db,
            execution_id=row.id,
            tx_hash=SWAP_HASH,
            capability_token=capability_token,
            capability=capability,
        )
        self.assertTrue(first_changed)
        self.assertFalse(second_changed)
        self.assertEqual(str(first.id), str(second.id))
        with self.assertRaises(rh_router.HTTPException) as ctx:
            rh_router._record_generic_wallet_swap_submission(
                self.db,
                execution_id=row.id,
                tx_hash=NATIVE_SWAP_HASH,
                capability_token=capability_token,
                capability=capability,
            )
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertEqual(ctx.exception.detail["error"], "wallet_swap_submission_hash_conflict")

    def test_lost_historical_preflight_preview_and_recovery_do_not_invent_quote_or_minimum(self) -> None:
        with (
            patch.object(rh_router, "_resolve_robinhood_chain_execution_taker", return_value=WALLET),
            patch.object(rh_router, "get_robinhood_chain_registry_discovery_service", return_value=_FakeRegistryService()),
            patch.object(rh_router, "get_robinhood_chain_client", return_value=self.rpc),
        ):
            preview = asyncio.run(
                rh_router._persist_generic_wallet_swap_reconciliation(
                    self.db,
                    tx_hash=SWAP_HASH,
                    symbol="INDEX-USDG",
                    side="buy",
                    requested_amount="1",
                    quoted_output_amount=None,
                    minimum_received=None,
                    approval_tx_hash=APPROVAL_HASH,
                    source="rh_order_miss_1b_orphan_recovery",
                    historical_preflight_available=False,
                    expected_actual_output_amount="77.75",
                    persist=False,
                )
            )
        self.assertTrue(preview["preview"])
        self.assertFalse(preview["historical_preflight_available"])
        self.assertIsNone(preview["quoted_output_amount"])
        self.assertIsNone(preview["minimum_received"])
        self.assertEqual(self.db.query(RobinhoodChainSwapExecution).count(), 0)

        with (
            patch.object(rh_router, "_resolve_robinhood_chain_execution_taker", return_value=WALLET),
            patch.object(rh_router, "get_robinhood_chain_registry_discovery_service", return_value=_FakeRegistryService()),
            patch.object(rh_router, "get_robinhood_chain_client", return_value=self.rpc),
        ):
            recovered = asyncio.run(
                rh_router._persist_generic_wallet_swap_reconciliation(
                    self.db,
                    tx_hash=SWAP_HASH,
                    symbol="INDEX-USDG",
                    side="buy",
                    requested_amount="1",
                    quoted_output_amount=None,
                    minimum_received=None,
                    approval_tx_hash=APPROVAL_HASH,
                    source="rh_order_miss_1b_orphan_recovery",
                    historical_preflight_available=False,
                    expected_actual_output_amount="77.75",
                    persist=True,
                )
            )
            self.db.commit()
        self.assertTrue(recovered["created"])
        self.assertFalse(recovered["historical_preflight_available"])
        row = self.db.query(RobinhoodChainSwapExecution).one()
        self.assertEqual(row.expected_output_amount, "0")
        self.assertEqual(row.expected_output_amount_atomic, "0")
        self.assertEqual(row.minimum_output_amount, "0")
        self.assertEqual(row.minimum_output_amount_atomic, "0")
        self.assertFalse(row.route["historical_preflight"]["available"])
        self.assertEqual(row.route["execution_reconciliation"]["output_amount"], "77.75")
        unified = _to_unified_robinhood_chain_swap_execution(row)
        self.assertAlmostEqual(unified["filled_qty"], 77.75)

    def test_native_eth_input_reconciliation_uses_transaction_value_and_erc20_output_logs(self) -> None:
        rpc = _FakeNativeRpcClient()
        with (
            patch.object(rh_router, "_resolve_robinhood_chain_execution_taker", return_value=WALLET),
            patch.object(rh_router, "get_robinhood_chain_registry_discovery_service", return_value=_FakeNativeRegistryService()),
            patch.object(rh_router, "get_robinhood_chain_client", return_value=rpc),
        ):
            first = asyncio.run(
                rh_router._persist_generic_wallet_swap_reconciliation(
                    self.db,
                    tx_hash=NATIVE_SWAP_HASH,
                    symbol="INDEX-ETH",
                    side="buy",
                    requested_amount="0.0006",
                    quoted_output_amount="89.885774502876327271",
                    minimum_received="88.986916757847563998",
                    approval_tx_hash=None,
                    source="unit_test_native",
                )
            )
            self.db.commit()

        self.assertTrue(first["created"])
        self.assertEqual(first["actual_input_asset"], "ETH")
        self.assertEqual(first["actual_input_amount"], "0.0006")
        self.assertEqual(first["actual_input_amount_atomic"], "600000000000000")
        self.assertEqual(first["actual_output_asset"], "INDEX")
        self.assertEqual(first["actual_output_amount"], "89.5")
        self.assertTrue(first["minimum_received_satisfied"])
        self.assertEqual(first["approval_tx_hash"], None)
        self.assertEqual(first["approval_network_fee_wei"], "0")
        self.assertEqual(first["residual_allowance_atomic"], None)

        row = self.db.query(RobinhoodChainSwapExecution).one()
        self.assertEqual(row.symbol, "INDEX-ETH")
        self.assertTrue(row.from_native)
        self.assertFalse(row.to_native)
        self.assertEqual(
            row.from_contract_address.lower(),
            rh_router.UNISWAP_NATIVE_TOKEN.lower(),
        )
        self.assertFalse(row.approval_required)
        self.assertEqual(row.allowance_read_method, "not_applicable")
        self.assertEqual(row.allowance_required_atomic, "0")
        self.assertEqual(row.approval_amount_atomic, "0")
        self.assertEqual(row.swap_transaction_value_wei, "600000000000000")

        unified = _to_unified_robinhood_chain_swap_execution(row)
        self.assertEqual(unified["venue"], "robinhood_chain")
        self.assertEqual(unified["type"], "swap")
        self.assertEqual(unified["status"], "confirmed")
        self.assertAlmostEqual(unified["filled_qty"], 89.5)
        self.assertAlmostEqual(unified["avg_fill_price"], 0.0006 / 89.5)
        self.assertEqual(unified["transaction_id"].lower(), NATIVE_SWAP_HASH.lower())

        with (
            patch.object(rh_router, "_resolve_robinhood_chain_execution_taker", return_value=WALLET),
            patch.object(rh_router, "get_robinhood_chain_registry_discovery_service", return_value=_FakeNativeRegistryService()),
            patch.object(rh_router, "get_robinhood_chain_client", return_value=rpc),
        ):
            second = asyncio.run(
                rh_router._persist_generic_wallet_swap_reconciliation(
                    self.db,
                    tx_hash=NATIVE_SWAP_HASH,
                    symbol="INDEX-ETH",
                    side="buy",
                    requested_amount="0.0006",
                    quoted_output_amount="89.885774502876327271",
                    minimum_received="88.986916757847563998",
                    approval_tx_hash=None,
                    source="unit_test_native",
                )
            )
        self.assertTrue(second["idempotent"])
        self.assertTrue(second["already_reconciled"])
        self.assertFalse(second["database_mutation"])
        self.assertEqual(self.db.query(RobinhoodChainSwapExecution).count(), 1)

    def test_native_eth_output_reconciliation_remains_blocked(self) -> None:
        with (
            patch.object(rh_router, "_resolve_robinhood_chain_execution_taker", return_value=WALLET),
            patch.object(rh_router, "get_robinhood_chain_registry_discovery_service", return_value=_FakeNativeOutputRegistryService()),
        ):
            with self.assertRaises(rh_router.HTTPException) as ctx:
                asyncio.run(
                    rh_router._persist_generic_wallet_swap_reconciliation(
                        self.db,
                        tx_hash=NATIVE_SWAP_HASH,
                        symbol="INDEX-ETH",
                        side="sell",
                        requested_amount="1",
                        quoted_output_amount="0.000006",
                        minimum_received="0.0000059",
                        approval_tx_hash=None,
                        source="unit_test_native_output",
                    )
                )
        self.assertEqual(ctx.exception.status_code, 409)
        self.assertEqual(
            ctx.exception.detail["error"],
            "wallet_swap_reconcile_native_output_not_supported",
        )

    def test_ledger_sync_creates_one_index_buy_lot_and_is_idempotent(self) -> None:
        self._reconcile()
        first = sync_lots_from_activity(
            self.db,
            wallet_id="default",
            mode="VENUE",
            limit=500,
            venue="robinhood_chain",
            symbol_canon="INDEX-USDG",
            dry_run=False,
        )
        self.db.commit()
        self.assertEqual(first["robinhood_chain_rows_fetched"], 1)
        self.assertEqual(first["created_lots"], 1)
        lot = self.db.execute(select(BasisLot)).scalars().one()
        self.assertEqual(lot.venue, "robinhood_chain")
        self.assertEqual(lot.wallet_id, "default")
        self.assertEqual(lot.asset, "INDEX")
        self.assertAlmostEqual(lot.qty_total, 77.75)
        self.assertAlmostEqual(lot.qty_remaining, 77.75)
        self.assertAlmostEqual(lot.total_basis_usd or 0.0, 1.0, places=10)
        self.assertFalse(lot.basis_is_missing)
        journal = self.db.execute(select(LotJournal)).scalars().one()
        self.assertEqual(journal.origin_type, "RH_CHAIN_SWAP")
        self.assertTrue(journal.applied)

        second = sync_lots_from_activity(
            self.db,
            wallet_id="default",
            mode="VENUE",
            limit=500,
            venue="robinhood_chain",
            symbol_canon="INDEX-USDG",
            dry_run=False,
        )
        self.db.commit()
        self.assertEqual(second["robinhood_chain_rows_fetched"], 0)
        self.assertEqual(second["created_lots"], 0)
        self.assertEqual(self.db.execute(select(BasisLot)).scalars().all().__len__(), 1)
        self.assertEqual(self.db.execute(select(LotJournal)).scalars().all().__len__(), 1)


if __name__ == "__main__":
    unittest.main()
