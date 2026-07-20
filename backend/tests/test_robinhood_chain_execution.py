from __future__ import annotations

import hashlib
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.config import settings  # noqa: E402
from app.models import RobinhoodChainExecution, TokenRegistry, WalletAddress, WalletAddressSnapshot  # noqa: E402
from app.services.robinhood_chain_execution import (  # noqa: E402
    ROBINHOOD_CHAIN_EXECUTION_INPUT_WEI,
    RobinhoodChainExecutionService,
    validate_execution_saved_wallet,
)


TAKER = "0x70c1ddd03bc4cb74efac3f12a41465d028ae490c"
DESTINATION = "0x0000000000001ff3684f28c67538d4d072c22734"
TX_HASH = "0x" + "ab" * 32
CLAIM_ID = "cd" * 32
CALldata = "0x1234abcdef"
CALldata_HASH = hashlib.sha256(bytes.fromhex(CALldata[2:])).hexdigest()
TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ACTUAL_USDG_ATOMIC = 3_710_769

ETH = {
    "symbol": "ETH",
    "contract_address": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    "decimals": 18,
    "native": True,
}
USDG = {
    "symbol": "USDG",
    "contract_address": "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168",
    "decimals": 6,
    "native": False,
}


class FakePlanningService:
    def __init__(self) -> None:
        self.calls = []
        self.quote_id = "11" * 32
        self.fetched = datetime.now(timezone.utc)
        self.expires = self.fetched + timedelta(seconds=30)

    async def firm_quote_plan(self, **kwargs):
        self.calls.append(dict(kwargs))
        fetched = self.fetched
        expires = self.expires
        return {
            "ok": True,
            "chain_id": 4663,
            "symbol": "ETH-USDG",
            "side": "sell",
            "input_asset": "ETH",
            "input_amount": "0.002",
            "input_amount_atomic": str(ROBINHOOD_CHAIN_EXECUTION_INPUT_WEI),
            "output_asset": "USDG",
            "output_amount": "3.727812",
            "minimum_received": "3.690612",
            "minimum_received_atomic": "3690612",
            "slippage_bps": int(kwargs["slippage_bps"]),
            "approval_required": False,
            "quote_id": self.quote_id,
            "fetched_at": fetched.isoformat(),
            "plan_expires_at": expires.isoformat(),
            "route": {
                "fills": [{"source": "Uniswap_V3", "proportion_bps": "10000"}],
            },
            "unsigned_transaction_plan": {
                "from": TAKER,
                "to": DESTINATION,
                "value_wei": str(ROBINHOOD_CHAIN_EXECUTION_INPUT_WEI),
                "gas_limit": "300000",
                "gas_price_wei": "80000000",
                "calldata": CALldata,
                "calldata_sha256": CALldata_HASH,
                "calldata_bytes": len(bytes.fromhex(CALldata[2:])),
                "native_input": True,
                "destination_allowlisted": True,
            },
        }


class FakeRpcClient:
    def __init__(self, *, receipt=None, mutate_tx=None, transaction_available=True) -> None:
        self.receipt = receipt
        self.mutate_tx = mutate_tx
        self.transaction_available = transaction_available
        self.calls = []

    async def verify_expected_chain(self, *, force_refresh=False):
        self.calls.append(("verify_expected_chain", force_refresh))
        return {
            "ok": True,
            "chain_id_matches": True,
            "expected_chain_id": 4663,
            "actual_chain_id": "0x1237",
        }

    async def rpc_read(self, method, params, *, cache_namespace=None, force_refresh=False):
        self.calls.append((method, list(params), cache_namespace, force_refresh))
        if method == "eth_getTransactionByHash":
            if not self.transaction_available:
                return {"ok": True, "result": None}
            tx = {
                "hash": TX_HASH,
                "from": TAKER,
                "to": DESTINATION,
                "value": hex(ROBINHOOD_CHAIN_EXECUTION_INPUT_WEI),
                "input": CALldata,
            }
            if self.mutate_tx:
                self.mutate_tx(tx)
            return {"ok": True, "result": tx}
        if method == "eth_getTransactionReceipt":
            return {"ok": True, "result": self.receipt}
        raise AssertionError(f"unexpected RPC method: {method}")


def receipt(status: int):
    payload = {
        "transactionHash": TX_HASH,
        "from": TAKER,
        "to": DESTINATION,
        "status": hex(status),
        "blockNumber": hex(123456),
        "gasUsed": hex(245000),
        "effectiveGasPrice": hex(80000000),
        "logs": [],
    }
    if status == 1:
        payload["logs"] = [
            {
                "address": USDG["contract_address"],
                "topics": [
                    TRANSFER_TOPIC0,
                    "0x" + DESTINATION[2:].lower().rjust(64, "0"),
                    "0x" + TAKER[2:].lower().rjust(64, "0"),
                ],
                "data": hex(ACTUAL_USDG_ATOMIC),
            }
        ]
    return payload


class FakeBalanceClient:
    def __init__(self) -> None:
        self.calls = []

    async def get_native_balance(self, address, *, block_tag="latest", force_refresh=False):
        self.calls.append(("ETH", address, block_tag, force_refresh))
        return {
            "ok": True,
            "balance_wei": "28812857556354184",
            "balance_eth": "0.028812857556354184",
            "block_tag": block_tag,
            "cached": False,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }

    async def get_erc20_balance(
        self,
        address,
        contract_address,
        decimals,
        *,
        block_tag="latest",
        force_refresh=False,
    ):
        self.calls.append(("USDG", address, contract_address, decimals, block_tag, force_refresh))
        return {
            "ok": True,
            "balance_atomic": "3710769",
            "balance_token": "3.710769",
            "block_tag": block_tag,
            "cached": False,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
        }


class RobinhoodChainExecutionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        engine = create_engine("sqlite:///:memory:", future=True)
        RobinhoodChainExecution.__table__.create(bind=engine)
        self.Session = sessionmaker(bind=engine, expire_on_commit=False, future=True)
        self.db = self.Session()
        self.planning = FakePlanningService()
        self.service = RobinhoodChainExecutionService(planning_service=self.planning)

    def tearDown(self) -> None:
        self.db.close()

    async def prepare(self):
        return await self.service.prepare(
            self.db,
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
            slippage_bps=100,
            confirm_prepare=True,
        )

    async def prepare_and_claim(self):
        prepared = await self.prepare()
        with (
            patch.object(settings.__class__, "robinhood_chain_effective_enabled", return_value=True),
            patch.object(settings, "robinhood_chain_live_execution_enabled", True),
            patch.object(settings, "armed", True),
            patch.object(settings, "dry_run", False),
        ):
            claim = self.service.claim_send(
                self.db,
                execution_id=prepared["execution"]["id"],
                wallet_address=TAKER,
                plan_hash=prepared["execution"]["plan_hash"],
                claim_id=CLAIM_ID,
                confirm_send_claim=True,
            )
        return prepared, claim

    async def prepare_claim_and_record(self):
        prepared, claim = await self.prepare_and_claim()
        recorded = self.service.record_submission(
            self.db,
            execution_id=prepared["execution"]["id"],
            tx_hash=TX_HASH,
            wallet_address=TAKER,
            claim_id=CLAIM_ID,
            confirm_record=True,
        )
        return prepared, claim, recorded

    async def test_prepare_is_fixed_and_idempotent(self):
        first = await self.prepare()
        second = await self.prepare()
        self.assertTrue(first["ok"])
        self.assertFalse(first["idempotent"])
        self.assertTrue(second["idempotent"])
        row = first["execution"]
        self.assertEqual(row["symbol"], "ETH-USDG")
        self.assertEqual(row["side"], "sell")
        self.assertEqual(row["input_amount"], "0.002")
        self.assertEqual(row["input_amount_atomic"], str(ROBINHOOD_CHAIN_EXECUTION_INPUT_WEI))
        self.assertEqual(row["status"], "prepared")
        self.assertIsNone(row["tx_hash"])
        self.assertEqual(self.db.query(RobinhoodChainExecution).count(), 1)

    async def test_previous_0001_prepared_row_cannot_receive_send_claim(self):
        prepared = await self.prepare()
        row = self.db.query(RobinhoodChainExecution).one()
        row.input_amount = "0.001"
        row.input_amount_atomic = "1000000000000000"
        row.transaction_value_wei = "1000000000000000"
        self.db.commit()
        with (
            patch.object(settings.__class__, "robinhood_chain_effective_enabled", return_value=True),
            patch.object(settings, "robinhood_chain_live_execution_enabled", True),
            patch.object(settings, "armed", True),
            patch.object(settings, "dry_run", False),
        ):
            with self.assertRaisesRegex(ValueError, "locked_input_amount_mismatch"):
                self.service.claim_send(
                    self.db,
                    execution_id=prepared["execution"]["id"],
                    wallet_address=TAKER,
                    plan_hash=prepared["execution"]["plan_hash"],
                    claim_id=CLAIM_ID,
                    confirm_send_claim=True,
                )

    async def test_post_confirmation_balance_snapshot_helper_force_refreshes_eth_and_usdg(self):
        engine = create_engine("sqlite:///:memory:", future=True)
        WalletAddress.__table__.create(bind=engine)
        TokenRegistry.__table__.create(bind=engine)
        WalletAddressSnapshot.__table__.create(bind=engine)
        Session = sessionmaker(bind=engine, expire_on_commit=False, future=True)
        db = Session()
        try:
            wallet = WalletAddress(
                asset="ALL",
                network="robinhood_chain",
                wallet_id="robinhood_chain",
                address=TAKER,
                label="MetaMask",
                owner_scope="user",
            )
            token = TokenRegistry(
                chain="robinhood_chain",
                venue="robinhood_chain",
                symbol="USDG",
                address=USDG["contract_address"],
                decimals=6,
                label="Global Dollar",
            )
            db.add_all([wallet, token])
            db.commit()

            fake = FakeBalanceClient()
            from app.routers import robinhood_chain as robinhood_chain_router

            with patch.object(
                robinhood_chain_router,
                "get_robinhood_chain_client",
                return_value=fake,
            ):
                result = await robinhood_chain_router._refresh_robinhood_chain_execution_balance_snapshots(
                    db,
                    TAKER,
                )

            self.assertTrue(result["ok"])
            self.assertEqual(result["refreshed"], 2)
            self.assertEqual([item["asset"] for item in result["items"]], ["ETH", "USDG"])
            self.assertTrue(all(call[-1] is True for call in fake.calls))
            rows = db.query(WalletAddressSnapshot).order_by(WalletAddressSnapshot.asset.asc()).all()
            self.assertEqual([row.asset for row in rows], ["ETH", "USDG"])
            self.assertAlmostEqual(rows[0].balance_qty, 0.028812857556354184)
            self.assertAlmostEqual(rows[1].balance_qty, 3.710769)
            self.assertTrue(all(row.balance_raw.get("post_execution_refresh") is True for row in rows))
        finally:
            db.close()

    async def test_execution_wallet_must_match_saved_wallet(self):
        self.assertEqual(validate_execution_saved_wallet(TAKER, TAKER[:2] + TAKER[2:].upper()), TAKER)
        with self.assertRaisesRegex(ValueError, "saved_wallet_mismatch"):
            validate_execution_saved_wallet(TAKER, "0x" + "12" * 20)

    async def test_prepare_requires_explicit_confirmation(self):
        with self.assertRaisesRegex(ValueError, "confirm_prepare_required"):
            await self.service.prepare(
                self.db,
                taker_address=TAKER,
                eth_token=ETH,
                usdg_token=USDG,
                slippage_bps=100,
                confirm_prepare=False,
            )

    async def test_send_claim_fails_closed_until_dedicated_gate_is_ready(self):
        prepared = await self.prepare()
        with (
            patch.object(settings.__class__, "robinhood_chain_effective_enabled", return_value=True),
            patch.object(settings, "robinhood_chain_live_execution_enabled", False),
            patch.object(settings, "armed", True),
            patch.object(settings, "dry_run", False),
        ):
            with self.assertRaisesRegex(ValueError, "live_send_gate_blocked"):
                self.service.claim_send(
                    self.db,
                    execution_id=prepared["execution"]["id"],
                    wallet_address=TAKER,
                    plan_hash=prepared["execution"]["plan_hash"],
                    claim_id=CLAIM_ID,
                    confirm_send_claim=True,
                )

    async def test_expired_plan_cannot_receive_a_send_claim(self):
        prepared = await self.prepare()
        row = self.db.query(RobinhoodChainExecution).one()
        row.plan_expires_at = datetime.utcnow() - timedelta(seconds=1)
        self.db.commit()
        with (
            patch.object(settings.__class__, "robinhood_chain_effective_enabled", return_value=True),
            patch.object(settings, "robinhood_chain_live_execution_enabled", True),
            patch.object(settings, "armed", True),
            patch.object(settings, "dry_run", False),
        ):
            with self.assertRaisesRegex(ValueError, "plan_expired"):
                self.service.claim_send(
                    self.db,
                    execution_id=prepared["execution"]["id"],
                    wallet_address=TAKER,
                    plan_hash=prepared["execution"]["plan_hash"],
                    claim_id=CLAIM_ID,
                    confirm_send_claim=True,
                )

    async def test_one_send_claim_is_idempotent_but_a_different_claim_is_blocked(self):
        prepared, first = await self.prepare_and_claim()
        self.assertEqual(first["execution"]["status"], "send_claimed")
        self.assertEqual(first["execution"]["submission_attempts"], 1)
        with (
            patch.object(settings.__class__, "robinhood_chain_effective_enabled", return_value=True),
            patch.object(settings, "robinhood_chain_live_execution_enabled", True),
            patch.object(settings, "armed", True),
            patch.object(settings, "dry_run", False),
        ):
            same = self.service.claim_send(
                self.db,
                execution_id=prepared["execution"]["id"],
                wallet_address=TAKER,
                plan_hash=prepared["execution"]["plan_hash"],
                claim_id=CLAIM_ID,
                confirm_send_claim=True,
            )
            self.assertTrue(same["idempotent"])
            with self.assertRaisesRegex(ValueError, "send_already_claimed"):
                self.service.claim_send(
                    self.db,
                    execution_id=prepared["execution"]["id"],
                    wallet_address=TAKER,
                    plan_hash=prepared["execution"]["plan_hash"],
                    claim_id="ef" * 32,
                    confirm_send_claim=True,
                )

    async def test_full_transaction_hash_is_recorded_once(self):
        prepared, _, first = await self.prepare_claim_and_record()
        self.assertEqual(first["execution"]["status"], "pending")
        self.assertEqual(first["execution"]["tx_hash"], TX_HASH)
        self.assertEqual(len(first["execution"]["tx_hash"]), 66)
        second = self.service.record_submission(
            self.db,
            execution_id=prepared["execution"]["id"],
            tx_hash=TX_HASH,
            wallet_address=TAKER,
            claim_id=CLAIM_ID,
            confirm_record=True,
        )
        self.assertTrue(second["idempotent"])
        with self.assertRaisesRegex(ValueError, "different_tx_hash"):
            self.service.record_submission(
                self.db,
                execution_id=prepared["execution"]["id"],
                tx_hash="0x" + "12" * 32,
                wallet_address=TAKER,
                claim_id=CLAIM_ID,
                confirm_record=True,
            )

    async def test_wallet_rejection_terminates_claim_without_transaction_hash(self):
        prepared, _ = await self.prepare_and_claim()
        failed = self.service.record_submission_failure(
            self.db,
            execution_id=prepared["execution"]["id"],
            wallet_address=TAKER,
            claim_id=CLAIM_ID,
            reason="wallet_rejected",
            message="MetaMask request was declined.",
            confirm_failure=True,
        )
        self.assertEqual(failed["execution"]["status"], "wallet_rejected")
        self.assertIsNone(failed["execution"]["tx_hash"])
        with self.assertRaisesRegex(ValueError, "not_prepared"):
            with (
                patch.object(settings.__class__, "robinhood_chain_effective_enabled", return_value=True),
                patch.object(settings, "robinhood_chain_live_execution_enabled", True),
                patch.object(settings, "armed", True),
                patch.object(settings, "dry_run", False),
            ):
                self.service.claim_send(
                    self.db,
                    execution_id=prepared["execution"]["id"],
                    wallet_address=TAKER,
                    plan_hash=prepared["execution"]["plan_hash"],
                    claim_id="ef" * 32,
                    confirm_send_claim=True,
                )

    async def test_generic_wallet_request_failure_maps_to_submission_failed(self):
        prepared, _ = await self.prepare_and_claim()
        failed = self.service.record_submission_failure(
            self.db,
            execution_id=prepared["execution"]["id"],
            wallet_address=TAKER,
            claim_id=CLAIM_ID,
            reason="wallet_request_failed",
            message="MetaMask returned no transaction hash.",
            confirm_failure=True,
        )
        self.assertEqual(failed["execution"]["status"], "submission_failed")
        self.assertEqual(failed["execution"]["error_code"], "wallet_request_failed")
        self.assertIsNone(failed["execution"]["tx_hash"])

    async def test_receipt_refresh_preserves_pending_until_transaction_details_exist(self):
        prepared, _, _ = await self.prepare_claim_and_record()
        self.service.rpc_client = FakeRpcClient(receipt=receipt(1), transaction_available=False)
        refreshed = await self.service.refresh_receipt(self.db, execution_id=prepared["execution"]["id"])
        self.assertFalse(refreshed["terminal"])
        self.assertFalse(refreshed["transaction_details_available"])
        self.assertEqual(refreshed["execution"]["status"], "pending")

    async def test_confirmed_receipt_updates_only_execution_lifecycle(self):
        prepared, _, _ = await self.prepare_claim_and_record()
        self.service.rpc_client = FakeRpcClient(receipt=receipt(1))
        refreshed = await self.service.refresh_receipt(self.db, execution_id=prepared["execution"]["id"])
        self.assertTrue(refreshed["terminal"])
        row = refreshed["execution"]
        self.assertEqual(row["status"], "confirmed")
        self.assertEqual(row["receipt_status"], 1)
        self.assertEqual(row["block_number"], 123456)
        self.assertEqual(row["gas_used"], "245000")
        self.assertEqual(row["actual_output_amount"], "3.710769")
        self.assertEqual(row["actual_output_amount_atomic"], str(ACTUAL_USDG_ATOMIC))
        self.assertEqual(row["actual_average_fill_price"], "1855.3845")
        self.assertEqual(row["actual_network_fee"], "0.0000196")
        self.assertEqual(row["actual_network_fee_asset"], "ETH")
        self.assertTrue(row["reconciliation"]["reconciled"])
        self.assertEqual(self.service.status()["tranche"], "RH-CHAIN.10D.1B")
        self.assertFalse(self.service.status()["ledger_mutation_enabled"])
        self.assertFalse(self.service.status()["fifo_mutation_enabled"])
        self.assertFalse(self.service.status()["basis_mutation_enabled"])

    async def test_confirmed_receipt_without_wallet_usdg_transfer_fails_closed(self):
        prepared, _, _ = await self.prepare_claim_and_record()
        missing_transfer = receipt(1)
        missing_transfer["logs"] = []
        self.service.rpc_client = FakeRpcClient(receipt=missing_transfer)
        refreshed = await self.service.refresh_receipt(self.db, execution_id=prepared["execution"]["id"])
        self.assertTrue(refreshed["terminal"])
        self.assertEqual(refreshed["execution"]["status"], "verification_failed")
        self.assertEqual(refreshed["execution"]["error_code"], "receipt_reconciliation_failed")
        self.assertEqual(
            refreshed["execution"]["error_message"],
            "robinhood_chain_usdg_transfer_to_wallet_not_found",
        )

    async def test_confirmed_row_can_be_backfilled_after_earlier_tranche(self):
        prepared, _, _ = await self.prepare_claim_and_record()
        row = self.db.query(RobinhoodChainExecution).one()
        row.status = "confirmed"
        row.confirmed_at = datetime.utcnow()
        row.receipt_status = 1
        self.db.commit()
        self.service.rpc_client = FakeRpcClient(receipt=receipt(1))
        refreshed = await self.service.refresh_receipt(self.db, execution_id=prepared["execution"]["id"])
        self.assertEqual(refreshed["execution"]["status"], "confirmed")
        self.assertTrue(refreshed["reconciled"])
        self.assertEqual(refreshed["execution"]["actual_output_amount"], "3.710769")

    async def test_reverted_receipt_is_terminal(self):
        prepared, _, _ = await self.prepare_claim_and_record()
        self.service.rpc_client = FakeRpcClient(receipt=receipt(0))
        refreshed = await self.service.refresh_receipt(self.db, execution_id=prepared["execution"]["id"])
        self.assertEqual(refreshed["execution"]["status"], "reverted")
        self.assertEqual(refreshed["execution"]["error_code"], "transaction_reverted")

    async def test_transaction_mismatch_fails_closed(self):
        prepared, _, _ = await self.prepare_claim_and_record()

        def mutate(tx):
            tx["value"] = hex(ROBINHOOD_CHAIN_EXECUTION_INPUT_WEI + 1)

        self.service.rpc_client = FakeRpcClient(receipt=None, mutate_tx=mutate)
        refreshed = await self.service.refresh_receipt(self.db, execution_id=prepared["execution"]["id"])
        self.assertTrue(refreshed["terminal"])
        self.assertEqual(refreshed["execution"]["status"], "verification_failed")
        self.assertIn("value", refreshed["execution"]["error_message"])

    async def test_all_orders_mapping_keeps_unreconciled_confirmed_values_blank(self):
        _, _, _ = await self.prepare_claim_and_record()
        row = self.db.query(RobinhoodChainExecution).one()
        row.status = "confirmed"
        row.confirmed_at = datetime.utcnow()
        row.receipt_status = 1
        self.db.commit()

        try:
            from app.services.all_orders import _to_unified_robinhood_chain_execution
        except ModuleNotFoundError as exc:
            if "models_lot_journal" not in str(exc):
                raise
            stub = types.ModuleType("app.models_lot_journal")
            stub.LotJournal = type("LotJournal", (), {})
            sys.modules["app.models_lot_journal"] = stub
            from app.services.all_orders import _to_unified_robinhood_chain_execution

        mapped = _to_unified_robinhood_chain_execution(row)
        self.assertEqual(mapped["venue"], "robinhood_chain")
        self.assertEqual(mapped["venue_order_id"], TX_HASH)
        self.assertEqual(len(mapped["venue_order_id"]), 66)
        self.assertEqual(mapped["status_bucket"], "terminal")
        self.assertIsNone(mapped["avg_fill_price"])
        self.assertIsNone(mapped["total_after_fee"])
        self.assertIsNone(mapped["fee"])

    async def test_all_orders_mapping_uses_receipt_verified_realized_values(self):
        prepared, _, _ = await self.prepare_claim_and_record()
        self.service.rpc_client = FakeRpcClient(receipt=receipt(1))
        await self.service.refresh_receipt(self.db, execution_id=prepared["execution"]["id"])
        row = self.db.query(RobinhoodChainExecution).one()

        try:
            from app.services.all_orders import _to_unified_robinhood_chain_execution
        except ModuleNotFoundError as exc:
            if "models_lot_journal" not in str(exc):
                raise
            stub = types.ModuleType("app.models_lot_journal")
            stub.LotJournal = type("LotJournal", (), {})
            sys.modules["app.models_lot_journal"] = stub
            from app.services.all_orders import _to_unified_robinhood_chain_execution

        mapped = _to_unified_robinhood_chain_execution(row)
        self.assertAlmostEqual(mapped["avg_fill_price"], 1855.3845)
        self.assertAlmostEqual(mapped["limit_price"], 1845.306)
        self.assertAlmostEqual(mapped["fee"], 0.0000196)
        self.assertEqual(mapped["fee_asset"], "ETH")
        self.assertAlmostEqual(mapped["total_after_fee"], 3.710769)
        self.assertEqual(mapped["actual_output_asset"], "USDG")
        self.assertTrue(mapped["execution_reconciled"])


if __name__ == "__main__":
    unittest.main()
