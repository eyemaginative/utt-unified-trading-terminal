from __future__ import annotations

import hashlib
import sys
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
from app.models import RobinhoodChainBuyExecution  # noqa: E402
from app.services.evm_rpc import encode_erc20_approve  # noqa: E402
from app.services.robinhood_chain_buy_execution import (  # noqa: E402
    ROBINHOOD_CHAIN_BUY_APPROVAL_ATOMIC,
    ROBINHOOD_CHAIN_BUY_EXACT_OUTPUT_WEI,
    RobinhoodChainBuyExecutionService,
)

TAKER = "0x70c1ddd03bc4cb74efac3f12a41465d028ae490c"
USDG_CONTRACT = "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168"
SPENDER = "0x0000000000001ff3684f28c67538d4d072c22734"
APPROVAL_TX_HASH = "0x" + "aa" * 32
SWAP_TX_HASH = "0x" + "bb" * 32
APPROVAL_CLAIM = "cc" * 32
SWAP_CLAIM = "dd" * 32
SWAP_CALLDATA = "0x1234abcdef"
SWAP_CALLDATA_HASH = hashlib.sha256(bytes.fromhex(SWAP_CALLDATA[2:])).hexdigest()
TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

ETH = {
    "symbol": "ETH",
    "contract_address": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    "decimals": 18,
    "native": True,
}
USDG = {
    "symbol": "USDG",
    "contract_address": USDG_CONTRACT,
    "decimals": 6,
    "native": False,
}


class FakePlanningService:
    def __init__(self, allowance_atomic: int = 0) -> None:
        self.calls = []
        self.allowance_atomic = allowance_atomic
        self.counter = 0

    async def firm_quote_plan(self, **kwargs):
        self.calls.append(dict(kwargs))
        self.counter += 1
        fetched = datetime.now(timezone.utc) + timedelta(milliseconds=self.counter)
        expires = fetched + timedelta(seconds=30)
        return {
            "ok": True,
            "chain_id": 4663,
            "symbol": "ETH-USDG",
            "side": "buy",
            "amount_mode": "exact_output",
            "input_asset": "USDG",
            "input_amount": "1.85",
            "input_amount_atomic": "1850000",
            "output_asset": "ETH",
            "output_amount": "0.001",
            "output_amount_atomic": str(ROBINHOOD_CHAIN_BUY_EXACT_OUTPUT_WEI),
            "maximum_input_ceiling": "2",
            "maximum_input_ceiling_atomic": "2000000",
            "slippage_bps": 100,
            "allowance": {
                "read_method": "eth_call",
                "spender": SPENDER,
                "spender_allowlisted": True,
                "current_atomic": str(self.allowance_atomic),
                "required_atomic": "2000000",
                "approval_required": self.allowance_atomic < 2_000_000,
            },
            "approval_required": self.allowance_atomic < 2_000_000,
            "quote_id": f"{self.counter:064x}",
            "fetched_at": fetched.isoformat(),
            "plan_expires_at": expires.isoformat(),
            "route": {"fills": [{"source": "Uniswap_V3", "proportion_bps": "10000"}]},
            "route_sources": ["Uniswap_V3"],
            "unsigned_transaction_plan": {
                "from": TAKER,
                "to": SPENDER,
                "value_wei": "0",
                "gas_limit": "300000",
                "gas_price_wei": "80000000",
                "calldata": SWAP_CALLDATA,
                "calldata_sha256": SWAP_CALLDATA_HASH,
                "calldata_bytes": len(bytes.fromhex(SWAP_CALLDATA[2:])),
                "native_input": False,
                "destination_allowlisted": True,
            },
        }


class FakeRpcClient:
    def __init__(self) -> None:
        self.allowance_atomic = 0
        self.approval_receipt = None
        self.swap_receipt = None
        self.calls = []

    async def verify_expected_chain(self, *, force_refresh=False):
        return {"ok": True, "chain_id_matches": True, "actual_chain_id": "0x1237"}

    async def get_erc20_allowance(self, owner_address, contract_address, spender_address, decimals, *, force_refresh=True):
        self.calls.append(("allowance", owner_address, contract_address, spender_address, decimals, force_refresh))
        return {
            "ok": True,
            "allowance_atomic": str(self.allowance_atomic),
            "allowance_token": str(self.allowance_atomic),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "cached": False,
        }

    async def rpc_read(self, method, params, *, cache_namespace=None, force_refresh=False):
        self.calls.append((method, list(params)))
        tx_hash = params[0]
        if method == "eth_getTransactionByHash":
            if tx_hash == APPROVAL_TX_HASH:
                calldata = encode_erc20_approve(SPENDER, ROBINHOOD_CHAIN_BUY_APPROVAL_ATOMIC)
                return {"ok": True, "result": {"hash": APPROVAL_TX_HASH, "from": TAKER, "to": USDG_CONTRACT, "value": "0x0", "input": calldata}}
            if tx_hash == SWAP_TX_HASH:
                return {"ok": True, "result": {"hash": SWAP_TX_HASH, "from": TAKER, "to": SPENDER, "value": "0x0", "input": SWAP_CALLDATA}}
        if method == "eth_getTransactionReceipt":
            return {"ok": True, "result": self.approval_receipt if tx_hash == APPROVAL_TX_HASH else self.swap_receipt}
        raise AssertionError(f"unexpected rpc call: {method} {params}")


def receipt(tx_hash: str, status: int, *, logs=None):
    return {
        "transactionHash": tx_hash,
        "from": TAKER,
        "to": USDG_CONTRACT if tx_hash == APPROVAL_TX_HASH else SPENDER,
        "status": hex(status),
        "blockNumber": hex(123456),
        "gasUsed": hex(50_000 if tx_hash == APPROVAL_TX_HASH else 245_000),
        "effectiveGasPrice": hex(80_000_000),
        "logs": list(logs or []),
    }


def usdg_spend_log(amount_atomic: int):
    return {
        "address": USDG_CONTRACT,
        "topics": [
            TRANSFER_TOPIC0,
            "0x" + TAKER[2:].lower().rjust(64, "0"),
            "0x" + SPENDER[2:].lower().rjust(64, "0"),
        ],
        "data": hex(amount_atomic),
    }


class RobinhoodChainBuyExecutionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        engine = create_engine("sqlite:///:memory:", future=True)
        RobinhoodChainBuyExecution.__table__.create(bind=engine)
        self.Session = sessionmaker(bind=engine, expire_on_commit=False, future=True)
        self.db = self.Session()
        self.planning = FakePlanningService(allowance_atomic=0)
        self.rpc = FakeRpcClient()
        self.service = RobinhoodChainBuyExecutionService(planning_service=self.planning, rpc_client=self.rpc)

    def tearDown(self):
        self.db.close()

    async def prepare(self):
        return await self.service.prepare_approval(
            self.db,
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
            confirm_prepare=True,
        )

    async def claim_approval(self, prepared):
        with (
            patch.object(settings.__class__, "robinhood_chain_effective_enabled", return_value=True),
            patch.object(settings, "robinhood_chain_live_execution_enabled", True),
            patch.object(settings, "armed", True),
            patch.object(settings, "dry_run", False),
        ):
            return self.service.claim_approval_send(
                self.db,
                execution_id=prepared["execution"]["id"],
                wallet_address=TAKER,
                plan_hash=prepared["execution"]["approval"]["plan_hash"],
                claim_id=APPROVAL_CLAIM,
                confirm_send_claim=True,
            )

    async def approval_confirmed(self):
        prepared = await self.prepare()
        await self.claim_approval(prepared)
        self.service.record_approval_submission(
            self.db,
            execution_id=prepared["execution"]["id"],
            tx_hash=APPROVAL_TX_HASH,
            wallet_address=TAKER,
            claim_id=APPROVAL_CLAIM,
            confirm_record=True,
        )
        self.rpc.allowance_atomic = 2_000_000
        self.planning.allowance_atomic = 2_000_000
        self.rpc.approval_receipt = receipt(APPROVAL_TX_HASH, 1)
        return await self.service.refresh_approval(self.db, execution_id=prepared["execution"]["id"])

    async def test_status_exposes_exact_locks_and_safety_boundaries(self):
        status = self.service.status()
        self.assertEqual(status["tranche"], "RH-CHAIN.10D.2")
        self.assertEqual(status["exact_output_eth"], "0.001")
        self.assertEqual(status["maximum_usdg_spend"], "2")
        self.assertEqual(status["approval_amount_atomic"], "2000000")
        self.assertFalse(status["unlimited_approval_enabled"])
        self.assertFalse(status["automatic_second_transaction"])
        self.assertFalse(status["backend_transaction_sender"])
        self.assertFalse(status["ledger_mutation_enabled"])

    async def test_approval_calldata_is_finite_not_uint256_max(self):
        calldata = encode_erc20_approve(SPENDER, 2_000_000)
        self.assertTrue(calldata.startswith("0x095ea7b3"))
        self.assertTrue(calldata.endswith(f"{2_000_000:064x}"))
        self.assertNotIn("f" * 64, calldata[-64:])

    async def test_prepare_approval_is_fixed_and_idempotent(self):
        first = await self.prepare()
        second = await self.prepare()
        self.assertTrue(first["ok"])
        self.assertFalse(first["idempotent"])
        self.assertTrue(second["idempotent"])
        row = first["execution"]
        self.assertEqual(row["side"], "buy")
        self.assertEqual(row["exact_output_amount"], "0.001")
        self.assertEqual(row["maximum_input_amount"], "2")
        self.assertEqual(row["approval"]["amount_atomic"], "2000000")
        self.assertEqual(row["status"], "approval_prepared")
        self.assertEqual(self.db.query(RobinhoodChainBuyExecution).count(), 1)

    async def test_approval_send_requires_dedicated_gate_and_claim(self):
        prepared = await self.prepare()
        with self.assertRaisesRegex(ValueError, "send_gate_blocked"):
            self.service.claim_approval_send(
                self.db,
                execution_id=prepared["execution"]["id"],
                wallet_address=TAKER,
                plan_hash=prepared["execution"]["approval"]["plan_hash"],
                claim_id=APPROVAL_CLAIM,
                confirm_send_claim=True,
            )
        claimed = await self.claim_approval(prepared)
        self.assertEqual(claimed["execution"]["status"], "approval_send_claimed")
        self.assertEqual(claimed["approval_transaction_plan"]["approval_amount_atomic"], "2000000")

    async def test_approval_receipt_requires_confirmed_allowance(self):
        result = await self.approval_confirmed()
        self.assertEqual(result["execution"]["status"], "approval_confirmed")
        self.assertEqual(result["execution"]["approval"]["allowance_confirmed_atomic"], "2000000")
        self.assertEqual(result["execution"]["approval"]["receipt_status"], 1)

    async def test_approval_confirmation_does_not_automatically_prepare_swap(self):
        result = await self.approval_confirmed()
        self.assertEqual(result["execution"]["status"], "approval_confirmed")
        self.assertIsNone(result["execution"]["swap"]["quote_id"])
        self.assertIsNone(result["execution"]["swap"]["tx_hash"])
        self.assertEqual(len(self.planning.calls), 1)

    async def test_prepare_swap_requires_fresh_quote_and_separate_claim(self):
        approved = await self.approval_confirmed()
        execution_id = approved["execution"]["id"]
        prepared = await self.service.prepare_swap(
            self.db,
            execution_id=execution_id,
            wallet_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
            confirm_prepare=True,
        )
        self.assertEqual(prepared["execution"]["status"], "swap_prepared")
        self.assertEqual(prepared["execution"]["swap"]["expected_input_amount_atomic"], "1850000")
        self.assertEqual(len(self.planning.calls), 2)
        with (
            patch.object(settings.__class__, "robinhood_chain_effective_enabled", return_value=True),
            patch.object(settings, "robinhood_chain_live_execution_enabled", True),
            patch.object(settings, "armed", True),
            patch.object(settings, "dry_run", False),
        ):
            claimed = self.service.claim_swap_send(
                self.db,
                execution_id=execution_id,
                wallet_address=TAKER,
                plan_hash=prepared["execution"]["swap"]["plan_hash"],
                claim_id=SWAP_CLAIM,
                confirm_send_claim=True,
            )
        self.assertEqual(claimed["execution"]["status"], "swap_send_claimed")

    async def test_confirmed_swap_reconciles_actual_spend_fee_and_output(self):
        approved = await self.approval_confirmed()
        execution_id = approved["execution"]["id"]
        prepared = await self.service.prepare_swap(self.db, execution_id=execution_id, wallet_address=TAKER, eth_token=ETH, usdg_token=USDG, confirm_prepare=True)
        with (
            patch.object(settings.__class__, "robinhood_chain_effective_enabled", return_value=True),
            patch.object(settings, "robinhood_chain_live_execution_enabled", True),
            patch.object(settings, "armed", True),
            patch.object(settings, "dry_run", False),
        ):
            self.service.claim_swap_send(self.db, execution_id=execution_id, wallet_address=TAKER, plan_hash=prepared["execution"]["swap"]["plan_hash"], claim_id=SWAP_CLAIM, confirm_send_claim=True)
        self.service.record_swap_submission(self.db, execution_id=execution_id, tx_hash=SWAP_TX_HASH, wallet_address=TAKER, claim_id=SWAP_CLAIM, confirm_record=True)
        self.rpc.swap_receipt = receipt(SWAP_TX_HASH, 1, logs=[usdg_spend_log(1_847_321)])
        result = await self.service.refresh_swap(self.db, execution_id=execution_id)
        row = result["execution"]
        self.assertEqual(row["status"], "confirmed")
        self.assertEqual(row["actual_input_amount"], "1.847321")
        self.assertEqual(row["actual_output_amount"], "0.001")
        self.assertEqual(row["actual_average_fill_price"], "1847.321")
        self.assertIsNotNone(row["actual_network_fee"])
        self.assertIsNotNone(row["actual_approval_network_fee"])

    async def test_wallet_rejection_is_stage_specific_and_terminal(self):
        prepared = await self.prepare()
        await self.claim_approval(prepared)
        result = self.service.record_submission_failure(
            self.db,
            execution_id=prepared["execution"]["id"],
            stage="approval",
            wallet_address=TAKER,
            claim_id=APPROVAL_CLAIM,
            reason="wallet_rejected",
            message="declined",
            confirm_failure=True,
        )
        self.assertEqual(result["execution"]["status"], "approval_wallet_rejected")
        self.assertIsNone(result["execution"]["approval"]["tx_hash"])
        self.assertIsNone(result["execution"]["swap"]["tx_hash"])


if __name__ == "__main__":
    unittest.main()
