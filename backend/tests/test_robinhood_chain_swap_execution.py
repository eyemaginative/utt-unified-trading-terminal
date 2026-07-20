from __future__ import annotations

import ast
import inspect
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.models import RobinhoodChainSwapExecution  # noqa: E402
from app.services.robinhood_chain_swap_execution import (  # noqa: E402
    ROBINHOOD_CHAIN_SWAP_USDG_CONTRACT,
    RobinhoodChainSwapExecutionService,
)


TAKER = "0x70c1ddd03bc4cb74efac3f12a41465d028ae490c"
SPENDER = "0x0000000000001ff3684f28c67538d4d072c22734"
ETH = {
    "symbol": "ETH",
    "contract_address": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    "decimals": 18,
    "native": True,
}
USDG = {
    "symbol": "USDG",
    "contract_address": ROBINHOOD_CHAIN_SWAP_USDG_CONTRACT,
    "decimals": 6,
    "native": False,
}
SWAP_CALLDATA = "0x1234abcdef"


class FakePlanningService:
    def __init__(self, allowance_atomic: int = 0) -> None:
        self.allowance_atomic = int(allowance_atomic)
        self.calls: list[dict] = []
        self.counter = 0

    async def firm_quote_plan(self, **kwargs):
        self.calls.append(dict(kwargs))
        self.counter += 1
        input_amount = str(kwargs["total_quote"])
        input_atomic = str(int(float(input_amount) * 1_000_000))
        output_atomic = str(int(float(input_amount) * 530_000_000_000_000))
        minimum_atomic = str(int(int(output_atomic) * 0.99))
        fetched = datetime.now(timezone.utc) + timedelta(milliseconds=self.counter)
        expires = fetched + timedelta(seconds=30)
        import hashlib
        digest = hashlib.sha256(bytes.fromhex(SWAP_CALLDATA[2:])).hexdigest()
        return {
            "ok": True,
            "chain_id": 4663,
            "symbol": "ETH-USDG",
            "side": "buy",
            "amount_mode": "exact_input",
            "input_asset": "USDG",
            "input_amount": input_amount,
            "input_amount_atomic": input_atomic,
            "output_asset": "ETH",
            "output_amount": str(int(output_atomic) / 10**18),
            "output_amount_atomic": output_atomic,
            "minimum_received": str(int(minimum_atomic) / 10**18),
            "minimum_received_atomic": minimum_atomic,
            "slippage_bps": int(kwargs["slippage_bps"]),
            "allowance": {
                "applicable": True,
                "read_method": "eth_call",
                "spender": SPENDER,
                "spender_allowlisted": True,
                "current_atomic": str(self.allowance_atomic),
                "required_atomic": input_atomic,
                "shortfall_atomic": str(max(0, int(input_atomic) - self.allowance_atomic)),
                "approval_required": self.allowance_atomic < int(input_atomic),
            },
            "approval_required": self.allowance_atomic < int(input_atomic),
            "quote_id": f"{self.counter:064x}",
            "fetched_at": fetched.isoformat(),
            "plan_expires_at": expires.isoformat(),
            "route": {"fills": [{"source": "Uniswap_V3", "proportion_bps": "10000"}]},
            "unsigned_transaction_plan": {
                "from": TAKER,
                "to": SPENDER,
                "value_wei": "0",
                "gas_limit": "270124",
                "gas_price_wei": "80000000",
                "calldata": SWAP_CALLDATA,
                "calldata_sha256": digest,
                "calldata_bytes": len(bytes.fromhex(SWAP_CALLDATA[2:])),
                "native_input": False,
                "destination_allowlisted": True,
            },
        }


class RobinhoodChainSwapExecutionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        engine = create_engine("sqlite:///:memory:", future=True)
        RobinhoodChainSwapExecution.__table__.create(bind=engine)
        self.Session = sessionmaker(bind=engine, expire_on_commit=False, future=True)
        self.db = self.Session()
        self.planning = FakePlanningService(allowance_atomic=0)
        self.service = RobinhoodChainSwapExecutionService(planning_service=self.planning)

    def tearDown(self):
        self.db.close()

    def test_router_prepare_keywords_match_service_signature(self):
        router_path = BACKEND_ROOT / "app" / "routers" / "robinhood_chain.py"
        tree = ast.parse(router_path.read_text(encoding="utf-8"), filename=str(router_path))
        endpoint = next(
            node for node in tree.body
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name == "robinhood_chain_swap_execution_prepare"
        )
        call = next(
            node for node in ast.walk(endpoint)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "prepare"
        )
        endpoint_keywords = {keyword.arg for keyword in call.keywords if keyword.arg}
        service_keywords = set(inspect.signature(RobinhoodChainSwapExecutionService.prepare).parameters) - {"self"}
        self.assertTrue(endpoint_keywords <= service_keywords)
        self.assertIn("exact_input_amount", endpoint_keywords)
        self.assertIn("confirm_prepare", endpoint_keywords)

    async def test_status_is_review_only_and_secret_free(self):
        status = self.service.status()
        self.assertEqual(status["tranche"], "RH-CHAIN.10D.2-R5A")
        self.assertEqual(status["from_asset"], "USDG")
        self.assertEqual(status["to_asset"], "ETH")
        self.assertTrue(status["finite_approval_only"])
        self.assertFalse(status["unlimited_approval_enabled"])
        self.assertFalse(status["signing_enabled"])
        self.assertFalse(status["broadcast_enabled"])
        self.assertFalse(status["execution_enabled"])
        self.assertFalse(status["automatic_second_transaction"])
        self.assertFalse(status["will_mutate"])

    async def test_prepare_requires_explicit_confirmation(self):
        with self.assertRaisesRegex(ValueError, "confirm_robinhood_chain_swap_prepare_required"):
            await self.service.prepare(
                self.db,
                taker_address=TAKER,
                exact_input_amount="2",
                slippage_bps=100,
                eth_token=ETH,
                usdg_token=USDG,
                confirm_prepare=False,
            )
        self.assertEqual(self.planning.calls, [])

    async def test_prepare_persists_generalized_exact_spend_review(self):
        result = await self.service.prepare(
            self.db,
            taker_address=TAKER,
            exact_input_amount="2",
            slippage_bps=100,
            eth_token=ETH,
            usdg_token=USDG,
            confirm_prepare=True,
        )
        self.assertTrue(result["ok"])
        self.assertFalse(result["idempotent"])
        self.assertTrue(result["approval_required"])
        row = result["execution"]
        self.assertEqual(row["from_asset"], "USDG")
        self.assertEqual(row["to_asset"], "ETH")
        self.assertEqual(row["amount_mode"], "exact_input")
        self.assertEqual(row["exact_input_amount"], "2")
        self.assertEqual(row["exact_input_amount_atomic"], "2000000")
        self.assertEqual(row["approval"]["amount_atomic"], "2000000")
        self.assertEqual(row["swap"]["transaction_value_wei"], "0")
        self.assertEqual(row["status"], "approval_prepared")
        self.assertEqual(self.db.query(RobinhoodChainSwapExecution).count(), 1)

    async def test_approval_calldata_is_finite_and_bound_to_input(self):
        result = await self.service.prepare(
            self.db,
            taker_address=TAKER,
            exact_input_amount="2",
            slippage_bps=100,
            eth_token=ETH,
            usdg_token=USDG,
            confirm_prepare=True,
        )
        plan = result["approval_transaction_plan"]
        self.assertEqual(plan["approval_amount"], "2")
        self.assertEqual(plan["approval_amount_atomic"], "2000000")
        self.assertTrue(plan["finite_approval"])
        self.assertFalse(plan["unlimited_approval"])
        self.assertEqual(plan["value_wei"], "0")
        self.assertTrue(plan["calldata"].startswith("0x095ea7b3"))
        self.assertTrue(plan["calldata"].endswith(f"{2_000_000:064x}"))
        self.assertNotEqual(plan["calldata"][-64:], "f" * 64)
        self.assertFalse(plan["signing_requested"])
        self.assertFalse(plan["broadcast_requested"])

    async def test_prepare_is_idempotent_while_plan_is_fresh(self):
        first = await self.service.prepare(
            self.db, taker_address=TAKER, exact_input_amount="2", slippage_bps=100,
            eth_token=ETH, usdg_token=USDG, confirm_prepare=True,
        )
        second = await self.service.prepare(
            self.db, taker_address=TAKER, exact_input_amount="2", slippage_bps=100,
            eth_token=ETH, usdg_token=USDG, confirm_prepare=True,
        )
        self.assertFalse(first["idempotent"])
        self.assertTrue(second["idempotent"])
        self.assertEqual(first["execution"]["id"], second["execution"]["id"])
        self.assertEqual(len(self.planning.calls), 1)

    async def test_sufficient_allowance_returns_no_approval_transaction(self):
        self.planning.allowance_atomic = 2_000_000
        result = await self.service.prepare(
            self.db, taker_address=TAKER, exact_input_amount="2", slippage_bps=100,
            eth_token=ETH, usdg_token=USDG, confirm_prepare=True,
        )
        self.assertFalse(result["approval_required"])
        self.assertIsNone(result["approval_transaction_plan"])
        self.assertEqual(result["execution"]["status"], "allowance_sufficient")
        self.assertEqual(result["execution"]["approval_status"], "not_required")

    async def test_input_cap_fails_before_provider(self):
        with self.assertRaisesRegex(ValueError, "robinhood_chain_swap_input_exceeds_cap"):
            await self.service.prepare(
                self.db, taker_address=TAKER, exact_input_amount="5.000001", slippage_bps=100,
                eth_token=ETH, usdg_token=USDG, confirm_prepare=True,
            )
        self.assertEqual(self.planning.calls, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
