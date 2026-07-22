from __future__ import annotations

import ast
import hashlib
import inspect
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
from app.models import RobinhoodChainSwapExecution  # noqa: E402
from app.services.evm_rpc import EvmRpcClient, encode_erc20_approve  # noqa: E402
from app.services.robinhood_chain_swap_execution import (  # noqa: E402
    ROBINHOOD_CHAIN_SWAP_USDG_CONTRACT,
    RobinhoodChainSwapExecutionService,
)


TAKER = "0x70c1ddd03bc4cb74efac3f12a41465d028ae490c"
SPENDER = "0x0000000000001ff3684f28c67538d4d072c22734"
APPROVAL_TX_HASH = "0x" + "aa" * 32
SWAP_TX_HASH = "0x" + "bb" * 32
APPROVAL_CLAIM = "cc" * 32
SWAP_CLAIM = "dd" * 32
SWAP_CALLDATA = "0x1234abcdef"
TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

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
WETH = {
    "symbol": "WETH",
    "contract_address": "0x0bd7d308f8e1639fab988df18a8011f41eacad73",
    "decimals": 18,
    "native": False,
}
WETH_CAPABILITY = {
    "from_asset": "USDG",
    "to_asset": "WETH",
    "amount_mode": "exact_input",
    "mechanism": "swap",
    "indicative_status": "available",
    "execution_status": "disabled",
    "enabled": False,
}


class FakePlanningService:
    def __init__(self, allowance_atomic: int = 0) -> None:
        self.allowance_atomic = int(allowance_atomic)
        self.calls: list[dict] = []
        self.counter = 0
        self.output_asset_override: str | None = None

    async def firm_quote_plan(self, **kwargs):
        self.calls.append(dict(kwargs))
        self.counter += 1
        input_amount = str(kwargs["total_quote"])
        input_atomic = str(int(float(input_amount) * 1_000_000))
        output_token = kwargs.get("base_token") or kwargs.get("eth_token") or ETH
        output_asset = self.output_asset_override or str(output_token.get("symbol") or "ETH").upper()
        output_atomic = str(int(float(input_amount) * 530_000_000_000_000))
        minimum_atomic = str(int(int(output_atomic) * 0.99))
        fetched = datetime.now(timezone.utc) + timedelta(milliseconds=self.counter)
        expires = fetched + timedelta(seconds=30)
        digest = hashlib.sha256(bytes.fromhex(SWAP_CALLDATA[2:])).hexdigest()
        return {
            "ok": True,
            "chain_id": 4663,
            "symbol": str(kwargs.get("symbol") or f"{output_asset}-USDG"),
            "side": "buy",
            "amount_mode": "exact_input",
            "input_asset": "USDG",
            "input_amount": input_amount,
            "input_amount_atomic": input_atomic,
            "output_asset": output_asset,
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
            "route_sources": ["Uniswap_V3"],
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


class FakeRpcClient:
    def __init__(self) -> None:
        self.allowance_atomic = 0
        self.approval_receipt = None
        self.swap_receipt = None
        self.native_balance_wei = 10 * 10**18
        self.usdg_balance_atomic = 3_710_769
        self.native_balances_by_tag: dict[str, int] = {}
        self.usdg_balances_by_tag: dict[str, int] = {}
        self.approval_amount_atomic = 2_000_000
        self.calls: list[tuple] = []

    async def verify_expected_chain(self, *, force_refresh=False):
        self.calls.append(("verify_expected_chain", force_refresh))
        return {"ok": True, "chain_id_matches": True, "actual_chain_id": "0x1237"}

    async def get_erc20_allowance(
        self, owner_address, contract_address, spender_address, decimals, *, force_refresh=True
    ):
        self.calls.append(("allowance", owner_address, contract_address, spender_address, decimals, force_refresh))
        return {
            "ok": True,
            "allowance_atomic": str(self.allowance_atomic),
            "allowance_token": str(self.allowance_atomic / 1_000_000),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "cached": False,
        }

    async def get_native_balance(self, address, *, block_tag="latest", force_refresh=True):
        self.calls.append(("native_balance", address, block_tag, force_refresh))
        balance = self.native_balances_by_tag.get(str(block_tag), self.native_balance_wei)
        return {"ok": True, "balance_wei": str(balance)}

    async def get_erc20_balance(
        self, address, contract_address, decimals, *, block_tag="latest", force_refresh=True
    ):
        self.calls.append(("erc20_balance", address, contract_address, decimals, block_tag, force_refresh))
        balance = self.usdg_balances_by_tag.get(str(block_tag), self.usdg_balance_atomic)
        return {"ok": True, "balance_atomic": str(balance)}

    async def rpc_read(self, method, params, *, cache_namespace=None, force_refresh=False):
        self.calls.append((method, list(params)))
        tx_hash = params[0]
        if method == "eth_getTransactionByHash":
            if tx_hash == APPROVAL_TX_HASH:
                return {
                    "ok": True,
                    "result": {
                        "hash": APPROVAL_TX_HASH,
                        "from": TAKER,
                        "to": ROBINHOOD_CHAIN_SWAP_USDG_CONTRACT,
                        "value": "0x0",
                        "input": encode_erc20_approve(SPENDER, self.approval_amount_atomic),
                    },
                }
            if tx_hash == SWAP_TX_HASH:
                return {
                    "ok": True,
                    "result": {
                        "hash": SWAP_TX_HASH,
                        "from": TAKER,
                        "to": SPENDER,
                        "value": "0x0",
                        "input": SWAP_CALLDATA,
                    },
                }
        if method == "eth_getTransactionReceipt":
            result = self.approval_receipt if tx_hash == APPROVAL_TX_HASH else self.swap_receipt
            return {"ok": True, "result": result}
        raise AssertionError(f"unexpected rpc call: {method} {params}")


def receipt(tx_hash: str, status: int, *, logs=None):
    return {
        "transactionHash": tx_hash,
        "from": TAKER,
        "to": ROBINHOOD_CHAIN_SWAP_USDG_CONTRACT if tx_hash == APPROVAL_TX_HASH else SPENDER,
        "status": hex(status),
        "blockNumber": hex(123456),
        "gasUsed": hex(50_000 if tx_hash == APPROVAL_TX_HASH else 245_000),
        "effectiveGasPrice": hex(80_000_000),
        "logs": list(logs or []),
    }


def usdg_spend_log(amount_atomic: int):
    return {
        "address": ROBINHOOD_CHAIN_SWAP_USDG_CONTRACT,
        "topics": [
            TRANSFER_TOPIC0,
            "0x" + TAKER[2:].lower().rjust(64, "0"),
            "0x" + SPENDER[2:].lower().rjust(64, "0"),
        ],
        "data": hex(amount_atomic),
    }


class RobinhoodChainSwapExecutionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", future=True)
        RobinhoodChainSwapExecution.__table__.create(bind=self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False, future=True)
        self.db = self.Session()
        self.planning = FakePlanningService(allowance_atomic=0)
        self.rpc = FakeRpcClient()
        self.service = RobinhoodChainSwapExecutionService(
            planning_service=self.planning,
            rpc_client=self.rpc,
        )

    def tearDown(self):
        self.db.close()
        self.engine.dispose()

    def live_gate(self):
        return (
            patch.object(settings.__class__, "robinhood_chain_effective_enabled", return_value=True),
            patch.object(settings, "robinhood_chain_live_execution_enabled", True),
            patch.object(settings, "armed", True),
            patch.object(settings, "dry_run", False),
        )

    async def prepare(self, amount="2"):
        return await self.service.prepare(
            self.db,
            taker_address=TAKER,
            exact_input_amount=amount,
            slippage_bps=100,
            eth_token=ETH,
            usdg_token=USDG,
            confirm_prepare=True,
        )

    async def prepare_weth(self, amount="1"):
        return await self.service.prepare(
            self.db,
            taker_address=TAKER,
            exact_input_amount=amount,
            slippage_bps=100,
            eth_token=None,
            usdg_token=USDG,
            to_asset="WETH",
            to_token=WETH,
            route_capability=WETH_CAPABILITY,
            confirm_prepare=True,
        )

    async def approval_confirmed(self):
        prepared = await self.prepare()
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            self.service.claim_approval_send(
                self.db,
                execution_id=prepared["execution"]["id"],
                wallet_address=TAKER,
                plan_hash=prepared["execution"]["approval"]["plan_hash"],
                claim_id=APPROVAL_CLAIM,
                confirm_send_claim=True,
            )
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

    def test_router_exposes_separate_approval_and_swap_stage_routes(self):
        source = (BACKEND_ROOT / "app" / "routers" / "robinhood_chain.py").read_text(encoding="utf-8")
        for route in (
            "/swap-execution/{execution_id}/approval/claim-send",
            "/swap-execution/{execution_id}/approval/submission",
            "/swap-execution/{execution_id}/approval/submission-failure",
            "/swap-execution/{execution_id}/approval/refresh",
            "/swap-execution/{execution_id}/prepare-swap",
            "/swap-execution/{execution_id}/swap/claim-send",
            "/swap-execution/{execution_id}/swap/submission",
            "/swap-execution/{execution_id}/swap/submission-failure",
            "/swap-execution/{execution_id}/swap/refresh",
        ):
            self.assertIn(route, source)

    def test_router_r5b_keywords_match_service_signatures(self):
        router_path = BACKEND_ROOT / "app" / "routers" / "robinhood_chain.py"
        tree = ast.parse(router_path.read_text(encoding="utf-8"), filename=str(router_path))
        endpoint_methods = {
            "robinhood_chain_swap_execution_claim_approval_send": "claim_approval_send",
            "robinhood_chain_swap_execution_record_approval_submission": "record_approval_submission",
            "robinhood_chain_swap_execution_record_approval_failure": "record_submission_failure",
            "robinhood_chain_swap_execution_refresh_approval": "refresh_approval",
            "robinhood_chain_swap_execution_prepare_fresh_swap": "prepare_swap",
            "robinhood_chain_swap_execution_claim_swap_send": "claim_swap_send",
            "robinhood_chain_swap_execution_record_swap_submission": "record_swap_submission",
            "robinhood_chain_swap_execution_record_swap_failure": "record_submission_failure",
            "robinhood_chain_swap_execution_refresh_swap": "refresh_swap",
        }
        functions = {
            node.name: node for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        for endpoint_name, method_name in endpoint_methods.items():
            endpoint = functions[endpoint_name]
            call = next(
                node for node in ast.walk(endpoint)
                if isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == method_name
            )
            endpoint_keywords = {keyword.arg for keyword in call.keywords if keyword.arg}
            service_keywords = set(
                inspect.signature(getattr(RobinhoodChainSwapExecutionService, method_name)).parameters
            ) - {"self"}
            self.assertTrue(
                endpoint_keywords <= service_keywords,
                f"{endpoint_name} passes unsupported keywords to {method_name}",
            )

    def test_backend_never_sends_or_signs_transactions(self):
        service_source = (
            BACKEND_ROOT / "app" / "services" / "robinhood_chain_swap_execution.py"
        ).read_text(encoding="utf-8")
        router_source = (
            BACKEND_ROOT / "app" / "routers" / "robinhood_chain.py"
        ).read_text(encoding="utf-8")
        self.assertNotIn("eth_sendTransaction", service_source)
        self.assertNotIn("eth_sendRawTransaction", service_source)
        self.assertNotIn("eth_sendTransaction", router_source)
        self.assertNotIn("eth_sendRawTransaction", router_source)

    async def test_status_uses_dedicated_gate_and_is_secret_free(self):
        with (
            patch.object(settings.__class__, "robinhood_chain_effective_enabled", return_value=True),
            patch.object(settings, "robinhood_chain_live_execution_enabled", False),
            patch.object(settings, "armed", True),
            patch.object(settings, "dry_run", False),
        ):
            status = self.service.status()
        self.assertEqual(status["tranche"], "RH-CHAIN.10D.2-R5B")
        self.assertFalse(status["send_enabled"])
        self.assertFalse(status["execution_enabled"])
        self.assertIn("ROBINHOOD_CHAIN_LIVE_EXECUTION_ENABLED=1", status["missing_requirements"])
        self.assertFalse(status["unlimited_approval_enabled"])
        self.assertFalse(status["automatic_second_transaction"])
        self.assertFalse(status["backend_transaction_sender"])
        self.assertFalse(status["generic_live_venues_required"])
        self.assertFalse(status["ledger_mutation_enabled"])
        self.assertFalse(status["fifo_mutation_enabled"])
        self.assertFalse(status["basis_mutation_enabled"])

    async def test_send_gate_requires_dedicated_gate_armed_and_non_dry_run(self):
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            status = self.service.status()
        self.assertTrue(status["send_enabled"])
        self.assertTrue(status["execution_enabled"])
        self.assertEqual(status["missing_requirements"], [])

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
        result = await self.prepare()
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
        result = await self.prepare()
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
        first = await self.prepare()
        second = await self.prepare()
        self.assertFalse(first["idempotent"])
        self.assertTrue(second["idempotent"])
        self.assertEqual(first["execution"]["id"], second["execution"]["id"])
        self.assertEqual(len(self.planning.calls), 1)

    async def test_sufficient_allowance_returns_no_approval_transaction(self):
        self.planning.allowance_atomic = 2_000_000
        result = await self.prepare()
        self.assertFalse(result["approval_required"])
        self.assertIsNone(result["approval_transaction_plan"])
        self.assertEqual(result["execution"]["status"], "allowance_sufficient")
        self.assertEqual(result["execution"]["approval_status"], "not_required")

    async def test_input_cap_fails_before_provider(self):
        with self.assertRaisesRegex(ValueError, "robinhood_chain_swap_input_exceeds_cap"):
            await self.prepare(amount="5.000001")
        self.assertEqual(self.planning.calls, [])

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
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            claimed = self.service.claim_approval_send(
                self.db,
                execution_id=prepared["execution"]["id"],
                wallet_address=TAKER,
                plan_hash=prepared["execution"]["approval"]["plan_hash"],
                claim_id=APPROVAL_CLAIM,
                confirm_send_claim=True,
            )
        self.assertEqual(claimed["execution"]["status"], "approval_send_claimed")
        self.assertEqual(claimed["approval_transaction_plan"]["approval_amount_atomic"], "2000000")

    async def test_approval_submission_is_idempotent_and_hash_is_separate(self):
        prepared = await self.prepare()
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            self.service.claim_approval_send(
                self.db, execution_id=prepared["execution"]["id"], wallet_address=TAKER,
                plan_hash=prepared["execution"]["approval"]["plan_hash"], claim_id=APPROVAL_CLAIM,
                confirm_send_claim=True,
            )
        first = self.service.record_approval_submission(
            self.db, execution_id=prepared["execution"]["id"], tx_hash=APPROVAL_TX_HASH,
            wallet_address=TAKER, claim_id=APPROVAL_CLAIM, confirm_record=True,
        )
        second = self.service.record_approval_submission(
            self.db, execution_id=prepared["execution"]["id"], tx_hash=APPROVAL_TX_HASH,
            wallet_address=TAKER, claim_id=APPROVAL_CLAIM, confirm_record=True,
        )
        self.assertEqual(first["execution"]["status"], "approval_pending")
        self.assertTrue(second["idempotent"])
        self.assertEqual(first["execution"]["approval"]["tx_hash"], APPROVAL_TX_HASH)
        self.assertIsNone(first["execution"]["swap"]["tx_hash"])

    async def test_approval_receipt_confirms_allowance_without_auto_swap(self):
        result = await self.approval_confirmed()
        self.assertEqual(result["execution"]["status"], "approval_confirmed")
        self.assertEqual(result["execution"]["approval"]["allowance_confirmed_atomic"], "2000000")
        self.assertEqual(result["execution"]["approval"]["receipt_status"], 1)
        self.assertIsNone(result["execution"]["swap"]["tx_hash"])
        self.assertEqual(len(self.planning.calls), 1)

    async def test_wallet_rejection_is_stage_specific_and_terminal(self):
        prepared = await self.prepare()
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            self.service.claim_approval_send(
                self.db, execution_id=prepared["execution"]["id"], wallet_address=TAKER,
                plan_hash=prepared["execution"]["approval"]["plan_hash"], claim_id=APPROVAL_CLAIM,
                confirm_send_claim=True,
            )
        result = self.service.record_submission_failure(
            self.db, execution_id=prepared["execution"]["id"], stage="approval",
            wallet_address=TAKER, claim_id=APPROVAL_CLAIM, reason="wallet_rejected",
            message="declined", confirm_failure=True,
        )
        self.assertEqual(result["execution"]["status"], "approval_wallet_rejected")
        self.assertIsNone(result["execution"]["approval"]["tx_hash"])
        self.assertIsNone(result["execution"]["swap"]["tx_hash"])

    async def test_prepare_swap_requires_confirmed_approval_and_fresh_plan(self):
        prepared = await self.prepare()
        with self.assertRaisesRegex(ValueError, "approval_not_confirmed"):
            await self.service.prepare_swap(
                self.db, execution_id=prepared["execution"]["id"], wallet_address=TAKER,
                eth_token=ETH, usdg_token=USDG, confirm_prepare=True,
            )
        approved = await self.approval_confirmed()
        swap = await self.service.prepare_swap(
            self.db, execution_id=approved["execution"]["id"], wallet_address=TAKER,
            eth_token=ETH, usdg_token=USDG, confirm_prepare=True,
        )
        self.assertEqual(swap["execution"]["status"], "swap_prepared")
        self.assertEqual(swap["execution"]["exact_input_amount_atomic"], "2000000")
        self.assertEqual(swap["execution"]["swap"]["transaction_value_wei"], "0")
        self.assertTrue(swap["unsigned_transaction_plan"]["calldata"].startswith("0x"))
        self.assertEqual(len(self.planning.calls), 2)

    async def test_swap_claim_captures_balances_and_submission_is_separate(self):
        approved = await self.approval_confirmed()
        swap = await self.service.prepare_swap(
            self.db, execution_id=approved["execution"]["id"], wallet_address=TAKER,
            eth_token=ETH, usdg_token=USDG, confirm_prepare=True,
        )
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            claimed = await self.service.claim_swap_send(
                self.db, execution_id=approved["execution"]["id"], wallet_address=TAKER,
                plan_hash=swap["execution"]["swap"]["plan_hash"], claim_id=SWAP_CLAIM,
                confirm_send_claim=True,
            )
        self.assertEqual(claimed["execution"]["status"], "swap_send_claimed")
        recorded = self.service.record_swap_submission(
            self.db, execution_id=approved["execution"]["id"], tx_hash=SWAP_TX_HASH,
            wallet_address=TAKER, claim_id=SWAP_CLAIM, confirm_record=True,
        )
        self.assertEqual(recorded["execution"]["status"], "swap_pending")
        self.assertEqual(recorded["execution"]["approval"]["tx_hash"], APPROVAL_TX_HASH)
        self.assertEqual(recorded["execution"]["swap"]["tx_hash"], SWAP_TX_HASH)

    async def test_confirmed_swap_reconciles_exact_input_output_and_both_fees(self):
        approved = await self.approval_confirmed()
        execution_id = approved["execution"]["id"]
        swap = await self.service.prepare_swap(
            self.db, execution_id=execution_id, wallet_address=TAKER,
            eth_token=ETH, usdg_token=USDG, confirm_prepare=True,
        )
        pre_eth = self.rpc.native_balance_wei
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            await self.service.claim_swap_send(
                self.db, execution_id=execution_id, wallet_address=TAKER,
                plan_hash=swap["execution"]["swap"]["plan_hash"], claim_id=SWAP_CLAIM,
                confirm_send_claim=True,
            )
        self.service.record_swap_submission(
            self.db, execution_id=execution_id, tx_hash=SWAP_TX_HASH,
            wallet_address=TAKER, claim_id=SWAP_CLAIM, confirm_record=True,
        )
        actual_output_wei = 1_050_000_000_000_000
        swap_fee_wei = 245_000 * 80_000_000
        before_tag = hex(123455)
        after_tag = hex(123456)
        self.rpc.native_balances_by_tag[before_tag] = pre_eth
        self.rpc.native_balances_by_tag[after_tag] = pre_eth + actual_output_wei - swap_fee_wei
        self.rpc.usdg_balances_by_tag[before_tag] = 3_710_769
        self.rpc.usdg_balances_by_tag[after_tag] = 1_710_769
        self.rpc.swap_receipt = receipt(SWAP_TX_HASH, 1, logs=[usdg_spend_log(2_000_000)])
        result = await self.service.refresh_swap(self.db, execution_id=execution_id)
        row = result["execution"]
        self.assertEqual(row["status"], "confirmed")
        self.assertEqual(row["actual_input_amount"], "2")
        self.assertEqual(row["actual_output_amount_atomic"], str(actual_output_wei))
        self.assertEqual(row["actual_output_amount"], "0.00105")
        self.assertEqual(row["actual_average_fill_price"], "1904.761904761904761904761905")
        self.assertEqual(row["actual_network_fee_wei"], str(swap_fee_wei))
        self.assertEqual(row["actual_approval_network_fee_wei"], str(50_000 * 80_000_000))
        self.assertNotEqual(row["approval"]["tx_hash"], row["swap"]["tx_hash"])

    async def test_swap_reconciliation_fails_on_usdg_balance_delta_mismatch(self):
        approved = await self.approval_confirmed()
        execution_id = approved["execution"]["id"]
        swap = await self.service.prepare_swap(
            self.db, execution_id=execution_id, wallet_address=TAKER,
            eth_token=ETH, usdg_token=USDG, confirm_prepare=True,
        )
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            await self.service.claim_swap_send(
                self.db, execution_id=execution_id, wallet_address=TAKER,
                plan_hash=swap["execution"]["swap"]["plan_hash"], claim_id=SWAP_CLAIM,
                confirm_send_claim=True,
            )
        self.service.record_swap_submission(
            self.db, execution_id=execution_id, tx_hash=SWAP_TX_HASH,
            wallet_address=TAKER, claim_id=SWAP_CLAIM, confirm_record=True,
        )
        before_tag = hex(123455)
        after_tag = hex(123456)
        self.rpc.native_balances_by_tag[before_tag] = self.rpc.native_balance_wei
        self.rpc.native_balances_by_tag[after_tag] = self.rpc.native_balance_wei + 1_050_000_000_000_000 - (245_000 * 80_000_000)
        self.rpc.usdg_balances_by_tag[before_tag] = 3_710_769
        self.rpc.usdg_balances_by_tag[after_tag] = 1_710_770
        self.rpc.swap_receipt = receipt(SWAP_TX_HASH, 1, logs=[usdg_spend_log(2_000_000)])
        with self.assertRaisesRegex(ValueError, "usdg_balance_delta_mismatch"):
            await self.service.refresh_swap(self.db, execution_id=execution_id)

    async def test_status_exposes_weth_approval_only_boundary(self):
        status = self.service.status()
        self.assertIn("WETH", status["approval_to_assets"])
        self.assertIn("WETH", status["approval_only_to_assets"])
        self.assertTrue(status["weth_approval_enabled"])
        self.assertFalse(status["weth_swap_enabled"])
        self.assertNotIn("WETH", status["swap_stage_enabled_to_assets"])

    async def test_prepare_weth_persists_finite_approval_only_lifecycle(self):
        result = await self.prepare_weth("1")
        self.assertTrue(result["ok"])
        row = result["execution"]
        self.assertEqual(row["symbol"], "WETH-USDG")
        self.assertEqual(row["from_asset"], "USDG")
        self.assertEqual(row["to_asset"], "WETH")
        self.assertFalse(row["to_native"])
        self.assertEqual(row["to_contract_address"].lower(), WETH["contract_address"].lower())
        self.assertEqual(row["exact_input_amount"], "1")
        self.assertEqual(row["approval"]["amount_atomic"], "1000000")
        self.assertEqual(row["approval"]["amount_policy"], "set_total_required_allowance")
        self.assertTrue(row["approval_only"])
        self.assertFalse(row["swap_execution_enabled"])
        self.assertEqual(row["swap_stage_locked_reason"], "RH-CHAIN.10D.2-R5C.3C")
        self.assertEqual(row["swap_status"], "locked_r5c3c")
        self.assertIsNone(self.service.get(self.db, row["id"])["unsigned_transaction_plan"])

    async def test_weth_requires_distinct_non_native_token_identity(self):
        bad_weth = {**WETH, "native": True}
        with self.assertRaisesRegex(ValueError, "robinhood_chain_swap_weth_identity_mismatch"):
            await self.service.prepare(
                self.db, taker_address=TAKER, exact_input_amount="1", slippage_bps=100,
                eth_token=None, usdg_token=USDG, to_asset="WETH", to_token=bad_weth,
                route_capability=WETH_CAPABILITY, confirm_prepare=True,
            )
        self.assertEqual(self.planning.calls, [])

    async def test_weth_plan_output_identity_mismatch_is_rejected(self):
        self.planning.output_asset_override = "ETH"
        with self.assertRaisesRegex(ValueError, "robinhood_chain_swap_plan_output_asset_mismatch"):
            await self.prepare_weth("1")

    async def test_partial_allowance_uses_finite_required_total_not_shortfall(self):
        self.planning.allowance_atomic = 400_000
        result = await self.prepare_weth("1")
        row = result["execution"]
        self.assertEqual(row["allowance"]["current_atomic"], "400000")
        self.assertEqual(row["allowance"]["required_atomic"], "1000000")
        self.assertEqual(row["allowance"]["shortfall_atomic"], "600000")
        self.assertEqual(row["approval"]["amount_atomic"], "1000000")
        self.assertEqual(row["allowance"]["approval_amount_policy"], "set_total_required_allowance")
        expected = encode_erc20_approve(SPENDER, 1_000_000)
        self.assertEqual(result["approval_transaction_plan"]["calldata"], expected)

    async def test_weth_approval_claim_and_wallet_rejection_do_not_open_swap(self):
        prepared = await self.prepare_weth("1")
        execution_id = prepared["execution"]["id"]
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            claimed = self.service.claim_approval_send(
                self.db, execution_id=execution_id, wallet_address=TAKER,
                plan_hash=prepared["execution"]["approval"]["plan_hash"],
                claim_id=APPROVAL_CLAIM, confirm_send_claim=True,
            )
        self.assertEqual(claimed["execution"]["status"], "approval_send_claimed")
        failed = self.service.record_submission_failure(
            self.db, execution_id=execution_id, stage="approval", wallet_address=TAKER,
            claim_id=APPROVAL_CLAIM, reason="wallet_rejected", message="declined",
            confirm_failure=True,
        )
        self.assertEqual(failed["execution"]["status"], "approval_wallet_rejected")
        self.assertIsNone(failed["execution"]["approval"]["tx_hash"])
        self.assertTrue(failed["execution"]["approval_only"])
        self.assertFalse(failed["execution"]["automatic_second_transaction"])

    async def test_weth_approval_confirmation_refreshes_allowance_and_stops(self):
        prepared = await self.prepare_weth("1")
        execution_id = prepared["execution"]["id"]
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            self.service.claim_approval_send(
                self.db, execution_id=execution_id, wallet_address=TAKER,
                plan_hash=prepared["execution"]["approval"]["plan_hash"],
                claim_id=APPROVAL_CLAIM, confirm_send_claim=True,
            )
        self.service.record_approval_submission(
            self.db, execution_id=execution_id, tx_hash=APPROVAL_TX_HASH,
            wallet_address=TAKER, claim_id=APPROVAL_CLAIM, confirm_record=True,
        )
        self.rpc.allowance_atomic = 1_000_000
        self.rpc.approval_amount_atomic = 1_000_000
        self.rpc.approval_receipt = receipt(APPROVAL_TX_HASH, 1)
        refreshed = await self.service.refresh_approval(self.db, execution_id=execution_id)
        self.assertEqual(refreshed["execution"]["status"], "approval_confirmed")
        self.assertEqual(refreshed["execution"]["allowance"]["shortfall_atomic"], "0")
        self.assertFalse(refreshed["execution"]["allowance"]["approval_required"])
        self.assertTrue(refreshed["execution"]["approval_only"])
        self.assertFalse(refreshed["execution"]["swap_execution_enabled"])
        self.assertIsNone(self.service.get(self.db, execution_id)["unsigned_transaction_plan"])

    async def test_weth_prepare_swap_is_locked_until_r5c3c(self):
        prepared = await self.prepare_weth("1")
        row = self.db.get(RobinhoodChainSwapExecution, prepared["execution"]["id"])
        row.status = "approval_confirmed"
        row.approval_status = "confirmed"
        row.allowance_current_atomic = row.exact_input_amount_atomic
        row.allowance_shortfall_atomic = "0"
        row.approval_required = False
        self.db.add(row); self.db.commit()
        with self.assertRaisesRegex(ValueError, "robinhood_chain_swap_stage_locked_r5c3c"):
            await self.service.prepare_swap(
                self.db, execution_id=row.id, wallet_address=TAKER,
                eth_token=ETH, usdg_token=USDG, confirm_prepare=True,
            )
        self.assertEqual(len(self.planning.calls), 1)

    async def test_weth_swap_claim_is_locked_even_when_global_gate_is_live(self):
        prepared = await self.prepare_weth("1")
        row = self.db.get(RobinhoodChainSwapExecution, prepared["execution"]["id"])
        row.status = "swap_prepared"
        row.swap_status = "prepared"
        self.db.add(row); self.db.commit()
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            with self.assertRaisesRegex(ValueError, "robinhood_chain_swap_stage_locked_r5c3c"):
                await self.service.claim_swap_send(
                    self.db, execution_id=row.id, wallet_address=TAKER,
                    plan_hash=row.swap_plan_hash, claim_id=SWAP_CLAIM,
                    confirm_send_claim=True,
                )

    def test_router_weth_prepare_uses_database_capability_and_token_registry_identity(self):
        source = (BACKEND_ROOT / "app" / "routers" / "robinhood_chain.py").read_text(encoding="utf-8")
        self.assertIn('market_symbol = f"{to_asset}-USDG"', source)
        self.assertIn("_resolve_robinhood_chain_review_market", source)
        self.assertIn("to_asset=to_asset", source)
        self.assertIn("to_token=base_token", source)
        self.assertIn("route_capability=capability", source)

    async def test_evm_rpc_supports_read_only_historical_balance_tags(self):
        client = EvmRpcClient(name="test", rpc_url="http://example.invalid", expected_chain_id=4663)
        calls = []

        async def fake_verify_expected_chain(*, force_refresh=False):
            return {"ok": True, "chain_id_matches": True}

        async def fake_rpc_read(method, params, *, cache_namespace=None, force_refresh=False):
            calls.append((method, params, cache_namespace, force_refresh))
            return {"ok": True, "result": "0x2a"}

        client.verify_expected_chain = fake_verify_expected_chain
        client.rpc_read = fake_rpc_read
        native = await client.get_native_balance(TAKER, block_tag="0x10", force_refresh=True)
        token = await client.get_erc20_balance(
            TAKER, ROBINHOOD_CHAIN_SWAP_USDG_CONTRACT, 6, block_tag="0x10", force_refresh=True
        )
        self.assertTrue(native["ok"])
        self.assertEqual(native["balance_wei"], "42")
        self.assertTrue(token["ok"])
        self.assertEqual(token["balance_atomic"], "42")
        self.assertEqual(calls[0][0:2], ("eth_getBalance", [TAKER, "0x10"]))
        self.assertEqual(calls[1][0], "eth_call")
        self.assertEqual(calls[1][1][1], "0x10")

    def test_all_orders_excludes_approval_only_and_maps_swap_rows(self):
        source = (BACKEND_ROOT / "app" / "services" / "all_orders.py").read_text(encoding="utf-8")
        self.assertIn("RobinhoodChainSwapExecution.swap_tx_hash.is_not(None)", source)
        self.assertIn('"source": "RHCHAIN"', source)
        self.assertIn('"venue": "robinhood_chain"', source)
        self.assertIn('"side": "buy"', source)
        self.assertIn('"type": "swap"', source)
        self.assertIn('"expected_input_asset": "USDG"', source)
        self.assertIn('"expected_output_asset": "ETH"', source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
