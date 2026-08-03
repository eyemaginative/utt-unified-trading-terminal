from __future__ import annotations

import ast
import hashlib
import inspect
import sys
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.config import settings  # noqa: E402
from app.models import RobinhoodChainSwapExecution  # noqa: E402
from app.services.evm_rpc import EvmRpcClient, encode_erc20_approve  # noqa: E402
from app.services.all_orders import _to_unified_robinhood_chain_swap_execution  # noqa: E402
from app.services.robinhood_chain_swap_execution import (  # noqa: E402
    ROBINHOOD_CHAIN_SWAP_USDG_CONTRACT,
    RobinhoodChainSwapExecutionService,
)
from app.services.robinhood_chain_transaction_planning import (  # noqa: E402
    RobinhoodChainTransactionPlanningService,
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


def weth_preparation_authority_capability():
    evidence = {
        "preparation_verified": True,
        "live_accepted": False,
        "successful_broadcast": False,
        "symbol": "WETH-USDG",
        "side": "buy",
        "amount_mode": "exact_input",
        "provider": "0x",
        "from_asset": "USDG",
        "to_asset": "WETH",
        "verified_input_amount": "1",
        "firm_plan_input_ceiling": "1",
    }
    authority = {
        "symbol": "WETH-USDG",
        "side": "buy",
        "amount_mode": "exact_input",
        "provider": "0x",
        "execution_adapter": "erc20_exact_input",
        "execution_permitted": True,
        "authority_level": "preparation_verified",
        "preparation_verified": True,
        "live_execution_verified": False,
        "initial_acceptance_wallet_reject_only": True,
        "successful_broadcast_authorized": False,
        "input": USDG,
        "output": WETH,
        "objective": {"id": "weth-usdg-objective"},
        "capability": {
            "id": "weth-usdg-preparation-capability",
            "enabled": True,
            "indicative_status": "available",
            "firm_plan_status": "available",
            "execution_status": "preparation_verified",
            "probe_amount": "1",
            "evidence": evidence,
        },
        "execution_ceiling": {"amount": "1", "asset": "USDG"},
    }
    return {
        "from_asset": "USDG",
        "to_asset": "WETH",
        "amount_mode": "exact_input",
        "mechanism": "swap",
        "indicative_status": "available",
        "firm_plan_status": "available",
        "execution_status": "preparation_verified",
        "enabled": True,
        "probe_amount": "1",
        "evidence": evidence,
        "execution_authority": authority,
    }


def eth_buy_preparation_authority_capability():
    """Mirror the router-resolved ETH-USDG direction authority used in production."""
    capability = weth_preparation_authority_capability()
    capability["symbol"] = "ETH-USDG"
    capability["to_asset"] = "ETH"
    capability["probe_amount"] = "2"

    authority = capability["execution_authority"]
    authority["symbol"] = "ETH-USDG"
    authority["output"] = ETH
    authority["objective"] = {"id": "eth-usdg-objective"}
    authority["execution_ceiling"] = {"amount": "2", "asset": "USDG"}

    authority_capability = authority["capability"]
    authority_capability["id"] = "eth-usdg-preparation-capability"
    authority_capability["probe_amount"] = "2"

    evidence = authority_capability["evidence"]
    evidence["symbol"] = "ETH-USDG"
    evidence["to_asset"] = "ETH"
    evidence["verified_input_amount"] = "2"
    evidence["firm_plan_input_ceiling"] = "2"
    capability["evidence"] = evidence
    return capability


def weth_live_authorized_capability():
    capability = weth_preparation_authority_capability()
    authority = capability["execution_authority"]
    evidence = authority["capability"]["evidence"]
    authorization = {
        "status": "live_authorized_pending_confirmation",
        "tranche": "R5C.5A",
        "authorization_id": "ee" * 16,
        "operator_confirmed": True,
        "authorized_at": datetime.now(timezone.utc).isoformat(),
        "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        "symbol": "WETH-USDG",
        "side": "buy",
        "amount_mode": "exact_input",
        "provider": "0x",
        "input_asset": "USDG",
        "output_asset": "WETH",
        "exact_input_amount": "1",
        "wallet_address": TAKER,
        "approval_model": "finite_exact_input",
        "unlimited_approval_enabled": False,
        "approval_transaction_value_wei": "0",
        "swap_transaction_value_wei": "0",
        "separate_wallet_requests_required": True,
        "automatic_second_transaction": False,
        "automatic_retry": False,
        "automatic_execution_promotion": False,
    }
    evidence["successful_broadcast_authorized"] = True
    evidence["live_authorized_pending_confirmation"] = True
    evidence["live_authorization"] = authorization
    authority["authority_level"] = "live_authorized_pending_confirmation"
    authority["live_authorized_pending_confirmation"] = True
    authority["preparation_verified"] = True
    authority["live_execution_verified"] = False
    authority["initial_acceptance_wallet_reject_only"] = False
    authority["successful_broadcast_authorized"] = True
    authority["live_authorization"] = authorization
    return capability


def weth_sell_preparation_authority_capability():
    evidence = {
        "preparation_verified": True,
        "live_accepted": False,
        "successful_broadcast": False,
        "tranche": "R5C.4B",
        "symbol": "WETH-USDG",
        "side": "sell",
        "amount_mode": "exact_input",
        "provider": "0x",
        "from_asset": "WETH",
        "to_asset": "USDG",
        "verified_input_amount": "0.0001",
        "firm_plan_input_ceiling": "0.0001",
    }
    authority = {
        "symbol": "WETH-USDG",
        "side": "sell",
        "amount_mode": "exact_input",
        "provider": "0x",
        "execution_adapter": "erc20_exact_input",
        "execution_permitted": True,
        "authority_level": "preparation_verified",
        "preparation_verified": True,
        "live_execution_verified": False,
        "initial_acceptance_wallet_reject_only": True,
        "successful_broadcast_authorized": False,
        "input": WETH,
        "output": USDG,
        "objective": {"id": "weth-usdg-objective"},
        "capability": {
            "id": "weth-usdg-sell-preparation-capability",
            "enabled": True,
            "indicative_status": "available",
            "firm_plan_status": "available",
            "execution_status": "preparation_verified",
            "probe_amount": "0.0001",
            "evidence": evidence,
        },
        "execution_ceiling": {"amount": "0.0001", "asset": "WETH"},
    }
    return {
        "from_asset": "WETH",
        "to_asset": "USDG",
        "amount_mode": "exact_input",
        "mechanism": "swap",
        "indicative_status": "available",
        "firm_plan_status": "available",
        "execution_status": "preparation_verified",
        "enabled": True,
        "probe_amount": "0.0001",
        "evidence": evidence,
        "execution_authority": authority,
    }


def weth_sell_live_authorized_capability():
    capability = weth_sell_preparation_authority_capability()
    authority = capability["execution_authority"]
    evidence = authority["capability"]["evidence"]
    authorization = {
        "status": "live_authorized_pending_confirmation",
        "tranche": "R5C.5B",
        "authorization_id": "ff" * 16,
        "operator_confirmed": True,
        "authorized_at": datetime.now(timezone.utc).isoformat(),
        "expires_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat(),
        "symbol": "WETH-USDG",
        "side": "sell",
        "amount_mode": "exact_input",
        "provider": "0x",
        "input_asset": "WETH",
        "output_asset": "USDG",
        "exact_input_amount": "0.0001",
        "wallet_address": TAKER,
        "approval_model": "finite_exact_input",
        "unlimited_approval_enabled": False,
        "approval_transaction_value_wei": "0",
        "swap_transaction_value_wei": "0",
        "separate_wallet_requests_required": True,
        "automatic_second_transaction": False,
        "automatic_retry": False,
        "automatic_execution_promotion": False,
    }
    evidence["successful_broadcast_authorized"] = True
    evidence["live_authorized_pending_confirmation"] = True
    evidence["live_authorization"] = authorization
    authority["authority_level"] = "live_authorized_pending_confirmation"
    authority["live_authorized_pending_confirmation"] = True
    authority["preparation_verified"] = True
    authority["live_execution_verified"] = False
    authority["initial_acceptance_wallet_reject_only"] = False
    authority["successful_broadcast_authorized"] = True
    authority["live_authorization"] = authorization
    return capability


class FakePlanningService:
    def __init__(self, allowance_atomic: int = 0) -> None:
        self.allowance_atomic = int(allowance_atomic)
        self.calls: list[dict] = []
        self.counter = 0
        self.output_asset_override: str | None = None

    async def firm_quote_plan(self, **kwargs):
        self.calls.append(dict(kwargs))
        self.counter += 1
        side = str(kwargs.get("side") or "buy").strip().lower()
        base_token = dict(kwargs.get("base_token") or kwargs.get("eth_token") or ETH)
        quote_token = dict(kwargs.get("quote_token") or USDG)
        input_token = base_token if side == "sell" else quote_token
        output_token = quote_token if side == "sell" else base_token
        input_amount = str(kwargs.get("quantity") if side == "sell" else kwargs.get("total_quote"))
        input_decimals = int(input_token.get("decimals") or 0)
        input_atomic = str(int(Decimal(input_amount) * (Decimal(10) ** input_decimals)))
        output_asset = self.output_asset_override or str(output_token.get("symbol") or "ETH").upper()
        if side == "sell":
            output_amount = Decimal("0.25")
        else:
            output_amount = Decimal(input_amount) * Decimal("0.00053")
        output_decimals = int(output_token.get("decimals") or 0)
        output_atomic = str(int(output_amount * (Decimal(10) ** output_decimals)))
        minimum_atomic = str(int(Decimal(output_atomic) * Decimal("0.99")))
        fetched = datetime.now(timezone.utc) + timedelta(milliseconds=self.counter)
        expires = fetched + timedelta(seconds=30)
        digest = hashlib.sha256(bytes.fromhex(SWAP_CALLDATA[2:])).hexdigest()
        return {
            "ok": True,
            "chain_id": 4663,
            "symbol": str(kwargs.get("symbol") or f"{base_token.get('symbol')}-{quote_token.get('symbol')}"),
            "side": side,
            "amount_mode": "exact_input",
            "input_asset": str(input_token.get("symbol") or "").upper(),
            "input_amount": input_amount,
            "input_amount_atomic": input_atomic,
            "output_asset": output_asset,
            "output_amount": str(Decimal(output_atomic) / (Decimal(10) ** output_decimals)),
            "output_amount_atomic": output_atomic,
            "minimum_received": str(Decimal(minimum_atomic) / (Decimal(10) ** output_decimals)),
            "minimum_received_atomic": minimum_atomic,
            "slippage_bps": int(kwargs["slippage_bps"]),
            "allowance": {
                "applicable": True,
                "read_method": "eth_call",
                "token": {"symbol": str(input_token.get("symbol") or "").upper()},
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
        self.weth_balance_atomic = 0
        self.native_balances_by_tag: dict[str, int] = {}
        self.usdg_balances_by_tag: dict[str, int] = {}
        self.weth_balances_by_tag: dict[str, int] = {}
        self.approval_amount_atomic = 2_000_000
        self.approval_token_address = ROBINHOOD_CHAIN_SWAP_USDG_CONTRACT
        self.historical_state_error: dict | None = None
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
        if str(block_tag) != "latest" and isinstance(self.historical_state_error, dict):
            return {
                "ok": False,
                "address": address,
                "block_tag": str(block_tag),
                "error": "native_balance_rpc_failed",
                "rpc": {"ok": False, "error": dict(self.historical_state_error)},
            }
        balance = self.native_balances_by_tag.get(str(block_tag), self.native_balance_wei)
        return {"ok": True, "balance_wei": str(balance)}

    async def get_erc20_balance(
        self, address, contract_address, decimals, *, block_tag="latest", force_refresh=True
    ):
        self.calls.append(("erc20_balance", address, contract_address, decimals, block_tag, force_refresh))
        if str(block_tag) != "latest" and isinstance(self.historical_state_error, dict):
            return {
                "ok": False,
                "owner_address": address,
                "contract_address": contract_address,
                "block_tag": str(block_tag),
                "error": "erc20_balance_rpc_failed",
                "rpc": {"ok": False, "error": dict(self.historical_state_error)},
            }
        is_weth = str(contract_address or "").lower() == WETH["contract_address"].lower()
        if is_weth:
            balance = self.weth_balances_by_tag.get(str(block_tag), self.weth_balance_atomic)
        else:
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
                        "to": self.approval_token_address,
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


def weth_receive_log(amount_atomic: int):
    return {
        "address": WETH["contract_address"],
        "topics": [
            TRANSFER_TOPIC0,
            "0x" + SPENDER[2:].lower().rjust(64, "0"),
            "0x" + TAKER[2:].lower().rjust(64, "0"),
        ],
        "data": hex(amount_atomic),
    }


def weth_spend_log(amount_atomic: int):
    return {
        "address": WETH["contract_address"],
        "topics": [
            TRANSFER_TOPIC0,
            "0x" + TAKER[2:].lower().rjust(64, "0"),
            "0x" + SPENDER[2:].lower().rjust(64, "0"),
        ],
        "data": hex(amount_atomic),
    }


def usdg_receive_log(amount_atomic: int):
    return {
        "address": ROBINHOOD_CHAIN_SWAP_USDG_CONTRACT,
        "topics": [
            TRANSFER_TOPIC0,
            "0x" + SPENDER[2:].lower().rjust(64, "0"),
            "0x" + TAKER[2:].lower().rjust(64, "0"),
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
            route_capability=eth_buy_preparation_authority_capability(),
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
            route_capability=weth_live_authorized_capability(),
            confirm_prepare=True,
        )

    async def prepare_weth_sell(self, amount="0.0001", *, live_authorized=False):
        return await self.service.prepare(
            self.db,
            taker_address=TAKER,
            exact_input_amount=amount,
            slippage_bps=100,
            eth_token=None,
            usdg_token=USDG,
            side="sell",
            from_asset="WETH",
            from_token=WETH,
            to_asset="USDG",
            to_token=USDG,
            symbol="WETH-USDG",
            route_capability=(
                weth_sell_live_authorized_capability()
                if live_authorized
                else weth_sell_preparation_authority_capability()
            ),
            confirm_prepare=True,
        )

    async def submitted_r5c5b_sell(self):
        prepared = await self.prepare_weth_sell(live_authorized=True)
        execution_id = prepared["execution"]["id"]
        row = self.db.get(RobinhoodChainSwapExecution, execution_id)
        row.status = "approval_confirmed"
        row.approval_status = "confirmed"
        row.allowance_current_atomic = row.exact_input_amount_atomic
        row.allowance_shortfall_atomic = "0"
        row.approval_required = False
        row.approval_tx_hash = APPROVAL_TX_HASH
        route = dict(row.route or {})
        lifecycle = dict(route.get("execution_lifecycle") or {})
        lifecycle["approval"] = {
            "tx_hash": APPROVAL_TX_HASH,
            "receipt_status": 1,
            "submission_attempts": 1,
            "gas_used": "50000",
            "effective_gas_price_wei": "80000000",
        }
        route["execution_lifecycle"] = lifecycle
        row.route = route
        self.db.add(row)
        self.db.commit()
        self.rpc.allowance_atomic = 100_000_000_000_000
        self.planning.allowance_atomic = 100_000_000_000_000
        capability = weth_sell_live_authorized_capability()
        swap = await self.service.prepare_swap(
            self.db,
            execution_id=execution_id,
            wallet_address=TAKER,
            eth_token=None,
            usdg_token=WETH,
            output_token=USDG,
            route_capability=capability,
            confirm_prepare=True,
        )
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            await self.service.claim_swap_send(
                self.db,
                execution_id=execution_id,
                wallet_address=TAKER,
                plan_hash=swap["execution"]["swap"]["plan_hash"],
                claim_id=SWAP_CLAIM,
                confirm_send_claim=True,
                execution_authority=capability["execution_authority"],
            )
        self.service.record_swap_submission(
            self.db,
            execution_id=execution_id,
            tx_hash=SWAP_TX_HASH,
            wallet_address=TAKER,
            claim_id=SWAP_CLAIM,
            confirm_record=True,
        )
        return execution_id

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

    def test_registry_planner_calls_match_current_signature(self):
        service_path = (
            BACKEND_ROOT / "app" / "services" / "robinhood_chain_swap_execution.py"
        )
        tree = ast.parse(
            service_path.read_text(encoding="utf-8"),
            filename=str(service_path),
        )
        planner_keywords = (
            set(
                inspect.signature(
                    RobinhoodChainTransactionPlanningService.firm_quote_plan
                ).parameters
            )
            - {"self"}
        )
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "firm_quote_plan"
        ]
        registry_calls = []
        for call in calls:
            keywords = {keyword.arg for keyword in call.keywords if keyword.arg}
            if "requested_amount" in keywords:
                registry_calls.append(keywords)

        self.assertEqual(len(registry_calls), 2)
        required = {
            "symbol",
            "side",
            "amount_mode",
            "requested_amount",
            "maximum_input_amount",
            "taker_address",
            "base_token",
            "quote_token",
            "native_token",
            "registry_tokens",
            "route_capability",
            "slippage_bps",
        }
        for keywords in registry_calls:
            self.assertTrue(keywords <= planner_keywords)
            self.assertTrue(required <= keywords)
            self.assertNotIn("quantity", keywords)
            self.assertNotIn("total_quote", keywords)

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

    async def test_historical_authorization_id_change_does_not_replace_fresh_current_amount_lifecycle(self):
        first_capability = weth_live_authorized_capability()
        first = await self.service.prepare(
            self.db, taker_address=TAKER, exact_input_amount="1", slippage_bps=100,
            eth_token=None, usdg_token=USDG, to_asset="WETH", to_token=WETH,
            route_capability=first_capability, confirm_prepare=True,
        )
        first_id = first["execution"]["id"]

        renewed_capability = weth_live_authorized_capability()
        renewed_authorization = renewed_capability["execution_authority"]["live_authorization"]
        renewed_authorization["authorization_id"] = "ff" * 16
        renewed_capability["execution_authority"]["capability"]["evidence"][
            "live_authorization"
        ]["authorization_id"] = "ff" * 16

        second = await self.service.prepare(
            self.db, taker_address=TAKER, exact_input_amount="1", slippage_bps=100,
            eth_token=None, usdg_token=USDG, to_asset="WETH", to_token=WETH,
            route_capability=renewed_capability, confirm_prepare=True,
        )

        self.assertTrue(second["idempotent"])
        self.assertEqual(first_id, second["execution"]["id"])
        self.assertEqual(len(self.planning.calls), 1)
        self.assertNotEqual(
            self.db.get(RobinhoodChainSwapExecution, first_id).status,
            "expired",
        )

    async def test_prepare_starts_new_lifecycle_after_matching_confirmed_execution(self):
        self.planning.allowance_atomic = 2_000_000
        first = await self.prepare_weth()
        first_id = first["execution"]["id"]
        first_row = self.db.get(RobinhoodChainSwapExecution, first_id)
        first_row.status = "confirmed"
        first_row.approval_status = "not_required"
        first_row.swap_status = "confirmed"
        first_row.swap_tx_hash = SWAP_TX_HASH
        self.db.commit()

        second = await self.prepare_weth()

        self.assertFalse(second["idempotent"])
        self.assertNotEqual(first_id, second["execution"]["id"])
        self.assertEqual(second["execution"]["status"], "allowance_sufficient")
        self.assertEqual(self.db.query(RobinhoodChainSwapExecution).count(), 2)
        self.assertEqual(len(self.planning.calls), 2)
        self.assertEqual(
            self.db.get(RobinhoodChainSwapExecution, first_id).status,
            "confirmed",
        )

    async def test_latest_matching_lifecycle_is_read_only_and_returns_newest_row(self):
        self.planning.allowance_atomic = 2_000_000
        first = await self.prepare_weth()
        first_id = first["execution"]["id"]
        first_row = self.db.get(RobinhoodChainSwapExecution, first_id)
        first_row.status = "confirmed"
        first_row.approval_status = "not_required"
        first_row.swap_status = "confirmed"
        first_row.swap_tx_hash = SWAP_TX_HASH
        self.db.commit()
        second = await self.prepare_weth()
        before_count = self.db.query(RobinhoodChainSwapExecution).count()

        restored = self.service.latest(
            self.db,
            symbol="WETH-USDG",
            side="buy",
            amount_mode="exact_spend",
            wallet_address=TAKER,
        )

        self.assertTrue(restored["ok"])
        self.assertTrue(restored["read_only"])
        self.assertFalse(restored["will_mutate"])
        self.assertEqual(restored["lookup"]["kind"], "latest_matching_lifecycle")
        self.assertEqual(restored["lookup"]["wallet_address"], TAKER)
        self.assertEqual(restored["execution"]["id"], second["execution"]["id"])
        self.assertEqual(self.db.query(RobinhoodChainSwapExecution).count(), before_count)

    def test_router_latest_lifecycle_route_precedes_dynamic_execution_route(self):
        source = (BACKEND_ROOT / "app" / "routers" / "robinhood_chain.py").read_text(encoding="utf-8")
        latest_route = '@router.get("/swap-execution/latest")'
        dynamic_route = '@router.get("/swap-execution/{execution_id}")'
        self.assertIn(latest_route, source)
        self.assertLess(source.index(latest_route), source.index(dynamic_route))
        self.assertIn("get_robinhood_chain_swap_execution_service().latest", source)
        self.assertIn('"read_only": True', source)
        self.assertIn('"wallet_connection_requested": False', source)
        self.assertIn('"signing_enabled": False', source)
        self.assertIn('"broadcast_enabled": False', source)

    async def test_sufficient_allowance_returns_no_approval_transaction(self):
        self.planning.allowance_atomic = 2_000_000
        result = await self.prepare()
        self.assertFalse(result["approval_required"])
        self.assertIsNone(result["approval_transaction_plan"])
        self.assertEqual(result["execution"]["status"], "allowance_sufficient")
        self.assertEqual(result["execution"]["approval_status"], "not_required")

    async def test_current_amount_above_historical_five_usdg_limit_is_allowed(self):
        result = await self.prepare(amount="5.000001")
        self.assertEqual(result["execution"]["exact_input_amount"], "5.000001")
        self.assertEqual(result["execution"]["approval"]["amount_atomic"], "5000001")
        self.assertEqual(len(self.planning.calls), 1)

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

    async def test_status_enables_separate_weth_swap_stage(self):
        status = self.service.status()
        self.assertIn("WETH", status["approval_to_assets"])
        self.assertNotIn("WETH", status["approval_only_to_assets"])
        self.assertTrue(status["weth_approval_enabled"])
        self.assertTrue(status["weth_swap_enabled"])
        self.assertIn("WETH", status["swap_stage_enabled_to_assets"])
        self.assertFalse(status["automatic_second_transaction"])

    async def test_prepare_weth_persists_finite_approval_and_separate_swap_lifecycle(self):
        result = await self.prepare_weth("1")
        self.assertTrue(result["ok"])
        row = result["execution"]
        self.assertEqual(row["tranche"], "RH-EXEC.AMT.1")
        self.assertEqual(row["symbol"], "WETH-USDG")
        self.assertEqual(row["from_asset"], "USDG")
        self.assertEqual(row["to_asset"], "WETH")
        self.assertFalse(row["to_native"])
        self.assertEqual(row["to_contract_address"].lower(), WETH["contract_address"].lower())
        self.assertEqual(row["exact_input_amount"], "1")
        self.assertEqual(row["approval"]["amount_atomic"], "1000000")
        self.assertEqual(row["approval"]["amount_policy"], "set_total_required_allowance")
        self.assertFalse(row["approval_only"])
        self.assertTrue(row["swap_stage_enabled"])
        self.assertFalse(row["swap_execution_enabled"])
        self.assertIsNone(row["swap_stage_locked_reason"])
        self.assertEqual(row["swap_status"], "review_only")
        self.assertFalse(row["automatic_second_transaction"])

    async def test_weth_requires_distinct_non_native_token_identity(self):
        bad_weth = {**WETH, "native": True}
        with self.assertRaisesRegex(ValueError, "robinhood_chain_swap_weth_identity_mismatch"):
            await self.service.prepare(
                self.db, taker_address=TAKER, exact_input_amount="1", slippage_bps=100,
                eth_token=None, usdg_token=USDG, to_asset="WETH", to_token=bad_weth,
                route_capability=weth_live_authorized_capability(), confirm_prepare=True,
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

    async def test_preparation_verified_direction_can_claim_finite_approval_without_amount_preauthorization(self):
        prepared = await self.service.prepare(
            self.db,
            taker_address=TAKER,
            exact_input_amount="1",
            slippage_bps=100,
            eth_token=None,
            usdg_token=USDG,
            to_asset="WETH",
            to_token=WETH,
            route_capability=weth_preparation_authority_capability(),
            confirm_prepare=True,
        )
        execution_id = prepared["execution"]["id"]
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            claimed = self.service.claim_approval_send(
                self.db,
                execution_id=execution_id,
                wallet_address=TAKER,
                plan_hash=prepared["execution"]["approval"]["plan_hash"],
                claim_id=APPROVAL_CLAIM,
                confirm_send_claim=True,
            )
        self.assertTrue(claimed["ok"])
        row = self.db.get(RobinhoodChainSwapExecution, execution_id)
        self.assertEqual(row.status, "approval_send_claimed")
        self.assertIsNone(row.approval_tx_hash)
        self.assertIsNone(row.swap_tx_hash)

    async def test_historical_live_authorization_wallet_does_not_replace_saved_wallet_validation(self):
        capability = weth_live_authorized_capability()
        capability["execution_authority"]["live_authorization"]["wallet_address"] = "0x" + "99" * 20
        capability["execution_authority"]["capability"]["evidence"]["live_authorization"]["wallet_address"] = "0x" + "99" * 20
        prepared = await self.service.prepare(
            self.db, taker_address=TAKER, exact_input_amount="1", slippage_bps=100,
            eth_token=None, usdg_token=USDG, to_asset="WETH", to_token=WETH,
            route_capability=capability, confirm_prepare=True,
        )
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            claimed = self.service.claim_approval_send(
                self.db, execution_id=prepared["execution"]["id"], wallet_address=TAKER,
                plan_hash=prepared["execution"]["approval"]["plan_hash"],
                claim_id=APPROVAL_CLAIM, confirm_send_claim=True,
            )
        self.assertTrue(claimed["ok"])

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
        self.assertFalse(failed["execution"]["approval_only"])
        self.assertTrue(failed["execution"]["swap_stage_enabled"])
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
        self.assertFalse(refreshed["execution"]["approval_only"])
        self.assertTrue(refreshed["execution"]["swap_stage_enabled"])
        self.assertFalse(refreshed["execution"]["automatic_second_transaction"])

    async def test_historical_authorization_id_change_does_not_require_second_approval(self):
        prepared = await self.prepare_weth("1")
        execution_id = prepared["execution"]["id"]
        original_authorization_id = (
            prepared["execution"]["swap"]["route"]["execution_authority"]
            ["live_authorization"]["authorization_id"]
        )
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
        approved = await self.service.refresh_approval(self.db, execution_id=execution_id)
        self.assertEqual(approved["execution"]["status"], "approval_confirmed")

        renewed_capability = weth_live_authorized_capability()
        renewed_authorization = renewed_capability["execution_authority"]["live_authorization"]
        renewed_authorization["authorization_id"] = "ff" * 16
        renewed_capability["execution_authority"]["capability"]["evidence"][
            "live_authorization"
        ]["authorization_id"] = "ff" * 16
        self.planning.allowance_atomic = 1_000_000

        swap = await self.service.prepare_swap(
            self.db, execution_id=execution_id, wallet_address=TAKER,
            eth_token=None, usdg_token=USDG, output_token=WETH,
            route_capability=renewed_capability, confirm_prepare=True,
        )

        self.assertEqual(swap["execution"]["id"], execution_id)
        self.assertEqual(swap["execution"]["status"], "swap_prepared")
        self.assertEqual(swap["execution"]["approval"]["tx_hash"], APPROVAL_TX_HASH)
        self.assertEqual(swap["execution"]["approval"]["receipt_status"], 1)
        self.assertEqual(swap["execution"]["approval"]["submission_attempts"], 1)
        self.assertEqual(swap["execution"]["allowance"]["current_atomic"], "1000000")
        self.assertEqual(swap["execution"]["allowance"]["shortfall_atomic"], "0")
        self.assertIsNone(swap["execution"]["swap"]["tx_hash"])
        self.assertFalse(swap["execution"]["swap"]["send_claimed"])
        self.assertFalse(swap["execution"]["automatic_second_transaction"])
        rebound_authorization_id = (
            swap["execution"]["swap"]["route"]["execution_authority"]
            ["live_authorization"]["authorization_id"]
        )
        self.assertNotEqual(rebound_authorization_id, original_authorization_id)
        self.assertEqual(rebound_authorization_id, "ff" * 16)
        self.assertEqual(self.planning.calls[-1]["route_capability"], renewed_capability)

    async def test_weth_prepare_swap_requires_fresh_allowance_and_weth_plan(self):
        prepared = await self.prepare_weth("1")
        row = self.db.get(RobinhoodChainSwapExecution, prepared["execution"]["id"])
        row.status = "approval_confirmed"
        row.approval_status = "confirmed"
        row.allowance_current_atomic = row.exact_input_amount_atomic
        row.allowance_shortfall_atomic = "0"
        row.approval_required = False
        self.db.add(row); self.db.commit()

        with self.assertRaisesRegex(ValueError, "fresh_allowance_insufficient"):
            await self.service.prepare_swap(
                self.db, execution_id=row.id, wallet_address=TAKER,
                eth_token=None, usdg_token=USDG, output_token=WETH,
                route_capability=weth_live_authorized_capability(), confirm_prepare=True,
            )

        self.rpc.allowance_atomic = 1_000_000
        self.planning.allowance_atomic = 1_000_000
        live_capability = weth_live_authorized_capability()
        swap = await self.service.prepare_swap(
            self.db, execution_id=row.id, wallet_address=TAKER,
            eth_token=None, usdg_token=USDG, output_token=WETH,
            route_capability=live_capability, confirm_prepare=True,
        )
        self.assertEqual(swap["execution"]["status"], "swap_prepared")
        self.assertEqual(swap["execution"]["symbol"], "WETH-USDG")
        self.assertEqual(swap["execution"]["to_asset"], "WETH")
        self.assertEqual(swap["unsigned_transaction_plan"]["output_asset"], "WETH")
        self.assertEqual(swap["unsigned_transaction_plan"]["value_wei"], "0")
        self.assertFalse(swap["execution"]["automatic_second_transaction"])
        self.assertEqual(self.planning.calls[-1]["symbol"], "WETH-USDG")
        self.assertEqual(self.planning.calls[-1]["base_token"]["symbol"], "WETH")
        self.assertEqual(self.planning.calls[-1]["route_capability"], live_capability)

    async def test_weth_swap_claim_and_wallet_rejection_are_separate_and_hash_free(self):
        prepared = await self.prepare_weth("1")
        row = self.db.get(RobinhoodChainSwapExecution, prepared["execution"]["id"])
        row.status = "approval_confirmed"
        row.approval_status = "confirmed"
        row.allowance_current_atomic = row.exact_input_amount_atomic
        row.allowance_shortfall_atomic = "0"
        row.approval_required = False
        self.db.add(row); self.db.commit()
        self.rpc.allowance_atomic = 1_000_000
        self.planning.allowance_atomic = 1_000_000
        swap = await self.service.prepare_swap(
            self.db, execution_id=row.id, wallet_address=TAKER,
            eth_token=None, usdg_token=USDG, output_token=WETH,
            route_capability=weth_live_authorized_capability(), confirm_prepare=True,
        )
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            claimed = await self.service.claim_swap_send(
                self.db, execution_id=row.id, wallet_address=TAKER,
                plan_hash=swap["execution"]["swap"]["plan_hash"], claim_id=SWAP_CLAIM,
                confirm_send_claim=True,
            )
        self.assertEqual(claimed["execution"]["status"], "swap_send_claimed")
        failed = self.service.record_submission_failure(
            self.db, execution_id=row.id, stage="swap", wallet_address=TAKER,
            claim_id=SWAP_CLAIM, reason="wallet_rejected", message="declined",
            confirm_failure=True,
        )
        self.assertEqual(failed["execution"]["status"], "swap_wallet_rejected")
        self.assertIsNone(failed["execution"]["swap"]["tx_hash"])
        self.assertFalse(failed["execution"]["automatic_second_transaction"])

    async def test_confirmed_weth_swap_reconciles_transfer_balance_and_eth_gas(self):
        prepared = await self.prepare_weth("1")
        execution_id = prepared["execution"]["id"]
        row = self.db.get(RobinhoodChainSwapExecution, execution_id)
        row.status = "approval_confirmed"
        row.approval_status = "confirmed"
        row.allowance_current_atomic = row.exact_input_amount_atomic
        row.allowance_shortfall_atomic = "0"
        row.approval_required = False
        self.db.add(row); self.db.commit()
        self.rpc.allowance_atomic = 1_000_000
        self.planning.allowance_atomic = 1_000_000
        swap = await self.service.prepare_swap(
            self.db, execution_id=execution_id, wallet_address=TAKER,
            eth_token=None, usdg_token=USDG, output_token=WETH,
            route_capability=weth_live_authorized_capability(), confirm_prepare=True,
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
        actual_output_atomic = 530_000_000_000_000
        swap_fee_wei = 245_000 * 80_000_000
        before_tag = hex(123455)
        after_tag = hex(123456)
        self.rpc.native_balances_by_tag[before_tag] = self.rpc.native_balance_wei
        self.rpc.native_balances_by_tag[after_tag] = self.rpc.native_balance_wei - swap_fee_wei
        self.rpc.usdg_balances_by_tag[before_tag] = 3_710_769
        self.rpc.usdg_balances_by_tag[after_tag] = 2_710_769
        self.rpc.weth_balances_by_tag[before_tag] = 0
        self.rpc.weth_balances_by_tag[after_tag] = actual_output_atomic
        self.rpc.swap_receipt = receipt(
            SWAP_TX_HASH,
            1,
            logs=[usdg_spend_log(1_000_000), weth_receive_log(actual_output_atomic)],
        )
        result = await self.service.refresh_swap(self.db, execution_id=execution_id)
        reconciled = result["execution"]
        self.assertEqual(reconciled["status"], "confirmed")
        self.assertEqual(reconciled["actual_input_asset"], "USDG")
        self.assertEqual(reconciled["actual_input_amount"], "1")
        self.assertEqual(reconciled["actual_output_asset"], "WETH")
        self.assertEqual(reconciled["actual_output_amount_atomic"], str(actual_output_atomic))
        self.assertEqual(reconciled["actual_output_amount"], "0.00053")
        self.assertEqual(reconciled["actual_network_fee_wei"], str(swap_fee_wei))
        self.assertEqual(reconciled["reconciliation"]["output_transfer_log_count"], 1)
        self.assertFalse(reconciled["automatic_second_transaction"])

    async def test_weth_reconciliation_rejects_transfer_and_balance_mismatch(self):
        prepared = await self.prepare_weth("1")
        execution_id = prepared["execution"]["id"]
        row = self.db.get(RobinhoodChainSwapExecution, execution_id)
        row.status = "approval_confirmed"
        row.approval_status = "confirmed"
        row.allowance_current_atomic = row.exact_input_amount_atomic
        row.allowance_shortfall_atomic = "0"
        row.approval_required = False
        self.db.add(row); self.db.commit()
        self.rpc.allowance_atomic = 1_000_000
        self.planning.allowance_atomic = 1_000_000
        swap = await self.service.prepare_swap(
            self.db, execution_id=execution_id, wallet_address=TAKER,
            eth_token=None, usdg_token=USDG, output_token=WETH,
            route_capability=weth_live_authorized_capability(), confirm_prepare=True,
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
        self.rpc.native_balances_by_tag[after_tag] = self.rpc.native_balance_wei - (245_000 * 80_000_000)
        self.rpc.usdg_balances_by_tag[before_tag] = 3_710_769
        self.rpc.usdg_balances_by_tag[after_tag] = 2_710_769
        self.rpc.weth_balances_by_tag[before_tag] = 0
        self.rpc.weth_balances_by_tag[after_tag] = 529_999_999_999_999
        self.rpc.swap_receipt = receipt(
            SWAP_TX_HASH,
            1,
            logs=[usdg_spend_log(1_000_000), weth_receive_log(530_000_000_000_000)],
        )
        with self.assertRaisesRegex(ValueError, "erc20_output_balance_delta_mismatch"):
            await self.service.refresh_swap(self.db, execution_id=execution_id)

    async def test_r5c4a_preparation_authority_builds_bounded_weth_lifecycle(self):
        capability = weth_preparation_authority_capability()
        result = await self.service.prepare(
            self.db,
            taker_address=TAKER,
            exact_input_amount="1",
            slippage_bps=100,
            eth_token=None,
            usdg_token=USDG,
            to_asset="WETH",
            to_token=WETH,
            route_capability=capability,
            confirm_prepare=True,
        )
        row = result["execution"]
        self.assertEqual(row["tranche"], "RH-EXEC.AMT.1")
        self.assertEqual(row["symbol"], "WETH-USDG")
        self.assertEqual(row["exact_input_amount"], "1")
        self.assertFalse(row["to_native"])
        stored = row["swap"]["route"]["execution_authority"]
        self.assertEqual(stored["authority_level"], "preparation_verified")
        self.assertTrue(stored["preparation_verified"])
        self.assertFalse(stored["live_execution_verified"])
        self.assertTrue(stored["initial_acceptance_wallet_reject_only"])
        self.assertFalse(stored["successful_broadcast_authorized"])
        self.assertFalse(row["automatic_second_transaction"])

    async def test_preparation_authority_allows_current_amount_above_historical_evidence(self):
        result = await self.service.prepare(
            self.db,
            taker_address=TAKER,
            exact_input_amount="1.000001",
            slippage_bps=100,
            eth_token=None,
            usdg_token=USDG,
            to_asset="WETH",
            to_token=WETH,
            route_capability=weth_preparation_authority_capability(),
            confirm_prepare=True,
        )
        self.assertEqual(result["execution"]["exact_input_amount"], "1.000001")
        self.assertEqual(len(self.planning.calls), 1)

    async def test_r5c4a_malformed_preparation_evidence_fails_before_provider(self):
        capability = weth_preparation_authority_capability()
        capability["execution_authority"]["capability"]["evidence"]["to_asset"] = "ETH"
        with self.assertRaisesRegex(ValueError, "robinhood_chain_swap_execution_capability_not_verified"):
            await self.service.prepare(
                self.db,
                taker_address=TAKER,
                exact_input_amount="1",
                slippage_bps=100,
                eth_token=None,
                usdg_token=USDG,
                to_asset="WETH",
                to_token=WETH,
                route_capability=capability,
                confirm_prepare=True,
            )
        self.assertEqual(self.planning.calls, [])

    async def test_weth_sell_preparation_builds_finite_approval_and_enables_separate_swap_stage(self):
        result = await self.prepare_weth_sell()
        self.assertTrue(result["ok"])
        self.assertFalse(result["idempotent"])
        self.assertTrue(result["approval_required"])
        row = result["execution"]
        self.assertEqual(row["tranche"], "RH-EXEC.AMT.1")
        self.assertEqual(row["symbol"], "WETH-USDG")
        self.assertEqual(row["side"], "sell")
        self.assertEqual(row["from_asset"], "WETH")
        self.assertEqual(row["to_asset"], "USDG")
        self.assertEqual(row["exact_input_amount"], "0.0001")
        self.assertEqual(row["exact_input_amount_atomic"], "100000000000000")
        self.assertFalse(row["from_native"])
        self.assertFalse(row["to_native"])
        self.assertFalse(row["approval_only"])
        self.assertTrue(row["swap_stage_enabled"])
        self.assertFalse(row["swap_execution_enabled"])
        self.assertIsNone(row["swap_stage_locked_reason"])
        self.assertEqual(row["allowance"]["token_address"].lower(), WETH["contract_address"].lower())
        self.assertEqual(row["approval"]["transaction_to"].lower(), WETH["contract_address"].lower())
        self.assertEqual(row["approval"]["amount"], "0.0001")
        self.assertEqual(row["approval"]["amount_atomic"], "100000000000000")
        self.assertEqual(row["approval"]["transaction_value_wei"], "0")
        self.assertTrue(row["approval"]["finite_approval"])
        self.assertFalse(row["approval"]["unlimited_approval"])
        self.assertIsNone(result.get("unsigned_transaction_plan"))
        approval = result["approval_transaction_plan"]
        self.assertEqual(approval["token"].lower(), WETH["contract_address"].lower())
        self.assertEqual(approval["to"].lower(), WETH["contract_address"].lower())
        self.assertEqual(approval["approval_amount_atomic"], "100000000000000")
        self.assertEqual(approval["value_wei"], "0")
        self.assertTrue(approval["finite_approval"])
        self.assertFalse(approval["unlimited_approval"])
        self.assertFalse(approval["wallet_connection_requested"])
        self.assertFalse(approval["signing_requested"])
        self.assertFalse(approval["broadcast_requested"])
        stored = row["swap"]["route"]["execution_authority"]
        self.assertEqual(stored["authority_level"], "preparation_verified")
        self.assertEqual(stored["side"], "sell")
        self.assertEqual(stored["input"]["symbol"], "WETH")
        self.assertEqual(stored["output"]["symbol"], "USDG")
        self.assertTrue(stored["initial_acceptance_wallet_reject_only"])
        self.assertFalse(stored["successful_broadcast_authorized"])
        call = self.planning.calls[0]
        self.assertEqual(call["side"], "sell")
        self.assertEqual(call["quantity"], "0.0001")
        self.assertIsNone(call["total_quote"])
        self.assertEqual(call["base_token"]["symbol"], "WETH")
        self.assertEqual(call["quote_token"]["symbol"], "USDG")

    async def test_weth_sell_current_amount_is_not_locked_to_historical_probe(self):
        result = await self.prepare_weth_sell("0.000100000000000001")
        self.assertEqual(result["execution"]["exact_input_amount"], "0.000100000000000001")
        self.assertEqual(len(self.planning.calls), 1)

    async def test_weth_sell_preparation_direction_can_claim_finite_approval(self):
        prepared = await self.prepare_weth_sell()
        execution_id = prepared["execution"]["id"]
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            claimed = self.service.claim_approval_send(
                self.db,
                execution_id=execution_id,
                wallet_address=TAKER,
                plan_hash=prepared["execution"]["approval"]["plan_hash"],
                claim_id=APPROVAL_CLAIM,
                confirm_send_claim=True,
            )
        self.assertTrue(claimed["ok"])
        row = self.db.get(RobinhoodChainSwapExecution, execution_id)
        self.assertEqual(row.status, "approval_send_claimed")
        self.assertIsNone(row.approval_tx_hash)
        self.assertIsNone(row.swap_tx_hash)
        self.assertEqual(
            ((row.route or {}).get("execution_lifecycle") or {}).get("approval", {}).get("send_claim_id"),
            APPROVAL_CLAIM,
        )

    async def test_historical_live_sell_evidence_preserves_finite_approval_and_separate_swap_stage(self):
        result = await self.prepare_weth_sell(live_authorized=True)
        row = result["execution"]
        self.assertEqual(row["tranche"], "RH-EXEC.AMT.1")
        self.assertEqual(row["side"], "sell")
        self.assertEqual(row["from_asset"], "WETH")
        self.assertEqual(row["to_asset"], "USDG")
        self.assertEqual(row["exact_input_amount"], "0.0001")
        self.assertEqual(row["exact_input_amount_atomic"], "100000000000000")
        self.assertFalse(row["approval_only"])
        self.assertTrue(row["swap_stage_enabled"])
        self.assertIsNone(row["swap_stage_locked_reason"])
        self.assertEqual(row["approval"]["amount_atomic"], "100000000000000")
        self.assertEqual(row["approval"]["transaction_to"].lower(), WETH["contract_address"].lower())
        self.assertEqual(row["approval"]["transaction_value_wei"], "0")
        self.assertTrue(row["approval"]["finite_approval"])
        self.assertFalse(row["approval"]["unlimited_approval"])
        authority = row["swap"]["route"]["execution_authority"]
        self.assertEqual(authority["authority_level"], "live_authorized_pending_confirmation")
        self.assertEqual(authority["side"], "sell")
        self.assertTrue(authority["successful_broadcast_authorized"])
        self.assertEqual(authority["live_authorization"]["authorization_id"], "ff" * 16)
        self.assertFalse(row["automatic_second_transaction"])

    async def test_r5c5b_buy_authority_cannot_satisfy_sell_claim(self):
        prepared = await self.prepare_weth_sell(live_authorized=True)
        row = self.db.get(RobinhoodChainSwapExecution, prepared["execution"]["id"])
        route = dict(row.route or {})
        route["execution_authority"] = weth_live_authorized_capability()["execution_authority"]
        row.route = route
        self.db.add(row)
        self.db.commit()
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            with self.assertRaisesRegex(ValueError, "robinhood_chain_swap_execution_input_authority_mismatch"):
                self.service.claim_approval_send(
                    self.db,
                    execution_id=row.id,
                    wallet_address=TAKER,
                    plan_hash=prepared["execution"]["approval"]["plan_hash"],
                    claim_id=APPROVAL_CLAIM,
                    confirm_send_claim=True,
                )

    async def test_r5c5b_confirmed_approval_builds_fresh_sell_plan_with_same_authority(self):
        prepared = await self.prepare_weth_sell(live_authorized=True)
        execution_id = prepared["execution"]["id"]
        self.rpc.approval_token_address = WETH["contract_address"]
        self.rpc.approval_amount_atomic = 100_000_000_000_000
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            claimed = self.service.claim_approval_send(
                self.db,
                execution_id=execution_id,
                wallet_address=TAKER,
                plan_hash=prepared["execution"]["approval"]["plan_hash"],
                claim_id=APPROVAL_CLAIM,
                confirm_send_claim=True,
            )
        self.assertEqual(claimed["execution"]["status"], "approval_send_claimed")
        self.service.record_approval_submission(
            self.db,
            execution_id=execution_id,
            tx_hash=APPROVAL_TX_HASH,
            wallet_address=TAKER,
            claim_id=APPROVAL_CLAIM,
            confirm_record=True,
        )
        self.rpc.allowance_atomic = 100_000_000_000_000
        self.planning.allowance_atomic = 100_000_000_000_000
        self.rpc.approval_receipt = receipt(APPROVAL_TX_HASH, 1)
        approved = await self.service.refresh_approval(self.db, execution_id=execution_id)
        self.assertEqual(approved["execution"]["status"], "approval_confirmed")
        self.assertEqual(approved["execution"]["allowance"]["required_atomic"], "100000000000000")
        self.assertEqual(approved["execution"]["allowance"]["shortfall_atomic"], "0")
        self.assertTrue(approved["execution"]["swap_stage_enabled"])

        capability = weth_sell_live_authorized_capability()
        swap = await self.service.prepare_swap(
            self.db,
            execution_id=execution_id,
            wallet_address=TAKER,
            eth_token=None,
            usdg_token=WETH,
            output_token=USDG,
            route_capability=capability,
            confirm_prepare=True,
        )
        plan = swap["unsigned_transaction_plan"]
        self.assertEqual(swap["execution"]["status"], "swap_prepared")
        self.assertEqual(plan["exact_input_amount"], "0.0001")
        self.assertEqual(plan["exact_input_asset"], "WETH")
        self.assertIsNone(plan["exact_input_usdg"])
        self.assertEqual(plan["output_asset"], "USDG")
        self.assertEqual(plan["value_wei"], "0")
        self.assertEqual(self.planning.calls[-1]["side"], "sell")
        self.assertEqual(self.planning.calls[-1]["quantity"], "0.0001")
        self.assertEqual(self.planning.calls[-1]["base_token"]["symbol"], "WETH")
        self.assertEqual(self.planning.calls[-1]["quote_token"]["symbol"], "USDG")
        stored_authority = swap["execution"]["swap"]["route"]["execution_authority"]
        self.assertEqual(stored_authority["live_authorization"]["authorization_id"], "ff" * 16)
        self.assertEqual(swap["execution"]["approval"]["tx_hash"], APPROVAL_TX_HASH)
        self.assertFalse(swap["execution"]["automatic_second_transaction"])

    async def test_r5c5b_swap_wallet_rejection_is_separate_hash_free_and_no_retry(self):
        prepared = await self.prepare_weth_sell(live_authorized=True)
        row = self.db.get(RobinhoodChainSwapExecution, prepared["execution"]["id"])
        row.status = "approval_confirmed"
        row.approval_status = "confirmed"
        row.allowance_current_atomic = row.exact_input_amount_atomic
        row.allowance_shortfall_atomic = "0"
        row.approval_required = False
        self.db.add(row)
        self.db.commit()
        self.rpc.allowance_atomic = 100_000_000_000_000
        self.planning.allowance_atomic = 100_000_000_000_000
        capability = weth_sell_live_authorized_capability()
        swap = await self.service.prepare_swap(
            self.db,
            execution_id=row.id,
            wallet_address=TAKER,
            eth_token=None,
            usdg_token=WETH,
            output_token=USDG,
            route_capability=capability,
            confirm_prepare=True,
        )
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            claimed = await self.service.claim_swap_send(
                self.db,
                execution_id=row.id,
                wallet_address=TAKER,
                plan_hash=swap["execution"]["swap"]["plan_hash"],
                claim_id=SWAP_CLAIM,
                confirm_send_claim=True,
                execution_authority=capability["execution_authority"],
            )
        self.assertEqual(claimed["execution"]["status"], "swap_send_claimed")
        failed = self.service.record_submission_failure(
            self.db,
            execution_id=row.id,
            stage="swap",
            wallet_address=TAKER,
            claim_id=SWAP_CLAIM,
            reason="wallet_rejected",
            message="declined",
            confirm_failure=True,
        )
        self.assertEqual(failed["execution"]["status"], "swap_wallet_rejected")
        self.assertIsNone(failed["execution"]["swap"]["tx_hash"])
        self.assertFalse(failed["execution"]["automatic_second_transaction"])
        self.assertEqual(failed["execution"]["swap"]["submission_attempts"], 0)

    async def test_r5c5b_confirmed_sell_reconciles_weth_input_usdg_output_and_both_fees(self):
        prepared = await self.prepare_weth_sell(live_authorized=True)
        execution_id = prepared["execution"]["id"]
        row = self.db.get(RobinhoodChainSwapExecution, execution_id)
        row.status = "approval_confirmed"
        row.approval_status = "confirmed"
        row.allowance_current_atomic = row.exact_input_amount_atomic
        row.allowance_shortfall_atomic = "0"
        row.approval_required = False
        row.approval_tx_hash = APPROVAL_TX_HASH
        route = dict(row.route or {})
        lifecycle = dict(route.get("execution_lifecycle") or {})
        lifecycle["approval"] = {
            "tx_hash": APPROVAL_TX_HASH,
            "receipt_status": 1,
            "gas_used": "50000",
            "effective_gas_price_wei": "80000000",
        }
        route["execution_lifecycle"] = lifecycle
        row.route = route
        self.db.add(row)
        self.db.commit()
        self.rpc.allowance_atomic = 100_000_000_000_000
        self.planning.allowance_atomic = 100_000_000_000_000
        capability = weth_sell_live_authorized_capability()
        swap = await self.service.prepare_swap(
            self.db,
            execution_id=execution_id,
            wallet_address=TAKER,
            eth_token=None,
            usdg_token=WETH,
            output_token=USDG,
            route_capability=capability,
            confirm_prepare=True,
        )
        with self.live_gate()[0], self.live_gate()[1], self.live_gate()[2], self.live_gate()[3]:
            await self.service.claim_swap_send(
                self.db,
                execution_id=execution_id,
                wallet_address=TAKER,
                plan_hash=swap["execution"]["swap"]["plan_hash"],
                claim_id=SWAP_CLAIM,
                confirm_send_claim=True,
                execution_authority=capability["execution_authority"],
            )
        self.service.record_swap_submission(
            self.db,
            execution_id=execution_id,
            tx_hash=SWAP_TX_HASH,
            wallet_address=TAKER,
            claim_id=SWAP_CLAIM,
            confirm_record=True,
        )
        input_atomic = 100_000_000_000_000
        output_atomic = 250_000
        swap_fee_wei = 245_000 * 80_000_000
        approval_fee_wei = 50_000 * 80_000_000
        before_tag = hex(123455)
        after_tag = hex(123456)
        self.rpc.native_balances_by_tag[before_tag] = self.rpc.native_balance_wei
        self.rpc.native_balances_by_tag[after_tag] = self.rpc.native_balance_wei - swap_fee_wei
        self.rpc.weth_balances_by_tag[before_tag] = 200_000_000_000_000
        self.rpc.weth_balances_by_tag[after_tag] = 100_000_000_000_000
        self.rpc.usdg_balances_by_tag[before_tag] = 1_000_000
        self.rpc.usdg_balances_by_tag[after_tag] = 1_250_000
        self.rpc.swap_receipt = receipt(
            SWAP_TX_HASH,
            1,
            logs=[weth_spend_log(input_atomic), usdg_receive_log(output_atomic)],
        )
        result = await self.service.refresh_swap(self.db, execution_id=execution_id)
        reconciled = result["execution"]
        self.assertEqual(reconciled["status"], "confirmed")
        self.assertEqual(reconciled["tranche"], "RH-EXEC.AMT.1")
        self.assertEqual(reconciled["actual_input_asset"], "WETH")
        self.assertEqual(reconciled["actual_input_amount_atomic"], str(input_atomic))
        self.assertEqual(reconciled["actual_input_amount"], "0.0001")
        self.assertEqual(reconciled["actual_output_asset"], "USDG")
        self.assertEqual(reconciled["actual_output_amount_atomic"], str(output_atomic))
        self.assertEqual(reconciled["actual_output_amount"], "0.25")
        self.assertEqual(reconciled["actual_average_fill_price"], "2500")
        self.assertEqual(reconciled["actual_network_fee_wei"], str(swap_fee_wei))
        reconciliation = reconciled["reconciliation"]
        self.assertEqual(reconciliation["approval_network_fee_wei"], str(approval_fee_wei))
        self.assertEqual(reconciliation["total_network_fee_wei"], str(swap_fee_wei + approval_fee_wei))
        self.assertEqual(reconciliation["input_transfer_log_count"], 1)
        self.assertEqual(reconciliation["usdg_transfer_log_count"], 1)
        self.assertEqual(reconciliation["output_transfer_log_count"], 1)
        self.assertEqual(reconciliation["approval_tx_hash"], APPROVAL_TX_HASH)
        self.assertEqual(reconciliation["swap_tx_hash"], SWAP_TX_HASH)
        self.assertFalse(reconciled["automatic_second_transaction"])

    async def test_r5c5b_non_archive_rpc_reconciles_sell_from_receipt_logs(self):
        execution_id = await self.submitted_r5c5b_sell()
        input_atomic = 100_000_000_000_000
        output_atomic = 250_000
        self.rpc.historical_state_error = {
            "code": -32000,
            "message": "metadata is not found, 123456",
        }
        self.rpc.swap_receipt = receipt(
            SWAP_TX_HASH,
            1,
            logs=[weth_spend_log(input_atomic), usdg_receive_log(output_atomic)],
        )

        result = await self.service.refresh_swap(self.db, execution_id=execution_id)

        reconciled = result["execution"]
        self.assertEqual(reconciled["status"], "confirmed")
        self.assertEqual(reconciled["actual_input_amount_atomic"], str(input_atomic))
        self.assertEqual(reconciled["actual_output_amount_atomic"], str(output_atomic))
        self.assertEqual(reconciled["actual_output_amount"], "0.25")
        reconciliation = reconciled["reconciliation"]
        self.assertEqual(
            reconciliation["reconciliation_mode"],
            "receipt_logs_historical_state_unavailable",
        )
        self.assertFalse(reconciliation["historical_balance_snapshot_available"])
        self.assertEqual(
            {item["read"] for item in reconciliation["historical_state_unavailable_reads"]},
            {
                "eth_before", "eth_after", "input_before",
                "input_after", "output_before", "output_after",
            },
        )
        lifecycle = reconciled["swap"]["route"]["execution_lifecycle"]["swap"]
        self.assertNotIn("receipt_block_balance_snapshot", lifecycle)
        self.assertFalse(
            lifecycle["receipt_log_reconciliation"]
            ["historical_balance_snapshot_available"]
        )
        self.assertEqual(reconciled["approval"]["submission_attempts"], 1)
        self.assertEqual(reconciled["swap"]["submission_attempts"], 1)
        self.assertFalse(reconciled["automatic_second_transaction"])

    async def test_r5c5b_non_archive_fallback_rejects_unrecognized_rpc_error(self):
        execution_id = await self.submitted_r5c5b_sell()
        self.rpc.historical_state_error = {
            "code": -32000,
            "message": "temporary upstream error",
        }
        self.rpc.swap_receipt = receipt(
            SWAP_TX_HASH,
            1,
            logs=[
                weth_spend_log(100_000_000_000_000),
                usdg_receive_log(250_000),
            ],
        )

        with self.assertRaisesRegex(
            ValueError,
            "robinhood_chain_swap_receipt_block_balance_snapshot_failed",
        ):
            await self.service.refresh_swap(self.db, execution_id=execution_id)

        row = self.db.get(RobinhoodChainSwapExecution, execution_id)
        self.assertEqual(row.status, "swap_pending")
        self.assertNotIn("execution_reconciliation", dict(row.route or {}))

    async def test_r5c5b_non_archive_fallback_still_enforces_minimum_output(self):
        execution_id = await self.submitted_r5c5b_sell()
        self.rpc.historical_state_error = {
            "code": -32000,
            "message": "metadata is not found, 123456",
        }
        self.rpc.swap_receipt = receipt(
            SWAP_TX_HASH,
            1,
            logs=[
                weth_spend_log(100_000_000_000_000),
                usdg_receive_log(240_000),
            ],
        )

        with self.assertRaisesRegex(
            ValueError,
            "robinhood_chain_swap_erc20_output_missing_or_below_minimum",
        ):
            await self.service.refresh_swap(self.db, execution_id=execution_id)

        row = self.db.get(RobinhoodChainSwapExecution, execution_id)
        self.assertEqual(row.status, "swap_pending")
        self.assertNotIn("execution_reconciliation", dict(row.route or {}))

    def test_router_swap_prepare_uses_execution_authority_and_token_registry_identity(self):
        source = (BACKEND_ROOT / "app" / "routers" / "robinhood_chain.py").read_text(encoding="utf-8")
        self.assertIn("_resolve_robinhood_chain_execution_authority_or_http", source)
        self.assertIn("/execution-authority/verify-preparation", source)
        self.assertIn("/execution-authority/authorize-controlled-buy", source)
        self.assertIn("/execution-authority/authorize-controlled-sell", source)
        self.assertIn("require_successful_broadcast=False", source)
        self.assertIn("symbol=request.symbol", source)
        self.assertIn('if str(authority.get("execution_adapter") or "") != "erc20_exact_input"', source)
        self.assertIn('input_token = dict(authority.get("input") or {})', source)
        self.assertIn('output_token = dict(authority.get("output") or {})', source)
        self.assertIn("robinhood_chain_swap_market_identity_mismatch", source)
        self.assertIn("side=str(authority.get(\"side\") or request.side)", source)
        self.assertIn("symbol=str(authority.get(\"symbol\") or request.symbol)", source)
        self.assertIn("from_asset=from_asset", source)
        self.assertIn("from_token=input_token", source)
        self.assertIn("to_asset=to_asset", source)
        self.assertIn("to_token=output_token", source)
        self.assertIn("route_capability=capability", source)
        self.assertIn("output_token=output_token", source)
        self.assertIn("additional_erc20_assets=[output_asset]", source)

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

    async def test_blocked_execution_authority_fails_before_provider(self):
        blocked_capability = {
            "from_asset": "USDG",
            "to_asset": "ETH",
            "amount_mode": "exact_input",
            "mechanism": "swap",
            "execution_authority": {
                "symbol": "ETH-USDG",
                "side": "buy",
                "amount_mode": "exact_input",
                "provider": "0x",
                "execution_adapter": "erc20_exact_input",
                "execution_permitted": False,
                "blocking_reasons": ["execution_not_live_verified"],
                "input": USDG,
                "output": ETH,
                "capability": {
                    "id": "blocked-capability",
                    "enabled": False,
                    "firm_plan_status": "not_tested",
                    "execution_status": "disabled",
                },
                "execution_ceiling": {"amount": "2", "asset": "USDG"},
            },
        }
        with self.assertRaisesRegex(ValueError, "robinhood_chain_swap_execution_authority_blocked"):
            await self.service.prepare(
                self.db,
                taker_address=TAKER,
                exact_input_amount="2",
                slippage_bps=100,
                eth_token=ETH,
                usdg_token=USDG,
                to_asset="ETH",
                to_token=ETH,
                route_capability=blocked_capability,
                confirm_prepare=True,
            )
        self.assertEqual(self.planning.calls, [])

    def test_all_orders_excludes_approval_only_and_maps_swap_rows(self):
        source = (BACKEND_ROOT / "app" / "services" / "all_orders.py").read_text(encoding="utf-8")
        normalizer_source = inspect.getsource(_to_unified_robinhood_chain_swap_execution)
        self.assertIn("RobinhoodChainSwapExecution.swap_tx_hash.is_not(None)", source)
        self.assertIn('"source": "RHCHAIN"', normalizer_source)
        self.assertIn('"venue": "robinhood_chain"', normalizer_source)
        self.assertIn('"side": side', normalizer_source)
        self.assertNotIn('"side": "buy"', normalizer_source)
        self.assertIn('"type": "swap"', normalizer_source)
        self.assertIn('"expected_input_asset": str(row.from_asset', normalizer_source)
        self.assertIn('"expected_output_asset": str(row.to_asset', normalizer_source)
        self.assertIn('"actual_output_asset": reconciliation.get("output_asset")', normalizer_source)

    def test_all_orders_normalizes_confirmed_r5c5b_sell_economics(self):
        row = SimpleNamespace(
            id="d8527da5-c356-49cc-a8a7-eee0b91ad02f",
            status="confirmed",
            side="sell",
            symbol="WETH-USDG",
            from_asset="WETH",
            to_asset="USDG",
            exact_input_amount="0.0001",
            expected_output_amount="0.186912",
            minimum_output_amount="0.18504",
            swap_tx_hash="0x" + "b2" * 32,
            approval_tx_hash="0x" + "a9" * 32,
            swap_plan_hash="sell-plan",
            quote_id="sell-quote",
            error_message=None,
            created_at=datetime(2026, 7, 31, 17, 56, 46),
            updated_at=datetime(2026, 7, 31, 21, 9, 41),
            route={
                "execution_lifecycle": {
                    "swap": {
                        "submitted_at": "2026-07-31T17:57:09.354550+00:00",
                        "confirmed_at": "2026-07-31T21:09:41.328675+00:00",
                    }
                },
                "execution_reconciliation": {
                    "reconciled": True,
                    "input_asset": "WETH",
                    "input_amount": "0.0001",
                    "output_asset": "USDG",
                    "output_amount": "0.186905",
                    "average_fill_price": "1869.05",
                    "swap_network_fee": "0.000004430041632",
                    "approval_network_fee": "0.00000107162069",
                    "total_network_fee": "0.000005501662322",
                },
            },
        )

        unified = _to_unified_robinhood_chain_swap_execution(row)

        self.assertEqual(unified["side"], "sell")
        self.assertFalse(unified["cross_asset_buy"])
        self.assertAlmostEqual(unified["qty"], 0.0001)
        self.assertAlmostEqual(unified["filled_qty"], 0.0001)
        self.assertAlmostEqual(unified["avg_fill_price"], 1869.05)
        self.assertAlmostEqual(unified["limit_price"], 1850.4)
        self.assertAlmostEqual(
            unified["qty"] * unified["avg_fill_price"],
            0.186905,
        )
        self.assertAlmostEqual(unified["total_after_fee"], 0.186905)
        self.assertAlmostEqual(unified["fee"], 0.000004430041632)
        self.assertEqual(unified["fee_asset"], "ETH")

    def test_all_orders_preserves_confirmed_r5c5a_buy_economics(self):
        row = SimpleNamespace(
            id="a8f0b018-6423-4956-813a-7927ba44240b",
            status="confirmed",
            side="buy",
            symbol="WETH-USDG",
            from_asset="USDG",
            to_asset="WETH",
            exact_input_amount="1",
            expected_output_amount="0.000526600573698485",
            minimum_output_amount="0.0005214000635712",
            swap_tx_hash="0x" + "0d" * 32,
            approval_tx_hash="0x" + "bb" * 32,
            swap_plan_hash="buy-plan",
            quote_id="buy-quote",
            error_message=None,
            created_at=datetime(2026, 7, 29, 17, 20, 12),
            updated_at=datetime(2026, 7, 29, 17, 20, 18),
            route={
                "execution_lifecycle": {
                    "swap": {
                        "submitted_at": "2026-07-29T17:20:12.335194+00:00",
                        "confirmed_at": "2026-07-29T17:20:18.072882+00:00",
                    }
                },
                "execution_reconciliation": {
                    "reconciled": True,
                    "input_asset": "USDG",
                    "input_amount": "1",
                    "output_asset": "WETH",
                    "output_amount": "0.000526600573698485",
                    "average_fill_price": "1898.972484926628",
                    "swap_network_fee": "0.000003992236556",
                    "approval_network_fee": "0.000001",
                    "total_network_fee": "0.000004992236556",
                },
            },
        )

        unified = _to_unified_robinhood_chain_swap_execution(row)

        self.assertEqual(unified["side"], "buy")
        self.assertTrue(unified["cross_asset_buy"])
        self.assertAlmostEqual(unified["qty"], 0.000526600573698485)
        self.assertAlmostEqual(unified["filled_qty"], 0.000526600573698485)
        self.assertAlmostEqual(unified["avg_fill_price"], 1898.972484926628)
        self.assertAlmostEqual(unified["limit_price"], 1917.9130764786423)
        self.assertAlmostEqual(
            unified["qty"] * unified["avg_fill_price"],
            1.0,
        )
        self.assertAlmostEqual(
            unified["total_after_fee"],
            0.000526600573698485,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
