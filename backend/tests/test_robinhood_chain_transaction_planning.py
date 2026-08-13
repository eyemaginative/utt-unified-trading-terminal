from __future__ import annotations

import ast
import inspect
import json
import unittest
from decimal import Decimal
from pathlib import Path
import sys

import httpx


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services.evm_rpc import encode_erc20_allowance  # noqa: E402
from app.services.robinhood_chain_transaction_planning import (  # noqa: E402
    ROBINHOOD_CHAIN_ALLOWANCE_HOLDER_ALLOWLIST,
    RobinhoodChainTransactionPlanningService,
)
from app.services.robinhood_chain_uniswap_quote import (  # noqa: E402
    RobinhoodChainUniswapQuoteService,
)



def _synthetic_address(seed: int) -> str:
    return "0x" + int(seed).to_bytes(20, "big").hex()


GASX = {
    "symbol": "GASX",
    "contract_address": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    "registry_contract_address": None,
    "decimals": 9,
    "native": True,
    "asset_kind": "native",
    "identity_source": "token_registry",
    "registry_status": "registered",
    "registry_id": 101,
}
CRED = {
    "symbol": "CRED",
    "contract_address": _synthetic_address(202),
    "registry_contract_address": _synthetic_address(202),
    "decimals": 5,
    "native": False,
    "asset_kind": "erc20",
    "identity_source": "token_registry",
    "registry_status": "registered",
    "registry_id": 202,
}
WGAS = {
    "symbol": "WGAS",
    "contract_address": _synthetic_address(303),
    "registry_contract_address": _synthetic_address(303),
    "decimals": 7,
    "native": False,
    "asset_kind": "erc20",
    "identity_source": "token_registry",
    "registry_status": "registered",
    "registry_id": 303,
}
TAKER = _synthetic_address(404)
WGAS_TO_CRED = {
    "symbol": "WGAS-CRED",
    "mechanism": "swap",
    "from_asset": "WGAS",
    "to_asset": "CRED",
    "amount_mode": "exact_input",
    "display_mode": "exact_spend",
    "indicative_status": "available",
    "firm_plan_status": "available",
    "execution_status": "disabled",
    "enabled": False,
    "probe_amount": "0.01",
    "firm_plan_max_input_amount": "0.01",
}
CRED_TO_WGAS = {
    "symbol": "WGAS-CRED",
    "mechanism": "swap",
    "from_asset": "CRED",
    "to_asset": "WGAS",
    "amount_mode": "exact_input",
    "display_mode": "exact_spend",
    "indicative_status": "available",
    "firm_plan_status": "available",
    "execution_status": "disabled",
    "enabled": False,
    "probe_amount": "5",
    "firm_plan_max_input_amount": "5",
}
GASX_TO_CRED = {
    **WGAS_TO_CRED,
    "symbol": "GASX-CRED",
    "from_asset": "GASX",
    "probe_amount": "0.002",
    "firm_plan_max_input_amount": "0.01",
}
CRED_TO_GASX = {**CRED_TO_WGAS, "symbol": "GASX-CRED", "to_asset": "GASX"}

ALLOWANCE_HOLDER = next(iter(ROBINHOOD_CHAIN_ALLOWANCE_HOLDER_ALLOWLIST))


class FakeRpcClient:
    def __init__(self, allowance_atomic: int = 0) -> None:
        self.allowance_atomic = int(allowance_atomic)
        self.allowance_calls: list[dict] = []
        self.rpc_calls: list[dict] = []

    async def verify_expected_chain(self, *, force_refresh: bool = False) -> dict:
        return {
            "ok": True,
            "expected_chain_id": 4663,
            "expected_chain_id_hex": "0x1237",
            "actual_chain_id": "0x1237",
            "chain_id_matches": True,
        }

    async def rpc_read(self, method, params, *, cache_namespace=None, force_refresh=False) -> dict:
        self.rpc_calls.append({"method": method, "params": params, "force_refresh": force_refresh})
        if method == "eth_getCode":
            return {"ok": True, "result": "0x6001600055", "cached": False}
        raise AssertionError(f"unexpected rpc method {method}")

    async def get_erc20_allowance(
        self,
        owner_address,
        contract_address,
        spender_address,
        decimals,
        *,
        force_refresh=True,
    ) -> dict:
        self.allowance_calls.append(
            {
                "owner": owner_address,
                "contract": contract_address,
                "spender": spender_address,
                "decimals": decimals,
                "force_refresh": force_refresh,
            }
        )
        return {
            "ok": True,
            "owner_address": owner_address,
            "contract_address": contract_address,
            "spender_address": spender_address,
            "decimals": decimals,
            "allowance_atomic": str(self.allowance_atomic),
            "allowance_token": str(self.allowance_atomic),
            "cached": False,
            "fetched_at": "2026-07-18T12:00:00+00:00",
            "read_only": True,
        }


def firm_body(request: httpx.Request, *, destination: str = ALLOWANCE_HOLDER, value: str | None = None) -> dict:
    params = request.url.params
    sell_token = params["sellToken"]
    buy_token = params["buyToken"]
    exact_output = "buyAmount" in params
    requested_buy_amount = params.get("buyAmount")
    sell_amount = params.get("sellAmount")
    native_sell = sell_token.lower() == GASX["contract_address"].lower()
    if native_sell:
        buy_amount = str(max(1, int(sell_amount or "0") * 2))
        min_buy_amount = str(max(1, int(Decimal(buy_amount) * Decimal("0.99"))))
        fee_token = CRED["contract_address"]
        allowance_issue = None
        transaction_value = sell_amount if value is None else value
    elif exact_output:
        sell_amount = "150000"
        buy_amount = str(requested_buy_amount)
        min_buy_amount = str(requested_buy_amount)
        fee_token = buy_token
        allowance_issue = {"actual": "0", "spender": ALLOWANCE_HOLDER}
        transaction_value = "0" if value is None else value
    elif sell_token.lower() == CRED["contract_address"].lower():
        buy_amount = str(max(1, int(sell_amount or "0") * 3))
        min_buy_amount = str(max(1, int(Decimal(buy_amount) * Decimal("0.99"))))
        fee_token = buy_token
        allowance_issue = {"actual": "0", "spender": ALLOWANCE_HOLDER}
        transaction_value = "0" if value is None else value
    else:
        buy_amount = str(max(1, int(sell_amount or "0") * 2))
        min_buy_amount = str(max(1, int(Decimal(buy_amount) * Decimal("0.99"))))
        fee_token = buy_token
        allowance_issue = {"actual": "0", "spender": ALLOWANCE_HOLDER}
        transaction_value = "0" if value is None else value
    return {
        "allowanceTarget": ALLOWANCE_HOLDER,
        "blockNumber": "12345678",
        "buyAmount": buy_amount,
        "buyToken": buy_token,
        "fees": {
            "integratorFee": None,
            "zeroExFee": {"amount": "275", "token": fee_token, "type": "volume"},
            "gasFee": None,
        },
        "issues": {
            "allowance": allowance_issue,
            "balance": None,
            "simulationIncomplete": False,
            "invalidSourcesPassed": [],
        },
        "liquidityAvailable": True,
        "minBuyAmount": min_buy_amount,
        "route": {
            "fills": [
                {
                    "source": "Uniswap_V3",
                    "proportionBps": "10000",
                    "from": sell_token,
                    "to": buy_token,
                }
            ],
            "tokens": [sell_token, buy_token],
        },
        "sellAmount": sell_amount,
        "sellToken": sell_token,
        "totalNetworkFee": "24000000",
        "transaction": {
            "to": destination,
            "data": "0x1234abcdef",
            "gas": "300000",
            "gasPrice": "80000000",
            "value": transaction_value,
        },
    }



async def _plan_request(
    service: RobinhoodChainTransactionPlanningService,
    *,
    symbol: str,
    side: str,
    quantity: str | None = None,
    total_quote: str | None = None,
    exact_output_quantity: str | None = None,
    maximum_total_quote: str | None = None,
    taker_address: str,
    base_token: dict | None = None,
    quote_token: dict | None = None,
    route_capability: dict | None = None,
    slippage_bps: int = 100,
) -> dict:
    base = dict(base_token or GASX)
    quote = dict(quote_token or CRED)
    normalized_side = str(side).lower()
    amount_mode = "exact_output" if exact_output_quantity is not None else "exact_input"
    requested_amount = exact_output_quantity if amount_mode == "exact_output" else (
        quantity if normalized_side == "sell" else total_quote
    )
    if route_capability is None:
        if amount_mode == "exact_output":
            from_asset = quote["symbol"] if normalized_side == "buy" else base["symbol"]
            to_asset = base["symbol"] if normalized_side == "buy" else quote["symbol"]
            route_capability = {
                "symbol": f"{base['symbol']}-{quote['symbol']}",
                "mechanism": "swap",
                "from_asset": from_asset,
                "to_asset": to_asset,
                "amount_mode": "exact_output",
                "display_mode": "exact_receive",
                "indicative_status": "available",
                "firm_plan_status": "not_tested",
                "probe_amount": str(requested_amount or "1"),
            }
        else:
            route_capability = (
                {**GASX_TO_CRED, "symbol": f"{base['symbol']}-{quote['symbol']}", "from_asset": base["symbol"], "to_asset": quote["symbol"]}
                if normalized_side == "sell"
                else {**CRED_TO_GASX, "symbol": f"{base['symbol']}-{quote['symbol']}", "from_asset": quote["symbol"], "to_asset": base["symbol"]}
            )
    return await service.firm_quote_plan(
        symbol=symbol,
        side=side,
        amount_mode=amount_mode,
        requested_amount=str(requested_amount or ""),
        maximum_input_amount=maximum_total_quote,
        taker_address=taker_address,
        base_token=base,
        quote_token=quote,
        native_token=GASX,
        registry_tokens=[GASX, CRED, WGAS, base, quote],
        route_capability=route_capability,
        slippage_bps=slippage_bps,
    )


class RobinhoodChainTransactionPlanningTests(unittest.IsolatedAsyncioTestCase):
    def test_router_firm_plan_keywords_match_provider_service_signatures(self) -> None:
        router_path = BACKEND_ROOT / "app" / "routers" / "robinhood_chain.py"
        tree = ast.parse(router_path.read_text(encoding="utf-8"), filename=str(router_path))
        endpoint = next(
            node
            for node in tree.body
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name == "robinhood_chain_firm_quote_plan"
        )

        calls = [
            node
            for node in ast.walk(endpoint)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "firm_quote_plan"
        ]
        self.assertEqual(len(calls), 2)

        def receiver_getter_name(call: ast.Call) -> str:
            receiver = call.func.value
            self.assertIsInstance(receiver, ast.Call)
            self.assertIsInstance(receiver.func, ast.Name)
            return receiver.func.id

        by_receiver = {receiver_getter_name(call): call for call in calls}
        self.assertEqual(
            set(by_receiver),
            {
                "get_robinhood_chain_transaction_planning_service",
                "get_robinhood_chain_uniswap_quote_service",
            },
        )

        zerox_keywords = {
            keyword.arg
            for keyword in by_receiver[
                "get_robinhood_chain_transaction_planning_service"
            ].keywords
            if keyword.arg
        }
        zerox_service_keywords = set(
            inspect.signature(RobinhoodChainTransactionPlanningService.firm_quote_plan).parameters
        ) - {"self"}
        self.assertTrue(zerox_keywords <= zerox_service_keywords)
        self.assertNotIn("exact_output_quantity", zerox_keywords)
        self.assertNotIn("maximum_total_quote", zerox_keywords)
        for required in {
            "amount_mode",
            "requested_amount",
            "maximum_input_amount",
            "base_token",
            "quote_token",
            "route_capability",
        }:
            self.assertIn(required, zerox_keywords)

        uniswap_keywords = {
            keyword.arg
            for keyword in by_receiver[
                "get_robinhood_chain_uniswap_quote_service"
            ].keywords
            if keyword.arg
        }
        uniswap_service_keywords = set(
            inspect.signature(RobinhoodChainUniswapQuoteService.firm_quote_plan).parameters
        ) - {"self"}
        self.assertTrue(uniswap_keywords <= uniswap_service_keywords)
        for required in {
            "symbol",
            "side",
            "amount_mode",
            "requested_amount",
            "slippage_bps",
            "swapper_address",
            "input_token",
            "output_token",
        }:
            self.assertIn(required, uniswap_keywords)
        self.assertNotIn("maximum_input_amount", uniswap_keywords)
        self.assertNotIn("route_capability", uniswap_keywords)

    def make_service(self, *, allowance_atomic: int = 0, body_mutator=None):
        rpc = FakeRpcClient(allowance_atomic=allowance_atomic)

        def handler(request: httpx.Request) -> httpx.Response:
            body = firm_body(request)
            if body_mutator is not None:
                body_mutator(body)
            return httpx.Response(200, json=body, request=request)

        service = RobinhoodChainTransactionPlanningService(
            api_base="https://api.0x.org",
            timeout_s=10,
            max_concurrent=1,
            credential_getter=lambda: {"api_key": "secret-test-key", "source": "profile_vault", "venue": "zerox"},
            rpc_client=rpc,
            transport=httpx.MockTransport(handler),
        )
        return service, rpc

    async def test_allowance_encoder_uses_owner_and_spender(self) -> None:
        encoded = encode_erc20_allowance(TAKER, ALLOWANCE_HOLDER)
        self.assertTrue(encoded.startswith("0xdd62ed3e"))
        self.assertEqual(len(encoded), 2 + 8 + 64 + 64)
        self.assertIn(TAKER[2:].lower(), encoded)
        self.assertIn(ALLOWANCE_HOLDER[2:].lower(), encoded)

    async def test_native_registry_asset_sell_plan_uses_value_and_skips_allowance(self) -> None:
        service, rpc = self.make_service(allowance_atomic=0)
        result = await _plan_request(service,
            symbol="GASX-CRED",
            side="sell",
            quantity="0.0001",
            total_quote=None,
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            slippage_bps=100,
        )
        self.assertTrue(result["ok"])
        self.assertTrue(result["firm_quote"])
        self.assertTrue(result["unsigned_transaction_plan_present"])
        self.assertFalse(result["signing_enabled"])
        self.assertFalse(result["broadcast_enabled"])
        self.assertFalse(result["will_mutate"])
        self.assertNotIn("transaction_calldata", result)
        self.assertNotIn("transaction_destination", result)
        plan = result["unsigned_transaction_plan"]
        self.assertEqual(plan["chain_id"], 4663)
        self.assertEqual(plan["to"].lower(), ALLOWANCE_HOLDER.lower())
        self.assertEqual(plan["value_atomic"], "100000")
        self.assertEqual(plan["value"], "0.0001")
        self.assertEqual(plan["value_asset"], "GASX")
        self.assertTrue(plan["native_input"])
        self.assertEqual(plan["calldata"], "0x1234abcdef")
        self.assertTrue(plan["destination_allowlisted"])
        self.assertFalse(result["allowance"]["applicable"])
        self.assertEqual(result["allowance"]["read_method"], "not_applicable_native_input")
        self.assertFalse(result["allowance"]["approval_required"])
        self.assertEqual(result["allowance"]["shortfall_atomic"], "0")
        self.assertEqual(rpc.allowance_calls, [])

    async def test_buy_plan_uses_quote_asset_allowance_and_can_be_ready(self) -> None:
        service, rpc = self.make_service(allowance_atomic=10_000_000)
        result = await _plan_request(service,
            symbol="GASX-CRED",
            side="buy",
            quantity=None,
            total_quote="1",
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            slippage_bps=100,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["maximum_spent"], "1")
        self.assertFalse(result["approval_required"])
        self.assertEqual(result["unsigned_transaction_plan"]["status"], "ready_for_wallet_review")
        self.assertEqual(rpc.allowance_calls[0]["contract"].lower(), CRED["contract_address"].lower())

    async def test_exact_output_buy_plan_is_blocked_before_provider_contact(self) -> None:
        service, rpc = self.make_service(allowance_atomic=0)
        result = await _plan_request(service,
            symbol="GASX-CRED",
            side="buy",
            quantity=None,
            total_quote=None,
            exact_output_quantity="0.001",
            maximum_total_quote="2",
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            slippage_bps=100,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "firm_quote_route_capability_unavailable")
        self.assertEqual(result["amount_mode"], "exact_output")
        self.assertEqual(result["display_mode"], "exact_receive")
        self.assertEqual(result["output_asset"], "GASX")
        self.assertEqual(result["route_capability"]["firm_plan_status"], "not_tested")
        self.assertEqual(rpc.rpc_calls, [])
        self.assertEqual(rpc.allowance_calls, [])

    def test_status_delegates_route_capabilities_to_database_router(self) -> None:
        service, _ = self.make_service()
        status = service.status()
        self.assertTrue(status["exact_input_enabled"])
        self.assertFalse(status["exact_output_enabled"])
        self.assertTrue(status["provider_declared_exact_output_supported"])
        self.assertEqual(status["route_capabilities"], [])
        self.assertEqual(status["pair_capability_source"], "database_router")
        self.assertFalse(status["token_contracts_hardcoded"])

    async def test_erc20_buy_plan_uses_quote_asset_allowance_and_review_only_output(self) -> None:
        service, rpc = self.make_service(allowance_atomic=200_000)
        result = await _plan_request(service,
            symbol="WGAS-CRED",
            side="buy",
            quantity=None,
            total_quote="1.25",
            taker_address=TAKER,
            base_token=WGAS,
            quote_token=CRED,
            route_capability=CRED_TO_WGAS,
            slippage_bps=100,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["symbol"], "WGAS-CRED")
        self.assertEqual(result["input_amount"], "1.25")
        self.assertEqual(result["minimum_received_asset"], "WGAS")
        self.assertEqual(result["token_identity_source"], "token_registry")
        self.assertEqual(result["pair_capability_source"], "database")
        self.assertEqual(result["route_capability"]["from_asset"], "CRED")
        self.assertFalse(result["approval_required"])
        self.assertEqual(result["allowance"]["token"]["symbol"], "CRED")
        self.assertEqual(rpc.allowance_calls[0]["contract"].lower(), CRED["contract_address"].lower())
        self.assertEqual(result["unsigned_transaction_plan"]["value_wei"], "0")
        self.assertFalse(result["unsigned_transaction_plan"]["native_input"])
        self.assertTrue(result["review_only"])
        self.assertFalse(result["execution_enabled"])
        self.assertFalse(result["signing_enabled"])
        self.assertFalse(result["broadcast_enabled"])

    async def test_erc20_sell_plan_uses_input_allowance_and_zero_transaction_value(self) -> None:
        service, rpc = self.make_service(allowance_atomic=0)
        result = await _plan_request(service,
            symbol="WGAS-CRED",
            side="sell",
            quantity="0.0005",
            total_quote=None,
            taker_address=TAKER,
            base_token=WGAS,
            quote_token=CRED,
            route_capability=WGAS_TO_CRED,
            slippage_bps=100,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["input_asset"], "WGAS")
        self.assertEqual(result["output_asset"], "CRED")
        self.assertTrue(result["approval_required"])
        self.assertEqual(result["allowance"]["token"]["symbol"], "WGAS")
        self.assertEqual(rpc.allowance_calls[0]["contract"].lower(), WGAS["contract_address"].lower())
        self.assertEqual(result["unsigned_transaction_plan"]["value_wei"], "0")
        self.assertFalse(result["unsigned_transaction_plan"]["native_input"])


    async def test_r5c5b_weth_sell_uses_exact_atomic_allowance_and_usdg_output(self) -> None:
        weth = {
            "symbol": "WETH",
            "contract_address": _synthetic_address(505),
            "registry_contract_address": _synthetic_address(505),
            "decimals": 18,
            "native": False,
            "asset_kind": "erc20",
            "identity_source": "token_registry",
            "registry_status": "registered",
            "registry_id": 505,
        }
        usdg = {
            "symbol": "USDG",
            "contract_address": _synthetic_address(606),
            "registry_contract_address": _synthetic_address(606),
            "decimals": 6,
            "native": False,
            "asset_kind": "erc20",
            "identity_source": "token_registry",
            "registry_status": "registered",
            "registry_id": 606,
        }
        capability = {
            "symbol": "WETH-USDG",
            "mechanism": "swap",
            "from_asset": "WETH",
            "to_asset": "USDG",
            "amount_mode": "exact_input",
            "display_mode": "exact_spend",
            "indicative_status": "available",
            "firm_plan_status": "available",
            "execution_status": "preparation_verified",
            "enabled": True,
            "probe_amount": "0.0001",
            "firm_plan_input_ceiling": "0.0001",
            "evidence": {
                "preparation_verified": True,
                "firm_plan_input_ceiling": "0.0001",
            },
        }
        service, rpc = self.make_service(allowance_atomic=0)
        result = await _plan_request(
            service,
            symbol="WETH-USDG",
            side="sell",
            quantity="0.0001",
            total_quote=None,
            taker_address=TAKER,
            base_token=weth,
            quote_token=usdg,
            route_capability=capability,
            slippage_bps=100,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["side"], "sell")
        self.assertEqual(result["input_asset"], "WETH")
        self.assertEqual(result["input_amount"], "0.0001")
        self.assertEqual(result["input_amount_atomic"], "100000000000000")
        self.assertEqual(result["output_asset"], "USDG")
        self.assertEqual(result["allowance"]["token"]["symbol"], "WETH")
        self.assertEqual(result["allowance"]["required_atomic"], "100000000000000")
        self.assertEqual(result["allowance"]["shortfall_atomic"], "100000000000000")
        self.assertTrue(result["approval_required"])
        self.assertEqual(
            rpc.allowance_calls[0]["contract"].lower(),
            weth["contract_address"].lower(),
        )
        self.assertEqual(rpc.allowance_calls[0]["decimals"], 18)
        self.assertEqual(result["unsigned_transaction_plan"]["value_wei"], "0")
        self.assertFalse(result["unsigned_transaction_plan"]["native_input"])
        self.assertFalse(result["signing_enabled"])
        self.assertFalse(result["broadcast_enabled"])
        self.assertFalse(result["will_mutate"])

    async def test_erc20_blocked_capability_fails_before_rpc_or_provider(self) -> None:
        service, rpc = self.make_service(allowance_atomic=0)
        blocked = {**CRED_TO_WGAS, "firm_plan_status": "provider_error"}
        result = await _plan_request(service,
            symbol="WGAS-CRED",
            side="buy",
            quantity=None,
            total_quote="1",
            taker_address=TAKER,
            base_token=WGAS,
            quote_token=CRED,
            route_capability=blocked,
            slippage_bps=100,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "firm_quote_route_capability_unavailable")
        self.assertEqual(rpc.rpc_calls, [])
        self.assertEqual(rpc.allowance_calls, [])

    async def test_erc20_exact_output_custom_amounts_remain_blocked_before_provider(self) -> None:
        service, rpc = self.make_service(allowance_atomic=0)
        result = await _plan_request(service,
            symbol="WGAS-CRED",
            side="buy",
            quantity=None,
            total_quote=None,
            exact_output_quantity="0.0007",
            maximum_total_quote="1.5",
            taker_address=TAKER,
            base_token=WGAS,
            quote_token=CRED,
            route_capability=None,
            slippage_bps=100,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "firm_quote_route_capability_unavailable")
        self.assertEqual(rpc.rpc_calls, [])
        self.assertEqual(rpc.allowance_calls, [])

    async def test_erc20_identity_cannot_be_substituted_with_native_asset(self) -> None:
        service, rpc = self.make_service(allowance_atomic=0)
        result = await _plan_request(service,
            symbol="WGAS-CRED",
            side="buy",
            quantity=None,
            total_quote="1",
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            route_capability=CRED_TO_WGAS,
            slippage_bps=100,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "firm_quote_token_identity_mismatch")
        self.assertEqual(rpc.rpc_calls, [])
        self.assertEqual(rpc.allowance_calls, [])

    async def test_unsupported_symbol_fails_before_provider(self) -> None:
        service, rpc = self.make_service()
        result = await _plan_request(service,
            symbol="ALPHA-CRED",
            side="sell",
            quantity="0.0001",
            total_quote=None,
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            slippage_bps=100,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "firm_quote_token_identity_mismatch")
        self.assertEqual(rpc.rpc_calls, [])

    async def test_exact_input_can_exceed_historical_probe_evidence(self) -> None:
        service, _ = self.make_service()
        result = await _plan_request(
            service,
            symbol="GASX-CRED",
            side="sell",
            quantity="0.004",
            total_quote=None,
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            route_capability=GASX_TO_CRED,
            slippage_bps=100,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["probe_amount"], "0.002")
        self.assertEqual(result["firm_plan_input_ceiling"], "0.01")
        self.assertEqual(result["firm_plan_ceiling_source"], "database_direction_capability")
        self.assertFalse(result["firm_plan_ceiling_enforced"])

    async def test_exact_input_above_historical_firm_plan_evidence_is_allowed(self) -> None:
        service, _ = self.make_service()
        result = await _plan_request(service,
            symbol="GASX-CRED",
            side="sell",
            quantity="0.01000001",
            total_quote=None,
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            slippage_bps=100,
        )
        self.assertTrue(result["ok"])
        self.assertFalse(result["firm_plan_ceiling_enforced"])
        self.assertEqual(result["current_amount_policy"], "user_selected_exact_input")

    async def test_slippage_bounds_fail_closed(self) -> None:
        service, _ = self.make_service()
        result = await _plan_request(service,
            symbol="GASX-CRED",
            side="sell",
            quantity="0.0001",
            total_quote=None,
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            slippage_bps=301,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "invalid_slippage_bps")

    async def test_destination_mismatch_fails_without_calldata_leak(self) -> None:
        def mutate(body):
            body["transaction"]["to"] = _synthetic_address(505)

        service, _ = self.make_service(body_mutator=mutate)
        result = await _plan_request(service,
            symbol="GASX-CRED",
            side="sell",
            quantity="0.0001",
            total_quote=None,
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            slippage_bps=100,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "firm_quote_destination_not_allowlisted")
        self.assertNotIn("unsigned_transaction_plan", result)
        self.assertNotIn("calldata", json.dumps(result).lower())

    async def test_nonzero_transaction_value_fails_closed(self) -> None:
        def mutate(body):
            body["transaction"]["value"] = "1"

        service, _ = self.make_service(body_mutator=mutate)
        result = await _plan_request(service,
            symbol="GASX-CRED",
            side="buy",
            quantity=None,
            total_quote="1",
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            slippage_bps=100,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "firm_quote_transaction_value_mismatch")
        self.assertNotIn("unsigned_transaction_plan", result)

    async def test_native_registry_value_mismatch_fails_closed(self) -> None:
        def mutate(body):
            body["transaction"]["value"] = "0"

        service, _ = self.make_service(body_mutator=mutate)
        result = await _plan_request(service,
            symbol="GASX-CRED",
            side="sell",
            quantity="0.0001",
            total_quote=None,
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            slippage_bps=100,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "firm_quote_transaction_value_mismatch")
        self.assertEqual(result["expected_transaction_value_wei"], "100000")
        self.assertNotIn("unsigned_transaction_plan", result)

    async def test_status_is_secret_free_and_fail_closed(self) -> None:
        service, _ = self.make_service()
        status = service.status()
        serialized = json.dumps(status)
        self.assertTrue(status["provider_configured"])
        self.assertFalse(status["approval_transaction_enabled"])
        self.assertFalse(status["signing_enabled"])
        self.assertFalse(status["broadcast_enabled"])
        self.assertNotIn("secret-test-key", serialized)
        self.assertIn(ALLOWANCE_HOLDER, status["allowance_holder_allowlist"])


if __name__ == "__main__":
    unittest.main()
