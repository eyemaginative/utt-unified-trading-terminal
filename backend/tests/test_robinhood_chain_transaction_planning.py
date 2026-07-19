from __future__ import annotations

import json
import unittest
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
TAKER = "0x70c1ddd03bc4cb74efac3f12a41465d028ae490c"
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
    sell_amount = params["sellAmount"]
    native_sell = sell_token.lower() == ETH["contract_address"].lower()
    if native_sell:
        buy_amount = "183402"
        min_buy_amount = "181567"
        fee_token = USDG["contract_address"]
        allowance_issue = None
        transaction_value = sell_amount if value is None else value
    else:
        buy_amount = "545250000000000"
        min_buy_amount = "539797500000000"
        fee_token = ETH["contract_address"]
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
        "totalNetworkFee": "24000000000000",
        "transaction": {
            "to": destination,
            "data": "0x1234abcdef",
            "gas": "300000",
            "gasPrice": "80000000",
            "value": transaction_value,
        },
    }



class RobinhoodChainTransactionPlanningTests(unittest.IsolatedAsyncioTestCase):
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

    async def test_native_eth_sell_plan_uses_value_and_skips_allowance(self) -> None:
        service, rpc = self.make_service(allowance_atomic=0)
        result = await service.firm_quote_plan(
            symbol="ETH-USDG",
            side="sell",
            quantity="0.0001",
            total_quote=None,
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
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
        self.assertEqual(plan["value_wei"], "100000000000000")
        self.assertEqual(plan["value_eth"], "0.0001")
        self.assertTrue(plan["native_input"])
        self.assertEqual(plan["calldata"], "0x1234abcdef")
        self.assertTrue(plan["destination_allowlisted"])
        self.assertFalse(result["allowance"]["applicable"])
        self.assertEqual(result["allowance"]["read_method"], "not_applicable_native_input")
        self.assertFalse(result["allowance"]["approval_required"])
        self.assertEqual(result["allowance"]["shortfall_atomic"], "0")
        self.assertEqual(rpc.allowance_calls, [])

    async def test_buy_plan_uses_usdg_allowance_and_can_be_ready(self) -> None:
        service, rpc = self.make_service(allowance_atomic=10_000_000)
        result = await service.firm_quote_plan(
            symbol="ETH-USDG",
            side="buy",
            quantity=None,
            total_quote="1",
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
            slippage_bps=100,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["input_asset"], "USDG")
        self.assertEqual(result["maximum_spent"], "1")
        self.assertFalse(result["approval_required"])
        self.assertEqual(result["unsigned_transaction_plan"]["status"], "ready_for_wallet_review")
        self.assertEqual(rpc.allowance_calls[0]["contract"].lower(), USDG["contract_address"].lower())

    async def test_unsupported_symbol_fails_before_provider(self) -> None:
        service, rpc = self.make_service()
        result = await service.firm_quote_plan(
            symbol="WETH-USDG",
            side="sell",
            quantity="0.0001",
            total_quote=None,
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
            slippage_bps=100,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "unsupported_robinhood_chain_quote_symbol")
        self.assertEqual(rpc.rpc_calls, [])

    async def test_amount_caps_fail_closed(self) -> None:
        service, _ = self.make_service()
        result = await service.firm_quote_plan(
            symbol="ETH-USDG",
            side="sell",
            quantity="0.00200001",
            total_quote=None,
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
            slippage_bps=100,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "firm_quote_amount_exceeds_cap")

    async def test_slippage_bounds_fail_closed(self) -> None:
        service, _ = self.make_service()
        result = await service.firm_quote_plan(
            symbol="ETH-USDG",
            side="sell",
            quantity="0.0001",
            total_quote=None,
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
            slippage_bps=301,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "invalid_slippage_bps")

    async def test_destination_mismatch_fails_without_calldata_leak(self) -> None:
        def mutate(body):
            body["transaction"]["to"] = "0x1111111111111111111111111111111111111111"

        service, _ = self.make_service(body_mutator=mutate)
        result = await service.firm_quote_plan(
            symbol="ETH-USDG",
            side="sell",
            quantity="0.0001",
            total_quote=None,
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
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
        result = await service.firm_quote_plan(
            symbol="ETH-USDG",
            side="buy",
            quantity=None,
            total_quote="1",
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
            slippage_bps=100,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "firm_quote_transaction_value_mismatch")
        self.assertNotIn("unsigned_transaction_plan", result)

    async def test_native_eth_value_mismatch_fails_closed(self) -> None:
        def mutate(body):
            body["transaction"]["value"] = "0"

        service, _ = self.make_service(body_mutator=mutate)
        result = await service.firm_quote_plan(
            symbol="ETH-USDG",
            side="sell",
            quantity="0.0001",
            total_quote=None,
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
            slippage_bps=100,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "firm_quote_transaction_value_mismatch")
        self.assertEqual(result["expected_transaction_value_wei"], "100000000000000")
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
