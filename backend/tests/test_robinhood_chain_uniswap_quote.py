from __future__ import annotations

import json
import unittest
from pathlib import Path

import httpx

from app.services.robinhood_chain_uniswap_quote import (
    RobinhoodChainUniswapQuoteService,
    UNISWAP_AMM_PROTOCOLS,
    UNISWAP_CHAIN_ID,
    UNISWAP_ROUTER_VERSION,
)


def _address(seed: int) -> str:
    return "0x" + int(seed).to_bytes(20, "big").hex()


USDG = {
    "symbol": "USDG",
    "contract_address": _address(1),
    "decimals": 6,
    "native": False,
    "identity_source": "token_registry",
    "registry_status": "registered",
    "registry_id": 11,
}
SPCX = {
    "symbol": "SPCX",
    "contract_address": _address(2),
    "decimals": 18,
    "native": False,
    "identity_source": "token_registry",
    "registry_status": "registered",
    "registry_id": 12,
}
SWAPPER = _address(3)


def _safe_credential() -> dict:
    return {
        "api_key": "test-uniswap-key",
        "api_key_configured": True,
        "source": "profile_vault",
        "venue": "uniswap_api",
        "key_version": 1,
        "scope_read": True,
        "scope_trade": False,
        "scope_transfer": False,
        "scope_withdraw": False,
        "scope_source": "operator_declared",
        "declared_read_only": True,
        "dangerous_scope_present": False,
    }


def _classic_response(*, permit_data=None, encoded_order=None) -> dict:
    quote = {
        "input": {"amount": "1000000", "token": USDG["contract_address"]},
        "output": {
            "amount": "9120000000000000",
            "minimumAmount": "9074400000000000",
            "token": SPCX["contract_address"],
            "recipient": SWAPPER,
        },
        "route": [[{"type": "v3-pool", "fee": "500"}]],
        "gasUseEstimate": "240000",
        "gasUseEstimateUSD": "0.04",
        "slippageTolerance": 0.5,
    }
    if encoded_order is not None:
        quote["encodedOrder"] = encoded_order
    return {
        "requestId": "request-123",
        "routing": "CLASSIC",
        "quote": quote,
        "isTokenApprovalApplicable": True,
        "permitData": permit_data,
        "permitTransaction": None,
    }


class RobinhoodChainUniswapQuoteTests(unittest.IsolatedAsyncioTestCase):
    async def test_status_is_metadata_only_and_requires_strict_read_only_scope(self):
        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
        )
        status = service.status()
        self.assertTrue(status["quote_endpoint_enabled"])
        self.assertTrue(status["api_key_configured"])
        self.assertTrue(status["declared_read_only"])
        self.assertFalse(status["dangerous_scope_present"])
        self.assertFalse(status["swap_endpoint_enabled"])
        self.assertFalse(status["order_endpoint_enabled"])
        self.assertNotIn("test-uniswap-key", repr(status))

    async def test_exact_input_quote_uses_amm_only_headers_and_sanitizes_response(self):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            captured["headers"] = dict(request.headers)
            captured["body"] = json.loads(request.content.decode("utf-8"))
            return httpx.Response(200, json=_classic_response())

        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
            transport=httpx.MockTransport(handler),
        )
        result = await service.quote(
            symbol="SPCX-USDG",
            side="buy",
            amount_mode="exact_input",
            requested_amount="1",
            slippage_bps=50,
            swapper_address=SWAPPER,
            input_token=USDG,
            output_token=SPCX,
            confirm_quote=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["provider"], "uniswap_api")
        self.assertEqual(result["routing"], "CLASSIC")
        self.assertEqual(result["route_protocols"], ["V3"])
        self.assertEqual(result["input_amount"], "1")
        self.assertEqual(result["output_amount"], "0.00912")
        self.assertEqual(result["minimum_received"], "0.0090744")
        self.assertEqual(result["price_quote_per_base"], "109.6491228070175438596491228")
        self.assertIsNone(result["transaction"])
        self.assertIsNone(result["transaction_calldata"])
        self.assertIsNone(result["permit_data"])
        self.assertIsNone(result["encoded_order"])
        self.assertFalse(result["database_mutation"])
        self.assertFalse(result["execution_authority"])
        self.assertNotIn("test-uniswap-key", repr(result))

        self.assertEqual(captured["url"], "https://trade-api.gateway.uniswap.org/v1/quote")
        headers = captured["headers"]
        self.assertEqual(headers["x-api-key"], "test-uniswap-key")
        self.assertEqual(headers["x-universal-router-version"], UNISWAP_ROUTER_VERSION)
        self.assertEqual(headers["x-permit2-disabled"], "true")
        body = captured["body"]
        self.assertEqual(body["type"], "EXACT_INPUT")
        self.assertEqual(body["amount"], "1000000")
        self.assertEqual(body["tokenInChainId"], UNISWAP_CHAIN_ID)
        self.assertEqual(body["tokenOutChainId"], UNISWAP_CHAIN_ID)
        self.assertEqual(body["protocols"], list(UNISWAP_AMM_PROTOCOLS))
        self.assertEqual(body["routingPreference"], "BEST_PRICE")
        self.assertEqual(body["permitAmount"], "EXACT")
        self.assertFalse(body["generatePermitAsTransaction"])

    async def test_unsafe_scope_blocks_before_provider_contact(self):
        calls = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            return httpx.Response(200, json=_classic_response())

        unsafe = _safe_credential()
        unsafe.update(
            {
                "api_key": None,
                "scope_trade": True,
                "declared_read_only": False,
                "dangerous_scope_present": True,
            }
        )
        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=lambda: unsafe,
            transport=httpx.MockTransport(handler),
        )
        result = await service.quote(
            symbol="SPCX-USDG",
            side="buy",
            amount_mode="exact_input",
            requested_amount="1",
            slippage_bps=50,
            swapper_address=SWAPPER,
            input_token=USDG,
            output_token=SPCX,
            confirm_quote=True,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "uniswap_quote_not_configured")
        self.assertFalse(result["provider_contacted"])
        self.assertEqual(calls, 0)

    async def test_explicit_confirmation_and_exact_input_are_mandatory(self):
        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
        )
        common = dict(
            symbol="SPCX-USDG",
            side="buy",
            requested_amount="1",
            slippage_bps=50,
            swapper_address=SWAPPER,
            input_token=USDG,
            output_token=SPCX,
        )
        missing_confirmation = await service.quote(
            amount_mode="exact_input", confirm_quote=False, **common
        )
        self.assertEqual(missing_confirmation["error"], "uniswap_quote_confirmation_required")
        exact_output = await service.quote(
            amount_mode="exact_output", confirm_quote=True, **common
        )
        self.assertEqual(exact_output["error"], "uniswap_quote_exact_input_only")

    async def test_permit_or_order_artifacts_fail_closed(self):
        responses = [
            _classic_response(permit_data={}),
            _classic_response(encoded_order="0xdeadbeef"),
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=responses.pop(0))

        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
            transport=httpx.MockTransport(handler),
        )
        for expected in ("permitData", "quote.encodedOrder"):
            result = await service.quote(
                symbol="SPCX-USDG",
                side="buy",
                amount_mode="exact_input",
                requested_amount="1",
                slippage_bps=50,
                swapper_address=SWAPPER,
                input_token=USDG,
                output_token=SPCX,
                confirm_quote=True,
            )
            self.assertFalse(result["ok"])
            self.assertEqual(result["error"], "uniswap_quote_prohibited_artifact")
            self.assertEqual(result["prohibited_artifact"], expected)
            self.assertIsNone(result["transaction_calldata"])

    async def test_uniswapx_or_chained_routing_is_rejected(self):
        body = _classic_response()
        body["routing"] = "DUTCH_V3"
        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json=body)),
        )
        result = await service.quote(
            symbol="SPCX-USDG",
            side="buy",
            amount_mode="exact_input",
            requested_amount="1",
            slippage_bps=50,
            swapper_address=SWAPPER,
            input_token=USDG,
            output_token=SPCX,
            confirm_quote=True,
        )
        self.assertEqual(result["error"], "uniswap_quote_routing_not_allowed")
        self.assertFalse(result["execution_enabled"])

    async def test_provider_errors_are_bounded_and_do_not_expose_key(self):
        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    401,
                    json={"errorCode": "UNAUTHORIZED", "message": "bad key"},
                )
            ),
        )
        result = await service.quote(
            symbol="SPCX-USDG",
            side="buy",
            amount_mode="exact_input",
            requested_amount="1",
            slippage_bps=50,
            swapper_address=SWAPPER,
            input_token=USDG,
            output_token=SPCX,
            confirm_quote=True,
        )
        self.assertEqual(result["error"], "uniswap_quote_authentication_failed")
        self.assertEqual(result["http_status"], 401)
        self.assertTrue(result["provider_contacted"])
        self.assertNotIn("test-uniswap-key", repr(result))

    def test_router_exposes_quote_canary_only(self):
        router_path = Path(__file__).resolve().parents[1] / "app" / "routers" / "robinhood_chain.py"
        text = router_path.read_text(encoding="utf-8")
        self.assertIn('@router.get("/uniswap/status")', text)
        self.assertIn('@router.post("/uniswap/quote")', text)
        self.assertNotIn('@router.post("/uniswap/swap")', text)
        self.assertNotIn('@router.post("/uniswap/order")', text)
        self.assertIn("confirm_quote", text)


if __name__ == "__main__":
    unittest.main()
