from __future__ import annotations

import json
import os
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx

from app.services.robinhood_chain_uniswap_quote import (
    RobinhoodChainUniswapQuoteService,
    UNISWAP_AMM_PROTOCOLS,
    UNISWAP_CHAIN_ID,
    UNISWAP_ROUTER_VERSION,
    create_wallet_approval_capability,
    create_wallet_swap_capability,
    decode_wallet_approval_capability,
    decode_wallet_swap_capability,
    validate_wallet_approval_transaction,
    validate_wallet_rejection_handoff,
    validate_wallet_successful_approval_handoff,
    validate_wallet_successful_swap_handoff,
    validate_wallet_swap_transaction,
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
    "external_price_source": "stable",
    "external_price_id": "stable",
}
ETH = {
    "symbol": "ETH",
    "contract_address": None,
    "decimals": 18,
    "native": True,
    "identity_source": "token_registry",
    "registry_status": "registered",
    "registry_id": 10,
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
    def setUp(self):
        self._old_kms_master_key = os.environ.get("UTT_KMS_MASTER_KEY")
        os.environ["UTT_KMS_MASTER_KEY"] = "utt-test-wallet-approval-capability-master-key-v1"

    def tearDown(self):
        if self._old_kms_master_key is None:
            os.environ.pop("UTT_KMS_MASTER_KEY", None)
        else:
            os.environ["UTT_KMS_MASTER_KEY"] = self._old_kms_master_key

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
        self.assertTrue(status["swap_calldata_endpoint_enabled"])
        self.assertTrue(status["check_approval_endpoint_enabled"])
        self.assertFalse(status["order_endpoint_enabled"])
        self.assertEqual(status["interactive_firm_quote_retry_policy"], "interactive_firm_plan_quote_v2")
        self.assertEqual(status["interactive_firm_quote_max_attempts"], 3)
        self.assertEqual(status["interactive_firm_quote_transient_retry_delays_s"], [0.75, 1.5])
        self.assertEqual(status["interactive_firm_quote_no_quotes_retry_delays_s"], [5.0, 15.0])
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


    async def test_firm_plan_recovers_observed_provider_404_without_changing_slippage(self):
        quote_calls = 0
        captured_paths = []
        captured_slippage = []
        spender = _address(9)

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal quote_calls
            captured_paths.append(request.url.path)
            if request.url.path.endswith("/quote"):
                quote_calls += 1
                body = json.loads(request.content.decode("utf-8"))
                captured_slippage.append(body["slippageTolerance"])
                if quote_calls == 1:
                    return httpx.Response(
                        404,
                        json={
                            "errorCode": "ResourceNotFound",
                            "detail": "No quotes available",
                        },
                    )
                return httpx.Response(200, json=_classic_response())
            if request.url.path.endswith("/check_approval"):
                return httpx.Response(
                    200,
                    json={
                        "requestId": "approval-none",
                        "approval": None,
                        "cancel": None,
                    },
                )
            if request.url.path.endswith("/swap"):
                return httpx.Response(
                    200,
                    json={
                        "requestId": "swap-recovered",
                        "swap": {
                            "to": spender,
                            "from": SWAPPER,
                            "data": "0x12345678",
                            "value": "0",
                            "gasLimit": "250000",
                            "chainId": UNISWAP_CHAIN_ID,
                        },
                        "gasFee": "2000",
                    },
                )
            return httpx.Response(500, json={"message": "unexpected"})

        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
            transport=httpx.MockTransport(handler),
        )
        with patch(
            "app.services.robinhood_chain_uniswap_quote.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep_mock:
            result = await service.firm_quote_plan(
                symbol="SPCX-USDG",
                side="buy",
                amount_mode="exact_input",
                requested_amount="1",
                slippage_bps=50,
                swapper_address=SWAPPER,
                input_token=USDG,
                output_token=SPCX,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(quote_calls, 2)
        self.assertEqual(captured_slippage, [0.5, 0.5])
        self.assertEqual(
            captured_paths,
            ["/v1/quote", "/v1/quote", "/v1/check_approval", "/v1/swap"],
        )
        self.assertEqual(result["provider_quote_retry_policy"], "interactive_firm_plan_quote_v2")
        self.assertEqual(result["provider_quote_attempts"], 2)
        self.assertEqual(result["provider_quote_retries"], 1)
        self.assertTrue(result["provider_quote_recovery_applied"])
        self.assertEqual(
            result["provider_quote_retry_reasons"],
            ["provider_404_no_quotes_available"],
        )
        self.assertFalse(result["automatic_retry"])
        self.assertFalse(result["automatic_second_transaction"])
        sleep_mock.assert_awaited_once_with(5.0)

    async def test_firm_plan_exhausts_observed_provider_404_without_followup_calls(self):
        captured_paths = []
        captured_slippage = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured_paths.append(request.url.path)
            if request.url.path.endswith("/quote"):
                body = json.loads(request.content.decode("utf-8"))
                captured_slippage.append(body["slippageTolerance"])
                return httpx.Response(
                    404,
                    json={
                        "errorCode": "ResourceNotFound",
                        "detail": "No quotes available",
                    },
                )
            return httpx.Response(500, json={"message": "must not be called"})

        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
            transport=httpx.MockTransport(handler),
        )
        with patch(
            "app.services.robinhood_chain_uniswap_quote.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep_mock:
            result = await service.firm_quote_plan(
                symbol="SPCX-USDG",
                side="buy",
                amount_mode="exact_input",
                requested_amount="1",
                slippage_bps=50,
                swapper_address=SWAPPER,
                input_token=USDG,
                output_token=SPCX,
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "uniswap_quote_provider_error")
        self.assertEqual(result["http_status"], 404)
        self.assertEqual(captured_paths, ["/v1/quote", "/v1/quote", "/v1/quote"])
        self.assertEqual(captured_slippage, [0.5, 0.5, 0.5])
        self.assertEqual(result["provider_quote_attempts"], 3)
        self.assertEqual(result["provider_quote_retries"], 2)
        self.assertFalse(result["provider_quote_recovery_applied"])
        self.assertEqual(
            result["provider_quote_retry_reasons"],
            [
                "provider_404_no_quotes_available",
                "provider_404_no_quotes_available",
                "provider_404_no_quotes_available",
            ],
        )
        self.assertFalse(result["automatic_retry"])
        self.assertFalse(result["automatic_second_transaction"])
        self.assertEqual(sleep_mock.await_count, 2)
        self.assertEqual([call.args[0] for call in sleep_mock.await_args_list], [5.0, 15.0])

    async def test_firm_plan_does_not_retry_non_retryable_provider_4xx(self):
        captured_paths = []

        def handler(request: httpx.Request) -> httpx.Response:
            captured_paths.append(request.url.path)
            return httpx.Response(
                400,
                json={
                    "errorCode": "BadRequest",
                    "detail": "invalid request",
                },
            )

        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
            transport=httpx.MockTransport(handler),
        )
        with patch(
            "app.services.robinhood_chain_uniswap_quote.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep_mock:
            result = await service.firm_quote_plan(
                symbol="SPCX-USDG",
                side="buy",
                amount_mode="exact_input",
                requested_amount="1",
                slippage_bps=50,
                swapper_address=SWAPPER,
                input_token=USDG,
                output_token=SPCX,
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "uniswap_quote_provider_error")
        self.assertEqual(result["http_status"], 400)
        self.assertEqual(captured_paths, ["/v1/quote"])
        self.assertEqual(result["provider_quote_attempts"], 1)
        self.assertEqual(result["provider_quote_retries"], 0)
        self.assertFalse(result["provider_quote_recovery_applied"])
        self.assertEqual(result["provider_quote_retry_reasons"], [])
        self.assertFalse(result["automatic_retry"])
        self.assertFalse(result["automatic_second_transaction"])
        sleep_mock.assert_not_awaited()

    async def test_firm_plan_recovers_transient_provider_error_before_followup_calls(self):
        quote_calls = 0
        captured_paths = []
        spender = _address(9)

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal quote_calls
            captured_paths.append(request.url.path)
            if request.url.path.endswith("/quote"):
                quote_calls += 1
                if quote_calls == 1:
                    return httpx.Response(
                        502,
                        headers={"Retry-After": "1"},
                        json={"message": "temporary upstream failure"},
                    )
                return httpx.Response(200, json=_classic_response())
            if request.url.path.endswith("/check_approval"):
                return httpx.Response(
                    200,
                    json={
                        "requestId": "approval-none",
                        "approval": None,
                        "cancel": None,
                    },
                )
            if request.url.path.endswith("/swap"):
                return httpx.Response(
                    200,
                    json={
                        "requestId": "swap-after-transient",
                        "swap": {
                            "to": spender,
                            "from": SWAPPER,
                            "data": "0x12345678",
                            "value": "0",
                            "gasLimit": "250000",
                            "chainId": UNISWAP_CHAIN_ID,
                        },
                    },
                )
            return httpx.Response(500, json={"message": "unexpected"})

        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
            transport=httpx.MockTransport(handler),
        )
        with patch(
            "app.services.robinhood_chain_uniswap_quote.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep_mock:
            result = await service.firm_quote_plan(
                symbol="SPCX-USDG",
                side="buy",
                amount_mode="exact_input",
                requested_amount="1",
                slippage_bps=100,
                swapper_address=SWAPPER,
                input_token=USDG,
                output_token=SPCX,
            )

        self.assertTrue(result["ok"])
        self.assertEqual(quote_calls, 2)
        self.assertEqual(
            captured_paths,
            ["/v1/quote", "/v1/quote", "/v1/check_approval", "/v1/swap"],
        )
        self.assertEqual(result["provider_quote_attempts"], 2)
        self.assertEqual(result["provider_quote_retries"], 1)
        self.assertTrue(result["provider_quote_recovery_applied"])
        self.assertEqual(
            result["provider_quote_retry_reasons"],
            ["uniswap_quote_provider_transient_error"],
        )
        self.assertFalse(result["automatic_retry"])
        self.assertFalse(result["automatic_second_transaction"])
        sleep_mock.assert_awaited_once_with(1.0)

    async def test_generic_erc20_firm_plan_returns_exact_approval_and_unsigned_swap(self):
        spender = _address(9)
        captured_paths = []

        def approval_data(amount: int) -> str:
            return "0x095ea7b3" + ("0" * 24) + spender[2:] + int(amount).to_bytes(32, "big").hex()

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            captured_paths.append(path)
            if path.endswith("/quote"):
                return httpx.Response(200, json=_classic_response())
            if path.endswith("/check_approval"):
                return httpx.Response(200, json={
                    "requestId": "approval-1",
                    "approval": {
                        "to": USDG["contract_address"],
                        "from": SWAPPER,
                        "data": approval_data(1_000_000),
                        "value": "0",
                        "gasLimit": "65000",
                        "chainId": UNISWAP_CHAIN_ID,
                    },
                    "cancel": None,
                    "gasFee": "1000",
                })
            if path.endswith("/swap"):
                return httpx.Response(200, json={
                    "requestId": "swap-1",
                    "swap": {
                        "to": spender,
                        "from": SWAPPER,
                        "data": "0x12345678",
                        "value": "0",
                        "gasLimit": "250000",
                        "chainId": UNISWAP_CHAIN_ID,
                    },
                    "gasFee": "2000",
                })
            return httpx.Response(404, json={"message": "unexpected"})

        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
            transport=httpx.MockTransport(handler),
        )
        result = await service.firm_quote_plan(
            symbol="SPCX-USDG",
            side="buy",
            amount_mode="exact_input",
            requested_amount="1",
            slippage_bps=50,
            swapper_address=SWAPPER,
            input_token=USDG,
            output_token=SPCX,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["provider"], "uniswap_api")
        self.assertTrue(result["approval_required"])
        approval = result["allowance"]["approval_transaction_plan"]
        self.assertEqual(approval["approved_amount_atomic"], "1000000")
        self.assertEqual(approval["provider_approved_amount_atomic"], "1000000")
        self.assertFalse(approval["provider_approval_rewritten"])
        self.assertTrue(approval["approval_exact"])
        self.assertFalse(approval["unlimited_approval"])
        self.assertEqual(approval["spender"].lower(), spender.lower())
        unsigned = result["unsigned_transaction_plan"]
        self.assertEqual(unsigned["to"].lower(), spender.lower())
        self.assertEqual(unsigned["value_wei"], "0")
        self.assertFalse(unsigned["native_input"])
        self.assertFalse(result["wallet_connection_requested"])
        self.assertFalse(result["signing_enabled"])
        self.assertFalse(result["broadcast_enabled"])
        self.assertEqual(captured_paths, ["/v1/quote", "/v1/check_approval", "/v1/swap"])
        self.assertNotIn("test-uniswap-key", repr(result))

    async def test_provider_oversized_approval_is_rewritten_to_exact_input(self):
        spender = _address(9)
        provider_amount = (1 << 256) - 1

        def approval_data(amount: int) -> str:
            return "0x095ea7b3" + ("0" * 24) + spender[2:] + int(amount).to_bytes(32, "big").hex()

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/quote"):
                return httpx.Response(200, json=_classic_response())
            if request.url.path.endswith("/check_approval"):
                return httpx.Response(200, json={
                    "requestId": "approval-provider-max",
                    "approval": {
                        "to": USDG["contract_address"],
                        "from": SWAPPER,
                        "data": approval_data(provider_amount),
                        "value": "0",
                        "gasLimit": "65000",
                        "chainId": UNISWAP_CHAIN_ID,
                    },
                    "cancel": None,
                })
            if request.url.path.endswith("/swap"):
                return httpx.Response(200, json={
                    "requestId": "swap-provider-max",
                    "swap": {
                        "to": spender,
                        "from": SWAPPER,
                        "data": "0x12345678",
                        "value": "0",
                        "gasLimit": "250000",
                        "chainId": UNISWAP_CHAIN_ID,
                    },
                })
            return httpx.Response(404, json={"message": "unexpected"})

        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
            transport=httpx.MockTransport(handler),
        )
        result = await service.firm_quote_plan(
            symbol="SPCX-USDG",
            side="buy",
            amount_mode="exact_input",
            requested_amount="1",
            slippage_bps=50,
            swapper_address=SWAPPER,
            input_token=USDG,
            output_token=SPCX,
        )

        self.assertTrue(result["ok"])
        approval = result["allowance"]["approval_transaction_plan"]
        self.assertEqual(approval["approved_amount_atomic"], "1000000")
        self.assertEqual(approval["provider_approved_amount_atomic"], str(provider_amount))
        self.assertTrue(approval["provider_approval_rewritten"])
        self.assertTrue(approval["approval_exact"])
        self.assertFalse(approval["unlimited_approval"])
        self.assertEqual(approval["data"], approval_data(1_000_000))


    async def test_approval_required_plan_defers_swap_simulation_until_approval(self):
        spender = _address(9)
        captured_swap_body = {}

        def approval_data(amount: int) -> str:
            return "0x095ea7b3" + ("0" * 24) + spender[2:] + int(amount).to_bytes(32, "big").hex()

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/quote"):
                return httpx.Response(200, json=_classic_response())
            if request.url.path.endswith("/check_approval"):
                return httpx.Response(200, json={
                    "requestId": "approval-deferred-simulation",
                    "approval": {
                        "to": USDG["contract_address"],
                        "from": SWAPPER,
                        "data": approval_data(1_000_000),
                        "value": "0",
                        "gasLimit": "65000",
                        "chainId": UNISWAP_CHAIN_ID,
                    },
                    "cancel": None,
                })
            if request.url.path.endswith("/swap"):
                captured_swap_body.update(json.loads(request.content.decode("utf-8")))
                return httpx.Response(200, json={
                    "requestId": "swap-deferred-simulation",
                    "swap": {
                        "to": spender,
                        "from": SWAPPER,
                        "data": "0x12345678",
                        "value": "0",
                        "chainId": UNISWAP_CHAIN_ID,
                    },
                })
            return httpx.Response(404, json={"message": "unexpected"})

        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
            transport=httpx.MockTransport(handler),
        )
        result = await service.firm_quote_plan(
            symbol="SPCX-USDG",
            side="buy",
            amount_mode="exact_input",
            requested_amount="1",
            slippage_bps=50,
            swapper_address=SWAPPER,
            input_token=USDG,
            output_token=SPCX,
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["approval_required"])
        self.assertFalse(captured_swap_body["simulateTransaction"])
        self.assertFalse(result["swap_simulation_requested"])
        self.assertTrue(result["swap_simulation_deferred_until_approval"])
        self.assertTrue(result["requires_refresh_after_approval"])
        self.assertFalse(result["unsigned_transaction_plan"]["provider_simulation_requested"])
        self.assertFalse(result["unsigned_transaction_plan"]["gas_limit_estimated"])
        self.assertIsNone(result["unsigned_transaction_plan"]["gas_limit"])
        self.assertTrue(result["unsigned_transaction_plan"]["requires_refresh_after_approval"])

    async def test_native_input_plan_skips_approval_and_binds_transaction_value(self):
        captured_paths = []
        native_quote = {
            "requestId": "native-quote",
            "routing": "CLASSIC",
            "quote": {
                "input": {"amount": "100000000000000", "token": "0x0000000000000000000000000000000000000000"},
                "output": {
                    "amount": "1000000000000000000",
                    "minimumAmount": "995000000000000000",
                    "token": SPCX["contract_address"],
                },
                "route": [[{"type": "v3-pool"}]],
                "gasUseEstimate": "200000",
            },
            "isTokenApprovalApplicable": False,
            "permitData": None,
            "permitTransaction": None,
        }

        def handler(request: httpx.Request) -> httpx.Response:
            captured_paths.append(request.url.path)
            if request.url.path.endswith("/quote"):
                return httpx.Response(200, json=native_quote)
            if request.url.path.endswith("/swap"):
                return httpx.Response(200, json={
                    "requestId": "native-swap",
                    "swap": {
                        "to": _address(9),
                        "from": SWAPPER,
                        "data": "0x12345678",
                        "value": "100000000000000",
                        "gasLimit": "250000",
                        "chainId": UNISWAP_CHAIN_ID,
                    },
                })
            return httpx.Response(500, json={"message": "approval must not be called"})

        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
            transport=httpx.MockTransport(handler),
        )
        result = await service.firm_quote_plan(
            symbol="SPCX-ETH",
            side="buy",
            amount_mode="exact_input",
            requested_amount="0.0001",
            slippage_bps=50,
            swapper_address=SWAPPER,
            input_token=ETH,
            output_token=SPCX,
        )
        self.assertTrue(result["ok"])
        self.assertFalse(result["approval_required"])
        self.assertFalse(result["allowance"]["applicable"])
        self.assertTrue(result["unsigned_transaction_plan"]["native_input"])
        self.assertEqual(result["unsigned_transaction_plan"]["value_wei"], "100000000000000")
        self.assertTrue(result["swap_simulation_requested"])
        self.assertFalse(result["swap_simulation_deferred_until_approval"])
        self.assertFalse(result["requires_refresh_after_approval"])
        self.assertTrue(result["unsigned_transaction_plan"]["provider_simulation_requested"])
        self.assertTrue(result["unsigned_transaction_plan"]["gas_limit_estimated"])
        self.assertFalse(result["unsigned_transaction_plan"]["requires_refresh_after_approval"])
        self.assertEqual(captured_paths, ["/v1/quote", "/v1/swap"])

    async def test_hex_transaction_quantities_are_normalized_for_erc20_plan(self):
        spender = _address(9)

        def approval_data(amount: int) -> str:
            return "0x095ea7b3" + ("0" * 24) + spender[2:] + int(amount).to_bytes(32, "big").hex()

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/quote"):
                return httpx.Response(200, json=_classic_response())
            if request.url.path.endswith("/check_approval"):
                return httpx.Response(200, json={
                    "requestId": "approval-hex",
                    "approval": {
                        "to": USDG["contract_address"],
                        "from": SWAPPER,
                        "data": approval_data(1_000_000),
                        "value": "0x0",
                        "gasLimit": hex(65_000),
                        "chainId": hex(UNISWAP_CHAIN_ID),
                        "maxFeePerGas": "0x64",
                    },
                    "cancel": None,
                })
            if request.url.path.endswith("/swap"):
                return httpx.Response(200, json={
                    "requestId": "swap-hex",
                    "swap": {
                        "to": spender,
                        "from": SWAPPER,
                        "data": "0x12345678",
                        "value": "0x0",
                        "gasLimit": hex(250_000),
                        "chainId": hex(UNISWAP_CHAIN_ID),
                        "maxFeePerGas": "0x64",
                        "maxPriorityFeePerGas": "0x2",
                    },
                })
            return httpx.Response(404, json={"message": "unexpected"})

        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
            transport=httpx.MockTransport(handler),
        )
        result = await service.firm_quote_plan(
            symbol="SPCX-USDG",
            side="buy",
            amount_mode="exact_input",
            requested_amount="1",
            slippage_bps=50,
            swapper_address=SWAPPER,
            input_token=USDG,
            output_token=SPCX,
        )
        self.assertTrue(result["ok"])
        approval = result["allowance"]["approval_transaction_plan"]
        self.assertEqual(approval["value_wei"], "0")
        self.assertEqual(approval["gas_limit"], "65000")
        self.assertEqual(approval["chain_id"], UNISWAP_CHAIN_ID)
        self.assertEqual(approval["max_fee_per_gas"], "100")
        unsigned = result["unsigned_transaction_plan"]
        self.assertEqual(unsigned["value_wei"], "0")
        self.assertEqual(unsigned["gas_limit"], "250000")
        self.assertEqual(unsigned["chain_id"], UNISWAP_CHAIN_ID)
        self.assertEqual(unsigned["max_fee_per_gas"], "100")
        self.assertEqual(unsigned["max_priority_fee_per_gas"], "2")

    async def test_hex_native_transaction_value_is_normalized_and_bound(self):
        native_input_atomic = 100_000_000_000_000
        native_quote = {
            "requestId": "native-quote-hex",
            "routing": "CLASSIC",
            "quote": {
                "input": {
                    "amount": str(native_input_atomic),
                    "token": "0x0000000000000000000000000000000000000000",
                },
                "output": {
                    "amount": "1000000000000000000",
                    "minimumAmount": "995000000000000000",
                    "token": SPCX["contract_address"],
                },
                "route": [[{"type": "v3-pool"}]],
                "gasUseEstimate": "200000",
            },
            "isTokenApprovalApplicable": False,
            "permitData": None,
            "permitTransaction": None,
        }

        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/quote"):
                return httpx.Response(200, json=native_quote)
            if request.url.path.endswith("/swap"):
                return httpx.Response(200, json={
                    "requestId": "native-swap-hex",
                    "swap": {
                        "to": _address(9),
                        "from": SWAPPER,
                        "data": "0x12345678",
                        "value": hex(native_input_atomic),
                        "gasLimit": hex(250_000),
                        "chainId": hex(UNISWAP_CHAIN_ID),
                        "gasPrice": "0x3b9aca00",
                    },
                })
            return httpx.Response(500, json={"message": "approval must not be called"})

        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
            transport=httpx.MockTransport(handler),
        )
        result = await service.firm_quote_plan(
            symbol="SPCX-ETH",
            side="buy",
            amount_mode="exact_input",
            requested_amount="0.0001",
            slippage_bps=50,
            swapper_address=SWAPPER,
            input_token=ETH,
            output_token=SPCX,
        )
        self.assertTrue(result["ok"])
        unsigned = result["unsigned_transaction_plan"]
        self.assertEqual(unsigned["value_wei"], str(native_input_atomic))
        self.assertEqual(unsigned["gas_limit"], "250000")
        self.assertEqual(unsigned["chain_id"], UNISWAP_CHAIN_ID)
        self.assertEqual(unsigned["gas_price"], "1000000000")
        self.assertTrue(unsigned["native_input"])
        self.assertFalse(result["approval_required"])

    async def test_synthetic_orderbook_uses_uniswap_for_both_directions(self):
        captured = []

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode("utf-8"))
            amount = int(body["amount"])
            token_in = body["tokenIn"].lower()
            captured.append({"token_in": token_in, "amount": amount})
            if token_in == SPCX["contract_address"].lower():
                # 1 SPCX -> 99 USDG on the bid side.
                output_amount = str(max(1, amount * 99 // 1_000_000_000_000))
                output_token = USDG["contract_address"]
            else:
                # 100 USDG/SPCX on the ask side.
                output_amount = str(amount * 10_000_000_000)
                output_token = SPCX["contract_address"]
            return httpx.Response(200, json={
                "requestId": f"book-{token_in[-4:]}-{amount}",
                "routing": "CLASSIC",
                "quote": {
                    "input": {"amount": str(amount), "token": body["tokenIn"]},
                    "output": {
                        "amount": output_amount,
                        "minimumAmount": output_amount,
                        "token": output_token,
                    },
                    "route": [[{"type": "v4-pool"}]],
                    "gasUseEstimate": "200000",
                },
                "isTokenApprovalApplicable": True,
                "permitData": None,
                "permitTransaction": None,
            })

        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
            transport=httpx.MockTransport(handler),
        )
        common_capability = {
            "provider": "uniswap_api",
            "amount_mode": "exact_input",
            "indicative_status": "available",
        }
        result = await service.synthetic_orderbook_for_pair(
            symbol="SPCX-USDG",
            depth=2,
            taker_address=SWAPPER,
            base_token=SPCX,
            quote_token=USDG,
            base_to_quote_capability={
                **common_capability,
                "from_asset": "SPCX",
                "to_asset": "USDG",
                "probe_amount": "0.0001",
            },
            quote_to_base_capability={
                **common_capability,
                "from_asset": "USDG",
                "to_asset": "SPCX",
                "probe_amount": "0.0001",
            },
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["provider"], "uniswap_api")
        self.assertEqual(result["sampling_policy"], "quote_notional_matched_v1")
        self.assertEqual(result["quote_probe_seed"], "1")
        self.assertEqual(result["quote_probe_seed_source"], "token_registry_stable_quote")
        self.assertEqual(result["minimum_quote_input_atomic"], 1000)
        self.assertEqual(result["minimum_quote_output_atomic"], 100)
        self.assertEqual(result["depth_returned"], 2)
        self.assertEqual(len(result["bids"]), 2)
        self.assertEqual(len(result["asks"]), 2)
        self.assertEqual(result["best_bid"], "99")
        self.assertEqual(result["best_ask"], "100")
        self.assertFalse(result["crossed"])
        self.assertEqual([item["amount"] for item in captured if item["token_in"] == USDG["contract_address"].lower()], [250000, 500000])
        self.assertEqual([item["token_in"] for item in captured], [
            USDG["contract_address"].lower(),
            SPCX["contract_address"].lower(),
            USDG["contract_address"].lower(),
            SPCX["contract_address"].lower(),
        ])
        for bid, ask in zip(result["bids"], result["asks"]):
            self.assertEqual(bid["sample_quote_notional"], ask["sample_quote_notional"])
            self.assertEqual(bid["paired_base_amount"], ask["paired_base_amount"])
            self.assertEqual(bid["input_amount"], ask["output_amount"])
            self.assertEqual(bid["paired_provider_request_id"], ask["provider_request_id"])
            self.assertEqual(ask["paired_provider_request_id"], bid["provider_request_id"])
            self.assertEqual(bid["sampling_policy"], "quote_notional_matched_v1")
            self.assertEqual(ask["sampling_policy"], "quote_notional_matched_v1")
            self.assertEqual(bid["provider"], "uniswap_api")
            self.assertEqual(ask["provider"], "uniswap_api")
        self.assertFalse(result["execution_enabled"])

    async def test_synthetic_orderbook_drops_unpaired_levels_when_provider_quote_fails(self):
        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode("utf-8"))
            amount = int(body["amount"])
            token_in = body["tokenIn"].lower()
            if token_in == USDG["contract_address"].lower() and amount == 500000:
                return httpx.Response(502, json={"error": "temporary_provider_failure"})
            if token_in == SPCX["contract_address"].lower():
                output_amount = str(max(1, amount * 99 // 1_000_000_000_000))
                output_token = USDG["contract_address"]
            else:
                output_amount = str(amount * 10_000_000_000)
                output_token = SPCX["contract_address"]
            return httpx.Response(200, json={
                "requestId": f"book-partial-{amount}",
                "routing": "CLASSIC",
                "quote": {
                    "input": {"amount": str(amount), "token": body["tokenIn"]},
                    "output": {
                        "amount": output_amount,
                        "minimumAmount": output_amount,
                        "token": output_token,
                    },
                    "route": [[{"type": "v4-pool"}]],
                    "gasUseEstimate": "200000",
                },
                "isTokenApprovalApplicable": True,
                "permitData": None,
                "permitTransaction": None,
            })

        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
            transport=httpx.MockTransport(handler),
        )
        common_capability = {
            "provider": "uniswap_api",
            "amount_mode": "exact_input",
            "indicative_status": "available",
        }
        result = await service.synthetic_orderbook_for_pair(
            symbol="SPCX-USDG",
            depth=3,
            taker_address=SWAPPER,
            base_token=SPCX,
            quote_token=USDG,
            base_to_quote_capability={
                **common_capability,
                "from_asset": "SPCX",
                "to_asset": "USDG",
                "probe_amount": "1",
            },
            quote_to_base_capability={
                **common_capability,
                "from_asset": "USDG",
                "to_asset": "SPCX",
                "probe_amount": "1",
            },
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["depth_returned"], 2)
        self.assertEqual(len(result["bids"]), 2)
        self.assertEqual(len(result["asks"]), 2)
        self.assertEqual(result["warning_count"], 1)
        self.assertEqual(result["errors"][0]["side"], "ask")
        self.assertEqual(result["errors"][0]["sample_quote_notional"], "0.5")
        self.assertFalse(result["crossed"])
        self.assertEqual(
            {row["pair_index"] for row in result["bids"]},
            {row["pair_index"] for row in result["asks"]},
        )

    async def test_synthetic_orderbook_precision_floor_clamps_nonstable_quote_seed(self):
        captured_quote_inputs = []

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content.decode("utf-8"))
            amount = int(body["amount"])
            token_in = body["tokenIn"].lower()
            if token_in == USDG["contract_address"].lower():
                captured_quote_inputs.append(amount)
                output_amount = str(amount * 10_000_000_000)
                output_token = SPCX["contract_address"]
            else:
                output_amount = str(max(1, amount * 99 // 1_000_000_000_000))
                output_token = USDG["contract_address"]
            return httpx.Response(200, json={
                "requestId": f"book-floor-{amount}",
                "routing": "CLASSIC",
                "quote": {
                    "input": {"amount": str(amount), "token": body["tokenIn"]},
                    "output": {
                        "amount": output_amount,
                        "minimumAmount": output_amount,
                        "token": output_token,
                    },
                    "route": [[{"type": "v4-pool"}]],
                    "gasUseEstimate": "200000",
                },
                "isTokenApprovalApplicable": True,
                "permitData": None,
                "permitTransaction": None,
            })

        service = RobinhoodChainUniswapQuoteService(
            api_base="https://trade-api.gateway.uniswap.org/v1",
            timeout_s=15,
            max_concurrent=1,
            credential_getter=_safe_credential,
            transport=httpx.MockTransport(handler),
        )
        common_capability = {
            "provider": "uniswap_api",
            "amount_mode": "exact_input",
            "indicative_status": "available",
        }
        nonstable_quote = {
            **USDG,
            "external_price_source": "coingecko",
            "external_price_id": "usd-generic",
        }
        result = await service.synthetic_orderbook_for_pair(
            symbol="SPCX-USDG",
            depth=1,
            taker_address=SWAPPER,
            base_token=SPCX,
            quote_token=nonstable_quote,
            base_to_quote_capability={
                **common_capability,
                "from_asset": "SPCX",
                "to_asset": "USDG",
                "probe_amount": "0.0001",
            },
            quote_to_base_capability={
                **common_capability,
                "from_asset": "USDG",
                "to_asset": "SPCX",
                "probe_amount": "0.0001",
            },
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["quote_probe_seed"], "0.004")
        self.assertEqual(result["quote_probe_seed_source"], "quote_direction_capability")
        self.assertEqual(captured_quote_inputs, [1000])
        self.assertEqual(result["bids"][0]["quote_quantity"], "0.00099")
        self.assertEqual(result["asks"][0]["quote_quantity"], "0.001")
        self.assertFalse(result["crossed"])

    def test_wallet_rejection_handoff_accepts_exact_finite_approval(self):
        spender = _address(9)
        exact = "500000"
        provider_max = str((1 << 256) - 1)
        approval = {
            "from": SWAPPER,
            "to": USDG["contract_address"],
            "data": "0x095ea7b3" + ("0" * 24) + spender[2:] + int(exact).to_bytes(32, "big").hex(),
            "value_wei": "0",
            "gas_limit": "65000",
            "max_fee_per_gas": "100",
            "max_priority_fee_per_gas": "2",
            "gas_price": None,
            "chain_id": UNISWAP_CHAIN_ID,
            "token": USDG["contract_address"],
            "token_symbol": "USDG",
            "spender": spender,
            "approved_amount_atomic": exact,
            "provider_approved_amount_atomic": provider_max,
            "provider_approval_rewritten": True,
            "approval_exact": True,
            "unlimited_approval": False,
        }
        plan = {
            "ok": True,
            "provider": "uniswap_api",
            "read_only": True,
            "execution_enabled": False,
            "signing_enabled": False,
            "broadcast_enabled": False,
            "approval_required": True,
            "requires_refresh_after_approval": True,
            "allowance": {
                "applicable": True,
                "approval_exact": True,
                "unlimited_approval": False,
                "required_amount_atomic": exact,
                "approval_transaction_plan": approval,
            },
            "unsigned_transaction_plan": {
                "from": SWAPPER,
                "to": _address(10),
                "data": "0x12345678",
                "value_wei": "0",
                "gas_limit": "200000",
                "max_fee_per_gas": "100",
                "max_priority_fee_per_gas": "2",
                "gas_price": None,
                "chain_id": UNISWAP_CHAIN_ID,
                "native_input": False,
                "input_asset": "USDG",
                "output_asset": "SPCX",
                "input_amount": "0.5",
                "input_amount_atomic": exact,
                "provider_simulation_requested": False,
            },
        }
        result = validate_wallet_rejection_handoff(
            plan,
            wallet_address=SWAPPER,
            native_balance_wei="1000000000000000000",
            input_balance_atomic="3754869",
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["action"], "approval")
        self.assertEqual(result["approval"]["approved_amount_atomic"], exact)
        self.assertEqual(result["approval"]["provider_approved_amount_atomic"], provider_max)
        self.assertTrue(result["approval"]["provider_approval_rewritten"])
        self.assertFalse(result["approval"]["unlimited_approval"])
        self.assertEqual(result["transaction"]["value_wei"], "0")
        self.assertFalse(result["successful_broadcast_authorized"])
        self.assertTrue(result["reject_only"])

    def test_wallet_rejection_handoff_blocks_non_exact_or_insufficient_erc20(self):
        exact = "500000"
        spender = _address(9)
        approval = {
            "from": SWAPPER,
            "to": USDG["contract_address"],
            "data": "0x095ea7b3" + ("0" * 24) + spender[2:] + int(exact).to_bytes(32, "big").hex(),
            "value_wei": "0",
            "gas_limit": "65000",
            "max_fee_per_gas": "100",
            "chain_id": UNISWAP_CHAIN_ID,
            "token": USDG["contract_address"],
            "token_symbol": "USDG",
            "spender": spender,
            "approved_amount_atomic": exact,
            "provider_approved_amount_atomic": exact,
            "provider_approval_rewritten": False,
            "approval_exact": True,
            "unlimited_approval": False,
        }
        plan = {
            "ok": True, "provider": "uniswap_api", "read_only": True,
            "execution_enabled": False, "signing_enabled": False, "broadcast_enabled": False,
            "approval_required": True, "requires_refresh_after_approval": True,
            "allowance": {"applicable": True, "approval_exact": True, "unlimited_approval": False,
                          "required_amount_atomic": exact, "approval_transaction_plan": approval},
            "unsigned_transaction_plan": {"from": SWAPPER, "to": _address(10), "data": "0x12345678",
                                          "value_wei": "0", "gas_limit": "200000", "max_fee_per_gas": "100",
                                          "chain_id": UNISWAP_CHAIN_ID, "native_input": False, "input_asset": "USDG",
                                          "output_asset": "SPCX", "input_amount": "0.5", "input_amount_atomic": exact,
                                          "provider_simulation_requested": False},
        }
        with self.assertRaisesRegex(ValueError, "wallet_rejection_insufficient_input_balance"):
            validate_wallet_rejection_handoff(
                plan, wallet_address=SWAPPER, native_balance_wei="1000000000000000000", input_balance_atomic="499999"
            )
        plan["allowance"]["unlimited_approval"] = True
        with self.assertRaisesRegex(ValueError, "wallet_rejection_exact_approval_required"):
            validate_wallet_rejection_handoff(
                plan, wallet_address=SWAPPER, native_balance_wei="1000000000000000000", input_balance_atomic="500000"
            )

    def test_wallet_rejection_handoff_accepts_native_swap_and_blocks_overspend(self):
        exact = "100000000000000"
        plan = {
            "ok": True, "provider": "uniswap_api", "read_only": True,
            "execution_enabled": False, "signing_enabled": False, "broadcast_enabled": False,
            "approval_required": False, "requires_refresh_after_approval": False,
            "allowance": {"applicable": False, "approval_required": False, "unlimited_approval": False},
            "unsigned_transaction_plan": {
                "from": SWAPPER, "to": _address(10), "data": "0x12345678",
                "value_wei": exact, "gas_limit": "200000", "max_fee_per_gas": "100",
                "max_priority_fee_per_gas": "2", "gas_price": None, "chain_id": UNISWAP_CHAIN_ID,
                "native_input": True, "input_asset": "ETH", "output_asset": "SPCX",
                "input_amount": "0.0001", "input_amount_atomic": exact,
                "provider_simulation_requested": True,
            },
        }
        result = validate_wallet_rejection_handoff(
            plan, wallet_address=SWAPPER, native_balance_wei="1000000000000000000"
        )
        self.assertEqual(result["action"], "swap")
        self.assertEqual(result["transaction"]["value_wei"], exact)
        self.assertEqual(result["balance_checks"]["required_native_wei"], str(int(exact) + 20_000_000))
        with self.assertRaisesRegex(ValueError, "wallet_rejection_insufficient_native_balance"):
            validate_wallet_rejection_handoff(
                plan, wallet_address=SWAPPER, native_balance_wei=exact
            )

    def test_wallet_rejection_router_stays_reject_only_after_ui_harness_removal(self):
        root = Path(__file__).resolve().parents[2]
        router_text = (root / "backend" / "app" / "routers" / "robinhood_chain.py").read_text(encoding="utf-8")
        api_text = (root / "frontend" / "src" / "lib" / "api.js").read_text(encoding="utf-8")
        ticket_text = (root / "frontend" / "src" / "OrderTicketWidget.jsx").read_text(encoding="utf-8")
        self.assertIn('@router.post("/wallet-rejection/prepare")', router_text)
        self.assertNotIn('@router.post("/wallet-rejection/submission")', router_text)
        self.assertIn('"successful_broadcast_authorized": False', router_text)
        self.assertIn('"automatic_retry": False', router_text)
        self.assertIn('prepareRobinhoodChainWalletRejection', api_text)
        self.assertNotIn('data-rh-wallet-rejection="r5c-5d-2f-1"', ticket_text)
        self.assertNotIn('FIRST METAMASK REQUEST REJECTION TEST', ticket_text)
        self.assertNotIn('Prepare First Wallet-Rejection Test', ticket_text)
        self.assertNotIn('Open MetaMask — REJECT THIS REQUEST', ticket_text)
        self.assertIn('automatic_retry: false', ticket_text)

    def test_wallet_successful_approval_router_is_approval_only_and_has_no_swap_handoff(self):
        root = Path(__file__).resolve().parents[2]
        router_text = (root / "backend" / "app" / "routers" / "robinhood_chain.py").read_text(encoding="utf-8")
        api_text = (root / "frontend" / "src" / "lib" / "api.js").read_text(encoding="utf-8")
        ticket_text = (root / "frontend" / "src" / "OrderTicketWidget.jsx").read_text(encoding="utf-8")
        self.assertIn('@router.post("/wallet-approval/prepare")', router_text)
        self.assertIn('@router.post("/wallet-approval/receipt")', router_text)
        self.assertNotIn('@router.post("/wallet-approval/swap")', router_text)
        self.assertIn('"approval_only": True', router_text)
        self.assertIn('"swap_request_authorized": False', router_text)
        self.assertIn('"automatic_second_transaction": False', router_text)
        self.assertIn('prepareRobinhoodChainWalletApproval', api_text)
        self.assertIn('refreshRobinhoodChainWalletApprovalReceipt', api_text)
        self.assertIn('SUCCESSFUL FINITE APPROVAL ONLY', ticket_text)
        self.assertIn('Open MetaMask — EXACT FINITE APPROVAL', ticket_text)
        self.assertIn('SWAP REQUEST AUTHORIZED: NO', ticket_text)
        self.assertIn('ROBINHOOD_CHAIN_LIFECYCLE_PREFLIGHT_TIMEOUT_MS = 120000', ticket_text)
        self.assertIn('wallet_approval_prepare', ticket_text)
        self.assertIn('wallet_swap_prepare', ticket_text)

    def _wallet_approval_test_handoff(self, *, exact: str = "500000"):
        spender = _address(9)
        provider_max = str((1 << 256) - 1)
        approval = {
            "from": SWAPPER,
            "to": USDG["contract_address"],
            "data": "0x095ea7b3" + ("0" * 24) + spender[2:] + int(exact).to_bytes(32, "big").hex(),
            "value_wei": "0",
            "gas_limit": "65000",
            "max_fee_per_gas": "100",
            "max_priority_fee_per_gas": "2",
            "gas_price": None,
            "chain_id": UNISWAP_CHAIN_ID,
            "token": USDG["contract_address"],
            "token_symbol": "USDG",
            "spender": spender,
            "approved_amount_atomic": exact,
            "provider_approved_amount_atomic": provider_max,
            "provider_approval_rewritten": True,
            "approval_exact": True,
            "unlimited_approval": False,
        }
        plan = {
            "ok": True,
            "provider": "uniswap_api",
            "read_only": True,
            "execution_enabled": False,
            "signing_enabled": False,
            "broadcast_enabled": False,
            "approval_required": True,
            "requires_refresh_after_approval": True,
            "allowance": {
                "applicable": True,
                "approval_exact": True,
                "unlimited_approval": False,
                "required_amount_atomic": exact,
                "approval_transaction_plan": approval,
            },
            "unsigned_transaction_plan": {
                "from": SWAPPER,
                "to": _address(10),
                "data": "0x12345678",
                "value_wei": "0",
                "gas_limit": "200000",
                "max_fee_per_gas": "100",
                "max_priority_fee_per_gas": "2",
                "gas_price": None,
                "chain_id": UNISWAP_CHAIN_ID,
                "native_input": False,
                "input_asset": "USDG",
                "output_asset": "INDEX",
                "input_amount": "0.5",
                "input_amount_atomic": exact,
                "provider_simulation_requested": False,
            },
        }
        handoff = validate_wallet_rejection_handoff(
            plan,
            wallet_address=SWAPPER,
            native_balance_wei="1000000000000000000",
            input_balance_atomic="3754869",
        )
        return handoff

    def test_wallet_successful_approval_handoff_promotes_only_exact_approval(self):
        handoff = self._wallet_approval_test_handoff()
        promoted = validate_wallet_successful_approval_handoff(handoff)
        self.assertEqual(promoted["action"], "approval")
        self.assertTrue(promoted["successful_broadcast_authorized"])
        self.assertTrue(promoted["approval_only"])
        self.assertFalse(promoted["reject_only"])
        self.assertFalse(promoted["swap_request_authorized"])
        self.assertFalse(promoted["automatic_retry"])
        self.assertFalse(promoted["automatic_second_transaction"])
        self.assertEqual(promoted["approval"]["approved_amount_atomic"], "500000")
        self.assertFalse(promoted["approval"]["unlimited_approval"])

    def test_wallet_successful_approval_handoff_rejects_swap_action(self):
        handoff = self._wallet_approval_test_handoff()
        handoff["action"] = "swap"
        with self.assertRaisesRegex(ValueError, "wallet_approval_exact_finite_approval_required"):
            validate_wallet_successful_approval_handoff(handoff)

    def test_wallet_approval_capability_requires_stable_vault_master_key(self):
        promoted = validate_wallet_successful_approval_handoff(self._wallet_approval_test_handoff())
        os.environ.pop("UTT_KMS_MASTER_KEY", None)
        with self.assertRaisesRegex(ValueError, "wallet_approval_capability_key_unavailable"):
            create_wallet_approval_capability(
                promoted,
                symbol="SPCX-USDG",
                side="buy",
                requested_amount="0.5",
            )

    def test_wallet_approval_capability_round_trip_tamper_and_expiry(self):
        promoted = validate_wallet_successful_approval_handoff(self._wallet_approval_test_handoff())
        record = create_wallet_approval_capability(
            promoted,
            symbol="INDEX-USDG",
            side="buy",
            requested_amount="0.5",
            ttl_seconds=120,
        )
        payload = decode_wallet_approval_capability(
            record["token"],
            now_epoch=record["payload"]["issued_at"] + 1,
        )
        self.assertEqual(payload["symbol"], "INDEX-USDG")
        self.assertEqual(payload["approved_amount_atomic"], "500000")
        self.assertEqual(payload["spender"].lower(), _address(9).lower())
        token = record["token"]
        tampered = ("A" if token[0] != "A" else "B") + token[1:]
        with self.assertRaisesRegex(ValueError, "wallet_approval_capability_invalid"):
            decode_wallet_approval_capability(tampered)
        with self.assertRaisesRegex(ValueError, "wallet_approval_capability_expired"):
            decode_wallet_approval_capability(
                token,
                now_epoch=record["payload"]["expires_at"],
            )

    def test_wallet_approval_transaction_matches_signed_capability(self):
        promoted = validate_wallet_successful_approval_handoff(self._wallet_approval_test_handoff())
        record = create_wallet_approval_capability(
            promoted,
            symbol="INDEX-USDG",
            side="buy",
            requested_amount="0.5",
        )
        payload = record["payload"]
        tx_hash = "0x" + ("ab" * 32)
        tx = {
            "hash": tx_hash,
            "from": SWAPPER,
            "to": USDG["contract_address"],
            "value": "0x0",
            "input": promoted["transaction"]["data"],
        }
        verified = validate_wallet_approval_transaction(payload, tx_hash=tx_hash, transaction=tx)
        self.assertEqual(verified["tx_hash"], tx_hash)
        self.assertEqual(verified["value_wei"], "0")
        tx["input"] = tx["input"][:-1] + ("0" if tx["input"][-1] != "0" else "1")
        with self.assertRaisesRegex(ValueError, "wallet_approval_transaction_calldata_mismatch"):
            validate_wallet_approval_transaction(payload, tx_hash=tx_hash, transaction=tx)

    def test_wallet_successful_swap_router_is_fresh_swap_only_and_uses_metamask_as_final_decision(self):
        root = Path(__file__).resolve().parents[2]
        router_text = (root / "backend" / "app" / "routers" / "robinhood_chain.py").read_text(encoding="utf-8")
        api_text = (root / "frontend" / "src" / "lib" / "api.js").read_text(encoding="utf-8")
        ticket_text = (root / "frontend" / "src" / "OrderTicketWidget.jsx").read_text(encoding="utf-8")
        self.assertIn('@router.post("/wallet-swap/prepare")', router_text)
        self.assertIn('@router.post("/wallet-swap/receipt")', router_text)
        self.assertNotIn('@router.post("/wallet-swap/approval")', router_text)
        self.assertIn('"fresh_post_approval_plan": True', router_text)
        self.assertIn('"provider_simulation_complete": True', router_text)
        self.assertIn('"approval_request_authorized": False', router_text)
        self.assertIn('"automatic_second_transaction": False', router_text)
        self.assertIn('prepareRobinhoodChainWalletSwap', api_text)
        self.assertIn('refreshRobinhoodChainWalletSwapReceipt', api_text)
        self.assertIn('FRESH POST-APPROVAL SWAP ONLY', ticket_text)
        self.assertIn('Open MetaMask — SWAP EXACT INPUT', ticket_text)
        self.assertIn('MetaMask is the final approve/reject decision', ticket_text)
        self.assertNotIn('I authorize only this exact finite approval transaction.', ticket_text)

    def _wallet_swap_test_handoff(self, *, exact: str = "500000"):
        plan = {
            "ok": True,
            "provider": "uniswap_api",
            "read_only": True,
            "execution_enabled": False,
            "signing_enabled": False,
            "broadcast_enabled": False,
            "input_registry_id": USDG["registry_id"],
            "output_registry_id": 47,
            "output_amount": "45.123",
            "output_amount_atomic": "45123000000000000000",
            "minimum_received": "44.897385",
            "minimum_received_atomic": "44897385000000000000",
            "approval_required": False,
            "requires_refresh_after_approval": False,
            "allowance": {
                "applicable": True,
                "approval_required": False,
                "approval_exact": False,
                "unlimited_approval": False,
                "required_amount_atomic": exact,
                "approval_transaction_plan": None,
            },
            "unsigned_transaction_plan": {
                "from": SWAPPER,
                "to": _address(10),
                "data": "0x12345678" + ("ab" * 32),
                "value_wei": "0",
                "gas_limit": "210000",
                "max_fee_per_gas": "100",
                "max_priority_fee_per_gas": "2",
                "gas_price": None,
                "chain_id": UNISWAP_CHAIN_ID,
                "native_input": False,
                "input_asset": "USDG",
                "output_asset": "INDEX",
                "input_amount": "0.5",
                "input_amount_atomic": exact,
                "minimum_received": "44.897385",
                "minimum_received_atomic": "44897385000000000000",
                "provider_simulation_requested": True,
            },
        }
        return validate_wallet_rejection_handoff(
            plan,
            wallet_address=SWAPPER,
            native_balance_wei="1000000000000000000",
            input_balance_atomic="3754869",
        )

    def test_wallet_successful_swap_handoff_promotes_only_fresh_simulated_swap(self):
        handoff = self._wallet_swap_test_handoff()
        promoted = validate_wallet_successful_swap_handoff(handoff)
        self.assertEqual(promoted["action"], "swap")
        self.assertTrue(promoted["successful_broadcast_authorized"])
        self.assertTrue(promoted["swap_only"])
        self.assertFalse(promoted["approval_request_authorized"])
        self.assertTrue(promoted["swap_request_authorized"])
        self.assertTrue(promoted["provider_simulation_requested"])
        self.assertFalse(promoted["requires_refresh_after_approval"])
        self.assertFalse(promoted["automatic_retry"])
        self.assertFalse(promoted["automatic_second_transaction"])

    def test_wallet_successful_swap_handoff_rejects_approval_or_unsimulated_plan(self):
        approval_handoff = self._wallet_approval_test_handoff()
        with self.assertRaisesRegex(ValueError, "wallet_swap_fresh_swap_required"):
            validate_wallet_successful_swap_handoff(approval_handoff)
        handoff = self._wallet_swap_test_handoff()
        handoff["provider_simulation_requested"] = False
        with self.assertRaisesRegex(ValueError, "wallet_swap_simulation_required"):
            validate_wallet_successful_swap_handoff(handoff)

    def test_wallet_swap_capability_requires_stable_vault_master_key(self):
        handoff = self._wallet_swap_test_handoff()
        os.environ.pop("UTT_KMS_MASTER_KEY", None)
        with self.assertRaisesRegex(ValueError, "wallet_swap_capability_key_unavailable"):
            create_wallet_swap_capability(
                handoff,
                symbol="INDEX-USDG",
                side="buy",
                requested_amount="0.5",
            )

    def test_wallet_swap_capability_round_trip_tamper_and_expiry(self):
        handoff = self._wallet_swap_test_handoff()
        record = create_wallet_swap_capability(
            handoff,
            symbol="INDEX-USDG",
            side="buy",
            requested_amount="0.5",
            ttl_seconds=120,
        )
        payload = decode_wallet_swap_capability(
            record["token"],
            now_epoch=record["payload"]["issued_at"] + 1,
        )
        self.assertEqual(payload["symbol"], "INDEX-USDG")
        self.assertEqual(payload["input_amount_atomic"], "500000")
        self.assertEqual(payload["minimum_received_atomic"], "44897385000000000000")
        token = record["token"]
        tampered = ("A" if token[0] != "A" else "B") + token[1:]
        with self.assertRaisesRegex(ValueError, "wallet_swap_capability_invalid"):
            decode_wallet_swap_capability(tampered)
        with self.assertRaisesRegex(ValueError, "wallet_swap_capability_expired"):
            decode_wallet_swap_capability(
                token,
                now_epoch=record["payload"]["expires_at"],
            )
        receipt_payload = decode_wallet_swap_capability(
            token,
            now_epoch=record["payload"]["expires_at"],
            allow_expired_for_bound_receipt=True,
        )
        self.assertEqual(receipt_payload["symbol"], "INDEX-USDG")
        self.assertEqual(receipt_payload["input_amount_atomic"], "500000")

    def test_wallet_swap_capability_preserves_optional_approval_hash(self):
        handoff = self._wallet_swap_test_handoff()
        approval_hash = "0x" + ("ef" * 32)
        record = create_wallet_swap_capability(
            handoff,
            symbol="INDEX-USDG",
            side="buy",
            requested_amount="0.5",
            approval_tx_hash=approval_hash,
        )
        payload = decode_wallet_swap_capability(
            record["token"],
            now_epoch=record["payload"]["issued_at"] + 1,
        )
        self.assertEqual(payload["approval_tx_hash"], approval_hash)

    def test_wallet_swap_capability_rejects_invalid_approval_hash(self):
        handoff = self._wallet_swap_test_handoff()
        with self.assertRaisesRegex(ValueError, "wallet_swap_approval_transaction_hash_invalid"):
            create_wallet_swap_capability(
                handoff,
                symbol="INDEX-USDG",
                side="buy",
                requested_amount="0.5",
                approval_tx_hash="0x1234",
            )

    def test_closeout_router_persists_confirmed_swap_and_refreshes_all_orders(self):
        root = Path(__file__).resolve().parents[2]
        router_text = (root / "backend" / "app" / "routers" / "robinhood_chain.py").read_text(encoding="utf-8")
        ticket_text = (root / "frontend" / "src" / "OrderTicketWidget.jsx").read_text(encoding="utf-8")
        self.assertIn('@router.post("/wallet-swap/reconcile-confirmed")', router_text)
        self.assertIn("_persist_generic_wallet_swap_reconciliation", router_text)
        self.assertIn('source="wallet_swap_receipt"', router_text)
        self.assertIn("approval_tx_hash=request.approval_tx_hash", router_text)
        self.assertIn("if (data?.order_mutation === true) requestAllOrdersRefresh();", ticket_text)

    def test_wallet_swap_transaction_matches_signed_capability(self):
        handoff = self._wallet_swap_test_handoff()
        record = create_wallet_swap_capability(
            handoff,
            symbol="INDEX-USDG",
            side="buy",
            requested_amount="0.5",
        )
        payload = record["payload"]
        tx_hash = "0x" + ("cd" * 32)
        tx = {
            "hash": tx_hash,
            "from": SWAPPER,
            "to": handoff["transaction"]["to"],
            "value": "0x0",
            "input": handoff["transaction"]["data"],
        }
        verified = validate_wallet_swap_transaction(payload, tx_hash=tx_hash, transaction=tx)
        self.assertEqual(verified["tx_hash"], tx_hash)
        self.assertEqual(verified["value_wei"], "0")
        tx["input"] = tx["input"][:-1] + ("0" if tx["input"][-1] != "0" else "1")
        with self.assertRaisesRegex(ValueError, "wallet_swap_transaction_calldata_mismatch"):
            validate_wallet_swap_transaction(payload, tx_hash=tx_hash, transaction=tx)

    def test_router_and_frontend_route_selected_provider_without_token_allowlist(self):
        root = Path(__file__).resolve().parents[2]
        router_text = (root / "backend" / "app" / "routers" / "robinhood_chain.py").read_text(encoding="utf-8")
        registry_text = (root / "backend" / "app" / "services" / "robinhood_chain_registry_discovery.py").read_text(encoding="utf-8")
        ticket_text = (root / "frontend" / "src" / "OrderTicketWidget.jsx").read_text(encoding="utf-8")
        api_text = (root / "frontend" / "src" / "lib" / "api.js").read_text(encoding="utf-8")
        self.assertIn("preferred_orderbook_provider", registry_text)
        self.assertIn("same_provider_both_exact_input_directions_not_available", registry_text)
        self.assertIn("providers = (PROVIDER_ZEROX, PROVIDER_UNISWAP, PROVIDER_UNISWAP_V3_RPC)", registry_text)
        self.assertIn("for provider in providers", registry_text)
        self.assertIn("preferred_provider == UNISWAP_PROVIDER", router_text)
        self.assertIn("preferred_provider == UNISWAP_V3_RPC_PROVIDER", router_text)
        self.assertIn("get_robinhood_chain_uniswap_v3_quote_service().quote_for_pair", router_text)
        self.assertIn("get_robinhood_chain_uniswap_quote_service().firm_quote_plan", router_text)
        self.assertIn("robinhoodChainSelectedProvider", ticket_text)
        self.assertIn("rows.find(liveExecution)", ticket_text)
        self.assertIn('provider: String(payload?.provider || "0x")', api_text)
        for forbidden in ("SPCX", "STONKBROKER"):
            self.assertNotIn(f'== "{forbidden}"', router_text)
            self.assertNotIn(f'== "{forbidden}"', registry_text)

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
