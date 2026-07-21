from __future__ import annotations

import ast
import inspect
import json
import unittest
from decimal import Decimal
from pathlib import Path
import sys


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services import robinhood_chain_execution_discovery as discovery_module  # noqa: E402
from app.services.robinhood_chain_quotes import (  # noqa: E402
    ROBINHOOD_CHAIN_ASK_INPUT_AMOUNTS,
    ROBINHOOD_CHAIN_BID_INPUT_AMOUNTS,
    RobinhoodChainQuoteService,
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
WETH = {
    "symbol": "WETH",
    "contract_address": "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73",
    "decimals": 18,
    "native": False,
    "registry_id": 44,
}
TAKER = "0x70c1ddd03bc4cb74efac3f12a41465d028ae490c"


def decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


class FakeDiscoveryService:
    def __init__(self, *, fail_inputs: set[str] | None = None) -> None:
        self.calls: list[dict] = []
        self.fail_inputs = set(fail_inputs or set())

    def status(self) -> dict:
        return {
            "provider_configured": True,
            "api_key_configured": True,
            "credential_source": "profile_vault",
            "cache_ttl_s": 15.0,
            "error_backoff_s": 120.0,
            "discovery_max_sell_usd": 5.0,
        }

    async def probe(
        self,
        *,
        sell_token: dict,
        buy_token: dict,
        sell_amount: str | None,
        buy_amount: str | None,
        taker_address: str,
        force_refresh: bool,
        route_capability: dict | None = None,
        require_live_verified: bool = True,
        max_probe_amount: str | None = None,
    ) -> dict:
        amount_mode = "exact_output" if buy_amount is not None else "exact_input"
        amount_text = str(buy_amount if buy_amount is not None else sell_amount or "")
        self.calls.append(
            {
                "sell": sell_token["symbol"],
                "buy": buy_token["symbol"],
                "amount": amount_text,
                "amount_mode": amount_mode,
                "taker": taker_address,
                "force_refresh": force_refresh,
            }
        )
        if amount_text in self.fail_inputs:
            return {
                "ok": False,
                "provider": "0x",
                "error": "fake_level_failure",
                "http_status": 502,
            }

        amount = Decimal(amount_text)
        sell_symbol = sell_token["symbol"]
        buy_symbol = buy_token["symbol"]
        if sell_symbol in {"ETH", "WETH"} and buy_symbol == "USDG":
            price = Decimal("1800") - amount * Decimal("100")
            sell_value = amount
            buy_value = amount * price
            fee_token = USDG["contract_address"].lower()
            fee_atomic = "275"
        elif amount_mode == "exact_output":
            price = Decimal("1850")
            sell_value = amount * price
            buy_value = amount
            fee_token = buy_token["contract_address"].lower()
            fee_atomic = "100000000000"
        elif sell_symbol == "USDG" and buy_symbol in {"ETH", "WETH"}:
            price = Decimal("1820") + amount * Decimal("0.1")
            sell_value = amount
            buy_value = amount / price
            fee_token = buy_token["contract_address"].lower()
            fee_atomic = "100000000000"
        else:
            price = Decimal("2")
            sell_value = amount
            buy_value = amount / price
            fee_token = buy_token["contract_address"].lower()
            fee_atomic = "1"

        min_output = buy_value * Decimal("0.99")
        return {
            "ok": True,
            "provider": "0x",
            "sell_amount": decimal_text(sell_value),
            "buy_amount": decimal_text(buy_value),
            "min_buy_amount": decimal_text(min_output),
            "liquidity_available": True,
            "block_number": "12345678",
            "gas": "280000",
            "gas_price": "80000000",
            "total_network_fee": "22400000000000",
            "fees": {
                "zeroExFee": {
                    "amount": fee_atomic,
                    "token": fee_token,
                    "type": "volume",
                }
            },
            "allowance_required": True,
            "allowance_spender": "0x0000000000001ff3684f28c67538d4d072c22734",
            "provider_warnings": ["allowance_required"],
            "route": {
                "fill_count": 1,
                "fills": [
                    {
                        "source": "Uniswap_V3",
                        "proportion_bps": "10000",
                    }
                ],
            },
            "cached": False,
            "elapsed_ms": 12.3,
            "fetched_at": "2026-07-17T20:48:39+00:00",
            # Deliberately include executable-looking provider fields. The quote
            # service must never copy these through to a 10B response.
            "transaction_destination": "0x1111111111111111111111111111111111111111",
            "transaction_calldata": "0xdeadbeef",
        }


class RobinhoodChainQuoteServiceTests(unittest.IsolatedAsyncioTestCase):
    def test_router_indicative_quote_keywords_match_service_signature(self) -> None:
        router_path = BACKEND_ROOT / "app" / "routers" / "robinhood_chain.py"
        tree = ast.parse(router_path.read_text(encoding="utf-8"), filename=str(router_path))
        endpoint = next(
            node
            for node in tree.body
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name == "robinhood_chain_indicative_quote"
        )
        call = next(
            node
            for node in ast.walk(endpoint)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "indicative_quote"
        )
        endpoint_keywords = {keyword.arg for keyword in call.keywords if keyword.arg}
        service_keywords = set(
            inspect.signature(RobinhoodChainQuoteService.indicative_quote).parameters
        ) - {"self"}

        self.assertTrue(endpoint_keywords <= service_keywords)
        self.assertNotIn("exact_output_quantity", endpoint_keywords)
        self.assertNotIn("maximum_total_quote", endpoint_keywords)

    def make_service(self, *, fail_inputs: set[str] | None = None):
        discovery = FakeDiscoveryService(fail_inputs=fail_inputs)
        return RobinhoodChainQuoteService(discovery_service=discovery), discovery

    async def test_sell_quote_is_safe_exact_input(self) -> None:
        service, discovery = self.make_service()
        quote = await service.indicative_quote(
            symbol="ETH-USDG",
            side="sell",
            quantity="0.0001",
            total_quote=None,
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
            force_refresh=True,
        )

        self.assertTrue(quote["ok"])
        self.assertEqual(quote["input_asset"], "ETH")
        self.assertEqual(quote["output_asset"], "USDG")
        self.assertEqual(quote["amount_mode"], "exact_input")
        self.assertEqual(quote["route_source"], "Uniswap_V3")
        self.assertEqual(quote["zero_x_fee"]["asset"], "USDG")
        self.assertFalse(quote["allowance_required"])
        self.assertIsNone(quote["allowance_spender"])
        self.assertNotIn("allowance_required", quote["provider_warnings"])
        self.assertEqual(quote["transaction_calldata"], None)
        self.assertEqual(quote["transaction_destination"], None)
        self.assertFalse(quote["transaction_data_present"])
        self.assertTrue(quote["read_only"])
        self.assertTrue(quote["quote_only"])
        self.assertFalse(quote["execution_enabled"])
        self.assertFalse(quote["signing_enabled"])
        self.assertFalse(quote["transaction_construction_enabled"])
        self.assertFalse(quote["will_mutate"])
        self.assertGreaterEqual(len(discovery.calls), 1)

    async def test_buy_quote_maps_total_usdg_to_eth(self) -> None:
        service, _ = self.make_service()
        quote = await service.indicative_quote(
            symbol="eth/usdg",
            side="buy",
            quantity=None,
            total_quote="1.25",
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
            force_refresh=False,
        )

        self.assertTrue(quote["ok"])
        self.assertEqual(quote["input_asset"], "USDG")
        self.assertEqual(quote["input_amount"], "1.25")
        self.assertEqual(quote["output_asset"], "ETH")
        self.assertEqual(quote["minimum_received_asset"], "ETH")
        self.assertEqual(quote["zero_x_fee"]["asset"], "ETH")
        self.assertTrue(quote["allowance_required"])
        self.assertIsNotNone(quote["allowance_spender"])
        self.assertIsNotNone(quote["price_impact_bps"])

    async def test_exact_output_buy_is_blocked_before_provider_contact(self) -> None:
        service, discovery = self.make_service()
        quote = await service.indicative_quote(
            symbol="ETH-USDG",
            side="buy",
            quantity="0.001",
            total_quote=None,
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
            force_refresh=True,
        )

        self.assertFalse(quote["ok"])
        self.assertEqual(quote["error"], "robinhood_chain_exact_receive_route_unavailable")
        self.assertEqual(quote["amount_mode"], "exact_output")
        self.assertEqual(quote["requested_output"], "0.001")
        self.assertEqual(quote["maximum_input_ceiling"], "2")
        self.assertEqual(quote["maximum_input_ceiling_atomic"], "2000000")
        self.assertFalse(quote["provider_contacted"])
        self.assertFalse(quote["backoff_activated"])
        self.assertEqual(quote["route_capability"]["indicative_status"], "provider_failure")
        self.assertEqual(discovery.calls, [])

    async def test_database_capability_weth_usdg_synthetic_orderbook(self) -> None:
        service, discovery = self.make_service()
        base_to_quote = {
            "symbol": "WETH-USDG",
            "mechanism": "swap",
            "from_asset": "WETH",
            "to_asset": "USDG",
            "amount_mode": "exact_input",
            "indicative_status": "available",
            "execution_status": "disabled",
            "enabled": False,
            "probe_amount": "0.0005",
        }
        quote_to_base = {
            "symbol": "WETH-USDG",
            "mechanism": "swap",
            "from_asset": "USDG",
            "to_asset": "WETH",
            "amount_mode": "exact_input",
            "indicative_status": "available",
            "execution_status": "disabled",
            "enabled": False,
            "probe_amount": "1",
        }

        book = await service.synthetic_orderbook_for_pair(
            symbol="WETH-USDG",
            depth=3,
            taker_address=TAKER,
            base_token=WETH,
            quote_token={**USDG, "registry_id": 45},
            base_to_quote_capability=base_to_quote,
            quote_to_base_capability=quote_to_base,
            force_refresh=True,
        )

        self.assertTrue(book["ok"])
        self.assertEqual(book["tranche"], "RH-CHAIN.10D.2-R5C.2")
        self.assertEqual(book["symbol"], "WETH-USDG")
        self.assertEqual(book["base_asset"], "WETH")
        self.assertEqual(book["quote_asset"], "USDG")
        self.assertEqual(book["identity_source"], "token_registry")
        self.assertEqual(book["capability_source"], "database")
        self.assertEqual(len(book["bids"]), 3)
        self.assertEqual(len(book["asks"]), 3)
        self.assertTrue(all(row["synthetic"] for row in [*book["bids"], *book["asks"]]))
        self.assertTrue(all(row["quote_only"] for row in [*book["bids"], *book["asks"]]))
        self.assertIsNone(book["transaction_calldata"])
        self.assertFalse(book["execution_enabled"])
        self.assertEqual(len(discovery.calls), 6)

    async def test_generic_orderbook_blocks_provider_error_before_probe(self) -> None:
        service, discovery = self.make_service()
        blocked = {
            "symbol": "SPCX-USDG",
            "mechanism": "swap",
            "from_asset": "SPCX",
            "to_asset": "USDG",
            "amount_mode": "exact_input",
            "indicative_status": "provider_error",
            "execution_status": "disabled",
            "enabled": False,
            "probe_amount": "0.01",
        }
        available = {
            "symbol": "SPCX-USDG",
            "mechanism": "swap",
            "from_asset": "USDG",
            "to_asset": "SPCX",
            "amount_mode": "exact_input",
            "indicative_status": "available",
            "execution_status": "disabled",
            "enabled": False,
            "probe_amount": "1",
        }
        spcx = {
            "symbol": "SPCX",
            "contract_address": "0x" + "4a" * 20,
            "decimals": 18,
            "native": False,
            "registry_id": 47,
        }

        book = await service.synthetic_orderbook_for_pair(
            symbol="SPCX-USDG",
            depth=3,
            taker_address=TAKER,
            base_token=spcx,
            quote_token={**USDG, "registry_id": 45},
            base_to_quote_capability=blocked,
            quote_to_base_capability=available,
            force_refresh=True,
        )

        self.assertFalse(book["ok"])
        self.assertEqual(book["error"], "robinhood_chain_bid_direction_unavailable")
        self.assertFalse(book["provider_contacted"])
        self.assertEqual(discovery.calls, [])

    def test_route_capabilities_are_not_embedded_in_provider_service(self) -> None:
        source = inspect.getsource(discovery_module)

        self.assertNotIn("ROBINHOOD_CHAIN_ROUTE_CAPABILITIES", source)
        self.assertNotIn("ROBINHOOD_CHAIN_DISCOVERY_TOKENS", source)
        self.assertIn("route_capabilities", source)
        self.assertIn("token_contracts_hardcoded", source)
        self.assertIn("pair_capabilities_hardcoded", source)

    async def test_unsupported_symbol_fails_closed_without_provider_call(self) -> None:
        service, discovery = self.make_service()
        quote = await service.indicative_quote(
            symbol="WETH-USDG",
            side="sell",
            quantity="0.0001",
            total_quote=None,
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
        )

        self.assertFalse(quote["ok"])
        self.assertEqual(quote["error"], "unsupported_robinhood_chain_quote_symbol")
        self.assertEqual(discovery.calls, [])
        self.assertIsNone(quote["transaction_calldata"])

    async def test_synthetic_orderbook_labels_sorting_and_cache(self) -> None:
        service, discovery = self.make_service()
        book = await service.synthetic_orderbook(
            symbol="ETH-USDG",
            depth=3,
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
            force_refresh=True,
        )

        self.assertTrue(book["ok"])
        self.assertTrue(book["synthetic"])
        self.assertTrue(book["quote_only"])
        self.assertFalse(book["resting_order"])
        self.assertFalse(book["execution_enabled"])
        self.assertFalse(book["signing_enabled"])
        self.assertFalse(book["transaction_construction_enabled"])
        self.assertIsNone(book["transaction_calldata"])
        self.assertEqual(len(book["bids"]), 3)
        self.assertEqual(len(book["asks"]), 3)
        self.assertEqual(len(discovery.calls), 6)

        bid_prices = [Decimal(row["price"]) for row in book["bids"]]
        ask_prices = [Decimal(row["price"]) for row in book["asks"]]
        self.assertEqual(bid_prices, sorted(bid_prices, reverse=True))
        self.assertEqual(ask_prices, sorted(ask_prices))
        for row in [*book["bids"], *book["asks"]]:
            self.assertTrue(row["synthetic"])
            self.assertTrue(row["quote_only"])
            self.assertFalse(row["resting_order"])
            self.assertEqual(row["liquidity_label"], "SYNTH")
            self.assertEqual(row["source_type"], "robinhood_chain_0x_indicative")

        provider_call_count = len(discovery.calls)
        cached = await service.synthetic_orderbook(
            symbol="ETH-USDG",
            depth=3,
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
            force_refresh=False,
        )
        self.assertTrue(cached["ok"])
        self.assertTrue(cached["cached"])
        self.assertEqual(cached["snapshot_source"], "synthetic_book_cache")
        self.assertTrue(all(row["cached"] for row in [*cached["bids"], *cached["asks"]]))
        self.assertEqual(len(discovery.calls), provider_call_count)

    async def test_one_level_failure_does_not_erase_other_levels(self) -> None:
        service, _ = self.make_service(fail_inputs={ROBINHOOD_CHAIN_BID_INPUT_AMOUNTS[1]})
        book = await service.synthetic_orderbook(
            symbol="ETH-USDG",
            depth=3,
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
            force_refresh=True,
        )

        self.assertTrue(book["ok"])
        self.assertEqual(len(book["bids"]), 2)
        self.assertEqual(len(book["asks"]), 3)
        self.assertEqual(book["warning_count"], 1)
        self.assertEqual(book["errors"][0]["side"], "bid")

    async def test_book_depth_is_bounded_to_five(self) -> None:
        service, discovery = self.make_service()
        book = await service.synthetic_orderbook(
            symbol="ETH-USDG",
            depth=200,
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
            force_refresh=True,
        )
        self.assertEqual(len(book["bids"]), len(ROBINHOOD_CHAIN_BID_INPUT_AMOUNTS))
        self.assertEqual(len(book["asks"]), len(ROBINHOOD_CHAIN_ASK_INPUT_AMOUNTS))
        self.assertEqual(len(discovery.calls), 10)

    async def test_safe_json_contains_no_provider_calldata(self) -> None:
        service, _ = self.make_service()
        quote = await service.indicative_quote(
            symbol="ETH-USDG",
            side="sell",
            quantity="0.0001",
            total_quote=None,
            taker_address=TAKER,
            eth_token=ETH,
            usdg_token=USDG,
        )
        encoded = json.dumps(quote, sort_keys=True)
        self.assertNotIn("deadbeef", encoded)
        self.assertNotIn("1111111111111111111111111111111111111111", encoded)
        self.assertIn('"transaction_calldata": null', encoded)


if __name__ == "__main__":
    unittest.main(verbosity=2)
