from __future__ import annotations

import json
import unittest
from decimal import Decimal
from pathlib import Path
import sys


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

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
    ) -> dict:
        assert buy_amount is None
        amount_text = str(sell_amount or "")
        self.calls.append(
            {
                "sell": sell_token["symbol"],
                "buy": buy_token["symbol"],
                "amount": amount_text,
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
        if sell_token["symbol"] == "ETH":
            price = Decimal("1800") - amount * Decimal("100")
            output = amount * price
            fee_token = USDG["contract_address"].lower()
            fee_atomic = "275"
        else:
            price = Decimal("1820") + amount * Decimal("0.1")
            output = amount / price
            fee_token = ETH["contract_address"].lower()
            fee_atomic = "100000000000"

        min_output = output * Decimal("0.99")
        return {
            "ok": True,
            "provider": "0x",
            "sell_amount": decimal_text(amount),
            "buy_amount": decimal_text(output),
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
