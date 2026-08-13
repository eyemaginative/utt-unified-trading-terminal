from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app.routers import wallet_addresses as wallet_router


TOKEN_A = "0x1111111111111111111111111111111111111111"
TOKEN_B = "0x2222222222222222222222222222222222222222"
TOKEN_C = "0x3333333333333333333333333333333333333333"
TOKEN_D = "0x5555555555555555555555555555555555555555"
TOKEN_E = "0x6666666666666666666666666666666666666666"
WALLET = "0x4444444444444444444444444444444444444444"


def _meta(
    *,
    registry_id: str,
    address: str,
    decimals: int,
    source: str | None = None,
    price_id: str | None = None,
):
    return {
        "registry_id": registry_id,
        "registry_venue": "robinhood_chain",
        "contract_address": address,
        "decimals": decimals,
        "external_price_source": source,
        "external_price_id": price_id,
    }


class RobinhoodChainBalancePricingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        pending = [
            task
            for task in wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_INFLIGHT.values()
            if not task.done()
        ]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_INFLIGHT.clear()
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_CACHE.clear()
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_BACKOFF_UNTIL.clear()
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_REFRESH_LOCK = asyncio.Lock()

    async def asyncTearDown(self):
        pending = [
            task
            for task in wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_INFLIGHT.values()
            if not task.done()
        ]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_INFLIGHT.clear()

    def test_probe_amount_is_positive_precise_and_never_exceeds_balance(self):
        self.assertEqual(
            wallet_router._robinhood_chain_quote_price_probe_amount(0.007366945714746708, 18),
            "0.007366945714746708",
        )
        self.assertEqual(
            wallet_router._robinhood_chain_quote_price_probe_amount(4.25, 18),
            "1",
        )
        self.assertEqual(
            wallet_router._robinhood_chain_quote_price_probe_amount(0.0000019, 6),
            "0.000001",
        )
        self.assertIsNone(
            wallet_router._robinhood_chain_quote_price_probe_amount(0.0000001, 6)
        )
        self.assertIsNone(
            wallet_router._robinhood_chain_quote_price_probe_amount(0, 18)
        )

    async def test_uniswap_quote_fallback_uses_registry_identities_and_cache(self):
        metadata = {
            "AAPL": _meta(
                registry_id="1",
                address=TOKEN_A,
                decimals=18,
                source="coingecko",
            ),
            "USDG": _meta(
                registry_id="2",
                address=TOKEN_B,
                decimals=6,
                source="stable",
                price_id="stable",
            ),
        }
        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(
            return_value={
                "ok": True,
                "price_quote_per_base": "212.345",
            }
        )

        with patch.object(
            wallet_router,
            "get_robinhood_chain_uniswap_quote_service",
            return_value=fake_service,
        ):
            first, first_stale = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata,
                {"AAPL": 0.007366945714746708},
                WALLET,
            )
            second, second_stale = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata,
                {"AAPL": 0.007366945714746708},
                WALLET,
            )

        self.assertEqual(first, {"AAPL": 212.345})
        self.assertEqual(second, {"AAPL": 212.345})
        self.assertEqual(first_stale, set())
        self.assertEqual(second_stale, set())
        self.assertEqual(fake_service.quote.await_count, 1)

        kwargs = fake_service.quote.await_args.kwargs
        self.assertEqual(kwargs["symbol"], "AAPL-USDG")
        self.assertEqual(kwargs["side"], "sell")
        self.assertEqual(kwargs["amount_mode"], "exact_input")
        self.assertEqual(kwargs["requested_amount"], "0.007366945714746708")
        self.assertEqual(kwargs["swapper_address"], WALLET)
        self.assertEqual(kwargs["input_token"]["contract_address"], TOKEN_A)
        self.assertEqual(kwargs["output_token"]["contract_address"], TOKEN_B)
        self.assertTrue(kwargs["confirm_quote"])


    async def test_batch_timeout_keeps_late_quotes_inflight_without_duplicate_provider_work(self):
        metadata = {
            "AAPL": _meta(
                registry_id="1",
                address=TOKEN_A,
                decimals=18,
                source="coingecko",
            ),
            "TSLA": _meta(
                registry_id="2",
                address=TOKEN_C,
                decimals=18,
                source="coingecko",
            ),
            "UP": _meta(
                registry_id="3",
                address=TOKEN_D,
                decimals=18,
                source="coingecko",
            ),
            "USDG": _meta(
                registry_id="4",
                address=TOKEN_B,
                decimals=6,
                source="stable",
                price_id="stable",
            ),
        }

        provider_lock = asyncio.Lock()
        prices = {
            "AAPL-USDG": "212.0",
            "TSLA-USDG": "340.0",
            "UP-USDG": "0.305159",
        }

        async def slow_serial_quote(**kwargs):
            async with provider_lock:
                await asyncio.sleep(0.02)
                return {
                    "ok": True,
                    "price_quote_per_base": prices[kwargs["symbol"]],
                }

        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(side_effect=slow_serial_quote)

        with (
            patch.object(
                wallet_router,
                "get_robinhood_chain_uniswap_quote_service",
                return_value=fake_service,
            ),
            patch.object(
                wallet_router,
                "_ROBINHOOD_CHAIN_QUOTE_PRICE_BATCH_TIMEOUT_S",
                0.005,
            ),
        ):
            first, _ = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata,
                {"AAPL": 1.0, "TSLA": 1.0, "UP": 1.0},
                WALLET,
            )
            # A second poll while the same exact identities are still pending
            # must reuse those in-flight tasks instead of duplicating provider
            # requests.
            second, _ = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata,
                {"AAPL": 1.0, "TSLA": 1.0, "UP": 1.0},
                WALLET,
            )
            await asyncio.sleep(0.08)
            third, _ = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata,
                {"AAPL": 1.0, "TSLA": 1.0, "UP": 1.0},
                WALLET,
            )

        self.assertLess(len(first), 3)
        self.assertLess(len(second), 3)
        self.assertEqual(
            third,
            {"AAPL": 212.0, "TSLA": 340.0, "UP": 0.305159},
        )
        self.assertEqual(fake_service.quote.await_count, 3)
        self.assertEqual(wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_INFLIGHT, {})

    async def test_registry_prices_keep_explicit_sources_authoritative_then_quote_unresolved(self):
        metadata = {
            "AAPL": _meta(
                registry_id="1",
                address=TOKEN_A,
                decimals=18,
                source="coingecko",
            ),
            "USDG": _meta(
                registry_id="2",
                address=TOKEN_B,
                decimals=6,
                source="stable",
                price_id="stable",
            ),
            "WETH": _meta(
                registry_id="3",
                address=TOKEN_C,
                decimals=18,
                source="coingecko",
                price_id="ethereum",
            ),
            "NOPE": _meta(
                registry_id="4",
                address="0x5555555555555555555555555555555555555555",
                decimals=18,
                source="none",
            ),
        }

        with (
            patch.object(
                wallet_router,
                "_robinhood_chain_registry_price_metadata",
                return_value=metadata,
            ),
            patch.object(
                wallet_router,
                "_robinhood_chain_coingecko_prices",
                return_value={"ethereum": 1900.0},
            ),
            patch.object(
                wallet_router,
                "_robinhood_chain_saved_quote_wallet",
                return_value=WALLET,
            ),
            patch.object(
                wallet_router,
                "_robinhood_chain_uniswap_quote_prices",
                new=AsyncMock(return_value=({"AAPL": 212.0}, set())),
            ) as quote_prices,
        ):
            prices, mapped, sources = await wallet_router._robinhood_chain_registry_prices(
                object(),
                {
                    "AAPL": 0.01,
                    "USDG": 2.0,
                    "WETH": 0.001,
                    "NOPE": 1.0,
                },
            )

        self.assertEqual(prices["USDG"], 1.0)
        self.assertEqual(prices["WETH"], 1900.0)
        self.assertEqual(prices["AAPL"], 212.0)
        self.assertNotIn("NOPE", prices)
        self.assertEqual(mapped, {"AAPL", "USDG", "WETH", "NOPE"})
        self.assertEqual(sources["USDG"], "Token Registry · stable USD")
        self.assertEqual(sources["WETH"], "Token Registry · CoinGecko ethereum")
        self.assertEqual(sources["AAPL"], "RH Chain quote · AAPL-USDG")

        unresolved = quote_prices.await_args.args[1]
        self.assertEqual(unresolved, {"AAPL": 0.01})


if __name__ == "__main__":
    unittest.main()
