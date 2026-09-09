from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from sqlalchemy import create_engine

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
        warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
        if warmer is not None and not warmer.done():
            warmer.cancel()
            await asyncio.gather(warmer, return_exceptions=True)
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER = None
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
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_PENDING.clear()
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_RECOVERY_KEYS.clear()
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_RECOVERY_RETRY_PENDING.clear()
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_RECOVERY_RETRY_COUNT.clear()
        wallet_router._ROBINHOOD_CHAIN_EXACT_EXTERNAL_PRICE_CACHE.clear()
        wallet_router._ROBINHOOD_CHAIN_EXACT_EXTERNAL_PRICE_BACKOFF_UNTIL = 0.0
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_REFRESH_LOCK = asyncio.Lock()
        self._price_test_engine = create_engine("sqlite:///:memory:", future=True)
        self._engine_patch = patch.object(wallet_router, "engine", self._price_test_engine)
        self._engine_patch.start()
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_DB_READY = False

    async def asyncTearDown(self):
        warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
        if warmer is not None and not warmer.done():
            warmer.cancel()
            await asyncio.gather(warmer, return_exceptions=True)
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER = None
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
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_PENDING.clear()
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_RECOVERY_KEYS.clear()
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_RECOVERY_RETRY_PENDING.clear()
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_RECOVERY_RETRY_COUNT.clear()
        wallet_router._ROBINHOOD_CHAIN_EXACT_EXTERNAL_PRICE_CACHE.clear()
        wallet_router._ROBINHOOD_CHAIN_EXACT_EXTERNAL_PRICE_BACKOFF_UNTIL = 0.0
        self._engine_patch.stop()
        self._price_test_engine.dispose()
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_DB_READY = False

    def test_matched_price_policy_versions_cache_and_rejects_incoherent_marks(self):
        metadata = {
            "AAPL": _meta(registry_id="1", address=TOKEN_A, decimals=18, source="coingecko"),
            "USDG": _meta(registry_id="2", address=TOKEN_B, decimals=6, source="stable", price_id="stable"),
        }
        cache_key = wallet_router._robinhood_chain_quote_price_cache_key(
            "AAPL", metadata["AAPL"], metadata["USDG"]
        )
        self.assertTrue(cache_key.endswith("|matched_notional_v1"))
        self.assertEqual(wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_NOTIONAL_USDG, "1")
        self.assertEqual(
            wallet_router._robinhood_chain_quote_price_matched_mark(
                {"price_quote_per_base": "101"},
                {"price_quote_per_base": "99"},
            ),
            100.0,
        )
        self.assertIsNone(
            wallet_router._robinhood_chain_quote_price_matched_mark(
                {"price_quote_per_base": "99"},
                {"price_quote_per_base": "101"},
            )
        )
        self.assertIsNone(
            wallet_router._robinhood_chain_quote_price_matched_mark(
                {"price_quote_per_base": "3"},
                {"price_quote_per_base": "1"},
            )
        )

    async def test_uniswap_quote_fallback_is_nonblocking_then_reuses_cache(self):
        metadata = {
            "AAPL": _meta(registry_id="1", address=TOKEN_A, decimals=18, source="coingecko"),
            "USDG": _meta(registry_id="2", address=TOKEN_B, decimals=6, source="stable", price_id="stable"),
        }
        release = asyncio.Event()
        started = asyncio.Event()

        async def delayed_quote(**kwargs):
            if kwargs["side"] == "buy":
                started.set()
                await release.wait()
                return {
                    "ok": True,
                    "price_quote_per_base": "212.5",
                    "output_amount": "0.004705882352941176",
                }
            return {
                "ok": True,
                "price_quote_per_base": "211.5",
                "output_amount": "0.9952941176470588",
            }

        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(side_effect=delayed_quote)

        with patch.object(
            wallet_router,
            "get_robinhood_chain_uniswap_quote_service",
            return_value=fake_service,
        ):
            first, first_cached, first_status = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"AAPL": 0.007366945714746708}, WALLET
            )
            self.assertEqual(first, {})
            self.assertEqual(first_cached, set())
            self.assertEqual(first_status, {})

            second, _, _ = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"AAPL": 0.007366945714746708}, WALLET
            )
            self.assertEqual(second, {})
            await asyncio.wait_for(started.wait(), timeout=1.0)
            self.assertEqual(fake_service.quote.await_count, 1)

            release.set()
            warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
            await asyncio.wait_for(asyncio.shield(warmer), timeout=1.0)

            third, third_cached, third_status = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"AAPL": 0.007366945714746708}, WALLET
            )

        self.assertEqual(third, {"AAPL": 212.0})
        self.assertEqual(third_cached, set())
        self.assertEqual(third_status, {})
        self.assertEqual(fake_service.quote.await_count, 2)

        buy_kwargs = fake_service.quote.await_args_list[0].kwargs
        sell_kwargs = fake_service.quote.await_args_list[1].kwargs
        self.assertEqual(buy_kwargs["symbol"], "AAPL-USDG")
        self.assertEqual(buy_kwargs["side"], "buy")
        self.assertEqual(buy_kwargs["requested_amount"], "1")
        self.assertEqual(sell_kwargs["side"], "sell")
        self.assertEqual(sell_kwargs["requested_amount"], "0.004705882352941176")
        self.assertFalse(buy_kwargs["_retry_provider_errors"])
        self.assertEqual(buy_kwargs["_provider_priority"], "background")

    async def test_no_route_found_error_gets_short_same_leg_recovery(self):
        metadata = {
            "INDEX": _meta(registry_id="1", address=TOKEN_A, decimals=18, source="coingecko"),
            "USDG": _meta(registry_id="2", address=TOKEN_B, decimals=6, source="stable", price_id="stable"),
        }
        calls = []

        async def quote(**kwargs):
            calls.append(dict(kwargs))
            if len(calls) == 1:
                return {
                    "ok": False,
                    "error": "uniswap_quote_provider_error",
                    "http_status": 404,
                    "provider_error": {
                        "errorCode": "NoRouteFoundError",
                        "detail": "No route with sufficient liquidity was found for this pair.",
                    },
                }
            if kwargs["side"] == "buy":
                return {"ok": True, "price_quote_per_base": "0.051", "output_amount": "19.607843137254901960"}
            return {"ok": True, "price_quote_per_base": "0.049", "output_amount": "0.960784313725490196"}

        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(side_effect=quote)
        with (
            patch.object(
                wallet_router,
                "get_robinhood_chain_uniswap_quote_service",
                return_value=fake_service,
            ),
            patch.object(wallet_router.asyncio, "sleep", new=AsyncMock()) as sleep_mock,
        ):
            await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"INDEX": 1.0}, WALLET
            )
            warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
            await asyncio.wait_for(asyncio.shield(warmer), timeout=1.0)
            current, cached, status = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"INDEX": 1.0}, WALLET
            )

        self.assertEqual(current, {"INDEX": 0.05})
        self.assertEqual(cached, set())
        self.assertEqual(status, {})
        self.assertEqual(fake_service.quote.await_count, 3)
        self.assertEqual(calls[0]["side"], "buy")
        self.assertEqual(calls[1]["side"], "buy")
        self.assertEqual(calls[0]["requested_amount"], calls[1]["requested_amount"])
        self.assertEqual(calls[0]["input_token"], calls[1]["input_token"])
        self.assertEqual(calls[0]["output_token"], calls[1]["output_token"])
        self.assertFalse(calls[0]["_retry_provider_errors"])
        sleep_mock.assert_any_await(0.75)

    async def test_exhausted_no_route_found_error_uses_transient_recheck_not_six_hour_no_route(self):
        metadata = {
            "INDEX": _meta(registry_id="1", address=TOKEN_A, decimals=18, source="coingecko"),
            "USDG": _meta(registry_id="2", address=TOKEN_B, decimals=6, source="stable", price_id="stable"),
        }
        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(
            return_value={
                "ok": False,
                "error": "uniswap_quote_provider_error",
                "http_status": 404,
                "provider_error": {
                    "errorCode": "NoRouteFoundError",
                    "detail": "No route with sufficient liquidity was found for this pair.",
                },
            }
        )

        with (
            patch.object(
                wallet_router,
                "get_robinhood_chain_uniswap_quote_service",
                return_value=fake_service,
            ),
            patch.object(wallet_router.asyncio, "sleep", new=AsyncMock()) as sleep_mock,
            patch.object(wallet_router, "_ROBINHOOD_CHAIN_QUOTE_PRICE_ERROR_BACKOFF_S", 120.0),
            patch.object(wallet_router, "_ROBINHOOD_CHAIN_QUOTE_PRICE_NO_ROUTE_RECHECK_S", 21600.0),
            patch.object(wallet_router, "_ROBINHOOD_CHAIN_QUOTE_PRICE_RECOVERY_MAX_AUTORETRIES", 0),
        ):
            await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"INDEX": 1.0}, WALLET
            )
            warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
            await asyncio.wait_for(asyncio.shield(warmer), timeout=1.0)
            current, cached, status = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"INDEX": 1.0}, WALLET
            )

        self.assertEqual(current, {})
        self.assertEqual(cached, set())
        self.assertEqual(status, {"INDEX": "Unpriced · retrying"})
        self.assertEqual(fake_service.quote.await_count, 3)
        self.assertEqual(
            [call.args[0] for call in sleep_mock.await_args_list[:2]],
            [0.75, 1.5],
        )
        state = wallet_router._robinhood_chain_quote_price_persisted_state(
            "INDEX", metadata["INDEX"], metadata["USDG"]
        )
        self.assertEqual(state["status"], "transient")
        self.assertEqual(state["provider_error_class"], "transient")
        self.assertEqual(state["provider_http_status"], 404)
        self.assertGreater(float(state["retry_after"]), float(state["attempted_at"]))
        self.assertLessEqual(
            float(state["retry_after"]) - float(state["attempted_at"]),
            125.0,
        )

    async def test_incoherent_matched_sample_is_resampled_once_before_fail_closed(self):
        metadata = {
            "INDEX": _meta(registry_id="1", address=TOKEN_A, decimals=18, source="coingecko"),
            "USDG": _meta(registry_id="2", address=TOKEN_B, decimals=6, source="stable", price_id="stable"),
        }
        responses = [
            {"ok": True, "price_quote_per_base": "0.049", "output_amount": "20"},
            {"ok": True, "price_quote_per_base": "0.051", "output_amount": "1"},
            {"ok": True, "price_quote_per_base": "0.051", "output_amount": "19.607843137254901960"},
            {"ok": True, "price_quote_per_base": "0.049", "output_amount": "0.960784313725490196"},
        ]
        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(side_effect=responses)

        with (
            patch.object(
                wallet_router,
                "get_robinhood_chain_uniswap_quote_service",
                return_value=fake_service,
            ),
            patch.object(wallet_router.asyncio, "sleep", new=AsyncMock()) as sleep_mock,
        ):
            await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"INDEX": 1.0}, WALLET
            )
            warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
            await asyncio.wait_for(asyncio.shield(warmer), timeout=1.0)
            current, cached, status = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"INDEX": 1.0}, WALLET
            )

        self.assertEqual(current, {"INDEX": 0.05})
        self.assertEqual(cached, set())
        self.assertEqual(status, {})
        self.assertEqual(fake_service.quote.await_count, 4)
        sleep_mock.assert_any_await(0.75)

    async def test_persistent_incoherent_matched_samples_still_fail_closed(self):
        metadata = {
            "INDEX": _meta(registry_id="1", address=TOKEN_A, decimals=18, source="coingecko"),
            "USDG": _meta(registry_id="2", address=TOKEN_B, decimals=6, source="stable", price_id="stable"),
        }
        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(
            side_effect=[
                {"ok": True, "price_quote_per_base": "0.049", "output_amount": "20"},
                {"ok": True, "price_quote_per_base": "0.051", "output_amount": "1"},
                {"ok": True, "price_quote_per_base": "0.048", "output_amount": "20"},
                {"ok": True, "price_quote_per_base": "0.052", "output_amount": "1"},
            ]
        )

        with (
            patch.object(
                wallet_router,
                "get_robinhood_chain_uniswap_quote_service",
                return_value=fake_service,
            ),
            patch.object(wallet_router.asyncio, "sleep", new=AsyncMock()),
        ):
            await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"INDEX": 1.0}, WALLET
            )
            warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
            await asyncio.wait_for(asyncio.shield(warmer), timeout=1.0)
            current, cached, status = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"INDEX": 1.0}, WALLET
            )

        self.assertEqual(current, {})
        self.assertEqual(cached, set())
        self.assertEqual(status, {"INDEX": "Unpriced · incoherent USDG market"})
        self.assertEqual(fake_service.quote.await_count, 4)
        state = wallet_router._robinhood_chain_quote_price_persisted_state(
            "INDEX", metadata["INDEX"], metadata["USDG"]
        )
        self.assertEqual(state["status"], "incoherent")
        self.assertIsNone(state["usd_price"])

    async def test_background_warmer_is_sequential_and_does_not_duplicate_pending_work(self):
        metadata = {
            "AAPL": _meta(registry_id="1", address=TOKEN_A, decimals=18, source="coingecko"),
            "TSLA": _meta(registry_id="2", address=TOKEN_C, decimals=18, source="coingecko"),
            "UP": _meta(registry_id="3", address=TOKEN_D, decimals=18, source="coingecko"),
            "USDG": _meta(registry_id="4", address=TOKEN_B, decimals=6, source="stable", price_id="stable"),
        }
        active = 0
        max_active = 0
        prices = {"AAPL-USDG": "212.0", "TSLA-USDG": "340.0", "UP-USDG": "0.305159"}

        async def serial_quote(**kwargs):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            try:
                await asyncio.sleep(0.005)
                price = prices[kwargs["symbol"]]
                if kwargs["side"] == "buy":
                    return {"ok": True, "price_quote_per_base": price, "output_amount": "1"}
                return {"ok": True, "price_quote_per_base": price, "output_amount": "1"}
            finally:
                active -= 1

        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(side_effect=serial_quote)

        with patch.object(
            wallet_router,
            "get_robinhood_chain_uniswap_quote_service",
            return_value=fake_service,
        ):
            first, _, _ = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"AAPL": 1.0, "TSLA": 1.0, "UP": 1.0}, WALLET
            )
            second, _, _ = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"AAPL": 1.0, "TSLA": 1.0, "UP": 1.0}, WALLET
            )
            self.assertEqual(first, {})
            self.assertEqual(second, {})
            warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
            await asyncio.wait_for(asyncio.shield(warmer), timeout=1.0)
            final, _, _ = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"AAPL": 1.0, "TSLA": 1.0, "UP": 1.0}, WALLET
            )

        self.assertEqual(final, {"AAPL": 212.0, "TSLA": 340.0, "UP": 0.305159})
        self.assertEqual(fake_service.quote.await_count, 6)
        self.assertEqual(max_active, 1)
        self.assertEqual(wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_INFLIGHT, {})
        self.assertEqual(wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_PENDING, {})

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
                new=AsyncMock(return_value=({"AAPL": 212.0}, set(), {})),
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
        self.assertEqual(sources["AAPL"], "RH Chain matched quote · AAPL-USDG")

        unresolved = quote_prices.await_args.args[1]
        self.assertEqual(unresolved, {"AAPL": 0.01})

    async def test_registry_prices_surfaces_no_route_status_without_fabricated_price(self):
        metadata = {
            "AAPL": _meta(registry_id="1", address=TOKEN_A, decimals=18, source="coingecko"),
            "USDG": _meta(registry_id="2", address=TOKEN_B, decimals=6, source="stable", price_id="stable"),
        }
        with (
            patch.object(wallet_router, "_robinhood_chain_registry_price_metadata", return_value=metadata),
            patch.object(wallet_router, "_robinhood_chain_coingecko_prices", return_value={}),
            patch.object(wallet_router, "_robinhood_chain_saved_quote_wallet", return_value=WALLET),
            patch.object(
                wallet_router,
                "_robinhood_chain_uniswap_quote_prices",
                new=AsyncMock(return_value=({}, set(), {"AAPL": "Unpriced · no USDG route"})),
            ),
        ):
            prices, mapped, sources = await wallet_router._robinhood_chain_registry_prices(
                object(), {"AAPL": 1.0}
            )

        self.assertNotIn("AAPL", prices)
        self.assertEqual(mapped, {"AAPL"})
        self.assertEqual(sources["AAPL"], "Unpriced · no USDG route")


    def test_registered_row_selection_has_no_100_cap_and_preserves_exact_contract_identities(self):
        rows = [
            SimpleNamespace(
                id=index + 1,
                symbol=f"T{index:03d}",
                address=f"0x{index + 1:040x}",
                venue=None,
            )
            for index in range(124)
        ]
        selected = wallet_router._select_robinhood_chain_registered_erc20_rows([], rows)
        self.assertEqual(len(selected), 124)
        self.assertEqual(len({str(row.address).lower() for row in selected}), 124)

        duplicate_symbol_rows = [
            SimpleNamespace(id=1001, symbol="DUP", address=TOKEN_A, venue=None),
            SimpleNamespace(id=1002, symbol="DUP", address=TOKEN_B, venue=None),
        ]
        duplicate_selected = wallet_router._select_robinhood_chain_registered_erc20_rows(
            [],
            duplicate_symbol_rows,
        )
        self.assertEqual([row.id for row in duplicate_selected], [1001, 1002])

        override = SimpleNamespace(id=2001, symbol="DUP", address=TOKEN_C, venue="robinhood_chain")
        overridden = wallet_router._select_robinhood_chain_registered_erc20_rows(
            [override],
            duplicate_symbol_rows,
        )
        self.assertEqual([row.id for row in overridden], [2001])

    def test_snapshot_identity_uses_exact_registry_and_contract(self):
        snap = SimpleNamespace(
            asset="DUP",
            network="robinhood_chain",
            balance_raw={
                "registry_id": 77,
                "registry_venue": "robinhood_chain",
                "contract_address": "0x" + TOKEN_A[2:].upper(),
                "decimals": 18,
            },
        )
        addr = SimpleNamespace(asset="ALL", network="robinhood_chain", wallet_id="robinhood_chain")
        identity = wallet_router._robinhood_chain_snapshot_identity(snap, addr)
        self.assertEqual(identity["registry_id"], 77)
        self.assertEqual(identity["contract_address"], TOKEN_A)
        self.assertEqual(identity["token_decimals"], 18)
        self.assertEqual(identity["identity_key"], f"rh:77:{TOKEN_A}")

    def test_latest_snapshot_selection_suppresses_legacy_partition_when_exact_identity_exists(self):
        addr = SimpleNamespace(
            id="wallet-1",
            asset="ALL",
            network="robinhood_chain",
            wallet_id="robinhood_chain",
        )
        legacy = SimpleNamespace(
            asset="DUP",
            network="robinhood_chain",
            balance_raw={},
        )
        exact_a = SimpleNamespace(
            asset="DUP",
            network="robinhood_chain",
            balance_raw={
                "registry_id": 77,
                "registry_venue": "robinhood_chain",
                "contract_address": TOKEN_A,
                "decimals": 18,
            },
        )
        exact_b = SimpleNamespace(
            asset="DUP",
            network="robinhood_chain",
            balance_raw={
                "registry_id": 78,
                "registry_venue": "robinhood_chain",
                "contract_address": TOKEN_B,
                "decimals": 18,
            },
        )
        native = SimpleNamespace(
            asset="ETH",
            network="robinhood_chain",
            balance_raw={"source": "robinhood_chain_rpc"},
        )

        latest = wallet_router._select_latest_wallet_snapshot_rows(
            [(exact_a, addr), (exact_b, addr), (legacy, addr), (native, addr)]
        )
        self.assertEqual(len(latest), 3)
        self.assertIn(("wallet-1", f"rh:77:{TOKEN_A}"), latest)
        self.assertIn(("wallet-1", f"rh:78:{TOKEN_B}"), latest)
        self.assertIn(("wallet-1", "asset:ETH"), latest)
        self.assertNotIn(("wallet-1", "asset:DUP"), latest)

    async def test_persistent_last_good_survives_process_cache_clear(self):
        metadata = {
            "AAPL": _meta(registry_id="1", address=TOKEN_A, decimals=18, source="coingecko"),
            "USDG": _meta(registry_id="2", address=TOKEN_B, decimals=6, source="stable", price_id="stable"),
        }

        async def quote(**kwargs):
            if kwargs["side"] == "buy":
                return {"ok": True, "price_quote_per_base": "213", "output_amount": "1"}
            return {"ok": True, "price_quote_per_base": "212", "output_amount": "1"}

        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(side_effect=quote)

        with patch.object(
            wallet_router,
            "get_robinhood_chain_uniswap_quote_service",
            return_value=fake_service,
        ):
            await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"AAPL": 1.0}, WALLET
            )
            warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
            await asyncio.wait_for(asyncio.shield(warmer), timeout=1.0)

        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_CACHE.clear()
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_BACKOFF_UNTIL.clear()
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER = None

        restored, cached, status = await wallet_router._robinhood_chain_uniswap_quote_prices(
            metadata, {"AAPL": 1.0}, WALLET
        )
        self.assertEqual(restored, {"AAPL": 212.5})
        self.assertEqual(cached, {"AAPL"})
        self.assertEqual(status, {})
        self.assertEqual(fake_service.quote.await_count, 2)

    async def test_no_route_state_is_persisted_and_suppresses_immediate_requeue(self):
        metadata = {
            "AAPL": _meta(registry_id="1", address=TOKEN_A, decimals=18, source="coingecko"),
            "USDG": _meta(registry_id="2", address=TOKEN_B, decimals=6, source="stable", price_id="stable"),
        }
        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(
            return_value={
                "ok": False,
                "error": "uniswap_quote_provider_error",
                "http_status": 404,
                "provider_error": {
                    "errorCode": "ResourceNotFound",
                    "detail": "No quotes available",
                },
            }
        )

        with (
            patch.object(
                wallet_router,
                "get_robinhood_chain_uniswap_quote_service",
                return_value=fake_service,
            ),
            patch.object(wallet_router, "_ROBINHOOD_CHAIN_QUOTE_PRICE_NO_ROUTE_RECHECK_S", 3600.0),
        ):
            await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"AAPL": 1.0}, WALLET
            )
            warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
            await asyncio.wait_for(asyncio.shield(warmer), timeout=1.0)
            wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_CACHE.clear()
            wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_BACKOFF_UNTIL.clear()
            second, cached, status = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"AAPL": 1.0}, WALLET
            )

        self.assertEqual(second, {})
        self.assertEqual(cached, set())
        self.assertEqual(status, {"AAPL": "Unpriced · no USDG route"})
        self.assertEqual(fake_service.quote.await_count, 1)
        self.assertEqual(wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_PENDING, {})

    async def test_transient_refresh_preserves_bounded_last_good_for_portfolio(self):
        metadata = {
            "AAPL": _meta(registry_id="1", address=TOKEN_A, decimals=18, source="coingecko"),
            "USDG": _meta(registry_id="2", address=TOKEN_B, decimals=6, source="stable", price_id="stable"),
        }
        now = wallet_router.time.time()
        wallet_router._robinhood_chain_quote_price_write_state(
            "AAPL",
            metadata["AAPL"],
            metadata["USDG"],
            status="success",
            usd_price=210.0,
            price_source="RH Chain matched quote · AAPL-USDG",
            price_fetched_at=now,
            provider_error_class=None,
            provider_http_status=200,
            attempted_at=now,
            retry_after=now - 1,
        )
        cache_key = wallet_router._robinhood_chain_quote_price_cache_key(
            "AAPL", metadata["AAPL"], metadata["USDG"]
        )
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_CACHE[cache_key] = (
            wallet_router.time.monotonic() - wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_TTL_S - 1,
            210.0,
        )
        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(
            return_value={
                "ok": False,
                "error": "uniswap_quote_provider_error",
                "http_status": 404,
                "provider_error": {
                    "errorCode": "NoRouteFoundError",
                    "detail": "No route with sufficient liquidity was found for this pair.",
                },
            }
        )

        with (
            patch.object(
                wallet_router,
                "get_robinhood_chain_uniswap_quote_service",
                return_value=fake_service,
            ),
            patch.object(wallet_router.asyncio, "sleep", new=AsyncMock()),
        ):
            initial, initial_cached, _ = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"AAPL": 1.0}, WALLET
            )
            self.assertEqual(initial, {"AAPL": 210.0})
            self.assertEqual(initial_cached, {"AAPL"})
            warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
            await asyncio.wait_for(asyncio.shield(warmer), timeout=1.0)
            wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_CACHE.clear()
            restored, cached, status = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"AAPL": 1.0}, WALLET
            )

        self.assertEqual(restored, {"AAPL": 210.0})
        self.assertEqual(cached, {"AAPL"})
        self.assertEqual(
            status,
            {"AAPL": "RH Chain matched quote · AAPL-USDG · cached · retrying"},
        )
        self.assertEqual(fake_service.quote.await_count, 3)
        state = wallet_router._robinhood_chain_quote_price_persisted_state(
            "AAPL", metadata["AAPL"], metadata["USDG"]
        )
        self.assertEqual(state["usd_price"], 210.0)
        self.assertEqual(state["price_source"], "RH Chain matched quote · AAPL-USDG")
        self.assertEqual(state["price_fetched_at"], now)
        self.assertEqual(state["status"], "transient")

    async def test_expired_transient_last_good_is_not_portfolio_authority(self):
        metadata = {
            "AAPL": _meta(registry_id="1", address=TOKEN_A, decimals=18, source="coingecko"),
            "USDG": _meta(registry_id="2", address=TOKEN_B, decimals=6, source="stable", price_id="stable"),
        }
        now = wallet_router.time.time()
        wallet_router._robinhood_chain_quote_price_write_state(
            "AAPL",
            metadata["AAPL"],
            metadata["USDG"],
            status="transient",
            usd_price=210.0,
            price_source="RH Chain matched quote · AAPL-USDG",
            price_fetched_at=now - wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_STALE_MAX_S - 1,
            provider_error_class="transient",
            provider_http_status=404,
            attempted_at=now,
            retry_after=now + 120,
        )
        restored, cached, status = await wallet_router._robinhood_chain_uniswap_quote_prices(
            metadata, {"AAPL": 1.0}, WALLET
        )
        self.assertEqual(restored, {})
        self.assertEqual(cached, set())
        self.assertEqual(status, {"AAPL": "Unpriced · retrying"})

    async def test_matched_sell_failure_fails_closed_and_clears_prior_numeric_mark(self):
        metadata = {
            "AAPL": _meta(registry_id="1", address=TOKEN_A, decimals=18, source="coingecko"),
            "USDG": _meta(registry_id="2", address=TOKEN_B, decimals=6, source="stable", price_id="stable"),
        }
        now = wallet_router.time.time()
        wallet_router._robinhood_chain_quote_price_write_state(
            "AAPL", metadata["AAPL"], metadata["USDG"],
            status="success", usd_price=210.0,
            price_source="RH Chain matched quote · AAPL-USDG", price_fetched_at=now,
            provider_error_class=None, provider_http_status=200,
            attempted_at=now, retry_after=now - 1,
        )
        cache_key = wallet_router._robinhood_chain_quote_price_cache_key(
            "AAPL", metadata["AAPL"], metadata["USDG"]
        )
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_CACHE[cache_key] = (
            wallet_router.time.monotonic() - wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_TTL_S - 1,
            210.0,
        )

        async def quote(**kwargs):
            if kwargs["side"] == "buy":
                return {"ok": True, "price_quote_per_base": "212", "output_amount": "1"}
            return {
                "ok": False,
                "error": "uniswap_quote_provider_error",
                "http_status": 404,
                "provider_error": {"errorCode": "ResourceNotFound", "detail": "No quotes available"},
            }

        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(side_effect=quote)
        with patch.object(
            wallet_router, "get_robinhood_chain_uniswap_quote_service", return_value=fake_service
        ):
            await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"AAPL": 1.0}, WALLET
            )
            warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
            await asyncio.wait_for(asyncio.shield(warmer), timeout=1.0)
            current, cached, status = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"AAPL": 1.0}, WALLET
            )

        self.assertEqual(current, {})
        self.assertEqual(cached, set())
        self.assertEqual(status, {"AAPL": "Unpriced · no USDG route"})
        self.assertNotIn(cache_key, wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_CACHE)
        state = wallet_router._robinhood_chain_quote_price_persisted_state(
            "AAPL", metadata["AAPL"], metadata["USDG"]
        )
        self.assertIsNone(state["usd_price"])
        self.assertEqual(state["status"], "no_route")

    async def test_quote_background_warmer_uses_fifo_enqueue_order_not_cache_key_sort(self):
        metadata = {
            "USDG": _meta(
                registry_id="999", address=TOKEN_B, decimals=6, source="stable", price_id="stable"
            ),
            "A": _meta(
                registry_id="1", address="0xfffffffffffffffffffffffffffffffffffffff1", decimals=18, source="coingecko"
            ),
            "B": _meta(
                registry_id="2", address="0x0000000000000000000000000000000000000001", decimals=18, source="coingecko"
            ),
            "C": _meta(
                registry_id="3", address="0x8888888888888888888888888888888888888888", decimals=18, source="coingecko"
            ),
        }
        quantities = {"A": 1.0, "B": 1.0, "C": 1.0}
        buy_symbols = []

        async def quote(**kwargs):
            if kwargs["side"] == "buy":
                buy_symbols.append(kwargs["symbol"])
            return {"ok": True, "price_quote_per_base": "1.25", "output_amount": "1"}

        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(side_effect=quote)
        with patch.object(
            wallet_router,
            "get_robinhood_chain_uniswap_quote_service",
            return_value=fake_service,
        ):
            await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, quantities, WALLET
            )
            warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
            await asyncio.wait_for(asyncio.shield(warmer), timeout=1.0)

        self.assertEqual(buy_symbols, ["A-USDG", "B-USDG", "C-USDG"])

    async def test_quote_background_warmer_advances_all_candidates_sequentially(self):
        metadata = {
            "USDG": _meta(
                registry_id="999", address=TOKEN_B, decimals=6, source="stable", price_id="stable"
            ),
        }
        quantities = {}
        for index in range(5):
            symbol = f"T{index}"
            address = f"0x{index + 10:040x}"
            metadata[symbol] = _meta(
                registry_id=str(index + 1), address=address, decimals=18, source="coingecko"
            )
            quantities[symbol] = 1.0

        active = 0
        max_active = 0

        async def quote(**kwargs):
            nonlocal active, max_active
            active += 1
            max_active = max(max_active, active)
            try:
                await asyncio.sleep(0.002)
                return {"ok": True, "price_quote_per_base": "1.25", "output_amount": "1"}
            finally:
                active -= 1

        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(side_effect=quote)

        with patch.object(
            wallet_router,
            "get_robinhood_chain_uniswap_quote_service",
            return_value=fake_service,
        ):
            initial, _, _ = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, quantities, WALLET
            )
            self.assertEqual(initial, {})
            warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
            self.assertIsNotNone(warmer)
            await asyncio.wait_for(asyncio.shield(warmer), timeout=1.0)
            final, _, _ = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, quantities, WALLET
            )

        self.assertEqual(set(final), set(quantities))
        self.assertEqual(fake_service.quote.await_count, 10)
        self.assertEqual(max_active, 1)

    async def test_unpriced_due_identity_is_serviced_before_stale_valued_refresh(self):
        metadata = {
            "AAPL": _meta(
                registry_id="1", address=TOKEN_A, decimals=18, source="coingecko"
            ),
            "INDEX": _meta(
                registry_id="2", address=TOKEN_C, decimals=18, source="coingecko"
            ),
            "USDG": _meta(
                registry_id="3", address=TOKEN_B, decimals=6, source="stable", price_id="stable"
            ),
        }
        now = wallet_router.time.time()
        wallet_router._robinhood_chain_quote_price_write_state(
            "AAPL", metadata["AAPL"], metadata["USDG"],
            status="success", usd_price=210.0,
            price_source="RH Chain matched quote · AAPL-USDG",
            price_fetched_at=now - wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_TTL_S - 1,
            provider_error_class=None, provider_http_status=200,
            attempted_at=now - 600, retry_after=now - 1,
        )
        wallet_router._robinhood_chain_quote_price_write_state(
            "INDEX", metadata["INDEX"], metadata["USDG"],
            status="transient", usd_price=None, price_source=None,
            price_fetched_at=None, provider_error_class="transient",
            provider_http_status=404, attempted_at=now - 600, retry_after=now - 1,
        )

        buy_symbols = []

        async def quote(**kwargs):
            if kwargs["side"] == "buy":
                buy_symbols.append(kwargs["symbol"])
            return {"ok": True, "price_quote_per_base": "1", "output_amount": "1"}

        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(side_effect=quote)
        with patch.object(
            wallet_router,
            "get_robinhood_chain_uniswap_quote_service",
            return_value=fake_service,
        ):
            initial, cached, _ = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"AAPL": 1.0, "INDEX": 1.0}, WALLET
            )
            self.assertEqual(initial, {"AAPL": 210.0})
            self.assertEqual(cached, {"AAPL"})
            warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
            await asyncio.wait_for(asyncio.shield(warmer), timeout=1.0)

        self.assertEqual(buy_symbols[:2], ["INDEX-USDG", "AAPL-USDG"])

    async def test_transient_null_autoretry_recovers_without_second_balance_request(self):
        metadata = {
            "INDEX": _meta(registry_id="1", address=TOKEN_A, decimals=18, source="coingecko"),
            "USDG": _meta(registry_id="2", address=TOKEN_B, decimals=6, source="stable", price_id="stable"),
        }
        calls = []

        async def quote(**kwargs):
            calls.append(dict(kwargs))
            if len(calls) == 1:
                return {
                    "ok": False,
                    "error": "uniswap_quote_provider_error",
                    "http_status": 404,
                    "provider_error": {
                        "errorCode": "NoRouteFoundError",
                        "detail": "No route with sufficient liquidity was found for this pair.",
                    },
                }
            if kwargs["side"] == "buy":
                return {"ok": True, "price_quote_per_base": "0.051", "output_amount": "20"}
            return {"ok": True, "price_quote_per_base": "0.049", "output_amount": "0.98"}

        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(side_effect=quote)
        with (
            patch.object(
                wallet_router,
                "get_robinhood_chain_uniswap_quote_service",
                return_value=fake_service,
            ),
            patch.object(wallet_router, "_ROBINHOOD_CHAIN_QUOTE_PRICE_SHORT_RETRY_ATTEMPTS", 1),
            patch.object(wallet_router, "_ROBINHOOD_CHAIN_QUOTE_PRICE_ERROR_BACKOFF_S", 0.0),
            patch.object(wallet_router, "_ROBINHOOD_CHAIN_QUOTE_PRICE_RECOVERY_MAX_AUTORETRIES", 1),
            patch.object(wallet_router, "_ROBINHOOD_CHAIN_QUOTE_PRICE_RECOVERY_WAKE_POLL_S", 0.01),
        ):
            initial, _, _ = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"INDEX": 1.0}, WALLET
            )
            self.assertEqual(initial, {})
            warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
            await asyncio.wait_for(asyncio.shield(warmer), timeout=1.0)

        state = wallet_router._robinhood_chain_quote_price_persisted_state(
            "INDEX", metadata["INDEX"], metadata["USDG"]
        )
        self.assertEqual(state["status"], "success")
        self.assertAlmostEqual(float(state["usd_price"]), 0.05, places=12)
        self.assertEqual(fake_service.quote.await_count, 3)
        self.assertEqual(wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_RECOVERY_RETRY_PENDING, {})

    async def test_transient_null_autoretry_is_bounded(self):
        metadata = {
            "INDEX": _meta(registry_id="1", address=TOKEN_A, decimals=18, source="coingecko"),
            "USDG": _meta(registry_id="2", address=TOKEN_B, decimals=6, source="stable", price_id="stable"),
        }
        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(
            return_value={
                "ok": False,
                "error": "uniswap_quote_provider_error",
                "http_status": 404,
                "provider_error": {
                    "errorCode": "NoRouteFoundError",
                    "detail": "No route with sufficient liquidity was found for this pair.",
                },
            }
        )

        with (
            patch.object(
                wallet_router,
                "get_robinhood_chain_uniswap_quote_service",
                return_value=fake_service,
            ),
            patch.object(wallet_router, "_ROBINHOOD_CHAIN_QUOTE_PRICE_SHORT_RETRY_ATTEMPTS", 1),
            patch.object(wallet_router, "_ROBINHOOD_CHAIN_QUOTE_PRICE_ERROR_BACKOFF_S", 0.0),
            patch.object(wallet_router, "_ROBINHOOD_CHAIN_QUOTE_PRICE_RECOVERY_MAX_AUTORETRIES", 2),
            patch.object(wallet_router, "_ROBINHOOD_CHAIN_QUOTE_PRICE_RECOVERY_WAKE_POLL_S", 0.01),
        ):
            await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata, {"INDEX": 1.0}, WALLET
            )
            warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
            await asyncio.wait_for(asyncio.shield(warmer), timeout=1.0)

        self.assertEqual(fake_service.quote.await_count, 3)
        state = wallet_router._robinhood_chain_quote_price_persisted_state(
            "INDEX", metadata["INDEX"], metadata["USDG"]
        )
        self.assertEqual(state["status"], "transient")
        self.assertIsNone(state["usd_price"])
        self.assertEqual(wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_RECOVERY_RETRY_PENDING, {})
        self.assertNotIn(
            wallet_router._robinhood_chain_quote_price_cache_key(
                "INDEX", metadata["INDEX"], metadata["USDG"]
            ),
            wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_RECOVERY_KEYS,
        )



    def test_exact_external_prices_are_contract_keyed_batched_and_cached(self):
        contracts = [f"0x{index + 1:040x}" for index in range(30)]
        calls = []

        class FakeResponse:
            def __init__(self, batch):
                self.batch = batch

            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "data": {
                        "attributes": {
                            "token_prices": {
                                contract: str(index + 1)
                                for index, contract in enumerate(self.batch)
                            }
                        }
                    }
                }

        class FakeClient:
            def __init__(self, *args, **kwargs):
                pass

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def get(self, url, headers=None):
                batch = url.rsplit("/", 1)[-1].split(",")
                calls.append(batch)
                return FakeResponse(batch)

        with patch.object(wallet_router.httpx, "Client", FakeClient):
            first, first_cached = wallet_router._robinhood_chain_exact_external_prices(contracts)
            second, second_cached = wallet_router._robinhood_chain_exact_external_prices(contracts)

        self.assertEqual(len(first), 30)
        self.assertEqual(first, second)
        self.assertEqual(first_cached, set())
        self.assertEqual(second_cached, set())
        self.assertEqual([len(batch) for batch in calls], [25, 5])
        self.assertEqual(set(first), {contract.lower() for contract in contracts})

    async def test_registry_prices_prefer_exact_external_before_matched_quote(self):
        metadata = {
            "AAPL": _meta(registry_id="1", address=TOKEN_A, decimals=18, source="coingecko"),
            "TSLA": _meta(registry_id="2", address=TOKEN_C, decimals=18, source="coingecko"),
            "USDG": _meta(registry_id="3", address=TOKEN_B, decimals=6, source="stable", price_id="stable"),
        }

        with (
            patch.object(wallet_router, "_robinhood_chain_registry_price_metadata", return_value=metadata),
            patch.object(wallet_router, "_robinhood_chain_coingecko_prices", return_value={}),
            patch.object(wallet_router, "_robinhood_chain_saved_quote_wallet", return_value=WALLET),
            patch.object(
                wallet_router,
                "_robinhood_chain_uniswap_quote_prices",
                new=AsyncMock(return_value=({"TSLA": 340.0}, set(), {})),
            ) as quote_prices,
        ):
            prices, mapped, sources = await wallet_router._robinhood_chain_registry_prices(
                object(),
                {"AAPL": 1.0, "TSLA": 1.0},
                exact_external_prices={TOKEN_A.lower(): 212.0},
                exact_external_cached_contracts=set(),
            )

        self.assertEqual(mapped, {"AAPL", "TSLA"})
        self.assertEqual(prices, {"AAPL": 212.0, "TSLA": 340.0})
        self.assertEqual(
            sources["AAPL"],
            "CoinGecko Onchain (GeckoTerminal) · exact contract",
        )
        quote_prices.assert_awaited_once()
        unresolved = quote_prices.await_args.args[1]
        self.assertEqual(unresolved, {"TSLA": 1.0})

    async def test_registry_explicit_coingecko_id_remains_ahead_of_exact_external(self):
        metadata = {
            "WETH": _meta(
                registry_id="1", address=TOKEN_A, decimals=18,
                source="coingecko", price_id="ethereum",
            ),
            "USDG": _meta(registry_id="2", address=TOKEN_B, decimals=6, source="stable", price_id="stable"),
        }

        with (
            patch.object(wallet_router, "_robinhood_chain_registry_price_metadata", return_value=metadata),
            patch.object(wallet_router, "_robinhood_chain_coingecko_prices", return_value={"ethereum": 1900.0}),
            patch.object(wallet_router, "_robinhood_chain_saved_quote_wallet", return_value=WALLET),
            patch.object(wallet_router, "_robinhood_chain_uniswap_quote_prices", new=AsyncMock()) as quote_prices,
        ):
            prices, _, sources = await wallet_router._robinhood_chain_registry_prices(
                object(),
                {"WETH": 1.0},
                exact_external_prices={TOKEN_A.lower(): 2000.0},
            )

        self.assertEqual(prices, {"WETH": 1900.0})
        self.assertEqual(sources["WETH"], "Token Registry · CoinGecko ethereum")
        quote_prices.assert_not_awaited()

    async def test_exact_external_gross_recent_matched_divergence_falls_back(self):
        metadata = {
            "LEAF": _meta(registry_id="1", address=TOKEN_A, decimals=18, source="coingecko"),
            "USDG": _meta(registry_id="2", address=TOKEN_B, decimals=6, source="stable", price_id="stable"),
        }
        now = wallet_router.time.time()
        wallet_router._robinhood_chain_quote_price_write_state(
            "LEAF", metadata["LEAF"], metadata["USDG"],
            status="success", usd_price=1.0,
            price_source="RH Chain matched quote · LEAF-USDG",
            price_fetched_at=now,
            provider_error_class=None, provider_http_status=200,
            attempted_at=now, retry_after=now + 300,
        )

        with (
            patch.object(wallet_router, "_robinhood_chain_registry_price_metadata", return_value=metadata),
            patch.object(wallet_router, "_robinhood_chain_coingecko_prices", return_value={}),
            patch.object(wallet_router, "_robinhood_chain_saved_quote_wallet", return_value=WALLET),
            patch.object(
                wallet_router,
                "_robinhood_chain_uniswap_quote_prices",
                new=AsyncMock(return_value=({"LEAF": 1.0}, set(), {})),
            ) as quote_prices,
        ):
            prices, _, sources = await wallet_router._robinhood_chain_registry_prices(
                object(),
                {"LEAF": 1.0},
                exact_external_prices={TOKEN_A.lower(): 4.0},
            )

        self.assertEqual(prices, {"LEAF": 1.0})
        self.assertEqual(sources["LEAF"], "RH Chain matched quote · LEAF-USDG")
        quote_prices.assert_awaited_once()

    def test_exact_external_duplicate_symbol_identity_is_contract_keyed(self):
        first = TOKEN_A.lower()
        second = TOKEN_C.lower()
        prices = {first: 25.0, second: 250.0}
        cached = {second}

        self.assertEqual(prices[first], 25.0)
        self.assertEqual(prices[second], 250.0)
        self.assertEqual(
            wallet_router._robinhood_chain_exact_external_source(first, cached),
            "CoinGecko Onchain (GeckoTerminal) · exact contract",
        )
        self.assertEqual(
            wallet_router._robinhood_chain_exact_external_source(second, cached),
            "CoinGecko Onchain (GeckoTerminal) · exact contract · cached",
        )



if __name__ == "__main__":
    unittest.main()
