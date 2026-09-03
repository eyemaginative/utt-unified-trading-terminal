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
        self._engine_patch.stop()
        self._price_test_engine.dispose()
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_DB_READY = False

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

    async def test_uniswap_quote_fallback_is_nonblocking_then_reuses_cache(self):
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
        release = asyncio.Event()
        started = asyncio.Event()

        async def delayed_quote(**kwargs):
            started.set()
            await release.wait()
            return {"ok": True, "price_quote_per_base": "212.345"}

        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(side_effect=delayed_quote)

        with patch.object(
            wallet_router,
            "get_robinhood_chain_uniswap_quote_service",
            return_value=fake_service,
        ):
            first, first_cached, first_status = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata,
                {"AAPL": 0.007366945714746708},
                WALLET,
            )
            self.assertEqual(first, {})
            self.assertEqual(first_cached, set())
            self.assertEqual(first_status, {})
            self.assertIsNotNone(wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER)

            second, _, _ = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata,
                {"AAPL": 0.007366945714746708},
                WALLET,
            )
            self.assertEqual(second, {})
            await asyncio.wait_for(started.wait(), timeout=1.0)
            self.assertEqual(fake_service.quote.await_count, 1)

            release.set()
            warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
            await asyncio.wait_for(asyncio.shield(warmer), timeout=1.0)

            third, third_cached, third_status = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata,
                {"AAPL": 0.007366945714746708},
                WALLET,
            )

        self.assertEqual(third, {"AAPL": 212.345})
        self.assertEqual(third_cached, set())
        self.assertEqual(third_status, {})
        self.assertEqual(fake_service.quote.await_count, 1)

        kwargs = fake_service.quote.await_args.kwargs
        self.assertEqual(kwargs["symbol"], "AAPL-USDG")
        self.assertEqual(kwargs["requested_amount"], "0.007366945714746708")
        self.assertFalse(kwargs["_retry_provider_errors"])
        self.assertEqual(kwargs["_provider_priority"], "background")


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
                return {"ok": True, "price_quote_per_base": prices[kwargs["symbol"]]}
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
                metadata,
                {"AAPL": 1.0, "TSLA": 1.0, "UP": 1.0},
                WALLET,
            )
            second, _, _ = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata,
                {"AAPL": 1.0, "TSLA": 1.0, "UP": 1.0},
                WALLET,
            )
            self.assertEqual(first, {})
            self.assertEqual(second, {})
            warmer = wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_WARMER
            await asyncio.wait_for(asyncio.shield(warmer), timeout=1.0)
            final, _, _ = await wallet_router._robinhood_chain_uniswap_quote_prices(
                metadata,
                {"AAPL": 1.0, "TSLA": 1.0, "UP": 1.0},
                WALLET,
            )

        self.assertEqual(final, {"AAPL": 212.0, "TSLA": 340.0, "UP": 0.305159})
        self.assertEqual(fake_service.quote.await_count, 3)
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
        self.assertEqual(sources["AAPL"], "RH Chain quote · AAPL-USDG")

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
        fake_service = unittest.mock.Mock()
        fake_service.quote = AsyncMock(return_value={"ok": True, "price_quote_per_base": "212.5"})

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
        self.assertEqual(fake_service.quote.await_count, 1)

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

    async def test_transient_state_uses_short_retry_class_and_preserves_last_good_column(self):
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
            price_source="RH Chain quote · AAPL-USDG",
            price_fetched_at=now,
            provider_error_class=None,
            provider_http_status=200,
            attempted_at=now,
            retry_after=now,
        )
        wallet_router._robinhood_chain_quote_price_write_state(
            "AAPL",
            metadata["AAPL"],
            metadata["USDG"],
            status="transient",
            usd_price=None,
            price_source=None,
            price_fetched_at=None,
            provider_error_class="transient",
            provider_http_status=None,
            attempted_at=now + 1,
            retry_after=now + 120,
        )
        wallet_router._ROBINHOOD_CHAIN_QUOTE_PRICE_CACHE.clear()
        restored, cached, status = await wallet_router._robinhood_chain_uniswap_quote_prices(
            metadata, {"AAPL": 1.0}, WALLET
        )
        self.assertEqual(restored, {"AAPL": 210.0})
        self.assertEqual(cached, {"AAPL"})
        self.assertEqual(status, {})

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
                return {"ok": True, "price_quote_per_base": "1.25"}
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
        self.assertEqual(fake_service.quote.await_count, 5)
        self.assertEqual(max_active, 1)



if __name__ == "__main__":
    unittest.main()
