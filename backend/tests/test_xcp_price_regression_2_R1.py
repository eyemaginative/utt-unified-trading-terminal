from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from app.routers import counterparty as counterparty_router
from app.services import market_metrics


class CounterpartyDerivedBalancePriceTests(unittest.TestCase):
    def test_crossed_book_is_not_valuation_capable(self):
        result = counterparty_router._counterparty_book_price_btc({
            "ok": True,
            "stale": False,
            "rate_limited": False,
            "best_bid": 7300.0,
            "best_ask": 0.00000888,
        })
        self.assertIsNone(result)

    def test_stale_book_is_not_valuation_capable(self):
        result = counterparty_router._counterparty_book_price_btc({
            "ok": True,
            "stale": True,
            "rate_limited": False,
            "best_bid": 0.00005,
            "best_ask": 0.000075,
        })
        self.assertIsNone(result)

    def test_one_sided_book_is_not_valuation_capable(self):
        bid_only = counterparty_router._counterparty_book_price_btc({
            "ok": True,
            "stale": False,
            "rate_limited": False,
            "best_bid": 0.00005,
            "best_ask": None,
        })
        ask_only = counterparty_router._counterparty_book_price_btc({
            "ok": True,
            "stale": False,
            "rate_limited": False,
            "best_bid": None,
            "best_ask": 0.000075,
        })
        self.assertIsNone(bid_only)
        self.assertIsNone(ask_only)

    def test_coherent_two_sided_book_uses_midpoint(self):
        with patch.dict(
            os.environ,
            {"COUNTERPARTY_BALANCE_DERIVED_MAX_SPREAD_PCT": "100"},
            clear=False,
        ):
            result = counterparty_router._counterparty_book_price_btc({
                "ok": True,
                "stale": False,
                "rate_limited": False,
                "best_bid": 0.00005,
                "best_ask": 0.000075,
            })

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["price_basis"], "validated_midpoint")
        self.assertAlmostEqual(result["price_btc"], 0.0000625, places=12)
        self.assertAlmostEqual(result["spread_pct_midpoint"], 40.0, places=9)

    def test_excessive_midpoint_relative_spread_is_rejected(self):
        with patch.dict(
            os.environ,
            {"COUNTERPARTY_BALANCE_DERIVED_MAX_SPREAD_PCT": "100"},
            clear=False,
        ):
            result = counterparty_router._counterparty_book_price_btc({
                "ok": True,
                "stale": False,
                "rate_limited": False,
                "best_bid": 1.0,
                "best_ask": 4.0,
            })

        self.assertIsNone(result)


class CoinGeckoDirectPriceFallbackTests(unittest.TestCase):
    def setUp(self):
        market_metrics._CG_BACKOFF_UNTIL = 0.0

    def test_simple_price_fallback_normalizes_explicit_id(self):
        captured = {}

        def fake_http(url, timeout_s=10.0):
            captured["url"] = url
            captured["timeout_s"] = timeout_s
            return {
                "counterparty": {
                    "usd": 5.25,
                    "last_updated_at": 1788892800,
                }
            }

        with patch.object(market_metrics, "_http_json", side_effect=fake_http):
            rows, errors = market_metrics._fetch_coingecko_simple_prices(
                ["counterparty"]
            )

        self.assertEqual(errors, [])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["id"], "counterparty")
        self.assertEqual(rows[0]["current_price"], 5.25)
        self.assertTrue(rows[0]["_utt_simple_price"])
        self.assertIn("/simple/price?", captured["url"])
        self.assertIn("ids=counterparty", captured["url"])

    def test_row_from_simple_price_keeps_explicit_coingecko_provenance(self):
        row = market_metrics._row_from_cg(
            "XCP",
            {
                "id": "counterparty",
                "name": "Counterparty",
                "chain": "bitcoin",
                "source": "token_registry",
            },
            {
                "id": "counterparty",
                "current_price": 5.25,
                "_utt_simple_price": True,
                "last_updated": "2026-09-08T16:00:00Z",
            },
            "2026-09-08T16:00:01Z",
        )

        self.assertEqual(row["asset"], "XCP")
        self.assertEqual(row["price_usd"], 5.25)
        self.assertEqual(row["price_source"], "coingecko:counterparty")
        self.assertTrue(
            any("simple-price fallback" in warning for warning in row["warnings"])
        )
        self.assertFalse(
            any("ticker symbol" in warning for warning in row["warnings"])
        )

    def test_summary_uses_simple_price_when_markets_omits_explicit_xcp_id(self):
        def meta_for_symbol(asset, allow_discovery=False):
            symbol = str(asset or "").strip().upper()
            if symbol == "XCP":
                return {
                    "id": "counterparty",
                    "name": "Counterparty",
                    "chain": "bitcoin",
                    "source": "token_registry",
                }
            if symbol == "BTC":
                return {
                    "id": "bitcoin",
                    "name": "Bitcoin",
                    "chain": "bitcoin",
                    "source": "token_registry",
                }
            return None

        bitcoin_market = {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 78000.0,
            "market_cap": 1.5e12,
            "total_volume": 1.0e10,
            "last_updated": "2026-09-08T16:00:00Z",
        }
        xcp_simple = {
            "id": "counterparty",
            "current_price": 5.25,
            "_utt_simple_price": True,
            "last_updated": "2026-09-08T16:00:00Z",
        }

        market_metrics._CACHE.clear()

        with (
            patch.object(
                market_metrics,
                "_db_owned_metric_assets",
                return_value=([], "test"),
            ),
            patch.object(
                market_metrics,
                "_db_market_metric_asset_context",
                return_value={},
            ),
            patch.object(
                market_metrics,
                "_coingecko_meta_for_symbol",
                side_effect=meta_for_symbol,
            ),
            patch.object(
                market_metrics,
                "_cg_raw_cache_rows",
                return_value={},
            ),
            patch.object(
                market_metrics,
                "_cg_raw_cache_symbol_rows",
                return_value={},
            ),
            patch.object(
                market_metrics,
                "_fetch_coingecko_markets",
                return_value=([bitcoin_market], []),
            ),
            patch.object(
                market_metrics,
                "_fetch_coingecko_simple_prices",
                return_value=([xcp_simple], []),
            ) as simple_fetch,
            patch.object(
                market_metrics,
                "_cg_raw_cache_update",
                return_value=None,
            ),
            patch.object(
                market_metrics,
                "_cache_get",
                return_value=None,
            ),
            patch.object(
                market_metrics,
                "_cache_set",
                return_value=None,
            ),
            patch.object(
                market_metrics,
                "_annotate_market_metric_rows",
                side_effect=lambda rows, **kwargs: rows,
            ),
        ):
            result = market_metrics.get_market_metrics_summary(
                assets="XCP,BTC",
                limit=10,
                ttl_s=300,
                force_refresh=True,
            )

        rows = {
            str(row.get("asset") or "").upper(): row
            for row in result.get("items") or []
            if isinstance(row, dict)
        }
        self.assertIn("XCP", rows)
        self.assertEqual(rows["XCP"]["price_usd"], 5.25)
        self.assertEqual(rows["XCP"]["price_source"], "coingecko:counterparty")
        simple_fetch.assert_called_once_with(["counterparty"])


if __name__ == "__main__":
    unittest.main()
