from __future__ import annotations

import ast
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, get_args
from unittest.mock import patch

from pydantic import ValidationError

from app.adapters import cexius as cexius_module
from app.adapters.cexius import CexiusAdapter
from app.config import Settings
from app.models import Order, VenueOrderRow
from app.services import orders as orders_service
from app.schemas import (
    AllOrderRow,
    BalanceRefreshRequest,
    BalanceRow,
    CancelAllRequest,
    OrderBookResponse,
    OrderCreate,
    OrderCreateVenue,
    OrderRulesResponse,
    ReadVenue,
    SymbolListResponse,
    SymbolResolveResponse,
    Venue,
    VenueOrderRefreshRequest,
    VenueOrderRowOut,
    VenueOrderVenue,
)
from app.venues.registry import get_venue_spec


class CexiusAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.adapter = CexiusAdapter()

    def test_vault_token_requires_read_scope_prefers_secret_and_falls_back_to_key(self):
        settings = Settings(_env_file=None, SQLITE_PATH="unused.db")
        record = {
            "scope_read": True,
            "scope_trade": True,
            "scope_transfer": False,
            "scope_withdraw": False,
            "bundle": {"api_key": "api-id", "api_secret": "bearer-token"},
        }
        with patch.object(settings, "_vault_latest_credential_record", return_value=record):
            self.assertEqual(settings.cexius_private_token(), "bearer-token")
            self.assertTrue(settings.cexius_effective_enabled())

        record["bundle"]["api_secret"] = ""
        with patch.object(settings, "_vault_latest_credential_record", return_value=record):
            self.assertEqual(settings.cexius_private_token(), "api-id")
            self.assertTrue(settings.cexius_effective_enabled())

        record["scope_read"] = False
        with patch.object(settings, "_vault_latest_credential_record", return_value=record):
            self.assertIsNone(settings.cexius_private_token())
            self.assertFalse(settings.cexius_effective_enabled())

    def test_registry_advertises_cexius_limit_trading_capability(self):
        spec = get_venue_spec("cexius")
        self.assertEqual(spec.key, "cexius")
        self.assertEqual(spec.display_name, "Cexius")
        self.assertTrue(spec.supports_trading)
        self.assertTrue(spec.supports_balances)
        self.assertTrue(spec.supports_orderbook)
        self.assertTrue(spec.supports_markets)
        self.assertIsInstance(spec.adapter_factory(), CexiusAdapter)

    def test_live_cancel_rejection_cannot_be_misclassified_as_already_closed(self):
        trade_record = {
            "scope_read": True,
            "scope_trade": True,
            "bundle": {"api_secret": "bearer-token"},
        }
        with patch.object(
            cexius_module.settings,
            "_vault_latest_credential_record",
            return_value=trade_record,
        ), patch.object(
            self.adapter,
            "_request_json",
            side_effect=RuntimeError("Cexius HTTP 400: cancel rejected"),
        ):
            with self.assertRaisesRegex(RuntimeError, "venue declined"):
                self.adapter.cancel_order("order-id", dry_run=False)

    def test_market_catalog_and_rules_are_normalized(self):
        payload = {
            "data": {
                "markets": [
                    {
                        "symbol": "BTC-USDT",
                        "base_currency": {"id": "BTC"},
                        "quote_currency": {"id": "USDT"},
                        "active": True,
                        "rules": {
                            "quantity_step": "0.00001",
                            "price_tick": "0.01",
                            "min_amount": "0.0001",
                            "min_total": "5",
                        },
                    },
                    {"symbol": "OFF-USDT", "active": False},
                ]
            }
        }
        with patch.object(self.adapter, "_request_json", return_value=payload):
            markets = self.adapter.list_markets(force=True)
            self.assertEqual(len(markets), 1)
            self.assertEqual(markets[0]["symbol_canon"], "BTC-USDT")
            self.assertEqual(markets[0]["symbol_venue"], "BTC-USDT")
            rules = self.adapter.get_order_rules("BTC-USDT")

        self.assertEqual(rules["base_increment"], 0.00001)
        self.assertEqual(rules["price_increment"], 0.01)
        self.assertEqual(rules["qty_decimals"], 5)
        self.assertEqual(rules["price_decimals"], 2)
        self.assertEqual(rules["min_qty"], 0.0001)
        self.assertEqual(rules["min_notional"], 5.0)
        self.assertEqual(rules["supported_order_types"], [])

    def test_live_market_precision_aliases_complete_strict_order_rules(self):
        payload = {
            "data": {
                "markets": [
                    {
                        "id": "DOGE-USDT",
                        "name": "DOGE-USDT",
                        "base_unit": "DOGE",
                        "quote_unit": "USDT",
                        "enabled": True,
                        "enable_trading": True,
                        "price_precision": 5,
                        "trading_amount_precision": 4,
                        "trading_min_amount": "0",
                        "trading_price_precision": 5,
                        "min_order_value": "0.05",
                    }
                ]
            }
        }
        with patch.object(self.adapter, "_request_json", return_value=payload):
            self.adapter.list_markets(force=True)
            rules = self.adapter.get_order_rules("DOGE-USDT")

        self.assertEqual(rules["qty_decimals"], 4)
        self.assertEqual(rules["price_decimals"], 5)
        self.assertEqual(rules["base_increment"], 0.0001)
        self.assertEqual(rules["price_increment"], 0.00001)
        self.assertEqual(rules["min_qty"], 0.0001)
        self.assertEqual(rules["min_notional"], 0.05)

        norm_qty, norm_price, errors = orders_service._validate_and_normalize_live_order(
            adapter=self.adapter,
            symbol_venue="DOGE-USDT",
            side="sell",
            type_="limit",
            qty=1.0,
            limit_price=0.1,
            tif=None,
            post_only=False,
        )
        self.assertEqual(errors, [])
        self.assertEqual(norm_qty, 1.0)
        self.assertEqual(norm_price, 0.1)

    def test_positive_trading_min_amount_remains_authoritative(self):
        payload = {
            "data": {
                "markets": [
                    {
                        "id": "DOGE-USDT",
                        "name": "DOGE-USDT",
                        "base_unit": "DOGE",
                        "quote_unit": "USDT",
                        "enabled": True,
                        "trading_amount_precision": 4,
                        "trading_min_amount": "0.25",
                        "trading_price_precision": 5,
                        "min_order_value": "0.05",
                    }
                ]
            }
        }
        with patch.object(self.adapter, "_request_json", return_value=payload):
            self.adapter.list_markets(force=True)
            rules = self.adapter.get_order_rules("DOGE-USDT")

        self.assertEqual(rules["base_increment"], 0.0001)
        self.assertEqual(rules["price_increment"], 0.00001)
        self.assertEqual(rules["min_qty"], 0.25)

    def test_orderbook_accepts_array_and_object_levels(self):
        payload = {
            "result": {
                "orderbook": {
                    "bids": [["100.00", "0.5"], {"price": "101.00", "amount": "0.25"}],
                    "asks": [{"rate": "103.00", "quantity": "0.4"}, ["102.00", "0.3"]],
                }
            }
        }
        with patch.object(self.adapter, "resolve_symbol", return_value="BTC-USDT"), patch.object(
            self.adapter, "_request_json", return_value=payload
        ):
            book = self.adapter.fetch_orderbook("BTC-USDT", depth=25, dry_run=True)

        self.assertEqual(book["bids"], [{"price": 101.0, "qty": 0.25}, {"price": 100.0, "qty": 0.5}])
        self.assertEqual(book["asks"], [{"price": 102.0, "qty": 0.3}, {"price": 103.0, "qty": 0.4}])


    def test_orderbook_fast_path_skips_redundant_market_catalog_resolution(self):
        payload = {
            "data": {
                "orderbook": {
                    "bids": [["0.06", "17"]],
                    "asks": [["0.08", "13"]],
                }
            }
        }

        def request(method, path, *, params=None, private=False):
            self.assertEqual(method, "GET")
            self.assertIsNone(params)
            self.assertFalse(private)
            if path == "/markets":
                self.fail("fetch_orderbook must not consult the Cexius market catalog")
            self.assertEqual(path, "/markets/DOGE-USDT/orderbook")
            return payload

        with patch.object(self.adapter, "_request_json", side_effect=request) as call:
            book = self.adapter.fetch_orderbook("doge_usdt", depth=25, dry_run=True)

        self.assertEqual(call.call_count, 1)
        self.assertEqual(book["bids"], [{"price": 0.06, "qty": 17.0}])
        self.assertEqual(book["asks"], [{"price": 0.08, "qty": 13.0}])

        repo = Path(__file__).resolve().parents[2]
        market_source = (repo / "backend" / "app" / "services" / "market.py").read_text(encoding="utf-8")
        self.assertIn('if v == "cexius":\n            symbol_venue = sym', market_source)

    def test_balances_are_read_only_normalized_and_zero_rows_are_skipped(self):
        payload = {
            "data": [
                {"currency": "BTC", "available": "0.25", "locked": "0.05", "balance": "0.30"},
                {"currency_id": "USDT", "free": "12", "reserved": "3"},
                {"currency": "ZERO", "available": "0", "locked": "0", "balance": "0"},
            ]
        }
        with patch.object(self.adapter, "_request_json", return_value=payload) as request:
            balances = self.adapter.fetch_balances(dry_run=False)

        request.assert_called_once_with("GET", "/balances", private=True)
        self.assertEqual(
            balances,
            [
                {"asset": "BTC", "total": 0.3, "available": 0.25, "hold": 0.05},
                {"asset": "USDT", "total": 15.0, "available": 12.0, "hold": 3.0},
            ],
        )

    def test_live_balance_shape_treats_balance_as_available_and_avoids_lock_double_counting(self):
        payload = {
            "balances": [
                {
                    "currency_id": "DOGE",
                    "balance": "100",
                    "locked": "2",
                    "reserved": "3",
                    "trading_locked": "10",
                }
            ]
        }
        with patch.object(self.adapter, "_request_json", return_value=payload):
            balances = self.adapter.fetch_balances(dry_run=False)

        self.assertEqual(
            balances,
            [{"asset": "DOGE", "total": 110.0, "available": 100.0, "hold": 10.0}],
        )

    def test_synthetic_cexius_balance_fields_map_to_available_hold_and_total(self):
        payload = {
            "balances": [
                {
                    "currency_id": "USDT",
                    "balance": "12.5",
                    "locked": "0",
                    "reserved": "0",
                    "trading_locked": "300",
                },
                {
                    "currency_id": "HEMP",
                    "balance": "250",
                    "locked": "0",
                    "reserved": "0",
                    "trading_locked": "50000",
                },
            ]
        }
        with patch.object(self.adapter, "_request_json", return_value=payload):
            balances = self.adapter.fetch_balances(dry_run=False)

        self.assertEqual(
            balances,
            [
                {
                    "asset": "HEMP",
                    "total": 50250.0,
                    "available": 250.0,
                    "hold": 50000.0,
                },
                {
                    "asset": "USDT",
                    "total": 312.5,
                    "available": 12.5,
                    "hold": 300.0,
                },
            ],
        )
    def test_api_min_notional_values_remain_market_specific_and_authoritative(self):
        payload = {
            "data": {
                "markets": [
                    {
                        "symbol": "BTC-USDT",
                        "base_currency": {"id": "BTC"},
                        "quote_currency": {"id": "USDT"},
                        "active": True,
                        "rules": {"min_total": "0.5"},
                    },
                    {
                        "symbol": "DOGE-USDT",
                        "base_currency": {"id": "DOGE"},
                        "quote_currency": {"id": "USDT"},
                        "active": True,
                        "rules": {"min_total": "0.05"},
                    },
                    {
                        "symbol": "HEMP-USDT",
                        "base_currency": {"id": "HEMP"},
                        "quote_currency": {"id": "USDT"},
                        "active": True,
                        "rules": {"min_total": "0.05"},
                    },
                ]
            }
        }
        with patch.object(self.adapter, "_request_json", return_value=payload):
            self.adapter.list_markets(force=True)
            self.assertEqual(self.adapter.get_order_rules("BTC-USDT")["min_notional"], 0.5)
            self.assertEqual(self.adapter.get_order_rules("DOGE-USDT")["min_notional"], 0.05)
            self.assertEqual(self.adapter.get_order_rules("HEMP-USDT")["min_notional"], 0.05)

    def test_open_orders_are_normalized_with_selected_order_cancel_reference(self):
        payload = {
            "items": [
                {
                    "id": "abc-123",
                    "market": "BTC-USDT",
                    "side": "BUY",
                    "type": "LIMIT",
                    "status": "OPEN",
                    "quantity": "0.2",
                    "filled_quantity": "0.05",
                    "price": "50000",
                    "created_at": "2026-08-03T12:00:00Z",
                }
            ]
        }
        with patch.object(self.adapter, "_request_json", return_value=payload):
            rows = self.adapter.fetch_orders()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["venue"], "cexius")
        self.assertEqual(rows[0]["venue_order_id"], "abc-123")
        self.assertEqual(rows[0]["symbol_canon"], "BTC-USDT")
        self.assertEqual(rows[0]["cancel_ref"], "cexius:abc-123")
        self.assertEqual(rows[0]["filled_qty"], 0.05)

        with patch.object(self.adapter, "_request_json") as request:
            self.assertTrue(self.adapter.cancel_order("abc-123", dry_run=True))
        request.assert_not_called()

    def test_filled_orders_without_executed_quantity_use_full_order_quantity(self):
        payload = {
            "items": [
                {
                    "id": "filled-order",
                    "market": "DOGE-USDT",
                    "side": "BUY",
                    "type": "LIMIT",
                    "status": "FILLED",
                    "quantity": "40",
                    "price": "0.05",
                },
                {
                    "id": "completed-order",
                    "market": "HEMP-USDT",
                    "side": "SELL",
                    "type": "LIMIT",
                    "status": "COMPLETED",
                    "quantity": "200",
                    "price": "0.00004",
                },
                {
                    "id": "open-order",
                    "market": "BTC-USDT",
                    "side": "BUY",
                    "type": "LIMIT",
                    "status": "OPEN",
                    "quantity": "0.00001",
                    "price": "60000",
                },
            ]
        }

        with patch.object(self.adapter, "_request_json", return_value=payload):
            rows = self.adapter.fetch_orders()

        by_id = {row["venue_order_id"]: row for row in rows}

        self.assertEqual(by_id["filled-order"]["filled_qty"], 40.0)
        self.assertEqual(by_id["completed-order"]["filled_qty"], 200.0)
        self.assertEqual(by_id["open-order"]["filled_qty"], 0.0)
    def test_open_orders_fetches_all_bounded_pages_and_deduplicates(self):
        def response(method, path, *, params=None, private=False):
            self.assertEqual(method, "GET")
            self.assertEqual(path, "/trading/orders")
            self.assertTrue(private)
            page = int((params or {}).get("page") or 1)
            if page == 1:
                return {
                    "data": {
                        "orders": [
                            {"id": "one", "market": "BTC-USDT", "status": "OPEN"},
                            {"id": "two", "market": "DOGE-USDT", "status": "OPEN"},
                        ],
                        "page": 1,
                        "limit": 200,
                        "total": 4,
                    }
                }
            if page == 2:
                return {
                    "data": {
                        "orders": [
                            {"id": "two", "market": "DOGE-USDT", "status": "OPEN"},
                            {"id": "three", "market": "HEMP-USDT", "status": "OPEN"},
                        ],
                        "page": 2,
                        "limit": 200,
                        "total": 4,
                    }
                }
            return {"data": {"orders": [], "page": page, "limit": 200, "total": 4}}

        with patch.object(self.adapter, "_request_json", side_effect=response) as request:
            rows = self.adapter.fetch_orders()

        self.assertEqual([row["venue_order_id"] for row in rows], ["one", "two", "three"])
        self.assertEqual(request.call_count, 2)

    def test_dry_run_order_create_and_cancel_are_non_mutating(self):
        with patch.object(self.adapter, "_request_json") as request:
            placed = self.adapter.place_order(
                "BTC-USDT",
                "buy",
                "limit",
                0.001,
                50000.0,
                "client-id",
                True,
            )
            self.assertEqual(placed["status"], "simulated")
            self.assertTrue(placed["raw"]["simulated"])
            self.assertFalse(placed["raw"]["venue_request_sent"])
            self.assertTrue(self.adapter.cancel_order("order-id", True))
        request.assert_not_called()

    def test_live_limit_order_uses_trade_scope_and_exact_documented_payload(self):
        trade_record = {
            "scope_read": True,
            "scope_trade": True,
            "bundle": {"api_key": "api-id", "api_secret": "bearer-token"},
        }
        response = {
            "id": "ord-123",
            "market": "DOGE-USDT",
            "side": "sell",
            "type": "limit",
            "amount": "2",
            "price": "0.071",
            "status": "open",
        }
        with patch.object(
            cexius_module.settings,
            "_vault_latest_credential_record",
            return_value=trade_record,
        ), patch.object(
            self.adapter,
            "_request_json",
            return_value=response,
        ) as request:
            placed = self.adapter.place_order(
                "doge_usdt",
                "sell",
                "limit",
                2.0,
                0.071,
                "internal-client-id",
                False,
                tif=None,
                post_only=False,
            )

        self.assertEqual(placed["venue_order_id"], "ord-123")
        self.assertEqual(placed["status"], "open")
        self.assertEqual(placed["cancel_ref"], "cexius:ord-123")
        request.assert_called_once_with(
            "POST",
            "/trading/order",
            json_body={
                "market": "DOGE-USDT",
                "side": "sell",
                "type": "limit",
                "amount": "2",
                "price": "0.071",
            },
            auth_token="bearer-token",
        )

    def test_cexius_order_create_rejects_market_and_undocumented_options(self):
        with self.assertRaisesRegex(ValueError, "limit orders only"):
            self.adapter.place_order(
                "DOGE-USDT", "buy", "market", 2.0, None, "cid", False
            )
        with self.assertRaisesRegex(ValueError, "time-in-force"):
            self.adapter.place_order(
                "DOGE-USDT", "buy", "limit", 2.0, 0.05, "cid", False, tif="gtc"
            )
        with self.assertRaisesRegex(ValueError, "post-only"):
            self.adapter.place_order(
                "DOGE-USDT", "buy", "limit", 2.0, 0.05, "cid", False, post_only=True
            )

    def test_live_cancel_requires_trade_scope_and_uses_one_delete_request(self):
        trade_record = {
            "scope_read": True,
            "scope_trade": True,
            "bundle": {"api_key": "api-id", "api_secret": "bearer-token"},
        }

        with patch.object(
            cexius_module.settings,
            "_vault_latest_credential_record",
            return_value=trade_record,
        ), patch.object(
            self.adapter,
            "_request_json",
            return_value={"success": True},
        ) as request:
            self.assertTrue(self.adapter.cancel_order("order/id", dry_run=False))

        request.assert_called_once_with(
            "DELETE",
            "/trading/orders/order%2Fid",
            auth_token="bearer-token",
        )

    def test_live_cancel_fails_closed_without_trade_scope_or_on_explicit_false(self):
        read_only_record = {
            "scope_read": True,
            "scope_trade": False,
            "bundle": {"api_secret": "bearer-token"},
        }
        with patch.object(
            cexius_module.settings,
            "_vault_latest_credential_record",
            return_value=read_only_record,
        ), patch.object(self.adapter, "_request_json") as request:
            with self.assertRaisesRegex(RuntimeError, "read and trade scopes"):
                self.adapter.cancel_order("order-id", dry_run=False)
        request.assert_not_called()

        trade_record = {
            "scope_read": True,
            "scope_trade": True,
            "bundle": {"api_secret": "bearer-token"},
        }
        with patch.object(
            cexius_module.settings,
            "_vault_latest_credential_record",
            return_value=trade_record,
        ), patch.object(
            self.adapter,
            "_request_json",
            return_value={"data": {"canceled": False}, "message": "not canceled"},
        ):
            with self.assertRaisesRegex(RuntimeError, "not canceled"):
                self.adapter.cancel_order("order-id", dry_run=False)


    def test_disarmed_venue_cancel_is_non_mutating_and_reports_simulation(self):
        snapshot = VenueOrderRow(
            venue="cexius",
            venue_order_id="simulated-venue-order",
            symbol_venue="DOGE-USDT",
            symbol_canon="DOGE-USDT",
            side="sell",
            type="limit",
            status="open",
            qty=2.0,
            filled_qty=0.0,
            limit_price=2.0,
            captured_at=datetime.now(timezone.utc),
        )
        local = Order(
            client_order_id="simulated-client-order",
            venue="cexius",
            symbol_canon="DOGE-USDT",
            symbol_venue="DOGE-USDT",
            side="sell",
            type="limit",
            qty=2.0,
            limit_price=2.0,
            status="open",
            filled_qty=0.0,
            venue_order_id="simulated-venue-order",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )

        class ScalarResult:
            def scalars(self):
                return self

            def first(self):
                return snapshot

        class FakeDb:
            def execute(self, _statement):
                return ScalarResult()

            def add(self, _row):
                raise AssertionError("dry-run cancellation must not add mutated rows")

            def commit(self):
                raise AssertionError("dry-run cancellation must not commit")

            def refresh(self, _row):
                raise AssertionError("dry-run cancellation must not refresh mutated rows")

        with patch.object(orders_service.settings, "dry_run", False), patch.object(
            orders_service.settings, "armed", False
        ), patch.object(orders_service, "get_adapter") as get_adapter, patch.object(
            orders_service, "_post_trade_refresh"
        ) as post_refresh:
            result = orders_service.cancel_by_ref(
                FakeDb(),
                "cexius:simulated-venue-order",
            )

        self.assertTrue(result["ok"])
        self.assertTrue(result["simulated"])
        self.assertFalse(result["venue_request_sent"])
        self.assertFalse(result["snapshot_updated"])
        self.assertEqual(result["snapshot_rows_updated"], 0)
        self.assertEqual(result["orders_rows_updated"], 0)
        self.assertEqual(result["status"], "open")
        self.assertEqual(snapshot.status, "open")
        self.assertEqual(local.status, "open")
        self.assertIsNone(getattr(snapshot, "closed_at", None))
        self.assertIsNone(getattr(local, "closed_at", None))
        get_adapter.assert_not_called()
        post_refresh.assert_not_called()

    def test_disarmed_local_cancel_is_non_mutating_and_reports_simulation(self):
        local = Order(
            id="simulated-local-order-id",
            client_order_id="simulated-local-client-order",
            venue="cexius",
            symbol_canon="DOGE-USDT",
            symbol_venue="DOGE-USDT",
            side="sell",
            type="limit",
            qty=2.0,
            limit_price=2.0,
            status="open",
            filled_qty=0.0,
            venue_order_id="simulated-local-venue-order",
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )

        class FakeDb:
            def get(self, model, key):
                self.last_get = (model, key)
                return local

            def add(self, _row):
                raise AssertionError("dry-run cancellation must not add mutated rows")

            def commit(self):
                raise AssertionError("dry-run cancellation must not commit")

            def refresh(self, _row):
                raise AssertionError("dry-run cancellation must not refresh mutated rows")

        db = FakeDb()

        with patch.object(orders_service.settings, "dry_run", False), patch.object(
            orders_service.settings, "armed", False
        ), patch.object(orders_service, "get_adapter") as get_adapter, patch.object(
            orders_service, "_post_trade_refresh"
        ) as post_refresh:
            result = orders_service.cancel_by_ref(
                db,
                f"LOCAL:{local.id}",
            )

        self.assertEqual(db.last_get, (Order, local.id))
        self.assertTrue(result["ok"])
        self.assertTrue(result["simulated"])
        self.assertFalse(result["venue_request_sent"])
        self.assertFalse(result["snapshot_updated"])
        self.assertEqual(result["orders_rows_updated"], 0)
        self.assertEqual(result["status"], "open")
        self.assertEqual(local.status, "open")
        self.assertIsNone(getattr(local, "closed_at", None))
        get_adapter.assert_not_called()
        post_refresh.assert_not_called()

    def test_frontend_simulated_cancel_reports_no_venue_request_and_skips_hold_refresh(self):
        repo = Path(__file__).resolve().parents[2]
        tables_source = (
            repo / "frontend" / "src" / "TerminalTablesWidget.jsx"
        ).read_text(encoding="utf-8")

        self.assertIn(
            "Simulation only — no venue cancellation request was sent.",
            tables_source,
        )
        self.assertIn(
            "The order remains open until confirmed otherwise by the venue.",
            tables_source,
        )

        marker = 'resp && typeof resp === "object" && resp.simulated === true'
        self.assertGreaterEqual(tables_source.count(marker), 2)

        first = tables_source.index(marker)
        first_end = tables_source.index(
            "// Confirmed live cancel:",
            first,
        )
        first_block = tables_source[first:first_end]

        second = tables_source.index(marker, first + len(marker))
        second_end = tables_source.index(
            "// Keep Local + Unified views consistent after a confirmed live cancel.",
            second,
        )
        second_block = tables_source[second:second_end]

        for block in (first_block, second_block):
            self.assertIn("doSyncAndLoadAllOrders", block)
            self.assertNotIn("postCancelRefresh", block)

    def test_frontend_and_market_service_contracts_include_cexius(self):
        repo = Path(__file__).resolve().parents[2]
        app_source = (repo / "frontend" / "src" / "App.jsx").read_text(encoding="utf-8")
        tables_source = (repo / "frontend" / "src" / "TerminalTablesWidget.jsx").read_text(encoding="utf-8")
        market_source = (repo / "backend" / "app" / "services" / "market.py").read_text(encoding="utf-8")

        self.assertIn('"cexius"', app_source)
        self.assertNotIn('READ_ONLY_ORDER_TICKET_VENUES = new Set(["cexius"', app_source)
        self.assertIn('if (value === "cexius") return "Cexius";', app_source)
        self.assertIn('"cexius", "okx"', tables_source)
        self.assertIn('if v == "cexius":', market_source)
        self.assertIn('adapter = get_adapter(v)', market_source)

    def test_cexius_order_create_schema_is_promoted_without_cancel_all_promotion(self):
        now = datetime.now(timezone.utc)

        self.assertIn("cexius", get_args(ReadVenue))
        self.assertIn("cexius", get_args(VenueOrderVenue))
        self.assertIn("cexius", get_args(OrderCreateVenue))
        self.assertNotIn("cexius", get_args(Venue))

        BalanceRefreshRequest(venue="cexius")
        BalanceRow(
            venue="cexius",
            asset="BTC",
            total=1.0,
            available=1.0,
            hold=0.0,
            captured_at=now,
        )
        SymbolListResponse(venue="cexius", symbols=["BTC-USDT"])
        SymbolResolveResponse(
            venue="cexius",
            symbol_canon="BTC-USDT",
            symbol_venue="BTC-USDT",
        )
        OrderBookResponse(
            venue="cexius",
            symbol_canon="BTC-USDT",
            bids=[],
            asks=[],
            ts=now,
        )
        OrderRulesResponse(
            venue="cexius",
            symbol_canon="BTC-USDT",
            symbol_venue="BTC-USDT",
        )
        VenueOrderRefreshRequest(venue="cexius")
        VenueOrderRowOut(
            venue="cexius",
            venue_order_id="cexius-order",
            symbol_venue="BTC-USDT",
            captured_at=now,
        )

        created = OrderCreate(
            venue="cexius",
            symbol="BTC-USDT",
            side="buy",
            type="limit",
            qty=0.001,
            limit_price=1.0,
        )
        self.assertEqual(created.venue, "cexius")

        with self.assertRaises(ValidationError):
            CancelAllRequest(venue="cexius")

    def test_all_venue_order_sync_includes_cexius_read_only(self):
        repo = Path(__file__).resolve().parents[2]
        router_source = (
            repo / "backend" / "app" / "routers" / "venue_orders.py"
        ).read_text(encoding="utf-8")
        app_source = (
            repo / "frontend" / "src" / "App.jsx"
        ).read_text(encoding="utf-8")
        all_orders_source = (
            repo / "backend" / "app" / "services" / "all_orders.py"
        ).read_text(encoding="utf-8")
        venue_orders_source = (
            repo / "backend" / "app" / "routers" / "venue_orders.py"
        ).read_text(encoding="utf-8")

        self.assertRegex(
            router_source,
            r'venues = \[[^\]]*"cexius"[^\]]*\]',
        )
        self.assertIn(
            'supportedVenues.includes("cexius") ? ["cexius"] : []',
            app_source,
        )
        self.assertIn(
            "const refreshCandidates = normalizeVenueList([",
            app_source,
        )
        self.assertNotIn(
            'if v == "cexius":\n            it["cancel_ref"] = None',
            all_orders_source,
        )
        self.assertIn(
            'cancel_ref = f"{venue_norm}:{venue_order_id}" if venue_norm and venue_order_id else None',
            venue_orders_source,
        )

    def test_frontend_cexius_limit_submit_and_unknown_precision_contracts(self):
        repo = Path(__file__).resolve().parents[2]
        ticket_source = (
            repo / "frontend" / "src" / "OrderTicketWidget.jsx"
        ).read_text(encoding="utf-8")
        tables_source = (
            repo / "frontend" / "src" / "TerminalTablesWidget.jsx"
        ).read_text(encoding="utf-8")

        self.assertIn(
            'const precisionDigitsOrNull = (value) => {',
            ticket_source,
        )
        self.assertIn(
            'const maxFrac = precisionDigitsOrNull(rules?.qty_decimals) ?? 8;',
            ticket_source,
        )
        self.assertIn(
            'const allowed = precisionDigitsOrNull(qtyDec);',
            ticket_source,
        )
        self.assertIn(
            'const allowed = precisionDigitsOrNull(pxDec);',
            ticket_source,
        )
        self.assertNotIn(
            'if (venueLower === "cexius") return;',
            tables_source,
        )
        self.assertNotIn(
            'Cexius order visibility is read-only in CEXIUS.1ABC.',
            tables_source,
        )
        self.assertNotIn(
            'if (venueLower === "cexius" && terminal) {',
            tables_source,
        )
        self.assertNotIn(
            'Terminal Cexius orders cannot be canceled.',
            tables_source,
        )
        self.assertIn(
            '// - cancelable open rows: Cancel only',
            tables_source,
        )
        self.assertIn(
            '// - terminal rows and noncancelable swaps: Details only',
            tables_source,
        )
        self.assertIn(
            'Cexius venue options: API default · TIF / post-only / Client OID not sent',
            ticket_source,
        )
        self.assertIn(
            'Simulation Only — No Venue Order Sent',
            ticket_source,
        )
        self.assertIn(
            'await doCancelUnifiedOrder({ cancel_ref: cancelRef, row: o });',
            tables_source,
        )
        self.assertIn(
            'setCancelingKeys((prev) => ({ ...prev, [k]: true }));',
            tables_source,
        )

    def test_order_details_schema_preserves_read_only_identity_fields(self):
        now = datetime.now(timezone.utc)
        row = AllOrderRow(
            source="VENUE",
            venue="cexius",
            submitted_at=now,
            transaction_id="tx-id",
            txid="tx-id",
            approval_transaction_id="approval-id",
            approval_txid="approval-id",
            transaction_url="https://example.invalid/tx-id",
            explorer_url="https://example.invalid/tx-id",
        )

        self.assertEqual(row.submitted_at, now)
        self.assertEqual(row.transaction_id, "tx-id")
        self.assertEqual(row.txid, "tx-id")
        self.assertEqual(row.approval_transaction_id, "approval-id")
        self.assertEqual(row.approval_txid, "approval-id")
        self.assertEqual(row.transaction_url, "https://example.invalid/tx-id")
        self.assertEqual(row.explorer_url, "https://example.invalid/tx-id")

    def test_generic_order_details_actions_and_modal_are_read_only(self):
        repo = Path(__file__).resolve().parents[2]
        tables_source = (
            repo / "frontend" / "src" / "TerminalTablesWidget.jsx"
        ).read_text(encoding="utf-8")
        all_orders_source = (
            repo / "backend" / "app" / "services" / "all_orders.py"
        ).read_text(encoding="utf-8")

        self.assertIn(
            'const [orderDetailsRow, setOrderDetailsRow] = useState(null);',
            tables_source,
        )
        self.assertIn(
            'function renderOrderDetailsModal() {',
            tables_source,
        )
        self.assertIn(
            'Read-only order, lifecycle, fee, and accounting data.',
            tables_source,
        )
        self.assertIn(
            'const showDetails = !canCancel && (terminal || isCounterpartyDispense || isSwapLike);',
            tables_source,
        )
        self.assertIn(
            'onClick={openDetails}',
            tables_source,
        )
        self.assertIn(
            '{renderOrderDetailsModal()}',
            tables_source,
        )
        self.assertNotIn(
            'Read only\n',
            tables_source,
        )

        modal_start = tables_source.index(
            'function renderOrderDetailsModal() {'
        )
        modal_end = tables_source.index(
            '// Cancel confirmation modal (rendered over widget)',
            modal_start,
        )
        modal_source = tables_source[modal_start:modal_end]

        for forbidden in (
            'doCancelUnifiedOrder(',
            'doCancelOrder(',
            'cancelOrderByRef(',
            'http.post(',
            'http.delete(',
            'axios.',
            'fetch(',
        ):
            self.assertNotIn(forbidden, modal_source)

        self.assertIn(
            '"submitted_at": o.submitted_at',
            all_orders_source,
        )
        self.assertIn(
            "'transaction_id': str(mp.get('signature') or '')",
            all_orders_source,
        )

    def test_cexius_mutation_surface_is_limit_create_and_selected_cancel_only(self):
        source = Path(__file__).resolve().parents[1] / "app" / "adapters" / "cexius.py"
        text = source.read_text(encoding="utf-8")
        self.assertIn('"POST",\n            "/trading/order"', text)
        self.assertIn('"DELETE",', text)
        self.assertIn('f"/trading/orders/{oid}"', text)
        self.assertNotIn('/trading/orders/cancel-all', text)
        self.assertNotIn('/withdraw', text.lower())
        self.assertIn('if type_norm != "limit":', text)

    def test_trade_router_cexius_simulation_is_non_persistent_and_limit_only(self):
        repo = Path(__file__).resolve().parents[2]
        source = (repo / "backend" / "app" / "routers" / "trade.py").read_text(encoding="utf-8")
        self.assertIn('def _validate_cexius_order_contract(req: OrderCreate) -> None:', source)
        self.assertIn('if venue == "cexius" and _effective_dry_run():', source)
        self.assertIn('return _cexius_simulated_order(req)', source)
        sim_start = source.index('def _cexius_simulated_order')
        sim_end = source.index('@router.post("/order"', sim_start)
        sim_source = source[sim_start:sim_end]
        self.assertNotIn('db.add(', sim_source)
        self.assertNotIn('create_order(', sim_source)
        self.assertIn('"simulated": True', sim_source)
        self.assertIn('"venue_request_sent": False', sim_source)
        self.assertIn('CEXIUS.2B permits limit orders only', source)

    def test_cexius_trade_gate_requires_trade_scope_without_exposing_secrets(self):
        repo = Path(__file__).resolve().parents[2]
        source = (repo / "backend" / "app" / "routers" / "venues.py").read_text(encoding="utf-8")
        self.assertIn('def _cexius_trade_scope_enabled() -> bool:', source)
        self.assertIn('and ((not is_cexius) or bool(cexius_trade_scope_enabled))', source)
        self.assertIn('Cexius credential read=true and trade=true', source)
        self.assertIn('"cexius_trade_scope_enabled": cexius_trade_scope_enabled', source)
        self.assertNotIn('"api_secret":', source)
        self.assertNotIn('"api_key":', source)

    def test_frontend_cexius_payload_omits_undocumented_fields_and_handles_simulation(self):
        repo = Path(__file__).resolve().parents[2]
        source = (repo / "frontend" / "src" / "OrderTicketWidget.jsx").read_text(encoding="utf-8")
        self.assertIn('...(v === "cexius"', source)
        self.assertIn('? {}', source)
        self.assertIn('j?.simulated === true', source)
        self.assertIn('no Cexius venue order request was sent and no local open order was created', source)
        self.assertIn('post_submit_refresh: {\n          status: "not_run"', source)

    def test_all_orders_hide_control_hides_canceled_and_rejected(self):
        repo = Path(__file__).resolve().parents[2]
        app_source = (repo / "frontend" / "src" / "App.jsx").read_text(encoding="utf-8")
        tables_source = (repo / "frontend" / "src" / "TerminalTablesWidget.jsx").read_text(encoding="utf-8")
        self.assertIn('s === "canceled" || s === "cancelled" || s === "rejected"', app_source)
        self.assertIn('Hide canceled / rejected', tables_source)
        self.assertIn('const isRejected = (s) => normalizeStatusLower(s) === "rejected";', tables_source)

    def test_all_orders_cancelability_is_open_only_for_cexius(self):
        repo = Path(__file__).resolve().parents[2]
        source_path = repo / "backend" / "app" / "services" / "all_orders.py"
        source = source_path.read_text(encoding="utf-8")
        tree = ast.parse(source)

        fn = next(
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef) and node.name == "_apply_cancelability"
        )
        isolated = ast.Module(body=[fn], type_ignores=[])
        ast.fix_missing_locations(isolated)

        namespace = {
            "Any": Any,
            "Dict": Dict,
            "List": List,
            "_norm_venue": lambda value: str(value or "").strip().lower(),
        }
        exec(compile(isolated, str(source_path), "exec"), namespace)

        rows = [
            {
                "source": "VENUE",
                "venue": "cexius",
                "venue_order_id": "open-order",
                "status_bucket": "open",
            },
            {
                "source": "VENUE",
                "venue": "cexius",
                "venue_order_id": "filled-order",
                "status_bucket": "terminal",
            },
        ]

        namespace["_apply_cancelability"](rows)

        self.assertEqual(rows[0]["cancel_ref"], "cexius:open-order")
        self.assertTrue(rows[0]["can_cancel"])
        self.assertEqual(rows[1]["cancel_ref"], "cexius:filled-order")
        self.assertFalse(rows[1]["can_cancel"])


if __name__ == "__main__":
    unittest.main()
