from __future__ import annotations

import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy import create_engine, inspect

from app.routers import auth as auth_module
from app.routers import robinhood_chain as robinhood_chain_router
from app.services.robinhood_chain_execution_discovery import RobinhoodChainExecutionDiscoveryService
from app.services.robinhood_chain_registry_discovery import _parse_probe_amount


class RobinhoodChainProfileLimitTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_engine = auth_module.engine
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp.name) / "profile-limits.sqlite"
        auth_module.engine = create_engine(f"sqlite+pysqlite:///{self.db_path}")

    def tearDown(self) -> None:
        try:
            auth_module.engine.dispose()
        except Exception:
            pass
        auth_module.engine = self._old_engine
        self._tmp.cleanup()

    def test_preferences_are_separate_user_scoped_rows_without_auth_fk(self) -> None:
        auth_module.ensure_user_preferences_schema()
        inspector = inspect(auth_module.engine)
        self.assertIn("utt_user_preferences", inspector.get_table_names())
        columns = {item["name"] for item in inspector.get_columns("utt_user_preferences")}
        self.assertEqual(
            columns,
            {"username", "preference_key", "preference_value", "updated_at"},
        )
        self.assertEqual(inspector.get_foreign_keys("utt_user_preferences"), [])

        alice = auth_module._set_robinhood_chain_preferences(
            {"user": "alice", "auth": True},
            discovery_max_usd="125.50",
            interactive_quote_max_usd="1000",
        )
        bob = auth_module.get_robinhood_chain_preferences({"user": "bob", "auth": True})
        local = auth_module.get_robinhood_chain_preferences({"user": "anonymous", "auth": False})

        self.assertEqual(alice["discovery_max_usd"], "125.5")
        self.assertEqual(alice["interactive_quote_max_usd"], "1000")
        self.assertEqual(alice["sources"]["discovery_max_usd"], "database")
        self.assertEqual(alice["sources"]["interactive_quote_max_usd"], "database")
        self.assertEqual(bob["sources"]["discovery_max_usd"], "bootstrap_default")
        self.assertEqual(local["user"], "local")

    def test_preferences_survive_engine_restart(self) -> None:
        auth_module._set_robinhood_chain_preferences(
            {"user": "restart-user", "auth": True},
            discovery_max_usd="250",
            interactive_quote_max_usd="5000.125",
        )
        auth_module.engine.dispose()
        auth_module.engine = create_engine(f"sqlite+pysqlite:///{self.db_path}")

        loaded = auth_module.get_robinhood_chain_preferences(
            {"user": "restart-user", "auth": True}
        )
        self.assertEqual(loaded["discovery_max_usd"], "250")
        self.assertEqual(loaded["interactive_quote_max_usd"], "5000.125")
        self.assertEqual(loaded["sources"]["discovery_max_usd"], "database")
        self.assertEqual(loaded["sources"]["interactive_quote_max_usd"], "database")

    def test_preferences_api_contract_reads_and_updates_same_subject(self) -> None:
        request = auth_module.UserPreferencesPatchRequest(
            robinhood_chain={
                "discovery_max_usd": "75.25",
                "interactive_quote_max_usd": "250",
            }
        )
        saved = auth_module.auth_preferences_patch(
            request,
            ident={"user": "profile-user", "auth": True},
        )
        self.assertTrue(saved["ok"])
        self.assertEqual(saved["robinhood_chain"]["user"], "profile-user")
        self.assertEqual(saved["robinhood_chain"]["discovery_max_usd"], "75.25")
        self.assertEqual(saved["robinhood_chain"]["interactive_quote_max_usd"], "250")

        loaded = auth_module.auth_preferences_get(
            ident={"user": "profile-user", "auth": True}
        )
        self.assertTrue(loaded["ok"])
        self.assertEqual(loaded["robinhood_chain"]["discovery_max_usd"], "75.25")
        self.assertEqual(loaded["robinhood_chain"]["interactive_quote_max_usd"], "250")
        self.assertEqual(loaded["robinhood_chain"]["sources"]["discovery_max_usd"], "database")
        self.assertEqual(loaded["robinhood_chain"]["sources"]["interactive_quote_max_usd"], "database")

    def test_invalid_preference_values_fail_closed(self) -> None:
        for value in ("", "0", "-1", "NaN", "Infinity", "1e3", ".5", "1.2.3"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    auth_module._normalize_positive_decimal_preference(
                        value,
                        field="test_limit",
                    )

    def test_execution_discovery_uses_dynamic_usd_cap(self) -> None:
        service = object.__new__(RobinhoodChainExecutionDiscoveryService)
        service.max_sell_usd = 5.0
        nonstable = {"external_price_source": None}
        stable = {"external_price_source": "stable"}

        # Historical max_probe_amount remains a discovery-probe boundary when
        # explicitly supplied to discovery itself.
        with self.assertRaisesRegex(ValueError, "discovery_amount_exceeds_cap"):
            service._enforce_pre_request_cap(
                amount_mode="exact_input",
                amount_display="2",
                sell_token=nonstable,
                buy_token=nonstable,
                max_probe_amount="1",
                max_sell_usd="100",
            )

        # User/Profile USD policy is independent from that historical probe cap.
        service._enforce_pre_request_cap(
            amount_mode="exact_input",
            amount_display="75",
            sell_token=stable,
            buy_token=nonstable,
            max_probe_amount=None,
            max_sell_usd="100",
        )
        with self.assertRaisesRegex(ValueError, "discovery_amount_exceeds_cap"):
            service._enforce_pre_request_cap(
                amount_mode="exact_input",
                amount_display="101",
                sell_token=stable,
                buy_token=nonstable,
                max_probe_amount=None,
                max_sell_usd="100",
            )

        capped = service._apply_value_cap(
            {"ok": True, "liquidity_available": True, "sell_amount": "1", "buy_amount": "125"},
            sell_token=nonstable,
            buy_token=stable,
            max_sell_usd="100",
        )
        self.assertFalse(capped["ok"])
        self.assertEqual(capped["error"], "discovery_amount_exceeds_cap")
        self.assertEqual(capped["discovery_value_usd_estimate"], "125")
        self.assertEqual(capped["discovery_value_cap_usd"], "100")

    def test_registry_probe_parser_has_no_hardcoded_25_display_unit_ceiling(self) -> None:
        self.assertEqual(_parse_probe_amount("100", 18), "100")
        self.assertEqual(_parse_probe_amount("0.0001", 18), "0.0001")
        with self.assertRaisesRegex(ValueError, "probe_amount_exceeds_token_precision"):
            _parse_probe_amount("0.0000001", 6)

    def test_interactive_profile_cap_checks_verified_stable_metadata_only(self) -> None:
        stable = {"symbol": "STABLE", "external_price_source": "stable"}
        volatile = {"symbol": "VOL", "external_price_source": "coingecko"}

        with self.assertRaises(HTTPException) as context:
            robinhood_chain_router._enforce_robinhood_chain_interactive_profile_cap_before_provider(
                requested_amount="6",
                amount_mode="exact_input",
                input_token=stable,
                output_token=volatile,
                maximum_usd=Decimal("5"),
            )
        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(
            context.exception.detail["error"],
            "robinhood_chain_interactive_quote_exceeds_profile_cap",
        )
        self.assertFalse(context.exception.detail["provider_contacted"])

        # An arbitrary symbol is not treated as USD-stable without exact Registry
        # metadata, so UTT does not fabricate a dollar conversion.
        robinhood_chain_router._enforce_robinhood_chain_interactive_profile_cap_before_provider(
            requested_amount="999",
            amount_mode="exact_input",
            input_token={"symbol": "USDLIKE", "external_price_source": None},
            output_token=volatile,
            maximum_usd=Decimal("5"),
        )

        post = robinhood_chain_router._apply_robinhood_chain_interactive_profile_cap_after_provider(
            {"ok": True, "output_amount": "12", "provider_contacted": True},
            input_token=volatile,
            output_token=stable,
            maximum_usd=Decimal("10"),
        )
        self.assertFalse(post["ok"])
        self.assertEqual(post["error"], "robinhood_chain_interactive_quote_exceeds_profile_cap")
        self.assertEqual(post["interactive_quote_value_usd_estimate"], "12")

    def test_frontend_and_source_contracts_are_wired(self) -> None:
        root = Path(__file__).resolve().parents[2]
        config = (root / "backend" / "app" / "config.py").read_text(encoding="utf-8")
        discovery = (
            root / "backend" / "app" / "services" / "robinhood_chain_execution_discovery.py"
        ).read_text(encoding="utf-8")
        registry = (
            root / "backend" / "app" / "services" / "robinhood_chain_registry_discovery.py"
        ).read_text(encoding="utf-8")
        quotes = (
            root / "backend" / "app" / "services" / "robinhood_chain_quotes.py"
        ).read_text(encoding="utf-8")
        profile = (
            root / "frontend" / "src" / "components" / "AppHeader.jsx"
        ).read_text(encoding="utf-8")
        api = (root / "frontend" / "src" / "lib" / "api.js").read_text(encoding="utf-8")
        orderbook = (root / "frontend" / "src" / "OrderBookWidget.jsx").read_text(
            encoding="utf-8"
        )
        ticket = (root / "frontend" / "src" / "OrderTicketWidget.jsx").read_text(
            encoding="utf-8"
        )

        self.assertNotIn("le=25.0", config)
        self.assertNotIn("min(float(max_sell_usd), 25.0)", discovery)
        self.assertNotIn('amount > Decimal("25")', registry)
        self.assertNotIn('amount > Decimal("25")', quotes)
        self.assertIn('const [profileRhLimitsBusy', profile)
        self.assertIn('const [profileRhDiscoveryMaxUsd', profile)
        self.assertIn('const [profileRhInteractiveMaxUsd', profile)
        self.assertIn("Robinhood Chain Limits", profile)
        self.assertIn("/api/auth/preferences", profile)
        self.assertIn("const headers = robinhoodChainAuthHeaders()", api)
        self.assertIn('headers: robinhoodChainAuthHeaders({ "Content-Type": "application/json" })', orderbook)
        self.assertNotIn('Number(totalQuote) > 5', ticket)
        self.assertNotIn('setTotalQuote("1")', ticket)
        self.assertIn('const restoredExpiredPrebroadcast = Boolean(', ticket)
        self.assertIn('!restoredHasBroadcastEvidence', ticket)
        self.assertIn('expired before any broadcast evidence', ticket)
        self.assertIn(
            'function robinhoodChainCapabilityFor(status, symbol, objectiveId, fromAsset, toAsset, displayMode)',
            ticket,
        )
        self.assertIn(
            'normalizeRobinhoodChainQuoteSymbol(row?.symbol) === marketSymbol',
            ticket,
        )
        self.assertIn(
            'String(row?.objective_id || "").trim() === objective',
            ticket,
        )
        self.assertIn(
            'robinhoodChainPair.symbol,\n      robinhoodChainSelectedMarket?.id,',
            ticket,
        )
        self.assertNotIn(
            'function robinhoodChainCapabilityFor(status, fromAsset, toAsset, displayMode)',
            ticket,
        )
        self.assertNotIn(
            'robinhoodChainSelectedCapability?.provider || "0x"',
            ticket,
        )
        self.assertNotIn(
            'const robinhoodChainLegacyExecutionMarket =',
            ticket,
        )
        self.assertNotIn(
            'RH-CHAIN.10D.2 BUY slippage is locked to 1.00%.',
            ticket,
        )
        self.assertNotIn(
            'Fetch a fresh 0x firm quote and display a validated unsigned plan.',
            ticket,
        )
        self.assertNotIn(
            'R5B EXACT-SPEND EXECUTION',
            ticket,
        )
        self.assertNotIn(
            'R5C.5D.2F.2 · SUCCESSFUL FINITE APPROVAL ONLY',
            ticket,
        )
        self.assertIn(
            'EXACT-SPEND EXECUTION REVIEW',
            ticket,
        )
        self.assertIn(
            'FINITE ERC-20 APPROVAL REVIEW',
            ticket,
        )
        self.assertIn(
            'normalizeRobinhoodChainQuoteSymbol(capability?.symbol) === robinhoodChainPair.symbol',
            ticket,
        )
        self.assertIn(
            'String(capability?.objective_id || "").trim() === String(robinhoodChainSelectedMarket?.id || "").trim()',
            ticket,
        )
        self.assertNotIn(
            'EXACT RECEIVE · 0x BLOCKED',
            ticket,
        )
        self.assertNotIn(
            'Prepare 2.00 USDG Approval',
            ticket,
        )
        self.assertIn("interactive_max_usd=interactive_max_usd", (
            root / "backend" / "app" / "routers" / "robinhood_chain.py"
        ).read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
