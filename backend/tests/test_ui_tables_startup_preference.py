from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy import create_engine

from app.routers import auth as auth_module


class UiTablesStartupPreferenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self._old_engine = auth_module.engine
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp.name) / "startup-preference.sqlite"
        auth_module.engine = create_engine(f"sqlite+pysqlite:///{self.db_path}")

    def tearDown(self) -> None:
        try:
            auth_module.engine.dispose()
        except Exception:
            pass
        auth_module.engine = self._old_engine
        self._tmp.cleanup()

    def test_unset_profile_defaults_to_all_orders_without_persisting(self) -> None:
        loaded = auth_module.get_ui_preferences({"user": "alice", "auth": True})
        self.assertEqual(loaded["tables_startup_tab"], "allOrders")
        self.assertEqual(loaded["sources"]["tables_startup_tab"], "default")

    def test_tables_startup_tab_is_user_scoped_and_persistent(self) -> None:
        saved = auth_module._set_ui_preferences(
            {"user": "alice", "auth": True},
            tables_startup_tab="discover",
        )
        self.assertEqual(saved["tables_startup_tab"], "discover")
        self.assertEqual(saved["sources"]["tables_startup_tab"], "database")

        bob = auth_module.get_ui_preferences({"user": "bob", "auth": True})
        self.assertEqual(bob["tables_startup_tab"], "allOrders")
        self.assertEqual(bob["sources"]["tables_startup_tab"], "default")

        auth_module.engine.dispose()
        auth_module.engine = create_engine(f"sqlite+pysqlite:///{self.db_path}")
        reloaded = auth_module.get_ui_preferences({"user": "alice", "auth": True})
        self.assertEqual(reloaded["tables_startup_tab"], "discover")
        self.assertEqual(reloaded["sources"]["tables_startup_tab"], "database")

    def test_allowed_tabs_and_invalid_values_fail_closed(self) -> None:
        for value in ("allOrders", "balances", "localOrders", "discover"):
            with self.subTest(value=value):
                self.assertEqual(auth_module._normalize_tables_startup_tab_preference(value), value)

        for value in ("", "orders", "ALLORDERS", "unknown"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    auth_module._normalize_tables_startup_tab_preference(value)

    def test_preferences_api_supports_independent_ui_and_robinhood_chain_patches(self) -> None:
        ui_request = auth_module.UserPreferencesPatchRequest(
            ui={"tables_startup_tab": "balances"}
        )
        ui_saved = auth_module.auth_preferences_patch(
            ui_request,
            ident={"user": "profile-user", "auth": True},
        )
        self.assertEqual(ui_saved["ui"]["tables_startup_tab"], "balances")
        self.assertEqual(ui_saved["ui"]["sources"]["tables_startup_tab"], "database")

        rh_request = auth_module.UserPreferencesPatchRequest(
            robinhood_chain={
                "discovery_max_usd": "75.25",
                "interactive_quote_max_usd": "250",
            }
        )
        rh_saved = auth_module.auth_preferences_patch(
            rh_request,
            ident={"user": "profile-user", "auth": True},
        )
        self.assertEqual(rh_saved["ui"]["tables_startup_tab"], "balances")
        self.assertEqual(rh_saved["robinhood_chain"]["discovery_max_usd"], "75.25")
        self.assertEqual(rh_saved["robinhood_chain"]["interactive_quote_max_usd"], "250")

        loaded = auth_module.auth_preferences_get(
            ident={"user": "profile-user", "auth": True}
        )
        self.assertEqual(loaded["ui"]["tables_startup_tab"], "balances")
        self.assertEqual(loaded["robinhood_chain"]["discovery_max_usd"], "75.25")

        with self.assertRaises(HTTPException) as context:
            auth_module.auth_preferences_patch(
                auth_module.UserPreferencesPatchRequest(),
                ident={"user": "profile-user", "auth": True},
            )
        self.assertEqual(context.exception.status_code, 400)

    def test_frontend_contract_defaults_to_all_orders_before_preference_resolution(self) -> None:
        root = Path(__file__).resolve().parents[2]
        app = (root / "frontend" / "src" / "App.jsx").read_text(encoding="utf-8")
        tables = (root / "frontend" / "src" / "TerminalTablesWidget.jsx").read_text(encoding="utf-8")
        profile = (root / "frontend" / "src" / "components" / "AppHeader.jsx").read_text(encoding="utf-8")

        self.assertIn('const APP_TABLES_STARTUP_TAB_DEFAULT = "allOrders";', app)
        self.assertIn('useState(APP_TABLES_STARTUP_TAB_DEFAULT)', app)
        self.assertIn('const [startupTablesTabResolved, setStartupTablesTabResolved] = useState(false);', app)
        self.assertIn('await appFetchJson("/api/auth/preferences")', app)
        self.assertIn('localStorage.getItem("utt_auth_token_v1")', app)
        self.assertIn('fetch(url, { headers: appAuthHeaders(), cache: "no-store" })', app)
        self.assertIn('data-utt-tables-startup-loading="1"', app)
        self.assertIn("Loading saved startup tab…", app)
        self.assertIn('if (!startupTablesTabResolved) return;', app)
        self.assertNotIn('const [tab, setTab] = useState("balances");', app)
        self.assertIn('const tabKey = String(tab || "allOrders");', tables)
        self.assertIn('Startup & Layout', profile)
        self.assertIn('Startup Tables Tab', profile)
        self.assertIn('ui: { tables_startup_tab:', profile)
        self.assertIn('value="allOrders">All Orders', profile)


if __name__ == "__main__":
    unittest.main()
