from __future__ import annotations

import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from sqlalchemy.exc import OperationalError

from app.services import balances as balances_service


class _FakeSession:
    def __init__(self, commit_errors=None):
        self.commit_errors = list(commit_errors or [])
        self.commit_calls = 0
        self.rollback_calls = 0
        self.batches = []

    def add_all(self, rows):
        self.batches.append(list(rows))

    def commit(self):
        self.commit_calls += 1
        if self.commit_errors:
            error = self.commit_errors.pop(0)
            if error is not None:
                raise error

    def rollback(self):
        self.rollback_calls += 1


def _locked_error(message: str = "database is locked") -> OperationalError:
    return OperationalError("INSERT INTO balance_snapshots ...", {}, Exception(message))


class GeminiPostSubmitHardeningTests(unittest.TestCase):
    def test_balance_snapshot_commit_retries_sqlite_lock(self):
        sleeps = []
        db = _FakeSession([_locked_error(), None])

        with (
            patch.object(balances_service, "_BAL_DB_LOCK_RETRY_ATTEMPTS", 2),
            patch.object(balances_service, "_BAL_DB_LOCK_RETRY_BASE_MS", 25),
            patch.object(balances_service.time, "sleep", side_effect=lambda seconds: sleeps.append(seconds)),
        ):
            rows = balances_service._persist_balance_snapshot_rows(
                db,
                venue="gemini",
                captured_at=datetime(2026, 7, 24, 12, 0, 0),
                rows=[{"asset": "DOT", "total": 2.0, "available": 1.5, "hold": 0.5}],
            )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].venue, "gemini")
        self.assertEqual(rows[0].asset, "DOT")
        self.assertEqual(db.commit_calls, 2)
        self.assertEqual(db.rollback_calls, 1)
        self.assertEqual(len(db.batches), 2)
        self.assertIsNot(db.batches[0][0], db.batches[1][0])
        self.assertEqual(sleeps, [0.025])

    def test_balance_snapshot_commit_does_not_retry_non_lock_error(self):
        db = _FakeSession([_locked_error("disk I/O error")])

        with (
            patch.object(balances_service, "_BAL_DB_LOCK_RETRY_ATTEMPTS", 3),
            patch.object(
                balances_service.time,
                "sleep",
                side_effect=AssertionError("unexpected retry sleep"),
            ),
            self.assertRaises(OperationalError),
        ):
            balances_service._persist_balance_snapshot_rows(
                db,
                venue="gemini",
                captured_at=datetime(2026, 7, 24, 12, 0, 0),
                rows=[{"asset": "USD", "total": 1.0, "available": 1.0, "hold": 0.0}],
            )

        self.assertEqual(db.commit_calls, 1)
        self.assertEqual(db.rollback_calls, 1)

    def test_frontend_heavy_writers_share_one_cross_tab_key(self):
        repo_root = Path(__file__).resolve().parents[2]
        app_source = (repo_root / "frontend" / "src" / "App.jsx").read_text(encoding="utf-8")
        ticket_source = (repo_root / "frontend" / "src" / "OrderTicketWidget.jsx").read_text(encoding="utf-8")

        key_literal = '"sqlite-heavy-write-v1"'
        self.assertIn(f"const SQLITE_HEAVY_WRITE_TASK_KEY = {key_literal};", app_source)
        self.assertIn(f"const CEX_POST_SUBMIT_SQLITE_TASK_KEY = {key_literal};", ticket_source)
        self.assertGreaterEqual(app_source.count("SQLITE_HEAVY_WRITE_TASK_KEY"), 4)
        self.assertIn("doSyncAndLoadAllOrders({ includeLedgerSync: true })", app_source)
        self.assertIn("setSubmitting(false);\n    setPostSubmitSyncing(true);", ticket_source)
        self.assertIn('postSubmitSyncing\n                ? "Synchronizing…"', ticket_source)

    def test_venue_order_refresh_releases_read_transaction_and_batches_commits(self):
        repo_root = Path(__file__).resolve().parents[2]
        source = (repo_root / "backend" / "app" / "services" / "venue_orders.py").read_text(encoding="utf-8")

        release_pos = source.index("db.rollback()", source.index("open_ids_for_detail"))
        network_pos = source.index("adapter.fetch_open_orders")
        self.assertLess(release_pos, network_pos)
        self.assertIn('venue_orders_commit_batch_size", 25', source)
        self.assertIn("if pending_changes >= commit_batch_size:", source)
        self.assertIn("if pending_changes:\n                db.commit()", source)


if __name__ == "__main__":
    unittest.main()
