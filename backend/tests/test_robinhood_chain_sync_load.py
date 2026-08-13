from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.models import RobinhoodChainWalletCheckpoint, WalletAddress
from app.services import robinhood_chain_wallet_sync as sync_mod


WALLET = "0x" + "a" * 40


def _session(*, fully_backfilled: bool | None = True):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    WalletAddress.__table__.create(engine)
    RobinhoodChainWalletCheckpoint.__table__.create(engine)
    Session = sessionmaker(
        bind=engine,
        expire_on_commit=False,
        autoflush=False,
        future=True,
    )
    db = Session()
    wallet = WalletAddress(
        asset="ALL",
        network="robinhood_chain",
        wallet_id="robinhood_chain",
        address=WALLET,
        owner_scope="user",
    )
    db.add(wallet)
    db.flush()
    if fully_backfilled is not None:
        db.add(
            RobinhoodChainWalletCheckpoint(
                wallet_address_id=wallet.id,
                chain_id=4663,
                fully_backfilled=bool(fully_backfilled),
                event_count=929,
                transaction_count=190,
            )
        )
        db.flush()
    return engine, db, wallet


class RobinhoodChainSyncLoadTests(unittest.IsolatedAsyncioTestCase):
    async def test_sync_load_requires_completed_historical_backfill(self):
        engine, db, _wallet = _session(fully_backfilled=None)
        self.addCleanup(engine.dispose)
        self.addCleanup(db.close)

        ingest = AsyncMock()
        with patch.object(sync_mod, "ingest_robinhood_chain_wallet_history", ingest):
            result = await sync_mod.sync_robinhood_chain_wallet_incremental(db)

        self.assertFalse(result["ok"])
        self.assertEqual(
            result["error"],
            "robinhood_chain_wallet_sync_requires_completed_backfill",
        )
        self.assertEqual(result["mode"], "incremental")
        self.assertFalse(result["full_backfill_requested"])
        self.assertFalse(result["provider_contacted"])
        ingest.assert_not_awaited()

    async def test_sync_load_always_calls_incremental_ingest_then_materialization(self):
        engine, db, wallet = _session(fully_backfilled=True)
        self.addCleanup(engine.dispose)
        self.addCleanup(db.close)

        ingest_result = {
            "ok": True,
            "created_events": 2,
            "updated_events": 1,
            "unchanged_events": 12,
            "summary": {
                "mode": "incremental",
                "checkpoint": {
                    "fully_backfilled": True,
                    "event_count": 931,
                    "transaction_count": 191,
                },
            },
        }
        materialization_result = {
            "ok": True,
            "created_external_swaps": 1,
            "updated_external_swaps": 0,
            "unchanged_external_swaps": 44,
            "summary": {
                "external_swap_existing": 45,
                "external_swap_new": 0,
            },
        }

        ingest = AsyncMock(return_value=ingest_result)
        with (
            patch.object(sync_mod, "ingest_robinhood_chain_wallet_history", ingest),
            patch.object(
                sync_mod,
                "materialize_robinhood_chain_wallet_history",
                return_value=materialization_result,
            ) as materialize,
        ):
            result = await sync_mod.sync_robinhood_chain_wallet_incremental(db)

        self.assertTrue(result["ok"])
        self.assertEqual(result["mode"], "incremental")
        self.assertFalse(result["full_backfill_requested"])
        self.assertEqual(result["created_events"], 2)
        self.assertEqual(result["created_external_swaps"], 1)
        self.assertFalse(result["ledger_mutation"])
        self.assertFalse(result["fifo_mutation"])
        self.assertFalse(result["basis_mutation"])
        ingest.assert_awaited_once_with(
            db,
            wallet_address_id=str(wallet.id),
            force_full=False,
            force_refresh=True,
        )
        materialize.assert_called_once_with(
            db,
            wallet_address_id=str(wallet.id),
        )

    async def test_ingest_failure_stops_before_materialization(self):
        engine, db, _wallet = _session(fully_backfilled=True)
        self.addCleanup(engine.dispose)
        self.addCleanup(db.close)

        ingest = AsyncMock(
            return_value={
                "ok": False,
                "error": "robinhood_chain_wallet_ingest_incomplete_scan",
                "database_mutation": False,
            }
        )
        with (
            patch.object(sync_mod, "ingest_robinhood_chain_wallet_history", ingest),
            patch.object(sync_mod, "materialize_robinhood_chain_wallet_history") as materialize,
        ):
            result = await sync_mod.sync_robinhood_chain_wallet_incremental(db)

        self.assertFalse(result["ok"])
        self.assertEqual(result["stage"], "ingest")
        self.assertEqual(
            result["error"],
            "robinhood_chain_wallet_ingest_incomplete_scan",
        )
        materialize.assert_not_called()

    async def test_frontend_sync_load_wires_incremental_rh_chain_endpoint(self):
        repo_root = Path(__file__).resolve().parents[2]
        app_source = (repo_root / "frontend" / "src" / "App.jsx").read_text(encoding="utf-8")
        api_source = (repo_root / "frontend" / "src" / "lib" / "api.js").read_text(encoding="utf-8")

        self.assertIn("syncRobinhoodChainWalletIncremental", app_source)
        self.assertIn("shouldSyncRobinhoodChainWallet", app_source)
        self.assertIn('v === ROBINHOOD_CHAIN_VENUE', app_source)
        self.assertIn("await syncRobinhoodChainWalletIncremental();", app_source)
        self.assertIn("/api/robinhood_chain/wallet-sync/incremental", api_source)
        self.assertNotIn("force_full=true", api_source)

    async def test_frontend_explicit_read_only_rh_venue_is_not_gated_by_trading_candidates(self):
        repo_root = Path(__file__).resolve().parents[2]
        app_source = (repo_root / "frontend" / "src" / "App.jsx").read_text(encoding="utf-8")

        self.assertIn('READ_ONLY_ORDER_TICKET_VENUES = new Set(["counterparty", ROBINHOOD_CHAIN_VENUE])', app_source)
        self.assertIn("const shouldSyncRobinhoodChainWallet =", app_source)
        self.assertIn('v === ROBINHOOD_CHAIN_VENUE', app_source)
        self.assertIn('(wantsAllVenues && supportedVenues.includes(ROBINHOOD_CHAIN_VENUE))', app_source)
        self.assertIn("if (shouldSyncRobinhoodChainWallet)", app_source)
        self.assertNotIn('if (scopeNorm !== "LOCAL" && venuesToRefresh.includes("robinhood_chain"))', app_source)

    async def test_frontend_sync_load_rh_scan_survives_general_heavy_task_follower(self):
        repo_root = Path(__file__).resolve().parents[2]
        app_source = (repo_root / "frontend" / "src" / "App.jsx").read_text(encoding="utf-8")

        general_pos = app_source.index("const syncResult = await runCrossTabHeavyTask(")
        rh_key_pos = app_source.index("robinhood_chain_wallet_incremental", general_pos)
        rh_task_pos = app_source.index("const rhWalletSyncResult = await runCrossTabHeavyTask(", general_pos)
        rh_call_pos = app_source.index("await syncRobinhoodChainWalletIncremental();", rh_task_pos)

        self.assertGreater(rh_key_pos, general_pos)
        self.assertGreater(rh_task_pos, general_pos)
        self.assertGreater(rh_call_pos, rh_task_pos)
        self.assertIn("while (syncInFlightRef.current && Date.now() < waitDeadline)", app_source)
        self.assertIn("await doLedgerSyncFromLocalStorage", app_source[rh_call_pos:])



if __name__ == "__main__":
    unittest.main()
