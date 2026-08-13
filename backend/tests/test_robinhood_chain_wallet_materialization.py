from __future__ import annotations

import unittest
from datetime import datetime
from unittest.mock import patch

from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from app.models import (
    BasisLot,
    RobinhoodChainBuyExecution,
    RobinhoodChainExecution,
    RobinhoodChainExternalSwap,
    RobinhoodChainSwapExecution,
    RobinhoodChainWalletCheckpoint,
    RobinhoodChainWalletEvent,
    TokenRegistry,
    VenueOrderRow,
    WalletAddress,
)
from app.models_lot_journal import LotJournal
from app.services import robinhood_chain_wallet_materialization as materialize_mod
from app.services.all_orders import _to_unified_robinhood_chain_external_swap
from app.services.lot_sync import sync_lots_from_activity


WALLET = "0x" + "a" * 40
ROUTER = "0x" + "b" * 40
TOKEN = "0x" + "c" * 40
COUNTERPARTY = "0x" + "d" * 40
TX_HASH = "0x" + "1" * 64


def _session(*, with_lot_tables: bool = False):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    for table in (
        WalletAddress.__table__,
        RobinhoodChainWalletEvent.__table__,
        RobinhoodChainWalletCheckpoint.__table__,
        RobinhoodChainExternalSwap.__table__,
        TokenRegistry.__table__,
        RobinhoodChainExecution.__table__,
        RobinhoodChainBuyExecution.__table__,
        RobinhoodChainSwapExecution.__table__,
    ):
        table.create(engine)
    if with_lot_tables:
        for table in (
            VenueOrderRow.__table__,
            BasisLot.__table__,
            LotJournal.__table__,
        ):
            table.create(engine)
    Session = sessionmaker(
        bind=engine,
        expire_on_commit=False,
        autoflush=False,
        future=True,
    )
    return engine, Session()


def _wallet(db):
    row = WalletAddress(
        asset="ALL",
        network="robinhood_chain",
        wallet_id="robinhood_chain",
        address=WALLET,
        owner_scope="user",
    )
    db.add(row)
    db.flush()
    return row


def _swap_events(
    db,
    wallet,
    *,
    tx_hash: str = TX_HASH,
    output_registered: bool = True,
    output_registry_id: int | None = 7,
    output_asset: str = "TEST",
    output_amount_atomic: str = "2000000",
):
    tx_time = datetime(2026, 8, 10, 1, 2, 3)
    out_row = RobinhoodChainWalletEvent(
        wallet_address_id=wallet.id,
        chain_id=4663,
        event_key=f"{tx_hash}:transaction",
        transaction_hash=tx_hash,
        event_type="transaction",
        block_number=123,
        tx_time=tx_time,
        status="ok",
        classification="contract_call",
        direction="out",
        asset="ETH",
        amount_atomic="600000000000000",
        decimals=18,
        fee_wei="4759227966000",
        contract_address=ROUTER,
        registry_id=None,
        registered=False,
        from_address=WALLET,
        to_address=ROUTER,
        source="blockscout_v2",
        raw={"synthetic": True},
    )
    in_row = RobinhoodChainWalletEvent(
        wallet_address_id=wallet.id,
        chain_id=4663,
        event_key=f"{tx_hash}:erc20:9",
        transaction_hash=tx_hash,
        event_type="erc20_transfer",
        log_index="9",
        block_number=123,
        tx_time=tx_time,
        status="ok",
        classification="erc20_transfer",
        direction="in",
        asset=output_asset,
        amount_atomic=output_amount_atomic,
        decimals=6,
        fee_wei="0",
        contract_address=TOKEN,
        registry_id=output_registry_id,
        registered=output_registered,
        from_address=COUNTERPARTY,
        to_address=WALLET,
        source="blockscout_v2",
        raw={"synthetic": True},
    )
    db.add_all([out_row, in_row])
    if output_registered and output_registry_id is not None:
        db.add(
            TokenRegistry(
                id=int(output_registry_id),
                chain="robinhood_chain",
                venue=None,
                symbol=output_asset,
                address=TOKEN,
                decimals=6,
                label="Synthetic test token",
            )
        )
    db.flush()
    return out_row, in_row


def _checkpoint(db, wallet, *, fully_backfilled: bool = True):
    event_count = (
        db.query(RobinhoodChainWalletEvent)
        .filter(RobinhoodChainWalletEvent.wallet_address_id == wallet.id)
        .count()
    )
    tx_count = len({
        value
        for value, in (
            db.query(RobinhoodChainWalletEvent.transaction_hash)
            .filter(RobinhoodChainWalletEvent.wallet_address_id == wallet.id)
            .distinct()
            .all()
        )
        if value
    })
    row = RobinhoodChainWalletCheckpoint(
        wallet_address_id=wallet.id,
        chain_id=4663,
        newest_block_number=123,
        oldest_block_number=123,
        fully_backfilled=fully_backfilled,
        event_count=event_count,
        transaction_count=tx_count,
        last_scan_at=datetime(2026, 8, 10, 1, 2, 4),
    )
    db.add(row)
    db.flush()
    return row


class RobinhoodChainWalletMaterializationTests(unittest.TestCase):
    def test_native_eth_transaction_call_target_is_not_treated_as_token_identity(self):
        engine, db = _session()
        self.addCleanup(engine.dispose)
        self.addCleanup(db.close)
        wallet = _wallet(db)
        _swap_events(db, wallet)
        _checkpoint(db, wallet)
        db.commit()

        preview = materialize_mod.preview_robinhood_chain_wallet_materialization(
            db,
            wallet_address_id=wallet.id,
        )

        self.assertTrue(preview["ok"])
        self.assertTrue(preview["summary"]["ready_for_materialization"])
        self.assertEqual(preview["summary"]["external_swap_ready"], 1)
        self.assertEqual(preview["summary"]["external_asset_pairs"], {"TEST-ETH": 1})
        candidate = materialize_mod._classification_snapshot(db, wallet)["candidates"][0]
        self.assertEqual(candidate["input_asset"], "ETH")
        self.assertEqual(candidate["output_asset"], "TEST")
        self.assertEqual(candidate["input_amount_atomic"], "600000000000000")
        self.assertEqual(candidate["output_amount_atomic"], "2000000")
        self.assertTrue(candidate["evidence_summary"]["call_target_present"])

    def test_unregistered_erc20_output_is_not_materializable(self):
        engine, db = _session()
        self.addCleanup(engine.dispose)
        self.addCleanup(db.close)
        wallet = _wallet(db)
        _swap_events(
            db,
            wallet,
            output_registered=False,
            output_registry_id=None,
        )
        _checkpoint(db, wallet)
        db.commit()

        preview = materialize_mod.preview_robinhood_chain_wallet_materialization(
            db,
            wallet_address_id=wallet.id,
        )
        self.assertEqual(preview["summary"]["external_swap_ready"], 0)
        self.assertEqual(preview["summary"]["quarantined_groups"], 1)

    def test_known_utt_swap_hash_is_reused_and_never_externalized(self):
        engine, db = _session()
        self.addCleanup(engine.dispose)
        self.addCleanup(db.close)
        wallet = _wallet(db)
        _swap_events(db, wallet)
        _checkpoint(db, wallet)
        db.commit()

        with patch.object(
            materialize_mod,
            "_known_lifecycle_hashes",
            return_value=({TX_HASH.lower()}, set()),
        ):
            preview = materialize_mod.preview_robinhood_chain_wallet_materialization(
                db,
                wallet_address_id=wallet.id,
            )
            result = materialize_mod.materialize_robinhood_chain_wallet_history(
                db,
                wallet_address_id=wallet.id,
            )

        self.assertEqual(preview["summary"]["known_utt_swap_groups"], 1)
        self.assertEqual(preview["summary"]["external_swap_ready"], 0)
        self.assertTrue(result["ok"])
        self.assertEqual(result["created_external_swaps"], 0)
        self.assertEqual(db.query(RobinhoodChainExternalSwap).count(), 0)

    def test_materialization_is_idempotent_for_identical_evidence(self):
        engine, db = _session()
        self.addCleanup(engine.dispose)
        self.addCleanup(db.close)
        wallet = _wallet(db)
        _swap_events(db, wallet)
        _checkpoint(db, wallet)
        db.commit()

        first = materialize_mod.materialize_robinhood_chain_wallet_history(
            db,
            wallet_address_id=wallet.id,
        )
        db.commit()
        second = materialize_mod.materialize_robinhood_chain_wallet_history(
            db,
            wallet_address_id=wallet.id,
        )
        db.commit()

        self.assertTrue(first["ok"])
        self.assertEqual(first["created_external_swaps"], 1)
        self.assertEqual(second["created_external_swaps"], 0)
        self.assertEqual(second["unchanged_external_swaps"], 1)
        self.assertEqual(db.query(RobinhoodChainExternalSwap).count(), 1)

    def test_materialization_blocks_when_checkpoint_is_not_complete(self):
        engine, db = _session()
        self.addCleanup(engine.dispose)
        self.addCleanup(db.close)
        wallet = _wallet(db)
        _swap_events(db, wallet)
        _checkpoint(db, wallet, fully_backfilled=False)
        db.commit()

        result = materialize_mod.materialize_robinhood_chain_wallet_history(
            db,
            wallet_address_id=wallet.id,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(
            result["error"],
            "robinhood_chain_wallet_materialization_checkpoint_incomplete",
        )
        self.assertEqual(db.query(RobinhoodChainExternalSwap).count(), 0)

    def test_external_swap_unified_all_orders_row_uses_actual_observed_values(self):
        engine, db = _session()
        self.addCleanup(engine.dispose)
        self.addCleanup(db.close)
        wallet = _wallet(db)
        _swap_events(db, wallet)
        _checkpoint(db, wallet)
        db.commit()
        result = materialize_mod.materialize_robinhood_chain_wallet_history(
            db,
            wallet_address_id=wallet.id,
        )
        db.commit()
        self.assertEqual(result["created_external_swaps"], 1)

        row = db.query(RobinhoodChainExternalSwap).one()
        unified = _to_unified_robinhood_chain_external_swap(row)
        self.assertEqual(unified["venue"], "robinhood_chain")
        self.assertEqual(unified["symbol"], "TEST-ETH")
        self.assertEqual(unified["side"], "buy")
        self.assertEqual(unified["status"], "confirmed")
        self.assertAlmostEqual(unified["filled_qty"], 2.0)
        self.assertAlmostEqual(unified["avg_fill_price"], 0.0006 / 2.0)
        self.assertAlmostEqual(unified["fee"], 0.000004759227966)
        self.assertEqual(unified["actual_input_asset"], "ETH")
        self.assertEqual(unified["actual_output_asset"], "TEST")
        self.assertTrue(unified["external_history"])

    def test_external_swap_lot_sync_creates_missing_basis_buy_lot_once(self):
        engine, db = _session(with_lot_tables=True)
        self.addCleanup(engine.dispose)
        self.addCleanup(db.close)
        wallet = _wallet(db)
        _swap_events(db, wallet)
        _checkpoint(db, wallet)
        db.commit()
        materialize_mod.materialize_robinhood_chain_wallet_history(
            db,
            wallet_address_id=wallet.id,
        )
        db.commit()

        first = sync_lots_from_activity(
            db,
            wallet_id="default",
            mode="VENUE",
            venue="robinhood_chain",
            symbol_canon="TEST-ETH",
            limit=500,
            dry_run=False,
        )
        db.commit()
        self.assertEqual(first["robinhood_chain_external_rows_fetched"], 1)
        self.assertEqual(first["created_lots"], 1)
        lot = db.execute(select(BasisLot)).scalars().one()
        self.assertEqual(lot.asset, "TEST")
        self.assertAlmostEqual(lot.qty_total, 2.0)
        self.assertTrue(lot.basis_is_missing)
        self.assertIsNone(lot.total_basis_usd)
        journal = db.execute(select(LotJournal)).scalars().one()
        self.assertEqual(journal.origin_type, "RH_CHAIN_EXTERNAL_SWAP")
        self.assertTrue(journal.applied)

        second = sync_lots_from_activity(
            db,
            wallet_id="default",
            mode="VENUE",
            venue="robinhood_chain",
            symbol_canon="TEST-ETH",
            limit=500,
            dry_run=False,
        )
        db.commit()
        self.assertEqual(second["robinhood_chain_external_rows_fetched"], 0)
        self.assertEqual(second["created_lots"], 0)
        self.assertEqual(len(db.execute(select(BasisLot)).scalars().all()), 1)
        self.assertEqual(len(db.execute(select(LotJournal)).scalars().all()), 1)


if __name__ == "__main__":
    unittest.main()
