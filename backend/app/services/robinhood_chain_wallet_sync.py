from __future__ import annotations

from typing import Any, Dict

from sqlalchemy.orm import Session

from ..models import RobinhoodChainWalletCheckpoint, WalletAddress
from .evm_rpc import validate_evm_address
from .robinhood_chain_wallet_ingest import ingest_robinhood_chain_wallet_history
from .robinhood_chain_wallet_materialization import materialize_robinhood_chain_wallet_history


_EXPECTED_CHAIN_ID = 4663
_NETWORK = "robinhood_chain"
_WALLET_ID = "robinhood_chain"


def _saved_wallet(db: Session) -> WalletAddress:
    row = (
        db.query(WalletAddress)
        .filter(
            WalletAddress.network == _NETWORK,
            WalletAddress.wallet_id == _WALLET_ID,
            WalletAddress.asset.in_(["ALL", "*"]),
        )
        .order_by(WalletAddress.created_at.desc())
        .first()
    )
    if row is None:
        raise ValueError("robinhood_chain_wallet_address_not_found")
    validate_evm_address(str(row.address or "").strip())
    return row


def _checkpoint(db: Session, wallet_id: str) -> RobinhoodChainWalletCheckpoint | None:
    return (
        db.query(RobinhoodChainWalletCheckpoint)
        .filter(RobinhoodChainWalletCheckpoint.wallet_address_id == str(wallet_id))
        .first()
    )


async def sync_robinhood_chain_wallet_incremental(db: Session) -> Dict[str, Any]:
    """Incrementally ingest and materialize saved RH Chain wallet history.

    This is the orchestration used by All Orders Sync+Load after the historical
    full backfill has already been accepted. It never requests a full scan.
    Evidence persistence and external-swap materialization share one caller
    transaction so a downstream materialization failure can be rolled back by
    the router before either stage is committed.
    """
    wallet = _saved_wallet(db)
    checkpoint_before = _checkpoint(db, str(wallet.id))
    if checkpoint_before is None or not bool(checkpoint_before.fully_backfilled):
        return {
            "ok": False,
            "error": "robinhood_chain_wallet_sync_requires_completed_backfill",
            "message": "Complete the Robinhood Chain historical Wallet Addresses backfill before using All Orders Sync+Load ingestion.",
            "wallet_address_id": str(wallet.id),
            "mode": "incremental",
            "full_backfill_requested": False,
            "provider_contacted": False,
            "database_mutation": False,
            "ledger_mutation": False,
            "fifo_mutation": False,
            "basis_mutation": False,
            "will_mutate": False,
        }

    ingest = await ingest_robinhood_chain_wallet_history(
        db,
        wallet_address_id=str(wallet.id),
        force_full=False,
        force_refresh=True,
    )
    if ingest.get("ok") is not True:
        return {
            "ok": False,
            "error": str(ingest.get("error") or "robinhood_chain_incremental_ingest_failed"),
            "message": ingest.get("message"),
            "wallet_address_id": str(wallet.id),
            "mode": "incremental",
            "full_backfill_requested": False,
            "stage": "ingest",
            "ingest": ingest,
            "database_mutation": False,
            "ledger_mutation": False,
            "fifo_mutation": False,
            "basis_mutation": False,
            "will_mutate": False,
        }

    # SessionLocal uses autoflush=False. Make checkpoint/evidence from the
    # incremental ingest authoritative to the classifier in this same unit of
    # work before materialization queries run.
    db.flush()

    materialization = materialize_robinhood_chain_wallet_history(
        db,
        wallet_address_id=str(wallet.id),
    )
    if materialization.get("ok") is not True:
        return {
            "ok": False,
            "error": str(materialization.get("error") or "robinhood_chain_incremental_materialization_failed"),
            "message": materialization.get("message"),
            "wallet_address_id": str(wallet.id),
            "mode": "incremental",
            "full_backfill_requested": False,
            "stage": "materialization",
            "ingest": ingest,
            "materialization": materialization,
            "database_mutation": False,
            "ledger_mutation": False,
            "fifo_mutation": False,
            "basis_mutation": False,
            "will_mutate": False,
        }

    db.flush()

    checkpoint_after = _checkpoint(db, str(wallet.id))
    checkpoint_summary = None
    if checkpoint_after is not None:
        checkpoint_summary = {
            "fully_backfilled": bool(checkpoint_after.fully_backfilled),
            "event_count": int(checkpoint_after.event_count or 0),
            "transaction_count": int(checkpoint_after.transaction_count or 0),
            "newest_block_number": checkpoint_after.newest_block_number,
            "oldest_block_number": checkpoint_after.oldest_block_number,
        }

    created_events = int(ingest.get("created_events") or 0)
    updated_events = int(ingest.get("updated_events") or 0)
    created_external = int(materialization.get("created_external_swaps") or 0)
    updated_external = int(materialization.get("updated_external_swaps") or 0)

    return {
        "ok": True,
        "tranche": "RH-WALLET.INGEST.1E-R1",
        "wallet_address_id": str(wallet.id),
        "chain_id": _EXPECTED_CHAIN_ID,
        "mode": "incremental",
        "full_backfill_requested": False,
        "created_events": created_events,
        "updated_events": updated_events,
        "unchanged_events": int(ingest.get("unchanged_events") or 0),
        "created_external_swaps": created_external,
        "updated_external_swaps": updated_external,
        "unchanged_external_swaps": int(materialization.get("unchanged_external_swaps") or 0),
        "checkpoint": checkpoint_summary,
        "ingest_summary": dict(ingest.get("summary") or {}),
        "materialization_summary": dict(materialization.get("summary") or {}),
        "checkpoint_mutation": True,
        "database_mutation": True,
        "orders_table_mutation": False,
        "all_orders_visibility_mutation": bool(created_external or updated_external),
        "ledger_mutation": False,
        "fifo_mutation": False,
        "basis_mutation": False,
        "token_registry_mutation": False,
        "metamask_request": False,
        "signing": False,
        "broadcast": False,
        "will_mutate": True,
        "next_stage": "RH-WALLET.INGEST.1E-ACCEPTANCE",
    }
