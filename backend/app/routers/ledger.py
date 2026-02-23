# backend/app/routers/ledger.py

from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..db import get_db
from ..services.lot_sync import sync_lots_from_activity

router = APIRouter(prefix="/api/ledger", tags=["ledger"])


def _sqlite_table_exists(db: Session, name: str) -> bool:
    """
    Defensive helper: your dev DB is SQLite.
    If you later migrate, replace this with SQLAlchemy Inspector.
    """
    try:
        row = db.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name=:n"),
            {"n": name},
        ).scalar()
        return bool(row)
    except Exception:
        return False


@router.post("/reset")
def ledger_reset(
    db: Session = Depends(get_db),
    wallet_id: str = Query(default="default"),
    preview: bool = Query(default=True, description="preview=true returns row counts only; preview=false performs deletion"),
    confirm: bool = Query(default=False, description="Set confirm=true to actually clear derived tables (preview=false only)"),
    confirm_text: Optional[str] = Query(default=None, description='Type RESET to confirm (preview=false only)'),
):
    """Clear derived ledger tables for a wallet so a full rebuild can be run.

    Safety rails:
      - preview=true (default) returns counts, does not delete
      - preview=false requires confirm=true AND confirm_text=RESET

    Clears (wallet-scoped):
      - lot_consumptions (if present)
      - lot_journal
      - basis_lots
    """
    has_consumptions = _sqlite_table_exists(db, "lot_consumptions")

    counts = db.execute(
        text(
            """
            SELECT
              (SELECT COUNT(*) FROM lot_journal WHERE wallet_id=:w) AS journal_rows,
              (SELECT COUNT(*) FROM basis_lots  WHERE wallet_id=:w) AS lot_rows
            """
        ),
        {"w": wallet_id},
    ).mappings().one()

    cons_rows = 0
    if has_consumptions:
        cons_rows = int(
            db.execute(
                text("SELECT COUNT(*) FROM lot_consumptions WHERE wallet_id=:w"),
                {"w": wallet_id},
            ).scalar()
            or 0
        )

    if preview:
        return {
            "ok": True,
            "preview": True,
            "wallet_id": wallet_id,
            **dict(counts),
            "has_lot_consumptions": has_consumptions,
            "lot_consumptions_rows": cons_rows,
        }

    if (not confirm) or ((confirm_text or "").strip().upper() != "RESET"):
        return {
            "ok": False,
            "error": "confirm=true&confirm_text=RESET required when preview=false",
            "wallet_id": wallet_id,
            **dict(counts),
            "has_lot_consumptions": has_consumptions,
            "lot_consumptions_rows": cons_rows,
        }

    # Delete in dependency-safe order:
    # - consumptions may reference lots
    # - journal is independent but derived
    if has_consumptions:
        db.execute(text("DELETE FROM lot_consumptions WHERE wallet_id = :w"), {"w": wallet_id})

    rj = db.execute(text("DELETE FROM lot_journal WHERE wallet_id = :w"), {"w": wallet_id})
    rl = db.execute(text("DELETE FROM basis_lots WHERE wallet_id = :w"), {"w": wallet_id})
    db.commit()

    return {
        "ok": True,
        "preview": False,
        "wallet_id": wallet_id,
        "deleted_lot_consumptions_rows": int(cons_rows or 0) if has_consumptions else 0,
        "deleted_journal_rows": int(getattr(rj, "rowcount", 0) or 0),
        "deleted_lot_rows": int(getattr(rl, "rowcount", 0) or 0),
    }


@router.post("/sync")
def ledger_sync(
    db: Session = Depends(get_db),
    wallet_id: str = Query(default="default"),
    mode: str = Query(default="ALL", description="ALL | LOCAL | VENUE"),
    limit: int = Query(default=500, ge=1, le=5000),
    venue: Optional[str] = Query(default=None),
    symbol_canon: Optional[str] = Query(default=None),
    since: Optional[datetime] = Query(default=None, description="ISO timestamp, e.g. 2021-01-01T00:00:00Z"),
    cursor: Optional[str] = Query(default=None),
    dry_run: bool = Query(default=True),
):
    """
    Lot ledger sync.

    - dry_run=true: no DB mutations (safe preview)
    - dry_run=false: applies lot creation + FIFO sell consumption, idempotent via LotJournal
    """
    result = sync_lots_from_activity(
        db,
        wallet_id=wallet_id,
        mode=mode,
        limit=limit,
        venue=venue,
        symbol_canon=symbol_canon,
        since=since,
        cursor=cursor,
        dry_run=dry_run,
    )
    if not dry_run:
        db.commit()
    return result


@router.post("/sync_all")
def ledger_sync_all(
    db: Session = Depends(get_db),
    wallet_id: str = Query(default="default"),
    mode: str = Query(default="VENUE", description="Typically VENUE for large history"),
    limit: int = Query(default=5000, ge=1, le=5000),
    venue: Optional[str] = Query(default=None),
    symbol_canon: Optional[str] = Query(default=None),
    since: Optional[datetime] = Query(default=None, description="ISO timestamp lower bound"),
    dry_run: bool = Query(default=True),
    max_batches: int = Query(default=50, ge=1, le=500),
):
    """
    Processes more than 5000 rows by looping in batches.

    NOTE: This relies on services/lot_sync.py returning a `next_cursor` for VENUE mode.
    Cursor semantics: resume AFTER cursor (ascending).
    """
    out_batches = []
    cursor = None

    total = {
        "created_lots": 0,
        "consumed_sells": 0,
        "skipped": 0,
        "errors": [],
        "skipped_already_applied": 0,
        "skipped_missing_data": 0,
        "skipped_unknown_side": 0,
    }

    for _ in range(max_batches):
        res = sync_lots_from_activity(
            db,
            wallet_id=wallet_id,
            mode=mode,
            limit=limit,
            venue=venue,
            symbol_canon=symbol_canon,
            since=since,
            cursor=cursor,
            dry_run=dry_run,
        )

        batch = {
            "rows_fetched": int(res.get("rows_fetched") or 0),
            "cursor_in": cursor,
            "cursor_out": res.get("next_cursor"),
            "created_lots": int(res.get("created_lots") or 0),
            "consumed_sells": int(res.get("consumed_sells") or 0),
            "skipped": int(res.get("skipped") or 0),
            "errors": res.get("errors", []) or [],
            "skipped_already_applied": int(res.get("skipped_already_applied") or 0),
            "skipped_missing_data": int(res.get("skipped_missing_data") or 0),
            "skipped_unknown_side": int(res.get("skipped_unknown_side") or 0),
        }
        out_batches.append(batch)

        total["created_lots"] += batch["created_lots"]
        total["consumed_sells"] += batch["consumed_sells"]
        total["skipped"] += batch["skipped"]
        total["errors"].extend(batch["errors"])

        total["skipped_already_applied"] += batch["skipped_already_applied"]
        total["skipped_missing_data"] += batch["skipped_missing_data"]
        total["skipped_unknown_side"] += batch["skipped_unknown_side"]

        cursor = res.get("next_cursor")
        if not cursor:
            break

    if not dry_run:
        db.commit()

    return {
        "mode": mode,
        "wallet_id": wallet_id,
        "dry_run": dry_run,
        "limit": limit,
        "batches": out_batches,
        "total": total,
        "exhausted": cursor is None,
        "final_cursor": cursor,
    }
