# backend/app/services/reconcile.py

from __future__ import annotations

from typing import Any, Dict, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text

from ..utils import now_utc

_TERMINAL = {"filled", "canceled", "rejected", "expired", "failed"}


def _norm_status(s: Any) -> Optional[str]:
    if s is None:
        return None
    st = str(s).strip().lower()
    if not st:
        return None

    # Open-ish
    if st in ("open", "pending", "new", "live", "active", "accepted", "partially_filled", "pending_cancel"):
        return "open"

    # Partial
    if st in ("partial", "partial_fill", "partial-filled", "partial fill", "partially_filled"):
        return "open"  # local schema doesn’t have "partial" today; treat as open

    # Filled
    if st in ("filled", "done", "completed", "settled", "closed"):
        return "filled"

    # Canceled
    if st in ("canceled", "cancelled", "canceling", "cancel"):
        return "canceled"

    # Expired
    if st in ("expired",):
        return "expired"

    # Rejected/failed
    if st in ("rejected", "failed", "error"):
        return "rejected"

    return st


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def reconcile_local_orders_from_venue_snapshots(db: Session, venue: str) -> int:
    v = (venue or "").strip().lower()
    if not v:
        return 0

    rows = db.execute(
        text(
            """
            SELECT
              venue_order_id,
              status,
              filled_qty,
              avg_fill_price,
              updated_at,
              captured_at
            FROM venue_orders
            WHERE lower(venue) = :venue
            ORDER BY captured_at DESC
            """
        ),
        {"venue": v},
    ).fetchall()

    latest: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        oid = str(r[0] or "").strip()
        if not oid:
            continue
        if oid in latest:
            continue
        latest[oid] = {
            "status": r[1],
            "filled_qty": r[2],
            "avg_fill_price": r[3],
            "updated_at": r[4],
            "captured_at": r[5],
        }

    if not latest:
        return 0

    local_rows = db.execute(
        text(
            """
            SELECT
              id,
              venue_order_id,
              status,
              filled_qty,
              avg_fill_price,
              updated_at
            FROM orders
            WHERE lower(venue) = :venue
              AND venue_order_id IS NOT NULL
              AND trim(venue_order_id) <> ''
            """
        ),
        {"venue": v},
    ).fetchall()

    if not local_rows:
        return 0

    updated = 0
    now = now_utc()

    for lr in local_rows:
        order_id = str(lr[0])
        venue_oid = str(lr[1] or "").strip()
        if not venue_oid:
            continue

        snap = latest.get(venue_oid)
        if not snap:
            continue

        snap_status = _norm_status(snap.get("status"))
        if not snap_status:
            continue

        local_status = _norm_status(lr[2]) or "open"
        local_filled = _to_float(lr[3]) or 0.0
        local_avg = _to_float(lr[4])

        snap_filled = _to_float(snap.get("filled_qty"))
        snap_avg = _to_float(snap.get("avg_fill_price"))

        status_changed = False
        filled_changed = False
        avg_changed = False

        if snap_status in _TERMINAL:
            if local_status != snap_status:
                local_status = snap_status
                status_changed = True
        else:
            if local_status in _TERMINAL:
                pass
            else:
                if local_status != snap_status:
                    local_status = snap_status
                    status_changed = True

        if snap_filled is not None and snap_filled >= 0:
            if snap_filled > local_filled + 1e-12:
                local_filled = snap_filled
                filled_changed = True

        if snap_avg is not None and snap_avg > 0:
            if local_avg is None or local_avg == 0:
                local_avg = snap_avg
                avg_changed = True

        if status_changed or filled_changed or avg_changed:
            db.execute(
                text(
                    """
                    UPDATE orders
                    SET
                      status = :status,
                      filled_qty = :filled_qty,
                      avg_fill_price = :avg_fill_price,
                      updated_at = :updated_at
                    WHERE id = :id
                    """
                ),
                {
                    "id": order_id,
                    "status": local_status,
                    "filled_qty": float(local_filled),
                    "avg_fill_price": (float(local_avg) if local_avg is not None else None),
                    "updated_at": now,
                },
            )
            updated += 1

    if updated:
        db.commit()

    return updated
