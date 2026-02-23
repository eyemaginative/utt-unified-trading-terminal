# backend/app/services/orders_sync.py

from __future__ import annotations

from sqlalchemy.orm import Session
from typing import Tuple

from ..models import Order
from ..utils import now_utc
from .symbols import get_adapter
from ..config import settings


def _effective_dry_run() -> bool:
    return settings.dry_run or (not settings.armed)


def refresh_venue_orders(db: Session, venue: str) -> Tuple[int, int]:
    """
    Pull orders from venue and upsert into local Order table.
    Returns: (upserted_count, total_fetched)

    NOTE:
    - This module appears to be legacy/alternate to backend/app/services/venue_orders.py
      (which is where the Robinhood stale-open fix lives).
    - We intentionally do NOT implement any "close missing from open snapshot" behavior
      here, because (a) this file may not be used, and (b) closing without per-id detail
      can misclassify FILLED vs CANCELED.
    """
    adapter = get_adapter(venue)

    fetched = adapter.fetch_orders(dry_run=_effective_dry_run()) or []
    total = len(fetched)
    upserted = 0
    now = now_utc()

    for vo in fetched:
        if not isinstance(vo, dict):
            continue

        # Robust id mapping (different adapters use different keys)
        external_id = (
            (vo.get("external_order_id") or "")
            or (vo.get("venue_order_id") or "")
            or (vo.get("order_id") or "")
            or (vo.get("id") or "")
        )
        external_id = str(external_id).strip()
        if not external_id:
            continue

        # Upsert key: (venue, external_order_id, source_name)
        existing = (
            db.query(Order)
            .filter(
                Order.venue == venue,
                Order.external_order_id == external_id,
                Order.source_name == venue.upper(),
            )
            .first()
        )

        # Prefer canonical status fields if present
        status = (vo.get("status") or "").strip() or "unknown"
        status_raw = vo.get("status_raw") or vo.get("raw_status")

        if existing is None:
            o = Order(
                client_order_id=f"venue-{venue}-{external_id}",  # unique placeholder
                source="venue",
                source_name=venue.upper(),
                external_order_id=external_id,
                venue=venue,
                symbol_canon=vo.get("symbol_canon") or "",
                symbol_venue=vo.get("symbol_venue") or "",
                side=vo.get("side") or "",
                type=vo.get("type") or "",
                qty=float(vo.get("qty") or 0.0),
                limit_price=vo.get("limit_price"),
                status=status,
                raw_status=status_raw,
                filled_qty=float(vo.get("filled_qty") or 0.0),
                avg_fill_price=vo.get("avg_fill_price"),
                fee_total=vo.get("fee_total") or vo.get("fee"),
                fee_asset=vo.get("fee_asset"),
                gross_total=vo.get("gross_total"),
                net_total_after_fee=vo.get("net_total_after_fee") or vo.get("total_after_fee"),
                venue_order_id=external_id,
                reject_reason=None,
                viewed_confirmed=0,
                last_seen_at=now,
                created_at=now,
                submitted_at=None,
                updated_at=now,
            )
            db.add(o)
            upserted += 1
        else:
            # Update fields but keep viewed_confirmed
            existing.symbol_canon = vo.get("symbol_canon") or existing.symbol_canon
            existing.symbol_venue = vo.get("symbol_venue") or existing.symbol_venue
            existing.side = vo.get("side") or existing.side
            existing.type = vo.get("type") or existing.type
            existing.qty = float(vo.get("qty") or existing.qty or 0.0)
            existing.limit_price = vo.get("limit_price")
            existing.status = status or existing.status
            existing.raw_status = status_raw
            existing.filled_qty = float(vo.get("filled_qty") or existing.filled_qty or 0.0)
            existing.avg_fill_price = vo.get("avg_fill_price")
            existing.fee_total = vo.get("fee_total") or vo.get("fee") or existing.fee_total
            existing.fee_asset = vo.get("fee_asset") or existing.fee_asset
            existing.gross_total = vo.get("gross_total") or existing.gross_total
            existing.net_total_after_fee = (
                vo.get("net_total_after_fee")
                or vo.get("total_after_fee")
                or existing.net_total_after_fee
            )
            existing.venue_order_id = external_id
            existing.last_seen_at = now
            existing.updated_at = now
            upserted += 1

    db.commit()
    return upserted, total
