# backend/app/routers/venue_orders.py

from __future__ import annotations

from fastapi import APIRouter, Depends, Query, HTTPException, Body
from sqlalchemy.orm import Session
from typing import Optional, Dict, Any, List
from datetime import datetime
import time
import logging

from ..db import get_db
from ..schemas import VenueOrdersLatestResponse, VenueOrderRefreshRequest
from ..services.venue_orders import refresh_venue_orders, latest_venue_orders

router = APIRouter(prefix="/api/venue_orders", tags=["venue_orders"])

logger = logging.getLogger("uvicorn.error")


@router.post("/refresh")
def post_refresh(
    req: Optional[VenueOrderRefreshRequest] = Body(default=None),
    force: bool = Query(default=False, description="If true, always bump captured_at even if unchanged"),
    db: Session = Depends(get_db),
):
    """Refresh venue orders snapshots.

    Resilience rules:
    - If venue is omitted/blank OR 'all'/'*', refresh known venues and return partial success.
    - One venue failure must not fail the whole refresh.
    - Return 200 if at least one venue succeeded; optionally return 503 only if all venues failed.

    Backward compatibility:
    - Preserve top-level {ok, count, results(list)}.
    - Add results_by_venue (map) + any_ok/all_failed metadata for richer UI.
    """

    venue = ""
    try:
        venue = ((req.venue if req else None) or "").strip().lower()
    except Exception:
        venue = ""

    # Treat blank/None as "all venues" to avoid accidental single-venue errors.
    is_all = (not venue) or (venue in {"all", "*"})

    if is_all:
        # Keep this list up to date as you add venues.
        venues = ["gemini", "kraken", "coinbase", "robinhood", "dex_trade", "cryptocom"]

        results_list: List[Dict[str, Any]] = []
        results_by_venue: Dict[str, Dict[str, Any]] = {}
        total_count = 0

        for v in venues:
            t0 = time.perf_counter()
            try:
                c = refresh_venue_orders(db, v, force=force)
                duration_ms = int((time.perf_counter() - t0) * 1000)

                total_count += int(c or 0)
                row = {"venue": v, "ok": True, "count": int(c or 0), "duration_ms": duration_ms}
                results_list.append(row)
                results_by_venue[v] = {"ok": True, "count": int(c or 0), "duration_ms": duration_ms}

            except Exception as e:
                duration_ms = int((time.perf_counter() - t0) * 1000)

                msg = str(e) or e.__class__.__name__
                transient = ("Too many errors" in msg) or ("Cooldown active until" in msg)
                if transient:
                    logger.warning("venue_orders refresh skipped for venue=%s: %s", v, msg)
                else:
                    logger.exception("venue_orders refresh failed for venue=%s", v)

                if len(msg) > 2000:
                    msg = msg[:2000] + "…"

                row = {"venue": v, "ok": False, "error": msg, "duration_ms": duration_ms}
                results_list.append(row)
                results_by_venue[v] = {"ok": False, "error": msg, "duration_ms": duration_ms}

        any_ok = any(r.get("ok") for r in results_list)
        all_failed = not any_ok

        payload = {
            # preserve existing keys
            "ok": bool(any_ok),
            "count": int(total_count),
            "results": results_list,
            # new, richer structure
            "results_by_venue": results_by_venue,
            "any_ok": bool(any_ok),
            "all_failed": bool(all_failed),
        }

        # Only error the HTTP status if *everything* failed.
        if all_failed:
            raise HTTPException(status_code=503, detail=payload)

        return payload

    # Single venue: do NOT hard-fail as a 500 without logging.
    # Return 200 with ok=false so the UI can display the error,
    # while still logging the traceback server-side.
    try:
        upsert_count = refresh_venue_orders(db, venue, force=force)
        return {"ok": True, "count": int(upsert_count or 0), "venue": venue}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        msg = str(e) or e.__class__.__name__
        transient = ("Too many errors" in msg) or ("Cooldown active until" in msg)
        if transient:
            logger.warning("venue_orders refresh skipped for single venue=%s: %s", venue, msg)
        else:
            logger.exception("venue_orders refresh failed for single venue=%s", venue)

        if len(msg) > 2000:
            msg = msg[:2000] + "…"
        return {"ok": False, "count": 0, "venue": venue, "error": msg}


@router.get("/latest", response_model=VenueOrdersLatestResponse)
def get_latest(
    venue: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    source: Optional[str] = Query(default=None),
    symbol: Optional[str] = Query(default=None),
    from_: Optional[datetime] = Query(default=None, alias="from"),
    to: Optional[datetime] = Query(default=None),
    sort: Optional[str] = Query(default="captured_at:desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    items, total, as_of = latest_venue_orders(
        db=db,
        venue=venue,
        status=status,
        source=source,
        symbol=symbol,
        dt_from=from_,
        dt_to=to,
        sort=sort,
        page=page,
        page_size=page_size,
    )

    out = []
    for v in items:
        cancel_ref = f"{(v.venue or '').strip().lower()}:{(v.venue_order_id or '').strip()}"

        out.append(
            {
                "venue": v.venue,
                "venue_order_id": v.venue_order_id,
                "cancel_ref": cancel_ref,
                "symbol_canon": v.symbol_canon,
                "symbol_venue": v.symbol_venue,
                "side": v.side,
                "type": v.type,
                "status": v.status,
                "qty": v.qty,
                "filled_qty": v.filled_qty,
                "limit_price": v.limit_price,
                "avg_fill_price": v.avg_fill_price,
                "fee": v.fee,
                "fee_asset": v.fee_asset,
                "total_after_fee": v.total_after_fee,
                "created_at": v.created_at,
                "updated_at": v.updated_at,
                "captured_at": v.captured_at,
            }
        )

    return {"items": out, "page": page, "page_size": page_size, "total": total, "as_of": as_of}
