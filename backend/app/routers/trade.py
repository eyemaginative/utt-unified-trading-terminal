# app/routers/trade.py

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..db import get_db
from ..config import settings
from ..schemas import OrderCreate, OrderOut
from ..services.orders import create_order, cancel_by_ref

# Auth moved to app/routers/auth.py (kept backwards-compatible behavior).
from .auth import require_auth


router = APIRouter(prefix="/api/trade", tags=["trade"])
def _effective_dry_run() -> bool:
    # Mirror services/orders.py policy:
    # live routing is only allowed when DRY_RUN=false AND ARMED=true
    return settings.dry_run or (not settings.armed)


def _enabled_live_venues() -> set[str]:
    """
    LIVE_VENUES env var (comma-separated) controls which venues are allowed
    for LIVE order routing. Example: LIVE_VENUES=gemini

    If unset/empty -> returns empty set. In LIVE mode we enforce that this must
    be configured (i.e., empty set will hard-reject live orders).
    """
    raw = getattr(settings, "live_venues", None)
    if raw is None:
        # If config wasn't added yet, behave permissively for dry-run only,
        # and explicitly require config for live (see enforcement below).
        return set()

    if isinstance(raw, (list, tuple, set)):
        return {str(x).strip().lower() for x in raw if str(x).strip()}

    s = str(raw or "").strip()
    if not s:
        return set()

    return {p.strip().lower() for p in s.split(",") if p.strip()}


@router.post("/order", response_model=OrderOut)
def post_trade_order(req: OrderCreate, db: Session = Depends(get_db), _auth: dict = Depends(require_auth)):
    """
    UI endpoint (OrderTicketWidget.jsx) calls POST /api/trade/order.

    Safety model:
      - DRY_RUN=true OR ARMED=false => always forced dry-run routing in services layer.
      - LIVE routing (DRY_RUN=false AND ARMED=true) is additionally gated by LIVE_VENUES.
    """
    venue = (req.venue or "").strip().lower()

    # Enforce "one exchange at a time" only for LIVE mode
    if not _effective_dry_run():
        enabled = _enabled_live_venues()
        if not enabled:
            raise HTTPException(
                status_code=400,
                detail="LIVE_VENUES is not configured. Set LIVE_VENUES=gemini (or desired venue) before live trading.",
            )
        if venue not in enabled:
            raise HTTPException(
                status_code=403,
                detail=f"Venue '{venue}' is not enabled for LIVE routing. Enabled LIVE_VENUES={sorted(enabled)}",
            )

    o = create_order(db, req)

    return {
        "id": o.id,
        "client_order_id": o.client_order_id,
        "venue": o.venue,
        "symbol_canon": o.symbol_canon,
        "symbol_venue": o.symbol_venue,
        "side": o.side,
        "type": o.type,
        "qty": o.qty,
        "limit_price": o.limit_price,
        "status": o.status,
        "filled_qty": o.filled_qty,
        "avg_fill_price": o.avg_fill_price,
        "venue_order_id": o.venue_order_id,
        "reject_reason": o.reject_reason,
        "created_at": o.created_at,
        "submitted_at": o.submitted_at,
        "updated_at": o.updated_at,
    }


class CancelRequest(BaseModel):
    cancel_ref: str


@router.post("/cancel")
def post_trade_cancel(req: CancelRequest, db: Session = Depends(get_db), _auth: dict = Depends(require_auth)):
    """
    UI endpoint (All Orders table Cancel button) calls POST /api/trade/cancel.

    NOTE: Venue-cancel execution is controlled by services/orders.py policy:
      effective_dry_run = DRY_RUN || !ARMED
    """
    cancel_ref = (req.cancel_ref or "").strip()
    if not cancel_ref:
        raise HTTPException(status_code=400, detail="cancel_ref is required")

    try:
        return cancel_by_ref(db, cancel_ref)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except KeyError as e:
        raise HTTPException(status_code=404, detail=str(e))
