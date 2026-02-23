# routers/orders.py
from fastapi import APIRouter, Depends, Query, HTTPException, Body
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from ..db import get_db
from ..schemas import OrderCreate, OrderOut, OrdersPage, CancelAllRequest
from ..services.orders import create_order, cancel_order, cancel_all, list_orders, cancel_by_ref
from ..services.venue_orders import refresh_venue_orders as refresh_venue_orders_ro

router = APIRouter(prefix="/api/orders", tags=["orders"])


def _to_order_out(o) -> dict:
    return {
        "id": o.id,
        "client_order_id": o.client_order_id,

        # NOTE: your Order model currently does NOT include these fields.
        # The getattr() guards keep the router stable even if they are absent.
        "source": getattr(o, "source", "local"),
        "source_name": getattr(o, "source_name", "LOCAL"),
        "external_order_id": getattr(o, "external_order_id", None),

        "venue": o.venue,
        "symbol_canon": o.symbol_canon,
        "symbol_venue": o.symbol_venue,
        "side": o.side,
        "type": o.type,
        "qty": o.qty,
        "limit_price": o.limit_price,
        "status": o.status,
        "raw_status": getattr(o, "raw_status", None),
        "filled_qty": o.filled_qty,
        "avg_fill_price": o.avg_fill_price,

        "fee_total": getattr(o, "fee_total", None),
        "fee_asset": getattr(o, "fee_asset", None),
        "gross_total": getattr(o, "gross_total", None),
        "net_total_after_fee": getattr(o, "net_total_after_fee", None),

        "viewed_confirmed": bool(getattr(o, "viewed_confirmed", 0)),

        "venue_order_id": o.venue_order_id,
        "reject_reason": o.reject_reason,
        "created_at": o.created_at,
        "submitted_at": o.submitted_at,
        "updated_at": o.updated_at,
    }


@router.get("", response_model=OrdersPage)
def get_orders(
    venue: Optional[str] = Query(default=None),
    source_name: Optional[str] = Query(default=None),
    symbol: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    side: Optional[str] = Query(default=None),
    type: Optional[str] = Query(default=None),
    viewed_confirmed: Optional[bool] = Query(default=None),
    from_: Optional[datetime] = Query(default=None, alias="from"),
    to: Optional[datetime] = Query(default=None),
    sort: Optional[str] = Query(default="created_at:desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    items, total = list_orders(
        db, venue, source_name, symbol, status, side, type, viewed_confirmed, from_, to, sort, page, page_size
    )
    return {"items": [_to_order_out(o) for o in items], "page": page, "page_size": page_size, "total": total}


@router.post("", response_model=OrderOut)
def post_order(req: OrderCreate, db: Session = Depends(get_db)):
    if req.type == "limit" and req.limit_price is None:
        raise HTTPException(status_code=400, detail="limit_price is required for limit orders")
    if req.type == "market":
        req.limit_price = None

    o = create_order(db, req)
    return _to_order_out(o)


@router.delete("/{order_id}", response_model=OrderOut)
def delete_order(order_id: str, db: Session = Depends(get_db)):
    try:
        o = cancel_order(db, order_id)
        return _to_order_out(o)
    except KeyError:
        raise HTTPException(status_code=404, detail="Order not found")


class CancelByRefRequest(BaseModel):
    cancel_ref: str


@router.post("/cancel_by_ref")
def post_cancel_by_ref(
    req: Optional[CancelByRefRequest] = Body(default=None),
    cancel_ref: Optional[str] = Query(default=None, description="Cancel reference (LOCAL:<id> or VENUE:<venue>:<venue_order_id> or <venue>:<venue_order_id>)"),
    db: Session = Depends(get_db),
):
    """
    Unified cancel endpoint used by the All Orders table when it has a cancel_ref.

    Supported:
      - LOCAL:<order_id>
      - VENUE:<venue>:<venue_order_id>
      - <venue>:<venue_order_id>   (shorthand; e.g. robinhood:<id>)

    Returns a truthful {ok: bool, ...} response. In LIVE mode, ok only becomes True if the
    venue confirms the cancellation.
    """
    cref = (cancel_ref or (req.cancel_ref if req else None) or "").strip()
    if not cref:
        raise HTTPException(status_code=400, detail="cancel_ref is required")

    # Defensive normalization (allow local:/venue: too)
    # Keep the original venue + id portion intact.
    parts = cref.split(":")
    if len(parts) >= 2:
        p0 = parts[0].strip().upper()
        if p0 in ("LOCAL", "VENUE"):
            parts[0] = p0
            cref = ":".join(parts)
        elif p0.lower() in ("local", "venue"):
            parts[0] = p0.upper()
            cref = ":".join(parts)

    try:
        return cancel_by_ref(db, cref)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cancel_all")
def post_cancel_all(req: CancelAllRequest, db: Session = Depends(get_db)):
    n = cancel_all(db, req.venue, req.symbol)
    return {"canceled": n}


@router.post("/refresh")
def post_refresh_orders(
    venue: str,
    force: bool = Query(default=False, description="If true, always bump captured_at even if unchanged"),
    db: Session = Depends(get_db),
):
    """
    SAFE refresh:
    - This refreshes the read-only VENUE ingestion table (venue_orders).
    - It does NOT upsert into the local orders table.
    - This prevents crashes caused by services/orders_sync.py referencing non-existent Order fields.
    """
    venue = (venue or "").strip().lower()
    if not venue:
        raise HTTPException(status_code=400, detail="venue is required")

    try:
        upserted = refresh_venue_orders_ro(db, venue, force=force)
        return {"ok": True, "venue": venue, "fetched": None, "upserted": int(upserted)}
    except (KeyError, ValueError) as e:
        # Service/adapter layer says this venue isn’t supported for this refresh path.
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{order_id}/confirm_viewed")
def post_confirm_viewed(order_id: str, confirmed: bool = True, db: Session = Depends(get_db)):
    o = db.get(__import__("app.models", fromlist=["Order"]).Order, order_id)
    if not o:
        raise HTTPException(status_code=404, detail="Order not found")

    o.viewed_confirmed = 1 if confirmed else 0
    o.updated_at = __import__("app.utils", fromlist=["now_utc"]).now_utc()
    db.add(o)
    db.commit()
    db.refresh(o)
    return {"ok": True, "id": o.id, "viewed_confirmed": bool(o.viewed_confirmed)}
