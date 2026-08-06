# app/routers/trade.py

import base64
import hashlib
import hmac
import secrets
import time
import os
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Header
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..db import get_db
from ..config import settings
from ..schemas import OrderCreate, OrderOut
from ..services.orders import create_order, cancel_by_ref
from ..routers.auth import require_auth
from ..models import RuntimeSetting
from ..utils import new_client_order_id, now_utc


router = APIRouter(prefix="/api/trade", tags=["trade"])

RUNTIME_REALIZED_FIELDS_KEY = "realized_fields_enabled"

def _parse_boolish(v, fallback: bool) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v or "").strip().lower()
    if s in ("1", "true", "yes", "on"): return True
    if s in ("0", "false", "no", "off"): return False
    return fallback

def _env_realized_fields_default() -> bool:
    raw = str(os.getenv("UTT_REALIZED_FIELDS_V1", "") or "").strip()
    if not raw:
        return True
    return _parse_boolish(raw, True)

def _get_runtime_setting(db: Session, key: str):
    return db.execute(select(RuntimeSetting).where(RuntimeSetting.key == key)).scalar_one_or_none()

def _get_realized_fields_enabled(db: Session) -> bool:
    row = _get_runtime_setting(db, RUNTIME_REALIZED_FIELDS_KEY)
    if row is not None:
        return _parse_boolish(getattr(row, "value_json", None), _env_realized_fields_default())
    return _env_realized_fields_default()

def _set_runtime_setting(db: Session, key: str, value):
    row = _get_runtime_setting(db, key)
    if row is None:
        row = RuntimeSetting(key=key, value_json=value)
        db.add(row)
    else:
        row.value_json = value
    db.commit()
    try:
        db.refresh(row)
    except Exception:
        pass
    return row

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


def _validate_cexius_order_contract(req: OrderCreate) -> None:
    if (req.venue or "").strip().lower() != "cexius":
        return
    if str(req.type or "").strip().lower() != "limit":
        raise HTTPException(status_code=400, detail="CEXIUS.2B permits limit orders only.")
    if req.limit_price is None or float(req.limit_price) <= 0:
        raise HTTPException(status_code=400, detail="Cexius limit_price is required.")
    if str(getattr(req, "tif", None) or "").strip():
        raise HTTPException(status_code=400, detail="Cexius time-in-force is unsupported and must be omitted.")
    if bool(getattr(req, "post_only", False)):
        raise HTTPException(status_code=400, detail="Cexius post-only is unsupported.")
    if str(getattr(req, "client_order_id", None) or "").strip():
        raise HTTPException(status_code=400, detail="Cexius client order ID is unsupported and must be omitted.")


def _cexius_simulated_order(req: OrderCreate) -> dict:
    now = now_utc()
    client_order_id = new_client_order_id()
    symbol = str(req.symbol or "").strip().upper().replace("/", "-").replace("_", "-")
    while "--" in symbol:
        symbol = symbol.replace("--", "-")
    return {
        "id": f"SIMULATED:{client_order_id}",
        "client_order_id": client_order_id,
        "venue": "cexius",
        "symbol_canon": symbol,
        "symbol_venue": symbol,
        "side": req.side,
        "type": "limit",
        "qty": float(req.qty),
        "limit_price": float(req.limit_price) if req.limit_price is not None else None,
        "status": "simulated",
        "filled_qty": 0.0,
        "avg_fill_price": None,
        "venue_order_id": None,
        "reject_reason": "simulation only: no Cexius venue order request was sent",
        "created_at": now,
        "submitted_at": now,
        "updated_at": now,
        "simulated": True,
        "venue_request_sent": False,
    }


@router.post("/order", response_model=OrderOut)
def post_trade_order(req: OrderCreate, db: Session = Depends(get_db), _auth: dict = Depends(require_auth)):
    """
    UI endpoint (OrderTicketWidget.jsx) calls POST /api/trade/order.

    Safety model:
      - DRY_RUN=true OR ARMED=false => always forced dry-run routing in services layer.
      - LIVE routing (DRY_RUN=false AND ARMED=true) is additionally gated by LIVE_VENUES.
    """
    venue = (req.venue or "").strip().lower()
    _validate_cexius_order_contract(req)

    # CEXIUS.2B simulation is intentionally non-persistent: no adapter call,
    # no local Order row, no fabricated open order, and no implied balance hold.
    if venue == "cexius" and _effective_dry_run():
        return _cexius_simulated_order(req)

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

    status = str(o.status or "").strip().lower()
    if venue == "cexius" and status in {"rejected", "failed", "error"}:
        raise HTTPException(
            status_code=400,
            detail=str(o.reject_reason or "Cexius order submission was rejected."),
        )

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
        "simulated": False,
        "venue_request_sent": True,
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


class RuntimeFlagsOut(BaseModel):
    realized_fields_enabled: bool


class RuntimeFlagsUpdate(BaseModel):
    realized_fields_enabled: Optional[bool] = None


@router.get("/runtime_flags", response_model=RuntimeFlagsOut)
def get_runtime_flags(db: Session = Depends(get_db), _auth: dict = Depends(require_auth)):
    return {
        "realized_fields_enabled": _get_realized_fields_enabled(db),
    }


@router.post("/runtime_flags", response_model=RuntimeFlagsOut)
def post_runtime_flags(req: RuntimeFlagsUpdate, db: Session = Depends(get_db), _auth: dict = Depends(require_auth)):
    if req.realized_fields_enabled is not None:
        _set_runtime_setting(db, RUNTIME_REALIZED_FIELDS_KEY, bool(req.realized_fields_enabled))
    return {
        "realized_fields_enabled": _get_realized_fields_enabled(db),
    }
