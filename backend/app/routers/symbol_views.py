# backend/app/routers/symbol_views.py

from __future__ import annotations

from datetime import datetime
from typing import Optional, Dict, Any, List

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ..db import get_db
from ..discovery_models.symbol_view import SymbolView

router = APIRouter(prefix="/api/symbols", tags=["symbols"])


def _norm_venue(v: str) -> str:
    return (v or "").strip().lower()


def _make_view_key(venue: str, symbol_canon: str) -> str:
    ven = _norm_venue(venue)
    sym = (symbol_canon or "").strip().upper()
    return f"{ven}:{sym}"


def _symbol_from_view_key(view_key: str) -> str:
    if not view_key:
        return ""
    if ":" not in view_key:
        return view_key
    return view_key.split(":", 1)[1].strip().upper()


@router.get("/views")
def get_symbol_views(
    venue: str = Query(..., description="Venue name, e.g., gemini/coinbase/kraken"),
    confirmed_only: bool = Query(False, description="If true, return only confirmed views"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Returns discovery 'viewed/confirmed' flags for a venue.

    Uses SymbolView.view_key format: "{venue}:{symbol_canon}"
    """
    ven = _norm_venue(venue)
    prefix = f"{ven}:"

    q = db.query(SymbolView).filter(SymbolView.view_key.like(prefix + "%"))
    if confirmed_only:
        q = q.filter(SymbolView.viewed_confirmed.is_(True))

    rows: List[SymbolView] = q.order_by(SymbolView.updated_at.desc()).all()

    views = []
    by_symbol: Dict[str, bool] = {}

    for r in rows:
        sym = _symbol_from_view_key(r.view_key)
        by_symbol[sym] = bool(r.viewed_confirmed)
        views.append(
            {
                "view_key": r.view_key,
                "symbol_canon": sym,
                "viewed_confirmed": bool(r.viewed_confirmed),
                "viewed_at": r.viewed_at.isoformat() if r.viewed_at else None,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "updated_at": r.updated_at.isoformat() if r.updated_at else None,
            }
        )

    return {
        "venue": ven,
        "count": len(views),
        "views": views,         # detailed list (handy for debugging/UI tables)
        "by_symbol": by_symbol, # simple map (handy for UI hydration)
    }


class SymbolViewUpsert(BaseModel):
    venue: str = Field(..., description="Venue name (gemini/coinbase/kraken)")
    symbol_canon: str = Field(..., description="Canonical symbol, e.g., BTC-USD")
    viewed_confirmed: bool = Field(True, description="Mark viewed/confirmed")
    viewed_at: Optional[datetime] = Field(None, description="Optional explicit viewed timestamp (UTC)")


@router.post("/views")
def upsert_symbol_view(payload: SymbolViewUpsert, db: Session = Depends(get_db)) -> Dict[str, Any]:
    """
    Upserts a symbol view flag. Not required to fix your current 404,
    but recommended so you can persist discovery viewed state server-side later.
    """
    view_key = _make_view_key(payload.venue, payload.symbol_canon)

    row = db.query(SymbolView).filter(SymbolView.view_key == view_key).one_or_none()
    now = datetime.utcnow()

    if row is None:
        row = SymbolView(
            view_key=view_key,
            viewed_confirmed=bool(payload.viewed_confirmed),
            viewed_at=payload.viewed_at or (now if payload.viewed_confirmed else None),
            created_at=now,
            updated_at=now,
        )
        db.add(row)
    else:
        row.viewed_confirmed = bool(payload.viewed_confirmed)
        # if confirming and no timestamp was provided, set it now
        if payload.viewed_confirmed:
            row.viewed_at = payload.viewed_at or row.viewed_at or now
        else:
            row.viewed_at = payload.viewed_at  # allow clearing if desired
        row.updated_at = now

    db.commit()
    db.refresh(row)

    return {
        "view_key": row.view_key,
        "symbol_canon": _symbol_from_view_key(row.view_key),
        "viewed_confirmed": bool(row.viewed_confirmed),
        "viewed_at": row.viewed_at.isoformat() if row.viewed_at else None,
        "created_at": row.created_at.isoformat() if row.created_at else None,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }
