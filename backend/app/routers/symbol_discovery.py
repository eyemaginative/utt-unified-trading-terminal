# backend/app/routers/symbol_discovery.py

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from ..db import get_db
from ..services import symbols as symbols_svc

router = APIRouter(prefix="/api", tags=["symbols"])


def _pick_venue(venue_q: Optional[str], payload: Optional[Dict[str, Any]]) -> str:
    v = (venue_q or "").strip()
    if not v and payload and isinstance(payload, dict):
        v = str(payload.get("venue") or "").strip()
    if not v:
        raise HTTPException(status_code=400, detail="venue is required")
    return v


# ─────────────────────────────────────────────────────────────
# Frontend routes: /api/symbols/*
# ─────────────────────────────────────────────────────────────

@router.get("/symbols/venues")
def list_symbol_venues(db: Session = Depends(get_db)):
    """
    Returns venues that have at least one VenueSymbolSnapshot row.
    Response: { "venues": ["gemini", "kraken", ...] }
    """
    return symbols_svc.list_symbol_venues(db)


@router.post("/symbols/refresh")
def refresh_symbols(
    venue: Optional[str] = Query(default=None),
    payload: Optional[Dict[str, Any]] = Body(default=None),
    db: Session = Depends(get_db),
):
    """
    Captures a new snapshot of tradable symbols for a venue.

    Supports BOTH calling styles:
      - POST /api/symbols/refresh?venue=gemini
      - POST /api/symbols/refresh  { "venue": "gemini" }

    NOTE: payload is optional to prevent 422s from callers that POST with no JSON body.
    """
    v = _pick_venue(venue, payload)
    try:
        return symbols_svc.refresh_symbols_for_venue(db, v)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except IntegrityError as e:
        # Common failure: NOT NULL constraint in venue_symbols (base_asset/quote_asset)
        raise HTTPException(
            status_code=502,
            detail=f"symbol refresh failed for venue='{v}': database constraint error: {str(e.orig) if getattr(e, 'orig', None) else str(e)}",
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"symbol refresh failed for venue='{v}': {e}")


@router.get("/symbols/latest")
def latest_symbols(
    venue: str = Query(...),
    db: Session = Depends(get_db),
):
    """
    Returns the latest snapshot rows for the venue.
    """
    try:
        return symbols_svc.latest_symbols_for_venue(db, venue)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/symbols/new")
def new_symbols(
    venue: str = Query(...),
    db: Session = Depends(get_db),
):
    """
    Returns symbols present in latest snapshot but not in previous snapshot.
    """
    try:
        return symbols_svc.new_symbols_since_last_snapshot(db, venue)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/symbols/unheld_new")
def unheld_new_symbols(
    venue: str = Query(...),
    eps: float = Query(default=1e-8, gt=0),
    db: Session = Depends(get_db),
):
    """
    Returns new symbols filtered by excluding any whose base asset is already held (abs(total) > eps).
    """
    try:
        return symbols_svc.unheld_new_symbols_since_last_snapshot(db, venue, eps=eps)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ─────────────────────────────────────────────────────────────
# Legacy compatibility: /api/symbol_discovery/*
# ─────────────────────────────────────────────────────────────

@router.post("/symbol_discovery/refresh")
def legacy_refresh_symbols(
    venue: Optional[str] = Query(default=None),
    payload: Optional[Dict[str, Any]] = Body(default=None),
    db: Session = Depends(get_db),
):
    """
    Legacy endpoint.
    Mirrors /api/symbols/refresh.

    Accepts BOTH:
      - POST /api/symbol_discovery/refresh?venue=gemini
      - POST /api/symbol_discovery/refresh  { "venue": "gemini" }

    (We keep payload optional so older callers that POST without JSON don't 422.)
    """
    v = _pick_venue(venue, payload)
    try:
        return symbols_svc.refresh_symbols_for_venue(db, v)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except IntegrityError as e:
        raise HTTPException(
            status_code=502,
            detail=f"symbol refresh failed for venue='{v}': database constraint error: {str(e.orig) if getattr(e, 'orig', None) else str(e)}",
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"symbol refresh failed for venue='{v}': {e}")
