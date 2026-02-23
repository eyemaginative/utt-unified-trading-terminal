# backend/app/routers/symbols.py

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ..db import get_db
from ..services.symbols import (
    refresh_symbols_for_venue,
    latest_symbols_for_venue,
    new_symbols_since_last_snapshot,
    unheld_new_symbols_since_last_snapshot,
    list_symbol_venues,
)

router = APIRouter(prefix="/api/symbols", tags=["symbols"])


def _normalize_venue(v: Optional[str]) -> str:
    return (v or "").strip().lower()


def _bad_request(detail: str) -> HTTPException:
    return HTTPException(status_code=400, detail=detail)


def _upstream_failure(detail: str) -> HTTPException:
    # Use 502 to indicate adapter / upstream dependency failure.
    return HTTPException(status_code=502, detail=detail)


@router.post("/refresh")
def refresh(
    venue: str = Query(..., description="Venue name, e.g. gemini|coinbase|kraken (robinhood only if enabled)"),
    db: Session = Depends(get_db),
):
    """
    Create a new snapshot of tradable symbols for a venue.
    """
    v = _normalize_venue(venue)
    if not v:
        raise _bad_request("venue is required")

    try:
        return refresh_symbols_for_venue(db, v)
    except ValueError as e:
        # e.g. Unsupported venue / Disabled venue
        raise _bad_request(str(e))
    except Exception as e:
        raise _upstream_failure(f"symbol refresh failed for venue='{v}': {e}")


@router.get("/latest")
def latest(
    venue: str = Query(..., description="Venue name, e.g. gemini|coinbase|kraken (robinhood only if enabled)"),
    db: Session = Depends(get_db),
):
    """
    Get the latest captured snapshot for a venue.
    """
    v = _normalize_venue(venue)
    if not v:
        raise _bad_request("venue is required")

    try:
        return latest_symbols_for_venue(db, v)
    except ValueError as e:
        raise _bad_request(str(e))
    except Exception as e:
        raise _upstream_failure(f"latest symbols failed for venue='{v}': {e}")


@router.get("/new")
def new_symbols(
    venue: str = Query(..., description="Venue name, e.g. gemini|coinbase|kraken (robinhood only if enabled)"),
    db: Session = Depends(get_db),
):
    """
    Get symbols that are present in the latest snapshot but not in the previous snapshot.
    If there is no previous snapshot, all latest symbols are treated as 'new'.
    """
    v = _normalize_venue(venue)
    if not v:
        raise _bad_request("venue is required")

    try:
        return new_symbols_since_last_snapshot(db, v)
    except ValueError as e:
        raise _bad_request(str(e))
    except Exception as e:
        raise _upstream_failure(f"new symbols failed for venue='{v}': {e}")


@router.get("/unheld_new")
def unheld_new(
    venue: str = Query(..., description="Venue name, e.g. gemini|coinbase|kraken (robinhood only if enabled)"),
    eps: float = Query(default=1e-8, gt=0.0, description="Holding threshold; asset is held if abs(total) > eps"),
    db: Session = Depends(get_db),
):
    """
    Get 'new' symbols minus those whose base asset appears in latest balances above EPS.
    """
    v = _normalize_venue(venue)
    if not v:
        raise _bad_request("venue is required")

    try:
        return unheld_new_symbols_since_last_snapshot(db, v, eps=eps)
    except ValueError as e:
        raise _bad_request(str(e))
    except Exception as e:
        raise _upstream_failure(f"unheld_new symbols failed for venue='{v}': {e}")


@router.get("/venues")
def venues(db: Session = Depends(get_db)):
    """
    List venues that currently exist in VenueSymbolSnapshot table (i.e., have at least one snapshot),
    plus supported venues (even if no snapshots exist yet).
    """
    try:
        return list_symbol_venues(db)
    except Exception as e:
        raise _upstream_failure(f"venues listing failed: {e}")
