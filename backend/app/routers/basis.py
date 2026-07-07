# backend/app/routers/basis.py
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ..db import get_db
from ..schemas import BasisLotsResponse
from ..services.basis_enrichment import basis_lot_details_for_key


router = APIRouter(prefix="/api/basis", tags=["basis"])


@router.get("/lots", response_model=BasisLotsResponse)
def get_basis_lots(
    venue: str = Query(..., min_length=1, description="Exact holding venue, e.g. coinbase"),
    wallet_id: str = Query(default="default", description="Exact wallet/account bucket, default for CEX balances"),
    asset: str = Query(..., min_length=1, description="Asset symbol, e.g. BAT"),
    limit: int = Query(default=200, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    """Read-only remaining basis-lot drilldown for one balance row.

    This endpoint never mutates FIFO state. It only reads basis_lots for the exact
    (venue, wallet_id, asset) key and returns rows with qty_remaining > 0.
    """
    return basis_lot_details_for_key(
        db,
        venue=venue,
        wallet_id=wallet_id,
        asset=asset,
        limit=limit,
    )
