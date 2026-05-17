# backend/app/routers/market_metrics.py
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query

from ..services.market_metrics import get_market_metrics_summary

router = APIRouter(prefix="/api/market_metrics", tags=["market_metrics"])


@router.get("/summary")
def market_metrics_summary(
    assets: Optional[str] = Query(
        None,
        description="Comma-separated asset symbols, or assets=owned/db to discover locally owned/tracked assets.",
    ),
    include_assets: Optional[str] = Query(
        None,
        description="Comma-separated asset symbols to append to the requested/discovered asset set.",
    ),
    limit: int = Query(250, ge=1, le=250),
    ttl_s: int = Query(300, ge=10, le=3600),
    force_refresh: bool = Query(False),
):
    return get_market_metrics_summary(
        assets=assets,
        include_assets=include_assets,
        limit=limit,
        ttl_s=ttl_s,
        force_refresh=force_refresh,
    )
