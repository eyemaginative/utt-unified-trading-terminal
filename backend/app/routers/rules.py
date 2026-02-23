# backend/app/routers/rules.py

from fastapi import APIRouter, Query
from typing import Optional

from ..services.rules import order_rules_for_symbol

router = APIRouter(prefix="/api/rules", tags=["rules"])


@router.get("/order")
def get_order_rules(
    venue: str = Query(..., description="Venue key, e.g. gemini/kraken/coinbase"),
    symbol: str = Query(..., description="Canonical symbol preferred, e.g. BTC-USD"),
    side: Optional[str] = Query(default=None, description="buy/sell"),
    type: str = Query(default="limit", description="limit/market"),
    tif: Optional[str] = Query(default=None, description="gtc/ioc/fok"),
    post_only: Optional[bool] = Query(default=None, description="post-only flag if relevant"),
):
    # Normalized, safe rules response for UI warnings (never raises due to adapter gaps)
    return order_rules_for_symbol(
        venue=venue,
        symbol_canon=symbol,
        side=side,
        order_type=type,
        tif=tif,
        post_only=post_only,
    )
