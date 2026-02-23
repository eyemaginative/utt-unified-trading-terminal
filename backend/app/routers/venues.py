# backend/app/routers/venues.py

from __future__ import annotations

from fastapi import APIRouter, Query

from ..venues.registry import list_venues

router = APIRouter(prefix="/api/venues", tags=["venues"])


@router.get("")
@router.get("/")
def get_venues(
    include_disabled: bool = Query(True, description="If true, include disabled venues in the response."),
):
    """
    Frontend should call this instead of hardcoding venue lists.

    Returns:
      [
        {
          "venue": "gemini",
          "display_name": "Gemini",
          "enabled": true,
          "supports": { ... }
        },
        ...
      ]
    """
    reg = list_venues(include_disabled=include_disabled)

    out = []
    for k, spec in sorted(reg.items(), key=lambda x: x[0]):
        enabled = False
        try:
            enabled = bool(spec.enabled())
        except Exception:
            enabled = False

        out.append(
            {
                "venue": spec.key,
                "display_name": spec.display_name,
                "enabled": enabled,
                "supports": {
                    "trading": bool(spec.supports_trading),
                    "balances": bool(spec.supports_balances),
                    "orderbook": bool(spec.supports_orderbook),
                    "markets": bool(spec.supports_markets),
                },
            }
        )

    return out
