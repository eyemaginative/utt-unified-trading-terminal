# backend/app/routers/okx.py

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query

from ..adapters.okx import OKXAdapter

router = APIRouter(prefix="/api/okx", tags=["okx"])


@router.get("/diagnostics")
def get_okx_diagnostics(
    private: bool = Query(default=False, description="If true, checks signed account balance read path."),
    ccy: Optional[str] = Query(default=None, description="Optional currency filter for private balance check, e.g. DOGE."),
):
    """Read-only OKX diagnostic endpoint.

    Never returns API key, secret, passphrase, signatures, or request headers.
    """
    return OKXAdapter().diagnostics(include_private=bool(private), ccy=ccy)

@router.get("/order_diagnostics")
def get_okx_order_diagnostics(
    symbol: Optional[str] = Query(default=None, description="Optional canonical/venue symbol filter, e.g. DOGE-USD."),
    limit: int = Query(default=100, ge=1, le=100, description="Maximum OKX history/fills rows to inspect."),
    include_samples: bool = Query(default=True, description="If true, returns small normalized order/fill samples."),
):
    """Read-only OKX order/fill diagnostics.

    Never returns API key, secret, passphrase, signatures, or request headers.
    Does not write fills, ledger rows, lot journals, or basis lots.
    """
    return OKXAdapter().order_diagnostics(
        symbol=symbol,
        limit=int(limit or 100),
        include_samples=bool(include_samples),
    )

