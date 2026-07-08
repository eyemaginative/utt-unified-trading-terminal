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
