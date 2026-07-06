# backend/app/routers/market_metrics.py
from __future__ import annotations

from typing import Optional

import os

from fastapi import APIRouter, Query


def _install_certifi_ca_env() -> None:
    """Point stdlib/requests-style HTTPS clients at certifi when available.

    Market metrics may be fetched by service code through urllib/urlopen, which
    does not always use the same certifi bundle that requests/httpx use.  Keep
    verification enabled, but make the CA bundle explicit before importing the
    service module.
    """
    try:
        import certifi  # type: ignore

        ca_path = certifi.where()
    except Exception:
        return

    if ca_path:
        os.environ.setdefault("SSL_CERT_FILE", ca_path)
        os.environ.setdefault("REQUESTS_CA_BUNDLE", ca_path)
        os.environ.setdefault("CURL_CA_BUNDLE", ca_path)


_install_certifi_ca_env()

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
    limit: int = Query(250, ge=1, le=1000),
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
