# backend/app/routers/hydration_wallet_history.py

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from ..db import get_db
from ..services.hydration_wallet_history import (
    hydration_wallet_history_status,
    ingest_hydration_wallet_history,
)

router = APIRouter(prefix="/api/hydration_wallet_history", tags=["hydration_wallet_history"])


@router.get("/status")
def get_hydration_wallet_history_status(
    provider: Optional[str] = Query(default=None, description="Override provider for this status check. Default: env UTT_HYDRATION_HISTORY_PROVIDER."),
):
    """Return provider/config status without touching RPC/indexers."""
    return hydration_wallet_history_status(provider=provider)


@router.post("/ingest")
async def post_hydration_wallet_history_ingest(
    address_id: Optional[str] = Query(default=None, description="Optional wallet_addresses.id to ingest only one registered address."),
    limit_per_address: int = Query(default=25, ge=1, le=100),
    dry_run: bool = Query(default=True, description="Default true. When true, performs no DB writes."),
    provider: Optional[str] = Query(default=None, description="none | subscan. Default: env UTT_HYDRATION_HISTORY_PROVIDER."),
    cache_txs: bool = Query(default=True, description="When dry_run=false, cache parsed tx candidates into wallet_address_txs."),
    trust_provider_amounts: bool = Query(default=False, description="Allow caching candidates whose amount scaling could not be independently trusted."),
    raw_debug: bool = Query(default=False, description="Include small raw provider samples in examples for debugging."),
    materialize: bool = Query(default=False, description="When true, preview/apply cached Hydration wallet tx rows into deposits/withdrawals."),
    materialize_limit: int = Query(default=100, ge=1, le=500, description="Maximum cached wallet tx rows to materialize/preview."),
    page_start: int = Query(default=0, ge=0, description="Subscan page number to start from for provider scans."),
    max_pages: int = Query(default=1, ge=1, le=25, description="Maximum Subscan pages to scan per supported address."),
    coverage_only: bool = Query(default=False, description="When true, build coverage diagnostics only; never cache or materialize rows."),
    untrusted_examples: bool = Query(default=False, description="When true, include redacted diagnostics for integer-only/untrusted amount rows."),
    untrusted_example_limit: int = Query(default=20, ge=1, le=100, description="Maximum untrusted amount examples to include."),
    trust_amount_v2_validated: bool = Query(default=False, description="Opt-in: trust integer-looking Subscan amount rows only when amount_v2 + decimals validates a safe display/scaled amount."),
    db: Session = Depends(get_db),
):
    """Hydration wallet-history ingestion.

    Safety rules:
      - dry_run=true by default
      - no SDK router quote calls
      - no broad RPC hammering
      - provider scans are bounded by limit_per_address and max_pages
      - coverage_only=true prevents tx caching and materialization
      - tx rows are cached only when dry_run=false and cache_txs=true
      - cached tx rows materialize only when materialize=true
    """
    return await ingest_hydration_wallet_history(
        db,
        address_id=address_id,
        limit_per_address=limit_per_address,
        dry_run=dry_run,
        provider=provider,
        cache_txs=cache_txs,
        trust_provider_amounts=trust_provider_amounts,
        raw_debug=raw_debug,
        materialize=materialize,
        materialize_limit=materialize_limit,
        page_start=page_start,
        max_pages=max_pages,
        coverage_only=coverage_only,
        untrusted_examples=untrusted_examples,
        untrusted_example_limit=untrusted_example_limit,
        trust_amount_v2_validated=trust_amount_v2_validated,
    )
