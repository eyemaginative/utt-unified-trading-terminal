from fastapi import APIRouter, Depends, Query, HTTPException, Body
from sqlalchemy.orm import Session
from sqlalchemy import select, desc, asc
from typing import Optional, Dict, List, Tuple, Set
import os
import re
import httpx
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError

from ..db import get_db
from ..schemas import BalancesLatestResponse, BalanceRefreshRequest
from ..models import BalanceSnapshot
from ..utils import now_utc, parse_sort
from ..services.balances import refresh_balances  # NOTE: we do NOT call latest_balances anymore
from ..services.market import prices_usd_from_assets

router = APIRouter(prefix="/api/balances", tags=["balances"])

_ALLOWED_SORT = {"asset", "total", "available", "captured_at"}


def _dex_trade_balance_dust() -> float:
    """
    Optional: filter Dex-Trade dust balances.

    If unset, we still always filter *exact* zeros for Dex-Trade to prevent
    huge lists of zero balances causing pricing fan-out.
    """
    s = (os.getenv("DEX_TRADE_BALANCE_DUST", "") or "").strip()
    if not s:
        s = (os.getenv("BALANCE_DUST_THRESHOLD", "") or "").strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def _price_asset_limit() -> int:
    """
    IMPORTANT (router behavior):
    This value is used as a *batch size* for pricing calls, NOT as a "only price first N assets" cap.

    Preference order:
      1) BAL_MAX_PRICE_ASSETS (matches services/balances.py hardening)
      2) BALANCES_PRICE_ASSET_LIMIT (legacy router knob)
      3) default 80 (safe)

    We clamp to [10..200] so one request cannot explode fan-out.
    """
    s = (os.getenv("BAL_MAX_PRICE_ASSETS", "") or "").strip()
    if not s:
        s = (os.getenv("BALANCES_PRICE_ASSET_LIMIT", "") or "").strip()
    if not s:
        return 80
    try:
        n = int(float(s))
        n = max(10, min(200, n))
        return n
    except Exception:
        return 80


def _price_timeout_ms() -> int:
    """
    Per-batch pricing timeout.

    Preference order:
      1) BALANCES_PRICE_TIMEOUT_MS (router knob)
      2) default 20000
    """
    s = (os.getenv("BALANCES_PRICE_TIMEOUT_MS", "") or "").strip()
    if not s:
        return 20000
    try:
        n = int(float(s))
        return max(1000, min(120000, n))
    except Exception:
        return 20000


def _price_total_budget_ms() -> int:
    """
    Total time budget for pricing within a single /api/balances/latest?with_prices=true request.

    This prevents the endpoint from exceeding the frontend’s typical 60s timeout.

    Env:
      BALANCES_PRICE_TOTAL_TIMEOUT_MS
    Default: 45000 (45s)
    Clamp: 5s..55s
    """
    s = (os.getenv("BALANCES_PRICE_TOTAL_TIMEOUT_MS", "") or "").strip()
    if not s:
        return 45000
    try:
        n = int(float(s))
        return max(5000, min(55000, n))
    except Exception:
        return 45000


def _looks_like_rate_limited(msg: str) -> bool:
    low = (msg or "").lower()
    return (
        "429" in low
        or "too many requests" in low
        or "rate limit" in low
        or "rate limited" in low
        or "cooldown" in low
        or "throttle" in low
        or "throttled" in low
        or "420" in low
    )


def _extract_http_status_from_text(msg: str) -> Optional[int]:
    s = str(msg or "")
    m = re.search(r"\b(\d{3})\b", s)
    if not m:
        return None
    try:
        code = int(m.group(1))
        if 100 <= code <= 599:
            return code
    except Exception:
        return None
    return None


def _map_upstream_exception(operation: str, e: Exception) -> HTTPException:
    # Preserve already-mapped errors
    if isinstance(e, HTTPException):
        return e

    # httpx-native classes if service layer uses them
    if isinstance(e, (httpx.TimeoutException, TimeoutError)):
        return HTTPException(status_code=504, detail=f"{operation}: upstream timeout")

    if isinstance(e, httpx.RequestError):
        return HTTPException(status_code=502, detail=f"{operation}: upstream network error: {e}")

    if isinstance(e, httpx.HTTPStatusError):
        try:
            status = int(e.response.status_code)
        except Exception:
            status = 502
        if status in (420, 429):
            return HTTPException(status_code=429, detail=f"{operation}: upstream rate limited")
        if 400 <= status < 500:
            # propagate 4xx (bad request, unauthorized, etc.)
            try:
                body = (e.response.text or "").strip()
            except Exception:
                body = ""
            return HTTPException(status_code=status, detail=body or f"{operation}: upstream error ({status})")
        return HTTPException(status_code=502, detail=f"{operation}: upstream error ({status})")

    # Fallback: infer from message text
    msg = str(e or "").strip()
    code = _extract_http_status_from_text(msg)

    if code in (420, 429) or _looks_like_rate_limited(msg):
        return HTTPException(status_code=429, detail=f"{operation}: rate limited")

    # Default: treat as upstream failure
    return HTTPException(status_code=502, detail=f"{operation}: failed: {msg or type(e).__name__}")


def _release_db_early(db: Session) -> None:
    """
    Critical hardening: release the pooled DB connection BEFORE slow work (pricing fan-out).

    SQLAlchemy sessions autobegin transactions on first use; with SQLite + multiple polling
    endpoints, holding the transaction open during network calls will exhaust the pool.

    - expunge_all() detaches ORM instances so they can be safely read after rollback
    - rollback() ends the transaction and returns the connection to the pool
    """
    try:
        db.expunge_all()
    except Exception:
        pass
    try:
        db.rollback()
    except Exception:
        pass


def _latest_balances_raw(db: Session, venue: Optional[str], sort: Optional[str]) -> Tuple[List[BalanceSnapshot], "datetime"]:
    """
    Router-local latest balances fetch that DOES NOT do any USD enrichment.
    This prevents hidden pricing fan-out when with_prices=false.

    Important: this function does not call _release_db_early() because the caller may
    still want to do other DB work. The route handler will release early right after
    calling this.
    """
    v = (venue or "").strip().lower() if venue else None

    base_stmt = select(BalanceSnapshot.captured_at)
    if v:
        base_stmt = base_stmt.where(BalanceSnapshot.venue == v)
    base_stmt = base_stmt.order_by(desc(BalanceSnapshot.captured_at)).limit(1)

    as_of = db.execute(base_stmt).scalar_one_or_none()
    if not as_of:
        return [], now_utc()

    stmt = select(BalanceSnapshot).where(BalanceSnapshot.captured_at == as_of)
    if v:
        stmt = stmt.where(BalanceSnapshot.venue == v)

    field, direction = parse_sort(sort, _ALLOWED_SORT, default=("asset", "asc"))
    col = getattr(BalanceSnapshot, field)
    stmt = stmt.order_by(desc(col) if direction == "desc" else asc(col))

    items = db.execute(stmt).scalars().all()
    return items, as_of


def _balance_pricing_venues() -> List[str]:
    """
    Pricing should not depend on the *holding venue*.
    Use a prioritized list so a price can be found on any of these venues.

    .env override:
      UTT_BALANCE_PRICING_VENUES=coinbase,kraken,gemini
    """
    raw = (os.getenv("UTT_BALANCE_PRICING_VENUES", "coinbase,kraken,gemini") or "").strip()
    parts = [p.strip().lower() for p in raw.split(",") if p.strip()]
    # de-dupe preserve order
    out: List[str] = []
    seen = set()
    for v in parts:
        if v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out or ["coinbase"]


# Shared executor for pricing batches (avoid per-batch threadpool creation overhead)
_PRICING_EXEC = ThreadPoolExecutor(max_workers=4)


def _chunk_list(xs: List[str], n: int) -> List[List[str]]:
    if n <= 0:
        return [xs]
    out: List[List[str]] = []
    for i in range(0, len(xs), n):
        out.append(xs[i : i + n])
    return out


def _prices_batch_with_timeout(pricing_venue: str, assets_batch: List[str], timeout_s: float) -> Dict[str, float]:
    """
    Call prices_usd_from_assets() with a hard timeout so one slow batch doesn't wedge the request.
    """
    fut = _PRICING_EXEC.submit(prices_usd_from_assets, pricing_venue, assets_batch)
    return fut.result(timeout=float(timeout_s)) or {}


@router.get("/latest", response_model=BalancesLatestResponse)
def get_latest(
    venue: Optional[str] = Query(default=None),
    sort: Optional[str] = Query(default="asset:asc"),
    with_prices: bool = Query(default=False, description="Opt-in: enrich balances with px_usd and *_usd fields"),
    db: Session = Depends(get_db),
):
    # Fetch from DB quickly
    items, as_of = _latest_balances_raw(db, venue, sort)

    # CRITICAL: release DB connection before any further work (filtering/pricing)
    _release_db_early(db)

    # Backstop filter: Dex-Trade may return/retain a huge list of zero balances.
    # We ALWAYS remove exact-zeros for dex_trade to prevent pricing fan-out and UI timeouts.
    dust = _dex_trade_balance_dust()
    filtered: List[BalanceSnapshot] = []
    for b in items:
        v = (b.venue or "").strip().lower()
        total = float(b.total or 0.0)
        available = float(b.available or 0.0)
        hold = float(b.hold or 0.0)

        if v == "dex_trade":
            # Always drop exact-zero rows (even if dust env is not set)
            if total == 0.0 and available == 0.0 and hold == 0.0:
                continue
            # If dust threshold is configured, drop near-zeros too
            if dust > 0.0 and abs(total) <= dust and abs(available) <= dust and abs(hold) <= dust:
                continue

        filtered.append(b)

    items = filtered

    # Fast path: no pricing
    if not with_prices:
        out = []
        for b in items:
            out.append(
                {
                    "venue": b.venue,
                    "asset": b.asset,
                    "total": float(b.total or 0.0),
                    "available": float(b.available or 0.0),
                    "hold": float(b.hold or 0.0),
                    "captured_at": b.captured_at,
                    "px_usd": None,
                    "total_usd": None,
                    "available_usd": None,
                    "hold_usd": None,
                    "usd_source_symbol": None,
                }
            )
        return {"items": out, "as_of": as_of, "portfolio_total_usd": 0.0}

    # ─────────────────────────────────────────────────────────────
    # Pricing enrichment (opt-in)
    # ─────────────────────────────────────────────────────────────

    # Locally handled stables (expand to cover what you visibly hold)
    stable_px = {
        "USD": 1.0,
        "USDT": 1.0,
        "USDC": 1.0,
        "DAI": 1.0,
        "TUSD": 1.0,
        "GUSD": 1.0,
        "PYUSD": 1.0,
        "USDP": 1.0,
        "FDUSD": 1.0,
    }

    # Build {asset: weight} across ALL items so we can price consistently
    # regardless of which venue holds the asset.
    asset_weight: Dict[str, float] = {}
    for b in items:
        asset = str(b.asset or "").strip().upper()
        if not asset:
            continue

        total = float(b.total or 0.0)
        available = float(b.available or 0.0)
        hold = float(b.hold or 0.0)

        # Generic zero-skip to reduce pricing fan-out
        if total == 0.0 and available == 0.0 and hold == 0.0:
            continue

        # Stablecoins handled locally (no upstream pricing)
        if asset in stable_px:
            continue

        w = abs(total) + abs(available) + abs(hold)
        if w > asset_weight.get(asset, 0.0):
            asset_weight[asset] = w

    # Sort assets by importance (but DO NOT cap to first N; we batch instead)
    aset_sorted = [a for a, _w in sorted(asset_weight.items(), key=lambda kv: kv[1], reverse=True)]

    # Batch sizing + budgets
    batch_size = _price_asset_limit()  # treated as batch size
    per_batch_timeout_s = max(0.5, _price_timeout_ms() / 1000.0)
    total_budget_s = max(5.0, _price_total_budget_ms() / 1000.0)
    deadline = time.monotonic() + float(total_budget_s)

    # Price using prioritized pricing venues and merge results.
    # We price in batches to avoid the "first 80 only" behavior when downstream also caps.
    px_merged: Dict[str, float] = {}
    priced: Set[str] = set()

    remaining = list(aset_sorted)
    pricing_venues = _balance_pricing_venues()

    for pv in pricing_venues:
        if not remaining:
            break
        if time.monotonic() >= deadline:
            break

        # Work off a snapshot of remaining at start of this venue
        batches = _chunk_list(remaining, batch_size)

        for batch in batches:
            if not batch:
                continue
            if time.monotonic() >= deadline:
                break

            # Reduce batch to only those still unpriced (can change mid-loop)
            batch2 = [a for a in batch if a not in priced]
            if not batch2:
                continue

            # Respect remaining time budget for this call
            time_left = deadline - time.monotonic()
            if time_left <= 0:
                break
            timeout_for_call = min(per_batch_timeout_s, max(0.5, time_left))

            try:
                px = _prices_batch_with_timeout(pv, batch2, timeout_for_call) or {}
            except FuturesTimeoutError:
                px = {}
            except Exception:
                px = {}

            if px:
                for k, vpx in px.items():
                    try:
                        kk = str(k).strip().upper()
                        if not kk or kk in priced:
                            continue
                        px_merged[kk] = float(vpx)
                        priced.add(kk)
                    except Exception:
                        continue

        # Recompute remaining after this venue pass
        remaining = [a for a in remaining if a not in priced]

    out = []
    portfolio_total_usd = 0.0

    for b in items:
        asset = str(b.asset or "").strip().upper()

        total = float(b.total or 0.0)
        available = float(b.available or 0.0)
        hold = float(b.hold or 0.0)

        if asset in stable_px:
            px_usd = stable_px[asset]
            usd_source_symbol = asset
        else:
            px_usd = px_merged.get(asset)
            usd_source_symbol = f"{asset}-USD" if px_usd is not None else None

        total_usd = None
        available_usd = None
        hold_usd = None

        if px_usd is not None:
            try:
                total_usd = float(total) * float(px_usd)
                available_usd = float(available) * float(px_usd)
                hold_usd = float(hold) * float(px_usd)
                portfolio_total_usd += float(total_usd)
            except Exception:
                total_usd = None
                available_usd = None
                hold_usd = None

        out.append(
            {
                "venue": b.venue,
                "asset": b.asset,
                "total": total,
                "available": available,
                "hold": hold,
                "captured_at": b.captured_at,
                "px_usd": px_usd,
                "total_usd": total_usd,
                "available_usd": available_usd,
                "hold_usd": hold_usd,
                "usd_source_symbol": usd_source_symbol,
            }
        )

    return {"items": out, "as_of": as_of, "portfolio_total_usd": portfolio_total_usd}


@router.post("/refresh")
def post_refresh(
    venue: Optional[str] = Query(default=None, description="Optional venue if not provided in JSON body"),
    req: Optional[BalanceRefreshRequest] = Body(default=None),
    db: Session = Depends(get_db),
):
    """
    Refresh balances for a venue.

    Accepts either:
      - JSON body: { "venue": "gemini" }
      - Query param: /api/balances/refresh?venue=gemini

    Important UX rule:
      - Empty balances should NOT be treated as an error by default.
        (A venue can legitimately have zero balances or only dust that rounds down.)
      - Strict mode can be enabled via BAL_EMPTY_IS_ERROR=1 (handled in service).
    """
    v = ""
    if req and getattr(req, "venue", None):
        v = str(req.venue or "").strip()
    elif venue:
        v = str(venue or "").strip()

    if not v:
        raise HTTPException(status_code=422, detail="Missing venue. Provide JSON body {venue} or query param ?venue=...")

    try:
        rows, msg = refresh_balances(db, v)
        resp = {"ok": True, "venue": v, "count": len(rows)}
        if msg:
            resp["message"] = msg
        return resp
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Map common upstream failures instead of always 500
        raise _map_upstream_exception("balances refresh", e)
