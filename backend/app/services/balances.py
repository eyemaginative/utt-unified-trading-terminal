# backend/app/services/balances.py

from __future__ import annotations

from sqlalchemy.orm import Session
from sqlalchemy import select, desc, asc
from datetime import datetime
from typing import Optional, Tuple, List, Dict, Any

import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

from ..models import BalanceSnapshot
from ..utils import now_utc, parse_sort
from .symbols import get_adapter

# USD pricing helpers (orderbook-mid derived)
from .market import prices_usd_from_assets

# NEW: use the venue registry as source of truth for “known venues”
from ..venues.registry import get_venue_spec, is_venue_enabled

_ALLOWED_SORT = {"asset", "total", "available", "captured_at"}


# ----------------------------
# Hardening knobs (env vars)
# ----------------------------
def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except Exception:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    return raw == "1" or raw.lower() in {"true", "yes", "y", "on"}


def _norm_venue(v: str) -> str:
    return (v or "").strip().lower()


def _now_mono() -> float:
    return time.monotonic()


def _validate_known_venue(v: str) -> None:
    """
    Validate that a venue exists in the registry and is effectively enabled.

    Notes:
      - Balances refresh is read-only; it must NOT be blocked by DRY_RUN/ARMED.
      - We *do* still require the venue to be configured/enabled (so we don't
        call adapters without creds where creds are required).
      - If you later want “public venues allowed even if not enabled”, mirror
        the dex_trade override logic from services/symbols.py here.
    """
    # Ensure the venue exists (raises KeyError if unknown)
    try:
        _ = get_venue_spec(v)
    except KeyError:
        raise ValueError(f"Unsupported venue '{v}'")

    # Require enabled/configured
    if not bool(is_venue_enabled(v)):
        raise ValueError(f"Venue '{v}' is disabled or not fully configured.")


# Per-venue fetch hard timeout (best-effort; does not kill hung thread, but prevents request-path blocking)
_BAL_FETCH_TIMEOUT_SECONDS = max(1, _env_int("BAL_FETCH_TIMEOUT_SECONDS", 12))

# Cooldown after a failure (avoid hammering a venue that is timing out / rate limiting)
_BAL_FAIL_TTL_SECONDS = max(0, _env_int("BAL_FAIL_TTL_SECONDS", 30))

# Cap number of assets to price per venue (prevents fan-out on dust-heavy accounts)
_BAL_MAX_PRICE_ASSETS = max(0, _env_int("BAL_MAX_PRICE_ASSETS", 80))

# Pricing timeout (hard stop for request path; pricing can fail without failing balances response)
_BAL_PRICE_TIMEOUT_SECONDS = max(1, _env_int("BAL_PRICE_TIMEOUT_SECONDS", 5))

# Per-asset USD price cache TTL (small TTL prevents thundering herd across windows)
_BAL_USD_CACHE_TTL_SECONDS = max(0.25, _env_float("BAL_USD_CACHE_TTL_SECONDS", 2.5))

# If set, refresh errors are fatal (raise). Default: non-fatal (return message)
_BAL_REFRESH_FATAL_ERRORS = _env_bool("BAL_REFRESH_FATAL_ERRORS", False)

# Backward-compatible empty-balances strictness
_BAL_EMPTY_IS_ERROR = _env_bool("BAL_EMPTY_IS_ERROR", False)

# Default behavior for latest_balances() pricing if caller does not specify with_prices
_BAL_WITH_PRICES_DEFAULT = _env_bool("BAL_WITH_PRICES_DEFAULT", False)

# Debug printouts
_BAL_USD_DEBUG = _env_bool("BAL_USD_DEBUG", False)

# Global executor to avoid per-call ThreadPool creation overhead
_BAL_THREADPOOL_WORKERS = max(2, _env_int("BAL_THREADPOOL_WORKERS", 12))
_EXEC = ThreadPoolExecutor(max_workers=_BAL_THREADPOOL_WORKERS)


# ----------------------------
# Failure cooldown cache (refresh path)
# ----------------------------
_FAIL_UNTIL: Dict[str, float] = {}
_FAIL_LOCK = threading.Lock()


def _cooldown_remaining(v: str) -> float:
    if _BAL_FAIL_TTL_SECONDS <= 0:
        return 0.0
    with _FAIL_LOCK:
        until = _FAIL_UNTIL.get(v, 0.0)
    rem = until - _now_mono()
    return rem if rem > 0 else 0.0


def _set_cooldown(v: str) -> None:
    if _BAL_FAIL_TTL_SECONDS <= 0:
        return
    with _FAIL_LOCK:
        _FAIL_UNTIL[v] = _now_mono() + float(_BAL_FAIL_TTL_SECONDS)


# ----------------------------
# In-flight suppression (refresh path)
# ----------------------------
_INFLIGHT_REFRESH: Dict[str, bool] = {}
_INFLIGHT_LOCK = threading.Lock()


def _try_mark_inflight(v: str) -> bool:
    """Returns True if we marked inflight; False if already inflight."""
    with _INFLIGHT_LOCK:
        if _INFLIGHT_REFRESH.get(v, False):
            return False
        _INFLIGHT_REFRESH[v] = True
        return True


def _clear_inflight(v: str) -> None:
    with _INFLIGHT_LOCK:
        _INFLIGHT_REFRESH.pop(v, None)


# ----------------------------
# USD price TTL cache (read path)
# ----------------------------
# Map: (venue, asset) -> (px_usd, expires_mono)
_USD_PX_CACHE: Dict[Tuple[str, str], Tuple[float, float]] = {}
_USD_PX_LOCK = threading.Lock()

# Prevent “double compute” for same (venue, asset) across concurrent requests
_USD_PX_INFLIGHT: Dict[Tuple[str, str], bool] = {}
_USD_PX_INFLIGHT_LOCK = threading.Lock()


def _usd_cache_get(v: str, asset: str) -> Optional[float]:
    key = (v, asset)
    now = _now_mono()
    with _USD_PX_LOCK:
        item = _USD_PX_CACHE.get(key)
    if not item:
        return None
    px, exp = item
    if exp <= now:
        return None
    return float(px)


def _usd_cache_set(v: str, asset: str, px: float) -> None:
    key = (v, asset)
    exp = _now_mono() + float(_BAL_USD_CACHE_TTL_SECONDS)
    with _USD_PX_LOCK:
        _USD_PX_CACHE[key] = (float(px), exp)


def _usd_inflight_try_mark(v: str, asset: str) -> bool:
    key = (v, asset)
    with _USD_PX_INFLIGHT_LOCK:
        if _USD_PX_INFLIGHT.get(key, False):
            return False
        _USD_PX_INFLIGHT[key] = True
        return True


def _usd_inflight_clear(v: str, asset: str) -> None:
    key = (v, asset)
    with _USD_PX_INFLIGHT_LOCK:
        _USD_PX_INFLIGHT.pop(key, None)


# ----------------------------
# Fetch wrapper with hard timeout
# ----------------------------
def _fetch_balances_with_timeout(adapter: Any, timeout_s: int) -> List[dict]:
    """
    Best-effort: returns early on timeout instead of blocking request path.
    Note: timed-out underlying thread may still run to completion in background.
    """
    fut = _EXEC.submit(adapter.fetch_balances, dry_run=False)
    return fut.result(timeout=float(timeout_s))


def refresh_balances(db: Session, venue: str) -> Tuple[List[BalanceSnapshot], Optional[str]]:
    """
    Read-only ingestion.

    Correctness rules:
      - This must NOT be blocked by DRY_RUN / ARMED. Those are trade-routing controls.
      - Always call adapter.fetch_balances(dry_run=False) because balances are read-only.

    Returns:
      (rows_inserted, message_or_none)

    UX rule:
      - If the adapter returns [], we treat it as a non-error by default and return count=0.
        (Enable strict mode via BAL_EMPTY_IS_ERROR=1.)
    """
    v = _norm_venue(venue)

    # NEW: registry-based validation (replaces hard-coded allow-list)
    _validate_known_venue(v)

    # Prevent same-venue concurrent refresh from stacking up (common with multiple windows)
    if not _try_mark_inflight(v):
        msg = f"Skipping refresh for {v}: refresh already in-flight."
        if _BAL_REFRESH_FATAL_ERRORS:
            raise TimeoutError(msg)
        return [], msg

    try:
        # Respect cooldown
        rem = _cooldown_remaining(v)
        if rem > 0:
            msg = f"Skipping refresh for {v}: in cooldown for ~{rem:.1f}s after recent failure."
            if _BAL_REFRESH_FATAL_ERRORS:
                raise TimeoutError(msg)
            return [], msg

        adapter = get_adapter(v)

        # Read-only: never gate on settings.dry_run
        try:
            rows = _fetch_balances_with_timeout(adapter, _BAL_FETCH_TIMEOUT_SECONDS)
        except FutureTimeout:
            _set_cooldown(v)
            msg = (
                f"Timeout fetching balances for {v} after {_BAL_FETCH_TIMEOUT_SECONDS}s; "
                f"entering cooldown {_BAL_FAIL_TTL_SECONDS}s."
            )
            if _BAL_REFRESH_FATAL_ERRORS:
                raise TimeoutError(msg)
            return [], msg
        except Exception as e:
            _set_cooldown(v)
            msg = f"Error fetching balances for {v}: {repr(e)}; entering cooldown {_BAL_FAIL_TTL_SECONDS}s."
            if _BAL_REFRESH_FATAL_ERRORS:
                raise
            return [], msg

        if not rows:
            msg = (
                f"No balances returned from {v}. This can be normal if the account is empty/dust-only. "
                f"If you expected balances: verify API keys/permissions and check backend logs for {v} "
                f"during /api/balances/refresh."
            )
            if _BAL_EMPTY_IS_ERROR:
                raise ValueError(msg)
            return [], msg

        ts = now_utc()
        out: List[BalanceSnapshot] = []

        for r in rows:
            asset = (r.get("asset") if isinstance(r, dict) else None) or None
            if not asset:
                continue

            total = r.get("total")
            available = r.get("available")
            hold = r.get("hold")

            # Normalize numerics
            try:
                total_f = float(total) if total is not None else 0.0
            except Exception:
                total_f = 0.0

            try:
                avail_f = float(available) if available is not None else 0.0
            except Exception:
                avail_f = 0.0

            # Derive hold if missing
            if hold is None:
                derived = total_f - avail_f
                hold_f = derived if derived > 0.0 else 0.0
            else:
                try:
                    hold_f = float(hold)
                except Exception:
                    hold_f = 0.0

            snap = BalanceSnapshot(
                venue=v,
                asset=str(asset),
                total=total_f,
                available=avail_f,
                hold=hold_f,
                captured_at=ts,
            )
            db.add(snap)
            out.append(snap)

        db.commit()
        return out, None
    finally:
        _clear_inflight(v)


def _price_assets_with_timeout(venue: str, assets: List[str]) -> Dict[str, float]:
    """
    Wrap prices_usd_from_assets() with a hard timeout so pricing can never wedge the request.
    """
    if not assets:
        return {}

    v = _norm_venue(venue)

    def _do() -> Dict[str, float]:
        got = prices_usd_from_assets(v, assets) or {}
        out: Dict[str, float] = {}
        for k, px in got.items():
            try:
                out[str(k).upper()] = float(px)
            except Exception:
                continue
        return out

    fut = _EXEC.submit(_do)
    return fut.result(timeout=float(_BAL_PRICE_TIMEOUT_SECONDS))


def _attach_usd_fields_for_group(items: List[BalanceSnapshot], venue: str) -> None:
    """
    Mutates BalanceSnapshot ORM objects by attaching dynamic fields:
      px_usd, total_usd, available_usd, hold_usd, usd_source_symbol

    We do NOT raise on pricing failure; we leave fields as None.

    Hardening:
      - Cap number of priced assets via BAL_MAX_PRICE_ASSETS (default 80)
      - Treat common USD stables as 1.0 to reduce pricing calls
      - Small TTL cache + single-flight per-asset avoids thundering herd across windows
      - Hard timeout around pricing prevents 30s UI timeouts
    """
    v = _norm_venue(venue)

    # Fast-path stablecoins / USD: no network pricing required
    stable_px: Dict[str, float] = {
        "USD": 1.0,
        "USDT": 1.0,
        "USDC": 1.0,
        "DAI": 1.0,
        "TUSD": 1.0,
    }

    # Build asset -> weight (rough importance) and preserve stablecoins
    asset_weight: Dict[str, float] = {}
    for b in items or []:
        a = (getattr(b, "asset", "") or "").strip().upper()
        if not a:
            continue
        total = float(getattr(b, "total", 0.0) or 0.0)
        avail = float(getattr(b, "available", 0.0) or 0.0)
        hold = float(getattr(b, "hold", 0.0) or 0.0)
        mag = abs(total) + abs(avail) + abs(hold)
        prev = asset_weight.get(a, 0.0)
        if mag > prev:
            asset_weight[a] = mag

    # Construct pricing list (exclude pure-zero non-stables)
    assets_all: List[Tuple[str, float]] = []
    for a, w in asset_weight.items():
        if a in stable_px:
            assets_all.append((a, float("inf")))  # always include stables
        else:
            if w <= 0.0:
                continue
            assets_all.append((a, w))

    # Sort: stables first, then by magnitude desc
    assets_all.sort(key=lambda t: t[1], reverse=True)
    assets_to_price = [a for a, _w in assets_all]

    # Cap
    if _BAL_MAX_PRICE_ASSETS > 0 and len(assets_to_price) > _BAL_MAX_PRICE_ASSETS:
        assets_to_price = assets_to_price[:_BAL_MAX_PRICE_ASSETS]

    # Fill stable prices immediately
    prices: Dict[str, float] = {a: stable_px[a] for a in assets_to_price if a in stable_px}

    # Attempt cache hits for non-stables
    assets_need: List[str] = []
    for a in assets_to_price:
        if a in stable_px:
            continue
        px_cached = _usd_cache_get(v, a)
        if px_cached is not None:
            prices[a] = float(px_cached)
        else:
            assets_need.append(a)

    # Single-flight marking for assets we will compute now
    assets_compute: List[str] = []
    for a in assets_need:
        if _usd_inflight_try_mark(v, a):
            assets_compute.append(a)

    # Compute missing prices (best effort) with timeout
    computed: Dict[str, float] = {}
    try:
        if assets_compute:
            computed = _price_assets_with_timeout(v, assets_compute)
    except FutureTimeout:
        computed = {}
        if _BAL_USD_DEBUG:
            print(f"BAL_USD_DEBUG pricing timeout venue={v} assets={assets_compute[:10]}...")
    except Exception as e:
        computed = {}
        if _BAL_USD_DEBUG:
            print(f"BAL_USD_DEBUG pricing error venue={v}: {repr(e)}")
    finally:
        for a in assets_compute:
            _usd_inflight_clear(v, a)

    # Update cache + prices dict
    for a, px in computed.items():
        try:
            px_f = float(px)
        except Exception:
            continue
        prices[a] = px_f
        _usd_cache_set(v, a, px_f)

    if _BAL_USD_DEBUG:
        have = sorted(prices.keys())
        print(
            f"BAL_USD_DEBUG venue={v} "
            f"assets_total={len(asset_weight)} "
            f"assets_pricing={len(assets_to_price)} "
            f"priced={len(have)} "
            f"sample_priced={have[:20]}"
        )

    # Attach fields
    for b in items or []:
        asset = (getattr(b, "asset", "") or "").strip().upper()
        total = float(getattr(b, "total", 0.0) or 0.0)
        avail = float(getattr(b, "available", 0.0) or 0.0)
        hold = float(getattr(b, "hold", 0.0) or 0.0)

        px = prices.get(asset)

        setattr(b, "px_usd", float(px) if px is not None else None)

        if px is None:
            setattr(b, "total_usd", None)
            setattr(b, "available_usd", None)
            setattr(b, "hold_usd", None)
            setattr(b, "usd_source_symbol", None)
        else:
            setattr(b, "total_usd", total * float(px))
            setattr(b, "available_usd", avail * float(px))
            setattr(b, "hold_usd", hold * float(px))

            if asset in stable_px:
                setattr(b, "usd_source_symbol", asset)
            else:
                setattr(b, "usd_source_symbol", f"{asset}-USD")


def latest_balances(
    db: Session,
    venue: Optional[str],
    sort: Optional[str],
    with_prices: Optional[bool] = None,
) -> Tuple[list[BalanceSnapshot], datetime]:
    """
    Important: this function does DB reads and may do network pricing enrichment.
    We must NOT hold a checked-out DB connection while doing network calls.

    Strategy:
      - Perform DB queries
      - Materialize rows
      - Detach (expunge) and end the transaction (rollback) to return the pooled connection
      - Then do pricing enrichment on detached objects (optional)
    """
    if with_prices is None:
        with_prices = bool(_BAL_WITH_PRICES_DEFAULT)

    v = _norm_venue(venue) if venue else None

    base_stmt = select(BalanceSnapshot.captured_at)
    if v:
        base_stmt = base_stmt.where(BalanceSnapshot.venue == v)
    base_stmt = base_stmt.order_by(desc(BalanceSnapshot.captured_at)).limit(1)

    as_of = db.execute(base_stmt).scalar_one_or_none()
    if not as_of:
        try:
            db.rollback()
        except Exception:
            pass
        return [], now_utc()

    stmt = select(BalanceSnapshot).where(BalanceSnapshot.captured_at == as_of)
    if v:
        stmt = stmt.where(BalanceSnapshot.venue == v)

    field, direction = parse_sort(sort, _ALLOWED_SORT, default=("asset", "asc"))
    col = getattr(BalanceSnapshot, field)
    stmt = stmt.order_by(desc(col) if direction == "desc" else asc(col))

    items = db.execute(stmt).scalars().all()

    # Detach rows + end transaction BEFORE any network pricing calls.
    try:
        db.expunge_all()
    except Exception:
        pass
    try:
        db.rollback()
    except Exception:
        pass

    # Optional enrichment (fast-path when disabled)
    if not with_prices:
        return items, as_of

    if v:
        _attach_usd_fields_for_group(items, v)
    else:
        by_venue: Dict[str, List[BalanceSnapshot]] = {}
        for b in items:
            bv = _norm_venue(getattr(b, "venue", "") or "")
            if not bv:
                continue
            by_venue.setdefault(bv, []).append(b)

        for bv, group in by_venue.items():
            _attach_usd_fields_for_group(group, bv)

    return items, as_of
