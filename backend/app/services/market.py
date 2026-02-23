# backend/app/services/market.py

from __future__ import annotations

from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed

import threading
import os
import time

from .symbols import get_adapter, resolve_symbol
from ..config import settings

import httpx  # kept here per your original file layout


# ─────────────────────────────────────────────────────────────
# Time helpers
# ─────────────────────────────────────────────────────────────

def _utcnow() -> datetime:
    return datetime.utcnow()


def _mono() -> float:
    return time.monotonic()


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


def _age_seconds(ts: datetime) -> Optional[float]:
    try:
        return float((_utcnow() - ts).total_seconds())
    except Exception:
        return None


def _cache_ok(ts: datetime, ttl: float) -> bool:
    try:
        age = (_utcnow() - ts).total_seconds()
        return age <= float(ttl)
    except Exception:
        return False


def _stale_within(ts: datetime, stale_max: float) -> bool:
    """
    Bounded stale policy.
    - stale_max <= 0: treat as "unbounded" (legacy behavior)
    - otherwise: allow stale fallback only if age <= stale_max
    """
    try:
        sm = float(stale_max)
        if sm <= 0:
            return True
        age = (_utcnow() - ts).total_seconds()
        return age <= sm
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────
# Tunables (env-overridable)
# ─────────────────────────────────────────────────────────────

# Orderbook TTL should be small (prevents thundering herd, stays “near realtime”)
_OB_TTL_SECONDS = float(os.getenv("UTT_ORDERBOOK_TTL_SECONDS", "2.0"))

# Price TTL (mid derived from depth=1 orderbook); for valuation use-cases we can keep slightly longer
# to avoid re-pricing the same symbol multiple times during one UI refresh.
_PRICE_TTL_SECONDS = float(os.getenv("UTT_PRICE_TTL_SECONDS", "10.0"))

# NEW: bounded stale fallback windows (prevents “stuck stale snapshot forever”)
# - Orderbook stale max: keep tight by default (UI wants near-realtime)
# - Price stale max: keep generous (balances should not flap to missing during transient outages)
_OB_STALE_MAX_SECONDS = float(os.getenv("UTT_ORDERBOOK_STALE_MAX_SECONDS", "120"))   # 2 minutes
_PRICE_STALE_MAX_SECONDS = float(os.getenv("UTT_PRICE_STALE_MAX_SECONDS", "3600"))  # 1 hour

# Markets TTL
_MARKETS_TTL_SECONDS = float(os.getenv("UTT_MARKETS_TTL_SECONDS", "300"))  # 5 minutes

# Hard timeout around adapter.fetch_orderbook()
_OB_TIMEOUT_SECONDS = float(os.getenv("UTT_ORDERBOOK_TIMEOUT_SECONDS", "6"))

# Negative cache TTLs
_NEG_PAIR_TTL_SECONDS = float(os.getenv("UTT_ORDERBOOK_NEG_PAIR_TTL_SECONDS", "120"))
_NEG_RATE_TTL_SECONDS = float(os.getenv("UTT_ORDERBOOK_NEG_RATE_TTL_SECONDS", "60"))

# Venue cooldown on rate limit (prevents hammering)
_RATE_COOLDOWN_SECONDS = float(os.getenv("UTT_RATE_COOLDOWN_SECONDS", "45"))

# Concurrency bounds
_OB_GLOBAL_MAX_CONCURRENCY = max(1, _env_int("UTT_ORDERBOOK_GLOBAL_MAX_CONCURRENCY", 8))
_OB_VENUE_MAX_CONCURRENCY = max(1, _env_int("UTT_ORDERBOOK_VENUE_MAX_CONCURRENCY", 3))

# Executors (shared, small)
_OB_EXEC_WORKERS = max(2, _env_int("UTT_ORDERBOOK_EXEC_WORKERS", 8))

# Pricing workers: keep modest; pricing uses many orderbooks across assets
_MAX_PRICE_WORKERS = max(2, _env_int("UTT_MAX_PRICE_WORKERS", 6))

# Hard cap on assets priced per call (router can still chunk, but this prevents worst-case fan-out)
# IMPORTANT: default raised so owned-asset valuation is not truncated.
_MAX_PRICE_ASSETS_PER_CALL = max(1, _env_int("UTT_MAX_PRICE_ASSETS_PER_CALL", 5000))

# Pricing fallbacks: if an asset can’t be priced on the primary pricing venue, try other venues.
# Example: UTT_PRICING_FALLBACK_VENUES=coinbase,kraken,gemini
_PRICING_FALLBACK_VENUES_RAW = (os.getenv("UTT_PRICING_FALLBACK_VENUES", "coinbase,kraken,gemini") or "").strip()

# IMPORTANT: orderbooks should be treated as public market data. Some adapters may
# behave differently when dry_run=False (auth / private client / extra latency).
# If UTT_ORDERBOOK_FORCE_PUBLIC=1, we always pass True into adapter.fetch_orderbook(..., dry_run_flag).
_ORDERBOOK_FORCE_PUBLIC = (os.getenv("UTT_ORDERBOOK_FORCE_PUBLIC", "1") or "").strip().lower() in {"1", "true", "yes", "y", "on"}

# Debug
_MARKET_DEBUG = (os.getenv("UTT_MARKET_DEBUG", "") or "").strip().lower() in {"1", "true", "yes", "y", "on"}


# ─────────────────────────────────────────────────────────────
# Caches
# ─────────────────────────────────────────────────────────────

# key: (venue, symbol_canon, depth) -> (ts_utc, data)
_OB_CACHE: Dict[Tuple[str, str, int], Tuple[datetime, Any]] = {}
_OB_CACHE_LOCK = threading.Lock()

# key: (venue, symbol_canon) -> (ts_utc, mid_price)
_PRICE_CACHE: Dict[Tuple[str, str], Tuple[datetime, float]] = {}
_PRICE_CACHE_LOCK = threading.Lock()

# Negative cache to avoid hammering failing upstream pairs
# key: (venue, symbol_canon) -> (ts_utc, kind, message)
# kind in {"pair_not_found", "rate_limited"}
_NEG_SYM_CACHE: Dict[Tuple[str, str], Tuple[datetime, str, str]] = {}
_NEG_SYM_LOCK = threading.Lock()

# Keep the depth-specific negative cache too (optional precision)
# key: (venue, symbol_canon, depth) -> (ts_utc, kind, message)
_OB_NEG_CACHE: Dict[Tuple[str, str, int], Tuple[datetime, str, str]] = {}
_OB_NEG_CACHE_LOCK = threading.Lock()

# Markets cache: key (venue) -> (ts_utc, items)
_MARKETS_CACHE: Dict[str, Tuple[datetime, List[Dict[str, Any]]]] = {}
_MARKETS_LOCK = threading.Lock()

# Venue cooldowns on 429s
# key: venue -> cooldown_until_mono
_VENUE_COOLDOWN: Dict[str, float] = {}
_VENUE_COOLDOWN_LOCK = threading.Lock()


# ─────────────────────────────────────────────────────────────
# Single-flight (in-flight de-duplication)
# ─────────────────────────────────────────────────────────────

# Orderbook single-flight per key
_OB_INFLIGHT: Dict[Tuple[str, str, int], threading.Event] = {}
_OB_INFLIGHT_ERR: Dict[Tuple[str, str, int], Exception] = {}
_OB_INFLIGHT_LOCK = threading.Lock()

# Price single-flight per key
_PRICE_INFLIGHT: Dict[Tuple[str, str], threading.Event] = {}
_PRICE_INFLIGHT_ERR: Dict[Tuple[str, str], Exception] = {}
_PRICE_INFLIGHT_LOCK = threading.Lock()


# ─────────────────────────────────────────────────────────────
# Concurrency bounds
# ─────────────────────────────────────────────────────────────

_OB_GLOBAL_SEM = threading.Semaphore(_OB_GLOBAL_MAX_CONCURRENCY)
_OB_VENUE_SEMS: Dict[str, threading.Semaphore] = {}
_OB_VENUE_SEMS_LOCK = threading.Lock()


def _venue_sem(venue: str) -> threading.Semaphore:
    v = (venue or "").strip().lower()
    with _OB_VENUE_SEMS_LOCK:
        sem = _OB_VENUE_SEMS.get(v)
        if sem is None:
            sem = threading.Semaphore(_OB_VENUE_MAX_CONCURRENCY)
            _OB_VENUE_SEMS[v] = sem
        return sem


# ─────────────────────────────────────────────────────────────
# Executors
# ─────────────────────────────────────────────────────────────

_OB_EXEC = ThreadPoolExecutor(max_workers=_OB_EXEC_WORKERS)
_PRICE_EXEC = ThreadPoolExecutor(max_workers=_MAX_PRICE_WORKERS)


# ─────────────────────────────────────────────────────────────
# Error classification
# ─────────────────────────────────────────────────────────────

def _looks_pair_not_found(msg: str) -> bool:
    low = (msg or "").lower()
    return (
        "pair not found" in low
        or "symbol not found" in low
        or "unknown symbol" in low
        or "invalid symbol" in low
        or "invalidsymbol" in low
        or ("not found" in low and "symbol" in low)
        or ("product not found" in low)
        or ("unknown product" in low)
        or ("not a valid symbol" in low)
    )


def _looks_rate_limited(msg: str) -> bool:
    low = (msg or "").lower()
    return (
        "429" in low
        or "too many requests" in low
        or "rate limit" in low
        or "rate limited" in low
        or "throttle" in low
        or "cooldown" in low
        or "420" in low
        or "too many errors" in low
    )


def _neg_ttl_for_kind(kind: str) -> float:
    if kind == "pair_not_found":
        return _NEG_PAIR_TTL_SECONDS
    if kind == "rate_limited":
        return _NEG_RATE_TTL_SECONDS
    return 30.0


def _neg_cache_ok(ts: datetime, kind: str) -> bool:
    return _cache_ok(ts, ttl=_neg_ttl_for_kind(kind))


def _cooldown_remaining(v: str) -> float:
    v = (v or "").strip().lower()
    now = _mono()
    with _VENUE_COOLDOWN_LOCK:
        until = _VENUE_COOLDOWN.get(v, 0.0)
    rem = until - now
    return rem if rem > 0 else 0.0


def _set_cooldown(v: str, seconds: float) -> None:
    v = (v or "").strip().lower()
    with _VENUE_COOLDOWN_LOCK:
        _VENUE_COOLDOWN[v] = _mono() + float(seconds)


# ─────────────────────────────────────────────────────────────
# Cache getters (with bounded stale fallback)
# ─────────────────────────────────────────────────────────────

def _get_cached_ob_entry(key: Tuple[str, str, int]) -> Optional[Tuple[datetime, Any]]:
    with _OB_CACHE_LOCK:
        return _OB_CACHE.get(key)


def _get_cached_ob(key: Tuple[str, str, int], stale_ok: bool) -> Optional[Tuple[str, Any]]:
    hit = _get_cached_ob_entry(key)
    if not hit:
        return None
    ts, book = hit

    if _cache_ok(ts, ttl=_OB_TTL_SECONDS):
        return key[1], book

    if stale_ok and _stale_within(ts, _OB_STALE_MAX_SECONDS):
        return key[1], book

    return None


def _get_cached_price_entry(k: Tuple[str, str]) -> Optional[Tuple[datetime, float]]:
    with _PRICE_CACHE_LOCK:
        return _PRICE_CACHE.get(k)


def _get_cached_price(k: Tuple[str, str], stale_ok: bool) -> Optional[float]:
    hit = _get_cached_price_entry(k)
    if not hit:
        return None
    ts, px = hit

    if _cache_ok(ts, ttl=_PRICE_TTL_SECONDS):
        try:
            return float(px)
        except Exception:
            return None

    if stale_ok and _stale_within(ts, _PRICE_STALE_MAX_SECONDS):
        try:
            return float(px)
        except Exception:
            return None

    return None


def _set_ob_cache(key: Tuple[str, str, int], book: Any) -> None:
    with _OB_CACHE_LOCK:
        _OB_CACHE[key] = (_utcnow(), book)


def _set_price_cache(k: Tuple[str, str], px: float) -> None:
    with _PRICE_CACHE_LOCK:
        _PRICE_CACHE[k] = (_utcnow(), float(px))


# ─────────────────────────────────────────────────────────────
# Negative cache
# ─────────────────────────────────────────────────────────────

def _neg_get_sym(v: str, sym: str) -> Optional[Tuple[str, str]]:
    key = (v, sym)
    with _NEG_SYM_LOCK:
        hit = _NEG_SYM_CACHE.get(key)
    if not hit:
        return None
    ts, kind, msg = hit
    if _neg_cache_ok(ts, kind):
        return kind, msg
    return None


def _neg_set_sym(v: str, sym: str, kind: str, msg: str) -> None:
    key = (v, sym)
    with _NEG_SYM_LOCK:
        _NEG_SYM_CACHE[key] = (_utcnow(), kind, msg)


def _neg_get_depth(v: str, sym: str, depth: int) -> Optional[Tuple[str, str]]:
    key = (v, sym, int(depth))
    with _OB_NEG_CACHE_LOCK:
        hit = _OB_NEG_CACHE.get(key)
    if not hit:
        return None
    ts, kind, msg = hit
    if _neg_cache_ok(ts, kind):
        return kind, msg
    return None


def _neg_set_depth(v: str, sym: str, depth: int, kind: str, msg: str) -> None:
    key = (v, sym, int(depth))
    with _OB_NEG_CACHE_LOCK:
        _OB_NEG_CACHE[key] = (_utcnow(), kind, msg)


# ─────────────────────────────────────────────────────────────
# Core: orderbook snapshot
# ─────────────────────────────────────────────────────────────

def orderbook_snapshot(
    venue: str,
    symbol_canon: str,
    depth: int,
    stale_ok: bool = False,
    force: bool = False,
):
    """
    Returns (symbol_canon, book) where book = {bids:[{price,qty}], asks:[{price,qty}]}

    Behavior:
      - Normal mode: uses TTL cache; stale_ok can serve bounded stale snapshots on upstream issues.
      - Force mode: bypasses TTL cache and attempts live fetch; still can fall back to bounded stale
        if stale_ok=True and upstream fails.

    Hardening:
      - TTL cache (small)
      - single-flight per (venue,symbol,depth)
      - negative cache for pair-not-found / rate-limited (skipped when force=True)
      - venue cooldown on 429-like responses (skipped when force=True)
      - bounded concurrency (global + per-venue)
      - strict timeout around adapter.fetch_orderbook()
    """
    v = (venue or "").strip().lower()
    sym = (symbol_canon or "").strip().upper()
    d = int(depth)

    if not v or not sym or d <= 0:
        raise ValueError("orderbook_snapshot: venue, symbol_canon, depth are required")

    key = (v, sym, d)

    # If venue is cooling down (rate limited), return cached if possible; else fail fast.
    # In force mode we DO attempt anyway (manual refresh intent).
    if not force:
        rem = _cooldown_remaining(v)
        if rem > 0:
            cached = _get_cached_ob(key, stale_ok=True)
            if cached is not None:
                return cached
            raise Exception(f"Rate limited by {v} (cooldown ~{rem:.1f}s)")

        # Negative cache checks (symbol-level and depth-level)
        neg = _neg_get_sym(v, sym) or _neg_get_depth(v, sym, d)
        if neg:
            kind, msg = neg
            if kind == "rate_limited":
                _set_cooldown(v, _RATE_COOLDOWN_SECONDS)
            cached = _get_cached_ob(key, stale_ok=True) if stale_ok else None
            if cached is not None:
                return cached
            raise Exception(msg or ("Pair Not Found" if kind == "pair_not_found" else "Rate limited"))

    # Positive cache check (skip entirely if force=True)
    if not force:
        cached = _get_cached_ob(key, stale_ok=stale_ok)
        if cached is not None:
            return cached

    # Single-flight: if another thread is already fetching this key, wait for it.
    leader = False
    with _OB_INFLIGHT_LOCK:
        evt = _OB_INFLIGHT.get(key)
        if evt is None:
            evt = threading.Event()
            _OB_INFLIGHT[key] = evt
            _OB_INFLIGHT_ERR.pop(key, None)
            leader = True

    if not leader:
        evt.wait(timeout=float(_OB_TIMEOUT_SECONDS) + 0.5)

        # In force mode, the in-flight leader might have refreshed the cache;
        # we still accept the result as it will be the newest available.
        cached2 = _get_cached_ob(key, stale_ok=stale_ok)
        if cached2 is not None:
            return cached2

        with _OB_INFLIGHT_LOCK:
            err = _OB_INFLIGHT_ERR.get(key)
        if err:
            raise err

        raise TimeoutError(f"orderbook: in-flight wait timeout for {v} {sym} depth={d}")

    # Leader path: do the upstream fetch with concurrency bounds + timeout
    try:
        # Re-check cache after becoming leader (only if not force)
        if not force:
            cached3 = _get_cached_ob(key, stale_ok=stale_ok)
            if cached3 is not None:
                return cached3

        # Resolve to venue symbol
        _, symbol_venue = resolve_symbol(v, sym)
        adapter = get_adapter(v)

        # Concurrency bounds: acquire global + per-venue
        acquired_global = _OB_GLOBAL_SEM.acquire(timeout=0.25)
        if not acquired_global:
            cached_stale = _get_cached_ob(key, stale_ok=True)
            if cached_stale is not None:
                return cached_stale
            raise TimeoutError(f"orderbook: global concurrency saturated for {v}")

        venue_sem = _venue_sem(v)
        acquired_venue = venue_sem.acquire(timeout=0.25)
        if not acquired_venue:
            _OB_GLOBAL_SEM.release()
            cached_stale = _get_cached_ob(key, stale_ok=True)
            if cached_stale is not None:
                return cached_stale
            raise TimeoutError(f"orderbook: venue concurrency saturated for {v}")

        try:
            # IMPORTANT: treat orderbooks as public market data
            dry_flag = True if _ORDERBOOK_FORCE_PUBLIC else bool(settings.dry_run)
            fut = _OB_EXEC.submit(adapter.fetch_orderbook, symbol_venue, d, dry_flag)
            book = fut.result(timeout=_OB_TIMEOUT_SECONDS)
        except FuturesTimeoutError:
            try:
                fut.cancel()
            except Exception:
                pass
            cached_stale = _get_cached_ob(key, stale_ok=True)
            if cached_stale is not None:
                return cached_stale
            raise TimeoutError(f"orderbook: {v} timeout after {_OB_TIMEOUT_SECONDS}s")
        except Exception as e:
            msg = str(e or "")
            kind = None
            if _looks_pair_not_found(msg):
                kind = "pair_not_found"
                msg = f"Pair Not Found at {v}"
            elif _looks_rate_limited(msg):
                kind = "rate_limited"
                msg = f"Rate limited by {v}"
                _set_cooldown(v, _RATE_COOLDOWN_SECONDS)

            # In force mode we still record negative cache (to protect the server),
            # but we did not block entry using those caches at the start of the call.
            if kind:
                _neg_set_sym(v, sym, kind, msg)
                _neg_set_depth(v, sym, d, kind, msg)

            cached_stale = _get_cached_ob(key, stale_ok=True) if stale_ok else None
            if cached_stale is not None:
                return cached_stale
            raise
        finally:
            try:
                venue_sem.release()
            except Exception:
                pass
            try:
                _OB_GLOBAL_SEM.release()
            except Exception:
                pass

        _set_ob_cache(key, book)
        return sym, book

    except Exception as e:
        with _OB_INFLIGHT_LOCK:
            _OB_INFLIGHT_ERR[key] = e
        raise
    finally:
        with _OB_INFLIGHT_LOCK:
            evt = _OB_INFLIGHT.pop(key, None)
        if evt:
            evt.set()


# ─────────────────────────────────────────────────────────────
# Mid price helpers
# ─────────────────────────────────────────────────────────────

def _mid_from_book(book: Dict[str, Any]) -> Optional[float]:
    """
    Mid = (best_bid + best_ask)/2 if both exist.
    """
    try:
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        if not bids or not asks:
            return None
        bb = float(bids[0]["price"])
        ba = float(asks[0]["price"])
        if bb <= 0 or ba <= 0:
            return None
        return (bb + ba) / 2.0
    except Exception:
        return None


def price_usd_for_symbol(venue: str, symbol_canon: str, stale_ok: bool = False) -> Optional[float]:
    """
    Best-effort USD-ish price for a canonical symbol like BTC-USD, ETH-USD.
    Derived from the orderbook mid (depth=1). Cached. Single-flight.
    """
    v = (venue or "").strip().lower()
    sym = (symbol_canon or "").strip().upper()
    if not v or not sym:
        return None

    k = (v, sym)

    cached = _get_cached_price(k, stale_ok=stale_ok)
    if cached is not None:
        return float(cached)

    # Single-flight
    leader = False
    with _PRICE_INFLIGHT_LOCK:
        evt = _PRICE_INFLIGHT.get(k)
        if evt is None:
            evt = threading.Event()
            _PRICE_INFLIGHT[k] = evt
            _PRICE_INFLIGHT_ERR.pop(k, None)
            leader = True

    if not leader:
        evt.wait(timeout=float(_OB_TIMEOUT_SECONDS) + 0.5)
        cached2 = _get_cached_price(k, stale_ok=stale_ok)
        if cached2 is not None:
            return float(cached2)
        with _PRICE_INFLIGHT_LOCK:
            err = _PRICE_INFLIGHT_ERR.get(k)
        if err:
            raise err
        return None

    try:
        # NOTE: pricing should NOT be "force" by default; keep it stable and TTL'd.
        _, book = orderbook_snapshot(v, sym, depth=1, stale_ok=stale_ok, force=False)
        mid = _mid_from_book(book)
        if mid is None:
            return None
        _set_price_cache(k, float(mid))
        return float(mid)
    except Exception as e:
        with _PRICE_INFLIGHT_LOCK:
            _PRICE_INFLIGHT_ERR[k] = e
        raise
    finally:
        with _PRICE_INFLIGHT_LOCK:
            evt = _PRICE_INFLIGHT.pop(k, None)
        if evt:
            evt.set()


# ─────────────────────────────────────────────────────────────
# Pricing from assets (market-aware, low-fanout)
# ─────────────────────────────────────────────────────────────

# Stablecoins treated as ~1.0 USD in pricing.
_STABLE_USD: Dict[str, float] = {
    "USD": 1.0,
    "USDT": 1.0,
    "USDC": 1.0,
    "GUSD": 1.0,
    "PYUSD": 1.0,
    "USDP": 1.0,
    "DAI": 1.0,
    "TUSD": 1.0,
    "FDUSD": 1.0,
}

# Preferred stable quote order (try these first if they exist on venue)
_STABLE_QUOTE_ORDER: List[str] = ["USD", "USDT", "USDC", "GUSD", "PYUSD", "USDP", "DAI", "TUSD", "FDUSD"]

# Bridge quotes: price ASSET-BTC then multiply by BTC-USD, etc.
_BRIDGE_QUOTES: List[str] = ["BTC", "ETH"]


def _pricing_venue_for_usd(venue: str) -> str:
    """
    Robinhood pricing fallback for USD valuations.
    Controlled by ROBINHOOD_PRICING_VENUE=coinbase|kraken|gemini (default: coinbase)
    """
    v = (venue or "").strip().lower()
    if v != "robinhood":
        return v

    pv = (os.getenv("ROBINHOOD_PRICING_VENUE", "coinbase") or "coinbase").strip().lower()
    if pv not in ("coinbase", "kraken", "gemini"):
        pv = "coinbase"
    return pv


def _parse_pricing_venues(primary: str) -> List[str]:
    """
    Returns a de-duped list where primary is first, then fallbacks.
    """
    p = (primary or "").strip().lower()
    raw = _PRICING_FALLBACK_VENUES_RAW
    parts = [x.strip().lower() for x in raw.split(",") if x.strip()] if raw else []
    out: List[str] = []
    seen = set()
    for v in [p] + parts:
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return out


def _canon(sym: str) -> str:
    return (sym or "").strip().upper()


def _clean_asset(a: Optional[str]) -> str:
    return (a or "").strip().upper()


def _markets_cache_ok(ts: datetime) -> bool:
    return _cache_ok(ts, ttl=_MARKETS_TTL_SECONDS)


def _coinbase_products_public() -> List[Dict[str, Any]]:
    url = "https://api.exchange.coinbase.com/products"
    with httpx.Client(timeout=20.0) as c:
        r = c.get(url)
        r.raise_for_status()
        data = r.json()

    items: List[Dict[str, Any]] = []
    if not isinstance(data, list):
        return items

    for it in data:
        try:
            pid = _canon(it.get("id", ""))
            base = _canon(it.get("base_currency", ""))
            quote = _canon(it.get("quote_currency", ""))
            if not pid or "-" not in pid:
                continue
            if not base or not quote:
                parts = pid.split("-")
                if len(parts) >= 2:
                    base = base or _canon(parts[0])
                    quote = quote or _canon("-".join(parts[1:]))
            items.append(
                {
                    "venue": "coinbase",
                    "symbol_canon": pid,
                    "base": base or None,
                    "quote": quote or None,
                    "symbol_venue": pid,
                }
            )
        except Exception:
            continue

    return items


def _gemini_symbols_public() -> List[Dict[str, Any]]:
    url = "https://api.gemini.com/v1/symbols"
    with httpx.Client(timeout=20.0) as c:
        r = c.get(url)
        r.raise_for_status()
        data = r.json()

    if not isinstance(data, list):
        return []

    quotes = ["USDT", "PYUSD", "GUSD", "USDC", "USD", "EUR", "GBP", "BTC", "ETH", "SOL", "DAI"]
    quotes_sorted = sorted(quotes, key=lambda x: len(x), reverse=True)

    def parse(sym: str) -> Tuple[str, str]:
        s = (sym or "").strip().upper()
        for q in quotes_sorted:
            if s.endswith(q) and len(s) > len(q):
                b = s[: -len(q)]
                return b, q
        return s, ""

    items: List[Dict[str, Any]] = []
    for raw in data:
        try:
            s = str(raw).strip().lower()
            if not s:
                continue
            base, quote = parse(s)
            if not base or not quote:
                continue
            canon = f"{base}-{quote}"
            items.append(
                {
                    "venue": "gemini",
                    "symbol_canon": canon,
                    "base": base,
                    "quote": quote,
                    "symbol_venue": s,
                }
            )
        except Exception:
            continue
    return items


def _kraken_assetpairs_public() -> List[Dict[str, Any]]:
    url = "https://api.kraken.com/0/public/AssetPairs"
    with httpx.Client(timeout=20.0) as c:
        r = c.get(url)
        r.raise_for_status()
        data = r.json()

    result = data.get("result") if isinstance(data, dict) else None
    if not isinstance(result, dict):
        return []

    map_codes = {"XBT": "BTC", "XDG": "DOGE"}

    def norm_code(x: str) -> str:
        s = _canon(x)
        return map_codes.get(s, s)

    items: List[Dict[str, Any]] = []
    for k, v in result.items():
        try:
            if not isinstance(v, dict):
                continue

            wsname = v.get("wsname")
            if wsname and isinstance(wsname, str) and "/" in wsname:
                b, q = wsname.split("/", 1)
                base = norm_code(b)
                quote = norm_code(q)
            else:
                continue

            if not base or not quote:
                continue

            canon = f"{base}-{quote}"
            altname = str(v.get("altname") or "")
            if altname.endswith(".d") or str(k).endswith(".d"):
                continue

            items.append(
                {
                    "venue": "kraken",
                    "symbol_canon": canon,
                    "base": base,
                    "quote": quote,
                    "symbol_venue": str(k),
                }
            )
        except Exception:
            continue

    seen = set()
    out = []
    for it in items:
        sym = it.get("symbol_canon")
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(it)
    return out


def _fetch_venue_markets_uncached(venue: str) -> List[Dict[str, Any]]:
    v = (venue or "").strip().lower()

    if v in ("cryptocom", "crypto_com", "crypto.com", "crypto-com"):
        # Crypto.com Exchange public instruments list
        try:
            url = "https://api.crypto.com/exchange/v1/public/get-instruments"
            with httpx.Client(timeout=15.0) as client:
                r = client.get(url, headers={"Accept": "application/json"})
                r.raise_for_status()
                data = r.json() if r.content else {}

            res = data.get("result") or {}
            rows = res.get("data") or res.get("instruments") or []
            if not isinstance(rows, list):
                rows = []

            out: List[Dict[str, Any]] = []
            for it in rows:
                if not isinstance(it, dict):
                    continue

                # Best-effort filters: reject explicit derivatives; accept spot-like/blank types.
                inst_type = str(it.get("inst_type") or it.get("type") or "").upper().strip()
                if inst_type and any(x in inst_type for x in ("PERP", "PERPETUAL", "FUT", "SWAP", "OPTION")):
                    continue

                tradable = it.get("tradable")
                if tradable is False:
                    continue

                base = str(it.get("base_ccy") or it.get("base_currency") or it.get("base") or "").strip().upper()
                quote = str(it.get("quote_ccy") or it.get("quote_currency") or it.get("quote") or "").strip().upper()
                if not base or not quote:
                    continue

                out.append(
                    {
                        "venue": "cryptocom",
                        "symbol_canon": f"{base}-{quote}",
                        "base": base,
                        "quote": quote,
                        # Use canonical dash format; adapter.resolve_symbol can map to venue instrument id.
                        "symbol_venue": f"{base}-{quote}",
                    }
                )

            return out
        except Exception:
            # Any failure here should not break the caller; fall through to empty.
            return []

    if v == "coinbase":
        return _coinbase_products_public()
    if v == "gemini":
        return _gemini_symbols_public()
    if v == "kraken":
        return _kraken_assetpairs_public()
    return []


def venue_markets_snapshot(venue: str, asset: Optional[str] = None, limit: int = 0) -> Tuple[List[Dict[str, Any]], bool]:
    v = (venue or "").strip().lower()
    a = _clean_asset(asset)

    cached = False

    with _MARKETS_LOCK:
        hit = _MARKETS_CACHE.get(v)
        if hit and _markets_cache_ok(hit[0]):
            base_items = hit[1]
            cached = True
        else:
            base_items = None

    if base_items is None:
        items = _fetch_venue_markets_uncached(v)
        with _MARKETS_LOCK:
            _MARKETS_CACHE[v] = (_utcnow(), items)
        base_items = items
        cached = False

    out = list(base_items or [])

    if a:
        out = [m for m in out if _canon(m.get("base")) == a or _canon(m.get("quote")) == a]

    if limit and limit > 0:
        out = out[: int(limit)]

    return out, cached


# ─────────────────────────────────────────────────────────────
# Market-aware pricing helpers
# ─────────────────────────────────────────────────────────────

class _MarketIndex:
    __slots__ = ["symbols", "quotes_by_base"]

    def __init__(self, symbols: Set[str], quotes_by_base: Dict[str, Set[str]]):
        self.symbols = symbols
        self.quotes_by_base = quotes_by_base


def _build_market_index(venue: str) -> _MarketIndex:
    """
    Build a fast lookup index of which canonical pairs exist on this venue.
    Uses venue_markets_snapshot(), which is cached for _MARKETS_TTL_SECONDS.
    """
    items, _cached = venue_markets_snapshot(venue=venue, asset=None, limit=0)
    symset: Set[str] = set()
    qbb: Dict[str, Set[str]] = {}
    for m in items or []:
        try:
            sc = _canon(m.get("symbol_canon") or "")
            b = _canon(m.get("base") or "")
            q = _canon(m.get("quote") or "")
            if not sc or "-" not in sc:
                continue
            symset.add(sc)
            if b and q:
                s = qbb.get(b)
                if s is None:
                    s = set()
                    qbb[b] = s
                s.add(q)
        except Exception:
            continue
    return _MarketIndex(symset, qbb)


def _pair_exists(idx: _MarketIndex, base: str, quote: str) -> bool:
    b = _canon(base)
    q = _canon(quote)
    if not b or not q:
        return False
    # Fast check by base->quotes when available
    qs = idx.quotes_by_base.get(b)
    if qs is not None:
        return q in qs
    # Fallback: full symbol set
    return f"{b}-{q}" in idx.symbols


def _try_mid_if_exists(venue: str, idx: _MarketIndex, base: str, quote: str, stale_ok: bool) -> Optional[float]:
    """
    Only attempt orderbook if the pair exists (prevents massive pair-not-found fan-out).
    """
    if not _pair_exists(idx, base, quote):
        return None
    sym = f"{_canon(base)}-{_canon(quote)}"
    try:
        _, book = orderbook_snapshot(venue, sym, depth=1, stale_ok=stale_ok, force=False)
        return _mid_from_book(book)
    except Exception:
        return None


def _price_asset_usd_best_effort(
    pricing_venues: List[str],
    asset: str,
    stale_ok: bool,
    market_index_by_venue: Dict[str, _MarketIndex],
    bridge_usd_by_venue: Dict[str, Dict[str, float]],
) -> Optional[float]:
    """
    Market-aware pricing:
      - Stable assets priced locally
      - Direct stable quotes ONLY if pair exists on venue
      - Bridge quotes BTC/ETH ONLY if bridge pair exists on venue
    """
    a = (asset or "").strip().upper()
    if not a:
        return None

    if a in _STABLE_USD:
        return float(_STABLE_USD[a])

    # 1) Direct stable quotes (fast path)
    for v in pricing_venues:
        idx = market_index_by_venue.get(v)
        if idx is None:
            continue

        # Prefer stable quotes that this venue actually supports for this base
        qs = idx.quotes_by_base.get(a)
        if not qs:
            candidate_quotes = _STABLE_QUOTE_ORDER
        else:
            candidate_quotes = [q for q in _STABLE_QUOTE_ORDER if q in qs]

        for q in candidate_quotes:
            if a == q:
                continue
            mid = _try_mid_if_exists(v, idx, a, q, stale_ok=stale_ok)
            if mid is None:
                continue
            q_usd = _STABLE_USD.get(q)
            if q_usd is None:
                continue
            return float(mid) * float(q_usd)

    # 2) Bridge via BTC/ETH
    for v in pricing_venues:
        idx = market_index_by_venue.get(v)
        if idx is None:
            continue

        # Compute bridge USD prices once per venue per call
        bridge = bridge_usd_by_venue.get(v)
        if bridge is None:
            bridge = {}
            for bq in _BRIDGE_QUOTES:
                px = _try_mid_if_exists(v, idx, bq, "USD", stale_ok=stale_ok)
                if px is None:
                    px = _try_mid_if_exists(v, idx, bq, "USDT", stale_ok=stale_ok)
                if px is None:
                    px = _try_mid_if_exists(v, idx, bq, "USDC", stale_ok=stale_ok)
                if px is not None:
                    bridge[bq] = float(px)
            bridge_usd_by_venue[v] = bridge

        if not bridge:
            continue

        for bq, bq_usd in bridge.items():
            mid = _try_mid_if_exists(v, idx, a, bq, stale_ok=stale_ok)
            if mid is None:
                continue
            return float(mid) * float(bq_usd)

    return None


def prices_usd_from_assets(venue: str, assets: List[str], stale_ok: bool = True) -> Dict[str, float]:
    """
    Return map: asset -> USD price (best-effort).

    Notes:
      - stale_ok defaults True to avoid wedging balances/scanners when orderbook degrades.
      - Pricing DOES NOT use force mode; it remains TTL+bounded-stale for stability.
    """
    v_in = (venue or "").strip().lower()
    primary = _pricing_venue_for_usd(v_in)
    pricing_venues = _parse_pricing_venues(primary)

    out: Dict[str, float] = {}

    # Normalize assets (dedupe)
    norm_assets: List[str] = []
    seen = set()
    for a in assets or []:
        asset = (a or "").strip().upper()
        if not asset or asset in seen:
            continue
        seen.add(asset)
        norm_assets.append(asset)

    # Pre-fill stable assets
    for a in norm_assets:
        if a in _STABLE_USD:
            out[a] = float(_STABLE_USD[a])

    # Worklist excludes stables
    work = [a for a in norm_assets if a not in _STABLE_USD]
    if not work:
        return out

    # Cap fan-out (default is high; router can still chunk)
    if len(work) > _MAX_PRICE_ASSETS_PER_CALL:
        work = work[:_MAX_PRICE_ASSETS_PER_CALL]

    # Build market indices once per venue (cached markets snapshot)
    market_index_by_venue: Dict[str, _MarketIndex] = {}
    for v in pricing_venues:
        try:
            market_index_by_venue[v] = _build_market_index(v)
        except Exception:
            continue

    bridge_usd_by_venue: Dict[str, Dict[str, float]] = {}

    futs: Dict[Any, str] = {}
    for asset in work:
        fut = _PRICE_EXEC.submit(
            _price_asset_usd_best_effort,
            pricing_venues,
            asset,
            stale_ok,
            market_index_by_venue,
            bridge_usd_by_venue,
        )
        futs[fut] = asset

    for f in as_completed(list(futs.keys())):
        asset = futs.get(f)
        try:
            px = f.result()
            if asset and px is not None:
                out[asset] = float(px)
        except Exception:
            continue

    return out
