# backend/app/routers/scanners.py

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional, List, Dict, Any, Tuple
import os
import asyncio
import time
from bisect import bisect_right

import httpx

from ..db import get_db
from ..services.balances import latest_balances
from ..services.market import prices_usd_from_assets

router = APIRouter(prefix="/api/scanners", tags=["scanners"])


# -----------------------------
# Config / caches
# -----------------------------

COINGECKO_BASE = (os.getenv("COINGECKO_BASE_URL", "") or "").strip() or "https://api.coingecko.com/api/v3"
COINGECKO_TIMEOUT_S = float((os.getenv("COINGECKO_TIMEOUT_S", "") or "6").strip() or 6)

# Cache coinlist mapping for a day by default (huge reduction in API calls vs /search per asset)
COINGECKO_COINLIST_TTL_S = int(
    float((os.getenv("COINGECKO_COINLIST_TTL_S", "") or str(24 * 3600)).strip() or (24 * 3600))
)

# Optional explicit symbol->id overrides (helps with ambiguous tickers)
# Format: "HYPE=hyperliquid,BTC=bitcoin"
COINGECKO_SYMBOL_OVERRIDES = (os.getenv("COINGECKO_SYMBOL_OVERRIDES", "") or "").strip()

# CoinGecko: symbol(lower) -> id
_CG_COINLIST_BY_SYMBOL: Dict[str, str] = {}
_CG_COINLIST_EXPIRES_AT: float = 0.0

# Asset cache: ASSET(upper) -> (coingecko_id_or_empty_string, expires_epoch)
# Empty string "" means "resolved miss" and should be treated as cached-none.
_CG_ID_CACHE: Dict[str, Tuple[str, float]] = {}
COINGECKO_ID_TTL_S = int(float((os.getenv("COINGECKO_ID_TTL_S", "") or str(7 * 24 * 3600)).strip() or (7 * 24 * 3600)))


# -----------------------------
# Helpers
# -----------------------------

def _dex_trade_balance_dust() -> float:
    s = (os.getenv("DEX_TRADE_BALANCE_DUST", "") or "").strip()
    if not s:
        s = (os.getenv("BALANCE_DUST_THRESHOLD", "") or "").strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def _normalize_venues_param(venues: Optional[List[str]]) -> List[str]:
    """
    Supports:
      - ?venues=gemini,coinbase   (single CSV string)
      - ?venues=gemini&venues=coinbase (repeated params)
    """
    if not venues:
        return []
    out: List[str] = []
    for v in venues:
        if v is None:
            continue
        s = str(v).strip().lower()
        if not s:
            continue
        if "," in s:
            out.extend([x.strip().lower() for x in s.split(",") if x.strip()])
        else:
            out.append(s)

    # de-dup preserving order
    seen = set()
    dedup: List[str] = []
    for v in out:
        if v in seen:
            continue
        seen.add(v)
        dedup.append(v)
    return dedup


def _venues_from_params(venue: Optional[str], venues: Optional[List[str]]) -> List[str]:
    """
    Your frontend sends:
      - single venue: venue=<v> and venues=<v> (compat)
      - all venues: repeated venues=<v>
    We accept both.
    """
    vlist = _normalize_venues_param(venues)
    v1 = (venue or "").strip().lower()
    if v1 and v1 not in vlist:
        vlist = [v1] + vlist
    return vlist


def _safe_float(x, dflt=None):
    try:
        if x is None:
            return dflt
        return float(x)
    except Exception:
        return dflt


def _pct_change(now_px: Optional[float], past_px: Optional[float]) -> Optional[float]:
    try:
        if now_px is None or past_px is None:
            return None
        now = float(now_px)
        past = float(past_px)
        if past == 0.0:
            return None
        return (now / past - 1.0) * 100.0
    except Exception:
        return None


def _gemini_symbol_from_asset(asset: str) -> str:
    a = str(asset or "").strip().lower()
    if not a:
        return ""
    return f"{a}usd"


def _coinbase_product_from_asset(asset: str) -> str:
    a = str(asset or "").strip().upper()
    if not a:
        return ""
    return f"{a}-USD"


async def _fetch_gemini_1hr_closes(client: httpx.AsyncClient, asset: str) -> List[float]:
    sym = _gemini_symbol_from_asset(asset)
    if not sym:
        return []
    url = f"https://api.gemini.com/v2/candles/{sym}/1hr"
    r = await client.get(url)
    if r.status_code != 200:
        return []
    j = r.json()
    if not isinstance(j, list):
        return []

    closes: List[float] = []
    for row in j:
        if not isinstance(row, (list, tuple)) or len(row) < 5:
            continue
        c = _safe_float(row[4], dflt=None)
        if c is None:
            continue
        closes.append(float(c))

    return closes  # newest-first typically


async def _fetch_coinbase_1hr_closes(client: httpx.AsyncClient, asset: str) -> List[float]:
    product = _coinbase_product_from_asset(asset)
    if not product:
        return []
    url = f"https://api.exchange.coinbase.com/products/{product}/candles"
    params = {"granularity": 3600}
    r = await client.get(url, params=params)
    if r.status_code != 200:
        return []
    j = r.json()
    if not isinstance(j, list):
        return []

    closes: List[float] = []
    for row in j:
        if not isinstance(row, (list, tuple)) or len(row) < 5:
            continue
        c = _safe_float(row[4], dflt=None)
        if c is None:
            continue
        closes.append(float(c))

    return closes  # newest-first commonly


async def _compute_market_stats_for_venue_assets(
    venue: str,
    assets: List[str],
    timeout_s: float = 6.0,
    max_concurrency: int = 12,
) -> Dict[str, Dict[str, Optional[float]]]:
    """
    Candle-derived stats (venue-specific). Useful when it works, but not universal.
    """
    v = (venue or "").strip().lower()
    out: Dict[str, Dict[str, Optional[float]]] = {}

    if not assets:
        return out

    if v not in ("gemini", "coinbase"):
        return out

    sem = asyncio.Semaphore(max_concurrency)
    async with httpx.AsyncClient(timeout=timeout_s) as client:

        async def one(asset: str):
            closes: List[float] = []
            try:
                async with sem:
                    if v == "gemini":
                        closes = await _fetch_gemini_1hr_closes(client, asset)
                    elif v == "coinbase":
                        closes = await _fetch_coinbase_1hr_closes(client, asset)
            except Exception:
                closes = []

            if not closes:
                out[asset] = {"px_usd": None, "change_1h": None, "change_1d": None, "change_1w": None}
                return

            now_px = closes[0] if len(closes) >= 1 else None
            px_1h = closes[1] if len(closes) >= 2 else None
            px_1d = closes[24] if len(closes) >= 25 else None
            px_1w = closes[168] if len(closes) >= 169 else None

            out[asset] = {
                "px_usd": now_px,
                "change_1h": _pct_change(now_px, px_1h),
                "change_1d": _pct_change(now_px, px_1d),
                "change_1w": _pct_change(now_px, px_1w),
            }

        await asyncio.gather(*(one(a) for a in assets))

    return out


# -----------------------------
# CoinGecko market enrichment (venue-agnostic, robust)
# -----------------------------

def _cg_parse_overrides() -> Dict[str, str]:
    out: Dict[str, str] = {}
    s = COINGECKO_SYMBOL_OVERRIDES
    if not s:
        return out
    for part in s.split(","):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        k = k.strip().lower()
        v = v.strip()
        if k and v:
            out[k] = v
    return out


_CG_OVERRIDES = _cg_parse_overrides()


def _cg_cache_get(asset: str) -> Tuple[bool, Optional[str]]:
    """
    Returns (has_cache_entry, cid_or_none).
    We store "" as a cached miss.
    """
    a = str(asset or "").strip().upper()
    if not a:
        return (False, None)

    hit = _CG_ID_CACHE.get(a)
    if not hit:
        return (False, None)

    cid, exp = hit
    if time.time() > exp:
        _CG_ID_CACHE.pop(a, None)
        return (False, None)

    if cid == "":
        return (True, None)

    return (True, cid)


def _cg_cache_set(asset: str, cid: Optional[str]) -> None:
    a = str(asset or "").strip().upper()
    if not a:
        return
    _CG_ID_CACHE[a] = ((cid or ""), time.time() + float(COINGECKO_ID_TTL_S))


async def _cg_ensure_coinlist(client: httpx.AsyncClient) -> None:
    global _CG_COINLIST_BY_SYMBOL, _CG_COINLIST_EXPIRES_AT

    now = time.time()
    if _CG_COINLIST_BY_SYMBOL and now < _CG_COINLIST_EXPIRES_AT:
        return

    url = f"{COINGECKO_BASE}/coins/list"
    r = await client.get(url)
    if r.status_code != 200:
        # do not wipe existing cache; just shorten retry window
        _CG_COINLIST_EXPIRES_AT = now + 60.0
        return

    j = r.json()
    if not isinstance(j, list):
        _CG_COINLIST_EXPIRES_AT = now + 60.0
        return

    m: Dict[str, str] = {}
    for row in j:
        if not isinstance(row, dict):
            continue
        cid = str(row.get("id", "")).strip()
        sym = str(row.get("symbol", "")).strip().lower()
        if not cid or not sym:
            continue
        if sym not in m:
            m[sym] = cid

    _CG_COINLIST_BY_SYMBOL = m
    _CG_COINLIST_EXPIRES_AT = now + float(COINGECKO_COINLIST_TTL_S)


async def _cg_resolve_id(client: httpx.AsyncClient, asset: str) -> Optional[str]:
    """
    Resolve ticker -> CoinGecko id using cached coinlist (+ optional overrides).
    No per-asset /search spam.
    """
    a = str(asset or "").strip().upper()
    if not a:
        return None

    has, cached = _cg_cache_get(a)
    if has:
        return cached  # can be None if cached miss

    await _cg_ensure_coinlist(client)

    sym = a.lower()

    # Override first
    if sym in _CG_OVERRIDES:
        cid = _CG_OVERRIDES[sym]
        _cg_cache_set(a, cid)
        return cid

    cid = _CG_COINLIST_BY_SYMBOL.get(sym)
    _cg_cache_set(a, cid)  # cache miss as "" too
    return cid


def _nearest_price_at_or_before(prices: List[List[float]], target_ms: int) -> Optional[float]:
    """
    prices: [[ts_ms, price], ...] (ascending ts)
    returns price at the nearest ts <= target_ms, or None
    """
    if not prices:
        return None
    ts_list = [int(p[0]) for p in prices if isinstance(p, (list, tuple)) and len(p) >= 2]
    if not ts_list:
        return None

    i = bisect_right(ts_list, target_ms) - 1
    if i < 0 or i >= len(prices):
        return None
    try:
        return float(prices[i][1])
    except Exception:
        return None


async def _cg_fetch_market_chart_changes(
    client: httpx.AsyncClient,
    cid: str,
) -> Dict[str, Optional[float]]:
    """
    Fallback: compute px + 1h/24h/7d changes from market_chart hourly prices.
    """
    out = {"px_usd": None, "change_1h": None, "change_1d": None, "change_1w": None}
    if not cid:
        return out

    url = f"{COINGECKO_BASE}/coins/{cid}/market_chart"
    params = {"vs_currency": "usd", "days": "7", "interval": "hourly"}

    try:
        r = await client.get(url, params=params)
        if r.status_code != 200:
            return out
        j = r.json()
        prices = j.get("prices")
        if not isinstance(prices, list) or not prices:
            return out

        # Ensure ascending by timestamp
        try:
            prices_sorted = sorted(
                [p for p in prices if isinstance(p, (list, tuple)) and len(p) >= 2],
                key=lambda x: int(x[0]),
            )
        except Exception:
            prices_sorted = prices

        last = prices_sorted[-1]
        now_ms = int(last[0])
        now_px = _safe_float(last[1], None)
        if now_px is None:
            return out

        px_1h = _nearest_price_at_or_before(prices_sorted, now_ms - 3600 * 1000)
        px_1d = _nearest_price_at_or_before(prices_sorted, now_ms - 86400 * 1000)
        px_1w = _nearest_price_at_or_before(prices_sorted, now_ms - 7 * 86400 * 1000)

        out["px_usd"] = float(now_px)
        out["change_1h"] = _pct_change(now_px, px_1h)
        out["change_1d"] = _pct_change(now_px, px_1d)
        out["change_1w"] = _pct_change(now_px, px_1w)
        return out
    except Exception:
        return out


async def _cg_fetch_markets(client: httpx.AsyncClient, ids: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
    """
    Fetch CoinGecko market data for ids.
    Returns dict keyed by id with:
      px_usd, change_1h, change_1d, change_1w, market_cap, volume_24h
    """
    out: Dict[str, Dict[str, Optional[float]]] = {}
    if not ids:
        return out

    # Dedup while preserving order
    seen = set()
    ids = [x for x in ids if x and (x not in seen and not seen.add(x))]

    chunk_size = 150
    for i in range(0, len(ids), chunk_size):
        chunk = ids[i : i + chunk_size]

        url = f"{COINGECKO_BASE}/coins/markets"
        params = {
            "vs_currency": "usd",
            "ids": ",".join(chunk),
            "price_change_percentage": "1h,24h,7d",
            "sparkline": "false",
        }

        for attempt in range(2):
            try:
                r = await client.get(url, params=params)
                if r.status_code == 429 and attempt == 0:
                    await asyncio.sleep(1.0)
                    continue
                if r.status_code != 200:
                    break

                j = r.json()
                if not isinstance(j, list):
                    break

                for row in j:
                    if not isinstance(row, dict):
                        continue
                    cid = str(row.get("id", "")).strip()
                    if not cid:
                        continue
                    out[cid] = {
                        "px_usd": _safe_float(row.get("current_price"), None),
                        "change_1h": _safe_float(row.get("price_change_percentage_1h_in_currency"), None),
                        "change_1d": _safe_float(row.get("price_change_percentage_24h_in_currency"), None),
                        "change_1w": _safe_float(row.get("price_change_percentage_7d_in_currency"), None),
                        "market_cap": _safe_float(row.get("market_cap"), None),
                        "volume_24h": _safe_float(row.get("total_volume"), None),
                    }
                break
            except Exception:
                if attempt == 0:
                    await asyncio.sleep(0.5)
                    continue
                break

    return out


def _norm_market_cap(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
        if v <= 0:
            return None
        return v
    except Exception:
        return None


def _norm_volume(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
        if v < 0:
            return None
        return v
    except Exception:
        return None


async def _cg_market_data_for_assets(assets: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
    """
    Returns dict keyed by ASSET (ticker) with CoinGecko market data.
    Adds a fallback to market_chart for ids missing pct-change fields.
    """
    out: Dict[str, Dict[str, Optional[float]]] = {}
    if not assets:
        return out

    headers = {"User-Agent": "UTT/1.0 (local dev)"}
    timeout = httpx.Timeout(COINGECKO_TIMEOUT_S)

    async with httpx.AsyncClient(timeout=timeout, headers=headers) as client:
        sem = asyncio.Semaphore(20)
        resolved: Dict[str, Optional[str]] = {}

        async def one(a: str):
            async with sem:
                try:
                    resolved[a] = await _cg_resolve_id(client, a)
                except Exception:
                    resolved[a] = None

        await asyncio.gather(*(one(a) for a in assets))

        id_to_asset: Dict[str, str] = {}
        ids: List[str] = []
        for a, cid in resolved.items():
            if cid:
                ids.append(cid)
                id_to_asset[cid] = a

        markets_by_id = await _cg_fetch_markets(client, ids)

        # Fallback fill for ids that are present but missing change_* fields (common cause of nulls).
        # Also fill for ids that didn't come back from /markets at all.
        missing_ids: List[str] = []
        for cid in ids:
            m = markets_by_id.get(cid)
            if not m:
                missing_ids.append(cid)
                continue
            if m.get("change_1h") is None or m.get("change_1d") is None or m.get("change_1w") is None:
                missing_ids.append(cid)

        if missing_ids:
            sem2 = asyncio.Semaphore(6)

            async def fill_one(cid: str):
                async with sem2:
                    ch = await _cg_fetch_market_chart_changes(client, cid)
                    if not ch:
                        return
                    m = markets_by_id.get(cid) or {}
                    # Only fill missing fields; do not overwrite existing non-null values.
                    if m.get("px_usd") is None and ch.get("px_usd") is not None:
                        m["px_usd"] = ch["px_usd"]
                    if m.get("change_1h") is None and ch.get("change_1h") is not None:
                        m["change_1h"] = ch["change_1h"]
                    if m.get("change_1d") is None and ch.get("change_1d") is not None:
                        m["change_1d"] = ch["change_1d"]
                    if m.get("change_1w") is None and ch.get("change_1w") is not None:
                        m["change_1w"] = ch["change_1w"]
                    markets_by_id[cid] = m

            await asyncio.gather(*(fill_one(cid) for cid in missing_ids))

        for cid, m in markets_by_id.items():
            a = id_to_asset.get(cid)
            if not a:
                continue
            out[a] = {
                "px_usd": _safe_float(m.get("px_usd"), None),
                "change_1h": _safe_float(m.get("change_1h"), None),
                "change_1d": _safe_float(m.get("change_1d"), None),
                "change_1w": _safe_float(m.get("change_1w"), None),
                "market_cap": _norm_market_cap(m.get("market_cap")),
                "volume_24h": _norm_volume(m.get("volume_24h")),
            }

    return out


# -----------------------------
# Routes
# -----------------------------

@router.get("/top_gainers")
def top_gainers(
    venue: Optional[str] = Query(
        default=None,
        description="Single venue filter. If provided, it is included alongside any `venues=` values.",
    ),
    venues: Optional[List[str]] = Query(
        default=None,
        description="List of enabled venues to include. Supports repeated params (?venues=a&venues=b) or CSV (?venues=a,b).",
    ),
    limit: int = Query(default=250, ge=1, le=2000),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """
    Held-assets scanner enriched with *external market data* so the frontend can sort
    owned assets by gains (1h/1d/1w) and later by market cap/volume.

    Enrichment precedence:
      1) Venue pricing service: prices_usd_from_assets()
      2) Venue candles (gemini/coinbase only) for px + changes
      3) CoinGecko (venue-agnostic) for px + changes (+ market cap/volume)

    NOTE:
      - change_1d is mapped to 24h percent change.
      - change_1w is mapped to 7d percent change.
    """
    venues_list = _venues_from_params(venue, venues)
    venues_set = set(venues_list)
    dust = _dex_trade_balance_dust()

    # 1) Pull latest balances PER venue
    balances_by_venue: Dict[str, Tuple[List[Any], Any]] = {}

    if venues_list:
        for v in venues_list:
            try:
                items_v, as_of_v = latest_balances(db, venue=v, sort="asset:asc")
            except Exception:
                items_v, as_of_v = [], None
            balances_by_venue[v] = (items_v, as_of_v)
    else:
        # fallback
        try:
            items_all, as_of_all = latest_balances(db, venue=None, sort="asset:asc")
        except Exception:
            items_all, as_of_all = [], None

        derived = sorted(
            {str(getattr(b, "venue", "") or "").strip().lower() for b in items_all if getattr(b, "venue", None)}
        )
        venues_list = derived
        venues_set = set(venues_list)
        for v in venues_list:
            balances_by_venue[v] = (
                [b for b in items_all if str(getattr(b, "venue", "") or "").strip().lower() == v],
                as_of_all,
            )

    # Choose an as_of for the response (max timestamp when comparable)
    as_of_values = [balances_by_venue[v][1] for v in venues_list if balances_by_venue.get(v)]
    as_of = None
    for a in as_of_values:
        if a is None:
            continue
        if as_of is None:
            as_of = a
            continue
        try:
            if a > as_of:
                as_of = a
        except Exception:
            if str(a) > str(as_of):
                as_of = a

    # 2) Aggregate holdings: asset -> venue -> total
    holdings: Dict[str, Dict[str, float]] = {}
    for v in venues_list:
        if venues_set and v not in venues_set:
            continue

        items_v = balances_by_venue.get(v, ([], None))[0] or []
        for b in items_v:
            asset = str(getattr(b, "asset", "") or "").strip().upper()
            if not asset or asset == "USD":
                continue

            total = _safe_float(getattr(b, "total", 0.0), 0.0) or 0.0
            available = _safe_float(getattr(b, "available", 0.0), 0.0) or 0.0
            hold = _safe_float(getattr(b, "hold", 0.0), 0.0) or 0.0

            if v == "dex_trade":
                if abs(total) <= dust and abs(available) <= dust and abs(hold) <= dust:
                    continue

            if abs(total) <= 0.0:
                continue

            holdings.setdefault(asset, {})
            holdings[asset][v] = holdings[asset].get(v, 0.0) + float(total)

    assets = sorted(list(holdings.keys()))
    if not assets:
        return {"as_of": as_of, "venues": venues_list, "items": []}

    # 3) Venue px (existing mechanism)
    px_by_venue: Dict[str, Dict[str, float]] = {}
    for v in venues_list:
        try:
            px_by_venue[v] = prices_usd_from_assets(v, assets)
        except Exception:
            px_by_venue[v] = {}

    # 4) Venue candle stats (gemini/coinbase only; best-effort)
    stats_by_venue: Dict[str, Dict[str, Dict[str, Optional[float]]]] = {}
    for v in venues_list:
        if v not in ("gemini", "coinbase"):
            stats_by_venue[v] = {}
            continue
        try:
            stats_by_venue[v] = asyncio.run(_compute_market_stats_for_venue_assets(v, assets))
        except Exception:
            stats_by_venue[v] = {}

    # 5) CoinGecko market enrichment (universal, with market_chart fallback)
    try:
        cg_by_asset = asyncio.run(_cg_market_data_for_assets(assets))
    except Exception:
        cg_by_asset = {}

    out_items: List[Dict[str, Any]] = []
    single_venue_tag = venues_list[0] if len(venues_list) == 1 else None

    for asset in assets:
        byv = holdings.get(asset, {})
        total_qty = sum(float(x) for x in byv.values())

        by_venue_rows = []
        total_usd = 0.0
        total_usd_known = False

        headline_px = None
        headline_c1h = None
        headline_c1d = None
        headline_c1w = None

        for v in venues_list:
            qty = float(byv.get(v, 0.0))
            if qty == 0.0:
                continue

            # Price per venue: prefer service, then candle px, then CoinGecko px
            px = px_by_venue.get(v, {}).get(asset)
            if px is None:
                px = (stats_by_venue.get(v, {}).get(asset, {}) or {}).get("px_usd")
            if px is None:
                px = (cg_by_asset.get(asset, {}) or {}).get("px_usd")

            # Changes: prefer candle changes for that venue, else CoinGecko
            st = stats_by_venue.get(v, {}).get(asset, {}) or {}
            c1h = st.get("change_1h")
            c1d = st.get("change_1d")
            c1w = st.get("change_1w")

            if c1h is None or c1d is None or c1w is None:
                cg = cg_by_asset.get(asset, {}) or {}
                c1h = c1h if c1h is not None else cg.get("change_1h")
                c1d = c1d if c1d is not None else cg.get("change_1d")
                c1w = c1w if c1w is not None else cg.get("change_1w")

            if headline_px is None and px is not None:
                headline_px = float(px)
            if headline_c1h is None and c1h is not None:
                headline_c1h = float(c1h)
            if headline_c1d is None and c1d is not None:
                headline_c1d = float(c1d)
            if headline_c1w is None and c1w is not None:
                headline_c1w = float(c1w)

            row_total_usd = None
            if px is not None:
                row_total_usd = float(qty) * float(px)
                total_usd += row_total_usd
                total_usd_known = True

            by_venue_rows.append(
                {
                    "venue": v,
                    "total": qty,
                    "px_usd": float(px) if px is not None else None,
                    "total_usd": float(row_total_usd) if row_total_usd is not None else None,
                    "change_1h": float(c1h) if c1h is not None else None,
                    "change_1d": float(c1d) if c1d is not None else None,
                    "change_1w": float(c1w) if c1w is not None else None,
                }
            )

        # If headline changes are still missing, fill directly from CoinGecko (not venue-dependent)
        cg = cg_by_asset.get(asset, {}) or {}
        if headline_px is None and cg.get("px_usd") is not None:
            headline_px = float(cg["px_usd"])
        if headline_c1h is None and cg.get("change_1h") is not None:
            headline_c1h = float(cg["change_1h"])
        if headline_c1d is None and cg.get("change_1d") is not None:
            headline_c1d = float(cg["change_1d"])
        if headline_c1w is None and cg.get("change_1w") is not None:
            headline_c1w = float(cg["change_1w"])

        item: Dict[str, Any] = {
            "asset": asset,
            "symbol": f"{asset}-USD",
            "total": float(total_qty),
            "px_usd": headline_px,
            "total_usd": float(total_usd) if total_usd_known else None,
            "by_venue": by_venue_rows,
            "change_1h": headline_c1h,
            "change_1d": headline_c1d,
            "change_1w": headline_c1w,
            "market_cap": _norm_market_cap(cg.get("market_cap")),
            "volume_24h": _norm_volume(cg.get("volume_24h")),
        }

        if single_venue_tag:
            item["venue"] = single_venue_tag

        out_items.append(item)

    # Sort: prefer change_1d desc when present, else total_usd desc.
    def sort_key(it: Dict[str, Any]) -> Tuple[int, float]:
        c = it.get("change_1d")
        if c is not None:
            return (0, float(c))
        v = it.get("total_usd")
        if v is None:
            return (2, 0.0)
        return (1, float(v))

    out_items.sort(key=sort_key, reverse=True)

    return {"as_of": as_of, "venues": venues_list, "items": out_items[: int(limit)]}
