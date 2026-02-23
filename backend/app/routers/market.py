from fastapi import APIRouter, Query, HTTPException
from datetime import datetime
from typing import Dict, List, Optional
import re

import httpx

from ..schemas import OrderBookResponse
from ..services.market import (
    orderbook_snapshot,
    prices_usd_from_assets,
    venue_markets_snapshot,
)

from ..venues.registry import get_venue_spec, venue_registry

router = APIRouter(prefix="/api/market", tags=["market"])


def _normalize_venue_param(v: str) -> str:
    s = (v or "").strip().lower()
    # If UI accidentally passes an object, JS often stringifies it as "[object Object]"
    if s.startswith("[object") and "object" in s:
        return ""
    if s in {"null", "none", "undefined", ""}:
        return ""
    return s


def _looks_like_pair_not_found(msg: str) -> bool:
    low = (msg or "").lower()
    return (
        "pair not found" in low
        or "symbol not found" in low
        or "unknown symbol" in low
        or "invalid symbol" in low
        or "invalidsymbol" in low
        or "unknown request" in low
        or "product not found" in low
        or "unknown product" in low
        or "market not found" in low
        or "unknown market" in low
        or "invalid market" in low
        or "not a valid symbol" in low
        or "does not exist" in low
        or ("not found" in low and "symbol" in low)
        # Gemini-style patterns often show up as /v1/book/<symbol> 400
        or (("/v1/book" in low or "/v2/book" in low or "/book/" in low) and ("400" in low or "bad request" in low))
    )


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
        or "throttling" in low
        or "420" in low
    )


def _extract_http_status_from_text(s: str) -> Optional[int]:
    """
    Many adapters wrap errors into strings like:
      - "HTTPStatusError: 429 Too Many Requests"
      - "upstream returned 404 ..."
      - "status_code=400"
    We try to recover an HTTP-ish code.
    """
    text = (s or "").strip()
    if not text:
        return None

    # Prefer explicit patterns first
    m = re.search(r"\bstatus[_\s-]*code\s*=\s*(\d{3})\b", text, re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None

    m = re.search(r"\bHTTP\s*(\d{3})\b", text, re.IGNORECASE)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None

    # Fallback: any 3-digit token
    m = re.search(r"\b(\d{3})\b", text)
    if not m:
        return None
    try:
        code = int(m.group(1))
        if 100 <= code <= 599:
            return code
    except Exception:
        return None
    return None


def _map_upstream_exception(venue: str, operation: str, e: Exception) -> HTTPException:
    """
    Translate upstream/adapters errors into appropriate HTTP status codes so the UI can react sensibly:
      - Pair not found => 404
      - Rate limited  => 429
      - Timeout       => 504
      - Network       => 502
      - Upstream 5xx  => 502
      - Other 4xx     => passthrough (when detectable)
      - Otherwise     => 502
    """

    # Preserve explicit HTTPExceptions
    if isinstance(e, HTTPException):
        return e

    # Timeouts
    if isinstance(e, (httpx.TimeoutException, TimeoutError)):
        return HTTPException(status_code=504, detail=f"{operation}: {venue} timeout")

    # Network layer errors (DNS, connect, reset)
    if isinstance(e, httpx.RequestError):
        return HTTPException(status_code=502, detail=f"{operation}: network error contacting {venue}: {e}")

    # HTTPStatusError (only if upstream raised_for_status)
    if isinstance(e, httpx.HTTPStatusError):
        status = 502
        body = ""
        try:
            status = int(e.response.status_code)
        except Exception:
            status = 502
        try:
            body = (e.response.text or "").strip()
        except Exception:
            body = ""

        # Use both body + exception message for classification
        msg = f"{body}\n{str(e)}".strip()

        if status in (400, 404) and _looks_like_pair_not_found(msg):
            return HTTPException(status_code=404, detail=f"Pair Not Found at {venue}")

        if status in (420, 429) or _looks_like_rate_limited(msg):
            return HTTPException(status_code=429, detail=f"Rate limited by {venue}")

        if 400 <= status < 500:
            # propagate other upstream 4xx
            return HTTPException(status_code=status, detail=body or f"{operation}: upstream {venue} error")

        # upstream 5xx -> 502
        return HTTPException(status_code=502, detail=f"{operation}: upstream {venue} error ({status})")

    # Generic exception: try to infer status + class
    msg = str(e or "").strip()
    code = _extract_http_status_from_text(msg)

    if code in (420, 429) or _looks_like_rate_limited(msg):
        return HTTPException(status_code=429, detail=f"{operation}: rate limited by {venue}")

    if code in (400, 404) and _looks_like_pair_not_found(msg):
        return HTTPException(status_code=404, detail=f"Pair Not Found at {venue}")

    if code is not None:
        if 400 <= code < 500:
            return HTTPException(status_code=code, detail=f"{operation}: upstream {venue} error ({code})")
        if 500 <= code <= 599:
            return HTTPException(status_code=502, detail=f"{operation}: upstream {venue} error ({code})")

    # Default: treat as upstream failure (not internal server bug)
    return HTTPException(status_code=502, detail=f"{operation}: failed contacting {venue}: {msg or type(e).__name__}")


@router.get("/orderbook", response_model=OrderBookResponse)
def get_orderbook(
    venue: str = Query(...),
    symbol: str = Query(..., description="Canonical symbol, e.g., USDT-USD"),
    depth: int = Query(default=25, ge=1, le=200),
    force: bool = Query(
        default=False,
        description="If true, bypass server cache/TTL and attempt a live fetch (used by manual Refresh).",
    ),
    stale_ok: bool = Query(
        default=True,
        description="If true, serve a recent cached snapshot when upstream is slow/rate-limited instead of 504/429.",
    ),
):
    venue_norm = _normalize_venue_param(venue)
    if not venue_norm:
        raise HTTPException(
            status_code=422,
            detail=(
                "Invalid venue value. Expected a string like 'coinbase', 'kraken', etc. "
                "The UI is likely passing an object (it becomes '[object Object]')."
            ),
        )

    # Capability guardrail: never call adapter orderbook methods for venues that do not support them.
    try:
        spec = get_venue_spec(venue_norm)
    except KeyError:
        reg = venue_registry()
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported venue '{venue_norm}'. Supported venues: {', '.join(sorted(reg.keys()))}",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to resolve venue '{venue_norm}': {e}")

    if not getattr(spec, "supports_orderbook", True):
        raise HTTPException(
            status_code=400,
            detail=f"Venue '{venue_norm}' does not support orderbook",
        )

    try:
        symbol_canon, book = orderbook_snapshot(
            venue_norm,
            symbol,
            depth,
            stale_ok=stale_ok,
            force=force,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise _map_upstream_exception(venue_norm, "orderbook", e)

    return {
        "venue": venue_norm,
        "symbol_canon": symbol_canon,
        "bids": book["bids"],
        "asks": book["asks"],
        "ts": datetime.utcnow(),
    }


@router.get("/prices_usd")
def get_prices_usd(
    venue: str = Query(..., description="gemini | kraken | coinbase"),
    assets: str = Query(
        "",
        description="Comma-separated asset codes, e.g. BTC,ETH,USD,USDT,DOGE",
    ),
    stale_ok: bool = Query(
        default=True,
        description="If true, allow cached/stale pricing snapshots when upstream is slow/rate-limited.",
    ),
) -> Dict[str, object]:
    venue_norm = _normalize_venue_param(venue)
    if not venue_norm:
        raise HTTPException(
            status_code=422,
            detail="Invalid venue value (likely '[object Object]' from UI).",
        )

    aset: List[str] = [x.strip().upper() for x in (assets or "").split(",") if x.strip()]
    try:
        prices = prices_usd_from_assets(venue=venue_norm, assets=aset, stale_ok=stale_ok)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise _map_upstream_exception(venue_norm, "prices_usd", e)

    return {"venue": venue_norm, "prices": prices, "ts": datetime.utcnow()}


@router.get("/venue_markets")
def get_venue_markets(
    venue: str = Query(..., description="gemini | kraken | coinbase"),
    asset: Optional[str] = Query(None, description="Optional asset filter: BTC, ETH, USD, USDT, etc."),
    limit: int = Query(default=0, ge=0, le=20000, description="Optional max items; 0 = no limit"),
) -> Dict[str, object]:
    venue_norm = _normalize_venue_param(venue)
    if not venue_norm:
        raise HTTPException(
            status_code=422,
            detail="Invalid venue value (likely '[object Object]' from UI).",
        )

    try:
        items, cached = venue_markets_snapshot(venue=venue_norm, asset=asset, limit=limit)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise _map_upstream_exception(venue_norm, "venue_markets", e)

    return {
        "venue": venue_norm,
        "asset": (asset or "").strip().upper() if asset else None,
        "items": items,
        "cached": bool(cached),
        "ts": datetime.utcnow(),
    }
