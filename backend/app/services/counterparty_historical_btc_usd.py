from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import threading
import time
from typing import Any, Dict, List, Optional, Tuple
import urllib.parse

import httpx


_CACHE_LOCK = threading.RLock()
_MEMORY_CACHE: Dict[str, Dict[str, Any]] = {}
_CACHE_LOADED = False


def _env_first(*keys: str) -> str:
    for key in keys:
        value = os.getenv(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def _clamp_int(value: Any, minimum: int, maximum: int, default: int) -> int:
    try:
        parsed = int(value)
    except Exception:
        parsed = int(default)
    return max(int(minimum), min(int(maximum), parsed))


def _parse_utc(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        dt = value
    elif value not in (None, ""):
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None
    else:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _historical_api_base_url() -> str:
    configured = _env_first(
        "COUNTERPARTY_HISTORICAL_PRICE_API_BASE_URL",
        "UTT_MARKET_METRICS_COINGECKO_BASE_URL",
    )
    if configured:
        return configured.rstrip("/")

    if _env_first("COINGECKO_PRO_API_KEY", "CG_PRO_API_KEY"):
        return "https://pro-api.coingecko.com/api/v3"
    return "https://api.coingecko.com/api/v3"


def _historical_lookup_timeout_s() -> float:
    try:
        value = float(os.getenv("COUNTERPARTY_HISTORICAL_PRICE_LOOKUP_TIMEOUT_S") or "10")
    except Exception:
        value = 10.0
    return max(2.0, min(value, 30.0))


def _historical_window_s() -> int:
    return _clamp_int(
        os.getenv("COUNTERPARTY_HISTORICAL_PRICE_WINDOW_S") or "21600",
        3600,
        86400,
        21600,
    )


def _historical_max_distance_s() -> int:
    return _clamp_int(
        os.getenv("COUNTERPARTY_HISTORICAL_PRICE_MAX_DISTANCE_S") or "7200",
        300,
        21600,
        7200,
    )


def _cache_path() -> Path:
    configured = str(os.getenv("COUNTERPARTY_HISTORICAL_PRICE_CACHE_PATH") or "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path(__file__).resolve().parents[2] / "data" / "counterparty_historical_btc_usd_cache.json"


def _load_cache_once() -> None:
    global _CACHE_LOADED
    with _CACHE_LOCK:
        if _CACHE_LOADED:
            return
        _CACHE_LOADED = True

        path = _cache_path()
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return

        rows = payload.get("rows") if isinstance(payload, dict) else None
        if not isinstance(rows, dict):
            return

        for key, value in rows.items():
            if isinstance(value, dict):
                _MEMORY_CACHE[str(key)] = dict(value)


def _save_cache() -> None:
    path = _cache_path()
    with _CACHE_LOCK:
        payload = {
            "version": 1,
            "rows": _MEMORY_CACHE,
            "updated_at": _iso_utc(datetime.now(timezone.utc)),
        }

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        os.replace(tmp, path)
    except Exception:
        return


def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    _load_cache_once()
    with _CACHE_LOCK:
        value = _MEMORY_CACHE.get(str(key))
        return dict(value) if isinstance(value, dict) else None


def _cache_put(key: str, value: Dict[str, Any]) -> None:
    _load_cache_once()
    clean = {
        k: v
        for k, v in dict(value or {}).items()
        if k not in {"cache", "cache_age_s", "external_error"}
    }
    clean["cached_at"] = _iso_utc(datetime.now(timezone.utc))
    with _CACHE_LOCK:
        _MEMORY_CACHE[str(key)] = clean
    _save_cache()


def _cache_age_s(value: Dict[str, Any]) -> Optional[int]:
    cached_at = _parse_utc(value.get("cached_at"))
    if cached_at is None:
        return None
    return max(0, int(time.time() - cached_at.timestamp()))


def _request_headers() -> Dict[str, str]:
    headers = {
        "accept": "application/json",
        "user-agent": "UTT Counterparty historical price preview/1.0",
    }
    pro_key = _env_first("COINGECKO_PRO_API_KEY", "CG_PRO_API_KEY")
    if pro_key:
        headers["x-cg-pro-api-key"] = pro_key
        return headers

    demo_key = _env_first(
        "COINGECKO_DEMO_API_KEY",
        "CG_DEMO_API_KEY",
        "COINGECKO_API_KEY",
        "CG_API_KEY",
    )
    if demo_key:
        headers["x-cg-demo-api-key"] = demo_key
    return headers


def _coingecko_range_url(*, from_s: int, to_s: int) -> str:
    params = urllib.parse.urlencode(
        {
            "vs_currency": "usd",
            "from": str(int(from_s)),
            "to": str(int(to_s)),
            "precision": "full",
        }
    )
    return f"{_historical_api_base_url()}/coins/bitcoin/market_chart/range?{params}"


def _fetch_coingecko_range(*, from_s: int, to_s: int) -> Dict[str, Any]:
    url = _coingecko_range_url(from_s=from_s, to_s=to_s)
    try:
        with httpx.Client(
            timeout=_historical_lookup_timeout_s(),
            headers=_request_headers(),
            follow_redirects=True,
        ) as client:
            response = client.get(url)

        if response.status_code >= 400:
            return {
                "ok": False,
                "source": "coingecko:bitcoin:market_chart_range",
                "url": url,
                "error": f"HTTP {response.status_code}",
                "body_preview": str(response.text or "")[:300],
            }

        payload = response.json()
        if not isinstance(payload, dict):
            return {
                "ok": False,
                "source": "coingecko:bitcoin:market_chart_range",
                "url": url,
                "error": "unexpected_response",
            }

        return {
            "ok": True,
            "source": "coingecko:bitcoin:market_chart_range",
            "url": url,
            "payload": payload,
        }
    except Exception as exc:
        return {
            "ok": False,
            "source": "coingecko:bitcoin:market_chart_range",
            "url": url,
            "error": f"{type(exc).__name__}: {exc}"[:500],
        }


def _price_points(payload: Any) -> List[Tuple[int, Decimal]]:
    if not isinstance(payload, dict):
        return []

    out: List[Tuple[int, Decimal]] = []
    for item in payload.get("prices") or []:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        try:
            ts_ms = int(item[0])
            price = Decimal(str(item[1]))
        except Exception:
            continue
        if ts_ms <= 0 or not price.is_finite() or price <= 0:
            continue
        out.append((ts_ms, price))
    return out


def _select_nearest_price(
    points: List[Tuple[int, Decimal]],
    *,
    target_ms: int,
) -> Optional[Tuple[int, Decimal, int]]:
    if not points:
        return None
    nearest_ts, nearest_price = min(
        points,
        key=lambda item: (abs(int(item[0]) - int(target_ms)), int(item[0])),
    )
    distance_s = int(round(abs(int(nearest_ts) - int(target_ms)) / 1000.0))
    return int(nearest_ts), nearest_price, distance_s


def lookup_historical_btc_usd(
    requested_at: Any,
    *,
    allow_external_lookup: bool = True,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    """Resolve an auditable historical BTC/USD observation without accounting writes."""
    requested_dt = _parse_utc(requested_at)
    if requested_dt is None:
        return {
            "ok": False,
            "status": "invalid_requested_timestamp",
            "requested_at": None,
            "source": "coingecko:bitcoin:market_chart_range",
            "cache": "none",
            "read_only": True,
        }

    requested_iso = _iso_utc(requested_dt)
    target_s = int(requested_dt.timestamp())
    target_ms = target_s * 1000
    key = str(target_s)
    cached = _cache_get(key)

    if cached is not None and not force_refresh:
        return {
            **cached,
            "cache": "hit",
            "cache_age_s": _cache_age_s(cached),
            "read_only": True,
        }

    if not allow_external_lookup:
        if cached is not None:
            return {
                **cached,
                "cache": "hit",
                "cache_age_s": _cache_age_s(cached),
                "read_only": True,
                "warnings": [
                    *list(cached.get("warnings") or []),
                    "External historical-price lookup was disabled; the immutable cached observation was used.",
                ],
            }
        return {
            "ok": False,
            "status": "external_lookup_disabled",
            "requested_at": requested_iso,
            "source": "coingecko:bitcoin:market_chart_range",
            "cache": "none",
            "read_only": True,
        }

    window_s = _historical_window_s()
    max_distance_s = _historical_max_distance_s()
    fetched = _fetch_coingecko_range(
        from_s=target_s - window_s,
        to_s=target_s + window_s,
    )

    if not fetched.get("ok"):
        if cached is not None:
            return {
                **cached,
                "cache": "last_good_fallback",
                "cache_age_s": _cache_age_s(cached),
                "read_only": True,
                "external_error": fetched,
                "warnings": [
                    *list(cached.get("warnings") or []),
                    "Historical-price refresh failed; the previously cached immutable observation was retained.",
                ],
            }
        return {
            "ok": False,
            "status": "external_lookup_failed",
            "requested_at": requested_iso,
            "source": fetched.get("source") or "coingecko:bitcoin:market_chart_range",
            "source_url": fetched.get("url"),
            "cache": "none",
            "read_only": True,
            "external_error": fetched,
        }

    points = _price_points(fetched.get("payload"))
    selected = _select_nearest_price(points, target_ms=target_ms)
    if selected is None:
        return {
            "ok": False,
            "status": "no_price_observations",
            "requested_at": requested_iso,
            "source": fetched.get("source"),
            "source_url": fetched.get("url"),
            "observation_count": len(points),
            "cache": "none",
            "read_only": True,
        }

    observation_ms, price, distance_s = selected
    observation_dt = datetime.fromtimestamp(observation_ms / 1000.0, tz=timezone.utc)
    base_result = {
        "ok": distance_s <= max_distance_s,
        "status": "resolved" if distance_s <= max_distance_s else "outside_tolerance",
        "requested_at": requested_iso,
        "requested_unix_s": target_s,
        "observation_at": _iso_utc(observation_dt),
        "observation_unix_ms": observation_ms,
        "distance_s": distance_s,
        "max_distance_s": max_distance_s,
        "window_s": window_s,
        "price_usd": float(price),
        "price_usd_exact": format(price, "f"),
        "source": fetched.get("source") or "coingecko:bitcoin:market_chart_range",
        "source_url": fetched.get("url"),
        "observation_count": len(points),
        "cache": "miss",
        "cache_age_s": 0,
        "read_only": True,
        "warnings": [],
    }

    if distance_s > max_distance_s:
        return base_result

    _cache_put(key, base_result)
    return base_result
