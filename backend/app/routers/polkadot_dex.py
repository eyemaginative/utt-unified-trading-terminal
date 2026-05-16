# backend/app/routers/polkadot_dex.py

from __future__ import annotations

import atexit
import asyncio
import hashlib
import json
import os
import subprocess
import time
import uuid
from urllib.parse import urlparse
from datetime import datetime
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, getcontext
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import inspect as sa_inspect, text
from sqlalchemy.orm import Session

from ..config import settings
from ..db import get_db
from ..models import TokenRegistry

router = APIRouter(prefix="/api/polkadot_dex", tags=["polkadot_dex"])

getcontext().prec = 50


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _hydration_route_mode(raw: Optional[str] = None) -> str:
    mode = str(raw or _HYDRATION_DEFAULT_ROUTE_MODE or "auto").strip().lower()
    aliases = {
        "managed": "sdk",
        "managed_sdk": "sdk",
        "sidecar": "sdk",
        "sdk_router": "sdk",
        "isolated": "isolated_helper",
        "helper": "isolated_helper",
        "manual": "manual_xyk",
        "xyk": "manual_xyk",
    }
    mode = aliases.get(mode, mode)
    if mode not in _HYDRATION_ROUTE_MODES:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "invalid_hydration_route_mode",
                "route_mode": raw,
                "allowed": sorted(_HYDRATION_ROUTE_MODES),
            },
        )
    return mode


def _hydration_route_mode_label(mode: str, *, manual: bool = False) -> str:
    m = _hydration_route_mode(mode)
    if manual:
        return "manual_xyk_pool_fallback"
    if m == "isolated_helper":
        return "galactic_sdk_next_isolated_helper"
    return "galactic_sdk_next_helper"


# Dwellir Hydration RPC configuration.
# Preferred: set a full URL in UTT_HYDRATION_RPC_URL, for example:
#   https://api-hydradx.dwellir.com/<key>
# Fallback: set UTT_DWELLIR_HYDRATION_API_KEY or DWELLIR_API_KEY and we build the URL.
_HYDRATION_RPC_URL_ENV = (
    os.getenv("UTT_HYDRATION_RPC_URL")
    or os.getenv("HYDRATION_RPC_URL")
    or ""
).strip()
_HYDRATION_RPC_PROVIDER = (
    os.getenv("UTT_HYDRATION_RPC_PROVIDER")
    or "dwellir"
).strip().lower()
_HYDRATION_RPC_URL_TEMPLATE = (
    os.getenv("UTT_HYDRATION_RPC_URL_TEMPLATE")
    or ""
).strip()
_HYDRATION_WS_URL_TEMPLATE = (
    os.getenv("UTT_HYDRATION_WS_URL_TEMPLATE")
    or ""
).strip()
_DWELLIR_HYDRATION_API_KEY = (
    os.getenv("UTT_DWELLIR_HYDRATION_API_KEY")
    or os.getenv("DWELLIR_HYDRATION_API_KEY")
    or os.getenv("DWELLIR_API_KEY")
    or ""
).strip()
_DWELLIR_HYDRATION_HTTP_BASE = (
    os.getenv("UTT_DWELLIR_HYDRATION_HTTP_BASE")
    or os.getenv("DWELLIR_HYDRATION_HTTP_BASE")
    or "https://api-hydradx.dwellir.com"
).strip().rstrip("/")
_DWELLIR_HYDRATION_WS_BASE = (
    os.getenv("UTT_DWELLIR_HYDRATION_WS_BASE")
    or os.getenv("DWELLIR_HYDRATION_WS_BASE")
    or "wss://api-hydradx.dwellir.com"
).strip().rstrip("/")
try:
    _HYDRATION_TIMEOUT_S = float(os.getenv("UTT_HYDRATION_TIMEOUT_S") or "20")
except Exception:
    _HYDRATION_TIMEOUT_S = 20.0

_HYDRATION_HELPER_PATH_ENV = (os.getenv("UTT_HYDRATION_HELPER_PATH") or "").strip()
_HYDRATION_NODE_BIN = (os.getenv("UTT_NODE_BIN") or "node").strip() or "node"
try:
    _HYDRATION_HELPER_TIMEOUT_S = float(os.getenv("UTT_HYDRATION_HELPER_TIMEOUT_S") or "45")
except Exception:
    _HYDRATION_HELPER_TIMEOUT_S = 45.0
try:
    _HYDRATION_HELPER_STEP_TIMEOUT_S = float(os.getenv("UTT_HYDRATION_HELPER_STEP_TIMEOUT_S") or "30")
except Exception:
    _HYDRATION_HELPER_STEP_TIMEOUT_S = 30.0
try:
    _HYDRATION_ORDERBOOK_STEP_TIMEOUT_S = float(
        os.getenv("UTT_HYDRATION_ORDERBOOK_STEP_TIMEOUT_S")
        or str(min(float(_HYDRATION_HELPER_STEP_TIMEOUT_S), 8.0))
    )
except Exception:
    _HYDRATION_ORDERBOOK_STEP_TIMEOUT_S = min(float(_HYDRATION_HELPER_STEP_TIMEOUT_S), 8.0)
try:
    _HYDRATION_ORDERBOOK_MAX_CONSECUTIVE_ERRORS = int(os.getenv("UTT_HYDRATION_ORDERBOOK_MAX_CONSECUTIVE_ERRORS") or "2")
except Exception:
    _HYDRATION_ORDERBOOK_MAX_CONSECUTIVE_ERRORS = 2
# Orderbook quote sampling can trigger SDK router calls that outlive Python's
# request timeout inside the persistent sidecar.  Default orderbook samples to
# short-lived helper processes so a bad pair cannot poison live SELL/swap routing.
_HYDRATION_ORDERBOOK_FORCE_ISOLATED_HELPER = _env_bool("UTT_HYDRATION_ORDERBOOK_FORCE_ISOLATED_HELPER", False)
_HYDRATION_ROUTE_MODES = {"auto", "sdk", "isolated_helper", "manual_xyk"}
_HYDRATION_DEFAULT_ROUTE_MODE = (os.getenv("UTT_HYDRATION_ROUTE_MODE") or "auto").strip().lower()
if _HYDRATION_DEFAULT_ROUTE_MODE not in _HYDRATION_ROUTE_MODES:
    _HYDRATION_DEFAULT_ROUTE_MODE = "auto"
try:
    _HYDRATION_NATIVE_ASSET_ID = int(os.getenv("UTT_HYDRATION_NATIVE_ASSET_ID") or "0")
except Exception:
    _HYDRATION_NATIVE_ASSET_ID = 0

_HYDRATION_ENABLE_HEAVY_INSPECT = _env_bool("UTT_HYDRATION_ENABLE_HEAVY_INSPECT", False)
_HYDRATION_ENABLE_ROUTER_QUOTES = _env_bool("UTT_HYDRATION_ENABLE_ROUTER_QUOTES", False)
_HYDRATION_ENABLE_STATE_CALL_QUOTES = _env_bool("UTT_HYDRATION_ENABLE_STATE_CALL_QUOTES", False)
_HYDRATION_ENABLE_SWAP_TX = _env_bool("UTT_HYDRATION_ENABLE_SWAP_TX", False)
_HYDRATION_ENABLE_EXACT_BUY = _env_bool("UTT_HYDRATION_ENABLE_EXACT_BUY", False)
_HYDRATION_ENABLE_BUY_DIAGNOSTICS = _env_bool("UTT_HYDRATION_ENABLE_BUY_DIAGNOSTICS", False)
_HYDRATION_BUY_PROBE_PATH_ENV = (os.getenv("UTT_HYDRATION_BUY_PROBE_PATH") or "").strip()
_HYDRATION_STATE_CALL_QUOTE_METHOD = (
    os.getenv("UTT_HYDRATION_STATE_CALL_QUOTE_METHOD")
    or "OmnipoolApi_quotePrice"
).strip()
_HYDRATION_USE_SIDECAR = _env_bool("UTT_HYDRATION_USE_SIDECAR", False)
_HYDRATION_SIDECAR_URL = (
    os.getenv("UTT_HYDRATION_SIDECAR_URL")
    or "http://127.0.0.1:8787"
).strip().rstrip("/")
_HYDRATION_AUTOSTART_SIDECAR = _env_bool("UTT_HYDRATION_AUTOSTART_SIDECAR", True)
_HYDRATION_SIDECAR_SCRIPT_PATH_ENV = (os.getenv("UTT_HYDRATION_SIDECAR_SCRIPT_PATH") or "").strip()
try:
    _HYDRATION_SIDECAR_START_TIMEOUT_S = float(os.getenv("UTT_HYDRATION_SIDECAR_START_TIMEOUT_S") or "12")
except Exception:
    _HYDRATION_SIDECAR_START_TIMEOUT_S = 12.0
_hydration_sidecar_process: Optional[subprocess.Popen[Any]] = None
_HYDRATION_ROUTER_QUOTES_UNAVAILABLE_REASON = (
    os.getenv("UTT_HYDRATION_ROUTER_QUOTES_UNAVAILABLE_REASON")
    or "Hydration SDK router quote/orderbook calls are disabled because controlled tests exhausted chainHead_follow subscriptions through the current RPC/PAPI path. Keep live quotes/swaps disabled until a lighter quote source, runtime state_call path, or supported RPC/indexer path is selected."
).strip()
_HYDRATION_INSPECT_MODE = (
    os.getenv("UTT_HYDRATION_INSPECT_MODE")
    or ("full" if _HYDRATION_ENABLE_HEAVY_INSPECT else "light")
).strip().lower()
if _HYDRATION_INSPECT_MODE not in {"light", "spot", "routes", "full"}:
    _HYDRATION_INSPECT_MODE = "light"

# Token Registry is the preferred source of Hydration asset metadata.
# Env JSON remains as a local fallback only.
# Recommended Token Registry rows for Hydration DEX routing:
#   chain=hydration, venue=hydration, symbol=HDX, address=native, decimals=12
#   chain=hydration, venue=hydration, symbol=DOT, address=5, decimals=10
#   chain=hydration, venue=hydration, symbol=USDT, address=10, decimals=6
# HDX is Hydration native asset id 0. The helper maps address=native to SDK id 0.
# Fallback env example:
#   UTT_HYDRATION_ASSET_IDS_JSON={"HDX":"native","DOT":"5","USDT":"10","UTTT":"50000456"}
#   UTT_HYDRATION_DECIMALS_JSON={"HDX":12,"DOT":10,"USDT":6,"UTTT":6}
_HYDRATION_ASSET_IDS_JSON = os.getenv("UTT_HYDRATION_ASSET_IDS_JSON") or "{}"
_HYDRATION_DECIMALS_JSON = os.getenv("UTT_HYDRATION_DECIMALS_JSON") or "{}"
# Optional per-symbol quote sampling override for the pseudo-orderbook.
# Example: UTT_HYDRATION_SAMPLE_SIZES_JSON={"DOT":[1,2,5],"HDX":[100,250,500]}
_HYDRATION_SAMPLE_SIZES_JSON = os.getenv("UTT_HYDRATION_SAMPLE_SIZES_JSON") or "{}"
# Optional manual pool quote fallback for custom Hydration assets that exist on-chain
# but are not yet admitted by sdk-next routing metadata.  Shape example:
#   UTT_HYDRATION_MANUAL_POOL_PRICES_JSON={"UTTT-HDX":{"baseReserve":1000000,"quoteReserve":832.45,"feeBps":30}}
_HYDRATION_ENABLE_MANUAL_POOL_FALLBACK = _env_bool("UTT_HYDRATION_ENABLE_MANUAL_POOL_FALLBACK", True)
_HYDRATION_MANUAL_POOL_PRICES_JSON = os.getenv("UTT_HYDRATION_MANUAL_POOL_PRICES_JSON") or "{}"
_HYDRATION_MANUAL_POOL_LIVE_RESERVES = _env_bool("UTT_HYDRATION_MANUAL_POOL_LIVE_RESERVES", True)
# Comma-separated fallback list for non-native Hydration balance lookup.
# The endpoint also accepts ?assets=DOT,USDT,UTTT for one-off tests.
_HYDRATION_BALANCE_ASSETS_CSV = (os.getenv("UTT_HYDRATION_BALANCE_ASSETS") or "DOT,USDT,UTTT").strip()
_HYDRATION_NATIVE_SYMBOL = (os.getenv("UTT_HYDRATION_NATIVE_SYMBOL") or "HDX").strip().upper()
try:
    _HYDRATION_NATIVE_DECIMALS = int(os.getenv("UTT_HYDRATION_NATIVE_DECIMALS") or "12")
except Exception:
    _HYDRATION_NATIVE_DECIMALS = 12

# Controlled SDK price cache.  This is intentionally separate from
# UTT_HYDRATION_ENABLE_ROUTER_QUOTES: normal orderbook sampling stays disabled
# unless explicitly enabled, while this cache can perform a tiny bounded set of
# SDK getBestSell calls behind TTL/backoff/singleflight guards.
_HYDRATION_ENABLE_SDK_PRICE_CACHE = _env_bool("UTT_HYDRATION_ENABLE_SDK_PRICE_CACHE", False)
try:
    _HYDRATION_PRICE_CACHE_TTL_S = float(os.getenv("UTT_HYDRATION_PRICE_CACHE_TTL_S") or "300")
except Exception:
    _HYDRATION_PRICE_CACHE_TTL_S = 300.0
try:
    _HYDRATION_PRICE_CACHE_ERROR_BACKOFF_S = float(os.getenv("UTT_HYDRATION_PRICE_CACHE_ERROR_BACKOFF_S") or "600")
except Exception:
    _HYDRATION_PRICE_CACHE_ERROR_BACKOFF_S = 600.0
try:
    _HYDRATION_PRICE_CACHE_STEP_TIMEOUT_S = float(os.getenv("UTT_HYDRATION_PRICE_CACHE_STEP_TIMEOUT_S") or "12")
except Exception:
    _HYDRATION_PRICE_CACHE_STEP_TIMEOUT_S = 12.0
try:
    _HYDRATION_PRICE_CACHE_MAX_QUOTE_ERRORS = int(os.getenv("UTT_HYDRATION_PRICE_CACHE_MAX_QUOTE_ERRORS") or "1")
except Exception:
    _HYDRATION_PRICE_CACHE_MAX_QUOTE_ERRORS = 1
_HYDRATION_PRICE_CACHE_USE_SIDECAR = _env_bool("UTT_HYDRATION_PRICE_CACHE_USE_SIDECAR", True)
_HYDRATION_PRICE_CACHE_AUTOSTART_SIDECAR = _env_bool("UTT_HYDRATION_PRICE_CACHE_AUTOSTART_SIDECAR", False)
_HYDRATION_PRICE_CACHE_FORCE_ISOLATED_HELPER = _env_bool("UTT_HYDRATION_PRICE_CACHE_FORCE_ISOLATED_HELPER", False)
_HYDRATION_PRICE_CACHE_STRATEGY = (
    os.getenv("UTT_HYDRATION_PRICE_CACHE_STRATEGY")
    or "spot_then_sell"
).strip().lower()
if _HYDRATION_PRICE_CACHE_STRATEGY not in {"spot_then_sell", "sell_then_spot", "spot_only", "sell_only"}:
    _HYDRATION_PRICE_CACHE_STRATEGY = "spot_then_sell"
_HYDRATION_PRICE_CACHE_SPOT_IMPLEMENTATION = (
    os.getenv("UTT_HYDRATION_PRICE_CACHE_SPOT_IMPLEMENTATION")
    or "direct"
).strip().lower()
if _HYDRATION_PRICE_CACHE_SPOT_IMPLEMENTATION not in {"direct", "context"}:
    _HYDRATION_PRICE_CACHE_SPOT_IMPLEMENTATION = "direct"
# Keep SDK price probing available for explicit diagnostics, but do not use it
# as the default UI price source.  Repeated sdk-next/PAPI price calls have
# already proven too quota-expensive through the current RPC path.
_HYDRATION_PRICE_CACHE_USE_SDK_FALLBACK = _env_bool("UTT_HYDRATION_PRICE_CACHE_USE_SDK_FALLBACK", False)
_HYDRATION_ENABLE_EXTERNAL_USD_PRICES = _env_bool("UTT_HYDRATION_ENABLE_EXTERNAL_USD_PRICES", True)
_HYDRATION_EXTERNAL_USD_PRICE_SOURCE = (
    os.getenv("UTT_HYDRATION_EXTERNAL_USD_PRICE_SOURCE")
    or "coingecko"
).strip().lower()
_HYDRATION_EXTERNAL_PRICE_IDS_JSON = (
    os.getenv("UTT_HYDRATION_EXTERNAL_PRICE_IDS_JSON")
    or '{"HDX":"hydration","DOT":"polkadot"}'
).strip()
_HYDRATION_COINGECKO_SIMPLE_PRICE_URL = (
    os.getenv("UTT_HYDRATION_COINGECKO_SIMPLE_PRICE_URL")
    or "https://api.coingecko.com/api/v3/simple/price"
).strip()
try:
    _HYDRATION_EXTERNAL_USD_PRICE_TIMEOUT_S = float(os.getenv("UTT_HYDRATION_EXTERNAL_USD_PRICE_TIMEOUT_S") or "5")
except Exception:
    _HYDRATION_EXTERNAL_USD_PRICE_TIMEOUT_S = 5.0

_hydration_usd_price_cache: Dict[str, Any] = {
    "prices": {},
    "sources": {},
    "errors": [],
    "updated_at": 0.0,
    "expires_at": 0.0,
    "error_until": 0.0,
    "last_error": None,
}
_hydration_usd_price_cache_lock = asyncio.Lock()

_REQUIRED_METHODS = [
    "system_chain",
    "system_name",
    "system_version",
    "system_properties",
    "state_getStorage",
    "state_getMetadata",
    "state_call",
    "payment_queryInfo",
    "author_submitExtrinsic",
]

_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_IDX = {c: i for i, c in enumerate(_B58_ALPHABET)}


def _redact_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    parts = u.rstrip("/").split("/")
    if len(parts) >= 4:
        parts[-1] = "***"
        return "/".join(parts)
    return u


def _looks_placeholder_secret(value: Any) -> bool:
    s = str(value or "").strip()
    if not s:
        return True
    lo = s.lower()
    return (
        "<" in s
        or ">" in s
        or "your_key" in lo
        or "your-key" in lo
        or "api_key_here" in lo
        or "replace_me" in lo
        or "changeme" in lo
    )


def _template_with_api_key(template: str, api_key: str) -> Optional[str]:
    t = str(template or "").strip()
    k = str(api_key or "").strip()
    if not t or not k:
        return None
    if "{api_key}" in t:
        return t.replace("{api_key}", k)
    if "{key}" in t:
        return t.replace("{key}", k)
    if "<api_key>" in t:
        return t.replace("<api_key>", k)
    if "<key>" in t:
        return t.replace("<key>", k)
    return f"{t.rstrip('/')}/{k}"


def _dwellir_hydration_profile_api_key() -> str:
    try:
        key = settings.polkadot_hydration_rpc_api_key()
        if key and not _looks_placeholder_secret(key):
            return str(key).strip()
    except Exception:
        pass
    return ""


def _dwellir_hydration_api_key() -> str:
    # Profile/API Keys DB is preferred.  Env fallback remains for backward
    # compatibility until local installs migrate their Dwellir key into Profile.
    key = _dwellir_hydration_profile_api_key()
    if key:
        return key

    if _DWELLIR_HYDRATION_API_KEY and not _looks_placeholder_secret(_DWELLIR_HYDRATION_API_KEY):
        return _DWELLIR_HYDRATION_API_KEY

    return ""


def _dwellir_hydration_key_source() -> Optional[str]:
    if _dwellir_hydration_profile_api_key():
        return "profile_db:polkadot_hydration"
    if _DWELLIR_HYDRATION_API_KEY and not _looks_placeholder_secret(_DWELLIR_HYDRATION_API_KEY):
        return "env"
    return None


def _hydration_rpc_url() -> str:
    if _HYDRATION_RPC_URL_ENV and not _looks_placeholder_secret(_HYDRATION_RPC_URL_ENV):
        return _HYDRATION_RPC_URL_ENV

    key = _dwellir_hydration_api_key()
    if key:
        if key.startswith("http://") or key.startswith("https://"):
            return key
        templated = _template_with_api_key(_HYDRATION_RPC_URL_TEMPLATE, key)
        if templated:
            return templated
        return f"{_DWELLIR_HYDRATION_HTTP_BASE}/{key}"

    raise HTTPException(
        status_code=503,
        detail={
            "error": "hydration_rpc_not_configured",
            "message": "Save the Dwellir key in Profile → API Keys with venue=polkadot_hydration, or set a temporary env fallback.",
            "profileVenue": "polkadot_hydration",
            "envFallbacks": [
                "UTT_HYDRATION_RPC_URL",
                "UTT_DWELLIR_HYDRATION_API_KEY",
                "DWELLIR_HYDRATION_API_KEY",
                "DWELLIR_API_KEY",
            ],
            "templateExample": "UTT_HYDRATION_RPC_URL_TEMPLATE=https://api-hydration.n.dwellir.com/{api_key}",
        },
    )


def _hydration_ws_url() -> Optional[str]:
    explicit = (os.getenv("UTT_HYDRATION_WS_URL") or os.getenv("HYDRATION_WS_URL") or "").strip()
    if explicit and not _looks_placeholder_secret(explicit):
        return explicit

    key = _dwellir_hydration_api_key()
    if key:
        if key.startswith("ws://") or key.startswith("wss://"):
            return key
        templated = _template_with_api_key(_HYDRATION_WS_URL_TEMPLATE, key)
        if templated:
            return templated
        return f"{_DWELLIR_HYDRATION_WS_BASE}/{key}"

    return None


async def _rpc(method: str, params: Optional[List[Any]] = None) -> Any:
    url = _hydration_rpc_url()
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}
    try:
        async with httpx.AsyncClient(timeout=_HYDRATION_TIMEOUT_S) as client:
            r = await client.post(url, json=payload, headers={"content-type": "application/json"})
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "hydration_rpc_request_failed",
                "method": method,
                "exc": type(e).__name__,
                "message": str(e),
                "rpc_url": _redact_url(url),
            },
        )

    if r.status_code == 429:
        raise HTTPException(status_code=429, detail={"error": "hydration_rpc_rate_limited", "rpc_url": _redact_url(url)})
    if r.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={"error": "hydration_rpc_http_error", "status": r.status_code, "body": (r.text or "")[:500]},
        )

    try:
        data = r.json() or {}
    except Exception:
        raise HTTPException(status_code=502, detail={"error": "hydration_rpc_non_json", "body": (r.text or "")[:500]})

    if isinstance(data, dict) and data.get("error") is not None:
        raise HTTPException(status_code=502, detail={"error": "hydration_rpc_error", "rpc_error": data.get("error")})
    return data.get("result") if isinstance(data, dict) else data


def _hydration_helper_path() -> Path:
    if _HYDRATION_HELPER_PATH_ENV:
        return Path(_HYDRATION_HELPER_PATH_ENV)
    # backend/app/routers/polkadot_dex.py -> backend/app/services/hydration_quote.mjs
    return Path(__file__).resolve().parents[1] / "services" / "hydration_quote.mjs"


def _hydration_buy_probe_path() -> Path:
    if _HYDRATION_BUY_PROBE_PATH_ENV:
        return Path(_HYDRATION_BUY_PROBE_PATH_ENV)
    # backend/app/routers/polkadot_dex.py -> backend/app/services/hydration_getbestbuy_probe_p1_8h1.mjs
    return Path(__file__).resolve().parents[1] / "services" / "hydration_getbestbuy_probe_p1_8h1.mjs"


def _hydration_sidecar_script_path() -> Path:
    if _HYDRATION_SIDECAR_SCRIPT_PATH_ENV:
        return Path(_HYDRATION_SIDECAR_SCRIPT_PATH_ENV)
    # backend/app/routers/polkadot_dex.py -> backend/app/services/hydration_sidecar.mjs
    return Path(__file__).resolve().parents[1] / "services" / "hydration_sidecar.mjs"


def _ui_to_atomic(amount_ui: float, decimals: int) -> int:
    try:
        v = float(amount_ui)
    except Exception:
        raise HTTPException(status_code=422, detail={"error": "invalid_amount", "amount": amount_ui})
    atomic = int(round(v * (10 ** int(decimals))))
    if atomic <= 0:
        raise HTTPException(status_code=422, detail={"error": "amount_too_small_after_decimal_conversion", "amount": amount_ui, "decimals": int(decimals)})
    return atomic


def _atomic_to_ui(amount_atomic: Any, decimals: int) -> float:
    try:
        return float(int(str(amount_atomic))) / (10 ** int(decimals))
    except Exception:
        return 0.0


def _helper_asset_payload(meta: Dict[str, Any]) -> Dict[str, Any]:
    asset_id = str((meta or {}).get("assetId") or "").strip()
    return {
        "symbol": str((meta or {}).get("symbol") or "").strip().upper(),
        "assetId": asset_id,
        "native": bool((meta or {}).get("native")),
        "decimals": int((meta or {}).get("decimals") or 0),
        "sdkAssetIdFallback": _HYDRATION_NATIVE_ASSET_ID if asset_id.lower() == "native" else None,
    }


def _hydration_sdk_asset_id(meta: Dict[str, Any]) -> int:
    asset_id = str((meta or {}).get("assetId") or "").strip()
    if asset_id.lower() == "native":
        return int(_HYDRATION_NATIVE_ASSET_ID)
    try:
        return int(asset_id)
    except Exception:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "invalid_hydration_asset_id",
                "symbol": (meta or {}).get("symbol"),
                "assetId": asset_id,
                "message": "Hydration state_call probing requires a numeric asset ID, except native HDX which maps to UTT_HYDRATION_NATIVE_ASSET_ID.",
            },
        )


def _scale_u32(v: int) -> bytes:
    n = int(v)
    if n < 0 or n > 0xFFFFFFFF:
        raise HTTPException(status_code=422, detail={"error": "u32_out_of_range", "value": n})
    return n.to_bytes(4, "little", signed=False)


def _scale_u128(v: int) -> bytes:
    n = int(v)
    if n < 0 or n >= 2 ** 128:
        raise HTTPException(status_code=422, detail={"error": "u128_out_of_range", "value": str(n)})
    return n.to_bytes(16, "little", signed=False)


def _hex_bytes(data: bytes) -> str:
    return "0x" + bytes(data).hex()


def _clean_hex(s: str) -> str:
    v = str(s or "").strip()
    if not v:
        raise HTTPException(status_code=422, detail={"error": "empty_hex"})
    if not v.startswith("0x"):
        v = "0x" + v
    try:
        bytes.fromhex(v[2:])
    except Exception:
        raise HTTPException(status_code=422, detail={"error": "invalid_hex", "value": str(s)[:120]})
    return v


def _state_call_quote_candidate_payloads(asset_in_id: int, asset_out_id: int, amount_atomic: int) -> List[Dict[str, Any]]:
    # Diagnostic candidates only. The exact OmnipoolApi_quotePrice SCALE signature still needs confirmation.
    # These are intentionally returned with names so we can identify which shape, if any, the runtime accepts.
    base = _scale_u32(asset_in_id) + _scale_u32(asset_out_id) + _scale_u128(amount_atomic)
    return [
        {"name": "u32_u32_u128", "data": _hex_bytes(base), "note": "assetIn:u32, assetOut:u32, amount:u128"},
        {"name": "u32_u32_u128_order_0", "data": _hex_bytes(base + b"\x00"), "note": "plus one SCALE enum/bool byte = 0"},
        {"name": "u32_u32_u128_order_1", "data": _hex_bytes(base + b"\x01"), "note": "plus one SCALE enum/bool byte = 1"},
    ]


def _decode_state_call_probe_result(raw: Any, output_decimals: int) -> Dict[str, Any]:
    h = str(raw or "").strip()
    if not h:
        return {"decoded": False, "reason": "empty_result"}
    if not h.startswith("0x"):
        return {"decoded": False, "reason": "non_hex_result", "rawType": type(raw).__name__}
    try:
        buf = bytes.fromhex(h[2:])
    except Exception:
        return {"decoded": False, "reason": "invalid_hex_result", "rawLen": len(h)}

    guesses: List[Dict[str, Any]] = []
    if len(buf) >= 16:
        v = int.from_bytes(buf[0:16], "little", signed=False)
        guesses.append({"shape": "u128_le_at_0", "amountAtomic": str(v), "uiAmount": _atomic_to_ui(v, output_decimals)})
    if len(buf) >= 17:
        tag = int(buf[0])
        v = int.from_bytes(buf[1:17], "little", signed=False)
        guesses.append({"shape": "tag_plus_u128_le", "tag": tag, "amountAtomic": str(v), "uiAmount": _atomic_to_ui(v, output_decimals)})
    return {
        "decoded": bool(guesses),
        "byteLen": len(buf),
        "guesses": guesses,
        "note": "Probe decoding is heuristic until the exact OmnipoolApi_quotePrice output SCALE type is confirmed.",
    }


async def _rpc_probe(method: str, params: Optional[List[Any]] = None) -> Dict[str, Any]:
    url = _hydration_rpc_url()
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}
    try:
        async with httpx.AsyncClient(timeout=_HYDRATION_TIMEOUT_S) as client:
            r = await client.post(url, json=payload, headers={"content-type": "application/json"})
    except httpx.RequestError as e:
        return {
            "ok": False,
            "error": "hydration_rpc_probe_request_failed",
            "method": method,
            "exc": type(e).__name__,
            "message": str(e),
            "rpc_url": _redact_url(url),
        }

    out: Dict[str, Any] = {
        "ok": r.status_code < 400,
        "httpStatus": r.status_code,
        "rpc_url": _redact_url(url),
    }
    try:
        data = r.json() or {}
    except Exception:
        out.update({"ok": False, "error": "hydration_rpc_probe_non_json", "body": (r.text or "")[:1000]})
        return out

    if isinstance(data, dict) and data.get("error") is not None:
        out.update({"ok": False, "error": "hydration_rpc_probe_error", "rpc_error": data.get("error")})
        return out

    out["result"] = data.get("result") if isinstance(data, dict) else data
    return out



def _runtime_api_default_method_candidates() -> List[str]:
    return [
        # Known safe sanity checks first.
        "Core_version",
        "Metadata_metadata_versions",
        "Metadata_metadata_at_version",
        # Dwellir/documented and likely naming variants.
        "OmnipoolApi_quotePrice",
        "OmnipoolApi_quote_price",
        "OmnipoolApi_quote",
        "OmnipoolApi_quote_sell",
        "OmnipoolApi_quote_buy",
        "OmnipoolRuntimeApi_quotePrice",
        "OmnipoolRuntimeApi_quote_price",
        "RouterApi_quotePrice",
        "RouterApi_quote_price",
        "TradeRouterApi_quotePrice",
        "TradeRouterApi_quote_price",
        "TradeExecutionApi_quotePrice",
        "TradeExecutionApi_quote_price",
        "PriceApi_quotePrice",
        "PriceApi_quote_price",
        "AssetRegistryApi_registeredAssets",
        "AssetRegistryApi_registered_assets",
    ]


def _runtime_api_list_from_version(runtime_version: Any) -> List[Dict[str, Any]]:
    if not isinstance(runtime_version, dict):
        return []
    out: List[Dict[str, Any]] = []
    for item in runtime_version.get("apis") or []:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            out.append({"apiId": item[0], "version": item[1]})
        elif isinstance(item, dict):
            out.append(item)
        else:
            out.append({"raw": item})
    return out


def _rpc_error_message(result: Dict[str, Any]) -> str:
    try:
        err = result.get("rpc_error") if isinstance(result, dict) else None
        if isinstance(err, dict):
            return str(err.get("message") or "")
        return str(err or result.get("error") or "")
    except Exception:
        return ""


def _classify_state_call_probe(method: str, result: Dict[str, Any]) -> str:
    if result.get("ok") and result.get("result") is not None:
        return "accepted"
    msg = _rpc_error_message(result).lower()
    if "is not found" in msg or ("exported method" in msg and "not found" in msg):
        return "not_found"
    if "decode" in msg or "codec" in msg or "invalid" in msg or "input" in msg:
        return "exported_decode_or_input_error"
    if "execution failed" in msg:
        return "exported_execution_error"
    return "unknown_error"


async def _state_call_probe_method(method: str, data: str = "0x") -> Dict[str, Any]:
    clean = _clean_hex(data or "0x")
    rpc_result = await _rpc_probe("state_call", [method, clean])
    return {
        "method": method,
        "data": clean,
        "classification": _classify_state_call_probe(method, rpc_result),
        "rpc": rpc_result,
    }


def _sidecar_url_host_port() -> Tuple[str, int]:
    parsed = urlparse(_HYDRATION_SIDECAR_URL or "http://127.0.0.1:8787")
    host = parsed.hostname or "127.0.0.1"
    port = int(parsed.port or 8787)
    return host, port


def _sidecar_process_running() -> bool:
    proc = globals().get("_hydration_sidecar_process")
    try:
        return bool(proc is not None and proc.poll() is None)
    except Exception:
        return False


def _stop_hydration_sidecar_process() -> None:
    global _hydration_sidecar_process
    proc = _hydration_sidecar_process
    _hydration_sidecar_process = None
    if proc is None:
        return
    try:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                proc.kill()
    except Exception:
        pass


atexit.register(_stop_hydration_sidecar_process)


async def _ensure_hydration_sidecar_running(*, price_cache: bool = False) -> Dict[str, Any]:
    if not _HYDRATION_USE_SIDECAR or not _HYDRATION_SIDECAR_URL:
        return {"enabled": bool(_HYDRATION_USE_SIDECAR), "ok": False, "skipped": True}

    effective_autostart = _hydration_effective_autostart_sidecar(price_cache=price_cache)
    current = await _sidecar_health()
    if current.get("ok"):
        return {
            **current,
            "autostart": effective_autostart,
            "autostartEnv": bool(_HYDRATION_AUTOSTART_SIDECAR),
            "priceCacheAutostartEnv": bool(_HYDRATION_PRICE_CACHE_AUTOSTART_SIDECAR),
            "priceCache": bool(price_cache),
            "managed": _sidecar_process_running(),
        }

    if not effective_autostart:
        reason = "price_cache_autostart_disabled" if price_cache else ("router_quotes_disabled" if not _HYDRATION_ENABLE_ROUTER_QUOTES else "autostart_disabled")
        return {
            **current,
            "autostart": False,
            "autostartEnv": bool(_HYDRATION_AUTOSTART_SIDECAR),
            "priceCacheAutostartEnv": bool(_HYDRATION_PRICE_CACHE_AUTOSTART_SIDECAR),
            "priceCache": bool(price_cache),
            "autostartSuppressedReason": reason,
            "managed": _sidecar_process_running(),
        }

    global _hydration_sidecar_process
    if not _sidecar_process_running():
        script = _hydration_sidecar_script_path()
        if not script.exists():
            raise HTTPException(
                status_code=501,
                detail={
                    "error": "hydration_sidecar_script_missing",
                    "message": "Hydration sidecar auto-start is enabled, but backend/app/services/hydration_sidecar.mjs was not found.",
                    "scriptPath": str(script),
                    "sidecar_url": _HYDRATION_SIDECAR_URL,
                },
            )

        host, port = _sidecar_url_host_port()
        env = os.environ.copy()
        env.setdefault("UTT_HYDRATION_SIDECAR_HOST", host)
        env.setdefault("UTT_HYDRATION_SIDECAR_PORT", str(port))
        backend_cwd = Path(__file__).resolve().parents[2]
        creationflags = 0
        if os.name == "nt":
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        try:
            _hydration_sidecar_process = subprocess.Popen(
                [_HYDRATION_NODE_BIN, str(script)],
                cwd=str(backend_cwd),
                env=env,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=creationflags,
            )
        except Exception as e:
            raise HTTPException(
                status_code=502,
                detail={
                    "error": "hydration_sidecar_autostart_failed",
                    "exc": type(e).__name__,
                    "message": str(e),
                    "node_bin": _HYDRATION_NODE_BIN,
                    "scriptPath": str(script),
                    "cwd": str(backend_cwd),
                    "sidecar_url": _HYDRATION_SIDECAR_URL,
                },
            )

    deadline = time.monotonic() + max(1.0, float(_HYDRATION_SIDECAR_START_TIMEOUT_S))
    last_health: Dict[str, Any] = {}
    while time.monotonic() < deadline:
        await asyncio.sleep(0.25)
        last_health = await _sidecar_health()
        if last_health.get("ok"):
            return {
                **last_health,
                "autostart": True,
                "managed": _sidecar_process_running(),
                "pid": getattr(_hydration_sidecar_process, "pid", None),
            }
        if _hydration_sidecar_process is not None and _hydration_sidecar_process.poll() is not None:
            break

    raise HTTPException(
        status_code=504,
        detail={
            "error": "hydration_sidecar_autostart_timeout",
            "message": "Hydration sidecar did not become healthy before the startup timeout.",
            "timeout_s": _HYDRATION_SIDECAR_START_TIMEOUT_S,
            "sidecar_url": _HYDRATION_SIDECAR_URL,
            "scriptPath": str(_hydration_sidecar_script_path()),
            "managed": _sidecar_process_running(),
            "pid": getattr(_hydration_sidecar_process, "pid", None),
            "lastHealth": last_health,
        },
    )


async def _sidecar_health() -> Dict[str, Any]:
    if not _HYDRATION_USE_SIDECAR or not _HYDRATION_SIDECAR_URL:
        return {"enabled": bool(_HYDRATION_USE_SIDECAR), "ok": False, "skipped": True}
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            r = await client.get(f"{_HYDRATION_SIDECAR_URL}/health")
        data = r.json() if r.content else {}
        if isinstance(data, dict):
            return {"enabled": True, "ok": r.status_code < 400, "status": r.status_code, **data}
        return {"enabled": True, "ok": False, "status": r.status_code, "body": (r.text or "")[:500]}
    except Exception as e:
        return {"enabled": True, "ok": False, "error": type(e).__name__, "message": str(e), "sidecar_url": _HYDRATION_SIDECAR_URL}


def _hydration_router_quote_status(
    *,
    symbol: Optional[str] = None,
    base_meta: Optional[Dict[str, Any]] = None,
    quote_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    quotes_enabled = bool(_HYDRATION_ENABLE_ROUTER_QUOTES)
    quotes_available = bool(quotes_enabled and _HYDRATION_USE_SIDECAR and (_hydration_ws_url() or "").strip())
    reason = (
        "Hydration router quotes are enabled for controlled testing through the local sidecar. "
        "Keep live swap submission disabled until quote shape, slippage handling, and SubWallet transaction building are verified."
        if quotes_available
        else _HYDRATION_ROUTER_QUOTES_UNAVAILABLE_REASON
    )
    return {
        "enabled": quotes_enabled,
        "available": quotes_available,
        "status": "available_experimental" if quotes_available else ("enabled_but_unavailable" if quotes_enabled else "disabled"),
        "reason": reason,
        "symbol": symbol,
        "base": base_meta,
        "quote": quote_meta,
        "safeEndpoints": [
            "/api/polkadot_dex/_debug",
            "/api/polkadot_dex/resolve",
            "/api/polkadot_dex/balances",
            "/api/polkadot_dex/hydration/status",
            "/api/polkadot_dex/hydration/inspect with inspect_mode=light",
            "/api/polkadot_dex/hydration/orderbook for configured manual XYK routes",
            "/api/polkadot_dex/hydration/orderbook for SDK pairs only when UTT_HYDRATION_ENABLE_ROUTER_QUOTES=1",
        ],
        "blockedMethods": [] if quotes_available else [
            "sdk.api.router.getSpotPrice",
            "sdk.api.router.getBestSell",
            "sdk.api.router.getRoutes",
            "sdk.api.router.getPools",
            "sdk.api.router.getTradeableAssets",
        ],
        "nextRequired": (
            "SDK router quotes are enabled for controlled testing. Watch Dwellir quota and disable immediately if chainHead calls spike."
            if quotes_available
            else "Keep router quotes disabled until a lighter quote source, cached price layer, or safe SDK reset strategy is implemented."
        ),
        "quotesExperimental": quotes_available,
        "liveSwapsRecommended": bool(quotes_available and _HYDRATION_ENABLE_SWAP_TX),
        "swapTxEnabled": bool(_HYDRATION_ENABLE_SWAP_TX),
        "exactBuyEnabled": bool(_HYDRATION_ENABLE_EXACT_BUY),
        "buyDiagnosticsEnabled": bool(_HYDRATION_ENABLE_BUY_DIAGNOSTICS),
        "liveExactBuyRecommended": bool(quotes_available and _HYDRATION_ENABLE_SWAP_TX and _HYDRATION_ENABLE_EXACT_BUY),
    }



def _hydration_payload_manual_custom_swap_enabled(payload: Dict[str, Any]) -> bool:
    try:
        m = (payload or {}).get("manualCustomSwap")
        return bool(isinstance(m, dict) and m.get("enabled"))
    except Exception:
        return False


def _hydration_payload_price_cache_enabled(payload: Dict[str, Any]) -> bool:
    try:
        return bool(_HYDRATION_ENABLE_SDK_PRICE_CACHE and (payload or {}).get("priceCache"))
    except Exception:
        return False


def _hydration_effective_autostart_sidecar(*, price_cache: bool = False) -> bool:
    """Return whether the managed JS sidecar may be auto-started.

    Normal SDK orderbook/router quote autostart remains gated by
    UTT_HYDRATION_ENABLE_ROUTER_QUOTES.  The controlled USD price cache has its
    own opt-in autostart flag so we can test SDK pricing without reopening the
    broad frontend orderbook quote path.
    """
    if price_cache:
        return bool(
            _HYDRATION_ENABLE_SDK_PRICE_CACHE
            and _HYDRATION_PRICE_CACHE_USE_SIDECAR
            and _HYDRATION_PRICE_CACHE_AUTOSTART_SIDECAR
        )
    return bool(_HYDRATION_AUTOSTART_SIDECAR and _HYDRATION_ENABLE_ROUTER_QUOTES)


def _hydration_router_quotes_disabled_detail(
    *,
    mode: str,
    payload: Optional[Dict[str, Any]] = None,
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "error": "hydration_router_quotes_disabled",
        "message": "Hydration SDK router quote calls are disabled to protect RPC quota. Manual XYK/live-reserve routes remain available for configured pairs such as UTTT-HDX.",
        "venue": "polkadot_hydration",
        "mode": mode,
        "symbol": symbol or (payload or {}).get("resolvedSymbol") or (payload or {}).get("rawSymbol"),
        "enableRouterQuotes": bool(_HYDRATION_ENABLE_ROUTER_QUOTES),
        "manualCustomSwap": _hydration_payload_manual_custom_swap_enabled(payload or {}),
        "quoteStatus": _hydration_router_quote_status(symbol=symbol or (payload or {}).get("resolvedSymbol")),
        "safePaths": [
            "route_mode=manual_xyk for configured manual routes",
            "route_mode=auto only when a manual route registry row exists",
            "/api/polkadot_dex/balances",
            "/api/polkadot_dex/hydration/route_registry",
        ],
        "blockedMethods": [
            "sdk.api.router.getSpotPrice",
            "sdk.api.router.getBestSell",
            "sdk.api.router.getBestBuy",
            "sdk.api.router.getRoutes",
            "sdk.api.router.getPools",
            "sdk.api.router.getTradeableAssets",
        ],
        "nextRequired": "Leave UTT_HYDRATION_ENABLE_ROUTER_QUOTES=0 until a lighter quote source, cached price layer, or safe SDK reset strategy is implemented.",
    }


def _hydration_requires_router_quotes(payload: Dict[str, Any]) -> bool:
    if _hydration_payload_price_cache_enabled(payload):
        return False
    mode = str((payload or {}).get("mode") or "").strip()
    if mode == "quote_sell":
        return not _hydration_payload_manual_custom_swap_enabled(payload)
    if mode == "swap_tx":
        return not _hydration_payload_manual_custom_swap_enabled(payload)
    return False


async def _run_hydration_buy_probe(payload: Dict[str, Any], *, timeout_s: float) -> Dict[str, Any]:
    """Run getBestBuy diagnostics in a short-lived Node process.

    This intentionally avoids the persistent sidecar.  Previous exact-buy tests
    were able to hang the sidecar and starve live SELL quotes/orderbooks.
    """
    script = _hydration_buy_probe_path()
    if not script.exists():
        raise HTTPException(
            status_code=501,
            detail={
                "error": "hydration_getbestbuy_probe_missing",
                "message": "Install backend/app/services/hydration_getbestbuy_probe_p1_8h1.mjs before running BUY diagnostics.",
                "scriptPath": str(script),
            },
        )

    # Bound the wrapper around this diagnostic, not around the global helper
    # timeout.  The probe script has its own per-stage timeout and now exits
    # immediately after emitting JSON, but this keeps failures fast if startup or
    # cleanup gets stuck.
    try:
        attempt_count = max(1, len((payload or {}).get("attempts") or []))
    except Exception:
        attempt_count = 1
    try:
        step_s = float((payload or {}).get("stepTimeoutS") or timeout_s or 12.0)
    except Exception:
        step_s = float(timeout_s or 12.0)
    run_timeout_s = max(float(timeout_s) + 12.0, (step_s * attempt_count) + 15.0, 20.0)

    def _call_node() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [_HYDRATION_NODE_BIN, str(script)],
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            timeout=run_timeout_s,
        )

    try:
        proc = await asyncio.to_thread(_call_node)
    except subprocess.TimeoutExpired as e:
        partial_stdout = e.stdout or ""
        partial_stderr = e.stderr or ""
        if not isinstance(partial_stdout, str):
            partial_stdout = partial_stdout.decode("utf-8", errors="replace") if hasattr(partial_stdout, "decode") else str(partial_stdout)
        if not isinstance(partial_stderr, str):
            partial_stderr = partial_stderr.decode("utf-8", errors="replace") if hasattr(partial_stderr, "decode") else str(partial_stderr)
        raise HTTPException(
            status_code=504,
            detail={
                "error": "hydration_getbestbuy_probe_timeout",
                "timeout_s": run_timeout_s,
                "scriptPath": str(script),
                "partial_stdout": partial_stdout[-3000:],
                "partial_stderr": partial_stderr[-3000:],
            },
        )
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail={"error": "hydration_getbestbuy_probe_spawn_failed", "exc": type(e).__name__, "message": str(e), "scriptPath": str(script)},
        )

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    try:
        data = json.loads(stdout) if stdout else {}
    except Exception:
        data = {}

    if proc.returncode != 0 or not isinstance(data, dict):
        detail = data if isinstance(data, dict) and data else {"error": "hydration_getbestbuy_probe_failed", "stderr": stderr[:1500], "stdout": stdout[:1500]}
        if isinstance(detail, dict):
            detail.setdefault("returncode", proc.returncode)
            detail.setdefault("scriptPath", str(script))
        raise HTTPException(status_code=502, detail=detail)
    return data


async def _run_hydration_sidecar(payload: Dict[str, Any]) -> Dict[str, Any]:
    mode = str((payload or {}).get("mode") or "").strip()
    if mode not in {"inspect", "quote_sell", "price_spot", "price_spot_direct", "swap_tx"}:
        raise HTTPException(status_code=422, detail={"error": "hydration_sidecar_unsupported_mode", "mode": mode})
    if _hydration_requires_router_quotes(payload) and not _HYDRATION_ENABLE_ROUTER_QUOTES:
        raise HTTPException(
            status_code=503,
            detail=_hydration_router_quotes_disabled_detail(mode=mode, payload=payload),
        )
    if not _HYDRATION_SIDECAR_URL:
        raise HTTPException(status_code=503, detail={"error": "hydration_sidecar_url_not_configured"})
    price_cache = _hydration_payload_price_cache_enabled(payload or {})
    sidecar_state = await _ensure_hydration_sidecar_running(price_cache=price_cache)
    if not sidecar_state.get("ok"):
        raise HTTPException(
            status_code=503,
            detail={
                "error": "hydration_sidecar_not_available",
                "message": "The Hydration sidecar is not running and autostart is disabled for this request.",
                "mode": mode,
                "priceCache": bool(price_cache),
                "sidecar": sidecar_state,
                "nextRequired": "For controlled SDK price-cache testing, set UTT_HYDRATION_PRICE_CACHE_AUTOSTART_SIDECAR=1 or start hydration_sidecar.mjs manually.",
            },
        )
    url = f"{_HYDRATION_SIDECAR_URL}/{mode}"
    timeout_s = float(_HYDRATION_HELPER_TIMEOUT_S)
    if mode == "swap_tx":
        # swap_tx has several independent SDK stages: quote, build, get, encode.
        # Make the HTTP wrapper longer than the per-stage timeout so the sidecar
        # can return the exact hanging stage instead of FastAPI timing out first.
        timeout_s = max(timeout_s, (float(_HYDRATION_HELPER_STEP_TIMEOUT_S) * 4.0) + 15.0)
    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            r = await client.post(url, json=payload, headers={"content-type": "application/json"})
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail={"error": "hydration_sidecar_timeout", "timeout_s": timeout_s, "sidecar_url": _HYDRATION_SIDECAR_URL, "mode": mode})
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail={"error": "hydration_sidecar_request_failed", "exc": type(e).__name__, "message": str(e), "sidecar_url": _HYDRATION_SIDECAR_URL, "mode": mode})
    try:
        data = r.json() or {}
    except Exception:
        raise HTTPException(status_code=502, detail={"error": "hydration_sidecar_non_json", "status": r.status_code, "body": (r.text or "")[:1000], "sidecar_url": _HYDRATION_SIDECAR_URL, "mode": mode})
    if r.status_code >= 400 or not isinstance(data, dict) or not data.get("ok"):
        detail = data if isinstance(data, dict) and data else {"error": "hydration_sidecar_error", "status": r.status_code, "body": (r.text or "")[:1000]}
        if isinstance(detail, dict):
            detail.setdefault("sidecar_url", _HYDRATION_SIDECAR_URL)
            detail.setdefault("mode", mode)
        raise HTTPException(status_code=int(detail.get("status") or r.status_code or 502), detail=detail)
    return data


async def _run_hydration_helper(payload: Dict[str, Any], *, force_isolated: bool = False) -> Dict[str, Any]:
    payload = dict(payload or {})
    # Never leak the full Dwellir key in returned errors. The helper/sidecar receives it, but Python redacts before surfacing.
    payload["wsUrl"] = _hydration_ws_url()
    if not payload.get("wsUrl"):
        raise HTTPException(
            status_code=503,
            detail={
                "error": "hydration_ws_not_configured",
                "message": "Set UTT_HYDRATION_WS_URL, or set UTT_DWELLIR_HYDRATION_API_KEY/DWELLIR_API_KEY so the helper can use WebSocket RPC.",
            },
        )

    if _hydration_requires_router_quotes(payload) and not _HYDRATION_ENABLE_ROUTER_QUOTES:
        raise HTTPException(
            status_code=503,
            detail=_hydration_router_quotes_disabled_detail(
                mode=str(payload.get("mode") or ""),
                payload=payload,
            ),
        )

    price_cache = _hydration_payload_price_cache_enabled(payload)
    use_sidecar_for_request = bool(_HYDRATION_USE_SIDECAR and not force_isolated)
    if price_cache:
        use_sidecar_for_request = bool(
            _HYDRATION_USE_SIDECAR
            and _HYDRATION_PRICE_CACHE_USE_SIDECAR
            and not _HYDRATION_PRICE_CACHE_FORCE_ISOLATED_HELPER
        )
    if use_sidecar_for_request:
        return await _run_hydration_sidecar(payload)

    helper = _hydration_helper_path()
    if not helper.exists():
        raise HTTPException(
            status_code=501,
            detail={
                "error": "hydration_helper_missing",
                "message": "Hydration JS helper is not installed yet. Add backend/app/services/hydration_quote.mjs and install SDK dependencies.",
                "helperPath": str(helper),
            },
        )

    helper_timeout_s = float(_HYDRATION_HELPER_TIMEOUT_S)
    if str(payload.get("mode") or "").strip() == "swap_tx":
        helper_timeout_s = max(helper_timeout_s, (float(_HYDRATION_HELPER_STEP_TIMEOUT_S) * 4.0) + 15.0)

    def _call_node() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [_HYDRATION_NODE_BIN, str(helper)],
            input=json.dumps(payload),
            text=True,
            capture_output=True,
            timeout=helper_timeout_s,
        )

    try:
        proc = await asyncio.to_thread(_call_node)
    except subprocess.TimeoutExpired as e:
        partial_stdout = e.stdout or ""
        partial_stderr = e.stderr or ""
        if not isinstance(partial_stdout, str):
            partial_stdout = partial_stdout.decode("utf-8", errors="replace") if hasattr(partial_stdout, "decode") else str(partial_stdout)
        if not isinstance(partial_stderr, str):
            partial_stderr = partial_stderr.decode("utf-8", errors="replace") if hasattr(partial_stderr, "decode") else str(partial_stderr)
        raise HTTPException(
            status_code=504,
            detail={
                "error": "hydration_helper_timeout",
                "timeout_s": helper_timeout_s,
                "step_timeout_s": _HYDRATION_HELPER_STEP_TIMEOUT_S,
                "helperPath": str(helper),
                "partial_stdout": partial_stdout[-3000:],
                "partial_stderr": partial_stderr[-3000:],
            },
        )
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail={"error": "hydration_helper_spawn_failed", "exc": type(e).__name__, "message": str(e), "helperPath": str(helper)},
        )

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    try:
        data = json.loads(stdout) if stdout else {}
    except Exception:
        data = {}

    if proc.returncode != 0:
        detail = data if isinstance(data, dict) and data else {"error": "hydration_helper_failed", "stderr": stderr[:1000], "stdout": stdout[:1000]}
        if isinstance(detail, dict):
            detail.setdefault("returncode", proc.returncode)
            detail.setdefault("helperPath", str(helper))
        raise HTTPException(status_code=502, detail=detail)

    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail={"error": "hydration_helper_unexpected_output", "stdout": stdout[:1000], "stderr": stderr[:1000]})
    if not data.get("ok"):
        detail = dict(data)
        detail.setdefault("error", "hydration_helper_not_ok")
        detail.setdefault("helperPath", str(helper))
        raise HTTPException(status_code=int(detail.get("status") or 502), detail=detail)
    return data


async def _hydration_quote_sell(
    *,
    raw_symbol: str,
    base: str,
    quote: str,
    asset_in: Dict[str, Any],
    asset_out: Dict[str, Any],
    amount_in_ui: float,
    step_timeout_s: Optional[float] = None,
    force_isolated: bool = False,
) -> Dict[str, Any]:
    amount_in_atomic = _ui_to_atomic(float(amount_in_ui), int(asset_in.get("decimals") or 0))
    return await _run_hydration_helper(
        {
            "mode": "quote_sell",
            "venue": "polkadot_hydration",
            "rawSymbol": raw_symbol,
            "resolvedSymbol": f"{base}-{quote}",
            "base": base,
            "quote": quote,
            "assetIn": _helper_asset_payload(asset_in),
            "assetOut": _helper_asset_payload(asset_out),
            "amountInAtomic": str(amount_in_atomic),
            "amountInUi": float(amount_in_ui),
            "stepTimeoutS": float(step_timeout_s if step_timeout_s is not None else _HYDRATION_HELPER_STEP_TIMEOUT_S),
            "enableRouterQuotes": bool(_HYDRATION_ENABLE_ROUTER_QUOTES),
            "forceIsolatedHelper": bool(force_isolated),
        },
        force_isolated=bool(force_isolated),
    )




def _hydration_price_cache_requested_symbols(raw: Optional[str]) -> List[str]:
    syms = _csv_symbols(raw) if raw is not None else ["HDX", "DOT", "USDT", "UTTT", "HOLLAR"]
    out: List[str] = []
    for sym in syms:
        s = str(sym or "").strip().upper()
        if s and s not in out:
            out.append(s)
    return out or ["HDX", "DOT", "USDT", "UTTT", "HOLLAR"]


def _hydration_price_cache_payload(*, status: str, requested: List[str]) -> Dict[str, Any]:
    prices = dict(_hydration_usd_price_cache.get("prices") or {})
    sources = dict(_hydration_usd_price_cache.get("sources") or {})
    errors = list(_hydration_usd_price_cache.get("errors") or [])
    now = time.monotonic()
    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "network": "hydration",
        "status": status,
        "requested": requested,
        "prices_usd": {k: v for k, v in prices.items() if k in requested or k in {"USDT", "USDC", "HOLLAR"}},
        "usd_prices": {k: v for k, v in prices.items() if k in requested or k in {"USDT", "USDC", "HOLLAR"}},
        "priceSources": {k: v for k, v in sources.items() if k in requested or k in {"USDT", "USDC", "HOLLAR"}},
        "errors": errors[-12:],
        "cache": {
            "enabled": bool(_HYDRATION_ENABLE_SDK_PRICE_CACHE),
            "ttl_s": _HYDRATION_PRICE_CACHE_TTL_S,
            "error_backoff_s": _HYDRATION_PRICE_CACHE_ERROR_BACKOFF_S,
            "step_timeout_s": _HYDRATION_PRICE_CACHE_STEP_TIMEOUT_S,
            "strategy": _HYDRATION_PRICE_CACHE_STRATEGY,
            "spot_implementation": _HYDRATION_PRICE_CACHE_SPOT_IMPLEMENTATION,
            "sdk_fallback_enabled": bool(_HYDRATION_PRICE_CACHE_USE_SDK_FALLBACK),
            "external_usd_prices_enabled": bool(_HYDRATION_ENABLE_EXTERNAL_USD_PRICES),
            "external_usd_price_source": _HYDRATION_EXTERNAL_USD_PRICE_SOURCE,
            "external_price_ids_json": _HYDRATION_EXTERNAL_PRICE_IDS_JSON,
            "external_price_id_priority": "token_registry_then_env_then_defaults",
            "external_usd_price_timeout_s": _HYDRATION_EXTERNAL_USD_PRICE_TIMEOUT_S,
            "use_sidecar": bool(_HYDRATION_PRICE_CACHE_USE_SIDECAR),
            "autostart_sidecar": bool(_HYDRATION_PRICE_CACHE_AUTOSTART_SIDECAR),
            "force_isolated_helper": bool(_HYDRATION_PRICE_CACHE_FORCE_ISOLATED_HELPER),
            "updated_at": _hydration_usd_price_cache.get("updated_at"),
            "expires_at": _hydration_usd_price_cache.get("expires_at"),
            "error_until": _hydration_usd_price_cache.get("error_until"),
            "now": now,
            "stale": bool(float(_hydration_usd_price_cache.get("expires_at") or 0) <= now),
            "last_error": _hydration_usd_price_cache.get("last_error"),
        },
    }


def _hydration_price_cache_force_isolated() -> bool:
    return bool(
        _HYDRATION_PRICE_CACHE_FORCE_ISOLATED_HELPER
        or not _HYDRATION_PRICE_CACHE_USE_SIDECAR
    )


async def _hydration_price_cache_quote_sell(
    *,
    raw_symbol: str,
    base: str,
    quote: str,
    asset_in: Dict[str, Any],
    asset_out: Dict[str, Any],
    amount_in_ui: float,
) -> Dict[str, Any]:
    amount_in_atomic = _ui_to_atomic(float(amount_in_ui), int(asset_in.get("decimals") or 0))
    force_isolated = _hydration_price_cache_force_isolated()
    return await _run_hydration_helper(
        {
            "mode": "quote_sell",
            "venue": "polkadot_hydration",
            "priceCache": True,
            "rawSymbol": raw_symbol,
            "resolvedSymbol": f"{base}-{quote}",
            "base": base,
            "quote": quote,
            "assetIn": _helper_asset_payload(asset_in),
            "assetOut": _helper_asset_payload(asset_out),
            "amountInAtomic": str(amount_in_atomic),
            "amountInUi": float(amount_in_ui),
            "stepTimeoutS": float(_HYDRATION_PRICE_CACHE_STEP_TIMEOUT_S),
            # Controlled cache calls deliberately bypass the broad orderbook flag.
            # This payload is only emitted by the backend cache layer, not by UI orderbook endpoints.
            "enableRouterQuotes": bool(_HYDRATION_ENABLE_SDK_PRICE_CACHE),
            "forceIsolatedHelper": bool(force_isolated),
            "priceCacheStrategy": _HYDRATION_PRICE_CACHE_STRATEGY,
        },
        force_isolated=bool(force_isolated),
    )


async def _hydration_price_cache_spot(
    *,
    raw_symbol: str,
    base: str,
    quote: str,
    asset_in: Dict[str, Any],
    asset_out: Dict[str, Any],
) -> Dict[str, Any]:
    force_isolated = _hydration_price_cache_force_isolated()
    return await _run_hydration_helper(
        {
            "mode": "price_spot",
            "venue": "polkadot_hydration",
            "priceCache": True,
            "rawSymbol": raw_symbol,
            "resolvedSymbol": f"{base}-{quote}",
            "base": base,
            "quote": quote,
            "assetIn": _helper_asset_payload(asset_in),
            "assetOut": _helper_asset_payload(asset_out),
            "stepTimeoutS": float(_HYDRATION_PRICE_CACHE_STEP_TIMEOUT_S),
            "enableRouterQuotes": bool(_HYDRATION_ENABLE_SDK_PRICE_CACHE),
            "forceIsolatedHelper": bool(force_isolated),
            "priceCacheStrategy": _HYDRATION_PRICE_CACHE_STRATEGY,
        },
        force_isolated=bool(force_isolated),
    )


async def _hydration_price_cache_spot_direct(
    *,
    raw_symbol: str,
    base: str,
    quote: str,
    asset_in: Dict[str, Any],
    asset_out: Dict[str, Any],
) -> Dict[str, Any]:
    force_isolated = _hydration_price_cache_force_isolated()
    return await _run_hydration_helper(
        {
            "mode": "price_spot_direct",
            "venue": "polkadot_hydration",
            "priceCache": True,
            "rawSymbol": raw_symbol,
            "resolvedSymbol": f"{base}-{quote}",
            "base": base,
            "quote": quote,
            "assetIn": _helper_asset_payload(asset_in),
            "assetOut": _helper_asset_payload(asset_out),
            "stepTimeoutS": float(_HYDRATION_PRICE_CACHE_STEP_TIMEOUT_S),
            "enableRouterQuotes": bool(_HYDRATION_ENABLE_SDK_PRICE_CACHE),
            "forceIsolatedHelper": bool(force_isolated),
            "priceCacheStrategy": _HYDRATION_PRICE_CACHE_STRATEGY,
            "priceCacheSpotImplementation": "direct",
        },
        force_isolated=bool(force_isolated),
    )


def _hydration_price_from_quote(qt: Dict[str, Any], *, amount_in_ui: float, out_decimals: int) -> Optional[float]:
    try:
        out_ui = qt.get("amountOutUi")
        if out_ui is None:
            out_atomic = qt.get("amountOutAtomic")
            if out_atomic is not None:
                out_ui = _atomic_to_ui(out_atomic, int(out_decimals))
        px = float(out_ui) / float(amount_in_ui)
        return px if px > 0 else None
    except Exception:
        return None


def _hydration_price_from_spot(qt: Dict[str, Any]) -> Optional[float]:
    for key in ("spotPrice", "price", "spot", "spotPriceNumber", "priceNumber"):
        try:
            value = (qt or {}).get(key)
            if value is None:
                continue
            px = float(str(value).replace(",", ""))
            if px > 0:
                return px
        except Exception:
            pass
    return None


async def _hydration_sdk_pair_price(
    *,
    db: Optional[Session],
    pair: str,
    amount_in_ui: float,
) -> Tuple[Optional[float], Dict[str, Any]]:
    base, quote = _parse_symbol(pair)
    base_meta = _resolve_asset(base, db=db)
    quote_meta = _resolve_asset(quote, db=db)
    attempts: List[Dict[str, Any]] = []

    async def _try_spot() -> Optional[float]:
        impl = _HYDRATION_PRICE_CACHE_SPOT_IMPLEMENTATION
        qt = await (
            _hydration_price_cache_spot_direct(
                raw_symbol=pair,
                base=base,
                quote=quote,
                asset_in=base_meta,
                asset_out=quote_meta,
            )
            if impl == "direct"
            else _hydration_price_cache_spot(
                raw_symbol=pair,
                base=base,
                quote=quote,
                asset_in=base_meta,
                asset_out=quote_meta,
            )
        )
        px = _hydration_price_from_spot(qt)
        attempts.append({"method": "getSpotPrice", "implementation": impl, "ok": px is not None, "price": px, "detail": qt})
        return px

    async def _try_sell() -> Optional[float]:
        qt = await _hydration_price_cache_quote_sell(
            raw_symbol=pair,
            base=base,
            quote=quote,
            asset_in=base_meta,
            asset_out=quote_meta,
            amount_in_ui=float(amount_in_ui),
        )
        px = _hydration_price_from_quote(qt, amount_in_ui=float(amount_in_ui), out_decimals=int(quote_meta.get("decimals") or 0))
        attempts.append({"method": "getBestSell", "ok": px is not None, "price": px, "amountInUi": amount_in_ui, "detail": qt})
        return px

    methods = ["spot", "sell"]
    if _HYDRATION_PRICE_CACHE_STRATEGY == "sell_then_spot":
        methods = ["sell", "spot"]
    elif _HYDRATION_PRICE_CACHE_STRATEGY == "spot_only":
        methods = ["spot"]
    elif _HYDRATION_PRICE_CACHE_STRATEGY == "sell_only":
        methods = ["sell"]

    last_error: Optional[Exception] = None
    for method in methods:
        try:
            px = await (_try_spot() if method == "spot" else _try_sell())
            if px is not None and px > 0:
                return px, {"pair": pair, "amountInUi": amount_in_ui, "strategy": _HYDRATION_PRICE_CACHE_STRATEGY, "attempts": attempts}
        except Exception as e:
            last_error = e
            attempts.append({"method": "getSpotPrice" if method == "spot" else "getBestSell", "ok": False, "error": type(e).__name__, "message": str(e), "detail": getattr(e, "detail", None)})

    if last_error is not None:
        raise last_error
    return None, {"pair": pair, "amountInUi": amount_in_ui, "strategy": _HYDRATION_PRICE_CACHE_STRATEGY, "attempts": attempts}


async def _hydration_try_usd_pair(
    *,
    db: Optional[Session],
    symbol: str,
    direct_amount_ui: float,
    inverse_amount_ui: float = 1.0,
) -> Tuple[Optional[float], Optional[str], List[Dict[str, Any]]]:
    sym = str(symbol or "").strip().upper()
    errors: List[Dict[str, Any]] = []
    try:
        px, detail = await _hydration_sdk_pair_price(db=db, pair=f"{sym}-USDT", amount_in_ui=direct_amount_ui)
        if px is not None and px > 0:
            return px, f"sdk:{sym}-USDT", errors
        errors.append({"pair": f"{sym}-USDT", "error": "empty_quote", "detail": detail})
    except Exception as e:
        errors.append({"pair": f"{sym}-USDT", "error": type(e).__name__, "message": str(e), "detail": getattr(e, "detail", None)})
    if len(errors) >= max(1, int(_HYDRATION_PRICE_CACHE_MAX_QUOTE_ERRORS)):
        return None, None, errors
    try:
        inv, detail = await _hydration_sdk_pair_price(db=db, pair=f"USDT-{sym}", amount_in_ui=inverse_amount_ui)
        if inv is not None and inv > 0:
            return 1.0 / inv, f"sdk:USDT-{sym}:inverse", errors
        errors.append({"pair": f"USDT-{sym}", "error": "empty_quote", "detail": detail})
    except Exception as e:
        errors.append({"pair": f"USDT-{sym}", "error": type(e).__name__, "message": str(e), "detail": getattr(e, "detail", None)})
    return None, None, errors


def _ensure_token_registry_external_price_columns(db: Optional[Session]) -> None:
    if db is None:
        return
    try:
        rows = db.execute(text("PRAGMA table_info(token_registry)")).mappings().all()
        cols = {str(r.get("name") or "") for r in rows}
        changed = False
        if "external_price_source" not in cols:
            db.execute(text("ALTER TABLE token_registry ADD COLUMN external_price_source TEXT"))
            changed = True
        if "external_price_id" not in cols:
            db.execute(text("ALTER TABLE token_registry ADD COLUMN external_price_id TEXT"))
            changed = True
        if changed:
            db.commit()
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass


def _hydration_registry_external_price_overrides(
    *,
    db: Optional[Session],
    requested: List[str],
) -> Dict[str, Dict[str, Optional[str]]]:
    """Return UI-managed external price metadata from Token Registry.

    Token Registry rows are preferred over env JSON for external price IDs so
    new Hydration assets can be configured from the UI.  Lookup priority mirrors
    _db_lookup_hydration_asset: exact polkadot/polkadot_hydration first, then
    hydration aliases, then global rows.
    """
    if db is None:
        return {}
    symbols = [str(s or "").strip().upper() for s in (requested or []) if str(s or "").strip()]
    if not symbols:
        return {}
    _ensure_token_registry_external_price_columns(db)
    out: Dict[str, Dict[str, Optional[str]]] = {}
    try:
        rows = db.execute(
            text(
                """
                SELECT symbol, chain, venue, external_price_source, external_price_id
                FROM token_registry
                WHERE UPPER(symbol) = :symbol
                ORDER BY
                  CASE
                    WHEN chain = 'polkadot' AND venue = 'polkadot_hydration' THEN 0
                    WHEN chain IN ('polkadot','hydration') AND venue IN ('polkadot_hydration','hydration') THEN 1
                    WHEN chain IN ('polkadot','hydration') AND venue IS NULL THEN 2
                    WHEN chain IN ('polkadot','hydration') AND venue = '' THEN 3
                    ELSE 9
                  END
                LIMIT 1
                """
            ),
            {"symbol": "__never__"},
        )
        # no-op probe above confirms columns exist on older SQLite builds
    except Exception:
        return out

    for sym in symbols:
        try:
            row = db.execute(
                text(
                    """
                    SELECT symbol, chain, venue, external_price_source, external_price_id
                    FROM token_registry
                    WHERE UPPER(symbol) = :symbol
                    ORDER BY
                      CASE
                        WHEN chain = 'polkadot' AND venue = 'polkadot_hydration' THEN 0
                        WHEN chain IN ('polkadot','hydration') AND venue IN ('polkadot_hydration','hydration') THEN 1
                        WHEN chain IN ('polkadot','hydration') AND venue IS NULL THEN 2
                        WHEN chain IN ('polkadot','hydration') AND venue = '' THEN 3
                        ELSE 9
                      END
                    LIMIT 1
                    """
                ),
                {"symbol": sym},
            ).mappings().first()
            if not row:
                continue
            src = str(row.get("external_price_source") or "").strip().lower() or None
            pid = str(row.get("external_price_id") or "").strip() or None
            if src or pid:
                out[sym] = {
                    "source": src,
                    "id": pid,
                    "chain": row.get("chain"),
                    "venue": row.get("venue"),
                }
        except Exception:
            continue
    return out


async def _hydration_fetch_external_usd_prices(
    *,
    db: Optional[Session],
    requested: List[str],
) -> Tuple[Dict[str, float], Dict[str, str], List[Dict[str, Any]]]:
    """Fetch lightweight non-SDK USD prices for liquid Hydration assets.

    This intentionally avoids Hydration sdk-next/PAPI router calls.  It is used
    only as a cached USD conversion source for balances/spread display; UTTT
    itself is still derived from the live manual UTTT-HDX pool reserves.
    """
    if not _HYDRATION_ENABLE_EXTERNAL_USD_PRICES:
        return {}, {}, []

    source = str(_HYDRATION_EXTERNAL_USD_PRICE_SOURCE or "").strip().lower()
    if source not in {"coingecko", "coingecko_simple"}:
        return {}, {}, [{"source": source, "error": "unsupported_external_price_source"}]

    try:
        id_map = _json_map(_HYDRATION_EXTERNAL_PRICE_IDS_JSON)
    except Exception:
        id_map = {}
    defaults = {"HDX": "hydration", "DOT": "polkadot"}
    for k, v in defaults.items():
        id_map.setdefault(k, v)

    registry_overrides = _hydration_registry_external_price_overrides(db=db, requested=requested)
    skip_external_symbols: set[str] = set()
    for sym, meta in (registry_overrides or {}).items():
        src = str((meta or {}).get("source") or "").strip().lower()
        price_id = str((meta or {}).get("id") or "").strip()
        if src in {"coingecko", "coingecko_simple"} and price_id:
            id_map[str(sym).upper()] = price_id
        elif src in {"stable", "derived", "none"}:
            skip_external_symbols.add(str(sym).upper())

    symbols: List[str] = []
    ids: List[str] = []
    for sym_raw in requested or []:
        sym = str(sym_raw or "").strip().upper()
        if sym in {"USDT", "USDC", "HOLLAR", "UTTT"}:
            continue
        if sym in skip_external_symbols:
            continue
        coin_id = str(id_map.get(sym) or "").strip()
        if sym and coin_id and coin_id not in ids:
            symbols.append(sym)
            ids.append(coin_id)

    if not ids:
        return {}, {}, []

    try:
        async with httpx.AsyncClient(timeout=max(1.0, float(_HYDRATION_EXTERNAL_USD_PRICE_TIMEOUT_S))) as client:
            r = await client.get(
                _HYDRATION_COINGECKO_SIMPLE_PRICE_URL,
                params={"ids": ",".join(ids), "vs_currencies": "usd"},
                headers={"accept": "application/json", "user-agent": "UTT/0.1 hydration-price-cache"},
            )
        if r.status_code >= 400:
            return {}, {}, [{
                "source": "coingecko",
                "error": "external_usd_price_http_error",
                "status": r.status_code,
                "body": (r.text or "")[:500],
            }]
        data = r.json() or {}
    except Exception as e:
        return {}, {}, [{
            "source": "coingecko",
            "error": type(e).__name__,
            "message": str(e),
        }]

    prices: Dict[str, float] = {}
    sources: Dict[str, str] = {}
    errors: List[Dict[str, Any]] = []
    reverse = {str(v).strip(): str(k).strip().upper() for k, v in id_map.items() if str(v).strip()}
    for coin_id in ids:
        sym = reverse.get(coin_id)
        try:
            px = _float_or_none((data.get(coin_id) or {}).get("usd"))
        except Exception:
            px = None
        if sym and px is not None and px > 0:
            prices[sym] = float(px)
            sources[sym] = f"coingecko:{coin_id}"
        elif sym:
            errors.append({"source": "coingecko", "symbol": sym, "coinId": coin_id, "error": "missing_usd_price"})
    return prices, sources, errors


async def _hydration_refresh_usd_price_cache(
    *,
    db: Optional[Session],
    requested: List[str],
    force_refresh: bool = False,
    allow_refresh: bool = True,
) -> Dict[str, Any]:
    now = time.monotonic()
    requested = _hydration_price_cache_requested_symbols(",".join(requested))

    # Always keep stablecoins available without SDK work.
    def _seed_stables() -> None:
        prices = dict(_hydration_usd_price_cache.get("prices") or {})
        sources = dict(_hydration_usd_price_cache.get("sources") or {})
        for stable in ("USDT", "USDC", "HOLLAR"):
            prices[stable] = 1.0
            sources[stable] = "stable"
        _hydration_usd_price_cache["prices"] = prices
        _hydration_usd_price_cache["sources"] = sources

    async def _derive_uttt_from_hdx(prices: Dict[str, Any], sources: Dict[str, Any], errors: List[Dict[str, Any]]) -> None:
        if "UTTT" not in requested:
            return
        if _float_or_none(prices.get("UTTT")) is not None:
            return
        hdx_usd = _float_or_none(prices.get("HDX"))
        if hdx_usd is None:
            return
        try:
            uttt_meta = _resolve_asset("UTTT", db=db)
            hdx_meta = _resolve_asset("HDX", db=db)
            cfg = await _hydration_manual_pool_config_with_live_reserves(base="UTTT", quote="HDX", base_meta=uttt_meta, quote_meta=hdx_meta, db=db)
            spot = _float_or_none((cfg or {}).get("spotPrice"))
            if spot is not None:
                prices["UTTT"] = float(spot * hdx_usd)
                src = str((cfg or {}).get("source") or "manual_xyk")
                hdx_src = str(sources.get("HDX") or "HDX-USD")
                sources["UTTT"] = f"derived:UTTT-HDX×HDX-USD:{src}:{hdx_src}"
        except Exception as e:
            errors.append({"pair": "UTTT-HDX", "error": type(e).__name__, "message": str(e), "detail": getattr(e, "detail", None)})

    _seed_stables()

    if not allow_refresh and not force_refresh:
        return _hydration_price_cache_payload(status="cache_only", requested=requested)

    cached_prices = _hydration_usd_price_cache.get("prices") or {}
    cache_fresh = float(_hydration_usd_price_cache.get("expires_at") or 0) > now
    has_requested = all((s in cached_prices and _float_or_none(cached_prices.get(s)) is not None) for s in requested if s not in {"USDT", "USDC", "HOLLAR"})
    if not force_refresh and cache_fresh and has_requested:
        return _hydration_price_cache_payload(status="fresh", requested=requested)

    error_until = float(_hydration_usd_price_cache.get("error_until") or 0)
    if not force_refresh and error_until > now:
        return _hydration_price_cache_payload(status="error_backoff_stale", requested=requested)

    async with _hydration_usd_price_cache_lock:
        now = time.monotonic()
        cached_prices = _hydration_usd_price_cache.get("prices") or {}
        cache_fresh = float(_hydration_usd_price_cache.get("expires_at") or 0) > now
        has_requested = all((s in cached_prices and _float_or_none(cached_prices.get(s)) is not None) for s in requested if s not in {"USDT", "USDC", "HOLLAR"})
        if not force_refresh and cache_fresh and has_requested:
            return _hydration_price_cache_payload(status="fresh", requested=requested)

        prices = dict(_hydration_usd_price_cache.get("prices") or {})
        sources = dict(_hydration_usd_price_cache.get("sources") or {})
        errors: List[Dict[str, Any]] = []
        for stable in ("USDT", "USDC", "HOLLAR"):
            prices[stable] = 1.0
            sources[stable] = "stable"

        try:
            external_prices, external_sources, external_errors = await _hydration_fetch_external_usd_prices(db=db, requested=requested)
            errors.extend(external_errors or [])
            for sym, px in (external_prices or {}).items():
                val = _float_or_none(px)
                if val is not None:
                    prices[str(sym).upper()] = float(val)
                    sources[str(sym).upper()] = external_sources.get(str(sym).upper()) or "external_usd_price"

            await _derive_uttt_from_hdx(prices, sources, errors)

            non_stable_requested = [s for s in requested if s not in {"USDT", "USDC", "HOLLAR"}]
            missing_requested = [s for s in non_stable_requested if _float_or_none(prices.get(s)) is None]

            # Optional SDK fallback is off by default.  It remains available for
            # explicit local diagnostics, but the UI price cache should not
            # reopen sdk-next/PAPI quote polling during normal refreshes.
            if missing_requested and _HYDRATION_PRICE_CACHE_USE_SDK_FALLBACK and _HYDRATION_ENABLE_SDK_PRICE_CACHE:
                need_dot_direct = "DOT" in missing_requested
                need_hdx = "HDX" in missing_requested or "UTTT" in missing_requested

                if need_dot_direct:
                    dot_px, dot_src, dot_errors = await _hydration_try_usd_pair(db=db, symbol="DOT", direct_amount_ui=1.0, inverse_amount_ui=1.0)
                    errors.extend(dot_errors)
                    if dot_px is not None:
                        prices["DOT"] = float(dot_px)
                        sources["DOT"] = dot_src or "sdk:DOT-USDT"

                if need_hdx:
                    hdx_px, hdx_src, hdx_errors = await _hydration_try_usd_pair(db=db, symbol="HDX", direct_amount_ui=100.0, inverse_amount_ui=1.0)
                    errors.extend(hdx_errors)
                    if hdx_px is None:
                        dot_usd = _float_or_none(prices.get("DOT"))
                        if dot_usd is not None:
                            try:
                                hdx_dot, detail = await _hydration_sdk_pair_price(db=db, pair="HDX-DOT", amount_in_ui=100.0)
                                if hdx_dot is not None:
                                    hdx_px = hdx_dot * dot_usd
                                    hdx_src = "sdk:HDX-DOT×DOT-USD"
                                else:
                                    errors.append({"pair": "HDX-DOT", "error": "empty_quote", "detail": detail})
                            except Exception as e:
                                errors.append({"pair": "HDX-DOT", "error": type(e).__name__, "message": str(e), "detail": getattr(e, "detail", None)})
                    if hdx_px is not None:
                        prices["HDX"] = float(hdx_px)
                        sources["HDX"] = hdx_src or "sdk:HDX-USDT"

                await _derive_uttt_from_hdx(prices, sources, errors)

            missing_requested = [s for s in non_stable_requested if _float_or_none(prices.get(s)) is None]
            partial_error = bool(missing_requested)
            if not partial_error:
                status = "refreshed_external" if external_prices else "refreshed"
                last_error = None
                error_until = 0.0
            else:
                status = "partial_prices_stale"
                error_until = now + max(30.0, float(_HYDRATION_PRICE_CACHE_ERROR_BACKOFF_S))
                last_error = {
                    "error": "hydration_usd_price_cache_partial",
                    "message": "One or more Hydration USD prices could not be resolved from the live non-SDK price source. Returning cached/stable prices and backing off before the next refresh attempt.",
                    "missing": missing_requested,
                    "sdkFallbackEnabled": bool(_HYDRATION_PRICE_CACHE_USE_SDK_FALLBACK),
                    "externalSource": _HYDRATION_EXTERNAL_USD_PRICE_SOURCE,
                }

            _hydration_usd_price_cache.update({
                "prices": prices,
                "sources": sources,
                "errors": errors[-12:],
                "updated_at": now,
                "expires_at": now + max(30.0, float(_HYDRATION_PRICE_CACHE_TTL_S)),
                "error_until": error_until,
                "last_error": last_error,
            })
            return _hydration_price_cache_payload(status=status, requested=requested)
        except Exception as e:
            _hydration_usd_price_cache.update({
                "errors": errors[-12:],
                "error_until": now + max(30.0, float(_HYDRATION_PRICE_CACHE_ERROR_BACKOFF_S)),
                "last_error": {"error": type(e).__name__, "message": str(e), "detail": getattr(e, "detail", None)},
            })
            return _hydration_price_cache_payload(status="refresh_failed_stale", requested=requested)

async def _hydration_swap_tx_build(
    *,
    raw_symbol: str,
    base: str,
    quote: str,
    side: str,
    asset_in: Dict[str, Any],
    asset_out: Dict[str, Any],
    amount_ui: float,
    amount_mode: str,
    slippage_bps: int,
    user_pubkey: str,
    manual_custom_swap: Optional[Dict[str, Any]] = None,
    route_mode: str = "auto",
) -> Dict[str, Any]:
    mode = str(amount_mode or "exact_in").strip().lower()
    if mode not in {"exact_in", "exact_out"}:
        raise HTTPException(status_code=422, detail={"error": "invalid_hydration_amount_mode", "amount_mode": amount_mode, "expected": "exact_in|exact_out"})

    amount_in_atomic: Optional[int] = None
    amount_out_atomic: Optional[int] = None
    if mode == "exact_out":
        amount_out_atomic = _ui_to_atomic(float(amount_ui), int(asset_out.get("decimals") or 0))
    else:
        amount_in_atomic = _ui_to_atomic(float(amount_ui), int(asset_in.get("decimals") or 0))

    route_mode_norm = _hydration_route_mode(route_mode)

    # Keep exact-out BUY isolated from the persistent sidecar.  The isolated
    # getBestBuy probe proved the quote path can complete, while the persistent
    # sidecar can still hang/starve on exact-out BUY swap_tx builds.  SELL stays
    # on the sidecar path; BUY uses a short-lived helper process that exits after
    # emitting the unsigned payload.  route_mode=isolated_helper also forces this
    # path for diagnostics.
    force_isolated_helper = bool(route_mode_norm == "isolated_helper" or mode == "exact_out")
    payload = {
        "mode": "swap_tx",
        "venue": "polkadot_hydration",
        "rawSymbol": raw_symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base,
        "quote": quote,
        "side": side,
        "amountMode": mode,
        "assetIn": _helper_asset_payload(asset_in),
        "assetOut": _helper_asset_payload(asset_out),
        "amountInAtomic": str(amount_in_atomic) if amount_in_atomic is not None else None,
        "amountInUi": float(amount_ui) if mode == "exact_in" else None,
        "amountOutAtomic": str(amount_out_atomic) if amount_out_atomic is not None else None,
        "amountOutUi": float(amount_ui) if mode == "exact_out" else None,
        "slippageBps": int(slippage_bps),
        "userPubkey": str(user_pubkey or "").strip(),
        "beneficiary": str(user_pubkey or "").strip(),
        "stepTimeoutS": float(_HYDRATION_HELPER_STEP_TIMEOUT_S),
        "enableRouterQuotes": bool(_HYDRATION_ENABLE_ROUTER_QUOTES),
        "enableSwapTx": bool(_HYDRATION_ENABLE_SWAP_TX),
        "enableExactBuy": bool(_HYDRATION_ENABLE_EXACT_BUY),
        "forceIsolatedHelper": bool(force_isolated_helper),
        "routeMode": route_mode_norm,
    }
    if isinstance(manual_custom_swap, dict) and manual_custom_swap.get("enabled"):
        payload["manualCustomSwap"] = manual_custom_swap
        # Manual custom-asset fallback bypasses sdk-next router quotes, so it is
        # safe to use the isolated helper for both exact-in and exact-out calls.
        force_isolated_helper = True
        payload["forceIsolatedHelper"] = True
        payload["enableRouterQuotes"] = bool(_HYDRATION_ENABLE_ROUTER_QUOTES)

    result = await _run_hydration_helper(
        payload,
        force_isolated=force_isolated_helper,
    )
    if isinstance(result, dict):
        result.setdefault("routeMode", route_mode_norm)
    if force_isolated_helper and isinstance(result, dict):
        result.setdefault("executionMode", "isolated_helper")
        result.setdefault("isolatedExactBuy", True)
    return result


def _suggest_price_decimals(levels: List[Dict[str, Any]], fallback: int) -> int:
    best = int(fallback)
    for lvl in levels or []:
        try:
            px = float(lvl.get("price"))
        except Exception:
            continue
        if not (px > 0):
            continue
        s2 = f"{px:.12f}".rstrip("0").rstrip(".")
        if "." not in s2:
            continue
        best = max(best, len(s2.split(".", 1)[1]))
    return max(1, min(best, 12))


def _hydration_sample_sizes(symbol: str, decimals: int, *, side: str, depth: int) -> List[float]:
    sym = (symbol or "").strip().upper()
    n = max(1, min(int(depth), 10))

    # Allow quick local tuning without another code patch.  This is useful because
    # Hydration router quotes can behave badly for dust-sized inputs.
    try:
        overrides = _json_map(_HYDRATION_SAMPLE_SIZES_JSON)
        raw_vals = overrides.get(sym)
        if isinstance(raw_vals, list):
            vals = [float(v) for v in raw_vals if float(v) > 0]
            if vals:
                return vals[:n]
    except Exception:
        pass

    # The old generic major-asset ladder started DOT/HDX at 0.001.  Direct SDK
    # testing proved 1 DOT -> HDX quotes cleanly, while 0.001 DOT/HDX samples
    # timed out in the backend orderbook path.  Use economically meaningful
    # first samples so depth=1 exercises the same route that already works.
    if sym == "HDX":
        vals = [100, 250, 500, 1000, 2000, 5000, 10000, 20000, 50000, 100000]
    elif sym in {"DOT", "USDT", "USDC", "DAI", "USDC.E", "USDT.E"}:
        vals = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000]
    elif sym == "KSM":
        vals = [0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100]
    elif sym == "WETH":
        vals = [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0]
    elif sym == "WBTC":
        vals = [0.00001, 0.00002, 0.00005, 0.0001, 0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01]
    elif int(decimals or 0) <= 6:
        vals = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000]
    else:
        vals = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000]
    return vals[:n]


def _parse_symbol(symbol: str) -> Tuple[str, str]:
    s = (symbol or "").strip()
    if "-" not in s:
        raise HTTPException(status_code=422, detail=f"Invalid symbol '{symbol}' (expected BASE-QUOTE)")
    left, right = s.split("-", 1)
    left = left.strip().upper()
    right = right.strip().upper()
    if not left or not right:
        raise HTTPException(status_code=422, detail=f"Invalid symbol '{symbol}' (expected BASE-QUOTE)")
    return left, right


def _json_map(raw: str) -> Dict[str, Any]:
    try:
        data = json.loads(raw or "{}")
        if isinstance(data, dict):
            return {str(k).upper(): v for k, v in data.items()}
    except Exception:
        pass
    return {}


def _asset_id_norm_for_compare(value: Any) -> str:
    raw = str(value if value is not None else "").strip().lower()
    if raw == "native":
        return str(_HYDRATION_NATIVE_ASSET_ID)
    return raw


def _is_asset_meta_id(meta: Dict[str, Any], expected: Any) -> bool:
    return _asset_id_norm_for_compare((meta or {}).get("assetId")) == _asset_id_norm_for_compare(expected)


def _float_or_none(value: Any) -> Optional[float]:
    try:
        x = float(value)
        if x > 0 and x != float("inf"):
            return x
    except Exception:
        pass
    return None


def _route_leg_asset_value(leg: Dict[str, Any], *keys: str) -> Optional[int]:
    if not isinstance(leg, dict):
        return None
    for key in keys:
        if key in leg:
            try:
                return int(str(leg.get(key)).strip())
            except Exception:
                return None
    return None


def _route_leg_pool_value(leg: Dict[str, Any]) -> str:
    pool = leg.get("pool") if isinstance(leg, dict) else None
    if isinstance(pool, dict):
        return str(pool.get("type") or pool.get("value") or "XYK").strip() or "XYK"
    return str(pool or "XYK").strip() or "XYK"


def _route_leg_with_assets(leg: Dict[str, Any], asset_in: int, asset_out: int) -> Dict[str, Any]:
    out = dict(leg or {})
    if "asset_in" in out or "asset_out" in out:
        out["asset_in"] = int(asset_in)
        out["asset_out"] = int(asset_out)
        out.pop("assetIn", None)
        out.pop("assetOut", None)
    else:
        out["assetIn"] = int(asset_in)
        out["assetOut"] = int(asset_out)
    if "pool" not in out:
        out["pool"] = "XYK"
    return out


def _reverse_hydration_route_legs(route: Optional[List[Dict[str, Any]]]) -> Optional[List[Dict[str, Any]]]:
    if not isinstance(route, list) or not route:
        return route
    reversed_route: List[Dict[str, Any]] = []
    for leg in reversed(route):
        if not isinstance(leg, dict):
            continue
        leg_in = _route_leg_asset_value(leg, "assetIn", "asset_in")
        leg_out = _route_leg_asset_value(leg, "assetOut", "asset_out")
        if leg_in is None or leg_out is None:
            reversed_route.append(dict(leg))
        else:
            reversed_route.append(_route_leg_with_assets(leg, leg_out, leg_in))
    return reversed_route or route


def _hydration_manual_route_for_direction(
    *,
    cfg_route: Any,
    asset_in_id: int,
    asset_out_id: int,
    pool: str = "XYK",
) -> List[Dict[str, Any]]:
    """Return manual Router route legs oriented to the actual trade direction.

    Route-registry rows are stored in pair orientation, e.g. UTTT-HDX.  When the
    same row is used in reverse orientation (HDX-UTTT) or when a BUY is executed
    as quote-spend exact-in, the submitted Router route must also be reversed.
    If the saved route cannot be proven to match the current direction, fall
    back to a single XYK leg in the requested direction rather than submitting a
    stale canonical route that the chain rejects with Router.InvalidRoute.
    """
    default_route = [{"pool": str(pool or "XYK"), "assetIn": int(asset_in_id), "assetOut": int(asset_out_id)}]
    if not isinstance(cfg_route, list) or not cfg_route:
        return default_route

    route = [dict(x) for x in cfg_route if isinstance(x, dict)]
    if not route:
        return default_route

    first_in = _route_leg_asset_value(route[0], "assetIn", "asset_in")
    last_out = _route_leg_asset_value(route[-1], "assetOut", "asset_out")
    if first_in == int(asset_in_id) and last_out == int(asset_out_id):
        return route

    reversed_route = _reverse_hydration_route_legs(route)
    if isinstance(reversed_route, list) and reversed_route:
        rev_first_in = _route_leg_asset_value(reversed_route[0], "assetIn", "asset_in")
        rev_last_out = _route_leg_asset_value(reversed_route[-1], "assetOut", "asset_out")
        if rev_first_in == int(asset_in_id) and rev_last_out == int(asset_out_id):
            return reversed_route

    return default_route



def _ensure_hydration_route_registry_table(db: Session) -> None:
    """Create/expand the local Hydration manual route registry table.

    This keeps manually configured Hydration XYK routes out of source code.  It
    is intentionally generic enough for custom pairs such as UTTT-HDX and future
    unsupported-sdk pairs, while normal SDK-supported pairs can continue using
    route_mode=auto/sdk with no registry row.
    """
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS hydration_route_registry (
            id TEXT PRIMARY KEY,
            symbol TEXT UNIQUE,
            base_symbol TEXT,
            quote_symbol TEXT,
            base_asset_id TEXT,
            quote_asset_id TEXT,
            base_decimals INTEGER,
            quote_decimals INTEGER,
            route_mode TEXT,
            pool_type TEXT,
            pool_account TEXT,
            enabled INTEGER DEFAULT 1,
            base_reserve REAL,
            quote_reserve REAL,
            fee_bps REAL,
            route_json TEXT,
            note TEXT,
            created_at TEXT,
            updated_at TEXT
        )
    """))
    db.commit()

    required = {
        "id": "TEXT",
        "symbol": "TEXT",
        "base_symbol": "TEXT",
        "quote_symbol": "TEXT",
        "base_asset_id": "TEXT",
        "quote_asset_id": "TEXT",
        "base_decimals": "INTEGER",
        "quote_decimals": "INTEGER",
        "route_mode": "TEXT",
        "pool_type": "TEXT",
        "pool_account": "TEXT",
        "enabled": "INTEGER DEFAULT 1",
        "base_reserve": "REAL",
        "quote_reserve": "REAL",
        "fee_bps": "REAL",
        "route_json": "TEXT",
        "note": "TEXT",
        "created_at": "TEXT",
        "updated_at": "TEXT",
    }
    try:
        bind = db.get_bind()
        cols = {c.get("name") for c in sa_inspect(bind).get_columns("hydration_route_registry")}
    except Exception:
        cols = set()
    for col, typ in required.items():
        if col not in cols:
            try:
                db.execute(text(f"ALTER TABLE hydration_route_registry ADD COLUMN {col} {typ}"))
            except Exception:
                pass
    try:
        db.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ux_hydration_route_registry_symbol ON hydration_route_registry(symbol)"))
        db.execute(text("CREATE INDEX IF NOT EXISTS ix_hydration_route_registry_enabled ON hydration_route_registry(enabled)"))
    except Exception:
        pass
    db.commit()


def _hydration_route_registry_row_to_cfg(row: Any, *, pair: str, reverse: bool = False) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    r = dict(row) if not isinstance(row, dict) else dict(row)
    if str(r.get("route_mode") or "manual_xyk").strip().lower() not in {"manual_xyk", "manual", "xyk"}:
        return None
    if str(r.get("pool_type") or "XYK").strip().upper() != "XYK":
        return None

    base_reserve = _float_or_none(r.get("base_reserve"))
    quote_reserve = _float_or_none(r.get("quote_reserve"))
    if base_reserve is None or quote_reserve is None:
        return None
    if reverse:
        base_reserve, quote_reserve = quote_reserve, base_reserve

    route_json = r.get("route_json")
    route: Optional[List[Dict[str, Any]]] = None
    try:
        parsed = json.loads(route_json or "[]")
        if isinstance(parsed, list):
            route = [x for x in parsed if isinstance(x, dict)]
            if reverse:
                route = _reverse_hydration_route_legs(route)
    except Exception:
        route = None

    source_symbol = str(r.get("symbol") or pair).strip().upper()
    return {
        "pair": pair,
        "baseReserve": float(base_reserve),
        "quoteReserve": float(quote_reserve),
        "feeBps": float(_float_or_none(r.get("fee_bps")) or 30.0),
        "pool": "XYK",
        "route": route,
        "poolAccount": str(r.get("pool_account") or "").strip() or None,
        "source": "db:hydration_route_registry" + (":reversed" if reverse else ""),
        "sourcePair": source_symbol,
        "routeRegistryId": r.get("id"),
        "routeRegistrySymbol": source_symbol,
        "note": r.get("note") or "Manual Hydration XYK route registry entry.",
    }


def _hydration_route_registry_manual_config(
    *,
    db: Optional[Session],
    base: str,
    quote: str,
) -> Optional[Dict[str, Any]]:
    if db is None:
        return None
    pair = f"{str(base or '').upper()}-{str(quote or '').upper()}"
    reverse_pair = f"{str(quote or '').upper()}-{str(base or '').upper()}"
    try:
        _ensure_hydration_route_registry_table(db)
        row = db.execute(
            text("""
                SELECT * FROM hydration_route_registry
                WHERE UPPER(symbol) = :symbol AND COALESCE(enabled, 1) = 1
                LIMIT 1
            """),
            {"symbol": pair},
        ).mappings().first()
        if row:
            return _hydration_route_registry_row_to_cfg(row, pair=pair, reverse=False)

        row = db.execute(
            text("""
                SELECT * FROM hydration_route_registry
                WHERE UPPER(symbol) = :symbol AND COALESCE(enabled, 1) = 1
                LIMIT 1
            """),
            {"symbol": reverse_pair},
        ).mappings().first()
        if row:
            return _hydration_route_registry_row_to_cfg(row, pair=pair, reverse=True)
    except Exception:
        return None
    return None




def _hydration_pool_account_from_cfg(cfg: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(cfg, dict):
        return None
    for key in ("poolAccount", "pool_account", "poolAddress", "pool_address", "account"):
        value = cfg.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


async def _hydration_pool_asset_reserve(
    *,
    pool_account: str,
    symbol: str,
    meta: Dict[str, Any],
) -> Dict[str, Any]:
    decimals = int((meta or {}).get("decimals") or 0)
    is_native = bool((meta or {}).get("native")) or str((meta or {}).get("assetId") or "").strip().lower() == "native"
    if is_native:
        storage_key = _system_account_storage_key(pool_account)
        raw = await _rpc("state_getStorage", [storage_key])
        decoded = _decode_system_account_info(raw)
        pallet = "System"
        item = "Account"
    else:
        asset_id = _hydration_sdk_asset_id(meta)
        storage_key = _tokens_account_storage_key(pool_account, asset_id)
        raw = await _rpc("state_getStorage", [storage_key])
        decoded = _decode_tokens_account_data(raw)
        pallet = "Tokens"
        item = "Accounts"

    free_atomic = int(decoded.get("free_atomic") or 0)
    reserved_atomic = int(decoded.get("reserved_atomic") or 0)
    frozen_atomic = int(decoded.get("frozen_atomic") or 0)
    total_atomic = free_atomic + reserved_atomic
    return {
        "symbol": str(symbol or (meta or {}).get("symbol") or "").upper(),
        "assetId": (meta or {}).get("assetId"),
        "decimals": decimals,
        "native": is_native,
        "freeAtomic": str(free_atomic),
        "reservedAtomic": str(reserved_atomic),
        "frozenAtomic": str(frozen_atomic),
        "totalAtomic": str(total_atomic),
        "free": _atomic_to_ui(free_atomic, decimals),
        "reserved": _atomic_to_ui(reserved_atomic, decimals),
        "frozen": _atomic_to_ui(frozen_atomic, decimals),
        "total": _atomic_to_ui(total_atomic, decimals),
        "reserveUi": _atomic_to_ui(free_atomic, decimals),
        "reserveAtomic": str(free_atomic),
        "reserveSource": "free",
        "storagePallet": pallet,
        "storageItem": item,
        "storageKey": storage_key,
    }


async def _hydration_manual_pool_config_with_live_reserves(
    *,
    base: str,
    quote: str,
    base_meta: Dict[str, Any],
    quote_meta: Dict[str, Any],
    db: Optional[Session] = None,
) -> Optional[Dict[str, Any]]:
    cfg = _hydration_manual_pool_config(
        base=base,
        quote=quote,
        base_meta=base_meta,
        quote_meta=quote_meta,
        db=db,
    )
    if not isinstance(cfg, dict):
        return cfg

    pool_account = _hydration_pool_account_from_cfg(cfg)
    if not _HYDRATION_MANUAL_POOL_LIVE_RESERVES:
        out = dict(cfg)
        out["liveReserves"] = {"enabled": False, "ok": False, "reason": "UTT_HYDRATION_MANUAL_POOL_LIVE_RESERVES=0"}
        return out
    if not pool_account:
        out = dict(cfg)
        out["liveReserves"] = {
            "enabled": True,
            "ok": False,
            "reason": "pool_account_missing",
            "message": "Add pool_account to this Hydration route registry row to enable live XYK reserve discovery.",
        }
        return out

    out = dict(cfg)
    out["poolAccount"] = pool_account
    try:
        base_reserve = await _hydration_pool_asset_reserve(pool_account=pool_account, symbol=base, meta=base_meta)
        quote_reserve = await _hydration_pool_asset_reserve(pool_account=pool_account, symbol=quote, meta=quote_meta)
        base_ui = _float_or_none(base_reserve.get("reserveUi"))
        quote_ui = _float_or_none(quote_reserve.get("reserveUi"))
        if base_ui is None or quote_ui is None:
            out["liveReserves"] = {
                "enabled": True,
                "ok": False,
                "reason": "zero_or_missing_pool_reserve",
                "poolAccount": pool_account,
                "base": base_reserve,
                "quote": quote_reserve,
                "fallback": "route_registry_snapshot",
            }
            return out

        out["baseReserveSnapshot"] = out.get("baseReserve")
        out["quoteReserveSnapshot"] = out.get("quoteReserve")
        out["baseReserve"] = float(base_ui)
        out["quoteReserve"] = float(quote_ui)
        out["spotPrice"] = float(quote_ui / base_ui) if base_ui > 0 else None
        out["inversePrice"] = float(base_ui / quote_ui) if quote_ui > 0 else None
        source = str(out.get("source") or "manual_xyk")
        if "+live_pool_account" not in source:
            out["source"] = source + "+live_pool_account"
        out["liveReserves"] = {
            "enabled": True,
            "ok": True,
            "poolAccount": pool_account,
            "base": base_reserve,
            "quote": quote_reserve,
            "baseReserveSnapshot": out.get("baseReserveSnapshot"),
            "quoteReserveSnapshot": out.get("quoteReserveSnapshot"),
            "source": "on_chain_pool_account_balances",
            "note": "Live reserves use the pool account free balance for each asset. If this is not the XYK pool account, remove or correct pool_account.",
        }
        return out
    except HTTPException as e:
        out["liveReserves"] = {
            "enabled": True,
            "ok": False,
            "poolAccount": pool_account,
            "detail": e.detail,
            "fallback": "route_registry_snapshot",
        }
        return out
    except Exception as e:
        out["liveReserves"] = {
            "enabled": True,
            "ok": False,
            "poolAccount": pool_account,
            "error": type(e).__name__,
            "message": str(e),
            "fallback": "route_registry_snapshot",
        }
        return out

def _hydration_manual_pool_config(
    *,
    base: str,
    quote: str,
    base_meta: Dict[str, Any],
    quote_meta: Dict[str, Any],
    db: Optional[Session] = None,
) -> Optional[Dict[str, Any]]:
    """Return a manual XYK quote config for custom Hydration pools.

    Lookup order:
      1. DB-backed hydration_route_registry rows.
      2. UTT_HYDRATION_MANUAL_POOL_PRICES_JSON env fallback.
      3. Built-in temporary UTTT-HDX bootstrap fallback.

    This lets new manual Hydration pairs be added without editing this router
    file again.  Normal SDK-supported pairs should continue using route_mode=auto
    or route_mode=sdk with no manual route row.
    """
    if not _HYDRATION_ENABLE_MANUAL_POOL_FALLBACK:
        return None

    pair = f"{str(base or '').upper()}-{str(quote or '').upper()}"
    reverse_pair = f"{str(quote or '').upper()}-{str(base or '').upper()}"

    cfg: Optional[Dict[str, Any]] = _hydration_route_registry_manual_config(db=db, base=base, quote=quote)
    source = "db:hydration_route_registry" if cfg is not None else "env:UTT_HYDRATION_MANUAL_POOL_PRICES_JSON"

    if cfg is None:
        try:
            raw = _json_map(_HYDRATION_MANUAL_POOL_PRICES_JSON)
            got = raw.get(pair)
            reverse = False
            if got is None:
                got = raw.get(reverse_pair)
                reverse = got is not None
            if isinstance(got, dict):
                cfg = dict(got)
                if reverse:
                    # Env was supplied in reverse orientation; invert reserves/price.
                    br = _float_or_none(cfg.get("baseReserve"))
                    qr = _float_or_none(cfg.get("quoteReserve"))
                    px = _float_or_none(cfg.get("price"))
                    if br is not None and qr is not None:
                        cfg["baseReserve"], cfg["quoteReserve"] = qr, br
                    elif px is not None:
                        cfg["price"] = 1.0 / px
                    cfg["sourcePair"] = reverse_pair
                    cfg["source"] = str(cfg.get("source") or source) + ":reversed"
            elif isinstance(got, (int, float, str)):
                px = _float_or_none(got)
                if px is not None:
                    cfg = {"price": px}
        except Exception:
            cfg = None

    # Current UTTT-HDX pool snapshot from Hydration UI / LP position:
    #   1,000,000 UTTT | 832.45 HDX
    #   1 UTTT ~= 0.00083245 HDX
    # This fallback is temporary until we replace it with direct on-chain pool
    # reserve discovery/manual swap builder support.
    is_uttt_hdx = (
        str(base or "").upper() == "UTTT"
        and str(quote or "").upper() == "HDX"
        and _is_asset_meta_id(base_meta, "1001331")
        and (_is_asset_meta_id(quote_meta, "native") or _is_asset_meta_id(quote_meta, _HYDRATION_NATIVE_ASSET_ID))
    )
    is_hdx_uttt = (
        str(base or "").upper() == "HDX"
        and str(quote or "").upper() == "UTTT"
        and (_is_asset_meta_id(base_meta, "native") or _is_asset_meta_id(base_meta, _HYDRATION_NATIVE_ASSET_ID))
        and _is_asset_meta_id(quote_meta, "1001331")
    )
    if cfg is None and is_uttt_hdx:
        cfg = {
            "baseReserve": 1_000_000.0,
            "quoteReserve": 832.45,
            "feeBps": 30,
            "pool": "XYK",
            "source": "hydration_ui_lp_snapshot",
            "note": "Temporary manual quote fallback for UTTT-HDX while sdk-next rejects Hydration asset 1001331.",
        }
        source = "hydration_ui_lp_snapshot"
    elif cfg is None and is_hdx_uttt:
        cfg = {
            "baseReserve": 832.45,
            "quoteReserve": 1_000_000.0,
            "feeBps": 30,
            "pool": "XYK",
            "source": "hydration_ui_lp_snapshot_reversed",
            "note": "Temporary manual quote fallback for HDX-UTTT while sdk-next rejects Hydration asset 1001331.",
        }
        source = "hydration_ui_lp_snapshot_reversed"

    if not isinstance(cfg, dict):
        return None

    base_reserve = _float_or_none(cfg.get("baseReserve"))
    quote_reserve = _float_or_none(cfg.get("quoteReserve"))
    price = _float_or_none(cfg.get("price"))
    if (base_reserve is None or quote_reserve is None) and price is not None:
        base_reserve = 1_000_000.0
        quote_reserve = base_reserve * price
    if base_reserve is None or quote_reserve is None:
        return None

    fee_bps = _float_or_none(cfg.get("feeBps"))
    if fee_bps is None:
        fee_bps = 30.0
    fee_rate = max(0.0, min(float(fee_bps) / 10_000.0, 0.25))
    spot_price = quote_reserve / base_reserve if base_reserve > 0 else None
    if spot_price is None or spot_price <= 0:
        return None

    route = cfg.get("route")
    if not isinstance(route, list):
        route = [{
            "pool": str(cfg.get("pool") or "XYK"),
            "assetIn": _hydration_sdk_asset_id(base_meta),
            "assetOut": _hydration_sdk_asset_id(quote_meta),
        }]

    out = dict(cfg)
    out.update({
        "pair": pair,
        "baseReserve": float(base_reserve),
        "quoteReserve": float(quote_reserve),
        "feeBps": float(fee_bps),
        "feeRate": float(fee_rate),
        "spotPrice": float(spot_price),
        "inversePrice": float(1.0 / spot_price) if spot_price > 0 else None,
        "source": str(cfg.get("source") or source),
        "pool": str(cfg.get("pool") or "XYK"),
        "route": route,
        "poolAccount": _hydration_pool_account_from_cfg(cfg),
    })
    return out

def _hydration_manual_level_sizes(reserve: float, depth: int) -> List[float]:
    n = max(1, min(int(depth), 10))
    reserve = float(reserve or 0.0)
    if reserve <= 0:
        return [1.0][:n]

    # Human-friendly sizes for the current UTTT-HDX pool.  Keep them tiny so the
    # displayed ladder approximates the pool ratio instead of moving the pool.
    preferred = [1, 2, 5, 10, 25, 50, 100, 250, 500, 1000]
    vals = [float(v) for v in preferred if float(v) < reserve * 0.05]
    if len(vals) >= n:
        return vals[:n]

    # Fallback for smaller reserve orientations.
    vals = []
    for frac in [0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.02, 0.03, 0.05]:
        v = reserve * frac
        if v > 0:
            vals.append(v)
    return vals[:n]


def _hydration_manual_xyk_levels(
    *,
    base_reserve: float,
    quote_reserve: float,
    fee_rate: float,
    depth: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Build a small synthetic ladder from XYK reserves.

    Price convention matches the rest of UTT: quote per base.
    bids = sell BASE into pool for QUOTE; asks = spend QUOTE to buy BASE.
    """
    x = float(base_reserve)
    y = float(quote_reserve)
    f = max(0.0, min(float(fee_rate), 0.25))
    eff = max(1.0 - f, 1e-12)
    k = x * y

    bids: List[Dict[str, Any]] = []
    asks: List[Dict[str, Any]] = []

    for base_in in _hydration_manual_level_sizes(x, depth):
        dx_eff = float(base_in) * eff
        if dx_eff <= 0:
            continue
        out_quote = y - (k / (x + dx_eff))
        if out_quote > 0:
            bids.append({
                "price": out_quote / float(base_in),
                "size": float(base_in),
                "outputSize": out_quote,
            })

    for base_out in _hydration_manual_level_sizes(x, depth):
        if float(base_out) <= 0 or float(base_out) >= x:
            continue
        quote_in = (y * float(base_out)) / ((x - float(base_out)) * eff)
        if quote_in > 0:
            asks.append({
                "price": quote_in / float(base_out),
                "size": float(base_out),
                "inputSize": quote_in,
            })

    return bids, asks


def _hydration_manual_pool_orderbook_response(
    *,
    symbol: str,
    base: str,
    quote: str,
    base_meta: Dict[str, Any],
    quote_meta: Dict[str, Any],
    depth: int,
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    n = max(1, min(int(depth), 10))
    bids, asks = _hydration_manual_xyk_levels(
        base_reserve=float(cfg["baseReserve"]),
        quote_reserve=float(cfg["quoteReserve"]),
        fee_rate=float(cfg.get("feeRate") or 0.0),
        depth=n,
    )
    asks.sort(key=lambda x: float(x.get("price") or 0.0))
    bids.sort(key=lambda x: -float(x.get("price") or 0.0))
    price_decimals = _suggest_price_decimals(asks + bids, int(quote_meta.get("decimals") or 0))
    size_decimals = min(int(base_meta.get("decimals") or 0), 8)

    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "router": "manual_xyk_pool_fallback",
        "manualFallback": True,
        "manualFallbackReason": cfg.get("note") or "Manual Hydration XYK route is configured for this pair.",
        "rawSymbol": symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base,
        "quote": quote,
        "baseAssetId": base_meta.get("assetId"),
        "quoteAssetId": quote_meta.get("assetId"),
        "baseDecimals": int(base_meta.get("decimals") or 0),
        "quoteDecimals": int(quote_meta.get("decimals") or 0),
        "baseMeta": base_meta,
        "quoteMeta": quote_meta,
        "priceDecimals": price_decimals,
        "displayPriceDecimals": max(1, min(price_decimals, 8)),
        "sizeDecimals": size_decimals,
        "pool": {
            "type": "xyk_manual_snapshot",
            "pool": cfg.get("pool") or "XYK",
            "route": cfg.get("route"),
            "routeRegistryId": cfg.get("routeRegistryId"),
            "routeRegistrySymbol": cfg.get("routeRegistrySymbol"),
            "poolAccount": cfg.get("poolAccount"),
            "source": cfg.get("source"),
            "baseReserve": cfg.get("baseReserve"),
            "quoteReserve": cfg.get("quoteReserve"),
            "baseReserveSnapshot": cfg.get("baseReserveSnapshot"),
            "quoteReserveSnapshot": cfg.get("quoteReserveSnapshot"),
            "liveReserves": cfg.get("liveReserves"),
            "spotPrice": cfg.get("spotPrice"),
            "inversePrice": cfg.get("inversePrice"),
            "feeBps": cfg.get("feeBps"),
            "note": cfg.get("note"),
        },
        "orderbookConfig": {
            "requestedDepth": int(depth),
            "sampleDepth": n,
            "source": "manual_xyk_pool_fallback",
        },
        "bids": bids,
        "asks": asks,
        "sampleErrors": [],
    }



def _decimal_from_any(value: Any, default: str = "0") -> Decimal:
    try:
        return Decimal(str(value))
    except Exception:
        return Decimal(default)


def _atomic_to_decimal_ui(amount_atomic: int, decimals: int) -> Decimal:
    return Decimal(int(amount_atomic)) / (Decimal(10) ** int(decimals))


def _decimal_ui_to_atomic_floor(value: Decimal, decimals: int) -> int:
    return int((value * (Decimal(10) ** int(decimals))).to_integral_value(rounding=ROUND_FLOOR))


def _decimal_ui_to_atomic_ceil(value: Decimal, decimals: int) -> int:
    return int((value * (Decimal(10) ** int(decimals))).to_integral_value(rounding=ROUND_CEILING))


def _hydration_manual_custom_swap_plan(
    *,
    base: str,
    quote: str,
    side: str,
    amount_ui: float,
    amount_mode: str,
    slippage_bps: int,
    base_meta: Dict[str, Any],
    quote_meta: Dict[str, Any],
    cfg: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Build a manual Hydration Router buy/sell call plan for configured XYK pairs.

    The returned object is consumed by hydration_quote.mjs / hydration_sidecar.mjs,
    which use PAPI to encode the Router call without sdk.api.router.getBest*.
    """
    if not isinstance(cfg, dict):
        return None

    side_norm = str(side or "").strip().lower()
    mode = str(amount_mode or "").strip().lower()
    if side_norm not in {"buy", "sell"} or mode not in {"exact_in", "exact_out"}:
        return None

    # UTT UI semantics for BASE-QUOTE:
    #   SELL = exact input BASE -> QUOTE, Hydration router.sell
    #   BUY  = exact output BASE paid with QUOTE, Hydration router.buy
    if side_norm == "buy":
        method = "buy"
        asset_in_meta = quote_meta
        asset_out_meta = base_meta
        required_mode = "exact_out"
    else:
        method = "sell"
        asset_in_meta = base_meta
        asset_out_meta = quote_meta
        required_mode = "exact_in"
    if mode != required_mode:
        return None

    asset_in_id = _hydration_sdk_asset_id(asset_in_meta)
    asset_out_id = _hydration_sdk_asset_id(asset_out_meta)
    asset_in_symbol = str(asset_in_meta.get("symbol") or "").upper()
    asset_out_symbol = str(asset_out_meta.get("symbol") or "").upper()
    base_reserve = _decimal_from_any(cfg.get("baseReserve"))
    quote_reserve = _decimal_from_any(cfg.get("quoteReserve"))
    if base_reserve <= 0 or quote_reserve <= 0:
        return None

    base_is_input = _asset_id_eq(asset_in_meta.get("assetId"), base_meta.get("assetId"))
    in_reserve = base_reserve if base_is_input else quote_reserve
    out_reserve = quote_reserve if base_is_input else base_reserve
    fee_rate = _decimal_from_any(cfg.get("feeRate"), "0")
    if fee_rate < 0:
        fee_rate = Decimal("0")
    if fee_rate >= Decimal("0.25"):
        fee_rate = Decimal("0.25")
    eff = Decimal("1") - fee_rate
    if eff <= 0:
        return None
    slippage_rate = Decimal(int(slippage_bps)) / Decimal(10_000)

    in_decimals = int(asset_in_meta.get("decimals") or 0)
    out_decimals = int(asset_out_meta.get("decimals") or 0)
    cfg_route = cfg.get("route")
    route = _hydration_manual_route_for_direction(
        cfg_route=cfg_route,
        asset_in_id=int(asset_in_id),
        asset_out_id=int(asset_out_id),
        pool=str(cfg.get("pool") or "XYK"),
    )

    plan: Dict[str, Any] = {
        "enabled": True,
        "provider": "manual_papi_router",
        "reason": cfg.get("note") or "Manual Hydration XYK route config is being used for quote/tx building.",
        "method": method,
        "assetInId": int(asset_in_id),
        "assetOutId": int(asset_out_id),
        "assetInSymbol": asset_in_symbol,
        "assetOutSymbol": asset_out_symbol,
        "route": route,
        "pool": "XYK",
        "poolSource": cfg.get("source"),
        "routeRegistryId": cfg.get("routeRegistryId"),
        "routeRegistrySymbol": cfg.get("routeRegistrySymbol"),
        "baseReserve": str(base_reserve),
        "quoteReserve": str(quote_reserve),
        "feeBps": cfg.get("feeBps"),
        "feeRate": str(fee_rate),
        "slippageBps": int(slippage_bps),
    }

    if method == "sell":
        amount_in_atomic = _ui_to_atomic(float(amount_ui), in_decimals)
        amount_in_ui_dec = _atomic_to_decimal_ui(amount_in_atomic, in_decimals)
        amount_in_eff = amount_in_ui_dec * eff
        amount_out_ui_dec = out_reserve - ((in_reserve * out_reserve) / (in_reserve + amount_in_eff))
        min_amount_out_ui_dec = amount_out_ui_dec * (Decimal("1") - slippage_rate)
        min_amount_out_atomic = _decimal_ui_to_atomic_floor(min_amount_out_ui_dec, out_decimals)
        if min_amount_out_atomic <= 0:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "hydration_manual_swap_min_out_too_small",
                    "message": "Manual XYK fallback produced a zero min_amount_out after decimals/slippage. Use a larger test amount.",
                    "amount": amount_ui,
                    "assetIn": asset_in_meta,
                    "assetOut": asset_out_meta,
                },
            )
        plan.update({
            "amountMode": "exact_in",
            "amountInAtomic": str(amount_in_atomic),
            "amountInUi": float(amount_in_ui_dec),
            "estimatedAmountOutAtomic": str(_decimal_ui_to_atomic_floor(amount_out_ui_dec, out_decimals)),
            "estimatedAmountOutUi": float(amount_out_ui_dec),
            "minAmountOutAtomic": str(min_amount_out_atomic),
            "minAmountOutUi": float(_atomic_to_decimal_ui(min_amount_out_atomic, out_decimals)),
        })
    else:
        amount_out_atomic = _ui_to_atomic(float(amount_ui), out_decimals)
        amount_out_ui_dec = _atomic_to_decimal_ui(amount_out_atomic, out_decimals)
        if amount_out_ui_dec >= out_reserve:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "hydration_manual_swap_output_exceeds_pool",
                    "message": "Requested exact output is too large for the manual XYK pool fallback reserves.",
                    "amount": amount_ui,
                    "assetIn": asset_in_meta,
                    "assetOut": asset_out_meta,
                    "outReserve": str(out_reserve),
                },
            )
        amount_in_ui_dec = (in_reserve * amount_out_ui_dec) / ((out_reserve - amount_out_ui_dec) * eff)
        max_amount_in_ui_dec = amount_in_ui_dec * (Decimal("1") + slippage_rate)
        max_amount_in_atomic = _decimal_ui_to_atomic_ceil(max_amount_in_ui_dec, in_decimals)
        if max_amount_in_atomic <= 0:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "hydration_manual_swap_max_in_too_small",
                    "message": "Manual XYK fallback produced a zero max_amount_in after decimals/slippage. Use a larger test amount.",
                    "amount": amount_ui,
                    "assetIn": asset_in_meta,
                    "assetOut": asset_out_meta,
                },
            )
        plan.update({
            "amountMode": "exact_out",
            "amountOutAtomic": str(amount_out_atomic),
            "amountOutUi": float(amount_out_ui_dec),
            "estimatedAmountInAtomic": str(_decimal_ui_to_atomic_ceil(amount_in_ui_dec, in_decimals)),
            "estimatedAmountInUi": float(amount_in_ui_dec),
            "maxAmountInAtomic": str(max_amount_in_atomic),
            "maxAmountInUi": float(_atomic_to_decimal_ui(max_amount_in_atomic, in_decimals)),
        })

    return plan


def _is_placeholder_asset_id(v: Any) -> bool:
    s = str(v or "").strip()
    return (not s) or ("<" in s) or (">" in s)


def _registry_asset_id(row: TokenRegistry) -> str:
    # TokenRegistry currently uses address for mint/contract-like identifiers.
    # Keep a few fallbacks so this router remains compatible if the registry model grows.
    for attr in ("asset_id", "address", "contract_address", "mint", "mint_address"):
        if hasattr(row, attr):
            v = getattr(row, attr, None)
            if v is not None and str(v).strip():
                return str(v).strip()
    return ""


def _db_lookup_hydration_asset(db: Optional[Session], symbol: str) -> Optional[TokenRegistry]:
    if db is None:
        return None
    sym = (symbol or "").strip().upper()
    if not sym:
        return None

    try:
        base_q = db.query(TokenRegistry).filter(TokenRegistry.symbol == sym)

        # Preferred exact UTT convention.
        row = (
            base_q
            .filter(TokenRegistry.chain == "polkadot")
            .filter(TokenRegistry.venue == "polkadot_hydration")
            .first()
        )
        if row is not None:
            return row

        # Tolerant aliases for early local rows.
        row = (
            base_q
            .filter(TokenRegistry.chain.in_(["polkadot", "hydration"]))
            .filter(TokenRegistry.venue.in_(["polkadot_hydration", "hydration"]))
            .first()
        )
        if row is not None:
            return row

        # Global Polkadot/Hydration registry rows are allowed as fallback.
        row = (
            base_q
            .filter(TokenRegistry.chain.in_(["polkadot", "hydration"]))
            .filter(TokenRegistry.venue.is_(None))
            .first()
        )
        if row is not None:
            return row

        row = (
            base_q
            .filter(TokenRegistry.chain.in_(["polkadot", "hydration"]))
            .filter(TokenRegistry.venue == "")
            .first()
        )
        return row
    except Exception:
        return None


def _resolve_asset(asset: str, db: Optional[Session] = None) -> Dict[str, Any]:
    sym = (asset or "").strip().upper()
    if not sym:
        raise HTTPException(status_code=422, detail="Empty asset")

    row = _db_lookup_hydration_asset(db, sym)
    if row is not None:
        asset_id = _registry_asset_id(row)
        dec_raw = getattr(row, "decimals", None)
        if _is_placeholder_asset_id(asset_id) or dec_raw is None:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "registry_hydration_asset_incomplete",
                    "symbol": sym,
                    "message": "Token Registry row found, but Hydration asset ID/address and decimals must both be set before DEX routing can use it.",
                    "chain": getattr(row, "chain", None),
                    "venue": getattr(row, "venue", None),
                    "assetId": asset_id or None,
                    "decimals": dec_raw,
                },
            )

        asset_id_norm = str(asset_id).strip()
        native = asset_id_norm.lower() == "native"
        return {
            "symbol": sym,
            "assetId": asset_id_norm,
            "decimals": int(dec_raw),
            "native": bool(native),
            "configured": True,
            "source": "token_registry",
            "registry": {
                "chain": getattr(row, "chain", None),
                "venue": getattr(row, "venue", None),
            },
        }

    ids = _json_map(_HYDRATION_ASSET_IDS_JSON)
    decimals = _json_map(_HYDRATION_DECIMALS_JSON)

    if sym == _HYDRATION_NATIVE_SYMBOL:
        asset_id = str(ids.get(sym, "native")).strip()
        return {
            "symbol": sym,
            "assetId": asset_id,
            "decimals": int(decimals.get(sym, _HYDRATION_NATIVE_DECIMALS) or _HYDRATION_NATIVE_DECIMALS),
            "native": asset_id.lower() == "native",
            "configured": True,
            "source": "native_fallback" if sym not in ids else "env",
        }

    if sym in ids:
        dec_raw = decimals.get(sym)
        asset_id = str(ids.get(sym))
        if _is_placeholder_asset_id(asset_id) or dec_raw is None:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "env_hydration_asset_incomplete",
                    "symbol": sym,
                    "message": "Env fallback found, but Hydration asset ID and decimals must both be real values. Prefer Token Registry for this asset.",
                    "assetId": asset_id,
                    "decimals": dec_raw,
                },
            )
        return {
            "symbol": sym,
            "assetId": asset_id,
            "decimals": int(dec_raw),
            "native": False,
            "configured": True,
            "source": "env",
        }

    raise HTTPException(
        status_code=422,
        detail={
            "error": "unknown_hydration_asset",
            "symbol": sym,
            "message": "Add this symbol in Token Registry with chain=hydration, venue=hydration, address=<Hydration asset ID>, and decimals. Native HDX should use address=native.",
        },
    )


def _twox128(data: bytes) -> bytes:
    try:
        import xxhash  # type: ignore

        h1 = xxhash.xxh64(data, seed=0).intdigest().to_bytes(8, "little")
        h2 = xxhash.xxh64(data, seed=1).intdigest().to_bytes(8, "little")
        return h1 + h2
    except Exception:
        # This fallback is intentionally not used for live storage-key generation.
        # Python's stdlib has no xxhash implementation; install xxhash for balances.
        raise HTTPException(
            status_code=503,
            detail={
                "error": "missing_xxhash_dependency",
                "message": "Install xxhash in the backend venv to enable Hydration native balance storage-key generation: pip install xxhash",
            },
        )


def _b58decode_raw(s: str) -> bytes:
    v = (s or "").strip()
    if not v:
        raise ValueError("empty base58")
    num = 0
    for ch in v:
        if ch not in _B58_IDX:
            raise ValueError(f"invalid base58 character: {ch!r}")
        num = num * 58 + _B58_IDX[ch]
    full = num.to_bytes((num.bit_length() + 7) // 8, "big") if num > 0 else b""
    pad = 0
    for ch in v:
        if ch == "1":
            pad += 1
        else:
            break
    return (b"\x00" * pad) + full


def _ss58_account_id32(address: str) -> bytes:
    raw = _b58decode_raw(address)
    if len(raw) not in (35, 36, 37, 38):
        raise HTTPException(status_code=422, detail={"error": "invalid_ss58_length", "length": len(raw)})

    first = raw[0]
    prefix_len = 1 if first < 64 else 2
    account = raw[prefix_len:prefix_len + 32]
    if len(account) != 32:
        raise HTTPException(status_code=422, detail={"error": "invalid_ss58_account_id_length"})
    return account


def _system_account_storage_key(address: str) -> str:
    account = _ss58_account_id32(address)
    key = _twox128(b"System") + _twox128(b"Account") + hashlib.blake2b(account, digest_size=16).digest() + account
    return "0x" + key.hex()


def _twox64_concat(data: bytes) -> bytes:
    try:
        import xxhash  # type: ignore

        return xxhash.xxh64(data, seed=0).intdigest().to_bytes(8, "little") + data
    except Exception:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "missing_xxhash_dependency",
                "message": "Install xxhash in the backend venv to enable Hydration token balance storage-key generation: pip install xxhash",
            },
        )


def _tokens_account_storage_key(address: str, asset_id: int) -> str:
    # ORML Tokens::Accounts is a double map:
    #   Blake2_128Concat(AccountId32) -> Twox64Concat(CurrencyId/AssetId)
    # Hydration AssetId values used by the router are u32 SCALE encoded here.
    account = _ss58_account_id32(address)
    asset = _scale_u32(int(asset_id))
    key = (
        _twox128(b"Tokens")
        + _twox128(b"Accounts")
        + hashlib.blake2b(account, digest_size=16).digest()
        + account
        + _twox64_concat(asset)
    )
    return "0x" + key.hex()


def _decode_tokens_account_data(hex_value: Optional[str]) -> Dict[str, int]:
    if not hex_value:
        return {"free_atomic": 0, "reserved_atomic": 0, "frozen_atomic": 0}

    h = str(hex_value or "").removeprefix("0x")
    try:
        buf = bytes.fromhex(h)
    except Exception:
        raise HTTPException(status_code=502, detail={"error": "invalid_tokens_account_storage_hex"})

    # orml_tokens::AccountData is usually free/reserved/frozen as u128 each.
    if len(buf) < 48:
        raise HTTPException(status_code=502, detail={"error": "tokens_account_storage_too_short", "length": len(buf)})

    return {
        "free_atomic": _decode_u128_le(buf, 0),
        "reserved_atomic": _decode_u128_le(buf, 16),
        "frozen_atomic": _decode_u128_le(buf, 32),
    }


def _csv_symbols(raw: Optional[str]) -> List[str]:
    out: List[str] = []
    for part in str(raw or "").split(","):
        sym = part.strip().upper()
        if sym and sym not in out:
            out.append(sym)
    return out


def _token_balance_payload(symbol: str, meta: Dict[str, Any], decoded: Dict[str, int], storage_key: str) -> Dict[str, Any]:
    decimals = int(meta.get("decimals") or 0)
    scale = 10 ** int(decimals)
    free_atomic = int(decoded.get("free_atomic") or 0)
    reserved_atomic = int(decoded.get("reserved_atomic") or 0)
    frozen_atomic = int(decoded.get("frozen_atomic") or 0)
    available_atomic = max(free_atomic - frozen_atomic, 0)

    free = free_atomic / scale
    reserved = reserved_atomic / scale
    frozen = frozen_atomic / scale
    available = available_atomic / scale
    total = (free_atomic + reserved_atomic) / scale

    return {
        "asset": symbol,
        "symbol": symbol,
        "assetId": meta.get("assetId"),
        "decimals": decimals,
        "native": False,
        "available": available,
        "transferable": available,
        "spendable": available,
        "free": free,
        "reserved": reserved,
        "frozen": frozen,
        "total": total,
        "available_atomic": str(available_atomic),
        "transferable_atomic": str(available_atomic),
        "spendable_atomic": str(available_atomic),
        "free_atomic": str(free_atomic),
        "reserved_atomic": str(reserved_atomic),
        "frozen_atomic": str(frozen_atomic),
        "availableSource": "tokens_free_minus_frozen",
        "storagePallet": "Tokens",
        "storageItem": "Accounts",
        "storageKey": storage_key,
        "source": meta.get("source"),
        "registry": meta.get("registry"),
    }


def _decode_u32_le(buf: bytes, off: int) -> int:
    return int.from_bytes(buf[off:off + 4], "little")


def _decode_u128_le(buf: bytes, off: int) -> int:
    return int.from_bytes(buf[off:off + 16], "little")


def _decode_system_account_info(hex_value: Optional[str]) -> Dict[str, int]:
    if not hex_value:
        return {
            "nonce": 0,
            "consumers": 0,
            "providers": 0,
            "sufficients": 0,
            "free_atomic": 0,
            "reserved_atomic": 0,
            "frozen_atomic": 0,
            "flags": 0,
        }

    h = str(hex_value or "").removeprefix("0x")
    try:
        buf = bytes.fromhex(h)
    except Exception:
        raise HTTPException(status_code=502, detail={"error": "invalid_account_storage_hex"})

    if len(buf) < 60:
        raise HTTPException(status_code=502, detail={"error": "account_storage_too_short", "length": len(buf)})

    # frame_system::AccountInfo<Index, pallet_balances::AccountData>
    # nonce, consumers, providers, sufficients are u32. AccountData is usually u128 fields.
    nonce = _decode_u32_le(buf, 0)
    consumers = _decode_u32_le(buf, 4)
    providers = _decode_u32_le(buf, 8)
    sufficients = _decode_u32_le(buf, 12)
    free_atomic = _decode_u128_le(buf, 16)
    reserved_atomic = _decode_u128_le(buf, 32)
    frozen_atomic = _decode_u128_le(buf, 48)
    flags = _decode_u128_le(buf, 64) if len(buf) >= 80 else 0
    return {
        "nonce": nonce,
        "consumers": consumers,
        "providers": providers,
        "sufficients": sufficients,
        "free_atomic": free_atomic,
        "reserved_atomic": reserved_atomic,
        "frozen_atomic": frozen_atomic,
        "flags": flags,
    }




def _hydrate_raw_json_safe(value: Any) -> Any:
    try:
        return json.loads(json.dumps(value, default=str))
    except Exception:
        try:
            return str(value)
        except Exception:
            return None


def _nested_get(obj: Any, *keys: str) -> Any:
    cur = obj
    for key in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _asset_id_eq(left: Any, right: Any) -> bool:
    a = str(left if left is not None else "").strip().lower()
    b = str(right if right is not None else "").strip().lower()
    if a == "native":
        a = str(_HYDRATION_NATIVE_ASSET_ID)
    if b == "native":
        b = str(_HYDRATION_NATIVE_ASSET_ID)
    return bool(a and b and a == b)


def _event_type(evt: Any) -> str:
    return str(_nested_get(evt, "event", "type") or evt.get("type") if isinstance(evt, dict) else "").strip()


def _event_value_type(evt: Any) -> str:
    return str(_nested_get(evt, "event", "value", "type") or "").strip()


def _event_value_value(evt: Any) -> Dict[str, Any]:
    val = _nested_get(evt, "event", "value", "value")
    return val if isinstance(val, dict) else {}


def _hydration_submit_events(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    events = _nested_get(payload, "submit", "submitResult", "events")
    if not isinstance(events, list):
        events = _nested_get(payload, "submitResult", "events")
    return [e for e in (events or []) if isinstance(e, dict)]


def _hydration_find_router_executed(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for evt in _hydration_submit_events(payload):
        if _event_type(evt) == "Router" and _event_value_type(evt) == "Executed":
            val = _event_value_value(evt)
            if val:
                return val
    return None


def _intish_or_none(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        s = str(value).strip()
        if not s:
            return None
        return int(s)
    except Exception:
        return None


def _hydration_router_exec_from_trade_payload(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Fallback execution summary for routed swaps without Router.Executed.

    DOT-HDX emitted a compact Router.Executed event, but broader routed pairs
    such as DOT-USDT can finalize as a sequence of Broadcast.Swapped3 / pool
    events without a top-level Router.Executed event.  The frontend submit
    payload still contains the SDK quote used to build the exact transaction,
    so use that route summary as the record_submit fallback.
    """
    tx = payload.get("tx") if isinstance(payload.get("tx"), dict) else {}
    trade = tx.get("rawTrade") if isinstance(tx.get("rawTrade"), dict) else None
    if not isinstance(trade, dict):
        trade = tx.get("trade") if isinstance(tx.get("trade"), dict) else None
    if not isinstance(trade, dict):
        trade = tx.get("human") if isinstance(tx.get("human"), dict) else None
    if not isinstance(trade, dict):
        trade = None

    if isinstance(trade, dict):
        swaps = trade.get("swaps") if isinstance(trade.get("swaps"), list) else []
        first_swap = next((s for s in swaps if isinstance(s, dict)), None)
        last_swap = next((s for s in reversed(swaps) if isinstance(s, dict)), None)

        asset_in = (
            (first_swap or {}).get("assetIn")
            or trade.get("assetIn")
            or tx.get("assetInId")
        )
        asset_out = (
            (last_swap or {}).get("assetOut")
            or trade.get("assetOut")
            or tx.get("assetOutId")
        )
        amount_in = (
            trade.get("amountIn")
            or tx.get("amountInAtomic")
        )
        amount_out = (
            trade.get("amountOut")
            or tx.get("amountOutAtomic")
            or tx.get("quotedAmountOutAtomic")
        )

        asset_in_i = _intish_or_none(asset_in)
        asset_out_i = _intish_or_none(asset_out)
        amount_in_i = _intish_or_none(amount_in)
        amount_out_i = _intish_or_none(amount_out)
        if asset_in_i is not None and asset_out_i is not None and amount_in_i is not None and amount_out_i is not None:
            return {
                "asset_in": asset_in_i,
                "asset_out": asset_out_i,
                "amount_in": str(amount_in_i),
                "amount_out": str(amount_out_i),
                "source": "tx.rawTrade",
            }

    # Last-resort on-chain fallback: Hydration may emit multiple Broadcast.Swapped3
    # events, one per route leg.  The aggregate route input is the first leg input
    # and the aggregate route output is the last leg output.
    route_legs: List[Dict[str, Any]] = []
    for evt in _hydration_submit_events(payload):
        if _event_type(evt) != "Broadcast" or _event_value_type(evt) != "Swapped3":
            continue
        val = _event_value_value(evt)
        if not isinstance(val, dict):
            continue
        stack = val.get("operation_stack") if isinstance(val.get("operation_stack"), list) else []
        has_router = any(isinstance(x, dict) and str(x.get("type") or "") == "Router" for x in stack)
        if not has_router:
            continue
        inputs = val.get("inputs") if isinstance(val.get("inputs"), list) else []
        outputs = val.get("outputs") if isinstance(val.get("outputs"), list) else []
        first_in = next((x for x in inputs if isinstance(x, dict)), None)
        last_out = next((x for x in reversed(outputs) if isinstance(x, dict)), None)
        if first_in and last_out:
            route_legs.append({"input": first_in, "output": last_out})

    if route_legs:
        first = route_legs[0]["input"]
        last = route_legs[-1]["output"]
        asset_in_i = _intish_or_none(first.get("asset"))
        asset_out_i = _intish_or_none(last.get("asset"))
        amount_in_i = _intish_or_none(first.get("amount"))
        amount_out_i = _intish_or_none(last.get("amount"))
        if asset_in_i is not None and asset_out_i is not None and amount_in_i is not None and amount_out_i is not None:
            return {
                "asset_in": asset_in_i,
                "asset_out": asset_out_i,
                "amount_in": str(amount_in_i),
                "amount_out": str(amount_out_i),
                "source": "Broadcast.Swapped3",
            }

    return None


def _hydration_find_tx_fee_native_atomic(payload: Dict[str, Any]) -> Optional[int]:
    for evt in _hydration_submit_events(payload):
        if _event_type(evt) == "TransactionPayment" and _event_value_type(evt) == "TransactionFeePaid":
            val = _event_value_value(evt)
            try:
                return int(str(val.get("actual_fee")))
            except Exception:
                return None
    return None


def _ensure_swap_orders_table(db: Session) -> None:
    """Create/expand the generic swap_orders table used by All Orders.

    Solana already uses this shape.  Hydration records into the same generic
    table so all_orders.py can reflect it without a separate model/migration.
    """
    bind = db.get_bind()
    db.execute(text("""
        CREATE TABLE IF NOT EXISTS swap_orders (
            id TEXT PRIMARY KEY,
            chain TEXT,
            venue TEXT,
            signature TEXT,
            ts TEXT,
            wallet_address TEXT,
            raw_symbol TEXT,
            resolved_symbol TEXT,
            side TEXT,
            base_mint TEXT,
            quote_mint TEXT,
            base_qty REAL,
            quote_qty REAL,
            price REAL,
            fee_quote REAL,
            status TEXT,
            raw JSON
        )
    """))
    db.commit()

    required = {
        "id": "TEXT",
        "chain": "TEXT",
        "venue": "TEXT",
        "signature": "TEXT",
        "ts": "TEXT",
        "wallet_address": "TEXT",
        "raw_symbol": "TEXT",
        "resolved_symbol": "TEXT",
        "side": "TEXT",
        "base_mint": "TEXT",
        "quote_mint": "TEXT",
        "base_qty": "REAL",
        "quote_qty": "REAL",
        "price": "REAL",
        "fee_quote": "REAL",
        "status": "TEXT",
        "raw": "JSON",
    }
    try:
        cols = {c.get("name") for c in sa_inspect(bind).get_columns("swap_orders")}
    except Exception:
        cols = set()
    for col, typ in required.items():
        if col not in cols:
            try:
                db.execute(text(f"ALTER TABLE swap_orders ADD COLUMN {col} {typ}"))
            except Exception:
                pass
    try:
        db.execute(text("CREATE INDEX IF NOT EXISTS ix_swap_orders_signature ON swap_orders(signature)"))
        db.execute(text("CREATE INDEX IF NOT EXISTS ix_swap_orders_venue_ts ON swap_orders(venue, ts)"))
        db.execute(text("CREATE INDEX IF NOT EXISTS ix_swap_orders_status_ts ON swap_orders(status, ts)"))
    except Exception:
        pass
    db.commit()


def _swap_orders_id_column_is_integer(db: Session) -> bool:
    """Return True when an existing swap_orders.id column is INTEGER-like.

    Older local DBs may already have swap_orders.id as INTEGER PRIMARY KEY from
    the Solana swap ingestion path.  SQLite rejects UUID strings in an INTEGER
    PRIMARY KEY column with ``datatype mismatch``.  Newer/empty DBs created by
    this router use TEXT ids, so record_submit must adapt to the live schema
    instead of assuming one shape.
    """
    try:
        bind = db.get_bind()
        for col in sa_inspect(bind).get_columns("swap_orders"):
            if str(col.get("name") or "").lower() == "id":
                typ = str(col.get("type") or "").upper()
                return "INT" in typ
    except Exception:
        pass
    return False


def _swap_orders_row_id_by_signature(db: Session, signature: str) -> Optional[Any]:
    try:
        row = db.execute(
            text("SELECT id FROM swap_orders WHERE signature = :signature LIMIT 1"),
            {"signature": signature},
        ).mappings().first()
        return row.get("id") if row else None
    except Exception:
        return None


def _hydration_swap_record_from_payload(payload: Dict[str, Any], db: Session) -> Dict[str, Any]:
    tx_hash = str(
        payload.get("txHash")
        or payload.get("signature")
        or _nested_get(payload, "submit", "txHash")
        or _nested_get(payload, "submit", "submitResult", "txHash")
        or ""
    ).strip()
    if not tx_hash:
        raise HTTPException(status_code=422, detail={"error": "hydration_record_missing_tx_hash"})

    submit = payload.get("submit") if isinstance(payload.get("submit"), dict) else {}
    submit_result = submit.get("submitResult") if isinstance(submit.get("submitResult"), dict) else {}
    on_chain_ok = bool(payload.get("onChainOk")) and bool(submit.get("ok", True)) and bool(submit_result.get("ok", True))
    if not on_chain_ok:
        raise HTTPException(status_code=422, detail={"error": "hydration_record_not_successful", "txHash": tx_hash})

    raw_symbol = str(payload.get("rawSymbol") or payload.get("symbol") or payload.get("resolvedSymbol") or "").strip()
    resolved_symbol = str(payload.get("resolvedSymbol") or raw_symbol).strip()
    base, quote = _parse_symbol(resolved_symbol or raw_symbol)

    base_meta = payload.get("base") if isinstance(payload.get("base"), dict) else _resolve_asset(base, db=db)
    quote_meta = payload.get("quote") if isinstance(payload.get("quote"), dict) else _resolve_asset(quote, db=db)
    side = str(payload.get("side") or "sell").strip().lower()
    if side not in {"buy", "sell"}:
        side = "sell"

    router_exec = _hydration_find_router_executed(payload)
    if not router_exec:
        router_exec = _hydration_router_exec_from_trade_payload(payload)
    if not router_exec:
        raise HTTPException(status_code=422, detail={"error": "hydration_record_missing_router_executed", "txHash": tx_hash})

    try:
        asset_in_id = router_exec.get("asset_in")
        asset_out_id = router_exec.get("asset_out")
        amount_in_atomic = int(str(router_exec.get("amount_in")))
        amount_out_atomic = int(str(router_exec.get("amount_out")))
    except Exception:
        raise HTTPException(status_code=422, detail={"error": "hydration_record_bad_router_executed", "routerExecuted": router_exec})

    base_qty: Optional[float] = None
    quote_qty: Optional[float] = None
    if _asset_id_eq(asset_in_id, base_meta.get("assetId")):
        base_qty = _atomic_to_ui(amount_in_atomic, int(base_meta.get("decimals") or 0))
    elif _asset_id_eq(asset_out_id, base_meta.get("assetId")):
        base_qty = _atomic_to_ui(amount_out_atomic, int(base_meta.get("decimals") or 0))

    if _asset_id_eq(asset_in_id, quote_meta.get("assetId")):
        quote_qty = _atomic_to_ui(amount_in_atomic, int(quote_meta.get("decimals") or 0))
    elif _asset_id_eq(asset_out_id, quote_meta.get("assetId")):
        quote_qty = _atomic_to_ui(amount_out_atomic, int(quote_meta.get("decimals") or 0))

    if base_qty is None or quote_qty is None:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "hydration_record_asset_mismatch",
                "routerExecuted": router_exec,
                "base": base_meta,
                "quote": quote_meta,
            },
        )

    price = (quote_qty / base_qty) if base_qty else None
    fee_quote: Optional[float] = None
    native_fee_atomic = _hydration_find_tx_fee_native_atomic(payload)
    if native_fee_atomic is not None and _asset_id_eq(quote_meta.get("assetId"), "native"):
        fee_quote = _atomic_to_ui(native_fee_atomic, int(quote_meta.get("decimals") or _HYDRATION_NATIVE_DECIMALS))

    block = submit_result.get("block") if isinstance(submit_result.get("block"), dict) else {}
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    wallet = str(payload.get("wallet_address") or payload.get("user_pubkey") or submit.get("address") or payload.get("userPubkey") or "").strip()

    raw_safe = _hydrate_raw_json_safe({
        "payload": payload,
        "hydration_record": {
            "routerExecuted": router_exec,
            "block": block,
            "feeNativeAtomic": str(native_fee_atomic) if native_fee_atomic is not None else None,
        },
    })

    return {
        "id": str(uuid.uuid4()),
        "chain": "hydration",
        "venue": "polkadot_hydration",
        "signature": tx_hash,
        "ts": ts,
        "wallet_address": wallet,
        "raw_symbol": raw_symbol or resolved_symbol,
        "resolved_symbol": resolved_symbol or raw_symbol,
        "side": side,
        "base_mint": str(base_meta.get("assetId") or ""),
        "quote_mint": str(quote_meta.get("assetId") or ""),
        "base_qty": float(base_qty),
        "quote_qty": float(quote_qty),
        "price": float(price) if price is not None else None,
        "fee_quote": float(fee_quote) if fee_quote is not None else None,
        "status": "confirmed",
        "raw": json.dumps(raw_safe, separators=(",", ":"), default=str),
        "block_number": block.get("number"),
        "block_hash": block.get("hash"),
    }


class HydrationRouteRegistryUpsertRequest(BaseModel):
    symbol: str = Field(..., description="BASE-QUOTE pair, e.g. UTTT-HDX")
    base_reserve: float = Field(..., gt=0, description="Manual XYK reserve for BASE, in human units")
    quote_reserve: float = Field(..., gt=0, description="Manual XYK reserve for QUOTE, in human units")
    fee_bps: float = Field(30, ge=0, le=2500, description="Pool fee in basis points")
    enabled: bool = Field(True, description="If false, the route stays saved but Auto/Manual XYK will ignore it")
    pool_type: str = Field("XYK", description="Currently only XYK is supported by the manual router builder")
    pool_account: Optional[str] = Field(None, description="Optional Hydration XYK pool account SS58 address. When set, UTT reads live pool reserves from this account instead of the saved snapshot.")
    route_json: Optional[List[Dict[str, Any]]] = Field(None, description="Optional Hydration Router route legs. Defaults to one XYK leg BASE -> QUOTE.")
    note: Optional[str] = Field(None, description="Optional operator note shown in diagnostics")


class HydrationSwapTxRequest(BaseModel):
    symbol: str = Field(..., description="BASE-QUOTE, e.g. UTTT-DOT")
    side: str = Field(..., description="buy|sell")
    amount: float = Field(..., gt=0, description="Human amount. For exact_in this is input; for exact_out this is requested output.")
    amount_mode: str = Field("exact_in", description="exact_in|exact_out. exact_out BUY/getBestBuy is disabled by default behind UTT_HYDRATION_ENABLE_EXACT_BUY after controlled testing caused sidecar timeouts.")
    quote_spend_estimate: Optional[float] = Field(None, description="Optional UI quote-spend estimate for BUY display/debug only.")
    route_mode: Optional[str] = Field(None, description="Hydration route source: auto|sdk|isolated_helper|manual_xyk. auto uses manual XYK only for configured custom pairs and managed sdk-next/sidecar for normal pairs.")
    slippage_bps: int = Field(100, ge=1, le=5000)
    user_pubkey: str = Field(..., description="Substrate/SS58 account address from SubWallet")


def _hydration_debug_rpc_url() -> str:
    try:
        return _hydration_rpc_url()
    except HTTPException:
        return _HYDRATION_RPC_URL_ENV or (_DWELLIR_HYDRATION_HTTP_BASE + "/***")
    except Exception:
        return _HYDRATION_RPC_URL_ENV or (_DWELLIR_HYDRATION_HTTP_BASE + "/***")


@router.get("/_debug")
async def polkadot_dex_debug() -> Dict[str, Any]:
    return {
        "ok": True,
        "module_file": __file__,
        "venue": "polkadot_hydration",
        "network": "hydration",
        "rpc_url": _redact_url((_HYDRATION_RPC_URL_ENV if _HYDRATION_RPC_URL_ENV and not _looks_placeholder_secret(_HYDRATION_RPC_URL_ENV) else "") or _hydration_debug_rpc_url()),
        "ws_url": _redact_url(_hydration_ws_url() or ""),
        "rpc_provider": _HYDRATION_RPC_PROVIDER,
        "has_dwellir_key": bool(_dwellir_hydration_api_key()),
        "dwellir_key_source": _dwellir_hydration_key_source(),
        "uses_profile_key": _dwellir_hydration_key_source() == "profile_db:polkadot_hydration",
        "timeout_s": _HYDRATION_TIMEOUT_S,
        "helper_path": str(_hydration_helper_path()),
        "helper_exists": _hydration_helper_path().exists(),
        "node_bin": _HYDRATION_NODE_BIN,
        "helper_timeout_s": _HYDRATION_HELPER_TIMEOUT_S,
        "helper_step_timeout_s": _HYDRATION_HELPER_STEP_TIMEOUT_S,
        "orderbook_step_timeout_s": _HYDRATION_ORDERBOOK_STEP_TIMEOUT_S,
        "orderbook_max_consecutive_errors": _HYDRATION_ORDERBOOK_MAX_CONSECUTIVE_ERRORS,
        "orderbook_force_isolated_helper": _HYDRATION_ORDERBOOK_FORCE_ISOLATED_HELPER,
        "default_route_mode": _HYDRATION_DEFAULT_ROUTE_MODE,
        "route_modes": sorted(_HYDRATION_ROUTE_MODES),
        "route_mode_note": "Auto returns manual XYK for configured custom pairs. Generic sdk/isolated_helper quote modes are explicit opt-in and are blocked while UTT_HYDRATION_ENABLE_ROUTER_QUOTES=0 to protect RPC quota.",
        "manual_pool_fallback_enabled": _HYDRATION_ENABLE_MANUAL_POOL_FALLBACK,
        "manual_pool_live_reserves_enabled": _HYDRATION_MANUAL_POOL_LIVE_RESERVES,
        "manual_pool_prices_configured": bool(str(_HYDRATION_MANUAL_POOL_PRICES_JSON or "{}").strip() not in {"", "{}"}),
        "hydration_route_registry": {
            "enabled": True,
            "table": "hydration_route_registry",
            "endpoints": [
                "/api/polkadot_dex/hydration/route_registry",
                "/api/polkadot_dex/hydration/route_registry/upsert",
                "/api/polkadot_dex/hydration/route_registry/{symbol}",
            ],
        },
        "native_sdk_asset_id_fallback": _HYDRATION_NATIVE_ASSET_ID,
        "enable_heavy_inspect": _HYDRATION_ENABLE_HEAVY_INSPECT,
        "enable_router_quotes": _HYDRATION_ENABLE_ROUTER_QUOTES,
        "enable_state_call_quotes": _HYDRATION_ENABLE_STATE_CALL_QUOTES,
        "enable_swap_tx": _HYDRATION_ENABLE_SWAP_TX,
        "enable_exact_buy": _HYDRATION_ENABLE_EXACT_BUY,
        "enable_buy_diagnostics": _HYDRATION_ENABLE_BUY_DIAGNOSTICS,
        "buy_probe_path": str(_hydration_buy_probe_path()),
        "buy_probe_exists": _hydration_buy_probe_path().exists(),
        "state_call_quote_method": _HYDRATION_STATE_CALL_QUOTE_METHOD,
        "router_quote_status": _hydration_router_quote_status(),
        "sdk_price_cache": {
            "enabled": bool(_HYDRATION_ENABLE_SDK_PRICE_CACHE),
            "ttl_s": _HYDRATION_PRICE_CACHE_TTL_S,
            "error_backoff_s": _HYDRATION_PRICE_CACHE_ERROR_BACKOFF_S,
            "step_timeout_s": _HYDRATION_PRICE_CACHE_STEP_TIMEOUT_S,
            "max_quote_errors": _HYDRATION_PRICE_CACHE_MAX_QUOTE_ERRORS,
            "strategy": _HYDRATION_PRICE_CACHE_STRATEGY,
            "spot_implementation": _HYDRATION_PRICE_CACHE_SPOT_IMPLEMENTATION,
            "sdk_fallback_enabled": bool(_HYDRATION_PRICE_CACHE_USE_SDK_FALLBACK),
            "external_usd_prices_enabled": bool(_HYDRATION_ENABLE_EXTERNAL_USD_PRICES),
            "external_usd_price_source": _HYDRATION_EXTERNAL_USD_PRICE_SOURCE,
            "external_price_ids_json": _HYDRATION_EXTERNAL_PRICE_IDS_JSON,
            "external_price_id_priority": "token_registry_then_env_then_defaults",
            "external_usd_price_timeout_s": _HYDRATION_EXTERNAL_USD_PRICE_TIMEOUT_S,
            "use_sidecar": bool(_HYDRATION_PRICE_CACHE_USE_SIDECAR),
            "autostart_sidecar": bool(_HYDRATION_PRICE_CACHE_AUTOSTART_SIDECAR),
            "force_isolated_helper": bool(_HYDRATION_PRICE_CACHE_FORCE_ISOLATED_HELPER),
            "cached_symbols": sorted(list((_hydration_usd_price_cache.get("prices") or {}).keys())),
            "updated_at_monotonic": _hydration_usd_price_cache.get("updated_at"),
            "expires_at_monotonic": _hydration_usd_price_cache.get("expires_at"),
            "error_until_monotonic": _hydration_usd_price_cache.get("error_until"),
            "last_error": _hydration_usd_price_cache.get("last_error"),
        },
        "use_sidecar": _HYDRATION_USE_SIDECAR,
        "sidecar_url": _HYDRATION_SIDECAR_URL,
        "autostart_sidecar": _hydration_effective_autostart_sidecar(),
        "autostart_sidecar_env": bool(_HYDRATION_AUTOSTART_SIDECAR),
        "autostart_sidecar_suppressed": bool(_HYDRATION_AUTOSTART_SIDECAR and not _hydration_effective_autostart_sidecar()),
        "price_cache_autostart_sidecar": _hydration_effective_autostart_sidecar(price_cache=True),
        "price_cache_autostart_sidecar_env": bool(_HYDRATION_PRICE_CACHE_AUTOSTART_SIDECAR),
        "sidecar_script_path": str(_hydration_sidecar_script_path()),
        "sidecar_script_exists": _hydration_sidecar_script_path().exists(),
        "sidecar_start_timeout_s": _HYDRATION_SIDECAR_START_TIMEOUT_S,
        "sidecar_managed_running": _sidecar_process_running(),
        "sidecar_managed_pid": getattr(_hydration_sidecar_process, "pid", None),
        "inspect_mode": _HYDRATION_INSPECT_MODE,
        "required_methods": _REQUIRED_METHODS,
    }


@router.get("/rpc_methods")
async def hydration_rpc_methods() -> Dict[str, Any]:
    result = await _rpc("rpc_methods", [])
    methods = []
    if isinstance(result, dict):
        methods = [str(m) for m in (result.get("methods") or [])]
    missing = [m for m in _REQUIRED_METHODS if m not in methods]
    return {"ok": True, "venue": "polkadot_hydration", "methods": methods, "missingRequiredMethods": missing}


@router.get("/chain_info")
async def hydration_chain_info() -> Dict[str, Any]:
    chain = await _rpc("system_chain", [])
    name = await _rpc("system_name", [])
    version = await _rpc("system_version", [])
    props = await _rpc("system_properties", [])
    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "chain": chain,
        "nodeName": name,
        "nodeVersion": version,
        "properties": props,
        "rpc_url": _redact_url(_hydration_rpc_url()),
    }


@router.get("/resolve")
async def hydration_resolve_asset(
    asset: str = Query(..., description="Ticker, e.g. HDX, DOT, UTTT"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    resolved = _resolve_asset(asset, db=db)
    return {"ok": True, "venue": "polkadot_hydration", **resolved}



def _hydration_route_registry_payload(row: Any) -> Dict[str, Any]:
    r = dict(row) if not isinstance(row, dict) else dict(row)
    try:
        route_json = json.loads(r.get("route_json") or "[]")
    except Exception:
        route_json = []
    return {
        "id": r.get("id"),
        "symbol": r.get("symbol"),
        "baseSymbol": r.get("base_symbol"),
        "quoteSymbol": r.get("quote_symbol"),
        "baseAssetId": r.get("base_asset_id"),
        "quoteAssetId": r.get("quote_asset_id"),
        "baseDecimals": r.get("base_decimals"),
        "quoteDecimals": r.get("quote_decimals"),
        "routeMode": r.get("route_mode") or "manual_xyk",
        "poolType": r.get("pool_type") or "XYK",
        "poolAccount": r.get("pool_account"),
        "enabled": bool(int(r.get("enabled") if r.get("enabled") is not None else 1)),
        "baseReserve": r.get("base_reserve"),
        "quoteReserve": r.get("quote_reserve"),
        "feeBps": r.get("fee_bps"),
        "route": route_json,
        "note": r.get("note"),
        "createdAt": r.get("created_at"),
        "updatedAt": r.get("updated_at"),
    }


@router.get("/hydration/route_registry")
async def hydration_route_registry_list(
    include_disabled: bool = Query(True, description="If false, only enabled manual routes are returned."),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    _ensure_hydration_route_registry_table(db)
    if include_disabled:
        rows = db.execute(text("SELECT * FROM hydration_route_registry ORDER BY symbol ASC")).mappings().all()
    else:
        rows = db.execute(text("SELECT * FROM hydration_route_registry WHERE COALESCE(enabled, 1) = 1 ORDER BY symbol ASC")).mappings().all()
    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "items": [_hydration_route_registry_payload(r) for r in rows],
        "count": len(rows),
        "note": "Manual XYK route rows are used only by route_mode=auto/manual_xyk. SDK-supported pairs do not need rows.",
    }


@router.post("/hydration/route_registry/upsert")
async def hydration_route_registry_upsert(
    req: HydrationRouteRegistryUpsertRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    _ensure_hydration_route_registry_table(db)
    base, quote = _parse_symbol(req.symbol)
    pool_type = str(req.pool_type or "XYK").strip().upper()
    if pool_type != "XYK":
        raise HTTPException(status_code=422, detail={"error": "unsupported_hydration_manual_pool_type", "poolType": req.pool_type, "supported": ["XYK"]})

    base_meta = _resolve_asset(base, db=db)
    quote_meta = _resolve_asset(quote, db=db)
    route = req.route_json
    if not isinstance(route, list) or not route:
        route = [{
            "pool": "XYK",
            "assetIn": _hydration_sdk_asset_id(base_meta),
            "assetOut": _hydration_sdk_asset_id(quote_meta),
        }]
    now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    existing = db.execute(
        text("SELECT id, created_at FROM hydration_route_registry WHERE UPPER(symbol) = :symbol LIMIT 1"),
        {"symbol": f"{base}-{quote}"},
    ).mappings().first()
    row_id = str(existing.get("id")) if existing and existing.get("id") else str(uuid.uuid4())
    created_at = str(existing.get("created_at")) if existing and existing.get("created_at") else now
    db.execute(
        text("""
            INSERT OR REPLACE INTO hydration_route_registry (
                id, symbol, base_symbol, quote_symbol, base_asset_id, quote_asset_id,
                base_decimals, quote_decimals, route_mode, pool_type, pool_account, enabled,
                base_reserve, quote_reserve, fee_bps, route_json, note, created_at, updated_at
            ) VALUES (
                :id, :symbol, :base_symbol, :quote_symbol, :base_asset_id, :quote_asset_id,
                :base_decimals, :quote_decimals, :route_mode, :pool_type, :pool_account, :enabled,
                :base_reserve, :quote_reserve, :fee_bps, :route_json, :note, :created_at, :updated_at
            )
        """),
        {
            "id": row_id,
            "symbol": f"{base}-{quote}",
            "base_symbol": base,
            "quote_symbol": quote,
            "base_asset_id": str(base_meta.get("assetId") or ""),
            "quote_asset_id": str(quote_meta.get("assetId") or ""),
            "base_decimals": int(base_meta.get("decimals") or 0),
            "quote_decimals": int(quote_meta.get("decimals") or 0),
            "route_mode": "manual_xyk",
            "pool_type": "XYK",
            "pool_account": str(req.pool_account or "").strip() or None,
            "enabled": 1 if req.enabled else 0,
            "base_reserve": float(req.base_reserve),
            "quote_reserve": float(req.quote_reserve),
            "fee_bps": float(req.fee_bps),
            "route_json": json.dumps(route, separators=(",", ":"), default=str),
            "note": req.note or "Manual Hydration XYK route registry entry.",
            "created_at": created_at,
            "updated_at": now,
        },
    )
    db.commit()
    row = db.execute(text("SELECT * FROM hydration_route_registry WHERE id = :id LIMIT 1"), {"id": row_id}).mappings().first()
    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "item": _hydration_route_registry_payload(row),
        "next": {
            "orderbook": f"/api/polkadot_dex/hydration/orderbook?symbol={base}-{quote}&route_mode=manual_xyk",
            "autoOrderbook": f"/api/polkadot_dex/hydration/orderbook?symbol={base}-{quote}&route_mode=auto",
        },
    }


@router.delete("/hydration/route_registry/{symbol}")
async def hydration_route_registry_delete(
    symbol: str,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    _ensure_hydration_route_registry_table(db)
    base, quote = _parse_symbol(symbol)
    res = db.execute(
        text("DELETE FROM hydration_route_registry WHERE UPPER(symbol) = :symbol"),
        {"symbol": f"{base}-{quote}"},
    )
    db.commit()
    return {"ok": True, "venue": "polkadot_hydration", "symbol": f"{base}-{quote}", "deleted": int(getattr(res, "rowcount", 0) or 0)}


@router.get("/hydration/route_registry/{symbol}/live_reserves")
async def hydration_route_registry_live_reserves(
    symbol: str,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    base, quote = _parse_symbol(symbol)
    base_meta = _resolve_asset(base, db=db)
    quote_meta = _resolve_asset(quote, db=db)
    cfg = await _hydration_manual_pool_config_with_live_reserves(
        base=base,
        quote=quote,
        base_meta=base_meta,
        quote_meta=quote_meta,
        db=db,
    )
    if not isinstance(cfg, dict):
        raise HTTPException(
            status_code=404,
            detail={
                "error": "hydration_manual_route_config_missing",
                "message": "No enabled manual XYK route exists for this Hydration pair.",
                "venue": "polkadot_hydration",
                "symbol": f"{base}-{quote}",
            },
        )
    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "symbol": f"{base}-{quote}",
        "base": base_meta,
        "quote": quote_meta,
        "poolAccount": cfg.get("poolAccount"),
        "source": cfg.get("source"),
        "baseReserve": cfg.get("baseReserve"),
        "quoteReserve": cfg.get("quoteReserve"),
        "baseReserveSnapshot": cfg.get("baseReserveSnapshot"),
        "quoteReserveSnapshot": cfg.get("quoteReserveSnapshot"),
        "spotPrice": cfg.get("spotPrice"),
        "inversePrice": cfg.get("inversePrice"),
        "liveReserves": cfg.get("liveReserves"),
    }

@router.get("/hydration/inspect")
async def hydration_inspect_pair(
    symbol: str = Query(..., description="Symbol pair, e.g. HDX-DOT (BASE-QUOTE)"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    base, quote = _parse_symbol(symbol)
    base_meta = _resolve_asset(base, db=db)
    quote_meta = _resolve_asset(quote, db=db)
    helper_result = await _run_hydration_helper(
        {
            "mode": "inspect",
            "venue": "polkadot_hydration",
            "rawSymbol": symbol,
            "resolvedSymbol": f"{base}-{quote}",
            "base": base,
            "quote": quote,
            "assetIn": _helper_asset_payload(base_meta),
            "assetOut": _helper_asset_payload(quote_meta),
            "stepTimeoutS": float(_HYDRATION_HELPER_STEP_TIMEOUT_S),
            "enableHeavyInspect": bool(_HYDRATION_ENABLE_HEAVY_INSPECT),
            "inspectMode": _HYDRATION_INSPECT_MODE,
        }
    )
    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "rawSymbol": symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base_meta,
        "quote": quote_meta,
        "helper": helper_result,
    }


@router.get("/hydration/status")
async def hydration_status(
    symbol: str = Query("HDX-DOT", description="Optional symbol pair, e.g. HDX-DOT or DOT-USDT"),
    include_sidecar_health: bool = Query(True, description="If true, performs a lightweight GET /health against the local sidecar only."),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    base, quote = _parse_symbol(symbol)
    base_meta = _resolve_asset(base, db=db)
    quote_meta = _resolve_asset(quote, db=db)
    status = _hydration_router_quote_status(symbol=f"{base}-{quote}", base_meta=base_meta, quote_meta=quote_meta)
    sidecar = await _sidecar_health() if include_sidecar_health else {"skipped": True}
    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "network": "hydration",
        "rawSymbol": symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base_meta,
        "quote": quote_meta,
        "sidecar": sidecar,
        "quoteStatus": status,
        "liveQuotesEnabled": bool(_HYDRATION_ENABLE_ROUTER_QUOTES),
        "liveQuotesAvailable": bool(status.get("available")),
        # This means unsigned swap transaction building is enabled/available.
        # Final wallet signing/submission is still handled by the frontend.
        "liveSwapsRecommended": bool(status.get("liveSwapsRecommended")),
        "swapTxEnabled": bool(status.get("swapTxEnabled")),
        "exactBuyEnabled": bool(status.get("exactBuyEnabled")),
        "buyDiagnosticsEnabled": bool(status.get("buyDiagnosticsEnabled")),
        "liveExactBuyRecommended": bool(status.get("liveExactBuyRecommended")),
    }


@router.get("/balances")
async def hydration_balances(
    address: str = Query(..., min_length=16, description="Hydration/Substrate SS58 address"),
    assets: Optional[str] = Query(None, description="Optional comma-separated non-native assets to fetch, e.g. DOT,USDT,UTTT. Defaults to UTT_HYDRATION_BALANCE_ASSETS."),
    include_zero: bool = Query(True, description="If false, omit zero non-native token balances from the tokens list."),
    with_prices: bool = Query(False, description="If true, include cached/controlled SDK USD price enrichment."),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    props = await _rpc("system_properties", [])
    symbol = _HYDRATION_NATIVE_SYMBOL
    decimals = _HYDRATION_NATIVE_DECIMALS
    try:
        token_symbol = props.get("tokenSymbol") if isinstance(props, dict) else None
        token_decimals = props.get("tokenDecimals") if isinstance(props, dict) else None
        if isinstance(token_symbol, list) and token_symbol:
            symbol = str(token_symbol[0]).upper()
        elif isinstance(token_symbol, str) and token_symbol:
            symbol = token_symbol.upper()
        if isinstance(token_decimals, list) and token_decimals:
            decimals = int(token_decimals[0])
        elif token_decimals is not None:
            decimals = int(token_decimals)
    except Exception:
        pass

    key = _system_account_storage_key(address)
    raw = await _rpc("state_getStorage", [key])
    decoded = _decode_system_account_info(raw)

    scale = 10 ** int(decimals)
    free_atomic = int(decoded["free_atomic"])
    reserved_atomic = int(decoded["reserved_atomic"])
    frozen_atomic = int(decoded["frozen_atomic"])

    # Substrate AccountData.free is not necessarily spendable/transferable;
    # staked/locked funds can still appear in free while also being represented
    # in frozen. For the trading ticket, expose a conservative native
    # available/spendable value as free - frozen, floored at zero.
    available_atomic = max(free_atomic - frozen_atomic, 0)

    free = free_atomic / scale
    reserved = reserved_atomic / scale
    frozen = frozen_atomic / scale
    available = available_atomic / scale
    total = (free_atomic + reserved_atomic) / scale

    requested_assets = _csv_symbols(assets) if assets is not None else _csv_symbols(_HYDRATION_BALANCE_ASSETS_CSV)
    tokens: List[Dict[str, Any]] = []
    token_errors: List[Dict[str, Any]] = []

    for token_symbol in requested_assets:
        if token_symbol == symbol or token_symbol == _HYDRATION_NATIVE_SYMBOL:
            continue
        try:
            meta = _resolve_asset(token_symbol, db=db)
            if bool(meta.get("native")) or str(meta.get("assetId") or "").strip().lower() == "native":
                continue
            asset_id = _hydration_sdk_asset_id(meta)
            token_key = _tokens_account_storage_key(address, asset_id)
            token_raw = await _rpc("state_getStorage", [token_key])
            token_decoded = _decode_tokens_account_data(token_raw)
            token_payload = _token_balance_payload(token_symbol, meta, token_decoded, token_key)
            if include_zero or any(int(token_decoded.get(k) or 0) > 0 for k in ("free_atomic", "reserved_atomic", "frozen_atomic")):
                tokens.append(token_payload)
        except HTTPException as e:
            token_errors.append({"symbol": token_symbol, "detail": e.detail})
        except Exception as e:
            token_errors.append({"symbol": token_symbol, "error": type(e).__name__, "message": str(e)})

    price_payload: Optional[Dict[str, Any]] = None
    if with_prices:
        price_assets = [symbol] + [str(t.get("asset") or t.get("symbol") or "").upper() for t in tokens if isinstance(t, dict)]
        price_payload = await _hydration_refresh_usd_price_cache(db=db, requested=price_assets, force_refresh=False, allow_refresh=False)
        prices = dict((price_payload or {}).get("prices_usd") or {})
        sources = dict((price_payload or {}).get("priceSources") or {})

        def _apply_price(row: Dict[str, Any]) -> Dict[str, Any]:
            sym = str(row.get("symbol") or row.get("asset") or "").strip().upper()
            px = _float_or_none(prices.get(sym))
            if px is None:
                return row
            total_qty = _float_or_none(row.get("total")) or 0.0
            row["px_usd"] = float(px)
            row["usd_price"] = float(px)
            row["total_usd"] = float(total_qty * px)
            row["usd_value"] = float(total_qty * px)
            row["usd_source_symbol"] = sources.get(sym) or "sdk_price_cache"
            return row

        tokens = [_apply_price(dict(t)) for t in tokens]

    native_payload = {
            "symbol": symbol,
            "decimals": decimals,
            "available": available,
            "transferable": available,
            "spendable": available,
            "free": free,
            "reserved": reserved,
            "frozen": frozen,
            "total": total,
            "available_atomic": str(available_atomic),
            "transferable_atomic": str(available_atomic),
            "spendable_atomic": str(available_atomic),
            "free_atomic": str(free_atomic),
            "reserved_atomic": str(reserved_atomic),
            "frozen_atomic": str(frozen_atomic),
            "availableSource": "free_minus_frozen",
            "nonce": decoded["nonce"],
            "consumers": decoded["consumers"],
            "providers": decoded["providers"],
            "sufficients": decoded["sufficients"],
            "flags": str(decoded["flags"]),
        }

    if with_prices and price_payload is not None:
        prices = dict((price_payload or {}).get("prices_usd") or {})
        sources = dict((price_payload or {}).get("priceSources") or {})
        px = _float_or_none(prices.get(str(symbol).upper()))
        if px is not None:
            native_payload["px_usd"] = float(px)
            native_payload["usd_price"] = float(px)
            native_payload["total_usd"] = float(total * px)
            native_payload["usd_value"] = float(total * px)
            native_payload["usd_source_symbol"] = sources.get(str(symbol).upper()) or "sdk_price_cache"

    response = {
        "ok": True,
        "venue": "polkadot_hydration",
        "network": "hydration",
        "address": address,
        "rpc_url": _redact_url(_hydration_rpc_url()),
        "native": native_payload,
        "tokens": tokens,
        "items": tokens,
        "balances": tokens,
        "tokenBalanceConfig": {
            "assets": requested_assets,
            "includeZero": bool(include_zero),
            "source": "query" if assets is not None else "env:UTT_HYDRATION_BALANCE_ASSETS",
            "storagePallet": "Tokens",
            "storageItem": "Accounts",
        },
        "tokenErrors": token_errors,
    }
    if price_payload is not None:
        response["prices_usd"] = price_payload.get("prices_usd")
        response["usd_prices"] = price_payload.get("usd_prices")
        response["priceSources"] = price_payload.get("priceSources")
        response["priceCache"] = price_payload.get("cache")
        response["priceStatus"] = price_payload.get("status")
        response["priceErrors"] = price_payload.get("errors")
    return response


@router.get("/hydration/prices")
async def hydration_usd_prices(
    assets: Optional[str] = Query("HDX,DOT,USDT,UTTT,HOLLAR", description="Comma-separated Hydration assets to price in USD."),
    refresh: bool = Query(False, description="If true, run a controlled SDK refresh when cache is stale. Default is cache-only so UI refreshes never block on SDK timeouts."),
    force_refresh: bool = Query(False, description="Force a controlled SDK price-cache refresh, ignoring TTL/backoff when possible."),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    requested = _hydration_price_cache_requested_symbols(assets)
    return await _hydration_refresh_usd_price_cache(
        db=db,
        requested=requested,
        force_refresh=force_refresh,
        allow_refresh=bool(refresh or force_refresh),
    )


@router.get("/hydration/runtime_api_inventory")
async def hydration_runtime_api_inventory(
    include_method_probes: bool = Query(True, description="If true, performs lightweight state_call probes for candidate runtime API method names."),
    method_candidates_csv: Optional[str] = Query(None, description="Optional comma-separated runtime API method candidates to probe."),
    probe_data: str = Query("0x", description="Raw hex payload used for method-name discovery probes. Default is empty SCALE input."),
    max_method_probes: int = Query(25, ge=0, le=50),
) -> Dict[str, Any]:
    runtime_version = await _rpc_probe("state_getRuntimeVersion", [])
    rpc_methods = await _rpc_probe("rpc_methods", [])
    core_version = await _state_call_probe_method("Core_version", "0x")
    metadata_versions = await _state_call_probe_method("Metadata_metadata_versions", "0x")

    runtime_result = runtime_version.get("result") if isinstance(runtime_version, dict) else None
    runtime_apis = _runtime_api_list_from_version(runtime_result)

    if method_candidates_csv:
        candidates = [m.strip() for m in str(method_candidates_csv or "").split(",") if m.strip()]
    else:
        candidates = _runtime_api_default_method_candidates()

    # Keep sanity probes out of the custom probe list because they are already included above.
    candidates = [m for m in candidates if m not in {"Core_version", "Metadata_metadata_versions"}]
    candidates = candidates[: max(0, int(max_method_probes))]

    method_probes: List[Dict[str, Any]] = []
    if include_method_probes and candidates:
        for method in candidates:
            method_probes.append(await _state_call_probe_method(method, probe_data))

    accepted = [p for p in method_probes if p.get("classification") == "accepted"]
    exported_nonaccepted = [
        p for p in method_probes
        if p.get("classification") in {"exported_decode_or_input_error", "exported_execution_error"}
    ]
    not_found = [p for p in method_probes if p.get("classification") == "not_found"]

    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "network": "hydration",
        "rpc_url": _redact_url(_hydration_rpc_url()),
        "runtimeVersion": runtime_version,
        "runtimeApiCount": len(runtime_apis),
        "runtimeApis": runtime_apis,
        "rpcMethods": rpc_methods,
        "sanity": {
            "Core_version": core_version,
            "Metadata_metadata_versions": metadata_versions,
        },
        "methodProbeConfig": {
            "includeMethodProbes": bool(include_method_probes),
            "probeData": _clean_hex(probe_data or "0x"),
            "maxMethodProbes": int(max_method_probes),
            "candidateCount": len(candidates),
        },
        "methodProbeSummary": {
            "acceptedCount": len(accepted),
            "exportedButNeedsSignatureCount": len(exported_nonaccepted),
            "notFoundCount": len(not_found),
            "acceptedMethods": [p.get("method") for p in accepted],
            "exportedButNeedsSignatureMethods": [p.get("method") for p in exported_nonaccepted],
        },
        "methodProbes": method_probes,
        "interpretation": {
            "accepted": "state_call returned a result for this method and payload.",
            "exported_decode_or_input_error": "Method likely exists, but the probe payload/signature is wrong.",
            "exported_execution_error": "Method may exist, but runtime execution failed with the probe payload.",
            "not_found": "Runtime did not export this method name.",
        },
    }


@router.get("/hydration/state_call_quote_probe")
async def hydration_state_call_quote_probe(
    symbol: str = Query(..., description="Symbol pair, e.g. DOT-USDT or HDX-DOT (BASE-QUOTE)"),
    amount: float = Query(1.0, gt=0, description="Human input amount to encode for the probe"),
    side: str = Query("sell", description="sell = sell BASE for QUOTE; buy = spend QUOTE to buy BASE"),
    method: Optional[str] = Query(None, description="Runtime API method, defaults to UTT_HYDRATION_STATE_CALL_QUOTE_METHOD / OmnipoolApi_quotePrice"),
    candidate: str = Query("auto", description="auto, u32_u32_u128, u32_u32_u128_order_0, u32_u32_u128_order_1, or raw"),
    raw_data: Optional[str] = Query(None, description="Optional raw hex SCALE input. If provided, candidate=raw is used."),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    if not _HYDRATION_ENABLE_STATE_CALL_QUOTES:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "hydration_state_call_quotes_disabled",
                "message": "State-call quote probing is disabled. Set UTT_HYDRATION_ENABLE_STATE_CALL_QUOTES=1 for one controlled test.",
                "safeMode": True,
                "method": _HYDRATION_STATE_CALL_QUOTE_METHOD,
            },
        )

    base, quote = _parse_symbol(symbol)
    side_norm = (side or "").strip().lower()
    if side_norm not in {"sell", "buy"}:
        raise HTTPException(status_code=422, detail={"error": "invalid_side", "side": side, "expected": "sell|buy"})

    base_meta = _resolve_asset(base, db=db)
    quote_meta = _resolve_asset(quote, db=db)

    if side_norm == "sell":
        asset_in_meta = base_meta
        asset_out_meta = quote_meta
    else:
        asset_in_meta = quote_meta
        asset_out_meta = base_meta

    asset_in_id = _hydration_sdk_asset_id(asset_in_meta)
    asset_out_id = _hydration_sdk_asset_id(asset_out_meta)
    amount_atomic = _ui_to_atomic(float(amount), int(asset_in_meta.get("decimals") or 0))
    call_method = (method or _HYDRATION_STATE_CALL_QUOTE_METHOD or "OmnipoolApi_quotePrice").strip()

    if raw_data:
        candidates = [{"name": "raw", "data": _clean_hex(raw_data), "note": "caller-provided raw SCALE input"}]
    else:
        all_candidates = _state_call_quote_candidate_payloads(asset_in_id, asset_out_id, amount_atomic)
        cand = (candidate or "auto").strip()
        if cand == "auto":
            candidates = all_candidates
        else:
            candidates = [c for c in all_candidates if c.get("name") == cand]
            if not candidates:
                raise HTTPException(
                    status_code=422,
                    detail={
                        "error": "unknown_state_call_candidate",
                        "candidate": candidate,
                        "available": ["auto"] + [c.get("name") for c in all_candidates] + ["raw"],
                    },
                )

    attempts: List[Dict[str, Any]] = []
    for c in candidates:
        params = [call_method, c["data"]]
        rpc_result = await _rpc_probe("state_call", params)
        attempt = {
            "candidate": c.get("name"),
            "note": c.get("note"),
            "method": call_method,
            "data": c.get("data"),
            "params": params,
            "rpc": rpc_result,
        }
        if rpc_result.get("ok") and rpc_result.get("result") is not None:
            attempt["decodeProbe"] = _decode_state_call_probe_result(rpc_result.get("result"), int(asset_out_meta.get("decimals") or 0))
        attempts.append(attempt)

    accepted = [a for a in attempts if a.get("rpc", {}).get("ok") and a.get("rpc", {}).get("result") is not None]

    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "network": "hydration",
        "rawSymbol": symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "side": side_norm,
        "amountUi": float(amount),
        "amountAtomic": str(amount_atomic),
        "method": call_method,
        "base": base_meta,
        "quote": quote_meta,
        "assetIn": asset_in_meta,
        "assetOut": asset_out_meta,
        "assetInId": asset_in_id,
        "assetOutId": asset_out_id,
        "candidate": candidate,
        "acceptedCandidateCount": len(accepted),
        "attempts": attempts,
        "warning": "Diagnostic only. Do not enable orderbook/swaps from this result until the accepted input/output SCALE shape is confirmed.",
    }


@router.get("/hydration/getbestbuy_probe")
async def hydration_getbestbuy_probe(
    symbol: str = Query(..., description="Symbol pair, e.g. DOT-HDX (BASE-QUOTE). BUY BASE means spend QUOTE to receive BASE."),
    amount: float = Query(0.1, gt=0, description="Target output amount in human units. For candidate=buy_base_with_quote this is BASE amount."),
    candidate: str = Query("buy_base_with_quote", description="buy_base_with_quote, buy_quote_with_base, or auto"),
    step_timeout_s: float = Query(12.0, ge=3.0, le=60.0, description="Per-stage Node-side timeout. Keep short so hangs do not poison the live sidecar."),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Isolated getBestBuy diagnostic.

    This endpoint runs in a short-lived Node process, not the persistent sidecar,
    so an exact-buy hang cannot starve live SELL quotes/orderbook.  It does not
    build, sign, submit, or record a swap.
    """
    if not _HYDRATION_ENABLE_BUY_DIAGNOSTICS:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "hydration_buy_diagnostics_disabled",
                "message": "Set UTT_HYDRATION_ENABLE_BUY_DIAGNOSTICS=1 for one controlled getBestBuy diagnostic test. Keep UTT_HYDRATION_ENABLE_EXACT_BUY=0 until this is stable.",
                "safeMode": True,
            },
        )

    ws_url = _hydration_ws_url()
    if not ws_url:
        raise HTTPException(status_code=503, detail={"error": "hydration_ws_not_configured"})

    base, quote = _parse_symbol(symbol)
    base_meta = _resolve_asset(base, db=db)
    quote_meta = _resolve_asset(quote, db=db)
    cand = str(candidate or "buy_base_with_quote").strip().lower()
    if cand not in {"buy_base_with_quote", "buy_quote_with_base", "auto"}:
        raise HTTPException(status_code=422, detail={"error": "invalid_getbestbuy_candidate", "candidate": candidate, "expected": "buy_base_with_quote|buy_quote_with_base|auto"})

    attempts: List[Dict[str, Any]] = []
    if cand in {"buy_base_with_quote", "auto"}:
        # UI BUY semantics for BASE-QUOTE: spend QUOTE to receive exact BASE.
        attempts.append({
            "name": "buy_base_with_quote",
            "assetInSymbol": quote,
            "assetOutSymbol": base,
            "assetInId": _hydration_sdk_asset_id(quote_meta),
            "assetOutId": _hydration_sdk_asset_id(base_meta),
            "amountOutUi": float(amount),
            "amountOutAtomic": str(_ui_to_atomic(float(amount), int(base_meta.get("decimals") or 0))),
            "meaning": f"Spend {quote} to receive exactly {amount} {base}.",
        })
    if cand in {"buy_quote_with_base", "auto"}:
        # Reverse diagnostic only.  This helps catch asset-order assumptions.
        attempts.append({
            "name": "buy_quote_with_base",
            "assetInSymbol": base,
            "assetOutSymbol": quote,
            "assetInId": _hydration_sdk_asset_id(base_meta),
            "assetOutId": _hydration_sdk_asset_id(quote_meta),
            "amountOutUi": float(amount),
            "amountOutAtomic": str(_ui_to_atomic(float(amount), int(quote_meta.get("decimals") or 0))),
            "meaning": f"Spend {base} to receive exactly {amount} {quote}. Reverse diagnostic only.",
        })

    payload = {
        "mode": "getbestbuy_probe",
        "venue": "polkadot_hydration",
        "rawSymbol": symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base_meta,
        "quote": quote_meta,
        "wsUrl": ws_url,
        "stepTimeoutS": float(step_timeout_s),
        "attempts": attempts,
    }
    result = await _run_hydration_buy_probe(payload, timeout_s=float(step_timeout_s) * max(1, len(attempts)))
    return {
        "ok": bool(result.get("ok")),
        "venue": "polkadot_hydration",
        "network": "hydration",
        "rawSymbol": symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base_meta,
        "quote": quote_meta,
        "candidate": cand,
        "amountOutUi": float(amount),
        "diagnosticOnly": True,
        "liveExactBuyEnabled": bool(_HYDRATION_ENABLE_EXACT_BUY),
        "warning": "Do not enable UI BUY from this result alone. This probe only isolates getBestBuy behavior outside the persistent sidecar.",
        "probe": result,
    }


@router.get("/hydration/orderbook")
async def hydration_pseudo_orderbook(
    symbol: str = Query(..., description="Symbol pair, e.g. UTTT-DOT (BASE-QUOTE)"),
    depth: int = Query(10, ge=1, le=50),
    route_mode: Optional[str] = Query(None, description="Hydration quote source: auto|sdk|isolated_helper|manual_xyk. auto uses manual XYK only for configured custom pairs and managed sdk-next/sidecar for normal pairs."),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    base, quote = _parse_symbol(symbol)
    base_meta = _resolve_asset(base, db=db)
    quote_meta = _resolve_asset(quote, db=db)

    route_mode_norm = _hydration_route_mode(route_mode)
    manual_cfg = await _hydration_manual_pool_config_with_live_reserves(base=base, quote=quote, base_meta=base_meta, quote_meta=quote_meta, db=db)
    if route_mode_norm == "manual_xyk" and manual_cfg is None:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "hydration_manual_route_config_missing",
                "message": "route_mode=manual_xyk was requested, but no manual XYK pool config exists for this Hydration pair.",
                "venue": "polkadot_hydration",
                "rawSymbol": symbol,
                "resolvedSymbol": f"{base}-{quote}",
                "base": base_meta,
                "quote": quote_meta,
                "routeMode": route_mode_norm,
                "nextRequired": "Add this pair to /api/polkadot_dex/hydration/route_registry/upsert, or use route_mode=auto/sdk for SDK-supported pairs.",
            },
        )
    if route_mode_norm in {"auto", "manual_xyk"} and manual_cfg is not None:
        resp = _hydration_manual_pool_orderbook_response(
            symbol=symbol,
            base=base,
            quote=quote,
            base_meta=base_meta,
            quote_meta=quote_meta,
            depth=depth,
            cfg=manual_cfg,
        )
        resp["routeMode"] = route_mode_norm
        resp["routeModeEffective"] = "manual_xyk"
        resp.setdefault("orderbookConfig", {})["routeMode"] = route_mode_norm
        resp.setdefault("orderbookConfig", {})["routeModeEffective"] = "manual_xyk"
        resp.setdefault("orderbookConfig", {})["legacyForceIsolatedHelperEnv"] = bool(_HYDRATION_ORDERBOOK_FORCE_ISOLATED_HELPER)
        return resp

    if not _HYDRATION_ENABLE_ROUTER_QUOTES:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "hydration_router_quotes_disabled",
                "message": "Hydration router quote/orderbook calls are intentionally disabled.",
                "venue": "polkadot_hydration",
                "rawSymbol": symbol,
                "resolvedSymbol": f"{base}-{quote}",
                "base": base_meta,
                "quote": quote_meta,
                "enableRouterQuotes": _HYDRATION_ENABLE_ROUTER_QUOTES,
                "quoteStatus": _hydration_router_quote_status(symbol=f"{base}-{quote}", base_meta=base_meta, quote_meta=quote_meta),
                "routeMode": route_mode_norm,
            },
        )

    n = max(1, min(int(depth), 10))
    bids: List[Dict[str, Any]] = []
    asks: List[Dict[str, Any]] = []
    sample_errors: List[Dict[str, Any]] = []
    orderbook_step_timeout_s = max(1.0, float(_HYDRATION_ORDERBOOK_STEP_TIMEOUT_S))
    max_consecutive_errors = max(1, int(_HYDRATION_ORDERBOOK_MAX_CONSECUTIVE_ERRORS))
    force_isolated_orderbook = bool(route_mode_norm == "isolated_helper")

    # Asks: buying BASE by selling QUOTE into Hydration. Price = QUOTE / BASE.
    ask_consecutive_errors = 0
    for qsz in _hydration_sample_sizes(quote, int(quote_meta.get("decimals") or 0), side="ask", depth=n):
        try:
            qt = await _hydration_quote_sell(
                raw_symbol=symbol,
                base=base,
                quote=quote,
                asset_in=quote_meta,
                asset_out=base_meta,
                amount_in_ui=float(qsz),
                step_timeout_s=orderbook_step_timeout_s,
                force_isolated=force_isolated_orderbook,
            )
            out_atomic = qt.get("amountOutAtomic")
            out_ui = qt.get("amountOutUi")
            base_ui = float(out_ui) if out_ui is not None else _atomic_to_ui(out_atomic, int(base_meta.get("decimals") or 0))
            quote_ui = float(qsz)
            if base_ui <= 0:
                raise ValueError("quote returned zero output")
            asks.append({"price": quote_ui / base_ui, "size": base_ui, "inputSize": quote_ui})
            ask_consecutive_errors = 0
        except HTTPException as e:
            ask_consecutive_errors += 1
            if len(sample_errors) < 12:
                sample_errors.append({"side": "ask", "amount_ui": qsz, "input": quote, "detail": e.detail})
            if ask_consecutive_errors >= max_consecutive_errors:
                if len(sample_errors) < 12:
                    sample_errors.append({"side": "ask", "stopped": True, "reason": "max_consecutive_quote_errors", "count": ask_consecutive_errors})
                break
        except Exception as e:
            ask_consecutive_errors += 1
            if len(sample_errors) < 12:
                sample_errors.append({"side": "ask", "amount_ui": qsz, "input": quote, "error": type(e).__name__, "message": str(e)})
            if ask_consecutive_errors >= max_consecutive_errors:
                if len(sample_errors) < 12:
                    sample_errors.append({"side": "ask", "stopped": True, "reason": "max_consecutive_quote_errors", "count": ask_consecutive_errors})
                break

    # Bids: selling BASE into Hydration for QUOTE. Price = QUOTE / BASE.
    bid_consecutive_errors = 0
    for bsz in _hydration_sample_sizes(base, int(base_meta.get("decimals") or 0), side="bid", depth=n):
        try:
            qt = await _hydration_quote_sell(
                raw_symbol=symbol,
                base=base,
                quote=quote,
                asset_in=base_meta,
                asset_out=quote_meta,
                amount_in_ui=float(bsz),
                step_timeout_s=orderbook_step_timeout_s,
                force_isolated=force_isolated_orderbook,
            )
            out_atomic = qt.get("amountOutAtomic")
            out_ui = qt.get("amountOutUi")
            quote_ui = float(out_ui) if out_ui is not None else _atomic_to_ui(out_atomic, int(quote_meta.get("decimals") or 0))
            base_ui = float(bsz)
            if quote_ui <= 0:
                raise ValueError("quote returned zero output")
            bids.append({"price": quote_ui / base_ui, "size": base_ui, "outputSize": quote_ui})
            bid_consecutive_errors = 0
        except HTTPException as e:
            bid_consecutive_errors += 1
            if len(sample_errors) < 12:
                sample_errors.append({"side": "bid", "amount_ui": bsz, "input": base, "detail": e.detail})
            if bid_consecutive_errors >= max_consecutive_errors:
                if len(sample_errors) < 12:
                    sample_errors.append({"side": "bid", "stopped": True, "reason": "max_consecutive_quote_errors", "count": bid_consecutive_errors})
                break
        except Exception as e:
            bid_consecutive_errors += 1
            if len(sample_errors) < 12:
                sample_errors.append({"side": "bid", "amount_ui": bsz, "input": base, "error": type(e).__name__, "message": str(e)})
            if bid_consecutive_errors >= max_consecutive_errors:
                if len(sample_errors) < 12:
                    sample_errors.append({"side": "bid", "stopped": True, "reason": "max_consecutive_quote_errors", "count": bid_consecutive_errors})
                break

    if not bids and not asks:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "hydration_no_quote_levels",
                "message": "No Hydration quote levels could be built from sampled helper quotes.",
                "venue": "polkadot_hydration",
                "rawSymbol": symbol,
                "resolvedSymbol": f"{base}-{quote}",
                "base": base_meta,
                "quote": quote_meta,
                "helperPath": str(_hydration_helper_path()),
                "orderbookConfig": {
                    "stepTimeoutS": orderbook_step_timeout_s,
                    "maxConsecutiveErrors": max_consecutive_errors,
                    "forceIsolatedHelper": force_isolated_orderbook,
                    "legacyForceIsolatedHelperEnv": bool(_HYDRATION_ORDERBOOK_FORCE_ISOLATED_HELPER),
                    "routeMode": route_mode_norm,
                    "routeModeEffective": "isolated_helper" if force_isolated_orderbook else "sdk",
                    "requestedDepth": int(depth),
                    "sampleDepth": n,
                },
                "sampleErrors": sample_errors,
            },
        )

    asks.sort(key=lambda x: float(x.get("price") or 0.0))
    bids.sort(key=lambda x: -float(x.get("price") or 0.0))
    price_decimals = _suggest_price_decimals(asks + bids, int(quote_meta.get("decimals") or 0))
    size_decimals = min(int(base_meta.get("decimals") or 0), 8)

    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "router": _hydration_route_mode_label(route_mode_norm, manual=False),
        "routeMode": route_mode_norm,
        "routeModeEffective": "isolated_helper" if force_isolated_orderbook else "sdk",
        "rawSymbol": symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base,
        "quote": quote,
        "baseAssetId": base_meta.get("assetId"),
        "quoteAssetId": quote_meta.get("assetId"),
        "baseDecimals": int(base_meta.get("decimals") or 0),
        "quoteDecimals": int(quote_meta.get("decimals") or 0),
        "baseMeta": base_meta,
        "quoteMeta": quote_meta,
        "priceDecimals": price_decimals,
        "displayPriceDecimals": max(1, min(price_decimals, 8)),
        "sizeDecimals": size_decimals,
        "orderbookConfig": {
            "stepTimeoutS": orderbook_step_timeout_s,
            "maxConsecutiveErrors": max_consecutive_errors,
            "forceIsolatedHelper": force_isolated_orderbook,
            "legacyForceIsolatedHelperEnv": bool(_HYDRATION_ORDERBOOK_FORCE_ISOLATED_HELPER),
            "routeMode": route_mode_norm,
            "routeModeEffective": "isolated_helper" if force_isolated_orderbook else "sdk",
            "requestedDepth": int(depth),
            "sampleDepth": n,
        },
        "bids": bids,
        "asks": asks,
        "sampleErrors": sample_errors,
    }


@router.post("/hydration/swap_tx")
async def hydration_swap_tx(req: HydrationSwapTxRequest, db: Session = Depends(get_db)) -> Dict[str, Any]:
    base, quote = _parse_symbol(req.symbol)
    side = (req.side or "").strip().lower()
    if side not in ("buy", "sell"):
        raise HTTPException(status_code=422, detail=f"Invalid side '{req.side}' (expected buy|sell)")

    base_meta = _resolve_asset(base, db=db)
    quote_meta = _resolve_asset(quote, db=db)
    quote_status = _hydration_router_quote_status(symbol=f"{base}-{quote}", base_meta=base_meta, quote_meta=quote_meta)
    route_mode_norm = _hydration_route_mode(req.route_mode)

    if not _HYDRATION_ENABLE_SWAP_TX:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "hydration_swap_tx_disabled",
                "message": "Hydration swap transaction building is disabled. Set UTT_HYDRATION_ENABLE_SWAP_TX=1 for one controlled unsigned transaction build test.",
                "venue": "polkadot_hydration",
                "rawSymbol": req.symbol,
                "resolvedSymbol": f"{base}-{quote}",
                "side": side,
                "amount": req.amount,
                "slippageBps": req.slippage_bps,
                "user_pubkey": req.user_pubkey,
                "base": base_meta,
                "quote": quote_meta,
                "quoteStatus": quote_status,
                "routeMode": route_mode_norm,
                "liveSwapsRecommended": False,
            },
        )

    # Keep exact-out BUY gated.  Isolated getBestBuy diagnostics now prove the
    # quote can succeed, but live BUY signing/submission remains opt-in behind
    # UTT_HYDRATION_ENABLE_EXACT_BUY=1 so the stable SELL path stays protected.
    if side == "buy" and not _HYDRATION_ENABLE_EXACT_BUY:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "hydration_buy_swap_disabled",
                "message": "Hydration BUY swaps are temporarily disabled while exact-buy/getBestBuy routing is isolated. SELL swaps remain available for controlled testing.",
                "venue": "polkadot_hydration",
                "rawSymbol": req.symbol,
                "resolvedSymbol": f"{base}-{quote}",
                "side": side,
                "amount": req.amount,
                "quoteSpendEstimate": req.quote_spend_estimate,
                "base": base_meta,
                "quote": quote_meta,
                "quoteStatus": quote_status,
                "routeMode": route_mode_norm,
            },
        )

    amount_mode = str(req.amount_mode or ("exact_out" if side == "buy" else "exact_in")).strip().lower()
    if amount_mode not in {"exact_in", "exact_out"}:
        raise HTTPException(status_code=422, detail={"error": "invalid_hydration_amount_mode", "amount_mode": req.amount_mode, "expected": "exact_in|exact_out"})

    if side == "buy":
        asset_in = quote_meta
        asset_out = base_meta
    else:
        asset_in = base_meta
        asset_out = quote_meta
        amount_mode = "exact_in"

    build_amount_ui = float(req.amount)
    build_amount_mode = amount_mode
    manual_plan_base = base
    manual_plan_quote = quote
    manual_plan_base_meta = base_meta
    manual_plan_quote_meta = quote_meta
    manual_plan_side = side
    manual_plan_amount_ui = float(req.amount)
    manual_plan_amount_mode = amount_mode
    manual_plan_slippage_bps = int(req.slippage_bps)
    manual_quote_spend_buy = False

    manual_cfg = await _hydration_manual_pool_config_with_live_reserves(base=base, quote=quote, base_meta=base_meta, quote_meta=quote_meta, db=db)
    if route_mode_norm == "manual_xyk" and manual_cfg is None:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "hydration_manual_route_config_missing",
                "message": "route_mode=manual_xyk was requested, but no manual XYK pool config exists for this Hydration pair.",
                "venue": "polkadot_hydration",
                "rawSymbol": req.symbol,
                "resolvedSymbol": f"{base}-{quote}",
                "side": side,
                "amount": req.amount,
                "base": base_meta,
                "quote": quote_meta,
                "routeMode": route_mode_norm,
                "nextRequired": "Add this pair to /api/polkadot_dex/hydration/route_registry/upsert, or use route_mode=auto/sdk for SDK-supported pairs.",
            },
        )
    if route_mode_norm not in {"auto", "manual_xyk"}:
        manual_cfg = None

    # For custom Hydration XYK BUYs, prefer spending the exact quote amount the
    # ticket already calculated instead of building router.buy exact-output with
    # max_amount_in from a stale manual reserve snapshot.  Hydration rejected the
    # exact-output path with Router.TradingLimitReached when the manual snapshot
    # under-estimated the HDX required.  This keeps the visible UTT side as BUY,
    # but builds the on-chain call as router.sell(HDX -> UTTT) with min output.
    quote_spend_estimate = _float_or_none(req.quote_spend_estimate)
    if side == "buy" and quote_spend_estimate is not None and route_mode_norm in {"auto", "manual_xyk"}:
        reverse_cfg = await _hydration_manual_pool_config_with_live_reserves(
            base=quote,
            quote=base,
            base_meta=quote_meta,
            quote_meta=base_meta,
            db=db,
        )
        if reverse_cfg is not None:
            manual_quote_spend_buy = True
            manual_cfg = reverse_cfg
            manual_plan_base = quote
            manual_plan_quote = base
            manual_plan_base_meta = quote_meta
            manual_plan_quote_meta = base_meta
            manual_plan_side = "sell"
            manual_plan_amount_ui = float(quote_spend_estimate)
            manual_plan_amount_mode = "exact_in"
            build_amount_ui = float(quote_spend_estimate)
            build_amount_mode = "exact_in"
            # With live pool-account reserves, the quote-spend BUY guard can
            # stay at the requested slippage. Keep the old extra fallback only
            # when live reserve discovery is unavailable/failing.
            reverse_live_ok = bool(((reverse_cfg or {}).get("liveReserves") or {}).get("ok"))
            try:
                extra_default = "0" if reverse_live_ok else "1000"
                extra_bps = int(os.getenv("UTT_HYDRATION_MANUAL_BUY_SPEND_EXTRA_SLIPPAGE_BPS") or extra_default)
            except Exception:
                extra_bps = 0 if reverse_live_ok else 1000
            extra_bps = max(0, min(int(extra_bps), 5000))
            manual_plan_slippage_bps = min(5000, int(req.slippage_bps) + extra_bps)

    manual_custom_swap = _hydration_manual_custom_swap_plan(
        base=manual_plan_base,
        quote=manual_plan_quote,
        side=manual_plan_side,
        amount_ui=manual_plan_amount_ui,
        amount_mode=manual_plan_amount_mode,
        slippage_bps=manual_plan_slippage_bps,
        base_meta=manual_plan_base_meta,
        quote_meta=manual_plan_quote_meta,
        cfg=manual_cfg,
    )
    if manual_quote_spend_buy and isinstance(manual_custom_swap, dict):
        manual_custom_swap.update({
            "semanticSide": "buy",
            "quoteSpendExactIn": True,
            "quoteSpendEstimate": float(quote_spend_estimate),
            "requestedBaseAmountUi": float(req.amount),
            "requestedAmountMode": amount_mode,
            "effectiveAmountMode": "exact_in",
            "effectiveAmountUi": float(quote_spend_estimate),
            "effectiveSlippageBps": int(manual_plan_slippage_bps),
            "note": "UTT BUY is built as Hydration router.sell exact-in quote spend for this custom XYK pair to avoid exact-output max_amount_in failures from stale manual reserves.",
        })

    if not manual_custom_swap and not _HYDRATION_ENABLE_ROUTER_QUOTES:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "hydration_swap_tx_requires_router_quotes",
                "message": "Hydration swap transaction building requires UTT_HYDRATION_ENABLE_ROUTER_QUOTES=1 unless a manual custom-asset fallback is available for this pair.",
                "venue": "polkadot_hydration",
                "rawSymbol": req.symbol,
                "resolvedSymbol": f"{base}-{quote}",
                "side": side,
                "amount": req.amount,
                "base": base_meta,
                "quote": quote_meta,
                "quoteStatus": quote_status,
                "routeMode": route_mode_norm,
            },
        )

    built = await _hydration_swap_tx_build(
        raw_symbol=req.symbol,
        base=base,
        quote=quote,
        side=side,
        asset_in=asset_in,
        asset_out=asset_out,
        amount_ui=build_amount_ui,
        amount_mode=build_amount_mode,
        slippage_bps=int(manual_plan_slippage_bps if manual_quote_spend_buy else req.slippage_bps),
        user_pubkey=req.user_pubkey,
        manual_custom_swap=manual_custom_swap,
        route_mode=route_mode_norm,
    )

    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "network": "hydration",
        "rawSymbol": req.symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "side": side,
        "amount": float(req.amount),
        "amountMode": amount_mode,
        "effectiveAmountMode": build_amount_mode,
        "effectiveAmount": float(build_amount_ui),
        "quoteSpendEstimate": req.quote_spend_estimate,
        "quoteSpendExactIn": bool(manual_quote_spend_buy),
        "slippageBps": int(req.slippage_bps),
        "effectiveSlippageBps": int(manual_plan_slippage_bps if manual_quote_spend_buy else req.slippage_bps),
        "routeMode": route_mode_norm,
        "routeModeEffective": "manual_xyk" if manual_custom_swap else ("isolated_helper" if route_mode_norm == "isolated_helper" else "sdk"),
        "user_pubkey": req.user_pubkey,
        "base": base_meta,
        "quote": quote_meta,
        "assetIn": asset_in,
        "assetOut": asset_out,
        "quoteStatus": quote_status,
        "manualCustomSwap": manual_custom_swap,
        "swapTxStatus": {
            "enabled": True,
            "signed": False,
            "submitted": False,
            "nextRequired": "Front-end SubWallet signing/submission is the next step. This endpoint returns unsigned encoded call data from the SDK path or manual custom-asset Router fallback.",
        },
        "tx": built,
    }

@router.post("/hydration/record_submit")
async def hydration_record_submit(
    payload: Dict[str, Any] = Body(...),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Record a successfully finalized Hydration swap into generic swap_orders.

    This endpoint records only confirmed on-chain successful swaps.  It is called
    by the frontend after SubWallet signing/submission/finalization returns
    onChainOk=true.  All Orders already reflects swap_orders generically.
    """
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail={"error": "hydration_record_payload_must_be_object"})

    _ensure_swap_orders_table(db)
    rec = _hydration_swap_record_from_payload(payload, db)

    existing = db.execute(
        text("SELECT id FROM swap_orders WHERE signature = :signature LIMIT 1"),
        {"signature": rec["signature"]},
    ).mappings().first()

    params = {
        "id": rec["id"],
        "chain": rec["chain"],
        "venue": rec["venue"],
        "signature": rec["signature"],
        "ts": rec["ts"],
        "wallet_address": rec["wallet_address"],
        "raw_symbol": rec["raw_symbol"],
        "resolved_symbol": rec["resolved_symbol"],
        "side": rec["side"],
        "base_mint": rec["base_mint"],
        "quote_mint": rec["quote_mint"],
        "base_qty": rec["base_qty"],
        "quote_qty": rec["quote_qty"],
        "price": rec["price"],
        "fee_quote": rec["fee_quote"],
        "status": rec["status"],
        "raw": rec["raw"],
    }

    id_is_integer = _swap_orders_id_column_is_integer(db)

    if existing:
        rec_id = existing.get("id") or rec["id"]
        params["id"] = rec_id
        db.execute(
            text("""
                UPDATE swap_orders
                SET chain=:chain,
                    venue=:venue,
                    ts=:ts,
                    wallet_address=:wallet_address,
                    raw_symbol=:raw_symbol,
                    resolved_symbol=:resolved_symbol,
                    side=:side,
                    base_mint=:base_mint,
                    quote_mint=:quote_mint,
                    base_qty=:base_qty,
                    quote_qty=:quote_qty,
                    price=:price,
                    fee_quote=:fee_quote,
                    status=:status,
                    raw=:raw
                WHERE signature=:signature
            """),
            params,
        )
        action = "updated"
    else:
        if id_is_integer:
            # Existing Solana-created DBs can have swap_orders.id as INTEGER
            # PRIMARY KEY.  Let SQLite allocate it; the tx signature remains the
            # stable DEX identifier used by All Orders.
            db.execute(
                text("""
                    INSERT INTO swap_orders
                        (chain, venue, signature, ts, wallet_address, raw_symbol, resolved_symbol, side,
                         base_mint, quote_mint, base_qty, quote_qty, price, fee_quote, status, raw)
                    VALUES
                        (:chain, :venue, :signature, :ts, :wallet_address, :raw_symbol, :resolved_symbol, :side,
                         :base_mint, :quote_mint, :base_qty, :quote_qty, :price, :fee_quote, :status, :raw)
                """),
                params,
            )
        else:
            db.execute(
                text("""
                    INSERT INTO swap_orders
                        (id, chain, venue, signature, ts, wallet_address, raw_symbol, resolved_symbol, side,
                         base_mint, quote_mint, base_qty, quote_qty, price, fee_quote, status, raw)
                    VALUES
                        (:id, :chain, :venue, :signature, :ts, :wallet_address, :raw_symbol, :resolved_symbol, :side,
                         :base_mint, :quote_mint, :base_qty, :quote_qty, :price, :fee_quote, :status, :raw)
                """),
                params,
            )
        action = "inserted"

    db.commit()

    stored_id = _swap_orders_row_id_by_signature(db, rec["signature"])
    if stored_id is not None:
        params["id"] = stored_id

    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "chain": "hydration",
        "action": action,
        "record": {
            "id": params["id"],
            "signature": rec["signature"],
            "txHash": rec["signature"],
            "rawSymbol": rec["raw_symbol"],
            "resolvedSymbol": rec["resolved_symbol"],
            "side": rec["side"],
            "base_qty": rec["base_qty"],
            "quote_qty": rec["quote_qty"],
            "price": rec["price"],
            "fee_quote": rec["fee_quote"],
            "status": rec["status"],
            "wallet_address": rec["wallet_address"],
            "block_number": rec.get("block_number"),
            "block_hash": rec.get("block_hash"),
        },
        "allOrdersSource": "swap_orders",
    }

