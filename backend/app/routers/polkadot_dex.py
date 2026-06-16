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
        "spot": "sdk_spot",
        "spot_price": "sdk_spot",
        "sdkspot": "sdk_spot",
        "sdk_spot_price": "sdk_spot",
        "isolated": "isolated_helper",
        "helper": "isolated_helper",
        "manual": "manual_xyk",
        "xyk": "manual_xyk",
        "router": "manual_router",
        "manual_router_fallback": "manual_router",
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
    if m == "manual_router":
        return "manual_router_route_registry"
    if m == "sdk_spot":
        return "galactic_sdk_next_spot_price"
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
# Visual orderbook fallback for generic Hydration pairs when sdk-next/PAPI
# getBestSell quote sampling times out or router quotes are intentionally gated.
# This uses cached/external USD prices only; it does not mark the pair tradable
# and does not build/sign/submit swaps.
_HYDRATION_ENABLE_ORDERBOOK_SYNTHETIC_FALLBACK = _env_bool("UTT_HYDRATION_ENABLE_ORDERBOOK_SYNTHETIC_FALLBACK", True)
_HYDRATION_ORDERBOOK_SYNTHETIC_REFRESH = _env_bool("UTT_HYDRATION_ORDERBOOK_SYNTHETIC_REFRESH", True)
try:
    _HYDRATION_ORDERBOOK_SYNTHETIC_SPREAD_BPS = float(os.getenv("UTT_HYDRATION_ORDERBOOK_SYNTHETIC_SPREAD_BPS") or "35")
except Exception:
    _HYDRATION_ORDERBOOK_SYNTHETIC_SPREAD_BPS = 35.0
# Temporary controlled manual-router fallback for known Hydration Omnipool pairs.
# This does not use sdk-next router quotes. It builds Router.sell call data from
# a fixed route candidate and a conservative min-out derived from cached/external
# USD prices. Keep this bounded until route-registry v2 can store confirmed
# non-XYK routes with a first-class quote source.
_HYDRATION_ENABLE_MANUAL_ROUTER_FALLBACK = _env_bool("UTT_HYDRATION_ENABLE_MANUAL_ROUTER_FALLBACK", True)
_HYDRATION_MANUAL_ROUTER_FALLBACK_PAIRS_CSV = (
    os.getenv("UTT_HYDRATION_MANUAL_ROUTER_FALLBACK_PAIRS")
    or ""
).strip()
_HYDRATION_MANUAL_ROUTER_FALLBACK_CONFIRMED_PAIRS_CSV = (
    os.getenv("UTT_HYDRATION_MANUAL_ROUTER_FALLBACK_CONFIRMED_PAIRS")
    or ""
).strip()
_HYDRATION_ALLOW_UNCONFIRMED_MANUAL_ROUTER_FALLBACK = _env_bool(
    "UTT_HYDRATION_ALLOW_UNCONFIRMED_MANUAL_ROUTER_FALLBACK",
    False,
)
_HYDRATION_MANUAL_ROUTER_FALLBACK_POOL = (
    os.getenv("UTT_HYDRATION_MANUAL_ROUTER_FALLBACK_POOL")
    or "Omnipool"
).strip() or "Omnipool"
_HYDRATION_MANUAL_ROUTER_FALLBACK_ROUTES_JSON = (
    os.getenv("UTT_HYDRATION_MANUAL_ROUTER_FALLBACK_ROUTES_JSON")
    or "{}"
).strip()
try:
    _HYDRATION_MANUAL_ROUTER_FALLBACK_MAX_INPUT_USD = float(os.getenv("UTT_HYDRATION_MANUAL_ROUTER_FALLBACK_MAX_INPUT_USD") or "5")
except Exception:
    _HYDRATION_MANUAL_ROUTER_FALLBACK_MAX_INPUT_USD = 5.0
_HYDRATION_ROUTE_MODES = {"auto", "sdk", "sdk_spot", "isolated_helper", "manual_xyk", "manual_router"}
_HYDRATION_DEFAULT_ROUTE_MODE = (os.getenv("UTT_HYDRATION_ROUTE_MODE") or "auto").strip().lower()
if _HYDRATION_DEFAULT_ROUTE_MODE not in _HYDRATION_ROUTE_MODES:
    _HYDRATION_DEFAULT_ROUTE_MODE = "auto"
try:
    _HYDRATION_NATIVE_ASSET_ID = int(os.getenv("UTT_HYDRATION_NATIVE_ASSET_ID") or "0")
except Exception:
    _HYDRATION_NATIVE_ASSET_ID = 0

_HYDRATION_ENABLE_HEAVY_INSPECT = _env_bool("UTT_HYDRATION_ENABLE_HEAVY_INSPECT", False)
_HYDRATION_ENABLE_ROUTER_QUOTES = _env_bool("UTT_HYDRATION_ENABLE_ROUTER_QUOTES", False)
_HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES = _env_bool("UTT_HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES", False)
# H-SDK.1 diagnostic/visual path: use sdk-next getSpotPrice for visible-pair
# pseudo-orderbooks without reopening getBestSell ladder sampling.  This is
# intentionally separate from UTT_HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES because
# getBestSell is the path currently timing out.
_HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK = _env_bool("UTT_HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK", False)
_HYDRATION_SDK_SPOT_ORDERBOOK_FORCE_ISOLATED_HELPER = _env_bool("UTT_HYDRATION_SDK_SPOT_ORDERBOOK_FORCE_ISOLATED_HELPER", True)
_HYDRATION_SDK_SPOT_ORDERBOOK_TRADABLE = _env_bool("UTT_HYDRATION_SDK_SPOT_ORDERBOOK_TRADABLE", False)
_HYDRATION_SDK_SPOT_ORDERBOOK_IMPLEMENTATION = (
    os.getenv("UTT_HYDRATION_SDK_SPOT_ORDERBOOK_IMPLEMENTATION")
    or "direct"
).strip().lower()
if _HYDRATION_SDK_SPOT_ORDERBOOK_IMPLEMENTATION not in {"direct", "context"}:
    _HYDRATION_SDK_SPOT_ORDERBOOK_IMPLEMENTATION = "direct"
try:
    _HYDRATION_SDK_SPOT_ORDERBOOK_MIN_DAILY_VOLUME_USD = float(os.getenv("UTT_HYDRATION_SDK_SPOT_ORDERBOOK_MIN_DAILY_VOLUME_USD") or "0")
except Exception:
    _HYDRATION_SDK_SPOT_ORDERBOOK_MIN_DAILY_VOLUME_USD = 0.0
_HYDRATION_SDK_SPOT_ORDERBOOK_POOL_TYPES_CSV = (
    os.getenv("UTT_HYDRATION_SDK_SPOT_ORDERBOOK_POOL_TYPES")
    or "Omnipool,XYK,StableSwap"
).strip()
_HYDRATION_ENABLE_SDK_ORDER_TICKET_QUOTES = _env_bool("UTT_HYDRATION_ENABLE_SDK_ORDER_TICKET_QUOTES", False)
_HYDRATION_ENABLE_SDK_SWAP_TX = _env_bool("UTT_HYDRATION_ENABLE_SDK_SWAP_TX", False)
_HYDRATION_ENABLE_BACKGROUND_SDK_PRICES = _env_bool("UTT_HYDRATION_ENABLE_BACKGROUND_SDK_PRICES", False)
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



# H-SDK.1 metadata diagnostics: lightweight Metadata v15 scanner.
#
# This intentionally does not attempt a full SCALE metadata decode.  The goal is
# to keep a safe, read-only diagnostic endpoint that can confirm useful runtime
# names and nearby call strings after Metadata_metadata_at_version succeeds.
# The raw metadata blob can be large, so responses return bounded ASCII windows.
def _metadata_v15_probe_payload() -> str:
    # SCALE u32 15 little-endian for Metadata_metadata_at_version(version: u32).
    return "0x0f000000"


def _metadata_result_hex_from_probe(probe: Dict[str, Any]) -> Optional[str]:
    try:
        result = ((probe or {}).get("rpc") or {}).get("result")
        if isinstance(result, str) and result.startswith("0x"):
            return result
    except Exception:
        pass
    return None


def _bytes_from_hex_result(hex_value: str) -> bytes:
    h = str(hex_value or "").strip()
    if h.startswith("0x"):
        h = h[2:]
    if not h:
        return b""
    try:
        return bytes.fromhex(h)
    except Exception:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "hydration_metadata_hex_decode_failed",
                "message": "Metadata_metadata_at_version returned non-decodable hex.",
                "rawPrefix": str(hex_value or "")[:120],
            },
        )


def _ascii_preview(data: bytes) -> str:
    chars: List[str] = []
    last_space = False
    for b in data or b"":
        if 32 <= int(b) <= 126:
            chars.append(chr(int(b)))
            last_space = False
        else:
            if not last_space:
                chars.append(" ")
                last_space = True
    return " ".join("".join(chars).split())


def _metadata_ascii_strings(data: bytes, *, min_len: int = 3, max_strings: int = 500) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    start: Optional[int] = None
    buf: List[str] = []

    def _flush(end_offset: int) -> None:
        nonlocal start, buf
        if start is not None and len(buf) >= int(min_len):
            text = "".join(buf)
            out.append({
                "offset": int(start),
                "endOffset": int(end_offset),
                "text": text,
            })
        start = None
        buf = []

    for idx, b in enumerate(data or b""):
        bi = int(b)
        if 32 <= bi <= 126:
            if start is None:
                start = idx
            buf.append(chr(bi))
        else:
            _flush(idx)
            if len(out) >= int(max_strings):
                break
    if len(out) < int(max_strings):
        _flush(len(data or b""))

    return out[: max(0, int(max_strings))]


def _metadata_strings_in_window(
    strings: List[Dict[str, Any]],
    *,
    start: int,
    end: int,
    max_items: int = 30,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in strings or []:
        try:
            off = int(item.get("offset") or 0)
            end_off = int(item.get("endOffset") or off)
        except Exception:
            continue
        if end_off < int(start) or off > int(end):
            continue
        out.append(item)
        if len(out) >= int(max_items):
            break
    return out


def _metadata_is_snakeish_name(value: Any) -> bool:
    s = str(value or "").strip()
    if not s or len(s) > 80:
        return False
    if " " in s or "\t" in s or "\n" in s:
        return False
    allowed = set("abcdefghijklmnopqrstuvwxyz0123456789_")
    lowered = s.lower()
    return bool(any(c.isalpha() for c in lowered) and all(c in allowed for c in lowered))


def _metadata_target_windows(
    data: bytes,
    *,
    targets: List[str],
    context_bytes: int = 220,
    max_windows_per_target: int = 8,
    strings: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    blob = bytes(data or b"")
    lower_blob = blob.lower()
    strings = strings or []
    out: Dict[str, Any] = {}

    for raw_target in targets or []:
        target = str(raw_target or "").strip()
        if not target:
            continue
        needle = target.encode("utf-8", errors="ignore").lower()
        if not needle:
            continue

        offsets: List[int] = []
        search_from = 0
        while True:
            idx = lower_blob.find(needle, search_from)
            if idx < 0:
                break
            offsets.append(int(idx))
            search_from = idx + max(1, len(needle))
            if len(offsets) >= int(max_windows_per_target):
                break

        windows: List[Dict[str, Any]] = []
        candidate_names: List[str] = []
        for idx in offsets:
            start = max(0, idx - int(context_bytes))
            end = min(len(blob), idx + len(needle) + int(context_bytes))
            nearby = _metadata_strings_in_window(strings, start=start, end=end, max_items=35)
            for item in nearby:
                name = str(item.get("text") or "").strip()
                if _metadata_is_snakeish_name(name) and name not in candidate_names:
                    candidate_names.append(name)
            windows.append({
                "offset": int(idx),
                "start": int(start),
                "end": int(end),
                "preview": _ascii_preview(blob[start:end]),
                "nearbyStrings": nearby,
            })

        out[target] = {
            "hitCountReturned": len(offsets),
            "windows": windows,
            "snakeishNearbyNames": candidate_names[:40],
        }

    return out


def _metadata_scan_summary(
    metadata_hex: str,
    *,
    targets: List[str],
    context_bytes: int,
    max_windows_per_target: int,
    max_ascii_strings: int,
) -> Dict[str, Any]:
    data = _bytes_from_hex_result(metadata_hex)
    strings = _metadata_ascii_strings(data, min_len=3, max_strings=max_ascii_strings)
    target_windows = _metadata_target_windows(
        data,
        targets=targets,
        context_bytes=context_bytes,
        max_windows_per_target=max_windows_per_target,
        strings=strings,
    )

    target_strings: Dict[str, List[Dict[str, Any]]] = {}
    all_call_candidates: List[str] = []
    for target in targets or []:
        t = str(target or "").strip()
        if not t:
            continue
        tl = t.lower()
        matches: List[Dict[str, Any]] = []
        for item in strings:
            s = str(item.get("text") or "")
            if tl in s.lower():
                matches.append(item)
                if len(matches) >= 40:
                    break
        target_strings[t] = matches

        for name in ((target_windows.get(t) or {}).get("snakeishNearbyNames") or []):
            if name not in all_call_candidates:
                all_call_candidates.append(name)

    return {
        "byteLen": len(data),
        "hexLen": len(str(metadata_hex or "")),
        "rawPrefix": str(metadata_hex or "")[:96],
        "rawSuffix": str(metadata_hex or "")[-96:],
        "asciiStringCountReturned": len(strings),
        "asciiStringsSample": strings[:80],
        "targetStrings": target_strings,
        "targetWindows": target_windows,
        "nearbyCallNameCandidates": all_call_candidates[:120],
        "scannerLimitations": [
            "This is a bounded ASCII/SCALE metadata scanner, not a full Metadata v15 decoder.",
            "Use target windows to confirm pallet/call names before adding manual builders.",
            "No signing, swap building, or state mutation is performed by this scanner.",
        ],
    }



def _metadata_focused_default_terms() -> List[str]:
    # These are deliberately human-readable byte/ASCII targets, not decoded SCALE
    # paths.  They focus the already-safe Metadata v15 scanner on Hydration DEX
    # call/storage areas relevant to manual route hardening.
    return [
        "pallet_route_executor",
        "pallet_omnipool",
        "pallet_xyk",
        "pallet_stableswap",
        "hydradx_traits",
        "router Trade",
        "PoolType",
        "AssetPair",
        "RouteExecuted",
        "Route execution",
        "Routes | Storing routes",
        "add_token",
        "add_liquidity",
        "remove_liquidity",
        "sell",
        "buy",
    ]


def _metadata_focus_terms(raw_csv: Optional[str]) -> List[str]:
    raw = str(raw_csv or "").strip()
    if not raw:
        return _metadata_focused_default_terms()

    out: List[str] = []
    for part in raw.split(","):
        term = str(part or "").strip()
        if term and term not in out:
            out.append(term)
    return out or _metadata_focused_default_terms()


def _metadata_find_term_offsets(
    data: bytes,
    *,
    term: str,
    max_hits: int,
) -> List[int]:
    blob = bytes(data or b"").lower()
    needle = str(term or "").encode("utf-8", errors="ignore").lower()
    if not needle:
        return []

    hits: List[int] = []
    start = 0
    while True:
        idx = blob.find(needle, start)
        if idx < 0:
            break
        hits.append(int(idx))
        start = idx + max(1, len(needle))
        if len(hits) >= int(max_hits):
            break
    return hits


def _metadata_focused_windows(
    metadata_hex: str,
    *,
    terms: List[str],
    context_bytes: int,
    max_hits_per_term: int,
    max_ascii_strings: int,
) -> Dict[str, Any]:
    data = _bytes_from_hex_result(metadata_hex)
    strings = _metadata_ascii_strings(data, min_len=3, max_strings=max_ascii_strings)
    focused: Dict[str, Any] = {}

    for term in terms or []:
        clean_term = str(term or "").strip()
        if not clean_term:
            continue

        hits = _metadata_find_term_offsets(
            data,
            term=clean_term,
            max_hits=max_hits_per_term,
        )
        windows: List[Dict[str, Any]] = []
        for idx in hits:
            start = max(0, int(idx) - int(context_bytes))
            end = min(len(data), int(idx) + len(clean_term.encode("utf-8", errors="ignore")) + int(context_bytes))
            preview = _ascii_preview(data[start:end])
            nearby = _metadata_strings_in_window(strings, start=start, end=end, max_items=60)
            windows.append({
                "offset": int(idx),
                "start": int(start),
                "end": int(end),
                "preview": preview,
                "nearbyStrings": nearby,
            })

        focused[clean_term] = {
            "hitCountReturned": len(hits),
            "windows": windows,
        }

    return {
        "byteLen": len(data),
        "hexLen": len(str(metadata_hex or "")),
        "rawPrefix": str(metadata_hex or "")[:96],
        "rawSuffix": str(metadata_hex or "")[-96:],
        "terms": terms,
        "focusedWindows": focused,
        "manualRouteShapeHints": {
            "routeLeg": {
                "pool": "hydradx_traits::router::PoolType<AssetId>",
                "assetIn": "AssetId",
                "assetOut": "AssetId",
            },
            "observedPoolTypeNames": ["XYK", "LBP", "Stableswap", "Omnipool", "Aave", "HSM"],
            "observedStorageHint": "Router Routes | Storing routes for asset pairs",
            "note": "These hints are extracted from ASCII metadata windows and should guide manual-route validation only; they are not a full SCALE Metadata v15 decode.",
        },
        "scannerLimitations": [
            "This endpoint is read-only and diagnostic-only.",
            "It searches bounded ASCII windows inside Metadata v15; it does not decode full SCALE type IDs.",
            "Do not use this output alone to enable new signing paths without a tiny live confirmation transaction.",
        ],
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


async def _ensure_hydration_sidecar_running(
    *,
    price_cache: bool = False,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if not _HYDRATION_USE_SIDECAR or not _HYDRATION_SIDECAR_URL:
        return {"enabled": bool(_HYDRATION_USE_SIDECAR), "ok": False, "skipped": True}

    effective_autostart = _hydration_effective_autostart_sidecar(price_cache=price_cache, payload=payload)
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
        reason = "price_cache_autostart_disabled" if price_cache else ("router_quotes_disabled_for_sdk_scope" if not _hydration_router_quotes_enabled_for_payload(payload or {}) else "autostart_disabled")
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
    use_case: Optional[str] = None,
) -> Dict[str, Any]:
    sdk_use_case = _normalize_hydration_sdk_use_case(use_case)
    if sdk_use_case:
        probe_payload = {
            "mode": "swap_tx" if sdk_use_case == "order_ticket" else "quote_sell",
            "sdkUseCase": sdk_use_case,
            "routeMode": "auto",
        }
        use_case_enabled = bool(_hydration_router_quotes_enabled_for_payload(probe_payload))
    else:
        use_case_enabled = bool(_HYDRATION_ENABLE_ROUTER_QUOTES)

    global_enabled = bool(_HYDRATION_ENABLE_ROUTER_QUOTES)
    ws_configured = bool((_hydration_ws_url() or "").strip())
    use_case_available = bool(use_case_enabled and _HYDRATION_USE_SIDECAR and ws_configured)
    quotes_available = use_case_available if sdk_use_case else bool(global_enabled and _HYDRATION_USE_SIDECAR and ws_configured)

    if quotes_available:
        reason = (
            f"Hydration SDK router quotes are enabled for {sdk_use_case or 'global'} controlled testing through the local sidecar. "
            "Watch RPC quota and disable immediately if chainHead calls spike."
        )
    elif sdk_use_case == "orderbook" and not use_case_enabled:
        reason = (
            "Hydration SDK OrderBook quotes are disabled. Manual routes and synthetic price-only fallback remain safe; "
            "set UTT_HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES=1 only for visible-pair SDK orderbook testing."
        )
    elif sdk_use_case == "order_ticket" and not use_case_enabled:
        reason = (
            "Hydration SDK OrderTicket quote/swap building is disabled. Confirmed manual routes may still build; "
            "set UTT_HYDRATION_ENABLE_SDK_ORDER_TICKET_QUOTES=1 and UTT_HYDRATION_ENABLE_SDK_SWAP_TX=1 only for controlled ticket testing."
        )
    else:
        reason = _HYDRATION_ROUTER_QUOTES_UNAVAILABLE_REASON

    return {
        "enabled": use_case_enabled if sdk_use_case else global_enabled,
        "available": quotes_available,
        "status": "available_experimental" if quotes_available else ("enabled_but_unavailable" if use_case_enabled else "disabled"),
        "reason": reason,
        "symbol": symbol,
        "base": base_meta,
        "quote": quote_meta,
        "sdkUseCase": sdk_use_case,
        "globalRouterQuotesEnabled": global_enabled,
        "sdkOrderbookQuotesEnabled": bool(_HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES),
        "sdkSpotOrderbookEnabled": bool(_HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK),
        "sdkSpotOrderbookImplementation": _HYDRATION_SDK_SPOT_ORDERBOOK_IMPLEMENTATION,
        "sdkSpotOrderbookForceIsolatedHelper": bool(_HYDRATION_SDK_SPOT_ORDERBOOK_FORCE_ISOLATED_HELPER),
        "sdkSpotOrderbookTradable": bool(_HYDRATION_SDK_SPOT_ORDERBOOK_TRADABLE),
        "sdkSpotOrderbookMinDailyVolumeUsd": float(_HYDRATION_SDK_SPOT_ORDERBOOK_MIN_DAILY_VOLUME_USD),
        "sdkOrderTicketQuotesEnabled": bool(_HYDRATION_ENABLE_SDK_ORDER_TICKET_QUOTES),
        "sdkSwapTxEnabled": bool(_HYDRATION_ENABLE_SDK_SWAP_TX),
        "backgroundSdkPricesEnabled": bool(_HYDRATION_ENABLE_BACKGROUND_SDK_PRICES),
        "useCaseEnabled": use_case_enabled,
        "useCaseAvailable": use_case_available,
        "wsConfigured": ws_configured,
        "sidecarEnabled": bool(_HYDRATION_USE_SIDECAR),
        "safeEndpoints": [
            "/api/polkadot_dex/_debug",
            "/api/polkadot_dex/resolve",
            "/api/polkadot_dex/balances",
            "/api/polkadot_dex/hydration/status",
            "/api/polkadot_dex/hydration/inspect with inspect_mode=light",
            "/api/polkadot_dex/hydration/orderbook for configured manual routes",
            "/api/polkadot_dex/hydration/orderbook SDK getBestSell sampling only when UTT_HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES=1 or global override is enabled",
            "/api/polkadot_dex/hydration/orderbook route_mode=sdk_spot only when UTT_HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK=1",
            "/api/polkadot_dex/hydration/sdk_path_diagnostics for explicit spot/sell/swap path separation",
            "/api/polkadot_dex/hydration/sdk_recovery_diagnostics for bounded recovery diagnostics",
            "/api/polkadot_dex/hydration/sdk_recovery_state_call_compare for disabled-by-default state_call probing",
            "/api/polkadot_dex/hydration/sdk_recovery_metadata_method_hunt for metadata-guided method hunting",
            "/api/polkadot_dex/hydration/sdk_recovery_closeout for the current no-SDK-quote decision",
            "/api/polkadot_dex/hydration/swap_tx SDK build only when UTT_HYDRATION_ENABLE_SDK_ORDER_TICKET_QUOTES=1 plus UTT_HYDRATION_ENABLE_SDK_SWAP_TX=1 or global override is enabled",
        ],
        "blockedMethods": [] if quotes_available else [
            "sdk.api.router.getSpotPrice",
            "sdk.api.router.getBestSell",
            "sdk.api.router.getBestBuy",
            "sdk.api.router.getRoutes",
            "sdk.api.router.getPools",
            "sdk.api.router.getTradeableAssets",
        ],
        "nextRequired": (
            "SDK router quotes are enabled for this scoped use-case. Watch Dwellir quota and disable immediately if chainHead calls spike."
            if quotes_available
            else "Keep SDK router quote scopes disabled except for deliberate visible OrderBook or user-action OrderTicket testing."
        ),
        "quotesExperimental": quotes_available,
        "liveSwapsRecommended": bool(quotes_available and _HYDRATION_ENABLE_SWAP_TX),
        "swapTxEnabled": bool(_HYDRATION_ENABLE_SWAP_TX),
        "exactBuyEnabled": bool(_HYDRATION_ENABLE_EXACT_BUY),
        "buyDiagnosticsEnabled": bool(_HYDRATION_ENABLE_BUY_DIAGNOSTICS),
        "liveExactBuyRecommended": bool(quotes_available and _HYDRATION_ENABLE_SWAP_TX and _HYDRATION_ENABLE_EXACT_BUY),
    }



def _normalize_hydration_sdk_use_case(raw: Optional[str]) -> Optional[str]:
    s = str(raw or "").strip().lower().replace("-", "_")
    if not s:
        return None
    aliases = {
        "order_book": "orderbook",
        "orderbook": "orderbook",
        "book": "orderbook",
        "order_ticket": "order_ticket",
        "orderticket": "order_ticket",
        "ticket": "order_ticket",
        "swap": "order_ticket",
        "swap_tx": "order_ticket",
        "trade": "order_ticket",
        "price_cache": "price_cache",
        "prices": "price_cache",
        "background": "background",
        "portfolio": "background",
        "balances": "background",
        "spread_bridge": "background",
        "bridge": "background",
    }
    return aliases.get(s, s)


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


def _hydration_payload_sdk_use_case(payload: Dict[str, Any]) -> Optional[str]:
    try:
        raw = (
            (payload or {}).get("sdkUseCase")
            or (payload or {}).get("sdk_use_case")
            or (payload or {}).get("useCase")
            or (payload or {}).get("use_case")
        )
        return _normalize_hydration_sdk_use_case(raw)
    except Exception:
        return None


def _hydration_router_quotes_enabled_for_payload(payload: Dict[str, Any]) -> bool:
    """Return whether SDK router quote methods may run for this request.

    Manual custom swap builders do not require sdk-next router quotes.  The broad
    UTT_HYDRATION_ENABLE_ROUTER_QUOTES flag remains a global emergency override,
    while H-SDK.1 adds narrow OrderBook and OrderTicket scopes so background
    pricing/balance/spread paths stay quiet by default.
    """
    payload = payload or {}
    if _hydration_payload_manual_custom_swap_enabled(payload):
        return True
    if bool(_HYDRATION_ENABLE_ROUTER_QUOTES):
        return True

    route_mode = _hydration_route_mode(payload.get("routeMode") or payload.get("route_mode") or "auto")
    if route_mode in {"manual_xyk", "manual_router"}:
        return False

    use_case = _hydration_payload_sdk_use_case(payload)
    mode = str(payload.get("mode") or "").strip().lower()

    if use_case == "orderbook":
        if mode in {"price_spot", "price_spot_direct"}:
            return bool(_HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK)
        if route_mode == "sdk_spot":
            return False
        return bool(_HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES)

    if use_case == "order_ticket":
        if mode == "swap_tx":
            return bool(_HYDRATION_ENABLE_SDK_ORDER_TICKET_QUOTES and _HYDRATION_ENABLE_SDK_SWAP_TX)
        return bool(_HYDRATION_ENABLE_SDK_ORDER_TICKET_QUOTES)

    if use_case == "price_cache":
        return bool(_HYDRATION_ENABLE_SDK_PRICE_CACHE and _HYDRATION_PRICE_CACHE_USE_SDK_FALLBACK)

    if use_case == "background":
        return bool(_HYDRATION_ENABLE_BACKGROUND_SDK_PRICES)

    return False


def _hydration_router_quote_scope_config(payload: Dict[str, Any]) -> Dict[str, Any]:
    use_case = _hydration_payload_sdk_use_case(payload or {})
    mode = str((payload or {}).get("mode") or "").strip().lower()
    route_mode_raw = (payload or {}).get("routeMode") or (payload or {}).get("route_mode") or "auto"
    route_mode = _hydration_route_mode(route_mode_raw)
    enabled_for_payload = bool(_hydration_router_quotes_enabled_for_payload(payload or {}))
    return {
        "sdkUseCase": use_case,
        "mode": mode,
        "routeMode": route_mode,
        "enabledForPayload": enabled_for_payload,
        "globalRouterQuotesEnabled": bool(_HYDRATION_ENABLE_ROUTER_QUOTES),
        "sdkOrderbookQuotesEnabled": bool(_HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES),
        "sdkSpotOrderbookEnabled": bool(_HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK),
        "sdkSpotOrderbookImplementation": _HYDRATION_SDK_SPOT_ORDERBOOK_IMPLEMENTATION,
        "sdkOrderTicketQuotesEnabled": bool(_HYDRATION_ENABLE_SDK_ORDER_TICKET_QUOTES),
        "sdkSwapTxEnabled": bool(_HYDRATION_ENABLE_SDK_SWAP_TX),
        "backgroundSdkPricesEnabled": bool(_HYDRATION_ENABLE_BACKGROUND_SDK_PRICES),
    }


def _hydration_effective_autostart_sidecar(
    *,
    price_cache: bool = False,
    payload: Optional[Dict[str, Any]] = None,
) -> bool:
    """Return whether the managed JS sidecar may be auto-started.

    Price-cache autostart remains separate.  Normal SDK router autostart is now
    permitted only for the request's scoped use-case: OrderBook, OrderTicket, or
    the legacy global emergency override.
    """
    if price_cache:
        return bool(
            _HYDRATION_ENABLE_SDK_PRICE_CACHE
            and _HYDRATION_PRICE_CACHE_USE_SIDECAR
            and _HYDRATION_PRICE_CACHE_AUTOSTART_SIDECAR
        )
    if not _HYDRATION_AUTOSTART_SIDECAR:
        return False
    if bool(_HYDRATION_ENABLE_ROUTER_QUOTES):
        return True
    if payload:
        return bool(_hydration_router_quotes_enabled_for_payload(payload))
    return False


def _hydration_router_quotes_disabled_detail(
    *,
    mode: str,
    payload: Optional[Dict[str, Any]] = None,
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    payload = payload or {}
    scope = _hydration_router_quote_scope_config(payload)
    use_case = scope.get("sdkUseCase")
    status = _hydration_router_quote_status(
        symbol=symbol or payload.get("resolvedSymbol") or payload.get("rawSymbol"),
        use_case=use_case,
    )
    return {
        "error": "hydration_router_quotes_disabled",
        "message": (
            "Hydration SDK router quote calls are disabled for this scoped use-case. "
            "Manual XYK/manual Router routes remain available for configured pairs."
        ),
        "venue": "polkadot_hydration",
        "mode": mode,
        "symbol": symbol or payload.get("resolvedSymbol") or payload.get("rawSymbol"),
        "enableRouterQuotes": bool(_HYDRATION_ENABLE_ROUTER_QUOTES),
        "manualCustomSwap": _hydration_payload_manual_custom_swap_enabled(payload),
        "sdkQuoteScope": scope,
        "quoteStatus": status,
        "safePaths": [
            "route_mode=manual_xyk for configured manual XYK routes",
            "route_mode=manual_router for confirmed DB manual Router routes",
            "route_mode=auto only uses SDK when the matching scoped SDK env flag is enabled",
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
        "nextRequired": (
            "For visible OrderBook getBestSell testing, set UTT_HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES=1. For visible OrderBook getSpotPrice testing, set UTT_HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK=1 and use route_mode=sdk_spot. "
            "For user-action OrderTicket SDK builds, set UTT_HYDRATION_ENABLE_SDK_ORDER_TICKET_QUOTES=1 and UTT_HYDRATION_ENABLE_SDK_SWAP_TX=1. "
            "Leave UTT_HYDRATION_ENABLE_ROUTER_QUOTES=0 unless deliberately using the global emergency override."
        ),
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
    if _hydration_requires_router_quotes(payload):
        payload["enableRouterQuotes"] = bool(_hydration_router_quotes_enabled_for_payload(payload))
        if not payload["enableRouterQuotes"]:
            raise HTTPException(
                status_code=503,
                detail=_hydration_router_quotes_disabled_detail(mode=mode, payload=payload),
            )
    if not _HYDRATION_SIDECAR_URL:
        raise HTTPException(status_code=503, detail={"error": "hydration_sidecar_url_not_configured"})
    price_cache = _hydration_payload_price_cache_enabled(payload or {})
    sidecar_state = await _ensure_hydration_sidecar_running(price_cache=price_cache, payload=payload)
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

    if _hydration_requires_router_quotes(payload):
        payload["enableRouterQuotes"] = bool(_hydration_router_quotes_enabled_for_payload(payload))
        if not payload["enableRouterQuotes"]:
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
    sdk_use_case: str = "orderbook",
    route_mode: str = "auto",
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
            "sdkUseCase": sdk_use_case,
            "routeMode": _hydration_route_mode(route_mode),
            "enableRouterQuotes": bool(_hydration_router_quotes_enabled_for_payload({
                "mode": "quote_sell",
                "sdkUseCase": sdk_use_case,
                "routeMode": _hydration_route_mode(route_mode),
            })),
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


_HYDRATION_STABLE_USD_SYMBOLS = {"USDT", "USDC", "HOLLAR"}


def _hydration_price_cache_missing_symbols(
    *,
    requested: List[str],
    prices: Dict[str, Any],
) -> List[str]:
    missing: List[str] = []
    for sym_raw in requested or []:
        sym = str(sym_raw or "").strip().upper()
        if not sym or sym in _HYDRATION_STABLE_USD_SYMBOLS:
            continue
        if _float_or_none((prices or {}).get(sym)) is None:
            missing.append(sym)
    return missing


def _hydration_price_cache_status_detail(
    *,
    status: str,
    requested: List[str],
    prices: Dict[str, Any],
) -> Dict[str, Any]:
    now = time.monotonic()
    expires_at = float(_hydration_usd_price_cache.get("expires_at") or 0)
    error_until = float(_hydration_usd_price_cache.get("error_until") or 0)
    missing = _hydration_price_cache_missing_symbols(requested=requested, prices=prices)
    stale = bool(expires_at <= now)
    in_error_backoff = bool(error_until > now)
    status_raw = str(status or "").strip().lower()

    if status_raw == "status_only":
        classification = "status_only"
        source_state = "status_only_no_refresh"
    elif status_raw == "cache_only":
        if missing and stale:
            classification = "cache_only_partial_stale"
        elif missing:
            classification = "cache_only_partial"
        elif stale:
            classification = "cache_only_stale"
        else:
            classification = "cache_only_fresh"
        source_state = "cache_only"
    elif status_raw in {"fresh", "refreshed", "refreshed_external"}:
        classification = "live_fresh" if status_raw != "fresh" else "cache_fresh"
        source_state = "external_refresh" if status_raw == "refreshed_external" else status_raw
    elif "backoff" in status_raw:
        classification = "error_backoff"
        source_state = "error_backoff"
    elif "failed" in status_raw:
        classification = "refresh_failed_stale"
        source_state = "refresh_failed"
    elif "partial" in status_raw:
        classification = "partial_stale"
        source_state = "partial"
    else:
        classification = status_raw or "unknown"
        source_state = status_raw or "unknown"

    return {
        "classification": classification,
        "source_state": source_state,
        "missing_prices": missing,
        "has_all_requested": len(missing) == 0,
        "stale": stale,
        "in_error_backoff": in_error_backoff,
        "seconds_until_expiry": max(0.0, expires_at - now) if expires_at else 0.0,
        "seconds_until_retry": max(0.0, error_until - now) if error_until else 0.0,
        "sdk_fallback_enabled": bool(_HYDRATION_PRICE_CACHE_USE_SDK_FALLBACK),
        "external_usd_prices_enabled": bool(_HYDRATION_ENABLE_EXTERNAL_USD_PRICES),
    }


def _hydration_price_cache_failure_mode(
    *,
    errors: List[Dict[str, Any]],
    sdk_fallback_attempted: bool,
    external_attempted: bool,
) -> str:
    try:
        joined = json.dumps(errors or [], default=str).lower()
    except Exception:
        joined = str(errors or "").lower()

    if sdk_fallback_attempted:
        if "hydration_sidecar_quote_backoff" in joined or "quote_backoff" in joined:
            return "sdk_fallback_backoff"
        if "timeout" in joined or "timed out" in joined:
            return "sdk_fallback_timeout"
        if "sidecar_not_available" in joined or "sidecar_not" in joined:
            return "sdk_fallback_unavailable"
        return "sdk_fallback_unavailable"

    if external_attempted:
        if "timeout" in joined or "timed out" in joined:
            return "external_price_timeout"
        if "http_error" in joined or "request" in joined:
            return "external_price_unavailable"
        return "external_price_unavailable"

    return "price_source_unavailable"


def _hydration_price_cache_failure_message(*, failure_mode: str) -> str:
    if str(failure_mode or "").startswith("sdk_fallback"):
        return (
            "One or more Hydration USD prices could not be resolved from the SDK fallback price path. "
            "Returning cached/stable prices and backing off before the next refresh attempt."
        )
    if str(failure_mode or "").startswith("external_price"):
        return (
            "One or more Hydration USD prices could not be resolved from the external USD price source. "
            "Returning cached/stable prices and backing off before the next refresh attempt."
        )
    return (
        "One or more Hydration USD prices could not be resolved from the configured price sources. "
        "Returning cached/stable prices and backing off before the next refresh attempt."
    )


def _hydration_price_cache_payload(*, status: str, requested: List[str]) -> Dict[str, Any]:
    prices = dict(_hydration_usd_price_cache.get("prices") or {})
    sources = dict(_hydration_usd_price_cache.get("sources") or {})
    errors = list(_hydration_usd_price_cache.get("errors") or [])
    now = time.monotonic()
    status_detail = _hydration_price_cache_status_detail(status=status, requested=requested, prices=prices)
    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "network": "hydration",
        "status": status,
        "statusDetail": status_detail,
        "requested": requested,
        "prices_usd": {k: v for k, v in prices.items() if k in requested or k in _HYDRATION_STABLE_USD_SYMBOLS},
        "usd_prices": {k: v for k, v in prices.items() if k in requested or k in _HYDRATION_STABLE_USD_SYMBOLS},
        "priceSources": {k: v for k, v in sources.items() if k in requested or k in _HYDRATION_STABLE_USD_SYMBOLS},
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
            "classification": status_detail.get("classification"),
            "source_state": status_detail.get("source_state"),
            "in_error_backoff": bool(status_detail.get("in_error_backoff")),
            "seconds_until_expiry": status_detail.get("seconds_until_expiry"),
            "seconds_until_retry": status_detail.get("seconds_until_retry"),
            "has_all_requested": bool(status_detail.get("has_all_requested")),
            "missing_prices": status_detail.get("missing_prices"),
            "last_error": _hydration_usd_price_cache.get("last_error"),
        },
    }


def _hydration_price_cache_force_isolated() -> bool:
    return bool(
        _HYDRATION_PRICE_CACHE_FORCE_ISOLATED_HELPER
        or not _HYDRATION_PRICE_CACHE_USE_SIDECAR
    )


def _hydration_sdk_spot_orderbook_force_isolated(route_mode: str) -> bool:
    return bool(
        _hydration_route_mode(route_mode) == "isolated_helper"
        or _HYDRATION_SDK_SPOT_ORDERBOOK_FORCE_ISOLATED_HELPER
    )


async def _hydration_sdk_spot_for_orderbook(
    *,
    raw_symbol: str,
    base: str,
    quote: str,
    asset_in: Dict[str, Any],
    asset_out: Dict[str, Any],
    step_timeout_s: Optional[float] = None,
    route_mode: str = "sdk_spot",
    implementation: Optional[str] = None,
) -> Dict[str, Any]:
    impl = str(implementation or _HYDRATION_SDK_SPOT_ORDERBOOK_IMPLEMENTATION or "direct").strip().lower()
    if impl not in {"direct", "context"}:
        impl = "direct"
    mode = "price_spot_direct" if impl == "direct" else "price_spot"
    force_isolated = _hydration_sdk_spot_orderbook_force_isolated(route_mode)
    payload = {
        "mode": mode,
        "venue": "polkadot_hydration",
        "rawSymbol": raw_symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base,
        "quote": quote,
        "assetIn": _helper_asset_payload(asset_in),
        "assetOut": _helper_asset_payload(asset_out),
        "stepTimeoutS": float(step_timeout_s if step_timeout_s is not None else _HYDRATION_ORDERBOOK_STEP_TIMEOUT_S),
        "sdkUseCase": "orderbook",
        "routeMode": _hydration_route_mode(route_mode),
        "enableRouterQuotes": bool(_hydration_router_quotes_enabled_for_payload({
            "mode": mode,
            "sdkUseCase": "orderbook",
            "routeMode": _hydration_route_mode(route_mode),
        })),
        "forceIsolatedHelper": bool(force_isolated),
        "sdkSpotOrderbook": True,
        "sdkSpotOrderbookImplementation": impl,
        "sdkSpotOrderbookMinDailyVolumeUsd": float(_HYDRATION_SDK_SPOT_ORDERBOOK_MIN_DAILY_VOLUME_USD),
        "sdkSpotOrderbookPoolTypes": _HYDRATION_SDK_SPOT_ORDERBOOK_POOL_TYPES_CSV,
    }
    return await _run_hydration_helper(payload, force_isolated=bool(force_isolated))


async def _hydration_sdk_spot_orderbook_response(
    *,
    symbol: str,
    base: str,
    quote: str,
    base_meta: Dict[str, Any],
    quote_meta: Dict[str, Any],
    depth: int,
    route_mode_norm: str,
    orderbook_config: Dict[str, Any],
    sample_errors: Optional[List[Dict[str, Any]]] = None,
    fallback_reason: str = "sdk_spot_orderbook",
) -> Optional[Dict[str, Any]]:
    if not _HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK:
        return None

    spot_result = await _hydration_sdk_spot_for_orderbook(
        raw_symbol=symbol,
        base=base,
        quote=quote,
        asset_in=base_meta,
        asset_out=quote_meta,
        step_timeout_s=float((orderbook_config or {}).get("stepTimeoutS") or _HYDRATION_ORDERBOOK_STEP_TIMEOUT_S),
        route_mode=route_mode_norm,
    )
    mid_price = _hydration_price_from_spot(spot_result)
    if mid_price is None or mid_price <= 0:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "hydration_sdk_spot_price_empty",
                "message": "SDK getSpotPrice returned no usable positive price for this pair.",
                "venue": "polkadot_hydration",
                "rawSymbol": symbol,
                "resolvedSymbol": f"{base}-{quote}",
                "base": base_meta,
                "quote": quote_meta,
                "spotResult": spot_result,
            },
        )

    bids, asks = _hydration_synthetic_spot_levels(
        base=base,
        base_meta=base_meta,
        quote_meta=quote_meta,
        mid_price=float(mid_price),
        depth=depth,
    )
    asks.sort(key=lambda x: float(x.get("price") or 0.0))
    bids.sort(key=lambda x: -float(x.get("price") or 0.0))
    price_decimals = _suggest_price_decimals(asks + bids, int((quote_meta or {}).get("decimals") or 0))
    size_decimals = min(int((base_meta or {}).get("decimals") or 0), 8)
    cfg = dict(orderbook_config or {})
    cfg.update({
        "routeModeEffective": "sdk_spot",
        "source": "sdk_spot_orderbook",
        "fallbackReason": fallback_reason,
        "sdkSpotOrderbook": True,
        "sdkSpotOrderbookImplementation": _HYDRATION_SDK_SPOT_ORDERBOOK_IMPLEMENTATION,
        "sdkSpotOrderbookForceIsolatedHelper": _hydration_sdk_spot_orderbook_force_isolated(route_mode_norm),
        "sdkSpotOrderbookMinDailyVolumeUsd": float(_HYDRATION_SDK_SPOT_ORDERBOOK_MIN_DAILY_VOLUME_USD),
        "sdkSpotOrderbookPoolTypes": _HYDRATION_SDK_SPOT_ORDERBOOK_POOL_TYPES_CSV,
        "spotPrice": float(mid_price),
        "tradable": bool(_HYDRATION_SDK_SPOT_ORDERBOOK_TRADABLE),
        "tradeRequiresSwapPreflight": True,
        "tradeRequiresRealRouterQuote": not bool(_HYDRATION_SDK_SPOT_ORDERBOOK_TRADABLE),
    })

    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "router": "galactic_sdk_next_spot_price",
        "routeMode": route_mode_norm,
        "routeModeEffective": "sdk_spot",
        "sdkSpotOrderbook": True,
        "spotPriceOnly": True,
        "tradable": bool(_HYDRATION_SDK_SPOT_ORDERBOOK_TRADABLE),
        "tradeRequiresSwapPreflight": True,
        "tradeRequiresRealRouterQuote": not bool(_HYDRATION_SDK_SPOT_ORDERBOOK_TRADABLE),
        "rawSymbol": symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base,
        "quote": quote,
        "baseAssetId": (base_meta or {}).get("assetId"),
        "quoteAssetId": (quote_meta or {}).get("assetId"),
        "baseDecimals": int((base_meta or {}).get("decimals") or 0),
        "quoteDecimals": int((quote_meta or {}).get("decimals") or 0),
        "baseMeta": base_meta,
        "quoteMeta": quote_meta,
        "priceDecimals": price_decimals,
        "displayPriceDecimals": max(1, min(price_decimals, 8)),
        "sizeDecimals": size_decimals,
        "midPrice": float(mid_price),
        "spotResult": spot_result,
        "orderbookConfig": cfg,
        "bids": bids,
        "asks": asks,
        "sampleErrors": sample_errors or [],
    }


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
                sources["UTTT"] = f"derived:UTTT-HDXxHDX-USD:{src}:{hdx_src}"
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
        sdk_fallback_attempted = False
        external_attempted = bool(_HYDRATION_ENABLE_EXTERNAL_USD_PRICES)
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
                sdk_fallback_attempted = True
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
                                    hdx_src = "sdk:HDX-DOTxDOT-USD"
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
                failure_mode = _hydration_price_cache_failure_mode(
                    errors=errors,
                    sdk_fallback_attempted=sdk_fallback_attempted,
                    external_attempted=external_attempted,
                )
                last_error = {
                    "error": "hydration_usd_price_cache_partial",
                    "message": _hydration_price_cache_failure_message(failure_mode=failure_mode),
                    "failureMode": failure_mode,
                    "classification": "partial_stale",
                    "missing": missing_requested,
                    "sdkFallbackEnabled": bool(_HYDRATION_PRICE_CACHE_USE_SDK_FALLBACK),
                    "sdkFallbackAttempted": bool(sdk_fallback_attempted),
                    "externalUsdPricesEnabled": bool(_HYDRATION_ENABLE_EXTERNAL_USD_PRICES),
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
            failure_mode = _hydration_price_cache_failure_mode(
                errors=errors + [{"error": type(e).__name__, "message": str(e), "detail": getattr(e, "detail", None)}],
                sdk_fallback_attempted=bool(_HYDRATION_PRICE_CACHE_USE_SDK_FALLBACK and _HYDRATION_ENABLE_SDK_PRICE_CACHE),
                external_attempted=bool(_HYDRATION_ENABLE_EXTERNAL_USD_PRICES),
            )
            _hydration_usd_price_cache.update({
                "errors": errors[-12:],
                "error_until": now + max(30.0, float(_HYDRATION_PRICE_CACHE_ERROR_BACKOFF_S)),
                "last_error": {
                    "error": type(e).__name__,
                    "message": str(e),
                    "detail": getattr(e, "detail", None),
                    "failureMode": failure_mode,
                    "classification": "refresh_failed_stale",
                    "sdkFallbackEnabled": bool(_HYDRATION_PRICE_CACHE_USE_SDK_FALLBACK),
                    "externalUsdPricesEnabled": bool(_HYDRATION_ENABLE_EXTERNAL_USD_PRICES),
                    "externalSource": _HYDRATION_EXTERNAL_USD_PRICE_SOURCE,
                },
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
        "sdkUseCase": "order_ticket",
        "enableRouterQuotes": False,
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
        payload["enableRouterQuotes"] = bool(_hydration_router_quotes_enabled_for_payload(payload))

    if not (isinstance(manual_custom_swap, dict) and manual_custom_swap.get("enabled")):
        payload["enableRouterQuotes"] = bool(_hydration_router_quotes_enabled_for_payload(payload))

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
            confirmed INTEGER DEFAULT 0,
            tested_at TEXT,
            last_test_tx_hash TEXT,
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
        "confirmed": "INTEGER DEFAULT 0",
        "tested_at": "TEXT",
        "last_test_tx_hash": "TEXT",
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

    route_mode = str(r.get("route_mode") or "manual_xyk").strip().lower()
    if route_mode in {"manual", "xyk"}:
        route_mode = "manual_xyk"
    if route_mode in {"router", "manual_router_fallback"}:
        route_mode = "manual_router"
    if route_mode not in {"manual_xyk", "manual_router"}:
        return None

    pool_type_raw = str(r.get("pool_type") or ("Router" if route_mode == "manual_router" else "XYK")).strip()
    pool_type_norm = pool_type_raw.lower()
    is_manual_router = route_mode == "manual_router" or pool_type_norm in {"router", "manual_router", "manual router"}
    if is_manual_router:
        route_mode = "manual_router"
        pool_type = "Router"
    else:
        pool_type = "XYK"
        if pool_type_norm != "xyk":
            return None

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

    if route_mode == "manual_router":
        if not isinstance(route, list) or not route:
            return None
        confirmed = bool(int(r.get("confirmed") if r.get("confirmed") is not None else 0))
        return {
            "pair": pair,
            "routeMode": "manual_router",
            "pool": "Router",
            "poolType": pool_type,
            "route": route,
            "poolAccount": str(r.get("pool_account") or "").strip() or None,
            "source": "db:hydration_route_registry:manual_router" + (":reversed" if reverse else ""),
            "sourcePair": source_symbol,
            "routeRegistryId": r.get("id"),
            "routeRegistrySymbol": source_symbol,
            "executionConfirmed": confirmed,
            "confirmed": confirmed,
            "testedAt": r.get("tested_at"),
            "lastTestTxHash": r.get("last_test_tx_hash"),
            "note": r.get("note") or "Manual Hydration Router route registry entry.",
        }

    base_reserve = _float_or_none(r.get("base_reserve"))
    quote_reserve = _float_or_none(r.get("quote_reserve"))
    if base_reserve is None or quote_reserve is None:
        return None
    if reverse:
        base_reserve, quote_reserve = quote_reserve, base_reserve

    return {
        "pair": pair,
        "routeMode": "manual_xyk",
        "baseReserve": float(base_reserve),
        "quoteReserve": float(quote_reserve),
        "feeBps": float(_float_or_none(r.get("fee_bps")) or 30.0),
        "pool": "XYK",
        "poolType": "XYK",
        "route": route,
        "poolAccount": str(r.get("pool_account") or "").strip() or None,
        "source": "db:hydration_route_registry" + (":reversed" if reverse else ""),
        "sourcePair": source_symbol,
        "routeRegistryId": r.get("id"),
        "routeRegistrySymbol": source_symbol,
        "executionConfirmed": True,
        "confirmed": True,
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




def _hydration_route_registry_manual_router_config(
    *,
    db: Optional[Session],
    base: str,
    quote: str,
    asset_in_id: int,
    asset_out_id: int,
) -> Optional[Dict[str, Any]]:
    """Return a DB-backed manual Router route oriented to asset_in -> asset_out.

    This is the v2 route-registry path for Hydration multi-leg routes such as
    DOT -> aDOT -> HDX. It keeps routing assets and confirmation state in the UI
    instead of hard-coded backend defaults.
    """
    if db is None:
        return None
    pair = f"{str(base or '').upper()}-{str(quote or '').upper()}"
    reverse_pair = f"{str(quote or '').upper()}-{str(base or '').upper()}"
    try:
        _ensure_hydration_route_registry_table(db)
        for symbol, reverse in ((pair, False), (reverse_pair, True)):
            row = db.execute(
                text("""
                    SELECT * FROM hydration_route_registry
                    WHERE UPPER(symbol) = :symbol
                      AND COALESCE(enabled, 1) = 1
                    LIMIT 1
                """),
                {"symbol": symbol},
            ).mappings().first()
            if not row:
                continue
            cfg = _hydration_route_registry_row_to_cfg(row, pair=pair, reverse=reverse)
            if not isinstance(cfg, dict) or str(cfg.get("routeMode") or "").lower() != "manual_router":
                continue
            route = _normalize_manual_router_route(
                cfg.get("route"),
                asset_in_id=int(asset_in_id),
                asset_out_id=int(asset_out_id),
            )
            if not route:
                continue
            out = dict(cfg)
            out["pair"] = pair
            out["route"] = route
            out["assetInId"] = int(asset_in_id)
            out["assetOutId"] = int(asset_out_id)
            return out
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


def _manual_router_fallback_pairs_from_csv(raw_csv: str) -> set[str]:
    out: set[str] = set()
    for raw in str(raw_csv or "").split(","):
        pair = str(raw or "").strip().upper()
        if not pair or "-" not in pair:
            continue
        try:
            left, right = _parse_symbol(pair)
            out.add(f"{left}-{right}")
        except Exception:
            continue
    return out


def _manual_router_fallback_allowed_pairs() -> set[str]:
    return _manual_router_fallback_pairs_from_csv(_HYDRATION_MANUAL_ROUTER_FALLBACK_PAIRS_CSV)


def _manual_router_fallback_confirmed_pairs() -> set[str]:
    return _manual_router_fallback_pairs_from_csv(_HYDRATION_MANUAL_ROUTER_FALLBACK_CONFIRMED_PAIRS_CSV)


def _manual_router_fallback_pair_allowed(base: str, quote: str) -> bool:
    pair = f"{str(base or '').upper()}-{str(quote or '').upper()}"
    return pair in _manual_router_fallback_allowed_pairs()


def _manual_router_fallback_pair_confirmed(base: str, quote: str) -> bool:
    pair = f"{str(base or '').upper()}-{str(quote or '').upper()}"
    return pair in _manual_router_fallback_confirmed_pairs()


def _manual_router_fallback_routes_map() -> Dict[str, List[Dict[str, Any]]]:
    try:
        raw = json.loads(_HYDRATION_MANUAL_ROUTER_FALLBACK_ROUTES_JSON or "{}")
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}

    out: Dict[str, List[Dict[str, Any]]] = {}
    for key, route in raw.items():
        try:
            base, quote = _parse_symbol(str(key or ""))
        except Exception:
            continue
        if not isinstance(route, list) or not route:
            continue
        clean_route = [dict(leg) for leg in route if isinstance(leg, dict)]
        if clean_route:
            out[f"{base}-{quote}"] = clean_route
    return out


_HYDRATION_MANUAL_ROUTER_POOL_TYPES = {"XYK", "LBP", "Stableswap", "Omnipool", "Aave", "HSM"}
_HYDRATION_MANUAL_ROUTER_POOL_ALIASES = {
    "xyk": "XYK",
    "lbp": "LBP",
    "stableswap": "Stableswap",
    "stable_swap": "Stableswap",
    "stable swap": "Stableswap",
    "stable": "Stableswap",
    "omnipool": "Omnipool",
    "omni": "Omnipool",
    "aave": "Aave",
    "hsm": "HSM",
}


def _manual_router_pool_type_raw(pool: Any) -> str:
    if isinstance(pool, dict):
        return str(
            pool.get("type")
            or pool.get("value")
            or pool.get("name")
            or pool.get("poolType")
            or pool.get("pool_type")
            or ""
        ).strip()
    return str(pool or "").strip()


def _manual_router_pool_type_canonical(pool: Any) -> str:
    raw = _manual_router_pool_type_raw(pool)
    if not raw:
        raw = str(_HYDRATION_MANUAL_ROUTER_FALLBACK_POOL or "Omnipool").strip()
    return _HYDRATION_MANUAL_ROUTER_POOL_ALIASES.get(raw.lower(), raw)


def _manual_router_pool_payload(pool: Any) -> Dict[str, str]:
    pool_type = _manual_router_pool_type_canonical(pool)
    if not pool_type:
        pool_type = "Omnipool"
    return {"type": pool_type}


def _manual_router_route_shape_hints() -> Dict[str, Any]:
    return {
        "source": "Metadata v15 focused scan",
        "routeLeg": {
            "pool": "hydradx_traits::router::PoolType<AssetId>",
            "assetIn": "AssetId",
            "assetOut": "AssetId",
        },
        "routerSell": {
            "asset_in": "AssetId",
            "asset_out": "AssetId",
            "amount_in": "Balance",
            "min_amount_out": "Balance",
            "route": "Route<AssetId>",
        },
        "routerBuy": {
            "asset_in": "AssetId",
            "asset_out": "AssetId",
            "amount_out": "Balance",
            "max_amount_in": "Balance",
            "route": "Route<AssetId>",
        },
        "observedPoolTypeNames": sorted(_HYDRATION_MANUAL_ROUTER_POOL_TYPES),
        "stableswapNote": "Metadata shows Stableswap carries an AssetId payload. Keep Stableswap route rows diagnostic-only until the JS manual route builder preserves that payload shape.",
    }


def _validate_manual_router_route(
    route: Any,
    *,
    asset_in_id: int,
    asset_out_id: int,
) -> Dict[str, Any]:
    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    out: List[Dict[str, Any]] = []

    if not isinstance(route, list) or not route:
        return {
            "ok": False,
            "route": None,
            "errors": [{"error": "manual_router_route_required", "message": "manual_router route_json must be a non-empty list of route legs."}],
            "warnings": warnings,
            "shapeHints": _manual_router_route_shape_hints(),
        }

    for idx, leg in enumerate(route):
        if not isinstance(leg, dict):
            errors.append({"error": "manual_router_route_leg_not_object", "index": idx, "leg": leg})
            continue

        leg_in = _route_leg_asset_value(leg, "assetIn", "asset_in")
        leg_out = _route_leg_asset_value(leg, "assetOut", "asset_out")
        if leg_in is None:
            errors.append({"error": "manual_router_route_leg_missing_asset_in", "index": idx, "leg": leg})
        if leg_out is None:
            errors.append({"error": "manual_router_route_leg_missing_asset_out", "index": idx, "leg": leg})
        if leg_in is None or leg_out is None:
            continue
        if int(leg_in) == int(leg_out):
            errors.append({"error": "manual_router_route_leg_same_asset", "index": idx, "assetId": int(leg_in), "leg": leg})

        pool_raw = _manual_router_pool_type_raw(leg.get("pool"))
        pool_payload = _manual_router_pool_payload(leg.get("pool"))
        pool_type = str(pool_payload.get("type") or "").strip()
        if pool_type not in _HYDRATION_MANUAL_ROUTER_POOL_TYPES:
            errors.append({
                "error": "manual_router_route_pool_type_unsupported",
                "index": idx,
                "poolType": pool_raw or pool_type,
                "canonicalPoolType": pool_type,
                "supportedPoolTypes": sorted(_HYDRATION_MANUAL_ROUTER_POOL_TYPES),
                "leg": leg,
            })

        if pool_type == "Stableswap":
            warnings.append({
                "warning": "manual_router_stableswap_payload_not_execution_hardened",
                "index": idx,
                "message": "Metadata shows PoolType::Stableswap carries an AssetId. The current JS manual route builder normalizes pools to type-only variants, so Stableswap rows should remain diagnostic-only until builder support is added.",
                "leg": leg,
            })

        out.append({
            "pool": pool_payload,
            "assetIn": int(leg_in),
            "assetOut": int(leg_out),
        })

    if not out:
        errors.append({"error": "manual_router_route_no_valid_legs"})

    if out:
        first_in = _route_leg_asset_value(out[0], "assetIn", "asset_in")
        last_out = _route_leg_asset_value(out[-1], "assetOut", "asset_out")
        if first_in != int(asset_in_id):
            errors.append({
                "error": "manual_router_route_wrong_start_asset",
                "expectedAssetInId": int(asset_in_id),
                "actualAssetInId": first_in,
                "firstLeg": out[0],
            })
        if last_out != int(asset_out_id):
            errors.append({
                "error": "manual_router_route_wrong_end_asset",
                "expectedAssetOutId": int(asset_out_id),
                "actualAssetOutId": last_out,
                "lastLeg": out[-1],
            })

    for idx in range(max(0, len(out) - 1)):
        cur_out = _route_leg_asset_value(out[idx], "assetOut", "asset_out")
        next_in = _route_leg_asset_value(out[idx + 1], "assetIn", "asset_in")
        if cur_out != next_in:
            errors.append({
                "error": "manual_router_route_disconnected_legs",
                "index": idx,
                "currentAssetOut": cur_out,
                "nextAssetIn": next_in,
                "currentLeg": out[idx],
                "nextLeg": out[idx + 1],
            })

    if len(out) > 8:
        warnings.append({
            "warning": "manual_router_route_many_legs",
            "legCount": len(out),
            "message": "Metadata exposes Router.MaxTradesExceeded; keep route rows short unless a tiny live confirmation has proven this path.",
        })

    return {
        "ok": not errors,
        "route": out if not errors else None,
        "errors": errors,
        "warnings": warnings,
        "legCount": len(out),
        "shapeHints": _manual_router_route_shape_hints(),
    }


def _normalize_manual_router_route(
    route: Any,
    *,
    asset_in_id: int,
    asset_out_id: int,
) -> Optional[List[Dict[str, Any]]]:
    checked = _validate_manual_router_route(
        route,
        asset_in_id=int(asset_in_id),
        asset_out_id=int(asset_out_id),
    )
    clean = checked.get("route") if checked.get("ok") else None
    return clean if isinstance(clean, list) and clean else None




def _hydration_route_asset_label(
    asset_id: Any,
    *,
    db: Optional[Session],
    base: str,
    quote: str,
    base_meta: Dict[str, Any],
    quote_meta: Dict[str, Any],
) -> str:
    asset_norm = _asset_id_norm_for_compare(asset_id)
    if asset_norm == _asset_id_norm_for_compare((base_meta or {}).get("assetId")):
        return str(base or (base_meta or {}).get("symbol") or asset_norm).upper()
    if asset_norm == _asset_id_norm_for_compare((quote_meta or {}).get("assetId")):
        return str(quote or (quote_meta or {}).get("symbol") or asset_norm).upper()
    if asset_norm == _asset_id_norm_for_compare(_HYDRATION_NATIVE_ASSET_ID):
        return _HYDRATION_NATIVE_SYMBOL or "HDX"

    known_intermediates = {
        "1001": "aDOT",
    }
    if asset_norm in known_intermediates:
        return known_intermediates[asset_norm]

    if db is not None and asset_norm:
        try:
            cols = {
                str(r.get("name") or "")
                for r in db.execute(text("PRAGMA table_info(token_registry)")).mappings().all()
            }
            lookup_cols = [c for c in ("address", "asset_id", "contract_address", "mint", "mint_address") if c in cols]
            if "symbol" in cols and lookup_cols:
                where_sql = " OR ".join([f"CAST({c} AS TEXT) = :asset_id" for c in lookup_cols])
                row = db.execute(
                    text(f"SELECT symbol FROM token_registry WHERE {where_sql} LIMIT 1"),
                    {"asset_id": str(asset_norm)},
                ).mappings().first()
                if row and row.get("symbol"):
                    return str(row.get("symbol")).upper()
        except Exception:
            pass

    return f"asset:{asset_norm}" if asset_norm else "asset:?"


def _hydration_route_asset_sequence(
    route: Any,
    *,
    fallback_asset_in_id: int,
    fallback_asset_out_id: int,
) -> List[int]:
    if not isinstance(route, list) or not route:
        return [int(fallback_asset_in_id), int(fallback_asset_out_id)]

    out: List[int] = []
    first_in = _route_leg_asset_value(route[0], "assetIn", "asset_in")
    if first_in is not None:
        out.append(int(first_in))
    for leg in route:
        if not isinstance(leg, dict):
            continue
        leg_out = _route_leg_asset_value(leg, "assetOut", "asset_out")
        if leg_out is not None:
            out.append(int(leg_out))

    if len(out) >= 2:
        return out
    return [int(fallback_asset_in_id), int(fallback_asset_out_id)]


def _hydration_route_direction_summary(
    route: Any,
    *,
    db: Optional[Session],
    base: str,
    quote: str,
    base_meta: Dict[str, Any],
    quote_meta: Dict[str, Any],
    fallback_asset_in_id: int,
    fallback_asset_out_id: int,
) -> Dict[str, Any]:
    asset_ids = _hydration_route_asset_sequence(
        route,
        fallback_asset_in_id=int(fallback_asset_in_id),
        fallback_asset_out_id=int(fallback_asset_out_id),
    )
    labels = [
        _hydration_route_asset_label(
            asset_id,
            db=db,
            base=base,
            quote=quote,
            base_meta=base_meta,
            quote_meta=quote_meta,
        )
        for asset_id in asset_ids
    ]
    hops: List[Dict[str, Any]] = []
    for idx in range(max(0, len(asset_ids) - 1)):
        hops.append({
            "fromAssetId": int(asset_ids[idx]),
            "toAssetId": int(asset_ids[idx + 1]),
            "from": labels[idx],
            "to": labels[idx + 1],
        })
    return {
        "label": " → ".join(labels),
        "assetIds": asset_ids,
        "labels": labels,
        "hops": hops,
    }


def _hydration_route_registry_validation_payload(
    req: HydrationRouteRegistryUpsertRequest,
    *,
    db: Optional[Session],
) -> Dict[str, Any]:
    try:
        base, quote = _parse_symbol(req.symbol)
    except HTTPException as e:
        return {
            "ok": False,
            "venue": "polkadot_hydration",
            "symbol": str(req.symbol or "").strip().upper(),
            "routeValidation": {
                "ok": False,
                "errors": [{"error": "invalid_symbol", "detail": e.detail}],
                "warnings": [],
                "legCount": 0,
                "route": None,
                "shapeHints": _manual_router_route_shape_hints(),
            },
            "writesDb": False,
        }

    route_mode = str(req.route_mode or "manual_xyk").strip().lower()
    aliases = {
        "manual": "manual_xyk",
        "xyk": "manual_xyk",
        "router": "manual_router",
        "manual router": "manual_router",
        "manual_router_fallback": "manual_router",
    }
    route_mode = aliases.get(route_mode, route_mode)

    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []
    if route_mode not in {"manual_xyk", "manual_router"}:
        errors.append({
            "error": "unsupported_hydration_route_mode",
            "routeMode": req.route_mode,
            "supported": ["manual_xyk", "manual_router"],
        })

    try:
        base_meta = _resolve_asset(base, db=db)
        quote_meta = _resolve_asset(quote, db=db)
    except HTTPException as e:
        return {
            "ok": False,
            "venue": "polkadot_hydration",
            "symbol": f"{base}-{quote}",
            "routeMode": route_mode,
            "base": base,
            "quote": quote,
            "routeValidation": {
                "ok": False,
                "errors": errors + [{"error": "asset_resolution_failed", "detail": e.detail}],
                "warnings": warnings,
                "legCount": 0,
                "route": None,
                "shapeHints": _manual_router_route_shape_hints(),
            },
            "writesDb": False,
        }

    asset_in_id = _hydration_sdk_asset_id(base_meta)
    asset_out_id = _hydration_sdk_asset_id(quote_meta)
    pool_type = "Router" if route_mode == "manual_router" else "XYK"

    if route_mode == "manual_router":
        pool_type_raw = str(req.pool_type or "Router").strip()
        if pool_type_raw.lower() not in {"router", "manual_router", "manual router"}:
            errors.append({
                "error": "unsupported_hydration_manual_router_pool_type",
                "poolType": req.pool_type,
                "supported": ["Router"],
            })

        route_validation = _validate_manual_router_route(
            req.route_json,
            asset_in_id=int(asset_in_id),
            asset_out_id=int(asset_out_id),
        )
        if errors:
            route_validation = {
                **route_validation,
                "ok": False,
                "errors": errors + list(route_validation.get("errors") or []),
            }
    else:
        pool_type_raw = str(req.pool_type or "XYK").strip()
        if pool_type_raw.lower() != "xyk":
            errors.append({
                "error": "unsupported_hydration_manual_pool_type",
                "poolType": req.pool_type,
                "supported": ["XYK", "Router"],
            })

        base_reserve = _float_or_none(req.base_reserve)
        quote_reserve = _float_or_none(req.quote_reserve)
        if base_reserve is None or quote_reserve is None:
            errors.append({
                "error": "manual_xyk_reserves_required",
                "message": "manual_xyk routes require positive base_reserve and quote_reserve.",
            })

        route = req.route_json
        if not isinstance(route, list) or not route:
            route = [{
                "pool": {"type": "XYK"},
                "assetIn": int(asset_in_id),
                "assetOut": int(asset_out_id),
            }]
        route_validation = {
            "ok": not errors,
            "route": route if not errors else None,
            "errors": errors,
            "warnings": warnings,
            "legCount": len(route) if isinstance(route, list) else 0,
            "shapeHints": {
                "manualXyk": {
                    "baseReserve": "required positive human-unit reserve for BASE",
                    "quoteReserve": "required positive human-unit reserve for QUOTE",
                    "feeBps": "pool fee in basis points",
                },
                **_manual_router_route_shape_hints(),
            },
        }

    clean_route = route_validation.get("route") if route_validation.get("ok") else (req.route_json or [])
    direction = _hydration_route_direction_summary(
        clean_route,
        db=db,
        base=base,
        quote=quote,
        base_meta=base_meta,
        quote_meta=quote_meta,
        fallback_asset_in_id=int(asset_in_id),
        fallback_asset_out_id=int(asset_out_id),
    )

    return {
        "ok": bool(route_validation.get("ok")),
        "venue": "polkadot_hydration",
        "symbol": f"{base}-{quote}",
        "routeMode": route_mode,
        "poolType": pool_type,
        "base": base,
        "quote": quote,
        "baseAssetId": int(asset_in_id),
        "quoteAssetId": int(asset_out_id),
        "baseMeta": base_meta,
        "quoteMeta": quote_meta,
        "enabled": bool(req.enabled),
        "confirmed": bool(req.confirmed),
        "routeValidation": route_validation,
        "direction": direction,
        "writesDb": False,
        "note": "Validate-only endpoint; no route registry row was inserted or updated.",
    }



def _hydration_route_registry_reverse_preview_payload(
    req: HydrationRouteRegistryUpsertRequest,
    *,
    db: Optional[Session],
) -> Dict[str, Any]:
    # Reverse-preview path for Route Registry UI. This intentionally does not
    # insert, update, delete, sign, build swaps, or submit transactions.
    try:
        base, quote = _parse_symbol(req.symbol)
    except HTTPException as e:
        return {
            "ok": False,
            "venue": "polkadot_hydration",
            "symbol": str(req.symbol or "").strip().upper(),
            "error": "invalid_symbol",
            "detail": e.detail,
            "writesDb": False,
        }

    mode_raw = str(req.route_mode or "manual_xyk").strip().lower()
    aliases = {
        "manual": "manual_xyk",
        "xyk": "manual_xyk",
        "router": "manual_router",
        "manual router": "manual_router",
        "manual_router_fallback": "manual_router",
    }
    route_mode = aliases.get(mode_raw, mode_raw)
    reversed_symbol = f"{quote}-{base}"

    original_validation = _hydration_route_registry_validation_payload(req, db=db)

    reversed_route = req.route_json
    if route_mode == "manual_router":
        reversed_route = _reverse_hydration_route_legs(req.route_json or [])
    elif route_mode == "manual_xyk":
        # XYK rows are reserve-oriented, so the reserves are swapped below.  The
        # route JSON is kept empty/default unless the operator supplied one.
        reversed_route = req.route_json

    reversed_req = HydrationRouteRegistryUpsertRequest(
        symbol=reversed_symbol,
        route_mode=route_mode,
        base_reserve=req.quote_reserve,
        quote_reserve=req.base_reserve,
        fee_bps=req.fee_bps,
        enabled=req.enabled,
        # Do not auto-confirm a mechanically reversed route.  Confirmation stays
        # an explicit operator action after a tiny live on-chain success.
        confirmed=False,
        pool_type=req.pool_type,
        pool_account=req.pool_account,
        route_json=reversed_route if isinstance(reversed_route, list) and reversed_route else None,
        tested_at=None,
        last_test_tx_hash=None,
        note=req.note,
    )
    reversed_validation = _hydration_route_registry_validation_payload(reversed_req, db=db)

    return {
        "ok": bool(reversed_validation.get("ok")),
        "venue": "polkadot_hydration",
        "symbol": reversed_symbol,
        "routeMode": reversed_validation.get("routeMode") or route_mode,
        "poolType": reversed_validation.get("poolType"),
        "base": reversed_validation.get("base"),
        "quote": reversed_validation.get("quote"),
        "baseAssetId": reversed_validation.get("baseAssetId"),
        "quoteAssetId": reversed_validation.get("quoteAssetId"),
        "enabled": bool(req.enabled),
        "confirmed": False,
        "routeValidation": reversed_validation.get("routeValidation") or {},
        "direction": reversed_validation.get("direction"),
        "reversedPayload": {
            "symbol": reversed_symbol,
            "route_mode": route_mode,
            "base_reserve": req.quote_reserve,
            "quote_reserve": req.base_reserve,
            "fee_bps": req.fee_bps,
            "enabled": bool(req.enabled),
            "confirmed": False,
            "pool_type": reversed_validation.get("poolType") or req.pool_type,
            "pool_account": req.pool_account,
            "route_json": (reversed_validation.get("routeValidation") or {}).get("route") or reversed_route or [],
            "note": req.note,
        },
        "original": {
            "symbol": f"{base}-{quote}",
            "routeValidation": original_validation.get("routeValidation") or {},
            "direction": original_validation.get("direction"),
            "ok": bool(original_validation.get("ok")),
        },
        "sourceType": "reverse_preview",
        "sourceLabel": "Reverse preview",
        "warningLevel": "warn",
        "warnings": [
            "Reverse preview clears Confirmed intentionally. Validate and live-test this exact direction before marking it confirmed."
        ],
        "recommendedNextAction": "Validate the reversed route, then save with Confirmed unchecked unless this direction was already live-tested.",
        "loadSafety": {
            "clearsConfirmed": True,
            "writesDb": False,
            "requiresValidateBeforeSave": True,
            "requiresLiveTestBeforeConfirmed": True,
        },
        "writesDb": False,
        "note": "Reverse-preview endpoint; no route registry row was inserted or updated.",
    }


def _manual_router_fallback_configured_route(
    *,
    base: str,
    quote: str,
    asset_in_id: int,
    asset_out_id: int,
    db: Optional[Session] = None,
) -> Optional[List[Dict[str, Any]]]:
    db_cfg = _hydration_route_registry_manual_router_config(
        db=db,
        base=base,
        quote=quote,
        asset_in_id=int(asset_in_id),
        asset_out_id=int(asset_out_id),
    )
    if isinstance(db_cfg, dict):
        route = _normalize_manual_router_route(
            db_cfg.get("route"),
            asset_in_id=int(asset_in_id),
            asset_out_id=int(asset_out_id),
        )
        if route:
            return route

    pair = f"{str(base or '').upper()}-{str(quote or '').upper()}"
    routes = _manual_router_fallback_routes_map()

    direct = _normalize_manual_router_route(
        routes.get(pair),
        asset_in_id=int(asset_in_id),
        asset_out_id=int(asset_out_id),
    )
    if direct:
        return direct

    # Env fallback only: if one direction was configured, try a mechanically
    # reversed version. DB route-registry confirmation still needs each live
    # direction to be explicitly confirmed before normal signing.
    reverse_pair = f"{str(quote or '').upper()}-{str(base or '').upper()}"
    reverse_route = _reverse_hydration_route_legs(routes.get(reverse_pair))
    return _normalize_manual_router_route(
        reverse_route,
        asset_in_id=int(asset_in_id),
        asset_out_id=int(asset_out_id),
    )


def _manual_router_fallback_route(
    asset_in_id: int,
    asset_out_id: int,
    *,
    base: Optional[str] = None,
    quote: Optional[str] = None,
    db: Optional[Session] = None,
) -> List[Dict[str, Any]]:
    if base and quote:
        configured = _manual_router_fallback_configured_route(
            base=base,
            quote=quote,
            asset_in_id=int(asset_in_id),
            asset_out_id=int(asset_out_id),
            db=db,
        )
        if configured:
            return configured

    return [{
        "pool": _manual_router_pool_payload(_HYDRATION_MANUAL_ROUTER_FALLBACK_POOL or "Omnipool"),
        "assetIn": int(asset_in_id),
        "assetOut": int(asset_out_id),
    }]


async def _hydration_manual_router_fallback_diagnostics(
    *,
    db: Optional[Session],
    base: str,
    quote: str,
    side: str,
    amount_ui: float,
    amount_mode: str,
    slippage_bps: int,
    base_meta: Dict[str, Any],
    quote_meta: Dict[str, Any],
) -> Dict[str, Any]:
    """Return an explainable preflight report for the manual Router fallback.

    This is intentionally diagnostic-only.  The real swap plan is still built by
    _hydration_manual_router_fallback_plan so we do not create a second execution
    path.  The goal is to make /swap_tx failures explain why the fallback did or
    did not attach before the SDK-router guard.
    """
    pair = f"{str(base or '').upper()}-{str(quote or '').upper()}"
    side_norm = str(side or "").strip().lower()
    mode = str(amount_mode or "").strip().lower()
    diag: Dict[str, Any] = {
        "enabled": bool(_HYDRATION_ENABLE_MANUAL_ROUTER_FALLBACK),
        "pair": pair,
        "side": side_norm,
        "amountMode": mode,
        "amount": float(amount_ui) if _float_or_none(amount_ui) is not None else amount_ui,
        "slippageBps": int(slippage_bps),
        "allowedPairs": sorted(_manual_router_fallback_allowed_pairs()),
        "confirmedPairs": sorted(_manual_router_fallback_confirmed_pairs()),
        "allowUnconfirmed": bool(_HYDRATION_ALLOW_UNCONFIRMED_MANUAL_ROUTER_FALLBACK),
        "maxInputUsd": float(_HYDRATION_MANUAL_ROUTER_FALLBACK_MAX_INPUT_USD),
        "eligibleSellExactIn": bool(side_norm == "sell" and mode == "exact_in"),
        "eligibleBuyExactOut": bool(side_norm == "buy" and mode == "exact_out"),
        "pairAllowed": bool(_manual_router_fallback_pair_allowed(base, quote)),
        "pairConfirmed": bool(_manual_router_fallback_pair_confirmed(base, quote)),
        "attached": False,
        "ready": False,
        "reason": None,
    }

    if not _HYDRATION_ENABLE_MANUAL_ROUTER_FALLBACK:
        diag["reason"] = "manual_router_fallback_disabled"
        return diag
    if not ((side_norm == "sell" and mode == "exact_in") or (side_norm == "buy" and mode == "exact_out")):
        diag["reason"] = "manual_router_fallback_requires_sell_exact_in_or_buy_exact_out"
        return diag

    try:
        if side_norm == "buy":
            asset_in_id = _hydration_sdk_asset_id(quote_meta)
            asset_out_id = _hydration_sdk_asset_id(base_meta)
            asset_in_symbol = str((quote_meta or {}).get("symbol") or quote or "").upper()
            asset_out_symbol = str((base_meta or {}).get("symbol") or base or "").upper()
            route_base = quote
            route_quote = base
        else:
            asset_in_id = _hydration_sdk_asset_id(base_meta)
            asset_out_id = _hydration_sdk_asset_id(quote_meta)
            asset_in_symbol = str((base_meta or {}).get("symbol") or base or "").upper()
            asset_out_symbol = str((quote_meta or {}).get("symbol") or quote or "").upper()
            route_base = base
            route_quote = quote

        diag["routeDirection"] = f"{str(route_base or '').upper()}-{str(route_quote or '').upper()}"
        diag["pairAllowed"] = bool(_manual_router_fallback_pair_allowed(route_base, route_quote))
        diag["pairConfirmed"] = bool(_manual_router_fallback_pair_confirmed(route_base, route_quote))
        if not diag["pairAllowed"]:
            diag["reason"] = "manual_router_fallback_pair_not_allowlisted"
            return diag
        configured_route = _manual_router_fallback_configured_route(
            base=route_base,
            quote=route_quote,
            asset_in_id=int(asset_in_id),
            asset_out_id=int(asset_out_id),
            db=db,
        )
        route = _manual_router_fallback_route(
            asset_in_id,
            asset_out_id,
            base=route_base,
            quote=route_quote,
            db=db,
        )
        diag.update({
            "assetInId": int(asset_in_id),
            "assetOutId": int(asset_out_id),
            "assetInSymbol": asset_in_symbol,
            "assetOutSymbol": asset_out_symbol,
            "configuredRouteAvailable": bool(configured_route),
            "routeSource": "manual_router_fallback_routes_json" if configured_route else "manual_router_fallback_generated",
            "route": route,
        })

        price_payload = await _hydration_refresh_usd_price_cache(
            db=db,
            requested=[asset_in_symbol, asset_out_symbol],
            force_refresh=False,
            allow_refresh=True,
        )
        prices = dict((price_payload or {}).get("prices_usd") or (price_payload or {}).get("usd_prices") or {})
        sources = dict((price_payload or {}).get("priceSources") or {})
        in_usd = _float_or_none(prices.get(asset_in_symbol))
        out_usd = _float_or_none(prices.get(asset_out_symbol))
        input_usd = float(amount_ui) * float(in_usd) if in_usd is not None else None
        diag.update({
            "priceCacheStatus": (price_payload or {}).get("status"),
            "priceCacheDetail": (price_payload or {}).get("statusDetail"),
            "usdPrices": {
                asset_in_symbol: in_usd,
                asset_out_symbol: out_usd,
            },
            "priceSources": {
                asset_in_symbol: sources.get(asset_in_symbol),
                asset_out_symbol: sources.get(asset_out_symbol),
            },
            "inputUsd": input_usd,
        })

        if in_usd is None or out_usd is None or out_usd <= 0:
            diag["reason"] = "manual_router_fallback_price_missing"
            return diag

        max_input_usd = max(0.0, float(_HYDRATION_MANUAL_ROUTER_FALLBACK_MAX_INPUT_USD))
        if max_input_usd > 0 and input_usd is not None and input_usd > max_input_usd:
            diag["reason"] = "manual_router_fallback_input_too_large"
            return diag

        if not diag["pairConfirmed"] and not _HYDRATION_ALLOW_UNCONFIRMED_MANUAL_ROUTER_FALLBACK:
            diag["reason"] = "manual_router_fallback_pair_unconfirmed"
            return diag

        diag["ready"] = True
        diag["reason"] = "manual_router_fallback_ready"
        return diag
    except HTTPException as e:
        diag["reason"] = "manual_router_fallback_http_error"
        diag["detail"] = e.detail
        return diag
    except Exception as e:
        diag["reason"] = "manual_router_fallback_exception"
        diag["error"] = type(e).__name__
        diag["message"] = str(e)
        return diag


def _hydration_mark_manual_router_diag_attached(
    diag: Optional[Dict[str, Any]],
    manual_custom_swap: Optional[Dict[str, Any]],
) -> None:
    """Normalize diagnostics after a DB/manual Router plan successfully attaches.

    The older diagnostics were originally env allow-list oriented.  Route
    Registry v2 can attach confirmed DB routes even when the env allow-list is
    intentionally empty, so the final response should not keep stale
    pair_not_allowlisted/ready=false wording after a manual Router plan is
    already attached.
    """
    if not isinstance(diag, dict):
        return
    if not (isinstance(manual_custom_swap, dict) and manual_custom_swap.get("manualRouterFallback")):
        return

    execution_confirmed = bool(manual_custom_swap.get("executionConfirmed"))
    pool_source = str(manual_custom_swap.get("poolSource") or "")
    db_backed = pool_source.startswith("db:") or bool(manual_custom_swap.get("routeRegistryId"))

    diag.update({
        "attempted": True,
        "attached": True,
        "ready": execution_confirmed,
        "reason": "manual_router_fallback_ready" if execution_confirmed else "manual_router_fallback_attached_unconfirmed",
        "planReason": "manual_router_fallback_attached",
        "configuredRouteAvailable": True,
        "routeSource": pool_source or diag.get("routeSource"),
        "route": manual_custom_swap.get("route") or diag.get("route"),
        "routeDirection": manual_custom_swap.get("routeDirection") or diag.get("routeDirection"),
        "assetInId": manual_custom_swap.get("assetInId", diag.get("assetInId")),
        "assetOutId": manual_custom_swap.get("assetOutId", diag.get("assetOutId")),
        "assetInSymbol": manual_custom_swap.get("assetInSymbol", diag.get("assetInSymbol")),
        "assetOutSymbol": manual_custom_swap.get("assetOutSymbol", diag.get("assetOutSymbol")),
        "routeRegistryId": manual_custom_swap.get("routeRegistryId") or diag.get("routeRegistryId"),
        "routeRegistrySymbol": manual_custom_swap.get("routeRegistrySymbol") or diag.get("routeRegistrySymbol"),
    })

    if db_backed:
        diag["pairAllowed"] = True
        diag["pairConfirmed"] = execution_confirmed
        diag["allowSource"] = "db:hydration_route_registry:manual_router"
        diag["confirmedSource"] = "db:hydration_route_registry:manual_router" if execution_confirmed else None


def _hydration_quote_status_with_manual_custom_swap(
    quote_status: Dict[str, Any],
    *,
    manual_custom_swap: Optional[Dict[str, Any]],
    side: str,
    amount_mode: str,
) -> Dict[str, Any]:
    """Return request-local quote/swap status with manual route availability.

    Global SDK flags can correctly remain disabled while a confirmed DB manual
    Router route is still executable.  Annotate the request payload so UI/debug
    output does not imply BUY/manual-router swaps are unavailable merely because
    sdk-next router quote loops are disabled.
    """
    out = dict(quote_status or {})
    if not (isinstance(manual_custom_swap, dict) and manual_custom_swap.get("manualRouterFallback")):
        return out

    execution_confirmed = bool(manual_custom_swap.get("executionConfirmed"))
    method = str(manual_custom_swap.get("method") or "").strip().lower()
    side_norm = str(side or "").strip().lower()
    mode_norm = str(amount_mode or "").strip().lower()
    route_exact_buy = bool(side_norm == "buy" and mode_norm == "exact_out" and method == "buy")

    out.update({
        "manualRouterFallbackAvailable": True,
        "manualRouterExecutionConfirmed": execution_confirmed,
        "manualRouterMethod": method or None,
        "manualRouterRouteDirection": manual_custom_swap.get("routeDirection"),
        "manualRouterPoolSource": manual_custom_swap.get("poolSource"),
        "manualRouterRouteRegistryId": manual_custom_swap.get("routeRegistryId"),
        "manualRouterRouteRegistrySymbol": manual_custom_swap.get("routeRegistrySymbol"),
        "sdkRouterQuotesEnabled": bool(out.get("enabled")),
        "sdkExactBuyEnabled": bool(out.get("exactBuyEnabled")),
    })

    if execution_confirmed:
        out["status"] = "manual_router_available"
        out["reason"] = (
            "Confirmed manual Router route is attached for this request; "
            "SDK router quotes remain disabled."
        )
        out["nextRequired"] = "Front-end SubWallet signing/submission is allowed for this execution-confirmed manual Router route."
        out["liveSwapsRecommended"] = True

    if route_exact_buy and execution_confirmed:
        out["exactBuyEnabled"] = True
        out["exactBuyEnabledForThisRoute"] = True
        out["exactBuyEnabledSource"] = "db:hydration_route_registry:manual_router"
        out["liveExactBuyRecommended"] = True
    elif route_exact_buy:
        out["exactBuyEnabledForThisRoute"] = False

    return out


async def _hydration_manual_router_fallback_plan(
    *,
    db: Optional[Session],
    base: str,
    quote: str,
    side: str,
    amount_ui: float,
    amount_mode: str,
    slippage_bps: int,
    base_meta: Dict[str, Any],
    quote_meta: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Build a DB/UI-controlled manual Router plan for confirmed Hydration routes.

    SELL is exact input BASE -> QUOTE through Router.sell.
    BUY is exact output BASE paid with QUOTE through Router.buy, so the route
    direction is QUOTE -> BASE. Both directions are confirmed independently in
    hydration_route_registry, with env routes left only as emergency fallback.
    """
    if not _HYDRATION_ENABLE_MANUAL_ROUTER_FALLBACK:
        return None

    side_norm = str(side or "").strip().lower()
    mode = str(amount_mode or "").strip().lower()
    if side_norm == "sell" and mode == "exact_in":
        method = "sell"
        asset_in_meta = base_meta
        asset_out_meta = quote_meta
        route_base = base
        route_quote = quote
    elif side_norm == "buy" and mode == "exact_out":
        method = "buy"
        asset_in_meta = quote_meta
        asset_out_meta = base_meta
        route_base = quote
        route_quote = base
    else:
        return None

    asset_in_id = _hydration_sdk_asset_id(asset_in_meta)
    asset_out_id = _hydration_sdk_asset_id(asset_out_meta)
    asset_in_symbol = str(asset_in_meta.get("symbol") or route_base or "").upper()
    asset_out_symbol = str(asset_out_meta.get("symbol") or route_quote or "").upper()

    db_cfg = _hydration_route_registry_manual_router_config(
        db=db,
        base=route_base,
        quote=route_quote,
        asset_in_id=int(asset_in_id),
        asset_out_id=int(asset_out_id),
    )
    route = None
    execution_confirmed = False
    pool_source = "manual_router_fallback_generated"
    route_registry_id = None
    route_registry_symbol = None
    note = None

    if isinstance(db_cfg, dict):
        route = _normalize_manual_router_route(
            db_cfg.get("route"),
            asset_in_id=int(asset_in_id),
            asset_out_id=int(asset_out_id),
        )
        execution_confirmed = bool(db_cfg.get("executionConfirmed") or db_cfg.get("confirmed"))
        pool_source = "db:hydration_route_registry:manual_router"
        route_registry_id = db_cfg.get("routeRegistryId")
        route_registry_symbol = db_cfg.get("routeRegistrySymbol")
        note = db_cfg.get("note")

    if not route:
        if not _manual_router_fallback_pair_allowed(route_base, route_quote):
            return None
        route = _manual_router_fallback_configured_route(
            base=route_base,
            quote=route_quote,
            asset_in_id=int(asset_in_id),
            asset_out_id=int(asset_out_id),
            db=None,
        )
        execution_confirmed = _manual_router_fallback_pair_confirmed(route_base, route_quote)
        pool_source = "env:UTT_HYDRATION_MANUAL_ROUTER_FALLBACK_ROUTES_JSON" if route else "manual_router_fallback_generated"
        if not route:
            route = _manual_router_fallback_route(
                asset_in_id,
                asset_out_id,
                base=route_base,
                quote=route_quote,
                db=None,
            )

    if not route:
        return None

    if not execution_confirmed and not _HYDRATION_ALLOW_UNCONFIRMED_MANUAL_ROUTER_FALLBACK:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "hydration_manual_router_fallback_unconfirmed",
                "message": "Manual Router route is buildable but not execution-confirmed for this direction. Save/confirm the route in Hydration Route Registry before signing.",
                "venue": "polkadot_hydration",
                "resolvedSymbol": f"{base}-{quote}",
                "routeDirection": f"{route_base}-{route_quote}",
                "side": side_norm,
                "amountMode": mode,
                "enabled": bool(_HYDRATION_ENABLE_MANUAL_ROUTER_FALLBACK),
                "allowUnconfirmed": bool(_HYDRATION_ALLOW_UNCONFIRMED_MANUAL_ROUTER_FALLBACK),
                "routeRegistryId": route_registry_id,
                "routeRegistrySymbol": route_registry_symbol,
                "confirmedPairs": sorted(_manual_router_fallback_confirmed_pairs()),
                "nextRequired": "Use Token Registry → Hydration Route Registry to mark the exact route direction confirmed only after a tiny live on-chain success.",
                "unsafeOverride": "Set UTT_HYDRATION_ALLOW_UNCONFIRMED_MANUAL_ROUTER_FALLBACK=1 only for a deliberate fee-risking local test.",
            },
        )

    price_payload = await _hydration_refresh_usd_price_cache(
        db=db,
        requested=[asset_in_symbol, asset_out_symbol],
        force_refresh=False,
        allow_refresh=True,
    )
    prices = dict((price_payload or {}).get("prices_usd") or (price_payload or {}).get("usd_prices") or {})
    sources = dict((price_payload or {}).get("priceSources") or {})
    in_usd = _float_or_none(prices.get(asset_in_symbol))
    out_usd = _float_or_none(prices.get(asset_out_symbol))
    if in_usd is None or out_usd is None or out_usd <= 0:
        return None

    max_input_usd = max(0.0, float(_HYDRATION_MANUAL_ROUTER_FALLBACK_MAX_INPUT_USD))
    slippage_rate = Decimal(int(slippage_bps)) / Decimal(10_000)

    common: Dict[str, Any] = {
        "enabled": True,
        "provider": "manual_papi_router",
        "reason": note or "Manual Router route registry fallback; min/max guard is derived from cached/external USD prices, not SDK router quotes.",
        "method": method,
        "routeModeEffective": "manual_router",
        "manualRouterFallback": True,
        "executionConfirmed": bool(execution_confirmed),
        "assetInId": int(asset_in_id),
        "assetOutId": int(asset_out_id),
        "assetInSymbol": asset_in_symbol,
        "assetOutSymbol": asset_out_symbol,
        "route": route,
        "pool": "manual_router_route_registry" if pool_source.startswith("db:") else "manual_router_route_json",
        "poolSource": pool_source,
        "routeRegistryId": route_registry_id,
        "routeRegistrySymbol": route_registry_symbol,
        "routeDirection": f"{route_base}-{route_quote}",
        "slippageBps": int(slippage_bps),
        "maxInputUsd": float(max_input_usd),
        "priceSources": {
            asset_in_symbol: sources.get(asset_in_symbol),
            asset_out_symbol: sources.get(asset_out_symbol),
        },
        "usdPrices": {
            asset_in_symbol: float(in_usd),
            asset_out_symbol: float(out_usd),
        },
        "priceCacheStatus": (price_payload or {}).get("status"),
        "safetyNote": "Controlled tiny-swap path only until a first-class quote source is added.",
    }

    if method == "sell":
        amount_in_atomic = _ui_to_atomic(float(amount_ui), int(asset_in_meta.get("decimals") or 0))
        amount_in_ui_dec = _atomic_to_decimal_ui(amount_in_atomic, int(asset_in_meta.get("decimals") or 0))
        input_usd = float(amount_in_ui_dec) * float(in_usd)
        if max_input_usd > 0 and input_usd > max_input_usd:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "hydration_manual_router_fallback_input_too_large",
                    "message": "Manual-router fallback is capped for controlled testing. Lower the amount or raise UTT_HYDRATION_MANUAL_ROUTER_FALLBACK_MAX_INPUT_USD locally after reviewing the route.",
                    "venue": "polkadot_hydration",
                    "resolvedSymbol": f"{base}-{quote}",
                    "routeDirection": f"{route_base}-{route_quote}",
                    "amount": amount_ui,
                    "inputUsd": input_usd,
                    "maxInputUsd": max_input_usd,
                    "assetIn": asset_in_meta,
                    "assetOut": asset_out_meta,
                },
            )
        mid_price = Decimal(str(in_usd)) / Decimal(str(out_usd))
        estimated_out_ui_dec = amount_in_ui_dec * mid_price
        min_out_ui_dec = estimated_out_ui_dec * (Decimal("1") - slippage_rate)
        min_out_atomic = _decimal_ui_to_atomic_floor(min_out_ui_dec, int(asset_out_meta.get("decimals") or 0))
        if min_out_atomic <= 0:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "hydration_manual_router_fallback_min_out_too_small",
                    "message": "Manual-router fallback produced zero min output from external/cached prices. Use a larger controlled test amount.",
                    "venue": "polkadot_hydration",
                    "resolvedSymbol": f"{base}-{quote}",
                    "amount": amount_ui,
                    "assetIn": asset_in_meta,
                    "assetOut": asset_out_meta,
                    "pricePayloadStatus": (price_payload or {}).get("status"),
                },
            )
        common.update({
            "amountMode": "exact_in",
            "amountInAtomic": str(amount_in_atomic),
            "amountInUi": float(amount_in_ui_dec),
            "estimatedAmountOutAtomic": str(_decimal_ui_to_atomic_floor(estimated_out_ui_dec, int(asset_out_meta.get("decimals") or 0))),
            "estimatedAmountOutUi": float(estimated_out_ui_dec),
            "minAmountOutAtomic": str(min_out_atomic),
            "minAmountOutUi": float(_atomic_to_decimal_ui(min_out_atomic, int(asset_out_meta.get("decimals") or 0))),
            "midPrice": float(mid_price),
            "inputUsd": float(input_usd),
        })
        return common

    amount_out_atomic = _ui_to_atomic(float(amount_ui), int(asset_out_meta.get("decimals") or 0))
    amount_out_ui_dec = _atomic_to_decimal_ui(amount_out_atomic, int(asset_out_meta.get("decimals") or 0))
    mid_price = Decimal(str(out_usd)) / Decimal(str(in_usd))
    estimated_in_ui_dec = amount_out_ui_dec * mid_price
    max_in_ui_dec = estimated_in_ui_dec * (Decimal("1") + slippage_rate)
    max_in_atomic = _decimal_ui_to_atomic_ceil(max_in_ui_dec, int(asset_in_meta.get("decimals") or 0))
    input_usd = float(estimated_in_ui_dec) * float(in_usd)
    if max_input_usd > 0 and input_usd > max_input_usd:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "hydration_manual_router_fallback_input_too_large",
                "message": "Manual-router BUY fallback is capped for controlled testing. Lower the exact output amount or raise UTT_HYDRATION_MANUAL_ROUTER_FALLBACK_MAX_INPUT_USD locally after reviewing the route.",
                "venue": "polkadot_hydration",
                "resolvedSymbol": f"{base}-{quote}",
                "routeDirection": f"{route_base}-{route_quote}",
                "amount": amount_ui,
                "estimatedInputUsd": input_usd,
                "maxInputUsd": max_input_usd,
                "assetIn": asset_in_meta,
                "assetOut": asset_out_meta,
            },
        )
    if max_in_atomic <= 0:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "hydration_manual_router_fallback_max_in_too_small",
                "message": "Manual-router fallback produced zero max input from external/cached prices. Use a larger controlled exact-output amount.",
                "venue": "polkadot_hydration",
                "resolvedSymbol": f"{base}-{quote}",
                "amount": amount_ui,
                "assetIn": asset_in_meta,
                "assetOut": asset_out_meta,
                "pricePayloadStatus": (price_payload or {}).get("status"),
            },
        )
    common.update({
        "amountMode": "exact_out",
        "amountOutAtomic": str(amount_out_atomic),
        "amountOutUi": float(amount_out_ui_dec),
        "estimatedAmountInAtomic": str(_decimal_ui_to_atomic_ceil(estimated_in_ui_dec, int(asset_in_meta.get("decimals") or 0))),
        "estimatedAmountInUi": float(estimated_in_ui_dec),
        "maxAmountInAtomic": str(max_in_atomic),
        "maxAmountInUi": float(_atomic_to_decimal_ui(max_in_atomic, int(asset_in_meta.get("decimals") or 0))),
        "midPrice": float(mid_price),
        "inputUsd": float(input_usd),
    })
    return common


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
    symbol: str = Field(..., description="BASE-QUOTE pair, e.g. UTTT-HDX or DOT-HDX")
    route_mode: str = Field("manual_xyk", description="manual_xyk|manual_router")
    base_reserve: Optional[float] = Field(None, gt=0, description="Manual XYK reserve for BASE, in human units. Required for manual_xyk.")
    quote_reserve: Optional[float] = Field(None, gt=0, description="Manual XYK reserve for QUOTE, in human units. Required for manual_xyk.")
    fee_bps: float = Field(30, ge=0, le=2500, description="Pool fee in basis points for manual_xyk routes")
    enabled: bool = Field(True, description="If false, the route stays saved but Auto/Manual routing will ignore it")
    confirmed: bool = Field(False, description="Manual-router execution confirmation. Only set true after a tiny on-chain success.")
    pool_type: str = Field("XYK", description="XYK for reserve-based manual pools, Router for manual Router route_json paths")
    pool_account: Optional[str] = Field(None, description="Optional Hydration XYK pool account SS58 address. When set, UTT reads live pool reserves from this account instead of the saved snapshot.")
    route_json: Optional[List[Dict[str, Any]]] = Field(None, description="Optional Hydration Router route legs. Required for manual_router; defaults to one XYK leg for manual_xyk.")
    tested_at: Optional[str] = Field(None, description="Optional ISO timestamp for the last route test.")
    last_test_tx_hash: Optional[str] = Field(None, description="Optional tx hash from the confirming route test.")
    note: Optional[str] = Field(None, description="Optional operator note shown in diagnostics")


class HydrationSwapTxRequest(BaseModel):
    symbol: str = Field(..., description="BASE-QUOTE, e.g. UTTT-DOT")
    side: str = Field(..., description="buy|sell")
    amount: float = Field(..., gt=0, description="Human amount. For exact_in this is input; for exact_out this is requested output.")
    amount_mode: str = Field("exact_in", description="exact_in|exact_out. exact_out BUY/getBestBuy is disabled by default behind UTT_HYDRATION_ENABLE_EXACT_BUY after controlled testing caused sidecar timeouts.")
    quote_spend_estimate: Optional[float] = Field(None, description="Optional UI quote-spend estimate for BUY display/debug only.")
    route_mode: Optional[str] = Field(None, description="Hydration route source: auto|sdk|isolated_helper|manual_xyk|manual_router. auto uses confirmed manual routes when available and managed sdk-next/sidecar for normal pairs.")
    slippage_bps: int = Field(100, ge=1, le=5000)
    user_pubkey: str = Field(..., description="Substrate/SS58 account address from SubWallet")


class HydrationManualRouteProbeRequest(BaseModel):
    symbol: str = Field(..., description="BASE-QUOTE pair to probe, e.g. DOT-HDX or HDX-DOT")
    side: str = Field("sell", description="Currently only sell/exact_in is supported for non-mutating manual route probes")
    amount: float = Field(..., gt=0, description="Human input amount for sell/exact_in probe")
    amount_mode: str = Field("exact_in", description="Currently only exact_in is supported for manual route probes")
    slippage_bps: int = Field(100, ge=1, le=5000)
    user_pubkey: str = Field(..., description="Substrate/SS58 account address used as beneficiary in the unsigned call builder")
    min_amount_out_atomic: Optional[str] = Field(None, description="Optional raw minimum output amount. If omitted, probe uses 1 atomic unit so it only tests call-data encoding, not execution economics.")
    min_amount_out_ui: Optional[float] = Field(None, gt=0, description="Optional minimum output in human units. Ignored when min_amount_out_atomic is provided.")
    route_candidates: Optional[List[List[Dict[str, Any]]]] = Field(None, description="Optional candidate Hydration Router route legs. If omitted, Omnipool and XYK single-leg candidates are tried.")
    pool_candidates: Optional[List[str]] = Field(None, description="Optional pool names for generated single-leg candidates, e.g. Omnipool, XYK")


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
        "orderbook_synthetic_fallback_enabled": bool(_HYDRATION_ENABLE_ORDERBOOK_SYNTHETIC_FALLBACK),
        "orderbook_synthetic_refresh_enabled": bool(_HYDRATION_ORDERBOOK_SYNTHETIC_REFRESH),
        "orderbook_synthetic_spread_bps": _HYDRATION_ORDERBOOK_SYNTHETIC_SPREAD_BPS,
        "manual_router_fallback_enabled": bool(_HYDRATION_ENABLE_MANUAL_ROUTER_FALLBACK),
        "manual_router_fallback_pairs": _HYDRATION_MANUAL_ROUTER_FALLBACK_PAIRS_CSV,
        "manual_router_fallback_confirmed_pairs": _HYDRATION_MANUAL_ROUTER_FALLBACK_CONFIRMED_PAIRS_CSV,
        "manual_router_fallback_allow_unconfirmed": bool(_HYDRATION_ALLOW_UNCONFIRMED_MANUAL_ROUTER_FALLBACK),
        "manual_router_fallback_pool": _HYDRATION_MANUAL_ROUTER_FALLBACK_POOL,
        "manual_router_fallback_route_pairs": sorted(_manual_router_fallback_routes_map().keys()),
        "manual_router_fallback_routes_configured": bool(_manual_router_fallback_routes_map()),
        "manual_router_fallback_max_input_usd": _HYDRATION_MANUAL_ROUTER_FALLBACK_MAX_INPUT_USD,
        "manual_router_fallback_note": "manual_route_probe only proves call-data encoding. UI signing is blocked unless a DB route-registry manual_router row is confirmed, the pair is listed in UTT_HYDRATION_MANUAL_ROUTER_FALLBACK_CONFIRMED_PAIRS, or the unsafe local override is enabled.",
        "default_route_mode": _HYDRATION_DEFAULT_ROUTE_MODE,
        "route_modes": sorted(_HYDRATION_ROUTE_MODES),
        "route_mode_note": "Auto returns confirmed manual routes first. SDK quote modes are scoped: OrderBook getBestSell uses UTT_HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES, OrderBook getSpotPrice uses UTT_HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK, OrderTicket uses UTT_HYDRATION_ENABLE_SDK_ORDER_TICKET_QUOTES plus UTT_HYDRATION_ENABLE_SDK_SWAP_TX, and the legacy UTT_HYDRATION_ENABLE_ROUTER_QUOTES flag remains a global emergency override.",
        "manual_pool_fallback_enabled": _HYDRATION_ENABLE_MANUAL_POOL_FALLBACK,
        "manual_pool_live_reserves_enabled": _HYDRATION_MANUAL_POOL_LIVE_RESERVES,
        "manual_pool_prices_configured": bool(str(_HYDRATION_MANUAL_POOL_PRICES_JSON or "{}").strip() not in {"", "{}"}),
        "hydration_route_registry": {
            "enabled": True,
            "table": "hydration_route_registry",
            "endpoints": [
                "/api/polkadot_dex/hydration/route_registry",
                "/api/polkadot_dex/hydration/route_registry/validate",
                "/api/polkadot_dex/hydration/route_registry/reverse_preview",
                "/api/polkadot_dex/hydration/route_registry/upsert",
                "/api/polkadot_dex/hydration/route_registry/{symbol}",
            ],
        },
        "native_sdk_asset_id_fallback": _HYDRATION_NATIVE_ASSET_ID,
        "enable_heavy_inspect": _HYDRATION_ENABLE_HEAVY_INSPECT,
        "enable_router_quotes": _HYDRATION_ENABLE_ROUTER_QUOTES,
        "enable_sdk_orderbook_quotes": _HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES,
        "enable_sdk_spot_orderbook": _HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK,
        "sdk_spot_orderbook_force_isolated_helper": _HYDRATION_SDK_SPOT_ORDERBOOK_FORCE_ISOLATED_HELPER,
        "sdk_spot_orderbook_tradable": _HYDRATION_SDK_SPOT_ORDERBOOK_TRADABLE,
        "sdk_spot_orderbook_implementation": _HYDRATION_SDK_SPOT_ORDERBOOK_IMPLEMENTATION,
        "sdk_spot_orderbook_min_daily_volume_usd": _HYDRATION_SDK_SPOT_ORDERBOOK_MIN_DAILY_VOLUME_USD,
        "sdk_spot_orderbook_pool_types": _HYDRATION_SDK_SPOT_ORDERBOOK_POOL_TYPES_CSV,
        "enable_sdk_order_ticket_quotes": _HYDRATION_ENABLE_SDK_ORDER_TICKET_QUOTES,
        "enable_sdk_swap_tx": _HYDRATION_ENABLE_SDK_SWAP_TX,
        "enable_background_sdk_prices": _HYDRATION_ENABLE_BACKGROUND_SDK_PRICES,
        "enable_state_call_quotes": _HYDRATION_ENABLE_STATE_CALL_QUOTES,
        "enable_swap_tx": _HYDRATION_ENABLE_SWAP_TX,
        "enable_exact_buy": _HYDRATION_ENABLE_EXACT_BUY,
        "enable_buy_diagnostics": _HYDRATION_ENABLE_BUY_DIAGNOSTICS,
        "buy_probe_path": str(_hydration_buy_probe_path()),
        "buy_probe_exists": _hydration_buy_probe_path().exists(),
        "state_call_quote_method": _HYDRATION_STATE_CALL_QUOTE_METHOD,
        "sdk_recovery_closeout": _hydration_sdk_recovery_closeout_payload(),
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



def _hydration_route_registry_asset_id_int(value: Any) -> int:
    norm = _asset_id_norm_for_compare(value)
    try:
        return int(norm)
    except Exception:
        return int(_HYDRATION_NATIVE_ASSET_ID)


def _hydration_route_registry_direction_for_row(
    row: Any,
    *,
    route_json: Optional[List[Dict[str, Any]]] = None,
    db: Optional[Session] = None,
) -> Dict[str, Any]:
    r = dict(row) if not isinstance(row, dict) else dict(row)
    base = str(r.get("base_symbol") or "").strip().upper()
    quote = str(r.get("quote_symbol") or "").strip().upper()
    base_asset_id = r.get("base_asset_id")
    quote_asset_id = r.get("quote_asset_id")
    base_id = _hydration_route_registry_asset_id_int(base_asset_id)
    quote_id = _hydration_route_registry_asset_id_int(quote_asset_id)
    base_meta = {
        "symbol": base,
        "assetId": base_asset_id,
        "decimals": r.get("base_decimals"),
        "native": _asset_id_norm_for_compare(base_asset_id) == _asset_id_norm_for_compare(_HYDRATION_NATIVE_ASSET_ID),
    }
    quote_meta = {
        "symbol": quote,
        "assetId": quote_asset_id,
        "decimals": r.get("quote_decimals"),
        "native": _asset_id_norm_for_compare(quote_asset_id) == _asset_id_norm_for_compare(_HYDRATION_NATIVE_ASSET_ID),
    }
    try:
        return _hydration_route_direction_summary(
            route_json or [],
            db=db,
            base=base,
            quote=quote,
            base_meta=base_meta,
            quote_meta=quote_meta,
            fallback_asset_in_id=int(base_id),
            fallback_asset_out_id=int(quote_id),
        )
    except Exception:
        return {
            "label": f"{base or base_id} → {quote or quote_id}",
            "assetIds": [int(base_id), int(quote_id)],
            "labels": [base or f"asset:{base_id}", quote or f"asset:{quote_id}"],
            "hops": [],
        }


def _hydration_route_registry_execution_status(
    row: Any,
    *,
    route_mode: str,
    confirmed: bool,
    enabled: bool,
) -> Dict[str, Any]:
    mode = str(route_mode or "").strip().lower()
    if not enabled:
        return {
            "status": "disabled",
            "label": "Disabled",
            "executable": False,
            "severity": "muted",
        }
    if mode == "manual_router":
        if confirmed:
            return {
                "status": "confirmed_executable",
                "label": "Confirmed executable",
                "executable": True,
                "severity": "ok",
            }
        return {
            "status": "unconfirmed_blocked",
            "label": "Needs confirmation",
            "executable": False,
            "severity": "warn",
        }
    if mode == "manual_xyk":
        return {
            "status": "manual_pool_available",
            "label": "Manual pool route",
            "executable": True,
            "severity": "ok" if confirmed else "info",
        }
    return {
        "status": "unknown",
        "label": "Unknown",
        "executable": False,
        "severity": "warn",
    }


def _hydration_route_registry_payload(row: Any, *, db: Optional[Session] = None) -> Dict[str, Any]:
    r = dict(row) if not isinstance(row, dict) else dict(row)
    try:
        route_json = json.loads(r.get("route_json") or "[]")
    except Exception:
        route_json = []
    route_mode = str(r.get("route_mode") or "manual_xyk").strip().lower()
    confirmed = bool(int(r.get("confirmed") if r.get("confirmed") is not None else 0))
    enabled = bool(int(r.get("enabled") if r.get("enabled") is not None else 1))
    direction = _hydration_route_registry_direction_for_row(r, route_json=route_json, db=db)
    execution_status = _hydration_route_registry_execution_status(
        r,
        route_mode=route_mode,
        confirmed=confirmed,
        enabled=enabled,
    )
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
        "enabled": enabled,
        "baseReserve": r.get("base_reserve"),
        "quoteReserve": r.get("quote_reserve"),
        "feeBps": r.get("fee_bps"),
        "route": route_json,
        "confirmed": confirmed,
        "direction": direction,
        "routeDirection": (direction or {}).get("label"),
        "executionStatus": execution_status,
        "executable": bool((execution_status or {}).get("executable")),
        "testedAt": r.get("tested_at"),
        "lastTestTxHash": r.get("last_test_tx_hash"),
        "note": r.get("note"),
        "createdAt": r.get("created_at"),
        "updatedAt": r.get("updated_at"),
    }




def _hydration_builtin_route_templates() -> List[Dict[str, Any]]:
    """Built-in Hydration route templates surfaced to the Route Registry UI.

    These are operator aids only. Loading a template should not mark a row
    confirmed; confirmation remains an explicit action after a tiny live test.
    """
    return [
        {
            "id": "builtin:DOT-HDX:manual_router",
            "source": "builtin",
            "label": "DOT-HDX manual Router · DOT → aDOT → HDX",
            "symbol": "DOT-HDX",
            "routeMode": "manual_router",
            "route_mode": "manual_router",
            "poolType": "Router",
            "pool_type": "Router",
            "baseReserve": None,
            "quoteReserve": None,
            "base_reserve": None,
            "quote_reserve": None,
            "feeBps": 30,
            "fee_bps": 30,
            "poolAccount": None,
            "pool_account": None,
            "routeJson": [
                {"pool": {"type": "Aave"}, "assetIn": 5, "assetOut": 1001},
                {"pool": {"type": "Omnipool"}, "assetIn": 1001, "assetOut": 0},
            ],
            "route_json": [
                {"pool": {"type": "Aave"}, "assetIn": 5, "assetOut": 1001},
                {"pool": {"type": "Omnipool"}, "assetIn": 1001, "assetOut": 0},
            ],
            "direction": {"label": "DOT → aDOT → HDX", "assetIds": [5, 1001, 0], "labels": ["DOT", "aDOT", "HDX"]},
            "routeDirection": "DOT → aDOT → HDX",
            "enabled": True,
            "confirmed": False,
            "templateConfirmedDefault": False,
            "requiresConfirmation": True,
            "note": "Built-in manual Router template. Confirm only after a tiny live on-chain success for this exact direction.",
        },
        {
            "id": "builtin:HDX-DOT:manual_router",
            "source": "builtin",
            "label": "HDX-DOT manual Router · HDX → aDOT → DOT",
            "symbol": "HDX-DOT",
            "routeMode": "manual_router",
            "route_mode": "manual_router",
            "poolType": "Router",
            "pool_type": "Router",
            "baseReserve": None,
            "quoteReserve": None,
            "base_reserve": None,
            "quote_reserve": None,
            "feeBps": 30,
            "fee_bps": 30,
            "poolAccount": None,
            "pool_account": None,
            "routeJson": [
                {"pool": {"type": "Omnipool"}, "assetIn": 0, "assetOut": 1001},
                {"pool": {"type": "Aave"}, "assetIn": 1001, "assetOut": 5},
            ],
            "route_json": [
                {"pool": {"type": "Omnipool"}, "assetIn": 0, "assetOut": 1001},
                {"pool": {"type": "Aave"}, "assetIn": 1001, "assetOut": 5},
            ],
            "direction": {"label": "HDX → aDOT → DOT", "assetIds": [0, 1001, 5], "labels": ["HDX", "aDOT", "DOT"]},
            "routeDirection": "HDX → aDOT → DOT",
            "enabled": True,
            "confirmed": False,
            "templateConfirmedDefault": False,
            "requiresConfirmation": True,
            "note": "Built-in manual Router template. Confirm only after a tiny live on-chain success for this exact direction.",
        },
        {
            "id": "builtin:UTTT-HDX:manual_xyk",
            "source": "builtin",
            "label": "UTTT-HDX manual XYK · UTTT → HDX snapshot",
            "symbol": "UTTT-HDX",
            "routeMode": "manual_xyk",
            "route_mode": "manual_xyk",
            "poolType": "XYK",
            "pool_type": "XYK",
            "baseReserve": 1000000.0,
            "quoteReserve": 832.45,
            "base_reserve": 1000000.0,
            "quote_reserve": 832.45,
            "feeBps": 30,
            "fee_bps": 30,
            "poolAccount": None,
            "pool_account": None,
            "routeJson": [
                {"pool": {"type": "XYK"}, "assetIn": 1001331, "assetOut": 0},
            ],
            "route_json": [
                {"pool": {"type": "XYK"}, "assetIn": 1001331, "assetOut": 0},
            ],
            "direction": {"label": "UTTT → HDX", "assetIds": [1001331, 0], "labels": ["UTTT", "HDX"]},
            "routeDirection": "UTTT → HDX",
            "enabled": True,
            "confirmed": False,
            "templateConfirmedDefault": False,
            "requiresConfirmation": True,
            "note": "Built-in manual XYK snapshot template. Add or verify pool account before relying on live reserves; confirm only after a tiny live on-chain success.",
        },
        {
            "id": "builtin:HDX-UTTT:manual_xyk",
            "source": "builtin",
            "label": "HDX-UTTT manual XYK · HDX → UTTT snapshot",
            "symbol": "HDX-UTTT",
            "routeMode": "manual_xyk",
            "route_mode": "manual_xyk",
            "poolType": "XYK",
            "pool_type": "XYK",
            "baseReserve": 832.45,
            "quoteReserve": 1000000.0,
            "base_reserve": 832.45,
            "quote_reserve": 1000000.0,
            "feeBps": 30,
            "fee_bps": 30,
            "poolAccount": None,
            "pool_account": None,
            "routeJson": [
                {"pool": {"type": "XYK"}, "assetIn": 0, "assetOut": 1001331},
            ],
            "route_json": [
                {"pool": {"type": "XYK"}, "assetIn": 0, "assetOut": 1001331},
            ],
            "direction": {"label": "HDX → UTTT", "assetIds": [0, 1001331], "labels": ["HDX", "UTTT"]},
            "routeDirection": "HDX → UTTT",
            "enabled": True,
            "confirmed": False,
            "templateConfirmedDefault": False,
            "requiresConfirmation": True,
            "note": "Built-in reverse manual XYK snapshot template. Add or verify pool account before relying on live reserves; confirm only after a tiny live on-chain success.",
        },
        {
            "id": "builtin:HDX-USDT:manual_router",
            "source": "builtin",
            "label": "HDX-USDT manual Router · HDX → USDT",
            "symbol": "HDX-USDT",
            "routeMode": "manual_router",
            "route_mode": "manual_router",
            "poolType": "Router",
            "pool_type": "Router",
            "baseReserve": None,
            "quoteReserve": None,
            "base_reserve": None,
            "quote_reserve": None,
            "feeBps": 30,
            "fee_bps": 30,
            "poolAccount": None,
            "pool_account": None,
            "routeJson": [
                {"pool": {"type": "Omnipool"}, "assetIn": 0, "assetOut": 10},
            ],
            "route_json": [
                {"pool": {"type": "Omnipool"}, "assetIn": 0, "assetOut": 10},
            ],
            "direction": {"label": "HDX → USDT", "assetIds": [0, 10], "labels": ["HDX", "USDT"]},
            "routeDirection": "HDX → USDT",
            "enabled": True,
            "confirmed": False,
            "templateConfirmedDefault": False,
            "requiresConfirmation": True,
            "liquidityClass": "high_liquidity_candidate",
            "templateNotes": [
                "High-liquidity candidate template using Hydration Omnipool HDX/USDT routing.",
                "USDT is expected to be Hydration asset 10 with 6 decimals; verify Token Registry metadata before confirming.",
            ],
            "note": "Built-in manual Router template. Confirm only after a tiny live on-chain success for this exact direction.",
        },
        {
            "id": "builtin:USDT-HDX:manual_router",
            "source": "builtin",
            "label": "USDT-HDX manual Router · USDT → HDX",
            "symbol": "USDT-HDX",
            "routeMode": "manual_router",
            "route_mode": "manual_router",
            "poolType": "Router",
            "pool_type": "Router",
            "baseReserve": None,
            "quoteReserve": None,
            "base_reserve": None,
            "quote_reserve": None,
            "feeBps": 30,
            "fee_bps": 30,
            "poolAccount": None,
            "pool_account": None,
            "routeJson": [
                {"pool": {"type": "Omnipool"}, "assetIn": 10, "assetOut": 0},
            ],
            "route_json": [
                {"pool": {"type": "Omnipool"}, "assetIn": 10, "assetOut": 0},
            ],
            "direction": {"label": "USDT → HDX", "assetIds": [10, 0], "labels": ["USDT", "HDX"]},
            "routeDirection": "USDT → HDX",
            "enabled": True,
            "confirmed": False,
            "templateConfirmedDefault": False,
            "requiresConfirmation": True,
            "liquidityClass": "high_liquidity_candidate",
            "templateNotes": [
                "High-liquidity candidate template using Hydration Omnipool USDT/HDX routing.",
                "USDT is expected to be Hydration asset 10 with 6 decimals; verify Token Registry metadata before confirming.",
            ],
            "note": "Built-in manual Router template. Confirm only after a tiny live on-chain success for this exact direction.",
        },
        {
            "id": "builtin:DOT-USDT:manual_router",
            "source": "builtin",
            "label": "DOT-USDT manual Router · DOT → aDOT → USDT",
            "symbol": "DOT-USDT",
            "routeMode": "manual_router",
            "route_mode": "manual_router",
            "poolType": "Router",
            "pool_type": "Router",
            "baseReserve": None,
            "quoteReserve": None,
            "base_reserve": None,
            "quote_reserve": None,
            "feeBps": 30,
            "fee_bps": 30,
            "poolAccount": None,
            "pool_account": None,
            "routeJson": [
                {"pool": {"type": "Aave"}, "assetIn": 5, "assetOut": 1001},
                {"pool": {"type": "Omnipool"}, "assetIn": 1001, "assetOut": 10},
            ],
            "route_json": [
                {"pool": {"type": "Aave"}, "assetIn": 5, "assetOut": 1001},
                {"pool": {"type": "Omnipool"}, "assetIn": 1001, "assetOut": 10},
            ],
            "direction": {"label": "DOT → aDOT → USDT", "assetIds": [5, 1001, 10], "labels": ["DOT", "aDOT", "USDT"]},
            "routeDirection": "DOT → aDOT → USDT",
            "enabled": True,
            "confirmed": False,
            "templateConfirmedDefault": False,
            "requiresConfirmation": True,
            "liquidityClass": "high_liquidity_candidate",
            "templateNotes": [
                "High-liquidity candidate template using DOT/aDOT wrapping plus Omnipool routing into USDT.",
                "USDT is expected to be Hydration asset 10 with 6 decimals; verify Token Registry metadata before confirming.",
            ],
            "note": "Built-in manual Router template. Confirm only after a tiny live on-chain success for this exact direction.",
        },
        {
            "id": "builtin:USDT-DOT:manual_router",
            "source": "builtin",
            "label": "USDT-DOT manual Router · USDT → aDOT → DOT",
            "symbol": "USDT-DOT",
            "routeMode": "manual_router",
            "route_mode": "manual_router",
            "poolType": "Router",
            "pool_type": "Router",
            "baseReserve": None,
            "quoteReserve": None,
            "base_reserve": None,
            "quote_reserve": None,
            "feeBps": 30,
            "fee_bps": 30,
            "poolAccount": None,
            "pool_account": None,
            "routeJson": [
                {"pool": {"type": "Omnipool"}, "assetIn": 10, "assetOut": 1001},
                {"pool": {"type": "Aave"}, "assetIn": 1001, "assetOut": 5},
            ],
            "route_json": [
                {"pool": {"type": "Omnipool"}, "assetIn": 10, "assetOut": 1001},
                {"pool": {"type": "Aave"}, "assetIn": 1001, "assetOut": 5},
            ],
            "direction": {"label": "USDT → aDOT → DOT", "assetIds": [10, 1001, 5], "labels": ["USDT", "aDOT", "DOT"]},
            "routeDirection": "USDT → aDOT → DOT",
            "enabled": True,
            "confirmed": False,
            "templateConfirmedDefault": False,
            "requiresConfirmation": True,
            "liquidityClass": "high_liquidity_candidate",
            "templateNotes": [
                "High-liquidity candidate template using Omnipool routing into aDOT plus DOT unwrap through Aave.",
                "USDT is expected to be Hydration asset 10 with 6 decimals; verify Token Registry metadata before confirming.",
            ],
            "note": "Built-in manual Router template. Confirm only after a tiny live on-chain success for this exact direction.",
        },
    ]


def _hydration_route_registry_saved_template_payload(row: Any, *, db: Optional[Session] = None) -> Dict[str, Any]:
    payload = _hydration_route_registry_payload(row, db=db)
    route_mode = str(payload.get("routeMode") or "manual_xyk").strip().lower()
    symbol = str(payload.get("symbol") or "").strip().upper()
    confirmed = bool(payload.get("confirmed"))
    direction_label = str(payload.get("routeDirection") or ((payload.get("direction") or {}).get("label") if isinstance(payload.get("direction"), dict) else "") or "").strip()
    mode_label = "manual Router" if route_mode == "manual_router" else "manual XYK"
    label_parts = [symbol, f"saved {mode_label}"]
    if direction_label:
        label_parts.append(direction_label)
    if confirmed:
        label_parts.append("confirmed source")

    return {
        "id": f"saved:{payload.get('id') or symbol}",
        "source": "saved_route_registry",
        "label": " · ".join([p for p in label_parts if p]),
        "symbol": symbol,
        "routeMode": route_mode,
        "route_mode": route_mode,
        "poolType": payload.get("poolType") or ("Router" if route_mode == "manual_router" else "XYK"),
        "pool_type": payload.get("poolType") or ("Router" if route_mode == "manual_router" else "XYK"),
        "baseReserve": payload.get("baseReserve"),
        "quoteReserve": payload.get("quoteReserve"),
        "base_reserve": payload.get("baseReserve"),
        "quote_reserve": payload.get("quoteReserve"),
        "feeBps": payload.get("feeBps") if payload.get("feeBps") is not None else 30,
        "fee_bps": payload.get("feeBps") if payload.get("feeBps") is not None else 30,
        "poolAccount": payload.get("poolAccount"),
        "pool_account": payload.get("poolAccount"),
        "routeJson": payload.get("route") if isinstance(payload.get("route"), list) else [],
        "route_json": payload.get("route") if isinstance(payload.get("route"), list) else [],
        "direction": payload.get("direction"),
        "routeDirection": payload.get("routeDirection"),
        "enabled": payload.get("enabled") is not False,
        "confirmed": False,
        "templateConfirmedDefault": False,
        "sourceConfirmed": confirmed,
        "requiresConfirmation": True,
        "routeRegistryId": payload.get("id"),
        "routeRegistrySymbol": symbol,
        "note": payload.get("note") or "Template loaded from a saved Hydration Route Registry row. Confirm remains unchecked until explicitly set.",
    }


def _hydration_route_template_source_type(item: Dict[str, Any]) -> str:
    source = str((item or {}).get("source") or "").strip().lower()
    if source in {"builtin", "built_in", "built-in"}:
        return "built_in"
    if source in {"saved_route_registry", "saved_registry", "saved"} or source.startswith("saved"):
        return "saved_registry"
    if source in {"reverse_preview", "reversed_preview"}:
        return "reverse_preview"
    if source in {"fallback", "local_fallback"}:
        return "fallback"
    return source or "template"


def _hydration_route_template_source_label(source_type: str) -> str:
    s = str(source_type or "").strip().lower()
    if s == "built_in":
        return "Built-in template"
    if s == "saved_registry":
        return "Saved Route Registry row"
    if s == "reverse_preview":
        return "Reverse preview"
    if s == "fallback":
        return "Local fallback template"
    return "Route template"


def _append_unique_message(items: List[str], message: str) -> None:
    msg = str(message or "").strip()
    if msg and msg not in items:
        items.append(msg)


def _hydration_route_template_route_legs(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ("routeJson", "route_json", "route"):
        value = (item or {}).get(key)
        if isinstance(value, list):
            return [dict(x) for x in value if isinstance(x, dict)]
    return []


def _hydration_route_template_pool_types(item: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for leg in _hydration_route_template_route_legs(item):
        pool = _manual_router_pool_type_canonical(leg.get("pool"))
        if pool and pool not in out:
            out.append(pool)
    return out


def _hydration_route_template_warning_metadata(item: Dict[str, Any]) -> Dict[str, Any]:
    symbol = str((item or {}).get("symbol") or "").strip().upper()
    source_type = _hydration_route_template_source_type(item or {})
    route_mode = str((item or {}).get("routeMode") or (item or {}).get("route_mode") or "manual_xyk").strip().lower()
    confirmed = bool((item or {}).get("sourceConfirmed") or (item or {}).get("source_confirmed") or (item or {}).get("confirmed"))
    enabled = (item or {}).get("enabled") is not False
    pool_types = _hydration_route_template_pool_types(item or {})
    pool_account = str((item or {}).get("poolAccount") or (item or {}).get("pool_account") or "").strip()
    route_asset_ids = {str(asset_id) for asset_id in _hydration_route_asset_sequence(
        _hydration_route_template_route_legs(item or {}),
        fallback_asset_in_id=0,
        fallback_asset_out_id=0,
    )}

    warnings: List[str] = []
    notes: List[str] = []

    if source_type == "built_in":
        _append_unique_message(warnings, "Built-in template: starter only. Validate before saving and confirm only after a tiny live on-chain success for this exact direction.")
    elif source_type == "saved_registry" and confirmed:
        _append_unique_message(warnings, "Saved source row is confirmed, but loading/cloning still clears Confirmed so executable routes cannot be copied accidentally.")
    elif source_type == "saved_registry":
        _append_unique_message(warnings, "Saved source row is not confirmed. Keep Confirmed unchecked until this exact direction has a tiny live on-chain success.")
    elif source_type == "fallback":
        _append_unique_message(warnings, "Fallback template: local helper only. Validate route shape carefully before saving.")
    elif source_type == "reverse_preview":
        _append_unique_message(warnings, "Reverse preview clears Confirmed intentionally. Validate and live-test the reversed direction before marking it confirmed.")

    if route_mode == "manual_router":
        _append_unique_message(warnings, "Manual Router route: direction-specific execution. BUY/SELL and exact-in/exact-out paths must be tested separately before relying on the row.")
        if "Aave" in pool_types:
            _append_unique_message(notes, "Aave leg detected: this route uses the DOT/aDOT wrapper path.")
        if "Omnipool" in pool_types:
            _append_unique_message(warnings, "Omnipool leg detected: live liquidity, routing, and price impact can change between validation and execution.")
        if "Stableswap" in pool_types:
            _append_unique_message(warnings, "Stableswap leg detected: keep diagnostic-only until the manual route builder preserves Stableswap payload shape.")
        if "USDT" in symbol or "10" in route_asset_ids:
            _append_unique_message(warnings, "USDT route detected: verify Hydration asset ID 10 and 6-decimal Token Registry metadata before marking this direction confirmed.")
    elif route_mode == "manual_xyk":
        _append_unique_message(warnings, "Manual XYK route: pricing depends on live pool reserves/TVL; stale reserve snapshots can mislead orderbook and limit checks.")
        if not pool_account:
            _append_unique_message(warnings, "No pool_account is set, so live reserve checks cannot confirm this XYK pool account yet.")
        if "UTTT" in symbol and "HDX" in symbol:
            _append_unique_message(warnings, "UTTT-HDX isolated pool: monitor TVL, slippage, and price impact; use small controlled tests only.")

    if not enabled:
        _append_unique_message(warnings, "Source route is disabled. Loading it is allowed for editing, but it is not executable while disabled.")

    _append_unique_message(notes, "Loading or cloning a template is read-only and never signs, submits, creates an order, or mutates ledger/FIFO state.")
    _append_unique_message(notes, "The UI intentionally leaves Confirmed unchecked after template load/clone; re-check only after validating and live-testing the exact direction.")

    return {
        "poolTypes": pool_types,
        "warnings": warnings,
        "notes": notes,
    }


def _hydration_route_template_enrich(item: Dict[str, Any]) -> Dict[str, Any]:
    """Add source/safety metadata used by the Route Registry UI.

    The template endpoint remains read-only.  This metadata is only display and
    operator guidance: loading a template must still clear confirmation in the UI.
    """
    out = dict(item or {})
    source_type = _hydration_route_template_source_type(out)
    route_mode = str(out.get("routeMode") or out.get("route_mode") or "manual_xyk").strip().lower()
    confirmed = bool(out.get("sourceConfirmed") or out.get("source_confirmed") or out.get("confirmed"))
    enabled = out.get("enabled") is not False
    executable = bool(source_type == "saved_registry" and confirmed and enabled)

    warning_meta = _hydration_route_template_warning_metadata(out)
    warnings: List[str] = []
    if isinstance(out.get("warnings"), list):
        for raw_warning in out.get("warnings"):
            if isinstance(raw_warning, dict):
                _append_unique_message(warnings, raw_warning.get("message") or raw_warning.get("warning"))
            else:
                _append_unique_message(warnings, str(raw_warning))
    for warning in warning_meta.get("warnings") or []:
        _append_unique_message(warnings, warning)

    notes: List[str] = []
    if isinstance(out.get("templateNotes"), list):
        for raw_note in out.get("templateNotes"):
            _append_unique_message(notes, str(raw_note))
    if isinstance(out.get("template_notes"), list):
        for raw_note in out.get("template_notes"):
            _append_unique_message(notes, str(raw_note))
    if out.get("note"):
        _append_unique_message(notes, str(out.get("note")))
    for note in warning_meta.get("notes") or []:
        _append_unique_message(notes, note)

    if source_type == "saved_registry" and executable:
        warning_level = "info"
        next_action = "Clone/load, validate, then re-check Confirmed only if this exact direction remains intentionally executable."
    elif source_type == "built_in":
        warning_level = "warn"
        next_action = "Load template, validate route, run a tiny live confirmation before checking Confirmed."
    else:
        warning_level = "warn"
        next_action = "Validate before saving. Keep Confirmed unchecked until route direction has been tested live."

    out.update({
        "sourceType": source_type,
        "source_type": source_type,
        "sourceLabel": _hydration_route_template_source_label(source_type),
        "source_label": _hydration_route_template_source_label(source_type),
        "sourceSymbol": out.get("routeRegistrySymbol") or out.get("symbol"),
        "source_symbol": out.get("routeRegistrySymbol") or out.get("symbol"),
        "sourceRouteMode": route_mode,
        "source_route_mode": route_mode,
        "sourceConfirmed": confirmed,
        "source_confirmed": confirmed,
        "sourceExecutable": executable,
        "source_executable": executable,
        "warningLevel": warning_level,
        "warning_level": warning_level,
        "warnings": warnings,
        "warningCount": len(warnings),
        "warning_count": len(warnings),
        "templateNotes": notes,
        "template_notes": notes,
        "routeHazards": {
            "poolTypes": warning_meta.get("poolTypes") or [],
            "requiresExactDirectionLiveTest": bool(route_mode == "manual_router"),
            "requiresLiveReserveReview": bool(route_mode == "manual_xyk"),
            "clearsConfirmedOnLoad": True,
        },
        "route_hazards": {
            "pool_types": warning_meta.get("poolTypes") or [],
            "requires_exact_direction_live_test": bool(route_mode == "manual_router"),
            "requires_live_reserve_review": bool(route_mode == "manual_xyk"),
            "clears_confirmed_on_load": True,
        },
        "recommendedNextAction": next_action,
        "recommended_next_action": next_action,
        "loadSafety": {
            "clearsConfirmed": True,
            "writesDb": False,
            "requiresValidateBeforeSave": True,
            "requiresLiveTestBeforeConfirmed": True,
        },
    })
    return out


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
        "items": [_hydration_route_registry_payload(r, db=db) for r in rows],
        "count": len(rows),
        "note": "manual_xyk rows provide reserve-based routes; manual_router rows provide confirmed multi-leg Router paths used by route_mode=auto/manual_router. SDK-supported pairs do not need rows.",
    }




@router.get("/hydration/route_registry/templates")
async def hydration_route_registry_templates(
    include_builtin: bool = Query(True, description="Include built-in Hydration route templates."),
    include_saved: bool = Query(True, description="Include saved route registry rows as reusable templates."),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    # Read-only template helper for the Route Registry UI. This does not insert,
    # update, delete, sign, build swaps, or submit transactions.
    _ensure_hydration_route_registry_table(db)
    templates: List[Dict[str, Any]] = []
    if include_builtin:
        templates.extend(_hydration_builtin_route_templates())

    if include_saved:
        rows = db.execute(text("SELECT * FROM hydration_route_registry ORDER BY symbol ASC")).mappings().all()
        for row in rows:
            try:
                templates.append(_hydration_route_registry_saved_template_payload(row, db=db))
            except Exception:
                continue

    seen: set[str] = set()
    deduped: List[Dict[str, Any]] = []
    for raw_item in templates:
        item = _hydration_route_template_enrich(raw_item)
        key = str(item.get("id") or "").strip()
        if not key:
            key = f"{item.get('source') or 'template'}:{item.get('symbol') or ''}:{item.get('routeMode') or item.get('route_mode') or ''}:{len(deduped)}"
            item["id"] = key
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "templates": deduped,
        "items": deduped,
        "count": len(deduped),
        "writesDb": False,
        "templateSourceTypes": {
            "built_in": "Bundled operator starter templates.",
            "saved_registry": "Existing Route Registry rows reused as clone templates.",
        },
        "templateSafety": {
            "loadingClearsConfirmed": True,
            "validateBeforeSave": True,
            "confirmOnlyAfterLiveTest": True,
            "writesDb": False,
        },
        "note": "Route templates are operator aids only. Loading a template does not confirm a route or submit any transaction.",
    }


@router.post("/hydration/route_registry/validate")
async def hydration_route_registry_validate(
    req: HydrationRouteRegistryUpsertRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    # Validate-only path for Route Registry UI. This intentionally does not
    # insert, update, delete, sign, build swaps, or submit transactions.
    return _hydration_route_registry_validation_payload(req, db=db)


@router.post("/hydration/route_registry/reverse_preview")
async def hydration_route_registry_reverse_preview(
    req: HydrationRouteRegistryUpsertRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    # Reverse-preview path for Route Registry UI. This intentionally does not
    # insert, update, delete, sign, build swaps, or submit transactions.
    return _hydration_route_registry_reverse_preview_payload(req, db=db)


@router.post("/hydration/route_registry/upsert")
async def hydration_route_registry_upsert(
    req: HydrationRouteRegistryUpsertRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    _ensure_hydration_route_registry_table(db)
    base, quote = _parse_symbol(req.symbol)
    route_mode = str(req.route_mode or "manual_xyk").strip().lower()
    aliases = {
        "manual": "manual_xyk",
        "xyk": "manual_xyk",
        "router": "manual_router",
        "manual router": "manual_router",
        "manual_router_fallback": "manual_router",
    }
    route_mode = aliases.get(route_mode, route_mode)
    if route_mode not in {"manual_xyk", "manual_router"}:
        raise HTTPException(status_code=422, detail={"error": "unsupported_hydration_route_mode", "routeMode": req.route_mode, "supported": ["manual_xyk", "manual_router"]})

    pool_type_raw = str(req.pool_type or ("Router" if route_mode == "manual_router" else "XYK")).strip()
    pool_type_norm = pool_type_raw.lower()
    if route_mode == "manual_router":
        if pool_type_norm not in {"router", "manual_router", "manual router"}:
            raise HTTPException(status_code=422, detail={"error": "unsupported_hydration_manual_router_pool_type", "poolType": req.pool_type, "supported": ["Router"]})
        pool_type = "Router"
    else:
        if pool_type_norm != "xyk":
            raise HTTPException(status_code=422, detail={"error": "unsupported_hydration_manual_pool_type", "poolType": req.pool_type, "supported": ["XYK", "Router"]})
        pool_type = "XYK"

    base_meta = _resolve_asset(base, db=db)
    quote_meta = _resolve_asset(quote, db=db)
    route = req.route_json
    route_validation: Optional[Dict[str, Any]] = None

    if route_mode == "manual_router":
        asset_in_id = _hydration_sdk_asset_id(base_meta)
        asset_out_id = _hydration_sdk_asset_id(quote_meta)
        route_validation = _validate_manual_router_route(
            route,
            asset_in_id=int(asset_in_id),
            asset_out_id=int(asset_out_id),
        )
        route = route_validation.get("route") if route_validation.get("ok") else None
        if not route:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "invalid_hydration_manual_router_route_json",
                    "message": "manual_router routes require route_json legs that start with BASE asset ID, end with QUOTE asset ID, connect each leg assetOut -> next assetIn, and use metadata-supported pool types.",
                    "symbol": f"{base}-{quote}",
                    "expectedAssetInId": int(asset_in_id),
                    "expectedAssetOutId": int(asset_out_id),
                    "route_json": req.route_json,
                    "routeValidation": route_validation,
                },
            )
        base_reserve = _float_or_none(req.base_reserve)
        quote_reserve = _float_or_none(req.quote_reserve)
    else:
        base_reserve = _float_or_none(req.base_reserve)
        quote_reserve = _float_or_none(req.quote_reserve)
        if base_reserve is None or quote_reserve is None:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "manual_xyk_reserves_required",
                    "message": "manual_xyk routes require positive base_reserve and quote_reserve.",
                    "symbol": f"{base}-{quote}",
                },
            )
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
                base_reserve, quote_reserve, fee_bps, route_json, confirmed, tested_at,
                last_test_tx_hash, note, created_at, updated_at
            ) VALUES (
                :id, :symbol, :base_symbol, :quote_symbol, :base_asset_id, :quote_asset_id,
                :base_decimals, :quote_decimals, :route_mode, :pool_type, :pool_account, :enabled,
                :base_reserve, :quote_reserve, :fee_bps, :route_json, :confirmed, :tested_at,
                :last_test_tx_hash, :note, :created_at, :updated_at
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
            "route_mode": route_mode,
            "pool_type": pool_type,
            "pool_account": str(req.pool_account or "").strip() or None,
            "enabled": 1 if req.enabled else 0,
            "base_reserve": float(base_reserve) if base_reserve is not None else None,
            "quote_reserve": float(quote_reserve) if quote_reserve is not None else None,
            "fee_bps": float(req.fee_bps),
            "route_json": json.dumps(route, separators=(",", ":"), default=str),
            "confirmed": 1 if req.confirmed else 0,
            "tested_at": str(req.tested_at or "").strip() or None,
            "last_test_tx_hash": str(req.last_test_tx_hash or "").strip() or None,
            "note": req.note or ("Manual Hydration Router route registry entry." if route_mode == "manual_router" else "Manual Hydration XYK route registry entry."),
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
        "routeValidation": route_validation if route_mode == "manual_router" else None,
        "next": {
            "orderbook": f"/api/polkadot_dex/hydration/orderbook?symbol={base}-{quote}&route_mode=manual_xyk" if route_mode == "manual_xyk" else None,
            "swapTx": f"/api/polkadot_dex/hydration/swap_tx",
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
    orderbook_status = _hydration_router_quote_status(symbol=f"{base}-{quote}", base_meta=base_meta, quote_meta=quote_meta, use_case="orderbook")
    order_ticket_status = _hydration_router_quote_status(symbol=f"{base}-{quote}", base_meta=base_meta, quote_meta=quote_meta, use_case="order_ticket")
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
        "orderbookQuoteStatus": orderbook_status,
        "orderTicketQuoteStatus": order_ticket_status,
        "liveQuotesEnabled": bool(_HYDRATION_ENABLE_ROUTER_QUOTES),
        "liveQuotesAvailable": bool(status.get("available")),
        "liveOrderbookQuotesEnabled": bool(orderbook_status.get("enabled")),
        "liveOrderbookQuotesAvailable": bool(orderbook_status.get("available")),
        "liveOrderTicketQuotesEnabled": bool(order_ticket_status.get("enabled")),
        "liveOrderTicketQuotesAvailable": bool(order_ticket_status.get("available")),
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


@router.get("/hydration/prices/status")
async def hydration_usd_prices_status(
    assets: Optional[str] = Query("HDX,DOT,USDT,UTTT,HOLLAR", description="Comma-separated Hydration assets to inspect in the USD price cache."),
    symbol: Optional[str] = Query("HDX-DOT", description="Optional pair used only for router-quote safety diagnostics."),
    include_sidecar_health: bool = Query(False, description="If true, performs only a lightweight GET /health against the local sidecar. It does not autostart the sidecar."),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Return Hydration price-cache/router safety state without refreshing prices.

    This endpoint is intentionally status-only:
      - no SDK router quote refresh
      - no sidecar autostart
      - no Dwellir/PAPI quote subscription work
      - optional sidecar health check is a local HTTP /health probe only
    """
    requested = _hydration_price_cache_requested_symbols(assets)
    cache_payload = _hydration_price_cache_payload(status="status_only", requested=requested)
    cache_meta = dict(cache_payload.get("cache") or {})
    prices = dict(cache_payload.get("prices_usd") or {})
    usd_prices = dict(cache_payload.get("usd_prices") or {})
    sources = dict(cache_payload.get("priceSources") or {})

    # Keep this status endpoint refresh-free while still reporting deterministic
    # stablecoin prices the same way /hydration/prices?refresh=false does.
    # This does not call SDK/router quotes, does not call external price APIs,
    # and does not autostart the sidecar.
    for stable in ("USDT", "USDC", "HOLLAR"):
        prices.setdefault(stable, 1.0)
        usd_prices.setdefault(stable, 1.0)
        sources.setdefault(stable, "stable")

    resolved_prices: Dict[str, float] = {}
    missing_prices: List[str] = []
    for sym in requested:
        val = _float_or_none(prices.get(sym))
        if val is not None:
            resolved_prices[sym] = float(val)
        else:
            missing_prices.append(sym)

    router_status: Dict[str, Any]
    resolved_symbol: Optional[str] = None
    if symbol:
        try:
            base, quote = _parse_symbol(symbol)
            base_meta = _resolve_asset(base, db=db)
            quote_meta = _resolve_asset(quote, db=db)
            resolved_symbol = f"{base}-{quote}"
            router_status = _hydration_router_quote_status(
                symbol=resolved_symbol,
                base_meta=base_meta,
                quote_meta=quote_meta,
            )
        except HTTPException as e:
            router_status = {
                "enabled": bool(_HYDRATION_ENABLE_ROUTER_QUOTES),
                "available": False,
                "status": "symbol_diagnostic_failed",
                "symbol": symbol,
                "error": getattr(e, "detail", None),
            }
        except Exception as e:
            router_status = {
                "enabled": bool(_HYDRATION_ENABLE_ROUTER_QUOTES),
                "available": False,
                "status": "symbol_diagnostic_failed",
                "symbol": symbol,
                "error": type(e).__name__,
                "message": str(e),
            }
    else:
        router_status = _hydration_router_quote_status(symbol=None)

    sidecar_health = await _sidecar_health() if include_sidecar_health else {
        "skipped": True,
        "reason": "include_sidecar_health=false",
        "autostart": False,
        "note": "No sidecar health request was made, and this status endpoint never autostarts the sidecar.",
    }

    now = time.monotonic()
    expires_at = float(cache_meta.get("expires_at") or 0)
    error_until = float(cache_meta.get("error_until") or 0)
    cache_stale = bool(expires_at <= now)
    in_error_backoff = bool(error_until > now)

    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "network": "hydration",
        "status": "status_only",
        "statusDetail": _hydration_price_cache_status_detail(status="status_only", requested=requested, prices=prices),
        "requested": requested,
        "rawAssets": assets,
        "symbol": symbol,
        "resolvedSymbol": resolved_symbol,
        "prices_usd": prices,
        "usd_prices": usd_prices or dict(prices),
        "resolvedPrices": resolved_prices,
        "missingPrices": missing_prices,
        "priceSources": sources,
        "errors": cache_payload.get("errors") or [],
        "cache": {
            **cache_meta,
            "status_only": True,
            "stale": cache_stale,
            "in_error_backoff": in_error_backoff,
            "seconds_until_expiry": max(0.0, expires_at - now) if expires_at else 0.0,
            "seconds_until_retry": max(0.0, error_until - now) if error_until else 0.0,
            "has_any_price": bool(resolved_prices),
            "missing_count": len(missing_prices),
        },
        "routerQuotes": router_status,
        "sidecar": {
            "enabled": bool(_HYDRATION_USE_SIDECAR),
            "url": _HYDRATION_SIDECAR_URL,
            "url_redacted": _redact_url(_HYDRATION_SIDECAR_URL),
            "managed_process_running": _sidecar_process_running(),
            "autostart_for_router_quotes": bool(_hydration_effective_autostart_sidecar(price_cache=False)),
            "autostart_for_orderbook_sdk": bool(_hydration_effective_autostart_sidecar(price_cache=False, payload={"mode": "quote_sell", "sdkUseCase": "orderbook", "routeMode": "auto"})),
            "autostart_for_order_ticket_sdk": bool(_hydration_effective_autostart_sidecar(price_cache=False, payload={"mode": "swap_tx", "sdkUseCase": "order_ticket", "routeMode": "auto"})),
            "autostart_for_price_cache": bool(_hydration_effective_autostart_sidecar(price_cache=True)),
            "health": sidecar_health,
        },
        "safety": {
            "status_endpoint_refreshes_prices": False,
            "status_endpoint_autostarts_sidecar": False,
            "include_sidecar_health_is_local_only": bool(include_sidecar_health),
            "router_quotes_enabled": bool(_HYDRATION_ENABLE_ROUTER_QUOTES),
            "sdk_orderbook_quotes_enabled": bool(_HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES),
            "sdk_order_ticket_quotes_enabled": bool(_HYDRATION_ENABLE_SDK_ORDER_TICKET_QUOTES),
            "sdk_swap_tx_enabled": bool(_HYDRATION_ENABLE_SDK_SWAP_TX),
            "background_sdk_prices_enabled": bool(_HYDRATION_ENABLE_BACKGROUND_SDK_PRICES),
            "sdk_price_cache_enabled": bool(_HYDRATION_ENABLE_SDK_PRICE_CACHE),
            "sdk_price_cache_fallback_enabled": bool(_HYDRATION_PRICE_CACHE_USE_SDK_FALLBACK),
            "external_usd_prices_enabled": bool(_HYDRATION_ENABLE_EXTERNAL_USD_PRICES),
            "external_usd_price_source": _HYDRATION_EXTERNAL_USD_PRICE_SOURCE,
            "safe_for_ui_polling": True,
        },
        "endpoints": {
            "status": "/api/polkadot_dex/hydration/prices/status",
            "cache_only": "/api/polkadot_dex/hydration/prices?refresh=false",
            "controlled_refresh": "/api/polkadot_dex/hydration/prices?refresh=true",
            "force_refresh": "/api/polkadot_dex/hydration/prices?force_refresh=true",
            "hydration_status": "/api/polkadot_dex/hydration/status",
        },
        "nextRequired": (
            "Use this endpoint for UI/status polling. Use /hydration/prices?refresh=true only for controlled cache refreshes. "
            "Keep UTT_HYDRATION_PRICE_CACHE_USE_SDK_FALLBACK=0 unless explicitly testing SDK fallback behavior."
        ),
    }


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



@router.get("/hydration/metadata_v15_scan")
async def hydration_metadata_v15_scan(
    targets_csv: Optional[str] = Query(
        "Router,Omnipool,XYK,Stableswap,AssetRegistry,Tokens,Currencies",
        description="Comma-separated ASCII targets to scan for in Metadata v15.",
    ),
    context_bytes: int = Query(220, ge=40, le=1200, description="Number of bytes before/after each target hit to include."),
    max_windows_per_target: int = Query(8, ge=1, le=25),
    max_ascii_strings: int = Query(500, ge=50, le=3000),
    include_raw_metadata: bool = Query(False, description="If true, includes the full raw metadata hex. Usually leave false."),
) -> Dict[str, Any]:
    """Fetch and scan Hydration Metadata v15 for pallet/call discovery.

    Diagnostic-only. This uses state_call Metadata_metadata_at_version with the
    correct SCALE u32 v15 probe data (0x0f000000), then returns bounded ASCII
    windows around Router/Omnipool/XYK/Stableswap-related names. It deliberately
    avoids signing, quote execution, swap building, or storage mutation.
    """
    targets = [t.strip() for t in str(targets_csv or "").split(",") if t.strip()]
    if not targets:
        targets = ["Router", "Omnipool", "XYK", "Stableswap"]

    metadata_probe = await _state_call_probe_method("Metadata_metadata_at_version", _metadata_v15_probe_payload())
    metadata_hex = _metadata_result_hex_from_probe(metadata_probe)
    if not metadata_hex or metadata_probe.get("classification") != "accepted":
        raise HTTPException(
            status_code=502,
            detail={
                "error": "hydration_metadata_v15_fetch_failed",
                "message": "Metadata_metadata_at_version did not return an accepted Metadata v15 blob.",
                "probe": metadata_probe,
                "probeData": _metadata_v15_probe_payload(),
            },
        )

    scan = _metadata_scan_summary(
        metadata_hex,
        targets=targets,
        context_bytes=int(context_bytes),
        max_windows_per_target=int(max_windows_per_target),
        max_ascii_strings=int(max_ascii_strings),
    )

    out: Dict[str, Any] = {
        "ok": True,
        "venue": "polkadot_hydration",
        "network": "hydration",
        "rpc_url": _redact_url(_hydration_rpc_url()),
        "method": "Metadata_metadata_at_version",
        "probeData": _metadata_v15_probe_payload(),
        "classification": metadata_probe.get("classification"),
        "metadataRpc": {
            "ok": ((metadata_probe.get("rpc") or {}).get("ok") if isinstance(metadata_probe.get("rpc"), dict) else None),
            "httpStatus": ((metadata_probe.get("rpc") or {}).get("httpStatus") if isinstance(metadata_probe.get("rpc"), dict) else None),
        },
        "scanConfig": {
            "targets": targets,
            "contextBytes": int(context_bytes),
            "maxWindowsPerTarget": int(max_windows_per_target),
            "maxAsciiStrings": int(max_ascii_strings),
            "includeRawMetadata": bool(include_raw_metadata),
        },
        "scan": scan,
        "interpretation": {
            "purpose": "Discover Hydration pallet/call names from Metadata v15 before hardening manual Router/Omnipool/XYK/Stableswap builders.",
            "safe": True,
            "mutation": False,
            "nextStep": "Review targetWindows.Router, targetWindows.Omnipool, targetWindows.XYK, and nearbyCallNameCandidates for exact call names and argument labels.",
        },
    }
    if include_raw_metadata:
        out["rawMetadataHex"] = metadata_hex
    return out



@router.get("/hydration/metadata_v15_focused_scan")
async def hydration_metadata_v15_focused_scan(
    terms_csv: Optional[str] = Query(
        None,
        description="Optional comma-separated focused byte/ASCII terms. Defaults to Hydration DEX call/storage terms.",
    ),
    context_bytes: int = Query(900, ge=120, le=4000, description="Number of bytes before/after each focused term hit to include."),
    max_hits_per_term: int = Query(8, ge=1, le=30),
    max_ascii_strings: int = Query(1800, ge=100, le=5000),
    include_raw_metadata: bool = Query(False, description="If true, includes the full raw metadata hex. Usually leave false."),
) -> Dict[str, Any]:
    """Focused Hydration Metadata v15 scan for route-builder hardening.

    This is narrower than /metadata_v15_scan.  It targets the byte/ASCII regions
    around pallet_route_executor, pallet_omnipool, pallet_xyk, pallet_stableswap,
    hydradx_traits::router types, RouteExecuted, and Router storage hints.
    Diagnostic-only: no signing, swap building, state mutation, or SDK router
    quote execution is performed.
    """
    terms = _metadata_focus_terms(terms_csv)
    metadata_probe = await _state_call_probe_method("Metadata_metadata_at_version", _metadata_v15_probe_payload())
    metadata_hex = _metadata_result_hex_from_probe(metadata_probe)
    if not metadata_hex or metadata_probe.get("classification") != "accepted":
        raise HTTPException(
            status_code=502,
            detail={
                "error": "hydration_metadata_v15_fetch_failed",
                "message": "Metadata_metadata_at_version did not return an accepted Metadata v15 blob.",
                "probe": metadata_probe,
                "probeData": _metadata_v15_probe_payload(),
            },
        )

    focused = _metadata_focused_windows(
        metadata_hex,
        terms=terms,
        context_bytes=int(context_bytes),
        max_hits_per_term=int(max_hits_per_term),
        max_ascii_strings=int(max_ascii_strings),
    )

    out: Dict[str, Any] = {
        "ok": True,
        "venue": "polkadot_hydration",
        "network": "hydration",
        "rpc_url": _redact_url(_hydration_rpc_url()),
        "method": "Metadata_metadata_at_version",
        "probeData": _metadata_v15_probe_payload(),
        "classification": metadata_probe.get("classification"),
        "metadataRpc": {
            "ok": ((metadata_probe.get("rpc") or {}).get("ok") if isinstance(metadata_probe.get("rpc"), dict) else None),
            "httpStatus": ((metadata_probe.get("rpc") or {}).get("httpStatus") if isinstance(metadata_probe.get("rpc"), dict) else None),
        },
        "scanConfig": {
            "terms": terms,
            "contextBytes": int(context_bytes),
            "maxHitsPerTerm": int(max_hits_per_term),
            "maxAsciiStrings": int(max_ascii_strings),
            "includeRawMetadata": bool(include_raw_metadata),
        },
        "scan": focused,
        "interpretation": {
            "purpose": "Focus Hydration Metadata v15 around Router/Omnipool/XYK/Stableswap call and storage regions for manual route hardening.",
            "safe": True,
            "mutation": False,
            "nextStep": "Review scan.focusedWindows['pallet_route_executor'], scan.focusedWindows['hydradx_traits'], and scan.manualRouteShapeHints before changing manual route builders.",
        },
    }
    if include_raw_metadata:
        out["rawMetadataHex"] = metadata_hex
    return out


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
                "message": "State-call quote probing is disabled. H-SDK.3C/3D found no usable exported quote method; leave UTT_HYDRATION_ENABLE_STATE_CALL_QUOTES=0 unless running one explicit diagnostic.",
                "safeMode": True,
                "sdkRecoveryCloseout": _hydration_sdk_recovery_closeout_payload(),
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


def _hydration_orderbook_common_config(
    *,
    orderbook_step_timeout_s: float,
    max_consecutive_errors: int,
    force_isolated_orderbook: bool,
    route_mode_norm: str,
    requested_depth: int,
    sample_depth: int,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {
        "stepTimeoutS": float(orderbook_step_timeout_s),
        "maxConsecutiveErrors": int(max_consecutive_errors),
        "forceIsolatedHelper": bool(force_isolated_orderbook),
        "legacyForceIsolatedHelperEnv": bool(_HYDRATION_ORDERBOOK_FORCE_ISOLATED_HELPER),
        "routeMode": route_mode_norm,
        "routeModeEffective": "isolated_helper" if force_isolated_orderbook else "sdk",
        "requestedDepth": int(requested_depth),
        "sampleDepth": int(sample_depth),
    }
    if isinstance(extra, dict):
        cfg.update(extra)
    return cfg


def _hydration_synthetic_spot_levels(
    *,
    base: str,
    base_meta: Dict[str, Any],
    quote_meta: Dict[str, Any],
    mid_price: float,
    depth: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    n = max(1, min(int(depth), 10))
    spread_rate = max(0.0001, min(float(_HYDRATION_ORDERBOOK_SYNTHETIC_SPREAD_BPS) / 10000.0, 0.05))
    bids: List[Dict[str, Any]] = []
    asks: List[Dict[str, Any]] = []
    sizes = _hydration_sample_sizes(base, int((base_meta or {}).get("decimals") or 0), side="synthetic", depth=n)
    for idx, raw_size in enumerate(sizes):
        try:
            size = float(raw_size)
        except Exception:
            continue
        if size <= 0:
            continue
        # Widen each synthetic level slightly away from the derived mid.
        step = spread_rate * float(idx + 1)
        bid_px = max(float(mid_price) * (1.0 - step), 0.0)
        ask_px = float(mid_price) * (1.0 + step)
        if bid_px > 0:
            bids.append({
                "price": bid_px,
                "size": size,
                "outputSize": size * bid_px,
                "synthetic": True,
            })
        if ask_px > 0:
            asks.append({
                "price": ask_px,
                "size": size,
                "inputSize": size * ask_px,
                "synthetic": True,
            })
    return bids, asks


async def _hydration_synthetic_spot_orderbook_response(
    *,
    symbol: str,
    base: str,
    quote: str,
    base_meta: Dict[str, Any],
    quote_meta: Dict[str, Any],
    depth: int,
    db: Optional[Session],
    route_mode_norm: str,
    orderbook_config: Dict[str, Any],
    sample_errors: Optional[List[Dict[str, Any]]] = None,
    fallback_reason: str = "sdk_quote_sampling_failed",
) -> Optional[Dict[str, Any]]:
    if not _HYDRATION_ENABLE_ORDERBOOK_SYNTHETIC_FALLBACK:
        return None

    requested = [str(base or "").upper(), str(quote or "").upper()]
    try:
        price_payload = await _hydration_refresh_usd_price_cache(
            db=db,
            requested=requested,
            force_refresh=False,
            allow_refresh=bool(_HYDRATION_ORDERBOOK_SYNTHETIC_REFRESH),
        )
    except Exception as e:
        return {
            "ok": False,
            "error": "hydration_synthetic_orderbook_price_source_failed",
            "message": str(e),
            "exc": type(e).__name__,
        }

    prices = dict((price_payload or {}).get("prices_usd") or (price_payload or {}).get("usd_prices") or {})
    sources = dict((price_payload or {}).get("priceSources") or {})
    base_usd = _float_or_none(prices.get(str(base).upper()))
    quote_usd = _float_or_none(prices.get(str(quote).upper()))
    if base_usd is None or quote_usd is None or quote_usd <= 0:
        return {
            "ok": False,
            "error": "hydration_synthetic_orderbook_prices_missing",
            "message": "Synthetic Hydration orderbook fallback needs USD prices for both BASE and QUOTE.",
            "requested": requested,
            "prices_usd": prices,
            "missing": [s for s in requested if _float_or_none(prices.get(s)) is None],
            "pricePayloadStatus": (price_payload or {}).get("status"),
            "pricePayloadErrors": (price_payload or {}).get("errors"),
        }

    mid_price = float(base_usd) / float(quote_usd)
    bids, asks = _hydration_synthetic_spot_levels(
        base=base,
        base_meta=base_meta,
        quote_meta=quote_meta,
        mid_price=mid_price,
        depth=depth,
    )
    if not bids and not asks:
        return {
            "ok": False,
            "error": "hydration_synthetic_orderbook_empty",
            "message": "Synthetic Hydration orderbook fallback could not build any levels.",
        }

    asks.sort(key=lambda x: float(x.get("price") or 0.0))
    bids.sort(key=lambda x: -float(x.get("price") or 0.0))
    price_decimals = _suggest_price_decimals(asks + bids, int((quote_meta or {}).get("decimals") or 0))
    size_decimals = min(int((base_meta or {}).get("decimals") or 0), 8)
    cfg = dict(orderbook_config or {})
    cfg.update({
        "routeModeEffective": "synthetic_spot_fallback",
        "source": "synthetic_spot_fallback",
        "fallbackReason": fallback_reason,
        "syntheticFallbackEnabled": True,
        "syntheticFallbackRefreshEnabled": bool(_HYDRATION_ORDERBOOK_SYNTHETIC_REFRESH),
        "syntheticSpreadBps": float(_HYDRATION_ORDERBOOK_SYNTHETIC_SPREAD_BPS),
        "tradable": False,
        "tradeRequiresRealRouterQuote": True,
    })

    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "router": "synthetic_spot_fallback",
        "syntheticFallback": True,
        "tradable": False,
        "tradeRequiresRealRouterQuote": True,
        "syntheticFallbackReason": (
            "SDK getBestSell quote sampling failed or was gated, so this visual orderbook was derived from cached/external USD prices. "
            "Do not use this response alone to mark swaps executable."
        ),
        "rawSymbol": symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base,
        "quote": quote,
        "baseAssetId": (base_meta or {}).get("assetId"),
        "quoteAssetId": (quote_meta or {}).get("assetId"),
        "baseDecimals": int((base_meta or {}).get("decimals") or 0),
        "quoteDecimals": int((quote_meta or {}).get("decimals") or 0),
        "baseMeta": base_meta,
        "quoteMeta": quote_meta,
        "priceDecimals": price_decimals,
        "displayPriceDecimals": max(1, min(price_decimals, 8)),
        "sizeDecimals": size_decimals,
        "midPrice": float(mid_price),
        "priceSources": {
            base: sources.get(base) or sources.get(str(base).upper()),
            quote: sources.get(quote) or sources.get(str(quote).upper()),
        },
        "usdPrices": {
            base: float(base_usd),
            quote: float(quote_usd),
        },
        "priceCacheStatus": (price_payload or {}).get("status"),
        "priceCache": (price_payload or {}).get("cache"),
        "routeMode": route_mode_norm,
        "routeModeEffective": "synthetic_spot_fallback",
        "orderbookConfig": cfg,
        "bids": bids,
        "asks": asks,
        "sampleErrors": sample_errors or [],
    }


async def _run_hydration_helper_sdk_recovery(
    payload: Dict[str, Any],
    *,
    timeout_s: float,
) -> Dict[str, Any]:
    """Run the Hydration JS helper for one explicit recovery diagnostic stage.

    This deliberately bypasses normal UI SDK quote gates, but only inside this
    diagnostic endpoint and only through a short-lived helper process.  It does
    not sign, submit, save routes, write ledger rows, or mutate FIFO state.
    """
    payload = dict(payload or {})
    payload["wsUrl"] = _hydration_ws_url()
    if not payload.get("wsUrl"):
        raise HTTPException(
            status_code=503,
            detail={
                "error": "hydration_ws_not_configured",
                "message": "Set UTT_HYDRATION_WS_URL, or configure the Dwellir Hydration API key so the diagnostic helper can use WebSocket RPC.",
            },
        )

    helper = _hydration_helper_path()
    if not helper.exists():
        raise HTTPException(
            status_code=501,
            detail={
                "error": "hydration_helper_missing",
                "message": "Hydration JS helper is not installed.",
                "helperPath": str(helper),
            },
        )

    payload.setdefault("venue", "polkadot_hydration")
    payload["enableRouterQuotes"] = True
    payload["sdkRecoveryDiagnostics"] = True
    payload["forceIsolatedHelper"] = True

    run_timeout_s = max(8.0, float(timeout_s) + 8.0)

    def _call_node() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [_HYDRATION_NODE_BIN, str(helper)],
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
                "error": "hydration_sdk_recovery_helper_timeout",
                "timeout_s": run_timeout_s,
                "step_timeout_s": payload.get("stepTimeoutS"),
                "helperPath": str(helper),
                "partial_stdout": partial_stdout[-3000:],
                "partial_stderr": partial_stderr[-3000:],
            },
        )
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "hydration_sdk_recovery_helper_spawn_failed",
                "exc": type(e).__name__,
                "message": str(e),
                "helperPath": str(helper),
            },
        )

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    try:
        data = json.loads(stdout) if stdout else {}
    except Exception:
        data = {}

    if proc.returncode != 0 or not isinstance(data, dict):
        detail = data if isinstance(data, dict) and data else {
            "error": "hydration_sdk_recovery_helper_failed",
            "stderr": stderr[-3000:],
            "stdout": stdout[-3000:],
        }
        if isinstance(detail, dict):
            detail.setdefault("returncode", proc.returncode)
            detail.setdefault("helperPath", str(helper))
            detail.setdefault("stderr", stderr[-3000:])
        raise HTTPException(status_code=int(detail.get("status") or 502), detail=detail)

    if not data.get("ok"):
        detail = dict(data)
        detail.setdefault("error", "hydration_sdk_recovery_helper_not_ok")
        detail.setdefault("helperPath", str(helper))
        detail.setdefault("stderr", stderr[-3000:])
        raise HTTPException(status_code=int(detail.get("status") or 502), detail=detail)
    return data


def _hydration_node_version_probe() -> Dict[str, Any]:
    t0 = time.monotonic()
    try:
        proc = subprocess.run(
            [_HYDRATION_NODE_BIN, "--version"],
            text=True,
            capture_output=True,
            timeout=5.0,
        )
        raw = (proc.stdout or proc.stderr or "").strip()
        major = None
        try:
            major = int(str(raw).lstrip("v").split(".")[0])
        except Exception:
            major = None
        return {
            "ok": proc.returncode == 0,
            "elapsed_s": round(time.monotonic() - t0, 4),
            "node_bin": _HYDRATION_NODE_BIN,
            "version": raw,
            "major": major,
            "meetsSdkReadmeMinimum": bool(major is not None and major >= 25),
            "warning": None if (major is not None and major >= 25) else "Current Galactic SDK helper code requires Node 25+ before relying on sdk-next live quote diagnostics.",
            "stderr": (proc.stderr or "")[-1000:],
        }
    except Exception as e:
        return {
            "ok": False,
            "elapsed_s": round(time.monotonic() - t0, 4),
            "node_bin": _HYDRATION_NODE_BIN,
            "error": type(e).__name__,
            "message": str(e),
        }


def _hydration_sdk_recovery_nested_state(result: Any) -> str:
    if isinstance(result, dict):
        if bool(result.get("skipped")):
            return "skipped"
        if result.get("ok") is False:
            return "failed"
    return "passed"


async def _hydration_sdk_recovery_stage(name: str, fn) -> Dict[str, Any]:
    t0 = time.monotonic()
    try:
        result = await fn()
        state = _hydration_sdk_recovery_nested_state(result)
        out: Dict[str, Any] = {
            "ok": state == "passed",
            "name": name,
            "elapsed_s": round(time.monotonic() - t0, 4),
            "result": result,
        }
        if state == "skipped":
            out["skipped"] = True
            out["reason"] = (
                (result or {}).get("reason")
                or (result or {}).get("message")
                or (result or {}).get("error")
                or "skipped"
            )
        elif state == "failed":
            out["error"] = (result or {}).get("error") or "hydration_sdk_recovery_inner_failed"
            out["detail"] = result
        return out
    except HTTPException as e:
        return {
            "ok": False,
            "name": name,
            "elapsed_s": round(time.monotonic() - t0, 4),
            "status_code": e.status_code,
            "detail": e.detail,
        }
    except Exception as e:
        return {
            "ok": False,
            "name": name,
            "elapsed_s": round(time.monotonic() - t0, 4),
            "error": type(e).__name__,
            "message": str(e),
        }


def _hydration_sdk_recovery_stage_text(stage: Dict[str, Any]) -> str:
    try:
        return json.dumps(stage or {}, default=str).lower()
    except Exception:
        return str(stage or "").lower()


def _hydration_sdk_recovery_stage_classification(name: str, stage: Dict[str, Any]) -> Dict[str, Any]:
    name_s = str(name or "")
    txt = _hydration_sdk_recovery_stage_text(stage)
    ok = bool((stage or {}).get("ok"))
    skipped = bool((stage or {}).get("skipped"))

    if skipped:
        if name_s == "sidecar_health":
            return {
                "status": "skipped",
                "classification": "sidecar_disabled_or_not_required",
                "meaning": "The persistent sidecar was not used for this standalone recovery run.",
                "action": "No action required unless you intentionally want to test the sidecar path.",
            }
        return {
            "status": "skipped",
            "classification": "stage_skipped",
            "meaning": str((stage or {}).get("reason") or "The stage was intentionally skipped."),
            "action": None,
        }

    if ok:
        if name_s == "get_best_buy_probe":
            result = (stage or {}).get("result") or {}
            success_count = int((result or {}).get("successCount") or 0) if isinstance(result, dict) else 0
            failure_count = int((result or {}).get("failureCount") or 0) if isinstance(result, dict) else 0
            return {
                "status": "passed",
                "classification": "get_best_buy_probe_completed",
                "meaning": f"getBestBuy probe returned {success_count} successful attempt(s) and {failure_count} failed attempt(s).",
                "action": "Keep BUY diagnostics isolated from normal UI quote paths until spot/sell recovery is stable.",
            }
        return {
            "status": "passed",
            "classification": "passed",
            "meaning": "Stage completed.",
            "action": None,
        }

    if name_s == "get_best_buy_probe":
        result = (stage or {}).get("detail") or (stage or {}).get("result") or {}
        success_count = int((result or {}).get("successCount") or 0) if isinstance(result, dict) else 0
        failure_count = int((result or {}).get("failureCount") or 0) if isinstance(result, dict) else 0
        if success_count <= 0 and failure_count > 0:
            return {
                "status": "failed",
                "classification": "get_best_buy_probe_no_successful_attempts",
                "meaning": f"getBestBuy probe completed, but returned {success_count} successful attempt(s) and {failure_count} failed attempt(s).",
                "action": "Keep exact BUY diagnostics isolated and keep OrderTicket SDK BUY/swap gates disabled.",
            }

    if "node_version_below_sdk_minimum" in txt:
        return {
            "status": "failed",
            "classification": "node_version_below_sdk_minimum",
            "meaning": "Node is below the sdk-next runtime requirement.",
            "action": "Use Node 25+ before relying on Hydration SDK quote diagnostics.",
        }

    if "direct_get_spot_price" in txt or "hydration_direct_spot_failed" in txt:
        return {
            "status": "failed",
            "classification": "sdk_direct_spot_timeout_after_context_init",
            "meaning": "SDK package import, provider setup, and sdk context creation reached getSpotPrice, but direct getSpotPrice did not return before the stage timeout.",
            "action": "Keep sdk_spot/orderbook spot paths disabled; test a different RPC/provider or a non-router state_call/indexer price source before enabling this path.",
        }

    if "get_spot_price" in txt:
        return {
            "status": "failed",
            "classification": "sdk_context_spot_timeout_after_context_init",
            "meaning": "The normal sdk context path reached router.getSpotPrice, then timed out.",
            "action": "Do not enable UTT_HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK yet.",
        }

    if "get_best_sell" in txt or "quote_sell" in txt or "getbestsell" in txt:
        return {
            "status": "failed",
            "classification": "sdk_get_best_sell_timeout_or_unavailable",
            "meaning": "The sdk-next getBestSell/sell-quote path is not independently stable.",
            "action": "Keep UTT_HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES=0 and continue using confirmed manual Router/XYK routes plus synthetic display fallback.",
        }

    if "get_best_buy" in txt or "getbestbuy" in txt:
        return {
            "status": "failed",
            "classification": "sdk_get_best_buy_timeout_or_unavailable",
            "meaning": "The isolated getBestBuy probe did not complete cleanly.",
            "action": "Keep exact-out BUY on manual route guardrails only.",
        }

    if "hydration_rpc" in txt or "rpc_" in txt:
        return {
            "status": "failed",
            "classification": "hydration_rpc_failure",
            "meaning": "The HTTP RPC sanity stage failed.",
            "action": "Check Dwellir Profile API key, HTTP URL template, and rate limits before running SDK diagnostics.",
        }

    return {
        "status": "failed",
        "classification": "failed_unclassified",
        "meaning": "Stage failed; inspect detail/stderr for the exact failing SDK call.",
        "action": "Keep normal SDK quote gates disabled until this failure is classified.",
    }



def _hydration_sdk_recovery_mark_buy_probe_no_success(stage: Dict[str, Any]) -> Dict[str, Any]:
    """Treat a completed getBestBuy probe with zero successes as a failed recovery stage."""
    if not isinstance(stage, dict):
        return stage
    result = stage.get("result")
    if not isinstance(result, dict):
        return stage
    try:
        success_count = int(result.get("successCount") or 0)
    except Exception:
        success_count = 0
    try:
        failure_count = int(result.get("failureCount") or 0)
    except Exception:
        failure_count = 0
    if success_count <= 0 and failure_count > 0:
        stage["ok"] = False
        stage["error"] = "hydration_get_best_buy_probe_no_successful_attempts"
        stage["detail"] = result
        stage["recoveryMeaning"] = "The getBestBuy probe process completed, but no exact-buy attempt returned a usable SDK route."
    return stage

def _hydration_sdk_recovery_analysis(
    *,
    stages: Dict[str, Any],
    classifications: Dict[str, Any],
) -> Dict[str, Any]:
    env_stage_names = ["node_version", "files", "ws_config", "rpc_system_chain"]
    environment_ok = all(bool((stages.get(k) or {}).get("ok")) for k in env_stage_names if k in stages)
    first_failed = next(
        (
            k
            for k, v in (stages or {}).items()
            if isinstance(v, dict) and not bool(v.get("ok")) and not bool(v.get("skipped"))
        ),
        None,
    )
    spot_failures = [
        k
        for k, v in (classifications or {}).items()
        if str((v or {}).get("classification") or "").startswith("sdk_")
        and "spot" in str((v or {}).get("classification") or "")
        and str((v or {}).get("status") or "") == "failed"
    ]
    sell_failures = [
        k
        for k, v in (classifications or {}).items()
        if "sell" in str((v or {}).get("classification") or "")
        and str((v or {}).get("status") or "") == "failed"
    ]
    buy_stage = stages.get("get_best_buy_probe") or {}
    buy_result = buy_stage.get("result") if isinstance(buy_stage, dict) else {}
    buy_success_count = 0
    if isinstance(buy_result, dict):
        try:
            buy_success_count = int(buy_result.get("successCount") or 0)
        except Exception:
            buy_success_count = 0

    if environment_ok and (spot_failures or sell_failures):
        finding = "Environment and RPC are healthy, but sdk-next router quote methods still hang or fail at the router call layer."
    elif environment_ok:
        finding = "Environment and RPC sanity checks are healthy."
    else:
        finding = "One or more environment/RPC prerequisites failed before SDK router recovery could be trusted."

    recommendations: List[str] = []
    if spot_failures:
        recommendations.append("Keep UTT_HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK=0; direct/context spot getSpotPrice is not recovered yet.")
    if sell_failures:
        recommendations.append("Keep UTT_HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES=0; getBestSell is not recovered yet.")
    if buy_success_count > 0:
        recommendations.append("getBestBuy can be probed in an isolated helper, but do not promote it into normal OrderTicket flow until spot/sell quote paths are stable.")
    if environment_ok and (spot_failures or sell_failures):
        recommendations.append("Next recovery step should compare RPC providers or test a non-router state_call/indexer price source instead of reopening background sdk-next polling.")
    if not recommendations:
        recommendations.append("Review individual stage classifications before changing SDK gates.")

    return {
        "environmentOk": bool(environment_ok),
        "firstFailedStage": first_failed,
        "spotFailures": spot_failures,
        "sellFailures": sell_failures,
        "getBestBuySuccessCount": buy_success_count,
        "normalSdkQuoteGatesShouldRemainDisabled": bool(spot_failures or sell_failures),
        "finding": finding,
        "recommendations": recommendations,
    }


@router.get("/hydration/sdk_recovery_diagnostics")
async def hydration_sdk_recovery_diagnostics(
    symbol: str = Query("DOT-HDX", description="Symbol pair to diagnose, e.g. DOT-HDX or HDX-DOT."),
    amount_ui: float = Query(1.0, gt=0, description="Human amount used for exact-in getBestSell diagnostics."),
    buy_amount_ui: Optional[float] = Query(None, gt=0, description="Human output amount used for getBestBuy diagnostics. Defaults to amount_ui."),
    step_timeout_s: float = Query(10.0, ge=3.0, le=60.0, description="Per-stage JS SDK timeout."),
    include_sidecar_health: bool = Query(True),
    include_spot_direct: bool = Query(True),
    include_spot_context: bool = Query(True),
    include_get_best_sell: bool = Query(True),
    include_get_best_buy: bool = Query(True),
    include_swap_tx_build: bool = Query(False, description="Unsigned SDK swap_tx build only; requires user_pubkey."),
    user_pubkey: Optional[str] = Query(None, description="Required only when include_swap_tx_build=true."),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Standalone, bounded Hydration sdk-next recovery diagnostics.

    This endpoint is explicit and read-only.  It uses short-lived helper probes
    so recovery testing cannot poison the persistent sidecar or background price
    paths.  No signing, submission, route-registry writes, ledger writes, or
    FIFO mutations occur here.
    """
    base, quote = _parse_symbol(symbol)
    base_meta = _resolve_asset(base, db=db)
    quote_meta = _resolve_asset(quote, db=db)
    timeout_s = float(step_timeout_s)
    buy_amount = float(buy_amount_ui if buy_amount_ui is not None else amount_ui)
    ws_url = _hydration_ws_url()
    helper = _hydration_helper_path()
    sidecar_script = _hydration_sidecar_script_path()
    buy_probe = _hydration_buy_probe_path()

    stages: Dict[str, Any] = {}
    stages["node_version"] = _hydration_node_version_probe()
    stages["files"] = {
        "ok": bool(helper.exists() and sidecar_script.exists()),
        "helper": {"path": str(helper), "exists": bool(helper.exists())},
        "sidecarScript": {"path": str(sidecar_script), "exists": bool(sidecar_script.exists())},
        "getBestBuyProbe": {
            "path": str(buy_probe),
            "exists": bool(buy_probe.exists()),
            "requiredOnlyFor": "include_get_best_buy=true or legacy /hydration/getbestbuy_probe endpoint",
        },
    }
    stages["ws_config"] = {
        "ok": bool(ws_url),
        "wsConfigured": bool(ws_url),
        "wsUrl": _redact_url(ws_url or ""),
        "keySource": _dwellir_hydration_key_source(),
        "message": None if ws_url else "Hydration WebSocket URL is not configured.",
    }

    stages["rpc_system_chain"] = await _hydration_sdk_recovery_stage(
        "rpc_system_chain",
        lambda: _rpc_probe("system_chain", []),
    )

    if include_sidecar_health:
        stages["sidecar_health"] = await _hydration_sdk_recovery_stage(
            "sidecar_health",
            lambda: _sidecar_health(),
        )

    common = {
        "venue": "polkadot_hydration",
        "rawSymbol": symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base,
        "quote": quote,
        "stepTimeoutS": timeout_s,
        "sdkUseCase": "diagnostic",
        "routeMode": "sdk_recovery",
    }

    if include_spot_direct:
        stages["spot_direct_forward"] = await _hydration_sdk_recovery_stage(
            "spot_direct_forward",
            lambda: _run_hydration_helper_sdk_recovery(
                {
                    **common,
                    "mode": "price_spot_direct",
                    "assetIn": _helper_asset_payload(base_meta),
                    "assetOut": _helper_asset_payload(quote_meta),
                },
                timeout_s=timeout_s,
            ),
        )
        stages["spot_direct_reverse"] = await _hydration_sdk_recovery_stage(
            "spot_direct_reverse",
            lambda: _run_hydration_helper_sdk_recovery(
                {
                    **common,
                    "mode": "price_spot_direct",
                    "rawSymbol": f"{quote}-{base}",
                    "resolvedSymbol": f"{quote}-{base}",
                    "base": quote,
                    "quote": base,
                    "assetIn": _helper_asset_payload(quote_meta),
                    "assetOut": _helper_asset_payload(base_meta),
                },
                timeout_s=timeout_s,
            ),
        )

    if include_spot_context:
        stages["spot_context_forward"] = await _hydration_sdk_recovery_stage(
            "spot_context_forward",
            lambda: _run_hydration_helper_sdk_recovery(
                {
                    **common,
                    "mode": "price_spot",
                    "assetIn": _helper_asset_payload(base_meta),
                    "assetOut": _helper_asset_payload(quote_meta),
                },
                timeout_s=timeout_s,
            ),
        )
        stages["spot_context_reverse"] = await _hydration_sdk_recovery_stage(
            "spot_context_reverse",
            lambda: _run_hydration_helper_sdk_recovery(
                {
                    **common,
                    "mode": "price_spot",
                    "rawSymbol": f"{quote}-{base}",
                    "resolvedSymbol": f"{quote}-{base}",
                    "base": quote,
                    "quote": base,
                    "assetIn": _helper_asset_payload(quote_meta),
                    "assetOut": _helper_asset_payload(base_meta),
                },
                timeout_s=timeout_s,
            ),
        )

    if include_get_best_sell:
        stages["get_best_sell_forward"] = await _hydration_sdk_recovery_stage(
            "get_best_sell_forward",
            lambda: _run_hydration_helper_sdk_recovery(
                {
                    **common,
                    "mode": "quote_sell",
                    "assetIn": _helper_asset_payload(base_meta),
                    "assetOut": _helper_asset_payload(quote_meta),
                    "amountInAtomic": str(_ui_to_atomic(float(amount_ui), int(base_meta.get("decimals") or 0))),
                    "amountInUi": float(amount_ui),
                },
                timeout_s=timeout_s,
            ),
        )
        stages["get_best_sell_reverse"] = await _hydration_sdk_recovery_stage(
            "get_best_sell_reverse",
            lambda: _run_hydration_helper_sdk_recovery(
                {
                    **common,
                    "mode": "quote_sell",
                    "rawSymbol": f"{quote}-{base}",
                    "resolvedSymbol": f"{quote}-{base}",
                    "base": quote,
                    "quote": base,
                    "assetIn": _helper_asset_payload(quote_meta),
                    "assetOut": _helper_asset_payload(base_meta),
                    "amountInAtomic": str(_ui_to_atomic(float(amount_ui), int(quote_meta.get("decimals") or 0))),
                    "amountInUi": float(amount_ui),
                },
                timeout_s=timeout_s,
            ),
        )

    if include_get_best_buy:
        async def _buy_probe_stage() -> Dict[str, Any]:
            if not buy_probe.exists():
                return {
                    "ok": False,
                    "skipped": True,
                    "error": "hydration_getbestbuy_probe_missing",
                    "message": "The getBestBuy probe file is missing. Save backend/app/services/hydration_getbestbuy_probe_p1_8h1.mjs from this patch to enable this stage.",
                    "probePath": str(buy_probe),
                }
            attempts = [
                {
                    "name": "buy_base_with_quote",
                    "assetInSymbol": quote,
                    "assetOutSymbol": base,
                    "assetInId": _hydration_sdk_asset_id(quote_meta),
                    "assetOutId": _hydration_sdk_asset_id(base_meta),
                    "amountOutUi": buy_amount,
                    "amountOutAtomic": str(_ui_to_atomic(buy_amount, int(base_meta.get("decimals") or 0))),
                    "meaning": f"Spend {quote} to receive exactly {buy_amount} {base}.",
                },
                {
                    "name": "buy_quote_with_base",
                    "assetInSymbol": base,
                    "assetOutSymbol": quote,
                    "assetInId": _hydration_sdk_asset_id(base_meta),
                    "assetOutId": _hydration_sdk_asset_id(quote_meta),
                    "amountOutUi": buy_amount,
                    "amountOutAtomic": str(_ui_to_atomic(buy_amount, int(quote_meta.get("decimals") or 0))),
                    "meaning": f"Spend {base} to receive exactly {buy_amount} {quote}. Reverse diagnostic only.",
                },
            ]
            return await _run_hydration_buy_probe(
                {
                    "mode": "getbestbuy_probe",
                    "venue": "polkadot_hydration",
                    "rawSymbol": symbol,
                    "resolvedSymbol": f"{base}-{quote}",
                    "base": base_meta,
                    "quote": quote_meta,
                    "wsUrl": ws_url,
                    "stepTimeoutS": timeout_s,
                    "attempts": attempts,
                },
                timeout_s=timeout_s * len(attempts),
            )
        stages["get_best_buy_probe"] = _hydration_sdk_recovery_mark_buy_probe_no_success(
            await _hydration_sdk_recovery_stage("get_best_buy_probe", _buy_probe_stage)
        )

    if include_swap_tx_build:
        if not str(user_pubkey or "").strip():
            stages["swap_tx_build_exact_in"] = {
                "ok": False,
                "skipped": True,
                "reason": "user_pubkey_required",
                "message": "Pass user_pubkey only for deliberate unsigned SDK swap_tx build diagnostics.",
            }
        else:
            stages["swap_tx_build_exact_in"] = await _hydration_sdk_recovery_stage(
                "swap_tx_build_exact_in",
                lambda: _run_hydration_helper_sdk_recovery(
                    {
                        **common,
                        "mode": "swap_tx",
                        "side": "sell",
                        "assetIn": _helper_asset_payload(base_meta),
                        "assetOut": _helper_asset_payload(quote_meta),
                        "amountMode": "exact_in",
                        "amountInAtomic": str(_ui_to_atomic(float(amount_ui), int(base_meta.get("decimals") or 0))),
                        "beneficiary": str(user_pubkey or "").strip(),
                        "slippageBps": 50,
                    },
                    timeout_s=max(timeout_s * 4.0, timeout_s + 15.0),
                ),
            )

    stage_values = [v for v in stages.values() if isinstance(v, dict)]
    failed = [k for k, v in stages.items() if isinstance(v, dict) and not bool(v.get("ok")) and not bool(v.get("skipped"))]
    skipped = [k for k, v in stages.items() if isinstance(v, dict) and bool(v.get("skipped"))]
    passed = [k for k, v in stages.items() if isinstance(v, dict) and bool(v.get("ok"))]
    stage_classifications = {
        k: _hydration_sdk_recovery_stage_classification(k, v)
        for k, v in stages.items()
        if isinstance(v, dict)
    }
    analysis = _hydration_sdk_recovery_analysis(
        stages=stages,
        classifications=stage_classifications,
    )

    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "network": "hydration",
        "diagnosticOnly": True,
        "mutation": False,
        "signing": False,
        "submission": False,
        "rawSymbol": symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base_meta,
        "quote": quote_meta,
        "amountUi": float(amount_ui),
        "buyAmountUi": buy_amount,
        "stepTimeoutS": timeout_s,
        "summary": {
            "passed": passed,
            "failed": failed,
            "skipped": skipped,
            "totalStages": len(stage_values),
            "firstFailedStage": analysis.get("firstFailedStage"),
            "normalSdkQuoteGatesShouldRemainDisabled": bool(analysis.get("normalSdkQuoteGatesShouldRemainDisabled")),
            "recommendation": " ".join(analysis.get("recommendations") or []),
        },
        "analysis": analysis,
        "stageClassifications": stage_classifications,
        "sdkScopes": {
            "globalRouterQuotesEnabled": bool(_HYDRATION_ENABLE_ROUTER_QUOTES),
            "sdkOrderbookQuotesEnabled": bool(_HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES),
            "sdkSpotOrderbookEnabled": bool(_HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK),
            "sdkOrderTicketQuotesEnabled": bool(_HYDRATION_ENABLE_SDK_ORDER_TICKET_QUOTES),
            "sdkSwapTxEnabled": bool(_HYDRATION_ENABLE_SDK_SWAP_TX),
            "backgroundSdkPricesEnabled": bool(_HYDRATION_ENABLE_BACKGROUND_SDK_PRICES),
            "diagnosticBypassesUiQuoteGates": True,
        },
        "safety": {
            "readOnly": True,
            "usesShortLivedHelper": True,
            "doesNotStartBackgroundPolling": True,
            "doesNotSign": True,
            "doesNotSubmit": True,
            "doesNotMutateRouteRegistry": True,
            "doesNotMutateLedgerOrFifo": True,
        },
        "stages": stages,
    }




def _hydration_sdk_recovery_state_call_methods(raw_csv: Optional[str], *, max_methods: int) -> List[str]:
    raw = str(raw_csv or "").strip()
    if raw:
        methods = [m.strip() for m in raw.split(",") if m.strip()]
    else:
        methods = [
            "OmnipoolApi_quotePrice",
            "OmnipoolApi_quote_price",
            "OmnipoolRuntimeApi_quotePrice",
            "OmnipoolRuntimeApi_quote_price",
            "RouterApi_quotePrice",
            "RouterApi_quote_price",
            "TradeRouterApi_quotePrice",
            "TradeRouterApi_quote_price",
            "TradeExecutionApi_quotePrice",
            "TradeExecutionApi_quote_price",
        ]

    out: List[str] = []
    for method in methods:
        if method and method not in out:
            out.append(method)
        if len(out) >= max(1, int(max_methods)):
            break
    return out


def _hydration_sdk_recovery_extra_rpc_urls(raw_csv: Optional[str]) -> List[str]:
    out: List[str] = []

    def _add(value: Any) -> None:
        s = str(value or "").strip()
        if not s or _looks_placeholder_secret(s):
            return
        if not (s.startswith("http://") or s.startswith("https://")):
            return
        if s not in out:
            out.append(s)

    for env_name in ("UTT_HYDRATION_RECOVERY_RPC_URLS_JSON", "UTT_HYDRATION_RECOVERY_RPC_URLS"):
        raw_env = os.getenv(env_name)
        if not raw_env:
            continue
        try:
            parsed = json.loads(raw_env)
            if isinstance(parsed, list):
                for item in parsed:
                    _add(item)
            elif isinstance(parsed, str):
                for part in parsed.split(","):
                    _add(part)
        except Exception:
            for part in str(raw_env or "").split(","):
                _add(part)

    for part in str(raw_csv or "").split(","):
        _add(part)

    return out


def _hydration_sdk_recovery_rpc_candidates(
    *,
    include_current_rpc: bool,
    extra_rpc_urls_csv: Optional[str],
    max_rpc_candidates: int,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()

    def _add(label: str, url: Optional[str], source: str) -> None:
        clean = str(url or "").strip()
        if not clean or clean in seen or _looks_placeholder_secret(clean):
            return
        if not (clean.startswith("http://") or clean.startswith("https://")):
            return
        seen.add(clean)
        out.append({
            "label": label,
            "url": clean,
            "source": source,
            "redactedUrl": _redact_url(clean),
        })

    if include_current_rpc:
        try:
            _add("current", _hydration_rpc_url(), _dwellir_hydration_key_source() or "current_config")
        except Exception:
            pass

    for idx, url in enumerate(_hydration_sdk_recovery_extra_rpc_urls(extra_rpc_urls_csv), start=1):
        _add(f"extra_{idx}", url, "query_or_env")

    return out[: max(1, int(max_rpc_candidates))]


async def _rpc_probe_url(
    url: str,
    method: str,
    params: Optional[List[Any]] = None,
    *,
    timeout_s: Optional[float] = None,
) -> Dict[str, Any]:
    clean_url = str(url or "").strip()
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}
    try:
        async with httpx.AsyncClient(timeout=float(timeout_s or _HYDRATION_TIMEOUT_S)) as client:
            r = await client.post(clean_url, json=payload, headers={"content-type": "application/json"})
    except httpx.RequestError as e:
        return {
            "ok": False,
            "error": "hydration_rpc_probe_request_failed",
            "method": method,
            "exc": type(e).__name__,
            "message": str(e),
            "rpc_url": _redact_url(clean_url),
        }

    out: Dict[str, Any] = {
        "ok": r.status_code < 400,
        "httpStatus": r.status_code,
        "rpc_url": _redact_url(clean_url),
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


async def _state_call_probe_method_at_url(url: str, method: str, data: str = "0x") -> Dict[str, Any]:
    clean = _clean_hex(data or "0x")
    rpc_result = await _rpc_probe_url(url, "state_call", [method, clean])
    return {
        "method": method,
        "data": clean,
        "classification": _classify_state_call_probe(method, rpc_result),
        "rpc": rpc_result,
    }


def _hydration_sdk_recovery_sides(raw_side: str) -> List[str]:
    side_norm = str(raw_side or "sell").strip().lower()
    if side_norm in {"both", "all"}:
        return ["sell", "buy"]
    if side_norm not in {"sell", "buy"}:
        raise HTTPException(status_code=422, detail={"error": "invalid_side", "side": raw_side, "expected": "sell|buy|both"})
    return [side_norm]


def _hydration_sdk_recovery_quote_candidates(
    *,
    candidate: str,
    raw_data: Optional[str],
    asset_in_id: int,
    asset_out_id: int,
    amount_atomic: int,
) -> List[Dict[str, Any]]:
    if raw_data:
        return [{"name": "raw", "data": _clean_hex(raw_data), "note": "caller-provided raw SCALE input"}]

    all_candidates = _state_call_quote_candidate_payloads(asset_in_id, asset_out_id, amount_atomic)
    cand = str(candidate or "auto").strip()
    if cand == "auto":
        return all_candidates

    selected = [c for c in all_candidates if c.get("name") == cand]
    if not selected:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "unknown_state_call_candidate",
                "candidate": candidate,
                "available": ["auto"] + [c.get("name") for c in all_candidates] + ["raw"],
            },
        )
    return selected


async def _hydration_sdk_recovery_state_call_attempts_for_rpc(
    *,
    rpc_url: str,
    symbol: str,
    amount: float,
    sides: List[str],
    methods: List[str],
    candidate: str,
    raw_data: Optional[str],
    base_meta: Dict[str, Any],
    quote_meta: Dict[str, Any],
) -> List[Dict[str, Any]]:
    attempts: List[Dict[str, Any]] = []
    base_symbol, quote_symbol = _parse_symbol(symbol)

    for side_norm in sides:
        if side_norm == "sell":
            asset_in_meta = base_meta
            asset_out_meta = quote_meta
        else:
            asset_in_meta = quote_meta
            asset_out_meta = base_meta

        asset_in_id = _hydration_sdk_asset_id(asset_in_meta)
        asset_out_id = _hydration_sdk_asset_id(asset_out_meta)
        amount_atomic = _ui_to_atomic(float(amount), int(asset_in_meta.get("decimals") or 0))
        candidates = _hydration_sdk_recovery_quote_candidates(
            candidate=candidate,
            raw_data=raw_data,
            asset_in_id=asset_in_id,
            asset_out_id=asset_out_id,
            amount_atomic=amount_atomic,
        )

        for call_method in methods:
            for c in candidates:
                rpc_result = await _rpc_probe_url(rpc_url, "state_call", [call_method, c["data"]])
                classification = _classify_state_call_probe(call_method, rpc_result)
                attempt = {
                    "side": side_norm,
                    "method": call_method,
                    "candidate": c.get("name"),
                    "candidateNote": c.get("note"),
                    "data": c.get("data"),
                    "classification": classification,
                    "assetIn": asset_in_meta,
                    "assetOut": asset_out_meta,
                    "assetInId": asset_in_id,
                    "assetOutId": asset_out_id,
                    "amountUi": float(amount),
                    "amountAtomic": str(amount_atomic),
                    "rpc": rpc_result,
                }
                if rpc_result.get("ok") and rpc_result.get("result") is not None:
                    attempt["decodeProbe"] = _decode_state_call_probe_result(
                        rpc_result.get("result"),
                        int(asset_out_meta.get("decimals") or 0),
                    )
                attempts.append(attempt)

    return attempts


def _hydration_sdk_recovery_state_call_summary(attempts: List[Dict[str, Any]]) -> Dict[str, Any]:
    accepted = [a for a in attempts if a.get("classification") == "accepted"]
    exported = [
        a for a in attempts
        if a.get("classification") in {"exported_decode_or_input_error", "exported_execution_error"}
    ]
    not_found = [a for a in attempts if a.get("classification") == "not_found"]
    unknown = [a for a in attempts if a.get("classification") == "unknown_error"]

    accepted_methods = []
    exported_methods = []
    for item in accepted:
        m = item.get("method")
        if m and m not in accepted_methods:
            accepted_methods.append(m)
    for item in exported:
        m = item.get("method")
        if m and m not in exported_methods:
            exported_methods.append(m)

    if accepted:
        finding = "At least one state_call quote candidate returned a runtime result. Decode the accepted shape before using it as a price source."
        next_action = "Inspect decodeProbe for the accepted candidate and compare against a known UI quote before any trading use."
    elif exported:
        finding = "One or more runtime API names appear exported, but the SCALE input shape is still wrong or incomplete."
        next_action = "Use metadata_v15_focused_scan around the exported method/pallet names, then add the exact SCALE shape as a later candidate."
    else:
        finding = "No tested quote method/payload shape returned a usable runtime result."
        next_action = "Do not enable state_call quotes from this run; compare another RPC/provider or add better method names from metadata scan."

    return {
        "acceptedCandidateCount": len(accepted),
        "exportedButNeedsSignatureCount": len(exported),
        "notFoundCount": len(not_found),
        "unknownErrorCount": len(unknown),
        "acceptedMethods": accepted_methods,
        "exportedButNeedsSignatureMethods": exported_methods,
        "finding": finding,
        "nextAction": next_action,
    }


def _hydration_runtime_method_hunt_terms(raw_csv: Optional[str]) -> List[str]:
    raw = str(raw_csv or "").strip()
    if raw:
        terms = [p.strip() for p in raw.split(",") if p.strip()]
    else:
        terms = [
            "quote",
            "price",
            "sell",
            "buy",
            "trade",
            "spot",
            "amount",
            "router",
            "omnipool",
            "RuntimeApi",
            "Api",
        ]

    out: List[str] = []
    for term in terms:
        clean = str(term or "").strip()
        if clean and clean not in out:
            out.append(clean)
    return out


def _hydration_metadata_identifier_like(value: Any) -> bool:
    s = str(value or "").strip()
    if not s or len(s) > 96:
        return False
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
    return bool(any(ch.isalpha() for ch in s) and all(ch in allowed for ch in s))


def _hydration_camel_to_snake(value: Any) -> str:
    s = str(value or "").strip()
    if not s:
        return ""
    out: List[str] = []
    prev = ""
    for ch in s:
        if ch.isupper() and prev and (prev.islower() or prev.isdigit()):
            out.append("_")
        out.append(ch.lower())
        prev = ch
    return "".join(out)


def _hydration_metadata_runtime_method_candidates(
    strings: List[Dict[str, Any]],
    *,
    terms: List[str],
    max_candidates: int,
) -> Dict[str, Any]:
    term_lowers = [str(t or "").strip().lower() for t in (terms or []) if str(t or "").strip()]
    api_like: List[str] = []
    method_like: List[str] = []
    direct_method_like: List[str] = []
    matched_strings: List[Dict[str, Any]] = []

    def _add_unique(items: List[str], value: Any) -> None:
        clean = str(value or "").strip()
        if clean and clean not in items:
            items.append(clean)

    for item in strings or []:
        text_value = str((item or {}).get("text") or "").strip()
        text_lower = text_value.lower()
        if not _hydration_metadata_identifier_like(text_value):
            continue

        if "api" in text_lower or "runtimeapi" in text_lower:
            _add_unique(api_like, text_value)

        if any(term and term in text_lower for term in term_lowers):
            matched_strings.append(item)
            if "api_" in text_lower or text_lower.endswith("_api"):
                _add_unique(direct_method_like, text_value)
            if not text_lower.endswith("api") and "runtimeapi" not in text_lower:
                _add_unique(method_like, text_value)

    # Prefer concrete quote/trade/price words before generic router strings.
    def _method_rank(value: str) -> Tuple[int, str]:
        lo = value.lower()
        score = 50
        for idx, needle in enumerate(["quote", "price", "sell", "buy", "trade", "spot", "amount"]):
            if needle in lo:
                score = min(score, idx)
        if lo in {"router", "omnipool", "runtime", "api"}:
            score += 20
        return (score, lo)

    api_like = sorted(api_like, key=lambda x: (0 if x.endswith("Api") else 1, x.lower()))
    method_like = sorted(method_like, key=_method_rank)

    candidates: List[str] = []
    for method in direct_method_like:
        _add_unique(candidates, method)

    # Generate state_call-style RuntimeApi_methodName candidates from metadata
    # strings.  This is intentionally speculative and diagnostic-only; any hit
    # still needs a real SCALE signature confirmation before use.
    for api_name in api_like[:24]:
        for method_name in method_like[:48]:
            if method_name == api_name:
                continue
            raw = method_name
            snake = _hydration_camel_to_snake(method_name)
            for variant in (raw, snake):
                if not variant:
                    continue
                _add_unique(candidates, f"{api_name}_{variant}")
                # Some runtime APIs use lower snake method names while metadata
                # nearby strings may expose CamelCase names.
                if variant != variant.lower():
                    _add_unique(candidates, f"{api_name}_{variant.lower()}")
            if len(candidates) >= int(max_candidates):
                break
        if len(candidates) >= int(max_candidates):
            break

    # Keep the older hand-written candidates at the end for regression context,
    # but do not let them crowd out metadata-derived candidates.
    for method in _runtime_api_default_method_candidates():
        if method not in {"Core_version", "Metadata_metadata_versions", "Metadata_metadata_at_version"}:
            _add_unique(candidates, method)

    return {
        "apiLikeStrings": api_like[:80],
        "methodLikeStrings": method_like[:120],
        "matchedStrings": matched_strings[:120],
        "candidateMethods": candidates[: max(0, int(max_candidates))],
        "generationNotes": [
            "Candidates are generated from bounded ASCII strings inside Metadata v15.",
            "A state_call candidate is useful only if the method probe is accepted or returns a decode/input error instead of not_found.",
            "Even accepted/exported candidates remain diagnostic-only until the exact SCALE signature and output decode are confirmed.",
        ],
    }


def _hydration_runtime_method_hunt_summary(probes: List[Dict[str, Any]]) -> Dict[str, Any]:
    accepted = [p for p in probes if p.get("classification") == "accepted"]
    exported = [
        p for p in probes
        if p.get("classification") in {"exported_decode_or_input_error", "exported_execution_error"}
    ]
    not_found = [p for p in probes if p.get("classification") == "not_found"]
    unknown = [p for p in probes if p.get("classification") == "unknown_error"]

    accepted_methods = [str(p.get("method") or "") for p in accepted if p.get("method")]
    exported_methods = [str(p.get("method") or "") for p in exported if p.get("method")]

    if accepted:
        finding = "Metadata-guided method hunt found at least one accepted state_call method."
        next_action = "Use sdk_recovery_state_call_compare with methods_csv set to the accepted method and candidate/raw_data variants, then decode against a known UI quote."
    elif exported:
        finding = "Metadata-guided method hunt found exported runtime method names, but the empty probe payload is not a valid signature."
        next_action = "Use metadata windows around the exported names to derive the exact SCALE input type before testing quote payloads."
    else:
        finding = "Metadata-guided method hunt did not find a callable quote/runtime method name from the bounded candidate set."
        next_action = "Keep state_call quotes disabled; either broaden metadata terms/max_method_probes or move to an indexer/manual confirmed route price source."

    return {
        "acceptedCount": len(accepted),
        "exportedButNeedsSignatureCount": len(exported),
        "notFoundCount": len(not_found),
        "unknownErrorCount": len(unknown),
        "acceptedMethods": accepted_methods,
        "exportedButNeedsSignatureMethods": exported_methods,
        "finding": finding,
        "nextAction": next_action,
    }


@router.get("/hydration/sdk_recovery_metadata_method_hunt")
async def hydration_sdk_recovery_metadata_method_hunt(
    terms_csv: Optional[str] = Query(None, description="Comma-separated metadata terms used to generate runtime method candidates."),
    method_candidates_csv: Optional[str] = Query(None, description="Optional explicit method candidates to prepend before metadata-generated candidates."),
    probe_data: str = Query("0x", description="Raw hex payload for method-name probes. Empty SCALE input is safest for discovery."),
    include_method_probes: bool = Query(True, description="If true, run state_call probes for generated method names."),
    max_ascii_strings: int = Query(5000, ge=250, le=12000),
    max_candidates: int = Query(120, ge=10, le=300),
    max_method_probes: int = Query(60, ge=0, le=150),
) -> Dict[str, Any]:
    """Metadata-guided runtime method discovery for H-SDK recovery.

    This is the follow-up for state_call compare runs where every tested quote
    method returned not_found.  It fetches Metadata v15, extracts bounded
    identifier-like strings around quote/price/trade terms, generates possible
    state_call method names, and optionally probes those names with empty input.

    It remains diagnostic-only: no sdk-next router, no WebSocket chainHead,
    no signing/submission, and no DB mutation.
    """
    terms = _hydration_runtime_method_hunt_terms(terms_csv)
    metadata_probe = await _state_call_probe_method("Metadata_metadata_at_version", _metadata_v15_probe_payload())
    metadata_hex = _metadata_result_hex_from_probe(metadata_probe)
    if not metadata_hex or metadata_probe.get("classification") != "accepted":
        raise HTTPException(
            status_code=502,
            detail={
                "error": "hydration_metadata_v15_fetch_failed",
                "message": "Metadata_metadata_at_version did not return an accepted Metadata v15 blob.",
                "probe": metadata_probe,
                "probeData": _metadata_v15_probe_payload(),
            },
        )

    metadata_bytes = _bytes_from_hex_result(metadata_hex)
    metadata_strings = _metadata_ascii_strings(metadata_bytes, min_len=3, max_strings=int(max_ascii_strings))
    generated = _hydration_metadata_runtime_method_candidates(
        metadata_strings,
        terms=terms,
        max_candidates=int(max_candidates),
    )

    candidates: List[str] = []
    def _add_candidate(value: Any) -> None:
        clean = str(value or "").strip()
        if clean and clean not in candidates:
            candidates.append(clean)

    for part in str(method_candidates_csv or "").split(","):
        _add_candidate(part)

    for method in generated.get("candidateMethods") or []:
        _add_candidate(method)

    probe_candidates = candidates[: max(0, int(max_method_probes))]
    method_probes: List[Dict[str, Any]] = []
    clean_probe_data = _clean_hex(probe_data or "0x")
    if include_method_probes and probe_candidates:
        for method in probe_candidates:
            method_probes.append(await _state_call_probe_method(method, clean_probe_data))

    summary = _hydration_runtime_method_hunt_summary(method_probes)
    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "network": "hydration",
        "diagnosticOnly": True,
        "mutation": False,
        "signing": False,
        "submission": False,
        "usesSdkNextRouter": False,
        "usesWebSocketChainHead": False,
        "rpc_url": _redact_url(_hydration_rpc_url()),
        "metadata": {
            "method": "Metadata_metadata_at_version",
            "probeData": _metadata_v15_probe_payload(),
            "classification": metadata_probe.get("classification"),
            "byteLen": len(metadata_bytes),
            "asciiStringCountReturned": len(metadata_strings),
        },
        "huntConfig": {
            "terms": terms,
            "probeData": clean_probe_data,
            "includeMethodProbes": bool(include_method_probes),
            "maxAsciiStrings": int(max_ascii_strings),
            "maxCandidates": int(max_candidates),
            "maxMethodProbes": int(max_method_probes),
            "explicitCandidateCount": len([p for p in str(method_candidates_csv or "").split(",") if p.strip()]),
            "candidateCount": len(candidates),
            "probedCandidateCount": len(probe_candidates) if include_method_probes else 0,
        },
        "candidateGeneration": generated,
        "candidateMethods": candidates[: max(0, int(max_candidates))],
        "methodProbeSummary": summary,
        "methodProbes": method_probes,
        "safety": {
            "readOnly": True,
            "doesNotUseSdkNextRouter": True,
            "doesNotUseWsChainHead": True,
            "doesNotSign": True,
            "doesNotSubmit": True,
            "doesNotMutateRouteRegistry": True,
            "doesNotMutateLedgerOrFifo": True,
        },
        "recommendation": (
            "If this hunt returns exportedButNeedsSignatureMethods, use those names in sdk_recovery_state_call_compare "
            "with candidate/raw_data variants. If all candidates are not_found, leave state_call quotes disabled and use "
            "confirmed manual routes plus external/cached prices until a reliable runtime/indexer source is identified."
        ),
    }


def _hydration_sdk_recovery_closeout_payload() -> Dict[str, Any]:
    """Summarize the current Hydration SDK recovery decision.

    H-SDK.3A-D proved that the local environment/RPC are healthy, but the
    sdk-next router quote path and guessed RuntimeApi state_call quote names are
    not reliable enough to reopen live quote gates.  This payload is deliberately
    static/config-derived: it does not run Node, state_call probes, signing, or
    any DB mutation.
    """
    recommended_disabled = {
        "UTT_HYDRATION_ENABLE_ROUTER_QUOTES": bool(_HYDRATION_ENABLE_ROUTER_QUOTES),
        "UTT_HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES": bool(_HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES),
        "UTT_HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK": bool(_HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK),
        "UTT_HYDRATION_ENABLE_SDK_ORDER_TICKET_QUOTES": bool(_HYDRATION_ENABLE_SDK_ORDER_TICKET_QUOTES),
        "UTT_HYDRATION_ENABLE_SDK_SWAP_TX": bool(_HYDRATION_ENABLE_SDK_SWAP_TX),
        "UTT_HYDRATION_ENABLE_BACKGROUND_SDK_PRICES": bool(_HYDRATION_ENABLE_BACKGROUND_SDK_PRICES),
        "UTT_HYDRATION_ENABLE_STATE_CALL_QUOTES": bool(_HYDRATION_ENABLE_STATE_CALL_QUOTES),
        "UTT_HYDRATION_PRICE_CACHE_USE_SDK_FALLBACK": bool(_HYDRATION_PRICE_CACHE_USE_SDK_FALLBACK),
    }
    unexpectedly_enabled = [name for name, enabled in recommended_disabled.items() if bool(enabled)]
    safe_enabled = {
        "UTT_HYDRATION_ENABLE_MANUAL_ROUTER_FALLBACK": bool(_HYDRATION_ENABLE_MANUAL_ROUTER_FALLBACK),
        "UTT_HYDRATION_ENABLE_MANUAL_POOL_FALLBACK": bool(_HYDRATION_ENABLE_MANUAL_POOL_FALLBACK),
        "UTT_HYDRATION_MANUAL_POOL_LIVE_RESERVES": bool(_HYDRATION_MANUAL_POOL_LIVE_RESERVES),
        "UTT_HYDRATION_ENABLE_ORDERBOOK_SYNTHETIC_FALLBACK": bool(_HYDRATION_ENABLE_ORDERBOOK_SYNTHETIC_FALLBACK),
        "UTT_HYDRATION_ENABLE_EXTERNAL_USD_PRICES": bool(_HYDRATION_ENABLE_EXTERNAL_USD_PRICES),
    }
    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "network": "hydration",
        "diagnosticOnly": True,
        "mutation": False,
        "signing": False,
        "submission": False,
        "decision": "manual_confirmed_routes_plus_external_cached_prices",
        "status": "sdk_router_and_state_call_recovery_closed_for_now",
        "normalSdkQuoteGatesShouldRemainDisabled": True,
        "stateCallQuotesShouldRemainDisabled": True,
        "routerQuotesShouldRemainDisabled": True,
        "sdkPriceFallbackShouldRemainDisabled": True,
        "manualRoutesRemainPrimary": True,
        "syntheticOrderbooksRemainVisualOnly": True,
        "findingSummary": [
            {
                "id": "H-SDK.3A",
                "finding": "Node, helper files, WebSocket configuration, Hydration RPC, and basic environment checks passed.",
            },
            {
                "id": "H-SDK.3A.2",
                "finding": "Diagnostic classification now correctly distinguishes skipped, failed, and nested ok:false stages.",
            },
            {
                "id": "H-SDK.3A.3",
                "finding": "getBestBuy probe completion without successful attempts is treated as failed recovery, not success.",
            },
            {
                "id": "H-SDK.3C",
                "finding": "Non-router state_call quote candidates returned no accepted/exported quote method names.",
            },
            {
                "id": "H-SDK.3D",
                "finding": "Metadata-guided method hunt still found no callable quote/runtime method name in the bounded candidate set.",
            },
        ],
        "safeProductionPriceSources": [
            "confirmed hydration_route_registry manual_router rows",
            "manual XYK/live-reserve fallback for configured custom pools",
            "external/cached USD prices for portfolio and synthetic visual books",
            "synthetic orderbook fallback marked visual/price-only unless an execution preflight succeeds",
        ],
        "disabledUntil": [
            "sdk-next router getSpotPrice/getBestSell/getBestBuy returns bounded successful quotes through the selected RPC/provider",
            "or a state_call RuntimeApi method is accepted/exported and its exact SCALE input/output is confirmed against a known UI quote",
            "or a supported indexer/API quote source is selected and cached behind TTL/backoff guards",
        ],
        "recommendedEnv": {
            "keepOff": [
                "UTT_HYDRATION_ENABLE_ROUTER_QUOTES=0",
                "UTT_HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES=0",
                "UTT_HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK=0 unless running one visible diagnostic",
                "UTT_HYDRATION_ENABLE_SDK_ORDER_TICKET_QUOTES=0",
                "UTT_HYDRATION_ENABLE_SDK_SWAP_TX=0",
                "UTT_HYDRATION_ENABLE_BACKGROUND_SDK_PRICES=0",
                "UTT_HYDRATION_ENABLE_STATE_CALL_QUOTES=0",
                "UTT_HYDRATION_PRICE_CACHE_USE_SDK_FALLBACK=0",
            ],
            "safeOn": [
                "UTT_HYDRATION_ENABLE_MANUAL_ROUTER_FALLBACK=1",
                "UTT_HYDRATION_ENABLE_MANUAL_POOL_FALLBACK=1 for confirmed custom pools",
                "UTT_HYDRATION_MANUAL_POOL_LIVE_RESERVES=1 where pool accounts are configured",
                "UTT_HYDRATION_ENABLE_ORDERBOOK_SYNTHETIC_FALLBACK=1",
                "UTT_HYDRATION_ENABLE_EXTERNAL_USD_PRICES=1",
            ],
        },
        "currentGateState": {
            "recommendedDisabledGates": recommended_disabled,
            "recommendedSafeEnabledGates": safe_enabled,
            "unexpectedlyEnabledSdkRecoveryGates": unexpectedly_enabled,
            "safeMode": len(unexpectedly_enabled) == 0,
        },
        "nextAction": (
            "Stop Hydration SDK/runtime quote recovery here for now. Keep confirmed manual routes and cached/external price sources active, "
            "then move to the next roadmap item unless a new Hydration SDK/RPC/indexer source is selected."
        ),
        "safeDiagnosticEndpoints": [
            "/api/polkadot_dex/hydration/sdk_recovery_diagnostics",
            "/api/polkadot_dex/hydration/sdk_recovery_state_call_compare",
            "/api/polkadot_dex/hydration/sdk_recovery_metadata_method_hunt",
            "/api/polkadot_dex/hydration/sdk_recovery_closeout",
        ],
    }


@router.get("/hydration/sdk_recovery_closeout")
async def hydration_sdk_recovery_closeout() -> Dict[str, Any]:
    """Return the H-SDK.3 recovery closeout decision without running probes."""
    return _hydration_sdk_recovery_closeout_payload()


@router.get("/hydration/sdk_recovery_state_call_compare")
async def hydration_sdk_recovery_state_call_compare(
    symbol: str = Query("DOT-HDX", description="Symbol pair to diagnose, e.g. DOT-HDX or HDX-DOT."),
    amount: float = Query(1.0, gt=0, description="Human amount used to build state_call quote probe payloads."),
    side: str = Query("sell", description="sell, buy, or both."),
    methods_csv: Optional[str] = Query(None, description="Comma-separated runtime API quote method candidates. Defaults to common Hydration/Router names."),
    candidate: str = Query("auto", description="auto, u32_u32_u128, u32_u32_u128_order_0, u32_u32_u128_order_1, or raw."),
    raw_data: Optional[str] = Query(None, description="Optional raw hex SCALE input. If supplied, candidate=raw is used."),
    include_current_rpc: bool = Query(True),
    extra_rpc_urls_csv: Optional[str] = Query(None, description="Optional comma-separated extra HTTP RPC URLs to compare. Returned redacted."),
    max_rpc_candidates: int = Query(3, ge=1, le=5),
    max_methods: int = Query(6, ge=1, le=12),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Compare non-router state_call quote candidates across HTTP RPC endpoints.

    This is a read-only recovery diagnostic.  It deliberately avoids sdk-next
    router methods, WebSocket chainHead subscriptions, signing, submission, and
    any DB mutation.  It is meant to answer whether a lighter runtime state_call
    quote source is viable before reopening sdk-next quote polling.
    """
    base, quote = _parse_symbol(symbol)
    base_meta = _resolve_asset(base, db=db)
    quote_meta = _resolve_asset(quote, db=db)
    sides = _hydration_sdk_recovery_sides(side)
    methods = _hydration_sdk_recovery_state_call_methods(methods_csv, max_methods=int(max_methods))
    rpc_candidates = _hydration_sdk_recovery_rpc_candidates(
        include_current_rpc=bool(include_current_rpc),
        extra_rpc_urls_csv=extra_rpc_urls_csv,
        max_rpc_candidates=int(max_rpc_candidates),
    )
    if not rpc_candidates:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "hydration_recovery_no_rpc_candidates",
                "message": "No usable HTTP RPC candidates were available. Configure the Profile Dwellir key or pass extra_rpc_urls_csv.",
            },
        )

    rpc_results: List[Dict[str, Any]] = []
    total_attempts: List[Dict[str, Any]] = []
    for candidate_rpc in rpc_candidates:
        url = str(candidate_rpc.get("url") or "")
        sanity = {
            "system_chain": await _rpc_probe_url(url, "system_chain", []),
            "state_getRuntimeVersion": await _rpc_probe_url(url, "state_getRuntimeVersion", []),
            "Core_version": await _state_call_probe_method_at_url(url, "Core_version", "0x"),
            "Metadata_metadata_versions": await _state_call_probe_method_at_url(url, "Metadata_metadata_versions", "0x"),
        }
        attempts = await _hydration_sdk_recovery_state_call_attempts_for_rpc(
            rpc_url=url,
            symbol=f"{base}-{quote}",
            amount=float(amount),
            sides=sides,
            methods=methods,
            candidate=candidate,
            raw_data=raw_data,
            base_meta=base_meta,
            quote_meta=quote_meta,
        )
        summary = _hydration_sdk_recovery_state_call_summary(attempts)
        total_attempts.extend([{**a, "rpcLabel": candidate_rpc.get("label"), "rpcUrl": candidate_rpc.get("redactedUrl")} for a in attempts])
        rpc_results.append({
            "label": candidate_rpc.get("label"),
            "source": candidate_rpc.get("source"),
            "rpc_url": candidate_rpc.get("redactedUrl"),
            "sanity": sanity,
            "summary": summary,
            "attempts": attempts,
        })

    total_summary = _hydration_sdk_recovery_state_call_summary(total_attempts)
    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "network": "hydration",
        "diagnosticOnly": True,
        "mutation": False,
        "signing": False,
        "submission": False,
        "usesSdkNextRouter": False,
        "usesWebSocketChainHead": False,
        "rawSymbol": symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "side": side,
        "amountUi": float(amount),
        "base": base_meta,
        "quote": quote_meta,
        "methods": methods,
        "candidate": "raw" if raw_data else candidate,
        "rpcCandidateCount": len(rpc_candidates),
        "summary": total_summary,
        "rpcResults": rpc_results,
        "safety": {
            "readOnly": True,
            "doesNotUseSdkNextRouter": True,
            "doesNotUseWsChainHead": True,
            "doesNotSign": True,
            "doesNotSubmit": True,
            "doesNotMutateRouteRegistry": True,
            "doesNotMutateLedgerOrFifo": True,
        },
        "recommendation": (
            "Use this only to identify a possible non-router runtime state_call quote source. "
            "Do not enable state_call quotes or trading from accepted probes until the SCALE input/output shape is confirmed against a known Hydration UI quote."
        ),
    }


@router.get("/hydration/sdk_path_diagnostics")
async def hydration_sdk_path_diagnostics(
    symbol: str = Query("DOT-HDX", description="Symbol pair to diagnose, e.g. DOT-HDX"),
    amount_ui: float = Query(1.0, gt=0),
    step_timeout_s: Optional[float] = Query(None, gt=0),
    route_mode: Optional[str] = Query("sdk_spot", description="auto|sdk|sdk_spot|isolated_helper"),
    force_isolated: Optional[bool] = Query(None, description="Override helper isolation for this diagnostic request."),
    include_spot_direct: bool = Query(True),
    include_spot_context: bool = Query(True),
    include_quote_sell: bool = Query(True),
    include_swap_tx: bool = Query(False),
    user_pubkey: Optional[str] = Query(None, description="Required only when include_swap_tx=true."),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Run scoped Hydration SDK diagnostics without reopening background pricing.

    This endpoint intentionally separates getSpotPrice from getBestSell and
    swap_tx so we can identify which sdk-next path is actually healthy.
    """
    base, quote = _parse_symbol(symbol)
    base_meta = _resolve_asset(base, db=db)
    quote_meta = _resolve_asset(quote, db=db)
    route_mode_norm = _hydration_route_mode(route_mode)
    timeout_s = float(step_timeout_s if step_timeout_s is not None else _HYDRATION_ORDERBOOK_STEP_TIMEOUT_S)
    if force_isolated is None:
        force_isolated_effective = bool(route_mode_norm == "isolated_helper" or _HYDRATION_SDK_SPOT_ORDERBOOK_FORCE_ISOLATED_HELPER)
    else:
        force_isolated_effective = bool(force_isolated)

    async def _attempt(name: str, fn) -> Dict[str, Any]:
        t0 = time.monotonic()
        try:
            result = await fn()
            return {
                "ok": True,
                "name": name,
                "elapsed_s": round(time.monotonic() - t0, 4),
                "result": result,
            }
        except HTTPException as e:
            return {
                "ok": False,
                "name": name,
                "elapsed_s": round(time.monotonic() - t0, 4),
                "status_code": e.status_code,
                "detail": e.detail,
            }
        except Exception as e:
            return {
                "ok": False,
                "name": name,
                "elapsed_s": round(time.monotonic() - t0, 4),
                "error": type(e).__name__,
                "message": str(e),
            }

    paths: Dict[str, Any] = {}

    if include_spot_direct:
        paths["spot_direct_forward"] = await _attempt(
            "spot_direct_forward",
            lambda: _hydration_sdk_spot_for_orderbook(
                raw_symbol=symbol,
                base=base,
                quote=quote,
                asset_in=base_meta,
                asset_out=quote_meta,
                step_timeout_s=timeout_s,
                route_mode="sdk_spot",
                implementation="direct",
            ),
        )
        paths["spot_direct_reverse"] = await _attempt(
            "spot_direct_reverse",
            lambda: _hydration_sdk_spot_for_orderbook(
                raw_symbol=f"{quote}-{base}",
                base=quote,
                quote=base,
                asset_in=quote_meta,
                asset_out=base_meta,
                step_timeout_s=timeout_s,
                route_mode="sdk_spot",
                implementation="direct",
            ),
        )

    if include_spot_context:
        paths["spot_context_forward"] = await _attempt(
            "spot_context_forward",
            lambda: _hydration_sdk_spot_for_orderbook(
                raw_symbol=symbol,
                base=base,
                quote=quote,
                asset_in=base_meta,
                asset_out=quote_meta,
                step_timeout_s=timeout_s,
                route_mode="sdk_spot",
                implementation="context",
            ),
        )
        paths["spot_context_reverse"] = await _attempt(
            "spot_context_reverse",
            lambda: _hydration_sdk_spot_for_orderbook(
                raw_symbol=f"{quote}-{base}",
                base=quote,
                quote=base,
                asset_in=quote_meta,
                asset_out=base_meta,
                step_timeout_s=timeout_s,
                route_mode="sdk_spot",
                implementation="context",
            ),
        )

    if include_quote_sell:
        paths["quote_sell_forward"] = await _attempt(
            "quote_sell_forward",
            lambda: _hydration_quote_sell(
                raw_symbol=symbol,
                base=base,
                quote=quote,
                asset_in=base_meta,
                asset_out=quote_meta,
                amount_in_ui=float(amount_ui),
                step_timeout_s=timeout_s,
                force_isolated=force_isolated_effective,
                sdk_use_case="orderbook",
                route_mode="sdk",
            ),
        )
        paths["quote_sell_reverse"] = await _attempt(
            "quote_sell_reverse",
            lambda: _hydration_quote_sell(
                raw_symbol=f"{quote}-{base}",
                base=quote,
                quote=base,
                asset_in=quote_meta,
                asset_out=base_meta,
                amount_in_ui=float(amount_ui),
                step_timeout_s=timeout_s,
                force_isolated=force_isolated_effective,
                sdk_use_case="orderbook",
                route_mode="sdk",
            ),
        )

    if include_swap_tx:
        if not str(user_pubkey or "").strip():
            paths["swap_tx"] = {
                "ok": False,
                "skipped": True,
                "reason": "user_pubkey_required",
                "message": "Pass user_pubkey only for deliberate OrderTicket SDK swap build diagnostics.",
            }
        else:
            paths["swap_tx_exact_in_sell"] = await _attempt(
                "swap_tx_exact_in_sell",
                lambda: _hydration_swap_tx_build(
                    raw_symbol=symbol,
                    base=base,
                    quote=quote,
                    side="sell",
                    asset_in=base_meta,
                    asset_out=quote_meta,
                    amount_ui=float(amount_ui),
                    amount_mode="exact_in",
                    slippage_bps=50,
                    user_pubkey=str(user_pubkey or "").strip(),
                    manual_custom_swap=None,
                    route_mode="sdk",
                ),
            )

    return {
        "ok": True,
        "venue": "polkadot_hydration",
        "network": "hydration",
        "rawSymbol": symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base_meta,
        "quote": quote_meta,
        "amountUi": float(amount_ui),
        "stepTimeoutS": timeout_s,
        "routeMode": route_mode_norm,
        "forceIsolated": force_isolated_effective,
        "sdkScopes": {
            "globalRouterQuotesEnabled": bool(_HYDRATION_ENABLE_ROUTER_QUOTES),
            "sdkOrderbookQuotesEnabled": bool(_HYDRATION_ENABLE_SDK_ORDERBOOK_QUOTES),
            "sdkSpotOrderbookEnabled": bool(_HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK),
            "sdkOrderTicketQuotesEnabled": bool(_HYDRATION_ENABLE_SDK_ORDER_TICKET_QUOTES),
            "sdkSwapTxEnabled": bool(_HYDRATION_ENABLE_SDK_SWAP_TX),
            "backgroundSdkPricesEnabled": bool(_HYDRATION_ENABLE_BACKGROUND_SDK_PRICES),
        },
        "spotOrderbookPolicy": {
            "implementation": _HYDRATION_SDK_SPOT_ORDERBOOK_IMPLEMENTATION,
            "forceIsolatedHelper": bool(_HYDRATION_SDK_SPOT_ORDERBOOK_FORCE_ISOLATED_HELPER),
            "tradable": bool(_HYDRATION_SDK_SPOT_ORDERBOOK_TRADABLE),
            "minDailyVolumeUsd": float(_HYDRATION_SDK_SPOT_ORDERBOOK_MIN_DAILY_VOLUME_USD),
            "poolTypes": _HYDRATION_SDK_SPOT_ORDERBOOK_POOL_TYPES_CSV,
            "note": "Daily-volume gating is surfaced here but not enforced until a trusted Hydration pool volume source is wired.",
        },
        "paths": paths,
    }


@router.get("/hydration/orderbook")
async def hydration_pseudo_orderbook(
    symbol: str = Query(..., description="Symbol pair, e.g. UTTT-DOT (BASE-QUOTE)"),
    depth: int = Query(10, ge=1, le=50),
    route_mode: Optional[str] = Query(None, description="Hydration quote source: auto|sdk|sdk_spot|isolated_helper|manual_xyk|manual_router. sdk_spot uses getSpotPrice for visible-pair pseudo-orderbook diagnostics."),
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

    if route_mode_norm == "sdk_spot":
        orderbook_step_timeout_s = max(1.0, float(_HYDRATION_ORDERBOOK_STEP_TIMEOUT_S))
        n0 = max(1, min(int(depth), 10))
        orderbook_config = _hydration_orderbook_common_config(
            orderbook_step_timeout_s=orderbook_step_timeout_s,
            max_consecutive_errors=max(1, int(_HYDRATION_ORDERBOOK_MAX_CONSECUTIVE_ERRORS)),
            force_isolated_orderbook=_hydration_sdk_spot_orderbook_force_isolated(route_mode_norm),
            route_mode_norm=route_mode_norm,
            requested_depth=int(depth),
            sample_depth=n0,
            extra={"sdkSpotOrderbookRequested": True},
        )
        sample_errors: List[Dict[str, Any]] = []
        spot_resp: Optional[Dict[str, Any]] = None
        try:
            spot_resp = await _hydration_sdk_spot_orderbook_response(
                symbol=symbol,
                base=base,
                quote=quote,
                base_meta=base_meta,
                quote_meta=quote_meta,
                depth=depth,
                route_mode_norm=route_mode_norm,
                orderbook_config=orderbook_config,
                sample_errors=[],
                fallback_reason="explicit_sdk_spot_route_mode",
            )
            if isinstance(spot_resp, dict) and spot_resp.get("ok"):
                return spot_resp
            sample_errors.append({
                "side": "both",
                "stage": "sdk_spot_orderbook",
                "detail": {
                    "error": "hydration_sdk_spot_orderbook_empty",
                    "message": "route_mode=sdk_spot returned no usable spot orderbook response.",
                    "spotOrderbook": spot_resp,
                },
            })
        except HTTPException as e:
            sample_errors.append({"side": "both", "stage": "sdk_spot_orderbook", "detail": e.detail})
        except Exception as e:
            sample_errors.append({
                "side": "both",
                "stage": "sdk_spot_orderbook",
                "error": type(e).__name__,
                "message": str(e),
            })

        fallback = await _hydration_synthetic_spot_orderbook_response(
            symbol=symbol,
            base=base,
            quote=quote,
            base_meta=base_meta,
            quote_meta=quote_meta,
            depth=depth,
            db=db,
            route_mode_norm=route_mode_norm,
            orderbook_config={
                **orderbook_config,
                "sdkSpotOrderbookFailed": True,
                "sdkSpotOrderbookFallback": "synthetic_spot_fallback",
            },
            sample_errors=sample_errors,
            fallback_reason="explicit_sdk_spot_failed_synthetic_fallback",
        )
        if isinstance(fallback, dict) and fallback.get("ok"):
            fallback["routeMode"] = route_mode_norm
            fallback.setdefault("orderbookConfig", {})["requestedRouteMode"] = route_mode_norm
            fallback.setdefault("orderbookConfig", {})["sdkSpotOrderbookFailed"] = True
            fallback.setdefault("orderbookConfig", {})["sdkSpotOrderbookFallback"] = "synthetic_spot_fallback"
            return fallback

        raise HTTPException(
            status_code=503,
            detail={
                "error": "hydration_sdk_spot_orderbook_unavailable",
                "message": "route_mode=sdk_spot was requested, but SDK getSpotPrice failed and no synthetic fallback could be built.",
                "venue": "polkadot_hydration",
                "rawSymbol": symbol,
                "resolvedSymbol": f"{base}-{quote}",
                "base": base_meta,
                "quote": quote_meta,
                "routeMode": route_mode_norm,
                "spotOrderbook": spot_resp,
                "sampleErrors": sample_errors,
                "syntheticFallback": fallback,
            },
        )

    orderbook_sdk_payload = {
        "mode": "quote_sell",
        "sdkUseCase": "orderbook",
        "routeMode": route_mode_norm,
        "rawSymbol": symbol,
        "resolvedSymbol": f"{base}-{quote}",
    }
    if not _hydration_router_quotes_enabled_for_payload(orderbook_sdk_payload):
        disabled_detail = _hydration_router_quotes_disabled_detail(
            mode="quote_sell",
            payload=orderbook_sdk_payload,
            symbol=f"{base}-{quote}",
        )
        disabled_detail.update({
            "message": "Hydration SDK OrderBook quote sampling is disabled for this route scope.",
            "rawSymbol": symbol,
            "resolvedSymbol": f"{base}-{quote}",
            "base": base_meta,
            "quote": quote_meta,
            "routeMode": route_mode_norm,
        })
        if route_mode_norm == "auto":
            n0 = max(1, min(int(depth), 10))
            if _HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK:
                spot_fallback = await _hydration_sdk_spot_orderbook_response(
                    symbol=symbol,
                    base=base,
                    quote=quote,
                    base_meta=base_meta,
                    quote_meta=quote_meta,
                    depth=depth,
                    route_mode_norm="sdk_spot",
                    orderbook_config=_hydration_orderbook_common_config(
                        orderbook_step_timeout_s=max(1.0, float(_HYDRATION_ORDERBOOK_STEP_TIMEOUT_S)),
                        max_consecutive_errors=max(1, int(_HYDRATION_ORDERBOOK_MAX_CONSECUTIVE_ERRORS)),
                        force_isolated_orderbook=_hydration_sdk_spot_orderbook_force_isolated("sdk_spot"),
                        route_mode_norm="sdk_spot",
                        requested_depth=int(depth),
                        sample_depth=n0,
                        extra={"sdkOrderbookQuotesDisabled": True, "sdkSpotFallbackFromAuto": True},
                    ),
                    sample_errors=[{"side": "both", "detail": disabled_detail}],
                    fallback_reason="sdk_orderbook_quotes_disabled_spot_fallback",
                )
                if isinstance(spot_fallback, dict) and spot_fallback.get("ok"):
                    spot_fallback["routeMode"] = route_mode_norm
                    spot_fallback.setdefault("orderbookConfig", {})["requestedRouteMode"] = route_mode_norm
                    return spot_fallback
            fallback = await _hydration_synthetic_spot_orderbook_response(
                symbol=symbol,
                base=base,
                quote=quote,
                base_meta=base_meta,
                quote_meta=quote_meta,
                depth=depth,
                db=db,
                route_mode_norm=route_mode_norm,
                orderbook_config=_hydration_orderbook_common_config(
                    orderbook_step_timeout_s=max(1.0, float(_HYDRATION_ORDERBOOK_STEP_TIMEOUT_S)),
                    max_consecutive_errors=max(1, int(_HYDRATION_ORDERBOOK_MAX_CONSECUTIVE_ERRORS)),
                    force_isolated_orderbook=bool(_HYDRATION_ORDERBOOK_FORCE_ISOLATED_HELPER),
                    route_mode_norm=route_mode_norm,
                    requested_depth=int(depth),
                    sample_depth=n0,
                    extra={"sdkOrderbookQuotesDisabled": True},
                ),
                sample_errors=[{"side": "both", "detail": disabled_detail}],
                fallback_reason="sdk_orderbook_quotes_disabled",
            )
            if isinstance(fallback, dict) and fallback.get("ok"):
                return fallback
            disabled_detail["syntheticFallback"] = fallback
        raise HTTPException(status_code=503, detail=disabled_detail)

    n = max(1, min(int(depth), 10))
    bids: List[Dict[str, Any]] = []
    asks: List[Dict[str, Any]] = []
    sample_errors: List[Dict[str, Any]] = []
    orderbook_step_timeout_s = max(1.0, float(_HYDRATION_ORDERBOOK_STEP_TIMEOUT_S))
    max_consecutive_errors = max(1, int(_HYDRATION_ORDERBOOK_MAX_CONSECUTIVE_ERRORS))
    force_isolated_orderbook = bool(route_mode_norm == "isolated_helper" or _HYDRATION_ORDERBOOK_FORCE_ISOLATED_HELPER)

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
                sdk_use_case="orderbook",
                route_mode=route_mode_norm,
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
                sdk_use_case="orderbook",
                route_mode=route_mode_norm,
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
        orderbook_config = _hydration_orderbook_common_config(
            orderbook_step_timeout_s=orderbook_step_timeout_s,
            max_consecutive_errors=max_consecutive_errors,
            force_isolated_orderbook=force_isolated_orderbook,
            route_mode_norm=route_mode_norm,
            requested_depth=int(depth),
            sample_depth=n,
        )
        if _HYDRATION_ENABLE_SDK_SPOT_ORDERBOOK:
            try:
                spot_fallback = await _hydration_sdk_spot_orderbook_response(
                    symbol=symbol,
                    base=base,
                    quote=quote,
                    base_meta=base_meta,
                    quote_meta=quote_meta,
                    depth=depth,
                    route_mode_norm="sdk_spot",
                    orderbook_config={
                        **orderbook_config,
                        "requestedRouteMode": route_mode_norm,
                        "sdkSpotFallbackFromQuoteSamplingFailure": True,
                    },
                    sample_errors=sample_errors,
                    fallback_reason="sdk_quote_sampling_failed_spot_fallback",
                )
                if isinstance(spot_fallback, dict) and spot_fallback.get("ok"):
                    spot_fallback["routeMode"] = route_mode_norm
                    spot_fallback.setdefault("orderbookConfig", {})["requestedRouteMode"] = route_mode_norm
                    return spot_fallback
            except HTTPException as e:
                if len(sample_errors) < 12:
                    sample_errors.append({"side": "both", "stage": "sdk_spot_fallback", "detail": e.detail})
            except Exception as e:
                if len(sample_errors) < 12:
                    sample_errors.append({"side": "both", "stage": "sdk_spot_fallback", "error": type(e).__name__, "message": str(e)})

        fallback = await _hydration_synthetic_spot_orderbook_response(
            symbol=symbol,
            base=base,
            quote=quote,
            base_meta=base_meta,
            quote_meta=quote_meta,
            depth=depth,
            db=db,
            route_mode_norm=route_mode_norm,
            orderbook_config=orderbook_config,
            sample_errors=sample_errors,
            fallback_reason="sdk_quote_sampling_failed",
        )
        if isinstance(fallback, dict) and fallback.get("ok"):
            return fallback
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
                "orderbookConfig": orderbook_config,
                "sampleErrors": sample_errors,
                "syntheticFallback": fallback,
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


def _manual_route_probe_pool_candidates(raw: Optional[List[str]]) -> List[str]:
    vals: List[str] = []
    for item in (raw or ["Omnipool", "XYK"]):
        s = str(item or "").strip()
        if s and s not in vals:
            vals.append(s)
    return vals or ["Omnipool", "XYK"]


def _manual_route_probe_candidates(
    *,
    asset_in_id: int,
    asset_out_id: int,
    req: HydrationManualRouteProbeRequest,
    base: Optional[str] = None,
    quote: Optional[str] = None,
    db: Optional[Session] = None,
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    if isinstance(req.route_candidates, list) and req.route_candidates:
        for idx, route in enumerate(req.route_candidates):
            if not isinstance(route, list) or not route:
                continue
            clean_route = _normalize_manual_router_route(
                route,
                asset_in_id=int(asset_in_id),
                asset_out_id=int(asset_out_id),
            )
            if clean_route:
                candidates.append({"name": f"request_candidate_{idx + 1}", "source": "request", "route": clean_route})

    if not candidates and base and quote:
        configured = _manual_router_fallback_configured_route(
            base=base,
            quote=quote,
            asset_in_id=int(asset_in_id),
            asset_out_id=int(asset_out_id),
            db=db,
        )
        if configured:
            candidates.append({
                "name": "configured_route_json",
                "source": "db_or_env_manual_router_route",
                "route": configured,
            })

    if not candidates:
        for pool in _manual_route_probe_pool_candidates(req.pool_candidates):
            candidates.append({
                "name": f"single_leg_{str(pool).strip().lower()}",
                "source": "generated",
                "route": [{"pool": _manual_router_pool_payload(pool), "assetIn": int(asset_in_id), "assetOut": int(asset_out_id)}],
            })
    return candidates


def _manual_route_probe_min_out_atomic(
    *,
    req: HydrationManualRouteProbeRequest,
    asset_out: Dict[str, Any],
) -> str:
    raw = str(req.min_amount_out_atomic or "").replace(",", "").strip()
    if raw:
        if not raw.isdigit() or int(raw) <= 0:
            raise HTTPException(status_code=422, detail={"error": "invalid_min_amount_out_atomic", "value": req.min_amount_out_atomic})
        return raw
    if req.min_amount_out_ui is not None:
        return str(_ui_to_atomic(float(req.min_amount_out_ui), int(asset_out.get("decimals") or 0)))
    # Probe default: 1 atomic output unit.  This proves PAPI call-data encoding
    # only.  It is intentionally not an execution-safe slippage minimum.
    return "1"


@router.post("/hydration/manual_route_probe")
async def hydration_manual_route_probe(
    req: HydrationManualRouteProbeRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Try manual Hydration Router call-data candidates without mutation.

    This endpoint is intentionally diagnostic-only.  It does not write route
    registry rows, sign, submit, or validate economic execution.  It only checks
    whether the existing manual PAPI Router call builder can encode unsigned
    Router.sell call data for candidate route legs such as DOT->HDX Omnipool.
    """
    base, quote = _parse_symbol(req.symbol)
    side = str(req.side or "sell").strip().lower()
    amount_mode = str(req.amount_mode or "exact_in").strip().lower()
    if side != "sell" or amount_mode != "exact_in":
        raise HTTPException(
            status_code=422,
            detail={
                "error": "hydration_manual_route_probe_sell_exact_in_only",
                "message": "Manual route probing currently supports only side=sell and amount_mode=exact_in. BUY/exact_out remains gated separately.",
                "side": side,
                "amountMode": amount_mode,
            },
        )

    base_meta = _resolve_asset(base, db=db)
    quote_meta = _resolve_asset(quote, db=db)
    asset_in = base_meta
    asset_out = quote_meta
    asset_in_id = _hydration_sdk_asset_id(asset_in)
    asset_out_id = _hydration_sdk_asset_id(asset_out)
    amount_in_atomic = _ui_to_atomic(float(req.amount), int(asset_in.get("decimals") or 0))
    min_amount_out_atomic = _manual_route_probe_min_out_atomic(req=req, asset_out=asset_out)
    candidates = _manual_route_probe_candidates(
        asset_in_id=asset_in_id,
        asset_out_id=asset_out_id,
        req=req,
        base=base,
        quote=quote,
        db=db,
    )

    results: List[Dict[str, Any]] = []
    first_success: Optional[Dict[str, Any]] = None
    for candidate in candidates:
        route = candidate.get("route") or []
        manual_custom_swap = {
            "enabled": True,
            "method": "sell",
            "reason": "manual route probe only - unsigned call-data encoding test; route execution is not confirmed",
            "amountMode": "exact_in",
            "assetInId": int(asset_in_id),
            "assetOutId": int(asset_out_id),
            "amountInAtomic": str(amount_in_atomic),
            "minAmountOutAtomic": str(min_amount_out_atomic),
            "estimatedAmountOutAtomic": None,
            "estimatedAmountOutUi": None,
            "route": route,
            "slippageBps": int(req.slippage_bps),
            "probeOnly": True,
        }
        try:
            built = await _hydration_swap_tx_build(
                raw_symbol=req.symbol,
                base=base,
                quote=quote,
                side="sell",
                asset_in=asset_in,
                asset_out=asset_out,
                amount_ui=float(req.amount),
                amount_mode="exact_in",
                slippage_bps=int(req.slippage_bps),
                user_pubkey=req.user_pubkey,
                manual_custom_swap=manual_custom_swap,
                route_mode="manual_xyk",
            )
            item = {
                "ok": True,
                "name": candidate.get("name"),
                "source": candidate.get("source"),
                "route": route,
                "provider": built.get("provider") if isinstance(built, dict) else None,
                "builderVariant": built.get("builderVariant") if isinstance(built, dict) else None,
                "encodedCallData": built.get("encodedCallData") if isinstance(built, dict) else None,
                "transactionData": built.get("transactionData") if isinstance(built, dict) else None,
                "tx": built,
                "executionConfirmed": False,
                "signed": False,
                "submitted": False,
            }
            results.append(item)
            if first_success is None:
                first_success = item
        except HTTPException as e:
            results.append({
                "ok": False,
                "name": candidate.get("name"),
                "source": candidate.get("source"),
                "route": route,
                "status": e.status_code,
                "detail": e.detail,
            })
        except Exception as e:
            results.append({
                "ok": False,
                "name": candidate.get("name"),
                "source": candidate.get("source"),
                "route": route,
                "error": type(e).__name__,
                "message": str(e),
            })

    successes = [r for r in results if r.get("ok")]
    return {
        "ok": True,
        "mode": "manual_route_probe",
        "willMutate": False,
        "mutationScope": "none_read_only",
        "venue": "polkadot_hydration",
        "rawSymbol": req.symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "side": side,
        "amount": float(req.amount),
        "amountMode": amount_mode,
        "assetIn": asset_in,
        "assetOut": asset_out,
        "assetInId": int(asset_in_id),
        "assetOutId": int(asset_out_id),
        "amountInAtomic": str(amount_in_atomic),
        "minAmountOutAtomic": str(min_amount_out_atomic),
        "candidateCount": len(results),
        "successfulCandidateCount": len(successes),
        "routeBuildAvailable": bool(successes),
        "firstSuccess": first_success,
        "candidates": results,
        "warnings": [
            "This endpoint only tests unsigned manual Router call-data encoding; it does not prove the route will execute on-chain.",
            "Default minAmountOutAtomic=1 is unsafe for real trading. Use this result only to identify buildable route shapes before adding a confirmed route/quote path.",
            "No route registry row is created and no bridge/swap/ledger state is mutated.",
        ],
        "nextRequired": (
            "If a candidate builds encoded call data, test a tiny controlled swap only after adding a real quote/min-output source or explicit manual route confirmation."
            if successes
            else "No manual route candidate encoded. Inspect candidate errors/router attempts and adjust route pool/type shape."
        ),
    }


@router.post("/hydration/swap_tx")
async def hydration_swap_tx(req: HydrationSwapTxRequest, db: Session = Depends(get_db)) -> Dict[str, Any]:
    base, quote = _parse_symbol(req.symbol)
    side = (req.side or "").strip().lower()
    if side not in ("buy", "sell"):
        raise HTTPException(status_code=422, detail=f"Invalid side '{req.side}' (expected buy|sell)")

    base_meta = _resolve_asset(base, db=db)
    quote_meta = _resolve_asset(quote, db=db)
    quote_status = _hydration_router_quote_status(symbol=f"{base}-{quote}", base_meta=base_meta, quote_meta=quote_meta, use_case="order_ticket")
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

    # BUY is no longer blocked here unconditionally.  Exact-out SDK BUY remains
    # gated below, but DB-confirmed manual_router routes can build Router.buy
    # without enabling the broad sdk-next getBestBuy/router quote path.

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

    manual_router_fallback_diag: Optional[Dict[str, Any]] = None
    if route_mode_norm in {"auto", "manual_router"}:
        manual_router_fallback_diag = await _hydration_manual_router_fallback_diagnostics(
            db=db,
            base=base,
            quote=quote,
            side=manual_plan_side,
            amount_ui=build_amount_ui,
            amount_mode=build_amount_mode,
            slippage_bps=int(req.slippage_bps),
            base_meta=base_meta,
            quote_meta=quote_meta,
        )

    if not manual_custom_swap and route_mode_norm in {"auto", "manual_router"}:
        try:
            manual_custom_swap = await _hydration_manual_router_fallback_plan(
                db=db,
                base=base,
                quote=quote,
                side=manual_plan_side,
                amount_ui=build_amount_ui,
                amount_mode=build_amount_mode,
                slippage_bps=int(req.slippage_bps),
                base_meta=base_meta,
                quote_meta=quote_meta,
            )
            if isinstance(manual_router_fallback_diag, dict):
                manual_router_fallback_diag["attempted"] = True
                manual_router_fallback_diag["attached"] = bool(isinstance(manual_custom_swap, dict) and manual_custom_swap.get("enabled"))
                manual_router_fallback_diag["planReason"] = (
                    "manual_router_fallback_attached"
                    if manual_router_fallback_diag.get("attached")
                    else "manual_router_fallback_plan_returned_none"
                )
        except HTTPException as e:
            if isinstance(manual_router_fallback_diag, dict):
                manual_router_fallback_diag["attempted"] = True
                manual_router_fallback_diag["attached"] = False
                manual_router_fallback_diag["planError"] = e.detail
            # Preserve precise safety errors instead of falling through to the
            # generic SDK-router-quotes-disabled error.
            detail_error = None
            try:
                detail_error = (e.detail or {}).get("error") if isinstance(e.detail, dict) else None
            except Exception:
                detail_error = None
            if detail_error in {
                "hydration_manual_router_fallback_unconfirmed",
                "hydration_manual_router_fallback_input_too_large",
                "hydration_manual_router_fallback_min_out_too_small",
                "hydration_manual_router_fallback_max_in_too_small",
            }:
                raise
        except Exception as e:
            if isinstance(manual_router_fallback_diag, dict):
                manual_router_fallback_diag["attempted"] = True
                manual_router_fallback_diag["attached"] = False
                manual_router_fallback_diag["planError"] = {
                    "error": type(e).__name__,
                    "message": str(e),
                }

    _hydration_mark_manual_router_diag_attached(manual_router_fallback_diag, manual_custom_swap)
    quote_status = _hydration_quote_status_with_manual_custom_swap(
        quote_status,
        manual_custom_swap=manual_custom_swap,
        side=side,
        amount_mode=amount_mode,
    )

    if side == "buy" and not _HYDRATION_ENABLE_EXACT_BUY and not manual_custom_swap:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "hydration_buy_swap_disabled",
                "message": "Hydration BUY swaps are disabled for SDK/router-quote paths, but confirmed manual_router BUY routes may still build without enabling SDK quotes. No confirmed manual_router BUY route attached for this request.",
                "venue": "polkadot_hydration",
                "rawSymbol": req.symbol,
                "resolvedSymbol": f"{base}-{quote}",
                "side": side,
                "amount": req.amount,
                "amountMode": amount_mode,
                "quoteSpendEstimate": req.quote_spend_estimate,
                "base": base_meta,
                "quote": quote_meta,
                "quoteStatus": quote_status,
                "routeMode": route_mode_norm,
                "manualRouterFallback": manual_router_fallback_diag,
                "nextRequired": "Save/confirm the reverse spend route in Hydration Route Registry, or set UTT_HYDRATION_ENABLE_EXACT_BUY=1 only for controlled SDK exact-buy diagnostics.",
            },
        )

    ticket_sdk_payload = {
        "mode": "swap_tx",
        "sdkUseCase": "order_ticket",
        "routeMode": route_mode_norm,
        "rawSymbol": req.symbol,
        "resolvedSymbol": f"{base}-{quote}",
    }
    if not manual_custom_swap and not _hydration_router_quotes_enabled_for_payload(ticket_sdk_payload):
        detail = _hydration_router_quotes_disabled_detail(
            mode="swap_tx",
            payload=ticket_sdk_payload,
            symbol=f"{base}-{quote}",
        )
        detail.update({
            "error": "hydration_swap_tx_requires_scoped_sdk_quotes",
            "message": "Hydration SDK swap transaction building is disabled for the OrderTicket scope unless a manual custom-asset fallback is available for this pair.",
            "venue": "polkadot_hydration",
            "rawSymbol": req.symbol,
            "resolvedSymbol": f"{base}-{quote}",
            "side": side,
            "amount": req.amount,
            "base": base_meta,
            "quote": quote_meta,
            "quoteStatus": quote_status,
            "routeMode": route_mode_norm,
            "manualRouterFallback": manual_router_fallback_diag,
        })
        raise HTTPException(status_code=503, detail=detail)

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
        "routeModeEffective": (
            "manual_router"
            if isinstance(manual_custom_swap, dict) and manual_custom_swap.get("manualRouterFallback")
            else ("manual_xyk" if manual_custom_swap else ("isolated_helper" if route_mode_norm == "isolated_helper" else "sdk"))
        ),
        "manualRouterFallback": bool(isinstance(manual_custom_swap, dict) and manual_custom_swap.get("manualRouterFallback")),
        "executionConfirmed": bool(
            isinstance(manual_custom_swap, dict)
            and (
                bool(manual_custom_swap.get("executionConfirmed"))
                or not manual_custom_swap.get("manualRouterFallback")
            )
        ),
        "manualRouterFallbackDiagnostics": manual_router_fallback_diag,
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
            "nextRequired": (
                "Front-end SubWallet signing/submission is the next step for execution-confirmed routes."
                if not (isinstance(manual_custom_swap, dict) and manual_custom_swap.get("manualRouterFallback") and not manual_custom_swap.get("executionConfirmed"))
                else "Manual Router fallback is buildable but not execution-confirmed. Do not sign/submit until the route is confirmed."
            ),
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
