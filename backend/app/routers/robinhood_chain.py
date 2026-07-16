from __future__ import annotations

import asyncio
import copy
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..config import settings


router = APIRouter(prefix="/api/robinhood_chain", tags=["robinhood_chain"])

_EXPECTED_CHAIN_ID_DECIMAL = 4663
_EXPECTED_CHAIN_ID_HEX = hex(_EXPECTED_CHAIN_ID_DECIMAL)
_NATIVE_CURRENCY = "ETH"
_EXPLORER_URL = "https://robinhoodchain.blockscout.com"

# Callers select stable check names, never arbitrary JSON-RPC methods or params.
_PROBE_DEFINITIONS: Dict[str, Tuple[str, List[Any]]] = {
    "chain_id": ("eth_chainId", []),
    "net_version": ("net_version", []),
    "client_version": ("web3_clientVersion", []),
    "block_number": ("eth_blockNumber", []),
    "latest_block": ("eth_getBlockByNumber", ["latest", False]),
    "gas_price": ("eth_gasPrice", []),
    "max_priority_fee_per_gas": ("eth_maxPriorityFeePerGas", []),
    "fee_history": ("eth_feeHistory", ["0x5", "latest", [25, 50, 75]]),
}
_DEFAULT_PROBE_CHECKS = list(_PROBE_DEFINITIONS.keys())


class RobinhoodChainProbeRequest(BaseModel):
    checks: Optional[List[str]] = Field(
        default=None,
        description="Optional fixed check names; arbitrary RPC methods are not accepted.",
    )
    force_refresh: bool = False


_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_CACHE_LOCK = asyncio.Lock()
_RPC_SEMAPHORE = asyncio.Semaphore(max(1, int(settings.robinhood_chain_max_concurrent)))

_LAST_GOOD_AT: Optional[datetime] = None
_LAST_ERROR: Optional[str] = None
_LAST_OBSERVED_CHAIN_ID: Optional[str] = None
_BACKOFF_UNTIL_MONOTONIC = 0.0
_BACKOFF_UNTIL_UTC: Optional[datetime] = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_or_none(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def _configured_rpc_http() -> str:
    return settings.robinhood_chain_effective_rpc_http()


def _status_payload() -> Dict[str, Any]:
    configured_chain_id = int(settings.robinhood_chain_chain_id)
    configured_match = configured_chain_id == _EXPECTED_CHAIN_ID_DECIMAL
    observed = str(_LAST_OBSERVED_CHAIN_ID or "").strip().lower() or None
    observed_match = observed == _EXPECTED_CHAIN_ID_HEX if observed is not None else None

    return {
        "venue": "robinhood_chain",
        "network": "mainnet",
        "native_currency": _NATIVE_CURRENCY,
        "explorer_url": _EXPLORER_URL,
        "read_only": True,
        "enabled": bool(settings.robinhood_chain_enabled),
        "configured": bool(_configured_rpc_http()),
        "effective_enabled": bool(settings.robinhood_chain_effective_enabled()),
        "chain_id": configured_chain_id,
        "expected_chain_id": _EXPECTED_CHAIN_ID_DECIMAL,
        "expected_chain_id_hex": _EXPECTED_CHAIN_ID_HEX,
        "configured_chain_id_matches": configured_match,
        "rpc_chain_id": observed,
        "chain_id_matches": observed_match if observed_match is not None else configured_match,
        "rpc_http_configured": bool(_configured_rpc_http()),
        "rpc_ws_configured": bool(settings.robinhood_chain_effective_rpc_ws()),
        "timeout_s": float(settings.robinhood_chain_timeout_s),
        "cache_ttl_s": float(settings.robinhood_chain_cache_ttl_s),
        "error_backoff_s": float(settings.robinhood_chain_error_backoff_s),
        "max_concurrent": int(settings.robinhood_chain_max_concurrent),
        "last_good_at": _iso_or_none(_LAST_GOOD_AT),
        "last_error": _LAST_ERROR,
        "backoff_until": _iso_or_none(_BACKOFF_UNTIL_UTC),
        "allowed_probe_checks": list(_DEFAULT_PROBE_CHECKS),
    }


def _set_transient_backoff(message: str) -> None:
    global _LAST_ERROR, _BACKOFF_UNTIL_MONOTONIC, _BACKOFF_UNTIL_UTC
    seconds = max(0.0, float(settings.robinhood_chain_error_backoff_s))
    _LAST_ERROR = str(message or "Robinhood Chain RPC transient error")
    _BACKOFF_UNTIL_MONOTONIC = time.monotonic() + seconds
    _BACKOFF_UNTIL_UTC = _utc_now() + timedelta(seconds=seconds) if seconds > 0 else None


def _clear_backoff_after_success() -> None:
    global _LAST_GOOD_AT, _LAST_ERROR, _BACKOFF_UNTIL_MONOTONIC, _BACKOFF_UNTIL_UTC
    _LAST_GOOD_AT = _utc_now()
    _LAST_ERROR = None
    _BACKOFF_UNTIL_MONOTONIC = 0.0
    _BACKOFF_UNTIL_UTC = None


def _normalize_checks(checks: Optional[List[str]]) -> List[str]:
    requested = checks if checks is not None else _DEFAULT_PROBE_CHECKS
    out: List[str] = []
    seen = set()
    unknown: List[str] = []

    for raw in requested:
        key = str(raw or "").strip().lower()
        if not key:
            continue
        if key not in _PROBE_DEFINITIONS:
            unknown.append(key)
            continue
        if key not in seen:
            seen.add(key)
            out.append(key)

    if unknown:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "unsupported_probe_check",
                "unknown": unknown,
                "allowed": list(_DEFAULT_PROBE_CHECKS),
            },
        )

    if not out:
        out = list(_DEFAULT_PROBE_CHECKS)

    # Identity is always checked first before any other read.
    if "chain_id" in out:
        out.remove("chain_id")
    return ["chain_id", *out]


async def _cached_result(check: str) -> Optional[Dict[str, Any]]:
    ttl = max(0.0, float(settings.robinhood_chain_cache_ttl_s))
    if ttl <= 0:
        return None
    now = time.monotonic()
    async with _CACHE_LOCK:
        item = _CACHE.get(check)
        if item is None:
            return None
        expires_at, result = item
        if expires_at <= now:
            _CACHE.pop(check, None)
            return None
        cached = copy.deepcopy(result)
        cached["cached"] = True
        return cached


async def _store_cache(check: str, result: Dict[str, Any]) -> None:
    ttl = max(0.0, float(settings.robinhood_chain_cache_ttl_s))
    if ttl <= 0:
        return
    async with _CACHE_LOCK:
        _CACHE[check] = (time.monotonic() + ttl, copy.deepcopy(result))


async def _rpc_check(check: str, *, force_refresh: bool) -> Dict[str, Any]:
    global _LAST_ERROR, _LAST_OBSERVED_CHAIN_ID

    if not force_refresh:
        cached = await _cached_result(check)
        if cached is not None:
            return cached

    now = time.monotonic()
    if _BACKOFF_UNTIL_MONOTONIC > now:
        return {
            "ok": False,
            "check": check,
            "method": _PROBE_DEFINITIONS[check][0],
            "cached": False,
            "error": "rpc_backoff_active",
            "backoff_until": _iso_or_none(_BACKOFF_UNTIL_UTC),
        }

    rpc = _configured_rpc_http()
    if not rpc:
        return {
            "ok": False,
            "check": check,
            "method": _PROBE_DEFINITIONS[check][0],
            "cached": False,
            "error": "ROBINHOOD_CHAIN_RPC_HTTP is not configured",
        }

    method, params = _PROBE_DEFINITIONS[check]
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    started = time.perf_counter()

    async with _RPC_SEMAPHORE:
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(float(settings.robinhood_chain_timeout_s)),
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "User-Agent": "UTT-Robinhood-Chain-ReadOnly/1.0",
                },
            ) as client:
                response = await client.post(rpc, json=payload)

            elapsed_ms = round((time.perf_counter() - started) * 1000.0, 1)
            retry_after = response.headers.get("Retry-After")

            try:
                body = response.json()
            except Exception:
                body = {"non_json_body": response.text[:1000]}

            if response.status_code == 429 or response.status_code >= 500:
                message = f"HTTP {response.status_code} from Robinhood Chain RPC"
                _set_transient_backoff(message)
                return {
                    "ok": False,
                    "check": check,
                    "method": method,
                    "cached": False,
                    "http_status": response.status_code,
                    "elapsed_ms": elapsed_ms,
                    "retry_after": retry_after,
                    "error": body,
                }

            if not response.is_success:
                _LAST_ERROR = f"HTTP {response.status_code} from Robinhood Chain RPC"
                return {
                    "ok": False,
                    "check": check,
                    "method": method,
                    "cached": False,
                    "http_status": response.status_code,
                    "elapsed_ms": elapsed_ms,
                    "error": body,
                }

            if isinstance(body, dict) and body.get("error") is not None:
                _LAST_ERROR = str(body.get("error"))
                return {
                    "ok": False,
                    "check": check,
                    "method": method,
                    "cached": False,
                    "http_status": response.status_code,
                    "elapsed_ms": elapsed_ms,
                    "error": body.get("error"),
                }

            result = body.get("result") if isinstance(body, dict) else body
            record = {
                "ok": True,
                "check": check,
                "method": method,
                "cached": False,
                "http_status": response.status_code,
                "elapsed_ms": elapsed_ms,
                "result": result,
            }

            if check == "chain_id":
                _LAST_OBSERVED_CHAIN_ID = str(result or "").strip().lower() or None

            _clear_backoff_after_success()
            await _store_cache(check, record)
            return record

        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
            elapsed_ms = round((time.perf_counter() - started) * 1000.0, 1)
            message = f"{type(exc).__name__}: {exc}"
            _set_transient_backoff(message)
            return {
                "ok": False,
                "check": check,
                "method": method,
                "cached": False,
                "http_status": None,
                "elapsed_ms": elapsed_ms,
                "error": message,
            }
        except Exception as exc:
            elapsed_ms = round((time.perf_counter() - started) * 1000.0, 1)
            message = f"{type(exc).__name__}: {exc}"
            _LAST_ERROR = message
            return {
                "ok": False,
                "check": check,
                "method": method,
                "cached": False,
                "http_status": None,
                "elapsed_ms": elapsed_ms,
                "error": message,
            }


@router.get("/status")
def robinhood_chain_status() -> Dict[str, Any]:
    return _status_payload()


@router.post("/rpc_probe")
async def robinhood_chain_rpc_probe(
    payload: Optional[RobinhoodChainProbeRequest] = None,
) -> Dict[str, Any]:
    if not bool(settings.robinhood_chain_enabled):
        raise HTTPException(status_code=503, detail="Robinhood Chain is disabled")
    if not _configured_rpc_http():
        raise HTTPException(status_code=503, detail="ROBINHOOD_CHAIN_RPC_HTTP is not configured")

    request = payload or RobinhoodChainProbeRequest()
    checks = _normalize_checks(request.checks)
    results: Dict[str, Dict[str, Any]] = {}

    chain_record = await _rpc_check("chain_id", force_refresh=bool(request.force_refresh))
    results["chain_id"] = chain_record

    actual_chain_id = str(chain_record.get("result") or "").strip().lower()
    chain_matches = bool(chain_record.get("ok")) and actual_chain_id == _EXPECTED_CHAIN_ID_HEX

    if not chain_matches:
        for check in checks[1:]:
            method = _PROBE_DEFINITIONS[check][0]
            results[check] = {
                "ok": False,
                "check": check,
                "method": method,
                "cached": False,
                "skipped": True,
                "error": "chain_id_mismatch_or_unavailable",
            }
    else:
        for check in checks[1:]:
            results[check] = await _rpc_check(check, force_refresh=bool(request.force_refresh))

    overall_ok = chain_matches and all(bool(record.get("ok")) for record in results.values())

    return {
        "ok": overall_ok,
        "read_only": True,
        "expected_chain_id": _EXPECTED_CHAIN_ID_DECIMAL,
        "expected_chain_id_hex": _EXPECTED_CHAIN_ID_HEX,
        "actual_chain_id": actual_chain_id or None,
        "chain_id_matches": chain_matches,
        "requested_checks": checks,
        "results": results,
        "status": _status_payload(),
    }
