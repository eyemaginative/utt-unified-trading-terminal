from __future__ import annotations

import asyncio
import copy
import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx


READ_ONLY_EVM_RPC_METHODS = frozenset(
    {
        "eth_chainId",
        "net_version",
        "web3_clientVersion",
        "eth_blockNumber",
        "eth_getBlockByNumber",
        "eth_gasPrice",
        "eth_maxPriorityFeePerGas",
        "eth_feeHistory",
        "eth_getBalance",
        "eth_getCode",
        "eth_call",
        "eth_getTransactionByHash",
        "eth_getTransactionReceipt",
    }
)

_EVM_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
_WEI_PER_ETH = 10**18
_ERC20_BALANCE_OF_SELECTOR = "70a08231"
_MAX_UINT256 = (1 << 256) - 1


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_or_none(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def validate_evm_address(value: str) -> str:
    address = str(value or "").strip()
    if not _EVM_ADDRESS_RE.fullmatch(address):
        raise ValueError("invalid EVM address: expected 0x followed by exactly 40 hexadecimal characters")
    return address


def decode_hex_quantity(value: Any) -> int:
    text = str(value or "").strip()
    if not text.startswith("0x"):
        raise ValueError("invalid EVM quantity: expected 0x-prefixed hexadecimal value")
    if text == "0x":
        raise ValueError("invalid EVM quantity: missing hexadecimal digits")
    try:
        quantity = int(text, 16)
    except Exception as exc:
        raise ValueError("invalid EVM quantity: malformed hexadecimal value") from exc
    if quantity < 0:
        raise ValueError("invalid EVM quantity: negative values are not permitted")
    return quantity


def format_wei_as_eth(wei: int) -> str:
    atomic = int(wei)
    if atomic < 0:
        raise ValueError("wei amount cannot be negative")
    whole, remainder = divmod(atomic, _WEI_PER_ETH)
    if remainder == 0:
        return str(whole)
    return f"{whole}.{remainder:018d}".rstrip("0")


def format_atomic_units(atomic: int, decimals: int) -> str:
    quantity = int(atomic)
    if quantity < 0:
        raise ValueError("atomic token amount cannot be negative")
    try:
        places = int(decimals)
    except Exception as exc:
        raise ValueError("token decimals must be an integer") from exc
    if places < 0 or places > 18:
        raise ValueError("token decimals must be between 0 and 18")
    if places == 0:
        return str(quantity)
    scale = 10**places
    whole, remainder = divmod(quantity, scale)
    if remainder == 0:
        return str(whole)
    return f"{whole}.{remainder:0{places}d}".rstrip("0")


def encode_erc20_balance_of(owner_address: str) -> str:
    owner = validate_evm_address(owner_address)
    owner_word = owner[2:].lower().rjust(64, "0")
    return f"0x{_ERC20_BALANCE_OF_SELECTOR}{owner_word}"


def decode_abi_uint256(value: Any) -> int:
    text = str(value or "").strip()
    if not text.startswith("0x"):
        raise ValueError("invalid ERC-20 balanceOf result: expected 0x-prefixed hexadecimal data")
    body = text[2:]
    if not body:
        raise ValueError("invalid ERC-20 balanceOf result: empty return data")
    if len(body) > 64:
        raise ValueError("invalid ERC-20 balanceOf result: exceeds uint256 width")
    if not re.fullmatch(r"[0-9a-fA-F]+", body):
        raise ValueError("invalid ERC-20 balanceOf result: malformed hexadecimal data")
    quantity = int(body, 16)
    if quantity < 0 or quantity > _MAX_UINT256:
        raise ValueError("invalid ERC-20 balanceOf result: outside uint256 range")
    return quantity


class EvmRpcClient:
    """Bounded, read-only JSON-RPC client for EVM-compatible chains.

    The client intentionally exposes only a fixed method allowlist. It keeps
    chain identity checks, request caching, concurrency limits, and transient
    backoff in one reusable service without providing transaction-signing or
    transaction-broadcast functionality.
    """

    def __init__(
        self,
        *,
        name: str,
        rpc_url: str,
        expected_chain_id: int,
        timeout_s: float = 15.0,
        cache_ttl_s: float = 30.0,
        error_backoff_s: float = 120.0,
        max_concurrent: int = 1,
        user_agent: str = "UTT-EVM-ReadOnly/1.0",
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> None:
        self.name = str(name or "evm").strip() or "evm"
        self.rpc_url = str(rpc_url or "").strip().rstrip("/")
        self.expected_chain_id = int(expected_chain_id)
        self.expected_chain_id_hex = hex(self.expected_chain_id)
        self.timeout_s = max(1.0, float(timeout_s))
        self.cache_ttl_s = max(0.0, float(cache_ttl_s))
        self.error_backoff_s = max(0.0, float(error_backoff_s))
        self.max_concurrent = max(1, int(max_concurrent))
        self.user_agent = str(user_agent or "UTT-EVM-ReadOnly/1.0")
        self.transport = transport

        self._cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._cache_lock = asyncio.Lock()
        self._rpc_semaphore = asyncio.Semaphore(self.max_concurrent)

        self._last_good_at: Optional[datetime] = None
        self._last_error: Optional[str] = None
        self._last_observed_chain_id: Optional[str] = None
        self._backoff_until_monotonic = 0.0
        self._backoff_until_utc: Optional[datetime] = None

    def status(self) -> Dict[str, Any]:
        observed = str(self._last_observed_chain_id or "").strip().lower() or None
        observed_match = observed == self.expected_chain_id_hex if observed is not None else None
        return {
            "name": self.name,
            "configured": bool(self.rpc_url),
            "expected_chain_id": self.expected_chain_id,
            "expected_chain_id_hex": self.expected_chain_id_hex,
            "rpc_chain_id": observed,
            "chain_id_matches": observed_match,
            "timeout_s": self.timeout_s,
            "cache_ttl_s": self.cache_ttl_s,
            "error_backoff_s": self.error_backoff_s,
            "max_concurrent": self.max_concurrent,
            "last_good_at": iso_or_none(self._last_good_at),
            "last_error": self._last_error,
            "backoff_until": iso_or_none(self._backoff_until_utc),
            "read_only": True,
        }

    def _cache_key(self, method: str, params: List[Any], namespace: Optional[str]) -> str:
        if namespace:
            return str(namespace)
        encoded = json.dumps(params or [], sort_keys=True, separators=(",", ":"), default=str)
        return f"{method}:{encoded}"

    async def _cached_result(self, key: str) -> Optional[Dict[str, Any]]:
        if self.cache_ttl_s <= 0:
            return None
        now = time.monotonic()
        async with self._cache_lock:
            item = self._cache.get(key)
            if item is None:
                return None
            expires_at, result = item
            if expires_at <= now:
                self._cache.pop(key, None)
                return None
            cached = copy.deepcopy(result)
            cached["cached"] = True
            return cached

    async def _store_cache(self, key: str, result: Dict[str, Any]) -> None:
        if self.cache_ttl_s <= 0:
            return
        async with self._cache_lock:
            self._cache[key] = (time.monotonic() + self.cache_ttl_s, copy.deepcopy(result))

    def _set_transient_backoff(self, message: str) -> None:
        self._last_error = str(message or f"{self.name} RPC transient error")
        self._backoff_until_monotonic = time.monotonic() + self.error_backoff_s
        self._backoff_until_utc = (
            utc_now() + timedelta(seconds=self.error_backoff_s)
            if self.error_backoff_s > 0
            else None
        )

    def _clear_backoff_after_success(self) -> None:
        self._last_good_at = utc_now()
        self._last_error = None
        self._backoff_until_monotonic = 0.0
        self._backoff_until_utc = None

    async def rpc_read(
        self,
        method: str,
        params: Optional[List[Any]] = None,
        *,
        cache_namespace: Optional[str] = None,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        method_name = str(method or "").strip()
        call_params = list(params or [])

        if method_name not in READ_ONLY_EVM_RPC_METHODS:
            return {
                "ok": False,
                "method": method_name,
                "cached": False,
                "error": "unsupported_read_only_rpc_method",
            }

        if not self.rpc_url:
            return {
                "ok": False,
                "method": method_name,
                "cached": False,
                "error": "rpc_url_not_configured",
            }

        cache_key = self._cache_key(method_name, call_params, cache_namespace)
        if not force_refresh:
            cached = await self._cached_result(cache_key)
            if cached is not None:
                return cached

        if self._backoff_until_monotonic > time.monotonic():
            return {
                "ok": False,
                "method": method_name,
                "cached": False,
                "error": "rpc_backoff_active",
                "backoff_until": iso_or_none(self._backoff_until_utc),
            }

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method_name,
            "params": call_params,
        }
        started = time.perf_counter()

        async with self._rpc_semaphore:
            try:
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(self.timeout_s),
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                        "User-Agent": self.user_agent,
                    },
                    transport=self.transport,
                ) as client:
                    response = await client.post(self.rpc_url, json=payload)

                elapsed_ms = round((time.perf_counter() - started) * 1000.0, 1)
                retry_after = response.headers.get("Retry-After")

                try:
                    body = response.json()
                except Exception:
                    body = {"non_json_body": response.text[:1000]}

                if response.status_code == 429 or response.status_code >= 500:
                    message = f"HTTP {response.status_code} from {self.name} RPC"
                    self._set_transient_backoff(message)
                    return {
                        "ok": False,
                        "method": method_name,
                        "cached": False,
                        "http_status": response.status_code,
                        "elapsed_ms": elapsed_ms,
                        "retry_after": retry_after,
                        "error": body,
                    }

                if not response.is_success:
                    self._last_error = f"HTTP {response.status_code} from {self.name} RPC"
                    return {
                        "ok": False,
                        "method": method_name,
                        "cached": False,
                        "http_status": response.status_code,
                        "elapsed_ms": elapsed_ms,
                        "error": body,
                    }

                if isinstance(body, dict) and body.get("error") is not None:
                    self._last_error = str(body.get("error"))
                    return {
                        "ok": False,
                        "method": method_name,
                        "cached": False,
                        "http_status": response.status_code,
                        "elapsed_ms": elapsed_ms,
                        "error": body.get("error"),
                    }

                result = body.get("result") if isinstance(body, dict) else body
                record = {
                    "ok": True,
                    "method": method_name,
                    "cached": False,
                    "http_status": response.status_code,
                    "elapsed_ms": elapsed_ms,
                    "result": result,
                    "fetched_at": utc_now().isoformat(),
                }

                if method_name == "eth_chainId":
                    self._last_observed_chain_id = str(result or "").strip().lower() or None

                self._clear_backoff_after_success()
                await self._store_cache(cache_key, record)
                return record

            except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
                elapsed_ms = round((time.perf_counter() - started) * 1000.0, 1)
                message = f"{type(exc).__name__}: {exc}"
                self._set_transient_backoff(message)
                return {
                    "ok": False,
                    "method": method_name,
                    "cached": False,
                    "http_status": None,
                    "elapsed_ms": elapsed_ms,
                    "error": message,
                }
            except Exception as exc:
                elapsed_ms = round((time.perf_counter() - started) * 1000.0, 1)
                message = f"{type(exc).__name__}: {exc}"
                self._last_error = message
                return {
                    "ok": False,
                    "method": method_name,
                    "cached": False,
                    "http_status": None,
                    "elapsed_ms": elapsed_ms,
                    "error": message,
                }

    async def verify_expected_chain(self, *, force_refresh: bool = False) -> Dict[str, Any]:
        record = await self.rpc_read(
            "eth_chainId",
            [],
            cache_namespace="identity:eth_chainId",
            force_refresh=force_refresh,
        )
        actual = str(record.get("result") or "").strip().lower()
        matches = bool(record.get("ok")) and actual == self.expected_chain_id_hex
        return {
            "ok": matches,
            "expected_chain_id": self.expected_chain_id,
            "expected_chain_id_hex": self.expected_chain_id_hex,
            "actual_chain_id": actual or None,
            "chain_id_matches": matches,
            "rpc": record,
        }

    async def get_native_balance(
        self,
        address: str,
        *,
        block_tag: str = "latest",
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        normalized_address = validate_evm_address(address)
        tag = str(block_tag or "latest").strip() or "latest"
        if tag != "latest":
            return {
                "ok": False,
                "address": normalized_address,
                "error": "unsupported_block_tag",
            }

        identity = await self.verify_expected_chain(force_refresh=force_refresh)
        if not identity.get("ok"):
            return {
                "ok": False,
                "address": normalized_address,
                "block_tag": tag,
                "error": "chain_id_mismatch_or_unavailable",
                "chain": identity,
            }

        balance_record = await self.rpc_read(
            "eth_getBalance",
            [normalized_address, tag],
            cache_namespace=f"balance:{normalized_address.lower()}:{tag}",
            force_refresh=force_refresh,
        )
        if not balance_record.get("ok"):
            return {
                "ok": False,
                "address": normalized_address,
                "block_tag": tag,
                "error": "native_balance_rpc_failed",
                "chain": identity,
                "rpc": balance_record,
            }

        try:
            balance_wei = decode_hex_quantity(balance_record.get("result"))
        except ValueError as exc:
            return {
                "ok": False,
                "address": normalized_address,
                "block_tag": tag,
                "error": str(exc),
                "chain": identity,
                "rpc": balance_record,
            }

        return {
            "ok": True,
            "address": normalized_address,
            "block_tag": tag,
            "balance_wei": str(balance_wei),
            "balance_eth": format_wei_as_eth(balance_wei),
            "cached": bool(balance_record.get("cached")),
            "fetched_at": balance_record.get("fetched_at") or utc_now().isoformat(),
            "chain": identity,
            "rpc": balance_record,
            "read_only": True,
        }

    async def get_erc20_balance(
        self,
        owner_address: str,
        contract_address: str,
        decimals: int,
        *,
        block_tag: str = "latest",
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        owner = validate_evm_address(owner_address)
        contract = validate_evm_address(contract_address)
        try:
            token_decimals = int(decimals)
        except Exception:
            return {
                "ok": False,
                "owner_address": owner,
                "contract_address": contract,
                "error": "invalid_token_decimals",
            }
        if token_decimals < 0 or token_decimals > 18:
            return {
                "ok": False,
                "owner_address": owner,
                "contract_address": contract,
                "decimals": token_decimals,
                "error": "invalid_token_decimals",
            }

        tag = str(block_tag or "latest").strip() or "latest"
        if tag != "latest":
            return {
                "ok": False,
                "owner_address": owner,
                "contract_address": contract,
                "error": "unsupported_block_tag",
            }

        identity = await self.verify_expected_chain(force_refresh=force_refresh)
        if not identity.get("ok"):
            return {
                "ok": False,
                "owner_address": owner,
                "contract_address": contract,
                "block_tag": tag,
                "error": "chain_id_mismatch_or_unavailable",
                "chain": identity,
            }

        call_data = encode_erc20_balance_of(owner)
        balance_record = await self.rpc_read(
            "eth_call",
            [{"to": contract, "data": call_data}, tag],
            cache_namespace=f"erc20_balance:{contract.lower()}:{owner.lower()}:{tag}",
            force_refresh=force_refresh,
        )
        if not balance_record.get("ok"):
            return {
                "ok": False,
                "owner_address": owner,
                "contract_address": contract,
                "block_tag": tag,
                "error": "erc20_balance_rpc_failed",
                "chain": identity,
                "rpc": balance_record,
            }

        try:
            balance_atomic = decode_abi_uint256(balance_record.get("result"))
            balance_token = format_atomic_units(balance_atomic, token_decimals)
        except ValueError as exc:
            return {
                "ok": False,
                "owner_address": owner,
                "contract_address": contract,
                "block_tag": tag,
                "decimals": token_decimals,
                "error": str(exc),
                "chain": identity,
                "rpc": balance_record,
            }

        return {
            "ok": True,
            "owner_address": owner,
            "contract_address": contract,
            "decimals": token_decimals,
            "block_tag": tag,
            "balance_atomic": str(balance_atomic),
            "balance_token": balance_token,
            "cached": bool(balance_record.get("cached")),
            "fetched_at": balance_record.get("fetched_at") or utc_now().isoformat(),
            "chain": identity,
            "rpc": balance_record,
            "read_only": True,
        }


_ROBINHOOD_CHAIN_CLIENT: Optional[EvmRpcClient] = None


def get_robinhood_chain_client() -> EvmRpcClient:
    global _ROBINHOOD_CHAIN_CLIENT
    if _ROBINHOOD_CHAIN_CLIENT is None:
        from ..config import settings

        _ROBINHOOD_CHAIN_CLIENT = EvmRpcClient(
            name="robinhood_chain",
            rpc_url=settings.robinhood_chain_effective_rpc_http(),
            expected_chain_id=4663,
            timeout_s=float(settings.robinhood_chain_timeout_s),
            cache_ttl_s=float(settings.robinhood_chain_cache_ttl_s),
            error_backoff_s=float(settings.robinhood_chain_error_backoff_s),
            max_concurrent=int(settings.robinhood_chain_max_concurrent),
            user_agent="UTT-Robinhood-Chain-EVM-ReadOnly/1.0",
        )
    return _ROBINHOOD_CHAIN_CLIENT
