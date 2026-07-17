from __future__ import annotations

import asyncio
import base64
import copy
import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Tuple

import httpx

from ..config import settings
from .evm_rpc import get_robinhood_chain_client, validate_evm_address


_HISTORY_SOURCE = "blockscout_v2"
_EXPLORER_URL = "https://robinhoodchain.blockscout.com"
_EXPECTED_CHAIN_ID = 4663
_EXPECTED_CHAIN_ID_HEX = hex(_EXPECTED_CHAIN_ID)
_TX_HASH_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
_ALLOWED_CURSOR_KEYS = frozenset({"block_number", "index", "items_count"})
_TRANSACTION_TOKEN_CURSOR_KEYS = frozenset({
    "block_number",
    "index",
    "items_count",
    "transaction_hash",
    "batch_block_hash",
    "batch_log_index",
    "batch_transaction_hash",
    "index_in_batch",
})
_APPROVE_SELECTOR = "0x095ea7b3"
_MAX_CACHE_ENTRIES = 256
_MAX_CURSOR_LENGTH = 4096


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return int(default)


def _address_hash(value: Any) -> Optional[str]:
    if isinstance(value, Mapping):
        value = value.get("hash") or value.get("address_hash")
    text = str(value or "").strip()
    try:
        return validate_evm_address(text)
    except ValueError:
        return None


def _transaction_hash(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text if _TX_HASH_RE.fullmatch(text) else None


def validate_transaction_hash(value: Any) -> str:
    """Validate one canonical EVM transaction hash for fixed Blockscout reads."""
    tx_hash = _transaction_hash(value)
    if not tx_hash:
        raise ValueError("transaction hash must be 0x followed by 64 hexadecimal characters")
    return tx_hash


def _format_atomic(atomic: int, decimals: int) -> str:
    quantity = max(0, int(atomic))
    places = max(0, min(18, int(decimals)))
    if places == 0:
        return str(quantity)
    scale = 10**places
    whole, remainder = divmod(quantity, scale)
    if remainder == 0:
        return str(whole)
    return f"{whole}.{remainder:0{places}d}".rstrip("0")


def _direction(owner: str, from_address: Optional[str], to_address: Optional[str]) -> str:
    owner_lower = owner.lower()
    from_lower = str(from_address or "").lower()
    to_lower = str(to_address or "").lower()
    if from_lower == owner_lower and to_lower == owner_lower:
        return "self"
    if from_lower == owner_lower:
        return "out"
    if to_lower == owner_lower:
        return "in"
    return "other"


def _method_name(tx: Mapping[str, Any]) -> str:
    direct = str(tx.get("method") or "").strip()
    if direct:
        return direct
    decoded = tx.get("decoded_input")
    if isinstance(decoded, Mapping):
        call = str(decoded.get("method_call") or "").strip()
        if call:
            return call.split("(", 1)[0].strip()
    return ""


def _tx_status(tx: Mapping[str, Any]) -> str:
    status = str(tx.get("status") or "").strip().lower()
    if status in {"ok", "success", "successful"}:
        return "ok"
    if status in {"error", "failed", "failure"}:
        return "error"
    result = str(tx.get("result") or "").strip().lower()
    revert_reason = str(tx.get("revert_reason") or "").strip().lower()
    if "revert" in result or "revert" in revert_reason:
        return "error"
    return status or "unknown"


def _looks_like_swap(method: str, tx: Mapping[str, Any]) -> bool:
    text = method.lower().replace("_", "")
    if any(marker in text for marker in ("swap", "exactinput", "exactoutput", "unoswap")):
        return True
    actions = tx.get("actions")
    if isinstance(actions, list):
        for action in actions[:20]:
            if isinstance(action, Mapping) and "swap" in str(action.get("type") or "").lower():
                return True
    return False


def _looks_like_bridge(method: str, tx: Mapping[str, Any]) -> bool:
    text = method.lower().replace("_", "")
    if any(marker in text for marker in ("bridge", "outboundtransfer", "sendtxtol1", "withdraweth")):
        return True
    target = tx.get("to")
    if isinstance(target, Mapping):
        name = " ".join(
            str(target.get(key) or "")
            for key in ("name", "implementation_name")
        ).lower()
        if "bridge" in name:
            return True
    return False


def _is_approval(method: str, raw_input: str) -> bool:
    method_lower = method.lower().replace("_", "")
    return "approve" in method_lower or str(raw_input or "").lower().startswith(_APPROVE_SELECTOR)


def _classify_transaction(
    tx: Mapping[str, Any],
    *,
    value_atomic: int,
    has_token_transfer: bool,
) -> str:
    status = _tx_status(tx)
    method = _method_name(tx)
    raw_input = str(tx.get("raw_input") or tx.get("input") or "").strip()
    result = str(tx.get("result") or "").lower()
    revert_reason = str(tx.get("revert_reason") or "").lower()

    if status == "error":
        return "reverted" if "revert" in result or "revert" in revert_reason else "failed"
    if _is_approval(method, raw_input):
        return "approval"
    if _looks_like_bridge(method, tx):
        return "bridge_candidate"
    if _looks_like_swap(method, tx):
        return "swap_candidate"
    if value_atomic > 0:
        return "native_transfer"
    if has_token_transfer:
        return "erc20_transfer"

    to_node = tx.get("to")
    to_is_contract = bool(to_node.get("is_contract")) if isinstance(to_node, Mapping) else False
    has_calldata = bool(raw_input and raw_input not in {"0x", "0X"})
    if method or has_calldata or to_is_contract:
        return "contract_call"

    fee_value = tx.get("fee")
    if isinstance(fee_value, Mapping):
        fee_value = fee_value.get("value")
    if _safe_int(fee_value, 0) > 0:
        return "fee_only"
    return "unknown"


def _registry_entry(
    registry_tokens: Mapping[str, Mapping[str, Any]],
    contract_address: Optional[str],
) -> Optional[Mapping[str, Any]]:
    if not contract_address:
        return None
    return registry_tokens.get(contract_address.lower())


def _next_page_params(value: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(value, Mapping):
        return None
    out: Dict[str, Any] = {}
    for key in _ALLOWED_CURSOR_KEYS:
        if key in value and value.get(key) is not None:
            out[key] = value.get(key)
    return out or None


def _transaction_token_next_page_params(value: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(value, Mapping):
        return None
    out: Dict[str, Any] = {}
    for key in _TRANSACTION_TOKEN_CURSOR_KEYS:
        if key in value and value.get(key) is not None:
            out[key] = value.get(key)
    return out or None


def _encode_cursor(page: int, tx_params: Optional[Dict[str, Any]], token_params: Optional[Dict[str, Any]]) -> str:
    payload = {
        "v": 1,
        "page": int(page),
        "transactions": tx_params,
        "token_transfers": token_params,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _decode_cursor(cursor: Optional[str], max_pages: int) -> Tuple[int, Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    text = str(cursor or "").strip()
    if not text:
        return 1, None, None
    if len(text) > _MAX_CURSOR_LENGTH:
        raise ValueError("history cursor exceeds the maximum supported length")
    try:
        padded = text + ("=" * (-len(text) % 4))
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    except Exception as exc:
        raise ValueError("invalid history cursor") from exc
    if not isinstance(payload, Mapping) or int(payload.get("v") or 0) != 1:
        raise ValueError("unsupported history cursor version")
    page = _safe_int(payload.get("page"), 0)
    if page < 2 or page > int(max_pages):
        raise ValueError("history cursor page is outside the configured bounds")
    return (
        page,
        _next_page_params(payload.get("transactions")),
        _next_page_params(payload.get("token_transfers")),
    )


def _normalize_token_transfer(
    item: Mapping[str, Any],
    *,
    owner: str,
    tx_lookup: Mapping[str, Mapping[str, Any]],
    registry_tokens: Mapping[str, Mapping[str, Any]],
) -> Optional[Dict[str, Any]]:
    tx_hash = _transaction_hash(item.get("transaction_hash") or item.get("tx_hash"))
    if not tx_hash:
        return None
    tx = tx_lookup.get(tx_hash.lower(), {})
    from_address = _address_hash(item.get("from"))
    to_address = _address_hash(item.get("to"))
    token = item.get("token") if isinstance(item.get("token"), Mapping) else {}
    contract = _address_hash(token.get("address_hash") or item.get("contract_address"))
    registry = _registry_entry(registry_tokens, contract)

    provider_decimals = _safe_int(
        (item.get("total") or {}).get("decimals") if isinstance(item.get("total"), Mapping) else None,
        _safe_int(token.get("decimals"), 0),
    )
    decimals = _safe_int(registry.get("decimals"), provider_decimals) if registry else provider_decimals
    decimals = max(0, min(18, decimals))
    total = item.get("total") if isinstance(item.get("total"), Mapping) else {}
    atomic = max(0, _safe_int(total.get("value") or item.get("value"), 0))
    provider_symbol = str(token.get("symbol") or item.get("symbol") or "").strip().upper()
    asset = str(registry.get("symbol") or provider_symbol or "UNKNOWN").strip().upper() if registry else (provider_symbol or "UNKNOWN")

    method = str(item.get("method") or _method_name(tx)).strip()
    classification = "erc20_transfer"
    tx_classification = _classify_transaction(tx, value_atomic=0, has_token_transfer=True) if tx else "erc20_transfer"
    if tx_classification in {"failed", "reverted", "swap_candidate", "bridge_candidate"}:
        classification = tx_classification

    status = _tx_status(tx) if tx else "ok"
    fee_value = tx.get("fee") if tx else None
    if isinstance(fee_value, Mapping):
        fee_value = fee_value.get("value")
    fee_atomic = max(0, _safe_int(fee_value, 0))

    return {
        "id": f"{tx_hash.lower()}:erc20:{str(item.get('log_index') or item.get('index') or 0)}",
        "timestamp": item.get("timestamp") or tx.get("timestamp"),
        "transaction_hash": tx_hash,
        "status": status,
        "classification": classification,
        "direction": _direction(owner, from_address, to_address),
        "asset": asset,
        "amount_atomic": str(atomic),
        "amount": _format_atomic(atomic, decimals),
        "decimals": decimals,
        "from_address": from_address,
        "to_address": to_address,
        "method": method or None,
        "fee_wei": str(fee_atomic),
        "fee_eth": _format_atomic(fee_atomic, 18),
        "block_number": _safe_int(item.get("block_number") or tx.get("block_number"), 0) or None,
        "confirmations": _safe_int(tx.get("confirmations"), 0) if tx else None,
        "contract_address": contract,
        "registry_id": registry.get("registry_id") if registry else None,
        "registry_venue": registry.get("registry_venue") if registry else None,
        "registry_label": registry.get("label") if registry else None,
        "registered": bool(registry),
        "explorer_url": f"{_EXPLORER_URL}/tx/{tx_hash}",
        "source": _HISTORY_SOURCE,
        "read_only": True,
        "provider_raw": {
            "type": item.get("type"),
            "log_index": item.get("log_index") or item.get("index"),
            "token_name": token.get("name"),
            "provider_symbol": provider_symbol or None,
            "provider_decimals": provider_decimals,
        },
    }


def _normalize_transaction(
    tx: Mapping[str, Any],
    *,
    owner: str,
    has_token_transfer: bool,
    registry_tokens: Mapping[str, Mapping[str, Any]],
) -> Optional[Dict[str, Any]]:
    tx_hash = _transaction_hash(tx.get("hash") or tx.get("transaction_hash"))
    if not tx_hash:
        return None
    from_address = _address_hash(tx.get("from"))
    to_address = _address_hash(tx.get("to"))
    value_atomic = max(0, _safe_int(tx.get("value"), 0))
    classification = _classify_transaction(tx, value_atomic=value_atomic, has_token_transfer=has_token_transfer)

    # Token-transfer rows already carry their asset and amount. Keep a separate
    # transaction row only when it conveys native value or a non-transfer action.
    if has_token_transfer and value_atomic == 0 and classification == "erc20_transfer":
        return None

    method = _method_name(tx)
    fee_value = tx.get("fee")
    if isinstance(fee_value, Mapping):
        fee_value = fee_value.get("value")
    fee_atomic = max(0, _safe_int(fee_value, 0))
    contract = to_address if isinstance(tx.get("to"), Mapping) and bool(tx.get("to", {}).get("is_contract")) else None
    registry = _registry_entry(registry_tokens, contract)

    asset = "ETH" if value_atomic > 0 or classification == "fee_only" else (
        str(registry.get("symbol") or "").strip().upper() if registry else ""
    )

    return {
        "id": f"{tx_hash.lower()}:transaction",
        "timestamp": tx.get("timestamp"),
        "transaction_hash": tx_hash,
        "status": _tx_status(tx),
        "classification": classification,
        "direction": _direction(owner, from_address, to_address),
        "asset": asset or None,
        "amount_atomic": str(value_atomic),
        "amount": _format_atomic(value_atomic, 18),
        "decimals": 18 if asset == "ETH" else (registry.get("decimals") if registry else None),
        "from_address": from_address,
        "to_address": to_address,
        "method": method or None,
        "fee_wei": str(fee_atomic),
        "fee_eth": _format_atomic(fee_atomic, 18),
        "block_number": _safe_int(tx.get("block_number"), 0) or None,
        "confirmations": _safe_int(tx.get("confirmations"), 0),
        "contract_address": contract,
        "registry_id": registry.get("registry_id") if registry else None,
        "registry_venue": registry.get("registry_venue") if registry else None,
        "registry_label": registry.get("label") if registry else None,
        "registered": bool(registry),
        "explorer_url": f"{_EXPLORER_URL}/tx/{tx_hash}",
        "source": _HISTORY_SOURCE,
        "read_only": True,
        "provider_raw": {
            "transaction_types": tx.get("transaction_types"),
            "result": tx.get("result"),
            "revert_reason": tx.get("revert_reason"),
            "raw_input_prefix": str(tx.get("raw_input") or tx.get("input") or "")[:18] or None,
        },
    }


def _sort_key(item: Mapping[str, Any]) -> Tuple[str, int, str]:
    timestamp = str(item.get("timestamp") or "")
    block_number = _safe_int(item.get("block_number"), 0)
    return timestamp, block_number, str(item.get("id") or "")


class RobinhoodChainHistoryService:
    """Bounded, display-only Robinhood Chain history reader.

    It consumes only fixed Blockscout v2 address endpoints, verifies chain ID
    4663 before uncached reads, and never writes to UTT tables or exposes an
    arbitrary explorer/RPC proxy.
    """

    def __init__(
        self,
        *,
        api_base: str,
        timeout_s: float,
        cache_ttl_s: float,
        error_backoff_s: float,
        max_pages: int,
        page_size: int,
        max_concurrent: int,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> None:
        self.api_base = str(api_base or "").strip().rstrip("/")
        self.timeout_s = max(2.0, min(30.0, float(timeout_s)))
        self.cache_ttl_s = max(0.0, min(3600.0, float(cache_ttl_s)))
        self.error_backoff_s = max(0.0, min(3600.0, float(error_backoff_s)))
        self.max_pages = max(1, min(20, int(max_pages)))
        self.page_size = max(10, min(100, int(page_size)))
        self.transport = transport
        self._semaphore = asyncio.Semaphore(max(1, min(8, int(max_concurrent))))
        self._cache: Dict[str, Tuple[float, float, Dict[str, Any]]] = {}
        self._cache_lock = asyncio.Lock()
        self._last_good_at: Optional[datetime] = None
        self._last_error: Optional[str] = None
        self._backoff_until_monotonic = 0.0
        self._backoff_until_utc: Optional[datetime] = None

    def status(self) -> Dict[str, Any]:
        return {
            "configured": bool(self.api_base.startswith("https://") or self.api_base.startswith("http://")),
            "timeout_s": self.timeout_s,
            "cache_ttl_s": self.cache_ttl_s,
            "error_backoff_s": self.error_backoff_s,
            "max_pages": self.max_pages,
            "page_size": self.page_size,
            "last_good_at": _iso(self._last_good_at),
            "last_error": self._last_error,
            "backoff_until": _iso(self._backoff_until_utc),
            "source": _HISTORY_SOURCE,
            "read_only": True,
        }

    def _cache_key(self, address: str, cursor: Optional[str]) -> str:
        return f"{address.lower()}:{str(cursor or '').strip()}"

    def _transaction_cache_key(self, address: str, tx_hash: str) -> str:
        return f"transaction:{address.lower()}:{tx_hash.lower()}"

    async def _cached(self, key: str, *, allow_stale: bool = False) -> Optional[Dict[str, Any]]:
        now = time.monotonic()
        stale_window = max(300.0, min(3600.0, self.cache_ttl_s * 10.0))
        async with self._cache_lock:
            item = self._cache.get(key)
            if item is None:
                return None
            expires_at, stored_at, payload = item
            if expires_at > now:
                out = copy.deepcopy(payload)
                out["cached"] = True
                out["stale"] = False
                return out
            age = max(0.0, now - stored_at)
            if age > stale_window:
                self._cache.pop(key, None)
                return None
            if allow_stale:
                out = copy.deepcopy(payload)
                out["cached"] = True
                out["stale"] = True
                return out
            # Preserve an expired-but-bounded last-good entry so a subsequent
            # provider failure can return it explicitly as stale.
            return None

    async def _store(self, key: str, payload: Dict[str, Any]) -> None:
        if self.cache_ttl_s <= 0:
            return
        now = time.monotonic()
        async with self._cache_lock:
            if len(self._cache) >= _MAX_CACHE_ENTRIES:
                oldest_key = min(self._cache, key=lambda existing: self._cache[existing][1])
                self._cache.pop(oldest_key, None)
            self._cache[key] = (now + self.cache_ttl_s, now, copy.deepcopy(payload))

    def _set_backoff(self, message: str) -> None:
        self._last_error = str(message or "Robinhood Chain history provider error")
        self._backoff_until_monotonic = time.monotonic() + self.error_backoff_s
        self._backoff_until_utc = (
            _utc_now() + timedelta(seconds=self.error_backoff_s)
            if self.error_backoff_s > 0
            else None
        )

    def _clear_backoff(self) -> None:
        self._last_good_at = _utc_now()
        self._last_error = None
        self._backoff_until_monotonic = 0.0
        self._backoff_until_utc = None

    async def _get_json(self, path: str, params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        url = f"{self.api_base}/{path.lstrip('/')}"
        async with self._semaphore:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout_s),
                headers={
                    "Accept": "application/json",
                    "User-Agent": "UTT-Robinhood-Chain-History/8.0",
                },
                transport=self.transport,
            ) as client:
                response = await client.get(url, params=params or {})
        try:
            body = response.json()
        except Exception:
            body = {"non_json_body": response.text[:1000]}
        if response.status_code == 429 or response.status_code >= 500:
            message = f"HTTP {response.status_code} from Robinhood Chain Blockscout"
            self._set_backoff(message)
            raise RuntimeError(message)
        if not response.is_success:
            raise RuntimeError(f"HTTP {response.status_code} from Robinhood Chain Blockscout: {body}")
        if not isinstance(body, Mapping):
            raise RuntimeError("Robinhood Chain Blockscout returned a non-object payload")
        return dict(body)

    async def get_transaction_activity(
        self,
        address: str,
        tx_hash: str,
        *,
        force_refresh: bool,
        registry_tokens: Mapping[str, Mapping[str, Any]],
    ) -> Dict[str, Any]:
        """Return one canonical transaction group for read-only accounting preview.

        Only the fixed Blockscout v2 transaction-info and transaction-token-transfer
        resources are used. The result remains display data and is never persisted.
        """
        try:
            normalized_address = validate_evm_address(address)
            normalized_tx_hash = validate_transaction_hash(tx_hash)
        except ValueError as exc:
            return {"ok": False, "error": "invalid_transaction_request", "message": str(exc), "read_only": True}

        cache_key = self._transaction_cache_key(normalized_address, normalized_tx_hash)
        if not force_refresh:
            cached = await self._cached(cache_key)
            if cached is not None:
                return cached

        if not self.api_base.startswith(("https://", "http://")):
            return {"ok": False, "error": "history_api_not_configured", "read_only": True}

        if self._backoff_until_monotonic > time.monotonic():
            stale = await self._cached(cache_key, allow_stale=True)
            if stale is not None:
                stale["provider_backoff_active"] = True
                stale["backoff_until"] = _iso(self._backoff_until_utc)
                return stale
            return {
                "ok": False,
                "error": "history_backoff_active",
                "backoff_until": _iso(self._backoff_until_utc),
                "read_only": True,
            }

        chain = await get_robinhood_chain_client().verify_expected_chain(force_refresh=bool(force_refresh))
        if not chain.get("ok"):
            return {
                "ok": False,
                "error": "chain_id_mismatch_or_unavailable",
                "expected_chain_id": _EXPECTED_CHAIN_ID,
                "expected_chain_id_hex": _EXPECTED_CHAIN_ID_HEX,
                "chain": chain,
                "read_only": True,
            }

        try:
            transaction_payload = await self._get_json(
                f"transactions/{normalized_tx_hash}",
                None,
            )
        except Exception as exc:
            stale = await self._cached(cache_key, allow_stale=True)
            if stale is not None:
                stale["provider_errors"] = [{"source": "transaction", "error": str(exc)}]
                stale["partial"] = True
                return stale
            error_text = str(exc)
            return {
                "ok": False,
                "error": "transaction_not_found" if "HTTP 404" in error_text else "transaction_provider_unavailable",
                "message": error_text,
                "read_only": True,
            }

        returned_hash = _transaction_hash(transaction_payload.get("hash") or transaction_payload.get("transaction_hash"))
        if not returned_hash or returned_hash.lower() != normalized_tx_hash.lower():
            return {
                "ok": False,
                "error": "transaction_hash_mismatch",
                "requested_transaction_hash": normalized_tx_hash,
                "provider_transaction_hash": returned_hash,
                "read_only": True,
            }

        provider_errors: List[Dict[str, Any]] = []
        token_items: List[Mapping[str, Any]] = []
        token_params: Dict[str, Any] = {"type": "ERC-20"}
        try:
            # A single transaction can emit more than one provider page. Keep the
            # exact-hash read bounded to two pages / 100 normalized ERC-20 rows.
            for _ in range(2):
                token_payload = await self._get_json(
                    f"transactions/{normalized_tx_hash}/token-transfers",
                    token_params,
                )
                page_items = [
                    item
                    for item in token_payload.get("items") or []
                    if isinstance(item, Mapping)
                    and str((item.get("token") or {}).get("type") or "ERC-20").strip().upper() == "ERC-20"
                ]
                token_items.extend(page_items[:50])
                next_params = _transaction_token_next_page_params(token_payload.get("next_page_params"))
                if not next_params or len(token_items) >= 100:
                    break
                token_params = dict(next_params)
                token_params["type"] = "ERC-20"
        except Exception as exc:
            provider_errors.append({"source": "transaction_token_transfers", "error": str(exc)})

        token_items = token_items[:100]
        tx_lookup: Dict[str, Mapping[str, Any]] = {normalized_tx_hash.lower(): transaction_payload}
        normalized: List[Dict[str, Any]] = []

        for item in token_items:
            row = _normalize_token_transfer(
                item,
                owner=normalized_address,
                tx_lookup=tx_lookup,
                registry_tokens=registry_tokens,
            )
            if row is not None:
                normalized.append(row)

        transaction_row = _normalize_transaction(
            transaction_payload,
            owner=normalized_address,
            has_token_transfer=bool(token_items),
            registry_tokens=registry_tokens,
        )
        if transaction_row is not None:
            normalized.append(transaction_row)

        deduped: Dict[str, Dict[str, Any]] = {}
        for row in normalized:
            row_id = str(row.get("id") or "")
            if row_id and row_id not in deduped:
                deduped[row_id] = row
        items = sorted(deduped.values(), key=_sort_key, reverse=True)

        related = any(str(row.get("direction") or "").lower() in {"in", "out", "self"} for row in items)
        if not related:
            return {
                "ok": False,
                "error": "transaction_not_related_to_address",
                "address": normalized_address,
                "transaction_hash": normalized_tx_hash,
                "read_only": True,
            }

        fetched_at = _utc_now().isoformat()
        payload = {
            "ok": True,
            "venue": "robinhood_chain",
            "network": "robinhood_chain",
            "chain_id": _EXPECTED_CHAIN_ID,
            "chain_id_hex": _EXPECTED_CHAIN_ID_HEX,
            "address": normalized_address,
            "transaction_hash": normalized_tx_hash,
            "transaction": transaction_payload,
            "items": items,
            "item_count": len(items),
            "cached": False,
            "stale": False,
            "partial": bool(provider_errors),
            "provider_errors": provider_errors,
            "provider_counts": {
                "transaction": 1,
                "token_transfers": len(token_items),
            },
            "fetched_at": fetched_at,
            "source": _HISTORY_SOURCE,
            "explorer_url": f"{_EXPLORER_URL}/tx/{normalized_tx_hash}",
            "read_only": True,
            "persistence": {
                "wallet_address_txs": False,
                "asset_deposits": False,
                "asset_withdrawals": False,
                "bridge_transfer_records": False,
                "ledger_entries": False,
                "basis_lots": False,
            },
        }
        if not provider_errors:
            self._clear_backoff()
        else:
            self._last_good_at = _utc_now()
        payload["history_status"] = self.status()
        await self._store(cache_key, payload)
        return payload


    async def get_address_history(
        self,
        address: str,
        *,
        cursor: Optional[str],
        force_refresh: bool,
        registry_tokens: Mapping[str, Mapping[str, Any]],
    ) -> Dict[str, Any]:
        try:
            normalized_address = validate_evm_address(address)
            page, tx_cursor, token_cursor = _decode_cursor(cursor, self.max_pages)
        except ValueError as exc:
            return {"ok": False, "error": "invalid_history_request", "message": str(exc), "read_only": True}

        cache_key = self._cache_key(normalized_address, cursor)
        if not force_refresh:
            cached = await self._cached(cache_key)
            if cached is not None:
                return cached

        if not self.api_base.startswith(("https://", "http://")):
            return {"ok": False, "error": "history_api_not_configured", "read_only": True}

        if self._backoff_until_monotonic > time.monotonic():
            stale = await self._cached(cache_key, allow_stale=True)
            if stale is not None:
                stale["provider_backoff_active"] = True
                stale["backoff_until"] = _iso(self._backoff_until_utc)
                return stale
            return {
                "ok": False,
                "error": "history_backoff_active",
                "backoff_until": _iso(self._backoff_until_utc),
                "read_only": True,
            }

        chain = await get_robinhood_chain_client().verify_expected_chain(force_refresh=bool(force_refresh))
        if not chain.get("ok"):
            return {
                "ok": False,
                "error": "chain_id_mismatch_or_unavailable",
                "expected_chain_id": _EXPECTED_CHAIN_ID,
                "expected_chain_id_hex": _EXPECTED_CHAIN_ID_HEX,
                "chain": chain,
                "read_only": True,
            }

        # On the first page both sources are queried. On later pages a None
        # cursor means that source is exhausted and is not queried again.
        fetch_transactions = page == 1 or tx_cursor is not None
        fetch_token_transfers = page == 1 or token_cursor is not None
        transaction_payload: Dict[str, Any] = {"items": [], "next_page_params": None}
        token_payload: Dict[str, Any] = {"items": [], "next_page_params": None}
        provider_errors: List[Dict[str, Any]] = []
        successful_sources = 0

        try:
            if fetch_transactions:
                transaction_payload = await self._get_json(
                    f"addresses/{normalized_address}/transactions",
                    tx_cursor,
                )
                successful_sources += 1
        except Exception as exc:
            provider_errors.append({"source": "transactions", "error": str(exc)})

        try:
            if fetch_token_transfers:
                params = dict(token_cursor or {})
                params["type"] = "ERC-20"
                token_payload = await self._get_json(
                    f"addresses/{normalized_address}/token-transfers",
                    params,
                )
                successful_sources += 1
        except Exception as exc:
            provider_errors.append({"source": "token_transfers", "error": str(exc)})

        if provider_errors and successful_sources == 0:
            stale = await self._cached(cache_key, allow_stale=True)
            if stale is not None:
                stale["provider_errors"] = provider_errors
                stale["partial"] = True
                return stale
            return {
                "ok": False,
                "error": "history_provider_unavailable",
                "provider_errors": provider_errors,
                "read_only": True,
            }

        tx_items = [item for item in transaction_payload.get("items") or [] if isinstance(item, Mapping)][:50]
        token_items = [item for item in token_payload.get("items") or [] if isinstance(item, Mapping)][:50]
        tx_lookup: Dict[str, Mapping[str, Any]] = {}
        for tx in tx_items:
            tx_hash = _transaction_hash(tx.get("hash") or tx.get("transaction_hash"))
            if tx_hash:
                tx_lookup[tx_hash.lower()] = tx

        token_hashes = {
            tx_hash.lower()
            for item in token_items
            for tx_hash in [_transaction_hash(item.get("transaction_hash") or item.get("tx_hash"))]
            if tx_hash
        }

        normalized: List[Dict[str, Any]] = []
        for item in token_items:
            row = _normalize_token_transfer(
                item,
                owner=normalized_address,
                tx_lookup=tx_lookup,
                registry_tokens=registry_tokens,
            )
            if row is not None:
                normalized.append(row)

        for tx in tx_items:
            tx_hash = _transaction_hash(tx.get("hash") or tx.get("transaction_hash"))
            row = _normalize_transaction(
                tx,
                owner=normalized_address,
                has_token_transfer=bool(tx_hash and tx_hash.lower() in token_hashes),
                registry_tokens=registry_tokens,
            )
            if row is not None:
                normalized.append(row)

        deduped: Dict[str, Dict[str, Any]] = {}
        for row in normalized:
            row_id = str(row.get("id") or "")
            if row_id and row_id not in deduped:
                deduped[row_id] = row
        items = sorted(deduped.values(), key=_sort_key, reverse=True)[: self.page_size]

        next_tx = _next_page_params(transaction_payload.get("next_page_params")) if fetch_transactions else None
        next_token = _next_page_params(token_payload.get("next_page_params")) if fetch_token_transfers else None
        next_cursor = None
        if page < self.max_pages and (next_tx is not None or next_token is not None):
            next_cursor = _encode_cursor(page + 1, next_tx, next_token)

        fetched_at = _utc_now().isoformat()
        payload = {
            "ok": True,
            "venue": "robinhood_chain",
            "network": "robinhood_chain",
            "chain_id": _EXPECTED_CHAIN_ID,
            "chain_id_hex": _EXPECTED_CHAIN_ID_HEX,
            "address": normalized_address,
            "page": page,
            "page_size": self.page_size,
            "items": items,
            "item_count": len(items),
            "has_more": bool(next_cursor),
            "next_cursor": next_cursor,
            "cached": False,
            "stale": False,
            "partial": bool(provider_errors),
            "provider_errors": provider_errors,
            "provider_counts": {
                "transactions": len(tx_items),
                "token_transfers": len(token_items),
            },
            "fetched_at": fetched_at,
            "source": _HISTORY_SOURCE,
            "explorer_url": _EXPLORER_URL,
            "history_status": self.status(),
            "read_only": True,
            "persistence": {
                "wallet_address_txs": False,
                "asset_deposits": False,
                "asset_withdrawals": False,
                "ledger_entries": False,
                "basis_lots": False,
            },
        }
        if not provider_errors:
            self._clear_backoff()
        else:
            self._last_good_at = _utc_now()
        payload["history_status"] = self.status()
        await self._store(cache_key, payload)
        return payload


_HISTORY_SERVICE: Optional[RobinhoodChainHistoryService] = None
_HISTORY_SERVICE_KEY: Optional[Tuple[Any, ...]] = None


def get_robinhood_chain_history_service() -> RobinhoodChainHistoryService:
    global _HISTORY_SERVICE, _HISTORY_SERVICE_KEY
    key = (
        settings.robinhood_chain_effective_explorer_api_base(),
        float(settings.robinhood_chain_history_timeout_s),
        float(settings.robinhood_chain_history_cache_ttl_s),
        float(settings.robinhood_chain_history_error_backoff_s),
        int(settings.robinhood_chain_history_max_pages),
        int(settings.robinhood_chain_history_page_size),
        int(settings.robinhood_chain_max_concurrent),
    )
    if _HISTORY_SERVICE is None or _HISTORY_SERVICE_KEY != key:
        _HISTORY_SERVICE = RobinhoodChainHistoryService(
            api_base=key[0],
            timeout_s=key[1],
            cache_ttl_s=key[2],
            error_backoff_s=key[3],
            max_pages=key[4],
            page_size=key[5],
            max_concurrent=key[6],
        )
        _HISTORY_SERVICE_KEY = key
    return _HISTORY_SERVICE
