from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import re
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional, Tuple

import httpx

from ..config import settings
from .evm_rpc import decode_abi_uint256, get_robinhood_chain_client, validate_evm_address


EXPECTED_CHAIN_ID = 4663
EXPECTED_CHAIN_ID_HEX = hex(EXPECTED_CHAIN_ID)
ZEROX_PROVIDER = "0x"
ZEROX_PRICE_PATH = "/swap/allowance-holder/price"
ZEROX_NATIVE_TOKEN = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
ERC20_DECIMALS_CALL_DATA = "0x313ce567"

# RH-CHAIN.10A is intentionally limited to canonical mainnet discovery assets.
# USDG remains an official candidate until the live probe is accepted and the
# user explicitly saves it through Token Registry.
ROBINHOOD_CHAIN_DISCOVERY_TOKENS: Dict[str, Dict[str, Any]] = {
    "ETH": {
        "symbol": "ETH",
        "contract_address": ZEROX_NATIVE_TOKEN,
        "decimals": 18,
        "native": True,
        "identity_source": "robinhood_chain_native",
    },
    "WETH": {
        "symbol": "WETH",
        "contract_address": "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73",
        "decimals": 18,
        "native": False,
        "identity_source": "robinhood_official_contract_registry",
    },
    "USDG": {
        "symbol": "USDG",
        "contract_address": "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168",
        "decimals": 6,
        "native": False,
        "identity_source": "robinhood_official_contract_registry",
    },
}

ROBINHOOD_CHAIN_ROUTE_CAPABILITIES: Tuple[Dict[str, Any], ...] = (
    {
        "from_asset": "ETH",
        "to_asset": "USDG",
        "amount_mode": "exact_input",
        "display_mode": "exact_spend",
        "provider": ZEROX_PROVIDER,
        "indicative_status": "live_verified",
        "firm_plan_status": "live_verified",
        "execution_status": "live_verified",
        "enabled": True,
        "evidence": "RH-CHAIN.10D.1B live acceptance",
        "reason": None,
    },
    {
        "from_asset": "USDG",
        "to_asset": "ETH",
        "amount_mode": "exact_input",
        "display_mode": "exact_spend",
        "provider": ZEROX_PROVIDER,
        "indicative_status": "live_verified",
        "firm_plan_status": "live_verified",
        "execution_status": "review_only",
        "enabled": True,
        "evidence": "RH-CHAIN.10D.2-R3 live diagnostic",
        "reason": "Quote and unsigned firm-plan review are verified; a generalized live execution lifecycle is not enabled yet.",
    },
    {
        "from_asset": "USDG",
        "to_asset": "WETH",
        "amount_mode": "exact_input",
        "display_mode": "exact_spend",
        "provider": ZEROX_PROVIDER,
        "indicative_status": "live_verified",
        "firm_plan_status": "not_verified",
        "execution_status": "disabled",
        "enabled": True,
        "evidence": "RH-CHAIN.10D.2-R3 live diagnostic",
        "reason": "Read-only discovery is verified; firm planning and execution remain disabled pending a dedicated tranche.",
    },
    {
        "from_asset": "USDG",
        "to_asset": "ETH",
        "amount_mode": "exact_output",
        "display_mode": "exact_receive",
        "provider": ZEROX_PROVIDER,
        "indicative_status": "provider_failure",
        "firm_plan_status": "provider_failure",
        "execution_status": "held",
        "enabled": False,
        "evidence": "RH-CHAIN.10D.2-R3 live diagnostic",
        "reason": "0x returned HTTP 500 for both indicative and firm exact-output native-ETH requests. Direct-router research is required.",
    },
    {
        "from_asset": "USDG",
        "to_asset": "WETH",
        "amount_mode": "exact_output",
        "display_mode": "exact_receive",
        "provider": ZEROX_PROVIDER,
        "indicative_status": "provider_failure",
        "firm_plan_status": "provider_failure",
        "execution_status": "disabled",
        "enabled": False,
        "evidence": "RH-CHAIN.10D.2-R3 live diagnostic",
        "reason": "0x returned HTTP 500 for exact-output WETH discovery. The route is blocked before provider contact.",
    },
)


def robinhood_chain_route_capability(
    sell_symbol: Any,
    buy_symbol: Any,
    amount_mode: Any,
) -> Optional[Dict[str, Any]]:
    sell = str(sell_symbol or "").strip().upper()
    buy = str(buy_symbol or "").strip().upper()
    mode = str(amount_mode or "").strip().lower()
    for item in ROBINHOOD_CHAIN_ROUTE_CAPABILITIES:
        if (
            item["from_asset"] == sell
            and item["to_asset"] == buy
            and item["amount_mode"] == mode
        ):
            return copy.deepcopy(item)
    return None


_DECIMAL_RE = re.compile(r"^(?:0|[1-9]\d*)(?:\.\d+)?$")
_HEX_DATA_RE = re.compile(r"^0x[0-9a-fA-F]*$")
_MAX_ROUTE_FILLS = 25
_MAX_PROVIDER_ERROR_TEXT = 1200
_MAX_PROBE_WETH = Decimal("0.002")
_MAX_PROBE_ETH = Decimal("0.002")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_or_none(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def _safe_int_string(value: Any) -> Optional[str]:
    text = str(value if value is not None else "").strip()
    if not text or not text.isdigit():
        return None
    return str(int(text))


def _format_atomic_units(atomic: str, decimals: int) -> str:
    quantity = int(atomic)
    places = int(decimals)
    if places == 0:
        return str(quantity)
    scale = 10**places
    whole, remainder = divmod(quantity, scale)
    if remainder == 0:
        return str(whole)
    return f"{whole}.{remainder:0{places}d}".rstrip("0")


def _display_amount_to_atomic(value: Any, decimals: int) -> Tuple[str, str]:
    text = str(value if value is not None else "").strip()
    if not text or not _DECIMAL_RE.fullmatch(text):
        raise ValueError("invalid_discovery_amount")
    try:
        amount = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError("invalid_discovery_amount") from exc
    if not amount.is_finite() or amount <= 0:
        raise ValueError("invalid_discovery_amount")

    places = int(decimals)
    exponent = max(0, -amount.as_tuple().exponent)
    if exponent > places:
        raise ValueError("invalid_discovery_amount")
    atomic = int(amount * (Decimal(10) ** places))
    if atomic <= 0:
        raise ValueError("invalid_discovery_amount")
    return str(atomic), _format_atomic_units(str(atomic), places)


def _bounded_error_body(value: Any) -> Any:
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key in ("name", "message", "reason", "code", "validationErrors"):
            if key in value:
                out[key] = value.get(key)
        if out:
            return out
    text = str(value or "").strip()
    return text[:_MAX_PROVIDER_ERROR_TEXT] if text else None


def _safe_address_or_none(value: Any) -> Optional[str]:
    try:
        return validate_evm_address(str(value or "").strip())
    except ValueError:
        return None


def _safe_token_identity(token: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "symbol": str(token.get("symbol") or "").strip().upper(),
        "contract_address": str(token.get("contract_address") or "").strip(),
        "decimals": int(token.get("decimals") or 0),
        "native": bool(token.get("native")),
        "identity_source": token.get("identity_source"),
        "registry_status": token.get("registry_status"),
        "registry_id": token.get("registry_id"),
        "registry_venue": token.get("registry_venue"),
    }


class RobinhoodChainExecutionDiscoveryService:
    """Mainnet-only, read-only 0x execution discovery.

    The service intentionally calls only the indicative AllowanceHolder price
    endpoint. It never returns provider calldata, constructs an EVM transaction,
    writes an order, requests a wallet signature, or broadcasts anything.
    """

    def __init__(
        self,
        *,
        api_base: str,
        timeout_s: float,
        cache_ttl_s: float,
        error_backoff_s: float,
        max_concurrent: int,
        max_sell_usd: float,
        credential_getter: Callable[[], Optional[dict]],
        rpc_client: Any = None,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> None:
        self.api_base = str(api_base or "").strip().rstrip("/")
        self.timeout_s = max(2.0, min(float(timeout_s), 30.0))
        self.cache_ttl_s = max(0.0, min(float(cache_ttl_s), 300.0))
        self.error_backoff_s = max(0.0, min(float(error_backoff_s), 3600.0))
        self.max_concurrent = max(1, min(int(max_concurrent), 4))
        self.max_sell_usd = max(0.01, min(float(max_sell_usd), 25.0))
        self.credential_getter = credential_getter
        self.rpc_client = rpc_client or get_robinhood_chain_client()
        self.transport = transport

        self._cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._cache_lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(self.max_concurrent)
        self._last_good_at: Optional[datetime] = None
        self._last_error: Optional[str] = None
        self._backoff_until_monotonic = 0.0
        self._backoff_until_utc: Optional[datetime] = None

    def _credential(self) -> Optional[dict]:
        try:
            raw = self.credential_getter()
        except Exception:
            return None
        if not isinstance(raw, dict):
            return None
        api_key = str(raw.get("api_key") or "").strip()
        if not api_key:
            return None
        return {
            "api_key": api_key,
            "source": str(raw.get("source") or "profile_vault").strip() or "profile_vault",
            "venue": str(raw.get("venue") or "zerox").strip() or "zerox",
        }

    def status(self) -> Dict[str, Any]:
        credential = self._credential()
        provider = str(settings.robinhood_chain_effective_swap_provider() or "").strip().lower()
        base_configured = bool(self.api_base.startswith("https://"))
        key_configured = credential is not None
        return {
            "ok": True,
            "venue": "robinhood_chain",
            "network": "robinhood_chain",
            "chain_id": EXPECTED_CHAIN_ID,
            "chain_id_hex": EXPECTED_CHAIN_ID_HEX,
            "mainnet_only": True,
            "provider": provider,
            "provider_configured": provider == ZEROX_PROVIDER and base_configured and key_configured,
            "api_key_configured": key_configured,
            "credential_source": credential.get("source") if credential else None,
            "credential_venue": credential.get("venue") if credential else None,
            "api_base": self.api_base if base_configured else None,
            "endpoint": ZEROX_PRICE_PATH,
            "approval_model": "allowance_holder",
            "permit2_supported_by_provider": True,
            "permit2_enabled": False,
            "exact_input_supported": True,
            "exact_output_supported": False,
            "provider_declared_exact_output_supported": True,
            "capability_policy": "live_verified_pair_direction_amount_mode",
            "route_capabilities": copy.deepcopy(list(ROBINHOOD_CHAIN_ROUTE_CAPABILITIES)),
            "timeout_s": self.timeout_s,
            "cache_ttl_s": self.cache_ttl_s,
            "error_backoff_s": self.error_backoff_s,
            "max_concurrent": self.max_concurrent,
            "discovery_max_sell_usd": self.max_sell_usd,
            "last_good_at": iso_or_none(self._last_good_at),
            "last_error": self._last_error,
            "backoff_until": iso_or_none(self._backoff_until_utc),
            "read_only": True,
            "execution_enabled": False,
            "signing_enabled": False,
            "transaction_construction_enabled": False,
            "will_mutate": False,
        }

    async def _cached_result(self, key: str) -> Optional[Dict[str, Any]]:
        if self.cache_ttl_s <= 0:
            return None
        async with self._cache_lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            expires_at, payload = entry
            if time.monotonic() >= expires_at:
                self._cache.pop(key, None)
                return None
            out = copy.deepcopy(payload)
            out["cached"] = True
            return out

    async def _store_cache(self, key: str, payload: Dict[str, Any]) -> None:
        if self.cache_ttl_s <= 0:
            return
        async with self._cache_lock:
            self._cache[key] = (time.monotonic() + self.cache_ttl_s, copy.deepcopy(payload))

    def _set_backoff(self, message: str) -> None:
        self._last_error = str(message or "0x provider transient error")
        self._backoff_until_monotonic = time.monotonic() + self.error_backoff_s
        self._backoff_until_utc = (
            utc_now() + timedelta(seconds=self.error_backoff_s)
            if self.error_backoff_s > 0
            else None
        )

    def _clear_backoff(self) -> None:
        self._last_good_at = utc_now()
        self._last_error = None
        self._backoff_until_monotonic = 0.0
        self._backoff_until_utc = None

    async def _verify_contract(self, token: Dict[str, Any], *, force_refresh: bool) -> Dict[str, Any]:
        identity = _safe_token_identity(token)
        if identity["native"]:
            return {
                "ok": True,
                "symbol": identity["symbol"],
                "contract_address": ZEROX_NATIVE_TOKEN,
                "native": True,
                "code_required": False,
                "code_present": None,
            }

        contract = validate_evm_address(identity["contract_address"])
        record = await self.rpc_client.rpc_read(
            "eth_getCode",
            [contract, "latest"],
            cache_namespace=f"execution_discovery:code:{contract.lower()}",
            force_refresh=force_refresh,
        )
        if not record.get("ok"):
            return {
                "ok": False,
                "symbol": identity["symbol"],
                "contract_address": contract,
                "native": False,
                "code_required": True,
                "code_present": False,
                "error": "contract_code_unavailable",
                "rpc_error": record.get("error"),
            }
        code = str(record.get("result") or "").strip()
        present = bool(_HEX_DATA_RE.fullmatch(code)) and code.lower() not in {"0x", "0x0", "0x00"} and len(code) > 4
        if not present:
            return {
                "ok": False,
                "symbol": identity["symbol"],
                "contract_address": contract,
                "native": False,
                "code_required": True,
                "code_present": False,
                "code_bytes": 0,
                "cached": bool(record.get("cached")),
                "fetched_at": record.get("fetched_at"),
                "error": "contract_code_unavailable",
            }

        decimals_record = await self.rpc_client.rpc_read(
            "eth_call",
            [{"to": contract, "data": ERC20_DECIMALS_CALL_DATA}, "latest"],
            cache_namespace=f"execution_discovery:decimals:{contract.lower()}",
            force_refresh=force_refresh,
        )
        if not decimals_record.get("ok"):
            return {
                "ok": False,
                "symbol": identity["symbol"],
                "contract_address": contract,
                "native": False,
                "code_required": True,
                "code_present": True,
                "code_bytes": max(0, (len(code) - 2) // 2),
                "error": "contract_decimals_unavailable",
                "rpc_error": decimals_record.get("error"),
            }
        try:
            observed_decimals = int(decode_abi_uint256(decimals_record.get("result")))
        except ValueError:
            observed_decimals = -1
        expected_decimals = int(identity["decimals"])
        decimals_match = observed_decimals == expected_decimals
        return {
            "ok": decimals_match,
            "symbol": identity["symbol"],
            "contract_address": contract,
            "native": False,
            "code_required": True,
            "code_present": True,
            "code_bytes": max(0, (len(code) - 2) // 2),
            "expected_decimals": expected_decimals,
            "observed_decimals": observed_decimals if observed_decimals >= 0 else None,
            "decimals_match": decimals_match,
            "cached": bool(record.get("cached")) and bool(decimals_record.get("cached")),
            "fetched_at": decimals_record.get("fetched_at") or record.get("fetched_at"),
            "error": None if decimals_match else "contract_decimals_mismatch",
        }

    def _enforce_pre_request_cap(
        self,
        *,
        amount_mode: str,
        amount_display: str,
        sell_token: Dict[str, Any],
        buy_token: Dict[str, Any],
    ) -> None:
        amount = Decimal(amount_display)
        sell_symbol = str(sell_token.get("symbol") or "").upper()
        buy_symbol = str(buy_token.get("symbol") or "").upper()
        max_usd = Decimal(str(self.max_sell_usd))

        if amount_mode == "exact_input":
            if sell_symbol == "USDG" and amount > max_usd:
                raise ValueError("discovery_amount_exceeds_cap")
            if sell_symbol == "WETH" and amount > _MAX_PROBE_WETH:
                raise ValueError("discovery_amount_exceeds_cap")
            if sell_symbol == "ETH" and amount > _MAX_PROBE_ETH:
                raise ValueError("discovery_amount_exceeds_cap")
        else:
            if buy_symbol == "USDG" and amount > max_usd:
                raise ValueError("discovery_amount_exceeds_cap")
            if buy_symbol == "WETH" and amount > _MAX_PROBE_WETH:
                raise ValueError("discovery_amount_exceeds_cap")
            if buy_symbol == "ETH" and amount > _MAX_PROBE_ETH:
                raise ValueError("discovery_amount_exceeds_cap")

    def _cache_key(self, params: Dict[str, str], sell_token: Dict[str, Any], buy_token: Dict[str, Any]) -> str:
        safe = {
            "endpoint": ZEROX_PRICE_PATH,
            "params": params,
            "sell_symbol": sell_token.get("symbol"),
            "buy_symbol": buy_token.get("symbol"),
        }
        raw = json.dumps(safe, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _normalize_fees(self, raw: Any) -> Dict[str, Any]:
        fees = raw if isinstance(raw, dict) else {}
        out: Dict[str, Any] = {}
        for name in ("integratorFee", "zeroExFee", "gasFee"):
            item = fees.get(name)
            if item is None:
                out[name] = None
                continue
            if not isinstance(item, dict):
                out[name] = {"present": True}
                continue
            out[name] = {
                "amount": _safe_int_string(item.get("amount")),
                "token": _safe_address_or_none(item.get("token")),
                "type": str(item.get("type") or "").strip() or None,
            }
        return out

    def _normalize_route(self, raw: Any) -> Dict[str, Any]:
        route = raw if isinstance(raw, dict) else {}
        fills_out: List[Dict[str, Any]] = []
        fills = route.get("fills") if isinstance(route.get("fills"), list) else []
        for fill in fills[:_MAX_ROUTE_FILLS]:
            if not isinstance(fill, dict):
                continue
            fills_out.append(
                {
                    "source": str(fill.get("source") or "").strip() or "unknown",
                    "proportion_bps": _safe_int_string(fill.get("proportionBps")),
                    "from": _safe_address_or_none(fill.get("from")),
                    "to": _safe_address_or_none(fill.get("to")),
                }
            )
        return {
            "fills": fills_out,
            "fill_count": len(fills_out),
            "token_path": [
                address
                for address in (
                    _safe_address_or_none(item)
                    for item in (route.get("tokens") if isinstance(route.get("tokens"), list) else [])[:12]
                )
                if address
            ],
        }

    def _normalize_issues(self, raw: Any) -> Dict[str, Any]:
        issues = raw if isinstance(raw, dict) else {}
        allowance = issues.get("allowance") if isinstance(issues.get("allowance"), dict) else None
        balance = issues.get("balance") if isinstance(issues.get("balance"), dict) else None
        return {
            "allowance": None
            if allowance is None
            else {
                "required": True,
                "spender": _safe_address_or_none(allowance.get("spender")),
                "actual": _safe_int_string(allowance.get("actual")),
            },
            "balance": None
            if balance is None
            else {
                "insufficient": True,
                "token": _safe_address_or_none(balance.get("token")),
                "actual": _safe_int_string(balance.get("actual")),
                "expected": _safe_int_string(balance.get("expected")),
            },
            "simulation_incomplete": bool(issues.get("simulationIncomplete")),
            "invalid_sources_passed": list(issues.get("invalidSourcesPassed") or [])[:20]
            if isinstance(issues.get("invalidSourcesPassed"), list)
            else [],
        }

    def _normalize_provider_response(
        self,
        body: Dict[str, Any],
        *,
        sell_token: Dict[str, Any],
        buy_token: Dict[str, Any],
        amount_mode: str,
        requested_atomic: str,
        requested_display: str,
        credential_source: str,
        elapsed_ms: float,
    ) -> Dict[str, Any]:
        sell_atomic = _safe_int_string(body.get("sellAmount"))
        buy_atomic = _safe_int_string(body.get("buyAmount"))
        min_buy_atomic = _safe_int_string(body.get("minBuyAmount"))
        max_sell_atomic = _safe_int_string(body.get("maxSellAmount"))
        sell_decimals = int(sell_token["decimals"])
        buy_decimals = int(buy_token["decimals"])

        sell_display = _format_atomic_units(sell_atomic, sell_decimals) if sell_atomic is not None else None
        buy_display = _format_atomic_units(buy_atomic, buy_decimals) if buy_atomic is not None else None
        min_buy_display = _format_atomic_units(min_buy_atomic, buy_decimals) if min_buy_atomic is not None else None
        max_sell_display = _format_atomic_units(max_sell_atomic, sell_decimals) if max_sell_atomic is not None else None

        price = None
        inverse_price = None
        if sell_display is not None and buy_display is not None:
            sell_value = Decimal(sell_display)
            buy_value = Decimal(buy_display)
            if sell_value > 0 and buy_value > 0:
                price = format(buy_value / sell_value, "f").rstrip("0").rstrip(".")
                inverse_price = format(sell_value / buy_value, "f").rstrip("0").rstrip(".")

        issues = self._normalize_issues(body.get("issues"))
        allowance_target = _safe_address_or_none(body.get("allowanceTarget"))
        transaction = body.get("transaction") if isinstance(body.get("transaction"), dict) else {}
        transaction_to = _safe_address_or_none(transaction.get("to"))
        transaction_data = str(transaction.get("data") or "").strip()

        response: Dict[str, Any] = {
            "ok": True,
            "venue": "robinhood_chain",
            "network": "robinhood_chain",
            "chain_id": EXPECTED_CHAIN_ID,
            "chain_id_hex": EXPECTED_CHAIN_ID_HEX,
            "mainnet_only": True,
            "provider": ZEROX_PROVIDER,
            "provider_endpoint": ZEROX_PRICE_PATH,
            "credential_source": credential_source,
            "amount_mode": amount_mode,
            "requested_amount_atomic": requested_atomic,
            "requested_amount": requested_display,
            "sell_token": _safe_token_identity(sell_token),
            "buy_token": _safe_token_identity(buy_token),
            "liquidity_available": bool(body.get("liquidityAvailable")),
            "sell_amount_atomic": sell_atomic,
            "sell_amount": sell_display,
            "buy_amount_atomic": buy_atomic,
            "buy_amount": buy_display,
            "min_buy_amount_atomic": min_buy_atomic,
            "min_buy_amount": min_buy_display,
            "max_sell_amount_atomic": max_sell_atomic,
            "max_sell_amount": max_sell_display,
            "price_buy_per_sell": price,
            "price_sell_per_buy": inverse_price,
            "block_number": _safe_int_string(body.get("blockNumber")),
            "gas": _safe_int_string(body.get("gas") or transaction.get("gas")),
            "gas_price": _safe_int_string(body.get("gasPrice") or transaction.get("gasPrice")),
            "total_network_fee": _safe_int_string(body.get("totalNetworkFee")),
            "fees": self._normalize_fees(body.get("fees")),
            "issues": issues,
            "allowance_required": issues.get("allowance") is not None,
            "allowance_spender": (issues.get("allowance") or {}).get("spender") or allowance_target,
            "allowance_target": allowance_target,
            "transaction_destination": transaction_to,
            "transaction_data_present": bool(transaction_data),
            "transaction_data_bytes": max(0, (len(transaction_data) - 2) // 2)
            if _HEX_DATA_RE.fullmatch(transaction_data)
            else None,
            "transaction_calldata": None,
            "route": self._normalize_route(body.get("route")),
            "provider_warnings": [
                warning
                for warning in (
                    "insufficient_balance" if issues.get("balance") else None,
                    "allowance_required" if issues.get("allowance") else None,
                    "simulation_incomplete" if issues.get("simulation_incomplete") else None,
                )
                if warning
            ],
            "cached": False,
            "elapsed_ms": round(float(elapsed_ms), 1),
            "fetched_at": utc_now().isoformat(),
            "read_only": True,
            "execution_enabled": False,
            "signing_enabled": False,
            "transaction_construction_enabled": False,
            "will_mutate": False,
        }

        # USDG is a dollar-denominated discovery asset. Enforce the configured
        # read-only probe cap against the provider-normalized result as well.
        max_usd = Decimal(str(self.max_sell_usd))
        sell_symbol = str(sell_token.get("symbol") or "").upper()
        buy_symbol = str(buy_token.get("symbol") or "").upper()
        usd_estimate: Optional[Decimal] = None
        if sell_symbol == "USDG" and sell_display is not None:
            usd_estimate = Decimal(sell_display)
        elif buy_symbol == "USDG" and buy_display is not None:
            usd_estimate = Decimal(buy_display)
        response["discovery_value_usd_estimate"] = format(usd_estimate, "f") if usd_estimate is not None else None
        response["discovery_value_cap_usd"] = format(max_usd, "f")
        response["discovery_value_cap_passed"] = usd_estimate is None or usd_estimate <= max_usd
        if usd_estimate is not None and usd_estimate > max_usd:
            response.update(
                {
                    "ok": False,
                    "error": "discovery_amount_exceeds_cap",
                    "liquidity_available": False,
                }
            )
        return response

    async def probe(
        self,
        *,
        sell_token: Dict[str, Any],
        buy_token: Dict[str, Any],
        sell_amount: Optional[str],
        buy_amount: Optional[str],
        taker_address: str,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        provider = str(settings.robinhood_chain_effective_swap_provider() or "").strip().lower()
        credential = self._credential()
        if provider != ZEROX_PROVIDER or not self.api_base.startswith("https://") or credential is None:
            return {
                "ok": False,
                "error": "execution_discovery_not_configured",
                "provider": provider,
                "api_base_configured": self.api_base.startswith("https://"),
                "api_key_configured": credential is not None,
                "read_only": True,
                "will_mutate": False,
            }

        if self._backoff_until_monotonic > time.monotonic():
            return {
                "ok": False,
                "error": "execution_discovery_backoff_active",
                "provider": ZEROX_PROVIDER,
                "backoff_until": iso_or_none(self._backoff_until_utc),
                "read_only": True,
                "will_mutate": False,
            }

        taker = validate_evm_address(taker_address)
        sell_identity = _safe_token_identity(sell_token)
        buy_identity = _safe_token_identity(buy_token)
        sell_symbol = sell_identity["symbol"]
        buy_symbol = buy_identity["symbol"]
        if sell_symbol not in ROBINHOOD_CHAIN_DISCOVERY_TOKENS or buy_symbol not in ROBINHOOD_CHAIN_DISCOVERY_TOKENS:
            return {"ok": False, "error": "unsupported_discovery_pair", "read_only": True, "will_mutate": False}
        if sell_symbol == buy_symbol:
            return {"ok": False, "error": "unsupported_discovery_pair", "read_only": True, "will_mutate": False}

        has_sell = sell_amount is not None and str(sell_amount).strip() != ""
        has_buy = buy_amount is not None and str(buy_amount).strip() != ""
        if not has_sell and not has_buy:
            return {"ok": False, "error": "discovery_amount_mode_required", "read_only": True, "will_mutate": False}
        if has_sell and has_buy:
            return {"ok": False, "error": "discovery_amount_modes_mutually_exclusive", "read_only": True, "will_mutate": False}

        try:
            if has_sell:
                amount_mode = "exact_input"
                requested_atomic, requested_display = _display_amount_to_atomic(sell_amount, sell_identity["decimals"])
            else:
                amount_mode = "exact_output"
                requested_atomic, requested_display = _display_amount_to_atomic(buy_amount, buy_identity["decimals"])
            self._enforce_pre_request_cap(
                amount_mode=amount_mode,
                amount_display=requested_display,
                sell_token=sell_identity,
                buy_token=buy_identity,
            )
        except ValueError as exc:
            return {"ok": False, "error": str(exc), "read_only": True, "will_mutate": False}

        capability = robinhood_chain_route_capability(sell_symbol, buy_symbol, amount_mode)
        if capability is None or capability.get("enabled") is not True:
            return {
                "ok": False,
                "error": "execution_discovery_route_mode_not_live_verified",
                "provider": ZEROX_PROVIDER,
                "sell_symbol": sell_symbol,
                "buy_symbol": buy_symbol,
                "amount_mode": amount_mode,
                "route_capability": capability,
                "provider_contacted": False,
                "backoff_activated": False,
                "read_only": True,
                "will_mutate": False,
            }

        chain = await self.rpc_client.verify_expected_chain(force_refresh=force_refresh)
        if not chain.get("ok"):
            return {
                "ok": False,
                "error": "chain_id_mismatch_or_unavailable",
                "chain": chain,
                "read_only": True,
                "will_mutate": False,
            }

        sell_contract = await self._verify_contract(sell_identity, force_refresh=force_refresh)
        buy_contract = await self._verify_contract(buy_identity, force_refresh=force_refresh)
        contract_checks = {"sell": sell_contract, "buy": buy_contract}
        if not sell_contract.get("ok") or not buy_contract.get("ok"):
            return {
                "ok": False,
                "error": "contract_code_unavailable",
                "chain": chain,
                "contract_checks": contract_checks,
                "read_only": True,
                "will_mutate": False,
            }

        params = {
            "chainId": str(EXPECTED_CHAIN_ID),
            "sellToken": sell_identity["contract_address"],
            "buyToken": buy_identity["contract_address"],
            "taker": taker,
        }
        params["sellAmount" if amount_mode == "exact_input" else "buyAmount"] = requested_atomic
        cache_key = self._cache_key(params, sell_identity, buy_identity)
        if not force_refresh:
            cached = await self._cached_result(cache_key)
            if cached is not None:
                cached["chain"] = chain
                cached["contract_checks"] = contract_checks
                return cached

        url = f"{self.api_base}{ZEROX_PRICE_PATH}"
        started = time.perf_counter()
        async with self._semaphore:
            try:
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(self.timeout_s),
                    headers={
                        "Accept": "application/json",
                        "0x-api-key": credential["api_key"],
                        "0x-version": "v2",
                        "User-Agent": "UTT-Robinhood-Chain-Discovery/1.0",
                    },
                    transport=self.transport,
                ) as client:
                    response = await client.get(url, params=params)
                elapsed_ms = (time.perf_counter() - started) * 1000.0
                try:
                    body = response.json()
                except Exception:
                    body = {"message": response.text[:_MAX_PROVIDER_ERROR_TEXT]}

                if response.status_code == 429 or response.status_code >= 500:
                    self._set_backoff(f"HTTP {response.status_code} from 0x Swap API")
                    return {
                        "ok": False,
                        "error": "execution_discovery_provider_transient_error",
                        "provider": ZEROX_PROVIDER,
                        "http_status": response.status_code,
                        "retry_after": response.headers.get("Retry-After"),
                        "provider_error": _bounded_error_body(body),
                        "backoff_until": iso_or_none(self._backoff_until_utc),
                        "read_only": True,
                        "will_mutate": False,
                    }

                if response.status_code in {401, 403}:
                    self._last_error = f"HTTP {response.status_code} from 0x Swap API"
                    return {
                        "ok": False,
                        "error": "provider_authentication_failed",
                        "provider": ZEROX_PROVIDER,
                        "http_status": response.status_code,
                        "provider_error": _bounded_error_body(body),
                        "read_only": True,
                        "will_mutate": False,
                    }

                if not response.is_success or not isinstance(body, dict):
                    self._last_error = f"HTTP {response.status_code} from 0x Swap API"
                    return {
                        "ok": False,
                        "error": "execution_discovery_provider_error",
                        "provider": ZEROX_PROVIDER,
                        "http_status": response.status_code,
                        "provider_error": _bounded_error_body(body),
                        "read_only": True,
                        "will_mutate": False,
                    }

                returned_sell = str(body.get("sellToken") or "").strip().lower()
                returned_buy = str(body.get("buyToken") or "").strip().lower()
                expected_sell = str(sell_identity["contract_address"] or "").strip().lower()
                expected_buy = str(buy_identity["contract_address"] or "").strip().lower()
                if returned_sell and returned_sell != expected_sell:
                    return {
                        "ok": False,
                        "error": "execution_discovery_provider_identity_mismatch",
                        "field": "sellToken",
                        "read_only": True,
                        "will_mutate": False,
                    }
                if returned_buy and returned_buy != expected_buy:
                    return {
                        "ok": False,
                        "error": "execution_discovery_provider_identity_mismatch",
                        "field": "buyToken",
                        "read_only": True,
                        "will_mutate": False,
                    }

                normalized = self._normalize_provider_response(
                    body,
                    sell_token=sell_identity,
                    buy_token=buy_identity,
                    amount_mode=amount_mode,
                    requested_atomic=requested_atomic,
                    requested_display=requested_display,
                    credential_source=credential["source"],
                    elapsed_ms=elapsed_ms,
                )
                normalized["chain"] = chain
                normalized["contract_checks"] = contract_checks
                self._clear_backoff()
                if normalized.get("ok"):
                    await self._store_cache(cache_key, normalized)
                return normalized

            except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
                self._set_backoff(f"{type(exc).__name__}: {exc}")
                return {
                    "ok": False,
                    "error": "execution_discovery_provider_transient_error",
                    "provider": ZEROX_PROVIDER,
                    "provider_error": type(exc).__name__,
                    "backoff_until": iso_or_none(self._backoff_until_utc),
                    "read_only": True,
                    "will_mutate": False,
                }
            except Exception as exc:
                self._last_error = f"{type(exc).__name__}: {exc}"
                return {
                    "ok": False,
                    "error": "execution_discovery_provider_error",
                    "provider": ZEROX_PROVIDER,
                    "provider_error": type(exc).__name__,
                    "read_only": True,
                    "will_mutate": False,
                }


_SERVICE: Optional[RobinhoodChainExecutionDiscoveryService] = None


def get_robinhood_chain_execution_discovery_service() -> RobinhoodChainExecutionDiscoveryService:
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = RobinhoodChainExecutionDiscoveryService(
            api_base=settings.robinhood_chain_effective_zerox_api_base(),
            timeout_s=float(settings.robinhood_chain_quote_timeout_s),
            cache_ttl_s=float(settings.robinhood_chain_quote_cache_ttl_s),
            error_backoff_s=float(settings.robinhood_chain_quote_error_backoff_s),
            max_concurrent=int(settings.robinhood_chain_quote_max_concurrent),
            max_sell_usd=float(settings.robinhood_chain_discovery_max_sell_usd),
            credential_getter=settings.robinhood_chain_zerox_api_credential,
        )
    return _SERVICE
