from __future__ import annotations

import asyncio
import hashlib
import re
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional, Tuple

import httpx

from ..config import settings
from .evm_rpc import get_robinhood_chain_client, validate_evm_address
from .robinhood_chain_execution_discovery import (
    ROBINHOOD_CHAIN_DISCOVERY_TOKENS,
    ROBINHOOD_CHAIN_ROUTE_CAPABILITIES,
    robinhood_chain_route_capability,
)


EXPECTED_CHAIN_ID = 4663
EXPECTED_CHAIN_ID_HEX = hex(EXPECTED_CHAIN_ID)
ZEROX_PROVIDER = "0x"
ZEROX_FIRM_QUOTE_PATH = "/swap/allowance-holder/quote"
ROBINHOOD_CHAIN_FIRM_QUOTE_SYMBOL = "ETH-USDG"
ROBINHOOD_CHAIN_DEFAULT_SLIPPAGE_BPS = 100
ROBINHOOD_CHAIN_MIN_SLIPPAGE_BPS = 10
ROBINHOOD_CHAIN_MAX_SLIPPAGE_BPS = 300
ROBINHOOD_CHAIN_PLAN_TTL_S = 30
ROBINHOOD_CHAIN_MAX_ETH_INPUT = Decimal("0.002")
ROBINHOOD_CHAIN_MAX_USDG_INPUT = Decimal("5")
ROBINHOOD_CHAIN_EXACT_OUTPUT_BUY_ETH = Decimal("0.001")
ROBINHOOD_CHAIN_EXACT_OUTPUT_BUY_WEI = "1000000000000000"
ROBINHOOD_CHAIN_EXACT_OUTPUT_MAX_USDG = Decimal("2")
ROBINHOOD_CHAIN_EXACT_OUTPUT_MAX_USDG_ATOMIC = "2000000"
ROBINHOOD_CHAIN_MAX_GAS_LIMIT = 2_000_000
ROBINHOOD_CHAIN_MAX_CALLDATA_BYTES = 131_072

# Accepted during RH-CHAIN.10A live discovery. RH-CHAIN.10C deliberately
# fails closed if 0x rotates the Robinhood Chain AllowanceHolder deployment.
# A deployment change requires an explicit code review and allowlist update.
ROBINHOOD_CHAIN_ALLOWANCE_HOLDER_ALLOWLIST = frozenset(
    {"0x0000000000001ff3684f28c67538d4d072c22734"}
)

_DECIMAL_RE = re.compile(r"^(?:0|[1-9]\d*)(?:\.\d+)?$")
_HEX_DATA_RE = re.compile(r"^0x[0-9a-fA-F]*$")
_MAX_PROVIDER_ERROR_TEXT = 1200
_MAX_ROUTE_FILLS = 25


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_or_none(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _safe_int_string(value: Any) -> Optional[str]:
    text = str(value if value is not None else "").strip()
    if not text or not text.isdigit():
        return None
    return str(int(text))


def _safe_decimal(value: Any) -> Optional[Decimal]:
    try:
        number = Decimal(str(value if value is not None else "").strip())
    except (InvalidOperation, ValueError, TypeError):
        return None
    return number if number.is_finite() else None


def _display_amount_to_atomic(value: Any, decimals: int) -> Tuple[str, str]:
    text = str(value if value is not None else "").strip()
    if not text or not _DECIMAL_RE.fullmatch(text):
        raise ValueError("invalid_firm_quote_amount")
    try:
        amount = Decimal(text)
    except InvalidOperation as exc:
        raise ValueError("invalid_firm_quote_amount") from exc
    if not amount.is_finite() or amount <= 0:
        raise ValueError("invalid_firm_quote_amount")
    places = int(decimals)
    exponent = max(0, -amount.as_tuple().exponent)
    if exponent > places:
        raise ValueError("invalid_firm_quote_amount")
    atomic = int(amount * (Decimal(10) ** places))
    if atomic <= 0:
        raise ValueError("invalid_firm_quote_amount")
    return str(atomic), _format_atomic_units(str(atomic), places)


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


def _safe_address_or_none(value: Any) -> Optional[str]:
    try:
        return validate_evm_address(str(value or "").strip())
    except ValueError:
        return None


def _address_key(value: Any) -> str:
    return str(value or "").strip().lower()


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


def _normalize_route(raw: Any) -> Dict[str, Any]:
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
    return {"fills": fills_out, "fill_count": len(fills_out)}


def _normalize_fees(raw: Any) -> Dict[str, Any]:
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


def _network_fee_eth(total_network_fee: Optional[str]) -> Optional[str]:
    if total_network_fee is None or not str(total_network_fee).isdigit():
        return None
    return _decimal_text(Decimal(str(total_network_fee)) / Decimal(10**18))


def _safe_failure(error: str, **context: Any) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "ok": False,
        "error": str(error or "robinhood_chain_firm_plan_failed"),
        "venue": "robinhood_chain",
        "network": "robinhood_chain",
        "chain_id": EXPECTED_CHAIN_ID,
        "chain_id_hex": EXPECTED_CHAIN_ID_HEX,
        "mainnet_only": True,
        "provider": ZEROX_PROVIDER,
        "provider_endpoint": ZEROX_FIRM_QUOTE_PATH,
        "firm_quote": False,
        "unsigned_transaction_plan_present": False,
        "approval_transaction_included": False,
        "signing_enabled": False,
        "broadcast_enabled": False,
        "execution_enabled": False,
        "will_mutate": False,
    }
    for key, value in context.items():
        if key in {"transaction_calldata", "calldata", "data"}:
            continue
        payload[key] = value
    return payload


class RobinhoodChainTransactionPlanningService:
    """Firm 0x quote plus a validated, unsigned, review-only transaction plan.

    This service never asks a wallet to connect, never constructs an approval
    transaction, never signs, never broadcasts, and never writes UTT state.
    Native ETH input is validated by requiring transaction.value to equal the
    exact sell amount; ERC-20 input continues to use a fresh eth_call allowance.
    """

    def __init__(
        self,
        *,
        api_base: str,
        timeout_s: float,
        max_concurrent: int,
        credential_getter: Callable[[], Optional[dict]],
        rpc_client: Any = None,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> None:
        self.api_base = str(api_base or "").strip().rstrip("/")
        self.timeout_s = max(2.0, min(float(timeout_s), 30.0))
        self.max_concurrent = max(1, min(int(max_concurrent), 4))
        self.credential_getter = credential_getter
        self.rpc_client = rpc_client or get_robinhood_chain_client()
        self.transport = transport
        self._semaphore = asyncio.Semaphore(self.max_concurrent)
        self._last_good_at: Optional[datetime] = None
        self._last_error: Optional[str] = None

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
        return {
            "ok": True,
            "venue": "robinhood_chain",
            "network": "robinhood_chain",
            "chain_id": EXPECTED_CHAIN_ID,
            "chain_id_hex": EXPECTED_CHAIN_ID_HEX,
            "mainnet_only": True,
            "provider": provider,
            "provider_endpoint": ZEROX_FIRM_QUOTE_PATH,
            "provider_configured": provider == ZEROX_PROVIDER and self.api_base.startswith("https://") and credential is not None,
            "api_key_configured": credential is not None,
            "credential_source": credential.get("source") if credential else None,
            "supported_symbols": [ROBINHOOD_CHAIN_FIRM_QUOTE_SYMBOL],
            "exact_input_only": True,
            "exact_input_enabled": True,
            "exact_output_enabled": False,
            "provider_declared_exact_output_supported": True,
            "capability_policy": "live_verified_pair_direction_amount_mode",
            "route_capabilities": [dict(item) for item in ROBINHOOD_CHAIN_ROUTE_CAPABILITIES],
            "max_eth_input": _decimal_text(ROBINHOOD_CHAIN_MAX_ETH_INPUT),
            "max_usdg_input": _decimal_text(ROBINHOOD_CHAIN_MAX_USDG_INPUT),
            "default_slippage_bps": ROBINHOOD_CHAIN_DEFAULT_SLIPPAGE_BPS,
            "minimum_slippage_bps": ROBINHOOD_CHAIN_MIN_SLIPPAGE_BPS,
            "maximum_slippage_bps": ROBINHOOD_CHAIN_MAX_SLIPPAGE_BPS,
            "plan_ttl_s": ROBINHOOD_CHAIN_PLAN_TTL_S,
            "allowance_holder_allowlist": sorted(ROBINHOOD_CHAIN_ALLOWANCE_HOLDER_ALLOWLIST),
            "allowance_read_method": "eth_call",
            "allowance_read_scope": "erc20_inputs_only",
            "native_input_supported": True,
            "approval_transaction_enabled": False,
            "signing_enabled": False,
            "broadcast_enabled": False,
            "execution_enabled": False,
            "will_mutate": False,
            "last_good_at": iso_or_none(self._last_good_at),
            "last_error": self._last_error,
        }

    @staticmethod
    def _validate_canonical_token(token: Dict[str, Any], symbol: str) -> Dict[str, Any]:
        expected = ROBINHOOD_CHAIN_DISCOVERY_TOKENS[symbol]
        identity = _safe_token_identity(token)
        if identity["symbol"] != symbol:
            raise ValueError("firm_quote_token_identity_mismatch")
        if int(identity["decimals"]) != int(expected["decimals"]):
            raise ValueError("firm_quote_token_identity_mismatch")
        if bool(identity.get("native")) != bool(expected.get("native")):
            raise ValueError("firm_quote_token_identity_mismatch")
        try:
            actual_contract = validate_evm_address(identity["contract_address"])
            expected_contract = validate_evm_address(str(expected["contract_address"]))
        except ValueError as exc:
            raise ValueError("firm_quote_token_identity_mismatch") from exc
        if actual_contract.lower() != expected_contract.lower():
            raise ValueError("firm_quote_token_identity_mismatch")
        identity["contract_address"] = actual_contract
        return identity

    @staticmethod
    def _normalize_side(value: Any) -> str:
        side = str(value or "").strip().lower()
        if side not in {"buy", "sell"}:
            raise ValueError("invalid_quote_side")
        return side

    @staticmethod
    def _normalize_symbol(value: Any) -> str:
        raw = str(value or "").strip().upper().replace("/", "-").replace("_", "-")
        return "-".join(part for part in raw.split("-") if part)

    def _resolve_trade(
        self,
        *,
        symbol: str,
        side: str,
        quantity: Optional[str],
        total_quote: Optional[str],
        exact_output_quantity: Optional[str],
        maximum_total_quote: Optional[str],
        eth_token: Dict[str, Any],
        usdg_token: Dict[str, Any],
    ) -> Dict[str, Any]:
        normalized_symbol = self._normalize_symbol(symbol)
        if normalized_symbol != ROBINHOOD_CHAIN_FIRM_QUOTE_SYMBOL:
            raise ValueError("unsupported_robinhood_chain_quote_symbol")
        normalized_side = self._normalize_side(side)
        eth = self._validate_canonical_token(eth_token, "ETH")
        usdg = self._validate_canonical_token(usdg_token, "USDG")

        exact_output_requested = (
            normalized_side == "buy"
            and str(exact_output_quantity or "").strip() != ""
        )
        if exact_output_requested:
            if str(total_quote or "").strip() or str(quantity or "").strip():
                raise ValueError("conflicting_firm_quote_amounts")
            buy_atomic, buy_display = _display_amount_to_atomic(exact_output_quantity, eth["decimals"])
            maximum_sell_atomic, maximum_sell_display = _display_amount_to_atomic(
                maximum_total_quote,
                usdg["decimals"],
            )
            if Decimal(buy_display) != ROBINHOOD_CHAIN_EXACT_OUTPUT_BUY_ETH:
                raise ValueError("unsupported_exact_output_buy_quantity")
            if Decimal(maximum_sell_display) != ROBINHOOD_CHAIN_EXACT_OUTPUT_MAX_USDG:
                raise ValueError("unsupported_exact_output_maximum_usdg")
            return {
                "symbol": normalized_symbol,
                "side": normalized_side,
                "amount_mode": "exact_output",
                "sell_token": usdg,
                "buy_token": eth,
                "buy_amount_atomic": buy_atomic,
                "buy_amount": buy_display,
                "maximum_sell_amount_atomic": maximum_sell_atomic,
                "maximum_sell_amount": maximum_sell_display,
            }

        if str(maximum_total_quote or "").strip():
            raise ValueError("maximum_total_quote_requires_exact_output")
        if normalized_side == "sell":
            sell_token, buy_token = eth, usdg
            requested_atomic, requested_display = _display_amount_to_atomic(quantity, eth["decimals"])
            if Decimal(requested_display) > ROBINHOOD_CHAIN_MAX_ETH_INPUT:
                raise ValueError("firm_quote_amount_exceeds_cap")
        else:
            sell_token, buy_token = usdg, eth
            requested_atomic, requested_display = _display_amount_to_atomic(total_quote, usdg["decimals"])
            if Decimal(requested_display) > ROBINHOOD_CHAIN_MAX_USDG_INPUT:
                raise ValueError("firm_quote_amount_exceeds_cap")

        return {
            "symbol": normalized_symbol,
            "side": normalized_side,
            "amount_mode": "exact_input",
            "sell_token": sell_token,
            "buy_token": buy_token,
            "sell_amount_atomic": requested_atomic,
            "sell_amount": requested_display,
            "maximum_sell_amount_atomic": requested_atomic,
            "maximum_sell_amount": requested_display,
        }

    async def firm_quote_plan(
        self,
        *,
        symbol: str,
        side: str,
        quantity: Optional[str],
        total_quote: Optional[str],
        exact_output_quantity: Optional[str] = None,
        maximum_total_quote: Optional[str] = None,
        taker_address: str,
        eth_token: Dict[str, Any],
        usdg_token: Dict[str, Any],
        slippage_bps: int = ROBINHOOD_CHAIN_DEFAULT_SLIPPAGE_BPS,
    ) -> Dict[str, Any]:
        provider = str(settings.robinhood_chain_effective_swap_provider() or "").strip().lower()
        credential = self._credential()
        if provider != ZEROX_PROVIDER or not self.api_base.startswith("https://") or credential is None:
            return _safe_failure("firm_quote_planning_not_configured", provider=provider)

        try:
            taker = validate_evm_address(taker_address)
            slippage = int(slippage_bps)
            if slippage < ROBINHOOD_CHAIN_MIN_SLIPPAGE_BPS or slippage > ROBINHOOD_CHAIN_MAX_SLIPPAGE_BPS:
                raise ValueError("invalid_slippage_bps")
            trade = self._resolve_trade(
                symbol=symbol,
                side=side,
                quantity=quantity,
                total_quote=total_quote,
                exact_output_quantity=exact_output_quantity,
                maximum_total_quote=maximum_total_quote,
                eth_token=eth_token,
                usdg_token=usdg_token,
            )
        except (ValueError, TypeError) as exc:
            return _safe_failure(str(exc))

        capability = robinhood_chain_route_capability(
            trade["sell_token"]["symbol"],
            trade["buy_token"]["symbol"],
            trade["amount_mode"],
        )
        if capability is None or capability.get("enabled") is not True:
            return _safe_failure(
                "firm_quote_route_mode_not_live_verified",
                amount_mode=trade["amount_mode"],
                display_mode="exact_receive" if trade["amount_mode"] == "exact_output" else "exact_spend",
                input_asset=trade["sell_token"]["symbol"],
                output_asset=trade["buy_token"]["symbol"],
                route_capability=capability,
                provider_contacted=False,
            )

        chain = await self.rpc_client.verify_expected_chain(force_refresh=True)
        if not chain.get("ok"):
            return _safe_failure("chain_id_mismatch_or_unavailable", chain=chain)

        for key in ("sell_token", "buy_token"):
            token = trade[key]
            if bool(token.get("native")):
                continue
            code_record = await self.rpc_client.rpc_read(
                "eth_getCode",
                [token["contract_address"], "latest"],
                cache_namespace=f"firm_plan_contract:{token['contract_address'].lower()}",
                force_refresh=False,
            )
            code = str(code_record.get("result") or "").strip().lower()
            if not code_record.get("ok") or code in {"", "0x", "0x0"}:
                return _safe_failure("contract_code_unavailable", token=token, rpc=code_record)

        params = {
            "chainId": str(EXPECTED_CHAIN_ID),
            "sellToken": trade["sell_token"]["contract_address"],
            "buyToken": trade["buy_token"]["contract_address"],
            "taker": taker,
            "slippageBps": str(slippage),
        }
        if trade["amount_mode"] == "exact_output":
            params["buyAmount"] = trade["buy_amount_atomic"]
        else:
            params["sellAmount"] = trade["sell_amount_atomic"]
        url = f"{self.api_base}{ZEROX_FIRM_QUOTE_PATH}"
        started = time.perf_counter()
        async with self._semaphore:
            try:
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(self.timeout_s),
                    headers={
                        "Accept": "application/json",
                        "0x-api-key": credential["api_key"],
                        "0x-version": "v2",
                        "User-Agent": "UTT-Robinhood-Chain-Firm-Plan/1.0",
                    },
                    transport=self.transport,
                ) as client:
                    response = await client.get(url, params=params)
                elapsed_ms = (time.perf_counter() - started) * 1000.0
                try:
                    body = response.json()
                except Exception:
                    body = {"message": response.text[:_MAX_PROVIDER_ERROR_TEXT]}
            except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
                self._last_error = f"{type(exc).__name__}: {exc}"
                return _safe_failure("firm_quote_provider_transient_error", provider_error=type(exc).__name__)
            except Exception as exc:
                self._last_error = f"{type(exc).__name__}: {exc}"
                return _safe_failure("firm_quote_provider_error", provider_error=type(exc).__name__)

        if response.status_code == 429 or response.status_code >= 500:
            self._last_error = f"HTTP {response.status_code} from 0x Swap API"
            return _safe_failure(
                "firm_quote_provider_transient_error",
                http_status=response.status_code,
                retry_after=response.headers.get("Retry-After"),
                provider_error=_bounded_error_body(body),
            )
        if response.status_code in {401, 403}:
            self._last_error = f"HTTP {response.status_code} from 0x Swap API"
            return _safe_failure(
                "provider_authentication_failed",
                http_status=response.status_code,
                provider_error=_bounded_error_body(body),
            )
        if not response.is_success or not isinstance(body, dict):
            self._last_error = f"HTTP {response.status_code} from 0x Swap API"
            return _safe_failure(
                "firm_quote_provider_error",
                http_status=response.status_code,
                provider_error=_bounded_error_body(body),
            )

        expected_sell = _address_key(trade["sell_token"]["contract_address"])
        expected_buy = _address_key(trade["buy_token"]["contract_address"])
        returned_sell = _address_key(body.get("sellToken"))
        returned_buy = _address_key(body.get("buyToken"))
        if returned_sell != expected_sell or returned_buy != expected_buy:
            return _safe_failure("firm_quote_provider_identity_mismatch")
        if body.get("liquidityAvailable") is not True:
            return _safe_failure("firm_quote_liquidity_unavailable")

        sell_atomic = _safe_int_string(body.get("sellAmount"))
        buy_atomic = _safe_int_string(body.get("buyAmount"))
        min_buy_atomic = _safe_int_string(body.get("minBuyAmount")) or buy_atomic
        if sell_atomic is None or buy_atomic is None or min_buy_atomic is None:
            return _safe_failure("invalid_firm_quote_amounts")
        if int(sell_atomic) <= 0 or int(buy_atomic) <= 0 or int(min_buy_atomic) <= 0:
            return _safe_failure("invalid_firm_quote_amounts")

        if trade["amount_mode"] == "exact_output":
            if buy_atomic != trade["buy_amount_atomic"]:
                return _safe_failure("exact_output_firm_quote_amount_mismatch")
            if int(sell_atomic) > int(trade["maximum_sell_amount_atomic"]):
                return _safe_failure(
                    "exact_output_firm_quote_exceeds_maximum_usdg",
                    required_input_atomic=sell_atomic,
                    maximum_input_ceiling_atomic=trade["maximum_sell_amount_atomic"],
                )
            observed_protection_bps = Decimal("0")
        else:
            if sell_atomic != trade["sell_amount_atomic"] or int(min_buy_atomic) > int(buy_atomic):
                return _safe_failure("invalid_firm_quote_amounts")
            observed_protection_bps = (
                (Decimal(int(buy_atomic) - int(min_buy_atomic)) / Decimal(int(buy_atomic))) * Decimal(10000)
            )
            if observed_protection_bps > Decimal(slippage + 10):
                return _safe_failure(
                    "firm_quote_slippage_protection_mismatch",
                    requested_slippage_bps=slippage,
                    observed_protection_bps=_decimal_text(observed_protection_bps),
                )

        issues = body.get("issues") if isinstance(body.get("issues"), dict) else {}
        allowance_issue = issues.get("allowance") if isinstance(issues.get("allowance"), dict) else None
        allowance_target = _safe_address_or_none(body.get("allowanceTarget"))
        issue_spender = _safe_address_or_none(allowance_issue.get("spender")) if allowance_issue else None
        spender = issue_spender or allowance_target
        transaction = body.get("transaction") if isinstance(body.get("transaction"), dict) else {}
        transaction_to = _safe_address_or_none(transaction.get("to"))
        calldata = str(transaction.get("data") or "").strip()
        value_wei = _safe_int_string(transaction.get("value"))
        gas_limit = _safe_int_string(transaction.get("gas") or body.get("gas"))
        gas_price = _safe_int_string(transaction.get("gasPrice") or body.get("gasPrice"))

        sell_is_native = bool(trade["sell_token"].get("native"))
        allowed = ROBINHOOD_CHAIN_ALLOWANCE_HOLDER_ALLOWLIST
        if transaction_to is None:
            return _safe_failure("firm_quote_missing_verified_addresses")
        if _address_key(transaction_to) not in allowed:
            return _safe_failure(
                "firm_quote_destination_not_allowlisted",
                transaction_destination=transaction_to,
                allowance_target=allowance_target,
            )
        if allowance_target is not None and _address_key(allowance_target) not in allowed:
            return _safe_failure(
                "firm_quote_allowance_spender_not_allowlisted",
                allowance_target=allowance_target,
                allowance_spender=spender,
            )
        if not sell_is_native:
            if allowance_target is None or spender is None:
                return _safe_failure("firm_quote_missing_verified_addresses")
            if _address_key(spender) not in allowed:
                return _safe_failure(
                    "firm_quote_allowance_spender_not_allowlisted",
                    allowance_target=allowance_target,
                    allowance_spender=spender,
                )
            if _address_key(transaction_to) != _address_key(allowance_target):
                return _safe_failure(
                    "firm_quote_destination_not_allowlisted",
                    transaction_destination=transaction_to,
                    allowance_target=allowance_target,
                )
        elif allowance_issue is not None:
            return _safe_failure("firm_quote_unexpected_native_allowance_issue")
        if not _HEX_DATA_RE.fullmatch(calldata) or calldata == "0x" or len(calldata[2:]) % 2 != 0:
            return _safe_failure("firm_quote_invalid_calldata")
        calldata_bytes = len(calldata[2:]) // 2
        if calldata_bytes <= 0 or calldata_bytes > ROBINHOOD_CHAIN_MAX_CALLDATA_BYTES:
            return _safe_failure("firm_quote_calldata_exceeds_cap", calldata_bytes=calldata_bytes)
        expected_value_wei = int(sell_atomic) if sell_is_native else 0
        if value_wei is None or int(value_wei) != expected_value_wei:
            return _safe_failure(
                "firm_quote_transaction_value_mismatch",
                transaction_value_wei=value_wei,
                expected_transaction_value_wei=str(expected_value_wei),
                native_input=sell_is_native,
            )
        if gas_limit is None or int(gas_limit) <= 0 or int(gas_limit) > ROBINHOOD_CHAIN_MAX_GAS_LIMIT:
            return _safe_failure("firm_quote_gas_limit_exceeds_cap", gas_limit=gas_limit)

        warnings: List[str] = []
        if sell_is_native:
            current_allowance_atomic = 0
            required_allowance_atomic = 0
            shortfall_atomic = 0
            approval_required = False
            allowance_read_method = "not_applicable_native_input"
            allowance_fetched_at = None
            allowance_cached = False
            allowance_spender = None
        else:
            allowance = await self.rpc_client.get_erc20_allowance(
                owner_address=taker,
                contract_address=trade["sell_token"]["contract_address"],
                spender_address=spender,
                decimals=int(trade["sell_token"]["decimals"]),
                force_refresh=True,
            )
            if not allowance.get("ok"):
                return _safe_failure("allowance_read_failed", allowance_read=allowance)

            current_allowance_atomic = int(str(allowance.get("allowance_atomic") or "0"))
            required_allowance_atomic = (
                int(trade["maximum_sell_amount_atomic"])
                if trade["amount_mode"] == "exact_output"
                else int(sell_atomic)
            )
            shortfall_atomic = max(0, required_allowance_atomic - current_allowance_atomic)
            approval_required = shortfall_atomic > 0
            allowance_read_method = "eth_call"
            allowance_fetched_at = allowance.get("fetched_at")
            allowance_cached = bool(allowance.get("cached"))
            allowance_spender = spender
            provider_actual_atomic = _safe_int_string(allowance_issue.get("actual")) if allowance_issue else None
            if provider_actual_atomic is not None and int(provider_actual_atomic) != current_allowance_atomic:
                warnings.append("provider_allowance_actual_differs_from_fresh_rpc_read")
            if bool(allowance_issue) != approval_required:
                warnings.append("provider_allowance_issue_differs_from_fresh_rpc_read")
        if isinstance(issues.get("balance"), dict):
            warnings.append("insufficient_sell_token_balance")
        if bool(issues.get("simulationIncomplete")):
            warnings.append("simulation_incomplete")

        sell_decimals = int(trade["sell_token"]["decimals"])
        buy_decimals = int(trade["buy_token"]["decimals"])
        sell_display = _format_atomic_units(sell_atomic, sell_decimals)
        buy_display = _format_atomic_units(buy_atomic, buy_decimals)
        min_buy_display = _format_atomic_units(min_buy_atomic, buy_decimals)
        current_allowance_display = _format_atomic_units(str(current_allowance_atomic), sell_decimals)
        required_allowance_display = _format_atomic_units(str(required_allowance_atomic), sell_decimals)
        shortfall_display = _format_atomic_units(str(shortfall_atomic), sell_decimals)

        if trade["side"] == "sell":
            effective_price = Decimal(buy_display) / Decimal(sell_display)
        else:
            effective_price = Decimal(sell_display) / Decimal(buy_display)

        total_network_fee = _safe_int_string(body.get("totalNetworkFee"))
        if total_network_fee is None and gas_price is not None:
            total_network_fee = str(int(gas_limit) * int(gas_price))

        normalized_fees = _normalize_fees(body.get("fees"))
        zero_x_fee = normalized_fees.get("zeroExFee") if isinstance(normalized_fees.get("zeroExFee"), dict) else None
        zero_x_fee_display = None
        if zero_x_fee and zero_x_fee.get("amount") is not None:
            fee_token_key = _address_key(zero_x_fee.get("token"))
            for candidate in (trade["sell_token"], trade["buy_token"]):
                if fee_token_key == _address_key(candidate.get("contract_address")):
                    zero_x_fee_display = {
                        **zero_x_fee,
                        "asset": candidate.get("symbol"),
                        "amount_display": _format_atomic_units(
                            str(zero_x_fee["amount"]),
                            int(candidate["decimals"]),
                        ),
                    }
                    break
            if zero_x_fee_display is None:
                zero_x_fee_display = {**zero_x_fee, "asset": None, "amount_display": None}

        fetched_at = utc_now()
        expires_at = fetched_at + timedelta(seconds=ROBINHOOD_CHAIN_PLAN_TTL_S)
        quote_id_material = "|".join(
            [
                str(EXPECTED_CHAIN_ID),
                taker.lower(),
                expected_sell,
                expected_buy,
                sell_atomic,
                buy_atomic,
                transaction_to.lower(),
                hashlib.sha256(bytes.fromhex(calldata[2:])).hexdigest(),
                fetched_at.isoformat(),
            ]
        )
        quote_id = hashlib.sha256(quote_id_material.encode("utf-8")).hexdigest()
        route = _normalize_route(body.get("route"))
        route_sources = []
        for fill in route.get("fills") or []:
            source = str(fill.get("source") or "").strip()
            if source and source not in route_sources:
                route_sources.append(source)

        plan_status = "approval_required" if approval_required else "ready_for_wallet_review"
        payload: Dict[str, Any] = {
            "ok": True,
            "venue": "robinhood_chain",
            "network": "robinhood_chain",
            "chain_id": EXPECTED_CHAIN_ID,
            "chain_id_hex": EXPECTED_CHAIN_ID_HEX,
            "mainnet_only": True,
            "provider": ZEROX_PROVIDER,
            "provider_endpoint": ZEROX_FIRM_QUOTE_PATH,
            "credential_source": credential["source"],
            "symbol": trade["symbol"],
            "side": trade["side"],
            "amount_mode": trade["amount_mode"],
            "input_asset": trade["sell_token"]["symbol"],
            "input_amount": sell_display,
            "input_amount_atomic": sell_atomic,
            "output_asset": trade["buy_token"]["symbol"],
            "output_amount": buy_display,
            "output_amount_atomic": buy_atomic,
            "minimum_received": min_buy_display,
            "minimum_received_atomic": min_buy_atomic,
            "minimum_received_asset": trade["buy_token"]["symbol"],
            "maximum_spent": (
                trade["maximum_sell_amount"]
                if trade["amount_mode"] == "exact_output"
                else sell_display
            ),
            "maximum_spent_atomic": (
                trade["maximum_sell_amount_atomic"]
                if trade["amount_mode"] == "exact_output"
                else sell_atomic
            ),
            "maximum_spent_asset": trade["sell_token"]["symbol"],
            "maximum_input_ceiling": trade["maximum_sell_amount"],
            "maximum_input_ceiling_atomic": trade["maximum_sell_amount_atomic"],
            "maximum_input_ceiling_asset": trade["sell_token"]["symbol"],
            "effective_price": _decimal_text(effective_price),
            "slippage_bps": slippage,
            "observed_minimum_received_protection_bps": _decimal_text(observed_protection_bps),
            "liquidity_available": True,
            "block_number": _safe_int_string(body.get("blockNumber")),
            "route": route,
            "route_sources": route_sources,
            "fees": normalized_fees,
            "zero_x_fee": zero_x_fee_display,
            "total_network_fee_wei": total_network_fee,
            "total_network_fee_eth": _network_fee_eth(total_network_fee),
            "allowance": {
                "applicable": not sell_is_native,
                "read_method": allowance_read_method,
                "owner": taker,
                "token": trade["sell_token"],
                "spender": allowance_spender,
                "spender_allowlisted": True if allowance_spender else None,
                "current_atomic": str(current_allowance_atomic),
                "current": current_allowance_display,
                "required_atomic": str(required_allowance_atomic),
                "required": required_allowance_display,
                "shortfall_atomic": str(shortfall_atomic),
                "shortfall": shortfall_display,
                "approval_required": approval_required,
                "approval_transaction_included": False,
                "fetched_at": allowance_fetched_at,
                "cached": allowance_cached,
            },
            "approval_required": approval_required,
            "approval_transaction_included": False,
            "unsigned_transaction_plan": {
                "status": plan_status,
                "review_only": True,
                "source": "0x_firm_quote",
                "chain_id": EXPECTED_CHAIN_ID,
                "chain_id_hex": EXPECTED_CHAIN_ID_HEX,
                "from": taker,
                "to": transaction_to,
                "destination_allowlisted": True,
                "destination_allowlist": sorted(allowed),
                "value_wei": value_wei,
                "value_eth": _format_atomic_units(value_wei, 18),
                "expected_value_wei": str(expected_value_wei),
                "native_input": sell_is_native,
                "gas_limit": gas_limit,
                "gas_price_wei": gas_price,
                "calldata": calldata,
                "calldata_bytes": calldata_bytes,
                "calldata_sha256": hashlib.sha256(bytes.fromhex(calldata[2:])).hexdigest(),
                "wallet_connection_requested": False,
                "signing_requested": False,
                "broadcast_requested": False,
            },
            "quote_id": quote_id,
            "fetched_at": fetched_at.isoformat(),
            "plan_ttl_s": ROBINHOOD_CHAIN_PLAN_TTL_S,
            "plan_expires_at": expires_at.isoformat(),
            "expiration_source": "utt_local_safety_window",
            "elapsed_ms": round(float(elapsed_ms), 1),
            "warnings": warnings,
            "firm_quote": True,
            "unsigned_transaction_plan_present": True,
            "unsigned_transaction_planning_enabled": True,
            "transaction_construction_enabled": False,
            "wallet_connection_requested": False,
            "signing_enabled": False,
            "broadcast_enabled": False,
            "execution_enabled": False,
            "order_recording_enabled": False,
            "read_only": True,
            "review_only": True,
            "will_mutate": False,
        }
        self._last_good_at = fetched_at
        self._last_error = None
        return payload


_SERVICE: Optional[RobinhoodChainTransactionPlanningService] = None


def get_robinhood_chain_transaction_planning_service() -> RobinhoodChainTransactionPlanningService:
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = RobinhoodChainTransactionPlanningService(
            api_base=settings.robinhood_chain_effective_zerox_api_base(),
            timeout_s=float(settings.robinhood_chain_quote_timeout_s),
            max_concurrent=int(settings.robinhood_chain_quote_max_concurrent),
            credential_getter=settings.robinhood_chain_zerox_api_credential,
        )
    return _SERVICE
