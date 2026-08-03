from __future__ import annotations

import asyncio
import copy
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import httpx

from ..config import settings
from .evm_rpc import validate_evm_address


UNISWAP_PROVIDER = "uniswap_api"
UNISWAP_QUOTE_PATH = "/quote"
UNISWAP_CHAIN_ID = 4663
UNISWAP_ROUTER_VERSION = "2.1.1"
UNISWAP_AMM_PROTOCOLS = ("V2", "V3", "V4")
UNISWAP_NATIVE_TOKEN = "0x0000000000000000000000000000000000000000"
_ALLOWED_ROUTING = frozenset({"CLASSIC", "WRAP", "UNWRAP"})
_MAX_PROVIDER_ERROR_TEXT = 1200
_MAX_UINT256 = (1 << 256) - 1


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _decimal(value: Any, *, field: str) -> Decimal:
    text = str(value if value is not None else "").strip()
    try:
        number = Decimal(text)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid_{field}") from exc
    if not number.is_finite() or number <= 0:
        raise ValueError(f"invalid_{field}")
    return number


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _atomic_to_display(value: Any, decimals: int, *, field: str) -> Tuple[str, str]:
    text = str(value if value is not None else "").strip()
    if not text.isdigit():
        raise ValueError(f"invalid_{field}")
    atomic = int(text)
    if atomic < 0 or atomic > _MAX_UINT256:
        raise ValueError(f"invalid_{field}")
    display = Decimal(atomic) / (Decimal(10) ** int(decimals))
    return str(atomic), _decimal_text(display)


def _display_to_atomic(value: Any, decimals: int) -> Tuple[str, str]:
    number = _decimal(value, field="requested_amount")
    places = int(decimals)
    if places < 0 or places > 18:
        raise ValueError("invalid_token_decimals")
    exponent = max(0, -number.as_tuple().exponent)
    if exponent > places:
        raise ValueError("requested_amount_exceeds_token_precision")
    atomic = int(number * (Decimal(10) ** places))
    if atomic <= 0 or atomic > _MAX_UINT256:
        raise ValueError("invalid_requested_amount")
    return str(atomic), _decimal_text(Decimal(atomic) / (Decimal(10) ** places))


def _safe_provider_error(value: Any) -> Any:
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key in ("errorCode", "name", "message", "reason", "code", "detail"):
            if key in value and value.get(key) is not None:
                out[key] = value.get(key)
        validation = value.get("validationErrors")
        if isinstance(validation, list):
            out["validationErrors"] = copy.deepcopy(validation[:8])
        if out:
            return out
    text = str(value or "").strip()
    return text[:_MAX_PROVIDER_ERROR_TEXT] if text else None


def _normalize_token(token: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(token, dict):
        raise ValueError("invalid_uniswap_registry_identity")
    symbol = str(token.get("symbol") or "").strip().upper()
    registry_id = token.get("registry_id")
    identity_source = str(token.get("identity_source") or "").strip().lower()
    native = bool(token.get("native"))
    try:
        decimals = int(token.get("decimals"))
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid_uniswap_registry_identity") from exc
    if not symbol or registry_id is None or identity_source != "token_registry":
        raise ValueError("invalid_uniswap_registry_identity")
    if decimals < 0 or decimals > 18:
        raise ValueError("invalid_uniswap_registry_identity")
    if native:
        provider_address = UNISWAP_NATIVE_TOKEN
        registry_address = str(token.get("contract_address") or "").strip() or None
    else:
        try:
            provider_address = validate_evm_address(str(token.get("contract_address") or "").strip())
        except ValueError as exc:
            raise ValueError("invalid_uniswap_registry_identity") from exc
        registry_address = provider_address
    return {
        "symbol": symbol,
        "registry_id": int(registry_id),
        "decimals": decimals,
        "native": native,
        "contract_address": registry_address,
        "provider_address": provider_address,
        "identity_source": "token_registry",
        "registry_venue": token.get("registry_venue"),
        "registry_status": token.get("registry_status"),
    }


def _route_protocols(value: Any) -> List[str]:
    found: List[str] = []

    def visit(item: Any) -> None:
        if len(found) >= 12:
            return
        if isinstance(item, dict):
            for key, nested in item.items():
                key_text = str(key or "").strip().lower()
                if key_text in {"protocol", "protocolversion", "type", "version"}:
                    text = str(nested or "").strip().upper().replace("-", "_")
                    for protocol in UNISWAP_AMM_PROTOCOLS:
                        if protocol in text and protocol not in found:
                            found.append(protocol)
                visit(nested)
        elif isinstance(item, (list, tuple)):
            for nested in item:
                visit(nested)

    visit(value)
    return found


def _prohibited_artifact(body: Dict[str, Any]) -> Optional[str]:
    if body.get("permitData") is not None:
        return "permitData"
    if body.get("permitTransaction") is not None:
        return "permitTransaction"
    quote = body.get("quote") if isinstance(body.get("quote"), dict) else {}
    for key in (
        "encodedOrder",
        "orderInfo",
        "orderId",
        "transaction",
        "swap",
        "calldata",
        "methodParameters",
        "permitData",
        "permitTransaction",
    ):
        if quote.get(key) is not None:
            return f"quote.{key}"
    return None


def _failure(error: str, **extra: Any) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "ok": False,
        "error": str(error or "uniswap_quote_failed"),
        "provider": UNISWAP_PROVIDER,
        "chain_id": UNISWAP_CHAIN_ID,
        "read_only": True,
        "quote_only": True,
        "provider_contacted": False,
        "response_sanitized": True,
        "database_mutation": False,
        "will_mutate": False,
        "execution_authority": False,
        "execution_enabled": False,
        "wallet_connection_requested": False,
        "signing_enabled": False,
        "broadcast_enabled": False,
        "swap_endpoint_enabled": False,
        "order_endpoint_enabled": False,
        "transaction": None,
        "transaction_calldata": None,
        "permit_data": None,
        "permit_transaction": None,
        "encoded_order": None,
    }
    payload.update(extra)
    return payload


class RobinhoodChainUniswapQuoteService:
    """Explicit, backend-only Uniswap /quote canary for Robinhood Chain.

    The service performs one AMM-only exact-input quote after explicit operator
    confirmation. It never calls /swap or /order, writes database state, exposes
    raw provider payloads, constructs a transaction, or requests a signature.
    """

    def __init__(
        self,
        *,
        api_base: str,
        timeout_s: float,
        max_concurrent: int,
        credential_getter: Callable[[], Optional[dict]],
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> None:
        self.api_base = str(api_base or "").strip().rstrip("/")
        self.timeout_s = max(2.0, min(float(timeout_s), 30.0))
        self.max_concurrent = max(1, min(int(max_concurrent), 4))
        self.credential_getter = credential_getter
        self.transport = transport
        self._semaphore = asyncio.Semaphore(self.max_concurrent)
        self._last_good_at: Optional[str] = None
        self._last_error: Optional[str] = None

    def _credential_record(self) -> Optional[Dict[str, Any]]:
        try:
            raw = self.credential_getter()
        except Exception:
            return None
        return dict(raw) if isinstance(raw, dict) else None

    @staticmethod
    def _credential_usable(record: Optional[Dict[str, Any]]) -> bool:
        if not isinstance(record, dict):
            return False
        return bool(
            str(record.get("api_key") or "").strip()
            and bool(record.get("api_key_configured"))
            and bool(record.get("declared_read_only"))
            and not bool(record.get("dangerous_scope_present"))
            and str(record.get("venue") or "").strip().lower() == UNISWAP_PROVIDER
            and int(record.get("key_version") or 0) == 1
        )

    def status(self) -> Dict[str, Any]:
        credential = self._credential_record()
        base_configured = self.api_base.startswith("https://")
        usable = self._credential_usable(credential)
        return {
            "ok": True,
            "provider": UNISWAP_PROVIDER,
            "venue": "robinhood_chain",
            "network": "robinhood_chain",
            "chain_id": UNISWAP_CHAIN_ID,
            "mainnet_only": True,
            "api_base_configured": base_configured,
            "api_key_configured": bool((credential or {}).get("api_key_configured")),
            "credential_source": (credential or {}).get("source"),
            "credential_venue": (credential or {}).get("venue"),
            "credential_key_version": (credential or {}).get("key_version"),
            "declared_read_only": bool((credential or {}).get("declared_read_only")),
            "dangerous_scope_present": bool((credential or {}).get("dangerous_scope_present")),
            "scope_source": (credential or {}).get("scope_source"),
            "quote_endpoint_enabled": bool(base_configured and usable),
            "quote_path": UNISWAP_QUOTE_PATH,
            "quote_type": "EXACT_INPUT",
            "routing_preference": "BEST_PRICE",
            "protocols": list(UNISWAP_AMM_PROTOCOLS),
            "universal_router_version": UNISWAP_ROUTER_VERSION,
            "permit2_disabled": True,
            "permit_amount": "EXACT",
            "generate_permit_as_transaction": False,
            "swap_endpoint_enabled": False,
            "order_endpoint_enabled": False,
            "database_persistence_enabled": False,
            "execution_authority": False,
            "wallet_connection_requested": False,
            "signing_enabled": False,
            "broadcast_enabled": False,
            "last_good_at": self._last_good_at,
            "last_error": self._last_error,
            "read_only": True,
        }

    async def quote(
        self,
        *,
        symbol: str,
        side: str,
        amount_mode: str,
        requested_amount: str,
        slippage_bps: int,
        swapper_address: str,
        input_token: Dict[str, Any],
        output_token: Dict[str, Any],
        confirm_quote: bool,
    ) -> Dict[str, Any]:
        normalized_symbol = str(symbol or "").strip().upper().replace("/", "-").replace("_", "-")
        normalized_side = str(side or "").strip().lower()
        normalized_mode = str(amount_mode or "").strip().lower()
        if not confirm_quote:
            return _failure("uniswap_quote_confirmation_required", symbol=normalized_symbol)
        if normalized_side not in {"buy", "sell"}:
            return _failure("invalid_quote_side", symbol=normalized_symbol)
        if normalized_mode != "exact_input":
            return _failure("uniswap_quote_exact_input_only", symbol=normalized_symbol)
        try:
            slippage = int(slippage_bps)
        except (TypeError, ValueError):
            return _failure("invalid_slippage_bps", symbol=normalized_symbol)
        if slippage < 10 or slippage > 300:
            return _failure("invalid_slippage_bps", symbol=normalized_symbol)

        credential = self._credential_record()
        if not self._credential_usable(credential):
            return _failure(
                "uniswap_quote_not_configured",
                symbol=normalized_symbol,
                api_key_configured=bool((credential or {}).get("api_key_configured")),
                declared_read_only=bool((credential or {}).get("declared_read_only")),
                dangerous_scope_present=bool((credential or {}).get("dangerous_scope_present")),
            )
        if not self.api_base.startswith("https://"):
            return _failure("uniswap_quote_api_base_invalid", symbol=normalized_symbol)

        try:
            swapper = validate_evm_address(str(swapper_address or "").strip())
            input_identity = _normalize_token(input_token)
            output_identity = _normalize_token(output_token)
            if input_identity["registry_id"] == output_identity["registry_id"]:
                raise ValueError("uniswap_quote_same_asset")
            input_atomic, normalized_requested = _display_to_atomic(
                requested_amount,
                int(input_identity["decimals"]),
            )
        except ValueError as exc:
            return _failure(str(exc), symbol=normalized_symbol)

        request_body = {
            "type": "EXACT_INPUT",
            "amount": input_atomic,
            "tokenInChainId": UNISWAP_CHAIN_ID,
            "tokenOutChainId": UNISWAP_CHAIN_ID,
            "tokenIn": input_identity["provider_address"],
            "tokenOut": output_identity["provider_address"],
            "swapper": swapper,
            "slippageTolerance": float(Decimal(slippage) / Decimal(100)),
            "routingPreference": "BEST_PRICE",
            "protocols": list(UNISWAP_AMM_PROTOCOLS),
            "permitAmount": "EXACT",
            "generatePermitAsTransaction": False,
        }
        url = f"{self.api_base}{UNISWAP_QUOTE_PATH}"
        started = time.perf_counter()
        async with self._semaphore:
            try:
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(self.timeout_s),
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                        "x-api-key": str(credential.get("api_key") or ""),
                        "x-universal-router-version": UNISWAP_ROUTER_VERSION,
                        "x-permit2-disabled": "true",
                        "x-erc20eth-enabled": "false",
                        "User-Agent": "UTT-Robinhood-Chain-Uniswap-Quote/1.0",
                    },
                    transport=self.transport,
                ) as client:
                    response = await client.post(url, json=request_body)
                elapsed_ms = (time.perf_counter() - started) * 1000.0
                try:
                    body = response.json()
                except Exception:
                    body = {"message": response.text[:_MAX_PROVIDER_ERROR_TEXT]}

                if response.status_code in {401, 403}:
                    self._last_error = f"HTTP {response.status_code} from Uniswap API"
                    return _failure(
                        "uniswap_quote_authentication_failed",
                        symbol=normalized_symbol,
                        http_status=response.status_code,
                        provider_error=_safe_provider_error(body),
                        provider_contacted=True,
                    )
                if response.status_code == 429 or response.status_code >= 500:
                    self._last_error = f"HTTP {response.status_code} from Uniswap API"
                    return _failure(
                        "uniswap_quote_provider_transient_error",
                        symbol=normalized_symbol,
                        http_status=response.status_code,
                        retry_after=response.headers.get("Retry-After"),
                        provider_error=_safe_provider_error(body),
                        provider_contacted=True,
                    )
                if not response.is_success or not isinstance(body, dict):
                    self._last_error = f"HTTP {response.status_code} from Uniswap API"
                    return _failure(
                        "uniswap_quote_provider_error",
                        symbol=normalized_symbol,
                        http_status=response.status_code,
                        provider_error=_safe_provider_error(body),
                        provider_contacted=True,
                    )

                prohibited = _prohibited_artifact(body)
                if prohibited:
                    self._last_error = f"Prohibited provider artifact: {prohibited}"
                    return _failure(
                        "uniswap_quote_prohibited_artifact",
                        symbol=normalized_symbol,
                        prohibited_artifact=prohibited,
                        provider_contacted=True,
                        http_status=response.status_code,
                    )

                tx_failure = body.get("txFailureReason")
                quote_obj = body.get("quote") if isinstance(body.get("quote"), dict) else {}
                if not tx_failure:
                    tx_failure = quote_obj.get("txFailureReason")
                if tx_failure:
                    self._last_error = "Uniswap quote simulation failed"
                    return _failure(
                        "uniswap_quote_simulation_failed",
                        symbol=normalized_symbol,
                        provider_error=_safe_provider_error(tx_failure),
                        provider_contacted=True,
                        http_status=response.status_code,
                    )

                routing = str(body.get("routing") or "").strip().upper()
                if routing not in _ALLOWED_ROUTING:
                    self._last_error = f"Disallowed routing: {routing or 'missing'}"
                    return _failure(
                        "uniswap_quote_routing_not_allowed",
                        symbol=normalized_symbol,
                        routing=routing or None,
                        provider_contacted=True,
                        http_status=response.status_code,
                    )

                input_obj = quote_obj.get("input") if isinstance(quote_obj.get("input"), dict) else {}
                output_obj = quote_obj.get("output") if isinstance(quote_obj.get("output"), dict) else {}
                returned_input_token = str(input_obj.get("token") or "").strip().lower()
                returned_output_token = str(output_obj.get("token") or "").strip().lower()
                if returned_input_token != str(input_identity["provider_address"]).lower():
                    return _failure(
                        "uniswap_quote_provider_identity_mismatch",
                        symbol=normalized_symbol,
                        field="input.token",
                        provider_contacted=True,
                        http_status=response.status_code,
                    )
                if returned_output_token != str(output_identity["provider_address"]).lower():
                    return _failure(
                        "uniswap_quote_provider_identity_mismatch",
                        symbol=normalized_symbol,
                        field="output.token",
                        provider_contacted=True,
                        http_status=response.status_code,
                    )

                try:
                    returned_input_atomic, returned_input = _atomic_to_display(
                        input_obj.get("amount"),
                        int(input_identity["decimals"]),
                        field="provider_input_amount",
                    )
                    output_atomic, output_amount = _atomic_to_display(
                        output_obj.get("amount"),
                        int(output_identity["decimals"]),
                        field="provider_output_amount",
                    )
                    minimum_atomic, minimum_amount = _atomic_to_display(
                        output_obj.get("minimumAmount", output_obj.get("amount")),
                        int(output_identity["decimals"]),
                        field="provider_minimum_output_amount",
                    )
                except ValueError as exc:
                    self._last_error = str(exc)
                    return _failure(
                        "uniswap_quote_invalid_response",
                        symbol=normalized_symbol,
                        response_error=str(exc),
                        provider_contacted=True,
                        http_status=response.status_code,
                    )
                if returned_input_atomic != input_atomic:
                    return _failure(
                        "uniswap_quote_provider_amount_mismatch",
                        symbol=normalized_symbol,
                        expected_input_atomic=input_atomic,
                        returned_input_atomic=returned_input_atomic,
                        provider_contacted=True,
                        http_status=response.status_code,
                    )

                input_decimal = Decimal(returned_input)
                output_decimal = Decimal(output_amount)
                output_per_input = output_decimal / input_decimal
                if normalized_side == "buy":
                    quote_per_base = input_decimal / output_decimal
                else:
                    quote_per_base = output_decimal / input_decimal

                protocols = _route_protocols(quote_obj.get("route"))
                gas_estimate = quote_obj.get("gasUseEstimate")
                gas_estimate_usd = (
                    quote_obj.get("gasUseEstimateUSD")
                    or quote_obj.get("classicGasUseEstimateUSD")
                    or body.get("gasFeeUSD")
                )
                self._last_good_at = _utc_iso()
                self._last_error = None
                return {
                    "ok": True,
                    "provider": UNISWAP_PROVIDER,
                    "provider_contacted": True,
                    "credential_source": credential.get("source"),
                    "credential_venue": credential.get("venue"),
                    "chain_id": UNISWAP_CHAIN_ID,
                    "symbol": normalized_symbol,
                    "side": normalized_side,
                    "amount_mode": "exact_input",
                    "routing": routing,
                    "routing_preference": "BEST_PRICE",
                    "route_protocols": protocols,
                    "requested_amount": normalized_requested,
                    "input_asset": input_identity["symbol"],
                    "input_registry_id": input_identity["registry_id"],
                    "input_amount": returned_input,
                    "input_amount_atomic": returned_input_atomic,
                    "output_asset": output_identity["symbol"],
                    "output_registry_id": output_identity["registry_id"],
                    "output_amount": output_amount,
                    "output_amount_atomic": output_atomic,
                    "minimum_received": minimum_amount,
                    "minimum_received_atomic": minimum_atomic,
                    "price_output_per_input": _decimal_text(output_per_input),
                    "price_quote_per_base": _decimal_text(quote_per_base),
                    "slippage_bps": slippage,
                    "slippage_percent": _decimal_text(Decimal(slippage) / Decimal(100)),
                    "is_token_approval_applicable": bool(body.get("isTokenApprovalApplicable", True)),
                    "gas_use_estimate": str(gas_estimate).strip() if gas_estimate is not None else None,
                    "gas_use_estimate_usd": str(gas_estimate_usd).strip() if gas_estimate_usd is not None else None,
                    "request_id": str(body.get("requestId") or "").strip() or None,
                    "elapsed_ms": round(elapsed_ms, 2),
                    "fetched_at": self._last_good_at,
                    "read_only": True,
                    "quote_only": True,
                    "response_sanitized": True,
                    "raw_provider_response_returned": False,
                    "database_mutation": False,
                    "will_mutate": False,
                    "execution_authority": False,
                    "execution_enabled": False,
                    "wallet_connection_requested": False,
                    "signing_enabled": False,
                    "broadcast_enabled": False,
                    "swap_endpoint_enabled": False,
                    "order_endpoint_enabled": False,
                    "transaction": None,
                    "transaction_calldata": None,
                    "permit_data": None,
                    "permit_transaction": None,
                    "encoded_order": None,
                }
            except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
                self._last_error = f"{type(exc).__name__}: {exc}"
                return _failure(
                    "uniswap_quote_provider_transient_error",
                    symbol=normalized_symbol,
                    provider_error=type(exc).__name__,
                    provider_contacted=True,
                )
            except Exception as exc:
                self._last_error = f"{type(exc).__name__}: {exc}"
                return _failure(
                    "uniswap_quote_provider_error",
                    symbol=normalized_symbol,
                    provider_error=type(exc).__name__,
                    provider_contacted=True,
                )


_SERVICE: Optional[RobinhoodChainUniswapQuoteService] = None


def get_robinhood_chain_uniswap_quote_service() -> RobinhoodChainUniswapQuoteService:
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = RobinhoodChainUniswapQuoteService(
            api_base=settings.robinhood_chain_effective_uniswap_api_base(),
            timeout_s=float(settings.robinhood_chain_uniswap_quote_timeout_s),
            max_concurrent=int(settings.robinhood_chain_uniswap_quote_max_concurrent),
            credential_getter=settings.robinhood_chain_uniswap_api_credential,
        )
    return _SERVICE
