from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .evm_rpc import decode_abi_uint256, get_robinhood_chain_client, validate_evm_address


UNISWAP_V3_RPC_PROVIDER = "uniswap_v3_rpc"
UNISWAP_V3_CHAIN_ID = 4663
UNISWAP_V3_FACTORY = "0x1f7d7550b1b028f7571e69a784071f0205fd2efa"
UNISWAP_V3_QUOTER_V2 = "0x33e885ed0ec9bf04ecfb19341582aadcb4c8a9e7"
UNISWAP_V3_SWAP_ROUTER_02 = "0xcaf681a66d020601342297493863e78c959e5cb2"
UNISWAP_V3_PERMIT2 = "0x000000000022d473030f116ddee9f6b43ac78ba3"
UNISWAP_V3_UNIVERSAL_ROUTER = "0x8876789976decbfcbbbe364623c63652db8c0904"
UNISWAP_V3_FEE_TIERS: Tuple[int, ...] = (100, 500, 3000, 10000)
UNISWAP_V3_FACTORY_GET_POOL_SELECTOR = "1698ee82"
UNISWAP_V3_QUOTE_EXACT_INPUT_SINGLE_SELECTOR = "c6a5026a"
UNISWAP_V3_QUOTE_EXACT_INPUT_SELECTOR = "cdca1753"
UNISWAP_V3_ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
UNISWAP_V3_MAX_UINT256 = (1 << 256) - 1
UNISWAP_V3_BOOK_MULTIPLIERS: Tuple[Decimal, ...] = (
    Decimal("0.25"),
    Decimal("0.5"),
    Decimal("1"),
    Decimal("2"),
    Decimal("4"),
)


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


def _display_to_atomic(value: Any, decimals: int, *, field: str = "requested_amount") -> Tuple[int, str]:
    number = _decimal(value, field=field)
    places = int(decimals)
    if places < 0 or places > 18:
        raise ValueError("invalid_token_decimals")
    if max(0, -number.as_tuple().exponent) > places:
        raise ValueError(f"{field}_exceeds_token_precision")
    atomic = int(number * (Decimal(10) ** places))
    if atomic <= 0 or atomic > UNISWAP_V3_MAX_UINT256:
        raise ValueError(f"invalid_{field}")
    normalized = Decimal(atomic) / (Decimal(10) ** places)
    return atomic, _decimal_text(normalized)


def _atomic_to_display(value: int, decimals: int) -> str:
    atomic = int(value)
    places = int(decimals)
    if atomic < 0 or atomic > UNISWAP_V3_MAX_UINT256 or places < 0 or places > 18:
        raise ValueError("invalid_atomic_amount")
    return _decimal_text(Decimal(atomic) / (Decimal(10) ** places))


def _address_word(value: str) -> str:
    return validate_evm_address(value)[2:].lower().rjust(64, "0")


def _uint_word(value: int, bits: int = 256) -> str:
    number = int(value)
    if number < 0 or number >= (1 << int(bits)):
        raise ValueError("invalid_abi_uint")
    return f"{number:064x}"


def encode_factory_get_pool(token_a: str, token_b: str, fee: int) -> str:
    return "0x" + "".join(
        (
            UNISWAP_V3_FACTORY_GET_POOL_SELECTOR,
            _address_word(token_a),
            _address_word(token_b),
            _uint_word(fee, 24),
        )
    )


def decode_first_abi_uint256(value: Any) -> int:
    raw = str(value or "").strip().lower()
    if not raw.startswith("0x"):
        raise ValueError("invalid_uniswap_v3_quote_result")
    body = raw[2:]
    if len(body) < 64 or any(ch not in "0123456789abcdef" for ch in body[:64]):
        raise ValueError("invalid_uniswap_v3_quote_result")
    return int(body[:64], 16)


def decode_abi_address(value: Any) -> Optional[str]:
    raw = str(value or "").strip().lower()
    if not raw.startswith("0x"):
        return None
    body = raw[2:]
    if len(body) < 64:
        return None
    word = body[:64]
    if any(ch not in "0123456789abcdef" for ch in word):
        return None
    address = "0x" + word[-40:]
    if address == UNISWAP_V3_ZERO_ADDRESS:
        return None
    try:
        return validate_evm_address(address)
    except ValueError:
        return None


def encode_quote_exact_input_single(
    token_in: str,
    token_out: str,
    amount_in: int,
    fee: int,
) -> str:
    return "0x" + "".join(
        (
            UNISWAP_V3_QUOTE_EXACT_INPUT_SINGLE_SELECTOR,
            _address_word(token_in),
            _address_word(token_out),
            _uint_word(amount_in),
            _uint_word(fee, 24),
            _uint_word(0, 160),
        )
    )


def encode_v3_path(tokens: Sequence[str], fees: Sequence[int]) -> bytes:
    if len(tokens) < 2 or len(fees) != len(tokens) - 1:
        raise ValueError("invalid_uniswap_v3_path")
    encoded = bytearray()
    for index, token in enumerate(tokens):
        address = validate_evm_address(token)
        encoded.extend(bytes.fromhex(address[2:]))
        if index < len(fees):
            fee = int(fees[index])
            if fee < 0 or fee >= (1 << 24):
                raise ValueError("invalid_uniswap_v3_fee")
            encoded.extend(fee.to_bytes(3, "big"))
    return bytes(encoded)


def encode_quote_exact_input(path: bytes, amount_in: int) -> str:
    path_bytes = bytes(path)
    padded_length = ((len(path_bytes) + 31) // 32) * 32
    padded = path_bytes + (b"\x00" * (padded_length - len(path_bytes)))
    return "0x" + "".join(
        (
            UNISWAP_V3_QUOTE_EXACT_INPUT_SELECTOR,
            _uint_word(64),
            _uint_word(amount_in),
            _uint_word(len(path_bytes)),
            padded.hex(),
        )
    )


def _normalize_token(token: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(token, dict):
        raise ValueError("invalid_uniswap_v3_registry_identity")
    symbol = str(token.get("symbol") or "").strip().upper()
    registry_id = token.get("registry_id")
    identity_source = str(token.get("identity_source") or "").strip().lower()
    native = bool(token.get("native"))
    try:
        decimals = int(token.get("decimals"))
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid_uniswap_v3_registry_identity") from exc
    if not symbol or registry_id is None or identity_source != "token_registry":
        raise ValueError("invalid_uniswap_v3_registry_identity")
    if native:
        raise ValueError("uniswap_v3_rpc_requires_wrapped_native_identity")
    address = validate_evm_address(
        str(token.get("registry_contract_address") or token.get("contract_address") or "").strip()
    )
    if decimals < 0 or decimals > 18:
        raise ValueError("invalid_uniswap_v3_registry_identity")
    return {
        "symbol": symbol,
        "registry_id": int(registry_id),
        "decimals": decimals,
        "native": False,
        "contract_address": address,
        "identity_source": "token_registry",
        "registry_venue": token.get("registry_venue"),
        "registry_status": token.get("registry_status"),
    }


def _safe_rpc_error(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key in ("error", "message", "code", "reason", "backoff_until"):
            if value.get(key) is not None:
                out[key] = value.get(key)
        return out or {"message": str(value)[:1000]}
    text = str(value or "").strip()
    return {"message": text[:1000]} if text else {}


def _failure(error: str, **extra: Any) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "ok": False,
        "error": str(error or "uniswap_v3_rpc_quote_failed"),
        "provider": UNISWAP_V3_RPC_PROVIDER,
        "chain_id": UNISWAP_V3_CHAIN_ID,
        "provider_contacted": False,
        "liquidity_available": False,
        "read_only": True,
        "quote_only": True,
        "transaction_constructed": False,
        "transaction_calldata": None,
        "wallet_connection_requested": False,
        "signing_enabled": False,
        "broadcast_enabled": False,
        "execution_enabled": False,
        "will_mutate": False,
    }
    payload.update(extra)
    return payload


class RobinhoodChainUniswapV3QuoteService:
    """Direct, bounded Uniswap v3 Factory + QuoterV2 read service.

    This service performs only read-only eth_call operations. It never creates
    approvals, transaction calldata, signatures, or broadcasts. Native ETH must
    be mapped by the caller to the verified WETH Token Registry identity.
    """

    def __init__(self, *, rpc_client: Any = None) -> None:
        self.rpc_client = rpc_client or get_robinhood_chain_client()

    def status(self) -> Dict[str, Any]:
        rpc_status = self.rpc_client.status() if hasattr(self.rpc_client, "status") else {}
        return {
            "ok": True,
            "provider": UNISWAP_V3_RPC_PROVIDER,
            "chain_id": UNISWAP_V3_CHAIN_ID,
            "factory": UNISWAP_V3_FACTORY,
            "quoter_v2": UNISWAP_V3_QUOTER_V2,
            "swap_router_02": UNISWAP_V3_SWAP_ROUTER_02,
            "permit2": UNISWAP_V3_PERMIT2,
            "universal_router": UNISWAP_V3_UNIVERSAL_ROUTER,
            "fee_tiers": list(UNISWAP_V3_FEE_TIERS),
            "rpc_configured": bool(rpc_status.get("configured", True)),
            "read_only": True,
            "quote_only": True,
            "transaction_construction_enabled": False,
        }

    async def _pool_address(
        self,
        token_a: str,
        token_b: str,
        fee: int,
        *,
        force_refresh: bool,
    ) -> Tuple[Optional[str], Dict[str, Any]]:
        data = encode_factory_get_pool(token_a, token_b, fee)
        result = await self.rpc_client.rpc_read(
            "eth_call",
            [{"to": UNISWAP_V3_FACTORY, "data": data}, "latest"],
            cache_namespace=(
                f"rh_uniswap_v3_pool:{token_a.lower()}:{token_b.lower()}:{int(fee)}"
            ),
            force_refresh=force_refresh,
        )
        pool = decode_abi_address(result.get("result")) if result.get("ok") else None
        return pool, result

    async def _quote_single(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        fee: int,
        *,
        force_refresh: bool,
    ) -> Dict[str, Any]:
        data = encode_quote_exact_input_single(token_in, token_out, amount_in, fee)
        result = await self.rpc_client.rpc_read(
            "eth_call",
            [{"to": UNISWAP_V3_QUOTER_V2, "data": data}, "latest"],
            cache_namespace=(
                f"rh_uniswap_v3_quote_single:{token_in.lower()}:{token_out.lower()}:{int(fee)}:{int(amount_in)}"
            ),
            force_refresh=force_refresh,
        )
        if not result.get("ok"):
            return _failure(
                "uniswap_v3_quote_call_failed",
                provider_contacted=True,
                rpc_error=_safe_rpc_error(result.get("error")),
            )
        try:
            amount_out = decode_first_abi_uint256(result.get("result"))
        except Exception as exc:
            return _failure(
                "uniswap_v3_quote_decode_failed",
                provider_contacted=True,
                rpc_error={"message": str(exc)[:1000]},
            )
        if amount_out <= 0:
            return _failure("uniswap_v3_zero_quote", provider_contacted=True)
        return {
            "ok": True,
            "provider": UNISWAP_V3_RPC_PROVIDER,
            "provider_contacted": True,
            "amount_out_atomic": str(amount_out),
            "route_type": "direct",
            "fees": [int(fee)],
            "read_only": True,
            "will_mutate": False,
        }

    async def _quote_path(
        self,
        tokens: Sequence[str],
        fees: Sequence[int],
        amount_in: int,
        *,
        force_refresh: bool,
    ) -> Dict[str, Any]:
        path = encode_v3_path(tokens, fees)
        data = encode_quote_exact_input(path, amount_in)
        route_key = ":".join([*(item.lower() for item in tokens), *(str(int(item)) for item in fees)])
        result = await self.rpc_client.rpc_read(
            "eth_call",
            [{"to": UNISWAP_V3_QUOTER_V2, "data": data}, "latest"],
            cache_namespace=f"rh_uniswap_v3_quote_path:{route_key}:{int(amount_in)}",
            force_refresh=force_refresh,
        )
        if not result.get("ok"):
            return _failure(
                "uniswap_v3_quote_call_failed",
                provider_contacted=True,
                rpc_error=_safe_rpc_error(result.get("error")),
            )
        try:
            amount_out = decode_first_abi_uint256(result.get("result"))
        except Exception as exc:
            return _failure(
                "uniswap_v3_quote_decode_failed",
                provider_contacted=True,
                rpc_error={"message": str(exc)[:1000]},
            )
        if amount_out <= 0:
            return _failure("uniswap_v3_zero_quote", provider_contacted=True)
        return {
            "ok": True,
            "provider": UNISWAP_V3_RPC_PROVIDER,
            "provider_contacted": True,
            "amount_out_atomic": str(amount_out),
            "route_type": "weth_bridge",
            "fees": [int(item) for item in fees],
            "read_only": True,
            "will_mutate": False,
        }

    async def probe(
        self,
        *,
        requested_amount: str,
        input_token: Dict[str, Any],
        output_token: Dict[str, Any],
        bridge_token: Optional[Dict[str, Any]] = None,
        display_input_symbol: Optional[str] = None,
        display_output_symbol: Optional[str] = None,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        try:
            input_identity = _normalize_token(input_token)
            output_identity = _normalize_token(output_token)
            bridge_identity = _normalize_token(bridge_token) if bridge_token is not None else None
            amount_in_atomic, amount_in_display = _display_to_atomic(
                requested_amount,
                input_identity["decimals"],
            )
        except ValueError as exc:
            return _failure(str(exc))

        if input_identity["contract_address"].lower() == output_identity["contract_address"].lower():
            return _failure("uniswap_v3_same_token_pair")

        chain = await self.rpc_client.verify_expected_chain(force_refresh=force_refresh)
        if not chain.get("ok"):
            return _failure(
                "chain_id_mismatch_or_unavailable",
                provider_contacted=True,
                rpc_error=_safe_rpc_error(chain.get("error")),
            )

        input_address = input_identity["contract_address"]
        output_address = output_identity["contract_address"]
        candidates: List[Dict[str, Any]] = []
        pool_checks: List[Dict[str, Any]] = []

        for fee in UNISWAP_V3_FEE_TIERS:
            pool, pool_result = await self._pool_address(
                input_address,
                output_address,
                fee,
                force_refresh=force_refresh,
            )
            pool_checks.append({
                "tokens": [input_identity["symbol"], output_identity["symbol"]],
                "fee": int(fee),
                "pool": pool,
                "ok": bool(pool_result.get("ok")),
                "error": _safe_rpc_error(pool_result.get("error")),
            })
            if not pool:
                continue
            quote = await self._quote_single(
                input_address,
                output_address,
                amount_in_atomic,
                fee,
                force_refresh=force_refresh,
            )
            if quote.get("ok"):
                candidates.append({**quote, "pools": [pool], "tokens": [input_address, output_address]})

        if (
            bridge_identity is not None
            and bridge_identity["contract_address"].lower() not in {
                input_address.lower(),
                output_address.lower(),
            }
        ):
            bridge_address = bridge_identity["contract_address"]
            first_hops: List[Tuple[int, str]] = []
            second_hops: List[Tuple[int, str]] = []
            for fee in UNISWAP_V3_FEE_TIERS:
                pool, pool_result = await self._pool_address(
                    input_address,
                    bridge_address,
                    fee,
                    force_refresh=force_refresh,
                )
                pool_checks.append({
                    "tokens": [input_identity["symbol"], bridge_identity["symbol"]],
                    "fee": int(fee),
                    "pool": pool,
                    "ok": bool(pool_result.get("ok")),
                    "error": _safe_rpc_error(pool_result.get("error")),
                })
                if pool:
                    first_hops.append((int(fee), pool))
            for fee in UNISWAP_V3_FEE_TIERS:
                pool, pool_result = await self._pool_address(
                    bridge_address,
                    output_address,
                    fee,
                    force_refresh=force_refresh,
                )
                pool_checks.append({
                    "tokens": [bridge_identity["symbol"], output_identity["symbol"]],
                    "fee": int(fee),
                    "pool": pool,
                    "ok": bool(pool_result.get("ok")),
                    "error": _safe_rpc_error(pool_result.get("error")),
                })
                if pool:
                    second_hops.append((int(fee), pool))
            for first_fee, first_pool in first_hops:
                for second_fee, second_pool in second_hops:
                    quote = await self._quote_path(
                        [input_address, bridge_address, output_address],
                        [first_fee, second_fee],
                        amount_in_atomic,
                        force_refresh=force_refresh,
                    )
                    if quote.get("ok"):
                        candidates.append({
                            **quote,
                            "pools": [first_pool, second_pool],
                            "tokens": [input_address, bridge_address, output_address],
                            "bridge_symbol": bridge_identity["symbol"],
                        })

        if not candidates:
            discovered_pool_count = sum(1 for item in pool_checks if item.get("pool"))
            return _failure(
                "uniswap_v3_no_quotable_route" if discovered_pool_count else "uniswap_v3_pool_not_found",
                provider_contacted=True,
                sell_amount=amount_in_display,
                buy_amount=None,
                price_buy_per_sell=None,
                pool_checks=pool_checks,
                discovered_pool_count=discovered_pool_count,
            )

        best = max(candidates, key=lambda item: int(str(item.get("amount_out_atomic") or "0")))
        output_atomic = int(str(best["amount_out_atomic"]))
        output_display = _atomic_to_display(output_atomic, output_identity["decimals"])
        input_number = Decimal(amount_in_display)
        output_number = Decimal(output_display)
        price = output_number / input_number
        route_source = (
            "UNISWAP_V3_RPC_DIRECT"
            if best.get("route_type") == "direct"
            else "UNISWAP_V3_RPC_WETH_BRIDGE"
        )
        display_in = str(display_input_symbol or input_identity["symbol"]).strip().upper()
        display_out = str(display_output_symbol or output_identity["symbol"]).strip().upper()
        return {
            "ok": True,
            "tranche": "R5C.5D.2E-R1",
            "provider": UNISWAP_V3_RPC_PROVIDER,
            "provider_contacted": True,
            "liquidity_available": True,
            "sell_amount": amount_in_display,
            "buy_amount": output_display,
            "min_buy_amount": None,
            "price_buy_per_sell": _decimal_text(price),
            "input_asset": display_in,
            "output_asset": display_out,
            "input_token_symbol": input_identity["symbol"],
            "output_token_symbol": output_identity["symbol"],
            "input_amount_atomic": str(amount_in_atomic),
            "output_amount_atomic": str(output_atomic),
            "route": {"fills": [{"source": route_source}]},
            "route_type": best.get("route_type"),
            "route_tokens": list(best.get("tokens") or []),
            "route_fees": list(best.get("fees") or []),
            "route_pools": list(best.get("pools") or []),
            "bridge_symbol": best.get("bridge_symbol"),
            "candidate_count": len(candidates),
            "pool_checks": pool_checks,
            "provider_warnings": [],
            "fetched_at": _utc_iso(),
            "read_only": True,
            "quote_only": True,
            "transaction_constructed": False,
            "transaction_calldata": None,
            "wallet_connection_requested": False,
            "signing_enabled": False,
            "broadcast_enabled": False,
            "execution_enabled": False,
            "will_mutate": False,
        }

    async def quote_for_pair(
        self,
        *,
        symbol: str,
        side: str,
        amount_mode: str,
        requested_amount: str,
        base_token: Dict[str, Any],
        quote_token: Dict[str, Any],
        weth_token: Dict[str, Any],
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        normalized_symbol = str(symbol or "").strip().upper().replace("/", "-").replace("_", "-")
        normalized_side = str(side or "").strip().lower()
        normalized_mode = str(amount_mode or "").strip().lower()
        base_symbol = str(base_token.get("symbol") or "").strip().upper()
        quote_symbol = str(quote_token.get("symbol") or "").strip().upper()
        if normalized_symbol != f"{base_symbol}-{quote_symbol}":
            return _failure("uniswap_v3_quote_pair_identity_mismatch", symbol=normalized_symbol)
        if normalized_side not in {"buy", "sell"}:
            return _failure("invalid_quote_side", symbol=normalized_symbol)
        if normalized_mode != "exact_input":
            return _failure("uniswap_v3_quote_exact_input_only", symbol=normalized_symbol)

        display_input = quote_token if normalized_side == "buy" else base_token
        display_output = base_token if normalized_side == "buy" else quote_token
        provider_input = weth_token if bool(display_input.get("native")) else display_input
        provider_output = weth_token if bool(display_output.get("native")) else display_output
        result = await self.probe(
            requested_amount=requested_amount,
            input_token=provider_input,
            output_token=provider_output,
            bridge_token=weth_token,
            display_input_symbol=str(display_input.get("symbol") or "").strip().upper(),
            display_output_symbol=str(display_output.get("symbol") or "").strip().upper(),
            force_refresh=force_refresh,
        )
        if not result.get("ok"):
            result.update({
                "symbol": normalized_symbol,
                "side": normalized_side,
                "amount_mode": normalized_mode,
            })
            return result

        input_amount = Decimal(str(result.get("sell_amount") or "0"))
        output_amount = Decimal(str(result.get("buy_amount") or "0"))
        if input_amount <= 0 or output_amount <= 0:
            return _failure(
                "uniswap_v3_invalid_quote_amounts",
                symbol=normalized_symbol,
                side=normalized_side,
                amount_mode=normalized_mode,
                provider_contacted=True,
            )
        quote_per_base = (
            input_amount / output_amount
            if normalized_side == "buy"
            else output_amount / input_amount
        )
        result.update({
            "symbol": normalized_symbol,
            "side": normalized_side,
            "amount_mode": normalized_mode,
            "routing": "UNISWAP_V3_RPC",
            "routing_preference": "DIRECT_OR_WETH_BRIDGE",
            "requested_amount": str(requested_amount).strip(),
            "input_asset": str(display_input.get("symbol") or "").strip().upper(),
            "input_registry_id": display_input.get("registry_id"),
            "input_amount": result.get("sell_amount"),
            "output_asset": str(display_output.get("symbol") or "").strip().upper(),
            "output_registry_id": display_output.get("registry_id"),
            "output_amount": result.get("buy_amount"),
            "minimum_received": None,
            "price_output_per_input": result.get("price_buy_per_sell"),
            "price_quote_per_base": _decimal_text(quote_per_base),
            "effective_price": _decimal_text(quote_per_base),
            "base_quantity": (
                result.get("buy_amount")
                if normalized_side == "buy"
                else result.get("sell_amount")
            ),
            "quote_quantity": (
                result.get("sell_amount")
                if normalized_side == "buy"
                else result.get("buy_amount")
            ),
            "is_token_approval_applicable": None,
            "database_mutation": False,
            "response_sanitized": True,
            "raw_provider_response_returned": False,
        })
        return result

    @staticmethod
    def _scaled_amount(seed: Any, multiplier: Decimal, decimals: int) -> str:
        number = _decimal(seed, field="probe_amount") * multiplier
        atomic = int(number * (Decimal(10) ** int(decimals)))
        if atomic <= 0:
            atomic = 1
        return _atomic_to_display(atomic, int(decimals))

    async def synthetic_orderbook_for_pair(
        self,
        *,
        symbol: str,
        depth: int,
        base_token: Dict[str, Any],
        quote_token: Dict[str, Any],
        base_provider_token: Dict[str, Any],
        quote_provider_token: Dict[str, Any],
        bridge_token: Optional[Dict[str, Any]],
        base_to_quote_capability: Dict[str, Any],
        quote_to_base_capability: Dict[str, Any],
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        normalized_symbol = str(symbol or "").strip().upper().replace("/", "-").replace("_", "-")
        base_symbol = str(base_token.get("symbol") or "").strip().upper()
        quote_symbol = str(quote_token.get("symbol") or "").strip().upper()
        if normalized_symbol != f"{base_symbol}-{quote_symbol}":
            return _failure("uniswap_v3_orderbook_pair_identity_mismatch", symbol=normalized_symbol)
        for capability, from_symbol, to_symbol in (
            (base_to_quote_capability, base_symbol, quote_symbol),
            (quote_to_base_capability, quote_symbol, base_symbol),
        ):
            if (
                str(capability.get("provider") or "").strip().lower() != UNISWAP_V3_RPC_PROVIDER
                or str(capability.get("from_asset") or "").strip().upper() != from_symbol
                or str(capability.get("to_asset") or "").strip().upper() != to_symbol
                or str(capability.get("amount_mode") or "").strip().lower() != "exact_input"
                or str(capability.get("indicative_status") or "").strip().lower() not in {"available", "live_verified"}
            ):
                return _failure(
                    "uniswap_v3_orderbook_direction_unavailable",
                    symbol=normalized_symbol,
                    input_asset=from_symbol,
                    output_asset=to_symbol,
                )

        levels = max(1, min(int(depth), len(UNISWAP_V3_BOOK_MULTIPLIERS)))
        try:
            base_provider_identity = _normalize_token(base_provider_token)
            quote_provider_identity = _normalize_token(quote_provider_token)
            bid_amounts = [
                self._scaled_amount(
                    base_to_quote_capability.get("probe_amount"),
                    multiplier,
                    int(base_provider_identity["decimals"]),
                )
                for multiplier in UNISWAP_V3_BOOK_MULTIPLIERS[:levels]
            ]
            ask_amounts = [
                self._scaled_amount(
                    quote_to_base_capability.get("probe_amount"),
                    multiplier,
                    int(quote_provider_identity["decimals"]),
                )
                for multiplier in UNISWAP_V3_BOOK_MULTIPLIERS[:levels]
            ]
        except ValueError as exc:
            return _failure(str(exc), symbol=normalized_symbol)

        bids: List[Dict[str, Any]] = []
        asks: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []
        for amount in bid_amounts:
            quote = await self.probe(
                requested_amount=amount,
                input_token=base_provider_token,
                output_token=quote_provider_token,
                bridge_token=bridge_token,
                display_input_symbol=base_symbol,
                display_output_symbol=quote_symbol,
                force_refresh=force_refresh,
            )
            if quote.get("ok"):
                bids.append({
                    "price": quote.get("price_buy_per_sell"),
                    "size": quote.get("sell_amount"),
                    "base_quantity": quote.get("sell_amount"),
                    "quote_quantity": quote.get("buy_amount"),
                    "input_asset": base_symbol,
                    "input_amount": quote.get("sell_amount"),
                    "output_asset": quote_symbol,
                    "output_amount": quote.get("buy_amount"),
                    "minimum_received": None,
                    "route_sources": [item.get("source") for item in (quote.get("route", {}).get("fills") or [])],
                    "route_type": quote.get("route_type"),
                    "route_fees": quote.get("route_fees"),
                    "route_pools": quote.get("route_pools"),
                    "provider": UNISWAP_V3_RPC_PROVIDER,
                    "synthetic": True,
                    "resting_order": False,
                    "fetched_at": quote.get("fetched_at"),
                })
            else:
                errors.append({"side": "bid", "sample_input_amount": amount, "error": quote.get("error")})
        for amount in ask_amounts:
            quote = await self.probe(
                requested_amount=amount,
                input_token=quote_provider_token,
                output_token=base_provider_token,
                bridge_token=bridge_token,
                display_input_symbol=quote_symbol,
                display_output_symbol=base_symbol,
                force_refresh=force_refresh,
            )
            if quote.get("ok"):
                output_base = Decimal(str(quote.get("buy_amount") or "0"))
                input_quote = Decimal(str(quote.get("sell_amount") or "0"))
                if output_base <= 0:
                    errors.append({"side": "ask", "sample_input_amount": amount, "error": "invalid_output_amount"})
                    continue
                price_quote_per_base = input_quote / output_base
                asks.append({
                    "price": _decimal_text(price_quote_per_base),
                    "size": quote.get("buy_amount"),
                    "base_quantity": quote.get("buy_amount"),
                    "quote_quantity": quote.get("sell_amount"),
                    "input_asset": quote_symbol,
                    "input_amount": quote.get("sell_amount"),
                    "output_asset": base_symbol,
                    "output_amount": quote.get("buy_amount"),
                    "minimum_received": None,
                    "route_sources": [item.get("source") for item in (quote.get("route", {}).get("fills") or [])],
                    "route_type": quote.get("route_type"),
                    "route_fees": quote.get("route_fees"),
                    "route_pools": quote.get("route_pools"),
                    "provider": UNISWAP_V3_RPC_PROVIDER,
                    "synthetic": True,
                    "resting_order": False,
                    "fetched_at": quote.get("fetched_at"),
                })
            else:
                errors.append({"side": "ask", "sample_input_amount": amount, "error": quote.get("error")})

        bids.sort(key=lambda item: Decimal(str(item.get("price") or "0")), reverse=True)
        asks.sort(key=lambda item: Decimal(str(item.get("price") or "0")))
        best_bid = Decimal(str(bids[0]["price"])) if bids else None
        best_ask = Decimal(str(asks[0]["price"])) if asks else None
        spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None
        midpoint = (best_ask + best_bid) / Decimal(2) if spread is not None else None
        spread_bps = spread / midpoint * Decimal(10000) if midpoint and midpoint > 0 else None
        sources: List[str] = []
        for row in [*bids, *asks]:
            for source in row.get("route_sources") or []:
                if source and source not in sources:
                    sources.append(source)
        return {
            "ok": bool(bids and asks),
            "tranche": "R5C.5D.2E-R1",
            "venue": "robinhood_chain",
            "network": "robinhood_chain",
            "chain_id": UNISWAP_V3_CHAIN_ID,
            "chain_id_hex": hex(UNISWAP_V3_CHAIN_ID),
            "mainnet_only": True,
            "provider": UNISWAP_V3_RPC_PROVIDER,
            "router": "quoter_v2",
            "factory": UNISWAP_V3_FACTORY,
            "quoter_v2": UNISWAP_V3_QUOTER_V2,
            "symbol": normalized_symbol,
            "resolvedSymbol": normalized_symbol,
            "base_asset": base_symbol,
            "quote_asset": quote_symbol,
            "base_token_registry_id": base_token.get("registry_id"),
            "quote_token_registry_id": quote_token.get("registry_id"),
            "identity_source": "token_registry",
            "capability_source": "database",
            "depth_requested": int(depth),
            "depth_returned": min(len(bids), len(asks)),
            "max_depth": len(UNISWAP_V3_BOOK_MULTIPLIERS),
            "bids": bids,
            "asks": asks,
            "best_bid": _decimal_text(best_bid) if best_bid is not None else None,
            "best_ask": _decimal_text(best_ask) if best_ask is not None else None,
            "spread": _decimal_text(spread) if spread is not None else None,
            "spread_bps": _decimal_text(spread_bps) if spread_bps is not None else None,
            "midpoint": _decimal_text(midpoint) if midpoint is not None else None,
            "sources": sources,
            "route_sources": sources,
            "errors": errors[:20],
            "warning_count": len(errors),
            "liquidity_available": bool(bids and asks),
            "priceDecimals": max(6, min(12, int(quote_token.get("decimals") or 18))),
            "sizeDecimals": max(0, min(18, int(base_token.get("decimals") or 18))),
            "cached": False,
            "fetched_at": max([str(row.get("fetched_at") or "") for row in [*bids, *asks]] or [""]) or None,
            "snapshot_source": "uniswap_v3_factory_quoter_v2_samples",
            "stale": False,
            "synthetic": True,
            "resting_order": False,
            "quote_only": True,
            "read_only": True,
            "execution_enabled": False,
            "signing_enabled": False,
            "transaction_construction_enabled": False,
            "firm_quote": False,
            "transaction_calldata": None,
            "will_mutate": False,
            **({"error": "synthetic_orderbook_liquidity_incomplete"} if not (bids and asks) else {}),
        }


_SERVICE: Optional[RobinhoodChainUniswapV3QuoteService] = None


def get_robinhood_chain_uniswap_v3_quote_service() -> RobinhoodChainUniswapV3QuoteService:
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = RobinhoodChainUniswapV3QuoteService()
    return _SERVICE
