from __future__ import annotations

import asyncio
import copy
import time
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from .robinhood_chain_execution_discovery import (
    RobinhoodChainExecutionDiscoveryService,
    get_robinhood_chain_execution_discovery_service,
)


ROBINHOOD_CHAIN_QUOTE_SYMBOL = "ETH-USDG"
ROBINHOOD_CHAIN_REVIEW_QUOTE_SYMBOLS: Tuple[str, ...] = ("ETH-USDG", "WETH-USDG")
ROBINHOOD_CHAIN_QUOTE_PROVIDER = "0x"
ROBINHOOD_CHAIN_MAX_BOOK_LEVELS = 5
ROBINHOOD_CHAIN_EXACT_OUTPUT_BUY_ETH = Decimal("0.001")
ROBINHOOD_CHAIN_MAXIMUM_BUY_USDG = Decimal("2")
ROBINHOOD_CHAIN_BID_INPUT_AMOUNTS: Tuple[str, ...] = (
    "0.00005",
    "0.0001",
    "0.00025",
    "0.0005",
    "0.001",
)
ROBINHOOD_CHAIN_ASK_INPUT_AMOUNTS: Tuple[str, ...] = (
    "0.10",
    "0.25",
    "0.50",
    "1.00",
    "2.00",
)


def _normalize_symbol(value: Any) -> str:
    raw = str(value or "").strip().upper().replace("/", "-").replace("_", "-")
    parts = [part.strip() for part in raw.split("-") if part.strip()]
    return "-".join(parts)


def _normalize_side(value: Any) -> str:
    side = str(value or "").strip().lower()
    if side not in {"buy", "sell"}:
        raise ValueError("invalid_quote_side")
    return side


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


def _safe_decimal(value: Any) -> Optional[Decimal]:
    try:
        number = Decimal(str(value if value is not None else "").strip())
    except (InvalidOperation, ValueError, TypeError):
        return None
    return number if number.is_finite() else None


def _token_symbol(token: Dict[str, Any]) -> str:
    return str(token.get("symbol") or "").strip().upper()


def _bounded_sample_amounts(seed: Any, decimals: int, levels: int) -> Tuple[str, ...]:
    seed_amount = _safe_decimal(seed)
    if seed_amount is None or seed_amount <= 0:
        raise ValueError("invalid_robinhood_chain_book_probe_amount")
    places = max(0, min(18, int(decimals)))
    quantum = Decimal(1).scaleb(-places) if places > 0 else Decimal(1)
    factors = (Decimal("0.25"), Decimal("0.5"), Decimal("1"), Decimal("2"), Decimal("4"))
    out: List[str] = []
    for factor in factors:
        amount = (seed_amount * factor).quantize(quantum, rounding=ROUND_DOWN)
        if amount <= 0 or amount > Decimal("25"):
            continue
        text = _decimal_text(amount)
        if text not in out:
            out.append(text)
        if len(out) >= max(1, min(int(levels), ROBINHOOD_CHAIN_MAX_BOOK_LEVELS)):
            break
    if not out:
        raise ValueError("robinhood_chain_book_probe_amount_below_precision")
    return tuple(out)


def _route_sources(result: Dict[str, Any]) -> List[str]:
    route = result.get("route") if isinstance(result.get("route"), dict) else {}
    fills = route.get("fills") if isinstance(route.get("fills"), list) else []
    out: List[str] = []
    for fill in fills:
        if not isinstance(fill, dict):
            continue
        source = str(fill.get("source") or "").strip()
        if source and source not in out:
            out.append(source)
    return out[:12]


def _address_key(value: Any) -> str:
    return str(value or "").strip().lower()


def _network_fee_eth(result: Dict[str, Any]) -> Optional[str]:
    raw = str(result.get("total_network_fee") or "").strip()
    if not raw.isdigit():
        return None
    return _decimal_text(Decimal(raw) / Decimal(10**18))


def _normalize_zero_x_fee(
    result: Dict[str, Any],
    *,
    eth_token: Dict[str, Any],
    usdg_token: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    fees = result.get("fees") if isinstance(result.get("fees"), dict) else {}
    raw = fees.get("zeroExFee") if isinstance(fees.get("zeroExFee"), dict) else None
    if raw is None:
        return None

    amount_atomic = str(raw.get("amount") or "").strip()
    token_address = str(raw.get("token") or "").strip()
    if not amount_atomic.isdigit():
        return {
            "amount_atomic": None,
            "amount": None,
            "asset": None,
            "token": token_address or None,
            "type": str(raw.get("type") or "").strip() or None,
        }

    token_map = {
        _address_key(eth_token.get("contract_address")): eth_token,
        _address_key(usdg_token.get("contract_address")): usdg_token,
    }
    token = token_map.get(_address_key(token_address))
    decimals = int(token.get("decimals")) if token is not None else None
    amount = None
    if decimals is not None and 0 <= decimals <= 18:
        amount = _decimal_text(Decimal(amount_atomic) / (Decimal(10) ** decimals))

    return {
        "amount_atomic": amount_atomic,
        "amount": amount,
        "asset": str(token.get("symbol") or "").strip().upper() if token else None,
        "token": token_address or None,
        "type": str(raw.get("type") or "").strip() or None,
    }


def _safe_failure(result: Dict[str, Any], *, context: str) -> Dict[str, Any]:
    return {
        "ok": False,
        "error": str(result.get("error") or "robinhood_chain_quote_failed"),
        "context": context,
        "provider": str(result.get("provider") or ROBINHOOD_CHAIN_QUOTE_PROVIDER),
        "provider_error": result.get("provider_error"),
        "http_status": result.get("http_status"),
        "backoff_until": result.get("backoff_until"),
        "read_only": True,
        "quote_only": True,
        "synthetic": True,
        "resting_order": False,
        "execution_enabled": False,
        "signing_enabled": False,
        "transaction_construction_enabled": False,
        "transaction_calldata": None,
        "will_mutate": False,
    }


class RobinhoodChainQuoteService:
    """Bounded quote-only market view built from 0x indicative prices.

    The service reuses RH-CHAIN.10A's fixed-token, fixed-chain discovery client.
    It never requests a firm quote, exposes calldata, constructs a transaction,
    writes an order, asks a wallet to sign, or broadcasts anything.
    """

    def __init__(
        self,
        *,
        discovery_service: Optional[RobinhoodChainExecutionDiscoveryService] = None,
    ) -> None:
        self.discovery_service = discovery_service or get_robinhood_chain_execution_discovery_service()
        self._book_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
        self._book_cache_lock = asyncio.Lock()

    def status(self) -> Dict[str, Any]:
        discovery = self.discovery_service.status()
        return {
            "ok": True,
            "venue": "robinhood_chain",
            "network": "robinhood_chain",
            "chain_id": 4663,
            "chain_id_hex": "0x1237",
            "mainnet_only": True,
            "provider": ROBINHOOD_CHAIN_QUOTE_PROVIDER,
            "provider_configured": bool(discovery.get("provider_configured")),
            "api_key_configured": bool(discovery.get("api_key_configured")),
            "credential_source": discovery.get("credential_source"),
            "supported_symbols": list(ROBINHOOD_CHAIN_REVIEW_QUOTE_SYMBOLS),
            "max_book_levels_per_side": ROBINHOOD_CHAIN_MAX_BOOK_LEVELS,
            "bid_input_amounts_eth": list(ROBINHOOD_CHAIN_BID_INPUT_AMOUNTS),
            "ask_input_amounts_usdg": list(ROBINHOOD_CHAIN_ASK_INPUT_AMOUNTS),
            "cache_ttl_s": float(discovery.get("cache_ttl_s") or 0.0),
            "error_backoff_s": float(discovery.get("error_backoff_s") or 0.0),
            "discovery_max_sell_usd": discovery.get("discovery_max_sell_usd"),
            "read_only": True,
            "quote_only": True,
            "synthetic_orderbook": True,
            "resting_orders": False,
            "execution_enabled": False,
            "signing_enabled": False,
            "transaction_construction_enabled": False,
            "firm_quote_enabled": False,
            "exact_input_enabled": True,
            "exact_output_enabled": False,
            "capability_policy": "live_verified_pair_direction_amount_mode",
            "route_capabilities": [],
            "pair_capability_source": "database_router",
            "will_mutate": False,
        }

    def _normalize_quote(
        self,
        result: Dict[str, Any],
        *,
        side: str,
        symbol: str,
        base_token: Dict[str, Any],
        quote_token: Dict[str, Any],
        route_capability: Optional[Dict[str, Any]] = None,
        amount_mode: str = "exact_input",
    ) -> Dict[str, Any]:
        normalized_side = _normalize_side(side)
        normalized_amount_mode = str(amount_mode or "exact_input").strip().lower()
        if normalized_amount_mode not in {"exact_input", "exact_output"}:
            return _safe_failure(
                {"error": "invalid_quote_amount_mode", "provider": result.get("provider")},
                context="indicative",
            )
        sell_amount = _safe_decimal(result.get("sell_amount"))
        buy_amount = _safe_decimal(result.get("buy_amount"))
        min_buy_amount = _safe_decimal(result.get("min_buy_amount"))
        if sell_amount is None or buy_amount is None or sell_amount <= 0 or buy_amount <= 0:
            return _safe_failure(
                {"error": "invalid_provider_quote_amounts", "provider": result.get("provider")},
                context="indicative",
            )

        base_symbol = _token_symbol(base_token)
        quote_symbol = _token_symbol(quote_token)
        if normalized_side == "sell":
            sell_token = base_token
            base_quantity = sell_amount
            quote_quantity = buy_amount
            input_asset = base_symbol
            output_asset = quote_symbol
            input_amount = sell_amount
            output_amount = buy_amount
            minimum_received = min_buy_amount
            minimum_received_asset = quote_symbol
        else:
            sell_token = quote_token
            base_quantity = buy_amount
            quote_quantity = sell_amount
            input_asset = quote_symbol
            output_asset = base_symbol
            input_amount = sell_amount
            output_amount = buy_amount
            minimum_received = min_buy_amount
            minimum_received_asset = base_symbol

        if base_quantity <= 0 or quote_quantity <= 0:
            return _safe_failure(
                {"error": "invalid_provider_quote_price", "provider": result.get("provider")},
                context="indicative",
            )

        effective_price = quote_quantity / base_quantity
        sources = _route_sources(result)
        sell_is_native = bool(sell_token.get("native"))
        allowance_required = not sell_is_native and bool(result.get("allowance_required"))
        provider_warnings = list(result.get("provider_warnings") or [])[:20]
        if sell_is_native:
            provider_warnings = [warning for warning in provider_warnings if str(warning) != "allowance_required"]
        return {
            "ok": True,
            "venue": "robinhood_chain",
            "network": "robinhood_chain",
            "chain_id": 4663,
            "chain_id_hex": "0x1237",
            "mainnet_only": True,
            "provider": ROBINHOOD_CHAIN_QUOTE_PROVIDER,
            "symbol": symbol,
            "side": normalized_side,
            "amount_mode": normalized_amount_mode,
            "input_asset": input_asset,
            "input_amount": _decimal_text(input_amount),
            "output_asset": output_asset,
            "output_amount": _decimal_text(output_amount),
            "base_asset": base_symbol,
            "quote_asset": quote_symbol,
            "base_quantity": _decimal_text(base_quantity),
            "quote_quantity": _decimal_text(quote_quantity),
            "effective_price": _decimal_text(effective_price),
            "minimum_received": _decimal_text(minimum_received) if minimum_received is not None else None,
            "minimum_received_asset": minimum_received_asset if minimum_received is not None else None,
            "maximum_input_ceiling": _decimal_text(ROBINHOOD_CHAIN_MAXIMUM_BUY_USDG)
            if normalized_side == "buy" and normalized_amount_mode == "exact_output" and quote_symbol == "USDG"
            else None,
            "maximum_input_ceiling_atomic": "2000000"
            if normalized_side == "buy" and normalized_amount_mode == "exact_output" and quote_symbol == "USDG"
            else None,
            "maximum_input_ceiling_asset": quote_symbol
            if normalized_side == "buy" and normalized_amount_mode == "exact_output"
            else None,
            "price_impact_bps": None,
            "route_sources": sources,
            "route_source": sources[0] if sources else None,
            "route_fill_count": int((result.get("route") or {}).get("fill_count") or 0)
            if isinstance(result.get("route"), dict)
            else 0,
            "block_number": result.get("block_number"),
            "gas": result.get("gas"),
            "gas_price": result.get("gas_price"),
            "total_network_fee_wei": result.get("total_network_fee"),
            "total_network_fee_eth": _network_fee_eth(result),
            "zero_x_fee": _normalize_zero_x_fee(
                result,
                eth_token=base_token,
                usdg_token=quote_token,
            ),
            "allowance_required": allowance_required,
            "allowance_spender": result.get("allowance_spender") if allowance_required else None,
            "provider_warnings": provider_warnings,
            "liquidity_available": bool(result.get("liquidity_available")),
            "route_capability": copy.deepcopy(route_capability) if isinstance(route_capability, dict) else None,
            "token_identity_source": "token_registry",
            "pair_capability_source": "database" if isinstance(route_capability, dict) else "legacy",
            "cached": bool(result.get("cached")),
            "elapsed_ms": result.get("elapsed_ms"),
            "fetched_at": result.get("fetched_at"),
            "read_only": True,
            "quote_only": True,
            "synthetic": True,
            "resting_order": False,
            "execution_enabled": False,
            "signing_enabled": False,
            "transaction_construction_enabled": False,
            "firm_quote": False,
            "transaction_destination": None,
            "transaction_data_present": False,
            "transaction_data_bytes": 0,
            "transaction_calldata": None,
            "will_mutate": False,
        }

    async def _probe_quote(
        self,
        *,
        symbol: str,
        side: str,
        requested_amount: str,
        amount_mode: str,
        taker_address: str,
        base_token: Dict[str, Any],
        quote_token: Dict[str, Any],
        route_capability: Optional[Dict[str, Any]],
        force_refresh: bool,
    ) -> Dict[str, Any]:
        normalized_side = _normalize_side(side)
        normalized_amount_mode = str(amount_mode or "exact_input").strip().lower()
        if normalized_side == "sell":
            sell_token = base_token
            buy_token = quote_token
            sell_amount = requested_amount
            buy_amount = None
        elif normalized_amount_mode == "exact_output":
            sell_token = quote_token
            buy_token = base_token
            sell_amount = None
            buy_amount = requested_amount
        else:
            sell_token = quote_token
            buy_token = base_token
            sell_amount = requested_amount
            buy_amount = None

        probe_kwargs: Dict[str, Any] = {
            "sell_token": sell_token,
            "buy_token": buy_token,
            "sell_amount": sell_amount,
            "buy_amount": buy_amount,
            "taker_address": taker_address,
            "force_refresh": force_refresh,
        }
        if isinstance(route_capability, dict):
            probe_kwargs.update(
                {
                    "route_capability": route_capability,
                    "require_live_verified": False,
                    "max_probe_amount": requested_amount,
                }
            )
        result = await self.discovery_service.probe(**probe_kwargs)
        if not result.get("ok"):
            failure = _safe_failure(result, context="indicative")
            failure["symbol"] = symbol
            failure["route_capability"] = copy.deepcopy(route_capability) if isinstance(route_capability, dict) else None
            return failure
        quote = self._normalize_quote(
            result,
            side=normalized_side,
            symbol=symbol,
            base_token=base_token,
            quote_token=quote_token,
            route_capability=route_capability,
            amount_mode=normalized_amount_mode,
        )
        if (
            quote.get("ok")
            and normalized_side == "buy"
            and normalized_amount_mode == "exact_output"
        ):
            output_amount = _safe_decimal(quote.get("output_amount"))
            input_amount = _safe_decimal(quote.get("input_amount"))
            requested = _safe_decimal(requested_amount)
            if output_amount != requested:
                return _safe_failure(
                    {"error": "exact_output_quote_amount_mismatch", "provider": result.get("provider")},
                    context="indicative",
                )
            if input_amount is None or input_amount > ROBINHOOD_CHAIN_MAXIMUM_BUY_USDG:
                return _safe_failure(
                    {"error": "exact_output_quote_exceeds_maximum_usdg", "provider": result.get("provider")},
                    context="indicative",
                )
        return quote

    async def indicative_quote(
        self,
        *,
        symbol: str,
        side: str,
        quantity: Optional[str],
        total_quote: Optional[str],
        taker_address: str,
        eth_token: Optional[Dict[str, Any]] = None,
        usdg_token: Optional[Dict[str, Any]] = None,
        base_token: Optional[Dict[str, Any]] = None,
        quote_token: Optional[Dict[str, Any]] = None,
        route_capability: Optional[Dict[str, Any]] = None,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        normalized_symbol = _normalize_symbol(symbol)
        resolved_base = base_token if isinstance(base_token, dict) else eth_token
        resolved_quote = quote_token if isinstance(quote_token, dict) else usdg_token
        base_symbol = _token_symbol(resolved_base or {})
        quote_symbol = _token_symbol(resolved_quote or {})
        if (
            normalized_symbol not in ROBINHOOD_CHAIN_REVIEW_QUOTE_SYMBOLS
            or normalized_symbol != f"{base_symbol}-{quote_symbol}"
        ):
            return _safe_failure(
                {
                    "error": "unsupported_robinhood_chain_quote_symbol",
                    "provider": ROBINHOOD_CHAIN_QUOTE_PROVIDER,
                },
                context="indicative",
            )
        try:
            normalized_side = _normalize_side(side)
            exact_output_buy = (
                normalized_side == "buy"
                and str(quantity or "").strip() != ""
                and str(total_quote or "").strip() == ""
            )
            amount_mode = "exact_output" if exact_output_buy else "exact_input"
            requested = _decimal(
                quantity if exact_output_buy or normalized_side == "sell" else total_quote,
                field="exact_output_quantity" if exact_output_buy else ("quote_amount" if normalized_side == "buy" else "quantity"),
            )
        except ValueError as exc:
            return _safe_failure(
                {"error": str(exc), "provider": ROBINHOOD_CHAIN_QUOTE_PROVIDER},
                context="indicative",
            )

        if (
            amount_mode == "exact_input"
            and normalized_symbol != ROBINHOOD_CHAIN_QUOTE_SYMBOL
            and not self._pair_capability_available(route_capability)
        ):
            failure = _safe_failure(
                {
                    "error": "robinhood_chain_quote_route_unavailable",
                    "provider": ROBINHOOD_CHAIN_QUOTE_PROVIDER,
                },
                context="indicative",
            )
            failure["symbol"] = normalized_symbol
            failure["route_capability"] = copy.deepcopy(route_capability) if isinstance(route_capability, dict) else None
            failure["provider_contacted"] = False
            return failure

        if amount_mode == "exact_output":
            capability = {
                "from_asset": quote_symbol,
                "to_asset": base_symbol,
                "amount_mode": "exact_output",
                "display_mode": "exact_receive",
                "provider": ROBINHOOD_CHAIN_QUOTE_PROVIDER,
                "indicative_status": "provider_failure",
                "firm_plan_status": "provider_failure",
                "execution_status": "held",
                "enabled": False,
                "reason": "Exact-receive remains blocked pending direct-router research.",
                "capability_source": "local_fail_closed_policy",
            }
            failure = _safe_failure(
                {
                    "error": "robinhood_chain_exact_receive_route_unavailable",
                    "provider": ROBINHOOD_CHAIN_QUOTE_PROVIDER,
                },
                context="indicative",
            )
            failure.update(
                {
                    "symbol": normalized_symbol,
                    "amount_mode": "exact_output",
                    "display_mode": "exact_receive",
                    "input_asset": quote_symbol,
                    "output_asset": base_symbol,
                    "requested_output": _decimal_text(requested),
                    "maximum_input_ceiling": _decimal_text(ROBINHOOD_CHAIN_MAXIMUM_BUY_USDG)
                    if quote_symbol == "USDG"
                    else None,
                    "maximum_input_ceiling_atomic": "2000000" if quote_symbol == "USDG" else None,
                    "route_capability": capability,
                    "provider_contacted": False,
                    "backoff_activated": False,
                }
            )
            return failure

        quote = await self._probe_quote(
            symbol=normalized_symbol,
            side=normalized_side,
            requested_amount=_decimal_text(requested),
            amount_mode=amount_mode,
            taker_address=taker_address,
            base_token=resolved_base or {},
            quote_token=resolved_quote or {},
            route_capability=route_capability,
            force_refresh=force_refresh,
        )
        if not quote.get("ok"):
            return quote

        capability_reference = _safe_decimal((route_capability or {}).get("probe_amount"))
        reference_amount = _decimal_text(capability_reference) if capability_reference and capability_reference > 0 else (
            ROBINHOOD_CHAIN_ASK_INPUT_AMOUNTS[0]
            if normalized_side == "buy"
            else ROBINHOOD_CHAIN_BID_INPUT_AMOUNTS[0]
        )
        reference_price: Optional[Decimal] = None
        if _decimal_text(requested) == reference_amount:
            reference_price = _safe_decimal(quote.get("effective_price"))
        else:
            reference = await self._probe_quote(
                symbol=normalized_symbol,
                side=normalized_side,
                requested_amount=reference_amount,
                amount_mode="exact_input",
                taker_address=taker_address,
                base_token=resolved_base or {},
                quote_token=resolved_quote or {},
                route_capability=route_capability,
                force_refresh=False,
            )
            if reference.get("ok"):
                reference_price = _safe_decimal(reference.get("effective_price"))

        effective = _safe_decimal(quote.get("effective_price"))
        if reference_price is not None and effective is not None and reference_price > 0:
            if normalized_side == "sell":
                impact = max(Decimal("0"), (reference_price - effective) / reference_price * Decimal(10000))
            else:
                impact = max(Decimal("0"), (effective - reference_price) / reference_price * Decimal(10000))
            quote["price_impact_bps"] = _decimal_text(impact)
            quote["reference_price"] = _decimal_text(reference_price)
            quote["reference_input_amount"] = reference_amount
        return quote

    def _book_cache_ttl(self) -> float:
        try:
            return max(0.0, min(float(self.discovery_service.status().get("cache_ttl_s") or 0.0), 300.0))
        except Exception:
            return 0.0

    async def _cached_book(self, key: str) -> Optional[Dict[str, Any]]:
        ttl = self._book_cache_ttl()
        if ttl <= 0:
            return None
        async with self._book_cache_lock:
            entry = self._book_cache.get(key)
            if entry is None:
                return None
            expires_at, payload = entry
            if time.monotonic() >= expires_at:
                self._book_cache.pop(key, None)
                return None
            out = copy.deepcopy(payload)
            out["cached"] = True
            out["cache_mixed"] = False
            out["snapshot_source"] = "synthetic_book_cache"
            for row in [*(out.get("bids") or []), *(out.get("asks") or [])]:
                if isinstance(row, dict):
                    row["cached"] = True
                    row["snapshot_source"] = "synthetic_book_cache"
            return out

    async def _store_book(self, key: str, payload: Dict[str, Any]) -> None:
        ttl = self._book_cache_ttl()
        if ttl <= 0:
            return
        async with self._book_cache_lock:
            self._book_cache[key] = (time.monotonic() + ttl, copy.deepcopy(payload))
            if len(self._book_cache) > 20:
                oldest = next(iter(self._book_cache), None)
                if oldest is not None:
                    self._book_cache.pop(oldest, None)

    @staticmethod
    def _book_level(quote: Dict[str, Any], *, side: str, sample_input: str) -> Dict[str, Any]:
        return {
            "price": quote.get("effective_price"),
            "size": quote.get("base_quantity"),
            "side": side,
            "sample_input_amount": sample_input,
            "input_asset": quote.get("input_asset"),
            "input_amount": quote.get("input_amount"),
            "output_asset": quote.get("output_asset"),
            "output_amount": quote.get("output_amount"),
            "minimum_received": quote.get("minimum_received"),
            "minimum_received_asset": quote.get("minimum_received_asset"),
            "route_source": quote.get("route_source"),
            "route_sources": quote.get("route_sources") or [],
            "allowance_required": bool(quote.get("allowance_required")),
            "allowance_spender": quote.get("allowance_spender"),
            "network_fee_wei": quote.get("total_network_fee_wei"),
            "network_fee_eth": quote.get("total_network_fee_eth"),
            "zero_x_fee": quote.get("zero_x_fee"),
            "cached": bool(quote.get("cached")),
            "fetched_at": quote.get("fetched_at"),
            "synthetic": True,
            "resting_order": False,
            "quote_only": True,
            "provider": ROBINHOOD_CHAIN_QUOTE_PROVIDER,
            "liquidity_type": "synthetic_quote_sample",
            "liquidity_label": "SYNTH",
            "source_type": "robinhood_chain_0x_indicative",
        }

    async def synthetic_orderbook(
        self,
        *,
        symbol: str,
        depth: int,
        taker_address: str,
        eth_token: Dict[str, Any],
        usdg_token: Dict[str, Any],
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        normalized_symbol = _normalize_symbol(symbol)
        if normalized_symbol != ROBINHOOD_CHAIN_QUOTE_SYMBOL:
            return _safe_failure(
                {"error": "unsupported_robinhood_chain_quote_symbol", "provider": ROBINHOOD_CHAIN_QUOTE_PROVIDER},
                context="orderbook",
            )
        levels = max(1, min(int(depth), ROBINHOOD_CHAIN_MAX_BOOK_LEVELS))
        cache_key = f"{normalized_symbol}|{taker_address.lower()}|{levels}"
        if not force_refresh:
            cached = await self._cached_book(cache_key)
            if cached is not None:
                return cached

        bids: List[Dict[str, Any]] = []
        asks: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []

        for amount in ROBINHOOD_CHAIN_BID_INPUT_AMOUNTS[:levels]:
            quote = await self._probe_quote(
                side="sell",
                requested_amount=amount,
                amount_mode="exact_input",
                taker_address=taker_address,
                symbol=normalized_symbol,
                base_token=eth_token,
                quote_token=usdg_token,
                route_capability=None,
                force_refresh=force_refresh,
            )
            if quote.get("ok") and quote.get("liquidity_available"):
                bids.append(self._book_level(quote, side="bid", sample_input=amount))
            else:
                errors.append({
                    "side": "bid",
                    "sample_input_amount": amount,
                    "error": quote.get("error"),
                    "backoff_until": quote.get("backoff_until"),
                })

        for amount in ROBINHOOD_CHAIN_ASK_INPUT_AMOUNTS[:levels]:
            quote = await self._probe_quote(
                side="buy",
                requested_amount=amount,
                amount_mode="exact_input",
                taker_address=taker_address,
                symbol=normalized_symbol,
                base_token=eth_token,
                quote_token=usdg_token,
                route_capability=None,
                force_refresh=force_refresh,
            )
            if quote.get("ok") and quote.get("liquidity_available"):
                asks.append(self._book_level(quote, side="ask", sample_input=amount))
            else:
                errors.append({
                    "side": "ask",
                    "sample_input_amount": amount,
                    "error": quote.get("error"),
                    "backoff_until": quote.get("backoff_until"),
                })

        bids.sort(key=lambda row: _safe_decimal(row.get("price")) or Decimal("0"), reverse=True)
        asks.sort(key=lambda row: _safe_decimal(row.get("price")) or Decimal("0"))

        best_bid = _safe_decimal(bids[0].get("price")) if bids else None
        best_ask = _safe_decimal(asks[0].get("price")) if asks else None
        spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None
        midpoint = (best_ask + best_bid) / Decimal(2) if best_bid is not None and best_ask is not None else None
        spread_bps = spread / midpoint * Decimal(10000) if spread is not None and midpoint and midpoint > 0 else None

        sources: List[str] = []
        for row in [*bids, *asks]:
            for source in row.get("route_sources") or []:
                text = str(source or "").strip()
                if text and text not in sources:
                    sources.append(text)

        all_rows = [*bids, *asks]
        fetched_values = [str(row.get("fetched_at") or "") for row in all_rows if row.get("fetched_at")]
        payload: Dict[str, Any] = {
            "ok": bool(bids and asks),
            "venue": "robinhood_chain",
            "network": "robinhood_chain",
            "chain_id": 4663,
            "chain_id_hex": "0x1237",
            "mainnet_only": True,
            "provider": ROBINHOOD_CHAIN_QUOTE_PROVIDER,
            "router": ROBINHOOD_CHAIN_QUOTE_PROVIDER,
            "symbol": normalized_symbol,
            "resolvedSymbol": normalized_symbol,
            "base_asset": "ETH",
            "quote_asset": "USDG",
            "depth_requested": int(depth),
            "depth_returned": min(len(bids), len(asks)),
            "max_depth": ROBINHOOD_CHAIN_MAX_BOOK_LEVELS,
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
            "priceDecimals": 6,
            "sizeDecimals": 8,
            "cached": bool(all_rows) and all(bool(row.get("cached")) for row in all_rows),
            "cache_mixed": bool(all_rows) and any(bool(row.get("cached")) for row in all_rows) and not all(bool(row.get("cached")) for row in all_rows),
            "fetched_at": max(fetched_values) if fetched_values else None,
            "snapshot_source": "0x_indicative_samples",
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
        }
        if not payload["ok"]:
            payload["error"] = "synthetic_orderbook_liquidity_incomplete"
        else:
            await self._store_book(cache_key, payload)
        return payload


    @staticmethod
    def _pair_capability_available(capability: Optional[Dict[str, Any]]) -> bool:
        if not isinstance(capability, dict):
            return False
        return (
            str(capability.get("amount_mode") or "").strip().lower() == "exact_input"
            and str(capability.get("indicative_status") or "").strip().lower() in {"available", "live_verified"}
            and str(capability.get("mechanism") or "swap").strip().lower() == "swap"
        )

    async def _probe_pair_quote(
        self,
        *,
        symbol: str,
        base_token: Dict[str, Any],
        quote_token: Dict[str, Any],
        sell_token: Dict[str, Any],
        buy_token: Dict[str, Any],
        requested_amount: str,
        taker_address: str,
        capability: Dict[str, Any],
        force_refresh: bool,
    ) -> Dict[str, Any]:
        result = await self.discovery_service.probe(
            sell_token=sell_token,
            buy_token=buy_token,
            sell_amount=requested_amount,
            buy_amount=None,
            taker_address=taker_address,
            force_refresh=force_refresh,
            route_capability=capability,
            require_live_verified=False,
            max_probe_amount=requested_amount,
        )
        if not result.get("ok"):
            failure = _safe_failure(result, context="orderbook")
            failure["symbol"] = symbol
            failure["route_capability"] = copy.deepcopy(capability)
            return failure

        sell_amount = _safe_decimal(result.get("sell_amount"))
        buy_amount = _safe_decimal(result.get("buy_amount"))
        min_buy_amount = _safe_decimal(result.get("min_buy_amount"))
        if sell_amount is None or buy_amount is None or sell_amount <= 0 or buy_amount <= 0:
            failure = _safe_failure(
                {"error": "invalid_provider_quote_amounts", "provider": result.get("provider")},
                context="orderbook",
            )
            failure["symbol"] = symbol
            return failure

        base_symbol = _token_symbol(base_token)
        quote_symbol = _token_symbol(quote_token)
        sell_symbol = _token_symbol(sell_token)
        buy_symbol = _token_symbol(buy_token)
        if {base_symbol, quote_symbol} != {sell_symbol, buy_symbol}:
            failure = _safe_failure(
                {"error": "robinhood_chain_pair_identity_mismatch", "provider": result.get("provider")},
                context="orderbook",
            )
            failure["symbol"] = symbol
            return failure

        if sell_symbol == base_symbol and buy_symbol == quote_symbol:
            book_side = "bid"
            base_quantity = sell_amount
            quote_quantity = buy_amount
            effective_price = quote_quantity / base_quantity
        elif sell_symbol == quote_symbol and buy_symbol == base_symbol:
            book_side = "ask"
            base_quantity = buy_amount
            quote_quantity = sell_amount
            effective_price = quote_quantity / base_quantity
        else:
            failure = _safe_failure(
                {"error": "robinhood_chain_pair_direction_mismatch", "provider": result.get("provider")},
                context="orderbook",
            )
            failure["symbol"] = symbol
            return failure

        sources = _route_sources(result)
        return {
            "ok": True,
            "symbol": symbol,
            "side": book_side,
            "amount_mode": "exact_input",
            "input_asset": sell_symbol,
            "input_amount": _decimal_text(sell_amount),
            "output_asset": buy_symbol,
            "output_amount": _decimal_text(buy_amount),
            "base_asset": base_symbol,
            "quote_asset": quote_symbol,
            "base_quantity": _decimal_text(base_quantity),
            "quote_quantity": _decimal_text(quote_quantity),
            "effective_price": _decimal_text(effective_price),
            "minimum_received": _decimal_text(min_buy_amount) if min_buy_amount is not None else None,
            "minimum_received_asset": buy_symbol if min_buy_amount is not None else None,
            "route_sources": sources,
            "route_source": sources[0] if sources else None,
            "allowance_required": bool(result.get("allowance_required")),
            "allowance_spender": result.get("allowance_spender"),
            "total_network_fee_wei": result.get("total_network_fee"),
            "total_network_fee_eth": _network_fee_eth(result),
            "zero_x_fee": _normalize_zero_x_fee(
                result,
                eth_token=base_token,
                usdg_token=quote_token,
            ),
            "cached": bool(result.get("cached")),
            "fetched_at": result.get("fetched_at"),
            "liquidity_available": bool(result.get("liquidity_available")),
            "route_capability": copy.deepcopy(capability),
            "read_only": True,
            "quote_only": True,
            "execution_enabled": False,
            "transaction_calldata": None,
            "will_mutate": False,
        }

    @staticmethod
    def _pair_book_level(quote: Dict[str, Any], *, sample_input: str) -> Dict[str, Any]:
        return {
            "price": quote.get("effective_price"),
            "size": quote.get("base_quantity"),
            "side": quote.get("side"),
            "sample_input_amount": sample_input,
            "input_asset": quote.get("input_asset"),
            "input_amount": quote.get("input_amount"),
            "output_asset": quote.get("output_asset"),
            "output_amount": quote.get("output_amount"),
            "minimum_received": quote.get("minimum_received"),
            "minimum_received_asset": quote.get("minimum_received_asset"),
            "route_source": quote.get("route_source"),
            "route_sources": quote.get("route_sources") or [],
            "allowance_required": bool(quote.get("allowance_required")),
            "allowance_spender": quote.get("allowance_spender"),
            "network_fee_wei": quote.get("total_network_fee_wei"),
            "network_fee_eth": quote.get("total_network_fee_eth"),
            "zero_x_fee": quote.get("zero_x_fee"),
            "cached": bool(quote.get("cached")),
            "fetched_at": quote.get("fetched_at"),
            "synthetic": True,
            "resting_order": False,
            "quote_only": True,
            "provider": ROBINHOOD_CHAIN_QUOTE_PROVIDER,
            "liquidity_type": "synthetic_quote_sample",
            "liquidity_label": "SYNTH",
            "source_type": "robinhood_chain_0x_indicative",
        }

    async def synthetic_orderbook_for_pair(
        self,
        *,
        symbol: str,
        depth: int,
        taker_address: str,
        base_token: Dict[str, Any],
        quote_token: Dict[str, Any],
        base_to_quote_capability: Dict[str, Any],
        quote_to_base_capability: Dict[str, Any],
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        normalized_symbol = _normalize_symbol(symbol)
        base_symbol = _token_symbol(base_token)
        quote_symbol = _token_symbol(quote_token)
        if normalized_symbol != f"{base_symbol}-{quote_symbol}":
            return _safe_failure(
                {"error": "robinhood_chain_pair_identity_mismatch", "provider": ROBINHOOD_CHAIN_QUOTE_PROVIDER},
                context="orderbook",
            )
        if not self._pair_capability_available(base_to_quote_capability):
            failure = _safe_failure(
                {"error": "robinhood_chain_bid_direction_unavailable", "provider": ROBINHOOD_CHAIN_QUOTE_PROVIDER},
                context="orderbook",
            )
            failure["route_capability"] = copy.deepcopy(base_to_quote_capability)
            failure["provider_contacted"] = False
            return failure
        if not self._pair_capability_available(quote_to_base_capability):
            failure = _safe_failure(
                {"error": "robinhood_chain_ask_direction_unavailable", "provider": ROBINHOOD_CHAIN_QUOTE_PROVIDER},
                context="orderbook",
            )
            failure["route_capability"] = copy.deepcopy(quote_to_base_capability)
            failure["provider_contacted"] = False
            return failure

        levels = max(1, min(int(depth), ROBINHOOD_CHAIN_MAX_BOOK_LEVELS))
        try:
            bid_amounts = _bounded_sample_amounts(
                base_to_quote_capability.get("probe_amount"),
                int(base_token.get("decimals")),
                levels,
            )
            ask_amounts = _bounded_sample_amounts(
                quote_to_base_capability.get("probe_amount"),
                int(quote_token.get("decimals")),
                levels,
            )
        except (ValueError, TypeError) as exc:
            return _safe_failure(
                {"error": str(exc), "provider": ROBINHOOD_CHAIN_QUOTE_PROVIDER},
                context="orderbook",
            )

        cache_key = f"pair|{normalized_symbol}|{taker_address.lower()}|{levels}|{','.join(bid_amounts)}|{','.join(ask_amounts)}"
        if not force_refresh:
            cached = await self._cached_book(cache_key)
            if cached is not None:
                return cached

        bids: List[Dict[str, Any]] = []
        asks: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []

        for amount in bid_amounts:
            quote = await self._probe_pair_quote(
                symbol=normalized_symbol,
                base_token=base_token,
                quote_token=quote_token,
                sell_token=base_token,
                buy_token=quote_token,
                requested_amount=amount,
                taker_address=taker_address,
                capability=base_to_quote_capability,
                force_refresh=force_refresh,
            )
            if quote.get("ok") and quote.get("liquidity_available"):
                bids.append(self._pair_book_level(quote, sample_input=amount))
            else:
                errors.append({
                    "side": "bid",
                    "sample_input_amount": amount,
                    "error": quote.get("error"),
                    "backoff_until": quote.get("backoff_until"),
                })

        for amount in ask_amounts:
            quote = await self._probe_pair_quote(
                symbol=normalized_symbol,
                base_token=base_token,
                quote_token=quote_token,
                sell_token=quote_token,
                buy_token=base_token,
                requested_amount=amount,
                taker_address=taker_address,
                capability=quote_to_base_capability,
                force_refresh=force_refresh,
            )
            if quote.get("ok") and quote.get("liquidity_available"):
                asks.append(self._pair_book_level(quote, sample_input=amount))
            else:
                errors.append({
                    "side": "ask",
                    "sample_input_amount": amount,
                    "error": quote.get("error"),
                    "backoff_until": quote.get("backoff_until"),
                })

        bids.sort(key=lambda row: _safe_decimal(row.get("price")) or Decimal("0"), reverse=True)
        asks.sort(key=lambda row: _safe_decimal(row.get("price")) or Decimal("0"))
        best_bid = _safe_decimal(bids[0].get("price")) if bids else None
        best_ask = _safe_decimal(asks[0].get("price")) if asks else None
        spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None
        midpoint = (best_ask + best_bid) / Decimal(2) if best_bid is not None and best_ask is not None else None
        spread_bps = spread / midpoint * Decimal(10000) if spread is not None and midpoint and midpoint > 0 else None
        sources: List[str] = []
        for row in [*bids, *asks]:
            for source in row.get("route_sources") or []:
                text = str(source or "").strip()
                if text and text not in sources:
                    sources.append(text)
        all_rows = [*bids, *asks]
        fetched_values = [str(row.get("fetched_at") or "") for row in all_rows if row.get("fetched_at")]
        payload: Dict[str, Any] = {
            "ok": bool(bids and asks),
            "tranche": "RH-CHAIN.10D.2-R5C.2",
            "venue": "robinhood_chain",
            "network": "robinhood_chain",
            "chain_id": 4663,
            "chain_id_hex": "0x1237",
            "mainnet_only": True,
            "provider": ROBINHOOD_CHAIN_QUOTE_PROVIDER,
            "router": ROBINHOOD_CHAIN_QUOTE_PROVIDER,
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
            "max_depth": ROBINHOOD_CHAIN_MAX_BOOK_LEVELS,
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
            "priceDecimals": max(6, min(12, int(quote_token.get("decimals") or 0))),
            "sizeDecimals": max(0, min(18, int(base_token.get("decimals") or 0))),
            "cached": bool(all_rows) and all(bool(row.get("cached")) for row in all_rows),
            "cache_mixed": bool(all_rows) and any(bool(row.get("cached")) for row in all_rows) and not all(bool(row.get("cached")) for row in all_rows),
            "fetched_at": max(fetched_values) if fetched_values else None,
            "snapshot_source": "0x_database_capability_samples",
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
        }
        if not payload["ok"]:
            payload["error"] = "synthetic_orderbook_liquidity_incomplete"
        else:
            await self._store_book(cache_key, payload)
        return payload


_SERVICE: Optional[RobinhoodChainQuoteService] = None


def get_robinhood_chain_quote_service() -> RobinhoodChainQuoteService:
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = RobinhoodChainQuoteService()
    return _SERVICE
