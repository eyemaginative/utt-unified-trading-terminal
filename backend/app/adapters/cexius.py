# backend/app/adapters/cexius.py
#
# Cexius REST adapter.
# Venue key: "cexius"
#
# CEXIUS.1A/1B + CEXIUS.2A/2B boundary:
# - Public market metadata, markets, rules, tickers, order books, trades, candles, and fees.
# - Authenticated balances, histories, open orders, order details, and order trades.
# - Limit BUY/SELL orders may be created through POST /trading/order.
# - One exact selected order may be canceled through DELETE /trading/orders/{id}.
# - Market orders, cancel-all, deposits, withdrawals, and every other mutation remain disabled.

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote

import httpx

from .base import BalanceItem, ExchangeAdapter, OrderBook, OrderRules, PlacedOrder, VenueOrder
from ..config import settings


class CexiusAdapter(ExchangeAdapter):
    venue = "cexius"

    _DEFAULT_BASE_URL = "https://cexius.com/api/v2"
    _TIMEOUT_S = 15.0
    _MARKETS_CACHE_TTL_S = 300.0

    def __init__(self) -> None:
        self._markets_cache_at: float = 0.0
        self._markets_cache: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    # HTTP and response helpers
    # ------------------------------------------------------------------
    def _base_url(self) -> str:
        fn = getattr(settings, "cexius_effective_base_url", None)
        if callable(fn):
            try:
                value = str(fn() or "").strip().rstrip("/")
                if value:
                    return value
            except Exception:
                pass
        value = str(getattr(settings, "cexius_base_url", "") or "").strip().rstrip("/")
        return value or self._DEFAULT_BASE_URL

    def _require_token(self) -> str:
        fn = getattr(settings, "cexius_private_token", None)
        token = ""
        if callable(fn):
            try:
                token = str(fn() or "").strip()
            except Exception:
                token = ""
        if not token:
            raise RuntimeError(
                "Missing read-scoped Cexius credential in Profile → API Keys for venue='cexius'."
            )
        return token

    def _require_trade_token(self) -> str:
        """Return the Cexius Bearer token only when the vault record is trade-scoped."""
        fn = getattr(settings, "_vault_latest_credential_record", None)
        record: Any = None
        if callable(fn):
            try:
                record = fn("cexius")
            except Exception:
                record = None

        if (
            not isinstance(record, dict)
            or not bool(record.get("scope_read"))
            or not bool(record.get("scope_trade"))
        ):
            raise RuntimeError(
                "Cexius trading requires read and trade scopes in Profile → API Keys."
            )

        bundle = record.get("bundle")
        if not isinstance(bundle, dict):
            raise RuntimeError("Cexius trade-scoped vault credential is missing its encrypted bundle.")

        token = str(bundle.get("api_secret") or bundle.get("api_key") or "").strip()
        if not token:
            raise RuntimeError("Cexius trade-scoped vault credential contains no Bearer token.")
        return token

    @staticmethod
    def _payload_message(payload: Any) -> str:
        if not isinstance(payload, dict):
            return ""
        candidates: List[Any] = [
            payload.get("message"),
            payload.get("detail"),
            payload.get("error"),
            payload.get("errors"),
        ]
        for value in candidates:
            if isinstance(value, str) and value.strip():
                return value.strip()
            if isinstance(value, dict):
                nested = value.get("message") or value.get("detail") or value.get("error")
                if isinstance(nested, str) and nested.strip():
                    return nested.strip()
            if isinstance(value, list) and value:
                return "; ".join(str(x) for x in value[:3])
        return ""

    @staticmethod
    def _payload_code(payload: Any) -> str:
        if not isinstance(payload, dict):
            return ""
        candidates: List[Any] = [
            payload.get("code"),
            payload.get("error_code"),
            payload.get("errorCode"),
        ]
        error = payload.get("error")
        if isinstance(error, dict):
            candidates.extend([error.get("code"), error.get("error_code")])
        for value in candidates:
            text = str(value or "").strip()
            if text:
                return text
        return ""

    @classmethod
    def _object_payload(cls, payload: Any) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        for key in ("order", "data", "result", "payload", "response"):
            value = payload.get(key)
            if isinstance(value, dict):
                return value
        return payload

    @staticmethod
    def _decimal_text(value: Any) -> str:
        try:
            dec = Decimal(str(value).strip())
        except (InvalidOperation, ValueError, TypeError):
            raise ValueError("Cexius order amount or price is invalid")
        if not dec.is_finite() or dec <= 0:
            raise ValueError("Cexius order amount and price must be positive")
        text = format(dec, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text or "0"

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        private: bool = False,
        auth_token: Optional[str] = None,
    ) -> Any:
        route = "/" + str(path or "").lstrip("/")
        url = f"{self._base_url()}{route}"
        headers = {
            "Accept": "application/json",
            "User-Agent": "UTT-Cexius/1.0",
        }
        token = str(auth_token or "").strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        elif private:
            headers["Authorization"] = f"Bearer {self._require_token()}"

        with httpx.Client(timeout=self._TIMEOUT_S, follow_redirects=True) as client:
            response = client.request(
                str(method or "GET").upper(),
                url,
                params=params or None,
                json=json_body if json_body is not None else None,
                headers=headers,
            )

        payload: Any = {}
        try:
            payload = response.json() if response.content else {}
        except Exception:
            payload = {}

        if response.status_code >= 400:
            message = self._payload_message(payload) or response.text.strip() or "upstream request failed"
            code = self._payload_code(payload)
            detail = f"{code}: {message}" if code and code.lower() not in message.lower() else message
            if response.status_code in {401, 403}:
                raise RuntimeError(f"Cexius authentication failed ({response.status_code}): {detail}")
            if response.status_code == 404:
                raise RuntimeError(f"Cexius pair or resource not found: {detail}")
            if response.status_code == 429:
                raise RuntimeError("Cexius rate limited the request (429).")
            raise RuntimeError(f"Cexius HTTP {response.status_code}: {detail}")

        if isinstance(payload, dict):
            success = payload.get("success")
            if success is False:
                message = self._payload_message(payload) or "Cexius returned success=false"
                raise RuntimeError(message)

        return payload

    @classmethod
    def _rows(cls, payload: Any, keys: Sequence[str]) -> List[Any]:
        if isinstance(payload, list):
            return list(payload)
        if not isinstance(payload, dict):
            return []

        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                return list(value)
            if isinstance(value, dict):
                mapped: List[Any] = []
                for map_key, map_value in value.items():
                    if isinstance(map_value, dict):
                        row = dict(map_value)
                        row.setdefault("id", map_key)
                        row.setdefault("symbol", map_key)
                        mapped.append(row)
                if mapped:
                    return mapped

        for wrapper in ("data", "result", "payload", "response"):
            value = payload.get(wrapper)
            if isinstance(value, list):
                return list(value)
            if isinstance(value, dict) and value is not payload:
                rows = cls._rows(value, keys)
                if rows:
                    return rows
        return []

    @staticmethod
    def _dict_value(row: Dict[str, Any], *names: str) -> Any:
        for name in names:
            if name in row and row.get(name) is not None:
                return row.get(name)
        return None

    @classmethod
    def _text(cls, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, dict):
            for key in ("id", "symbol", "code", "ticker", "name", "currency", "asset"):
                nested = value.get(key)
                if nested is not None and str(nested).strip():
                    return str(nested).strip()
            return ""
        return str(value).strip()

    @staticmethod
    def _asset(value: Any) -> str:
        text = CexiusAdapter._text(value).strip().upper()
        aliases = {"XBT": "BTC"}
        return aliases.get(text, text)

    @staticmethod
    def _float(value: Any) -> Optional[float]:
        if value is None or isinstance(value, bool):
            return None
        try:
            number = float(str(value).replace(",", "").strip())
            if number != number:  # NaN
                return None
            return number
        except Exception:
            return None

    @staticmethod
    def _int(value: Any) -> Optional[int]:
        try:
            return int(value) if value is not None else None
        except Exception:
            return None

    @staticmethod
    def _decimals_from_increment(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            dec = Decimal(str(value).strip())
            if dec <= 0:
                return None
            return max(0, -dec.normalize().as_tuple().exponent)
        except (InvalidOperation, ValueError, TypeError):
            return None

    @staticmethod
    def _increment_from_decimals(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            decimals = int(value)
        except (TypeError, ValueError):
            return None
        if decimals < 0 or decimals > 18:
            return None
        return float(Decimal(1).scaleb(-decimals))

    @classmethod
    def _first_numeric(cls, row: Dict[str, Any], names: Iterable[str]) -> Optional[float]:
        nested_sources = [row]
        for key in ("rules", "limits", "filters", "trading_rules", "precision"):
            value = row.get(key)
            if isinstance(value, dict):
                nested_sources.append(value)
        for source in nested_sources:
            for name in names:
                value = source.get(name)
                number = cls._float(value)
                if number is not None:
                    return number
        return None

    @classmethod
    def _first_integer(cls, row: Dict[str, Any], names: Iterable[str]) -> Optional[int]:
        nested_sources = [row]
        for key in ("rules", "limits", "filters", "trading_rules", "precision"):
            value = row.get(key)
            if isinstance(value, dict):
                nested_sources.append(value)
        for source in nested_sources:
            for name in names:
                value = source.get(name)
                number = cls._int(value)
                if number is not None:
                    return number
        return None

    @classmethod
    def _pagination_int(cls, payload: Any, names: Iterable[str]) -> Optional[int]:
        if not isinstance(payload, dict):
            return None
        for name in names:
            if name in payload:
                value = cls._int(payload.get(name))
                if value is not None:
                    return value
        for wrapper in ("data", "result", "payload", "response", "meta", "pagination"):
            nested = payload.get(wrapper)
            if isinstance(nested, dict) and nested is not payload:
                value = cls._pagination_int(nested, names)
                if value is not None:
                    return value
        return None

    @classmethod
    def _sum_numeric_fields(cls, row: Dict[str, Any], names: Iterable[str]) -> float:
        total = 0.0
        for name in names:
            if name not in row:
                continue
            value = cls._float(row.get(name))
            if value is not None and value > 0:
                total += float(value)
        return total

    # ------------------------------------------------------------------
    # Public metadata and market discovery
    # ------------------------------------------------------------------
    def fetch_exchange_metadata(self) -> Any:
        return self._request_json("GET", "/exchange")

    def fetch_statistics(self) -> Any:
        return self._request_json("GET", "/statistics")

    def fetch_currencies(self) -> List[Any]:
        return self._rows(
            self._request_json("GET", "/currencies"),
            ("currencies", "items", "rows"),
        )

    def fetch_currency_networks(self, currency_id: Optional[str] = None) -> List[Any]:
        if currency_id:
            path = f"/currencies/{quote(str(currency_id).strip(), safe='')}/networks"
        else:
            path = "/currencies/networks"
        return self._rows(
            self._request_json("GET", path),
            ("networks", "items", "rows", "currencies"),
        )

    def fetch_public_fees(self) -> Any:
        return self._request_json("GET", "/fees")

    def fetch_tickers(self, market: Optional[str] = None) -> Any:
        if market:
            symbol = quote(self.resolve_symbol(market), safe="-")
            return self._request_json("GET", f"/markets/{symbol}/tickers")
        return self._request_json("GET", "/markets/tickers")

    def fetch_public_trades(
        self,
        market: str,
        *,
        limit: int = 50,
        page: int = 1,
    ) -> List[Any]:
        symbol = quote(self.resolve_symbol(market), safe="-")
        payload = self._request_json(
            "GET",
            f"/markets/{symbol}/trades",
            params={"limit": max(1, min(int(limit or 50), 500)), "page": max(1, int(page or 1))},
        )
        return self._rows(payload, ("trades", "items", "rows"))

    def fetch_candles(
        self,
        market: str,
        *,
        interval: str = "1d",
        time_from: Optional[Any] = None,
        time_to: Optional[Any] = None,
        limit: int = 500,
    ) -> List[Any]:
        symbol = quote(self.resolve_symbol(market), safe="-")
        params: Dict[str, Any] = {
            "interval": str(interval or "1d"),
            "limit": max(1, min(int(limit or 500), 5000)),
        }
        if time_from is not None:
            params["time_from"] = time_from
        if time_to is not None:
            params["time_to"] = time_to
        payload = self._request_json("GET", f"/markets/{symbol}/k-line", params=params)
        return self._rows(payload, ("candles", "klines", "kline", "items", "rows"))

    @classmethod
    def _market_from_row(cls, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(row, dict):
            return None

        active = cls._dict_value(row, "active", "enabled", "tradable", "is_active", "isActive")
        if active is False or str(active).strip().lower() in {"0", "false", "no", "off"}:
            return None

        status = cls._text(cls._dict_value(row, "status", "state", "trading_status")).lower()
        if status in {"disabled", "inactive", "suspended", "closed", "delisted"}:
            return None

        symbol = cls._text(cls._dict_value(row, "symbol", "market", "pair", "code", "id", "name"))
        base = cls._asset(
            cls._dict_value(
                row,
                "base",
                "base_currency",
                "baseCurrency",
                "base_asset",
                "baseAsset",
                "base_id",
                "baseId",
            )
        )
        quote_asset = cls._asset(
            cls._dict_value(
                row,
                "quote",
                "quote_currency",
                "quoteCurrency",
                "quote_asset",
                "quoteAsset",
                "quote_id",
                "quoteId",
            )
        )

        normalized_symbol = symbol.upper().replace("/", "-").replace("_", "-").replace(" ", "-")
        while "--" in normalized_symbol:
            normalized_symbol = normalized_symbol.replace("--", "-")

        if (not base or not quote_asset) and "-" in normalized_symbol:
            left, right = normalized_symbol.split("-", 1)
            base = base or cls._asset(left)
            quote_asset = quote_asset or cls._asset(right)

        if not base or not quote_asset:
            return None

        canonical = f"{base}-{quote_asset}"
        symbol_venue = normalized_symbol or canonical
        return {
            "venue": "cexius",
            "symbol_canon": canonical,
            "symbol_venue": symbol_venue,
            "base": base,
            "quote": quote_asset,
            "raw": row,
        }

    def list_markets(self, *, force: bool = False) -> List[Dict[str, Any]]:
        import time

        now = time.monotonic()
        if not force and self._markets_cache and (now - self._markets_cache_at) <= self._MARKETS_CACHE_TTL_S:
            return [dict(x) for x in self._markets_cache]

        payload = self._request_json("GET", "/markets")
        rows = self._rows(payload, ("markets", "items", "rows"))
        out: List[Dict[str, Any]] = []
        seen = set()
        for row in rows:
            normalized = self._market_from_row(row) if isinstance(row, dict) else None
            if not normalized:
                continue
            key = normalized["symbol_canon"]
            if key in seen:
                continue
            seen.add(key)
            out.append(normalized)
        out.sort(key=lambda x: str(x.get("symbol_canon") or ""))
        self._markets_cache = [dict(x) for x in out]
        self._markets_cache_at = now
        return out

    def list_symbols(self) -> List[str]:
        return [str(row.get("symbol_canon") or "") for row in self.list_markets() if row.get("symbol_canon")]

    def resolve_symbol(self, symbol_canon: str) -> str:
        normalized = str(symbol_canon or "").strip().upper().replace("/", "-").replace("_", "-")
        while "--" in normalized:
            normalized = normalized.replace("--", "-")
        if not normalized or "-" not in normalized:
            return normalized
        try:
            for market in self.list_markets():
                if str(market.get("symbol_canon") or "").upper() == normalized:
                    return str(market.get("symbol_venue") or normalized)
        except Exception:
            pass
        return normalized

    def get_order_rules(self, symbol_venue: str) -> OrderRules:
        requested = str(symbol_venue or "").strip().upper().replace("/", "-").replace("_", "-")
        market: Optional[Dict[str, Any]] = None
        for item in self.list_markets():
            if requested in {
                str(item.get("symbol_venue") or "").upper(),
                str(item.get("symbol_canon") or "").upper(),
            }:
                market = item
                break
        if not market:
            return {"symbol_venue": requested, "raw": {}}

        raw = market.get("raw") if isinstance(market.get("raw"), dict) else {}
        base_increment = self._first_numeric(
            raw,
            (
                "base_increment",
                "amount_step",
                "quantity_step",
                "qty_step",
                "step_size",
                "amount_increment",
                "quantity_increment",
            ),
        )
        price_increment = self._first_numeric(
            raw,
            ("price_increment", "price_step", "tick_size", "price_tick", "tickSize"),
        )
        qty_decimals = self._first_integer(
            raw,
            (
                "qty_decimals",
                "quantity_decimals",
                "amount_precision",
                "trading_amount_precision",
                "base_precision",
                "quantity_precision",
            ),
        )
        price_decimals = self._first_integer(
            raw,
            (
                "price_decimals",
                "trading_price_precision",
                "price_precision",
                "quote_precision",
            ),
        )
        if base_increment is None:
            base_increment = self._increment_from_decimals(qty_decimals)
        if price_increment is None:
            price_increment = self._increment_from_decimals(price_decimals)
        if qty_decimals is None:
            qty_decimals = self._decimals_from_increment(base_increment)
        if price_decimals is None:
            price_decimals = self._decimals_from_increment(price_increment)

        min_qty = self._first_numeric(
            raw,
            (
                "min_qty",
                "minimum_quantity",
                "min_amount",
                "minimum_amount",
                "min_order_amount",
                "trading_min_amount",
            ),
        )
        if (min_qty is None or min_qty <= 0) and base_increment is not None and base_increment > 0:
            min_qty = base_increment

        return {
            "symbol_venue": str(market.get("symbol_venue") or requested),
            "base_increment": base_increment,
            "price_increment": price_increment,
            "qty_decimals": qty_decimals,
            "price_decimals": price_decimals,
            "min_qty": min_qty,
            "max_qty": self._first_numeric(
                raw,
                ("max_qty", "maximum_quantity", "max_amount", "maximum_amount", "max_order_amount"),
            ),
            "min_notional": self._first_numeric(
                raw,
                ("min_notional", "minimum_notional", "min_total", "minimum_total", "min_order_value"),
            ),
            "max_notional": self._first_numeric(
                raw,
                ("max_notional", "maximum_notional", "max_total", "maximum_total", "max_order_value"),
            ),
            "supports_post_only": False,
            "supported_tifs": [],
            "supported_order_types": [],
            "raw": raw,
        }

    # ------------------------------------------------------------------
    # Public order book
    # ------------------------------------------------------------------
    @classmethod
    def _level(cls, value: Any) -> Optional[Dict[str, float]]:
        price_value: Any = None
        qty_value: Any = None
        if isinstance(value, (list, tuple)) and len(value) >= 2:
            price_value, qty_value = value[0], value[1]
        elif isinstance(value, dict):
            price_value = cls._dict_value(value, "price", "rate", "p")
            qty_value = cls._dict_value(value, "qty", "quantity", "amount", "volume", "size", "q")
        price = cls._float(price_value)
        qty = cls._float(qty_value)
        if price is None or qty is None or price <= 0 or qty <= 0:
            return None
        return {"price": float(price), "qty": float(qty)}

    @classmethod
    def _book_sides(cls, payload: Any) -> Tuple[List[Any], List[Any]]:
        if isinstance(payload, dict):
            bids = payload.get("bids") if payload.get("bids") is not None else payload.get("buy")
            asks = payload.get("asks") if payload.get("asks") is not None else payload.get("sell")
            if isinstance(bids, list) or isinstance(asks, list):
                return (list(bids or []), list(asks or []))
            for wrapper in ("data", "result", "payload", "response", "orderbook", "book"):
                nested = payload.get(wrapper)
                if isinstance(nested, dict):
                    found_bids, found_asks = cls._book_sides(nested)
                    if found_bids or found_asks:
                        return found_bids, found_asks
                elif isinstance(nested, list) and nested and isinstance(nested[0], dict):
                    found_bids, found_asks = cls._book_sides(nested[0])
                    if found_bids or found_asks:
                        return found_bids, found_asks
        return [], []

    def fetch_orderbook(self, symbol_venue: str, depth: int, dry_run: bool) -> OrderBook:
        del dry_run
        # Cexius order-book routes accept the canonical dash market directly.
        # Do not call resolve_symbol() here: it consults /markets and would add a
        # redundant catalog request ahead of every live order-book request.
        normalized = str(symbol_venue or "").strip().upper().replace("/", "-").replace("_", "-")
        while "--" in normalized:
            normalized = normalized.replace("--", "-")
        symbol = quote(normalized, safe="-")
        limit = max(1, min(int(depth or 25), 200))
        payload = self._request_json("GET", f"/markets/{symbol}/orderbook")
        bids_raw, asks_raw = self._book_sides(payload)
        bids = [level for level in (self._level(x) for x in bids_raw) if level][:limit]
        asks = [level for level in (self._level(x) for x in asks_raw) if level][:limit]
        bids.sort(key=lambda x: x["price"], reverse=True)
        asks.sort(key=lambda x: x["price"])
        return {"bids": bids, "asks": asks}

    # ------------------------------------------------------------------
    # Authenticated read-only account surfaces
    # ------------------------------------------------------------------
    def fetch_balances(self, dry_run: bool) -> List[BalanceItem]:
        del dry_run
        payload = self._request_json("GET", "/balances", private=True)
        rows = self._rows(payload, ("balances", "wallets", "accounts", "items", "rows"))
        out: List[BalanceItem] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            asset = self._asset(
                self._dict_value(row, "asset", "currency", "currency_id", "currencyId", "symbol", "code")
            )
            if not asset:
                continue

            available = self._float(
                self._dict_value(
                    row,
                    "available",
                    "free",
                    "available_balance",
                    "availableBalance",
                    "spendable",
                    "spendable_balance",
                    "free_balance",
                )
            )
            balance = self._float(self._dict_value(row, "balance"))
            explicit_total = self._float(
                self._dict_value(row, "total", "total_balance", "totalBalance")
            )

            other_hold = self._sum_numeric_fields(
                row,
                (
                    "hold",
                    "locked",
                    "reserved",
                    "frozen",
                    "in_order",
                    "inOrder",
                ),
            )
            trading_hold = self._float(
                self._dict_value(row, "trading_locked", "tradingLocked")
            )
            hold = max(other_hold, float(trading_hold or 0.0))

            if available is None and balance is not None:
                # Cexius live balances expose spendable funds as `balance` and
                # open-order funds separately as `trading_locked`.
                available = float(balance)

            if available is None and explicit_total is not None:
                available = max(float(explicit_total) - hold, 0.0)

            available = float(available or 0.0)

            if explicit_total is not None:
                total = float(explicit_total)
            elif balance is not None and self._dict_value(
                row,
                "available",
                "free",
                "available_balance",
                "availableBalance",
                "spendable",
                "spendable_balance",
                "free_balance",
            ) is not None:
                # Legacy/alternate shapes may use `balance` as the total while
                # also exposing an explicit free/available amount.
                total = max(float(balance), available + hold)
            else:
                total = available + hold

            if total == 0.0 and available == 0.0 and hold == 0.0:
                continue
            out.append({"asset": asset, "total": total, "available": available, "hold": hold})
        out.sort(key=lambda x: x["asset"])
        return out

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        if value is None:
            return None
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, (int, float)) or str(value).strip().isdigit():
            try:
                number = float(value)
                if number > 10_000_000_000:
                    number /= 1000.0
                dt = datetime.fromtimestamp(number, tz=timezone.utc)
            except Exception:
                return None
        else:
            text = str(value).strip().replace("Z", "+00:00")
            try:
                dt = datetime.fromisoformat(text)
            except Exception:
                return None
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt

    def _normalize_order(self, row: Dict[str, Any]) -> Optional[VenueOrder]:
        order_id = self._text(self._dict_value(row, "id", "order_id", "orderId", "uuid"))
        if not order_id:
            return None
        symbol_venue = self._text(self._dict_value(row, "market", "symbol", "pair", "market_id", "marketId"))
        symbol_venue = symbol_venue.upper().replace("/", "-").replace("_", "-")
        symbol_canon = symbol_venue
        side = self._text(self._dict_value(row, "side", "direction")).lower()
        type_ = self._text(self._dict_value(row, "type", "order_type", "orderType")).lower()
        status_raw = self._text(self._dict_value(row, "status", "state")).lower()
        qty = self._float(self._dict_value(row, "qty", "quantity", "amount", "size", "original_quantity")) or 0.0
        filled = self._float(
            self._dict_value(row, "filled_qty", "filled_quantity", "filled", "executed_quantity", "executedQty")
        ) or 0.0
        if (
            status_raw in {"filled", "done", "closed", "complete", "completed", "settled"}
            and filled <= 0.0
            and qty > 0.0
        ):
            # Cexius list rows report a terminal filled status and full order quantity
            # but may omit executed quantity. Preserve the shared poison-row guard by
            # normalizing a fully filled order to its authoritative order quantity.
            filled = float(qty)
        limit_price = self._float(self._dict_value(row, "price", "limit_price", "limitPrice"))
        avg_fill_price = self._float(
            self._dict_value(row, "avg_price", "average_price", "avgFillPrice", "averageFillPrice")
        )
        fee = self._float(self._dict_value(row, "fee", "fees", "commission"))
        fee_asset = self._asset(self._dict_value(row, "fee_asset", "fee_currency", "commission_asset")) or None
        created = self._parse_datetime(
            self._dict_value(row, "created_at", "createdAt", "created", "timestamp", "time")
        )
        updated = self._parse_datetime(
            self._dict_value(row, "updated_at", "updatedAt", "updated", "modified_at", "modifiedAt")
        )
        return {
            "venue": self.venue,
            "venue_order_id": order_id,
            "symbol_canon": symbol_canon,
            "symbol_venue": symbol_venue,
            "side": side,
            "type": type_,
            "status": status_raw,
            "status_raw": status_raw,
            "cancel_ref": f"cexius:{order_id}",
            "qty": float(qty),
            "filled_qty": float(filled),
            "limit_price": limit_price,
            "avg_fill_price": avg_fill_price,
            "fee": fee,
            "fee_asset": fee_asset,
            "created_at": created,
            "updated_at": updated,
        }

    def fetch_orders(self, dry_run: bool = False) -> List[VenueOrder]:
        del dry_run
        page = 1
        limit = 200
        max_pages = 25
        raw_rows: List[Any] = []
        seen_page_signatures = set()

        while page <= max_pages and len(raw_rows) < (limit * max_pages):
            payload = self._request_json(
                "GET",
                "/trading/orders",
                params={"page": page, "limit": limit},
                private=True,
            )
            rows = self._rows(payload, ("orders", "items", "rows"))
            if not rows:
                break

            signature = tuple(
                self._text(self._dict_value(row, "id", "order_id", "orderId", "uuid"))
                for row in rows[:10]
                if isinstance(row, dict)
            )
            if signature and signature in seen_page_signatures:
                break
            if signature:
                seen_page_signatures.add(signature)

            raw_rows.extend(rows)
            total = self._pagination_int(payload, ("total", "count"))
            if total is not None and len(raw_rows) >= total:
                break
            if len(rows) < limit and total is None:
                break
            page += 1

        out: List[VenueOrder] = []
        seen_order_ids = set()
        for row in raw_rows:
            normalized = self._normalize_order(row) if isinstance(row, dict) else None
            if not normalized:
                continue
            order_id = str(normalized.get("venue_order_id") or "").strip()
            if order_id and order_id in seen_order_ids:
                continue
            if order_id:
                seen_order_ids.add(order_id)
            out.append(normalized)
        return out

    def fetch_transaction_history(self, **params: Any) -> List[Any]:
        payload = self._request_json("GET", "/history/tx", params=params or None, private=True)
        return self._rows(payload, ("transactions", "history", "items", "rows"))

    def fetch_trade_history(self, **params: Any) -> List[Any]:
        payload = self._request_json("GET", "/history/trades", params=params or None, private=True)
        return self._rows(payload, ("trades", "history", "items", "rows"))

    def fetch_order_detail(self, order_id: str) -> Any:
        oid = quote(str(order_id or "").strip(), safe="")
        if not oid:
            raise ValueError("Cexius order id is required")
        return self._request_json("GET", f"/trading/orders/{oid}", private=True)

    def fetch_order_trades(self, order_id: str) -> List[Any]:
        oid = quote(str(order_id or "").strip(), safe="")
        if not oid:
            raise ValueError("Cexius order id is required")
        payload = self._request_json("GET", f"/trading/orders/{oid}/trades", private=True)
        return self._rows(payload, ("trades", "items", "rows"))

    # ------------------------------------------------------------------
    # Explicit mutation boundary
    # ------------------------------------------------------------------
    def place_order(
        self,
        symbol_venue: str,
        side: str,
        type_: str,
        qty: float,
        limit_price: Optional[float],
        client_order_id: str,
        dry_run: bool,
        tif: Optional[str] = None,
        post_only: bool = False,
    ) -> PlacedOrder:
        del client_order_id  # Cexius does not document a client-order-id field.

        if dry_run:
            return {
                "status": "simulated",
                "raw": {"simulated": True, "venue_request_sent": False},
            }

        symbol = str(symbol_venue or "").strip().upper().replace("/", "-").replace("_", "-")
        while "--" in symbol:
            symbol = symbol.replace("--", "-")
        if not symbol or "-" not in symbol:
            raise ValueError("Cexius market symbol is required")

        side_norm = str(side or "").strip().lower()
        if side_norm not in {"buy", "sell"}:
            raise ValueError("Cexius side must be buy or sell")

        type_norm = str(type_ or "").strip().lower()
        if type_norm != "limit":
            raise ValueError("CEXIUS.2B permits limit orders only")
        if limit_price is None:
            raise ValueError("Cexius limit price is required")

        tif_norm = str(tif or "").strip().lower()
        if tif_norm:
            raise ValueError("Cexius time-in-force is not supported and must be omitted")
        if bool(post_only):
            raise ValueError("Cexius post-only is not supported")

        request_body = {
            "market": symbol,
            "side": side_norm,
            "type": "limit",
            "amount": self._decimal_text(qty),
            "price": self._decimal_text(limit_price),
        }
        trade_token = self._require_trade_token()
        payload = self._request_json(
            "POST",
            "/trading/order",
            json_body=request_body,
            auth_token=trade_token,
        )
        row = self._object_payload(payload)

        order_id = self._text(self._dict_value(row, "id", "order_id", "orderId", "uuid"))
        if not order_id:
            raise RuntimeError("Cexius create-order response did not contain an order id")

        status_raw = self._text(self._dict_value(row, "status", "state")).lower() or "acked"
        if status_raw in {"failed", "rejected", "error"}:
            message = self._payload_message(row) or self._payload_message(payload) or status_raw
            code = self._payload_code(row) or self._payload_code(payload)
            detail = f"{code}: {message}" if code and code.lower() not in message.lower() else message
            raise RuntimeError(f"Cexius order placement failed: {detail}")

        return {
            "venue_order_id": order_id,
            "status": status_raw,
            "status_raw": status_raw,
            "cancel_ref": f"cexius:{order_id}",
            "raw": payload if isinstance(payload, dict) else {"response": payload},
        }

    def cancel_order(self, venue_order_id: str, dry_run: bool) -> bool:
        oid_raw = str(venue_order_id or "").strip()
        if not oid_raw:
            raise ValueError("Cexius order id is required")

        if dry_run:
            return True

        # Require the explicit UTT trade scope before any mutating upstream request.
        trade_token = self._require_trade_token()

        def truthful_failure(message: str) -> str:
            value = str(message or "Cexius cancellation failed").strip()
            lowered = value.lower()
            # The shared cancel service historically treats the phrase "cancel rejected"
            # as an already-closed success. Rewrite ambiguous venue-decline phrases so an
            # actual rejection cannot be misclassified as a successful cancellation.
            if any(
                phrase in lowered
                for phrase in ("cancel rejected", "cannot cancel", "unable to cancel")
            ):
                return "Cexius cancellation failed: venue declined the request."
            return value

        oid = quote(oid_raw, safe="")
        try:
            payload = self._request_json(
                "DELETE",
                f"/trading/orders/{oid}",
                auth_token=trade_token,
            )
        except RuntimeError as exc:
            message = truthful_failure(str(exc))
            if message != str(exc):
                raise RuntimeError(message) from exc
            raise

        # A successful 2xx/204 response is authoritative. Reject any explicit false
        # result that may be nested in a normal 2xx JSON response.
        candidates: List[Dict[str, Any]] = []
        if isinstance(payload, dict):
            candidates.append(payload)
            for key in ("data", "result", "payload", "response"):
                nested = payload.get(key)
                if isinstance(nested, dict):
                    candidates.append(nested)

        for candidate in candidates:
            for key in ("success", "ok", "canceled", "cancelled", "deleted"):
                if candidate.get(key) is False:
                    message = self._payload_message(payload) or f"Cexius returned {key}=false"
                    raise RuntimeError(truthful_failure(message))

            status = self._text(
                self._dict_value(candidate, "status", "state", "order_status", "orderStatus")
            ).lower()
            if status in {"failed", "rejected", "error"}:
                message = self._payload_message(payload) or f"Cexius cancellation status={status}"
                raise RuntimeError(truthful_failure(message))

        return True
