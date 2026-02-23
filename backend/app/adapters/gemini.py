# backend/app/adapters/gemini.py

from __future__ import annotations

from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone
import os
import json
import time
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING, InvalidOperation

import httpx

from .base import ExchangeAdapter, PlacedOrder, BalanceItem, OrderBook, VenueOrder, OrderRules
from ..config import settings


class GeminiAdapter(ExchangeAdapter):
    venue = "gemini"

    # Canonical -> Gemini symbol
    _map = {
        "USDT-USD": "usdtusd",
        "BTC-USD": "btcusd",
        "ETH-USD": "ethusd",
    }

    # Gemini base URL (prod)
    _base_url = "https://api.gemini.com"

    # What we consider terminal at the venue
    _TERMINAL = {"filled", "canceled", "cancelled", "rejected", "done", "closed"}

    # Symbol details cache (public endpoint)
    # sym -> (fetched_epoch, details_dict)
    _symbol_details_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
    _symbol_details_ttl_s: int = 600  # 10 minutes

    # ─────────────────────────────────────────────────────────────
    # Helpers: env toggles (trade backfill / pagination)
    # ─────────────────────────────────────────────────────────────
    def _env_bool(self, k: str, default: bool = False) -> bool:
        v = (os.getenv(k, "") or "").strip().lower()
        if not v:
            return default
        return v in ("1", "true", "yes", "y", "on")

    def _env_int(self, k: str, default: int) -> int:
        try:
            v = (os.getenv(k, "") or "").strip()
            return int(v) if v else default
        except Exception:
            return default

    def _env_iso_dt(self, k: str) -> Optional[datetime]:
        s = (os.getenv(k, "") or "").strip()
        if not s:
            return None
        try:
            # Accept ...Z
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            return None

    def resolve_symbol(self, symbol_canon: str) -> str:
        v = self._map.get(symbol_canon, symbol_canon.replace("-", ""))
        return v.lower()

    def _canon_from_symbol_venue(self, sym: str) -> Optional[str]:
        """
        Best-effort conversion:
        - If sym matches our known mapping values, invert it.
        - Otherwise, try to infer common XXXUSD/XXXUSDT/XXXUSDC/XXXBTC/XXXETH patterns.
        """
        s = (sym or "").lower().strip()
        if not s:
            return None

        # invert known map
        for k, v in self._map.items():
            if v.lower() == s:
                return k

        # Prefer longer suffixes first to avoid "usd" matching inside "usdt/usdc"
        suffixes = [
            ("usdt", "USDT"),
            ("usdc", "USDC"),
            ("usd", "USD"),
            ("btc", "BTC"),
            ("eth", "ETH"),
        ]

        for suf, quote in suffixes:
            if s.endswith(suf) and len(s) > len(suf):
                base = s[: -len(suf)].upper()
                if base:
                    return f"{base}-{quote}"

        return None

    # ─────────────────────────────────────────────────────────────
    # Discovery: list symbols (canonical BASE-QUOTE)
    # ─────────────────────────────────────────────────────────────
    def list_symbols(self) -> List[str]:
        """
        Returns canonical symbols like BTC-USD, DOGE-USD, SOL-USDC, etc.

        Gemini endpoint:
          GET /v1/symbols  -> ["btcusd","ethusd",...]

        We filter by allowed QUOTES (USD/USDT/USDC/BTC/(optionally ETH)).
        IMPORTANT: This discovers ALL bases paired to those quotes, not just the quote assets themselves.
        """
        # Import here to avoid import-order/cycle issues at module import time.
        try:
            from ..services.symbol_policy import ALLOWED_QUOTES  # type: ignore
            allowed_quotes = {str(x).upper().strip() for x in (ALLOWED_QUOTES or set()) if str(x).strip()}
        except Exception:
            allowed_quotes = {"USD", "USDT", "USDC", "BTC"}  # safe default

        quote_to_suffix = {
            "USDT": "usdt",
            "USDC": "usdc",
            "USD": "usd",
            "BTC": "btc",
            "ETH": "eth",
        }

        suffixes: List[Tuple[str, str]] = []
        for q in allowed_quotes:
            suf = quote_to_suffix.get(q)
            if suf:
                suffixes.append((suf, q))
        suffixes.sort(key=lambda x: len(x[0]), reverse=True)

        url = f"{self._base_url}/v1/symbols"

        try:
            with httpx.Client(timeout=15.0) as client:
                r = client.get(url)
                r.raise_for_status()
                data = r.json() if r.content else []
        except Exception:
            return [k for k in self._map.keys()]

        if not isinstance(data, list):
            return [k for k in self._map.keys()]

        out: List[str] = []
        seen: set[str] = set()

        for raw in data:
            try:
                s = str(raw or "").strip().lower()
                if not s:
                    continue

                canon = self._canon_from_symbol_venue(s)

                if canon is None:
                    for suf, quote in suffixes:
                        if s.endswith(suf) and len(s) > len(suf):
                            base = s[: -len(suf)].upper().strip()
                            if base:
                                canon = f"{base}-{quote}"
                                break

                if not canon:
                    continue

                canon_u = canon.upper().strip()
                if "-" not in canon_u:
                    continue
                _, q = canon_u.split("-", 1)
                if q not in allowed_quotes:
                    continue

                if canon_u not in seen:
                    seen.add(canon_u)
                    out.append(canon_u)
            except Exception:
                continue

        out.sort()
        return out

    # ─────────────────────────────────────────────────────────────
    # Signing
    # ─────────────────────────────────────────────────────────────
    def _sign_payload(self, request_path: str, payload: Dict[str, Any]) -> Dict[str, str]:
        import json as _json, base64, hmac, hashlib, time as _time

        if not settings.gemini_api_key or not settings.gemini_api_secret:
            raise Exception("Missing Gemini credentials: set GEMINI_API_KEY and GEMINI_API_SECRET")

        p = dict(payload or {})
        p["request"] = request_path
        p["nonce"] = str(int(_time.time() * 1000))

        payload_json = _json.dumps(p)
        payload_b64 = base64.b64encode(payload_json.encode("utf-8")).decode("utf-8")

        signature = hmac.new(
            settings.gemini_api_secret.encode("utf-8"),
            payload_b64.encode("utf-8"),
            hashlib.sha384,
        ).hexdigest()

        return {
            "Content-Type": "text/plain",
            "X-GEMINI-APIKEY": settings.gemini_api_key,
            "X-GEMINI-PAYLOAD": payload_b64,
            "X-GEMINI-SIGNATURE": signature,
            "Cache-Control": "no-cache",
        }

    def _dt_from_ms(self, ms: Any) -> Optional[datetime]:
        try:
            ms_i = int(ms)
            return datetime.fromtimestamp(ms_i / 1000.0, tz=timezone.utc).replace(tzinfo=None)
        except Exception:
            return None

    def _dt_from_s(self, sec: Any) -> Optional[datetime]:
        try:
            s = float(sec)
            return datetime.fromtimestamp(s, tz=timezone.utc).replace(tzinfo=None)
        except Exception:
            return None

    def _safe_float(self, x: Any) -> Optional[float]:
        try:
            if x is None:
                return None
            if isinstance(x, str) and x.strip() == "":
                return None
            return float(x)
        except Exception:
            return None

    # ─────────────────────────────────────────────────────────────
    # Public symbol details (for qty/price normalization + rules)
    # ─────────────────────────────────────────────────────────────
    def _get_symbol_details(self, symbol_venue: str) -> Dict[str, Any]:
        sym = (symbol_venue or "").strip().lower()
        if not sym:
            return {}

        now = time.time()
        cached = self._symbol_details_cache.get(sym)
        if cached:
            fetched_at, details = cached
            if now - fetched_at <= float(self._symbol_details_ttl_s):
                return details or {}

        url = f"{self._base_url}/v1/symbols/details/{sym}"
        with httpx.Client(timeout=10.0) as client:
            r = client.get(url)
            r.raise_for_status()
            details = r.json() if r.content else {}

        if not isinstance(details, dict):
            details = {}

        self._symbol_details_cache[sym] = (now, details)
        return details

    def _decimals_from_any(self, v: Any) -> Optional[int]:
        """
        Gemini may return increments as:
          - strings like "0.01"
          - scientific notation like "1e-8"
          - numbers

        We compute the decimal precision as abs(exponent) for Decimal values.
        """
        try:
            if v is None:
                return None
            s = str(v).strip()
            if not s:
                return None
            d = Decimal(s)
            exp = d.as_tuple().exponent
            return int(-exp) if exp < 0 else 0
        except Exception:
            return None

    def get_order_rules(self, symbol_venue: str) -> OrderRules:
        """
        Normalized Gemini rule surface, derived from /v1/symbols/details/{symbol}.

        IMPORTANT (Gemini semantics):
          - tick_size       => base currency increment (qty step)
          - quote_increment => quote currency increment (price step)
          - min_order_size  => minimum qty (base currency)
        """
        sym = (symbol_venue or "").strip().lower()
        if not sym:
            return {"symbol_venue": symbol_venue}

        details = self._get_symbol_details(sym) or {}

        tick_size = details.get("tick_size")
        quote_increment = details.get("quote_increment")
        min_order_size = details.get("min_order_size")

        base_inc = self._safe_float(tick_size)
        price_inc = self._safe_float(quote_increment)
        min_qty = self._safe_float(min_order_size)

        qty_dec = self._decimals_from_any(tick_size)
        px_dec = self._decimals_from_any(quote_increment)

        return {
            "symbol_venue": sym,
            "base_increment": base_inc if (base_inc is not None and base_inc > 0) else None,
            "price_increment": price_inc if (price_inc is not None and price_inc > 0) else None,
            "qty_decimals": qty_dec,
            "price_decimals": px_dec,
            "min_qty": min_qty if (min_qty is not None and min_qty > 0) else None,
            "max_qty": None,
            "min_notional": None,
            "max_notional": None,
            "supports_post_only": True,
            "supported_tifs": ["gtc"],
            "supported_order_types": ["limit"],
            "raw": details if isinstance(details, dict) else {},
        }

    def _d(self, x: Any) -> Optional[Decimal]:
        try:
            if x is None:
                return None
            if isinstance(x, Decimal):
                return x
            s = str(x).strip()
            if not s:
                return None
            return Decimal(s)
        except (InvalidOperation, ValueError):
            return None

    def _floor_to_step(self, x: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return x
        q = (x / step).to_integral_value(rounding=ROUND_FLOOR)
        return q * step

    def _ceil_to_step(self, x: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return x
        q = (x / step).to_integral_value(rounding=ROUND_CEILING)
        return q * step

    def _normalize_qty_price(
        self,
        symbol_venue: str,
        side: str,
        qty: float,
        limit_price: float,
    ) -> Tuple[str, str]:
        details = self._get_symbol_details(symbol_venue) or {}

        min_order_size_d = self._d(details.get("min_order_size")) or Decimal("0")
        qty_step = self._d(details.get("tick_size")) or Decimal("0")
        px_step = self._d(details.get("quote_increment")) or Decimal("0")

        qty_d = self._d(qty)
        px_d = self._d(limit_price)
        if qty_d is None or px_d is None:
            raise Exception("GeminiAdapter: invalid qty/price (non-numeric)")

        qty_norm = self._floor_to_step(qty_d, qty_step) if qty_step > 0 else qty_d

        s = (side or "").lower().strip()
        if px_step > 0:
            if s == "sell":
                px_norm = self._ceil_to_step(px_d, px_step)
            else:
                px_norm = self._floor_to_step(px_d, px_step)
        else:
            px_norm = px_d

        if min_order_size_d > 0 and qty_norm < min_order_size_d:
            raise Exception(
                f"GeminiAdapter: qty below min_order_size after normalization "
                f"(qty_in={qty_d}, qty_norm={qty_norm}, min_order_size={min_order_size_d})"
            )

        return (format(qty_norm, "f"), format(px_norm, "f"))

    # ─────────────────────────────────────────────────────────────
    # Transfers (Deposits / Withdrawals) — private POST /v1/transfers
    # ─────────────────────────────────────────────────────────────
    def _fetch_transfers_page(
        self,
        *,
        currency: Optional[str],
        since_ms: Optional[int],
        limit_transfers: int,
    ) -> List[Dict[str, Any]]:
        """
        Gemini: POST /v1/transfers

        Signed payload supports (per Gemini API):
          - currency (optional)
          - timestamp (optional): only return transfers after this timestamp
          - limit_transfers: max 50

        NOTE: Gemini responses include `timestampms` on each row. We prefer ms-level
        paging by passing ms to `timestamp` so the cursor can advance precisely.
        """
        request_path = "/v1/transfers"
        url = f"{self._base_url}{request_path}"

        payload: Dict[str, Any] = {"limit_transfers": int(max(1, min(50, int(limit_transfers or 50))))}
        if currency:
            payload["currency"] = str(currency).upper().strip()
        if since_ms is not None:
            payload["timestamp"] = int(since_ms)

        headers = self._sign_payload(request_path, payload)
        with httpx.Client(timeout=30.0) as client:
            r = client.post(url, headers=headers)
            r.raise_for_status()
            data = r.json() if r.content else []
        return data if isinstance(data, list) else []

    def fetch_transfers(
        self,
        *,
        since: Optional[datetime],
        currency: Optional[str] = None,
        limit_transfers: int = 50,
        max_pages: Optional[int] = None,
        sleep_ms: Optional[int] = None,
        debug: Optional[bool] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch transfer history (deposits + withdrawals) from Gemini.

        Pagination strategy (Gemini /v1/transfers has no explicit cursor):
          - request transfers after `timestamp` (ms)
          - advance cursor to (max timestampms seen) + 1
          - stop on: empty page, no progress, short page, or max_pages

        Env controls (optional):
          - GEMINI_TRANSFERS_DEBUG     (default 0)
          - GEMINI_TRANSFERS_MAX_PAGES (default 200)
          - GEMINI_TRANSFERS_SLEEP_MS  (default 5100)  # Gemini can be strict on rate limits
        """
        if since is None:
            since = datetime.utcnow().replace(tzinfo=None)
        DEBUG = (
            bool(debug)
            if debug is not None
            else self._env_bool(
                "UTT_GEMINI_DEBUG_TRANSFERS",
                default=self._env_bool("GEMINI_TRANSFERS_DEBUG", default=False),
            )
        )
        MAX_PAGES = (
            int(max_pages)
            if max_pages is not None
            else self._env_int("UTT_GEMINI_TRANSFERS_MAX_PAGES", self._env_int("GEMINI_TRANSFERS_MAX_PAGES", 200))
        )
        LIMIT = int(max(1, min(50, int(limit_transfers or 50))))
        SLEEP_MS = (
            int(sleep_ms)
            if sleep_ms is not None
            else self._env_int("UTT_GEMINI_TRANSFERS_SLEEP_MS", self._env_int("GEMINI_TRANSFERS_SLEEP_MS", 5100))
        )
        cursor_ms = int(since.replace(tzinfo=timezone.utc).timestamp() * 1000)

        out: List[Dict[str, Any]] = []
        seen: set[str] = set()

        for page in range(int(MAX_PAGES)):
            rows = self._fetch_transfers_page(currency=currency, since_ms=cursor_ms, limit_transfers=LIMIT)
            if not rows:
                break

            max_ts = None
            added = 0

            for r in rows:
                if not isinstance(r, dict):
                    continue

                # Gemini transfer ID: `eid`. Include type+timestamp for extra safety.
                k = f"{r.get('type')}:{r.get('eid')}:{r.get('timestampms')}"
                if k in seen:
                    continue
                seen.add(k)
                out.append(r)
                added += 1

                try:
                    tms = int(r.get("timestampms"))
                except Exception:
                    tms = None
                if tms is not None:
                    max_ts = tms if max_ts is None else max(max_ts, tms)

            if DEBUG:
                print(f"GEMINI DEBUG transfers page={page+1} rows={len(rows)} added={added} cursor_ms={cursor_ms} max_ts={max_ts}")

            if max_ts is None:
                break
            if int(max_ts) <= int(cursor_ms):
                # No progress -> avoid infinite loop
                break

            cursor_ms = int(max_ts) + 1

            # Short page => likely exhausted
            if len(rows) < LIMIT:
                break

            if SLEEP_MS > 0:
                time.sleep(float(SLEEP_MS) / 1000.0)

        if DEBUG:
            print(f"GEMINI DEBUG transfers total_rows={len(out)}")
            if out:
                print("GEMINI DEBUG transfers sample row=", out[0])

        return out

    # ─────────────────────────────────────────────────────────────
    # Trading (REAL Gemini behind safety gates)
    # ─────────────────────────────────────────────────────────────
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
        if dry_run:
            return {"venue_order_id": f"dry-{client_order_id}", "status": "acked"}

        if str(type_ or "").lower().strip() != "limit":
            raise Exception("GeminiAdapter.place_order: only limit orders are enabled in Phase 3.1")

        if limit_price is None or float(limit_price) <= 0:
            raise Exception("GeminiAdapter.place_order: limit_price required for limit orders")

        sym = str(symbol_venue or "").strip().lower()
        if not sym:
            raise Exception("GeminiAdapter.place_order: missing symbol_venue")

        s = str(side or "").lower().strip()
        if s not in ("buy", "sell"):
            raise Exception("GeminiAdapter.place_order: side must be buy or sell")

        if qty is None or float(qty) <= 0:
            raise Exception("GeminiAdapter.place_order: qty must be > 0")

        amt_str, px_str = self._normalize_qty_price(sym, s, float(qty), float(limit_price))

        request_path = "/v1/order/new"

        payload: Dict[str, Any] = {
            "client_order_id": str(client_order_id),
            "symbol": sym,
            "amount": amt_str,
            "price": px_str,
            "side": s,
            "type": "exchange limit",
        }

        if post_only:
            payload["options"] = ["maker-or-cancel"]

        headers = self._sign_payload(request_path, payload)
        url = f"{self._base_url}{request_path}"

        with httpx.Client(timeout=25.0) as client:
            r = client.post(url, headers=headers)
            r.raise_for_status()
            data = r.json()

        oid = str(data.get("order_id") or data.get("id") or "").strip()
        if not oid:
            raise Exception(f"Gemini place_order: missing order_id in response: {data}")

        is_live = data.get("is_live")
        is_cancelled = data.get("is_cancelled")

        if is_cancelled is True:
            st = "canceled"
        elif is_live is True:
            st = "open"
        else:
            st = "acked"

        return {"venue_order_id": oid, "status": st}

    def _fetch_order_status(self, venue_order_id: str) -> Dict[str, Any]:
        oid = str(venue_order_id or "").strip()
        if not oid:
            return {}

        request_path = "/v1/order/status"
        payload: Dict[str, Any] = {}

        try:
            payload["order_id"] = int(oid)
        except Exception:
            payload["order_id"] = oid

        headers = self._sign_payload(request_path, payload)
        url = f"{self._base_url}{request_path}"

        with httpx.Client(timeout=25.0) as client:
            r = client.post(url, headers=headers)
            r.raise_for_status()
            data = r.json() if r.content else {}
        return data if isinstance(data, dict) else {}

    def cancel_order(self, venue_order_id: str, dry_run: bool) -> bool:
        if dry_run:
            return True

        oid = str(venue_order_id or "").strip()
        if not oid:
            raise Exception("GeminiAdapter.cancel_order: missing venue_order_id")

        request_path = "/v1/order/cancel"

        payload: Dict[str, Any] = {}
        try:
            payload["order_id"] = int(oid)
        except Exception:
            payload["order_id"] = oid

        headers = self._sign_payload(request_path, payload)
        url = f"{self._base_url}{request_path}"

        with httpx.Client(timeout=25.0) as client:
            r = client.post(url, headers=headers)
            r.raise_for_status()
            data = r.json() if r.content else {}

        if isinstance(data, dict):
            if data.get("is_cancelled") is True:
                return True

            res = str(data.get("result") or "").strip().lower()
            if res in ("ok", "success", "true"):
                try:
                    st = self._fetch_order_status(oid)
                    if st.get("is_cancelled") is True:
                        return True
                    if st.get("is_live") is False and st.get("is_cancelled") is not False:
                        return True
                except Exception:
                    return False

            reason = str(data.get("reason") or "").strip().lower()
            if reason in ("requested", "cancelled", "canceled"):
                try:
                    st = self._fetch_order_status(oid)
                    if st.get("is_cancelled") is True:
                        return True
                    if st.get("is_live") is False and st.get("is_cancelled") is not False:
                        return True
                except Exception:
                    return False

        return False

    # ─────────────────────────────────────────────────────────────
    # Balances
    # ─────────────────────────────────────────────────────────────
    def fetch_balances(self, dry_run: bool) -> List[BalanceItem]:
        """
        IMPORTANT: dry_run should NOT disable read-only ingestion.
        """
        try:
            url = f"{self._base_url}/v1/balances"
            headers = self._sign_payload("/v1/balances", {})

            with httpx.Client(timeout=20.0) as client:
                r = client.post(url, headers=headers)
                r.raise_for_status()
                data = r.json()

            out: List[BalanceItem] = []
            if isinstance(data, list):
                for row in data:
                    if not isinstance(row, dict):
                        continue
                    try:
                        cur = str(row.get("currency", "")).upper().strip()
                        if not cur:
                            continue

                        total = self._safe_float(row.get("amount")) or 0.0
                        avail = self._safe_float(row.get("available"))
                        if avail is None:
                            avail = self._safe_float(row.get("availableForWithdrawal"))
                        if avail is None:
                            avail = float(total)

                        hold = max(float(total) - float(avail), 0.0)
                        out.append({"asset": cur, "total": float(total), "available": float(avail), "hold": float(hold)})
                    except Exception:
                        continue

            return out

        except Exception as e:
            print("GEMINI DEBUG fetch_balances ERROR:", repr(e))
            raise

    # ─────────────────────────────────────────────────────────────
    # Orders + Trades (read-only ingestion)
    # ─────────────────────────────────────────────────────────────
    def _fetch_open_orders(self) -> List[Dict[str, Any]]:
        url = f"{self._base_url}/v1/orders"
        headers = self._sign_payload("/v1/orders", {})
        with httpx.Client(timeout=25.0) as client:
            r = client.post(url, headers=headers)
            r.raise_for_status()
            data = r.json()
        return data if isinstance(data, list) else []

    # ✅ FIXED: Gemini orders history uses limit_orders (not limit_trades).
    def _fetch_order_history(self, limit_orders: int = 500, timestamp: int = 0) -> List[Dict[str, Any]]:
        """
        Fetch a single page of orders history.

        Payload keys:
          - limit_orders
          - timestamp (epoch seconds) (best-effort; Gemini semantics vary by endpoint version)
        """
        url = f"{self._base_url}/v1/orders/history"
        payload: Dict[str, Any] = {"limit_orders": int(limit_orders)}
        # include timestamp always (some Gemini variants expect it)
        payload["timestamp"] = int(timestamp)

        headers = self._sign_payload("/v1/orders/history", payload)
        with httpx.Client(timeout=30.0) as client:
            r = client.post(url, headers=headers)
            r.raise_for_status()
            data = r.json()
        return data if isinstance(data, list) else []

    def _fetch_order_history_paged(
        self,
        *,
        since: Optional[datetime],
        limit_orders: int,
        max_pages: int,
        sleep_ms: int,
        debug: bool,
    ) -> List[Dict[str, Any]]:
        """
        Paged orders history retrieval (primary backfill).

        Strategy (guarded "walk backwards"):
          - Try starting at timestamp=0; if empty, start at now().
          - Track the oldest timestamp seen; next cursor = oldest_ts - 1
          - Deduplicate by order_id
          - Stop when we cross `since` or when no progress is made
        """
        since_epoch: Optional[int] = None
        if since is not None:
            try:
                since_epoch = int(since.replace(tzinfo=None).timestamp())
            except Exception:
                since_epoch = None

        def order_ts_s(o: Dict[str, Any]) -> Optional[int]:
            # Gemini variants include timestamp (s) and/or timestampms
            if o.get("timestamp") is not None:
                try:
                    return int(o.get("timestamp"))
                except Exception:
                    pass
            if o.get("timestampms") is not None:
                try:
                    return int(int(o.get("timestampms")) / 1000)
                except Exception:
                    pass
            return None

        def post_with_ts(ts_val: int) -> List[Dict[str, Any]]:
            return self._fetch_order_history(limit_orders=int(limit_orders), timestamp=int(ts_val))

        # Attempt 1: timestamp=0 (per common Gemini patterns). If empty, fallback to now().
        start_ts = 0
        try:
            first = post_with_ts(start_ts)
        except Exception as e:
            if debug:
                print("GEMINI DEBUG history_paged first-call failed:", repr(e))
            return []

        if not first:
            start_ts = int(time.time())
            try:
                first = post_with_ts(start_ts)
            except Exception as e:
                if debug:
                    print("GEMINI DEBUG history_paged fallback-call failed:", repr(e))
                return []

        out: List[Dict[str, Any]] = []
        seen_oid: set[str] = set()

        def ingest(batch: List[Dict[str, Any]]) -> Tuple[Optional[int], Optional[int], int]:
            min_ts: Optional[int] = None
            max_ts: Optional[int] = None
            added = 0

            for o in batch or []:
                if not isinstance(o, dict):
                    continue
                oid = str(o.get("order_id") or o.get("id") or "").strip()
                if not oid or oid in seen_oid:
                    continue

                ts = order_ts_s(o)
                if ts is not None:
                    min_ts = ts if min_ts is None else min(min_ts, ts)
                    max_ts = ts if max_ts is None else max(max_ts, ts)

                seen_oid.add(oid)
                out.append(o)
                added += 1

            return min_ts, max_ts, added

        min_ts, max_ts, added = ingest(first)
        if debug:
            print(f"GEMINI DEBUG history_paged page=1 rows={len(first)} added={added} min_ts={min_ts} max_ts={max_ts}")

        if min_ts is None:
            return out

        cursor_ts = int(min_ts) - 1
        last_out_len = len(out)
        last_cursor = cursor_ts

        pages = 1
        while pages < int(max_pages):
            pages += 1

            # Stop once we reached/covered history older than since bound.
            if since_epoch is not None and cursor_ts <= since_epoch:
                break

            if sleep_ms > 0:
                time.sleep(float(sleep_ms) / 1000.0)

            try:
                batch = post_with_ts(cursor_ts)
            except Exception as e:
                if debug:
                    print(f"GEMINI DEBUG history_paged page={pages} failed:", repr(e))
                break

            if not batch:
                if debug:
                    print(f"GEMINI DEBUG history_paged page={pages} empty -> stop")
                break

            b_min, b_max, b_added = ingest(batch)
            if debug:
                print(
                    f"GEMINI DEBUG history_paged page={pages} rows={len(batch)} added={b_added} "
                    f"min_ts={b_min} max_ts={b_max} cursor_in={cursor_ts}"
                )

            if b_min is not None:
                cursor_ts = int(b_min) - 1
            else:
                cursor_ts = cursor_ts - 1

            if len(out) == last_out_len and cursor_ts == last_cursor:
                if debug:
                    print(f"GEMINI DEBUG history_paged no progress -> stop (out_len={len(out)})")
                break

            last_out_len = len(out)
            last_cursor = cursor_ts

        # Post-filter to since lower-bound (keep only >= since)
        if since_epoch is not None:
            filtered: List[Dict[str, Any]] = []
            for o in out:
                if not isinstance(o, dict):
                    continue
                ts = order_ts_s(o)
                if ts is None:
                    filtered.append(o)  # keep unknown ts; harmless
                elif ts >= since_epoch:
                    filtered.append(o)
            return filtered

        return out

    def _fetch_mytrades(self, symbol: Optional[str] = None, limit_trades: int = 500) -> List[Dict[str, Any]]:
        url = f"{self._base_url}/v1/mytrades"
        payload: Dict[str, Any] = {"limit_trades": int(limit_trades)}
        if symbol:
            payload["symbol"] = symbol.lower()

        headers = self._sign_payload("/v1/mytrades", payload)
        with httpx.Client(timeout=30.0) as client:
            r = client.post(url, headers=headers)
            r.raise_for_status()
            data = r.json()
        return data if isinstance(data, list) else []

    def _fetch_mytrades_paged(
        self,
        *,
        symbol: str,
        since: Optional[datetime],
        limit_trades: int,
        max_pages: int,
        sleep_ms: int,
        debug: bool,
    ) -> List[Dict[str, Any]]:
        """
        Optional paged trade history retrieval (secondary backfill).
        """
        sym = (symbol or "").strip().lower()
        if not sym:
            return []

        url = f"{self._base_url}/v1/mytrades"

        since_epoch: Optional[int] = None
        if since is not None:
            try:
                since_epoch = int(since.replace(tzinfo=None).timestamp())
            except Exception:
                since_epoch = None

        def post_with_ts(ts_val: int) -> List[Dict[str, Any]]:
            payload: Dict[str, Any] = {"limit_trades": int(limit_trades), "symbol": sym, "timestamp": int(ts_val)}
            headers = self._sign_payload("/v1/mytrades", payload)
            with httpx.Client(timeout=30.0) as client:
                r = client.post(url, headers=headers)
                r.raise_for_status()
                data = r.json() if r.content else []
            return data if isinstance(data, list) else []

        start_ts = 0
        try:
            first = post_with_ts(start_ts)
        except Exception as e:
            if debug:
                print("GEMINI DEBUG mytrades_paged first-call failed:", repr(e))
            return []

        if not first:
            start_ts = int(time.time())
            try:
                first = post_with_ts(start_ts)
            except Exception as e:
                if debug:
                    print("GEMINI DEBUG mytrades_paged fallback-call failed:", repr(e))
                return []

        out: List[Dict[str, Any]] = []
        seen_tid: set[int] = set()

        def ingest_batch(batch: List[Dict[str, Any]]) -> Tuple[Optional[int], Optional[int]]:
            min_ts: Optional[int] = None
            max_ts: Optional[int] = None

            for t in batch or []:
                if not isinstance(t, dict):
                    continue
                tid = t.get("tid")
                try:
                    tid_i = int(tid) if tid is not None else None
                except Exception:
                    tid_i = None

                if tid_i is not None and tid_i in seen_tid:
                    continue

                ts = t.get("timestamp")
                try:
                    ts_i = int(ts) if ts is not None else None
                except Exception:
                    ts_i = None

                if ts_i is not None:
                    min_ts = ts_i if min_ts is None else min(min_ts, ts_i)
                    max_ts = ts_i if max_ts is None else max(max_ts, ts_i)

                if tid_i is not None:
                    seen_tid.add(tid_i)

                out.append(t)

            return min_ts, max_ts

        min_ts, max_ts = ingest_batch(first)
        if debug:
            print(f"GEMINI DEBUG mytrades_paged[{sym}] page=1 rows={len(first)} min_ts={min_ts} max_ts={max_ts}")

        if since_epoch is not None and min_ts is not None and min_ts <= since_epoch:
            return out

        cursor_ts = (min_ts - 1) if (min_ts is not None) else (int(time.time()) - 1)

        last_out_len = len(out)
        last_cursor = cursor_ts

        pages = 1
        while pages < int(max_pages):
            pages += 1

            if since_epoch is not None and cursor_ts <= since_epoch:
                break

            if sleep_ms > 0:
                time.sleep(float(sleep_ms) / 1000.0)

            try:
                batch = post_with_ts(cursor_ts)
            except Exception as e:
                if debug:
                    print(f"GEMINI DEBUG mytrades_paged[{sym}] page={pages} failed:", repr(e))
                break

            if not batch:
                if debug:
                    print(f"GEMINI DEBUG mytrades_paged[{sym}] page={pages} empty -> stop")
                break

            b_min, b_max = ingest_batch(batch)
            if debug:
                print(f"GEMINI DEBUG mytrades_paged[{sym}] page={pages} rows={len(batch)} min_ts={b_min} max_ts={b_max}")

            if b_min is not None:
                cursor_ts = int(b_min) - 1
            else:
                cursor_ts = cursor_ts - 1

            if len(out) == last_out_len and cursor_ts == last_cursor:
                if debug:
                    print(f"GEMINI DEBUG mytrades_paged[{sym}] no progress -> stop (out_len={len(out)})")
                break

            last_out_len = len(out)
            last_cursor = cursor_ts

            if since_epoch is not None and b_min is not None and b_min <= since_epoch:
                break

        return out

    def _is_terminal(self, status: Optional[str]) -> bool:
        if not status:
            return False
        return str(status).lower().strip() in self._TERMINAL

    def _infer_status(self, o: Dict[str, Any]) -> Optional[str]:
        s = o.get("status")
        if isinstance(s, str) and s.strip():
            return s.lower().strip()

        is_live = o.get("is_live")
        is_cancelled = o.get("is_cancelled")

        filled = self._safe_float(o.get("executed_amount"))
        qty = self._safe_float(o.get("original_amount")) or self._safe_float(o.get("amount"))

        if is_cancelled is True:
            return "canceled"
        if is_live is True:
            return "open"
        if filled is not None and qty is not None and qty > 0 and filled >= qty:
            return "filled"

        rem = self._safe_float(o.get("remaining_amount"))
        if is_live is False and rem is not None and rem == 0:
            return "filled"

        return None

    def _is_probably_terminal(self, o: Dict[str, Any], status: Optional[str]) -> bool:
        if self._is_terminal(status) or (status and str(status).lower().strip() == "filled"):
            return True

        is_live = o.get("is_live")
        is_cancelled = o.get("is_cancelled")

        if is_cancelled is True:
            return True

        if is_live is False:
            rem = self._safe_float(o.get("remaining_amount"))
            if rem is not None and rem == 0:
                return True

            filled = self._safe_float(o.get("executed_amount"))
            qty = self._safe_float(o.get("original_amount")) or self._safe_float(o.get("amount"))
            if filled is not None and qty is not None and qty > 0 and filled >= qty:
                return True

        return False

    def _max_trade_ms(self, o: Dict[str, Any]) -> Optional[int]:
        trades = o.get("trades")
        if not isinstance(trades, list) or not trades:
            return None

        max_ms: Optional[int] = None
        for t in trades:
            if not isinstance(t, dict):
                continue
            if t.get("timestampms") is not None:
                try:
                    v = int(t.get("timestampms"))
                    max_ms = v if max_ms is None else max(max_ms, v)
                except Exception:
                    pass
            elif t.get("timestamp") is not None:
                try:
                    v = int(float(t.get("timestamp")) * 1000.0)
                    max_ms = v if max_ms is None else max(max_ms, v)
                except Exception:
                    pass
        return max_ms

    def _order_times(self, o: Dict[str, Any], status: Optional[str]) -> Tuple[Optional[datetime], Optional[datetime]]:
        created_at: Optional[datetime] = None
        updated_at: Optional[datetime] = None

        if o.get("timestampms") is not None:
            created_at = self._dt_from_ms(o.get("timestampms"))
        elif o.get("timestamp") is not None:
            created_at = self._dt_from_s(o.get("timestamp"))

        if self._is_probably_terminal(o, status):
            if o.get("last_update_timestampms") is not None:
                updated_at = self._dt_from_ms(o.get("last_update_timestampms"))
            else:
                ua = o.get("updated_at")
                if ua is not None:
                    updated_at = self._dt_from_ms(ua) or self._dt_from_s(ua)

            if updated_at is None:
                max_ms = self._max_trade_ms(o)
                if max_ms is not None:
                    updated_at = self._dt_from_ms(max_ms)

        return created_at, updated_at

    def _debug_delayed_fills_from_history(self, hist_orders: List[Dict[str, Any]]) -> None:
        if (os.getenv("GEMINI_DEBUG_DELAYED", "") or "").strip() != "1":
            return

        min_delay_ms = 2000
        try:
            min_delay_ms = int((os.getenv("GEMINI_DEBUG_DELAY_MS", "") or "2000").strip())
        except Exception:
            min_delay_ms = 2000

        dump_json = (os.getenv("GEMINI_DEBUG_DUMP_JSON", "") or "").strip() == "1"

        delayed_count = 0
        first_delayed: Optional[Dict[str, Any]] = None
        first_pair: Optional[Tuple[int, int]] = None

        for ho in hist_orders or []:
            if not isinstance(ho, dict):
                continue

            status = self._infer_status(ho)
            if not self._is_probably_terminal(ho, status):
                continue

            o_ms = ho.get("timestampms")
            if o_ms is None:
                continue
            try:
                o_ms_i = int(o_ms)
            except Exception:
                continue

            t_ms_i = self._max_trade_ms(ho)
            if t_ms_i is None:
                continue

            delta = t_ms_i - o_ms_i
            if delta >= min_delay_ms:
                delayed_count += 1
                if first_delayed is None:
                    first_delayed = ho
                    first_pair = (o_ms_i, t_ms_i)

        print(f"GEMINI DEBUG delayed-fill count in history payload: {delayed_count} (min_delay_ms={min_delay_ms})")

        if first_delayed is None or first_pair is None:
            print("GEMINI DEBUG delayed-fill sample: none found in current history payload")
            return

        o_ms_i, t_ms_i = first_pair
        oid = str(first_delayed.get("order_id") or first_delayed.get("id") or "").strip()
        sym = str(first_delayed.get("symbol") or "").strip()
        print(
            f"GEMINI DEBUG delayed-fill first order_id={oid} symbol={sym} "
            f"delta_ms={t_ms_i - o_ms_i} order_ms={o_ms_i} trade_max_ms={t_ms_i}"
        )

        if dump_json:
            print("GEMINI RAW HISTORY ORDER SAMPLE (delayed fill):")
            print(json.dumps(first_delayed, indent=2, default=str))

    def _aggregate_trades_by_order(self, trades: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        agg: Dict[str, Dict[str, Any]] = {}
        for t in trades or []:
            if not isinstance(t, dict):
                continue
            oid = str(t.get("order_id") or "").strip()
            if not oid:
                continue

            amt = self._safe_float(t.get("amount"))
            px = self._safe_float(t.get("price"))
            fee_amt = self._safe_float(t.get("fee_amount"))
            fee_ccy = t.get("fee_currency")

            t_side = str(t.get("type") or "").strip().lower()

            a = agg.get(oid)
            if a is None:
                a = {
                    "filled_qty": 0.0,
                    "notional": 0.0,
                    "fee": 0.0,
                    "fee_asset": None,
                    "fee_asset_mixed": False,
                    "side": None,
                    "first_ts": None,
                    "last_ts": None,
                    "symbol": None,
                }
                agg[oid] = a

            if a.get("symbol") is None and t.get("symbol"):
                a["symbol"] = str(t.get("symbol")).strip().lower()

            if t_side in ("buy", "sell") and a.get("side") is None:
                a["side"] = t_side

            ts = t.get("timestamp")
            try:
                ts_i = int(ts) if ts is not None else None
            except Exception:
                ts_i = None
            if ts_i is not None:
                a["first_ts"] = ts_i if a["first_ts"] is None else min(int(a["first_ts"]), ts_i)
                a["last_ts"] = ts_i if a["last_ts"] is None else max(int(a["last_ts"]), ts_i)

            if amt is not None and amt > 0:
                a["filled_qty"] += float(amt)
                if px is not None and px > 0:
                    a["notional"] += float(px) * float(amt)

            if fee_amt is not None and fee_amt >= 0:
                a["fee"] += float(fee_amt)

            if fee_ccy:
                fee_ccy_u = str(fee_ccy).upper()
                if a["fee_asset"] is None:
                    a["fee_asset"] = fee_ccy_u
                elif a["fee_asset"] != fee_ccy_u:
                    a["fee_asset_mixed"] = True

        return agg

    def fetch_orders(self, dry_run: bool) -> List[VenueOrder]:
        """
        IMPORTANT: dry_run should NOT disable read-only ingestion.

        Primary backfill (orders history paging):
          GEMINI_TRADES_PAGED=1
          GEMINI_TRADES_SINCE_ISO=2025-01-01T00:00:00Z  (optional)
          GEMINI_TRADES_MAX_PAGES=25                    (optional)
          GEMINI_TRADES_PAGE_LIMIT=500                  (optional)
          GEMINI_TRADES_SLEEP_MS=50                     (optional)

        Optional secondary backfill (mytrades paging):
          GEMINI_MYTRADES_PAGED=1
        """
        try:
            DEBUG = (os.getenv("GEMINI_DEBUG", "0") or "").strip() == "1"

            HISTORY_PAGED = self._env_bool("GEMINI_TRADES_PAGED", default=False)
            HISTORY_SINCE = self._env_iso_dt("GEMINI_TRADES_SINCE_ISO")
            HISTORY_MAX_PAGES = self._env_int("GEMINI_TRADES_MAX_PAGES", 25)
            HISTORY_PAGE_LIMIT = self._env_int("GEMINI_TRADES_PAGE_LIMIT", 500)
            HISTORY_SLEEP_MS = self._env_int("GEMINI_TRADES_SLEEP_MS", 50)

            MYTRADES_PAGED = self._env_bool("GEMINI_MYTRADES_PAGED", default=False)

            open_orders = self._fetch_open_orders()

            if HISTORY_PAGED:
                hist_orders = self._fetch_order_history_paged(
                    since=HISTORY_SINCE,
                    limit_orders=HISTORY_PAGE_LIMIT,
                    max_pages=HISTORY_MAX_PAGES,
                    sleep_ms=HISTORY_SLEEP_MS,
                    debug=DEBUG,
                )
            else:
                # legacy single window
                hist_orders = self._fetch_order_history(limit_orders=500, timestamp=0)

            self._debug_delayed_fills_from_history(hist_orders)

            # Determine symbols we care about, to avoid pulling all trades for all time when paging mytrades.
            sym_set: set[str] = set()
            for src in (open_orders, hist_orders):
                if not isinstance(src, list):
                    continue
                for o in src:
                    if not isinstance(o, dict):
                        continue
                    s = str(o.get("symbol") or "").strip().lower()
                    if s:
                        sym_set.add(s)

            trades: List[Dict[str, Any]] = []
            if MYTRADES_PAGED and sym_set:
                max_pages = HISTORY_MAX_PAGES
                sleep_ms = HISTORY_SLEEP_MS
                limit_trades = HISTORY_PAGE_LIMIT
                for sym in sorted(sym_set):
                    t = self._fetch_mytrades_paged(
                        symbol=sym,
                        since=HISTORY_SINCE,
                        limit_trades=limit_trades,
                        max_pages=max_pages,
                        sleep_ms=sleep_ms,
                        debug=DEBUG,
                    )
                    trades.extend(t)
            else:
                # IMPORTANT: pull per symbol when possible (Gemini /v1/mytrades is commonly symbol-scoped)
                if sym_set:
                    for sym in sorted(sym_set):
                        try:
                            trades.extend(self._fetch_mytrades(symbol=sym, limit_trades=500))
                        except Exception as e:
                            if DEBUG:
                                print(f"GEMINI DEBUG mytrades({sym}) failed:", repr(e))
                            continue
                else:
                    trades = self._fetch_mytrades(symbol=None, limit_trades=500)

            by_oid = self._aggregate_trades_by_order(trades)

            out: List[VenueOrder] = []

            def map_one(o: Dict[str, Any]) -> Optional[VenueOrder]:
                oid = str(o.get("order_id") or o.get("id") or "").strip()
                if not oid:
                    return None

                symbol_venue = str(o.get("symbol") or "").lower().strip()
                symbol_canon = self._canon_from_symbol_venue(symbol_venue)

                side = (o.get("side") or None)
                typ = (o.get("type") or None)

                qty = self._safe_float(o.get("original_amount")) or self._safe_float(o.get("amount"))
                limit_price = self._safe_float(o.get("price"))

                agg = by_oid.get(oid) or {}
                filled_qty = None
                avg_fill_price = None
                fee = None
                fee_asset = None
                total_after_fee = None

                filled_from_trades = agg.get("filled_qty")
                notional = agg.get("notional")
                fee_sum = agg.get("fee")
                fee_ccy = agg.get("fee_asset")
                fee_mixed = agg.get("fee_asset_mixed")

                if isinstance(filled_from_trades, (int, float)) and filled_from_trades > 0:
                    filled_qty = float(filled_from_trades)
                    if isinstance(notional, (int, float)) and notional > 0:
                        avg_fill_price = float(notional) / float(filled_from_trades)

                if filled_qty is None:
                    filled_qty = self._safe_float(o.get("executed_amount"))
                if avg_fill_price is None:
                    avg_fill_price = self._safe_float(o.get("avg_execution_price"))

                # Fee: allow 0.0 (do not require > 0)
                if isinstance(fee_sum, (int, float)) and fee_sum >= 0:
                    fee = float(fee_sum)
                    if (not fee_mixed) and fee_ccy:
                        fee_asset = str(fee_ccy).upper()

                # Notional: trade-derived if present, else fallback to filled_qty * avg_fill_price
                notional_used: Optional[float] = None
                if isinstance(notional, (int, float)) and float(notional) > 0:
                    notional_used = float(notional)
                elif filled_qty is not None and avg_fill_price is not None and filled_qty > 0 and avg_fill_price > 0:
                    notional_used = float(filled_qty) * float(avg_fill_price)

                # Net: "Total minus fee" (per your requirement)
                if notional_used is not None and notional_used > 0:
                    if fee is not None:
                        total_after_fee = float(notional_used) - float(fee)
                    else:
                        total_after_fee = float(notional_used)

                status = self._infer_status(o)
                created_at, updated_at = self._order_times(o, status)

                return {
                    "venue": self.venue,
                    "venue_order_id": oid,
                    "symbol_venue": symbol_venue or "",
                    "symbol_canon": symbol_canon,
                    "side": side,
                    "type": typ,
                    "status": status,
                    "qty": qty,
                    "filled_qty": filled_qty,
                    "limit_price": limit_price,
                    "avg_fill_price": avg_fill_price,
                    "fee": fee,
                    "fee_asset": fee_asset,
                    "total_after_fee": total_after_fee,
                    "created_at": created_at,
                    "updated_at": updated_at,
                }

            seen = set()
            for src in (open_orders, hist_orders):
                if not isinstance(src, list):
                    continue
                for o in src:
                    if not isinstance(o, dict):
                        continue
                    mapped = map_one(o)
                    if not mapped:
                        continue
                    oid = mapped.get("venue_order_id")
                    if oid in seen:
                        continue
                    seen.add(oid)
                    out.append(mapped)

            # Optional: synthesize executed orders from trades outside the history window
            for oid, a in (by_oid or {}).items():
                if not oid or oid in seen:
                    continue

                filled_qty = a.get("filled_qty")
                notional = a.get("notional")
                if not isinstance(filled_qty, (int, float)) or filled_qty <= 0:
                    continue
                if not isinstance(notional, (int, float)) or notional <= 0:
                    continue

                sym = a.get("symbol")
                if not sym:
                    continue

                side = a.get("side")
                if side not in ("buy", "sell"):
                    continue

                avg_px = float(notional) / float(filled_qty)
                ts = a.get("last_ts")
                created_at = self._dt_from_s(ts) if ts is not None else None

                fee = a.get("fee") if isinstance(a.get("fee"), (int, float)) else None
                fee_asset = a.get("fee_asset") if isinstance(a.get("fee_asset"), str) else None

                total_after_fee = None
                if fee is not None:
                    total_after_fee = float(notional) - float(fee)
                else:
                    total_after_fee = float(notional)

                mapped: VenueOrder = {
                    "venue": self.venue,
                    "venue_order_id": str(oid),
                    "symbol_venue": str(sym).lower(),
                    "symbol_canon": self._canon_from_symbol_venue(str(sym).lower()),
                    "side": side,
                    "type": "exchange limit",
                    "status": "filled",
                    "qty": float(filled_qty),
                    "filled_qty": float(filled_qty),
                    "limit_price": None,
                    "avg_fill_price": float(avg_px),
                    "fee": float(fee) if fee is not None else None,
                    "fee_asset": str(fee_asset).upper() if fee_asset else None,
                    "total_after_fee": total_after_fee,
                    "created_at": created_at,
                    "updated_at": created_at,
                }

                seen.add(str(oid))
                out.append(mapped)

            if DEBUG:
                print("GEMINI DEBUG orders rows=", len(out))
                if out:
                    print("GEMINI DEBUG orders sample row=", out[0])

            return out

        except Exception as e:
            print("GEMINI DEBUG fetch_orders ERROR:", repr(e))
            raise

    # ─────────────────────────────────────────────────────────────
    # Public order book
    # ─────────────────────────────────────────────────────────────
    def fetch_orderbook(self, symbol_venue: str, depth: int, dry_run: bool) -> OrderBook:
        url = f"{self._base_url}/v1/book/{symbol_venue}"
        params = {"limit_bids": str(depth), "limit_asks": str(depth)}

        with httpx.Client(timeout=15.0) as client:
            r = client.get(url, params=params)
            r.raise_for_status()
            data = r.json()

        bids_raw = (data.get("bids") or [])[:depth]
        asks_raw = (data.get("asks") or [])[:depth]

        bids = []
        for b in bids_raw:
            try:
                bids.append({"price": float(b["price"]), "qty": float(b["amount"])})
            except Exception:
                continue

        asks = []
        for a in asks_raw:
            try:
                asks.append({"price": float(a["price"]), "qty": float(a["amount"])})
            except Exception:
                continue

        return {"bids": bids, "asks": asks}
