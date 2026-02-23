# backend/app/adapters/dex_trade.py

from __future__ import annotations

from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone
import time
import threading
import hashlib
import os
import logging

import httpx

from .base import ExchangeAdapter, PlacedOrder, BalanceItem, OrderBook, VenueOrder, OrderRules
from ..config import settings

logger = logging.getLogger(__name__)


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _concat_values_sorted(obj: Any) -> str:
    """
    Dex-Trade signing algorithm (per docs):
      - sort keys alphabetically (recursively)
      - concatenate ALL values into a single string (recursively) in sorted key order
      - append secret
      - sha256
    """
    if obj is None:
        return ""
    if isinstance(obj, dict):
        out = []
        for k in sorted(obj.keys(), key=lambda x: str(x)):
            out.append(_concat_values_sorted(obj[k]))
        return "".join(out)
    if isinstance(obj, list):
        return "".join(_concat_values_sorted(v) for v in obj)
    return str(obj)


def _sget(name: str, default: Any = None) -> Any:
    try:
        return getattr(settings, name, default)
    except Exception:
        return default


def _sget_str(name: str, default: str = "") -> str:
    v = _sget(name, default)
    if v is None:
        return ""
    return str(v).strip()


def _sget_bool(name: str, default: bool = False) -> bool:
    v = _sget(name, default)
    try:
        return bool(v)
    except Exception:
        return default


def _float_or(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        s = (os.getenv(name, "") or "").strip()
        if not s:
            return int(default)
        return int(s)
    except Exception:
        return int(default)


def _dex_trade_balance_dust() -> float:
    v = _sget("dex_trade_balance_dust", None)
    if v is not None:
        try:
            return float(v)
        except Exception:
            pass

    s = (os.getenv("DEX_TRADE_BALANCE_DUST", "") or "").strip()
    if not s:
        s = (os.getenv("BALANCE_DUST_THRESHOLD", "") or "").strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


class DexTradeAdapter(ExchangeAdapter):
    venue = "dex_trade"

    _DEFAULT_BASE_URL = "https://api.dex-trade.com"

    _rid_lock = threading.Lock()
    _last_request_id: int = 0

    _symbols_cache_lock = threading.Lock()
    _pair_to_base_quote: Dict[str, Tuple[str, str]] = {}
    _pair_to_decimals: Dict[str, Tuple[int, int, int]] = {}  # pair -> (rate_dec, base_dec, quote_dec)
    _pair_to_id: Dict[str, str] = {}
    _id_to_pair: Dict[str, str] = {}
    _canon_to_pair: Dict[str, str] = {}

    # allow resolving DOGEUSDT <-> DOGE_USDT <-> DOGE-USDT reliably
    _pair_sanitized_to_pair: Dict[str, str] = {}

    _symbols_cache_ts: float = 0.0
    _SYMBOLS_CACHE_TTL_SEC = 300.0

    _TERMINAL = {"filled", "canceled", "cancelled", "rejected", "done", "closed", "expired", "failed"}

    # ─────────────────────────────────────────────────────────────
    # Dex-Trade PUBLIC API protection (Dex-only)
    # ─────────────────────────────────────────────────────────────
    _public_lock = threading.Lock()
    _public_next_ts: float = 0.0
    _public_cooldown_until: float = 0.0  # when 429 happens, pause public calls briefly

    _book_cache_lock = threading.Lock()
    _book_cache: Dict[str, Tuple[float, OrderBook]] = {}  # key -> (ts, ob)
    _book_neg_cache: Dict[str, float] = {}               # key -> neg_cache_until (bad request / not found)

    _log_suppress_lock = threading.Lock()
    _log_suppress_until: Dict[str, float] = {}           # key -> ts until we suppress logs

    def __init__(self) -> None:
        base_url = _sget_str("dex_trade_base_url", "") or self._DEFAULT_BASE_URL
        self._base_url = base_url.rstrip("/")

        self._token = _sget_str("dex_trade_login_token", "")
        self._secret = _sget_str("dex_trade_secret", "")

        self._timeout = httpx.Timeout(15.0, connect=10.0)

        # Tunables (Dex only). Defaults are conservative to avoid 429.
        # You can override via env if you want it faster.
        self._public_min_interval = max(0.01, float(_env_int("DEX_TRADE_PUBLIC_MIN_INTERVAL_MS", 250)) / 1000.0)
        self._public_429_cooldown = max(0.5, float(_env_int("DEX_TRADE_PUBLIC_429_COOLDOWN_MS", 5000)) / 1000.0)
        self._book_cache_ttl = max(0.1, float(_env_int("DEX_TRADE_BOOK_CACHE_MS", 800)) / 1000.0)
        self._book_neg_cache_ttl = max(2.0, float(_env_int("DEX_TRADE_BOOK_NEG_CACHE_MS", 60000)) / 1000.0)
        self._log_suppress_ttl = max(0.5, float(_env_int("DEX_TRADE_BOOK_LOG_SUPPRESS_MS", 3000)) / 1000.0)

    # ─────────────────────────────────────────────────────────────
    # Private API enablement
    # ─────────────────────────────────────────────────────────────
    def _effective_enabled(self) -> bool:
        fn = _sget("dex_trade_effective_enabled", None)
        if callable(fn):
            try:
                return bool(fn())
            except Exception:
                return False

        enabled = _sget_bool("dex_trade_enabled", False)
        if not enabled:
            return False
        if not (self._token and self._secret):
            return False
        if not (self._base_url.startswith("https://") or self._base_url.startswith("http://")):
            return False
        return True

    # ─────────────────────────────────────────────────────────────
    # HTTP helpers
    # ─────────────────────────────────────────────────────────────
    @classmethod
    def _next_request_id(cls) -> str:
        with cls._rid_lock:
            now = time.time_ns() // 1_000  # microseconds
            rid = now if now > cls._last_request_id else cls._last_request_id + 1
            cls._last_request_id = rid
            return str(rid)

    def _public_throttle(self) -> None:
        """
        Global throttle for Dex-Trade public endpoints (Dex-only).
        Prevents bursts that trigger 429.
        """
        with self._public_lock:
            now = time.time()
            if now < self._public_cooldown_until:
                # still in cooldown after a 429; do not spam
                raise RuntimeError("DexTrade public API in cooldown after 429")

            if now < self._public_next_ts:
                sleep_s = self._public_next_ts - now
            else:
                sleep_s = 0.0

            # schedule the next slot
            self._public_next_ts = max(now, self._public_next_ts) + self._public_min_interval

        if sleep_s > 0:
            # small sleep outside lock
            time.sleep(min(sleep_s, 2.0))

    def _set_public_cooldown(self) -> None:
        with self._public_lock:
            self._public_cooldown_until = max(self._public_cooldown_until, time.time() + self._public_429_cooldown)

    def _should_log(self, key: str) -> bool:
        now = time.time()
        with self._log_suppress_lock:
            until = self._log_suppress_until.get(key, 0.0)
            if now < until:
                return False
            self._log_suppress_until[key] = now + self._log_suppress_ttl
            return True

    def _public_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Dex-Trade public GET with Dex-only throttling and 429 cooldown.
        """
        url = f"{self._base_url}{path}"
        self._public_throttle()

        with httpx.Client(timeout=self._timeout) as client:
            r = client.get(url, params=params)
            if r.status_code == 429:
                # enter cooldown so we stop hammering
                self._set_public_cooldown()
            r.raise_for_status()
            return r.json()

    def _private_post(self, path: str, payload: Dict[str, Any]) -> Any:
        if not self._effective_enabled():
            raise RuntimeError(
                "Dex-Trade private API is not enabled/configured. "
                "Set DEX_TRADE_ENABLED=true and provide DEX_TRADE_LOGIN_TOKEN + DEX_TRADE_SECRET."
            )

        if not payload.get("request_id"):
            payload["request_id"] = self._next_request_id()

        sig_base = _concat_values_sorted(payload) + self._secret
        sig = _sha256_hex(sig_base)

        headers = {
            "content-type": "application/json",
            "login-token": self._token,
            "X-Auth-Sign": sig,
        }

        url = f"{self._base_url}{path}"
        with httpx.Client(timeout=self._timeout) as client:
            r = client.post(url, json=payload, headers=headers)
            r.raise_for_status()
            return r.json()

    # ─────────────────────────────────────────────────────────────
    # Symbols cache
    # ─────────────────────────────────────────────────────────────
    @staticmethod
    def _canon(base: str, quote: str) -> str:
        return f"{base.upper()}-{quote.upper()}"

    @staticmethod
    def _pow10_inv(n: Optional[int]) -> Optional[float]:
        if n is None:
            return None
        try:
            nn = int(n)
            if nn < 0:
                return None
            return 10 ** (-nn)
        except Exception:
            return None

    @staticmethod
    def _sanitize_pair_key(s: str) -> str:
        # keep alnum only; this makes DOGE_USDT, DOGE-USDT, DOGE/USDT => DOGEUSDT
        return "".join(ch for ch in (s or "").upper() if ch.isalnum())

    def _ensure_symbols_cache(self, force: bool = False) -> None:
        now = time.time()
        with self._symbols_cache_lock:
            if not force and self._pair_to_base_quote and (now - self._symbols_cache_ts) < self._SYMBOLS_CACHE_TTL_SEC:
                return

        try:
            js = self._public_get("/v1/public/symbols")
        except Exception:
            return

        if not isinstance(js, dict):
            return
        if js.get("status") is False:
            return

        data = js.get("data") or []
        if not isinstance(data, list):
            return

        pair_to_bq: Dict[str, Tuple[str, str]] = {}
        pair_to_dec: Dict[str, Tuple[int, int, int]] = {}
        pair_to_id: Dict[str, str] = {}
        id_to_pair: Dict[str, str] = {}
        canon_to_pair: Dict[str, str] = {}
        sanitized_to_pair: Dict[str, str] = {}

        for row in data:
            if not isinstance(row, dict):
                continue
            try:
                rid = row.get("id")
                pair = str(row.get("pair", "")).upper().strip()
                base = str(row.get("base", "")).upper().strip()
                quote = str(row.get("quote", "")).upper().strip()
                rate_dec = int(row.get("rate_decimal", 0) or 0)
                base_dec = int(row.get("base_decimal", 0) or 0)
                quote_dec = int(row.get("quote_decimal", 0) or 0)

                if pair and base and quote:
                    pair_to_bq[pair] = (base, quote)
                    pair_to_dec[pair] = (rate_dec, base_dec, quote_dec)

                    canon = self._canon(base, quote)
                    canon_to_pair[canon] = pair

                    sp = self._sanitize_pair_key(pair)
                    if sp and sp not in sanitized_to_pair:
                        sanitized_to_pair[sp] = pair

                    if rid is not None and str(rid).strip() != "":
                        rid_s = str(rid).strip()
                        pair_to_id[pair] = rid_s
                        id_to_pair[rid_s] = pair
            except Exception:
                continue

        with self._symbols_cache_lock:
            self._pair_to_base_quote = pair_to_bq
            self._pair_to_decimals = pair_to_dec
            self._pair_to_id = pair_to_id
            self._id_to_pair = id_to_pair
            self._canon_to_pair = canon_to_pair
            self._pair_sanitized_to_pair = sanitized_to_pair
            self._symbols_cache_ts = time.time()

    def _canon_from_pair(self, pair: str) -> str:
        self._ensure_symbols_cache()
        p = (pair or "").upper().strip()
        with self._symbols_cache_lock:
            bq = self._pair_to_base_quote.get(p)
        if bq:
            return self._canon(bq[0], bq[1])

        if "_" in p:
            a, b = p.split("_", 1)
            if a and b:
                return self._canon(a, b)
        if "-" in p:
            a, b = p.split("-", 1)
            if a and b:
                return self._canon(a, b)
        if "/" in p:
            a, b = p.split("/", 1)
            if a and b:
                return self._canon(a, b)
        return p

    def _pair_from_pair_id(self, pair_id: Any) -> Optional[str]:
        if pair_id is None or pair_id == "":
            return None
        self._ensure_symbols_cache()
        pid = str(pair_id).strip()
        if not pid:
            return None
        with self._symbols_cache_lock:
            return self._id_to_pair.get(pid)

    def _normalize_pair(self, symbol_or_pair: str) -> str:
        """
        Accepts any of:
          - Dex pair variants: DOGEUSDT, DOGE_USDT
          - Canonical: DOGE-USDT
          - Slash: DOGE/USDT
        Returns the exact Dex 'pair' string from /public/symbols when possible.

        IMPORTANT:
          - If the symbols cache isn't populated/ready, DO NOT guess (no sanitized fallback).
          - Only pass through an underscore-form pair when cache isn't ready (best available non-guess).
        """
        s = (symbol_or_pair or "").strip().upper()
        if not s:
            return ""

        self._ensure_symbols_cache()
        with self._symbols_cache_lock:
            cache_ready = bool(self._pair_to_base_quote)

            if s in self._pair_to_base_quote:
                return s
            if s in self._canon_to_pair:
                return self._canon_to_pair[s]

        if not cache_ready:
            if "_" in s and all(ch.isalnum() or ch == "_" for ch in s):
                return s
            return ""

        sp = self._sanitize_pair_key(s)
        if sp:
            with self._symbols_cache_lock:
                p2 = self._pair_sanitized_to_pair.get(sp)
            if p2:
                return p2

        base = quote = None
        if "-" in s:
            base, quote = s.split("-", 1)
        elif "_" in s:
            base, quote = s.split("_", 1)
        elif "/" in s:
            base, quote = s.split("/", 1)

        if base and quote:
            canon = f"{base.strip().upper()}-{quote.strip().upper()}"
            with self._symbols_cache_lock:
                p = self._canon_to_pair.get(canon)
            if p:
                return p

        return ""

    def _assets_from_pair(self, pair: str) -> Tuple[Optional[str], Optional[str]]:
        p = (pair or "").upper().strip()
        if not p:
            return None, None
        self._ensure_symbols_cache()
        with self._symbols_cache_lock:
            bq = self._pair_to_base_quote.get(p)
        if bq:
            return bq[0], bq[1]

        if "_" in p:
            a, b = p.split("_", 1)
            return (a or None), (b or None)
        if "-" in p:
            a, b = p.split("-", 1)
            return (a or None), (b or None)
        if "/" in p:
            a, b = p.split("/", 1)
            return (a or None), (b or None)
        return None, None

    # ─────────────────────────────────────────────────────────────
    # Normalization helpers
    # ─────────────────────────────────────────────────────────────
    @staticmethod
    def _map_side_to_int(side: str) -> int:
        s = (side or "").lower()
        return 0 if s == "buy" else 1

    @staticmethod
    def _map_type_trade(type_: str) -> int:
        t = (type_ or "").lower()
        return 1 if t == "market" else 0

    @staticmethod
    def _status_from_dex(status_int: Any, vol: Optional[float] = None, vol_done: Optional[float] = None) -> str:
        if isinstance(status_int, str):
            s = status_int.strip().lower()
            if s in {"0", "1", "2", "3"}:
                try:
                    status_int = int(s)
                except Exception:
                    return "unknown"
            else:
                if s in {"in process", "in_process", "processing"}:
                    return "acked"
                if s in {"added", "added to book", "book"}:
                    return "open"
                if s in {"filled"}:
                    return "filled"
                if s in {"canceled", "cancelled", "cancel"}:
                    return "canceled"
                if s in {"rejected", "failed", "expired"}:
                    return "rejected"
                if s in {"done", "closed"}:
                    return "filled"
                return s or "unknown"

        try:
            st = int(status_int)
        except Exception:
            return "unknown"

        if st == 0:
            return "acked"
        if st == 1:
            return "open"
        if st == 2:
            return "filled"
        if st == 3:
            try:
                if vol is not None and vol_done is not None and vol > 0 and vol_done >= vol:
                    return "filled"
            except Exception:
                pass
            return "canceled"
        return "unknown"

    @staticmethod
    def _parse_any_dt(ts: Any) -> Optional[datetime]:
        if ts is None or ts == "":
            return None

        try:
            n = float(ts)
            if n >= 1_000_000_000_000:
                n = n / 1000.0
            return datetime.fromtimestamp(int(n), tz=timezone.utc)
        except Exception:
            pass

        try:
            s = str(ts).strip()
            if not s:
                return None
            s = s.replace("Z", "+00:00")
            if " " in s and "T" not in s:
                s = s.replace(" ", "T", 1)
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt
        except Exception:
            return None

    @staticmethod
    def _first(*vals: Any) -> Any:
        for v in vals:
            if v is None:
                continue
            if isinstance(v, str) and v.strip() == "":
                continue
            return v
        return None

    def _build_venue_order_from_row(self, row: Dict[str, Any]) -> Optional[VenueOrder]:
        try:
            oid_raw = self._first(row.get("id"), row.get("order_id"), row.get("orderId"), row.get("orderID"))
            oid = str(oid_raw or "").strip()
            if not oid:
                return None

            pair_raw = self._first(row.get("pair"), row.get("symbol"), row.get("pair_name"), row.get("pairName"))
            pair = str(pair_raw or "").upper().strip() if pair_raw is not None else ""
            if not pair:
                pid = self._first(row.get("pair_id"), row.get("pairId"), row.get("pairID"))
                pair_mapped = self._pair_from_pair_id(pid)
                pair = (pair_mapped or "").upper().strip()

            if not pair:
                return None

            base_asset, quote_asset = self._assets_from_pair(pair)

            vol = _float_or(self._first(row.get("volume"), row.get("qty"), row.get("amount")), 0.0)
            vol_done = _float_or(
                self._first(row.get("volume_done"), row.get("volumeDone"), row.get("filled"), row.get("executed")),
                0.0,
            )

            st_raw = self._first(row.get("status"), row.get("status_id"), row.get("statusId"), row.get("state"))
            status = self._status_from_dex(st_raw, vol, vol_done)

            side = "buy"
            side_int = self._first(row.get("type"), row.get("side_type"), row.get("sideType"))
            if isinstance(side_int, str) and side_int.lower() in {"buy", "sell"}:
                side = side_int.lower()
            else:
                try:
                    side = "buy" if int(side_int or 0) == 0 else "sell"
                except Exception:
                    side_s = str(self._first(row.get("side"), "")).lower().strip()
                    if side_s in {"buy", "sell"}:
                        side = side_s

            type_trade = self._first(
                row.get("type_trade"),
                row.get("typeTrade"),
                row.get("ord_type"),
                row.get("order_type"),
            )
            otype = "limit"
            try:
                otype = "market" if int(type_trade or 0) == 1 else "limit"
            except Exception:
                t = str(type_trade or "").lower().strip()
                if t == "market":
                    otype = "market"
                elif t == "limit":
                    otype = "limit"

            t_create = self._first(row.get("time_create"), row.get("created_at"), row.get("time"), row.get("createdAt"))
            created_at = self._parse_any_dt(t_create)

            t_upd = self._first(
                row.get("time_update"),
                row.get("time_done"),
                row.get("time_close"),
                row.get("updated_at"),
                row.get("updatedAt"),
                row.get("closed_at"),
                row.get("closedAt"),
            )
            updated_at = self._parse_any_dt(t_upd)

            symbol_canon = self._canon_from_pair(pair)

            limit_price = None
            px_raw = self._first(row.get("rate"), row.get("price"), row.get("limit_price"), row.get("limitPrice"))
            if otype == "limit" and px_raw is not None:
                try:
                    limit_price = float(px_raw)
                except Exception:
                    limit_price = None

            avg_fill_price = None
            avg_raw = self._first(
                row.get("avg_fill_price"),
                row.get("avgFillPrice"),
                row.get("rate_done"),
                row.get("avg_rate"),
            )
            if avg_raw is not None:
                try:
                    avg_fill_price = float(avg_raw)
                except Exception:
                    avg_fill_price = None

            fee = None
            fee_asset = None

            fee_raw = self._first(row.get("fee"), row.get("commission"))
            fee_asset_raw = self._first(
                row.get("fee_asset"),
                row.get("feeAsset"),
                row.get("commission_asset"),
                row.get("commissionAsset"),
            )

            if fee_asset_raw is not None:
                fee_asset = str(fee_asset_raw).upper().strip() or None

            fee_val: Optional[float] = None
            if fee_raw is not None:
                try:
                    fee_val = float(fee_raw)
                except Exception:
                    fee_val = None

            qty_for_gross = vol_done if (vol_done or 0.0) > 0 else vol
            px_for_gross = avg_fill_price if (avg_fill_price is not None) else limit_price
            gross_quote: Optional[float] = None
            if qty_for_gross and px_for_gross is not None:
                try:
                    gross_quote = float(qty_for_gross) * float(px_for_gross)
                except Exception:
                    gross_quote = None

            if fee_val is not None:
                if fee_asset:
                    fee = fee_val
                else:
                    if gross_quote is not None and gross_quote > 0 and 0 < fee_val <= 5:
                        fee = gross_quote * (fee_val / 100.0)
                        fee_asset = quote_asset or fee_asset
                    else:
                        fee = fee_val
                        fee_asset = quote_asset or fee_asset

            total_after_fee = None
            if gross_quote is not None and fee is not None and quote_asset is not None:
                if fee_asset is None or fee_asset == quote_asset:
                    try:
                        total_after_fee = float(gross_quote) - float(fee)
                    except Exception:
                        total_after_fee = None

            return {
                "venue": self.venue,
                "venue_order_id": oid,
                "symbol_canon": symbol_canon,
                "symbol_venue": pair,
                "side": side,
                "type": otype,
                "status": status,
                "status_raw": str(st_raw),
                "cancel_ref": f"{self.venue}:{oid}",
                "qty": float(vol) if vol is not None else 0.0,
                "filled_qty": float(vol_done) if vol_done is not None else 0.0,
                "limit_price": limit_price,
                "avg_fill_price": avg_fill_price,
                "fee": fee,
                "fee_asset": fee_asset,
                "total_after_fee": total_after_fee,
                "created_at": created_at,
                "updated_at": updated_at,
            }
        except Exception:
            return None

    # ─────────────────────────────────────────────────────────────
    # Orders endpoints
    # ─────────────────────────────────────────────────────────────
    def _history_pagination_cfg(self) -> Tuple[int, int, int]:
        page_start = _env_int("DEX_TRADE_HISTORY_PAGE_START", 1)
        max_pages = _env_int("DEX_TRADE_HISTORY_MAX_PAGES", 5)
        limit = _env_int("DEX_TRADE_HISTORY_LIMIT", 200)

        s_page_start = _sget("dex_trade_history_page_start", None)
        s_max_pages = _sget("dex_trade_history_max_pages", None)
        s_limit = _sget("dex_trade_history_limit", None)

        try:
            if s_page_start is not None:
                page_start = int(s_page_start)
        except Exception:
            pass
        try:
            if s_max_pages is not None:
                max_pages = int(s_max_pages)
        except Exception:
            pass
        try:
            if s_limit is not None:
                limit = int(s_limit)
        except Exception:
            pass

        if max_pages < 1:
            max_pages = 1
        if max_pages > 50:
            max_pages = 50
        if limit < 10:
            limit = 10
        if limit > 500:
            limit = 500

        return page_start, max_pages, limit

    def _fetch_active_orders(self) -> List[VenueOrder]:
        try:
            js = self._private_post("/v1/private/orders", {"request_id": self._next_request_id()})
        except Exception:
            return []

        if not isinstance(js, dict) or not bool(js.get("status")):
            return []

        rows = ((js.get("data") or {}).get("list")) or []
        if not isinstance(rows, list):
            if isinstance(js.get("data"), list):
                rows = js.get("data")
            else:
                return []

        out: List[VenueOrder] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            vo = self._build_venue_order_from_row(row)
            if vo:
                out.append(vo)
        return out

    # ─────────────────────────────────────────────────────────────
    # Transfers (Deposits / Withdrawals)
    # Normalized shape expected by routers:
    #   {type: "Deposit"|"Withdrawal", eid, currency, amount, timestampms, status, txHash, raw}
    # ─────────────────────────────────────────────────────────────
    def fetch_transfers(
        self,
        since_dt,
        kinds,
        currency=None,
        limit_transfers: int = 200,
        max_pages: int = 200,
        mode: str = "days",
    ):
        import hashlib
        import json
        from datetime import datetime, timezone

        def _to_ms(val):
            if val is None:
                return None
            # numeric seconds/ms
            if isinstance(val, (int, float)):
                x = float(val)
                if x > 1e12:
                    return int(x)
                if x > 1e9:
                    return int(x * 1000.0)
                return int(x)
            # numeric string
            if isinstance(val, str):
                s = val.strip()
                if not s:
                    return None
                if s.isdigit():
                    return _to_ms(int(s))
                # ISO-ish
                try:
                    s2 = s.replace("Z", "+00:00")
                    dt = datetime.fromisoformat(s2)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return int(dt.timestamp() * 1000)
                except Exception:
                    return None
            return None

        def _kind_of_row(r: dict):
            k = str(
                r.get("type")
                or r.get("kind")
                or r.get("operation")
                or r.get("action")
                or r.get("category")
                or ""
            ).lower()
            if "deposit" in k:
                return "deposit"
            if "withdraw" in k:
                return "withdrawal"
            return None

        want = {str(k).lower() for k in (kinds or [])}
        want = {"withdrawal" if k in ("withdraw", "withdrawals") else k for k in want}
        want = {"deposit" if k in ("deposits",) else k for k in want}

        since_ms = None
        if since_dt is not None:
            try:
                since_ms = int(since_dt.timestamp() * 1000)
            except Exception:
                since_ms = None

        out = []
        page = 1
        api_limit = max(1, min(int(limit_transfers or 200), 200))

        while page <= int(max_pages or 1):
            payload = {
                "page": page,
                "limit": api_limit,
                "format_number": 1,
                "request_id": self._next_request_id(),
            }
            js = self._private_post("/v1/private/history", payload)
            rows = []
            if isinstance(js, dict):
                rows = js.get("data") or []
            if not isinstance(rows, list) or not rows:
                break

            for r in rows:
                if not isinstance(r, dict):
                    continue

                kind = _kind_of_row(r)
                if kind is None:
                    continue
                if want and kind not in want:
                    continue

                cur = r.get("currency") or r.get("coin") or r.get("asset") or r.get("symbol")
                if currency and cur and str(cur).upper() != str(currency).upper():
                    continue

                ts_ms = _to_ms(
                    r.get("timestampms")
                    or r.get("timestamp")
                    or r.get("time")
                    or r.get("created_at")
                    or r.get("created")
                    or r.get("date")
                )
                if since_ms is not None and ts_ms is not None and ts_ms < since_ms:
                    continue

                amt = r.get("amount")
                if amt is None:
                    amt = r.get("qty") or r.get("quantity") or r.get("value")

                status = r.get("status") or r.get("state") or "UNKNOWN"
                txh = r.get("txHash") or r.get("txid") or r.get("hash") or r.get("tx_hash")

                eid = r.get("id") or r.get("txid") or r.get("tx_hash") or r.get("hash")
                if not eid:
                    blob = json.dumps(r, sort_keys=True, separators=(",", ":"), default=str)
                    eid = hashlib.sha1(blob.encode("utf-8")).hexdigest()

                out.append(
                    {
                        "type": "Deposit" if kind == "deposit" else "Withdrawal",
                        "eid": str(eid),
                        "currency": str(cur) if cur is not None else None,
                        "amount": str(amt) if amt is not None else None,
                        "timestampms": ts_ms,
                        "status": str(status),
                        "txHash": txh,
                        "raw": r,
                    }
                )

            # continue paging; stop early if we've clearly exceeded target
            if len(out) >= int(limit_transfers or 0) > 0:
                break

            page += 1

        return out

    def _fetch_history_orders(self, max_pages: int = 5, page_limit: int = 200) -> List[VenueOrder]:
        page_start, cfg_max_pages, cfg_limit = self._history_pagination_cfg()
        max_pages_eff = int(max_pages or cfg_max_pages)
        limit_eff = int(page_limit or cfg_limit)

        out: List[VenueOrder] = []
        pages_to_fetch: List[int] = [page_start + i for i in range(max_pages_eff)]
        seen_pages = set()
        total_pages: Optional[int] = None

        def _fetch_page(page: int) -> Optional[List[Dict[str, Any]]]:
            nonlocal total_pages
            payload: Dict[str, Any] = {
                "request_id": self._next_request_id(),
                "page": page,
                "limit": limit_eff,
                "format_number": 1,
            }
            try:
                js = self._private_post("/v1/private/history", payload)
            except Exception:
                return None

            if not isinstance(js, dict) or not bool(js.get("status")):
                return None

            data = js.get("data") or {}
            rows = None

            if isinstance(data, dict):
                for k in ("pages", "total_pages", "totalPages", "page_count", "pageCount"):
                    if data.get(k) is not None:
                        try:
                            tp = int(data.get(k))
                            if tp > 0 and (total_pages is None or tp > total_pages):
                                total_pages = tp
                            break
                        except Exception:
                            pass
                rows = data.get("list") or data.get("orders") or data.get("history")
            elif isinstance(data, list):
                rows = data

            if not isinstance(rows, list):
                return []
            return rows

        for page in pages_to_fetch:
            if page in seen_pages:
                continue
            seen_pages.add(page)

            rows = _fetch_page(page)
            if rows is None:
                break
            if len(rows) == 0:
                break

            for row in rows:
                if not isinstance(row, dict):
                    continue
                vo = self._build_venue_order_from_row(row)
                if vo:
                    out.append(vo)

            if len(rows) < int(limit_eff):
                break

        if total_pages and total_pages > 1:
            start_tail = max(1, total_pages - (max_pages_eff - 1))
            for page in range(start_tail, total_pages + 1):
                if page in seen_pages:
                    continue
                seen_pages.add(page)

                rows = _fetch_page(page)
                if rows is None:
                    break
                if len(rows) == 0:
                    continue

                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    vo = self._build_venue_order_from_row(row)
                    if vo:
                        out.append(vo)

        return out

    # ─────────────────────────────────────────────────────────────
    # ExchangeAdapter required methods
    # ─────────────────────────────────────────────────────────────
    def resolve_symbol(self, symbol_canon: str) -> str:
        return self._normalize_pair(symbol_canon)

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
        _ = client_order_id
        _ = tif
        _ = post_only

        pair = self._normalize_pair(symbol_venue)
        if not pair:
            return {"status": "rejected", "reject_reason": "Missing symbol_venue"}

        type_trade = self._map_type_trade(type_)
        side_int = self._map_side_to_int(side)

        payload: Dict[str, Any] = {
            "type_trade": type_trade,  # 0 limit, 1 market
            "type": side_int,          # 0 buy, 1 sell
            "volume": str(qty),
            "pair": pair,
            "request_id": self._next_request_id(),
        }

        if type_trade == 0:
            if limit_price is None:
                return {"status": "rejected", "reject_reason": "Limit order missing limit_price"}
            payload["rate"] = str(limit_price)
        else:
            if limit_price is not None:
                payload["rate"] = str(limit_price)

        if dry_run:
            return {"status": "acked", "venue_order_id": "dryrun", "raw": {"dry_run": True, "payload": payload}}

        try:
            js = self._private_post("/v1/private/create-order", payload)
        except Exception as e:
            return {"status": "rejected", "reject_reason": f"create-order failed: {e}"}

        if not isinstance(js, dict) or not bool(js.get("status")):
            msg = ""
            if isinstance(js, dict):
                msg = str(js.get("error") or js.get("message") or "Unknown error")
            return {"status": "rejected", "reject_reason": msg or "create-order failed", "raw": js}

        order_id = ""
        try:
            order_id = str((js.get("data") or {}).get("id") or "").strip()
        except Exception:
            order_id = ""

        placed: PlacedOrder = {
            "status": "acked",
            "venue_order_id": order_id,
            "raw": js,
            "cancel_ref": f"{self.venue}:{order_id}" if order_id else "",
            "status_raw": "created",
        }

        if order_id:
            try:
                st_js = self._private_post(
                    "/v1/private/get-order",
                    {"request_id": self._next_request_id(), "order_id": order_id},
                )
                if isinstance(st_js, dict):
                    data = st_js.get("data") or {}
                    if isinstance(data, dict):
                        st = self._status_from_dex(data.get("status"), data.get("volume"), data.get("volume_done"))
                        if st in ("open", "filled", "canceled", "cancelled", "rejected"):
                            placed["status"] = "canceled" if st == "cancelled" else st
                            placed["status_raw"] = str(data.get("status"))
            except Exception:
                pass

        return placed

    def cancel_order(self, venue_order_id: str, dry_run: bool) -> bool:
        oid = (venue_order_id or "").strip()
        if not oid:
            return False
        if dry_run:
            return True
        try:
            js = self._private_post(
                "/v1/private/delete-order",
                {"request_id": self._next_request_id(), "order_id": oid},
            )
            return bool(js.get("status")) if isinstance(js, dict) else False
        except Exception:
            return False

    def fetch_balances(self, dry_run: bool) -> List[BalanceItem]:
        if dry_run:
            return []
        try:
            js = self._private_post("/v1/private/balances", {"request_id": self._next_request_id()})
        except Exception:
            return []

        if not isinstance(js, dict) or not bool(js.get("status")):
            return []

        rows = ((js.get("data") or {}).get("list")) or []
        if not isinstance(rows, list):
            return []

        dust = _dex_trade_balance_dust()
        out: List[BalanceItem] = []

        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                cur = (row.get("currency") or {}).get("iso3")
                asset = str(cur or "").upper().strip()
                if not asset:
                    continue

                balances = row.get("balances") or {}
                total = _float_or(balances.get("total"), 0.0)
                avail = _float_or(balances.get("available"), 0.0)
                hold = total - avail
                if hold < 0:
                    hold = 0.0

                if abs(total) <= dust and abs(avail) <= dust and abs(hold) <= dust:
                    continue

                out.append({"asset": asset, "total": total, "available": avail, "hold": hold})
            except Exception:
                continue

        return out

    def fetch_orderbook(self, symbol_venue: str, depth: int, dry_run: bool) -> OrderBook:
        _ = dry_run
        d = max(1, min(200, int(depth or 25)))

        def _parse_book(js: Any) -> OrderBook:
            if not isinstance(js, dict) or js.get("status") is False:
                return {"bids": [], "asks": []}

            data = js.get("data") or {}
            if not isinstance(data, dict):
                return {"bids": [], "asks": []}

            buys = data.get("buy") if data.get("buy") is not None else data.get("bids", [])
            sells = data.get("sell") if data.get("sell") is not None else data.get("asks", [])

            if not isinstance(buys, list):
                buys = []
            if not isinstance(sells, list):
                sells = []

            def _parse_level(lvl: Any) -> Optional[Tuple[float, float]]:
                if isinstance(lvl, dict):
                    px = lvl.get("rate", lvl.get("price"))
                    qy = lvl.get("volume", lvl.get("qty"))
                    try:
                        return float(px), float(qy)
                    except Exception:
                        return None
                if isinstance(lvl, (list, tuple)) and len(lvl) >= 2:
                    try:
                        return float(lvl[0]), float(lvl[1])
                    except Exception:
                        return None
                return None

            bids: List[Dict[str, float]] = []
            asks: List[Dict[str, float]] = []

            for lvl in buys[:d]:
                p = _parse_level(lvl)
                if p:
                    bids.append({"price": p[0], "qty": p[1]})

            for lvl in sells[:d]:
                p = _parse_level(lvl)
                if p:
                    asks.append({"price": p[0], "qty": p[1]})

            bids.sort(key=lambda x: x["price"], reverse=True)
            asks.sort(key=lambda x: x["price"])
            return {"bids": bids[:d], "asks": asks[:d]}

        def _cache_key(pair_param: str) -> str:
            return f"book:{pair_param.upper().strip()}"

        def _get_cached(pair_param: str) -> Optional[OrderBook]:
            key = _cache_key(pair_param)
            now = time.time()
            with self._book_cache_lock:
                neg_until = self._book_neg_cache.get(key, 0.0)
                if now < neg_until:
                    return {"bids": [], "asks": []}

                ent = self._book_cache.get(key)
                if ent:
                    ts, ob = ent
                    if (now - ts) <= self._book_cache_ttl:
                        return ob
            return None

        def _put_cached(pair_param: str, ob: OrderBook) -> None:
            key = _cache_key(pair_param)
            with self._book_cache_lock:
                self._book_cache[key] = (time.time(), ob)

        def _put_neg_cached(pair_param: str) -> None:
            key = _cache_key(pair_param)
            with self._book_cache_lock:
                self._book_neg_cache[key] = time.time() + self._book_neg_cache_ttl

        def _try_book(pair_param: str) -> Optional[OrderBook]:
            if not pair_param:
                return None

            cached = _get_cached(pair_param)
            if cached is not None:
                return cached

            try:
                js = self._public_get("/v1/public/book", params={"pair": pair_param})
                ob = _parse_book(js)
                _put_cached(pair_param, ob)
                return ob
            except httpx.HTTPStatusError as e:
                sc = None
                try:
                    sc = int(getattr(e.response, "status_code", None) or 0)
                except Exception:
                    sc = None

                # 429: enter cooldown and DO NOT try more fallbacks right now
                if sc == 429:
                    self._set_public_cooldown()
                    if self._should_log(f"429:{pair_param}"):
                        logger.warning("DexTrade book rate-limited (429) pair=%s", pair_param)
                    return None

                # 400/404: negative cache to avoid repeated hammering of bad ids
                if sc in (400, 404):
                    _put_neg_cached(pair_param)

                if self._should_log(f"err:{pair_param}:{sc}"):
                    logger.warning("DexTrade book fetch failed pair=%s status=%s err=%s", pair_param, sc, e)
                return None
            except Exception as e:
                # cooldown RuntimeError from throttle is expected after 429 bursts; suppress spam
                if "cooldown" in str(e).lower():
                    if self._should_log("cooldown"):
                        logger.warning("DexTrade public API cooldown active; skipping book fetches briefly")
                    return None

                if self._should_log(f"ex:{pair_param}"):
                    logger.warning("DexTrade book fetch failed pair=%s err=%s", pair_param, e)
                return None

        pair = self._normalize_pair(symbol_venue)
        if not pair:
            self._ensure_symbols_cache(force=True)
            pair = self._normalize_pair(symbol_venue)

        if not pair:
            return {"bids": [], "asks": []}

        self._ensure_symbols_cache()
        with self._symbols_cache_lock:
            pair_id = self._pair_to_id.get(pair)

        # First attempt: real pair
        ob = _try_book(pair)
        if ob and (ob["bids"] or ob["asks"]):
            return ob

        # If we got rate-limited or in cooldown, do not keep trying fallbacks.
        with self._public_lock:
            if time.time() < self._public_cooldown_until:
                return {"bids": [], "asks": []}

        # Optional fallback: pair_id (only if not in cooldown)
        if pair_id:
            ob2 = _try_book(str(pair_id))
            if ob2 and (ob2["bids"] or ob2["asks"]):
                return ob2

        # Refresh symbols cache and try again once (still not in cooldown)
        self._ensure_symbols_cache(force=True)
        pair2 = self._normalize_pair(symbol_venue)

        if pair2 and pair2 != pair:
            with self._symbols_cache_lock:
                pair_id2 = self._pair_to_id.get(pair2)
        else:
            pair_id2 = pair_id

        ob3 = _try_book(pair2 or pair)
        if ob3 and (ob3["bids"] or ob3["asks"]):
            return ob3

        if pair_id2:
            ob4 = _try_book(str(pair_id2))
            if ob4 and (ob4["bids"] or ob4["asks"]):
                return ob4

        return {"bids": [], "asks": []}

    def fetch_orders(self, dry_run: bool) -> List[VenueOrder]:
        if dry_run:
            return []

        def _is_terminal(s: Optional[str]) -> bool:
            return (s or "").strip().lower() in self._TERMINAL

        hist_orders = self._fetch_history_orders(max_pages=5, page_limit=200)
        active_orders = self._fetch_active_orders()

        by_id: Dict[str, VenueOrder] = {}
        for o in hist_orders:
            oid = o.get("venue_order_id")
            if oid:
                by_id[str(oid)] = o

        for o in active_orders:
            oid = o.get("venue_order_id")
            if not oid:
                continue
            oid = str(oid)

            existing = by_id.get(oid)
            if existing is None:
                by_id[oid] = o
                continue

            if _is_terminal(existing.get("status")) and not _is_terminal(o.get("status")):
                continue

            if (existing.get("status") or "").strip().lower() == "filled":
                st2 = (o.get("status") or "").strip().lower()
                if st2 in self._TERMINAL and st2 != "filled":
                    continue

            by_id[oid] = o

        return list(by_id.values())

    # ─────────────────────────────────────────────────────────────
    # Optional: discovery + rules
    # ─────────────────────────────────────────────────────────────
    def list_symbols(self) -> List[str]:
        self._ensure_symbols_cache()
        with self._symbols_cache_lock:
            pairs = list(self._pair_to_base_quote.values())

        seen = set()
        out: List[str] = []
        for base, quote in pairs:
            canon = self._canon(base, quote)
            if canon not in seen:
                seen.add(canon)
                out.append(canon)
        return out

    def get_order_rules(self, symbol_venue: str) -> OrderRules:
        pair = self._normalize_pair(symbol_venue)
        if not pair:
            return {}

        self._ensure_symbols_cache()
        with self._symbols_cache_lock:
            decs = self._pair_to_decimals.get(pair)
            pair_id = self._pair_to_id.get(pair)

        declared_rate_dec = decs[0] if decs else None
        base_dec = decs[1] if decs else None
        quote_dec = decs[2] if decs else None

        # IMPORTANT: do not infer decimals from book here (would cause extra /book calls).
        # Use declared_rate_dec from /public/symbols which is stable and cacheable.
        effective_rate_dec = declared_rate_dec

        base_inc = self._pow10_inv(base_dec)
        price_inc = self._pow10_inv(effective_rate_dec)

        # Attempt to read min_trade from ticker (this is a lighter call than /book; still throttled)
        min_trade = None
        raw_ticker = None
        try:
            ticker_pair = pair_id or pair
            raw_ticker = self._public_get("/v1/public/ticker", params={"pair": ticker_pair})
            data = raw_ticker.get("data") if isinstance(raw_ticker, dict) else None

            candidates = []
            if isinstance(raw_ticker, dict):
                candidates.append(raw_ticker.get("min_trade"))
            if isinstance(data, dict):
                candidates.append(data.get("min_trade"))
                candidates.append(data.get("minTrade"))

            for c in candidates:
                if c is None:
                    continue
                try:
                    min_trade = float(c)
                    break
                except Exception:
                    continue
        except Exception:
            pass

        if min_trade is None:
            min_trade = float(base_inc) if base_inc is not None else 0.0

        return {
            "venue": self.venue,
            "symbol_venue": pair,  # normalized Dex pair
            "min_qty": min_trade,
            "min_notional": None,
            "qty_decimals": base_dec,
            "price_decimals": effective_rate_dec,
            "base_increment": base_inc,
            "price_increment": price_inc,
            "raw": {
                "symbols_decimals": {
                    "rate_decimal_declared": declared_rate_dec,
                    "rate_decimal_effective": effective_rate_dec,
                    "base_decimal": base_dec,
                    "quote_decimal": quote_dec,
                    "pair_id": pair_id,
                },
                "ticker": raw_ticker,
                "input_symbol_venue": symbol_venue,
                "normalized_pair": pair,
            },
        }
