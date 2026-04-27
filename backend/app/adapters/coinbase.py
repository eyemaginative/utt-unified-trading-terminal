# backend/app/adapters/coinbase.py

from __future__ import annotations

from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone, timedelta
import httpx
import os
import json
import time
import secrets
import threading
import logging
from decimal import Decimal, ROUND_FLOOR, InvalidOperation

from .base import ExchangeAdapter, PlacedOrder, BalanceItem, OrderBook, VenueOrder, OrderRules
from ..config import settings
import base64
import hashlib
import hmac
import urllib.parse

_LOG = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _is_http_5xx_error(msg: str) -> bool:
    """Heuristic match for transient Coinbase gateway/server failures.

    We only have the rendered exception message from _private_request(), so we
    key off common HTTP status strings.
    """
    m = (msg or "").upper()
    return ("HTTP 500" in m) or ("HTTP 502" in m) or ("HTTP 503" in m) or ("HTTP 504" in m)
class CoinbaseAdapter(ExchangeAdapter):
    venue = "coinbase"

    # Manual overrides still supported
    _map = {
        "USDT-USD": "USDT-USD",
        "BTC-USD": "BTC-USD",
        "ETH-USD": "ETH-USD",
    }

    _TERMINAL = {"filled", "canceled", "cancelled", "rejected", "done", "closed", "expired", "failed"}

    # User-facing hint to attach when Coinbase returns "Too many errors" / cooldown rejections.
    _TOO_MANY_ERRORS_HINT = " Hint: Try Using Qty to Auto Calc instead of Total."

    # ─────────────────────────────────────────────────────────────
    # Precision / increment overrides (per product_id)
    # ─────────────────────────────────────────────────────────────
    _PRECISION_OVERRIDES: Dict[str, Dict[str, Any]] = {
        # Force correct display/rounding for SUI-USD (adjust as needed)
        "SUI-USD": {
            "price_decimals": 4,
            # "price_increment": "0.0001",
            # "qty_decimals": 1,
        },
    }

    # ─────────────────────────────────────────────────────────────
    # Coinbase "Too many errors" cooldown (process-local)
    # ─────────────────────────────────────────────────────────────
    _cooldown_lock = threading.Lock()
    _cooldown_until_monotonic: float = 0.0
    _cooldown_until_utc_iso: Optional[str] = None
    _cooldown_reason: Optional[str] = None

    # ─────────────────────────────────────────────────────────────
    # Stabilization: serialize private Coinbase calls (single-flight)
    # ─────────────────────────────────────────────────────────────
    _private_call_lock = threading.RLock()

    # ─────────────────────────────────────────────────────────────
    # Stabilization: negative cache for product detail endpoint
    # Prevent repeated /products/{pid} calls for unsupported or failing products.
    # ─────────────────────────────────────────────────────────────
    _product_detail_negative_until: Dict[str, float] = {}
    _product_detail_negative_ttl_s: float = float(os.getenv("COINBASE_PRODUCT_DETAIL_NEG_TTL_S", "900"))

    def _env_bool(self, k: str, default: bool = False) -> bool:
        v = (os.getenv(k, "") or "").strip().lower()
        if v in ("1", "true", "yes", "y", "on"):
            return True
        if v in ("0", "false", "no", "n", "off"):
            return False
        return default

    def _cooldown_seconds(self) -> int:
        """
        Cooldown duration in seconds.

        Preferred configuration source (recommended):
          - settings.coinbase_too_many_errors_cooldown_s  (env alias: COINBASE_TOO_MANY_ERRORS_COOLDOWN_S)
          - settings.disable_venue_cooldown               (env alias: DISABLE_VENUE_COOLDOWN)

        Fallback OS env vars (still supported):
          - COINBASE_COOLDOWN_SECONDS
          - COINBASE_TOO_MANY_ERRORS_COOLDOWN_S
          - DISABLE_VENUE_COOLDOWN=1  (bypass cooldown checks entirely)

        Behavior:
          - unset/blank -> default 120
          - negative -> default 120
          - 0 -> disables cooldown
          - >0 -> normal cooldown seconds
        """
        # 1) Global dev bypass via Settings (so backend/.env works)
        try:
            if bool(getattr(settings, "disable_venue_cooldown", False)):
                return 0
        except Exception:
            pass

        # 2) Primary: Settings value (so backend/.env works)
        try:
            v = getattr(settings, "coinbase_too_many_errors_cooldown_s", None)
            if isinstance(v, int):
                if v < 0:
                    return 120
                return v  # allows 0
        except Exception:
            pass

        # 3) Backward/compat: OS env bypass
        if self._env_bool("DISABLE_VENUE_COOLDOWN", False):
            return 0

        # 4) Fallback: OS env vars
        raw = (os.getenv("COINBASE_COOLDOWN_SECONDS", "") or "").strip()
        if raw == "":
            raw = (os.getenv("COINBASE_TOO_MANY_ERRORS_COOLDOWN_S", "") or "").strip()

        if raw == "":
            return 120

        try:
            v2 = int(float(raw))
            if v2 < 0:
                return 120
            return v2  # allows 0
        except Exception:
            return 120

    def _cooldown_active(self) -> Tuple[bool, Optional[str], Optional[str]]:
        """
        Returns (is_active, until_iso, reason)
        """
        # If cooldown is disabled via config, it's never active.
        if self._cooldown_seconds() <= 0:
            return False, None, None

        with CoinbaseAdapter._cooldown_lock:
            nowm = time.monotonic()
            if nowm < CoinbaseAdapter._cooldown_until_monotonic:
                return True, CoinbaseAdapter._cooldown_until_utc_iso, CoinbaseAdapter._cooldown_reason
            return False, None, None

    def cooldown_status(self) -> Dict[str, Any]:
        """
        Public status used by refresh-all to skip Coinbase cleanly during cooldown.
        """
        active, until_iso, reason = self._cooldown_active()
        remaining_s = 0
        with CoinbaseAdapter._cooldown_lock:
            if active:
                remaining = CoinbaseAdapter._cooldown_until_monotonic - time.monotonic()
                if remaining > 0:
                    remaining_s = int(round(remaining))
        return {
            "active": bool(active),
            "until_iso": until_iso,
            "reason": reason,
            "remaining_s": remaining_s,
            "cooldown_seconds_configured": self._cooldown_seconds(),
        }

    def _set_cooldown(self, reason: str) -> None:
        secs = self._cooldown_seconds()
        # Respect disable / 0 seconds
        if secs <= 0:
            return

        until_m = time.monotonic() + float(secs)
        until_dt = datetime.now(timezone.utc) + timedelta(seconds=secs)
        until_iso = until_dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")

        with CoinbaseAdapter._cooldown_lock:
            CoinbaseAdapter._cooldown_until_monotonic = until_m
            CoinbaseAdapter._cooldown_until_utc_iso = until_iso
            CoinbaseAdapter._cooldown_reason = (reason or "").strip() or "Coinbase cooldown"

    def _is_too_many_errors_body(self, body_text: str) -> bool:
        """
        Coinbase sometimes returns 403 with a message like "Too many errors".
        We match case-insensitively and tolerate JSON or plaintext.
        """
        try:
            if not body_text:
                return False
            s = str(body_text)
            return "too many errors" in s.lower()
        except Exception:
            return False

    def _truncate(self, s: str, n: int = 2000) -> str:
        if s is None:
            return ""
        ss = str(s)
        if len(ss) <= n:
            return ss
        return ss[:n] + "…"

    def _append_qty_hint_if_too_many_errors(self, msg: str) -> str:
        """
        Ensures the user hint appears in reject_reason for Coinbase 'Too many errors' / cooldown paths.
        """
        try:
            m = str(msg or "")
            ml = m.lower()
            if ("too many errors" in ml) or ("403 too many errors" in ml):
                if CoinbaseAdapter._TOO_MANY_ERRORS_HINT.strip() not in m:
                    return m + CoinbaseAdapter._TOO_MANY_ERRORS_HINT
            return m
        except Exception:
            return msg

    # ─────────────────────────────────────────────────────────────
    # Instrumentation: single choke-point logging for Coinbase failures
    # ─────────────────────────────────────────────────────────────
    def _log_coinbase_http_error(self, status_code: int, method: str, url: str, body_text: str) -> None:
        """
        Log Coinbase HTTP failures without leaking secrets.

        IMPORTANT:
          - Do NOT log headers (may include auth).
          - Keep body truncated.
        """
        try:
            m = (method or "").upper().strip() or "GET"
            u = str(url or "")
            bt = self._truncate(body_text or "", 800)
            print(f"[coinbase][HTTP {int(status_code)}] {m} {u} :: {bt}")
        except Exception:
            # never allow logging itself to break trading flows
            pass

    def _log_coinbase_exc(self, method: str, url: str, exc: Exception) -> None:
        try:
            m = (method or "").upper().strip() or "GET"
            u = str(url or "")
            msg = self._truncate(str(exc or ""), 800)
            print(f"[coinbase][EXC] {m} {u} :: {msg}")
        except Exception:
            pass

    # ─────────────────────────────────────────────────────────────
    # Negative cache helpers
    # ─────────────────────────────────────────────────────────────
    def _neg_active(self, product_id: str) -> bool:
        try:
            pid = (product_id or "").strip().upper()
            if not pid:
                return False
            until = CoinbaseAdapter._product_detail_negative_until.get(pid)
            if until is None:
                return False
            return time.monotonic() < float(until)
        except Exception:
            return False

    def _neg_mark(self, product_id: str) -> None:
        try:
            pid = (product_id or "").strip().upper()
            if not pid:
                return
            ttl = float(CoinbaseAdapter._product_detail_negative_ttl_s or 0.0)
            if ttl <= 0:
                return
            CoinbaseAdapter._product_detail_negative_until[pid] = time.monotonic() + ttl
        except Exception:
            pass

    def _get_precision_override(self, product_id: str) -> Dict[str, Any]:
        pid = (product_id or "").strip().upper()
        if not pid:
            return {}
        return CoinbaseAdapter._PRECISION_OVERRIDES.get(pid, {})

    def _rules_get_with_override(self, product_id: str, rules: Dict[str, str], key: str) -> Optional[str]:
        """
        Fetch a rule value, preferring per-product overrides over cached Coinbase product rules.
        """
        ov = self._get_precision_override(product_id)
        if key in ov and ov.get(key) is not None:
            v = str(ov.get(key)).strip()
            return v if v else None
        v2 = rules.get(key)
        if v2 is None:
            return None
        s = str(v2).strip()
        return s if s else None

    # Product cache: maps "BASE-QUOTE" -> True and base -> set(quotes)
    _products_by_id: Dict[str, bool] = {}
    _quotes_by_base: Dict[str, set] = {}

    # Rules cache per product_id (increments/min/max)
    _product_rules: Dict[str, Dict[str, str]] = {}

    _products_cache_ts: float = 0.0
    _products_cache_ttl_s: float = float(os.getenv("COINBASE_PRODUCTS_CACHE_TTL_S", "300"))

    # ─────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────
    def _safe_float(self, x: Any) -> Optional[float]:
        try:
            if x is None:
                return None
            s = str(x).strip()
            if not s:
                return None
            return float(s)
        except Exception:
            return None

    def _dt_from_iso(self, s: Any) -> Optional[datetime]:
        try:
            if not s:
                return None
            st = str(s).strip()
            if not st:
                return None
            if st.endswith("Z"):
                st = st[:-1] + "+00:00"
            dt = datetime.fromisoformat(st)
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except Exception:
            return None

    def _cfg_get(self, o: Dict[str, Any], key: str) -> Any:
        """
        Coinbase order fields (base_size, limit_price, post_only, etc.) can live inside order_configuration.
        This helper searches the first level of that map for a dict containing `key`.
        """
        try:
            cfg = o.get("order_configuration")
            if not isinstance(cfg, dict):
                return None
            for v in cfg.values():
                if isinstance(v, dict) and key in v:
                    return v.get(key)
            return None
        except Exception:
            return None

    def _normalize_order_status(self, status_raw: Any) -> Optional[str]:
        """
        Normalize Coinbase order status strings to our app status vocabulary.
        Coinbase docs show status values like PENDING, OPEN, FILLED, CANCELLED, etc.
        """
        try:
            s = str(status_raw or "").strip().lower()
            if not s:
                return None
            if s in ("open", "pending", "new", "live", "active", "accepted", "partially_filled", "pending_cancel"):
                return "open"
            if s in ("filled", "done", "completed", "settled", "closed"):
                return "filled"
            if s in ("canceled", "cancelled", "canceling"):
                return "canceled"
            if s in ("rejected", "failed", "expired", "error"):
                return "rejected"
            return "acked"
        except Exception:
            return "acked"

    # ─────────────────────────────────────────────────────────────
    # Rules normalization (Phase 1)
    # ─────────────────────────────────────────────────────────────
    def _decimals_from_str(self, v: Any) -> Optional[int]:
        try:
            if v is None:
                return None
            s = str(v).strip()
            if not s:
                return None
            if "e" in s.lower():
                return None
            if "." not in s:
                return 0
            frac = s.split(".", 1)[1]
            frac = frac.rstrip("0")
            return len(frac)
        except Exception:
            return None

    # ─────────────────────────────────────────────────────────────
    # Decimal helpers (precision-safe flooring to exchange increments)
    # ─────────────────────────────────────────────────────────────
    def _dec(self, x: Any) -> Optional[Decimal]:
        if x is None:
            return None
        try:
            s = str(x).strip()
            if not s:
                return None
            return Decimal(s)
        except (InvalidOperation, ValueError):
            return None

    def _floor_to_step(self, value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        q = (value / step).to_integral_value(rounding=ROUND_FLOOR)
        return q * step

    def _fmt_decimal(self, d: Decimal) -> str:
        s = format(d, "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s if s else "0"

    def _get_rules(self, product_id: str) -> Dict[str, str]:
        return CoinbaseAdapter._product_rules.get(product_id, {}) if product_id else {}

    def _extract_rules_from_product(self, p: Dict[str, Any]) -> Dict[str, str]:
        rules: Dict[str, str] = {}
        for k in (
            "base_increment",
            "quote_increment",
            "price_increment",
            "base_min_size",
            "base_max_size",
            "quote_min_size",
            "quote_max_size",
        ):
            v = p.get(k)
            if v is not None:
                sv = str(v).strip()
                if sv:
                    rules[k] = sv
        return rules

    def _fetch_and_cache_product_rules(self, product_id: str) -> Dict[str, str]:
        pid = (product_id or "").strip().upper()
        if not pid:
            return {}

        cached = self._get_rules(pid) or {}

        # If this pid is in negative cache, never call product detail endpoint
        if self._neg_active(pid):
            return cached or {}

        # Ensure products list cache first; if pid is not in it, do NOT hit /products/{pid}
        # This is the core fix to stop repeated 404s from poisoning Coinbase error-rate gates.
        try:
            self._ensure_products_cache()
        except Exception:
            # If we cannot validate supported products, fall back to previous behavior (best effort).
            pass
        else:
            if CoinbaseAdapter._products_by_id and (pid not in CoinbaseAdapter._products_by_id):
                # Not supported according to bulk products list; mark negative and stop.
                self._neg_mark(pid)
                return cached or {}

        has_incs = bool(cached.get("base_increment")) and bool(
            cached.get("price_increment") or cached.get("quote_increment")
        )
        has_minmax = bool(cached.get("base_min_size"))

        # If we already have increments AND min size, no need for product detail call
        if has_incs and has_minmax:
            return cached

        # If we have increments but min is missing, we still prefer not to spam detail calls.
        # Only attempt detail if pid is supported (above), and not negatively cached.
        try:
            data = self._private_request("GET", f"/api/v3/brokerage/products/{pid}")
        except Exception:
            # If product detail fails (404/403/etc), mark negative to avoid repeated retries.
            self._neg_mark(pid)
            return cached or {}

        p = data.get("product") if isinstance(data, dict) else None
        if not isinstance(p, dict):
            return cached or {}

        rules = self._extract_rules_from_product(p)
        if rules:
            merged = dict(cached)
            merged.update(rules)
            CoinbaseAdapter._product_rules[pid] = merged

            CoinbaseAdapter._products_by_id[pid] = True
            if "-" in pid:
                base, quote = pid.split("-", 1)
                base = base.strip().upper()
                quote = quote.strip().upper()
                if base and quote:
                    CoinbaseAdapter._quotes_by_base.setdefault(base, set()).add(quote)

        return CoinbaseAdapter._product_rules.get(pid, cached or {})

    def get_order_rules(self, symbol_venue: str) -> OrderRules:
        pid = (symbol_venue or "").strip().upper()
        if not pid:
            return {"symbol_venue": symbol_venue}

        try:
            self._ensure_products_cache()
        except Exception:
            pass

        # If products cache is populated and pid not supported, return minimal rules without triggering detail lookup.
        if CoinbaseAdapter._products_by_id and (pid not in CoinbaseAdapter._products_by_id):
            return {
                "symbol_venue": pid,
                "base_increment": None,
                "price_increment": None,
                "qty_decimals": None,
                "price_decimals": None,
                "min_qty": None,
                "max_qty": None,
                "min_notional": None,
                "max_notional": None,
                "supports_post_only": True,
                "supported_tifs": ["gtc", "ioc", "fok"],
                "supported_order_types": ["limit"],
                "raw": {"_utt_not_supported": True, "reason": "Product not in Coinbase products cache"},
            }

        rules = self._fetch_and_cache_product_rules(pid) or (self._get_rules(pid) or {})

        base_inc_s = self._rules_get_with_override(pid, rules, "base_increment")
        price_inc_s = (
            self._rules_get_with_override(pid, rules, "price_increment")
            or self._rules_get_with_override(pid, rules, "quote_increment")
        )

        base_min_repr = self._rules_get_with_override(pid, rules, "base_min_size")
        base_max_repr = self._rules_get_with_override(pid, rules, "base_max_size")
        quote_min_repr = self._rules_get_with_override(pid, rules, "quote_min_size")
        quote_max_repr = self._rules_get_with_override(pid, rules, "quote_max_size")

        ov = self._get_precision_override(pid)
        ov_price_decimals = ov.get("price_decimals")
        ov_qty_decimals = ov.get("qty_decimals")

        def f(x: Any) -> Optional[float]:
            try:
                if x is None:
                    return None
                s = str(x).strip()
                if not s:
                    return None
                return float(s)
            except Exception:
                return None

        raw = dict(rules) if isinstance(rules, dict) else {}
        if ov:
            raw["_utt_precision_override"] = {k: ov.get(k) for k in sorted(list(ov.keys()))}

        if base_min_repr is not None:
            raw["base_min_size_repr"] = base_min_repr
        if base_max_repr is not None:
            raw["base_max_size_repr"] = base_max_repr
        if quote_min_repr is not None:
            raw["quote_min_size_repr"] = quote_min_repr
        if quote_max_repr is not None:
            raw["quote_max_size_repr"] = quote_max_repr

        qty_decimals = None
        price_decimals = None

        if isinstance(ov_qty_decimals, int) and ov_qty_decimals >= 0:
            qty_decimals = ov_qty_decimals
        else:
            qty_decimals = self._decimals_from_str(base_inc_s)

        if isinstance(ov_price_decimals, int) and ov_price_decimals >= 0:
            price_decimals = ov_price_decimals
        else:
            price_decimals = self._decimals_from_str(price_inc_s)

        return {
            "symbol_venue": pid,
            "base_increment": f(base_inc_s),
            "price_increment": f(price_inc_s),
            "qty_decimals": qty_decimals,
            "price_decimals": price_decimals,
            "min_qty": f(base_min_repr),
            "max_qty": f(base_max_repr),
            "min_notional": None,
            "max_notional": None,
            "supports_post_only": True,
            "supported_tifs": ["gtc", "ioc", "fok"],
            "supported_order_types": ["limit"],
            "raw": raw,
        }

    def _apply_size_price_rules(self, product_id: str, qty: float, limit_price: float) -> Tuple[str, str]:
        rules = self._get_rules(product_id) or {}
        rules = self._fetch_and_cache_product_rules(product_id) or rules or {}

        qd = self._dec(qty)
        pd = self._dec(limit_price)
        if qd is None or qd <= 0:
            raise Exception("CoinbaseAdapter.place_order requires qty > 0")
        if pd is None or pd <= 0:
            raise Exception("CoinbaseAdapter.place_order requires limit_price > 0")

        base_inc_s = self._rules_get_with_override(product_id, rules, "base_increment")
        price_inc_s = (
            self._rules_get_with_override(product_id, rules, "price_increment")
            or self._rules_get_with_override(product_id, rules, "quote_increment")
        )

        base_inc = self._dec(base_inc_s)
        price_inc = self._dec(price_inc_s)

        base_min = self._dec(self._rules_get_with_override(product_id, rules, "base_min_size"))
        base_max = self._dec(self._rules_get_with_override(product_id, rules, "base_max_size"))

        if base_inc is not None and base_inc > 0:
            qd2 = self._floor_to_step(qd, base_inc)
        else:
            qd2 = qd

        if qd2 <= 0:
            raise Exception(
                f"Coinbase order qty rounds to 0 under base_increment={base_inc_s or rules.get('base_increment') or 'unknown'}"
            )

        if base_min is not None and qd2 < base_min:
            raise Exception(
                f"Coinbase min size not met: qty={self._fmt_decimal(qd2)} < base_min_size={self._rules_get_with_override(product_id, rules, 'base_min_size')}"
            )
        if base_max is not None and qd2 > base_max:
            raise Exception(
                f"Coinbase max size exceeded: qty={self._fmt_decimal(qd2)} > base_max_size={self._rules_get_with_override(product_id, rules, 'base_max_size')}"
            )

        if price_inc is not None and price_inc > 0:
            pd2 = self._floor_to_step(pd, price_inc)
        else:
            pd2 = pd

        if pd2 <= 0:
            raise Exception(
                f"Coinbase limit_price rounds to 0 under price_increment={price_inc_s or rules.get('price_increment') or rules.get('quote_increment') or 'unknown'}"
            )

        return (self._fmt_decimal(qd2), self._fmt_decimal(pd2))

    # ─────────────────────────────────────────────────────────────
    # Symbol resolution
    # ─────────────────────────────────────────────────────────────
    def resolve_symbol(self, symbol_canon: str) -> str:
        sym = (symbol_canon or "").strip().upper()
        if not sym:
            return sym

        if sym in self._map:
            return self._map[sym]

        try:
            self._ensure_products_cache()
        except Exception:
            return sym

        if CoinbaseAdapter._products_by_id.get(sym):
            return sym

        if "-" in sym:
            base, quote = sym.split("-", 1)
            base = base.strip().upper()
            quote = quote.strip().upper()

            if quote == "USD":
                for q in ("USDC", "USDT"):
                    cand = f"{base}-{q}"
                    if CoinbaseAdapter._products_by_id.get(cand):
                        return cand

        return sym

    # ─────────────────────────────────────────────────────────────
    # Discovery: list symbols (canonical BASE-QUOTE)
    # ─────────────────────────────────────────────────────────────
    def list_symbols(self) -> List[str]:
        try:
            from ..services.symbol_policy import ALLOWED_QUOTES  # type: ignore

            allowed_quotes = {str(x).upper().strip() for x in (ALLOWED_QUOTES or set()) if str(x).strip()}
        except Exception:
            allowed_quotes = {"USD", "USDT", "USDC", "BTC", "ETH"}

        try:
            self._ensure_products_cache()
        except Exception:
            return sorted(set(self._map.keys()))

        out: List[str] = []
        for pid in (CoinbaseAdapter._products_by_id or {}).keys():
            try:
                if not pid or "-" not in pid:
                    continue
                base, quote = pid.split("-", 1)
                base = base.strip().upper()
                quote = quote.strip().upper()
                if not base or not quote:
                    continue
                if quote not in allowed_quotes:
                    continue
                out.append(f"{base}-{quote}")
            except Exception:
                continue

        return sorted(set(out))

    # ─────────────────────────────────────────────────────────────
    # Product cache – PRIVATE (uses _private_request)
    # ─────────────────────────────────────────────────────────────
    def _ensure_products_cache(self) -> None:
        now = time.time()
        if (
            CoinbaseAdapter._products_by_id
            and (now - CoinbaseAdapter._products_cache_ts) < CoinbaseAdapter._products_cache_ttl_s
        ):
            return
        self._refresh_products_cache()

    def _refresh_products_cache(self) -> None:
        debug = (os.getenv("COINBASE_PRODUCTS_DEBUG", "") or "").strip() == "1"

        path = "/api/v3/brokerage/products"
        cursor: Optional[str] = None

        products_by_id: Dict[str, bool] = {}
        quotes_by_base: Dict[str, set] = {}
        product_rules: Dict[str, Dict[str, str]] = {}

        for _ in range(120):
            params: Dict[str, Any] = {"limit": "250"}
            if cursor:
                params["cursor"] = cursor

            data = self._private_request("GET", path, params=params)

            prods = data.get("products") or data.get("data") or []
            if not isinstance(prods, list):
                raise Exception(f"Unexpected Coinbase 'products' type: {type(prods)}")

            for p in prods:
                if not isinstance(p, dict):
                    continue

                pid = str(p.get("product_id") or p.get("id") or "").strip().upper()
                if not pid or "-" not in pid:
                    continue

                trading_disabled = p.get("trading_disabled")
                status = str(p.get("status") or "").lower().strip()

                if trading_disabled is True:
                    continue
                if status and status not in ("online", "active", "available"):
                    continue

                products_by_id[pid] = True
                base, quote = pid.split("-", 1)
                base = base.strip().upper()
                quote = quote.strip().upper()
                if base and quote:
                    quotes_by_base.setdefault(base, set()).add(quote)

                rules = self._extract_rules_from_product(p)
                if rules:
                    product_rules[pid] = rules

            pag = data.get("pagination") or {}
            next_cursor = None
            has_next = False
            if isinstance(pag, dict):
                has_next = bool(pag.get("has_next", False))
                next_cursor = pag.get("next_cursor") or pag.get("cursor")
            else:
                has_next = bool(data.get("has_next", False))
                next_cursor = data.get("cursor")

            cursor = str(next_cursor).strip() if next_cursor else None
            if not has_next or not cursor:
                break

        CoinbaseAdapter._products_by_id = products_by_id
        CoinbaseAdapter._quotes_by_base = quotes_by_base
        CoinbaseAdapter._product_rules = product_rules
        CoinbaseAdapter._products_cache_ts = time.time()

        if debug:
            sample = sorted(list(products_by_id.keys()))[:30]
            print(
                "COINBASE PRODUCTS DEBUG "
                f"cached_products={len(products_by_id)} rules_cached={len(product_rules)} sample={sample}"
            )

    # ─────────────────────────────────────────────────────────────
    # Auth (JWT) – Advanced Trade private endpoints
    # ─────────────────────────────────────────────────────────────
    def _read_pem_from_path(self, p: str) -> Optional[str]:
        try:
            pp = (p or "").strip()
            if not pp:
                return None
            with open(pp, "r", encoding="utf-8") as f:
                s = f.read()
            s = (s or "").strip()
            return s if s else None
        except Exception:
            return None

    # ✅ CHANGE 1: scoped credentials selection (trade vs transfers)
    def _require_creds(self, scope: str = "trade") -> Tuple[str, str]:
        scope_s = (scope or "trade").strip().lower()

        if scope_s == "transfers":
            # Dedicated transfers key (read-only usage enforced elsewhere)
            key_name = (
                getattr(settings, "coinbase_transfers_api_key", None)
                or os.getenv("COINBASE_TRANSFERS_API_KEY")
            )

            pem = (
                getattr(settings, "coinbase_transfers_api_secret", None)
                or os.getenv("COINBASE_TRANSFERS_API_SECRET")
                or os.getenv("COINBASE_TRANSFERS_API_PRIVATE_KEY")
            )

            pem_path = (
                getattr(settings, "coinbase_transfers_api_secret_path", None)
                or os.getenv("COINBASE_TRANSFERS_API_SECRET_PATH")
                or os.getenv("COINBASE_TRANSFERS_API_PRIVATE_KEY_PATH")
            )
        else:
            # Default trading key (existing behavior)
            key_name = (
                getattr(settings, "coinbase_api_key", None)
                or os.getenv("COINBASE_API_KEY")
                or os.getenv("COINBASE_CDP_API_KEY")
            )

            pem = (
                getattr(settings, "coinbase_api_secret", None)
                or os.getenv("COINBASE_API_SECRET")
                or os.getenv("COINBASE_CDP_API_SECRET")
                or os.getenv("COINBASE_API_PRIVATE_KEY")
                or os.getenv("COINBASE_CDP_API_PRIVATE_KEY")
            )

            pem_path = (
                getattr(settings, "coinbase_api_secret_path", None)
                or os.getenv("COINBASE_API_SECRET_PATH")
                or os.getenv("COINBASE_CDP_API_SECRET_PATH")
                or os.getenv("COINBASE_API_PRIVATE_KEY_PATH")
                or os.getenv("COINBASE_CDP_API_PRIVATE_KEY_PATH")
            )



        # DB-vault fallback (Profile → API Keys)
        if (not key_name) or (not pem):
            try:
                if scope_s == "transfers":
                    vc = getattr(settings, "coinbase_transfers_private_creds", None)
                else:
                    vc = getattr(settings, "coinbase_trade_private_creds", None)

                if callable(vc):
                    v = vc()
                    vk = vs = None
                    if isinstance(v, (list, tuple)) and len(v) >= 2:
                        vk, vs = v[0], v[1]
                    elif isinstance(v, dict):
                        vk = v.get("api_key") or v.get("key_name") or v.get("key")
                        vs = v.get("api_secret") or v.get("secret") or v.get("pem")
                    if (not key_name) and vk is not None:
                        key_name = str(vk).strip()
                    if (not pem) and vs is not None:
                        pem = str(vs).strip()
            except Exception:
                pass


        key_name = (key_name or "").strip()
        pem = (pem or "").strip()

        if (not pem) and pem_path:
            pem_from_file = self._read_pem_from_path(pem_path)
            if pem_from_file:
                pem = pem_from_file

        if not key_name or not pem:
            raise Exception(
                "Missing Coinbase credentials. Provide an API key name + EC private key PEM.\n"
                "Trade key:\n"
                "  Key: COINBASE_API_KEY or COINBASE_CDP_API_KEY\n"
                "  PEM: COINBASE_API_SECRET / COINBASE_CDP_API_SECRET / COINBASE_API_PRIVATE_KEY / COINBASE_CDP_API_PRIVATE_KEY\n"
                "  PEM Path: COINBASE_API_SECRET_PATH / COINBASE_API_PRIVATE_KEY_PATH (or CDP variants)\n"
                "Transfers key:\n"
                "  Key: COINBASE_TRANSFERS_API_KEY\n"
                "  PEM: COINBASE_TRANSFERS_API_SECRET / COINBASE_TRANSFERS_API_PRIVATE_KEY\n"
                "  PEM Path: COINBASE_TRANSFERS_API_SECRET_PATH / COINBASE_TRANSFERS_API_PRIVATE_KEY_PATH\n"
                "Note: Coinbase expects the API key value to be the 'key name' (e.g., organizations/{org_id}/apiKeys/{key_id})."
            )

        return key_name, pem

    # ✅ CHANGE 2: scope= passed through JWT creation
    def _jwt_for(self, method: str, path: str, *, scope: str = "trade") -> str:
        key_name, api_secret_pem = self._require_creds(scope=scope)

        try:
            import jwt  # PyJWT
        except Exception as e:
            raise Exception(f"PyJWT is required for Coinbase ES256 auth but is not installed: {e}")

        now = int(time.time())
        m = (method or "GET").upper().strip()
        p = (path or "").strip()
        if not p.startswith("/"):
            p = "/" + p

        request_host = "api.coinbase.com"
        p_no_q = p.split("?", 1)[0]
        uri = f"{m} {request_host}{p_no_q}"

        payload = {
            "sub": key_name,
            "iss": "cdp",
            "nbf": now,
            "exp": now + 120,
            "uri": uri,
        }
        headers = {
            "kid": key_name,
            "nonce": secrets.token_hex(16),
        }

        token = jwt.encode(
            payload,
            api_secret_pem,
            algorithm="ES256",
            headers=headers,
        )

        if isinstance(token, bytes):
            token = token.decode("utf-8")
        return token

    # ✅ CHANGE 3: scope= on private request + transfer mutation safety
    def _private_request(
        self,
        method: str,
        path: str,
        json_body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        *,
        scope: str = "trade",
    ) -> Dict[str, Any]:
        # Serialize all private Coinbase calls to avoid overlapping bursts
        with CoinbaseAdapter._private_call_lock:
            # Cooldown short-circuit (avoid hammering + avoid generating JWT)
            active, until_iso, reason = self._cooldown_active()
            if active:
                msg = f"Coinbase cooldown active until {until_iso or 'unknown'}"
                if reason:
                    msg += f" ({reason})"
                # If this cooldown is due to "Too many errors", attach the UI hint.
                msg = self._append_qty_hint_if_too_many_errors(msg)
                raise Exception(msg)

            scope_s = (scope or "trade").strip().lower()

            # Safety: if we ever accidentally wire a transfer-enabled key into a mutating call, block it.
            # NOTE: we still allow POST to the historical batch endpoint because Coinbase may require it
            # in some environments and it is still a read surface.
            if scope_s == "transfers":
                allow_mut = self._env_bool("COINBASE_ALLOW_TRANSFER_MUTATIONS", False)
                m_up = (method or "GET").upper().strip()
                if m_up != "GET":
                    safe_post_paths = {"/api/v3/brokerage/transactions/historical/batch"}
                    if (not allow_mut) and (path not in safe_post_paths):
                        raise Exception(
                            f"Coinbase transfer mutations disabled (COINBASE_ALLOW_TRANSFER_MUTATIONS=0). Blocked {m_up} {path}."
                        )

            url = "https://api.coinbase.com" + path
            token = self._jwt_for(method, path, scope=scope_s)
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }

            try:
                with httpx.Client(timeout=25.0) as client:
                    if method.upper() == "GET":
                        r = client.get(url, headers=headers, params=params or None)
                    else:
                        r = client.request(
                            method.upper(), url, headers=headers, params=params or None, json=json_body or {}
                        )
            except Exception as e:
                self._log_coinbase_exc(method, url, e)
                raise

            body_text = ""
            try:
                body_text = r.text if r is not None else ""
            except Exception:
                body_text = ""

            if not (200 <= r.status_code < 300):
                self._log_coinbase_http_error(r.status_code, method, url, body_text)

                # Trigger cooldown on specific Coinbase block response
                if r.status_code == 403 and self._is_too_many_errors_body(body_text):
                    self._set_cooldown("403 Too many errors")
                    active2, until_iso2, _ = self._cooldown_active()
                    if active2:
                        msg = (
                            f"Coinbase private {method} {path} HTTP 403: Too many errors. "
                            f"Cooldown active until {until_iso2 or 'unknown'}."
                        )
                        msg = self._append_qty_hint_if_too_many_errors(msg)
                        raise Exception(msg)

                raise Exception(f"Coinbase private {method} {path} HTTP {r.status_code}: {self._truncate(body_text)}")

            try:
                data = r.json() if r.content else {}
            except Exception as e:
                raise Exception(f"Coinbase private {method} {path} invalid JSON: {e}; body={self._truncate(body_text)}")

            if not isinstance(data, dict):
                raise Exception(f"Unexpected Coinbase private response type: {type(data)}")
            return data

    def _env_str(self, k: str, default: str = "") -> str:
        v = (os.getenv(k, "") or "").strip()
        return v if v else default

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
        post_only: Optional[bool] = None,
    ) -> PlacedOrder:
        if dry_run:
            return {"venue_order_id": f"dry-{client_order_id}", "status": "acked"}

        if (type_ or "").lower().strip() != "limit":
            raise Exception(f"CoinbaseAdapter.place_order supports only type=limit for now (got {type_})")

        q = float(qty)
        if not (q > 0.0):
            raise Exception("CoinbaseAdapter.place_order requires qty > 0")

        if limit_price is None:
            raise Exception("CoinbaseAdapter.place_order requires limit_price for limit orders")
        px = float(limit_price)
        if not (px > 0.0):
            raise Exception("CoinbaseAdapter.place_order requires limit_price > 0")

        coid = (client_order_id or "").strip() or f"utt-{int(time.time() * 1000)}"
        product_id = self.resolve_symbol(symbol_venue)

        side_up = (side or "").strip().upper()
        if side_up not in ("BUY", "SELL"):
            raise Exception(f"Invalid side: {side}")

        tif_eff = (tif or self._env_str("COINBASE_DEFAULT_TIF", "gtc") or "gtc").lower().strip()
        post_only_eff = bool(post_only) if post_only is not None else self._env_bool("COINBASE_DEFAULT_POST_ONLY", False)

        cfg_key = "limit_limit_gtc"
        if tif_eff == "ioc":
            cfg_key = "limit_limit_ioc"
        elif tif_eff == "fok":
            cfg_key = "limit_limit_fok"

        try:
            self._ensure_products_cache()
        except Exception:
            pass

        self._fetch_and_cache_product_rules(product_id)

        base_size_str, limit_price_str = self._apply_size_price_rules(product_id, q, px)

        cfg_obj: Dict[str, Any] = {"base_size": base_size_str, "limit_price": limit_price_str}
        if post_only_eff:
            cfg_obj["post_only"] = True

        body: Dict[str, Any] = {
            "client_order_id": coid,
            "product_id": product_id,
            "side": side_up,
            "order_configuration": {cfg_key: cfg_obj},
        }

        path = "/api/v3/brokerage/orders"
        data = self._private_request("POST", path, json_body=body)

        if data.get("success") is False:
            raise Exception(f"Coinbase order rejected: {json.dumps(data, indent=2)}")

        sr = data.get("success_response") or {}
        order_id = None
        if isinstance(sr, dict):
            order_id = sr.get("order_id") or sr.get("id")

        if not order_id:
            raise Exception(f"Coinbase order placed but missing order_id: {json.dumps(data, indent=2)}")

        status_norm = "open"
        try:
            od = self._private_request("GET", f"/api/v3/brokerage/orders/historical/{order_id}")
            o = od.get("order") if isinstance(od, dict) else None
            if isinstance(o, dict):
                status_norm = self._normalize_order_status(o.get("status")) or "open"
        except Exception:
            status_norm = "open"

        return {"venue_order_id": str(order_id), "status": status_norm}

    def cancel_order(self, venue_order_id: str, dry_run: bool) -> bool:
        if dry_run:
            return True

        oid = (venue_order_id or "").strip()
        if not oid:
            return False

        path = "/api/v3/brokerage/orders/batch_cancel"
        body = {"order_ids": [oid]}
        data = self._private_request("POST", path, json_body=body)

        if data.get("success") is True:
            return True

        canceled = data.get("canceled_order_ids") or data.get("cancelled_order_ids")
        if isinstance(canceled, list) and oid in [str(x) for x in canceled]:
            return True

        results = data.get("results")
        if isinstance(results, list):
            for r in results:
                if not isinstance(r, dict):
                    continue
                roid = str(r.get("order_id") or r.get("id") or "").strip()
                if roid and roid == oid:
                    return bool(r.get("success", False))
            return False

        return False

    # ─────────────────────────────────────────────────────────────
    # Balances (private) – paginated + aggregated
    # ─────────────────────────────────────────────────────────────
    def fetch_balances(self, dry_run: bool) -> List[BalanceItem]:
        path = "/api/v3/brokerage/accounts"

        cursor: Optional[str] = None
        accounts_all: List[Dict[str, Any]] = []

        for _ in range(80):
            params: Dict[str, Any] = {"limit": "250"}
            if cursor:
                params["cursor"] = cursor

            data = self._private_request("GET", path, params=params)

            accounts = data.get("accounts") or data.get("data") or []
            if isinstance(accounts, list):
                for a in accounts:
                    if isinstance(a, dict):
                        accounts_all.append(a)

            pag = data.get("pagination") or {}
            next_cursor = None
            has_next = False
            if isinstance(pag, dict):
                has_next = bool(pag.get("has_next", False))
                next_cursor = pag.get("next_cursor") or pag.get("cursor")
            else:
                has_next = bool(data.get("has_next", False))
                next_cursor = data.get("cursor")

            cursor = str(next_cursor).strip() if next_cursor else None
            if not has_next or not cursor:
                break

        if not accounts_all:
            return []

        agg: Dict[str, Dict[str, Decimal]] = {}

        def dec0(x: Any) -> Decimal:
            d = self._dec(x)
            return d if d is not None else Decimal("0")

        eps = Decimal("0.000000000001")  # 1e-12

        def parse_money_field(v: Any) -> Optional[Decimal]:
            if v is None:
                return None
            if isinstance(v, dict):
                return self._dec(v.get("value"))
            if isinstance(v, (str, int, float, Decimal)):
                return self._dec(v)
            return None

        def sum_holds_list(lst: List[Any]) -> Optional[Decimal]:
            total = Decimal("0")
            saw_any = False
            for it in lst:
                if not isinstance(it, dict):
                    continue
                cand = (
                    it.get("amount")
                    or it.get("value")
                    or it.get("hold_amount")
                    or it.get("hold_value")
                    or it.get("quantity")
                )
                d = self._dec(cand)
                if d is None:
                    continue
                total += d
                saw_any = True
            return total if saw_any else None

        for a in accounts_all:
            cur = str(a.get("currency") or a.get("asset") or "").strip().upper()
            if not cur:
                continue

            avail_d = parse_money_field(a.get("available_balance"))

            total_d = parse_money_field(a.get("balance"))
            if total_d is None:
                total_d = parse_money_field(a.get("total_balance"))

            hold_raw = a.get("hold")
            if hold_raw is None:
                hold_raw = a.get("holds")

            hold_d: Optional[Decimal] = None
            if isinstance(hold_raw, list):
                hold_d = sum_holds_list(hold_raw)
            else:
                hold_d = parse_money_field(hold_raw)

            if total_d is None:
                if avail_d is not None and hold_d is not None:
                    total_d = avail_d + hold_d
                elif hold_d is not None and avail_d is None:
                    total_d = hold_d
                    avail_d = Decimal("0")
                elif avail_d is not None:
                    total_d = avail_d

            if avail_d is None and total_d is not None:
                h0 = hold_d if hold_d is not None else Decimal("0")
                implied = total_d - h0
                avail_d = implied if implied > 0 else Decimal("0")

            if hold_d is None and total_d is not None and avail_d is not None:
                diff = total_d - avail_d
                hold_d = diff if diff > 0 else Decimal("0")

            if total_d is None:
                total_d = Decimal("0")
            if avail_d is None:
                avail_d = Decimal("0")
            if hold_d is None:
                hold_d = Decimal("0")

            if total_d < 0:
                total_d = Decimal("0")
            if avail_d < 0:
                avail_d = Decimal("0")
            if hold_d < 0:
                hold_d = Decimal("0")

            if total_d + eps < hold_d:
                total_d = avail_d + hold_d

            if avail_d > total_d + eps:
                implied_av = total_d - hold_d
                avail_d = implied_av if implied_av > 0 else total_d

            if (avail_d + hold_d) > (total_d + eps):
                total_d = avail_d + hold_d

            slot = agg.setdefault(cur, {"total": Decimal("0"), "available": Decimal("0"), "hold": Decimal("0")})
            slot["total"] += dec0(total_d)
            slot["available"] += dec0(avail_d)
            slot["hold"] += dec0(hold_d)

        out: List[BalanceItem] = []
        for cur, v in agg.items():
            out.append(
                {
                    "asset": cur,
                    "total": float(v.get("total") or 0),
                    "available": float(v.get("available") or 0),
                    "hold": float(v.get("hold") or 0),
                }
            )

        out.sort(key=lambda x: str(x.get("asset") or ""))
        return out

    # ─────────────────────────────────────────────────────────────
    # Orders (private) – best-effort (GET + pagination)
    # ─────────────────────────────────────────────────────────────

    def _parse_order(self, o: Dict[str, Any]) -> Optional[VenueOrder]:
        """Parse a Coinbase Advanced Trade order payload into our VenueOrder dict.

        This is intentionally tolerant to field-shape variations.
        """
        try:
            if not isinstance(o, dict):
                return None

            venue_order_id = o.get("order_id") or o.get("id")
            if not venue_order_id:
                return None

            product_id = (o.get("product_id") or o.get("productId") or o.get("symbol") or "").strip().upper()
            if not product_id:
                return None

            # Canonicalize: keep Coinbase's PRODUCT_ID (BASE-QUOTE)
            symbol_venue = product_id
            symbol_canon = product_id

            side = (o.get("side") or "").strip().lower() or None
            order_type = (o.get("order_type") or o.get("type") or "").strip().lower() or None

            status_norm = self._normalize_order_status(o.get("status") or o.get("order_status") or o.get("state"))
            if not status_norm:
                status_norm = "acked"

            # Sizes / prices may be at top-level or inside order_configuration
            qty = self._safe_float(
                o.get("size")
                or o.get("base_size")
                or self._cfg_get(o, "base_size")
                or self._cfg_get(o, "size")
            )

            filled_qty = self._safe_float(
                o.get("filled_size")
                or o.get("filled_quantity")
                or o.get("filled_qty")
                or self._cfg_get(o, "filled_size")
            )

            limit_price = self._safe_float(o.get("limit_price") or self._cfg_get(o, "limit_price"))

            avg_fill_price = self._safe_float(
                o.get("average_filled_price") or o.get("avg_filled_price") or o.get("avg_fill_price")
            )

            fee_paid = self._safe_float(o.get("total_fees") or o.get("fees") or o.get("fee"))

            # best-effort timestamps
            created_at = self._dt_from_iso(o.get("created_time") or o.get("created_at"))
            updated_at = self._dt_from_iso(
                o.get("last_update_time")
                or o.get("last_fill_time")
                or o.get("updated_time")
                or o.get("updated_at")
            )

            # fee asset: default to quote if product_id is BASE-QUOTE
            fee_asset = None
            try:
                if "-" in product_id:
                    fee_asset = product_id.split("-", 1)[1].strip().upper() or None
            except Exception:
                fee_asset = None

            out: VenueOrder = {
                "venue": "coinbase",
                "venue_order_id": str(venue_order_id),
                "cancel_ref": f"coinbase:{venue_order_id}",
                "symbol_canon": symbol_canon,
                "symbol_venue": symbol_venue,
                "side": side,
                "type": order_type,
                "status": status_norm,
                "qty": qty,
                "filled_qty": filled_qty,
                "limit_price": limit_price,
                "avg_fill_price": avg_fill_price,
                "fee": fee_paid,
                "fee_asset": fee_asset,
                "total_after_fee": None,
                "created_at": created_at,
                "updated_at": updated_at,
            }

            # keep raw payload for debugging
            out["raw"] = o

            return out
        except Exception:
            return None

    def fetch_orders(
        self,
        dry_run: bool,
        *,
        limit: Optional[int] = None,
        max_pages: Optional[int] = None,
        order_status: Optional[str] = None,
    ) -> List[VenueOrder]:
        """Fetch orders from Coinbase Advanced Trade.

        Uses the historical batch endpoint (cursor paginated, newest-first).

        Env knobs:
          - COINBASE_ORDERS_PAGE_LIMIT (default 100)
          - COINBASE_ORDERS_MAX_PAGES  (default 80)

        Note:
          If you have extremely large order history, older-but-still-open orders can
          fall beyond a shallow history scan. Prefer `fetch_open_orders()` for the
          venue_orders snapshot refresh path.
        """

        limit_i = int(limit) if limit is not None else _env_int("COINBASE_ORDERS_PAGE_LIMIT", 100)
        max_pages_i = int(max_pages) if max_pages is not None else _env_int("COINBASE_ORDERS_MAX_PAGES", 80)
        limit_i = max(1, int(limit_i))
        max_pages_i = max(1, int(max_pages_i))

        path = "/api/v3/brokerage/orders/historical/batch"
        out: List[VenueOrder] = []
        cursor: Optional[str] = None
        last_has_next = False

        cooldown_s = 0.1

        for _ in range(max_pages_i):
            params: Dict[str, str] = {"limit": str(limit_i)}
            if cursor:
                params["cursor"] = cursor
            if order_status:
                params["order_status"] = order_status

            try:
                j = self._private_request("GET", path, params=params)
            except Exception as e:
                # Be gentle on transient errors / rate limiting.
                if "429" in str(e) or "rate" in str(e).lower():
                    time.sleep(min(2.0, cooldown_s))
                    cooldown_s = min(2.0, cooldown_s * 1.5)
                    continue
                raise

            for o in (j.get("orders") or []):
                parsed = self._parse_order(o)
                if parsed:
                    out.append(parsed)

            pag = j.get("pagination") or {}
            last_has_next = bool(pag.get("has_next"))
            cursor = pag.get("next_cursor")
            if not last_has_next or not cursor:
                break

        if last_has_next and cursor:
            _LOG.warning(
                "coinbase.fetch_orders hit max_pages cap: max_pages=%s limit=%s returned=%s",
                max_pages_i,
                limit_i,
                len(out),
            )

        out = self._augment_orders_with_fills(out)
        return out

    def fetch_open_orders(
        self,
        dry_run: bool,
        *,
        limit: Optional[int] = None,
        max_pages: Optional[int] = None,
    ) -> List[VenueOrder]:
        """Fetch OPEN orders for snapshot refreshes (server-side filtered, if supported).

        Env knobs:
          - COINBASE_OPEN_ORDERS_PAGE_LIMIT (fallback COINBASE_ORDERS_PAGE_LIMIT)
          - COINBASE_OPEN_ORDERS_MAX_PAGES  (fallback COINBASE_ORDERS_MAX_PAGES)
        """

        limit_i = int(limit) if limit is not None else _env_int(
            "COINBASE_OPEN_ORDERS_PAGE_LIMIT",
            _env_int("COINBASE_ORDERS_PAGE_LIMIT", 100),
        )
        max_pages_i = int(max_pages) if max_pages is not None else _env_int(
            "COINBASE_OPEN_ORDERS_MAX_PAGES",
            _env_int("COINBASE_ORDERS_MAX_PAGES", 80),
        )

        return self.fetch_orders(
            dry_run,
            limit=limit_i,
            max_pages=max_pages_i,
            order_status="OPEN",
        )

    def _fetch_recent_fills(self) -> List[Dict[str, Any]]:
        """Best-effort fetch of recent fills.

        Coinbase sometimes omits non-API-origin orders from the historical orders endpoint. The fills
        endpoint is typically a more complete source of executed trades. We use it to:
          1) synthesize filled orders that are missing from order history, and
          2) enrich existing orders with fill qty/avg price/fee when those fields are absent.

        This method is defensive: if the endpoint is unavailable or the key lacks scope, it returns [].
        """
        try:
            lookback_days = int(os.getenv("COINBASE_FILL_LOOKBACK_DAYS", "14") or "14")
        except Exception:
            lookback_days = 14
        lookback_days = max(1, min(90, lookback_days))
        since_dt = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=lookback_days)

        path = "/api/v3/brokerage/orders/historical/fills"
        cursor: Optional[str] = None
        out: List[Dict[str, Any]] = []

        max_pages = 80  # hard cap
        page_limit = str(int(os.getenv("COINBASE_FILLS_PAGE_LIMIT", "250") or "250"))
        for _ in range(max_pages):
            params: Dict[str, Any] = {"limit": page_limit}
            if cursor:
                params["cursor"] = cursor

            data = self._private_request("GET", path, params=params)

            items = (
                data.get("fills")
                or data.get("items")
                or data.get("results")
                or (data.get("data", {}) if isinstance(data.get("data"), dict) else data.get("data"))
                or []
            )
            if isinstance(items, dict):
                # sometimes nested under { fills: [...] }
                items = items.get("fills") or items.get("items") or []

            if not isinstance(items, list) or len(items) == 0:
                break

            out.extend([x for x in items if isinstance(x, dict)])

            # pagination
            pag = data.get("pagination") if isinstance(data, dict) else None
            if isinstance(pag, dict):
                has_next = bool(pag.get("has_next") or pag.get("hasNext") or pag.get("has_more") or pag.get("hasMore"))
                next_cursor = pag.get("next_cursor") or pag.get("nextCursor") or pag.get("cursor")
            else:
                has_next = bool(data.get("has_next") or data.get("hasNext"))
                next_cursor = data.get("cursor") or data.get("next_cursor") or data.get("nextCursor")

            if not has_next or not next_cursor:
                break
            cursor = str(next_cursor)

        # filter to lookback window (defensive)
        filtered: List[Dict[str, Any]] = []
        for f in out:
            ts = f.get("trade_time") or f.get("created_time") or f.get("time") or f.get("created_at") or f.get(
                "executed_at"
            )
            dt = self._dt_from_iso(ts) if ts else None
            if dt is None or dt >= since_dt:
                filtered.append(f)

        # Make downstream logic deterministic (newest first), regardless of API ordering.
        def _rank(x: Dict[str, Any]) -> float:
            ts2 = x.get("trade_time") or x.get("created_time") or x.get("time") or x.get("created_at") or x.get(
                "executed_at"
            )
            dt2 = self._dt_from_iso(ts2) if ts2 else None
            try:
                return dt2.timestamp() if dt2 else 0.0
            except Exception:
                return 0.0

        filtered.sort(key=_rank, reverse=True)
        return filtered

    def _augment_orders_with_fills(self, orders: List[VenueOrder]) -> List[VenueOrder]:
        """Augment order history with fills to avoid missing executed trades."""
        try:
            fills = self._fetch_recent_fills()
        except Exception:
            return orders

        if not fills:
            return orders

        by_id: Dict[str, VenueOrder] = {
            str(o.get("venue_order_id")): o for o in orders if isinstance(o, dict) and o.get("venue_order_id")
        }

        def _to_float(x: Any) -> Optional[float]:
            try:
                if x is None or x == "":
                    return None
                return float(x)
            except Exception:
                return None

        agg: Dict[str, Dict[str, Any]] = {}

        for f in fills:
            if not isinstance(f, dict):
                continue
            oid = f.get("order_id") or f.get("orderId") or f.get("orderID") or f.get("order")
            if not oid:
                continue
            oid = str(oid)

            product_id = f.get("product_id") or f.get("productId") or f.get("product") or f.get("symbol")
            side = (f.get("side") or f.get("order_side") or f.get("orderSide") or "").lower()

            size = _to_float(f.get("size") or f.get("filled_size") or f.get("filled_qty") or f.get("qty"))
            price = _to_float(f.get("price") or f.get("fill_price") or f.get("executed_price") or f.get("avg_price"))
            fee = _to_float(f.get("commission") or f.get("fee") or f.get("fees") or f.get("commission_amount"))

            ts = f.get("trade_time") or f.get("created_time") or f.get("time") or f.get("created_at") or f.get(
                "executed_at"
            )
            dt = self._dt_from_iso(ts) if ts else None

            a = agg.get(oid)
            if a is None:
                a = {
                    "product_id": product_id,
                    "side": side,
                    "sum_qty": 0.0,
                    "sum_notional": 0.0,
                    "sum_fee": 0.0,
                    "first_dt": dt,
                    "last_dt": dt,
                }
                agg[oid] = a

            if product_id and not a.get("product_id"):
                a["product_id"] = product_id
            if side and not a.get("side"):
                a["side"] = side

            if size is not None and size > 0:
                a["sum_qty"] += float(size)
                if price is not None:
                    a["sum_notional"] += float(size) * float(price)

                if fee is not None and float(fee) != 0.0:
                    a["sum_fee"] += float(fee)

            if dt is not None:
                if a["first_dt"] is None or dt < a["first_dt"]:
                    a["first_dt"] = dt
                if a["last_dt"] is None or dt > a["last_dt"]:
                    a["last_dt"] = dt

        synthesized: List[VenueOrder] = []
        for oid, a in agg.items():
            sum_qty = float(a.get("sum_qty") or 0.0)
            sum_notional = float(a.get("sum_notional") or 0.0)
            sum_fee = float(a.get("sum_fee") or 0.0)

            filled_qty = sum_qty if sum_qty > 0 else None
            avg_fill_price = (sum_notional / sum_qty) if sum_qty > 0 else None
            fee_val = sum_fee if abs(sum_fee) > 0 else None

            if oid in by_id:
                o = by_id[oid]
                try:
                    o_filled = _to_float(o.get("filled_qty"))
                except Exception:
                    o_filled = None
                if filled_qty is not None and (o_filled is None or o_filled <= 0 or filled_qty > o_filled):
                    o["filled_qty"] = filled_qty
                if avg_fill_price is not None and (
                    o.get("avg_fill_price") is None or _to_float(o.get("avg_fill_price")) in (None, 0.0)
                ):
                    o["avg_fill_price"] = avg_fill_price
                if fee_val is not None and (o.get("fee") is None or _to_float(o.get("fee")) in (None, 0.0)):
                    o["fee"] = fee_val
                if o.get("created_at") is None and a.get("first_dt") is not None:
                    o["created_at"] = a["first_dt"]
                if o.get("updated_at") is None and a.get("last_dt") is not None:
                    o["updated_at"] = a["last_dt"]
                continue

            product_id = a.get("product_id")
            if not product_id:
                continue
            product_id = str(product_id).upper()

            side = (a.get("side") or "").lower()
            created_at = a.get("first_dt")
            updated_at = a.get("last_dt")

            total_after_fee = None
            if filled_qty is not None and avg_fill_price is not None:
                notional = float(filled_qty) * float(avg_fill_price)
                if fee_val is None:
                    total_after_fee = notional
                else:
                    if side == "sell":
                        total_after_fee = max(0.0, notional - float(fee_val))
                    else:
                        total_after_fee = max(0.0, notional + float(fee_val))

            synthesized.append(
                {
                    "venue": "coinbase",
                    "venue_order_id": oid,
                    "cancel_ref": f"coinbase:{oid}",
                    "symbol_canon": product_id,
                    "symbol_venue": product_id,
                    "side": side or None,
                    "type": None,
                    "status": "filled",
                    "qty": filled_qty,
                    "filled_qty": filled_qty,
                    "limit_price": None,
                    "avg_fill_price": avg_fill_price,
                    "fee": fee_val,
                    "fee_asset": None,
                    "total_after_fee": total_after_fee,
                    "created_at": created_at,
                    "updated_at": updated_at,
                }
            )

        if synthesized:
            orders = list(orders) + synthesized

        return orders

    def fetch_orderbook(self, symbol_venue: str, depth: int, dry_run: bool) -> OrderBook:
        pid_in = (symbol_venue or "").strip()
        if not pid_in:
            return {"bids": [], "asks": []}

        pid = self.resolve_symbol(pid_in)

        try:
            self._ensure_products_cache()
        except Exception:
            pass
        try:
            self._fetch_and_cache_product_rules(pid)
        except Exception:
            pass

        rules = self._get_rules(pid) or {}
        ov = self._get_precision_override(pid)

        base_inc_s = self._rules_get_with_override(pid, rules, "base_increment")
        price_inc_s = (
            self._rules_get_with_override(pid, rules, "price_increment")
            or self._rules_get_with_override(pid, rules, "quote_increment")
        )

        price_decimals = None
        qty_decimals = None
        if isinstance(ov.get("price_decimals"), int) and ov.get("price_decimals") >= 0:
            price_decimals = int(ov.get("price_decimals"))
        else:
            price_decimals = self._decimals_from_str(price_inc_s)
        if isinstance(ov.get("qty_decimals"), int) and ov.get("qty_decimals") >= 0:
            qty_decimals = int(ov.get("qty_decimals"))
        else:
            qty_decimals = self._decimals_from_str(base_inc_s)

        if price_decimals is None:
            price_decimals = 8
        if qty_decimals is None:
            qty_decimals = 8

        url = f"https://api.exchange.coinbase.com/products/{pid}/book"
        params = {"level": "2"}

        with httpx.Client(timeout=15.0) as client:
            r = client.get(url, params=params, headers={"Accept": "application/json"})
            r.raise_for_status()
            data = r.json() if r.content else {}

        bids_raw = (data.get("bids") or [])[:depth]
        asks_raw = (data.get("asks") or [])[:depth]

        def qround(x: float, d: int) -> float:
            try:
                return float(round(float(x), int(d)))
            except Exception:
                return float(x)

        bids: List[Dict[str, float]] = []
        for b in bids_raw:
            try:
                if isinstance(b, list) and len(b) >= 2:
                    px = float(b[0])
                    qt = float(b[1])
                    bids.append({"price": qround(px, price_decimals), "qty": qround(qt, qty_decimals)})
            except Exception:
                continue

        asks: List[Dict[str, float]] = []
        for a in asks_raw:
            try:
                if isinstance(a, list) and len(a) >= 2:
                    px = float(a[0])
                    qt = float(a[1])
                    asks.append({"price": qround(px, price_decimals), "qty": qround(qt, qty_decimals)})
            except Exception:
                continue

        return {"bids": bids, "asks": asks}

    # ─────────────────────────────────────────────────────────────
    # Transfers (Deposits / Withdrawals) – CDP / Advanced Trade (JWT)
    #
    # IMPORTANT:
    # - This does NOT use Exchange HMAC/passphrase auth.
    # - It uses the same JWT auth as your trading/orders paths (_private_request).
    # - Coinbase may still 401/404 this endpoint depending on entitlement.
    # ─────────────────────────────────────────────────────────────

    # ✅ CHANGE 4: scope key_permissions helper
    def _fetch_key_permissions_best_effort(self, *, scope: str = "trade") -> Dict[str, Any]:
        """
        Best-effort helper: if Coinbase exposes key permissions for this key, include it
        in errors for better diagnosis. Never hard-fail transfers just because this fails.
        """
        try:
            return self._private_request("GET", "/api/v3/brokerage/key_permissions", scope=scope)
        except Exception:
            return {}

    def _normalize_raw_transfer(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Best-effort normalizer to the "Gemini-like" transfer dict shape used by the
        deposits/withdrawals ingest routers.

        Output shapes:
          Deposit:
            { type: "Deposit", status, timestampms, eid, currency, amount, transferId, ... }

          Withdrawal:
            { type: "Withdrawal", status, timestampms, eid, currency, amount, feeAmount, feeCurrency, txHash, withdrawalId, ... }
        """
        try:
            if not isinstance(item, dict):
                return None

            # classify type
            t_raw = (
                item.get("type")
                or item.get("transaction_type")
                or item.get("transactionType")
                or item.get("kind")
                or item.get("category")
                or ""
            )
            t = str(t_raw or "").strip().lower()

            is_dep = ("deposit" in t) or (t == "credit")
            is_wd = ("withdraw" in t) or ("withdrawal" in t) or (t == "debit")

            if not (is_dep or is_wd):
                # some payloads use "TRANSFER_IN/TRANSFER_OUT"
                if "in" in t and "transfer" in t:
                    is_dep = True
                elif "out" in t and "transfer" in t:
                    is_wd = True

            if not (is_dep or is_wd):
                # if unknown, skip (prevents polluting ingest with unrelated tx types)
                return None

            # timestamps
            ts_iso = (
                item.get("created_at")
                or item.get("created_time")
                or item.get("timestamp")
                or item.get("time")
                or item.get("completed_at")
                or item.get("updated_at")
            )
            dt = self._dt_from_iso(ts_iso) if ts_iso else None
            ts_ms = int(dt.timestamp() * 1000) if dt else None

            # ids
            transfer_id = (
                item.get("transfer_id")
                or item.get("transferId")
                or item.get("id")
                or item.get("transaction_id")
                or item.get("transactionId")
            )
            transfer_id_s = str(transfer_id) if transfer_id is not None else None

            # stable-ish numeric eid for UTT (Gemini uses 'eid')
            eid = None
            try:
                if transfer_id_s:
                    hv = hashlib.sha1(transfer_id_s.encode("utf-8")).hexdigest()
                    eid = int(hv[:12], 16)
            except Exception:
                eid = None

            # currency / amount can appear in various shapes
            currency = (
                item.get("currency")
                or item.get("asset")
                or item.get("symbol")
                or (item.get("amount", {}) if isinstance(item.get("amount"), dict) else {}).get("currency")
                or ""
            )
            currency = str(currency or "").strip().upper()

            amt = item.get("amount")
            amount_val = None
            if isinstance(amt, dict):
                amount_val = amt.get("value") or amt.get("amount") or amt.get("quantity")
                if not currency:
                    currency = str(amt.get("currency") or "").strip().upper()
            else:
                amount_val = amt

            if amount_val is None:
                amount_val = item.get("quantity") or item.get("size") or item.get("value")

            amount_s = str(amount_val) if amount_val is not None else None
            if amount_s is not None:
                amount_s = amount_s.strip()

            # status
            status_raw = item.get("status") or item.get("state") or item.get("result") or ""
            status_s = str(status_raw or "").strip().title() if status_raw is not None else ""

            # fee
            fee_raw = item.get("fee") or item.get("fees")
            fee_amount = None
            fee_currency = None
            if isinstance(fee_raw, dict):
                fee_amount = fee_raw.get("value") or fee_raw.get("amount")
                fee_currency = fee_raw.get("currency")
            elif fee_raw is not None:
                fee_amount = fee_raw

            if fee_currency is None:
                fee_currency = currency

            # tx hash (if any)
            details = item.get("details") or item.get("network") or {}
            tx_hash = (
                item.get("tx_hash")
                or item.get("txHash")
                or item.get("hash")
                or (details.get("tx_hash") if isinstance(details, dict) else None)
                or (details.get("transaction_hash") if isinstance(details, dict) else None)
                or (details.get("crypto_transaction_hash") if isinstance(details, dict) else None)
            )

            if is_dep:
                return {
                    "type": "Deposit",
                    "status": status_s,
                    "timestampms": ts_ms,
                    "eid": eid,
                    "currency": currency,
                    "amount": amount_s,
                    "transferId": transfer_id_s,
                    "raw": item,
                }

            # withdrawal
            return {
                "type": "Withdrawal",
                "status": status_s,
                "timestampms": ts_ms,
                "eid": eid,
                "currency": currency,
                "amount": amount_s,
                "feeAmount": str(fee_amount).strip() if fee_amount is not None else None,
                "feeCurrency": str(fee_currency).strip().upper() if fee_currency is not None else None,
                "txHash": str(tx_hash).strip() if tx_hash is not None else None,
                "withdrawalId": transfer_id_s,
                "raw": item,
            }
        except Exception:
            return None

    def fetch_transfers(
        self,
        since_dt,
        kinds=("deposit", "withdrawal"),
        limit: int = 250,
        max_pages: int = 20,
        currency: Optional[str] = None,
        **_ignored,
    ) -> List[Dict[str, Any]]:
        """
        Fetch transfers (deposits/withdrawals) via CDP / Advanced Trade auth.

        Routers call this and then "ingest" into DB. This method only FETCHES.

        Notes:
        - Coinbase may not grant this endpoint for all keys/accounts.
        - We tolerate shape differences and client-side filter by kind/currency/since_dt.
        """
        path = "/api/v3/brokerage/transactions/historical/batch"

        # normalize inputs
        kinds_l = [str(k).strip().lower() for k in (kinds or []) if str(k).strip()]
        want_deposit = any(k in ("deposit", "deposits") for k in kinds_l) or not kinds_l
        want_withdrawal = any(k in ("withdrawal", "withdrawals", "withdraw") for k in kinds_l) or not kinds_l

        ccy = str(currency).strip().upper() if currency else None

        # since_dt may be aware or naive. Normalize to naive UTC for comparisons.
        since_naive = None
        try:
            if since_dt is not None:
                if isinstance(since_dt, datetime):
                    if since_dt.tzinfo is not None:
                        since_naive = since_dt.astimezone(timezone.utc).replace(tzinfo=None)
                    else:
                        since_naive = since_dt
        except Exception:
            since_naive = None

        out: List[Dict[str, Any]] = []
        cursor: Optional[str] = None

        limit_i = max(1, int(limit or 250))
        max_pages_i = max(1, int(max_pages or 20))
        prefer_v2 = self._env_bool("COINBASE_TRANSFERS_PREFER_V2", True)
        force_v2 = self._env_bool("COINBASE_TRANSFERS_FORCE_V2", False)

        # --- FIX: define since_iso + limit_transfers for v2 path ---
        since_iso = None
        try:
            if since_naive is not None:
                # since_naive is naive UTC; encode as Zulu for v2 helper
                since_iso = since_naive.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            since_iso = None

        # Total transfer cap for v2 helper (avoid unbounded scans)
        # Keep it reasonable; v3 path already pages up to limit_i * max_pages_i.
        try:
            limit_transfers = min(5000, int(limit_i) * int(max_pages_i))
        except Exception:
            limit_transfers = 5000
        # --- end FIX ---

        if prefer_v2:
            try:
                return self._fetch_transfers_v2(
                    since_iso=since_iso,
                    max_pages=max_pages_i,
                    limit_transfers=limit_transfers,
                )
            except Exception:
                if force_v2:
                    raise
                # else fall back to v3


        def _items_from_payload(data: Any) -> List[Dict[str, Any]]:
            if isinstance(data, dict):
                items = (
                    data.get("transactions")
                    or data.get("items")
                    or data.get("data")
                    or data.get("results")
                    or []
                )
                if isinstance(items, dict):
                    items = items.get("transactions") or items.get("items") or []
                if isinstance(items, list):
                    return [x for x in items if isinstance(x, dict)]
                return []
            if isinstance(data, list):
                return [x for x in data if isinstance(x, dict)]
            return []

        def _pagination_from_payload(data: Any) -> Tuple[bool, Optional[str]]:
            if not isinstance(data, dict):
                return (False, None)
            pag = data.get("pagination") or {}
            if isinstance(pag, dict):
                has_next = bool(pag.get("has_next") or pag.get("hasNext") or pag.get("has_more") or pag.get("hasMore"))
                next_cursor = pag.get("next_cursor") or pag.get("nextCursor") or pag.get("cursor")
                return (has_next, str(next_cursor).strip() if next_cursor else None)

            # fallback top-level
            has_next = bool(data.get("has_next") or data.get("hasNext"))
            next_cursor = data.get("next_cursor") or data.get("nextCursor") or data.get("cursor")
            return (has_next, str(next_cursor).strip() if next_cursor else None)

        for _ in range(max_pages_i):
            params: Dict[str, Any] = {"limit": str(limit_i)}
            if cursor:
                params["cursor"] = cursor

            try:
                data = self._private_request("GET", path, params=params, scope="transfers")
            except Exception as e_get:
                # Some environments might require POST. Try a conservative POST fallback only for obvious method/path issues.
                em = str(e_get or "")
                if ("HTTP 401" in em) or ("HTTP 404" in em) or ("HTTP 405" in em) or ("not found" in em.lower()):
                    try:
                        data = self._private_request(
                            "POST",
                            path,
                            json_body={"limit": limit_i, "cursor": cursor} if cursor else {"limit": limit_i},
                            scope="transfers",
                        )
                    except Exception as e_post:
                        # Optional escape hatch:
                        # Some Coinbase environments gate this endpoint behind "trade-enabled" keys
                        # even though it looks like "transfer history". If enabled, retry once using
                        # the normal trade credentials (scope="trade") before failing.
                        trade_fallback_enabled = self._env_bool("COINBASE_TRANSFERS_FALLBACK_TO_TRADE_KEY", False)
                        trade_fallback_err = None
                        trade_key_permissions = None

                        if trade_fallback_enabled:
                            try:
                                # Try GET first (some tenancies only allow one method).
                                try:
                                    data = self._private_request("GET", path, params=params, scope="trade")
                                except Exception:
                                    data = self._private_request(
                                        "POST",
                                        path,
                                        json_body={"limit": limit_i, "cursor": cursor} if cursor else {"limit": limit_i},
                                        scope="trade",
                                    )

                                # If trade-key call works, continue flow as normal.
                                items = _items_from_payload(data)
                                if not items:
                                    break

                                has_next, next_cursor = _pagination_from_payload(data)

                                for it in items:
                                    norm = self._normalize_raw_transfer(it)
                                    if not norm:
                                        continue
                                    t = str(norm.get("type") or "").strip().lower()
                                    if t == "deposit" and not want_deposit:
                                        continue
                                    if t == "withdrawal" and not want_withdrawal:
                                        continue
                                    if ccy:
                                        if str(norm.get("currency") or "").strip().upper() != ccy:
                                            continue
                                    if since_naive is not None:
                                        ts_ms = norm.get("timestampms")
                                        if isinstance(ts_ms, int) and ts_ms > 0:
                                            try:
                                                dt_item = datetime.fromtimestamp(ts_ms / 1000.0)
                                                if dt_item < since_naive:
                                                    continue
                                            except Exception:
                                                pass
                                    out.append(norm)

                                cursor = next_cursor
                                if not has_next or not cursor:
                                    break

                                # continue outer paging loop
                                continue

                            except Exception as e_trade:
                                # Keep diagnostics, then fall through to the normal rich error below.
                                try:
                                    trade_fallback_err = str(e_trade)
                                except Exception:
                                    trade_fallback_err = "trade fallback failed"
                                try:
                                    trade_key_permissions = self._fetch_key_permissions_best_effort(scope="trade")
                                except Exception:
                                    trade_key_permissions = None

                        perms = self._fetch_key_permissions_best_effort(scope="transfers")
                        raise Exception(
                            f"Coinbase transfers endpoint failed (GET then POST fallback). "
                            f"GET err={self._truncate(em, 600)}; POST err={self._truncate(str(e_post), 600)}; "
                            f"key_permissions={self._truncate(json.dumps(perms), 600)}"
                            + (
                                ""
                                if not trade_fallback_enabled
                                else (
                                    f"; trade_fallback_err={self._truncate(str(trade_fallback_err), 600)}; "
                                    f"trade_key_permissions={self._truncate(json.dumps(trade_key_permissions or {}), 600)}"
                                )
                            )
                        )
                else:
                    perms = self._fetch_key_permissions_best_effort(scope="transfers")
                    raise Exception(
                        f"Coinbase transfers endpoint failed. err={self._truncate(em, 800)}; "
                        f"key_permissions={self._truncate(json.dumps(perms), 600)}"
                    )

            items = _items_from_payload(data)
            if not items:
                break

            for it in items:
                norm = self._normalize_raw_transfer(it)
                if not norm:
                    continue

                t = str(norm.get("type") or "").strip().lower()
                if t == "deposit" and not want_deposit:
                    continue
                if t == "withdrawal" and not want_withdrawal:
                    continue

                if ccy:
                    if str(norm.get("currency") or "").strip().upper() != ccy:
                        continue

                if since_naive is not None:
                    ts_ms = norm.get("timestampms")
                    if isinstance(ts_ms, int) and ts_ms > 0:
                        try:
                            dt_item = datetime.fromtimestamp(ts_ms / 1000.0)
                            if dt_item < since_naive:
                                continue
                        except Exception:
                            pass

                out.append(norm)

            has_next, next_cursor = _pagination_from_payload(data)
            cursor = next_cursor
            if not has_next or not cursor:
                break

        return out


    # -------------------------------------------------------------------------
    # v2 Track API (Coinbase App) helpers for external deposits/withdrawals
    # -------------------------------------------------------------------------

    def _parse_iso_to_epoch_ms(self, iso_s: Optional[str]) -> Optional[int]:
        if not iso_s:
            return None
        try:
            # Coinbase v2 uses Zulu timestamps like 2026-02-04T01:09:41Z
            s = str(iso_s).strip()
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except Exception:
            return None

    def _v2_list_accounts(self, starting_after: Optional[str] = None) -> Dict[str, Any]:
        p = "/v2/accounts"
        if starting_after:
            p = f"{p}?starting_after={starting_after}"
        retries = _env_int("COINBASE_V2_RETRY", 3)
        backoff_base_s = float(os.getenv("COINBASE_V2_RETRY_BACKOFF_S", "0.5"))
        for attempt in range(max(1, retries)):
            try:
                return self._private_request("GET", p, scope="transfers")
            except Exception as e:
                if _is_http_5xx_error(str(e)) and attempt < max(1, retries) - 1:
                    time.sleep(backoff_base_s * (2 ** attempt))
                    continue
                raise

    def _v2_list_account_transactions(self, account_id: str, starting_after: Optional[str] = None) -> Dict[str, Any]:
        p = f"/v2/accounts/{account_id}/transactions"
        if starting_after:
            p = f"{p}?starting_after={starting_after}"
        retries = _env_int("COINBASE_V2_RETRY", 3)
        backoff_base_s = float(os.getenv("COINBASE_V2_RETRY_BACKOFF_S", "0.5"))
        last_err: Optional[Exception] = None
        for attempt in range(max(1, retries)):
            try:
                return self._private_request("GET", p, scope="transfers")
            except Exception as e:
                last_err = e
                if _is_http_5xx_error(str(e)) and attempt < max(1, retries) - 1:
                    time.sleep(backoff_base_s * (2 ** attempt))
                    continue
                break

        # Persistent v2 server errors are unfortunately common on deep pagination.
        # Don’t fail the entire ingest — stop pagination for this account.
        try:
            _LOG.warning("[coinbase:v2] transactions endpoint error; skipping further pages for account %s: %s", account_id, str(last_err))
        except Exception:
            pass
        return {"data": [], "pagination": {"next_starting_after": None}}

    def _normalize_v2_transaction_as_transfer(self, tx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # Router-compatible shape:
        # {type: Deposit|Withdrawal, eid, currency, amount, timestampms, status, txHash, kind, raw}
        if not isinstance(tx, dict):
            return None

        tx_type = str(tx.get("type") or "").strip().lower()

        # Human-facing details often reveal fills/trades; exclude those.
        details = tx.get("details") or {}
        details_title = ""
        details_sub = ""
        if isinstance(details, dict):
            details_title = str(details.get("title") or "").lower()
            details_sub = str(details.get("subtitle") or "").lower()

        if any(k in (details_title + " " + details_sub) for k in ("fill", "trade", "exchange", "buy", "sell", "order", "fee")):
            return None

        # Hard exclude common trade/fill types
        if any(k in tx_type for k in ("fill", "trade", "exchange", "buy", "sell", "order", "fee")):
            return None

        amt = (tx.get("amount") or {}).get("amount") if isinstance(tx.get("amount"), dict) else None
        cur = (tx.get("amount") or {}).get("currency") if isinstance(tx.get("amount"), dict) else None
        if amt is None or cur is None:
            return None

        try:
            amt_val = float(str(amt))
        except Exception:
            amt_val = None

        created_at = tx.get("created_at")
        timestampms = self._parse_iso_to_epoch_ms(str(created_at) if created_at else None)
        if timestampms is None:
            return None

        # External hints:
        network = tx.get("network") if isinstance(tx.get("network"), dict) else {}
        tx_hash = None
        if isinstance(network, dict):
            tx_hash = network.get("hash") or network.get("transaction_hash")


        to_obj = tx.get("to") or {}
        from_obj = tx.get("from") or {}

        # Coinbase v2 "transactions" is a superset feed (fills, buys/sells, internal moves, etc.).
        # For deposits/withdrawals ingest we only want *external* in/out of the venue:
        #   - on-chain movements (network hash) OR explicit crypto address objects
        #   - fiat rails (ACH/wire/bank/card/etc.)
        to_resource = ""
        from_resource = ""
        to_address = None
        from_address = None
        if isinstance(to_obj, dict):
            to_resource = str(to_obj.get("resource") or "").lower()
            # only treat explicit address fields as "external"; ignore email / resource_path
            to_address = to_obj.get("address") or None
        if isinstance(from_obj, dict):
            from_resource = str(from_obj.get("resource") or "").lower()
            from_address = from_obj.get("address") or None

        # strong on-chain indicator
        net = tx.get("network") or {}
        net_hash = None
        if isinstance(net, dict):
            net_hash = net.get("hash") or net.get("transaction_hash") or None

        # keep any hash-like field that might be added by other normalizers, but don't drop net_hash
        tx_hash = tx.get("txHash") or tx.get("tx_hash") or None
        if (not tx_hash) and net_hash:
            tx_hash = net_hash

        onchain_hint = bool(net_hash or tx_hash)

        # Strict address hint: only treat as external when Coinbase marks it as an address
        # AND it actually looks like a crypto address (not an email / internal identifier).
        def _addr_ok(a: Any) -> bool:
            try:
                s = str(a or "").strip()
                if not s:
                    return False
                if "@" in s:
                    return False
                if "/" in s or " " in s:
                    return False
                return len(s) >= 20
            except Exception:
                return False

        address_hint = ((to_resource == "address" and _addr_ok(to_address)) or (from_resource == "address" and _addr_ok(from_address)))

        # fiat rails are considered external even without on-chain data
        fiat_rail = False
        fiat_words = ("ach", "wire", "bank", "card", "debit", "credit", "sepa", "swift", "fiat", "cash", "payroll")
        if tx_type.startswith("fiat_") or "fiat" in tx_type:
            fiat_rail = True
        elif any(w in details_title or w in details_sub for w in fiat_words):
            fiat_rail = True
        elif to_resource in ("bank", "payment_method", "card") or from_resource in ("bank", "payment_method", "card"):
            fiat_rail = True

        external_hint = bool(onchain_hint or address_hint or fiat_rail)

        # "send"/"receive" can be internal Coinbase-to-Coinbase moves (email/user).
        # Keep SEND strict: require on-chain proof or fiat rail.
        if tx_type == "send" and not (onchain_hint or fiat_rail):
            return None

        # RECEIVE is often an external crypto deposit even when Coinbase omits a hash.
        # Allow RECEIVE when it's clearly address-external (address_hint) or has on-chain proof or fiat rail.
        if tx_type == "receive" and not (onchain_hint or fiat_rail or address_hint):
            return None

        if not external_hint:
            return None

        kind = None

        # Coinbase v2 may label both inbound and outbound crypto moves as "send"/"receive" depending on context.
        # The most reliable direction signal is the signed amount:
        #   amt > 0  => funds credited to this account (deposit)
        #   amt < 0  => funds debited from this account (withdrawal)
        if tx_type in ("send", "receive"):
            if amt_val is None or amt_val == 0:
                return None
            kind = "deposit" if amt_val > 0 else "withdrawal"
        else:
            deposit_types = {"receive", "deposit", "pro_deposit", "fiat_deposit"}
            withdrawal_types = {"send", "withdrawal", "pro_withdrawal", "fiat_withdrawal"}

            if tx_type in deposit_types or tx_type.endswith("_deposit"):
                kind = "deposit"
            elif tx_type in withdrawal_types or tx_type.endswith("_withdrawal"):
                kind = "withdrawal"
            else:
                # External-only fallback by sign (after trade exclusions)
                if amt_val is not None and amt_val != 0:
                    kind = "deposit" if amt_val > 0 else "withdrawal"

        if kind not in ("deposit", "withdrawal"):
            return None

        return {
            "type": "Deposit" if kind == "deposit" else "Withdrawal",
            "eid": str(tx.get("id") or ""),
            "currency": str(cur),
            "amount": str(amt),
            "timestampms": int(timestampms),
            "status": str(tx.get("status") or ""),
            "txHash": tx_hash,
            "kind": kind,
            "timestamp": str(created_at) if created_at else None,
            "feeAmount": None,
            "feeCurrency": None,
            "raw": tx,
            "origin_ref": f"coinbase:{kind}:{tx.get('id')}",
            "venue": "coinbase",
        }

    def _fetch_transfers_v2(
        self,
        since_iso: Optional[str],
        max_pages: int,
        limit_transfers: int,
    ) -> List[Dict[str, Any]]:
        debug_types = self._env_bool("COINBASE_V2_TX_DEBUG", False)
        seen_types: Dict[str, int] = {}
        keep_debug = self._env_bool("COINBASE_V2_KEEP_DEBUG", False)
        kept_kinds: Dict[str, int] = {}
        kept_samples: List[str] = []

        since_ms = None
        try:
            since_ms = self._parse_iso_to_epoch_ms(since_iso) if since_iso else None
        except Exception:
            since_ms = None

        out: List[Dict[str, Any]] = []
        seen_tx_ids: set[str] = set()

        # accounts pagination via starting_after (best-effort)
        after = None
        pages = 0
        while pages < max_pages:
            pages += 1
            resp = self._v2_list_accounts(starting_after=after)
            accounts = (resp or {}).get("data") or []
            if not accounts:
                break

            # update cursor
            after = str(accounts[-1].get("id") or "") if accounts else None

            for acct in accounts:
                aid = acct.get("id")
                if not aid:
                    continue

                tx_after = None
                tx_pages = 0
                while tx_pages < max_pages:
                    tx_pages += 1
                    tx_resp = self._v2_list_account_transactions(account_id=str(aid), starting_after=tx_after)
                    txs = (tx_resp or {}).get("data") or []
                    if not txs:
                        break
                    tx_after = str(txs[-1].get("id") or "")

                    for tx in txs:
                        if debug_types:
                            try:
                                t = str((tx or {}).get("type") or "").strip().lower()
                                if t:
                                    seen_types[t] = seen_types.get(t, 0) + 1
                            except Exception:
                                pass

                        norm = self._normalize_v2_transaction_as_transfer(tx)
                        if not norm:
                            continue

                        if since_ms is not None:
                            try:
                                tms = int(norm.get("timestampms") or 0)
                                if tms and tms < int(since_ms):
                                    continue
                            except Exception:
                                pass
                        eid = norm.get("eid")
                        if not eid or eid in seen_tx_ids:
                            continue
                        seen_tx_ids.add(eid)
                        out.append(norm)

                        if keep_debug:
                            try:
                                k = str(norm.get("kind") or "").strip().lower()
                                if k:
                                    kept_kinds[k] = kept_kinds.get(k, 0) + 1
                                if len(kept_samples) < 20:
                                    kept_samples.append({
                                        "type": str((tx or {}).get("type") or ""),
                                        "kind": k,
                                        "amount": (tx.get("amount") or {}).get("amount") if isinstance(tx.get("amount"), dict) else None,
                                        "currency": (tx.get("amount") or {}).get("currency") if isinstance(tx.get("amount"), dict) else None,
                                        "created_at": tx.get("created_at"),
                                        "to_resource": (tx.get("to") or {}).get("resource") if isinstance(tx.get("to"), dict) else None,
                                        "from_resource": (tx.get("from") or {}).get("resource") if isinstance(tx.get("from"), dict) else None,
                                        "has_network_hash": bool(((tx.get("network") or {}) if isinstance(tx.get("network"), dict) else {}).get("hash")),
                                        "details_title": (tx.get("details") or {}).get("title") if isinstance(tx.get("details"), dict) else None,
                                    })
                            except Exception:
                                pass
                        if len(out) >= int(limit_transfers or 50):
                            break
                    if len(out) >= int(limit_transfers or 50):
                        break
                if len(out) >= int(limit_transfers or 50):
                    break
            if len(out) >= int(limit_transfers or 50):
                break

            # stop if no cursor
            if not after:
                break

        if debug_types and seen_types:
            try:
                top = sorted(seen_types.items(), key=lambda kv: kv[1], reverse=True)[:30]
                _LOG.warning("[coinbase:v2] top tx types: %s", top)
            except Exception:
                pass

        if keep_debug:
            try:
                topk = sorted(kept_kinds.items(), key=lambda kv: kv[1], reverse=True)
                _LOG.warning("[coinbase:v2] kept kinds: %s", topk)
                if kept_samples:
                    _LOG.warning("[coinbase:v2] kept samples (first %s): %s", len(kept_samples), kept_samples)
            except Exception:
                pass

        return out