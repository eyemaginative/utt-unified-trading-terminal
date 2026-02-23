# backend/app/adapters/cryptocom_exchange.py
#
# Crypto.com Exchange API v1 adapter (REST-only).
#
# Venue key: "cryptocom"
#
# Notes:
# - Public endpoints are GET:   /public/get-instruments, /public/get-book, /public/get-risk-parameters
# - Private endpoints are POST: /private/* and require api_key + sig
# - Signature: HMAC-SHA256 hex of: method + id + api_key + (sorted params key+value concat) + nonce
# - Per docs: numeric params should be sent as STRINGS in JSON (e.g. "0.01", not 0.01).

from __future__ import annotations

from typing import List, Optional, Dict, Any, Set, Tuple
import time
import os
import hmac
import hashlib
from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING, InvalidOperation
import logging
import threading

import httpx

from .base import ExchangeAdapter, PlacedOrder, BalanceItem, OrderBook, VenueOrder, OrderRules
from ..config import settings


logger = logging.getLogger("uvicorn.error")


def clamp_int(x: Any, lo: int, hi: int) -> int:
    try:
        n = int(float(x))
    except Exception:
        n = lo
    if n < lo:
        return lo
    if n > hi:
        return hi
    return n


class CryptoComExchangeAdapter(ExchangeAdapter):
    venue = "cryptocom"

    # ─────────────────────────────────────────────────────────────
    # Class-level caches (shared across instances)
    # ─────────────────────────────────────────────────────────────
    _instruments_cache_ts: float = 0.0
    _instruments_cache_ttl_s: float = float(os.getenv("CRYPTOCOM_INSTRUMENTS_CACHE_TTL_S", "300"))
    _instruments_cache: Optional[List[Dict[str, Any]]] = None

    # Risk params cache (min/max order notional per base token)
    _risk_params_cache_ts: float = 0.0
    _risk_params_cache_ttl_s: float = float(os.getenv("CRYPTOCOM_RISK_PARAMS_CACHE_TTL_S", "300"))
    _risk_params_cache: Optional[Dict[str, Dict[str, Decimal]]] = None  # {"BTC": {"min": D, "max": D}, ...}

    # Top-of-book cache (depth=1), very short TTL to prevent fan-out
    _book1_cache_ttl_s: float = float(os.getenv("CRYPTOCOM_BOOK1_CACHE_TTL_S", "1.5"))
    _book1_cache: Dict[str, Dict[str, Any]] = {}  # {instrument_name: {"ts": float, "bid": Decimal|None, "ask": Decimal|None}}

    # Lightweight internal TTL cache for private calls (open orders / history)
    _ttl_cache: Dict[str, Dict[str, Any]] = {}  # {key: {"exp": float, "val": Any}}

    # Recent instruments hint (for optional per-instrument history fallback)
    _recent_instruments_ttl_s: float = float(os.getenv("CRYPTOCOM_RECENT_INSTRUMENTS_TTL_S", "21600"))  # 6h
    _recent_instruments: Dict[str, float] = {}  # {inst: last_seen_ts}
    # Round-robin scan cursor for "fetch all history" fallback (avoids needing a static instrument allow-list).
    # We ONLY use this when unfiltered history is empty/unsupported and we have no symbol hints.
    _history_scan_lock = threading.Lock()
    _history_scan_cursor: int = 0

    # Recent cancel/closed hints (adapter-local): helps reconcile "ghost open" rows.
    _recent_cancels_lock = threading.Lock()
    _recent_cancels_ttl_s: float = float(os.getenv("CRYPTOCOM_RECENT_CANCELS_TTL_S", "180"))  # 3 min
    _recent_cancels: Dict[str, float] = {}  # {order_id: ts}


    # Safety toggles (to avoid rate-limit / spam on private endpoints)
    _include_history_default: bool = (
        os.getenv("CRYPTOCOM_INCLUDE_ORDER_HISTORY", "0").strip().lower()
        in ("1", "true", "yes", "y", "on")
    )

    # Advanced endpoints toggle:
    # Many keys appear to be valid for legacy private endpoints but not private/advanced/*
    # Default OFF to avoid repeated 40101 spam; can be enabled explicitly.
    _use_advanced_default: bool = (
        os.getenv("CRYPTOCOM_USE_ADVANCED", "0").strip().lower()
        in ("1", "true", "yes", "y", "on")
    )

    # Prefer SPOT instruments when listing/resolving
    _prefer_spot: bool = True

    # Guardrails toggles
    _ENFORCE_MIN_NOTIONAL: bool = (
        os.getenv("CRYPTOCOM_ENFORCE_MIN_NOTIONAL", "1").strip().lower()
        in ("1", "true", "yes", "y", "on")
    )
    # IMPORTANT: default OFF until proven necessary (can enable via env)
    _ENFORCE_TRADING_BANDWIDTH: bool = (
        os.getenv("CRYPTOCOM_ENFORCE_TRADING_BANDWIDTH", "0").strip().lower()
        in ("1", "true", "yes", "y", "on")
    )

    # Publish mark-derived effective min qty for UI (optional; default OFF)
    # NOTE: we compute effective min qty unconditionally; this flag is retained but no longer gates correctness.
    _PUBLISH_EFFECTIVE_MIN_QTY: bool = (
        os.getenv("CRYPTOCOM_PUBLISH_EFFECTIVE_MIN_QTY", "0").strip().lower()
        in ("1", "true", "yes", "y", "on")
    )

    # Default fallback min-notional (USD) if risk-params are unavailable
    _MIN_NOTIONAL_USD_FALLBACK: Decimal = Decimal((os.getenv("CRYPTOCOM_MIN_NOTIONAL_USD", "1") or "1").strip())

    # Default bandwidth parameters (best-effort; keep configurable)
    _TB_PCT: Decimal = Decimal((os.getenv("CRYPTOCOM_TRADING_BANDWIDTH_PCT", "0.15") or "0.15").strip())
    _TB_BUY_MIN_MULT: Decimal = Decimal((os.getenv("CRYPTOCOM_TRADING_BANDWIDTH_BUY_MIN_MULT", "0.10") or "0.10").strip())
    _TB_SELL_MAX_MULT: Decimal = Decimal((os.getenv("CRYPTOCOM_TRADING_BANDWIDTH_SELL_MAX_MULT", "10") or "10").strip())

    # What we consider "USD-equivalent" quotes for min/max notional enforcement
    _USD_EQ_QUOTES: Set[str] = {"USD", "USDT", "USDC"}

    # Some venues use alternative asset codes; keep minimal here
    _asset_alias = {
        "XBT": "BTC",
    }

    # ─────────────────────────────────────────────────────────────
    # Monotonic nonce/id (critical for Crypto.com 40101 avoidance)
    # ─────────────────────────────────────────────────────────────
    _nonce_lock = threading.Lock()
    _last_nonce: int = 0
    _last_req_id: int = 0

    @classmethod
    def _monotonic_ms(cls) -> int:
        """
        Return a strictly increasing millisecond integer across the whole process.
        Crypto.com is strict about nonces; duplicate/reused nonces can yield 40101.
        """
        now = int(time.time() * 1000)
        with cls._nonce_lock:
            if now <= cls._last_nonce:
                now = cls._last_nonce + 1
            cls._last_nonce = now
            return now

    @classmethod
    def _monotonic_req_id(cls) -> int:
        """
        Also keep request ids strictly increasing (helps avoid subtle server-side replay detection).
        """
        now = int(time.time() * 1000)
        with cls._nonce_lock:
            if now <= cls._last_req_id:
                now = cls._last_req_id + 1
            cls._last_req_id = now
            return now

    # ─────────────────────────────────────────────────────────────
    # TTL cache helpers
    # ─────────────────────────────────────────────────────────────
    def _cache_get(self, key: str) -> Any:
        try:
            row = CryptoComExchangeAdapter._ttl_cache.get(key)
            if not row:
                return None
            exp = float(row.get("exp", 0.0))
            if time.time() >= exp:
                CryptoComExchangeAdapter._ttl_cache.pop(key, None)
                return None
            return row.get("val")
        except Exception:
            return None

    def _cache_set(self, key: str, val: Any, ttl_s: float) -> None:
        try:
            ttl_s = float(ttl_s or 0.0)
            if ttl_s <= 0:
                return
            CryptoComExchangeAdapter._ttl_cache[key] = {"exp": time.time() + ttl_s, "val": val}
        except Exception:
            return

    def _mark_recent_instrument(self, instrument_name: str) -> None:
        inst = (instrument_name or "").strip()
        if not inst:
            return
        CryptoComExchangeAdapter._recent_instruments[inst] = time.time()
        cutoff = time.time() - float(CryptoComExchangeAdapter._recent_instruments_ttl_s)
        for k, ts in list(CryptoComExchangeAdapter._recent_instruments.items()):
            if float(ts) < cutoff:
                CryptoComExchangeAdapter._recent_instruments.pop(k, None)


    # ─────────────────────────────────────────────────────────────
    # Recent cancel hint cache (adapter-local)
    # ─────────────────────────────────────────────────────────────
    def _mark_recent_cancel(self, order_id: str) -> None:
        """Remember an order_id that was just canceled (or already closed) for a short TTL."""
        try:
            oid = (order_id or "").strip()
            if not oid:
                return
            now = time.time()
            CryptoComExchangeAdapter._recent_cancels[oid] = now
            ttl_s = float(CryptoComExchangeAdapter._recent_cancels_ttl_s or 0.0)
            if ttl_s <= 0:
                return
            cutoff = now - ttl_s
            for k, ts in list(CryptoComExchangeAdapter._recent_cancels.items()):
                if float(ts) < cutoff:
                    CryptoComExchangeAdapter._recent_cancels.pop(k, None)
        except Exception:
            return

    def _was_recently_canceled(self, order_id: str) -> bool:
        try:
            oid = (order_id or "").strip()
            if not oid:
                return False
            ts = CryptoComExchangeAdapter._recent_cancels.get(oid)
            if ts is None:
                return False
            ttl_s = float(CryptoComExchangeAdapter._recent_cancels_ttl_s or 0.0)
            if ttl_s <= 0:
                return True
            return (time.time() - float(ts)) <= ttl_s
        except Exception:
            return False

    # ─────────────────────────────────────────────────────────────
    # Small utilities
    # ─────────────────────────────────────────────────────────────
    def _now_ts(self) -> float:
        return time.time()

    def _env_bool(self, k: str, default: bool = False) -> bool:
        v = (os.getenv(k, "") or "").strip().lower()
        if v in ("1", "true", "yes", "y", "on"):
            return True
        if v in ("0", "false", "no", "n", "off"):
            return False
        return default

    def _env_int(self, k: str, default: int, lo: int = 0, hi: int = 10_000_000_000) -> int:
        """Best-effort integer env reader with clamping."""
        try:
            v = os.getenv(k)
            if v is None:
                return clamp_int(default, lo, hi)
            return clamp_int(v, lo, hi)
        except Exception:
            return clamp_int(default, lo, hi)

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

    def _dt_from_ms(self, ms: Any) -> Optional[datetime]:
        try:
            if ms is None:
                return None
            m = float(ms)
            sec = m / 1000.0
            return datetime.fromtimestamp(sec, tz=timezone.utc).replace(tzinfo=None)
        except Exception:
            return None

    def _dt_from_ns(self, ns: Any) -> Optional[datetime]:
        try:
            if ns is None:
                return None
            n = float(ns)
            sec = n / 1_000_000_000.0
            return datetime.fromtimestamp(sec, tz=timezone.utc).replace(tzinfo=None)
        except Exception:
            return None

    def _canon_asset(self, a: str) -> str:
        x = (a or "").strip().upper()
        return self._asset_alias.get(x, x)

    def _get_field(self, d: Dict[str, Any], *names: str) -> Any:
        for n in names:
            if not n:
                continue
            if n in d:
                return d.get(n)
        return None

    # ─────────────────────────────────────────────────────────────
    # Decimal helpers
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

    def _fmt_decimal(self, d: Decimal) -> str:
        s = format(d, "f")
        if "." in s:
            s = s.rstrip("0").rstrip(".")
        return s if s else "0"

    def _floor_to_step(self, value: Decimal, step: Decimal) -> Decimal:
        if step is None or step <= 0:
            return value
        q = (value / step).to_integral_value(rounding=ROUND_FLOOR)
        return q * step

    def _ceil_to_step(self, value: Decimal, step: Decimal) -> Decimal:
        if step is None or step <= 0:
            return value
        q = (value / step).to_integral_value(rounding=ROUND_CEILING)
        return q * step

    def _is_multiple_of_step(self, value: Decimal, step: Optional[Decimal]) -> bool:
        try:
            if step is None or step <= 0:
                return False
            if value is None:
                return False
            q = (value / step)
            q_int = q.to_integral_value(rounding=ROUND_FLOOR)
            return abs(q - q_int) < Decimal("0.000000001")
        except Exception:
            return False

    def _parse_book_level(
        self,
        level: Any,
        *,
        price_step: Optional[Decimal],
        qty_step: Optional[Decimal],
    ) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """
        Crypto.com levels are typically [price, qty, ...], but some payloads can come back [qty, price, ...].
        We score both interpretations using tick-step alignment and choose the better one.
        """
        if not isinstance(level, list) or len(level) < 2:
            return None, None

        a0 = self._dec(level[0])
        a1 = self._dec(level[1])
        if a0 is None or a1 is None:
            return None, None

        # Candidate 1: [price, qty]
        p1, q1 = a0, a1
        s1 = Decimal("0")
        if p1 > 0:
            s1 += Decimal("0.5")
        if q1 > 0:
            s1 += Decimal("0.5")
        if self._is_multiple_of_step(p1, price_step):
            s1 += Decimal("2")
        if self._is_multiple_of_step(q1, qty_step):
            s1 += Decimal("2")

        # Candidate 2: [qty, price]
        p2, q2 = a1, a0
        s2 = Decimal("0")
        if p2 > 0:
            s2 += Decimal("0.5")
        if q2 > 0:
            s2 += Decimal("0.5")
        if self._is_multiple_of_step(p2, price_step):
            s2 += Decimal("2")
        if self._is_multiple_of_step(q2, qty_step):
            s2 += Decimal("2")

        # Prefer higher score; tie-break to documented [price, qty]
        if s2 > s1:
            return p2, q2
        return p1, q1

    # ─────────────────────────────────────────────────────────────
    # Config / creds
    # ─────────────────────────────────────────────────────────────
    def _base_url(self) -> str:
        u = (
            getattr(settings, "cryptocom_exchange_base_url", None)
            or os.getenv("CRYPTOCOM_EXCHANGE_BASE_URL")
            or "https://api.crypto.com/exchange/v1"
        )
        return str(u).strip().rstrip("/")

    def _require_creds(self) -> Tuple[str, str]:
        key = getattr(settings, "cryptocom_exchange_api_key", None) or os.getenv("CRYPTOCOM_EXCHANGE_API_KEY")
        sec = getattr(settings, "cryptocom_exchange_api_secret", None) or os.getenv("CRYPTOCOM_EXCHANGE_API_SECRET")
        key = (key or "").strip()
        sec = (sec or "").strip()
        if not key or not sec:
            raise Exception("Missing Crypto.com Exchange credentials: set CRYPTOCOM_EXCHANGE_API_KEY and CRYPTOCOM_EXCHANGE_API_SECRET")
        return key, sec

    # ─────────────────────────────────────────────────────────────
    # Signing
    # ─────────────────────────────────────────────────────────────
    def _param_string(self, params: Optional[Dict[str, Any]]) -> str:
        """
        Crypto.com: sort request parameter keys ascending; concatenate key + value (no delimiters).
        Values are stringified exactly as we send them in JSON.
        """
        if not params or not isinstance(params, dict):
            return ""
        out: List[str] = []
        for k in sorted(params.keys(), key=lambda x: str(x)):
            kk = str(k)
            v = params.get(k)

            # IMPORTANT: do not include None in the signature string (omit such keys)
            if v is None:
                continue

            if isinstance(v, list):
                vv = "".join(str(x) for x in v if x is not None)
            elif isinstance(v, dict):
                vv = self._param_string(v)
            else:
                vv = str(v)
            out.append(kk + vv)
        return "".join(out)

    def _hmac_sig(self, *, method: str, req_id: int, api_key: str, params: Optional[Dict[str, Any]], nonce: int, secret: str) -> str:
        ps = self._param_string(params)
        payload = f"{method}{req_id}{api_key}{ps}{nonce}"
        return hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()

    def _next_req_id(self) -> int:
        return CryptoComExchangeAdapter._monotonic_req_id()

    def _nonce(self) -> int:
        return CryptoComExchangeAdapter._monotonic_ms()

    # ─────────────────────────────────────────────────────────────
    # HTTP helpers
    # ─────────────────────────────────────────────────────────────
    def _public_get(self, path: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        url = f"{self._base_url()}{path}"
        with httpx.Client(timeout=20.0) as client:
            r = client.get(url, params=params or {})
            r.raise_for_status()
            data = r.json() if r.content else {}
        if not isinstance(data, dict):
            raise Exception(f"Crypto.com public GET unexpected response type: {type(data)}")
        if int(data.get("code", 0) or 0) != 0:
            raise Exception(f"Crypto.com public GET error code={data.get('code')} message={data.get('message')}")
        return data

    def _private_post(self, method: str, params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        api_key, secret = self._require_creds()
        req_id = self._next_req_id()
        nonce = self._nonce()

        p_in: Dict[str, Any] = dict(params or {})
        p: Dict[str, Any] = {k: v for k, v in p_in.items() if v is not None}

        for nk in ("quantity", "price", "limit_price", "order_value", "slippage", "amount"):
            if nk in p and p[nk] is not None and not isinstance(p[nk], str):
                p[nk] = str(p[nk])

        sig = self._hmac_sig(method=method, req_id=req_id, api_key=api_key, params=p, nonce=nonce, secret=secret)

        body = {
            "id": req_id,
            "method": method,
            "api_key": api_key,
            "params": p,
            "nonce": nonce,
            "sig": sig,
        }

        url = f"{self._base_url()}/{method}"
        headers = {"Content-Type": "application/json"}

        with httpx.Client(timeout=30.0) as client:
            try:
                r = client.post(url, headers=headers, json=body)
                r.raise_for_status()
            except httpx.HTTPStatusError as e:
                txt = ""
                try:
                    txt = (e.response.text or "")[:1200]
                except Exception:
                    txt = ""
                try:
                    if int(getattr(e.response, "status_code", 0) or 0) == 401:
                        logger.warning(
                            "Crypto.com HTTP 401 debug: base_url=%s method=%s id=%s nonce=%s api_key=***%s param_keys=%s",
                            self._base_url(),
                            method,
                            req_id,
                            nonce,
                            (api_key[-6:] if api_key else ""),
                            sorted(list(p.keys())),
                        )
                except Exception:
                    pass
                raise Exception(f"Crypto.com private POST HTTP {e.response.status_code} method={method} body={txt}") from e

            data = r.json() if r.content else {}

        if not isinstance(data, dict):
            raise Exception(f"Crypto.com private POST unexpected response type: {type(data)}")

        code = int(data.get("code", 0) or 0)
        if code != 0:
            try:
                if code == 40101:
                    logger.warning(
                        "Crypto.com auth failure (40101): base_url=%s method=%s id=%s nonce=%s api_key=***%s param_keys=%s message=%s",
                        self._base_url(),
                        method,
                        req_id,
                        nonce,
                        (api_key[-6:] if api_key else ""),
                        sorted(list(p.keys())),
                        str(data.get("message") or ""),
                    )
            except Exception:
                pass
            raise Exception(f"Crypto.com private POST error code={code} message={data.get('message')} method={method}")

        return data

    # ─────────────────────────────────────────────────────────────
    # Transfers (deposits/withdrawals) for SAFE ingestion
    # ─────────────────────────────────────────────────────────────

    # Crypto.com Exchange: status codes are documented as ints.
    _CC_DEPOSIT_STATUS = {
        0: "NOT_ARRIVED",
        1: "ARRIVED",
        2: "FAILED",
        3: "PENDING",
    }

    # Crypto.com Exchange withdrawal status mapping (docs):
    # 0=PENDING,1=PROCESSING,2=REJECTED,3=PAYMENT_IN_PROGRESS,4=REFUNDED,5=COMPLETED,6=CANCELLED
    _CC_WITHDRAWAL_STATUS = {
        0: "PENDING",
        1: "PROCESSING",
        2: "REJECTED",
        3: "PAYMENT_IN_PROGRESS",
        4: "REFUNDED",
        5: "COMPLETED",
        6: "CANCELLED",
    }

    def _ts_ms_safe(self, dt: datetime) -> int:
        """Return epoch-ms for dt; clamp pre-epoch / invalid to 0 (Windows-safe)."""
        try:
            ts = dt.replace(tzinfo=None).timestamp()
            if ts < 0:
                return 0
            return int(float(ts) * 1000.0)
        except Exception:
            return 0

    def _status_deposit(self, st: Any) -> str:
        try:
            i = int(st)
            return self._CC_DEPOSIT_STATUS.get(i, str(st))
        except Exception:
            s = str(st or "").strip()
            return s.upper() if s else "UNKNOWN"

    def _status_withdrawal(self, st: Any) -> str:
        try:
            i = int(st)
            return self._CC_WITHDRAWAL_STATUS.get(i, str(st))
        except Exception:
            s = str(st or "").strip()
            return s.upper() if s else "UNKNOWN"

    def _eid_from_row(self, row: Dict[str, Any], prefix: str) -> str:
        """Stable id component. Prefer `id`, else withdrawal_id/deposit_id, else fallback."""
        rid = row.get("id") or row.get("withdrawal_id") or row.get("deposit_id")
        if rid is not None and str(rid).strip():
            return str(rid).strip()
        return f"{prefix}:{row.get('create_time')}:{row.get('amount')}:{row.get('currency')}"

    def fetch_transfers(
        self,
        *,
        since_dt: datetime,
        kinds: Optional[List[str]] = None,
        currency: Optional[str] = None,
        limit_transfers: int = 50,
        max_pages: int = 200,
    ) -> List[Dict[str, Any]]:
        """Read-only transfer ingest via Crypto.com Exchange.

        Endpoints:
          - private/get-deposit-history  -> result.deposit_list
          - private/get-withdrawal-history -> result.withdrawal_list

        Returns canonical dicts matching deposits.py / withdrawals.py ingest expectations.
        """
        want = {str(x).strip().lower() for x in (kinds or []) if str(x).strip()}
        if not want:
            want = {"deposit", "withdrawal"}

        cur = (currency or "").strip().upper() or None
        start_ms = self._ts_ms_safe(since_dt)
        end_ms = int(time.time() * 1000)

        out: List[Dict[str, Any]] = []
        seen: Set[str] = set()

        def keep_row(ts_ms: int, ccy: str) -> bool:
            if ts_ms and ts_ms < start_ms:
                return False
            if cur and (ccy or "").upper() != cur:
                return False
            return True

        def push(kind: str, row: Dict[str, Any]) -> None:
            eid = self._eid_from_row(row, kind)
            key = f"{kind}:{eid}"
            if key in seen:
                return
            seen.add(key)

            ccy = str(row.get("currency") or row.get("symbol") or "").strip().upper()
            if not ccy:
                return
            try:
                amt = float(row.get("amount"))
            except Exception:
                return

            try:
                ts_ms = int(row.get("create_time") or row.get("timestamp") or 0)
            except Exception:
                ts_ms = 0
            if 0 < ts_ms < 10_000_000_000:
                ts_ms = ts_ms * 1000

            if not keep_row(ts_ms, ccy):
                return

            txid = row.get("txid") or row.get("tx_hash") or row.get("transaction_id") or row.get("hash")
            tx_hash = str(txid).strip() if txid is not None and str(txid).strip() else None

            base: Dict[str, Any] = {
                "eid": eid,
                "currency": ccy,
                "amount": amt,
                "timestampms": int(ts_ms) if ts_ms else 0,
                "txHash": tx_hash,
                "raw_cryptocom": dict(row),
            }

            if kind == "deposit":
                base["type"] = "Deposit"
                base["status"] = self._status_deposit(row.get("status"))
            else:
                base["type"] = "Withdrawal"
                base["status"] = self._status_withdrawal(row.get("status"))
                dest = row.get("address") or row.get("to_address") or row.get("destination") or row.get("wallet_address")
                if dest is not None:
                    ds = str(dest).strip()
                    base["destination"] = ds if ds else None

            out.append(base)

        def page_loop(endpoint: str, kind: str) -> None:
            page = 0
            while page < int(max_pages or 0):
                payload: Dict[str, Any] = {
                    "start_ts": start_ms,
                    "end_ts": end_ms,
                    "page_size": int(limit_transfers or 50),
                    "page": int(page),
                }
                if cur:
                    payload["currency"] = cur

                resp = self._private_post(endpoint, payload)
                result = (resp or {}).get("result") or {}

                # Crypto.com Exchange returns deposit_list / withdrawal_list (not "data").
                if kind == "deposit":
                    data = (result or {}).get("deposit_list") or []
                else:
                    data = (result or {}).get("withdrawal_list") or []
                if not isinstance(data, list):
                    data = []

                for r in data:
                    if isinstance(r, dict):
                        push(kind, r)

                if not data or (limit_transfers and len(data) < int(limit_transfers)):
                    break
                page += 1

        if "deposit" in want:
            page_loop("private/get-deposit-history", "deposit")
        if "withdrawal" in want:
            page_loop("private/get-withdrawal-history", "withdrawal")

        out.sort(key=lambda x: int(x.get("timestampms") or 0), reverse=True)
        if limit_transfers and limit_transfers > 0:
            out = out[: int(limit_transfers)]
        return out

    # ─────────────────────────────────────────────────────────────
    # Instruments cache
    # ─────────────────────────────────────────────────────────────
    def _get_instruments(self) -> List[Dict[str, Any]]:
        now = self._now_ts()
        if (
            CryptoComExchangeAdapter._instruments_cache is not None
            and (now - CryptoComExchangeAdapter._instruments_cache_ts) < CryptoComExchangeAdapter._instruments_cache_ttl_s
        ):
            return CryptoComExchangeAdapter._instruments_cache or []

        data = self._public_get("/public/get-instruments", params={})
        rows = (data.get("result") or {}).get("data") or []
        if not isinstance(rows, list):
            rows = []

        out: List[Dict[str, Any]] = []
        for r in rows:
            if isinstance(r, dict):
                out.append(r)

        CryptoComExchangeAdapter._instruments_cache = out
        CryptoComExchangeAdapter._instruments_cache_ts = now
        return out

    def _inst_id(self, inst: Dict[str, Any]) -> str:
        """
        Crypto.com is inconsistent: some payloads use `instrument_name`, some use `symbol`.
        We treat BOTH as valid "instrument identifier" for public endpoints like get-book.
        """
        if not isinstance(inst, dict):
            return ""
        return (
            str(inst.get("instrument_name") or "").strip()
            or str(inst.get("symbol") or "").strip()
            or str(inst.get("name") or "").strip()
        )

    def _find_instrument(self, symbol_venue: str) -> Optional[Dict[str, Any]]:
        sv = (symbol_venue or "").strip()
        if not sv:
            return None
        svu = sv.upper()

        for r in self._get_instruments():
            if not isinstance(r, dict):
                continue
            rid = self._inst_id(r)
            if rid == sv or rid.upper() == svu:
                return r

            sym = str(r.get("symbol") or "").strip()
            if sym == sv or sym.upper() == svu:
                return r

            iname = str(r.get("instrument_name") or "").strip()
            if iname == sv or iname.upper() == svu:
                return r

        return None

    def _is_derivative_type(self, inst_type: str) -> bool:
        t = str(inst_type or "").upper().strip()
        if not t:
            return False
        return any(x in t for x in ("PERP", "PERPETUAL", "FUT", "SWAP", "OPTION"))

    def _prefer_instrument(self, inst: Dict[str, Any]) -> bool:
        """
        IMPORTANT:
        Some Crypto.com instrument payloads do not reliably label SPOT as "SPOT".
        For correctness (especially FX discovery), we:
          - reject explicit derivatives,
          - accept blank/unknown inst_type as spot-like if tradable.
        """
        if not isinstance(inst, dict):
            return False
        if inst.get("tradable") is False:
            return False

        it = str(inst.get("inst_type") or "").upper().strip()
        if self._is_derivative_type(it):
            return False

        if not self._prefer_spot:
            return True

        if "SPOT" in it or "CASH" in it:
            return True
        if not it:
            return True
        return True

    

    def _all_tradable_spot_instruments(self) -> List[str]:
        """Return a stable, de-duped list of tradable spot-like instrument ids.

        IMPORTANT: This is ONLY used for the round-robin history scan fallback when:
          - unfiltered history returns empty / unsupported, AND
          - we have no useful instrument hints (open orders / recent instruments), AND
          - operator enables CRYPTOCOM_ORDER_HISTORY_SCAN_ALL=1.

        We deliberately filter out obvious derivatives and non-tradable instruments to keep call volume sane.
        """
        out: List[str] = []
        for r in self._get_instruments():
            if not isinstance(r, dict):
                continue
            if not self._prefer_instrument(r):
                continue
            rid = self._inst_id(r)
            if rid:
                out.append(rid)
        return sorted(set(out))
# ─────────────────────────────────────────────────────────────
    # Risk parameters cache (min/max order notional per base token)
    # ─────────────────────────────────────────────────────────────
    def _get_risk_params(self) -> Dict[str, Dict[str, Decimal]]:
        now = self._now_ts()
        if (
            CryptoComExchangeAdapter._risk_params_cache is not None
            and (now - CryptoComExchangeAdapter._risk_params_cache_ts) < CryptoComExchangeAdapter._risk_params_cache_ttl_s
        ):
            return CryptoComExchangeAdapter._risk_params_cache or {}

        out: Dict[str, Dict[str, Decimal]] = {}

        try:
            data = self._public_get("/public/get-risk-parameters", params={})
            res = data.get("result") or {}
            rows = res.get("base_currency_config") or []
            if not isinstance(rows, list):
                rows = []

            for r in rows:
                if not isinstance(r, dict):
                    continue

                base_raw = (
                    r.get("instrument_name")
                    or r.get("base_currency")
                    or r.get("currency")
                    or r.get("name")
                )
                base = self._canon_asset(str(base_raw or "").strip()).upper()
                if not base:
                    continue

                mn = self._dec(r.get("min_order_notional_usd"))
                mx = self._dec(r.get("max_order_notional_usd"))

                d: Dict[str, Decimal] = {}
                if mn is not None and mn > 0:
                    d["min"] = mn
                if mx is not None and mx > 0:
                    d["max"] = mx

                if d:
                    out[base] = d
        except Exception:
            out = {}

        CryptoComExchangeAdapter._risk_params_cache = out
        CryptoComExchangeAdapter._risk_params_cache_ts = now
        return out

    def _risk_for_base(self, base_ccy: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        b = self._canon_asset(str(base_ccy or "").strip()).upper()
        if not b:
            return None, None
        mp = self._get_risk_params()
        row = mp.get(b) or {}
        mn = row.get("min")
        mx = row.get("max")
        return mn, mx

    # ─────────────────────────────────────────────────────────────
    # Discovery: list symbols (canonical BASE-QUOTE)
    # ─────────────────────────────────────────────────────────────
    def list_symbols(self) -> List[str]:
        try:
            from ..services.symbol_policy import ALLOWED_QUOTES  # type: ignore
            allowed_quotes = {str(x).upper().strip() for x in (ALLOWED_QUOTES or set()) if str(x).strip()}
        except Exception:
            allowed_quotes = {"USD", "USDT", "USDC", "BTC", "ETH", "EUR"}

        out: Set[str] = set()
        for r in self._get_instruments():
            if not isinstance(r, dict):
                continue
            if not self._prefer_instrument(r):
                continue

            base = self._canon_asset(str(r.get("base_ccy") or "").strip())
            quote = self._canon_asset(str(r.get("quote_ccy") or "").strip())
            if not base or not quote:
                continue
            if quote.upper() not in allowed_quotes:
                continue
            out.add(f"{base.upper()}-{quote.upper()}")

        return sorted(out)

    # ─────────────────────────────────────────────────────────────
    # Symbol resolution: canonical BASE-QUOTE -> venue instrument id string
    # ─────────────────────────────────────────────────────────────
    def resolve_symbol(self, symbol_canon: str) -> str:
        s = (symbol_canon or "").strip()
        if not s:
            return s

        inst = self._find_instrument(s)
        if inst:
            rid = self._inst_id(inst)
            return rid or s

        if "-" in s:
            base, quote = s.split("-", 1)
            base_u = self._canon_asset(base).upper()
            quote_u = self._canon_asset(quote).upper()

            best: Optional[str] = None
            best_score = -10
            for r in self._get_instruments():
                if not isinstance(r, dict):
                    continue
                b = self._canon_asset(str(r.get("base_ccy") or "").strip()).upper()
                q = self._canon_asset(str(r.get("quote_ccy") or "").strip()).upper()
                if b != base_u or q != quote_u:
                    continue

                rid = self._inst_id(r)
                if not rid:
                    continue

                score = 0
                it = str(r.get("inst_type") or "").upper().strip()
                if "SPOT" in it or "CASH" in it or not it:
                    score += 10
                if r.get("tradable") is True:
                    score += 3
                if self._is_derivative_type(it):
                    score -= 10

                if score > best_score:
                    best_score = score
                    best = rid

            if best:
                return best

            # Fallback to what UI uses (BASE-QUOTE) — may still work on some payloads.
            return f"{base_u}-{quote_u}"

        return s

    # ─────────────────────────────────────────────────────────────
    # Spot helpers
    # ─────────────────────────────────────────────────────────────
    def _get_top_of_book(self, instrument_name: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """
        Depth-1 best bid/ask with hardened parsing:
        Crypto.com is usually [price, qty], but some payloads can be [qty, price].
        We use tick-size alignment to choose interpretation deterministically.
        """
        inst_name = (instrument_name or "").strip()
        if not inst_name:
            return None, None

        now = self._now_ts()
        cached = CryptoComExchangeAdapter._book1_cache.get(inst_name)
        if cached and (now - float(cached.get("ts", 0.0))) < CryptoComExchangeAdapter._book1_cache_ttl_s:
            return cached.get("bid"), cached.get("ask")

        inst = self._find_instrument(inst_name) or {}
        pstep = self._dec(inst.get("price_tick_size"))
        qstep = self._dec(inst.get("qty_tick_size"))

        try:
            data = self._public_get("/public/get-book", params={"instrument_name": inst_name, "depth": "1"})
            res = (data.get("result") or {})
            arr = res.get("data") or []
            if not isinstance(arr, list) or not arr:
                CryptoComExchangeAdapter._book1_cache[inst_name] = {"ts": now, "bid": None, "ask": None}
                return None, None

            row = arr[0] if isinstance(arr[0], dict) else {}
            bids_raw = row.get("bids") or []
            asks_raw = row.get("asks") or []

            best_bid = None
            best_ask = None

            if isinstance(bids_raw, list) and bids_raw:
                p, _q = self._parse_book_level(bids_raw[0], price_step=pstep, qty_step=qstep)
                best_bid = p

            if isinstance(asks_raw, list) and asks_raw:
                p, _q = self._parse_book_level(asks_raw[0], price_step=pstep, qty_step=qstep)
                best_ask = p

            CryptoComExchangeAdapter._book1_cache[inst_name] = {"ts": now, "bid": best_bid, "ask": best_ask}
            return best_bid, best_ask
        except Exception:
            CryptoComExchangeAdapter._book1_cache[inst_name] = {"ts": now, "bid": None, "ask": None}
            return None, None

    def _instrument_base_quote(self, instrument_name: str) -> Tuple[Optional[str], Optional[str]]:
        inst = self._find_instrument(instrument_name)
        if not isinstance(inst, dict):
            return None, None
        b = self._canon_asset(str(inst.get("base_ccy") or "").strip()).upper() or None
        q = self._canon_asset(str(inst.get("quote_ccy") or "").strip()).upper() or None
        return b, q

    def _mark_from_bid_ask(self, bid: Optional[Decimal], ask: Optional[Decimal]) -> Optional[Decimal]:
        try:
            if bid is not None and ask is not None and bid > 0 and ask > 0:
                return (bid + ask) / Decimal("2")
            if ask is not None and ask > 0:
                return ask
            if bid is not None and bid > 0:
                return bid
        except Exception:
            return None
        return None

    def _find_quote_usd_instrument(self, quote_ccy: str) -> Optional[str]:
        """
        Preferred direction: QUOTE -> USD (e.g., BTC-USDT / BTC-USD / BTC-USDC).
        Returns the instrument id (instrument_name/symbol) to use with get-book.
        """
        q = self._canon_asset(str(quote_ccy or "").strip()).upper()
        if not q or q in self._USD_EQ_QUOTES:
            return None

        best_sym: Optional[str] = None
        best_score = -1_000

        for r in self._get_instruments():
            if not isinstance(r, dict):
                continue
            if r.get("tradable") is False:
                continue

            it = str(r.get("inst_type") or "").upper().strip()
            if self._is_derivative_type(it):
                continue

            b = self._canon_asset(str(r.get("base_ccy") or "").strip()).upper()
            qc = self._canon_asset(str(r.get("quote_ccy") or "").strip()).upper()

            if b != q:
                continue
            if qc not in self._USD_EQ_QUOTES:
                continue

            rid = self._inst_id(r)
            if not rid:
                continue

            score = 0
            if qc == "USD":
                score += 300
            elif qc == "USDT":
                score += 200
            elif qc == "USDC":
                score += 100

            if "SPOT" in it or "CASH" in it or not it:
                score += 20
            if r.get("tradable") is True:
                score += 5

            if score > best_score:
                best_score = score
                best_sym = rid

        return best_sym

    def _find_usd_quote_instrument(self, quote_ccy: str) -> Optional[str]:
        """
        Fallback direction: USD -> QUOTE (e.g., USD-BTC / USDT-BTC).
        If found, we must invert the mark.
        Returns the instrument id to use with get-book.
        """
        q = self._canon_asset(str(quote_ccy or "").strip()).upper()
        if not q or q in self._USD_EQ_QUOTES:
            return None

        best_sym: Optional[str] = None
        best_score = -1_000

        for r in self._get_instruments():
            if not isinstance(r, dict):
                continue
            if r.get("tradable") is False:
                continue

            it = str(r.get("inst_type") or "").upper().strip()
            if self._is_derivative_type(it):
                continue

            b = self._canon_asset(str(r.get("base_ccy") or "").strip()).upper()
            qc = self._canon_asset(str(r.get("quote_ccy") or "").strip()).upper()

            if qc != q:
                continue
            if b not in self._USD_EQ_QUOTES:
                continue

            rid = self._inst_id(r)
            if not rid:
                continue

            score = 0
            if b == "USD":
                score += 300
            elif b == "USDT":
                score += 200
            elif b == "USDC":
                score += 100

            if "SPOT" in it or "CASH" in it or not it:
                score += 20
            if r.get("tradable") is True:
                score += 5

            if score > best_score:
                best_score = score
                best_sym = rid

        return best_sym

    # ─────────────────────────────────────────────────────────────
    # FX helpers (NEW): dual mark parsing + plausibility selection
    # ─────────────────────────────────────────────────────────────
    def _book1_mark_dual(self, instrument_name: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """
        Fetch depth=1 and compute two possible marks:
          - mark0: interpret level[0] as price
          - mark1: interpret level[1] as price
        Returns (mark0, mark1).

        This is used for FX where Crypto.com may swap [price, qty] vs [qty, price]
        and instrument tick metadata can be missing/unreliable.
        """
        inst_name = (instrument_name or "").strip()
        if not inst_name:
            return None, None

        try:
            data = self._public_get("/public/get-book", params={"instrument_name": inst_name, "depth": "1"})
            res = (data.get("result") or {})
            arr = res.get("data") or []
            if not isinstance(arr, list) or not arr:
                return None, None

            row = arr[0] if isinstance(arr[0], dict) else {}
            bids_raw = row.get("bids") or []
            asks_raw = row.get("asks") or []
            if not (isinstance(bids_raw, list) and bids_raw and isinstance(asks_raw, list) and asks_raw):
                return None, None

            b0 = bids_raw[0]
            a0 = asks_raw[0]
            if not (isinstance(b0, list) and len(b0) >= 2 and isinstance(a0, list) and len(a0) >= 2):
                return None, None

            b_a0 = self._dec(b0[0])
            b_a1 = self._dec(b0[1])
            a_a0 = self._dec(a0[0])
            a_a1 = self._dec(a0[1])
            if b_a0 is None or b_a1 is None or a_a0 is None or a_a1 is None:
                return None, None

            # Interpretation 0: element0 is price
            bid0 = b_a0
            ask0 = a_a0
            mark0 = self._mark_from_bid_ask(bid0, ask0)

            # Interpretation 1: element1 is price
            bid1 = b_a1
            ask1 = a_a1
            mark1 = self._mark_from_bid_ask(bid1, ask1)

            return mark0, mark1
        except Exception:
            return None, None

    def _choose_fx_mark(
        self,
        *,
        base_ccy: str,
        quote_ccy: str,
        mark0: Optional[Decimal],
        mark1: Optional[Decimal],
        inverted_pair: bool,
    ) -> Optional[Decimal]:
        """
        Pick the more plausible FX mark between two interpretations.
        For BTC/USD-like pairs, correct price is large; for USD/BTC-like pairs (inverted_pair=True),
        correct price is small (BTC per USD).
        """
        m0 = mark0 if (mark0 is not None and mark0 > 0) else None
        m1 = mark1 if (mark1 is not None and mark1 > 0) else None
        if m0 is None and m1 is None:
            return None
        if m1 is None:
            return m0
        if m0 is None:
            return m1

        b = (base_ccy or "").upper().strip()
        q = (quote_ccy or "").upper().strip()

        majors = {"BTC", "ETH"}
        usd_like = self._USD_EQ_QUOTES

        # If we are looking at USD->COIN instruments (inverted_pair), price should be small (coin per USD).
        if inverted_pair and b in usd_like and q in majors:
            return m0 if m0 < m1 else m1

        # If we are looking at COIN->USD instruments (normal), price should be large for majors.
        if (not inverted_pair) and b in majors and q in usd_like:
            # Choose the one that is clearly "large"
            if (m0 >= Decimal("10")) != (m1 >= Decimal("10")):
                return m0 if m0 >= Decimal("10") else m1
            return m0 if m0 > m1 else m1

        # Generic fallback:
        # Prefer the interpretation that is not suspiciously ~1 when base is a major.
        if b in majors:
            near1_0 = abs(m0 - Decimal("1")) < Decimal("0.01")
            near1_1 = abs(m1 - Decimal("1")) < Decimal("0.01")
            if near1_0 != near1_1:
                return m1 if near1_0 else m0
            return m0 if m0 > m1 else m1

        # Otherwise keep existing bias toward mark0 (documented form).
        return m0

    def _quote_to_usd_rate(self, quote_ccy: str) -> Tuple[Optional[Decimal], Optional[str], bool]:
        """
        Return USD conversion rate for 1 unit of quote_ccy (e.g., BTC->USD).
        Returns: (rate, instrument_symbol_used, inverted_flag)

        Hardened: for FX instruments, do NOT rely on tick-based book parsing;
        compute two possible marks and select plausibly.
        """
        q = self._canon_asset(str(quote_ccy or "").strip()).upper()
        if not q:
            return None, None, False
        if q in self._USD_EQ_QUOTES:
            return Decimal("1"), None, False

        # 1) Preferred: QUOTE -> USD(ish)
        inst_fx = self._find_quote_usd_instrument(q)
        if inst_fx:
            mark0, mark1 = self._book1_mark_dual(inst_fx)
            picked = self._choose_fx_mark(
                base_ccy=q,
                quote_ccy="USD",
                mark0=mark0,
                mark1=mark1,
                inverted_pair=False,
            )
            if picked is not None and picked > 0:
                return picked, inst_fx, False

        # 2) Fallback: USD(ish) -> QUOTE (invert)
        inst_inv = self._find_usd_quote_instrument(q)
        if inst_inv:
            mark0, mark1 = self._book1_mark_dual(inst_inv)

            # If either interpretation looks like a real BTCUSD/ETHUSD price (very large),
            # then this is NOT an inverted USD->COIN style book; treat it as quote->USD directly.
            m0 = mark0 if (mark0 is not None and mark0 > 0) else None
            m1 = mark1 if (mark1 is not None and mark1 > 0) else None
            mmax = None
            try:
                mmax = max([x for x in (m0, m1) if x is not None])
            except Exception:
                mmax = None

            if q in ("BTC", "ETH") and mmax is not None and mmax > Decimal("100"):
                return mmax, inst_inv, False  # treat as direct quote->USD

            picked = self._choose_fx_mark(
                base_ccy="USD",
                quote_ccy=q,
                mark0=mark0,
                mark1=mark1,
                inverted_pair=True,
            )
            if picked is not None and picked > 0:
                try:
                    return (Decimal("1") / picked), inst_inv, True
                except Exception:
                    return None, inst_inv, True

        # 3) Brute-force common naming variants
        candidates: List[Tuple[str, bool]] = []
        for usd in ("USD", "USDT", "USDC"):
            candidates.extend([
                (f"{q}-{usd}", False),
                (f"{q}_{usd}", False),
                (f"{q}{usd}", False),
                (f"{usd}-{q}", True),
                (f"{usd}_{q}", True),
                (f"{usd}{q}", True),
            ])

        seen: Set[str] = set()
        for sym, inv in candidates:
            s = sym.strip()
            if not s or s in seen:
                continue
            seen.add(s)

            mark0, mark1 = self._book1_mark_dual(s)
            picked = self._choose_fx_mark(
                base_ccy=("USD" if inv else q),
                quote_ccy=(q if inv else "USD"),
                mark0=mark0,
                mark1=mark1,
                inverted_pair=inv,
            )
            if picked is None or picked <= 0:
                continue

            if inv:
                try:
                    return (Decimal("1") / picked), s, True
                except Exception:
                    continue
            else:
                return picked, s, False

        return None, None, False

    def get_order_rules(self, symbol_venue: str) -> OrderRules:
        sv = (symbol_venue or "").strip()
        if not sv:
            return {"symbol_venue": symbol_venue}

        inst = self._find_instrument(sv) or {}

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

        qty_tick = f(inst.get("qty_tick_size"))
        px_tick = f(inst.get("price_tick_size"))

        qty_decimals = None
        price_decimals = None
        try:
            if inst.get("quantity_decimals") is not None:
                qty_decimals = int(str(inst.get("quantity_decimals")).strip())
        except Exception:
            qty_decimals = None
        try:
            if inst.get("quote_decimals") is not None:
                price_decimals = int(str(inst.get("quote_decimals")).strip())
        except Exception:
            price_decimals = None

        base_ccy = self._canon_asset(str(inst.get("base_ccy") or "").strip()).upper()
        quote_ccy = self._canon_asset(str(inst.get("quote_ccy") or "").strip()).upper()

        # Risk params are expressed as USD notional per BASE currency and apply regardless of quote currency.
        mn_usd: Optional[Decimal] = None
        mx_usd: Optional[Decimal] = None
        if base_ccy:
            mn_usd, mx_usd = self._risk_for_base(base_ccy)
            if mn_usd is None or mn_usd <= 0:
                mn_usd = self._MIN_NOTIONAL_USD_FALLBACK

        # NOTE: min/max notional are published in QUOTE units for UI checks (qty * limit_price).
        # We compute the final published values below after quote->USD discovery.
        min_notional_out: Optional[float] = None
        max_notional_out: Optional[float] = None

        # HARD MIN QTY POLICY (Crypto.com):
        # Do NOT trust instrument-provided "min_quantity" style fields (they can be nonsensical).
        # Use only tick-size as the hard minimum. The true minimum is enforced via USD min-notional.
        qstep_dec: Optional[Decimal] = self._dec(qty_tick) if (qty_tick is not None and qty_tick > 0) else None
        min_qty_hard_dec: Optional[Decimal] = qstep_dec if (qstep_dec is not None and qstep_dec > 0) else None

        # Compute effective min qty from USD-min-notional (works for USD and non-USD quotes).
        eff_min_qty_dec: Optional[Decimal] = None
        min_qty_out: Optional[float] = float(min_qty_hard_dec) if (min_qty_hard_dec is not None) else None

        # We also expose min_quote_notional (in QUOTE units)
        min_quote_notional: Optional[Decimal] = None

        # Debug: quote->USD source
        q_usd: Optional[Decimal] = None
        q_usd_inst: Optional[str] = None
        q_usd_inverted: bool = False

        try:
            if mn_usd is not None and mn_usd > 0 and quote_ccy:
                q_usd, q_usd_inst, q_usd_inverted = self._quote_to_usd_rate(quote_ccy)
                if q_usd is not None and q_usd > 0:
                    # QUOTE amount equivalent to the USD min notional
                    min_quote_notional = (mn_usd / q_usd)

                if qstep_dec is not None and qstep_dec > 0:
                    bid, ask = self._get_top_of_book(sv)
                    mark_quote = self._mark_from_bid_ask(bid, ask)

                    # mark_quote is BASE priced in QUOTE (e.g., DOGE priced in BTC)
                    # q_usd is QUOTE priced in USD (e.g., BTCUSD)
                    # so BASEUSD = mark_quote * q_usd
                    if mark_quote is not None and mark_quote > 0 and q_usd is not None and q_usd > 0:
                        px_usd = mark_quote * q_usd  # USD per 1 BASE
                        if px_usd > 0:
                            req = (mn_usd / px_usd)  # BASE qty needed to satisfy min USD
                            req2 = self._ceil_to_step(req, qstep_dec)
                            eff_min_qty_dec = req2

                            # final published min qty is max(hard_tick, req)
                            if min_qty_hard_dec is not None and min_qty_hard_dec > 0:
                                eff_final = req2 if req2 > min_qty_hard_dec else min_qty_hard_dec
                            else:
                                eff_final = req2
                            min_qty_out = float(eff_final)
        except Exception:
            eff_min_qty_dec = None

        # Publish notional in QUOTE units for UI checks: (qty * limit_price) >= min_notional.
        # Default to USD notionals; override when quote->USD conversion succeeded.
        if mn_usd is not None:
            min_notional_out = float(mn_usd)
        if mx_usd is not None:
            max_notional_out = float(mx_usd)

        # If quote is non-USD and we computed the QUOTE-equivalent notional, publish that.
        # (This is critical for pairs like DOGE-BTC where UI enforces quote notional.)
        if (
            quote_ccy
            and quote_ccy not in self._USD_EQ_QUOTES
            and min_quote_notional is not None
            and min_quote_notional > 0
        ):
            min_notional_out = float(min_quote_notional)
            if mx_usd is not None and q_usd is not None and q_usd > 0:
                try:
                    max_notional_out = float(mx_usd / q_usd)
                except Exception:
                    pass

        raw_out = inst if isinstance(inst, dict) else {}
        try:
            raw_out = dict(raw_out)

            raw_out["min_qty_hard"] = float(min_qty_hard_dec) if min_qty_hard_dec is not None else None
            raw_out["min_qty_effective_usd"] = float(eff_min_qty_dec) if eff_min_qty_dec is not None else None
            raw_out["min_notional_usd"] = float(mn_usd) if mn_usd is not None else None
            raw_out["min_quote_notional"] = float(min_quote_notional) if min_quote_notional is not None else None
            raw_out["min_quote_notional_ccy"] = quote_ccy or None

            raw_out["quote_usd_rate"] = float(q_usd) if q_usd is not None else None
            raw_out["quote_usd_instrument"] = q_usd_inst
            raw_out["quote_usd_instrument_inverted"] = bool(q_usd_inverted)

            try:
                bid, ask = self._get_top_of_book(sv)
                mk = self._mark_from_bid_ask(bid, ask)
                raw_out["book_bid_px"] = float(bid) if bid is not None else None
                raw_out["book_ask_px"] = float(ask) if ask is not None else None
                raw_out["book_mark_px"] = float(mk) if mk is not None else None
            except Exception:
                pass
        except Exception:
            pass

        return {
            "symbol_venue": sv,
            "base_increment": qty_tick,
            "price_increment": px_tick,
            "qty_decimals": qty_decimals,
            "price_decimals": price_decimals,
            "min_qty": min_qty_out,
            "max_qty": None,
            "min_notional": min_notional_out,
            "max_notional": max_notional_out,
            "supports_post_only": True,
            "supported_tifs": ["gtc", "ioc", "fok"],
            "supported_order_types": ["limit", "market"],
            "raw": raw_out,
        }

    def _normalize_status(self, st: Optional[str]) -> str:
        s = str(st or "").strip().upper()
        if not s:
            return "acked"
        if s in ("PENDING", "ACTIVE", "OPEN", "NEW", "WORKING", "LIVE"):
            return "open"
        if s in ("PARTIALLY_FILLED", "PARTIAL_FILL", "PARTIAL"):
            return "partial"
        if s in ("FILLED", "CLOSED", "DONE", "COMPLETE", "COMPLETED"):
            return "filled"
        if s in ("CANCELED", "CANCELLED", "CANCEL"):
            return "canceled"
        if s in ("REJECTED", "FAILED"):
            return "rejected"
        if s in ("EXPIRED",):
            return "expired"
        return "open" if "PART" in s else "acked"

    def normalize_order_status(self, st: Optional[str]) -> str:
        return self._normalize_status(st)

    def _map_side(self, side: Optional[str]) -> Optional[str]:
        s = str(side or "").strip().lower()
        if s in ("buy", "b"):
            return "buy"
        if s in ("sell", "s"):
            return "sell"
        return s or None

    def _map_type(self, t: Optional[str]) -> Optional[str]:
        x = str(t or "").strip().lower()
        if x in ("limit", "market"):
            return x
        if x == "limit_order":
            return "limit"
        if x == "market_order":
            return "market"
        return x or None

    def _map_tif_to_api(self, tif: Optional[str]) -> str:
        t = str(tif or "").strip().lower()
        if t in ("ioc", "immediate_or_cancel", "immediate-or-cancel"):
            return "IMMEDIATE_OR_CANCEL"
        if t in ("fok", "fill_or_kill", "fill-or-kill"):
            return "FILL_OR_KILL"
        return "GOOD_TILL_CANCEL"

    # ─────────────────────────────────────────────────────────────
    # Balances (private)
    # ─────────────────────────────────────────────────────────────
    def fetch_balances(self, dry_run: bool) -> List[BalanceItem]:
        resp = self._private_post("private/user-balance", params={})
        result = resp.get("result") or {}
        data = result.get("data") or []
        if not isinstance(data, list):
            return []

        out: List[BalanceItem] = []

        for acct in data:
            if not isinstance(acct, dict):
                continue
            pbs = acct.get("position_balances") or []
            if not isinstance(pbs, list):
                continue

            for pb in pbs:
                if not isinstance(pb, dict):
                    continue
                asset = self._canon_asset(str(pb.get("instrument_name") or "").strip()).upper()
                if not asset:
                    continue

                total = self._safe_float(pb.get("quantity"))
                hold = self._safe_float(pb.get("reserved_qty"))
                avail = self._safe_float(pb.get("max_withdrawal_balance"))

                if avail is None and (total is not None) and (hold is not None):
                    avail = float(max(0.0, float(total) - float(hold)))

                out.append(
                    {
                        "asset": asset,
                        "total": float(total or 0.0),
                        "available": float(avail or 0.0),
                        "hold": float(hold or 0.0),
                    }
                )

        agg: Dict[str, Dict[str, float]] = {}
        for b in out:
            a = str(b.get("asset") or "")
            if not a:
                continue
            if a not in agg:
                agg[a] = {"total": 0.0, "available": 0.0, "hold": 0.0}
            agg[a]["total"] += float(b.get("total") or 0.0)
            agg[a]["available"] += float(b.get("available") or 0.0)
            agg[a]["hold"] += float(b.get("hold") or 0.0)

        out2: List[BalanceItem] = []
        for a, v in agg.items():
            out2.append({"asset": a, "total": v["total"], "available": v["available"], "hold": v["hold"]})

        out2.sort(key=lambda x: str(x.get("asset") or ""))
        return out2

    # ─────────────────────────────────────────────────────────────
    # Orders (private): open + optional history (cached)
    # ─────────────────────────────────────────────────────────────

    def _extract_result_rows(self, resp: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not isinstance(resp, dict):
            return []
        res = resp.get("result") or {}
        for k in ("data", "order_list", "orders"):
            rows = res.get(k)
            if isinstance(rows, list):
                return [r for r in rows if isinstance(r, dict)]
        data = res.get("data")
        if isinstance(data, dict):
            for k in ("order_list", "orders", "data"):
                rows = data.get(k)
                if isinstance(rows, list):
                    return [r for r in rows if isinstance(r, dict)]
        return []

    def _use_advanced(self) -> bool:
        return self._env_bool("CRYPTOCOM_USE_ADVANCED", CryptoComExchangeAdapter._use_advanced_default)

    def _get_open_orders(self) -> List[Dict[str, Any]]:
        use_adv = self._use_advanced()

        try:
            resp = self._private_post("private/get-open-orders", params={})
            rows = self._extract_result_rows(resp)
            if rows:
                return rows
        except Exception as e:
            logger.warning("Crypto.com legacy get-open-orders failed: %s", e)

        if use_adv:
            try:
                resp = self._private_post("private/advanced/get-open-orders", params={})
                rows = self._extract_result_rows(resp)
                if rows:
                    return rows
            except Exception as e:
                logger.warning("Crypto.com advanced get-open-orders failed: %s", e)

        try:
            now = time.time()
            cutoff = now - float(CryptoComExchangeAdapter._recent_instruments_ttl_s)
            insts = [k for (k, ts) in CryptoComExchangeAdapter._recent_instruments.items() if float(ts) >= cutoff]
            insts = sorted(set(i for i in insts if i))[:50]
        except Exception:
            insts = []

        if insts:
            acc: List[Dict[str, Any]] = []
            for inst in insts:
                try:
                    resp = self._private_post("private/get-open-orders", params={"instrument_name": inst})
                    acc.extend(self._extract_result_rows(resp))
                except Exception:
                    pass

                if use_adv:
                    try:
                        resp = self._private_post("private/advanced/get-open-orders", params={"instrument_name": inst})
                        acc.extend(self._extract_result_rows(resp))
                    except Exception:
                        pass

            merged: Dict[str, Dict[str, Any]] = {}
            for r in acc:
                oid = str(self._get_field(r, "order_id", "orderId", "id") or "").strip()
                if oid and oid not in merged:
                    merged[oid] = r
            return list(merged.values())

        return []

    def _get_open_orders_cached(self) -> List[Dict[str, Any]]:
        ttl = clamp_int(os.getenv("CRYPTOCOM_OPEN_ORDERS_CACHE_TTL_S", "5"), 0, 600)
        key = "open_orders:v3"
        if ttl > 0:
            cached = self._cache_get(key)
            if cached is not None:
                return cached

        rows = self._get_open_orders()
        if ttl > 0:
            self._cache_set(key, rows, ttl_s=ttl)
        return rows

    def _get_order_history_cached(
        self,
        instrument_name: Optional[str],
        *,
        lookback_ms: int,
        limit: int,
        bypass_cache: bool = False,
    ) -> List[Dict[str, Any]]:
        ttl = 0 if bypass_cache else clamp_int(os.getenv("CRYPTOCOM_ORDER_HISTORY_CACHE_TTL_S", "30"), 0, 3600)

        inst_key = (instrument_name or "").strip()

        limit = clamp_int(limit, 1, 100)
        lookback_max_ms = clamp_int(
            os.getenv("CRYPTOCOM_ORDER_HISTORY_LOOKBACK_MAX_MS", str(180 * 24 * 60 * 60 * 1000)),
            60_000,
            3650 * 24 * 60 * 60 * 1000,
        )
        lookback_ms = clamp_int(lookback_ms, 60_000, lookback_max_ms)

        key = f"order_history:{inst_key}:{lookback_ms}:{limit}"
        if ttl > 0:
            cached = self._cache_get(key)
            if cached is not None:
                return cached

        end_ms = int(time.time() * 1000)
        start_ms = int(end_ms - lookback_ms)

        end_ns = int(end_ms * 1_000_000)
        start_ns = int(start_ms * 1_000_000)

        use_adv = self._use_advanced()

        rows: List[Dict[str, Any]] = []

        params_legacy: Dict[str, Any] = {
            # IMPORTANT:
            # Docs currently allow numeric or string, but many JSON stacks lose precision on 18–19 digit ints.
            # Send ns timestamps as STRINGS to avoid any 53-bit float rounding issues.
            "start_time": str(int(start_ns)),
            "end_time": str(int(end_ns)),
            "limit": int(limit),
        }
        if inst_key:
            params_legacy["instrument_name"] = inst_key

        try:
            resp = self._private_post("private/get-order-history", params=params_legacy)
            rows = self._extract_result_rows(resp)
        except Exception as e2:
            logger.warning("Crypto.com legacy get-order-history failed: %s", e2)
            rows = []

        # Compatibility fallback:
        # Some deployments / older behaviors have been observed using ms for start_time/end_time.
        # If the ns call returns empty, optionally try ms once (no extra cost once ns works).
        if (not rows) and self._env_bool("CRYPTOCOM_ORDER_HISTORY_TRY_MS_FALLBACK", True):
            params_legacy_ms: Dict[str, Any] = {
                "start_time": int(start_ms),
                "end_time": int(end_ms),
                "limit": int(limit),
            }
            if inst_key:
                params_legacy_ms["instrument_name"] = inst_key
            try:
                resp = self._private_post("private/get-order-history", params=params_legacy_ms)
                rows = self._extract_result_rows(resp) or []
            except Exception:
                # silent; the ns attempt already logged any hard failure
                rows = rows or []
        try_adv = bool(use_adv) or self._env_bool("CRYPTOCOM_TRY_ADV_HISTORY", False)
        if not rows and try_adv:
            params_adv: Dict[str, Any] = {
                "start_time": str(int(start_ns)),
                "end_time": str(int(end_ns)),
                "limit": int(limit),
            }
            if inst_key:
                params_adv["instrument_name"] = inst_key
            try:
                resp = self._private_post("private/advanced/get-order-history", params=params_adv)
                rows = self._extract_result_rows(resp)
            except Exception as e:
                logger.warning("Crypto.com advanced get-order-history failed: %s", e)
                rows = rows or []

        if ttl > 0:
            self._cache_set(key, rows, ttl_s=ttl)
        return rows

    # --------------------------------------------------------------------------
    # Trades fallback (FILLED recovery)
    # --------------------------------------------------------------------------
    def _get_trades_cached(
        self,
        instrument_name: Optional[str],
        lookback_ms: int,
        limit: int,
        bypass_cache: bool,
    ) -> List[Dict[str, Any]]:
        """Fetch recent trades (executions). If instrument_name is None, returns across all instruments."""
        ttl = clamp_int(os.getenv("CRYPTOCOM_TRADES_CACHE_TTL_S", "5"), 0, 3600)
        if bypass_cache:
            ttl = 0

        limit = clamp_int(int(limit), 1, 100)  # docs: max 100

        now_ms = int(time.time() * 1000)
        start_ms = now_ms - int(lookback_ms)
        end_ms = now_ms

        inst_key = instrument_name if instrument_name else "ALL"
        key = f"cryptocom:trades:{inst_key}:{start_ms}:{end_ms}:{int(limit)}"
        if ttl > 0:
            hit = self._cache_get(key)
            if hit is not None:
                return hit

        # Exchange docs recommend nanosecond unix timestamps; send as strings to avoid any downstream precision issues.
        start_ns = str(int(start_ms) * 1_000_000)
        end_ns = str(int(end_ms) * 1_000_000)

        params: Dict[str, Any] = {
            "start_time": start_ns,
            "end_time": end_ns,
            "limit": int(limit),
        }
        if instrument_name:
            params["instrument_name"] = instrument_name

        resp = self._private_post("private/get-trades", params=params)
        rows = self._extract_result_rows(resp)

        if ttl > 0:
            self._cache_set(key, rows, ttl_s=ttl)
        return rows
    def _filled_orders_from_trades(self, trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Aggregate trades into FILLED order-like rows consumable by _map_order_row()."""
        by_oid: Dict[str, Dict[str, Any]] = {}

        def fnum(x: Any) -> float:
            try:
                return float(x)
            except Exception:
                return 0.0

        for t in trades or []:
            oid = str((t or {}).get("order_id") or "").strip()
            if not oid:
                continue

            inst = str((t or {}).get("instrument_name") or "").strip()
            side = str((t or {}).get("side") or "").strip().upper()
            px = fnum((t or {}).get("price") or (t or {}).get("trade_price") or (t or {}).get("traded_price") or (t or {}).get("execution_price"))
            qty = fnum((t or {}).get("quantity") or (t or {}).get("qty") or (t or {}).get("traded_quantity") or (t or {}).get("traded_qty") or (t or {}).get("amount") or (t or {}).get("executed_quantity"))
            fee = fnum((t or {}).get("fee"))
            fee_ccy = str((t or {}).get("fee_instrument_name") or "").strip()

            # get-trades uses ms timestamps (schema varies a bit across API versions)
            ts_ms = (t or {}).get("create_time") or (t or {}).get("time") or (t or {}).get("timestamp") or (t or {}).get("ts")
            try:
                ts_ms_i = int(float(ts_ms))
            except Exception:
                ts_ms_i = 0

            r = by_oid.get(oid)
            if r is None:
                r = {
                    "order_id": oid,
                    "instrument_name": inst,
                    "side": side,
                    "order_type": "MARKET",
                    "status": "FILLED",
                    "create_time": ts_ms_i,
                    "update_time": ts_ms_i,
                    "quantity": 0.0,
                    "cumulative_quantity": 0.0,
                    "avg_price": 0.0,
                    "cumulative_fee": 0.0,
                    "fee_instrument_name": fee_ccy,
                    "cumulative_value": 0.0,
                }
                by_oid[oid] = r

            r["cumulative_quantity"] = float(r.get("cumulative_quantity", 0.0)) + qty
            # best guess for original order qty: at least the filled qty
            r["quantity"] = max(float(r.get("quantity", 0.0)), float(r["cumulative_quantity"]))
            r["cumulative_fee"] = float(r.get("cumulative_fee", 0.0)) + fee
            r["cumulative_value"] = float(r.get("cumulative_value", 0.0)) + (px * qty)

            cq = float(r.get("cumulative_quantity", 0.0))
            if cq > 0:
                r["avg_price"] = float(r.get("cumulative_value", 0.0)) / cq

            # earliest/latest
            ct = int(r.get("create_time") or 0)
            ut = int(r.get("update_time") or 0)
            if ct == 0 or (ts_ms_i and ts_ms_i < ct):
                r["create_time"] = ts_ms_i
            if ts_ms_i and ts_ms_i > ut:
                r["update_time"] = ts_ms_i

            if not r.get("fee_instrument_name") and fee_ccy:
                r["fee_instrument_name"] = fee_ccy

        rows = list(by_oid.values())
        # Defensive: if the trades schema changes or a trade row is missing qty/price,
        # we can end up synthesizing "filled" orders with zero/null execution fields.
        # Drop any aggregated rows that have no executed quantity.
        rows = [r for r in rows if float(r.get("cumulative_quantity") or 0) > 0]
        return rows

    def fetch_orders(self, dry_run: bool = False) -> List[VenueOrder]:
        # NOTE: This venue requires pulling *history* to see FILLED/CANCELED orders.
        # Relying on open-orders only will stall ingestion whenever you have no open orders.
        hist_limit_env = os.getenv("CRYPTOCOM_ORDER_HISTORY_LIMIT", "100")
        hist_limit = clamp_int(hist_limit_env, 1, 100)

        # History include: default ON unless explicitly disabled.
        # Support both env names (older docs sometimes used CRYPTOCOM_INCLUDE_HISTORY).
        if os.getenv("CRYPTOCOM_INCLUDE_ORDER_HISTORY") is not None:
            include_history = self._env_bool("CRYPTOCOM_INCLUDE_ORDER_HISTORY", True)
        elif os.getenv("CRYPTOCOM_INCLUDE_HISTORY") is not None:
            include_history = self._env_bool("CRYPTOCOM_INCLUDE_HISTORY", True)
        else:
            include_history = True

        lookback_max_ms = clamp_int(
            os.getenv("CRYPTOCOM_ORDER_HISTORY_LOOKBACK_MAX_MS", str(180 * 24 * 60 * 60 * 1000)),
            60_000,
            3650 * 24 * 60 * 60 * 1000,
        )

        lookback_ms = clamp_int(
            os.getenv(
                "CRYPTOCOM_ORDER_HISTORY_LOOKBACK_MS",
                os.getenv("CRYPTOCOM_ORDERS_LOOKBACK_MS", str(7 * 24 * 60 * 60 * 1000)),
            ),
            60_000,
            lookback_max_ms,
        )

        open_rows = self._get_open_orders_cached()

        # Instruments seen in the open snapshot (best hint if we need per-instrument history)
        insts: Set[str] = set()
        for r in open_rows:
            if not isinstance(r, dict):
                continue
            inst = str(self._get_field(r, "instrument_name", "instrumentName", "instrument") or "").strip()
            if inst:
                insts.add(inst)
                try:
                    self._mark_recent_instrument(inst)
                except Exception:
                    pass

        hist_rows: List[Dict[str, Any]] = []
        scan_all = self._env_bool("CRYPTOCOM_ORDER_HISTORY_SCAN_ALL", False)
        scanned_insts: List[str] = []
        trades_fallback_used = 0
        trades_fallback_max = clamp_int(os.getenv("CRYPTOCOM_TRADES_FALLBACK_MAX_INSTRUMENTS_PER_REFRESH", "5"), 0, 500)

        if include_history:
            # Controls
            bypass_cache = self._env_bool("CRYPTOCOM_ORDER_HISTORY_BYPASS_CACHE", False)
            require_inst = self._env_bool("CRYPTOCOM_ORDER_HISTORY_REQUIRE_INSTRUMENT", False)
            debug = self._env_bool("CRYPTOCOM_DEBUG_ORDERS", False)

            # First try: unfiltered history (if supported by your account/key).
            if not require_inst:
                try:
                    hist_rows = self._get_order_history_cached(
                        None,
                        lookback_ms=lookback_ms,
                        limit=hist_limit,
                        bypass_cache=bypass_cache,
                    )
                except Exception as e:
                    logger.warning("Crypto.com unfiltered get-order-history failed: %s", e)
                    hist_rows = []

            # If order-history returns empty for this key/account, recover FILLED orders via trades.
            # This is a single call (instrument_name omitted) and avoids per-instrument hammering.
            if (not hist_rows) and self._env_bool("CRYPTOCOM_FALLBACK_TRADES", True):
                try:
                    trades_lookback_ms = clamp_int(
                        os.getenv("CRYPTOCOM_TRADES_LOOKBACK_MS", str(lookback_ms)),
                        60_000,
                        lookback_max_ms,
                    )
                    trades_all = self._get_trades_cached(
                        None,
                        lookback_ms=trades_lookback_ms,
                        limit=min(hist_limit * 2, 100),
                        bypass_cache=bypass_cache,
                    )
                    filled_all = self._filled_orders_from_trades(trades_all)
                    if filled_all:
                        hist_rows.extend(filled_all)
                        if debug:
                            logger.info("Crypto.com trades-derived fills: %d", len(filled_all))
                        # Seed the recent-instruments cache from trades so future per-instrument calls are smarter.
                        try:
                            for tr in trades_all:
                                instn = tr.get("instrument_name")
                                if isinstance(instn, str) and instn:
                                    self._mark_recent_instrument(instn)
                        except Exception:
                            pass
                except Exception as e:
                    if debug:
                        logger.warning("Crypto.com unfiltered get-trades fallback failed: %s", e)

            # Fallback: per-instrument history.
            # If we couldn't obtain any history rows from unfiltered history or unfiltered trades,
            # try per-instrument history using hint instruments (open orders + recent cache + optional env list).
            if not hist_rows:
                scan_batch = int(self._env_int("CRYPTOCOM_ORDER_HISTORY_SCAN_BATCH", 50))
                max_insts_per_refresh = int(self._env_int("CRYPTOCOM_ORDER_HISTORY_MAX_INSTRUMENTS_PER_REFRESH", 80))
                max_calls = int(self._env_int("CRYPTOCOM_ORDER_HISTORY_MAX_CALLS_PER_REFRESH", 50))

                scan_batch = max(1, min(scan_batch, 500))
                max_insts_per_refresh = max(1, min(max_insts_per_refresh, 500))
                max_calls = max(1, min(max_calls, 500))

                # Build hint instrument set.
                hint_set: Set[str] = set(insts)

                # Include a TTL-based "recent instruments" cache (helps catch venue-only fills after open orders are gone).
                recent_items: List[Tuple[int, str]] = []
                try:
                    ttl_s = int(self._env_int("CRYPTOCOM_RECENT_INSTRUMENTS_TTL_S", 6 * 3600))
                except Exception:
                    ttl_s = 6 * 3600
                now_s = int(time.time())
                cutoff_s = now_s - max(0, ttl_s)
                for k, ts in list(self._recent_instruments.items()):
                    if ts >= cutoff_s:
                        recent_items.append((int(ts), k))
                recent_items.sort(reverse=True)
                for _, k in recent_items[:500]:
                    hint_set.add(k)

                # Optional: allow explicit instrument hints via env (comma-separated).
                env_list: List[str] = []
                for env_key in ("CRYPTOCOM_ORDER_HISTORY_INSTRUMENTS", "CRYPTOCOM_HISTORY_INSTRUMENTS", "CRYPTOCOM_INSTRUMENTS"):
                    raw = (os.getenv(env_key) or "").strip()
                    if raw:
                        env_list.extend([x.strip() for x in raw.split(",") if x.strip()])
                for k in env_list:
                    hint_set.add(k)

                # If we have no hints and scan_all is enabled, round-robin through the full instrument list in batches.
                if scan_all and not hint_set:
                    try:
                        all_defs = self._get_instruments()
                        all_names = [self._inst_id(d) for d in (all_defs or [])]
                        all_names = [n for n in all_names if n]
                    except Exception:
                        all_names = []

                    if all_names:
                        start_idx = int(self._history_scan_cursor) % len(all_names)
                        ordered_scan: List[str] = []
                        for i in range(len(all_names)):
                            ordered_scan.append(all_names[(start_idx + i) % len(all_names)])
                            if len(ordered_scan) >= max_insts_per_refresh:
                                break
                        self._history_scan_cursor = (start_idx + len(ordered_scan)) % len(all_names)
                        hint_set.update(ordered_scan)
                        scanned_insts = ordered_scan[:]

                # Deterministic ordering: open instruments first, then recent, then env list, then the rest alpha.
                ordered_hints: List[str] = []
                for k in insts:
                    if k in hint_set and k not in ordered_hints:
                        ordered_hints.append(k)
                for _, k in recent_items:
                    if k in hint_set and k not in ordered_hints:
                        ordered_hints.append(k)
                for k in env_list:
                    if k in hint_set and k not in ordered_hints:
                        ordered_hints.append(k)
                for k in sorted(hint_set):
                    if k not in ordered_hints:
                        ordered_hints.append(k)

                ordered_hints = ordered_hints[:max_insts_per_refresh]

                calls = 0
                for inst_key in ordered_hints:
                    if calls >= max_calls:
                        break
                    # Correct signature. The old positional call here broke per-instrument history recovery.
                    rows = self._get_order_history_cached(
                        inst_key,
                        lookback_ms=lookback_ms,
                        limit=hist_limit,
                        bypass_cache=bypass_cache,
                    )
                    calls += 1
                    if rows:
                        hist_rows.extend(rows)

            if self._env_bool("CRYPTOCOM_DEBUG_ORDERS", False):
                try:
                    logger.info(
                        "Crypto.com fetch_orders debug: include_history=%s require_inst=%s bypass_cache=%s open_insts=%d hist_rows=%d scan_all=%s scanned=%d lookback_ms=%s limit=%s",
                        bool(include_history),
                        bool(require_inst),
                        bool(bypass_cache),
                        len(insts),
                        len(hist_rows),
                        bool(self._env_bool("CRYPTOCOM_ORDER_HISTORY_SCAN_ALL", True)),
                        len(scanned_insts),
                        int(lookback_ms),
                        int(hist_limit),
                    )
                except Exception:
                    pass

        terminal = {"canceled", "filled", "rejected", "expired"}

        def oid(row: Dict[str, Any]) -> str:
            return str(self._get_field(row, "order_id", "orderId", "id") or "").strip()

        merged: Dict[str, Dict[str, Any]] = {}

        for r in open_rows:
            k = oid(r)
            if k:
                merged[k] = r

        for r in hist_rows:
            k = oid(r)
            if not k:
                continue
            st = self._normalize_status(str(self._get_field(r, "status", "order_status") or ""))
            if st in terminal:
                merged[k] = r
                continue
            if k not in merged:
                merged[k] = r

        out: List[VenueOrder] = []
        for r in merged.values():
            if not isinstance(r, dict):
                continue
            try:
                out.append(self._map_order_row(r))
            except Exception:
                continue

        return out
    def _canon_from_instrument(self, instrument_name: str) -> Optional[str]:
        inst = self._find_instrument(instrument_name)
        if not isinstance(inst, dict):
            return None
        base = self._canon_asset(str(inst.get("base_ccy") or "").strip()).upper()
        quote = self._canon_asset(str(inst.get("quote_ccy") or "").strip()).upper()
        if not base or not quote:
            return None
        return f"{base}-{quote}"

    def _map_order_row(self, r: Dict[str, Any]) -> VenueOrder:
        instrument_name = str(r.get("instrument_name") or r.get("instrumentName") or "").strip()
        symbol_venue = instrument_name or str(r.get("symbol") or "").strip()
        symbol_canon = self._canon_from_instrument(symbol_venue) if symbol_venue else None

        side = self._map_side(r.get("side"))
        typ_api = str(r.get("order_type") or r.get("type") or "").strip()
        typ = self._map_type(typ_api)

        qty = self._safe_float(r.get("quantity"))
        filled_qty = self._safe_float(r.get("cumulative_quantity") or r.get("filled_quantity") or r.get("filled_qty"))
        limit_price = self._safe_float(r.get("price") or r.get("limit_price"))
        avg_price = self._safe_float(r.get("avg_price") or r.get("avg_fill_price"))

        fee = self._safe_float(r.get("cumulative_fee") or r.get("fee"))
        fee_asset = str(r.get("fee_instrument_name") or r.get("fee_asset") or "").strip() or None

        st_raw = str(r.get("status") or r.get("order_status") or "").strip()
        status = self._normalize_status(st_raw)

        created_at = self._dt_from_ns(r.get("create_time_ns")) or self._dt_from_ms(r.get("create_time"))
        updated_at = self._dt_from_ms(r.get("update_time")) or self._dt_from_ns(r.get("update_time_ns")) or None

        total_after_fee = None
        cum_value = self._safe_float(r.get("cumulative_value") or r.get("filled_value") or r.get("notional"))
        if cum_value is not None:
            if fee is not None:
                if side == "buy":
                    total_after_fee = float(cum_value) + float(fee)
                elif side == "sell":
                    total_after_fee = float(cum_value) - float(fee)
            else:
                total_after_fee = float(cum_value)

        venue_order_id = str(r.get("order_id") or r.get("orderId") or r.get("id") or "").strip()

        return {
            "venue": self.venue,
            "venue_order_id": venue_order_id,
            "symbol_venue": symbol_venue,
            "symbol_canon": symbol_canon,
            "side": side,
            "type": typ,
            "status": status if status else "acked",
            "qty": qty,
            "filled_qty": filled_qty,
            "limit_price": limit_price if typ == "limit" else None,
            "avg_fill_price": avg_price if (avg_price is not None and avg_price > 0) else None,
            "fee": fee,
            "fee_asset": fee_asset,
            "total_after_fee": total_after_fee,
            "created_at": created_at,
            "updated_at": updated_at,
        }

    # ─────────────────────────────────────────────────────────────
    # Trading (REAL)
    # ─────────────────────────────────────────────────────────────
    def _apply_rules(
        self,
        *,
        instrument_name: str,
        side_l: str,
        type_l: str,
        qty: float,
        limit_price: Optional[float],
    ) -> Tuple[str, Optional[str]]:
        enforce_precision = self._env_bool("CRYPTOCOM_ENFORCE_PRECISION", True)

        qd = self._dec(qty)
        if qd is None or qd <= 0:
            raise Exception("Crypto.com place_order: qty must be > 0")

        pd = self._dec(limit_price) if limit_price is not None else None
        if type_l == "limit":
            if pd is None or pd <= 0:
                raise Exception("Crypto.com place_order: limit_price is required and must be > 0 for limit orders")

        rules = self.get_order_rules(instrument_name) or {}

        q2 = qd
        p2 = pd

        if enforce_precision:
            qstep = self._dec(rules.get("base_increment"))
            if qstep is not None and qstep > 0:
                q2 = self._floor_to_step(q2, qstep)

            pstep = self._dec(rules.get("price_increment"))
            if type_l == "limit" and p2 is not None and pstep is not None and pstep > 0:
                p2 = self._floor_to_step(p2, pstep)

        if q2 <= 0:
            raise Exception("Crypto.com place_order: qty rounds to 0 under current instrument rules")
        if type_l == "limit" and (p2 is None or p2 <= 0):
            raise Exception("Crypto.com place_order: limit_price rounds to 0 under current instrument rules")

        return (self._fmt_decimal(q2), self._fmt_decimal(p2) if p2 is not None else None)

    def _enforce_spot_guardrails(
        self,
        *,
        instrument_name: str,
        side_l: str,
        type_l: str,
        qty_str: str,
        price_str: Optional[str],
    ) -> None:
        qd = self._dec(qty_str)
        if qd is None or qd <= 0:
            raise Exception("Crypto.com place_order: invalid qty after rule application")

        rules = self.get_order_rules(instrument_name) or {}
        raw = (rules.get("raw") or {}) if isinstance(rules.get("raw"), dict) else {}

        # Enforce hard min qty (tick-size)
        try:
            min_qty_hard = self._dec(raw.get("min_qty_hard"))
            if min_qty_hard is None:
                min_qty_hard = self._dec(rules.get("min_qty"))
            if min_qty_hard is not None and min_qty_hard > 0 and qd < min_qty_hard:
                raise Exception(
                    f"Crypto.com minimum quantity check failed for {instrument_name}: "
                    f"qty={self._fmt_decimal(qd)} is below min_qty={self._fmt_decimal(min_qty_hard)}"
                )
        except Exception:
            pass

        base, quote = self._instrument_base_quote(instrument_name)
        quote_u = (quote or "").upper().strip()

        bid, ask = self._get_top_of_book(instrument_name)
        mark = self._mark_from_bid_ask(bid, ask)

        if self._ENFORCE_TRADING_BANDWIDTH and type_l == "limit":
            pd = self._dec(price_str) if price_str is not None else None
            if pd is None or pd <= 0:
                raise Exception("Crypto.com place_order: invalid limit price after rule application")

            if mark is not None and mark > 0:
                if side_l == "buy":
                    low = mark * self._TB_BUY_MIN_MULT
                    high = mark * (Decimal("1") + self._TB_PCT)
                else:
                    low = mark * (Decimal("1") - self._TB_PCT)
                    high = mark * self._TB_SELL_MAX_MULT

                if pd < low or pd > high:
                    raise Exception(
                        f"Crypto.com trading bandwidth check failed for {instrument_name}: "
                        f"limit_price={self._fmt_decimal(pd)} is outside allowed range "
                        f"[{self._fmt_decimal(low)}, {self._fmt_decimal(high)}] based on mark≈{self._fmt_decimal(mark)}"
                    )

        # Enforce USD min/max notional for all quotes (when conversion is available).
        if base:
            mn, mx = self._risk_for_base(base)
            if mn is None or mn <= 0:
                mn = self._MIN_NOTIONAL_USD_FALLBACK

            eff_px: Optional[Decimal] = None
            if type_l == "limit":
                pd = self._dec(price_str) if price_str is not None else None
                if pd is None or pd <= 0:
                    raise Exception("Crypto.com place_order: invalid limit price after rule application")
                eff_px = pd
            else:
                if side_l == "buy":
                    eff_px = ask if (ask is not None and ask > 0) else mark
                else:
                    eff_px = bid if (bid is not None and bid > 0) else mark

            if eff_px is None or eff_px <= 0:
                return

            if quote_u in self._USD_EQ_QUOTES:
                q_usd = Decimal("1")
                notional_usd = (qd * eff_px)
            else:
                q_usd, _inst, _inv = self._quote_to_usd_rate(quote_u)
                if q_usd is None or q_usd <= 0:
                    # If we cannot convert quote->USD, do not mis-reject.
                    return
                notional_usd = (qd * eff_px) * q_usd

            if self._ENFORCE_MIN_NOTIONAL and mn is not None and notional_usd < mn:
                qstep = self._dec(rules.get("base_increment"))
                req_qty = (mn / (eff_px * q_usd)) if (eff_px > 0 and q_usd > 0) else None
                if req_qty is not None and qstep is not None and qstep > 0:
                    req_qty = self._ceil_to_step(req_qty, qstep)

                hint = ""
                if req_qty is not None:
                    hint = (
                        f" (min_qty≈{self._fmt_decimal(req_qty)} at px≈{self._fmt_decimal(eff_px)} {quote_u}, "
                        f"{quote_u}USD≈{self._fmt_decimal(q_usd)})"
                    )

                raise Exception(
                    f"Crypto.com minimum order size check failed for {instrument_name}: "
                    f"notional≈{self._fmt_decimal(notional_usd)} USD is below minimum "
                    f"{self._fmt_decimal(mn)} USD{hint}"
                )
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
            return {"venue_order_id": f"dry-{client_order_id}", "status": "acked", "raw_status": "dry_run"}

        side_l = (side or "").lower().strip()
        if side_l not in ("buy", "sell"):
            raise Exception(f"Crypto.com place_order: invalid side '{side}' (expected buy/sell)")

        t_l = (type_ or "").lower().strip()
        if t_l not in ("limit", "market"):
            raise Exception(f"Crypto.com place_order: invalid type '{type_}' (expected limit/market)")

        inst_name = str(symbol_venue or "").strip()
        if not inst_name:
            raise Exception("Crypto.com place_order: missing symbol_venue/instrument_name")

        qty_str, price_str = self._apply_rules(
            instrument_name=inst_name,
            side_l=side_l,
            type_l=t_l,
            qty=qty,
            limit_price=limit_price,
        )

        self._enforce_spot_guardrails(
            instrument_name=inst_name,
            side_l=side_l,
            type_l=t_l,
            qty_str=qty_str,
            price_str=price_str,
        )

        params: Dict[str, Any] = {
            "instrument_name": inst_name,
            "side": side_l.upper(),
            "type": t_l.upper(),
            "quantity": qty_str,
            "client_oid": str(client_order_id),
            "time_in_force": self._map_tif_to_api(tif),
        }

        if t_l == "limit":
            if not price_str:
                raise Exception("Crypto.com place_order: missing price after rule application")
            params["price"] = price_str

        if post_only:
            params["exec_inst"] = ["POST_ONLY"]

        resp = self._private_post("private/create-order", params=params)
        result = resp.get("result") or {}
        oid = str(result.get("order_id") or "").strip()

        if not oid:
            raise Exception(f"Crypto.com create-order missing order_id. result={result}")

        try:
            self._mark_recent_instrument(inst_name)
        except Exception:
            pass

        raw_status = str(result.get("status") or "").strip() or "submitted"
        status = self._normalize_status(raw_status) or "open"
        if status not in ("open", "partial", "filled", "canceled", "rejected", "expired"):
            status = "open"

        return {"venue_order_id": oid, "status": status if status != "partial" else "open", "raw_status": raw_status}

    
    def cancel_order(self, venue_order_id: str, dry_run: bool) -> bool:
        if dry_run:
            return True
        oid = (venue_order_id or "").strip()
        if not oid:
            return False
        try:
            _ = self._private_post("private/cancel-order", params={"order_id": str(oid)})
            try:
                self._mark_recent_cancel(oid)
            except Exception:
                pass
            return True
        except Exception as e:
            # If venue says it’s already closed / not found / not cancelable, treat as success.
            msg = str(e).lower()
            if (
                "not found" in msg or "not_found" in msg or "40401" in msg
                or "already" in msg
                or "filled" in msg
                or ("cancel" in msg and "unable" in msg)
                or "not cancelable" in msg
            ):
                try:
                    self._mark_recent_cancel(oid)
                except Exception:
                    pass
                return True
            raise
    # ─────────────────────────────────────────────────────────────
    # Public order book
    # ─────────────────────────────────────────────────────────────
    def fetch_orderbook(self, symbol_venue: str, depth: int, dry_run: bool) -> OrderBook:
        inst_name = (symbol_venue or "").strip()
        if not inst_name:
            return {"bids": [], "asks": []}

        d = max(1, min(int(depth or 10), 50))

        inst = self._find_instrument(inst_name) or {}
        pstep = self._dec(inst.get("price_tick_size"))
        qstep = self._dec(inst.get("qty_tick_size"))

        data = self._public_get("/public/get-book", params={"instrument_name": inst_name, "depth": str(d)})
        res = (data.get("result") or {})
        arr = res.get("data") or []
        if not isinstance(arr, list) or not arr:
            return {"bids": [], "asks": []}

        row = arr[0] if isinstance(arr[0], dict) else {}
        bids_raw = row.get("bids") or []
        asks_raw = row.get("asks") or []

        bids: List[Dict[str, float]] = []
        if isinstance(bids_raw, list):
            for b in bids_raw[:d]:
                try:
                    p, q = self._parse_book_level(b, price_step=pstep, qty_step=qstep)
                    if p is None or q is None:
                        continue
                    bids.append({"price": float(p), "qty": float(q)})
                except Exception:
                    continue

        asks: List[Dict[str, float]] = []
        if isinstance(asks_raw, list):
            for a in asks_raw[:d]:
                try:
                    p, q = self._parse_book_level(a, price_step=pstep, qty_step=qstep)
                    if p is None or q is None:
                        continue
                    asks.append({"price": float(p), "qty": float(q)})
                except Exception:
                    continue

        return {"bids": bids, "asks": asks}