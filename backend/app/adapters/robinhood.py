# backend/app/adapters/robinhood.py

from __future__ import annotations

from typing import List, Optional, Dict, Any, Tuple, Iterable
from datetime import datetime, timezone
import os
import time
import json
import base64
import re
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from collections import deque
from urllib.parse import urlencode, quote, urlparse

import httpx

from .base import ExchangeAdapter, PlacedOrder, BalanceItem, OrderBook, VenueOrder, OrderRules
from ..config import settings


class RobinhoodAdapter(ExchangeAdapter):
    venue = "robinhood"

    # Known quote assets for heuristic splitting when RH returns "BTCUSD" style.
    # Order matters: longer first to avoid USD vs USDT confusion.
    _KNOWN_QUOTES = ("USDT", "USDC", "USD", "EUR", "GBP", "BTC", "ETH")

    # Raw terminal-ish states we may see from venues; used only as a hint.
    _TERMINAL_RAW = {
        "filled", "canceled", "cancelled", "rejected", "failed", "expired",
        "done", "closed", "completed", "executed", "error", "denied", "voided"
    }

    # If Robinhood rules discovery is incomplete for some symbols, we still want
    # correct UX and correct submission formatting for known constraints.
    #
    # Notes (from your live observations):
    # - BTC supports up to 8 decimals (step 1e-8)
    # - ETH supports up to 6 decimals (step 1e-6)
    # - DOGE requires 2 decimals (step 0.01) on BOTH buy/sell
    # - You observed you needed XRP/LTC handled like majors for quantity auto-calc.
    _BASE_INCREMENT_OVERRIDES: Dict[str, float] = {
        "BTC": 1e-8,   # 8 decimals
        "ETH": 1e-6,   # 6 decimals
        "DOGE": 0.01,  # 2 decimals (QTY)
        "XRP": 1e-3,   # keep your current behavior (you can change later if needed)
        "LTC": 1e-8,   # 8 decimals
    }

    # PRICE tick overrides (independent from quantity increment).
    # You requested DOGE orderbook prices display at 6 decimals.
    # This is applied for USD-quoted symbols unless RH rules provide something else.
    _PRICE_INCREMENT_OVERRIDES: Dict[str, float] = {
        "DOGE": 1e-6,  # 6 decimals (PRICE)
    }

    # Best-effort caching (avoid repeated signed calls during UI polling)
    _RULES_CACHE_TTL_S: int = 60
    _rules_cache: Dict[str, Tuple[float, OrderRules]] = {}

    # Best-bid/ask cache:
    # Robinhood sometimes returns 0/0 for certain symbols sporadically (DOGE is a common offender).
    # We keep a "fresh" TTL and a "stale-ok" TTL to avoid the UI snapping to 0/0.
    _OB_CACHE_TTL_S: int = 90          # must exceed your 30s UI refresh interval
    _OB_CACHE_STALE_OK_S: int = 600    # allow fallback to last-known-good for up to 10 minutes
    _ob_cache: Dict[str, Tuple[float, float, float, float, float]] = {}
    # tuple: (ts, bid_px, ask_px, bid_qty, ask_qty)

    # Local price-history cache for venue-native % changes (since RH API may not expose candles).
    # This is populated opportunistically whenever fetch_orderbook() is called.
    _PX_HIST_MAX_AGE_S: int = 9 * 24 * 3600   # keep ~9 days
    _PX_HIST_MAXLEN: int = 20000             # upper bound per symbol (safety)
    _px_hist: Dict[str, Any] = {}            # sym -> deque[(ts, mid_px)]

    def _base_url(self) -> str:
        u = (
            getattr(settings, "robinhood_crypto_base_url", None)
            or os.getenv("ROBINHOOD_CRYPTO_BASE_URL")
            or "https://trading.robinhood.com"
        )
        u = (u or "").strip().rstrip("/")
        return u or "https://trading.robinhood.com"

    def _safe_float(self, x: Any) -> Optional[float]:
        try:
            if x is None:
                return None
            if isinstance(x, (int, float)):
                return float(x)
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

    def _decimals_from_increment(self, inc: Any) -> Optional[int]:
        if inc is None:
            return None
        try:
            s = str(inc).strip().lower()
            if not s:
                return None

            m = re.search(r"e-(\d+)", s)
            if m:
                return int(m.group(1))

            if "." in s:
                frac = s.split(".", 1)[1]
                frac = frac.rstrip("0")
                return len(frac)

            return 0
        except Exception:
            return None

    def _decimals_from_num_str(self, s: Any) -> Optional[int]:
        try:
            if s is None:
                return None
            st = str(s).strip()
            if not st:
                return None
            if st[0] in "+-":
                st = st[1:]
            if "." not in st:
                return 0
            frac = st.split(".", 1)[1]
            return len(frac)
        except Exception:
            return None

    def _base_asset_from_symbol(self, sym: str) -> str:
        s = (sym or "").strip().upper()
        if "-" in s:
            return (s.split("-", 1)[0] or "").strip().upper()
        return s

    def _quote_asset_from_symbol(self, sym: str) -> str:
        s = (sym or "").strip().upper()
        if "-" in s:
            return (s.split("-", 1)[1] or "").strip().upper()
        for qt in self._KNOWN_QUOTES:
            if s.endswith(qt) and len(s) > len(qt):
                return qt
        return ""

    # ─────────────────────────────────────────────────────────────
    # Price tick helpers (critical for Robinhood)
    # ─────────────────────────────────────────────────────────────
    def _get_price_tick(self, sym: str) -> Tuple[Decimal, int]:
        """
        Return (tick, decimals) for this symbol's price.

        Preference order:
          1) get_order_rules() price_increment / price_decimals
          2) price_increment override (e.g., DOGE)
          3) sensible defaults (USD -> 0.01, non-USD -> 1e-8)

        IMPORTANT:
          We do NOT force USD to 2 decimals anymore, because DOGE (and potentially other
          assets) may legitimately trade at finer increments. The user requested DOGE at 6.
        """
        sym = self.resolve_symbol(sym)
        quote = self._quote_asset_from_symbol(sym)
        base = self._base_asset_from_symbol(sym)

        # Default ticks
        default_tick = Decimal("0.01") if quote == "USD" else Decimal("0.00000001")

        # Apply override default if configured (still overridden by actual rules if present)
        override_pi = None
        if quote == "USD":
            override_pi = self._PRICE_INCREMENT_OVERRIDES.get(base)
            if override_pi is not None:
                try:
                    default_tick = Decimal(str(override_pi))
                except Exception:
                    pass

        try:
            rules = self.get_order_rules(sym)
            pi = rules.get("price_increment")
            pd = rules.get("price_decimals")

            tick: Optional[Decimal] = None
            if pi is not None:
                try:
                    tick = Decimal(str(pi))
                except Exception:
                    tick = None

            if tick is None or tick <= 0:
                # If rules don't provide tick, use override default (already applied) or standard default.
                tick = default_tick

            decs: Optional[int] = None
            if pd is not None:
                try:
                    decs = int(pd)
                except Exception:
                    decs = None

            if decs is None or decs < 0:
                # Derive decimals from tick
                decs = self._decimals_from_increment(str(tick))
                if decs is None:
                    decs = 2 if quote == "USD" else 8

            return tick, decs
        except Exception:
            decs_fallback = self._decimals_from_increment(str(default_tick))
            if decs_fallback is None:
                decs_fallback = 2 if quote == "USD" else 8
            return default_tick, decs_fallback

    def _quantize_price(self, sym: str, price: Optional[float], side: str) -> Optional[float]:
        """
        Quantize a market price to the venue's tick.
        - bid: round DOWN to tick
        - ask: round UP to tick
        """
        try:
            if price is None:
                return None
            px = float(price)
            if not (px > 0.0):
                return None

            tick, decs = self._get_price_tick(sym)
            if tick <= 0:
                return px

            q = Decimal(str(px))
            step = tick

            rounding = ROUND_DOWN if str(side or "").lower().strip() == "bid" else ROUND_UP

            n = (q / step).to_integral_value(rounding=rounding)
            qq = (n * step)

            # Format with fixed decimals to ensure stable float stringification in UI
            fmt = f"{{0:.{int(decs)}f}}"
            s = fmt.format(qq)

            out = float(s)
            return out if out > 0.0 else None
        except Exception:
            try:
                return float(price) if price is not None else None
            except Exception:
                return None

    def _coerce_price_str(self, sym: str, side: str, price: float) -> str:
        """
        Enforce venue-accepted LIMIT price tick/precision for order submission.
        - buy: round DOWN to tick
        - sell: round UP to tick
        """
        sym = self.resolve_symbol(sym)
        sd = str(side or "").strip().lower()
        rounding = ROUND_DOWN if sd == "buy" else ROUND_UP

        tick, decs = self._get_price_tick(sym)

        try:
            qd = Decimal(str(price))
            if qd <= 0:
                raise Exception("price must be > 0")

            step = Decimal(str(tick))
            if step <= 0:
                # fallback: just format to decimals
                fmt = f"{{0:.{int(decs)}f}}"
                return fmt.format(qd)

            n = (qd / step).to_integral_value(rounding=rounding)
            qf = (n * step)

            fmt = f"{{0:.{int(decs)}f}}"
            return fmt.format(qf)
        except Exception:
            # fallback: still keep it bounded
            try:
                fmt = f"{{0:.{int(decs)}f}}"
                return fmt.format(Decimal(str(price)))
            except Exception:
                return str(float(price))

    def _get_ob_cache(
        self,
        sym: str,
        *,
        allow_stale: bool = False
    ) -> Optional[Tuple[float, float, float, float]]:
        """
        Return cached (bid_px, ask_px, bid_qty, ask_qty) if fresh.
        If allow_stale=True, allow fallback to older cached values up to _OB_CACHE_STALE_OK_S.
        """
        try:
            now = float(time.time())
            rec = self._ob_cache.get(sym)
            if not rec:
                return None
            ts, bid_px, ask_px, bid_qty, ask_qty = rec
            age = now - float(ts)

            if age <= float(self._OB_CACHE_TTL_S):
                return (float(bid_px), float(ask_px), float(bid_qty), float(ask_qty))

            if allow_stale and age <= float(self._OB_CACHE_STALE_OK_S):
                return (float(bid_px), float(ask_px), float(bid_qty), float(ask_qty))

            return None
        except Exception:
            return None

    def _set_ob_cache(
        self,
        sym: str,
        bid_px: Optional[float],
        ask_px: Optional[float],
        bid_qty: Optional[float],
        ask_qty: Optional[float]
    ) -> None:
        try:
            if not sym:
                return
            bp = float(bid_px or 0.0)
            ap = float(ask_px or 0.0)
            bq = float(bid_qty or 0.0)
            aq = float(ask_qty or 0.0)
            # Only store if we have at least one meaningful (positive) price.
            if not (bp > 0.0 or ap > 0.0):
                return
            self._ob_cache[sym] = (float(time.time()), bp, ap, bq, aq)
        except Exception:
            return

    # ─────────────────────────────────────────────────────────────
    # Local history → venue-native percent changes
    # ─────────────────────────────────────────────────────────────
    def _record_mid_price(self, sym: str, bid_px: Optional[float], ask_px: Optional[float]) -> None:
        """
        Store a mid-price point for later 1h/1d/1w change computation.
        Populated from fetch_orderbook() (poll-driven).
        """
        try:
            s = (sym or "").strip().upper()
            if not s:
                return
            bp = float(bid_px or 0.0)
            ap = float(ask_px or 0.0)

            mid = 0.0
            if bp > 0.0 and ap > 0.0:
                mid = (bp + ap) / 2.0
            elif bp > 0.0:
                mid = bp
            elif ap > 0.0:
                mid = ap
            else:
                return

            now = float(time.time())
            dq = self._px_hist.get(s)
            if dq is None or not isinstance(dq, deque):
                dq = deque(maxlen=int(self._PX_HIST_MAXLEN))
                self._px_hist[s] = dq

            dq.append((now, float(mid)))

            # prune old
            cutoff = now - float(self._PX_HIST_MAX_AGE_S)
            while dq and isinstance(dq[0], tuple) and len(dq[0]) == 2 and float(dq[0][0]) < cutoff:
                dq.popleft()
        except Exception:
            return

    def _pct(self, now_px: float, then_px: float) -> Optional[float]:
        try:
            if not (now_px > 0.0 and then_px > 0.0):
                return None
            return (float(now_px) / float(then_px) - 1.0) * 100.0
        except Exception:
            return None

    def _hist_get_price_at_or_before(self, sym: str, target_ts: float) -> Optional[float]:
        """
        Return the last recorded mid-price at or before target_ts.
        """
        try:
            dq = self._px_hist.get((sym or "").strip().upper())
            if dq is None or not isinstance(dq, deque) or not dq:
                return None

            # Scan from newest backwards until we cross target_ts
            last_good: Optional[float] = None
            for ts, px in reversed(dq):
                try:
                    t = float(ts)
                    p = float(px)
                except Exception:
                    continue
                if t <= float(target_ts):
                    return p if p > 0.0 else None
                last_good = p if p > 0.0 else last_good

            # If everything is newer than target, we don't have a true "then" value.
            return None
        except Exception:
            return None

    def get_pct_changes_for_symbol(self, symbol_venue: str) -> Dict[str, Optional[float]]:
        """
        Venue-native percent changes computed from locally recorded mid-prices.
        Requires that fetch_orderbook() has been called for this symbol over time.
        """
        sym = self.resolve_symbol(symbol_venue)
        if not sym:
            return {"change_1h": None, "change_1d": None, "change_1w": None}

        dq = self._px_hist.get(sym)
        if dq is None or not isinstance(dq, deque) or not dq:
            return {"change_1h": None, "change_1d": None, "change_1w": None}

        try:
            now_ts, now_px = dq[-1]
            now_ts = float(now_ts)
            now_px = float(now_px)
        except Exception:
            return {"change_1h": None, "change_1d": None, "change_1w": None}

        px_1h = self._hist_get_price_at_or_before(sym, now_ts - 3600.0)
        px_1d = self._hist_get_price_at_or_before(sym, now_ts - 86400.0)
        px_1w = self._hist_get_price_at_or_before(sym, now_ts - 7.0 * 86400.0)

        return {
            "change_1h": self._pct(now_px, px_1h) if px_1h is not None else None,
            "change_1d": self._pct(now_px, px_1d) if px_1d is not None else None,
            "change_1w": self._pct(now_px, px_1w) if px_1w is not None else None,
        }

    def get_pct_changes_for_asset(self, asset: str, quote: str = "USD") -> Dict[str, Optional[float]]:
        """
        Convenience for routers that operate on base assets:
          asset=BTC -> symbol=BTC-USD
        """
        a = (asset or "").strip().upper()
        q = (quote or "").strip().upper() or "USD"
        if not a:
            return {"change_1h": None, "change_1d": None, "change_1w": None}
        return self.get_pct_changes_for_symbol(f"{a}-{q}")

    def _coerce_qty_str(self, sym: str, qty: float) -> str:
        """
        Enforce venue-accepted quantity precision.
        This does NOT affect orderbook; it only affects order submission formatting.
        """
        base = self._base_asset_from_symbol(sym)
        inc = self._BASE_INCREMENT_OVERRIDES.get(base)

        if inc is None:
            try:
                rules = self.get_order_rules(sym)
                inc_any = rules.get("base_increment")
                if inc_any is not None:
                    inc = float(inc_any)
            except Exception:
                inc = None

        if inc is None or not (float(inc) > 0):
            return str(float(qty))

        try:
            qd = Decimal(str(qty))
            step = Decimal(str(inc))
            if qd <= 0 or step <= 0:
                return str(float(qty))

            n = (qd / step).to_integral_value(rounding=ROUND_DOWN)
            qf = (n * step)

            decs = self._decimals_from_increment(inc)
            if decs is None:
                return format(qf.normalize(), "f")
            fmt = f"{{0:.{decs}f}}"
            return fmt.format(qf)
        except Exception:
            # Keep this strict fallback: it prevents real rejections if something odd occurs during Decimal ops.
            if base == "DOGE":
                try:
                    qd2 = Decimal(str(qty)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                    return f"{qd2:.2f}"
                except Exception:
                    return str(float(qty))
            return str(float(qty))

    # ─────────────────────────────────────────────────────────────
    # Canonical symbol normalization
    # ─────────────────────────────────────────────────────────────
    def _to_canon_symbol(self, sym: Any, *, base: Any = None, quote: Any = None) -> Optional[str]:
        b = str(base).strip().upper() if base else ""
        q = str(quote).strip().upper() if quote else ""
        if b and q:
            return f"{b}-{q}"

        s = str(sym).strip().upper() if sym else ""
        if not s:
            return None

        s = re.sub(r"[\s/_:]+", "-", s)
        s = s.replace("/", "-")
        s = s.replace("--", "-")

        if "-" in s:
            parts = [p for p in s.split("-") if p]
            if len(parts) >= 2:
                return f"{parts[0]}-{parts[1]}"
            return None

        for qt in self._KNOWN_QUOTES:
            if s.endswith(qt) and len(s) > len(qt):
                base_part = s[: -len(qt)]
                if base_part:
                    return f"{base_part}-{qt}"

        return None

    def resolve_symbol(self, symbol_canon: str) -> str:
        canon = self._to_canon_symbol(symbol_canon) or (symbol_canon or "").strip().upper()
        return canon

    # ─────────────────────────────────────────────────────────────
    # Auth (Ed25519 signing)
    # ─────────────────────────────────────────────────────────────
    def _require_creds(self) -> Tuple[str, bytes]:
        api_key = (
            getattr(settings, "robinhood_crypto_api_key_id", None)
            or os.getenv("ROBINHOOD_CRYPTO_API_KEY_ID")
        )
        priv_b64 = (
            getattr(settings, "robinhood_crypto_private_key_b64", None)
            or os.getenv("ROBINHOOD_CRYPTO_PRIVATE_KEY_B64")
        )

        api_key = (api_key or "").strip()
        priv_b64 = (priv_b64 or "").strip()

        # Vault fallback (Profile → API Keys)
        if (not api_key) or (not priv_b64):
            try:
                vc = getattr(settings, "robinhood_private_creds", None)
                if callable(vc):
                    v = vc()
                    vk = vs = vp = None
                    if isinstance(v, (list, tuple)) and len(v) >= 2:
                        vk, vs = v[0], v[1]
                        vp = v[2] if len(v) >= 3 else None
                    elif isinstance(v, dict):
                        vk = v.get("api_key") or v.get("api_key_id") or v.get("key")
                        vs = v.get("api_secret") or v.get("private_key_b64") or v.get("secret")
                        vp = v.get("passphrase") or v.get("public_key_b64")
                    if (not api_key) and vk is not None:
                        api_key = str(vk).strip()
                    if (not priv_b64) and vs is not None:
                        priv_b64 = str(vs).strip()
                    # public key (vp) not required by adapter today
            except Exception:
                pass



        if not api_key or not priv_b64:
            raise Exception(
                "Missing Robinhood credentials. Set ROBINHOOD_CRYPTO_API_KEY_ID and ROBINHOOD_CRYPTO_PRIVATE_KEY_B64."
            )

        try:
            priv_raw = base64.b64decode(priv_b64, validate=True)
        except Exception as e:
            raise Exception(f"Invalid base64 in ROBINHOOD_CRYPTO_PRIVATE_KEY_B64: {e}")

        if not isinstance(priv_raw, (bytes, bytearray)) or len(priv_raw) < 32:
            raise Exception("ROBINHOOD_CRYPTO_PRIVATE_KEY_B64 decoded to < 32 bytes; expected ed25519 seed/key bytes.")

        seed = bytes(priv_raw[:32])
        return api_key, seed

    def _sign_headers(self, method: str, path_with_query: str, body_str: str) -> Dict[str, str]:
        api_key, seed = self._require_creds()

        try:
            from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        except Exception as e:
            raise Exception(f"cryptography is required for Robinhood ed25519 signing but is not installed: {e}")

        ts = str(int(time.time()))
        m = (method or "GET").upper().strip()
        p = (path_with_query or "").strip()
        if not p.startswith("/"):
            p = "/" + p

        msg = f"{api_key}{ts}{p}{m}{body_str or ''}".encode("utf-8")

        priv = Ed25519PrivateKey.from_private_bytes(seed)
        sig = priv.sign(msg)
        sig_b64 = base64.b64encode(sig).decode("utf-8")

        return {
            "x-api-key": api_key,
            "x-timestamp": ts,
            "x-signature": sig_b64,
            "accept": "application/json",
        }

    def _canonical_query(self, params: Optional[Dict[str, Any]]) -> str:
        if not params:
            return ""
        items: List[Tuple[str, str]] = []
        for k, v in params.items():
            if v is None:
                continue
            if isinstance(v, (list, tuple)):
                for vv in v:
                    if vv is None:
                        continue
                    items.append((str(k), str(vv)))
            else:
                items.append((str(k), str(v)))
        if not items:
            return ""

        items.sort(key=lambda kv: (kv[0], kv[1]))
        qs = urlencode(items, doseq=True, quote_via=quote, safe="-_.~")
        return f"?{qs}" if qs else ""

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        auth: bool = True,
        timeout_s: float = 25.0,
    ) -> Tuple[int, Dict[str, Any]]:
        base = self._base_url()
        m = (method or "GET").upper().strip()

        p = (path or "").strip()
        if not p.startswith("/"):
            p = "/" + p

        q = self._canonical_query(params)
        path_with_query = p + q
        url = base + path_with_query

        body_str = ""
        content = None
        headers: Dict[str, str] = {}

        if json_body is not None:
            body_str = json.dumps(json_body, separators=(",", ":"), sort_keys=True)
            content = body_str.encode("utf-8")
            headers["content-type"] = "application/json"

        if auth:
            headers.update(self._sign_headers(m, path_with_query, body_str))

        with httpx.Client(timeout=timeout_s) as client:
            r = client.request(m, url, headers=headers, content=content)

        data: Dict[str, Any] = {}
        if r.content:
            try:
                j = r.json()
                if isinstance(j, dict):
                    data = j
                else:
                    data = {"raw": j}
            except Exception:
                data = {"raw_text": r.text}

        if not (200 <= r.status_code < 300):
            raise Exception(f"Robinhood {m} {path_with_query} HTTP {r.status_code}: {r.text}")

        return r.status_code, data

    # ─────────────────────────────────────────────────────────────
    # Discovery: list symbols
    # ─────────────────────────────────────────────────────────────
    def list_symbols(self) -> List[str]:
        try:
            _, data = self._request("GET", "/api/v1/crypto/trading/trading_pairs/", auth=True)
        except Exception:
            return []

        items = data.get("results") or data.get("data") or data.get("trading_pairs") or []
        out: List[str] = []
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict):
                    tradable_flag = it.get("tradable")
                    if tradable_flag is None:
                        tradable_flag = it.get("is_tradable")
                    if tradable_flag is None:
                        tradable_flag = it.get("tradeable")
                    if tradable_flag is False:
                        continue

                    status = str(it.get("status") or it.get("state") or "").strip().lower()
                    if status in ("inactive", "disabled", "delisted", "unavailable", "halted"):
                        continue

                    sym = it.get("symbol") or it.get("id") or it.get("pair") or it.get("name")
                    base = it.get("asset_code") or it.get("base_asset") or it.get("base")
                    quote = it.get("quote_asset") or it.get("quote") or it.get("quote_code")
                    canon = self._to_canon_symbol(sym, base=base, quote=quote)
                    if canon:
                        out.append(canon)
                    elif sym:
                        out.append(str(sym).strip().upper())
                elif isinstance(it, str):
                    canon = self._to_canon_symbol(it)
                    out.append(canon or it.strip().upper())

        seen = set()
        uniq = []
        for s in out:
            s2 = (s or "").strip().upper()
            if not s2 or s2 in seen:
                continue
            seen.add(s2)
            uniq.append(s2)
        return sorted(uniq)

    # ─────────────────────────────────────────────────────────────
    # Rules helpers
    # ─────────────────────────────────────────────────────────────
    def _infer_price_decimals_from_best_bid_ask(self, sym: str) -> Optional[int]:
        sym = self.resolve_symbol(sym)
        if not sym:
            return None

        # Do NOT force USD=2 here; we want to infer actual decimals (DOGE shows 6).
        try:
            _, data = self._request(
                "GET",
                "/api/v1/crypto/marketdata/best_bid_ask/",
                params={"symbol": sym},
                auth=True,
                timeout_s=15.0,
            )
        except Exception:
            return None

        def norm_sym(s: Any) -> str:
            t = (str(s or "")).strip().upper()
            t = re.sub(r"[^A-Z0-9]+", "", t)
            return t

        want = norm_sym(sym)

        def iter_dicts(obj: Any) -> Iterable[Dict[str, Any]]:
            if isinstance(obj, dict):
                yield obj
                for v in obj.values():
                    yield from iter_dicts(v)
            elif isinstance(obj, list):
                for it in obj:
                    yield from iter_dicts(it)

        candidates = list(iter_dicts(data))
        ordered: List[Dict[str, Any]] = []

        for d in candidates:
            sym_any = d.get("symbol") or d.get("pair") or d.get("product_id") or d.get("instrument")
            if sym_any and norm_sym(sym_any) == want:
                ordered.append(d)

        if not ordered and candidates:
            ordered = candidates

        rec = ordered[0] if ordered else {}
        if not isinstance(rec, dict):
            return None

        price_keys = [
            "bid_price", "ask_price", "bid", "ask", "best_bid", "best_ask",
            "best_bid_price", "best_ask_price",
            "bid_inclusive_of_buy_spread", "ask_inclusive_of_sell_spread",
            "bid_price_inclusive_of_buy_spread", "ask_price_inclusive_of_sell_spread",
            "bidPrice", "askPrice", "bidPriceInclusiveOfBuySpread", "askPriceInclusiveOfSellSpread",
        ]

        candidates_str: List[str] = []

        for k in price_keys:
            if k not in rec:
                continue
            v = rec.get(k)
            if isinstance(v, dict):
                for kk in ("price", "value", "amount", "rate"):
                    if kk in v:
                        vv = v.get(kk)
                        if vv is not None:
                            candidates_str.append(str(vv))
            elif v is not None:
                candidates_str.append(str(v))

        for k, v in rec.items():
            kl = str(k).lower()
            if ("bid" in kl or "ask" in kl) and ("price" in kl or "spread" in kl):
                if v is None:
                    continue
                if isinstance(v, dict):
                    for kk in ("price", "value", "amount", "rate"):
                        if kk in v and v.get(kk) is not None:
                            candidates_str.append(str(v.get(kk)))
                else:
                    candidates_str.append(str(v))

        decs: List[int] = []
        for s in candidates_str:
            d = self._decimals_from_num_str(s)
            if d is not None:
                decs.append(d)

        if not decs:
            return None

        inferred = max(decs)

        # Keep a sane bound (prevents nonsense values from unexpected strings)
        if inferred < 0:
            return None
        if inferred > 12:
            inferred = 12

        return inferred

    # ─────────────────────────────────────────────────────────────
    # Rules
    # ─────────────────────────────────────────────────────────────
    def get_order_rules(self, symbol_venue: str) -> OrderRules:
        sym = self.resolve_symbol(symbol_venue)
        if not sym:
            return {"symbol_venue": symbol_venue}

        try:
            now = float(time.time())
            cached = self._rules_cache.get(sym)
            if cached:
                ts, rules = cached
                if (now - float(ts)) <= float(self._RULES_CACHE_TTL_S):
                    return rules
        except Exception:
            pass

        try:
            _, data = self._request(
                "GET",
                "/api/v1/crypto/trading/trading_pairs/",
                params={"symbol": sym},
                auth=True,
            )
        except Exception:
            rules = {"symbol_venue": sym}
            try:
                self._rules_cache[sym] = (float(time.time()), rules)
            except Exception:
                pass
            return rules

        items = data.get("results") or data.get("data") or []

        def norm_sym(x: Any) -> str:
            return re.sub(r"[^A-Z0-9]+", "", str(x or "").upper().strip())

        want = norm_sym(sym)

        tp: Dict[str, Any] = {}
        if isinstance(items, list) and items:
            chosen = None
            for it in items:
                if not isinstance(it, dict):
                    continue
                s_any = it.get("symbol") or it.get("id") or it.get("trading_pair") or it.get("pair") or it.get("name")
                base = it.get("asset_code") or it.get("base_asset") or it.get("base")
                quote = it.get("quote_asset") or it.get("quote") or it.get("quote_code")
                canon = self._to_canon_symbol(s_any, base=base, quote=quote)
                if canon and norm_sym(canon) == want:
                    chosen = it
                    break
                if s_any and norm_sym(s_any) == want:
                    chosen = it
                    break
            if chosen is None and isinstance(items[0], dict):
                chosen = items[0]
            tp = chosen or {}
        elif isinstance(data, dict):
            tp = data

        def num_from_any(v: Any) -> Optional[float]:
            if v is None:
                return None
            if isinstance(v, (int, float)):
                return float(v)
            if isinstance(v, dict):
                for kk in ("amount", "value", "quantity", "qty", "increment", "min", "max"):
                    if kk in v:
                        x = self._safe_float(v.get(kk))
                        if x is not None:
                            return x
                return None
            return self._safe_float(v)

        def pick(tp_dict: dict, keys: list[str]) -> Optional[float]:
            for k in keys:
                if k not in tp_dict:
                    continue
                x = num_from_any(tp_dict.get(k))
                if x is not None:
                    return x
            return None

        def int_from_any(v: Any) -> Optional[int]:
            if v is None:
                return None
            if isinstance(v, bool):
                return None
            if isinstance(v, int):
                return int(v)
            if isinstance(v, float):
                try:
                    iv = int(v)
                    return iv if float(iv) == float(v) else None
                except Exception:
                    return None
            if isinstance(v, dict):
                for kk in ("decimals", "precision", "scale", "value", "amount"):
                    if kk in v:
                        try:
                            return int(str(v.get(kk)).strip())
                        except Exception:
                            continue
                return None
            try:
                s = str(v).strip()
                if not s:
                    return None
                return int(s)
            except Exception:
                return None

        def pick_int(tp_dict: dict, keys: list[str]) -> Optional[int]:
            for k in keys:
                if k not in tp_dict:
                    continue
                x = int_from_any(tp_dict.get(k))
                if x is not None:
                    return x
            return None

        base_inc = pick(
            tp,
            [
                "min_order_quantity_increment",
                "order_quantity_increment",
                "quantity_increment",
                "base_increment",
                "min_quantity_increment",
                "asset_quantity_increment",
                "min_asset_quantity_increment",
                "min_order_asset_quantity_increment",
                "min_trade_quantity_increment",
                "trade_quantity_increment",
                "order_size_increment",
                "min_order_size_increment",
                "min_trade_size_increment",
                "min_quantity_step",
                "quantity_step",
                "qty_increment",
                "min_qty_increment",
            ],
        )

        price_inc = pick(
            tp,
            [
                "min_price_increment",
                "price_increment",
                "quote_increment",
                "min_order_price_increment",
                "price_step",
                "min_price_step",
                "tick_size",
                "min_tick_size",
            ],
        )

        min_qty = pick(
            tp,
            [
                "min_order_quantity",
                "min_quantity",
                "min_qty",
                "min_order_size",
                "min_trade_quantity",
                "min_trade_size",
            ],
        )

        min_notional = pick(
            tp,
            [
                "min_order_notional",
                "min_notional",
                "min_order_value",
                "min_trade_notional",
            ],
        )

        qty_decimals_raw = pick_int(
            tp,
            [
                "asset_quantity_decimals",
                "quantity_decimals",
                "order_quantity_decimals",
                "min_order_quantity_decimals",
                "base_decimals",
                "base_precision",
                "quantity_precision",
                "qty_decimals",
            ],
        )

        price_decimals_raw = pick_int(
            tp,
            [
                "price_decimals",
                "quote_decimals",
                "order_price_decimals",
                "min_order_price_decimals",
                "price_precision",
                "quote_precision",
                "tick_decimals",
                "price_scale",
            ],
        )

        if base_inc is None and qty_decimals_raw is not None and qty_decimals_raw >= 0:
            try:
                base_inc = float(Decimal("1").scaleb(-int(qty_decimals_raw)))
            except Exception:
                pass

        if price_inc is None and price_decimals_raw is not None and price_decimals_raw >= 0:
            try:
                price_inc = float(Decimal("1").scaleb(-int(price_decimals_raw)))
            except Exception:
                pass

        base_asset = self._base_asset_from_symbol(sym)
        quote_asset = self._quote_asset_from_symbol(sym)

        override_inc = self._BASE_INCREMENT_OVERRIDES.get(base_asset)
        if override_inc is not None and base_inc is None:
            base_inc = float(override_inc)

        if base_inc is None and min_qty is not None:
            try:
                mq = float(min_qty)
                if mq > 0:
                    base_inc = mq
            except Exception:
                pass

        qty_decimals = qty_decimals_raw if qty_decimals_raw is not None else self._decimals_from_increment(base_inc)
        price_decimals = price_decimals_raw if price_decimals_raw is not None else self._decimals_from_increment(price_inc)

        # Keep this: it’s UI-safety for rules, not orderbook.
        if qty_decimals is None:
            if base_asset in ("BTC", "ETH", "LTC", "XRP"):
                qty_decimals = 8

        if base_inc is None and qty_decimals is not None and qty_decimals >= 0:
            try:
                base_inc = float(Decimal("1").scaleb(-int(qty_decimals)))
            except Exception:
                pass

        # If price decimals are missing, infer from marketdata (DOGE will infer 6).
        if price_decimals is None:
            inferred = self._infer_price_decimals_from_best_bid_ask(sym)
            if inferred is not None and inferred >= 0:
                price_decimals = inferred
                if price_inc is None:
                    try:
                        price_inc = float(Decimal("1").scaleb(-int(price_decimals)))
                    except Exception:
                        pass

        # Apply explicit price tick override for USD pairs (e.g., DOGE)
        if quote_asset == "USD":
            override_pi = self._PRICE_INCREMENT_OVERRIDES.get(base_asset)
            if override_pi is not None:
                price_inc = float(override_pi)
                od = self._decimals_from_increment(override_pi)
                if od is not None:
                    price_decimals = od

        # Only apply a conservative USD fallback when we truly have no better data.
        if quote_asset == "USD":
            if price_inc is None or not (float(price_inc) > 0):
                price_inc = 0.01
            if price_decimals is None:
                price_decimals = self._decimals_from_increment(price_inc) or 2

        rules: OrderRules = {
            "symbol_venue": sym,
            "base_increment": base_inc,
            "price_increment": price_inc,
            "qty_decimals": qty_decimals,
            "price_decimals": price_decimals,
            "min_qty": min_qty,
            "max_qty": None,
            "min_notional": min_notional,
            "max_notional": None,
            "supports_post_only": False,
            "supported_tifs": ["gtc"],
            "supported_order_types": ["market", "limit"],
            "raw": tp if isinstance(tp, dict) else {},
        }

        try:
            self._rules_cache[sym] = (float(time.time()), rules)
        except Exception:
            pass

        return rules

    # ─────────────────────────────────────────────────────────────
    # Market data: best bid/ask → map to OrderBook (depth=1)
    # ─────────────────────────────────────────────────────────────
    def fetch_orderbook(self, symbol_venue: str, depth: int, dry_run: bool) -> OrderBook:
        sym = self.resolve_symbol(symbol_venue)
        if not sym:
            return {"bids": [], "asks": []}

        debug = (os.getenv("ROBINHOOD_OB_DEBUG", "") or "").strip() == "1"

        def is_bad_px(x: Optional[float]) -> bool:
            try:
                return (x is None) or (float(x) <= 0.0)
            except Exception:
                return True

        def norm_sym(s: Any) -> str:
            t = (str(s or "")).strip().upper()
            t = re.sub(r"[^A-Z0-9]+", "", t)
            return t

        want = norm_sym(sym)

        def iter_dicts(obj: Any) -> Iterable[Dict[str, Any]]:
            if isinstance(obj, dict):
                yield obj
                for v in obj.values():
                    yield from iter_dicts(v)
            elif isinstance(obj, list):
                for it in obj:
                    yield from iter_dicts(it)

        def num_from_any(v: Any) -> Optional[float]:
            if v is None:
                return None
            if isinstance(v, (int, float)):
                return float(v)
            if isinstance(v, dict):
                for kk in ("price", "px", "rate", "value", "amount"):
                    if kk in v:
                        out = self._safe_float(v.get(kk))
                        if out is not None:
                            return out
                return None
            return self._safe_float(v)

        def pick_num(d: Dict[str, Any], keys: List[str]) -> Optional[float]:
            for k in keys:
                if k not in d:
                    continue
                vv = num_from_any(d.get(k))
                if vv is not None:
                    return vv
            return None

        def heuristic_pick_price(d: Dict[str, Any], side: str) -> Optional[float]:
            s = side.lower()
            for k, v in d.items():
                kl = str(k).lower()
                if s in kl and ("price" in kl or "spread" in kl or kl == s):
                    vv = num_from_any(v)
                    if vv is not None:
                        return vv
            return None

        def heuristic_pick_qty(d: Dict[str, Any], side: str) -> Optional[float]:
            s = side.lower()
            for k, v in d.items():
                kl = str(k).lower()
                if s in kl and ("qty" in kl or "quantity" in kl or "size" in kl or "amount" in kl):
                    vv = num_from_any(v)
                    if vv is not None:
                        return vv
            return None

        def call_best_bid_ask(sym_param: str) -> Dict[str, Any]:
            try:
                _, data = self._request(
                    "GET",
                    "/api/v1/crypto/marketdata/best_bid_ask/",
                    params={"symbol": sym_param},
                    auth=True,
                    timeout_s=15.0,
                )
                return data or {}
            except Exception as e:
                msg = str(e) or ""
                if ("HTTP 400" in msg) and ("Invalid symbol" in msg):
                    return {}
                raise

        # Call #1: canonical (BTC-USD)
        data = call_best_bid_ask(sym)

        # Extract from payload
        candidates = list(iter_dicts(data))
        match_first: List[Dict[str, Any]] = []
        have_prices: List[Dict[str, Any]] = []

        for d in candidates:
            sym_any = d.get("symbol") or d.get("pair") or d.get("product_id") or d.get("instrument")
            if sym_any and norm_sym(sym_any) == want:
                match_first.append(d)

            keys_l = " ".join([str(k).lower() for k in d.keys()])
            if ("bid" in keys_l and "ask" in keys_l) and ("price" in keys_l or "spread" in keys_l):
                have_prices.append(d)

        ordered: List[Dict[str, Any]] = []
        ordered.extend(match_first)
        for d in have_prices:
            if d not in ordered:
                ordered.append(d)
        for d in candidates:
            if d not in ordered:
                ordered.append(d)

        rec = (ordered[0] if ordered else {}) or {}

        bid_px = pick_num(
            rec,
            [
                "bid_price",
                "bid",
                "best_bid",
                "best_bid_price",
                "bid_inclusive_of_buy_spread",
                "bid_inclusive_of_buy_spread_price",
                "bid_price_inclusive_of_buy_spread",
                "bid_price_inclusive_of_spread",
                "bidPrice",
                "bidPriceInclusiveOfBuySpread",
            ],
        )
        ask_px = pick_num(
            rec,
            [
                "ask_price",
                "ask",
                "best_ask",
                "best_ask_price",
                "ask_inclusive_of_sell_spread",
                "ask_inclusive_of_sell_spread_price",
                "ask_price_inclusive_of_sell_spread",
                "ask_price_inclusive_of_spread",
                "askPrice",
                "askPriceInclusiveOfSellSpread",
            ],
        )

        bid_qty = pick_num(rec, ["bid_quantity", "bid_qty", "bid_size", "bid_amount"])
        ask_qty = pick_num(rec, ["ask_quantity", "ask_qty", "ask_size", "ask_amount"])

        if bid_qty is None:
            v = rec.get("bid")
            if isinstance(v, dict):
                bid_qty = pick_num(v, ["quantity", "qty", "size", "amount"])
        if ask_qty is None:
            v = rec.get("ask")
            if isinstance(v, dict):
                ask_qty = pick_num(v, ["quantity", "qty", "size", "amount"])

        if bid_px is None:
            bid_px = heuristic_pick_price(rec, "bid")
        if ask_px is None:
            ask_px = heuristic_pick_price(rec, "ask")
        if bid_qty is None:
            bid_qty = heuristic_pick_qty(rec, "bid")
        if ask_qty is None:
            ask_qty = heuristic_pick_qty(rec, "ask")

        # If still bad, fall back to cached (allow stale)
        if is_bad_px(bid_px) and is_bad_px(ask_px):
            cached = self._get_ob_cache(sym, allow_stale=True)
            if cached:
                cbid_px, cask_px, cbid_qty, cask_qty = cached
                if is_bad_px(bid_px) and cbid_px > 0:
                    bid_px = cbid_px
                if is_bad_px(ask_px) and cask_px > 0:
                    ask_px = cask_px
                if bid_qty is None or float(bid_qty or 0.0) == 0.0:
                    bid_qty = cbid_qty
                if ask_qty is None or float(ask_qty or 0.0) == 0.0:
                    ask_qty = cask_qty

        # Normalize prices to detected/overridden tick so UI + clicking levels does not propagate invalid precision.
        bid_px_q = self._quantize_price(sym, bid_px, "bid")
        ask_px_q = self._quantize_price(sym, ask_px, "ask")

        # If quantization killed prices (should not happen), fall back to raw.
        if bid_px_q is not None:
            bid_px = bid_px_q
        if ask_px_q is not None:
            ask_px = ask_px_q

        # Update cache if meaningful
        self._set_ob_cache(sym, bid_px, ask_px, bid_qty, ask_qty)

        # Record a mid-price point for % changes
        self._record_mid_price(sym, bid_px, ask_px)

        bids: List[Dict[str, float]] = []
        asks: List[Dict[str, float]] = []

        # IMPORTANT: never emit a 0-priced level
        if bid_px is not None and float(bid_px) > 0.0:
            bids.append({"price": float(bid_px), "qty": float(bid_qty or 0.0)})
        if ask_px is not None and float(ask_px) > 0.0:
            asks.append({"price": float(ask_px), "qty": float(ask_qty or 0.0)})

        if debug:
            try:
                print(
                    f"ROBINHOOD_OB_DEBUG symbol={sym} want={want} "
                    f"bid_px={bid_px} ask_px={ask_px} bid_qty={bid_qty} ask_qty={ask_qty} "
                    f"cache_fresh={self._get_ob_cache(sym, allow_stale=False) is not None} "
                    f"cache_stale={self._get_ob_cache(sym, allow_stale=True) is not None}"
                )
                if (not bids and not asks):
                    print(f"ROBINHOOD_OB_DEBUG raw={json.dumps(data)[:4000]}")
            except Exception:
                pass

        return {"bids": bids, "asks": asks}

    # ─────────────────────────────────────────────────────────────
    # Balances / Orders / Trading
    # ─────────────────────────────────────────────────────────────

    def fetch_balances(self, dry_run: bool) -> List[BalanceItem]:
        _, data = self._request("GET", "/api/v1/crypto/trading/holdings/", auth=True)

        items = data.get("results") or data.get("data") or data.get("holdings") or []
        out: List[BalanceItem] = []

        if not isinstance(items, list):
            return []

        for it in items:
            if not isinstance(it, dict):
                continue

            asset = str(
                it.get("asset_code")
                or it.get("currency_code")
                or it.get("asset")
                or it.get("symbol")
                or ""
            ).strip().upper()
            if not asset:
                continue

            total = self._safe_float(
                it.get("total_quantity")
                or it.get("quantity")
                or it.get("total")
                or it.get("balance")
            ) or 0.0

            available = self._safe_float(
                it.get("available_quantity")
                or it.get("quantity_available")
                or it.get("available")
            )
            if available is None:
                available = float(total)

            hold = self._safe_float(it.get("hold") or it.get("held_for_orders") or it.get("held"))
            if hold is None:
                try:
                    hold = float(total) - float(available)
                    if hold < 0:
                        hold = 0.0
                except Exception:
                    hold = 0.0

            out.append(
                {
                    "asset": asset,
                    "total": float(total),
                    "available": float(available),
                    "hold": float(hold),
                }
            )

        try:
            _, acct = self._request("GET", "/api/v1/crypto/trading/accounts/", auth=True, timeout_s=15.0)

            rec = None
            if isinstance(acct, dict):
                r = acct.get("results")
                if isinstance(r, list) and r and isinstance(r[0], dict):
                    rec = r[0]
                elif isinstance(r, dict):
                    rec = r
                else:
                    rec = acct

            def pick_money(d: dict, keys: list[str]) -> Optional[float]:
                for k in keys:
                    if k not in d:
                        continue
                    v = d.get(k)
                    if isinstance(v, dict):
                        for kk in ("amount", "value", "quantity", "qty"):
                            if kk in v:
                                x = self._safe_float(v.get(kk))
                                if x is not None:
                                    return x
                    x = self._safe_float(v)
                    if x is not None:
                        return x
                return None

            usd_available = None
            if isinstance(rec, dict):
                usd_available = pick_money(
                    rec,
                    [
                        "buying_power",
                        "crypto_buying_power",
                        "available_buying_power",
                        "available_cash",
                        "cash_available",
                        "cash_balance",
                        "available_to_trade",
                        "available",
                        "usd_available",
                        "usd_buying_power",
                    ],
                )

            if usd_available is not None:
                out.append(
                    {
                        "asset": "USD",
                        "total": float(usd_available),
                        "available": float(usd_available),
                        "hold": 0.0,
                    }
                )
        except Exception:
            pass

        def split_base_quote(sym_any: Any) -> Tuple[Optional[str], Optional[str]]:
            canon = self._to_canon_symbol(sym_any) or (str(sym_any or "").strip().upper() if sym_any else "")
            if not canon or "-" not in canon:
                return None, None
            base, quote = canon.split("-", 1)
            base = (base or "").strip().upper()
            quote = (quote or "").strip().upper()
            return (base or None), (quote or None)

        def add_to(d: Dict[str, float], k: Optional[str], amt: float) -> None:
            if not k:
                return
            if not (amt > 0):
                return
            d[k] = float(d.get(k, 0.0) + float(amt))

        reserved_base: Dict[str, float] = {}

        try:
            orders = self.fetch_orders(dry_run=dry_run)
            for o in orders or []:
                if not isinstance(o, dict):
                    continue
                if str(o.get("status") or "").strip().lower() != "open":
                    continue

                side = str(o.get("side") or "").strip().lower()
                if side != "sell":
                    continue

                qty = self._safe_float(o.get("qty")) or 0.0
                filled = self._safe_float(o.get("filled_qty")) or 0.0
                remaining = qty - filled
                if remaining <= 0:
                    continue

                sym_any = o.get("symbol_venue") or o.get("symbol_canon")
                base, _quote = split_base_quote(sym_any)
                if not base:
                    continue

                add_to(reserved_base, base, remaining)
        except Exception:
            reserved_base = {}

        by_asset: Dict[str, BalanceItem] = {}
        for b in out:
            a = str(b.get("asset") or "").strip().upper()
            if not a:
                continue
            by_asset[a] = b

        for asset, hold_amt in reserved_base.items():
            b = by_asset.get(asset)
            if b is None:
                by_asset[asset] = {
                    "asset": asset,
                    "total": float(hold_amt),
                    "available": 0.0,
                    "hold": float(hold_amt),
                }
                continue

            total = float(self._safe_float(b.get("total")) or 0.0)
            avail = float(self._safe_float(b.get("available")) or total)
            hold = float(self._safe_float(b.get("hold")) or 0.0)

            computed = float(max(hold_amt, 0.0))
            new_hold = max(hold, computed)

            computed_avail = max(total - new_hold, 0.0)
            b["hold"] = new_hold
            b["available"] = min(avail, computed_avail)

        out = list(by_asset.values())
        out.sort(key=lambda x: str(x.get("asset") or ""))
        return out

    def _normalize_status(self, raw: Any) -> Tuple[str, str]:
        st_raw = str(raw or "").strip()
        st = st_raw.lower().strip()

        if not st:
            return "open", st_raw

        if st in ("open", "queued", "pending", "confirmed", "new", "placed", "partially_filled", "working", "accepted"):
            return "open", st_raw

        if st in ("filled", "executed", "done", "completed"):
            return "filled", st_raw

        if st in ("canceled", "cancelled", "canceling", "voided"):
            return "canceled", st_raw

        if st in ("rejected", "failed", "expired", "error", "denied"):
            return "rejected", st_raw

        if st in self._TERMINAL_RAW:
            return "rejected", st_raw

        return "open", st_raw

    # ─────────────────────────────────────────────────────────────
    # Orders: paging + per-id fetch (NEW)
    # ─────────────────────────────────────────────────────────────
    def _path_from_next(self, next_url: Any) -> Optional[str]:
        """
        Robinhood paging commonly returns an absolute next URL.
        Convert it to a /path?query we can pass into _request().
        """
        try:
            if not next_url:
                return None
            u = urlparse(str(next_url))
            if not u.path:
                return None
            return u.path + (("?" + u.query) if u.query else "")
        except Exception:
            return None

    def _order_payload_to_item(self, o: Dict[str, Any]) -> Optional[VenueOrder]:
        if not isinstance(o, dict):
            return None

        oid = str(o.get("id") or o.get("order_id") or "").strip()
        if not oid:
            return None

        sym_raw = (o.get("symbol") or o.get("trading_pair") or o.get("pair") or "")
        base = o.get("asset_code") or o.get("base_asset") or o.get("base")
        quote = o.get("quote_asset") or o.get("quote") or o.get("quote_code")

        sym_venue = str(sym_raw).strip().upper() if sym_raw else None
        sym_canon = self._to_canon_symbol(sym_raw, base=base, quote=quote) or sym_venue

        side = str(o.get("side") or "").strip().lower() or None
        typ = str(o.get("type") or o.get("order_type") or "").strip().lower() or None

        status_unified, status_raw = self._normalize_status(o.get("state") or o.get("status"))

        qty = self._safe_float(
            o.get("asset_quantity")
            or o.get("quantity")
            or (o.get("limit_order_config") or {}).get("asset_quantity")
            or (o.get("market_order_config") or {}).get("asset_quantity")
        )
        filled_qty = self._safe_float(o.get("filled_asset_quantity") or o.get("filled_quantity"))

        limit_price = self._safe_float(
            o.get("limit_price") or (o.get("limit_order_config") or {}).get("limit_price")
        )
        avg_fill_price = self._safe_float(o.get("average_price") or o.get("avg_fill_price"))

        created_at = self._dt_from_iso(o.get("created_at") or o.get("created_time"))
        updated_at = self._dt_from_iso(o.get("updated_at") or o.get("last_update_time"))

        cancel_ref = f"robinhood:{oid}" if status_unified == "open" else None

        return {
            "venue": self.venue,
            "venue_order_id": oid,
            "symbol_venue": sym_venue,
            "symbol_canon": sym_canon,
            "side": side,
            "type": typ,
            "status": status_unified,
            "status_raw": status_raw,
            "cancel_ref": cancel_ref,
            "qty": qty,
            "filled_qty": filled_qty,
            "limit_price": limit_price if (typ == "limit") else None,
            "avg_fill_price": avg_fill_price,
            "fee": None,
            "fee_asset": None,
            "created_at": created_at,
            "updated_at": updated_at,
        }

    def fetch_order(self, venue_order_id: str, dry_run: bool = False) -> Optional[VenueOrder]:
        """
        Fetch a single order by id.
        Used by the service layer to resolve 'stale open' rows that fell out of list endpoints.
        """
        oid = (venue_order_id or "").strip()
        if not oid:
            return None
        _, data = self._request("GET", f"/api/v1/crypto/trading/orders/{oid}/", auth=True)
        if not isinstance(data, dict):
            return None
        return self._order_payload_to_item(data)

    def fetch_orders(self, dry_run: bool) -> List[VenueOrder]:
        """
        Robinhood list endpoint is paginated (results + next).
        If we only ingest the first page, older orders can remain 'open' in SQLite
        even after they fill/close at the venue.
        """
        out: List[VenueOrder] = []

        path: Optional[str] = "/api/v1/crypto/trading/orders/"
        pages = 0
        max_pages = int(getattr(settings, "robinhood_orders_max_pages", 20))

        while path and pages < max_pages:
            pages += 1
            _, data = self._request("GET", path, auth=True)

            if not isinstance(data, dict):
                break

            items = data.get("results") or data.get("data") or data.get("orders") or []
            if not isinstance(items, list):
                break

            for o in items:
                if not isinstance(o, dict):
                    continue
                it = self._order_payload_to_item(o)
                if it:
                    out.append(it)

            nxt = data.get("next") or data.get("next_url") or data.get("nextUrl")
            path = self._path_from_next(nxt) if nxt else None

        return out

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

        sym = self.resolve_symbol(symbol_venue)
        if not sym:
            raise Exception("RobinhoodAdapter.place_order requires symbol")

        sd = (side or "").strip().lower()
        if sd not in ("buy", "sell"):
            raise Exception(f"Invalid side: {side}")

        ot = (type_ or "").strip().lower()
        if ot not in ("market", "limit"):
            raise Exception(f"RobinhoodAdapter.place_order supports type_=market|limit (got {type_})")

        q = float(qty)
        if not (q > 0.0):
            raise Exception("RobinhoodAdapter.place_order requires qty > 0")

        qty_str = self._coerce_qty_str(sym, q)

        body: Dict[str, Any] = {
            "client_order_id": (client_order_id or "").strip() or f"utt-{int(time.time() * 1000)}",
            "side": sd,
            "type": ot,
            "symbol": sym,
        }

        if ot == "limit":
            if limit_price is None:
                raise Exception("RobinhoodAdapter.place_order requires limit_price for limit orders")
            px = float(limit_price)
            if not (px > 0.0):
                raise Exception("RobinhoodAdapter.place_order requires limit_price > 0")

            # Enforce tick-valid limit price string.
            limit_price_str = self._coerce_price_str(sym, sd, px)

            body["limit_order_config"] = {
                "asset_quantity": qty_str,
                "limit_price": limit_price_str,
            }
        else:
            body["market_order_config"] = {
                "asset_quantity": qty_str,
            }

        _status, data = self._request("POST", "/api/v1/crypto/trading/orders/", json_body=body, auth=True)

        oid = str(data.get("id") or data.get("order_id") or "").strip()
        raw_state = data.get("state") or data.get("status")
        status_unified, status_raw = self._normalize_status(raw_state)

        if not oid:
            sr = data.get("result") or data.get("order") or {}
            if isinstance(sr, dict):
                oid = str(sr.get("id") or sr.get("order_id") or "").strip()
                raw_state2 = sr.get("state") or sr.get("status") or raw_state
                status_unified, status_raw = self._normalize_status(raw_state2)

        if not oid:
            raise Exception(f"Robinhood order placed but missing order id. Raw: {json.dumps(data, indent=2)}")

        return {
            "venue_order_id": oid,
            "status": status_unified if status_unified else "acked",
            "raw": data,
            "cancel_ref": f"robinhood:{oid}",
            "status_raw": status_raw,
        }

    def cancel_order(self, venue_order_id: str, dry_run: bool) -> bool:
        if dry_run:
            return True

        oid = (venue_order_id or "").strip()
        if not oid:
            return False

        status_code, _ = self._request(
            "POST",
            f"/api/v1/crypto/trading/orders/{oid}/cancel/",
            json_body={},
            auth=True,
        )
        return 200 <= int(status_code) < 300
