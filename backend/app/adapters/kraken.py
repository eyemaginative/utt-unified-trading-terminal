# backend/app/adapters/kraken.py

from __future__ import annotations

from typing import List, Optional, Dict, Any, Set, Tuple
import time
import base64
import hashlib
import hmac
import urllib.parse
from datetime import datetime, timezone
import os
from decimal import Decimal, ROUND_FLOOR, InvalidOperation

import httpx

from .base import ExchangeAdapter, PlacedOrder, BalanceItem, OrderBook, VenueOrder, OrderRules
from ..config import settings


class KrakenAdapter(ExchangeAdapter):
    venue = "kraken"

    _base_url = "https://api.kraken.com/0"

    # Explicit overrides (Depth/AddOrder pair string)
    _map = {
        "USDT-USD": "USDTUSD",
        "BTC-USD": "XBTUSD",
        "ETH-USD": "ETHUSD",
        "DOGE-USD": "XDGUSD",
    }

    _TERMINAL = {"filled", "canceled", "cancelled", "rejected", "done", "closed", "expired"}

    # AssetPairs cache (shared across instances)
    _asset_pairs_cache_ts: float = 0.0
    _asset_pairs_cache_ttl_s: float = float(os.getenv("KRAKEN_ASSET_PAIRS_CACHE_TTL_S", "300"))
    _asset_pairs_cache: Optional[Dict[str, Any]] = None  # raw "result" dict from AssetPairs

    # OHLC cache for percent-change computation (shared)
    _ohlc_cache_ttl_s: float = float(os.getenv("KRAKEN_OHLC_CACHE_TTL_S", "20"))
    _ohlc_cache: Dict[str, Tuple[float, List[List[Any]]]] = {}  # key -> (ts, candles)

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

    def _dt_from_s(self, sec: Any) -> Optional[datetime]:
        try:
            s = float(sec)
            return datetime.fromtimestamp(s, tz=timezone.utc).replace(tzinfo=None)
        except Exception:
            return None

    def _canon_asset_to_kraken(self, asset: str) -> str:
        a = (asset or "").strip().upper()
        if not a:
            return a
        if a == "BTC":
            return "XBT"
        if a == "DOGE":
            return "XDG"
        return a

    def _asset_code_to_canon(self, code: str) -> str:
        """
        Convert Kraken asset codes to canonical assets.
        Examples seen:
          XXBT -> BTC
          XDG  / XXDG -> DOGE
          ZUSD -> USD
          USDT -> USDT
        """
        c = (code or "").strip().upper()
        if not c:
            return ""
        # Common exacts
        if c in ("XBT", "XXBT"):
            return "BTC"
        if c in ("XDG", "XXDG"):
            return "DOGE"
        if c in ("ETH", "XETH", "XXETH"):
            return "ETH"
        if c in ("USD", "ZUSD"):
            return "USD"
        if c in ("EUR", "ZEUR"):
            return "EUR"
        if c in ("USDT",):
            return "USDT"
        if c in ("USDC",):
            return "USDC"

        # Strip leading X/Z prefixes (Kraken uses X*/Z* a lot)
        if len(c) >= 3 and (c[0] in ("X", "Z")):
            c2 = c[1:]
        else:
            c2 = c

        # If still prefixed twice (XX?? / ZZ??)
        if len(c2) >= 3 and (c2[0] in ("X", "Z")):
            c2 = c2[1:]

        # Final mapping quirks
        if c2 == "XBT":
            return "BTC"
        if c2 == "XDG":
            return "DOGE"
        return c2

    def _normalize_kraken_status(self, st: Optional[str], is_open_hint: bool = False) -> str:
        """
        Normalize Kraken raw order status into our canonical set.

        Kraken statuses observed/expected:
          open, pending, closed, canceled, expired
          (some variants may appear; we handle defensively)

        We want:
          open / filled / canceled / rejected / acked (rare fallback)
        """
        s = str(st or "").strip().lower()
        if not s:
            return "open" if is_open_hint else "acked"

        if s in ("open", "pending"):
            return "open"
        if s in ("closed", "filled"):
            return "filled"
        if s in ("canceled", "cancelled"):
            return "canceled"
        if s in ("expired", "rejected"):
            return "rejected"

        # Unknown raw status: prefer "open" if we believe it's open; else acked.
        return "open" if is_open_hint else "acked"

    # ─────────────────────────────────────────────────────────────
    # Public HTTP
    # ─────────────────────────────────────────────────────────────
    def _public_get(self, path: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        url = f"{self._base_url}{path}"
        with httpx.Client(timeout=20.0) as client:
            r = client.get(url, params=params or {})
            r.raise_for_status()
            data = r.json() if r.content else {}
        if not isinstance(data, dict):
            raise Exception(f"Kraken public GET unexpected response type: {type(data)}")
        if data.get("error"):
            raise Exception(f"Kraken public GET error: {data.get('error')}")
        return data

    def _get_asset_pairs_result(self) -> Dict[str, Any]:
        now = self._now_ts()
        if KrakenAdapter._asset_pairs_cache is not None and (now - KrakenAdapter._asset_pairs_cache_ts) < KrakenAdapter._asset_pairs_cache_ttl_s:
            return KrakenAdapter._asset_pairs_cache or {}

        data = self._public_get("/public/AssetPairs", params={})
        res = data.get("result") or {}
        if not isinstance(res, dict):
            res = {}
        KrakenAdapter._asset_pairs_cache = res
        KrakenAdapter._asset_pairs_cache_ts = now
        return res

    # ─────────────────────────────────────────────────────────────
    # Venue-native percent changes via OHLC (intervals 60/1440/10080)
    # ─────────────────────────────────────────────────────────────
    def _public_ohlc(self, pair_str: str, interval_min: int, since_ts: int) -> List[List[Any]]:
        """
        Return OHLC rows for the given pair + interval, caching briefly.
        Kraken OHLC result rows shape:
          [time, open, high, low, close, vwap, volume, count]
        """
        pair_key = (pair_str or "").strip()
        if not pair_key:
            return []

        interval_i = int(interval_min)
        since_i = int(since_ts)

        cache_key = f"{pair_key}|{interval_i}|{since_i}"
        now = self._now_ts()
        try:
            cached = KrakenAdapter._ohlc_cache.get(cache_key)
            if cached:
                ts, rows = cached
                if (now - float(ts)) <= float(KrakenAdapter._ohlc_cache_ttl_s):
                    return rows or []
        except Exception:
            pass

        data = self._public_get(
            "/public/OHLC",
            params={"pair": pair_key, "interval": str(interval_i), "since": str(since_i)},
        )
        res = data.get("result") or {}
        if not isinstance(res, dict) or not res:
            return []

        rows: List[List[Any]] = []

        # The first non-"last" key should contain candles
        for k, v in res.items():
            if str(k).lower() == "last":
                continue
            if isinstance(v, list):
                rows = v
                break

        if not isinstance(rows, list):
            rows = []

        try:
            KrakenAdapter._ohlc_cache[cache_key] = (float(now), rows)
        except Exception:
            pass

        return rows

    def _pair_candidates_for_asset(self, asset: str, quote: str = "USD") -> List[str]:
        """
        Try to find an available quote pair for a given base asset.
        Returns Kraken "pair token" strings compatible with OHLC/Ticker/Depth.
        """
        a = (asset or "").strip().upper()
        if not a:
            return []

        qpref = (quote or "USD").strip().upper() or "USD"

        # Prefer requested quote first, then sensible fallbacks.
        quotes = [qpref]
        for q in ("USD", "USDT", "USDC", "EUR"):
            if q not in quotes:
                quotes.append(q)

        out: List[str] = []
        for q in quotes:
            try:
                cand = self.resolve_symbol(f"{a}-{q}")
                if cand and cand not in out:
                    out.append(cand)
            except Exception:
                continue

        return out

    def _pct(self, now_px: float, then_px: float) -> Optional[float]:
        try:
            if not (now_px > 0.0 and then_px > 0.0):
                return None
            return (float(now_px) / float(then_px) - 1.0) * 100.0
        except Exception:
            return None

    def _last_two_closes(self, rows: List[List[Any]]) -> Optional[Tuple[float, float]]:
        """
        From Kraken OHLC rows, return (prev_close, last_close) if possible.
        """
        if not rows or len(rows) < 2:
            return None
        closes: List[float] = []
        for r in rows:
            if not isinstance(r, list) or len(r) < 5:
                continue
            c = self._safe_float(r[4])
            if c is None or c <= 0:
                continue
            closes.append(float(c))
        if len(closes) < 2:
            return None
        return (closes[-2], closes[-1])

    def get_pct_changes_for_asset(self, asset: str, quote: str = "USD") -> Dict[str, Optional[float]]:
        """
        Venue-native percent changes for a base asset using Kraken OHLC intervals:
          1h -> interval=60
          1d -> interval=1440
          1w -> interval=10080

        Returns:
          {"change_1h": ..., "change_1d": ..., "change_1w": ..., "pair_used": "..."}
        """
        a = (asset or "").strip().upper()
        if not a:
            return {"change_1h": None, "change_1d": None, "change_1w": None, "pair_used": None}

        now = int(time.time())

        # Small lookbacks (3 candles worth) so we can compute prev->last.
        since_1h = now - int(3 * 3600)
        since_1d = now - int(3 * 86400)
        since_1w = now - int(3 * 7 * 86400)

        last_err: Optional[str] = None

        for pair_str in self._pair_candidates_for_asset(a, quote=quote):
            try:
                ch_1h = None
                ch_1d = None
                ch_1w = None

                r1 = self._public_ohlc(pair_str, interval_min=60, since_ts=since_1h)
                t2 = self._last_two_closes(r1)
                if t2:
                    prev_c, last_c = t2
                    ch_1h = self._pct(last_c, prev_c)

                r2 = self._public_ohlc(pair_str, interval_min=1440, since_ts=since_1d)
                t2 = self._last_two_closes(r2)
                if t2:
                    prev_c, last_c = t2
                    ch_1d = self._pct(last_c, prev_c)

                r3 = self._public_ohlc(pair_str, interval_min=10080, since_ts=since_1w)
                t2 = self._last_two_closes(r3)
                if t2:
                    prev_c, last_c = t2
                    ch_1w = self._pct(last_c, prev_c)

                # If we got at least one value, return.
                if (ch_1h is not None) or (ch_1d is not None) or (ch_1w is not None):
                    return {
                        "change_1h": ch_1h,
                        "change_1d": ch_1d,
                        "change_1w": ch_1w,
                        "pair_used": pair_str,
                    }
            except Exception as e:
                last_err = str(e)
                continue

        _ = last_err  # keep for debugging if you want to log later
        return {"change_1h": None, "change_1d": None, "change_1w": None, "pair_used": None}

    # ─────────────────────────────────────────────────────────────
    # Discovery: list symbols (canonical BASE-QUOTE)
    # ─────────────────────────────────────────────────────────────
    def list_symbols(self) -> List[str]:
        try:
            from ..services.symbol_policy import ALLOWED_QUOTES  # type: ignore
            allowed_quotes = {str(x).upper().strip() for x in (ALLOWED_QUOTES or set()) if str(x).strip()}
        except Exception:
            allowed_quotes = {"USD", "USDT", "USDC", "BTC", "ETH", "EUR"}

        try:
            ap = self._get_asset_pairs_result()
        except Exception:
            return sorted(set(self._map.keys()))

        out: Set[str] = set()

        for _, p in (ap or {}).items():
            if not isinstance(p, dict):
                continue

            status = str(p.get("status") or "").lower().strip()
            if status and status not in ("online", "active"):
                continue

            base = self._asset_code_to_canon(str(p.get("base") or "").strip())
            quote = self._asset_code_to_canon(str(p.get("quote") or "").strip())
            if not base or not quote:
                continue

            quote_u = quote.upper()
            if quote_u not in allowed_quotes:
                continue

            altname = str(p.get("altname") or "").strip()
            if "." in altname:
                continue

            out.add(f"{base.upper()}-{quote_u}")

        return sorted(out)

    # ─────────────────────────────────────────────────────────────
    # Symbol resolution
    # ─────────────────────────────────────────────────────────────
    def resolve_symbol(self, symbol_canon: str) -> str:
        s = (symbol_canon or "").strip()
        if not s:
            return s

        if s in self._map:
            return self._map[s]

        # Already looks like a Kraken pair token
        if "-" not in s and "/" not in s:
            return s.strip()

        # WS name style: XBT/USD -> XBTUSD
        if "/" in s and "-" not in s:
            return s.replace("/", "").strip()

        if "-" in s:
            base, quote = s.split("-", 1)
            base_u = (base or "").strip().upper()
            quote_u = (quote or "").strip().upper()

            try:
                ap = self._get_asset_pairs_result()
                cand = self._find_pair_string_for_canon(base_u, quote_u, ap)
                if cand:
                    return cand
            except Exception:
                pass

            b = self._canon_asset_to_kraken(base_u)
            q = self._canon_asset_to_kraken(quote_u)
            if b and q:
                return f"{b}{q}"

        return s

    def _normalize_pair_key(self, s: str) -> str:
        if not s:
            return ""
        x = str(s).strip().replace("/", "").replace("-", "").replace(" ", "")
        if "." in x:
            x = x.split(".", 1)[0]
        return x.upper()

    def _find_pair_string_for_canon(self, base_u: str, quote_u: str, ap: Dict[str, Any]) -> Optional[str]:
        best: Optional[str] = None
        for key, p in (ap or {}).items():
            if not isinstance(p, dict):
                continue
            status = str(p.get("status") or "").lower().strip()
            if status and status not in ("online", "active"):
                continue

            base = self._asset_code_to_canon(str(p.get("base") or "").strip()).upper()
            quote = self._asset_code_to_canon(str(p.get("quote") or "").strip()).upper()
            if base != base_u or quote != quote_u:
                continue

            altname = str(p.get("altname") or "").strip()
            wsname = str(p.get("wsname") or "").strip()
            candidates: List[str] = []
            if altname and "." not in altname:
                candidates.append(altname)
            if wsname and "." not in wsname:
                candidates.append(wsname)
            if key:
                candidates.append(str(key).strip())

            for c in candidates:
                cc = c.replace("/", "").replace("-", "").replace(" ", "").strip()
                if not cc:
                    continue
                if best is None or len(cc) < len(best):
                    best = cc
        return best

    def _find_assetpair_entry_for_pair(self, pair_str: str, ap: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        target = self._normalize_pair_key(pair_str)
        if not target:
            return None

        for key, p in (ap or {}).items():
            if not isinstance(p, dict):
                continue

            status = str(p.get("status") or "").lower().strip()
            if status and status not in ("online", "active"):
                continue

            altname = str(p.get("altname") or "").strip()
            wsname = str(p.get("wsname") or "").strip()
            candidates = [str(key or "").strip(), altname, wsname]
            for c in candidates:
                nk = self._normalize_pair_key(c)
                if nk and nk == target:
                    return p
        return None

    def _pair_rules(self, pair_str: str) -> Dict[str, Any]:
        try:
            ap = self._get_asset_pairs_result()
        except Exception:
            return {}

        entry = self._find_assetpair_entry_for_pair(pair_str, ap)
        if not isinstance(entry, dict):
            return {}

        out: Dict[str, Any] = {}
        for k in ("lot_decimals", "pair_decimals", "lot_multiplier", "ordermin", "costmin"):
            if k in entry:
                out[k] = entry.get(k)
        return out

    # ─────────────────────────────────────────────────────────────
    # Rules normalization (Phase 1)
    # ─────────────────────────────────────────────────────────────
    def get_order_rules(self, symbol_venue: str) -> OrderRules:
        pair_str = (symbol_venue or "").strip()
        if not pair_str:
            return {"symbol_venue": symbol_venue}

        rules = self._pair_rules(pair_str) or {}

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

        lot_decimals = None
        pair_decimals = None
        lot_multiplier = None
        try:
            if rules.get("lot_decimals") is not None:
                lot_decimals = int(str(rules.get("lot_decimals")).strip())
        except Exception:
            lot_decimals = None
        try:
            if rules.get("pair_decimals") is not None:
                pair_decimals = int(str(rules.get("pair_decimals")).strip())
        except Exception:
            pair_decimals = None
        try:
            if rules.get("lot_multiplier") is not None:
                lot_multiplier = int(str(rules.get("lot_multiplier")).strip())
        except Exception:
            lot_multiplier = None

        base_inc: Optional[float] = None
        try:
            if lot_decimals is not None and lot_decimals >= 0:
                base_inc = float(Decimal(10) ** Decimal(-lot_decimals))
            elif lot_multiplier is not None and lot_multiplier > 0:
                base_inc = float(Decimal(1) / Decimal(lot_multiplier))
        except Exception:
            base_inc = None

        price_inc: Optional[float] = None
        try:
            if pair_decimals is not None and pair_decimals >= 0:
                price_inc = float(Decimal(10) ** Decimal(-pair_decimals))
        except Exception:
            price_inc = None

        return {
            "symbol_venue": pair_str,
            "base_increment": base_inc,
            "price_increment": price_inc,
            "qty_decimals": lot_decimals,
            "price_decimals": pair_decimals,
            "min_qty": f(rules.get("ordermin")),
            "max_qty": None,
            "min_notional": f(rules.get("costmin")),
            "max_notional": None,
            "supports_post_only": True,
            "supported_tifs": ["gtc", "ioc", "fok"],
            "supported_order_types": ["limit", "market"],
            "raw": rules if isinstance(rules, dict) else {},
        }

    # ─────────────────────────────────────────────────────────────
    # Decimal helpers (precision-safe flooring)
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

    def _floor_to_decimals(self, value: Decimal, decimals: Optional[int]) -> Decimal:
        if decimals is None:
            return value
        try:
            n = int(decimals)
        except Exception:
            return value
        if n < 0:
            return value
        step = Decimal(10) ** Decimal(-n)
        return self._floor_to_step(value, step)

    # ─────────────────────────────────────────────────────────────
    # Private HTTP (balances/orders/trading)
    # ─────────────────────────────────────────────────────────────
    def _require_creds(self) -> Tuple[str, str]:
        key = getattr(settings, "kraken_api_key", None) or os.getenv("KRAKEN_API_KEY")
        sec = getattr(settings, "kraken_api_secret", None) or os.getenv("KRAKEN_API_SECRET")
        key = (key or "").strip()
        sec = (sec or "").strip()
        if not key or not sec:
            raise Exception("Missing Kraken credentials: set KRAKEN_API_KEY and KRAKEN_API_SECRET")
        return key, sec

    def _api_sign(self, urlpath: str, data: Dict[str, str], secret_b64: str) -> str:
        postdata = urllib.parse.urlencode(data)
        encoded = (str(data["nonce"]) + postdata).encode()
        message = urlpath.encode() + hashlib.sha256(encoded).digest()
        mac = hmac.new(base64.b64decode(secret_b64), message, hashlib.sha512)
        sigdigest = base64.b64encode(mac.digest())
        return sigdigest.decode()

    def _private_post(self, method: str, data: Dict[str, str]) -> Dict[str, Any]:
        key, sec = self._require_creds()
        urlpath = f"/0/private/{method}"
        url = f"https://api.kraken.com{urlpath}"

        data2 = dict(data or {})
        data2["nonce"] = str(int(time.time() * 1000))

        headers = {
            "API-Key": key,
            "API-Sign": self._api_sign(urlpath, data2, sec),
        }

        with httpx.Client(timeout=30.0) as client:
            r = client.post(url, headers=headers, data=data2)
            r.raise_for_status()
            resp = r.json() if r.content else {}

        if not isinstance(resp, dict):
            raise Exception(f"Kraken private POST unexpected type: {type(resp)}")
        if resp.get("error"):
            raise Exception(f"Kraken private POST error: {resp.get('error')}")
        return resp

    # ─────────────────────────────────────────────────────────────
    # Transfers (deposits/withdrawals) for SAFE ingestion
    # ─────────────────────────────────────────────────────────────

    def _canon_asset_for_status(self, asset: str) -> str:
        """Kraken DepositStatus/WithdrawStatus expects Kraken-style asset codes (XBT, XDG)."""
        return self._canon_asset_to_kraken((asset or "").strip().upper())

    def _status_from_transfer(self, st: Any) -> str:
        """Normalize Kraken transfer statuses into an upper string for storage."""
        s = str(st or "").strip()
        return s.upper() if s else "UNKNOWN"

    def _ts_ms_from_time(self, t: Any) -> int:
        """Kraken status endpoints use seconds (float); store as int milliseconds."""
        try:
            return int(float(t) * 1000.0)
        except Exception:
            return int(time.time() * 1000)

    def _eid_from_row(self, row: Dict[str, Any], prefix: str) -> str:
        """
        Stable idempotency component. Prefer Kraken 'refid', else 'txid',
        else deterministic-ish fallback.
        """
        refid = row.get("refid")
        if refid:
            return str(refid)
        txid = row.get("txid")
        if txid:
            return str(txid)
        return f"{prefix}:{row.get('time')}:{row.get('amount')}:{row.get('asset')}"

    def _list_assets_for_backfill(self) -> List[str]:
        """
        If Kraken requires 'asset' param for DepositStatus/WithdrawStatus,
        enumerate canonical assets from Balance().
        """
        try:
            resp = self._private_post("Balance", {})
            result = resp.get("result") or {}
            if isinstance(result, dict):
                out: List[str] = []
                for k in result.keys():
                    a = self._asset_code_to_canon(str(k)).upper()
                    if a:
                        out.append(a)
                return sorted(set(out))
        except Exception:
            pass
        return []

    def _try_status_call(self, method: str, asset: Optional[str]) -> List[Dict[str, Any]]:
        """Call Kraken DepositStatus/WithdrawStatus; return list of dict rows."""
        data: Dict[str, str] = {}
        if asset:
            data["asset"] = self._canon_asset_for_status(asset)

        resp = self._private_post(method, data)
        rows = (resp.get("result") or [])
        if isinstance(rows, list):
            return [r for r in rows if isinstance(r, dict)]
        return []

    def fetch_transfers(
        self,
        *,
        since_dt: datetime,
        kinds: Optional[List[str]] = None,
        currency: Optional[str] = None,
        limit_transfers: int = 50,
        max_pages: int = 200,
    ) -> List[Dict[str, Any]]:
        """
        Read-only transfer ingest via Kraken private DepositStatus/WithdrawStatus.

        Returns canonical dicts matching deposits.py / withdrawals.py ingest expectations:
          - type: "Deposit" | "Withdrawal"
          - eid: stable per-row id (refid preferred)
          - currency: canonical asset (BTC, DOGE, USD, etc.)
          - amount: float (router uses type to choose table)
          - timestampms: int ms
          - status: string
          - txHash: txid if present else None
          - destination: best-effort (withdrawals only)
        """
        _ = max_pages  # non-paginated endpoints; kept for signature parity

        want = {str(x).strip().lower() for x in (kinds or []) if str(x).strip()}
        if not want:
            want = {"deposit", "withdrawal"}

        # NOTE: In "all" mode routers may pass a very early since_dt (e.g. datetime.min).
        # On some platforms (notably Windows), datetime.timestamp() for pre-1970 values can
        # raise OSError: [Errno 22] Invalid argument. Clamp to epoch start.
        try:
            since_ts = float(since_dt.replace(tzinfo=None).timestamp())
        except (OSError, OverflowError, ValueError):
            since_ts = 0.0
        if since_ts < 0:
            since_ts = 0.0
        cur = (currency or "").strip().upper() or None

        out: List[Dict[str, Any]] = []
        seen_keys: Set[str] = set()

        def keep_row(kind: str, row: Dict[str, Any]) -> bool:
            try:
                t = float(row.get("time") or 0.0)
            except Exception:
                t = 0.0
            if t and t < since_ts:
                return False
            if cur:
                a = self._asset_code_to_canon(str(row.get("asset") or "")).upper()
                if a != cur:
                    return False
            return True

        def push(kind: str, row: Dict[str, Any]) -> None:
            eid = self._eid_from_row(row, kind)
            k = f"{kind}:{eid}"
            if k in seen_keys:
                return
            seen_keys.add(k)

            asset = self._asset_code_to_canon(str(row.get("asset") or "")).upper()
            if not asset:
                return

            try:
                amt = float(row.get("amount"))
            except Exception:
                return

            txid = row.get("txid") or None
            tx_hash = str(txid).strip() if txid is not None and str(txid).strip() else None

            base: Dict[str, Any] = {
                "eid": eid,
                "currency": asset,
                "amount": amt,
                "timestampms": self._ts_ms_from_time(row.get("time")),
                "status": self._status_from_transfer(row.get("status")),
                "txHash": tx_hash,
                "raw_kraken": dict(row),
            }

            if kind == "deposit":
                base["type"] = "Deposit"
            else:
                base["type"] = "Withdrawal"
                dest = row.get("info") or row.get("key") or row.get("address") or row.get("dest") or None
                if dest is not None:
                    try:
                        ds = str(dest).strip()
                        base["destination"] = ds if ds else None
                    except Exception:
                        base["destination"] = None

            out.append(base)

        assets_to_try: List[Optional[str]] = [cur] if cur else [None]

        def run_kind(kind: str) -> None:
            method = "DepositStatus" if kind == "deposit" else "WithdrawStatus"

            rows: List[Dict[str, Any]] = []
            tried = False

            # First: try no-asset (or the requested currency)
            for a in assets_to_try:
                try:
                    tried = True
                    rows = self._try_status_call(method, a)
                    if rows:
                        break
                except Exception:
                    rows = []

            # Fallback: enumerate assets when no currency filter is supplied
            if (not rows) and (not cur):
                for a2 in self._list_assets_for_backfill():
                    try:
                        rows2 = self._try_status_call(method, a2)
                    except Exception:
                        continue
                    for r in rows2:
                        if isinstance(r, dict) and keep_row(kind, r):
                            push(kind, r)
                return

            if not tried:
                return

            for r in rows:
                if isinstance(r, dict) and keep_row(kind, r):
                    push(kind, r)

        if "deposit" in want:
            run_kind("deposit")
        if "withdrawal" in want:
            run_kind("withdrawal")

        out.sort(key=lambda x: int(x.get("timestampms") or 0), reverse=True)
        if limit_transfers and limit_transfers > 0:
            out = out[: int(limit_transfers)]

        return out


    # ─────────────────────────────────────────────────────────────
    # Trading (REAL)
    # ─────────────────────────────────────────────────────────────
    def _apply_kraken_rules(
        self,
        *,
        pair_str: str,
        side_l: str,
        type_l: str,
        qty: float,
        limit_price: Optional[float],
        dry_run: bool,
    ) -> Tuple[str, Optional[str]]:
        enforce_precision = self._env_bool("KRAKEN_ENFORCE_PRECISION", True)
        enforce_ordermin = self._env_bool("KRAKEN_ENFORCE_ORDERMIN", True)
        enforce_costmin = self._env_bool("KRAKEN_ENFORCE_COSTMIN", True)

        qd = self._dec(qty)
        if qd is None or qd <= 0:
            raise Exception("Kraken place_order: qty must be > 0")

        pd = self._dec(limit_price) if limit_price is not None else None
        if type_l == "limit":
            if pd is None or pd <= 0:
                raise Exception("Kraken place_order: limit_price is required and must be > 0 for limit orders")

        rules = self._pair_rules(pair_str) or {}

        lot_decimals = None
        pair_decimals = None
        lot_multiplier = None
        try:
            if rules.get("lot_decimals") is not None:
                lot_decimals = int(str(rules.get("lot_decimals")).strip())
        except Exception:
            lot_decimals = None
        try:
            if rules.get("pair_decimals") is not None:
                pair_decimals = int(str(rules.get("pair_decimals")).strip())
        except Exception:
            pair_decimals = None
        try:
            if rules.get("lot_multiplier") is not None:
                lot_multiplier = int(str(rules.get("lot_multiplier")).strip())
        except Exception:
            lot_multiplier = None

        q2 = qd
        if enforce_precision:
            if lot_multiplier is not None and lot_multiplier > 1:
                step = Decimal(1) / Decimal(lot_multiplier)
                q2 = self._floor_to_step(q2, step)
            q2 = self._floor_to_decimals(q2, lot_decimals)

        if q2 <= 0:
            raise Exception("Kraken place_order: qty rounds to 0 under current pair rules")

        p2 = pd
        if type_l == "limit" and p2 is not None and enforce_precision:
            p2 = self._floor_to_decimals(p2, pair_decimals)
            if p2 <= 0:
                raise Exception("Kraken place_order: limit_price rounds to 0 under current pair rules")

        if enforce_ordermin:
            omin = self._dec(rules.get("ordermin"))
            if omin is not None and omin > 0 and q2 < omin:
                raise Exception(
                    f"Kraken minimum volume not met for {pair_str}: "
                    f"qty={self._fmt_decimal(q2)} < ordermin={self._fmt_decimal(omin)}"
                )

        if enforce_costmin:
            cmin = self._dec(rules.get("costmin"))
            if cmin is not None and cmin > 0:
                eff_price: Optional[Decimal] = None
                if type_l == "limit":
                    eff_price = p2
                else:
                    try:
                        book = self.fetch_orderbook(pair_str, depth=1, dry_run=dry_run)
                        bids = book.get("bids") or []
                        asks = book.get("asks") or []
                        if side_l == "buy":
                            if asks and isinstance(asks[0], dict):
                                eff_price = self._dec(asks[0].get("price"))
                        else:
                            if bids and isinstance(bids[0], dict):
                                eff_price = self._dec(bids[0].get("price"))
                    except Exception:
                        eff_price = None

                if eff_price is None or eff_price <= 0:
                    raise Exception(
                        f"Kraken costmin enforcement requires a price estimate for {pair_str}. "
                        f"Try a limit order, or set KRAKEN_ENFORCE_COSTMIN=0 to bypass."
                    )

                notional = q2 * eff_price
                if notional < cmin:
                    raise Exception(
                        f"Kraken minimum notional not met for {pair_str}: "
                        f"cost={self._fmt_decimal(notional)} < costmin={self._fmt_decimal(cmin)} "
                        f"(qty={self._fmt_decimal(q2)} price≈{self._fmt_decimal(eff_price)})"
                    )

        return (self._fmt_decimal(q2), self._fmt_decimal(p2) if p2 is not None else None)

    def _userref_from_client_oid(self, client_order_id: str) -> int:
        if not client_order_id:
            return 0
        h = hashlib.sha256(client_order_id.encode("utf-8")).digest()
        n = int.from_bytes(h[:4], "big", signed=False)
        return int(n & 0x7FFFFFFF)

    def _map_tif(self, tif: Optional[str]) -> Optional[str]:
        if not tif:
            return None
        t = str(tif).strip().lower()
        if t in ("gtc", "good_till_cancelled", "good-til-cancelled", "goodtilcancelled"):
            return None  # Kraken default
        if t in ("ioc", "immediate_or_cancel", "immediate-or-cancel"):
            return "IOC"
        if t in ("fok", "fill_or_kill", "fill-or-kill"):
            return "FOK"
        return None

    def _query_order_once(self, txid: str) -> Optional[Dict[str, Any]]:
        """
        Best-effort confirm of an order immediately after AddOrder.
        Returns the Kraken order dict for txid if present.
        """
        if not txid:
            return None
        try:
            resp = self._private_post("QueryOrders", {"txid": str(txid), "trades": "true"})
            result = resp.get("result") or {}
            if not isinstance(result, dict):
                return None
            row = result.get(str(txid))
            if isinstance(row, dict):
                return row
            for k, v in result.items():
                if str(k) == str(txid) and isinstance(v, dict):
                    return v
            return None
        except Exception:
            return None

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
            raise Exception(f"Kraken place_order: invalid side '{side}' (expected buy/sell)")

        t_l = (type_ or "").lower().strip()
        if t_l not in ("limit", "market"):
            raise Exception(f"Kraken place_order: invalid type '{type_}' (expected limit/market)")

        pair_str = str(symbol_venue or "").strip()
        if not pair_str:
            raise Exception("Kraken place_order: missing symbol_venue/pair")

        vol_str, price_str = self._apply_kraken_rules(
            pair_str=pair_str,
            side_l=side_l,
            type_l=t_l,
            qty=qty,
            limit_price=limit_price,
            dry_run=dry_run,
        )

        payload: Dict[str, str] = {
            "pair": pair_str,
            "type": side_l,
            "ordertype": t_l,
            "volume": vol_str,
            "userref": str(self._userref_from_client_oid(client_order_id)),
        }

        tif_mapped = self._map_tif(tif)
        if tif_mapped:
            payload["timeinforce"] = tif_mapped

        if post_only:
            payload["oflags"] = "post"

        if t_l == "limit":
            if not price_str:
                raise Exception("Kraken place_order: missing price after rule application")
            payload["price"] = price_str

        resp = self._private_post("AddOrder", payload)
        result = resp.get("result") or {}
        if not isinstance(result, dict):
            raise Exception(f"Kraken AddOrder unexpected result shape: {result}")

        txid = result.get("txid")
        venue_order_id = None
        if isinstance(txid, list) and txid:
            venue_order_id = str(txid[0])
        elif isinstance(txid, str) and txid:
            venue_order_id = txid

        if not venue_order_id:
            raise Exception(f"Kraken AddOrder missing txid. result keys={list(result.keys())}")

        status = "open"
        raw_status = "submitted"

        confirm_attempts = int(os.getenv("KRAKEN_SUBMIT_CONFIRM_ATTEMPTS", "4"))
        confirm_sleep_s = float(os.getenv("KRAKEN_SUBMIT_CONFIRM_SLEEP_S", "0.25"))

        for _ in range(max(0, confirm_attempts)):
            row = self._query_order_once(venue_order_id)
            if not row:
                time.sleep(max(0.0, confirm_sleep_s))
                continue

            kraken_st = str(row.get("status") or "").strip().lower()
            status = self._normalize_kraken_status(kraken_st, is_open_hint=True)
            raw_status = kraken_st or raw_status

            if status in ("open", "filled", "canceled", "rejected"):
                break

            time.sleep(max(0.0, confirm_sleep_s))

        return {"venue_order_id": venue_order_id, "status": status, "raw_status": raw_status}

    def cancel_order(self, venue_order_id: str, dry_run: bool) -> bool:
        if dry_run:
            return True
        if not venue_order_id:
            return False

        resp = self._private_post("CancelOrder", {"txid": str(venue_order_id)})
        result = resp.get("result") or {}
        if not isinstance(result, dict):
            return True
        try:
            count = int(result.get("count", 0))
            return count > 0
        except Exception:
            return True

    # ─────────────────────────────────────────────────────────────
    # Balances (private) – totals + best-effort holds from OpenOrders
    # ─────────────────────────────────────────────────────────────
    def fetch_balances(self, dry_run: bool) -> List[BalanceItem]:
        # IMPORTANT: dry_run should NOT disable read-only ingestion.
        resp = self._private_post("Balance", {})
        result = resp.get("result") or {}
        if not isinstance(result, dict):
            return []

        # Best-effort holds derived from open orders.
        holds: Dict[str, Decimal] = {}

        def d0(x: Any) -> Decimal:
            d = self._dec(x)
            return d if d is not None else Decimal("0")

        try:
            ap = self._get_asset_pairs_result()
        except Exception:
            ap = {}

        try:
            ro = self._private_post("OpenOrders", {"trades": "true"})
            open_map = (ro.get("result") or {}).get("open") or {}
            if isinstance(open_map, dict):
                for _, o in open_map.items():
                    if not isinstance(o, dict):
                        continue
                    descr = o.get("descr") or {}
                    if not isinstance(descr, dict):
                        descr = {}

                    pair_raw = str(descr.get("pair") or "").strip()
                    pair_str = pair_raw.replace("/", "").replace("-", "").strip()
                    if not pair_str:
                        continue

                    entry = self._find_assetpair_entry_for_pair(pair_str, ap) if isinstance(ap, dict) else None
                    base_canon = None
                    quote_canon = None
                    if isinstance(entry, dict):
                        base_canon = self._asset_code_to_canon(str(entry.get("base") or "").strip()).upper()
                        quote_canon = self._asset_code_to_canon(str(entry.get("quote") or "").strip()).upper()

                    side = str(descr.get("type") or "").strip().lower()

                    vol = d0(o.get("vol"))
                    vol_exec = d0(o.get("vol_exec"))
                    rem_base = vol - vol_exec
                    if rem_base < 0:
                        rem_base = Decimal("0")

                    if side == "sell":
                        if base_canon:
                            holds[base_canon] = holds.get(base_canon, Decimal("0")) + rem_base
                        continue

                    if side == "buy":
                        # Prefer explicit cost remaining if Kraken provides it.
                        cost = self._dec(o.get("cost"))
                        cost_exec = self._dec(o.get("cost_exec"))
                        rem_quote: Optional[Decimal] = None
                        if cost is not None and cost_exec is not None:
                            rem_quote = cost - cost_exec
                        if rem_quote is None or rem_quote <= 0:
                            px = self._dec(descr.get("price")) or self._dec(o.get("price"))
                            if px is not None and px > 0:
                                rem_quote = rem_base * px

                        if rem_quote is not None and rem_quote > 0 and quote_canon:
                            holds[quote_canon] = holds.get(quote_canon, Decimal("0")) + rem_quote
                        continue
        except Exception:
            # If OpenOrders fails, we still return totals (holds stay zero).
            pass

        out: List[BalanceItem] = []
        for k, v in result.items():
            asset = self._asset_code_to_canon(str(k)).upper()
            total_d = d0(v)
            hold_d = holds.get(asset, Decimal("0"))
            if hold_d < 0:
                hold_d = Decimal("0")
            avail_d = total_d - hold_d
            if avail_d < 0:
                avail_d = Decimal("0")

            out.append(
                {
                    "asset": asset,
                    "total": float(total_d),
                    "available": float(avail_d),
                    "hold": float(hold_d),
                }
            )

        # Include any holds for assets not present in Balance (rare), for visibility.
        for asset, hold_d in holds.items():
            if hold_d <= 0:
                continue
            if not any((x.get("asset") or "") == asset for x in out):
                out.append({"asset": asset, "total": 0.0, "available": 0.0, "hold": float(hold_d)})

        out.sort(key=lambda x: str(x.get("asset") or ""))
        return out

    # ─────────────────────────────────────────────────────────────
    # Orders (private) – best-effort (open + closed)
    # ─────────────────────────────────────────────────────────────
    def _canon_from_pair_str(self, pair_str: str) -> Optional[str]:
        """
        Attempt to convert a Kraken pair token into canonical BASE-QUOTE by scanning AssetPairs.
        This is best-effort; if it fails we return None.
        """
        ps = self._normalize_pair_key(pair_str)
        if not ps:
            return None
        try:
            ap = self._get_asset_pairs_result()
        except Exception:
            return None

        entry = self._find_assetpair_entry_for_pair(ps, ap)
        if not isinstance(entry, dict):
            return None

        base = self._asset_code_to_canon(str(entry.get("base") or "").strip())
        quote = self._asset_code_to_canon(str(entry.get("quote") or "").strip())
        if not base or not quote:
            return None
        return f"{base.upper()}-{quote.upper()}"

    def fetch_orders(self, dry_run: bool) -> List[VenueOrder]:
        # IMPORTANT: dry_run should NOT disable read-only ingestion.
        out: List[VenueOrder] = []
        seen: Set[str] = set()

        # OpenOrders
        try:
            ro = self._private_post("OpenOrders", {"trades": "true"})
            res = (ro.get("result") or {}).get("open") or {}
            if isinstance(res, dict):
                for oid, o in res.items():
                    if oid in seen or not isinstance(o, dict):
                        continue
                    seen.add(oid)
                    out.append(self._map_order_row(oid, o, is_open=True))
        except Exception:
            pass

        # ClosedOrders
        try:
            rc = self._private_post("ClosedOrders", {"trades": "true"})
            res = (rc.get("result") or {}).get("closed") or {}
            if isinstance(res, dict):
                for oid, o in res.items():
                    if oid in seen or not isinstance(o, dict):
                        continue
                    seen.add(oid)
                    out.append(self._map_order_row(oid, o, is_open=False))
        except Exception:
            pass

        return [x for x in out if isinstance(x, dict)]

    def _map_order_row(self, oid: str, o: Dict[str, Any], is_open: bool) -> VenueOrder:
        descr = o.get("descr") or {}
        if not isinstance(descr, dict):
            descr = {}

        pair = str(descr.get("pair") or "").strip()
        symbol_venue = pair.replace("/", "").replace("-", "").strip() if pair else ""
        symbol_canon = self._canon_from_pair_str(symbol_venue) or None

        side = str(descr.get("type") or "").strip().lower() or None
        typ = str(descr.get("ordertype") or "").strip().lower() or None

        vol = self._safe_float(o.get("vol"))
        vol_exec = self._safe_float(o.get("vol_exec"))
        price = self._safe_float(descr.get("price")) or self._safe_float(o.get("price"))
        avg_price = self._safe_float(o.get("price"))  # Kraken sometimes uses 'price' as avg
        fee = self._safe_float(o.get("fee"))
        cost = self._safe_float(o.get("cost"))

        created_at = self._dt_from_s(o.get("opentm"))
        updated_at = self._dt_from_s(o.get("closetm")) if o.get("closetm") is not None else None

        st = str(o.get("status") or "").strip().lower()
        status = self._normalize_kraken_status(st, is_open_hint=is_open)

        total_after_fee = None
        if isinstance(cost, (int, float)) and cost is not None:
            if fee is not None:
                if side == "buy":
                    total_after_fee = float(cost) + float(fee)
                elif side == "sell":
                    total_after_fee = float(cost) - float(fee)
            else:
                total_after_fee = float(cost)

        return {
            "venue": self.venue,
            "venue_order_id": str(oid),
            "symbol_venue": symbol_venue,
            "symbol_canon": symbol_canon,
            "side": side,
            "type": typ,
            "status": status,
            "qty": vol,
            "filled_qty": vol_exec,
            "limit_price": price if typ == "limit" else None,
            "avg_fill_price": avg_price if (avg_price is not None and avg_price > 0) else None,
            "fee": fee,
            "fee_asset": None,
            "total_after_fee": total_after_fee,
            "created_at": created_at,
            "updated_at": updated_at,
        }

    # ─────────────────────────────────────────────────────────────
    # Public order book
    # ─────────────────────────────────────────────────────────────
    def fetch_orderbook(self, symbol_venue: str, depth: int, dry_run: bool) -> OrderBook:
        pair_str = (symbol_venue or "").strip()
        if not pair_str:
            return {"bids": [], "asks": []}

        data = self._public_get("/public/Depth", params={"pair": pair_str, "count": str(int(depth))})
        res = data.get("result") or {}
        if not isinstance(res, dict) or not res:
            return {"bids": [], "asks": []}

        first_key = next(iter(res.keys()))
        book = res.get(first_key) or {}
        if not isinstance(book, dict):
            return {"bids": [], "asks": []}

        bids_raw = (book.get("bids") or [])[:depth]
        asks_raw = (book.get("asks") or [])[:depth]

        bids: List[Dict[str, float]] = []
        for b in bids_raw:
            try:
                if isinstance(b, list) and len(b) >= 2:
                    bids.append({"price": float(b[0]), "qty": float(b[1])})
            except Exception:
                continue

        asks: List[Dict[str, float]] = []
        for a in asks_raw:
            try:
                if isinstance(a, list) and len(a) >= 2:
                    asks.append({"price": float(a[0]), "qty": float(a[1])})
            except Exception:
                continue

        return {"bids": bids, "asks": asks}
