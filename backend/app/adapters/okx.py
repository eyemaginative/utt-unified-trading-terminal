# backend/app/adapters/okx.py
#
# OKX API v5 adapter.
# Venue key: "okx"
#
# First integration pass is read-only focused:
# - public instruments / orderbook / rules
# - private balances
# - basic read-only orders snapshot support
# - order placement remains disabled unless explicitly enabled in a later OKX.5 patch.

from __future__ import annotations

from typing import List, Optional, Dict, Any, Tuple, Set
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import base64
import hashlib
import hmac
import json
import os
import time
from urllib.parse import urlencode

import httpx

from .base import ExchangeAdapter, PlacedOrder, BalanceItem, OrderBook, VenueOrder, OrderRules
from ..config import settings


class OKXAdapter(ExchangeAdapter):
    venue = "okx"

    _INSTRUMENTS_CACHE_TTL_S: float = float(os.getenv("OKX_INSTRUMENTS_CACHE_TTL_S", "300"))
    _instruments_cache_ts: float = 0.0
    _instruments_cache: Optional[List[Dict[str, Any]]] = None
    _inst_by_id: Dict[str, Dict[str, Any]] = {}
    _canon_to_inst: Dict[str, str] = {}

    _TERMINAL = {"filled", "canceled", "cancelled", "rejected", "done", "closed", "expired", "failed"}

    def _base_url(self) -> str:
        fn = getattr(settings, "okx_effective_base_url", None)
        if callable(fn):
            try:
                u = fn()
            except Exception:
                u = None
        else:
            u = None
        if not u:
            u = getattr(settings, "okx_base_url", None) or os.getenv("OKX_BASE_URL") or "https://us.okx.com"
        u = str(u or "").strip().rstrip("/")
        if u.endswith("/api/v5"):
            u = u[: -len("/api/v5")]
        return u or "https://us.okx.com"

    def _api_url(self, path: str) -> str:
        p = str(path or "").strip()
        if not p.startswith("/"):
            p = "/" + p
        return f"{self._base_url()}{p}"

    @staticmethod
    def _safe_float(x: Any) -> Optional[float]:
        try:
            if x is None:
                return None
            s = str(x).strip()
            if not s:
                return None
            return float(s)
        except Exception:
            return None

    @staticmethod
    def _dt_from_ms(ms: Any) -> Optional[datetime]:
        try:
            if ms is None:
                return None
            m = float(ms)
            return datetime.fromtimestamp(m / 1000.0, tz=timezone.utc).replace(tzinfo=None)
        except Exception:
            return None

    @staticmethod
    def _decimals_from_step(x: Any) -> Optional[int]:
        if x is None:
            return None
        try:
            d = Decimal(str(x).strip())
        except (InvalidOperation, ValueError, TypeError):
            return None
        if d <= 0:
            return None
        d = d.normalize()
        exp = d.as_tuple().exponent
        return int(-exp) if exp < 0 else 0

    def _require_creds(self) -> Tuple[str, str, str]:
        key = secret = passphrase = ""

        fn = getattr(settings, "okx_private_creds", None)
        if callable(fn):
            try:
                vc = fn()
            except Exception:
                vc = None
            if isinstance(vc, (list, tuple)) and len(vc) >= 3:
                key = str(vc[0] or "").strip()
                secret = str(vc[1] or "").strip()
                passphrase = str(vc[2] or "").strip()

        if not (key and secret and passphrase):
            key = (getattr(settings, "okx_api_key", None) or os.getenv("OKX_API_KEY") or "").strip()
            secret = (getattr(settings, "okx_api_secret", None) or os.getenv("OKX_API_SECRET") or "").strip()
            passphrase = (getattr(settings, "okx_api_passphrase", None) or os.getenv("OKX_API_PASSPHRASE") or "").strip()

        if not (key and secret and passphrase):
            raise RuntimeError(
                "Missing OKX credentials: save venue='okx' in Profile → API Keys "
                "or set OKX_API_KEY, OKX_API_SECRET, and OKX_API_PASSPHRASE."
            )
        return key, secret, passphrase

    def _timestamp(self) -> str:
        return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def _request_path(self, path: str, params: Optional[Dict[str, Any]] = None) -> str:
        p = str(path or "").strip()
        if not p.startswith("/"):
            p = "/" + p
        if params:
            clean = {str(k): str(v) for k, v in (params or {}).items() if v is not None and str(v) != ""}
            if clean:
                p = f"{p}?{urlencode(clean)}"
        return p

    def _sign(self, *, timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
        prehash = f"{timestamp}{method.upper()}{request_path}{body or ''}"
        digest = hmac.new(secret.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
        return base64.b64encode(digest).decode("utf-8")

    def _headers(self, *, method: str, request_path: str, body: str = "") -> Dict[str, str]:
        key, secret, passphrase = self._require_creds()
        ts = self._timestamp()
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "OK-ACCESS-KEY": key,
            "OK-ACCESS-SIGN": self._sign(timestamp=ts, method=method, request_path=request_path, body=body, secret=secret),
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": passphrase,
        }

    def _check_okx_response(self, data: Any, *, op: str) -> Dict[str, Any]:
        if not isinstance(data, dict):
            raise RuntimeError(f"OKX {op} unexpected response type: {type(data).__name__}")
        code = str(data.get("code", "0"))
        if code != "0":
            msg = data.get("msg") or data.get("message") or ""
            raise RuntimeError(f"OKX {op} error code={code} msg={msg}")
        return data

    def _public_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        request_path = self._request_path(path, params)
        url = self._api_url(request_path)
        with httpx.Client(timeout=20.0) as client:
            r = client.get(url, headers={"Accept": "application/json"})
            r.raise_for_status()
            data = r.json() if r.content else {}
        return self._check_okx_response(data, op=f"public GET {path}")

    def _private_get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        request_path = self._request_path(path, params)
        url = self._api_url(request_path)
        headers = self._headers(method="GET", request_path=request_path, body="")
        with httpx.Client(timeout=30.0) as client:
            r = client.get(url, headers=headers)
            r.raise_for_status()
            data = r.json() if r.content else {}
        return self._check_okx_response(data, op=f"private GET {path}")

    # ─────────────────────────────────────────────────────────────
    # Instruments / symbols
    # ─────────────────────────────────────────────────────────────
    @staticmethod
    def _canon(base: str, quote: str) -> str:
        return f"{str(base or '').strip().upper()}-{str(quote or '').strip().upper()}"

    def _allowed_quotes(self) -> Set[str]:
        try:
            from ..services.symbol_policy import ALLOWED_QUOTES  # type: ignore
            out = {str(x or "").strip().upper() for x in ALLOWED_QUOTES if str(x or "").strip()}
            return out or {"USD", "USDT", "USDC", "BTC"}
        except Exception:
            return {"USD", "USDT", "USDC", "BTC"}

    def _ensure_instruments(self, force: bool = False) -> List[Dict[str, Any]]:
        now = time.time()
        if (
            not force
            and OKXAdapter._instruments_cache is not None
            and (now - OKXAdapter._instruments_cache_ts) <= OKXAdapter._INSTRUMENTS_CACHE_TTL_S
        ):
            return OKXAdapter._instruments_cache or []

        data = self._public_get("/api/v5/public/instruments", params={"instType": "SPOT"})
        rows = data.get("data") or []
        if not isinstance(rows, list):
            rows = []

        inst_by_id: Dict[str, Dict[str, Any]] = {}
        canon_to_inst: Dict[str, str] = {}
        for r in rows:
            if not isinstance(r, dict):
                continue
            inst_id = str(r.get("instId") or "").strip().upper()
            base = str(r.get("baseCcy") or "").strip().upper()
            quote = str(r.get("quoteCcy") or "").strip().upper()
            state = str(r.get("state") or "").strip().lower()
            if not inst_id or not base or not quote:
                continue
            if state and state not in {"live", "trading", "active"}:
                continue
            inst_by_id[inst_id] = r
            canon_to_inst[self._canon(base, quote)] = inst_id

        OKXAdapter._instruments_cache = rows
        OKXAdapter._instruments_cache_ts = now
        OKXAdapter._inst_by_id = inst_by_id
        OKXAdapter._canon_to_inst = canon_to_inst
        return rows

    def list_symbols(self) -> List[str]:
        allowed_quotes = self._allowed_quotes()
        rows = self._ensure_instruments()
        out: List[str] = []
        seen: Set[str] = set()
        for r in rows:
            if not isinstance(r, dict):
                continue
            base = str(r.get("baseCcy") or "").strip().upper()
            quote = str(r.get("quoteCcy") or "").strip().upper()
            if not base or not quote or quote not in allowed_quotes:
                continue
            canon = self._canon(base, quote)
            if canon not in seen:
                seen.add(canon)
                out.append(canon)
        return sorted(out)

    def resolve_symbol(self, symbol_canon: str) -> str:
        s = str(symbol_canon or "").strip().upper()
        if not s:
            return ""
        self._ensure_instruments()
        if s in OKXAdapter._inst_by_id:
            return s
        hit = OKXAdapter._canon_to_inst.get(s)
        if hit:
            return hit
        # If caller passes BASE only, prefer USD then USDT/USDC quotes if available.
        if "-" not in s:
            for q in ("USD", "USDT", "USDC"):
                hit = OKXAdapter._canon_to_inst.get(f"{s}-{q}")
                if hit:
                    return hit
        # OKX spot instrument ids are already BASE-QUOTE; preserve inspectability if unsupported.
        return s

    def _inst_row(self, inst_id: str) -> Dict[str, Any]:
        self._ensure_instruments()
        return OKXAdapter._inst_by_id.get(str(inst_id or "").strip().upper(), {})

    # ─────────────────────────────────────────────────────────────
    # ExchangeAdapter methods
    # ─────────────────────────────────────────────────────────────
    def fetch_orderbook(self, symbol_venue: str, depth: int, dry_run: bool) -> OrderBook:
        _ = dry_run
        inst_id = self.resolve_symbol(symbol_venue)
        if not inst_id:
            return {"bids": [], "asks": []}
        d = max(1, min(400, int(depth or 25)))
        data = self._public_get("/api/v5/market/books", params={"instId": inst_id, "sz": str(d)})
        rows = data.get("data") or []
        book = rows[0] if isinstance(rows, list) and rows and isinstance(rows[0], dict) else {}

        def levels(xs: Any) -> List[Dict[str, float]]:
            out: List[Dict[str, float]] = []
            if not isinstance(xs, list):
                return out
            for lvl in xs[:d]:
                if not isinstance(lvl, (list, tuple)) or len(lvl) < 2:
                    continue
                px = self._safe_float(lvl[0])
                qty = self._safe_float(lvl[1])
                if px is None or qty is None:
                    continue
                out.append({"price": float(px), "qty": float(qty)})
            return out

        bids = levels(book.get("bids"))
        asks = levels(book.get("asks"))
        bids.sort(key=lambda x: x["price"], reverse=True)
        asks.sort(key=lambda x: x["price"])
        return {"bids": bids, "asks": asks}

    def get_order_rules(self, symbol_venue: str) -> OrderRules:
        inst_id = self.resolve_symbol(symbol_venue)
        row = self._inst_row(inst_id)
        lot_sz = row.get("lotSz")
        tick_sz = row.get("tickSz")
        min_sz = row.get("minSz")
        return {
            "symbol_venue": inst_id,
            "base_increment": self._safe_float(lot_sz),
            "price_increment": self._safe_float(tick_sz),
            "qty_decimals": self._decimals_from_step(lot_sz),
            "price_decimals": self._decimals_from_step(tick_sz),
            "min_qty": self._safe_float(min_sz),
            "max_qty": None,
            "min_notional": None,
            "max_notional": None,
            "supports_post_only": True,
            "supported_tifs": ["gtc", "ioc", "fok"],
            "supported_order_types": ["limit", "market"],
            "raw": row,
        }

    def fetch_balances(self, dry_run: bool) -> List[BalanceItem]:
        if dry_run:
            return []

        by_asset: Dict[str, Dict[str, float]] = {}

        def add(asset: str, total: Optional[float], available: Optional[float], hold: Optional[float]) -> None:
            a = str(asset or "").strip().upper()
            if not a:
                return
            t = float(total or 0.0)
            av = float(available or 0.0)
            h = float(hold if hold is not None else max(t - av, 0.0))
            if abs(t) <= 0.0 and abs(av) <= 0.0 and abs(h) <= 0.0:
                return
            row = by_asset.setdefault(a, {"total": 0.0, "available": 0.0, "hold": 0.0})
            row["total"] += t
            row["available"] += av
            row["hold"] += h

        # Trading account balance.
        data = self._private_get("/api/v5/account/balance")
        for acct in data.get("data") or []:
            if not isinstance(acct, dict):
                continue
            details = acct.get("details") or []
            if not isinstance(details, list):
                continue
            for d in details:
                if not isinstance(d, dict):
                    continue
                ccy = str(d.get("ccy") or "").strip().upper()
                total = self._safe_float(d.get("cashBal"))
                if total is None:
                    total = self._safe_float(d.get("eq"))
                avail = self._safe_float(d.get("availBal"))
                if avail is None:
                    avail = self._safe_float(d.get("availEq"))
                frozen = self._safe_float(d.get("frozenBal"))
                if frozen is None:
                    frozen = self._safe_float(d.get("ordFrozen"))
                if total is None and avail is not None:
                    total = float(avail or 0.0) + float(frozen or 0.0)
                if avail is None and total is not None:
                    avail = max(float(total or 0.0) - float(frozen or 0.0), 0.0)
                add(ccy, total, avail, frozen)

        # Funding account balance.  This catches assets not transferred into the trading account yet.
        try:
            fdata = self._private_get("/api/v5/asset/balances")
            for d in fdata.get("data") or []:
                if not isinstance(d, dict):
                    continue
                ccy = str(d.get("ccy") or "").strip().upper()
                bal = self._safe_float(d.get("bal"))
                avail = self._safe_float(d.get("availBal"))
                frozen = self._safe_float(d.get("frozenBal"))
                if bal is None and avail is not None:
                    bal = float(avail or 0.0) + float(frozen or 0.0)
                if avail is None and bal is not None:
                    avail = max(float(bal or 0.0) - float(frozen or 0.0), 0.0)
                add(ccy, bal, avail, frozen)
        except Exception:
            # Some accounts/regions may not expose funding balances.  Do not fail trading-account balance ingestion.
            pass

        out: List[BalanceItem] = []
        for asset, row in sorted(by_asset.items()):
            total = float(row.get("total") or 0.0)
            available = float(row.get("available") or 0.0)
            hold = float(row.get("hold") or 0.0)
            if abs(total) == 0.0 and abs(available) == 0.0 and abs(hold) == 0.0:
                continue
            out.append({"asset": asset, "total": total, "available": available, "hold": hold})
        return out

    def _normalize_order_status(self, raw: Any, state: Any = None) -> str:
        s = str(raw or state or "").strip().lower()
        if not s:
            return "open"
        if s in {"live", "partially_filled", "mmp_canceled"}:
            return "open"
        if s in {"filled", "closed", "done"}:
            return "filled"
        if s in {"canceled", "cancelled"}:
            return "canceled"
        if s in {"rejected", "failed", "expired"}:
            return "rejected"
        return s

    def _order_from_row(self, row: Dict[str, Any]) -> Optional[VenueOrder]:
        try:
            oid = str(row.get("ordId") or row.get("clOrdId") or "").strip()
            inst_id = str(row.get("instId") or "").strip().upper()
            if not oid or not inst_id:
                return None
            side = str(row.get("side") or "").strip().lower() or None
            otype = str(row.get("ordType") or "").strip().lower() or None
            state = str(row.get("state") or "").strip().lower() or None
            status = self._normalize_order_status(state)
            qty = self._safe_float(row.get("sz"))
            filled = self._safe_float(row.get("accFillSz"))
            avg = self._safe_float(row.get("avgPx"))
            px = self._safe_float(row.get("px"))
            fee_raw = self._safe_float(row.get("fee"))
            fee_asset = str(row.get("feeCcy") or "").strip().upper() or None

            # OKX reports fees as signed values (usually negative for a fee charged).
            # UTT venue rows store fees as positive cost amounts, matching the
            # existing Gemini/Dex-Trade rows and allowing Net/Tax columns to use
            # total_after_fee consistently.
            fee = abs(float(fee_raw)) if fee_raw is not None else None

            quote_asset = ""
            try:
                if "-" in inst_id:
                    quote_asset = (inst_id.split("-", 1)[1] or "").strip().upper()
            except Exception:
                quote_asset = ""

            total_after_fee = None
            try:
                exec_qty = filled if (filled is not None and float(filled) > 0.0) else None
                exec_px = avg if (avg is not None and float(avg) > 0.0) else None
                if exec_qty is not None and exec_px is not None:
                    gross_quote = float(exec_qty) * float(exec_px)
                    if fee is not None and fee_asset and (fee_asset == quote_asset or fee_asset in {"USD", "USDT", "USDC"}):
                        total_after_fee = gross_quote - float(fee)
                    else:
                        total_after_fee = gross_quote
            except Exception:
                total_after_fee = None

            ctime = self._dt_from_ms(row.get("cTime"))
            utime = self._dt_from_ms(row.get("uTime"))
            return {
                "venue": self.venue,
                "venue_order_id": oid,
                "symbol_canon": inst_id,
                "symbol_venue": inst_id,
                "side": side or "",
                "type": otype or "",
                "status": status,
                "status_raw": state or "",
                "cancel_ref": f"{self.venue}:{oid}",
                "qty": float(qty or 0.0),
                "filled_qty": float(filled or 0.0),
                "limit_price": px,
                "avg_fill_price": avg,
                "fee": fee,
                "fee_asset": fee_asset,
                "total_after_fee": total_after_fee,
                "created_at": ctime,
                "updated_at": utime,
            }
        except Exception:
            return None

    def _orders_pending_rows(self, inst_id: Optional[str] = None) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {"instType": "SPOT"}
        if inst_id:
            params["instId"] = inst_id
        data = self._private_get("/api/v5/trade/orders-pending", params=params)
        rows = data.get("data") or []
        return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []

    def _orders_history_rows(self, inst_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        lim = max(1, min(100, int(limit or 100)))
        params: Dict[str, Any] = {"instType": "SPOT", "limit": str(lim)}
        if inst_id:
            params["instId"] = inst_id
        data = self._private_get("/api/v5/trade/orders-history", params=params)
        rows = data.get("data") or []
        return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []

    def _fills_history_rows(self, inst_id: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        lim = max(1, min(100, int(limit or 100)))
        params: Dict[str, Any] = {"instType": "SPOT", "limit": str(lim)}
        if inst_id:
            params["instId"] = inst_id
        data = self._private_get("/api/v5/trade/fills-history", params=params)
        rows = data.get("data") or []
        return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []

    def _quote_asset_for_inst(self, inst_id: Any) -> str:
        s = str(inst_id or "").strip().upper()
        if "-" not in s:
            return ""
        try:
            return (s.split("-", 1)[1] or "").strip().upper()
        except Exception:
            return ""

    def _fill_from_row(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize one OKX fills-history row into a read-only diagnostic shape.

        This does not write Fill rows, ledger rows, lot journals, or basis lots.
        It is intentionally diagnostic/basis-preview-ready only.
        """
        try:
            inst_id = str(row.get("instId") or "").strip().upper()
            trade_id = str(row.get("tradeId") or row.get("fillId") or "").strip()
            order_id = str(row.get("ordId") or "").strip()
            if not inst_id or not trade_id:
                return None

            side = str(row.get("side") or "").strip().lower() or None
            qty = self._safe_float(row.get("fillSz") or row.get("sz"))
            price = self._safe_float(row.get("fillPx") or row.get("px"))
            fee_raw = self._safe_float(row.get("fee"))
            fee_asset = str(row.get("feeCcy") or "").strip().upper() or None
            fee = abs(float(fee_raw)) if fee_raw is not None else None
            ts = self._dt_from_ms(row.get("ts") or row.get("fillTime") or row.get("uTime") or row.get("cTime"))

            quote_asset = self._quote_asset_for_inst(inst_id)
            gross_quote = None
            total_after_fee = None
            try:
                if qty is not None and price is not None:
                    gross_quote = float(qty) * float(price)
                    total_after_fee = gross_quote
                    if fee is not None and fee_asset and (fee_asset == quote_asset or fee_asset in {"USD", "USDT", "USDC"}):
                        if side == "buy":
                            # For buys, this is acquisition cost in quote terms.
                            total_after_fee = gross_quote + float(fee)
                        elif side == "sell":
                            # For sells, this is net proceeds in quote terms.
                            total_after_fee = gross_quote - float(fee)
            except Exception:
                gross_quote = None
                total_after_fee = None

            return {
                "venue": self.venue,
                "venue_trade_id": trade_id,
                "venue_order_id": order_id or None,
                "symbol_canon": inst_id,
                "symbol_venue": inst_id,
                "side": side or "",
                "qty": float(qty or 0.0),
                "price": price,
                "gross_quote": gross_quote,
                "fee": fee,
                "fee_asset": fee_asset,
                "total_after_fee": total_after_fee,
                "ts": ts,
                "exec_type": str(row.get("execType") or "").strip() or None,
            }
        except Exception:
            return None

    def fetch_fills_history(self, symbol: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        inst_id = self.resolve_symbol(symbol) if symbol else None
        rows = self._fills_history_rows(inst_id=inst_id, limit=limit)
        out: List[Dict[str, Any]] = []
        for row in rows:
            norm = self._fill_from_row(row)
            if norm:
                out.append(norm)
        return out

    def _summarize_fills_by_order(self, fills: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        by_order: Dict[str, Dict[str, Any]] = {}
        for f in fills or []:
            oid = str(f.get("venue_order_id") or "").strip()
            if not oid:
                oid = f"TRADE:{str(f.get('venue_trade_id') or '').strip()}"
            row = by_order.setdefault(
                oid,
                {
                    "venue_order_id": (None if oid.startswith("TRADE:") else oid),
                    "fill_count": 0,
                    "symbol_canon": f.get("symbol_canon"),
                    "side": f.get("side"),
                    "filled_qty": 0.0,
                    "gross_quote": 0.0,
                    "fee": 0.0,
                    "fee_asset": f.get("fee_asset"),
                    "total_after_fee": 0.0,
                    "first_ts": f.get("ts"),
                    "last_ts": f.get("ts"),
                },
            )

            row["fill_count"] = int(row.get("fill_count") or 0) + 1
            try:
                row["filled_qty"] = float(row.get("filled_qty") or 0.0) + float(f.get("qty") or 0.0)
            except Exception:
                pass
            try:
                if f.get("gross_quote") is not None:
                    row["gross_quote"] = float(row.get("gross_quote") or 0.0) + float(f.get("gross_quote") or 0.0)
            except Exception:
                pass
            try:
                if f.get("fee") is not None:
                    row["fee"] = float(row.get("fee") or 0.0) + float(f.get("fee") or 0.0)
            except Exception:
                pass
            try:
                if f.get("total_after_fee") is not None:
                    row["total_after_fee"] = float(row.get("total_after_fee") or 0.0) + float(f.get("total_after_fee") or 0.0)
            except Exception:
                pass

            ts = f.get("ts")
            if ts is not None:
                try:
                    if row.get("first_ts") is None or ts < row.get("first_ts"):
                        row["first_ts"] = ts
                    if row.get("last_ts") is None or ts > row.get("last_ts"):
                        row["last_ts"] = ts
                except Exception:
                    pass

        out = list(by_order.values())
        out.sort(key=lambda x: str(x.get("last_ts") or ""), reverse=True)
        return out

    def order_diagnostics(
        self,
        *,
        symbol: Optional[str] = None,
        limit: int = 100,
        include_samples: bool = True,
    ) -> Dict[str, Any]:
        """Read-only order/fill diagnostics for OKX.  Never returns secrets."""
        lim = max(1, min(100, int(limit or 100)))
        inst_id = self.resolve_symbol(symbol) if symbol else None

        out: Dict[str, Any] = {
            "ok": True,
            "venue": self.venue,
            "base_url": self._base_url(),
            "symbol": (str(symbol).strip().upper() if symbol else None),
            "inst_id": inst_id,
            "limit": lim,
            "counts": {
                "orders_pending": 0,
                "orders_history": 0,
                "fills_history": 0,
                "normalized_fills": 0,
            },
            "endpoint_errors": {},
        }

        pending_rows: List[Dict[str, Any]] = []
        history_rows: List[Dict[str, Any]] = []
        fill_rows: List[Dict[str, Any]] = []

        try:
            pending_rows = self._orders_pending_rows(inst_id=inst_id)
            out["counts"]["orders_pending"] = len(pending_rows)
        except Exception as e:
            out["ok"] = False
            out["endpoint_errors"]["orders_pending"] = str(e)

        try:
            history_rows = self._orders_history_rows(inst_id=inst_id, limit=lim)
            out["counts"]["orders_history"] = len(history_rows)
        except Exception as e:
            out["ok"] = False
            out["endpoint_errors"]["orders_history"] = str(e)

        try:
            fill_rows = self._fills_history_rows(inst_id=inst_id, limit=lim)
            out["counts"]["fills_history"] = len(fill_rows)
        except Exception as e:
            out["ok"] = False
            out["endpoint_errors"]["fills_history"] = str(e)

        normalized_orders: List[Dict[str, Any]] = []
        for row in pending_rows + history_rows:
            norm_order = self._order_from_row(row)
            if norm_order:
                normalized_orders.append(norm_order)

        normalized_fills: List[Dict[str, Any]] = []
        for row in fill_rows:
            norm_fill = self._fill_from_row(row)
            if norm_fill:
                normalized_fills.append(norm_fill)

        out["counts"]["normalized_orders"] = len(normalized_orders)
        out["counts"]["normalized_fills"] = len(normalized_fills)
        out["fills_by_order"] = self._summarize_fills_by_order(normalized_fills)

        if include_samples:
            out["samples"] = {
                "orders_pending": normalized_orders[:10],
                "fills_history": normalized_fills[:10],
                "fills_by_order": out["fills_by_order"][:10],
            }

        return out

    def fetch_orders(self, dry_run: bool) -> List[VenueOrder]:
        if dry_run:
            return []
        out: List[VenueOrder] = []
        try:
            for row in self._orders_pending_rows():
                vo = self._order_from_row(row)
                if vo:
                    out.append(vo)
        except Exception:
            pass

        # Recent terminal history, best-effort.  OKX.4B diagnostics surfaces endpoint errors separately.
        try:
            for row in self._orders_history_rows(limit=100):
                vo = self._order_from_row(row)
                if vo:
                    out.append(vo)
        except Exception:
            pass

        by_id: Dict[str, VenueOrder] = {}
        for o in out:
            oid = str(o.get("venue_order_id") or "").strip()
            if oid:
                by_id[oid] = o
        return list(by_id.values())

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
        inst_id = self.resolve_symbol(symbol_venue)
        payload = {
            "instId": inst_id,
            "tdMode": "cash",
            "side": str(side or "").lower(),
            "ordType": "market" if str(type_ or "").lower() == "market" else ("post_only" if post_only else "limit"),
            "sz": str(qty),
            "clOrdId": str(client_order_id or "")[:32] or None,
        }
        if payload["ordType"] == "limit":
            payload["px"] = str(limit_price or "")
        _ = tif  # OKX.5 will wire venue-specific TIF behavior after read-only validation.

        if dry_run:
            return {"status": "acked", "venue_order_id": "dryrun", "raw": {"dry_run": True, "payload": payload}}

        if str(os.getenv("OKX_ENABLE_TRADING") or "").strip().lower() not in {"1", "true", "yes", "on"}:
            return {"status": "rejected", "reject_reason": "OKX live trading is disabled. Set OKX_ENABLE_TRADING=1 only after OKX.5 validation.", "raw": {"payload": payload}}

        return {"status": "rejected", "reject_reason": "OKX live trading submit is intentionally not wired until OKX.5.", "raw": {"payload": payload}}

    def cancel_order(self, venue_order_id: str, dry_run: bool) -> bool:
        if dry_run:
            return True
        if str(os.getenv("OKX_ENABLE_TRADING") or "").strip().lower() not in {"1", "true", "yes", "on"}:
            return False
        return False

    # ─────────────────────────────────────────────────────────────
    # Diagnostics support for /api/okx/diagnostics
    # ─────────────────────────────────────────────────────────────
    def diagnostics(self, *, include_private: bool = False, ccy: Optional[str] = None) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "ok": True,
            "venue": self.venue,
            "base_url": self._base_url(),
            "public": {},
            "private": {"checked": False},
        }
        try:
            t = self._public_get("/api/v5/public/time")
            out["public"]["time_ok"] = True
            out["public"]["time_data"] = t.get("data") or []
        except Exception as e:
            out["ok"] = False
            out["public"]["time_ok"] = False
            out["public"]["error"] = str(e)

        try:
            syms = self.list_symbols()
            out["public"]["spot_symbol_count"] = len(syms)
            out["public"]["sample_symbols"] = syms[:20]
        except Exception as e:
            out["public"]["spot_symbol_count"] = 0
            out["public"]["symbols_error"] = str(e)

        if include_private:
            out["private"]["checked"] = True
            try:
                params = {"ccy": ccy.strip().upper()} if ccy and ccy.strip() else None
                data = self._private_get("/api/v5/account/balance", params=params)
                details_count = 0
                assets: List[str] = []
                for acct in data.get("data") or []:
                    if not isinstance(acct, dict):
                        continue
                    for d in acct.get("details") or []:
                        if not isinstance(d, dict):
                            continue
                        details_count += 1
                        c = str(d.get("ccy") or "").strip().upper()
                        if c and c not in assets:
                            assets.append(c)
                out["private"]["account_balance_ok"] = True
                out["private"]["account_detail_count"] = details_count
                out["private"]["assets"] = sorted(assets)[:50]
            except Exception as e:
                out["ok"] = False
                out["private"]["account_balance_ok"] = False
                out["private"]["error"] = str(e)
        return out
