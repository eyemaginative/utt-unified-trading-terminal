# backend/app/services/symbols.py

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from .symbol_policy import canonicalize_symbol
from ..config import settings

# Core model (module) lives in backend/app/models.py
from ..models import BalanceSnapshot

# Discovery models live in backend/app/discovery_models/ (but you shimmed them to models now)
from ..discovery_models import SymbolView, VenueSymbolSnapshot

# NEW: venue registry is now the single source of truth for supported venues + adapter factories
from ..venues.registry import get_venue_spec, list_venues, is_venue_enabled


# Simple per-process cache of instantiated adapters
_ADAPTER_CACHE: Dict[str, Any] = {}


_INVALID_VENUE_TOKENS = {
    "", "none", "null", "undefined", "nan",
    "[object object]", "[objectobject]",
}


def normalize_venue(venue: str) -> str:
    """
    Defensive normalization for venue query params.

    Common UI/JS failure mode:
      - passing an object to a URL or <select value>, resulting in "[object Object]"

    We treat these as invalid, returning "" so callers can raise a clean error.
    """
    if venue is None:
        return ""

    s = str(venue).strip()
    if not s:
        return ""

    low = s.lower()

    # Catch common JS stringification of objects (case-insensitive).
    # Examples: "[object Object]" / "[object object]"
    if low.startswith("[object") and "object" in low:
        return ""

    if low in _INVALID_VENUE_TOKENS:
        return ""

    return low


def normalize_symbol_canon(symbol_canon: str) -> str:
    # Canonical symbols are typically like "BTC-USD" or "USDT-USD"
    return (symbol_canon or "").strip().upper()


def _dex_trade_effective_enabled_override() -> bool:
    """
    Preserve your current behavior:
      - Dex-Trade public endpoints can be used without credentials, so we do NOT gate it here.

    Note:
      - Your venues registry may gate dex_trade via settings.dex_trade_effective_enabled().
        This override keeps prior behavior stable while you are streamlining venue onboarding.
    """
    return True


def _venue_effectively_enabled(v: str) -> bool:
    """
    Compatibility-preserving gating:
      - robinhood: must be effectively enabled (configured)
      - dex_trade: NOT gated (public endpoints smoke-test friendly)
      - others: defer to registry enabled()
    """
    v = normalize_venue(v)
    if v == "dex_trade":
        return _dex_trade_effective_enabled_override()
    return bool(is_venue_enabled(v))


def supported_venues() -> List[str]:
    """
    Returns venues that are supported AND effectively enabled.

    Preserves prior semantics:
      - Robinhood is gated by settings.robinhood_effective_enabled() (through registry enabled()).
      - Dex-Trade is NOT gated (always included).
    """
    reg = list_venues(include_disabled=True)
    venues = sorted(reg.keys())

    out: List[str] = []
    for v in venues:
        if _venue_effectively_enabled(v):
            out.append(v)
    return out


def get_adapter(venue: str):
    """
    Lazily instantiate and cache adapter instances.

    This prevents backend startup from failing if an adapter is incomplete/abstract.
    If an adapter cannot be instantiated, we raise a clear error only when that venue
    is requested.
    """
    v = normalize_venue(venue)
    if not v:
        raise ValueError(
            "Invalid venue value. Expected a string like 'coinbase', 'kraken', etc. "
            "Received an empty/invalid value (often caused by the UI passing an object, e.g. '[object Object]')."
        )

    # Preserve your existing hard gate for robinhood to prevent accidental calls
    # when not configured.
    if v == "robinhood" and not _venue_effectively_enabled(v):
        raise ValueError(
            "Venue 'robinhood' is disabled or not fully configured. "
            "Set ROBINHOOD_ENABLED=true and provide required Robinhood crypto env vars."
        )

    if v in _ADAPTER_CACHE:
        return _ADAPTER_CACHE[v]

    # Registry is source of truth for venue -> adapter factory
    try:
        spec = get_venue_spec(v)
    except KeyError:
        raise ValueError(f"Unsupported venue '{venue}'. Supported venues: {', '.join(supported_venues())}")

    # IMPORTANT: preserve dex_trade not-gated behavior
    if v != "dex_trade" and not _venue_effectively_enabled(v):
        raise ValueError(f"Venue '{v}' is disabled or not fully configured.")

    try:
        inst = spec.adapter_factory()
    except TypeError as e:
        raise ValueError(
            f"Adapter for venue '{v}' could not be instantiated. "
            f"It may be abstract or missing required methods. "
            f"Details: {e}"
        ) from e

    _ADAPTER_CACHE[v] = inst
    return inst


def split_canon_symbol(symbol_canon: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Best-effort parse for canonical symbols like BASE-QUOTE (e.g., BTC-USD).
    Returns (base, quote) or (None, None) if unparseable.
    """
    s = normalize_symbol_canon(symbol_canon)
    if "-" not in s:
        return None, None
    base, quote = s.split("-", 1)
    base = base.strip().upper()
    quote = quote.strip().upper()
    if not base or not quote:
        return None, None
    return base, quote


# Step B fix helper: never return None; used for DB NOT NULL constraints
def _parse_base_quote(sym: str) -> tuple[str, str]:
    s = (sym or "").strip()
    if not s:
        return ("", "")

    # Coinbase/Gemini style: BASE-QUOTE
    if "-" in s:
        a, b = s.split("-", 1)
        return (a.strip(), b.strip())

    # Kraken style sometimes uses "/" or other separators
    if "/" in s:
        a, b = s.split("/", 1)
        return (a.strip(), b.strip())

    # Fallback: unknown format
    return (s, "")


def list_symbols_for_venue(venue: str) -> List[str]:
    """
    MVP behavior:
      - Return a practical starter set if the venue adapter doesn't support symbol discovery yet.

    Future behavior (non-breaking):
      - If adapter implements list_symbols() or list_markets(), use it.
    """
    adapter = get_adapter(venue)

    # Preferred: adapter.list_symbols() -> list[str] in canonical form ("BTC-USD", etc.)
    fn = getattr(adapter, "list_symbols", None)
    if callable(fn):
        try:
            syms = fn()
            if isinstance(syms, list) and syms:
                return [normalize_symbol_canon(x) for x in syms if str(x).strip()]
        except Exception:
            pass

    # Alternate: adapter.list_markets() -> list[dict] or list[str]
    fn2 = getattr(adapter, "list_markets", None)
    if callable(fn2):
        try:
            mkts = fn2()
            out: List[str] = []
            if isinstance(mkts, list):
                for m in mkts:
                    if isinstance(m, str):
                        out.append(normalize_symbol_canon(m))
                    elif isinstance(m, dict):
                        sc = m.get("symbol_canon") or m.get("symbol") or None
                        if sc:
                            out.append(normalize_symbol_canon(sc))
                            continue
                        base = (m.get("base") or "").strip().upper()
                        quote = (m.get("quote") or "").strip().upper()
                        if base and quote:
                            out.append(f"{base}-{quote}")
            if out:
                # de-dup, stable
                seen = set()
                uniq = []
                for s in out:
                    if s in seen:
                        continue
                    seen.add(s)
                    uniq.append(s)
                return uniq
        except Exception:
            pass

    # Fallback starter set
    return ["USDT-USD", "BTC-USD", "ETH-USD"]


def resolve_symbol(venue: str, symbol_canon: str) -> Tuple[str, str]:
    """
    Resolve canonical symbol to venue-specific symbol via adapter if possible.

    Returns: (symbol_canon_norm, symbol_venue)
    """
    v = normalize_venue(venue)
    if not v:
        raise ValueError(
            "Invalid venue value. Expected a string like 'coinbase', 'kraken', etc. "
            "Received an empty/invalid value (often caused by the UI passing an object, e.g. '[object Object]')."
        )

    sc = normalize_symbol_canon(symbol_canon)
    if not sc:
        return "", ""

    adapter = get_adapter(v)

    fn = getattr(adapter, "resolve_symbol", None)
    if callable(fn):
        sym_venue = (fn(sc) or "").strip()
        return sc, sym_venue or sc

    # Fallback to canonical normalization
    return canonicalize_symbol(sc), canonicalize_symbol(sc)


# ─────────────────────────────────────────────────────────────
# Discovery snapshot + diff service surface (used by routers)
# ─────────────────────────────────────────────────────────────

def _view_key(venue: str, symbol_canon: str) -> str:
    return f"{normalize_venue(venue)}:{normalize_symbol_canon(symbol_canon)}"


def _merge_symbol_views(db: Session, venue: str, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return rows

    keys = [_view_key(venue, r.get("symbol_canon", "")) for r in rows]
    keys = [k for k in keys if k]
    if not keys:
        return rows

    views = db.execute(select(SymbolView).where(SymbolView.view_key.in_(keys))).scalars().all()
    by_key = {v.view_key: v for v in views}

    out: List[Dict[str, Any]] = []
    for r in rows:
        sym = r.get("symbol_canon", "")
        key = _view_key(venue, sym)
        v = by_key.get(key)
        r2 = dict(r)
        r2["view_key"] = key
        r2["viewed_confirmed"] = bool(v.viewed_confirmed) if v else False
        r2["viewed_at"] = v.viewed_at.isoformat() if (v and v.viewed_at) else None
        out.append(r2)
    return out


def refresh_symbols_for_venue(db: Session, venue: str) -> Dict[str, Any]:
    """
    Captures a new snapshot (VenueSymbolSnapshot rows with shared captured_at)
    from adapter discovery (or fallback starter set).
    """
    v = normalize_venue(venue)
    _ = get_adapter(v)  # validate venue early for cleaner API errors

    symbols = list_symbols_for_venue(v)
    captured_at = datetime.utcnow()

    rows: List[VenueSymbolSnapshot] = []
    for sym in symbols:
        sc = normalize_symbol_canon(sym)

        try:
            _, sv = resolve_symbol(v, sc)
            symbol_venue = sv
        except Exception:
            symbol_venue = sc

        # Step B fix: guarantee base_asset (and quote_asset) are never NULL
        base, quote = _parse_base_quote(sc)

        # Guarantee NOT NULL on base_asset
        if not base:
            base = sc or symbol_venue or str(sym or "").strip()

        # quote can be empty string; guarantee it's never None
        if quote is None:
            quote = ""

        # Normalize asset casing for consistency
        base_asset = (base or "").strip().upper()
        quote_asset = (quote or "").strip().upper()

        # Final fallback to satisfy DB NOT NULL constraints
        if not base_asset:
            base_asset = (sc or symbol_venue or "UNKNOWN").strip().upper()
        if quote_asset is None:
            quote_asset = ""

        # Some schemas have is_active NOT NULL; safe default True
        rows.append(
            VenueSymbolSnapshot(
                venue=v,
                symbol_venue=symbol_venue,
                symbol_canon=sc,
                base_asset=base_asset,
                quote_asset=quote_asset,
                is_active=True,
                captured_at=captured_at,
            )
        )

    if rows:
        db.add_all(rows)
        db.commit()

    return {"venue": v, "captured_at": captured_at.isoformat(), "count": len(rows)}


def _latest_captured_at(db: Session, venue: str) -> Optional[datetime]:
    return db.execute(
        select(func.max(VenueSymbolSnapshot.captured_at)).where(VenueSymbolSnapshot.venue == venue)
    ).scalar_one_or_none()


def _previous_captured_at(db: Session, venue: str, latest: datetime) -> Optional[datetime]:
    return db.execute(
        select(func.max(VenueSymbolSnapshot.captured_at)).where(
            and_(VenueSymbolSnapshot.venue == venue, VenueSymbolSnapshot.captured_at < latest)
        )
    ).scalar_one_or_none()


def latest_symbols_for_venue(db: Session, venue: str) -> Dict[str, Any]:
    v = normalize_venue(venue)
    _ = get_adapter(v)

    latest = _latest_captured_at(db, v)
    if not latest:
        return {"venue": v, "captured_at": None, "items": []}

    items = (
        db.execute(
            select(VenueSymbolSnapshot).where(
                and_(VenueSymbolSnapshot.venue == v, VenueSymbolSnapshot.captured_at == latest)
            )
        )
        .scalars()
        .all()
    )

    rows = [
        {
            "venue": s.venue,
            "symbol_venue": s.symbol_venue,
            "symbol_canon": s.symbol_canon,
            "captured_at": s.captured_at.isoformat(),
        }
        for s in items
    ]
    rows = _merge_symbol_views(db, v, rows)

    return {"venue": v, "captured_at": latest.isoformat(), "items": rows}


def new_symbols_since_last_snapshot(db: Session, venue: str) -> Dict[str, Any]:
    v = normalize_venue(venue)
    _ = get_adapter(v)

    latest = _latest_captured_at(db, v)
    if not latest:
        return {"venue": v, "captured_at": None, "prev_captured_at": None, "items": []}

    prev = _previous_captured_at(db, v, latest)
    if not prev:
        payload = latest_symbols_for_venue(db, v)
        payload["prev_captured_at"] = None
        return payload

    latest_rows = (
        db.execute(
            select(VenueSymbolSnapshot).where(
                and_(VenueSymbolSnapshot.venue == v, VenueSymbolSnapshot.captured_at == latest)
            )
        )
        .scalars()
        .all()
    )

    prev_syms = set(
        db.execute(
            select(VenueSymbolSnapshot.symbol_canon).where(
                and_(VenueSymbolSnapshot.venue == v, VenueSymbolSnapshot.captured_at == prev)
            )
        )
        .scalars()
        .all()
    )

    new_items = [r for r in latest_rows if r.symbol_canon not in prev_syms]
    new_items.sort(key=lambda x: (x.symbol_canon or ""))

    rows = [
        {
            "venue": v,
            "symbol_venue": r.symbol_venue,
            "symbol_canon": r.symbol_canon,
            "captured_at": latest.isoformat(),
        }
        for r in new_items
    ]
    rows = _merge_symbol_views(db, v, rows)

    return {
        "venue": v,
        "captured_at": latest.isoformat(),
        "prev_captured_at": prev.isoformat(),
        "items": rows,
    }


def unheld_new_symbols_since_last_snapshot(db: Session, venue: str, eps: float = 1e-8) -> Dict[str, Any]:
    """
    Filters "new" symbols by removing those whose base asset appears in the latest balances.
    """
    v = normalize_venue(venue)
    _ = get_adapter(v)

    payload = new_symbols_since_last_snapshot(db, v)
    items = payload.get("items", []) or []

    eps_num = float(eps) if eps is not None else 1e-8
    if not (eps_num > 0):
        eps_num = 1e-8

    bal_as_of = db.execute(
        select(func.max(BalanceSnapshot.captured_at)).where(BalanceSnapshot.venue == v)
    ).scalar_one_or_none()

    held_assets: set[str] = set()
    if bal_as_of:
        bals = (
            db.execute(
                select(BalanceSnapshot).where(
                    and_(BalanceSnapshot.venue == v, BalanceSnapshot.captured_at == bal_as_of)
                )
            )
            .scalars()
            .all()
        )
        for b in bals:
            asset = str(getattr(b, "asset", "") or "").upper()
            total = getattr(b, "total", None)
            try:
                total_f = float(total) if total is not None else 0.0
            except Exception:
                total_f = 0.0

            if asset and abs(total_f) > eps_num:
                held_assets.add(asset)

    def base_asset(sym: str) -> str:
        sym2 = (sym or "").upper()
        return (sym2.split("-", 1)[0] if sym2 else "").upper()

    filtered = [r for r in items if base_asset(r.get("symbol_canon", "")) not in held_assets]
    payload["items"] = filtered
    payload["balances_as_of"] = bal_as_of.isoformat() if bal_as_of else None
    payload["held_assets"] = sorted(list(held_assets))
    payload["eps"] = eps_num
    return payload


def list_symbol_venues(db: Session) -> Dict[str, Any]:
    """
    Non-breaking response:
      - keeps "venues" key (what your frontend expects)
      - ensures venues are returned even before any snapshots exist
      - adds extra keys for UI/diagnostics

    Gating rule:
      - Robinhood must not appear unless effectively enabled/configured.
      - Dex-Trade is allowed (not gated) to preserve prior behavior.
    """
    snapshot_venues = db.execute(select(VenueSymbolSnapshot.venue).distinct()).scalars().all()
    snapshot_venues = sorted({v for v in snapshot_venues if v})

    supported = supported_venues()

    # Filter snapshot venues through the same effective-enabled gate (prevents UI leakage).
    snapshot_venues_enabled = [v for v in snapshot_venues if _venue_effectively_enabled(v)]

    venues_union = sorted(set(snapshot_venues_enabled).union(set(supported)))

    return {
        "venues": venues_union,
        "supported_venues": supported,
        "snapshot_venues": snapshot_venues_enabled,
    }
