# backend/app/routers/market_intel.py

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Depends, Query

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import BalanceSnapshot
from ..services.market import prices_usd_from_assets
from ..services.symbols import normalize_venue, supported_venues, get_adapter

router = APIRouter(prefix="/api/market_intel", tags=["market_intel"])


# Conservative static mapping (extend as needed).
# If an asset isn’t in here, CoinGecko fallback returns null changes for that asset.
_COINGECKO_ID_BY_ASSET: Dict[str, str] = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "DOGE": "dogecoin",
    "SOL": "solana",
    "ADA": "cardano",
    "XRP": "ripple",
    "LTC": "litecoin",
    "DOT": "polkadot",
    "AVAX": "avalanche-2",
    "LINK": "chainlink",
    "MATIC": "matic-network",  # CoinGecko still accepts this id historically
    "USDT": "tether",
    "USDC": "usd-coin",
}


def _latest_balance_ts(db: Session, venue: str) -> Optional[datetime]:
    return db.execute(
        select(func.max(BalanceSnapshot.captured_at)).where(BalanceSnapshot.venue == venue)
    ).scalar_one_or_none()


def _load_balances_at(db: Session, venue: str, ts: datetime) -> List[BalanceSnapshot]:
    return (
        db.execute(
            select(BalanceSnapshot).where(
                and_(BalanceSnapshot.venue == venue, BalanceSnapshot.captured_at == ts)
            )
        )
        .scalars()
        .all()
    )


def _safe_f(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def _coingecko_changes_usd(assets: List[str]) -> Dict[str, Dict[str, Optional[float]]]:
    """
    Returns {ASSET: {"change_1h":..,"change_1d":..,"change_1w":..}} for assets we can map.
    If unmapped or API failure, values are None.
    """
    want = []
    asset_by_id: Dict[str, str] = {}
    for a in assets:
        aid = _COINGECKO_ID_BY_ASSET.get(a)
        if aid:
            want.append(aid)
            asset_by_id[aid] = a

    out: Dict[str, Dict[str, Optional[float]]] = {
        a: {"change_1h": None, "change_1d": None, "change_1w": None} for a in assets
    }
    if not want:
        return out

    # CoinGecko simple price supports include_1hr_change/include_24hr_change/include_7d_change.
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": ",".join(sorted(set(want))),
        "vs_currencies": "usd",
        "include_1hr_change": "true",
        "include_24hr_change": "true",
        "include_7d_change": "true",
    }

    try:
        with httpx.Client(timeout=15.0) as client:
            r = client.get(url, params=params)
            if not (200 <= r.status_code < 300):
                return out
            data = r.json() if r.content else {}
    except Exception:
        return out

    if not isinstance(data, dict):
        return out

    for cid, rec in data.items():
        if not isinstance(rec, dict):
            continue
        asset = asset_by_id.get(cid)
        if not asset:
            continue
        c1h = rec.get("usd_1h_change")
        c1d = rec.get("usd_24h_change")
        c1w = rec.get("usd_7d_change")
        out[asset] = {
            "change_1h": float(c1h) if c1h is not None else None,
            "change_1d": float(c1d) if c1d is not None else None,
            "change_1w": float(c1w) if c1w is not None else None,
        }

    return out


@router.get("/changes")
def market_intel_changes(
    venues: str = Query("", description="Comma-separated venues; blank = all enabled venues"),
    quote: str = Query("USD", description="Quote asset for % changes pairing, usually USD"),
    eps: float = Query(1e-8, ge=0.0, description="Min abs(total) to treat an asset as held"),
    include_stables: bool = Query(False, description="Include USD/USDT/USDC in held assets"),
    use_coingecko_fallback: bool = Query(
        True,
        description="If venue-native is null, fall back to CoinGecko where possible",
    ),
    include_debug: bool = Query(
        False,
        description="Include adapter debug metadata (e.g., pair_used) without affecting default response shape",
    ),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    q = (quote or "USD").strip().upper()
    epsv = float(eps if eps is not None else 1e-8)

    if venues.strip():
        venue_list = [normalize_venue(v) for v in venues.split(",")]
        venue_list = [v for v in venue_list if v]
    else:
        venue_list = supported_venues()

    stable_set = {"USD", "USDT", "USDC"}

    items: List[Dict[str, Any]] = []
    now = datetime.utcnow()

    for v in venue_list:
        ts = _latest_balance_ts(db, v)
        if not ts:
            continue

        bals = _load_balances_at(db, v, ts)
        held_assets: List[Tuple[str, float]] = []

        for b in bals:
            asset = (getattr(b, "asset", "") or "").strip().upper()
            total = _safe_f(getattr(b, "total", 0.0))
            if not asset:
                continue
            if not include_stables and asset in stable_set:
                continue
            if epsv > 0 and abs(total) <= epsv:
                continue
            held_assets.append((asset, total))

        if not held_assets:
            continue

        # Prices (USD) via your existing service
        assets_only = [a for (a, _t) in held_assets]
        try:
            px = prices_usd_from_assets(venue=v, assets=assets_only)
        except Exception:
            px = {}

        # Optional CoinGecko fallback (only for mapped assets)
        cg = (
            _coingecko_changes_usd(assets_only)
            if use_coingecko_fallback
            else {a: {"change_1h": None, "change_1d": None, "change_1w": None} for a in assets_only}
        )

        # Venue-native changes when adapters implement it
        adapter = None
        try:
            adapter = get_adapter(v)
        except Exception:
            adapter = None

        get_native = getattr(adapter, "get_pct_changes_for_asset", None) if adapter else None

        for asset, total in held_assets:
            usd_price = None
            try:
                if isinstance(px, dict) and asset in px:
                    usd_price = float(px.get(asset))
            except Exception:
                usd_price = None

            usd_value = None
            if usd_price is not None:
                usd_value = float(total) * float(usd_price)

            native_vals: Dict[str, Optional[float]] = {"change_1h": None, "change_1d": None, "change_1w": None}
            native_source: Optional[str] = None
            native_debug: Dict[str, Any] = {}

            if callable(get_native):
                try:
                    nv = get_native(asset, quote=q)
                    if isinstance(nv, dict):
                        # Pull standard keys (hardening: tolerate extra keys like pair_used)
                        for k in ("change_1h", "change_1d", "change_1w"):
                            if nv.get(k) is not None:
                                try:
                                    native_vals[k] = float(nv.get(k))  # type: ignore[arg-type]
                                except Exception:
                                    pass

                        # Capture debug metadata (e.g. pair_used), but only include in response if include_debug=True
                        for k, vv in nv.items():
                            if k in ("change_1h", "change_1d", "change_1w"):
                                continue
                            native_debug[k] = vv

                        if any(native_vals[k] is not None for k in native_vals):
                            native_source = v
                except Exception:
                    pass

            # Merge: native overrides CG only when non-null
            cgf = cg.get(asset) or {}
            change_1h = (
                native_vals["change_1h"]
                if native_vals["change_1h"] is not None
                else (cgf.get("change_1h") if use_coingecko_fallback else None)
            )
            change_1d = (
                native_vals["change_1d"]
                if native_vals["change_1d"] is not None
                else (cgf.get("change_1d") if use_coingecko_fallback else None)
            )
            change_1w = (
                native_vals["change_1w"]
                if native_vals["change_1w"] is not None
                else (cgf.get("change_1w") if use_coingecko_fallback else None)
            )

            source = native_source or (
                "coingecko"
                if use_coingecko_fallback
                and any(cgf.get(k) is not None for k in ("change_1h", "change_1d", "change_1w"))
                else None
            )

            row: Dict[str, Any] = {
                "venue": v,
                "balances_as_of": ts.isoformat(),
                "asset": asset,
                "total": float(total),
                "usd_price": usd_price,
                "usd_value": usd_value,
                "change_1h": change_1h,
                "change_1d": change_1d,
                "change_1w": change_1w,
                "change_source": source,
            }

            # Only add debug metadata when explicitly requested (prevents surprising frontend breakage)
            if include_debug and native_debug:
                row["native_debug"] = native_debug

            items.append(row)

    return {
        "quote": q,
        "venues": venue_list,
        "count": len(items),
        "items": items,
        "ts": now.isoformat(),
    }
