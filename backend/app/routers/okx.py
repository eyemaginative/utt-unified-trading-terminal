# backend/app/routers/okx.py

from __future__ import annotations

from typing import Optional, Any, Dict, List

from fastapi import APIRouter, Query, Depends
from sqlalchemy import select, asc
from sqlalchemy.orm import Session

from ..adapters.okx import OKXAdapter
from ..db import get_db
from ..models import BasisLot, VenueOrderRow
from ..models_lot_journal import LotJournal

router = APIRouter(prefix="/api/okx", tags=["okx"])


def _okx_safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        out = float(value)
        if out != out or out in (float("inf"), float("-inf")):
            return None
        return out
    except Exception:
        return None


def _okx_norm_wallet(wallet_id: Any) -> str:
    s = str(wallet_id or "default").strip()
    return s if s else "default"


def _okx_parse_base_asset(symbol: Any) -> str:
    s = str(symbol or "").strip().upper()
    if not s:
        return ""
    for sep in ("-", "/", "_", ":"):
        if sep in s:
            return (s.split(sep, 1)[0] or "").strip().upper()
    return s


def _okx_parse_quote_asset(symbol: Any) -> str:
    s = str(symbol or "").strip().upper()
    if not s:
        return ""
    for sep in ("-", "/", "_", ":"):
        if sep in s:
            parts = s.split(sep, 1)
            return (parts[1] if len(parts) > 1 else "").strip().upper()
    return ""


def _okx_iso(value: Any) -> Optional[str]:
    try:
        if value is None:
            return None
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)
    except Exception:
        return None


def _okx_fee_usd(fill: Dict[str, Any]) -> Optional[float]:
    fee = _okx_safe_float((fill or {}).get("fee"))
    if fee is None:
        return None
    fee_asset = str((fill or {}).get("fee_asset") or "").strip().upper()
    quote_asset = _okx_parse_quote_asset((fill or {}).get("symbol_canon") or (fill or {}).get("symbol_venue"))
    if fee_asset in {"USD", "USDC", "USDT"} or (quote_asset in {"USD", "USDC", "USDT"} and not fee_asset):
        return abs(float(fee))
    return None


def _okx_existing_journal_status(db: Session, fill: Dict[str, Any]) -> Dict[str, Any]:
    """Read-only idempotency context for future OKX fill import/apply work."""
    trade_id = str((fill or {}).get("venue_trade_id") or "").strip()
    order_id = str((fill or {}).get("venue_order_id") or "").strip()
    out: Dict[str, Any] = {
        "proposed_origin_type": "OKX_FILL",
        "proposed_origin_ref": trade_id or None,
        "per_fill_journal_status": "not_found",
        "per_fill_journal_id": None,
        "aggregate_venue_order_row_id": None,
        "aggregate_journal_status": "not_found",
        "aggregate_journal_id": None,
    }

    if trade_id:
        try:
            j = db.execute(
                select(LotJournal).where(
                    LotJournal.origin_type.in_(["OKX_FILL", "VENUE_FILL", "VENUE_FILL_OKX"]),
                    LotJournal.origin_ref == trade_id,
                )
            ).scalars().first()
            if j:
                out["per_fill_journal_status"] = "applied" if bool(getattr(j, "applied", False)) else "unapplied"
                out["per_fill_journal_id"] = str(getattr(j, "id", "") or "") or None
        except Exception as e:
            out["per_fill_journal_status"] = f"error:{type(e).__name__}"

    if order_id:
        try:
            vrow = db.execute(
                select(VenueOrderRow)
                .where(VenueOrderRow.venue == "okx")
                .where(VenueOrderRow.venue_order_id == order_id)
                .limit(1)
            ).scalars().first()
            if vrow:
                out["aggregate_venue_order_row_id"] = str(getattr(vrow, "id", "") or "") or None
                j2 = db.execute(
                    select(LotJournal).where(
                        LotJournal.action.in_(["BUY_LOT_CREATE", "SELL_FIFO_CONSUME"]),
                        LotJournal.origin_type == "VENUE_ORDER_AGG",
                        LotJournal.origin_ref == str(getattr(vrow, "id", "") or ""),
                    )
                ).scalars().first()
                if j2:
                    out["aggregate_journal_status"] = "applied" if bool(getattr(j2, "applied", False)) else "unapplied"
                    out["aggregate_journal_id"] = str(getattr(j2, "id", "") or "") or None
        except Exception as e:
            out["aggregate_journal_status"] = f"error:{type(e).__name__}"

    return out


def _okx_preview_sell_fifo(
    db: Session,
    *,
    venue: str,
    wallet_id: str,
    asset: str,
    qty_sold: float,
    price_usd: Optional[float],
    fee_usd: Optional[float],
    proceeds_usd: Optional[float],
    effective_at: Any,
    slice_limit: int,
) -> Dict[str, Any]:
    """Read-only FIFO preview.  Does not call fifo_consume_sell_fifo because that helper mutates ORM lots."""
    v = str(venue or "okx").strip().lower() or "okx"
    w = _okx_norm_wallet(wallet_id)
    a = str(asset or "").strip().upper()
    qty = float(qty_sold or 0.0)
    lim = max(1, min(int(slice_limit or 25), 100))

    stmt = (
        select(BasisLot)
        .where(
            BasisLot.venue == v,
            BasisLot.wallet_id == w,
            BasisLot.asset == a,
            BasisLot.qty_remaining > 0,
        )
        .order_by(asc(BasisLot.acquired_at), asc(BasisLot.created_at), asc(BasisLot.id))
    )
    lots = list(db.execute(stmt).scalars().all())

    total_available = 0.0
    for lot in lots:
        qr = _okx_safe_float(getattr(lot, "qty_remaining", None)) or 0.0
        if qr > 0:
            total_available += float(qr)

    remaining = float(qty)
    qty_consumed = 0.0
    basis_consumed = 0.0
    basis_defined = True
    any_basis_missing = False
    slices: List[Dict[str, Any]] = []
    omitted_slices = 0

    for lot in lots:
        if remaining <= 1e-12:
            break
        qr = _okx_safe_float(getattr(lot, "qty_remaining", None)) or 0.0
        if qr <= 0:
            continue
        take = min(float(qr), remaining)
        if take <= 0:
            continue

        lot_total_basis = _okx_safe_float(getattr(lot, "total_basis_usd", None))
        lot_qty_total = _okx_safe_float(getattr(lot, "qty_total", None)) or 0.0
        lot_basis_missing = bool(getattr(lot, "basis_is_missing", False))

        basis_moved = None
        if lot_total_basis is None or lot_basis_missing or lot_qty_total <= 0:
            any_basis_missing = True
            basis_defined = False
        else:
            basis_moved = float((lot_total_basis / lot_qty_total) * take)
            basis_consumed += basis_moved

        qty_consumed += float(take)
        remaining -= float(take)

        row = {
            "lot_id": str(getattr(lot, "id", "") or ""),
            "acquired_at": _okx_iso(getattr(lot, "acquired_at", None)),
            "qty_total": _okx_safe_float(getattr(lot, "qty_total", None)),
            "qty_remaining_before": float(qr),
            "qty_preview_consumed": float(take),
            "qty_remaining_after_preview": float(qr - take),
            "basis_moved_usd": basis_moved,
            "basis_is_missing": bool(lot_basis_missing) or lot_total_basis is None or lot_qty_total <= 0,
            "origin_type": getattr(lot, "origin_type", None),
            "origin_ref": getattr(lot, "origin_ref", None),
        }
        if len(slices) < lim:
            slices.append(row)
        else:
            omitted_slices += 1

    qty_missing = max(float(remaining), 0.0)
    proceeds = _okx_safe_float(proceeds_usd)
    if proceeds is None and price_usd is not None:
        proceeds = float(qty_consumed) * float(price_usd) - float(fee_usd or 0.0)

    basis_out = float(basis_consumed) if basis_defined else None
    realized = None
    if qty_missing <= 1e-12 and proceeds is not None and basis_out is not None:
        realized = float(proceeds) - float(basis_out)

    status = "sufficient_inventory" if qty_missing <= 1e-12 else "insufficient_inventory"
    if any_basis_missing and status == "sufficient_inventory":
        status = "basis_missing"

    return {
        "kind": "sell_fifo_preview",
        "status": status,
        "venue": v,
        "wallet_id": w,
        "asset": a,
        "qty_sold": float(qty),
        "qty_available": float(total_available),
        "qty_consumed": float(qty_consumed),
        "qty_missing": float(qty_missing),
        "price_usd": price_usd,
        "fee_usd": fee_usd,
        "proceeds_usd": proceeds,
        "basis_consumed_usd": basis_out,
        "realized_gain_usd": realized,
        "any_basis_missing": bool(any_basis_missing),
        "effective_at": _okx_iso(effective_at),
        "slice_count": len(slices) + int(omitted_slices),
        "omitted_slices": int(omitted_slices),
        "slices": slices,
    }


def _okx_preview_buy_lot(
    *,
    venue: str,
    wallet_id: str,
    asset: str,
    qty: float,
    price_usd: Optional[float],
    fee_usd: Optional[float],
    total_after_fee: Optional[float],
    effective_at: Any,
) -> Dict[str, Any]:
    basis_total = _okx_safe_float(total_after_fee)
    if basis_total is None and price_usd is not None:
        basis_total = float(qty or 0.0) * float(price_usd) + float(fee_usd or 0.0)
    return {
        "kind": "buy_lot_preview",
        "status": "would_create_known_basis_lot" if basis_total is not None else "would_create_missing_basis_lot",
        "venue": str(venue or "okx").strip().lower() or "okx",
        "wallet_id": _okx_norm_wallet(wallet_id),
        "asset": str(asset or "").strip().upper(),
        "qty": float(qty or 0.0),
        "price_usd": price_usd,
        "fee_usd": fee_usd,
        "total_basis_usd": basis_total,
        "basis_is_missing": basis_total is None,
        "effective_at": _okx_iso(effective_at),
    }


def _okx_preview_fill(db: Session, fill: Dict[str, Any], *, wallet_id: str, slice_limit: int) -> Dict[str, Any]:
    side = str((fill or {}).get("side") or "").strip().lower()
    symbol = (fill or {}).get("symbol_canon") or (fill or {}).get("symbol_venue")
    asset = _okx_parse_base_asset(symbol)
    qty = _okx_safe_float((fill or {}).get("qty")) or 0.0
    price = _okx_safe_float((fill or {}).get("price"))
    fee_usd = _okx_fee_usd(fill)
    total_after_fee = _okx_safe_float((fill or {}).get("total_after_fee"))
    ts = (fill or {}).get("ts")

    row: Dict[str, Any] = {
        "venue": "okx",
        "wallet_id": _okx_norm_wallet(wallet_id),
        "symbol_canon": str(symbol or "").strip().upper() or None,
        "asset": asset or None,
        "side": side or None,
        "venue_trade_id": (fill or {}).get("venue_trade_id"),
        "venue_order_id": (fill or {}).get("venue_order_id"),
        "qty": float(qty),
        "price": price,
        "gross_quote": _okx_safe_float((fill or {}).get("gross_quote")),
        "fee": _okx_safe_float((fill or {}).get("fee")),
        "fee_asset": (fill or {}).get("fee_asset"),
        "fee_usd": fee_usd,
        "total_after_fee": total_after_fee,
        "ts": _okx_iso(ts),
        "journal_context": _okx_existing_journal_status(db, fill),
    }

    if not asset or qty <= 0:
        row["preview"] = {"kind": "skip", "status": "missing_asset_or_qty"}
        return row

    if side == "sell":
        row["preview"] = _okx_preview_sell_fifo(
            db,
            venue="okx",
            wallet_id=wallet_id,
            asset=asset,
            qty_sold=float(qty),
            price_usd=price,
            fee_usd=fee_usd,
            proceeds_usd=total_after_fee,
            effective_at=ts,
            slice_limit=slice_limit,
        )
    elif side == "buy":
        row["preview"] = _okx_preview_buy_lot(
            venue="okx",
            wallet_id=wallet_id,
            asset=asset,
            qty=float(qty),
            price_usd=price,
            fee_usd=fee_usd,
            total_after_fee=total_after_fee,
            effective_at=ts,
        )
    else:
        row["preview"] = {"kind": "skip", "status": "unknown_side"}

    return row


@router.get("/diagnostics")
def get_okx_diagnostics(
    private: bool = Query(default=False, description="If true, checks signed account balance read path."),
    ccy: Optional[str] = Query(default=None, description="Optional currency filter for private balance check, e.g. DOGE."),
):
    """Read-only OKX diagnostic endpoint.

    Never returns API key, secret, passphrase, signatures, or request headers.
    """
    return OKXAdapter().diagnostics(include_private=bool(private), ccy=ccy)

@router.get("/order_diagnostics")
def get_okx_order_diagnostics(
    symbol: Optional[str] = Query(default=None, description="Optional canonical/venue symbol filter, e.g. DOGE-USD."),
    limit: int = Query(default=100, ge=1, le=100, description="Maximum OKX history/fills rows to inspect."),
    include_samples: bool = Query(default=True, description="If true, returns small normalized order/fill samples."),
):
    """Read-only OKX order/fill diagnostics.

    Never returns API key, secret, passphrase, signatures, or request headers.
    Does not write fills, ledger rows, lot journals, or basis lots.
    """
    return OKXAdapter().order_diagnostics(
        symbol=symbol,
        limit=int(limit or 100),
        include_samples=bool(include_samples),
    )

@router.get("/fill_basis_preview")
def get_okx_fill_basis_preview(
    symbol: Optional[str] = Query(default=None, description="Optional canonical/venue symbol filter, e.g. DOGE-USD."),
    wallet_id: str = Query(default="default", description="UTT wallet/account bucket for exact FIFO preview."),
    limit: int = Query(default=100, ge=1, le=100, description="Maximum OKX fills-history rows to preview."),
    trade_id: Optional[str] = Query(default=None, description="Optional OKX tradeId filter."),
    venue_order_id: Optional[str] = Query(default=None, description="Optional OKX ordId filter."),
    include_items: bool = Query(default=True, description="If true, returns per-fill preview rows."),
    slice_limit: int = Query(default=25, ge=1, le=100, description="Maximum FIFO lot slices returned per fill preview."),
    db: Session = Depends(get_db),
):
    """Read-only OKX fill → FIFO/basis preview.

    This endpoint never writes Fill rows, ledger rows, LotJournal rows, or BasisLot rows.
    It previews against the exact UTT key: venue=okx, wallet_id=<wallet_id>, asset=<base asset>.
    """
    adapter = OKXAdapter()
    fills = adapter.fetch_fills_history(symbol=symbol, limit=int(limit or 100))

    tid_filter = str(trade_id or "").strip()
    oid_filter = str(venue_order_id or "").strip()
    if tid_filter:
        fills = [f for f in fills if str(f.get("venue_trade_id") or "").strip() == tid_filter]
    if oid_filter:
        fills = [f for f in fills if str(f.get("venue_order_id") or "").strip() == oid_filter]

    previews: List[Dict[str, Any]] = []
    counts: Dict[str, Any] = {
        "fills_seen": len(fills),
        "previewed": 0,
        "buy_lot_candidates": 0,
        "sell_fifo_candidates": 0,
        "sell_sufficient_inventory": 0,
        "sell_insufficient_inventory": 0,
        "basis_missing": 0,
        "unknown_side_or_skipped": 0,
        "aggregate_journal_found": 0,
        "per_fill_journal_found": 0,
    }

    for fill in fills:
        preview = _okx_preview_fill(db, fill, wallet_id=wallet_id, slice_limit=int(slice_limit or 25))
        previews.append(preview)
        counts["previewed"] += 1

        side = str(preview.get("side") or "").strip().lower()
        p = preview.get("preview") if isinstance(preview.get("preview"), dict) else {}
        status = str((p or {}).get("status") or "").strip()
        if side == "buy":
            counts["buy_lot_candidates"] += 1
        elif side == "sell":
            counts["sell_fifo_candidates"] += 1
            if status == "sufficient_inventory":
                counts["sell_sufficient_inventory"] += 1
            elif status == "insufficient_inventory":
                counts["sell_insufficient_inventory"] += 1
            elif status == "basis_missing":
                counts["basis_missing"] += 1
        else:
            counts["unknown_side_or_skipped"] += 1

        jc = preview.get("journal_context") if isinstance(preview.get("journal_context"), dict) else {}
        if str(jc.get("aggregate_journal_status") or "not_found") != "not_found":
            counts["aggregate_journal_found"] += 1
        if str(jc.get("per_fill_journal_status") or "not_found") != "not_found":
            counts["per_fill_journal_found"] += 1

    return {
        "ok": True,
        "version": "okx_fill_basis_preview_v1",
        "venue": "okx",
        "wallet_id": _okx_norm_wallet(wallet_id),
        "symbol": (str(symbol).strip().upper() if symbol else None),
        "limit": int(limit or 100),
        "dry_run": True,
        "will_mutate": False,
        "filters": {
            "trade_id": tid_filter or None,
            "venue_order_id": oid_filter or None,
            "slice_limit": int(slice_limit or 25),
        },
        "counts": counts,
        "items": previews if include_items else [],
    }

