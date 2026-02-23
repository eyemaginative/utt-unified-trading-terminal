# backend/app/services/lots_ledger.py

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import select, asc

from ..models import BasisLot


@dataclass
class LotSlice:
    lot_id: str
    acquired_at: Optional[datetime]
    qty: float
    basis_moved_usd: Optional[float]
    basis_is_missing: bool


@dataclass
class TransferOutImpact:
    version: str
    venue: str
    wallet_id: str
    asset: str
    qty_requested: float
    qty_consumed: float
    qty_missing: float
    total_basis_moved_usd: Optional[float]
    any_basis_missing: bool
    effective_at: Optional[datetime]
    slices: List[LotSlice]


@dataclass
class SellImpact:
    version: str
    venue: str
    wallet_id: str
    asset: str
    qty_sold: float
    price_usd: Optional[float]
    fee_usd: Optional[float]
    proceeds_usd: Optional[float]
    basis_consumed_usd: Optional[float]
    realized_gain_usd: Optional[float]
    any_basis_missing: bool
    effective_at: Optional[datetime]
    slices: List[LotSlice]


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _dt_utcnow() -> datetime:
    return datetime.utcnow()


# ------------------------------------------------------------------------------
# FIFO CONSUMERS
# ------------------------------------------------------------------------------

def fifo_consume_transfer_out(
    db: Session,
    *,
    venue: str,
    wallet_id: str,
    asset: str,
    qty: float,
    as_of: Optional[datetime] = None,
    allow_partial: bool = True,
) -> TransferOutImpact:
    """
    Consume lots FIFO for a TRANSFER_OUT (non-taxable).

    Semantics:
      - effective_at (= as_of) is returned for audit timeline
      - BasisLot.updated_at is ALWAYS commit-time (utcnow), not effective-time

    Determinism:
      - FIFO ordering is stabilized with BasisLot.id as a final tiebreaker.
    """
    if qty is None:
        raise ValueError("qty is required")
    qty = float(qty)
    if qty <= 0:
        raise ValueError("qty must be > 0")

    v = (venue or "").strip().lower()
    w = (wallet_id or "default").strip()
    a = (asset or "").strip().upper()

    effective_at = as_of

    stmt = (
        select(BasisLot)
        .where(
            BasisLot.venue == v,
            BasisLot.wallet_id == w,
            BasisLot.asset == a,
            BasisLot.qty_remaining > 0,
        )
        # IMPORTANT: stable FIFO tie-breaker
        .order_by(asc(BasisLot.acquired_at), asc(BasisLot.created_at), asc(BasisLot.id))
    )
    lots = db.execute(stmt).scalars().all()

    remaining = qty
    slices: List[LotSlice] = []

    total_basis_moved: float = 0.0
    any_basis_missing = False
    basis_sum_defined = True

    total_available = 0.0
    for lot in lots:
        qr = _safe_float(lot.qty_remaining) or 0.0
        if qr > 0:
            total_available += qr

    if not allow_partial and total_available + 1e-12 < qty:
        raise ValueError("insufficient lot inventory for transfer_out")

    for lot in lots:
        if remaining <= 0:
            break

        qr = _safe_float(lot.qty_remaining) or 0.0
        if qr <= 0:
            continue

        take = min(qr, remaining)
        if take <= 0:
            continue

        lot_total_basis = _safe_float(getattr(lot, "total_basis_usd", None))
        lot_qty_total = _safe_float(getattr(lot, "qty_total", None)) or 0.0
        lot_basis_missing = bool(getattr(lot, "basis_is_missing", False))

        if lot_total_basis is None or lot_basis_missing:
            any_basis_missing = True
            basis_moved = None
            basis_sum_defined = False
        else:
            if lot_qty_total > 0:
                basis_per_unit = lot_total_basis / lot_qty_total
                basis_moved = float(basis_per_unit * take)
                total_basis_moved += basis_moved
            else:
                any_basis_missing = True
                basis_moved = None
                basis_sum_defined = False

        # decrement
        lot.qty_remaining = float(qr - take)

        # commit-time updated_at (do NOT use effective_at/as_of here)
        lot.updated_at = _dt_utcnow()

        db.add(lot)

        slices.append(
            LotSlice(
                lot_id=str(lot.id),
                acquired_at=getattr(lot, "acquired_at", None),
                qty=float(take),
                basis_moved_usd=basis_moved,
                basis_is_missing=bool(lot_basis_missing) or (lot_total_basis is None),
            )
        )

        remaining -= take

    qty_consumed = float(qty - max(remaining, 0.0))
    qty_missing = float(max(remaining, 0.0))

    return TransferOutImpact(
        version="transfer_out_v1",
        venue=v,
        wallet_id=w,
        asset=a,
        qty_requested=float(qty),
        qty_consumed=qty_consumed,
        qty_missing=qty_missing,
        total_basis_moved_usd=(float(total_basis_moved) if basis_sum_defined else None),
        any_basis_missing=bool(any_basis_missing),
        effective_at=effective_at,
        slices=slices,
    )


def fifo_consume_sell_fifo(
    db: Session,
    *,
    venue: str,
    wallet_id: str,
    asset: str,
    qty_sold: float,
    price_usd: Optional[float],
    fee_usd: Optional[float],
    as_of: Optional[datetime] = None,
    allow_partial: bool = False,
) -> SellImpact:
    """
    Consume lots FIFO for a SELL (taxable), and compute realized gain (USD).

    Minimal semantics (safe + deterministic):
      - Consumes FIFO from BasisLot.qty_remaining
      - BasisLot.updated_at is commit-time (utcnow)
      - If allow_partial=False and insufficient inventory -> ValueError
      - If basis is missing for any consumed slice -> basis_consumed/realized become None

    Determinism:
      - FIFO ordering is stabilized with BasisLot.id as a final tiebreaker.
    """
    if qty_sold is None:
        raise ValueError("qty_sold is required")
    qty_sold = float(qty_sold)
    if qty_sold <= 0:
        raise ValueError("qty_sold must be > 0")

    v = (venue or "").strip().lower()
    w = (wallet_id or "default").strip()
    a = (asset or "").strip().upper()

    effective_at = as_of

    stmt = (
        select(BasisLot)
        .where(
            BasisLot.venue == v,
            BasisLot.wallet_id == w,
            BasisLot.asset == a,
            BasisLot.qty_remaining > 0,
        )
        # IMPORTANT: stable FIFO tie-breaker
        .order_by(asc(BasisLot.acquired_at), asc(BasisLot.created_at), asc(BasisLot.id))
    )
    lots = db.execute(stmt).scalars().all()

    total_available = 0.0
    for lot in lots:
        qr = _safe_float(lot.qty_remaining) or 0.0
        if qr > 0:
            total_available += qr

    if not allow_partial and total_available + 1e-12 < qty_sold:
        raise ValueError("insufficient lot inventory for sell")

    remaining = qty_sold
    slices: List[LotSlice] = []

    basis_consumed = 0.0
    any_basis_missing = False
    basis_defined = True

    for lot in lots:
        if remaining <= 0:
            break

        qr = _safe_float(lot.qty_remaining) or 0.0
        if qr <= 0:
            continue

        take = min(qr, remaining)
        if take <= 0:
            continue

        lot_total_basis = _safe_float(getattr(lot, "total_basis_usd", None))
        lot_qty_total = _safe_float(getattr(lot, "qty_total", None)) or 0.0
        lot_basis_missing = bool(getattr(lot, "basis_is_missing", False))

        if lot_total_basis is None or lot_basis_missing or lot_qty_total <= 0:
            any_basis_missing = True
            basis_defined = False
            basis_moved = None
        else:
            basis_per_unit = lot_total_basis / lot_qty_total
            basis_moved = float(basis_per_unit * take)
            basis_consumed += basis_moved

        lot.qty_remaining = float(qr - take)
        lot.updated_at = _dt_utcnow()
        db.add(lot)

        slices.append(
            LotSlice(
                lot_id=str(lot.id),
                acquired_at=getattr(lot, "acquired_at", None),
                qty=float(take),
                basis_moved_usd=basis_moved,
                basis_is_missing=bool(lot_basis_missing) or (lot_total_basis is None) or (lot_qty_total <= 0),
            )
        )

        remaining -= take

    qty_effective = float(qty_sold - max(remaining, 0.0))

    p = _safe_float(price_usd)
    f = _safe_float(fee_usd)

    proceeds = None
    if p is not None:
        gross = float(qty_effective * p)
        proceeds = gross - float(f or 0.0)

    basis_out = float(basis_consumed) if basis_defined else None
    realized = (proceeds - basis_out) if (proceeds is not None and basis_out is not None) else None

    return SellImpact(
        version="sell_fifo_v1",
        venue=v,
        wallet_id=w,
        asset=a,
        qty_sold=float(qty_effective),
        price_usd=p,
        fee_usd=f,
        proceeds_usd=proceeds,
        basis_consumed_usd=basis_out,
        realized_gain_usd=realized,
        any_basis_missing=bool(any_basis_missing),
        effective_at=effective_at,
        slices=slices,
    )


# ------------------------------------------------------------------------------
# IMPACT SERIALIZATION / TRANSFER-IN LOT CREATION
# ------------------------------------------------------------------------------

def impact_to_json(impact: Any) -> Dict[str, Any]:
    """
    Supports TransferOutImpact and SellImpact.
    """
    d = asdict(impact)

    if isinstance(d.get("effective_at"), datetime):
        d["effective_at"] = d["effective_at"].isoformat()

    for s in d.get("slices", []):
        if isinstance(s.get("acquired_at"), datetime):
            s["acquired_at"] = s["acquired_at"].isoformat()
    return d


def _impact_slices_from_withdrawal_raw(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    impact = (raw or {}).get("lot_impact") or {}
    slices = impact.get("slices") or []
    out = []
    for s in slices:
        if isinstance(s, dict):
            out.append(s)
    return out


def create_transfer_in_lots_from_withdrawal(
    db: Session,
    *,
    dest_venue: str,
    dest_wallet_id: str,
    asset: str,
    origin_deposit_id: str,
    withdrawal_id: str,
    withdrawal_raw: Dict[str, Any],
    commit_at: Optional[datetime] = None,
) -> Tuple[List[str], Dict[str, Any]]:
    """
    Create destination lots that inherit acquired_at + basis from the withdrawal's lot_impact slices.
    Returns: (created_lot_ids, summary_dict)
    """
    v = (dest_venue or "").strip().lower()
    w = (dest_wallet_id or "default").strip()
    a = (asset or "").strip().upper()
    commit_at = commit_at or _dt_utcnow()

    slices = _impact_slices_from_withdrawal_raw(withdrawal_raw or {})
    if not slices:
        raise ValueError("withdrawal.raw.lot_impact.slices missing; cannot create transfer-in lots")

    created_ids: List[str] = []
    any_basis_missing = False
    total_qty = 0.0
    total_basis_defined = True
    total_basis = 0.0

    for s in slices:
        qty = _safe_float(s.get("qty")) or 0.0
        if qty <= 0:
            continue

        acquired_at = s.get("acquired_at")
        if isinstance(acquired_at, str):
            try:
                acquired_at = datetime.fromisoformat(acquired_at.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                acquired_at = None
        elif isinstance(acquired_at, datetime):
            acquired_at = acquired_at.replace(tzinfo=None) if acquired_at.tzinfo else acquired_at
        else:
            acquired_at = None

        basis_moved = _safe_float(s.get("basis_moved_usd"))
        basis_missing = bool(s.get("basis_is_missing")) or (basis_moved is None)

        if basis_missing:
            any_basis_missing = True
            total_basis_defined = False
        else:
            total_basis += float(basis_moved)

        total_qty += float(qty)

        lot = BasisLot(
            venue=v,
            wallet_id=w,
            asset=a,
            acquired_at=acquired_at or commit_at,
            qty_total=float(qty),
            qty_remaining=float(qty),
            total_basis_usd=(None if basis_missing else float(basis_moved)),
            basis_is_missing=bool(basis_missing),
            basis_source="TRANSFER_LINK",
            origin_type="TRANSFER_IN",
            origin_ref=str(origin_deposit_id),
            note=f"linked from withdrawal {withdrawal_id} (src_lot={s.get('lot_id')})",
            created_at=commit_at,
            updated_at=commit_at,
        )
        db.add(lot)
        db.flush()
        created_ids.append(str(lot.id))

    summary = {
        "version": "transfer_link_v1",
        "withdrawal_id": withdrawal_id,
        "deposit_id": origin_deposit_id,
        "dest_venue": v,
        "dest_wallet_id": w,
        "asset": a,
        "lots_created": len(created_ids),
        "qty_total": float(total_qty),
        "any_basis_missing": bool(any_basis_missing),
        "total_basis_usd": (float(total_basis) if total_basis_defined else None),
        "created_lot_ids": created_ids,
    }
    return created_ids, summary


# ------------------------------------------------------------------------------
# REVERSAL PRIMITIVE (service-level)
# ------------------------------------------------------------------------------

def reverse_transfer_out_from_withdrawal_raw(
    db: Session,
    *,
    withdrawal_raw: Dict[str, Any],
    commit_at: Optional[datetime] = None,
    clamp_to_qty_total: bool = True,
) -> Dict[str, Any]:
    """
    Reverse a previously-applied TRANSFER_OUT lot impact by restoring BasisLot.qty_remaining
    based on withdrawal_raw['lot_impact']['slices'].

    This does NOT delete the withdrawal row or any audit tables; it only restores lot inventory.

    Returns a summary dict:
      {
        "version": "transfer_out_reverse_v1",
        "restored_lots": int,
        "restored_qty_total": float,
        "missing_lots": [lot_id, ...],
        "errors": [str, ...]
      }

    Notes:
      - Uses commit_at (or utcnow) for BasisLot.updated_at (commit-time semantics).
      - If clamp_to_qty_total=True, qty_remaining will not exceed qty_total.
    """
    commit_at = commit_at or _dt_utcnow()
    slices = _impact_slices_from_withdrawal_raw(withdrawal_raw or {})

    restored_lots = 0
    restored_qty_total = 0.0
    missing_lots: List[str] = []
    errors: List[str] = []

    for s in slices:
        lot_id = s.get("lot_id") or s.get("basis_lot_id") or s.get("source_lot_id")
        if not lot_id:
            continue
        qty = _safe_float(s.get("qty"))
        if qty is None or qty <= 0:
            continue

        lot = db.get(BasisLot, str(lot_id))
        if not lot:
            missing_lots.append(str(lot_id))
            continue

        try:
            qr = _safe_float(getattr(lot, "qty_remaining", None)) or 0.0
            qt = _safe_float(getattr(lot, "qty_total", None)) or 0.0

            new_qr = float(qr + float(qty))
            if clamp_to_qty_total and qt > 0:
                new_qr = min(float(qt), new_qr)

            lot.qty_remaining = float(new_qr)
            lot.updated_at = commit_at
            db.add(lot)

            restored_lots += 1
            restored_qty_total += float(qty)
        except Exception as e:
            errors.append(f"restore lot {lot_id}: {type(e).__name__}: {e}")

    return {
        "version": "transfer_out_reverse_v1",
        "restored_lots": int(restored_lots),
        "restored_qty_total": float(restored_qty_total),
        "missing_lots": missing_lots,
        "errors": errors,
    }
