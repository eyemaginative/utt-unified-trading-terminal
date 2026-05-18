# backend/app/services/lots_ledger.py

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import select, asc, exists

from ..models import AssetDeposit, AssetWithdrawal, BasisLot


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


def _norm_venue(v: Any) -> str:
    return str(v or "").strip().lower()


def _norm_wallet(w: Any) -> str:
    s = str(w or "default").strip()
    return s if s else "default"



# ------------------------------------------------------------------------------
# DEPOSIT -> MISSING-BASIS LOT CREATION
# ------------------------------------------------------------------------------

def create_missing_basis_lots_from_deposits(
    db: Session,
    *,
    venue: Optional[str] = None,
    wallet_id: Optional[str] = None,
    asset: Optional[str] = None,
    status: Optional[str] = None,
    source_contains: Optional[str] = None,
    limit: int = 500,
    dry_run: bool = True,
) -> Dict[str, Any]:
    """
    Create one missing-basis BasisLot for each AssetDeposit that does not already
    have any lot rows linked by BasisLot.origin_ref == AssetDeposit.id.

    This is intentionally conservative:
      - no cost basis is invented
      - total_basis_usd remains None
      - basis_is_missing remains True
      - qty_total and qty_remaining mirror the deposit qty
      - idempotency comes from skipping deposits that already have linked lots
    """
    lim = max(1, min(int(limit or 500), 5000))

    lot_exists = exists(
        select(1)
        .select_from(BasisLot)
        .where(BasisLot.origin_ref == AssetDeposit.id)
    )

    stmt = (
        select(AssetDeposit)
        .where(~lot_exists)
        .order_by(AssetDeposit.deposit_time.asc(), AssetDeposit.id.asc())
        .limit(lim)
    )

    if venue:
        stmt = stmt.where(AssetDeposit.venue == _norm_venue(venue))
    if wallet_id:
        stmt = stmt.where(AssetDeposit.wallet_id == _norm_wallet(wallet_id))
    if asset:
        stmt = stmt.where(AssetDeposit.asset == str(asset).strip().upper())
    if status:
        stmt = stmt.where(AssetDeposit.status == str(status).strip().upper())
    if source_contains:
        stmt = stmt.where(AssetDeposit.source.like(f"%{str(source_contains).strip()}%"))

    rows = db.execute(stmt).scalars().all()

    out: Dict[str, Any] = {
        "version": "deposit_missing_basis_lots_v1",
        "dry_run": bool(dry_run),
        "limit": int(lim),
        "filters": {
            "venue": venue,
            "wallet_id": wallet_id,
            "asset": asset,
            "status": status,
            "source_contains": source_contains,
        },
        "considered": 0,
        "eligible": 0,
        "would_create_lots": 0,
        "created_lots": 0,
        "existing_lots": 0,
        "skip_reasons": {},
        "examples": [],
    }

    def bump(reason: str) -> None:
        out["skip_reasons"][reason] = int(out["skip_reasons"].get(reason) or 0) + 1

    def example(item: Dict[str, Any], cap: int = 20) -> None:
        if len(out["examples"]) < cap:
            out["examples"].append(item)

    for d in rows:
        out["considered"] += 1

        dep_id = str(getattr(d, "id", "") or "")
        if not dep_id:
            bump("missing_deposit_id")
            continue

        qty = _safe_float(getattr(d, "qty", None))
        if qty is None or qty <= 0:
            bump("invalid_qty")
            continue

        dep_asset = str(getattr(d, "asset", "") or "").strip().upper()
        if not dep_asset:
            bump("missing_asset")
            continue

        dep_venue = _norm_venue(getattr(d, "venue", None))
        dep_wallet = _norm_wallet(getattr(d, "wallet_id", None))
        acquired_at = getattr(d, "deposit_time", None) or _dt_utcnow()
        if not isinstance(acquired_at, datetime):
            acquired_at = _dt_utcnow()

        # Defensive re-check in case another path created a lot after the initial query.
        existing = db.execute(
            select(BasisLot).where(BasisLot.origin_ref == dep_id)
        ).scalars().first()
        if existing:
            out["existing_lots"] += 1
            continue

        out["eligible"] += 1
        out["would_create_lots"] += 1
        example({
            "kind": "missing_basis_lot_candidate",
            "deposit_id": dep_id,
            "venue": dep_venue,
            "wallet_id": dep_wallet,
            "asset": dep_asset,
            "qty": float(qty),
            "acquired_at": acquired_at.isoformat(),
            "txid": getattr(d, "txid", None),
            "source": getattr(d, "source", None),
        })

        if dry_run:
            continue

        lot = BasisLot(
            venue=dep_venue,
            wallet_id=dep_wallet,
            asset=dep_asset,
            acquired_at=acquired_at,
            qty_total=float(qty),
            qty_remaining=float(qty),
            total_basis_usd=None,
            basis_is_missing=True,
            basis_source="DEPOSIT_MISSING",
            origin_type="DEPOSIT",
            origin_ref=dep_id,
            note=f"auto missing-basis lot from deposit {dep_id}",
            created_at=_dt_utcnow(),
            updated_at=_dt_utcnow(),
        )
        db.add(lot)
        db.flush()

        if hasattr(d, "lot_id"):
            try:
                d.lot_id = str(lot.id)
            except Exception:
                pass
        if hasattr(d, "lot_ids"):
            try:
                d.lot_ids = [str(lot.id)]
            except Exception:
                pass
        if hasattr(d, "updated_at"):
            d.updated_at = _dt_utcnow()
        db.add(d)

        out["created_lots"] += 1

    out["status"] = "preview" if dry_run else "applied"
    return out


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



# ------------------------------------------------------------------------------
# TRANSFER LINK PREVIEW / MATCH-ONLY APPLY
# ------------------------------------------------------------------------------

def _raw_dict(obj: Any) -> Dict[str, Any]:
    try:
        raw = getattr(obj, "raw", None)
        if isinstance(raw, dict):
            return dict(raw)
    except Exception:
        pass
    return {}


def _get_deposit_transfer_withdrawal_id(d: AssetDeposit) -> Optional[str]:
    try:
        v = getattr(d, "transfer_withdrawal_id", None)
        if v:
            return str(v)
    except Exception:
        pass
    raw = _raw_dict(d)
    v = raw.get("transfer_withdrawal_id") or raw.get("transferWithdrawalId")
    return str(v) if v else None


def _set_deposit_transfer_withdrawal_id(d: AssetDeposit, withdrawal_id: str, *, metadata: Optional[Dict[str, Any]] = None) -> None:
    raw = _raw_dict(d)
    raw["transfer_withdrawal_id"] = str(withdrawal_id)
    raw["transfer_link"] = dict(metadata or {})
    setattr(d, "raw", raw)
    if hasattr(d, "transfer_withdrawal_id"):
        try:
            setattr(d, "transfer_withdrawal_id", str(withdrawal_id))
        except Exception:
            pass
    if hasattr(d, "updated_at"):
        d.updated_at = _dt_utcnow()


def _get_withdrawal_transfer_deposit_id(w: AssetWithdrawal) -> Optional[str]:
    try:
        v = getattr(w, "transfer_deposit_id", None)
        if v:
            return str(v)
    except Exception:
        pass
    raw = _raw_dict(w)
    v = raw.get("transfer_deposit_id") or raw.get("transferDepositId")
    return str(v) if v else None


def _set_withdrawal_transfer_deposit_id(w: AssetWithdrawal, deposit_id: str, *, metadata: Optional[Dict[str, Any]] = None) -> None:
    raw = _raw_dict(w)
    raw["transfer_deposit_id"] = str(deposit_id)
    raw["transfer_link"] = dict(metadata or {})
    setattr(w, "raw", raw)
    if hasattr(w, "transfer_deposit_id"):
        try:
            setattr(w, "transfer_deposit_id", str(deposit_id))
        except Exception:
            pass
    if hasattr(w, "updated_at"):
        w.updated_at = _dt_utcnow()


def _dt_or_none(x: Any) -> Optional[datetime]:
    if isinstance(x, datetime):
        try:
            return x.replace(tzinfo=None)
        except Exception:
            return x
    if x is None:
        return None
    try:
        return datetime.fromisoformat(str(x).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _amounts_match(a: Any, b: Any, *, abs_tol: float, pct_tol: float) -> Tuple[bool, float, float]:
    af = _safe_float(a)
    bf = _safe_float(b)
    if af is None or bf is None:
        return False, 0.0, 0.0
    diff = abs(float(af) - float(bf))
    base = max(abs(float(af)), abs(float(bf)), 1e-12)
    pct = diff / base
    ok = diff <= float(abs_tol) or pct <= float(pct_tol)
    return bool(ok), float(diff), float(pct)


def _tx_preview(txid: Any) -> Optional[str]:
    s = str(txid or "").strip()
    if not s:
        return None
    if len(s) <= 24:
        return s
    return f"{s[:10]}…{s[-8:]}"


def rebuild_transfer_links(
    db: Session,
    *,
    venue: Optional[str] = None,
    wallet_id: Optional[str] = None,
    asset: Optional[str] = None,
    status: Optional[str] = "DETECTED",
    source_contains: Optional[str] = None,
    time_window_minutes: int = 1440,
    amount_tolerance_abs: float = 1e-9,
    amount_tolerance_pct: float = 1e-9,
    limit_deposits: int = 1000,
    limit_withdrawals: int = 1000,
    apply_limit: int = 100,
    require_withdrawal_before_deposit: bool = True,
    dry_run: bool = True,
) -> Dict[str, Any]:
    """
    Preview/apply match-only transfer links between AssetWithdrawal and AssetDeposit.

    This function deliberately does NOT consume FIFO lots and does NOT create
    transfer-in lots. It only persists cross-row linkage metadata:
      - AssetDeposit.transfer_withdrawal_id / deposit.raw.transfer_withdrawal_id
      - withdrawal.raw.transfer_deposit_id

    Matching is conservative:
      - same asset required
      - amount must match within tolerances
      - txid must differ when both txids are present
      - absolute time delta must be within time_window_minutes
      - by default, deposit must occur at/after withdrawal
      - already-linked deposits/withdrawals are skipped
    """
    dep_lim = max(1, min(int(limit_deposits or 1000), 5000))
    wd_lim = max(1, min(int(limit_withdrawals or 1000), 5000))
    apply_lim = max(1, min(int(apply_limit or 100), 1000))
    window_s = max(1, int(time_window_minutes or 1440)) * 60

    v_filter = _norm_venue(venue) if venue else None
    w_filter = _norm_wallet(wallet_id) if wallet_id else None
    a_filter = str(asset or "").strip().upper() if asset else None
    st_filter = str(status or "").strip().upper() if status else None

    dep_stmt = select(AssetDeposit).order_by(AssetDeposit.deposit_time.asc(), AssetDeposit.id.asc()).limit(dep_lim)
    wd_stmt = select(AssetWithdrawal).order_by(AssetWithdrawal.withdraw_time.asc(), AssetWithdrawal.id.asc()).limit(wd_lim)

    if v_filter:
        dep_stmt = dep_stmt.where(AssetDeposit.venue == v_filter)
        wd_stmt = wd_stmt.where(AssetWithdrawal.venue == v_filter)
    if w_filter:
        dep_stmt = dep_stmt.where(AssetDeposit.wallet_id == w_filter)
        wd_stmt = wd_stmt.where(AssetWithdrawal.wallet_id == w_filter)
    if a_filter:
        dep_stmt = dep_stmt.where(AssetDeposit.asset == a_filter)
        wd_stmt = wd_stmt.where(AssetWithdrawal.asset == a_filter)
    if st_filter:
        dep_stmt = dep_stmt.where(AssetDeposit.status == st_filter)
        wd_stmt = wd_stmt.where(AssetWithdrawal.status == st_filter)
    if source_contains:
        needle = str(source_contains).strip()
        dep_stmt = dep_stmt.where(AssetDeposit.source.like(f"%{needle}%"))
        wd_stmt = wd_stmt.where(AssetWithdrawal.source.like(f"%{needle}%"))

    deposits = list(db.execute(dep_stmt).scalars().all())
    withdrawals = list(db.execute(wd_stmt).scalars().all())

    out: Dict[str, Any] = {
        "version": "transfer_link_preview_v1",
        "dry_run": bool(dry_run),
        "filters": {
            "venue": venue,
            "wallet_id": wallet_id,
            "asset": asset,
            "status": status,
            "source_contains": source_contains,
            "time_window_minutes": int(time_window_minutes or 1440),
            "amount_tolerance_abs": float(amount_tolerance_abs),
            "amount_tolerance_pct": float(amount_tolerance_pct),
            "limit_deposits": int(dep_lim),
            "limit_withdrawals": int(wd_lim),
            "apply_limit": int(apply_lim),
            "require_withdrawal_before_deposit": bool(require_withdrawal_before_deposit),
        },
        "deposits_considered": len(deposits),
        "withdrawals_considered": len(withdrawals),
        "candidates": 0,
        "strong_candidates": 0,
        "linked": 0,
        "skipped": {},
        "examples": [],
        "applied": [],
    }

    def bump(reason: str) -> None:
        out["skipped"][reason] = int(out["skipped"].get(reason) or 0) + 1

    def add_example(item: Dict[str, Any], cap: int = 25) -> None:
        if len(out["examples"]) < cap:
            out["examples"].append(item)

    # Pre-group deposits by asset to avoid O(n*m) across unrelated assets.
    deps_by_asset: Dict[str, List[AssetDeposit]] = {}
    for d in deposits:
        if _get_deposit_transfer_withdrawal_id(d):
            bump("deposit_already_linked")
            continue
        da = str(getattr(d, "asset", "") or "").strip().upper()
        if not da:
            bump("deposit_missing_asset")
            continue
        deps_by_asset.setdefault(da, []).append(d)

    candidate_rows: List[Dict[str, Any]] = []
    for wd in withdrawals:
        if _get_withdrawal_transfer_deposit_id(wd):
            bump("withdrawal_already_linked")
            continue

        wa = str(getattr(wd, "asset", "") or "").strip().upper()
        if not wa:
            bump("withdrawal_missing_asset")
            continue

        wqty = _safe_float(getattr(wd, "qty", None))
        if wqty is None or wqty <= 0:
            bump("withdrawal_invalid_qty")
            continue

        wtime = _dt_or_none(getattr(wd, "withdraw_time", None))
        if not wtime:
            bump("withdrawal_missing_time")
            continue

        wtx = str(getattr(wd, "txid", "") or "").strip()

        for dep in deps_by_asset.get(wa, []):
            dqty = _safe_float(getattr(dep, "qty", None))
            if dqty is None or dqty <= 0:
                continue

            ok_amt, diff, pct = _amounts_match(wqty, dqty, abs_tol=amount_tolerance_abs, pct_tol=amount_tolerance_pct)
            if not ok_amt:
                continue

            dtime = _dt_or_none(getattr(dep, "deposit_time", None))
            if not dtime:
                continue

            delta_s = (dtime - wtime).total_seconds()
            abs_delta_s = abs(delta_s)
            if abs_delta_s > window_s:
                continue
            if bool(require_withdrawal_before_deposit) and delta_s < 0:
                bump("deposit_before_withdrawal_skipped")
                continue

            dtx = str(getattr(dep, "txid", "") or "").strip()
            if wtx and dtx and wtx == dtx:
                bump("same_txid_skipped")
                continue

            # Scoring: prefer withdrawal-before-deposit, closer time, exact amount.
            score = 0.0
            if delta_s >= 0:
                score += 1000.0
            score += max(0.0, 500.0 - (abs_delta_s / 60.0))
            score += max(0.0, 100.0 - (pct * 1_000_000.0))

            dep_raw = _raw_dict(dep)
            wd_raw = _raw_dict(wd)
            row = {
                "score": float(score),
                "withdrawal_id": str(getattr(wd, "id", "")),
                "deposit_id": str(getattr(dep, "id", "")),
                "asset": wa,
                "withdrawal_qty": float(wqty),
                "deposit_qty": float(dqty),
                "amount_diff": float(diff),
                "amount_diff_pct": float(pct),
                "withdraw_time": wtime.isoformat(),
                "deposit_time": dtime.isoformat(),
                "delta_minutes": float(delta_s / 60.0),
                "withdrawal_txid_preview": _tx_preview(wtx),
                "deposit_txid_preview": _tx_preview(dtx),
                "withdrawal_source": getattr(wd, "source", None),
                "deposit_source": getattr(dep, "source", None),
                "withdrawal_network": getattr(wd, "network", None),
                "deposit_network": getattr(dep, "network", None),
                "withdrawal_counterparty_known": bool(getattr(wd, "destination", None) or wd_raw.get("counterparty")),
                "deposit_counterparty_known": bool(dep_raw.get("counterparty")),
                "match_type": "withdrawal_before_deposit" if delta_s >= 0 else "deposit_before_withdrawal",
                "would_apply": True,
            }
            candidate_rows.append({"withdrawal": wd, "deposit": dep, "public": row, "score": score})

    candidate_rows.sort(key=lambda x: x.get("score", 0.0), reverse=True)

    used_w: set[str] = set()
    used_d: set[str] = set()
    selected: List[Dict[str, Any]] = []
    for row in candidate_rows:
        pub = row["public"]
        wid = pub["withdrawal_id"]
        did = pub["deposit_id"]
        if wid in used_w or did in used_d:
            continue
        used_w.add(wid)
        used_d.add(did)
        selected.append(row)
        out["candidates"] += 1
        if pub.get("match_type") == "withdrawal_before_deposit":
            out["strong_candidates"] += 1
        add_example(pub)

    if dry_run:
        out["status"] = "preview"
        return out

    for row in selected[:apply_lim]:
        wd = row["withdrawal"]
        dep = row["deposit"]
        pub = dict(row["public"])

        # Re-check before applying to keep idempotency safe.
        if _get_deposit_transfer_withdrawal_id(dep) or _get_withdrawal_transfer_deposit_id(wd):
            bump("became_linked_before_apply")
            continue

        link_meta = {
            "version": "transfer_link_preview_v1",
            "linked_at": _dt_utcnow().isoformat(),
            "match_type": pub.get("match_type"),
            "score": pub.get("score"),
            "amount_diff": pub.get("amount_diff"),
            "amount_diff_pct": pub.get("amount_diff_pct"),
            "delta_minutes": pub.get("delta_minutes"),
            "withdrawal_txid_preview": pub.get("withdrawal_txid_preview"),
            "deposit_txid_preview": pub.get("deposit_txid_preview"),
            "mode": "match_only_no_fifo",
        }
        _set_deposit_transfer_withdrawal_id(dep, str(getattr(wd, "id", "")), metadata={**link_meta, "role": "deposit"})
        _set_withdrawal_transfer_deposit_id(wd, str(getattr(dep, "id", "")), metadata={**link_meta, "role": "withdrawal"})

        db.add(dep)
        db.add(wd)
        out["linked"] += 1
        if len(out["applied"]) < 25:
            out["applied"].append(pub)

    out["status"] = "applied"
    return out


# ------------------------------------------------------------------------------
# WITHDRAWAL FIFO LOT-IMPACT REBUILD / EXPLICIT TRANSFER_OUT APPLY
# ------------------------------------------------------------------------------

def _withdrawal_has_existing_lot_impact(w: AssetWithdrawal) -> bool:
    raw = _raw_dict(w)
    if bool(raw.get("lot_impact_applied")):
        return True
    impact = raw.get("lot_impact")
    if isinstance(impact, dict) and impact:
        return True
    slices = raw.get("lot_impact_slices")
    if isinstance(slices, list) and slices:
        return True
    return False



_LP_SPECIAL_ASSETS = {"2-POOL"}
_LP_SPECIAL_RAW_HINTS = (
    "modlomnipool",
    "omnipool",
    "stableswap",
    "stablepool",
)


def _is_lp_special_withdrawal(w: AssetWithdrawal) -> Tuple[bool, Dict[str, Any]]:
    """
    Identify withdrawals that should not be processed as normal FIFO transfer-outs.

    v1 intentionally starts with the known Hydration LP/pool token case:
      - asset == 2-POOL
      - raw/provider hints mentioning Omnipool / stable pool modules

    These rows remain visible, but default rebuild_lot_impacts skips them so they
    cannot accidentally consume normal inventory.
    """
    asset = str(getattr(w, "asset", "") or "").strip().upper()
    raw = _raw_dict(w)

    hints: List[str] = []
    if asset in _LP_SPECIAL_ASSETS:
        hints.append(f"asset:{asset}")

    raw_text = ""
    try:
        raw_text = str(raw).lower()
    except Exception:
        raw_text = ""

    for h in _LP_SPECIAL_RAW_HINTS:
        try:
            if h and h.lower() in raw_text:
                hints.append(f"raw_hint:{h}")
        except Exception:
            pass

    is_special = bool(hints)
    return is_special, {
        "version": "lp_special_handling_v1",
        "asset": asset,
        "reason": "LP_OR_POOL_TOKEN_SPECIAL_HANDLING" if is_special else None,
        "hints": hints,
        "policy": "excluded_from_normal_fifo_by_default",
    }


def _mark_withdrawal_lp_special(w: AssetWithdrawal, metadata: Dict[str, Any]) -> None:
    raw = _raw_dict(w)
    raw["lot_impact_status"] = "LP_SPECIAL_HANDLING"
    raw["lot_impact_mode"] = "lp_special_handling_skipped"
    raw["lp_special_handling"] = dict(metadata or {})
    raw["lp_special_marked_at"] = _dt_utcnow().isoformat()
    setattr(w, "raw", raw)
    if hasattr(w, "updated_at"):
        w.updated_at = _dt_utcnow()


def rebuild_withdrawal_lot_impacts(
    db: Session,
    *,
    venue: Optional[str] = None,
    wallet_id: Optional[str] = None,
    asset: Optional[str] = None,
    status: Optional[str] = "DETECTED",
    source_contains: Optional[str] = None,
    limit: int = 500,
    apply_limit: int = 100,
    allow_partial: bool = False,
    include_transfer_linked: bool = False,
    include_lp_special: bool = False,
    mark_lp_special: bool = False,
    force_rebuild: bool = False,
    dry_run: bool = True,
) -> Dict[str, Any]:
    """
    Preview/apply FIFO TRANSFER_OUT lot impact for existing AssetWithdrawal rows.

    This is deliberately explicit-only and conservative:
      - dry_run defaults true at the router
      - allow_partial defaults false
      - transfer-linked withdrawals are skipped unless include_transfer_linked=true
      - LP / pool-token withdrawals are skipped unless include_lp_special=true
      - already-applied withdrawals are skipped unless force_rebuild=true

    NOTE: force_rebuild currently permits reprocessing only for rows where the
    caller explicitly asks. It does not reverse existing lot impact first; use it
    only after manual review or a separate reset/reversal workflow.
    """
    lim = max(1, min(int(limit or 500), 5000))
    apply_lim = max(1, min(int(apply_limit or 100), 1000))

    v_filter = _norm_venue(venue) if venue else None
    w_filter = _norm_wallet(wallet_id) if wallet_id else None
    a_filter = str(asset or "").strip().upper() if asset else None
    st_filter = str(status or "").strip().upper() if status else None

    stmt = select(AssetWithdrawal).order_by(AssetWithdrawal.withdraw_time.asc(), AssetWithdrawal.id.asc()).limit(lim)
    if v_filter:
        stmt = stmt.where(AssetWithdrawal.venue == v_filter)
    if w_filter:
        stmt = stmt.where(AssetWithdrawal.wallet_id == w_filter)
    if a_filter:
        stmt = stmt.where(AssetWithdrawal.asset == a_filter)
    if st_filter:
        stmt = stmt.where(AssetWithdrawal.status == st_filter)
    if source_contains:
        needle = str(source_contains).strip()
        stmt = stmt.where(AssetWithdrawal.source.like(f"%{needle}%"))

    rows = list(db.execute(stmt).scalars().all())

    out: Dict[str, Any] = {
        "version": "withdrawal_lot_impact_rebuild_v1",
        "dry_run": bool(dry_run),
        "filters": {
            "venue": venue,
            "wallet_id": wallet_id,
            "asset": asset,
            "status": status,
            "source_contains": source_contains,
            "limit": int(lim),
            "apply_limit": int(apply_lim),
            "allow_partial": bool(allow_partial),
            "include_transfer_linked": bool(include_transfer_linked),
            "include_lp_special": bool(include_lp_special),
            "mark_lp_special": bool(mark_lp_special),
            "lp_special_assets": sorted(_LP_SPECIAL_ASSETS),
            "force_rebuild": bool(force_rebuild),
        },
        "considered": len(rows),
        "eligible": 0,
        "would_apply": 0,
        "applied": 0,
        "skipped": {},
        "examples": [],
        "applied_examples": [],
        "lp_special_marked": 0,
    }

    def bump(reason: str) -> None:
        out["skipped"][reason] = int(out["skipped"].get(reason) or 0) + 1

    def add_example(item: Dict[str, Any], *, key: str = "examples", cap: int = 25) -> None:
        if len(out[key]) < cap:
            out[key].append(item)

    applied_count = 0

    for w in rows:
        wid = str(getattr(w, "id", "") or "")
        if not wid:
            bump("missing_withdrawal_id")
            continue

        if _withdrawal_has_existing_lot_impact(w) and not bool(force_rebuild):
            bump("lot_impact_already_applied")
            continue

        transfer_deposit_id = _get_withdrawal_transfer_deposit_id(w)
        if transfer_deposit_id and not bool(include_transfer_linked):
            bump("transfer_linked_skipped")
            continue

        qty = _safe_float(getattr(w, "qty", None))
        if qty is None or qty <= 0:
            bump("invalid_qty")
            continue

        wd_asset = str(getattr(w, "asset", "") or "").strip().upper()
        if not wd_asset:
            bump("missing_asset")
            continue

        wd_venue = _norm_venue(getattr(w, "venue", None))
        wd_wallet = _norm_wallet(getattr(w, "wallet_id", None))
        wd_time = _dt_or_none(getattr(w, "withdraw_time", None))
        if not wd_time:
            bump("missing_withdraw_time")
            continue

        lp_special, lp_meta = _is_lp_special_withdrawal(w)
        if lp_special and not bool(include_lp_special):
            bump("lp_special_handling_skipped")
            raw_before_mark = _raw_dict(w)
            already_lp_marked = str(raw_before_mark.get("lot_impact_status") or "").strip().upper() == "LP_SPECIAL_HANDLING"
            if already_lp_marked:
                bump("lp_special_already_marked")

            special_summary = {
                "kind": "withdrawal_lp_special_handling_skipped",
                "withdrawal_id": wid,
                "venue": wd_venue,
                "wallet_id": wd_wallet,
                "asset": wd_asset,
                "qty": float(qty),
                "withdraw_time": wd_time.isoformat(),
                "txid": getattr(w, "txid", None),
                "source": getattr(w, "source", None),
                "transfer_deposit_id": transfer_deposit_id,
                "lp_special_handling": lp_meta,
                "already_marked": bool(already_lp_marked),
                "would_apply": False,
            }
            add_example(special_summary)

            if bool(mark_lp_special) and not bool(dry_run) and not already_lp_marked:
                _mark_withdrawal_lp_special(w, lp_meta)
                db.add(w)
                out["lp_special_marked"] += 1

            continue

        out["eligible"] += 1
        out["would_apply"] += 1

        try:
            impact = fifo_consume_transfer_out(
                db,
                venue=wd_venue,
                wallet_id=wd_wallet,
                asset=wd_asset,
                qty=float(qty),
                as_of=wd_time,
                allow_partial=bool(allow_partial),
            )
            impact_json = impact_to_json(impact)
        except ValueError as e:
            bump("fifo_error")
            add_example({
                "kind": "withdrawal_lot_impact_error",
                "withdrawal_id": wid,
                "venue": wd_venue,
                "wallet_id": wd_wallet,
                "asset": wd_asset,
                "qty": float(qty),
                "withdraw_time": wd_time.isoformat(),
                "txid": getattr(w, "txid", None),
                "source": getattr(w, "source", None),
                "error": str(e),
            })
            continue

        summary = {
            "kind": "withdrawal_lot_impact_candidate",
            "withdrawal_id": wid,
            "venue": wd_venue,
            "wallet_id": wd_wallet,
            "asset": wd_asset,
            "qty": float(qty),
            "withdraw_time": wd_time.isoformat(),
            "txid": getattr(w, "txid", None),
            "source": getattr(w, "source", None),
            "transfer_deposit_id": transfer_deposit_id,
            "qty_consumed": float(impact.qty_consumed),
            "qty_missing": float(impact.qty_missing),
            "slices": len(impact.slices),
            "any_basis_missing": bool(impact.any_basis_missing),
            "total_basis_moved_usd": impact.total_basis_moved_usd,
            "would_apply": applied_count < apply_lim,
        }
        add_example(summary)

        if dry_run:
            # fifo_consume_transfer_out mutates ORM lot objects for preview.
            # The router rolls back at the end when dry_run=true.
            continue

        if applied_count >= apply_lim:
            bump("apply_limit_reached")
            continue

        raw = _raw_dict(w)
        raw["lot_impact"] = impact_json
        raw["lot_impact_applied"] = True
        raw["lot_impact_applied_at"] = _dt_utcnow().isoformat()
        raw["lot_impact_mode"] = "explicit_withdrawal_rebuild"
        raw["lot_impact_allow_partial"] = bool(allow_partial)
        if transfer_deposit_id:
            raw["transfer_deposit_id"] = str(transfer_deposit_id)
        setattr(w, "raw", raw)
        if hasattr(w, "updated_at"):
            w.updated_at = _dt_utcnow()
        db.add(w)

        out["applied"] += 1
        applied_count += 1
        add_example(summary, key="applied_examples")

    out["status"] = "preview" if dry_run else "applied"
    return out


# ------------------------------------------------------------------------------
# WITHDRAWAL INVENTORY GAP DIAGNOSTICS
# ------------------------------------------------------------------------------

def diagnose_withdrawal_inventory_gaps(
    db: Session,
    *,
    venue: Optional[str] = None,
    wallet_id: Optional[str] = None,
    asset: Optional[str] = None,
    status: Optional[str] = "DETECTED",
    source_contains: Optional[str] = None,
    limit: int = 500,
    include_transfer_linked: bool = False,
    include_applied: bool = False,
) -> Dict[str, Any]:
    """
    Dry diagnostic for withdrawals that cannot pass strict FIFO TRANSFER_OUT.

    This function performs no mutations. It compares candidate withdrawals against
    current BasisLot.qty_remaining by asset and reports which assets have an
    inventory gap under the current ledger state.

    It intentionally mirrors the safe filters used by rebuild_withdrawal_lot_impacts:
      - transfer-linked withdrawals are excluded by default
      - already-applied withdrawals are excluded by default
      - source/status/venue/wallet filters are supported
    """
    lim = max(1, min(int(limit or 500), 5000))

    v_filter = _norm_venue(venue) if venue else None
    w_filter = _norm_wallet(wallet_id) if wallet_id else None
    a_filter = str(asset or "").strip().upper() if asset else None
    st_filter = str(status or "").strip().upper() if status else None

    wd_stmt = select(AssetWithdrawal).order_by(AssetWithdrawal.withdraw_time.asc(), AssetWithdrawal.id.asc()).limit(lim)
    if v_filter:
        wd_stmt = wd_stmt.where(AssetWithdrawal.venue == v_filter)
    if w_filter:
        wd_stmt = wd_stmt.where(AssetWithdrawal.wallet_id == w_filter)
    if a_filter:
        wd_stmt = wd_stmt.where(AssetWithdrawal.asset == a_filter)
    if st_filter:
        wd_stmt = wd_stmt.where(AssetWithdrawal.status == st_filter)
    if source_contains:
        needle = str(source_contains).strip()
        wd_stmt = wd_stmt.where(AssetWithdrawal.source.like(f"%{needle}%"))

    withdrawals = list(db.execute(wd_stmt).scalars().all())

    out: Dict[str, Any] = {
        "version": "withdrawal_inventory_gap_diagnostics_v1",
        "dry_run": True,
        "filters": {
            "venue": venue,
            "wallet_id": wallet_id,
            "asset": asset,
            "status": status,
            "source_contains": source_contains,
            "limit": int(lim),
            "include_transfer_linked": bool(include_transfer_linked),
            "include_applied": bool(include_applied),
        },
        "withdrawals_considered": len(withdrawals),
        "withdrawals_included": 0,
        "skipped": {},
        "assets": {},
        "examples": [],
        "status": "preview",
    }

    def bump(reason: str) -> None:
        out["skipped"][reason] = int(out["skipped"].get(reason) or 0) + 1

    def add_example(item: Dict[str, Any], cap: int = 30) -> None:
        if len(out["examples"]) < cap:
            out["examples"].append(item)

    grouped: Dict[str, List[AssetWithdrawal]] = {}

    for w in withdrawals:
        if _withdrawal_has_existing_lot_impact(w) and not bool(include_applied):
            bump("lot_impact_already_applied")
            continue

        if _get_withdrawal_transfer_deposit_id(w) and not bool(include_transfer_linked):
            bump("transfer_linked_skipped")
            continue

        qty = _safe_float(getattr(w, "qty", None))
        if qty is None or qty <= 0:
            bump("invalid_qty")
            continue

        wd_asset = str(getattr(w, "asset", "") or "").strip().upper()
        if not wd_asset:
            bump("missing_asset")
            continue

        grouped.setdefault(wd_asset, []).append(w)
        out["withdrawals_included"] += 1

    for a, rows in sorted(grouped.items()):
        # Work out the normalized venue/wallet to inspect lots for this asset.
        # Prefer explicit filters, then first withdrawal row.
        sample = rows[0] if rows else None
        lot_venue = v_filter or _norm_venue(getattr(sample, "venue", None))
        lot_wallet = w_filter or _norm_wallet(getattr(sample, "wallet_id", None))

        lot_stmt = (
            select(BasisLot)
            .where(
                BasisLot.venue == lot_venue,
                BasisLot.wallet_id == lot_wallet,
                BasisLot.asset == a,
            )
            .order_by(asc(BasisLot.acquired_at), asc(BasisLot.created_at), asc(BasisLot.id))
        )
        lots = list(db.execute(lot_stmt).scalars().all())

        total_qty_total = 0.0
        total_qty_remaining = 0.0
        basis_missing_lots = 0
        positive_lots = 0
        earliest_lot_at = None
        latest_lot_at = None

        # Local simulation inventory so diagnostics do not mutate ORM lots.
        sim_lots: List[Dict[str, Any]] = []

        for lot in lots:
            qt = _safe_float(getattr(lot, "qty_total", None)) or 0.0
            qr = _safe_float(getattr(lot, "qty_remaining", None)) or 0.0
            total_qty_total += float(qt)
            total_qty_remaining += float(max(qr, 0.0))
            if qr > 0:
                positive_lots += 1

            acquired_at = _dt_or_none(getattr(lot, "acquired_at", None))
            if acquired_at:
                earliest_lot_at = acquired_at if earliest_lot_at is None else min(earliest_lot_at, acquired_at)
                latest_lot_at = acquired_at if latest_lot_at is None else max(latest_lot_at, acquired_at)

            if bool(getattr(lot, "basis_is_missing", False)) or getattr(lot, "total_basis_usd", None) is None:
                basis_missing_lots += 1

            sim_lots.append({
                "lot_id": str(getattr(lot, "id", "")),
                "acquired_at": acquired_at,
                "qty_remaining": float(max(qr, 0.0)),
            })

        withdrawals_total_qty = 0.0
        earliest_withdrawal_at = None
        latest_withdrawal_at = None
        first_gap = None
        simulated_consumed = 0.0
        simulated_missing = 0.0

        for w in rows:
            qty = _safe_float(getattr(w, "qty", None)) or 0.0
            wd_time = _dt_or_none(getattr(w, "withdraw_time", None))
            withdrawals_total_qty += float(qty)
            if wd_time:
                earliest_withdrawal_at = wd_time if earliest_withdrawal_at is None else min(earliest_withdrawal_at, wd_time)
                latest_withdrawal_at = wd_time if latest_withdrawal_at is None else max(latest_withdrawal_at, wd_time)

            need = float(qty)
            consumed_here = 0.0
            for sl in sim_lots:
                if need <= 1e-12:
                    break
                avail = float(sl.get("qty_remaining") or 0.0)
                if avail <= 0:
                    continue
                take = min(avail, need)
                sl["qty_remaining"] = float(avail - take)
                need -= take
                consumed_here += take

            simulated_consumed += consumed_here
            if need > 1e-9:
                simulated_missing += float(need)
                if first_gap is None:
                    first_gap = {
                        "withdrawal_id": str(getattr(w, "id", "")),
                        "asset": a,
                        "qty": float(qty),
                        "qty_consumed_in_simulation": float(consumed_here),
                        "qty_missing_in_simulation": float(need),
                        "withdraw_time": wd_time.isoformat() if wd_time else None,
                        "txid": getattr(w, "txid", None),
                        "source": getattr(w, "source", None),
                    }
                    add_example({
                        "kind": "inventory_gap_first_failure",
                        **first_gap,
                    })

        gap_qty = max(float(withdrawals_total_qty - total_qty_remaining), 0.0)

        out["assets"][a] = {
            "venue": lot_venue,
            "wallet_id": lot_wallet,
            "withdrawal_count": len(rows),
            "withdrawal_qty_total": float(withdrawals_total_qty),
            "lot_count": len(lots),
            "positive_lot_count": int(positive_lots),
            "basis_missing_lot_count": int(basis_missing_lots),
            "qty_total": float(total_qty_total),
            "qty_remaining": float(total_qty_remaining),
            "simple_gap_qty": float(gap_qty),
            "simulated_consumed_qty": float(simulated_consumed),
            "simulated_missing_qty": float(simulated_missing),
            "earliest_withdrawal_at": earliest_withdrawal_at.isoformat() if earliest_withdrawal_at else None,
            "latest_withdrawal_at": latest_withdrawal_at.isoformat() if latest_withdrawal_at else None,
            "earliest_lot_acquired_at": earliest_lot_at.isoformat() if earliest_lot_at else None,
            "latest_lot_acquired_at": latest_lot_at.isoformat() if latest_lot_at else None,
            "first_gap": first_gap,
        }

    return out
