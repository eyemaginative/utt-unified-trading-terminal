# backend/app/routers/deposits.py

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session
from sqlalchemy import select, desc, delete, text

from ..db import get_db
from ..models import AssetDeposit, BasisLot, AssetWithdrawal
from ..schemas_deposits import (
    DepositCreate,
    DepositUpdate,
    LotUpdate,
    DepositOut,
    LotOut,
)
from ..services.lots_ledger import create_transfer_in_lots_from_withdrawal

router = APIRouter(prefix="/api/deposits", tags=["deposits"])


# ------------------------------------------------------------------------------
# INGEST COMPAT HELPERS
# ------------------------------------------------------------------------------

def _has_col(model, col_name: str) -> bool:
    """Return True if SQLAlchemy model has a mapped table column named col_name."""
    try:
        cols = set(model.__table__.columns.keys())
    except Exception:
        return False
    return col_name in cols


def _merge_raw(existing_raw: Any, new_raw: Any) -> Dict[str, Any]:
    """Safe merge for JSON-ish raw payloads. Returns a dict."""
    base: Dict[str, Any] = {}
    try:
        if isinstance(existing_raw, dict):
            base.update(existing_raw)
        elif existing_raw is not None:
            base.update(dict(existing_raw))
    except Exception:
        pass

    try:
        if isinstance(new_raw, dict):
            base.update(new_raw)
        elif new_raw is not None:
            base.update(dict(new_raw))
    except Exception:
        pass

    return base


def _withdrawal_has_fifo_slices(w) -> bool:
    """True if withdrawal has FIFO slice data available in raw (lot_impact)."""
    raw = dict(getattr(w, "raw", None) or {})
    impact = raw.get("lot_impact")
    if isinstance(impact, list) and len(impact) > 0:
        return True
    impact2 = raw.get("lot_impact_slices")
    if isinstance(impact2, list) and len(impact2) > 0:
        return True
    if raw.get("lot_impact_applied") and (impact or impact2):
        return True
    return False


def _compute_total_basis_usd(req: DepositCreate) -> Optional[float]:
    if req.basis_total_usd is not None:
        return float(req.basis_total_usd)
    if req.basis_usd_per_coin is not None:
        return float(req.basis_usd_per_coin) * float(req.qty)
    return None


def _set_note_on_obj(obj, note: Optional[str]) -> None:
    """Set note on an ORM object if it has a note-like column."""
    if obj is None:
        return
    if hasattr(obj, "note"):
        setattr(obj, "note", note)
        return
    for k in ("memo", "comment", "comments", "notes"):
        if hasattr(obj, k):
            setattr(obj, k, note)
            return
    raise HTTPException(status_code=400, detail=f"No note-like column on {type(obj).__name__}")


def _emit_lot(l: BasisLot) -> LotOut:
    return LotOut(
        id=str(l.id),
        venue=l.venue,
        wallet_id=l.wallet_id,
        asset=l.asset,
        acquired_at=l.acquired_at,
        qty_total=float(l.qty_total),
        qty_remaining=float(l.qty_remaining),
        total_basis_usd=(float(l.total_basis_usd) if l.total_basis_usd is not None else None),
        basis_is_missing=bool(l.basis_is_missing),
        basis_source=l.basis_source,
        origin_type=l.origin_type,
        origin_ref=l.origin_ref,
        note=l.note,
        created_at=l.created_at,
        updated_at=l.updated_at,
    )


def _aggregate_lots_for_deposit(
    lots: List[BasisLot],
) -> Tuple[Optional[str], Optional[List[str]], bool, Optional[float], bool, Optional[datetime]]:
    """
    For a given deposit origin_ref, lots can be:
      - normal deposit: 1 lot
      - transfer-in: multiple lots (one per slice)
    We aggregate for DepositOut display.
    """
    if not lots:
        return None, None, True, None, True, None

    lot_ids = [str(l.id) for l in lots]
    lot_id = lot_ids[0] if len(lot_ids) == 1 else None

    basis_missing = any(bool(l.basis_is_missing) or (l.total_basis_usd is None) for l in lots)
    needs_basis = bool(basis_missing)

    acquired_ats = [l.acquired_at for l in lots if l.acquired_at is not None]
    acquired_at = min(acquired_ats) if acquired_ats else None

    total_basis_defined = not basis_missing
    total_basis = None
    if total_basis_defined:
        total = 0.0
        for l in lots:
            total += float(l.total_basis_usd or 0.0)
        total_basis = float(total)

    return lot_id, (lot_ids if len(lot_ids) > 1 else None), needs_basis, total_basis, basis_missing, acquired_at


def _emit_deposits(db: Session, deposits: List[AssetDeposit]) -> List[DepositOut]:
    out: List[DepositOut] = []
    dep_ids = [d.id for d in deposits]
    if not dep_ids:
        return out

    lots = db.execute(select(BasisLot).where(BasisLot.origin_ref.in_(dep_ids))).scalars().all()

    lots_by_origin: Dict[str, List[BasisLot]] = {}
    for l in lots:
        if l.origin_ref:
            lots_by_origin.setdefault(l.origin_ref, []).append(l)

    def _pyd_has_field(model, name: str) -> bool:
        # pydantic v2: model_fields; v1: __fields__
        if hasattr(model, "model_fields"):
            return name in getattr(model, "model_fields")
        if hasattr(model, "__fields__"):
            return name in getattr(model, "__fields__")
        return False

    for d in deposits:
        dep_lots = lots_by_origin.get(d.id, [])
        lot_id, lot_ids, needs_basis, total_basis_usd, basis_is_missing, acquired_at = _aggregate_lots_for_deposit(dep_lots)

        payload: Dict[str, Any] = {
            "id": d.id,
            "venue": d.venue,
            "wallet_id": d.wallet_id,
            "asset": d.asset,
            "qty": float(d.qty),
            "deposit_time": d.deposit_time,
            "txid": d.txid,
            "network": d.network,
            "status": d.status,
            "source": d.source,
            "note": d.note,
            "lot_id": lot_id,
            "lot_ids": lot_ids,
            "needs_basis": bool(needs_basis),
            "total_basis_usd": total_basis_usd,
            "basis_is_missing": bool(basis_is_missing),
            "acquired_at": acquired_at,
        }

        # Transfer linking fields are not derived from lot aggregation.
        # If present in the response schema, populate them from the model / raw.
        d_raw = d.raw if isinstance(d.raw, dict) else None
        twi = getattr(d, "transfer_withdrawal_id", None)
        if not twi and d_raw:
            twi = d_raw.get("transfer_withdrawal_id") or d_raw.get("transferWithdrawalId")

        if _pyd_has_field(DepositOut, "transfer_withdrawal_id"):
            payload["transfer_withdrawal_id"] = str(twi) if twi else None
        if _pyd_has_field(DepositOut, "raw"):
            payload["raw"] = d_raw

        out.append(DepositOut(**payload))
    return out


# ------------------------------------------------------------------------------
# SQLITE INTROSPECTION HELPERS (delete semantics hardening)
# ------------------------------------------------------------------------------

def _sqlite_table_exists(db: Session, name: str) -> bool:
    try:
        row = db.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name=:n"),
            {"n": name},
        ).scalar()
        return bool(row)
    except Exception:
        return False


def _sqlite_table_columns(db: Session, name: str) -> List[str]:
    try:
        rows = db.execute(text(f"PRAGMA table_info({name})")).mappings().all()
        return [str(r.get("name")) for r in rows if r.get("name")]
    except Exception:
        return []


def _delete_lot_consumptions_for_lot_ids(db: Session, lot_ids: List[str]) -> int:
    """
    Best-effort cleanup of lot_consumptions rows referencing lots being deleted.

    We only delete if we can identify an unambiguous reference column:
      - lot_id / basis_lot_id / basisLotId / lotId / source_lot_id
    """
    if not lot_ids:
        return 0
    if not _sqlite_table_exists(db, "lot_consumptions"):
        return 0
    cols = set(_sqlite_table_columns(db, "lot_consumptions"))
    for key in ("lot_id", "basis_lot_id", "source_lot_id", "lotId", "basisLotId"):
        if key in cols:
            res = db.execute(
                text(f"DELETE FROM lot_consumptions WHERE {key} IN ({','.join(['?'] * len(lot_ids))})"),
                tuple(lot_ids),
            )
            try:
                return int(getattr(res, "rowcount", 0) or 0)
            except Exception:
                return 0
    return 0


def _count_lot_consumptions_for_lot_ids(db: Session, lot_ids: List[str]) -> int:
    if not lot_ids:
        return 0
    if not _sqlite_table_exists(db, "lot_consumptions"):
        return 0
    cols = set(_sqlite_table_columns(db, "lot_consumptions"))
    for key in ("lot_id", "basis_lot_id", "source_lot_id", "lotId", "basisLotId"):
        if key in cols:
            q = f"SELECT COUNT(*) FROM lot_consumptions WHERE {key} IN ({','.join(['?'] * len(lot_ids))})"
            try:
                return int(db.execute(text(q), tuple(lot_ids)).scalar() or 0)
            except Exception:
                return 0
    return 0


def _delete_lot_journal_for_origin_ref(db: Session, origin_ref: str) -> int:
    """
    Best-effort cleanup for lot_journal if present (prevents stale idempotency artifacts).
    """
    if not origin_ref:
        return 0
    if not _sqlite_table_exists(db, "lot_journal"):
        return 0
    cols = set(_sqlite_table_columns(db, "lot_journal"))
    if "origin_ref" not in cols:
        return 0
    # optionally restrict by origin_type if present
    if "origin_type" in cols:
        res = db.execute(
            text(
                """
                DELETE FROM lot_journal
                WHERE origin_ref = :r
                  AND origin_type IN ('DEPOSIT','deposit','Deposit','UI_MANUAL_DEPOSIT','TRANSFER_IN')
                """
            ),
            {"r": origin_ref},
        )
    else:
        res = db.execute(text("DELETE FROM lot_journal WHERE origin_ref = :r"), {"r": origin_ref})
    try:
        return int(getattr(res, "rowcount", 0) or 0)
    except Exception:
        return 0


@router.get("", response_model=List[DepositOut])
def list_deposits(
    db: Session = Depends(get_db),
    venue: Optional[str] = None,
    wallet_id: Optional[str] = None,
    asset: Optional[str] = None,
    needs_basis: Optional[bool] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=500),
):
    stmt = select(AssetDeposit)

    if venue:
        stmt = stmt.where(AssetDeposit.venue == venue)
    if wallet_id:
        stmt = stmt.where(AssetDeposit.wallet_id == wallet_id)
    if asset:
        stmt = stmt.where(AssetDeposit.asset == asset)

    stmt = stmt.order_by(desc(AssetDeposit.deposit_time)).limit(limit)
    deposits = db.execute(stmt).scalars().all()

    out = _emit_deposits(db, deposits)
    if needs_basis is not None:
        out = [x for x in out if bool(x.needs_basis) == bool(needs_basis)]
    return out


@router.post("", response_model=DepositOut)
def create_deposit(req: DepositCreate, db: Session = Depends(get_db)):
    # Ensure deposit_time is always set (response model expects datetime)
    dep_time = req.deposit_time or datetime.utcnow()

    # Manual deposit defaults
    d = AssetDeposit(
        venue=req.venue,
        wallet_id=req.wallet_id,
        asset=req.asset,
        qty=req.qty,
        deposit_time=dep_time,
        txid=req.txid,
        network=req.network,
        status="MANUAL",
        source="UI_MANUAL",
        note=req.note,
    )
    db.add(d)
    db.flush()  # ensure d.id exists for lot origin_ref / transfer linking

    # -------------------------------------------------------------------------
    # TRANSFER_IN (linked): build lots from the linked TRANSFER_OUT withdrawal
    # -------------------------------------------------------------------------
    if req.transfer_withdrawal_id:
        w = db.get(AssetWithdrawal, req.transfer_withdrawal_id)
        if not w:
            raise HTTPException(status_code=404, detail="transfer_withdrawal_id not found")

        # provenance + helpful note context
        d.source = "TRANSFER_IN_LINKED"
        if req.note:
            d.note = f"{req.note} | linked_withdrawal={w.id}"
        else:
            d.note = f"transfer in (linked) | linked_withdrawal={w.id}"

        # If we have FIFO slices, do the full TRANSFER_IN lot creation (existing behavior).
        # If not, do match-only linking with NO lot side effects.
        if _withdrawal_has_fifo_slices(w):
            # replace any prior lots (defensive for retries)
            db.execute(
                delete(BasisLot).where(
                    BasisLot.origin_type == "DEPOSIT",
                    BasisLot.origin_ref == d.id,
                )
            )

            lots = create_transfer_in_lots_from_withdrawal(
                db,
                dest_venue=d.venue,
                dest_wallet_id=d.wallet_id,
                deposit_time=d.deposit_time,
                withdrawal=w,
                deposit=d,
            )

            # link convenience fields if present on the deposit model
            if lots:
                if hasattr(d, "lot_id"):
                    d.lot_id = lots[0].id
                if hasattr(d, "lot_ids"):
                    d.lot_ids = [x.id for x in lots]

            db.commit()
            try:
                db.refresh(d)
            except Exception:
                pass
            return _emit_deposits(db, [d])[0]

        # Match-only link path (no FIFO slices on withdrawal) — NO lot creation
        draw = dict(getattr(d, "raw", None) or {})
        draw["transfer_withdrawal_id"] = str(w.id)
        d.raw = draw
        d.source = "TRANSFER_IN_MATCHED"

        wraw = dict(getattr(w, "raw", None) or {})
        wraw["transfer_deposit_id"] = str(d.id)
        w.raw = wraw
        if hasattr(w, "transfer_deposit_id"):
            w.transfer_deposit_id = str(d.id)
        if hasattr(w, "updated_at"):
            w.updated_at = datetime.utcnow()

        db.add(d)
        db.add(w)
        db.commit()
        try:
            db.refresh(d)
        except Exception:
            pass
        return _emit_deposits(db, [d])[0]

    # -------------------------------------------------------------------------
    # NORMAL manual deposit: create single lot (unless DB uses a different strategy)
    # -------------------------------------------------------------------------
    total_basis_usd = _compute_total_basis_usd(req)
    acquired_at = req.acquired_at_override or d.deposit_time

    lot = BasisLot(
        origin_type="DEPOSIT",
        origin_ref=d.id,
        venue=d.venue,
        wallet_id=d.wallet_id,
        asset=d.asset,
        qty_total=d.qty,
        qty_remaining=d.qty,
        acquired_at=acquired_at,
        total_basis_usd=total_basis_usd,
        basis_is_missing=(total_basis_usd is None),
        basis_source=("MANUAL_EDIT" if total_basis_usd is not None else "DEPOSIT"),
        note=req.note,
    )

    db.add(lot)
    db.flush()

    if hasattr(d, "lot_id"):
        d.lot_id = lot.id
    if hasattr(d, "lot_ids"):
        d.lot_ids = [lot.id]

    db.commit()
    try:
        db.refresh(d)
    except Exception:
        pass
    return _emit_deposits(db, [d])[0]


@router.patch("/{deposit_id}", response_model=DepositOut)
async def update_deposit(deposit_id: str, req: DepositUpdate, request: Request, db: Session = Depends(get_db)):
    try:
        d = db.get(AssetDeposit, deposit_id)
        if not d:
            raise HTTPException(status_code=404, detail="deposit not found")

        fields_set = getattr(req, "__fields_set__", None) or set()

        # Capture transfer_withdrawal_id even if DepositUpdate schema drops unknown fields.
        body: Dict[str, Any] = {}
        try:
            body = await request.json()
            if not isinstance(body, dict):
                body = {}
        except Exception:
            body = {}

        twid_present = ("transfer_withdrawal_id" in body) or ("transferWithdrawalId" in body)
        twid_value = body.get("transfer_withdrawal_id") if "transfer_withdrawal_id" in body else body.get("transferWithdrawalId")

        if twid_present:
            fields_set = set(fields_set)
            fields_set.add("transfer_withdrawal_id")
            try:
                setattr(req, "transfer_withdrawal_id", twid_value)
            except Exception:
                pass

        if "qty" in fields_set and req.qty is not None:
            d.qty = float(req.qty)

        if "deposit_time" in fields_set and req.deposit_time is not None:
            d.deposit_time = req.deposit_time

        if "txid" in fields_set:
            d.txid = req.txid

        if "network" in fields_set:
            d.network = req.network

        if "note" in fields_set:
            _set_note_on_obj(d, req.note)

        if "total_basis_usd" in fields_set and hasattr(d, "total_basis_usd"):
            d.total_basis_usd = req.total_basis_usd

        lots = list(db.execute(select(BasisLot).where(BasisLot.origin_ref == deposit_id)).scalars().all())

        def _is_missing(qty: float, total_basis_usd: Optional[float]) -> bool:
            if total_basis_usd is None:
                return True
            try:
                return float(total_basis_usd) <= 0 or float(qty) <= 0
            except Exception:
                return True

        if len(lots) == 1:
            lot = lots[0]
            if "qty" in fields_set and hasattr(lot, "qty_total") and hasattr(lot, "qty_remaining"):
                try:
                    qt = float(lot.qty_total)
                    qr = float(lot.qty_remaining)
                except Exception:
                    qt, qr = 0.0, 0.0
                if abs(qr - qt) < 1e-12 and req.qty is not None:
                    lot.qty_total = float(req.qty)
                    lot.qty_remaining = float(req.qty)

            if "total_basis_usd" in fields_set and hasattr(lot, "total_basis_usd"):
                lot.total_basis_usd = req.total_basis_usd
                if hasattr(lot, "basis_is_missing") and hasattr(lot, "qty_total"):
                    try:
                        q = float(lot.qty_total)
                    except Exception:
                        q = float(getattr(d, "qty", 0.0) or 0.0)
                    lot.basis_is_missing = _is_missing(q, req.total_basis_usd)

            if "acquired_at" in fields_set and getattr(req, "acquired_at", None) is not None and hasattr(lot, "acquired_at"):
                lot.acquired_at = req.acquired_at  # type: ignore[attr-defined]

            if "note" in fields_set:
                _set_note_on_obj(lot, req.note)

        else:
            if "acquired_at" in fields_set and getattr(req, "acquired_at", None) is not None:
                for lot in lots:
                    if hasattr(lot, "acquired_at"):
                        lot.acquired_at = req.acquired_at  # type: ignore[attr-defined]
            if "note" in fields_set:
                for lot in lots:
                    _set_note_on_obj(lot, req.note)

        # Match-only transfer link (NO lot-impact requirement)
        if hasattr(req, "transfer_withdrawal_id") and "transfer_withdrawal_id" in fields_set:
            twid = getattr(req, "transfer_withdrawal_id", None)

            draw = dict(getattr(d, "raw", None) or {})
            if twid is None or (isinstance(twid, str) and twid.strip() == ""):
                # clear
                draw.pop("transfer_withdrawal_id", None)
                d.raw = draw
                if hasattr(d, "transfer_withdrawal_id"):
                    d.transfer_withdrawal_id = None
            else:
                w = db.get(AssetWithdrawal, str(twid))
                if not w:
                    raise HTTPException(status_code=404, detail="linked withdrawal not found")

                # store on deposit (raw always, plus column if present)
                draw["transfer_withdrawal_id"] = str(w.id)
                d.raw = draw
                if hasattr(d, "transfer_withdrawal_id"):
                    d.transfer_withdrawal_id = w.id

                # mirror onto withdrawal so both directions are visible
                wraw = dict(getattr(w, "raw", None) or {})
                wraw["transfer_deposit_id"] = str(d.id)
                w.raw = wraw
                if hasattr(w, "transfer_deposit_id"):
                    w.transfer_deposit_id = str(d.id)

                d.source = "TRANSFER_IN_MATCHED"
                if hasattr(w, "updated_at"):
                    w.updated_at = datetime.utcnow()
                db.add(w)

        if hasattr(d, "updated_at"):
            d.updated_at = datetime.utcnow()

        db.commit()
        db.refresh(d)
        return _emit_deposits(db, [d])[0]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"update_deposit failed: {type(e).__name__}: {e}")


@router.patch("/lots/{lot_id}", response_model=LotOut)
def update_lot(lot_id: str, req: LotUpdate, db: Session = Depends(get_db)):
    lot = db.get(BasisLot, lot_id)
    if not lot:
        raise HTTPException(status_code=404, detail="lot not found")

    if req.total_basis_usd is not None:
        lot.total_basis_usd = float(req.total_basis_usd)
        lot.basis_is_missing = False
        lot.basis_source = "MANUAL_EDIT"
    else:
        lot.total_basis_usd = None
        lot.basis_is_missing = True
        lot.basis_source = "MANUAL_EDIT"

    if req.acquired_at is not None:
        lot.acquired_at = req.acquired_at

    if req.note is not None:
        lot.note = req.note

    if hasattr(lot, "updated_at"):
        lot.updated_at = datetime.utcnow()
    db.commit()
    return _emit_lot(lot)


@router.post("/{deposit_id}/link_withdrawal/{withdrawal_id}", response_model=DepositOut)
def link_deposit_to_withdrawal(deposit_id: str, withdrawal_id: str, db: Session = Depends(get_db)):
    """
    Transfer-link after-the-fact:
      - If withdrawal has FIFO slices -> create TRANSFER_IN lots (existing behavior)
      - If not -> match-only link (no lot changes)
    """
    d = db.get(AssetDeposit, deposit_id)
    if not d:
        raise HTTPException(status_code=404, detail="deposit not found")

    w = db.get(AssetWithdrawal, withdrawal_id)
    if not w:
        raise HTTPException(status_code=404, detail="withdrawal not found")

    existing_link = None
    if hasattr(w, "transfer_deposit_id"):
        existing_link = getattr(w, "transfer_deposit_id")
    else:
        _raw0 = dict(getattr(w, "raw", None) or {})
        existing_link = _raw0.get("transfer_deposit_id")
    if existing_link and str(existing_link) != str(deposit_id):
        raise HTTPException(status_code=409, detail="withdrawal already linked to a different deposit")

    if not _withdrawal_has_fifo_slices(w):
        # match-only linking (no lots)
        draw = dict(getattr(d, "raw", None) or {})
        draw["transfer_withdrawal_id"] = str(w.id)
        d.raw = draw
        d.source = "TRANSFER_IN_MATCHED"
        if hasattr(d, "updated_at"):
            d.updated_at = datetime.utcnow()

        wraw = dict(getattr(w, "raw", None) or {})
        wraw["transfer_deposit_id"] = str(d.id)
        w.raw = wraw
        if hasattr(w, "transfer_deposit_id"):
            w.transfer_deposit_id = str(d.id)
        if hasattr(w, "updated_at"):
            w.updated_at = datetime.utcnow()

        # Persist deposit-side link as both a dedicated column (if present)
        # and also in raw (above) for resilience.
        if hasattr(d, "transfer_withdrawal_id"):
            d.transfer_withdrawal_id = str(w.id)

        db.add(d)
        db.add(w)
        db.commit()
        try:
            db.refresh(d)
        except Exception:
            pass
        return _emit_deposits(db, [d])[0]

    # FIFO-slice path (existing behavior)
    if str(getattr(w, "asset", "")).strip().upper() != str(d.asset).strip().upper():
        raise HTTPException(status_code=409, detail="asset mismatch between deposit and withdrawal")

    existing_lots = db.execute(select(BasisLot).where(BasisLot.origin_ref == d.id)).scalars().all()
    for lot in existing_lots:
        qt = float(getattr(lot, "qty_total", 0.0) or 0.0)
        qr = float(getattr(lot, "qty_remaining", 0.0) or 0.0)
        if qr + 1e-12 < qt:
            raise HTTPException(
                status_code=409,
                detail="cannot link transfer: deposit lots already partially consumed; create a new deposit instead",
            )

    for lot in existing_lots:
        db.delete(lot)
    db.flush()

    raw = dict(getattr(w, "raw", None) or {})
    try:
        create_transfer_in_lots_from_withdrawal(
            db,
            dest_venue=d.venue,
            dest_wallet_id=d.wallet_id,
            asset=d.asset,
            origin_deposit_id=d.id,
            withdrawal_id=str(w.id),
            withdrawal_raw=raw,
            commit_at=datetime.utcnow(),
        )
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    d.source = "TRANSFER_IN_LINKED"
    if hasattr(d, "updated_at"):
        d.updated_at = datetime.utcnow()
    if d.note:
        d.note = f"{d.note} | linked_withdrawal={w.id}"
    else:
        d.note = f"linked_withdrawal={w.id}"

    raw = dict(getattr(w, "raw", None) or {})
    raw["transfer_deposit_id"] = str(d.id)
    w.raw = raw
    if hasattr(w, "transfer_deposit_id"):
        w.transfer_deposit_id = str(d.id)
    if hasattr(w, "updated_at"):
        w.updated_at = datetime.utcnow()

    # Persist deposit-side link (FIFO path) in BOTH places, so it always shows up
    # in GET /api/deposits and in the DepositOut response.
    draw = dict(getattr(d, "raw", None) or {})
    draw["transfer_withdrawal_id"] = str(w.id)
    d.raw = draw
    if hasattr(d, "transfer_withdrawal_id"):
        d.transfer_withdrawal_id = str(w.id)

    db.add(d)
    db.add(w)
    db.commit()
    return _emit_deposits(db, [d])[0]


@router.delete("/{deposit_id}")
def delete_deposit(
    deposit_id: str,
    force: bool = Query(
        False,
        description="If true, allow deleting deposits even if their lots were partially consumed / have consumptions (TEST DATA ONLY).",
    ),
    db: Session = Depends(get_db),
):
    """
    Delete semantics (explicit policy):

    - Default (force=false):
        * Block delete if any associated BasisLot is consumed (qty_remaining < qty_total)
        * Block delete if any lot_consumptions rows reference any associated lot ids (if table exists)

    - Force (force=true):
        * Best-effort delete lot_consumptions referencing the lots
        * Delete lots + deposit
        * This may invalidate downstream realized P&L; intended for purging junk/manual test rows only.
    """
    d = db.query(AssetDeposit).filter(AssetDeposit.id == deposit_id).first()
    if not d:
        raise HTTPException(status_code=404, detail="deposit not found")

    # Collect associated lot ids deterministically
    lot_ids: set[str] = set()
    for attr in ("lot_id", "lot_ids"):
        v = getattr(d, attr, None)
        if isinstance(v, str) and v:
            lot_ids.add(v)
        elif isinstance(v, (list, tuple)):
            lot_ids.update([str(x) for x in v if x])

    if hasattr(BasisLot, "origin_ref"):
        q = db.query(BasisLot).filter(BasisLot.origin_ref == deposit_id)
        if hasattr(BasisLot, "origin_type"):
            q = q.filter(BasisLot.origin_type.in_(["DEPOSIT", "deposit", "Deposit"]))
        for l in q.all():
            if getattr(l, "id", None):
                lot_ids.add(str(l.id))

    lots: List[BasisLot] = []
    if lot_ids:
        lots = db.query(BasisLot).filter(BasisLot.id.in_(list(lot_ids))).all()

    # Block if consumed lots (unless force)
    if not force:
        for l in lots:
            qt = getattr(l, "qty_total", None)
            qr = getattr(l, "qty_remaining", None)
            if qt is not None and qr is not None:
                try:
                    if float(qr) + 1e-12 < float(qt):
                        raise HTTPException(
                            status_code=409,
                            detail="deposit lots already consumed; delete blocked (use force=true if this is test data)",
                        )
                except HTTPException:
                    raise
                except Exception:
                    pass

    # Block if lot_consumptions exist referencing lots (unless force)
    lot_ids_list = [str(x) for x in lot_ids if x]
    cons_count = _count_lot_consumptions_for_lot_ids(db, lot_ids_list)
    if cons_count > 0 and not force:
        raise HTTPException(
            status_code=409,
            detail=f"deposit has lot_consumptions referencing its lots ({cons_count}); delete blocked (use force=true if this is test data)",
        )

    # If linked to a withdrawal, clear the reverse link first
    linked_wid = None
    if hasattr(d, "transfer_withdrawal_id"):
        linked_wid = getattr(d, "transfer_withdrawal_id", None)
    if not linked_wid:
        draw = getattr(d, "raw", None)
        if isinstance(draw, dict):
            linked_wid = draw.get("transfer_withdrawal_id") or draw.get("transferWithdrawalId")

    if linked_wid:
        w = db.query(AssetWithdrawal).filter(AssetWithdrawal.id == str(linked_wid)).first()
        if w:
            if hasattr(w, "transfer_deposit_id") and getattr(w, "transfer_deposit_id", None) == str(deposit_id):
                w.transfer_deposit_id = None
            wraw = dict(getattr(w, "raw", None) or {})
            if wraw.get("transfer_deposit_id") == str(deposit_id):
                wraw.pop("transfer_deposit_id", None)
                w.raw = wraw
            if hasattr(w, "updated_at"):
                w.updated_at = datetime.utcnow()
            db.add(w)

    # Force cleanup of lot_consumptions if applicable
    deleted_consumptions = 0
    if cons_count > 0 and force:
        deleted_consumptions = _delete_lot_consumptions_for_lot_ids(db, lot_ids_list)

    # Best-effort cleanup for lot_journal rows referencing this deposit id
    deleted_journal = _delete_lot_journal_for_origin_ref(db, str(deposit_id))

    # Delete lots and deposit
    for l in lots:
        db.delete(l)

    db.delete(d)
    db.commit()
    return {
        "ok": True,
        "deleted_lots": len(lots),
        "deleted_lot_consumptions": int(deleted_consumptions),
        "deleted_lot_journal_rows": int(deleted_journal),
    }


@router.post("/ingest")
def ingest_deposits(
    venue: str = Query(default="gemini"),
    wallet_id: str = Query(default="default"),
    mode: str = Query(default="days", description="days: use since (or default 90d); all: ignore since and fetch full history (bounded by max_pages)"),
    since: Optional[str] = Query(default=None, description="ISO datetime (used when mode=days)"),
    currency: Optional[str] = Query(default=None, description="Optional currency filter (e.g., BTC)"),
    limit_transfers: int = Query(default=50, ge=1, le=50),
    max_pages: int = Query(default=200, ge=1, le=2000),
    dry_run: bool = Query(default=False),
    db: Session = Depends(get_db),
):
    """
    Venue ingestion (SAFE):
      - Gemini + Coinbase
      - Upserts rows into asset_deposits
      - Idempotent via origin_ref when available (otherwise best-effort natural key)
      - NO lot creation / NO FIFO / NO auto-transfer linking
    """
    v = (venue or "").strip().lower().replace("-", "_")
    if v not in ("gemini", "coinbase", "kraken", "cryptocom", "dex_trade"):
        raise HTTPException(
            status_code=400,
            detail="deposit ingest currently supports venue=gemini, venue=coinbase, venue=kraken, venue=cryptocom, or venue=dex_trade",
        )

    venue_src = f"VENUE_{v.upper()}"

    # Mode controls whether we respect `since` (days) or attempt a full-history backfill (all).
    mode_v = (mode or "days").strip().lower()
    if mode_v not in ("days", "all"):
        raise HTTPException(status_code=400, detail="mode must be one of: days, all")

    if mode_v == "all":
        # Use an ancient sentinel; backend paging/max_pages still bounds work.
        since_dt = datetime(1970, 1, 1)
    else:
        # Parse since (default lookback 90d)
        if since:
            try:
                sdt = datetime.fromisoformat(str(since).replace("Z", "+00:00"))
                if getattr(sdt, "tzinfo", None) is not None:
                    sdt = sdt.astimezone(tz=None).replace(tzinfo=None)
                since_dt = sdt.replace(tzinfo=None)
            except Exception:
                raise HTTPException(status_code=400, detail="invalid since; expected ISO datetime")
        else:
            since_dt = datetime.utcnow() - timedelta(days=90)

    # Fetch venue transfers
    try:
        if v == "gemini":
            from ..adapters.gemini import GeminiAdapter

            ga = GeminiAdapter()
            transfers = ga.fetch_transfers(
                since=since_dt,
                currency=currency,
                limit_transfers=limit_transfers,
                max_pages=max_pages,
            )
        elif v == "coinbase":
            from ..adapters.coinbase import CoinbaseAdapter

            ca = CoinbaseAdapter()
            transfers = ca.fetch_transfers(
                since_dt=since_dt,
                kinds=["deposit"],
                currency=currency,
                limit_transfers=limit_transfers,
                max_pages=max_pages,
            )

        elif v == "kraken":
            from ..adapters.kraken import KrakenAdapter

            ka = KrakenAdapter()
            transfers = ka.fetch_transfers(
                since_dt=since_dt,
                kinds=["deposit"],
                currency=currency,
                limit_transfers=limit_transfers,
                max_pages=max_pages,
            )

        elif v == "cryptocom":
            from ..adapters.cryptocom_exchange import CryptoComExchangeAdapter

            ca = CryptoComExchangeAdapter()
            transfers = ca.fetch_transfers(
                since_dt=since_dt,
                kinds=["deposit"],
                currency=currency,
                limit_transfers=limit_transfers,
                max_pages=max_pages,
            )

        elif v == "dex_trade":
            from ..adapters.dex_trade import DexTradeAdapter

            da = DexTradeAdapter()
            transfers = da.fetch_transfers(
                since_dt=since_dt,
                kinds=["deposit"],
                currency=currency,
                limit_transfers=limit_transfers,
                max_pages=max_pages,
                mode=mode,
            )

        else:
            raise HTTPException(status_code=400, detail=f"Venue not supported for deposit ingest: {v}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"{v} fetch_transfers failed: {e}")

    inserted = 0
    updated = 0
    seen = 0
    has_origin_ref = _has_col(AssetDeposit, "origin_ref")

    # When AssetDeposit has no origin_ref column, we must respect the DB's UNIQUE(venue,wallet_id,txid).
    # Gemini history can include multiple deposit events sharing the same txHash (txid). We therefore:
    #   1) Deduplicate within this ingest run (pending objects) by txid
    #   2) Upsert by (venue, wallet_id, txid) first when txid is present
    pending_by_txid: Dict[str, AssetDeposit] = {}

    for t in (transfers or []):
        if not isinstance(t, dict):
            continue
        if str(t.get("type") or "").strip() != "Deposit":
            continue

        eid = t.get("eid")
        if eid is None:
            continue

        seen += 1
        origin_ref = f"{v}:deposit:{eid}"

        asset = str(t.get("currency") or "").strip().upper()
        if not asset:
            continue

        try:
            qty = float(t.get("amount"))
        except Exception:
            continue

        # time (UTC naive)
        try:
            dep_time = datetime.utcfromtimestamp(int(t.get("timestampms")) / 1000.0)
        except Exception:
            dep_time = datetime.utcnow()

        status = str(t.get("status") or "").strip().upper() or "UNKNOWN"
        txid = t.get("txHash")

        raw = dict(t)
        raw["origin_ref"] = origin_ref
        raw["venue"] = v

        txid_key: Optional[str] = None
        if txid is not None:
            try:
                txid_key = str(txid).strip()
            except Exception:
                txid_key = None
        if txid_key == "":
            txid_key = None

        # If we already staged an insert in THIS run with the same txid, treat it as existing and update it.
        if (not has_origin_ref) and txid_key and txid_key in pending_by_txid:
            existing = pending_by_txid[txid_key]
            try:
                if hasattr(existing, "status"):
                    existing.status = status
                if hasattr(existing, "source"):
                    existing.source = venue_src
                if hasattr(existing, "raw"):
                    existing.raw = _merge_raw(getattr(existing, "raw", None), raw)
                if hasattr(existing, "updated_at"):
                    existing.updated_at = datetime.utcnow()
                db.add(existing)
                updated += 1
            except Exception:
                pass
            continue

        existing = None
        if has_origin_ref:
            try:
                existing = (
                    db.execute(select(AssetDeposit).where(AssetDeposit.origin_ref == origin_ref))
                    .scalar_one_or_none()
                )
            except Exception:
                existing = None
        else:
            # No origin_ref column: must respect UNIQUE(venue,wallet_id,txid).
            # Prefer matching by txid first if present; fall back to a natural key when txid is absent.
            try:
                existing = None

                # 1) Strong match: (venue, wallet_id, txid)
                if txid_key:
                    q1 = select(AssetDeposit).where(
                        AssetDeposit.venue == v,
                        AssetDeposit.wallet_id == wallet_id,
                        AssetDeposit.txid == txid_key,
                    )
                    existing = db.execute(q1).scalar_one_or_none()

                # 2) Fallback: best-effort natural key when txid is missing
                if existing is None and (not txid_key):
                    q2 = select(AssetDeposit).where(
                        AssetDeposit.venue == v,
                        AssetDeposit.wallet_id == wallet_id,
                        AssetDeposit.asset == asset,
                        AssetDeposit.qty == qty,
                        AssetDeposit.deposit_time == dep_time,
                    )
                    existing = db.execute(q2).scalar_one_or_none()
            except Exception:
                existing = None

        if existing:
            # Update mutable fields only
            try:
                if hasattr(existing, "status"):
                    existing.status = status
                if txid_key and hasattr(existing, "txid"):
                    existing.txid = txid_key
                if hasattr(existing, "source"):
                    existing.source = venue_src
                if hasattr(existing, "raw"):
                    existing.raw = _merge_raw(getattr(existing, "raw", None), raw)
                if hasattr(existing, "updated_at"):
                    existing.updated_at = datetime.utcnow()
                db.add(existing)
                updated += 1
            except Exception:
                pass
            continue

        # Insert new
        try:
            d = AssetDeposit(
                venue=v,
                wallet_id=wallet_id,
                asset=asset,
                qty=qty,
                deposit_time=dep_time,
                txid=(txid_key if txid_key else None),
                status=status,
                source=venue_src,
                note=None,
            )
            if has_origin_ref:
                setattr(d, "origin_ref", origin_ref)
            if hasattr(d, "raw"):
                d.raw = raw
            if hasattr(d, "created_at") and getattr(d, "created_at", None) is None:
                d.created_at = datetime.utcnow()
            db.add(d)
            if (not has_origin_ref) and txid_key:
                pending_by_txid[txid_key] = d
            inserted += 1
        except Exception:
            continue

    if dry_run:
        db.rollback()
    else:
        db.commit()

    return {
        "venue": v,
        "wallet_id": wallet_id,
        "mode": mode_v,
        "since": since_dt.isoformat(),
        "seen_deposits": seen,
        "inserted": inserted,
        "updated": updated,
        "dry_run": bool(dry_run),
    }
