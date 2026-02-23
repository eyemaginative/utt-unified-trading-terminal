# backend/app/routers/withdrawals.py

from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sqlalchemy.orm import Session
from sqlalchemy import select, desc, update

from ..db import get_db
from ..models import AssetWithdrawal  # must exist in your chosen models.py
from ..schemas_withdrawals import WithdrawalCreate, WithdrawalPatch, WithdrawalOut
from ..services.lots_ledger import fifo_consume_transfer_out, impact_to_json

router = APIRouter(prefix="/api/withdrawals", tags=["withdrawals"])



# ------------------------------------------------------------------------------
# NOTE FIELD COMPATIBILITY
# ------------------------------------------------------------------------------
# Some historical schemas used different column names for free-form notes.
# The API surface uses `note`, but the mapped DB column may be `note`, `notes`,
# `memo`, etc. To avoid silent non-persistence (e.g. assigning to a non-mapped
# attribute), we detect actual table columns and, when patching, force an UPDATE
# against the real column(s).
_NOTE_CANDIDATES = [
    "note", "notes",
    "memo",
    "comment", "comments",
    "remark", "remarks",
    "description", "desc",
]

def _note_columns_for_model(model) -> list[str]:
    try:
        cols = set(model.__table__.columns.keys())
    except Exception:
        cols = set()
    picked = [c for c in _NOTE_CANDIDATES if c in cols]
    if picked:
        return picked
    # Fallback: any column containing a notes-like substring
    for c in cols:
        lc = str(c).lower()
        if any(k in lc for k in ["note", "memo", "comment", "remark", "description", "desc"]):
            picked.append(c)
    return picked


def _set_note_on_obj(obj, note_value: str) -> List[str]:
    """Persist note by setting only real mapped table columns on the already-loaded ORM object.
    This avoids Core UPDATE type-casting issues and guarantees the subsequent refresh reflects DB state.
    """
    cols = _note_columns_for_model(obj.__class__)
    if not cols:
        return []
    for c in cols:
        try:
            setattr(obj, c, note_value)
        except Exception:
            pass
    return cols

def _get_note_from_obj(obj):
    # Prefer real table columns first
    cols = _note_columns_for_model(obj.__class__)
    for c in cols:
        v = getattr(obj, c, None)
        if v is not None:
            return v
    # Fallback
    return getattr(obj, "note", None) or getattr(obj, "notes", None)

def _set_note_via_sql(db: Session, model, row_id: str, note_value: str) -> List[str]:
    """Force-persist note-like fields via Core UPDATE.

    Returns the list of columns that were updated. If no note-like columns exist,
    returns an empty list.
    """
    cols = _note_columns_for_model(model)
    if not cols:
        return []
    updated: List[str] = []
    for c in cols:
        db.execute(
            update(model)
            .where(model.id == row_id)  # type: ignore[attr-defined]
            .values({c: note_value})
        )
        updated.append(c)
    return updated

def _normalize_link_id(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _get_transfer_deposit_id(w: AssetWithdrawal) -> Optional[str]:
    # Prefer an actual ORM column if present; fall back to raw payload.
    if hasattr(w, "transfer_deposit_id"):
        return getattr(w, "transfer_deposit_id")
    raw = getattr(w, "raw", None) or {}
    return raw.get("transfer_deposit_id")


def _set_transfer_deposit_id(w: AssetWithdrawal, v: Optional[str]) -> None:
    v = _normalize_link_id(v)
    if hasattr(w, "transfer_deposit_id"):
        setattr(w, "transfer_deposit_id", v)
        return
    # Fallback for older schema: persist inside raw JSON so the UI can still display it.
    raw = getattr(w, "raw", None) or {}
    raw["transfer_deposit_id"] = v
    setattr(w, "raw", raw)


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
            # Best-effort: coerce to dict if possible
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


def _emit(w: AssetWithdrawal):
    # Normalize raw to a dict (never None) to avoid response-model validation issues
    raw = dict(getattr(w, "raw", None) or {})

    # Model field is withdraw_time (not withdrawal_time)
    wt = getattr(w, "withdraw_time", None) or getattr(w, "withdrawal_time", None)

    # Ensure transfer_deposit_id is visible even if stored in raw fallback
    tdi = getattr(w, "transfer_deposit_id", None) or raw.get("transfer_deposit_id")
    if tdi:
        raw["transfer_deposit_id"] = tdi

    created_at = getattr(w, "created_at", None) or wt

    payload = {
        "id": w.id,
        "venue": w.venue,
        "wallet_id": w.wallet_id,
        "asset": w.asset,
        "qty": float(w.qty) if w.qty is not None else None,
        "withdraw_time": wt.isoformat() if wt else None,
        "txid": w.txid,
        "chain": getattr(w, "chain", None),
        "network": w.network,
        "status": w.status,
        "source": w.source,
        "destination": w.destination,
        "note": _get_note_from_obj(w),
        "raw": raw,
        "created_at": created_at.isoformat() if created_at else None,
    }

    # Filter to the response model fields to avoid breaking older schema versions
    allowed = set(getattr(WithdrawalOut, "model_fields", {}).keys()) or set(payload.keys())
    return {k: v for k, v in payload.items() if k in allowed}

@router.get("", response_model=List[WithdrawalOut])
def list_withdrawals(
    db: Session = Depends(get_db),
    venue: Optional[str] = None,
    wallet_id: Optional[str] = None,
    asset: Optional[str] = None,
    limit: int = Query(default=200, ge=1, le=500),
):
    stmt = select(AssetWithdrawal)

    if venue:
        stmt = stmt.where(AssetWithdrawal.venue == venue.strip().lower())
    if wallet_id:
        stmt = stmt.where(AssetWithdrawal.wallet_id == wallet_id)
    if asset:
        stmt = stmt.where(AssetWithdrawal.asset == asset.strip().upper())

    stmt = stmt.order_by(desc(AssetWithdrawal.withdraw_time), desc(AssetWithdrawal.created_at)).limit(limit)
    items = db.execute(stmt).scalars().all()
    return [_emit(w) for w in items]


@router.get("/{withdrawal_id}", response_model=WithdrawalOut)
def get_withdrawal(withdrawal_id: str, db: Session = Depends(get_db)):
    """Fetch a single withdrawal by id.

    LedgerWindow's Edit flow calls GET /api/withdrawals/{id} to load the row
    into the editor. Without this route, FastAPI returns 405 because PATCH/DELETE
    exist at the same path.
    """
    w = db.get(AssetWithdrawal, withdrawal_id)
    if not w:
        raise HTTPException(status_code=404, detail="Withdrawal not found")
    return _emit(w)


@router.post("", response_model=WithdrawalOut)
def create_withdrawal(
    req: WithdrawalCreate,
    apply_lot_impact: bool = Query(default=True),
    db: Session = Depends(get_db),
):
    wt = req.withdraw_time or datetime.utcnow()

    w = AssetWithdrawal(
        venue=req.venue.strip().lower(),
        wallet_id=req.wallet_id or "default",
        asset=req.asset.strip().upper(),
        qty=float(req.qty),
        withdraw_time=wt,
        txid=req.txid,
        chain=req.chain,
        network=req.network,
        status=req.status or "MANUAL",
        source=req.source or "UI_MANUAL",
        destination=req.destination,
        note=req.note,
        raw=req.raw,
        created_at=datetime.utcnow(),
    )


    # Optional: match-only transfer link (no lot impact required)
    if hasattr(req, "transfer_deposit_id"):
        _set_transfer_deposit_id(w, getattr(req, "transfer_deposit_id", None))

    db.add(w)
    db.flush()  # w.id assigned

    if apply_lot_impact:
        try:
            impact = fifo_consume_transfer_out(
                db,
                venue=w.venue,
                wallet_id=w.wallet_id,
                asset=w.asset,
                qty=w.qty,
                as_of=wt,
                allow_partial=True,
            )
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))

        raw = dict(w.raw or {})
        raw["lot_impact"] = impact_to_json(impact)
        raw["lot_impact_applied"] = True
        w.raw = raw
        db.add(w)

    db.commit()
    try:
        db.refresh(w)
    except Exception:
        pass
    return _emit(w)


@router.delete("/{withdrawal_id}")
def delete_withdrawal(
    withdrawal_id: str,
    db: Session = Depends(get_db),
):
    w = db.get(AssetWithdrawal, withdrawal_id)
    if not w:
        raise HTTPException(status_code=404, detail="withdrawal not found")

    # Safety: do not allow deletion once lots have been consumed (if tracked).
    try:
        if int(getattr(w, "realized_lot_count", 0) or 0) > 0:
            raise HTTPException(status_code=409, detail="withdrawal has realized lots; cannot delete")
    except HTTPException:
        raise
    except Exception:
        # ignore: field may not exist in older schemas
        pass

    db.delete(w)
    db.commit()
    return {"ok": True}


@router.post("/ingest")
def ingest_withdrawals(
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
      - Upserts rows into asset_withdrawals
      - Idempotent via origin_ref when available (otherwise best-effort natural key)
      - NO FIFO / NO lot impact / NO auto-transfer linking
    """
    v = (venue or "").strip().lower().replace("-", "_")
    if v not in ("gemini", "coinbase", "kraken", "cryptocom", "dex_trade"):
        raise HTTPException(
            status_code=400,
            detail="withdrawal ingest currently supports venue=gemini, venue=coinbase, venue=kraken, venue=cryptocom, or venue=dex_trade",
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

    # Fetch venue transfers (withdrawals are type="Withdrawal")
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
                kinds=["withdrawal"],
                currency=currency,
                limit_transfers=limit_transfers,
                max_pages=max_pages,
            )
        elif v == "kraken":
            from ..adapters.kraken import KrakenAdapter

            ka = KrakenAdapter()
            transfers = ka.fetch_transfers(
                since_dt=since_dt,
                kinds=["withdrawal"],
                currency=currency,
                limit_transfers=limit_transfers,
                max_pages=max_pages,
            )

        elif v == "cryptocom":
            from ..adapters.cryptocom_exchange import CryptoComExchangeAdapter

            ca = CryptoComExchangeAdapter()
            transfers = ca.fetch_transfers(
                since_dt=since_dt,
                kinds=["withdrawal"],
                currency=currency,
                limit_transfers=limit_transfers,
                max_pages=max_pages,
            )

        elif v == "dex_trade":
            from ..adapters.dex_trade import DexTradeAdapter

            da = DexTradeAdapter()
            transfers = da.fetch_transfers(
                since_dt=since_dt,
                kinds=["withdrawal"],
                currency=currency,
                limit_transfers=limit_transfers,
                max_pages=max_pages,
                mode=mode,
            )
        else:
            raise HTTPException(status_code=400, detail=f"Venue not supported for transfer ingest: {v}")
    except HTTPException:
        raise
    except Exception as e:
        msg = str(e)
        if "429" in msg or "Too Many Requests" in msg:
            raise HTTPException(status_code=429, detail=f"{v} fetch_transfers rate-limited: {msg}")
        if "timeout" in msg.lower() or "timed out" in msg.lower():
            raise HTTPException(status_code=504, detail=f"{v} fetch_transfers timed out: {msg}")
        raise HTTPException(status_code=502, detail=f"{v} fetch_transfers failed: {msg}")

    inserted = 0
    updated_ct = 0
    seen = 0
    has_origin_ref = _has_col(AssetWithdrawal, "origin_ref")

    for t in (transfers or []):
        if not isinstance(t, dict):
            continue
        if str(t.get("type") or "").strip() != "Withdrawal":
            continue

        eid = t.get("eid")
        if eid is None:
            continue

        seen += 1
        origin_ref = f"{v}:withdrawal:{eid}"

        asset = str(t.get("currency") or "").strip().upper()
        if not asset:
            continue

        try:
            qty = float(t.get("amount"))
        except Exception:
            continue

        # time (UTC naive)
        wt = None
        try:
            wt = datetime.fromtimestamp(int(t.get("timestampms")) / 1000.0)
        except Exception:
            wt = datetime.utcnow()

        status = str(t.get("status") or "").strip().upper() or "UNKNOWN"
        txid = t.get("txHash")

        # Some Gemini payloads include destination / address fields; keep raw regardless.
        destination = t.get("destination") or t.get("address") or t.get("destinationAddress") or None

        raw: Dict[str, Any] = dict(t)
        raw["origin_ref"] = origin_ref
        raw["venue"] = v

        existing = None
        if has_origin_ref:
            try:
                existing = db.execute(
                    select(AssetWithdrawal).where(AssetWithdrawal.origin_ref == origin_ref)
                ).scalar_one_or_none()
            except Exception:
                existing = None
        else:
            # best-effort natural key
            try:
                q = select(AssetWithdrawal).where(
                    AssetWithdrawal.venue == v,
                    AssetWithdrawal.wallet_id == wallet_id,
                    AssetWithdrawal.asset == asset,
                    AssetWithdrawal.qty == qty,
                    AssetWithdrawal.withdraw_time == wt,
                )
                if txid:
                    q = q.where(AssetWithdrawal.txid == str(txid))
                existing = db.execute(q).scalar_one_or_none()
            except Exception:
                existing = None

        if existing:
            try:
                if hasattr(existing, "status"):
                    existing.status = status
                if txid and hasattr(existing, "txid"):
                    existing.txid = str(txid)
                if destination and hasattr(existing, "destination"):
                    existing.destination = str(destination)
                if hasattr(existing, "source"):
                    existing.source = venue_src
                if hasattr(existing, "raw"):
                    existing.raw = _merge_raw(getattr(existing, "raw", None), raw)
                if hasattr(existing, "updated_at"):
                    existing.updated_at = datetime.utcnow()
                db.add(existing)
                updated_ct += 1
            except Exception:
                pass
            continue

        try:
            w = AssetWithdrawal(
                venue=v,
                wallet_id=wallet_id,
                asset=asset,
                qty=qty,
                withdraw_time=wt,
                txid=(str(txid) if txid else None),
                status=status,
                source=venue_src,
                destination=(str(destination) if destination else None),
                note=None,
                chain=None,
                network=None,
                raw=raw,
                created_at=datetime.utcnow(),
            )
            if has_origin_ref:
                setattr(w, "origin_ref", origin_ref)
            db.add(w)
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
        "seen_withdrawals": seen,
        "inserted": inserted,
        "updated": updated_ct,
        "dry_run": bool(dry_run),
    }



@router.patch("/{withdrawal_id}", response_model=WithdrawalOut)
async def patch_withdrawal(
    withdrawal_id: str,
    request: Request,
    req: WithdrawalPatch,
    apply_lot_impact: bool = Query(default=False),
    db: Session = Depends(get_db),
):
    """
    Default apply_lot_impact=False for PATCH to avoid accidental ledger drift.
    If you truly need to apply impact via PATCH, set apply_lot_impact=true,
    BUT qty changes are blocked once lot_impact_applied is present.
    """
    w = db.get(AssetWithdrawal, withdrawal_id)
    if not w:
        raise HTTPException(status_code=404, detail="withdrawal not found")

    raw = dict(getattr(w, "raw", None) or {})
    already_applied = bool(raw.get("lot_impact_applied"))

    if req.qty is not None:
        if already_applied and float(req.qty) != float(w.qty) and apply_lot_impact:
            raise HTTPException(
                status_code=409,
                detail="cannot change qty after lot impact applied; create a new withdrawal instead (or patch with apply_lot_impact=false)",
            )
        w.qty = float(req.qty)

    if req.withdraw_time is not None:
        w.withdraw_time = req.withdraw_time

    if req.txid is not None:
        w.txid = req.txid
    if req.chain is not None:
        w.chain = req.chain
    if req.network is not None:
        w.network = req.network

    if req.status is not None:
        w.status = req.status
    if req.source is not None:
        w.source = req.source

    if req.destination is not None:
        w.destination = req.destination
    if req.note is not None:
        updated_cols = _set_note_on_obj(w, req.note)
        if not updated_cols:
            cols = list(AssetWithdrawal.__table__.columns.keys())
            raise HTTPException(
                status_code=400,
                detail=f"Cannot persist note: no note-like column found on AssetWithdrawal. Table columns={cols}",
            )

    if req.raw is not None:
        # merge
        merged = dict(raw)
        merged.update(req.raw)
        w.raw = merged


    # Optional: match-only transfer link (no lot impact required)
    # NOTE: transfer_deposit_id may be dropped by the Pydantic schema if it's not declared there.
    # We read raw JSON so the UI can send transfer_deposit_id without requiring a schema bump.
    body: Any = None
    try:
        body = await request.json()
    except Exception:
        body = None

    if isinstance(body, dict):
        for k in ("transfer_deposit_id", "transferDepositId"):
            if k in body:
                v = body.get(k)
                if isinstance(v, str) and not v.strip():
                    v = None
                _set_transfer_deposit_id(w, v)
                break


    if apply_lot_impact and not already_applied:
        try:
            impact = fifo_consume_transfer_out(
                db,
                venue=w.venue,
                wallet_id=w.wallet_id,
                asset=w.asset,
                qty=w.qty,
                as_of=w.withdraw_time,
                allow_partial=True,
            )
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e))

        raw2 = dict(getattr(w, "raw", None) or {})
        raw2["lot_impact"] = impact_to_json(impact)
        raw2["lot_impact_applied"] = True
        w.raw = raw2
        db.add(w)

    db.commit()

    db.expire_all()
    w2 = db.get(AssetWithdrawal, withdrawal_id)
    if not w2:
        raise HTTPException(status_code=404, detail="withdrawal not found")
    return _emit(w2)
