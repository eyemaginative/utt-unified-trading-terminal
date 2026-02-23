# backend/app/services/lot_sync.py

from __future__ import annotations

from datetime import datetime
import uuid
from typing import Any, Dict, Optional, List, Tuple

from sqlalchemy.orm import Session
from sqlalchemy import select, func, desc, asc, and_, or_, text, exists

from ..models import BasisLot, Order, VenueOrderRow
from ..services.lots_ledger import fifo_consume_sell_fifo, impact_to_json
from ..models_lot_journal import LotJournal


SYNC_MODES = ["ALL", "LOCAL", "VENUE", "DEPOSITS", "WITHDRAWALS"]


def _dt_utcnow() -> datetime:
    return datetime.utcnow()


def _norm_venue(v: Any) -> str:
    return str(v or "").strip().lower()


def _norm_wallet(w: Any) -> str:
    s = str(w or "default").strip()
    return s if s else "default"


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _parse_base_asset(symbol_canon: Optional[str], symbol_venue: Optional[str]) -> Optional[str]:
    s = (symbol_canon or symbol_venue or "").strip().upper()
    if not s:
        return None
    for sep in ("-", "/", "_", ":"):
        if sep in s:
            base = s.split(sep, 1)[0].strip().upper()
            return base or None
    return None


def _fee_usd_estimate(fee: Any, fee_asset: Any) -> Optional[float]:
    f = _safe_float(fee)
    a = str(fee_asset or "").strip().upper()
    if f is None:
        return None
    if a in ("USD", "USDC"):
        return float(f)
    return None


def _parse_cursor(cursor: Optional[str]) -> Optional[Tuple[datetime, str]]:
    """
    Cursor format: "<iso_ts>|<id>"
    Example: "2025-12-23T02:37:54.781000|ebf0bfee-f6ee-4d13-ba99-83b8219156b3"
    """
    s = str(cursor or "").strip()
    if not s or "|" not in s:
        return None
    ts_s, id_s = s.split("|", 1)
    ts_s = ts_s.strip()
    id_s = id_s.strip()
    if not ts_s or not id_s:
        return None
    try:
        ts = datetime.fromisoformat(ts_s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None
    return (ts, id_s)


def _available_qty(db: Session, *, venue: str, wallet_id: str, asset: str) -> float:
    v = _norm_venue(venue)
    w = _norm_wallet(wallet_id)
    a = str(asset or "").strip().upper()

    x = db.execute(
        select(func.coalesce(func.sum(BasisLot.qty_remaining), 0.0)).where(
            BasisLot.venue == v,
            BasisLot.wallet_id == w,
            BasisLot.asset == a,
            BasisLot.qty_remaining > 0,
        )
    ).scalar()
    try:
        return float(x or 0.0)
    except Exception:
        return 0.0


def _ensure_journal(
    db: Session,
    *,
    action: str,
    origin_type: str,
    origin_ref: str,
    venue: Optional[str],
    wallet_id: Optional[str],
    asset: Optional[str],
    qty: Optional[float],
    price_usd: Optional[float],
    fee_usd: Optional[float],
    effective_at: Optional[datetime],
) -> LotJournal:
    row = db.execute(
        select(LotJournal).where(
            LotJournal.action == action,
            LotJournal.origin_type == origin_type,
            LotJournal.origin_ref == origin_ref,
        )
    ).scalars().first()

    if row:
        return row

    # Race/loop safe insert:
    # - We want (action, origin_type, origin_ref) uniqueness to enforce idempotency.
    # - During rebuild loops (or accidental double-runs), a SELECT-then-INSERT can still collide.
    # - On SQLite, using INSERT OR IGNORE avoids IntegrityError and does not poison the Session.
    now = _dt_utcnow()
    new_id = str(uuid.uuid4())

    db.execute(
        text(
            """
            INSERT OR IGNORE INTO lot_journal
              (id, action, origin_type, origin_ref, venue, wallet_id, asset,
               qty, price_usd, fee_usd, effective_at, applied, impact, created_at)
            VALUES
              (:id, :action, :origin_type, :origin_ref, :venue, :wallet_id, :asset,
               :qty, :price_usd, :fee_usd, :effective_at, :applied, :impact, :created_at)
            """
        ),
        {
            "id": new_id,
            "action": action,
            "origin_type": origin_type,
            "origin_ref": origin_ref,
            "venue": venue,
            "wallet_id": wallet_id,
            "asset": asset,
            "qty": qty,
            "price_usd": price_usd,
            "fee_usd": fee_usd,
            "effective_at": effective_at,
            "applied": 0,
            "impact": None,
            "created_at": now,
        },
    )

    # Re-read (either the row we just inserted, or the existing one).
    row = db.execute(
        select(LotJournal).where(
            and_(
                LotJournal.action == action,
                LotJournal.origin_type == origin_type,
                LotJournal.origin_ref == origin_ref,
            )
        )
    ).scalars().first()

    # Defensive: should never be None, but keep old behavior if something is off.
    if row:
        return row

    # Fallback: construct an in-memory row (won't be used in normal operation).
    j = LotJournal(
        id=new_id,
        action=action,
        origin_type=origin_type,
        origin_ref=origin_ref,
        venue=venue,
        wallet_id=wallet_id,
        asset=asset,
        qty=qty,
        price_usd=price_usd,
        fee_usd=fee_usd,
        effective_at=effective_at,
        applied=False,
        impact=None,
        created_at=now,
    )
    return j


def _create_buy_lot_if_needed(
    db: Session,
    *,
    venue: str,
    wallet_id: str,
    asset: str,
    qty: float,
    price_usd: Optional[float],
    fee_usd: Optional[float],
    acquired_at: datetime,
    origin_type: str,
    origin_ref: str,
    note: Optional[str],
    dry_run: bool,
) -> Dict[str, Any]:
    j = _ensure_journal(
        db,
        action="BUY_LOT_CREATE",
        origin_type=origin_type,
        origin_ref=origin_ref,
        venue=venue,
        wallet_id=wallet_id,
        asset=asset,
        qty=qty,
        price_usd=price_usd,
        fee_usd=fee_usd,
        effective_at=acquired_at,
    )
    if j.applied:
        return {"skipped": True, "reason": "already_applied", "journal_id": j.id}

    basis_total = None
    if price_usd is not None and price_usd > 0:
        basis_total = float(qty * price_usd) + float(fee_usd or 0.0)

    impact = {
        "version": "buy_lot_v1",
        "venue": venue,
        "wallet_id": wallet_id,
        "asset": asset,
        "qty": float(qty),
        "price_usd": price_usd,
        "fee_usd": fee_usd,
        "total_basis_usd": basis_total,
        "acquired_at": acquired_at.isoformat(),
        "origin_type": origin_type,
        "origin_ref": origin_ref,
    }

    if not dry_run:
        lot = BasisLot(
            venue=venue,
            wallet_id=wallet_id,
            asset=asset,
            acquired_at=acquired_at,
            qty_total=float(qty),
            qty_remaining=float(qty),
            total_basis_usd=basis_total,
            basis_is_missing=(basis_total is None),
            basis_source=("FILL" if basis_total is not None else "FILL_MISSING"),
            origin_type="BUY_FILL",
            origin_ref=str(origin_ref),
            note=note,
            created_at=_dt_utcnow(),
            updated_at=_dt_utcnow(),
        )
        db.add(lot)
        db.flush()
        impact["created_lot_id"] = str(lot.id)

        j.applied = True
        j.impact = impact
        db.add(j)

    return {"skipped": False, "journal_id": j.id, "impact": impact, "dry_run": dry_run}


def _consume_sell_if_needed(
    db: Session,
    *,
    venue: str,
    wallet_id: str,
    asset: str,
    qty: float,
    price_usd: Optional[float],
    fee_usd: Optional[float],
    effective_at: datetime,
    origin_type: str,
    origin_ref: str,
    dry_run: bool,
) -> Dict[str, Any]:
    j = _ensure_journal(
        db,
        action="SELL_FIFO_CONSUME",
        origin_type=origin_type,
        origin_ref=origin_ref,
        venue=venue,
        wallet_id=wallet_id,
        asset=asset,
        qty=qty,
        price_usd=price_usd,
        fee_usd=fee_usd,
        effective_at=effective_at,
    )
    if j.applied:
        return {"skipped": True, "reason": "already_applied", "journal_id": j.id}

    avail = _available_qty(db, venue=venue, wallet_id=wallet_id, asset=asset)

    if dry_run:
        if float(avail) + 1e-12 < float(qty):
            impact = {
                "version": "sell_fifo_v1_dryrun",
                "error": "insufficient_inventory",
                "venue": venue,
                "wallet_id": wallet_id,
                "asset": asset,
                "qty_sold": float(qty),
                "qty_available": float(avail),
                "price_usd": price_usd,
                "fee_usd": fee_usd,
                "effective_at": effective_at.isoformat(),
            }
            return {
                "skipped": True,
                "reason": "insufficient_inventory",
                "journal_id": j.id,
                "impact": impact,
                "dry_run": True,
            }

        impact = {
            "version": "sell_fifo_v1_dryrun",
            "venue": venue,
            "wallet_id": wallet_id,
            "asset": asset,
            "qty_sold": float(qty),
            "qty_available": float(avail),
            "price_usd": price_usd,
            "fee_usd": fee_usd,
            "effective_at": effective_at.isoformat(),
        }
        return {"skipped": False, "journal_id": j.id, "impact": impact, "dry_run": True}

    try:
        impact_obj = fifo_consume_sell_fifo(
            db,
            venue=venue,
            wallet_id=wallet_id,
            asset=asset,
            qty_sold=qty,
            price_usd=price_usd,
            fee_usd=fee_usd,
            as_of=effective_at,
            allow_partial=False,
        )
        impact = impact_to_json(impact_obj)

        j.applied = True
        j.impact = impact
        db.add(j)

        return {"skipped": False, "journal_id": j.id, "impact": impact, "dry_run": False}

    except ValueError as e:
        impact = {
            "version": "sell_fifo_v1_error",
            "error": "insufficient_inventory",
            "message": str(e),
            "venue": venue,
            "wallet_id": wallet_id,
            "asset": asset,
            "qty_sold": float(qty),
            "qty_available": float(avail),
            "price_usd": price_usd,
            "fee_usd": fee_usd,
            "effective_at": effective_at.isoformat(),
            "origin_type": origin_type,
            "origin_ref": origin_ref,
        }

        j.applied = False
        j.impact = impact
        db.add(j)

        return {
            "skipped": True,
            "reason": "insufficient_inventory",
            "journal_id": j.id,
            "impact": impact,
            "dry_run": False,
        }


def sync_lots_from_activity(
    db: Session,
    *,
    wallet_id: str = "default",
    mode: str = "ALL",
    limit: int = 500,
    venue: Optional[str] = None,
    symbol_canon: Optional[str] = None,
    since: Optional[datetime] = None,
    dry_run: bool = True,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Manual-only sync for lot ledger.

    - LOCAL: per Fill row
    - VENUE: aggregate per VenueOrderRow

    Filters:
      - venue, symbol_canon, since

    Pagination (for sync_all):
      - accepts cursor/cursor_in: "<iso_ts>|<id>"
      - returns next_cursor (oldest row in fetched batch)
    """
    mode_u = str(mode or "ALL").strip().upper()
    if mode_u not in ("ALL", "LOCAL", "VENUE"):
        mode_u = "ALL"

    wallet_id = _norm_wallet(wallet_id)

    out: Dict[str, Any] = {
        "mode": mode_u,
        "dry_run": bool(dry_run),
        "limit": int(limit),
        "wallet_id": wallet_id,
    }

    created = 0
    consumed = 0
    skipped = 0

    # New: informative skip counters by reason-category
    skipped_already_applied = 0
    skipped_missing_data = 0
    skipped_unknown_side = 0

    errors: List[str] = []

    rows_fetched = 0
    next_cursor: Optional[str] = None

    def _bump_skip(reason: Optional[str]) -> None:
        nonlocal skipped, skipped_already_applied, skipped_missing_data, skipped_unknown_side
        skipped += 1
        r = str(reason or "").strip().lower()
        if r == "already_applied":
            skipped_already_applied += 1
        elif r == "unknown_side":
            skipped_unknown_side += 1
        else:
            # default bucket for anything data-related / guardrail-related:
            # - missing venue/symbol/side/base
            # - qty <= 0
            # - insufficient_inventory
            # - any other explicit reason
            skipped_missing_data += 1

    # ---- LOCAL fills ----
    if mode_u in ("ALL", "LOCAL"):
        try:
            from ..models import Fill  # type: ignore

            rows = db.execute(
                select(Fill, Order)
                .join(Order, Fill.order_id == Order.id)
                .order_by(Fill.ts.desc())
                .limit(limit)
            ).all()

            rows_fetched += len(rows)

            buy_rows = []
            sell_rows = []
            for fill, order in rows:
                side = str(getattr(order, "side", "") or "").strip().lower()
                if side == "buy":
                    buy_rows.append((fill, order))
                elif side == "sell":
                    sell_rows.append((fill, order))

            for fill, order in buy_rows + sell_rows:
                v = _norm_venue(getattr(order, "venue", None))
                side = str(getattr(order, "side", "") or "").strip().lower()
                base = _parse_base_asset(getattr(order, "symbol_canon", None), getattr(order, "symbol_venue", None))
                if not v or not side or not base:
                    _bump_skip("missing_data")
                    continue

                qty = _safe_float(getattr(fill, "qty", None)) or 0.0
                price = _safe_float(getattr(fill, "price", None))
                fee = _safe_float(getattr(fill, "fee", None))
                fee_asset = getattr(fill, "fee_asset", None)

                if qty <= 0:
                    _bump_skip("missing_data")
                    continue

                ts = getattr(fill, "ts", None) or _dt_utcnow()
                fee_usd = _fee_usd_estimate(fee, fee_asset)
                origin_ref = str(getattr(fill, "id"))

                if side == "buy":
                    r = _create_buy_lot_if_needed(
                        db,
                        venue=v,
                        wallet_id=wallet_id,
                        asset=base,
                        qty=qty,
                        price_usd=price,
                        fee_usd=fee_usd,
                        acquired_at=ts,
                        origin_type="LOCAL_FILL",
                        origin_ref=origin_ref,
                        note="auto lot from LOCAL fill",
                        dry_run=dry_run,
                    )
                    if r.get("skipped"):
                        _bump_skip(r.get("reason"))
                    else:
                        created += 1

                elif side == "sell":
                    r = _consume_sell_if_needed(
                        db,
                        venue=v,
                        wallet_id=wallet_id,
                        asset=base,
                        qty=qty,
                        price_usd=price,
                        fee_usd=fee_usd,
                        effective_at=ts,
                        origin_type="LOCAL_FILL",
                        origin_ref=origin_ref,
                        dry_run=dry_run,
                    )
                    if r.get("skipped"):
                        _bump_skip(r.get("reason"))
                    else:
                        consumed += 1
                else:
                    _bump_skip("unknown_side")

        except Exception as e:
            errors.append(f"LOCAL sync disabled/failed: {e!r}")

    # ---- VENUE aggregate orders ----
    if mode_u in ("ALL", "VENUE"):
        effective_ts = func.coalesce(
            VenueOrderRow.updated_at,
            VenueOrderRow.created_at,
            VenueOrderRow.captured_at,
        )

        # “Executed” heuristic: either filled_qty > 0 OR avg_fill_price > 0
        filled_expr = func.coalesce(VenueOrderRow.filled_qty, 0.0)
        price_expr = func.coalesce(VenueOrderRow.avg_fill_price, 0.0)
        executed_expr = or_(filled_expr > 0.0, price_expr > 0.0)

        cursor_in = kwargs.get("cursor") or kwargs.get("cursor_in")
        cursor_tuple = _parse_cursor(cursor_in)

        try:
            q = select(VenueOrderRow).where(executed_expr)

            # IMPORTANT: The lightweight /api/ledger/sync endpoint only processes a single batch.
            # If historical rows get "re-touched" (updated_at changes), they float into the newest
            # window and can saturate the batch with already-ledger-applied rows. That causes /sync
            # to do no work (lots:0 / sells:0) even when new orders exist.
            #
            # Fix: fetch only rows that have NOT produced any lot_journal output yet.
            q = q.where(
                ~exists(
                    select(1)
                    .select_from(LotJournal)
                    .where(LotJournal.origin_ref == VenueOrderRow.id)
                )
            )

            v_filter = (venue or "").strip().lower() or None
            if v_filter:
                q = q.where(func.lower(VenueOrderRow.venue) == v_filter)

            s_filter = (symbol_canon or "").strip().upper() or None
            if s_filter:
                q = q.where(func.upper(VenueOrderRow.symbol_canon) == s_filter)

            if since is not None:
                q = q.where(effective_ts >= since)

            if cursor_tuple is not None:
                c_ts, c_id = cursor_tuple
                q = q.where(or_(effective_ts < c_ts, and_(effective_ts == c_ts, VenueOrderRow.id < c_id)))

            fetched = db.execute(
                q.order_by(desc(effective_ts), desc(VenueOrderRow.id)).limit(limit)
            ).scalars().all()

            rows_fetched += len(fetched)

            if fetched:
                oldest = fetched[-1]
                oldest_ts = oldest.updated_at or oldest.created_at or oldest.captured_at or _dt_utcnow()
                oldest_id = str(getattr(oldest, "id", "") or "")
                next_cursor = f"{oldest_ts.isoformat()}|{oldest_id}" if oldest_id else None

            def _effective_at(o: VenueOrderRow) -> datetime:
                return o.updated_at or o.created_at or o.captured_at or _dt_utcnow()

            fetched.sort(
                key=lambda o: (
                    _effective_at(o),
                    0 if (getattr(o, "side", "") or "").strip().lower() == "buy" else 1,
                    str(getattr(o, "venue_order_id", "") or ""),
                    str(getattr(o, "id", "") or ""),
                )
            )

        except Exception as e:
            errors.append(f"VENUE query/sort failed: {e!r}")
            fetched = []

        for vrow in fetched:
            # Capture ID early so error reporting never triggers lazy loads on a failed session.
            vrow_id = str(getattr(vrow, "id", "") or "")
            try:
                # Isolate each row inside a SAVEPOINT so one bad row can't poison the whole batch.
                with db.begin_nested():
                    v = _norm_venue(getattr(vrow, "venue", None))
                    side = str(getattr(vrow, "side", "") or "").strip().lower()
                    base = _parse_base_asset(getattr(vrow, "symbol_canon", None), getattr(vrow, "symbol_venue", None))
                    if not v or not side or not base:
                        _bump_skip("missing_data")
                        continue

                    # qty selection: prefer filled_qty if >0, else fall back to qty if >0
                    filled_qty = _safe_float(getattr(vrow, "filled_qty", None))
                    order_qty = _safe_float(getattr(vrow, "qty", None))
                    qty = (filled_qty if (filled_qty is not None and filled_qty > 0) else (order_qty or 0.0))

                    if qty <= 0:
                        _bump_skip("missing_data")
                        continue

                    price = _safe_float(getattr(vrow, "avg_fill_price", None))
                    fee = _safe_float(getattr(vrow, "fee", None))
                    fee_asset = getattr(vrow, "fee_asset", None)
                    fee_usd = _fee_usd_estimate(fee, fee_asset)

                    ts = (
                        getattr(vrow, "updated_at", None)
                        or getattr(vrow, "created_at", None)
                        or getattr(vrow, "captured_at", None)
                        or _dt_utcnow()
                    )
                    origin_ref = vrow_id

                    if side == "buy":
                        r = _create_buy_lot_if_needed(
                            db,
                            venue=v,
                            wallet_id=wallet_id,
                            asset=base,
                            qty=qty,
                            price_usd=price,
                            fee_usd=fee_usd,
                            acquired_at=ts,
                            origin_type="VENUE_ORDER_AGG",
                            origin_ref=origin_ref,
                            note="auto lot from VENUE aggregate fill",
                            dry_run=dry_run,
                        )
                        if r.get("skipped"):
                            _bump_skip(r.get("reason"))
                        else:
                            created += 1

                    elif side == "sell":
                        r = _consume_sell_if_needed(
                            db,
                            venue=v,
                            wallet_id=wallet_id,
                            asset=base,
                            qty=qty,
                            price_usd=price,
                            fee_usd=fee_usd,
                            effective_at=ts,
                            origin_type="VENUE_ORDER_AGG",
                            origin_ref=origin_ref,
                            dry_run=dry_run,
                        )
                        if r.get("skipped"):
                            _bump_skip(r.get("reason"))
                        else:
                            consumed += 1
                    else:
                        _bump_skip("unknown_side")

            except Exception as e:
                errors.append(f"VENUE row failed (id={vrow_id}): {e!r}")
                _bump_skip("missing_data")

    out.update(
        {
            "created_lots": int(created),
            "consumed_sells": int(consumed),
            "skipped": int(skipped),
            "skipped_already_applied": int(skipped_already_applied),
            "skipped_missing_data": int(skipped_missing_data),
            "skipped_unknown_side": int(skipped_unknown_side),
            "errors": errors,
            "rows_fetched": int(rows_fetched),
            "next_cursor": next_cursor,
        }
    )
    return out
