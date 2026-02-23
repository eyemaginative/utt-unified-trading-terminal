# backend/app/services/venue_orders.py

from __future__ import annotations

import logging
from typing import Optional, Tuple, List, Dict
from datetime import datetime, timedelta, timezone

from sqlalchemy.orm import Session
from sqlalchemy import select, func, asc, desc
from sqlalchemy.exc import IntegrityError

from ..models import VenueOrderRow, VenueCooldown
from ..config import settings
from ..utils import now_utc, parse_sort
from .symbols import get_adapter

logger = logging.getLogger(__name__)

_ALLOWED_SORT = {
    "captured_at",
    "created_at",
    "updated_at",
    "venue",
    "status",
    "symbol_venue",
    "symbol_canon",
    "side",
    "type",
    "qty",
    "filled_qty",
    "limit_price",
    "avg_fill_price",
    "fee",
    "fee_asset",
    "total_after_fee",
}

# Canonical terminal statuses (after normalize_status()).
_TERMINAL = {"filled", "canceled", "rejected", "expired", "failed"}


def normalize_status(s: Optional[str]) -> Optional[str]:
    """
    Normalize venue status to lowercase canonical strings.

    Canonical vocabulary in UTT:
      open, partial, filled, canceled, rejected, expired, failed

    This function MUST handle venue-native variants (including Crypto.com):
      ACTIVE/FILLED/CANCELED (uppercase)
    """
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None

    u = t.strip().lower()

    # Open-ish states
    if u in {"open", "active", "new", "pending", "live", "working", "accepted", "ack", "acked"}:
        return "open"

    # Partial fill variants
    if u in {"partial", "partially_filled", "partial_fill", "partial-filled", "partial fill"}:
        return "partial"

    # Filled / done variants
    if u in {"filled", "done", "closed", "complete", "completed", "settled"}:
        return "filled"

    # Canceled variants
    if u in {"canceled", "cancelled", "canceling", "cancel"}:
        return "canceled"

    # Rejection / error variants
    if u in {"rejected", "failed", "error"}:
        return "rejected"

    # Expired variants
    if u in {"expired"}:
        return "expired"

    # Fall-through: store the lowercase string (best effort)
    return u


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def get_venue_cooldown_until(db: Session, venue: str) -> Optional[datetime]:
    row = db.get(VenueCooldown, venue)
    if not row or not row.blocked_until:
        return None
    # Treat naive as UTC if it ever occurs
    if row.blocked_until.tzinfo is None:
        return row.blocked_until.replace(tzinfo=timezone.utc)
    return row.blocked_until


def set_venue_cooldown(db: Session, venue: str, seconds: int, reason: str) -> None:
    until = _utcnow() + timedelta(seconds=seconds)
    row = db.get(VenueCooldown, venue)
    if row is None:
        row = VenueCooldown(venue=venue, blocked_until=until, reason=reason)
        db.add(row)
    else:
        row.blocked_until = until
        row.reason = reason
    db.commit()


def clear_venue_cooldown(db: Session, venue: str) -> None:
    row = db.get(VenueCooldown, venue)
    if row:
        row.blocked_until = None
        row.reason = None
        db.commit()


def _norm_status(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _should_update_status(old_s: Optional[str], new_s: Optional[str]) -> bool:
    if new_s is None:
        return False
    o = normalize_status(old_s) or _norm_status(old_s)
    n = normalize_status(new_s) or _norm_status(new_s)
    if not n:
        return False
    # Never regress terminal -> non-terminal.
    if o in _TERMINAL and n not in _TERMINAL:
        return False
    # Never regress filled -> other terminal.
    if o == "filled" and n in _TERMINAL and n != "filled":
        return False
    return True


def _safe_dt(x) -> Optional[datetime]:
    """
    Ensure we only store/compare naive UTC datetimes in this service layer.
    If an adapter returns timezone-aware datetimes, normalize to UTC and strip tzinfo.
    """
    if x is None:
        return None
    if isinstance(x, datetime):
        if x.tzinfo is not None:
            try:
                return x.astimezone(timezone.utc).replace(tzinfo=None)
            except Exception:
                return x.replace(tzinfo=None)
        return x
    return None


def _safe_str(x, max_len: int) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    return s[:max_len]


def _fnum(x) -> Optional[float]:
    try:
        return float(x) if x is not None else None
    except Exception:
        return None


def _is_filled_without_exec(status: Optional[str], filled_qty: Optional[float]) -> bool:
    """
    Guardrail: never persist a 'filled' order with no executed quantity.
    This prevents poisoned rows from any adapter regression.
    """
    if status != "filled":
        return False
    if filled_qty is None:
        return True
    try:
        return float(filled_qty) <= 0.0
    except Exception:
        return True


def _item_rank(it: dict, now: datetime) -> float:
    u = _safe_dt(it.get("updated_at"))
    c = _safe_dt(it.get("created_at"))
    ts = u or c or now
    try:
        return ts.timestamp()
    except Exception:
        return 0.0


def _dedupe_items(items: List[dict], now: datetime) -> List[dict]:
    best: Dict[str, dict] = {}
    best_rank: Dict[str, float] = {}

    for it in items or []:
        if not isinstance(it, dict):
            continue
        oid = _safe_str(it.get("venue_order_id"), 64)
        if not oid:
            continue

        r = _item_rank(it, now)
        if oid not in best or r >= best_rank.get(oid, -1e18):
            best[oid] = it
            best_rank[oid] = r

    return list(best.values())


def refresh_venue_orders(db: Session, venue: str, force: bool = False) -> int:
    # Shared cooldown check (SQLite-backed). If force=True, we bypass cooldown intentionally.
    blocked_until = get_venue_cooldown_until(db, venue)
    if blocked_until and (_utcnow() < blocked_until) and not force:
        return 0

    adapter = get_adapter(venue)

    # ---------------------------------------------------------------------
    # Stale-open hardening:
    # Some venues' list endpoints are newest-first and/or omit older closed
    # orders. If we have orders stored as open/partial in SQLite, but they no
    # longer appear in the list snapshot, we can ask the adapter for per-id
    # detail (when supported) to resolve them to filled/canceled/etc.
    # ---------------------------------------------------------------------
    _OPEN_VARIANTS = {"open", "active", "pending", "new", "live", "working", "accepted", "ack", "acked"}
    _PARTIAL_VARIANTS = {"partial", "partially_filled", "partial_fill", "partial-filled", "partial fill"}

    open_ids_for_detail: List[str] = []
    try:
        open_ids_for_detail = list(
            db.execute(
                select(VenueOrderRow.venue_order_id).where(
                    VenueOrderRow.venue == venue,
                    func.lower(VenueOrderRow.status).in_(sorted(_OPEN_VARIANTS | _PARTIAL_VARIANTS)),
                ).limit(int(getattr(settings, "venue_orders_detail_max_ids", 250)))
            ).scalars().all()
        )
    except Exception:
        open_ids_for_detail = []

    now = now_utc()
    now = _safe_dt(now) or datetime.utcnow()
    try:
        # Prefer OPEN-only snapshot if supported (prevents old-but-open orders falling off a newest-first history scan)
        if hasattr(adapter, "fetch_open_orders"):
            try:
                raw_items = adapter.fetch_open_orders(dry_run=False) or []
            except Exception:
                raw_items = adapter.fetch_orders(dry_run=False) or []
        else:
            raw_items = adapter.fetch_orders(dry_run=False) or []

        # If the adapter can fetch a single order by id, use it to resolve
        # any stored open/partial ids that are missing from the list snapshot.
        if open_ids_for_detail and hasattr(adapter, "fetch_order"):
            try:
                present_ids = {
                    str(x.get("venue_order_id"))
                    for x in (raw_items or [])
                    if isinstance(x, dict) and x.get("venue_order_id") is not None
                }
                missing = [oid for oid in open_ids_for_detail if oid and oid not in present_ids]
                if missing:
                    max_extra = int(getattr(settings, "venue_orders_detail_max_fetch", 150))
                    for oid in missing[:max_extra]:
                        try:
                            one = adapter.fetch_order(oid)
                            if isinstance(one, dict):
                                raw_items.append(one)
                            elif isinstance(one, list):
                                raw_items.extend([x for x in one if isinstance(x, dict)])
                        except Exception:
                            continue
            except Exception:
                pass

        # Success: clear cooldown so the venue is considered healthy again.
        clear_venue_cooldown(db, venue)
    except Exception as e:
        msg = str(e).lower()

        # Coinbase circuit-breaker: persist cooldown so all workers share it.
        if venue == "coinbase" and ("too many errors" in msg):
            set_venue_cooldown(
                db,
                venue,
                seconds=int(getattr(settings, "coinbase_too_many_errors_cooldown_s", 900)),
                reason="coinbase: too many errors",
            )
            return 0

        raise Exception(f"{venue} fetch_orders failed: {e}")

    items = _dedupe_items(raw_items, now)

    upsert_count = 0
    skipped_filled_noexec = 0

    for it in items:
        if not isinstance(it, dict):
            continue

        venue_order_id = _safe_str(it.get("venue_order_id"), 64)
        if not venue_order_id:
            continue

        it_created = _safe_dt(it.get("created_at"))
        it_updated = _safe_dt(it.get("updated_at"))

        symbol_venue_opt = _safe_str(it.get("symbol_venue"), 32)
        if not symbol_venue_opt:
            continue
        symbol_venue = symbol_venue_opt

        symbol_canon = _safe_str(it.get("symbol_canon"), 32)

        side = _safe_str(it.get("side"), 8)
        type_ = _safe_str(it.get("type"), 16)

        status_raw = _safe_str(it.get("status"), 32)
        status = normalize_status(status_raw) or (status_raw.lower() if status_raw else None)

        qty = _fnum(it.get("qty"))
        filled_qty = _fnum(it.get("filled_qty"))
        limit_price = _fnum(it.get("limit_price"))
        avg_fill_price = _fnum(it.get("avg_fill_price"))

        fee = _fnum(it.get("fee"))
        fee_asset = _safe_str(it.get("fee_asset"), 16)
        total_after_fee = _fnum(it.get("total_after_fee"))

        # -----------------------------------------------------------------
        # Guardrail: never persist poisoned "filled" rows with no execution.
        # -----------------------------------------------------------------
        if _is_filled_without_exec(status, filled_qty):
            skipped_filled_noexec += 1
            continue

        existing = db.execute(
            select(VenueOrderRow).where(
                VenueOrderRow.venue == venue,
                VenueOrderRow.venue_order_id == venue_order_id,
            )
        ).scalar_one_or_none()

        if existing:
            changed = False

            def set_if_diff(attr: str, new_val):
                nonlocal changed
                old_val = getattr(existing, attr)
                if old_val != new_val:
                    setattr(existing, attr, new_val)
                    changed = True

            def set_if_diff_opt(attr: str, new_val):
                if new_val is None:
                    return
                set_if_diff(attr, new_val)

            def set_if_diff_num_opt(attr: str, new_val):
                if new_val is None:
                    return
                set_if_diff(attr, new_val)

            set_if_diff("symbol_venue", symbol_venue)

            set_if_diff_opt("symbol_canon", symbol_canon)
            set_if_diff_opt("side", side)
            set_if_diff_opt("type", type_)

            if _should_update_status(existing.status, status):
                set_if_diff("status", status)

            set_if_diff_num_opt("qty", qty)
            set_if_diff_num_opt("filled_qty", filled_qty)
            set_if_diff_num_opt("limit_price", limit_price)
            set_if_diff_num_opt("avg_fill_price", avg_fill_price)

            set_if_diff_num_opt("fee", fee)
            set_if_diff_opt("fee_asset", fee_asset)
            set_if_diff_num_opt("total_after_fee", total_after_fee)

            if it_created is not None:
                if existing.created_at is None:
                    set_if_diff("created_at", it_created)
                else:
                    try:
                        if it_created < existing.created_at:
                            set_if_diff("created_at", it_created)
                    except Exception:
                        pass

            if it_updated is not None:
                if existing.updated_at is None:
                    set_if_diff("updated_at", it_updated)
                else:
                    try:
                        if it_updated >= existing.updated_at:
                            set_if_diff("updated_at", it_updated)
                    except Exception:
                        pass
            else:
                if existing.updated_at is None and existing.created_at is not None:
                    set_if_diff("updated_at", existing.created_at)

            if force:
                existing.captured_at = now
                changed = True
            else:
                if changed:
                    existing.captured_at = now

            if changed:
                db.add(existing)
                upsert_count += 1

        else:
            created_at = it_created or it_updated or now
            updated_at = it_updated or created_at

            row = VenueOrderRow(
                venue=venue,
                venue_order_id=venue_order_id,
                symbol_venue=symbol_venue,
                symbol_canon=symbol_canon,
                side=side,
                type=type_,
                status=status,
                qty=qty,
                filled_qty=filled_qty,
                limit_price=limit_price,
                avg_fill_price=avg_fill_price,
                fee=fee,
                fee_asset=fee_asset,
                total_after_fee=total_after_fee,
                created_at=created_at,
                updated_at=updated_at,
                captured_at=now,
            )

            try:
                db.add(row)
                db.flush()
                upsert_count += 1
            except IntegrityError:
                db.rollback()

                existing2 = db.execute(
                    select(VenueOrderRow).where(
                        VenueOrderRow.venue == venue,
                        VenueOrderRow.venue_order_id == venue_order_id,
                    )
                ).scalar_one_or_none()

                if existing2:
                    changed = False

                    def set_if_diff2(attr: str, new_val):
                        nonlocal changed
                        old_val = getattr(existing2, attr)
                        if old_val != new_val:
                            setattr(existing2, attr, new_val)
                            changed = True

                    def set_if_diff2_opt(attr: str, new_val):
                        if new_val is None:
                            return
                        set_if_diff2(attr, new_val)

                    def set_if_diff2_num_opt(attr: str, new_val):
                        if new_val is None:
                            return
                        set_if_diff2(attr, new_val)

                    set_if_diff2("symbol_venue", symbol_venue)

                    set_if_diff2_opt("symbol_canon", symbol_canon)
                    set_if_diff2_opt("side", side)
                    set_if_diff2_opt("type", type_)

                    if _should_update_status(existing2.status, status):
                        set_if_diff2("status", status)

                    set_if_diff2_num_opt("qty", qty)
                    set_if_diff2_num_opt("filled_qty", filled_qty)
                    set_if_diff2_num_opt("limit_price", limit_price)
                    set_if_diff2_num_opt("avg_fill_price", avg_fill_price)

                    set_if_diff2_num_opt("fee", fee)
                    set_if_diff2_opt("fee_asset", fee_asset)
                    set_if_diff2_num_opt("total_after_fee", total_after_fee)

                    if it_created is not None:
                        if existing2.created_at is None:
                            set_if_diff2("created_at", it_created)
                        else:
                            try:
                                if it_created < existing2.created_at:
                                    set_if_diff2("created_at", it_created)
                            except Exception:
                                pass

                    if it_updated is not None:
                        if existing2.updated_at is None:
                            set_if_diff2("updated_at", it_updated)
                        else:
                            try:
                                if it_updated >= existing2.updated_at:
                                    set_if_diff2("updated_at", it_updated)
                            except Exception:
                                pass
                    else:
                        if existing2.updated_at is None and existing2.created_at is not None:
                            set_if_diff2("updated_at", existing2.created_at)

                    if force or changed:
                        existing2.captured_at = now

                    if force or changed:
                        db.add(existing2)
                        upsert_count += 1

    db.commit()

    if skipped_filled_noexec > 0:
        logger.warning(
            "venue_orders refresh: skipped %d poisoned filled rows with filled_qty<=0 (venue=%s)",
            skipped_filled_noexec,
            venue,
        )

    try:
        from .reconcile import reconcile_local_orders_from_venue_snapshots
        reconcile_local_orders_from_venue_snapshots(db, venue)
    except Exception:
        pass

    return upsert_count


def latest_venue_orders(
    db: Session,
    venue: Optional[str],
    status: Optional[str],
    source: Optional[str],
    symbol: Optional[str],
    dt_from: Optional[datetime],
    dt_to: Optional[datetime],
    sort: Optional[str],
    page: int,
    page_size: int,
) -> Tuple[List[VenueOrderRow], int, Optional[datetime]]:
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)

    stmt = select(VenueOrderRow)
    count_stmt = select(func.count()).select_from(VenueOrderRow)

    # Status variants to correctly match legacy/mixed venue strings in DB.
    _STATUS_VARIANTS = {
        "open": {"open", "active", "pending", "new", "live", "working", "accepted"},
        "filled": {"filled", "done", "closed", "complete", "completed", "settled"},
        "canceled": {"canceled", "cancelled", "canceling", "cancel"},
        "partial": {"partial", "partially_filled", "partial_fill", "partial-filled", "partial fill"},
        "rejected": {"rejected", "failed", "error"},
        "expired": {"expired"},
    }

    def apply_filters(s):
        if venue:
            s = s.where(VenueOrderRow.venue == venue)

        if status:
            st = normalize_status(status)
            if st:
                v = _STATUS_VARIANTS.get(st)
                if v:
                    s = s.where(func.lower(VenueOrderRow.status).in_(sorted(v)))
                else:
                    s = s.where(func.lower(VenueOrderRow.status) == st)

        if symbol:
            s = s.where((VenueOrderRow.symbol_canon == symbol) | (VenueOrderRow.symbol_venue == symbol))
        if dt_from:
            s = s.where(VenueOrderRow.captured_at >= dt_from)
        if dt_to:
            s = s.where(VenueOrderRow.captured_at <= dt_to)
        return s

    stmt = apply_filters(stmt)
    count_stmt = apply_filters(count_stmt)

    total = db.execute(count_stmt).scalar_one()

    field, direction = parse_sort(sort, _ALLOWED_SORT, default=("captured_at", "desc"))
    col = getattr(VenueOrderRow, field)

    stmt = stmt.order_by(
        (desc(col) if direction == "desc" else asc(col)),
        desc(VenueOrderRow.captured_at),
        desc(VenueOrderRow.venue_order_id),
    )

    stmt = stmt.offset((page - 1) * page_size).limit(page_size)
    items = db.execute(stmt).scalars().all()

    as_of = None
    if items:
        as_of = max((r.captured_at for r in items if r.captured_at), default=None)

    return items, total, as_of
