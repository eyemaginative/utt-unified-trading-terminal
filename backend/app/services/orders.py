# backend/app/services/orders.py

from sqlalchemy.orm import Session
from sqlalchemy import select, func, asc, desc
from typing import Optional, Tuple
from datetime import datetime
import os
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING

from ..models import Order, VenueOrderRow
from ..schemas import OrderCreate
from ..utils import new_client_order_id, now_utc, parse_sort
from .symbols import get_adapter, resolve_symbol
from ..config import settings

_ALLOWED_SORT = {
    "created_at", "updated_at", "venue", "symbol_canon", "symbol_venue",
    "status",
    "side", "type",
    "qty", "limit_price",
    "filled_qty", "avg_fill_price",
    "venue_order_id",
}

_TERMINAL = {"filled", "canceled", "cancelled", "rejected", "done", "closed", "expired", "failed"}


# ─────────────────────────────────────────────────────────────
# Cancel semantics helper
# ─────────────────────────────────────────────────────────────
def _is_already_closed_cancel_error(msg: str) -> bool:
    """
    Treat these as "success" for cancel semantics because they mean the order is NOT open:
      - Crypto.com: code=40401 message=NOT_FOUND
      - Common variants: not found / not_found / unknown order / already canceled / already filled / not cancelable
    """
    m = (msg or "").strip().lower()
    if not m:
        return False

    # Crypto.com specific
    if "40401" in m and ("not_found" in m or "not found" in m or "notfound" in m):
        return True
    if "code=40401" in m:
        return True

    # Generic "already not open"
    needles = (
        "not_found", "not found", "unknown order", "order not found",
        "already canceled", "already cancelled", "already filled",
        "already closed", "not cancelable", "not cancellable",
        "cannot cancel", "unable to cancel", "cancel rejected",
    )
    return any(n in m for n in needles)


# ─────────────────────────────────────────────────────────────
# LIVE safety caps (env-driven; do not require settings.py changes)
# ─────────────────────────────────────────────────────────────
def _env_float(name: str, default: Optional[float]) -> Optional[float]:
    v = (os.getenv(name, "") or "").strip()
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if not v:
        return default
    return v in ("1", "true", "yes", "y", "on")


def _to_dec(x: Optional[float]) -> Optional[Decimal]:
    if x is None:
        return None
    try:
        return Decimal(str(x))
    except Exception:
        return None


def _floor_to_step(value: float, step: float) -> float:
    """
    Floors value down to the nearest multiple of step.
    """
    if step is None or step <= 0:
        return float(value)
    dv = _to_dec(float(value))
    ds = _to_dec(float(step))
    if dv is None or ds is None or ds <= 0:
        return float(value)
    q = (dv / ds).to_integral_value(rounding=ROUND_FLOOR)
    return float(q * ds)


def _ceil_to_step(value: float, step: float) -> float:
    """
    Ceils value up to the nearest multiple of step.
    """
    if step is None or step <= 0:
        return float(value)
    dv = _to_dec(float(value))
    ds = _to_dec(float(step))
    if dv is None or ds is None or ds <= 0:
        return float(value)
    q = (dv / ds).to_integral_value(rounding=ROUND_CEILING)
    return float(q * ds)


def _validate_and_normalize_live_order(
    *,
    adapter,
    symbol_venue: str,
    side: str,
    type_: str,
    qty: float,
    limit_price: Optional[float],
    tif: Optional[str],
    post_only: bool,
) -> Tuple[Optional[float], Optional[float], list[str]]:
    """
    Returns: (norm_qty, norm_limit_price, errors)

    - Floors qty to base_increment
    - Floors limit_price to price_increment (limit orders only)
    - Enforces min_qty, min_notional when possible
    - Blocks unsupported flags unless venue rules explicitly say supported
    """
    errs: list[str] = []

    # Best-effort rules fetch
    rules = {}
    try:
        rules = adapter.get_order_rules(symbol_venue) or {}
    except Exception as e:
        rules = {}
        errs.append(f"rules fetch failed: {e}")

    strict = _env_bool("STRICT_RULES_LIVE", True)

    base_inc = rules.get("base_increment")
    price_inc = rules.get("price_increment")
    min_qty = rules.get("min_qty")
    min_notional = rules.get("min_notional")

    supports_post_only = bool(rules.get("supports_post_only", False))
    supported_tifs = rules.get("supported_tifs") or []
    if not isinstance(supported_tifs, list):
        supported_tifs = []

    venue_name = str(getattr(adapter, "venue", "") or "").strip().lower()

    # If strict, require basic fields that prevent obvious bad orders.
    if strict:
        if base_inc is None:
            errs.append("missing rule: base_increment")
        if type_ == "limit" and price_inc is None:
            errs.append("missing rule: price_increment (limit order)")
        # Dex-Trade can omit/reshape min_trade; do not deadlock strict mode on min_qty alone.
        if min_qty is None and venue_name != "dex_trade":
            errs.append("missing rule: min_qty")

    # Reject unsupported fields (do not silently ignore)
    if post_only and not supports_post_only:
        errs.append("post_only not supported for this venue")

    # TIF:
    # - If venue supplies supported_tifs list, enforce it.
    # - If not supplied, we only allow None/""/"gtc" (conservative default).
    tif_norm = (tif or "").strip().lower()
    if tif_norm:
        if supported_tifs:
            if tif_norm not in [str(x).strip().lower() for x in supported_tifs if str(x).strip()]:
                errs.append(f"tif '{tif_norm}' not supported for this venue")
        else:
            if tif_norm != "gtc":
                errs.append(f"tif '{tif_norm}' not supported for this venue (only gtc supported currently)")

    # Normalize qty/price to increments
    norm_qty = float(qty)
    if base_inc is not None:
        norm_qty = _floor_to_step(norm_qty, float(base_inc))
        if norm_qty <= 0:
            errs.append("qty floors to 0 under base_increment")

    norm_px = limit_price
    if type_ == "limit":
        if norm_px is None or float(norm_px) <= 0:
            errs.append("limit_price required for limit orders")
        else:
            if price_inc is not None:
                norm_px = _floor_to_step(float(norm_px), float(price_inc))
                if float(norm_px) <= 0:
                    errs.append("limit_price floors to 0 under price_increment")

    # ─────────────────────────────────────────────────────────────
    # Crypto.com LIMIT semantics:
    # For min-notional-driven minimum quantity, Crypto.com effectively evaluates
    # LIMIT orders at the entered limit price (after tick rounding), not at mark.
    #
    # Our Crypto.com adapter may publish min_qty as "effective min at current mark"
    # for UI display; that is NOT appropriate for pretrade validation when the user
    # provides an explicit limit price.
    #
    # Therefore:
    #   if venue == cryptocom and type == limit and we have min_notional + base_inc + norm_px:
    #     effective_min_qty = ceil(min_notional / norm_px) to base_inc
    #     and use that for the min_qty check (Crypto.com only).
    # ─────────────────────────────────────────────────────────────
    eff_min_qty_for_check = min_qty
    if (
        venue_name == "cryptocom"
        and type_ == "limit"
        and norm_px is not None
        and base_inc is not None
        and min_notional is not None
    ):
        try:
            px = float(norm_px)
            step = float(base_inc)
            mn = float(min_notional)
            if px > 0 and step > 0 and mn > 0:
                raw_req = mn / px
                req_qty = _ceil_to_step(raw_req, step)
                # Ensure at least one step
                eff_min_qty_for_check = max(step, req_qty)
        except Exception:
            # If anything goes wrong, fall back to published min_qty
            eff_min_qty_for_check = min_qty

    # min_qty check
    if eff_min_qty_for_check is not None and norm_qty is not None:
        try:
            if float(norm_qty) < float(eff_min_qty_for_check):
                errs.append(f"qty {norm_qty} < min_qty {eff_min_qty_for_check}")
        except Exception:
            pass

    # notional checks are only safe/definite for limit orders (we know price).
    notional = None
    if type_ == "limit" and norm_px is not None:
        try:
            notional = float(norm_qty) * float(norm_px)
        except Exception:
            notional = None

    if min_notional is not None and notional is not None:
        try:
            if float(notional) < float(min_notional):
                errs.append(f"notional {notional} < min_notional {min_notional}")
        except Exception:
            pass
    return (norm_qty, (float(norm_px) if norm_px is not None else None), errs)


def _effective_dry_run() -> bool:
    """
    Effective dry-run is TRUE when:
      - settings.dry_run is truthy, OR
      - settings.armed is falsy / missing.

    Cast through bool() to avoid surprises if settings values are None/strings.
    """
    dry = bool(getattr(settings, "dry_run", False))
    armed = bool(getattr(settings, "armed", False))
    return dry or (not armed)


def _dry_run_reason() -> str:
    parts = []
    if bool(getattr(settings, "dry_run", False)):
        parts.append("settings.dry_run=True")
    if not bool(getattr(settings, "armed", False)):
        parts.append("settings.armed=False")
    return ", ".join(parts) if parts else "unknown"


def _req_client_oid(req: OrderCreate) -> Optional[str]:
    # OrderTicketWidget sends client_order_id optionally.
    v = getattr(req, "client_order_id", None)
    if v is None:
        v = getattr(req, "clientOid", None)  # legacy/alt naming safety
    if not v:
        return None
    s = str(v).strip()
    return s or None


def _req_tif(req: OrderCreate) -> Optional[str]:
    v = getattr(req, "tif", None)
    if not v:
        return None
    s = str(v).strip().lower()
    return s or None


def _req_post_only(req: OrderCreate) -> bool:
    v = getattr(req, "post_only", None)
    if v is None:
        v = getattr(req, "postOnly", None)  # legacy/alt naming safety
    return bool(v) if v is not None else False


def _place_order_safely(adapter, *, symbol_venue: str, side: str, type_: str, qty: float,
                        limit_price: Optional[float], client_order_id: str, dry_run: bool,
                        tif: Optional[str], post_only: bool):
    """
    Cross-adapter safety shim:
    - Preferred call includes tif/post_only (newer adapters).
    - If an adapter doesn't accept them, retry without those kwargs.
    This prevents one adapter's signature from breaking others.
    """
    base_kwargs = dict(
        symbol_venue=symbol_venue,
        side=side,
        type_=type_,
        qty=qty,
        limit_price=limit_price,
        client_order_id=client_order_id,
        dry_run=dry_run,
    )

    # Try with optional fields first.
    try:
        return adapter.place_order(
            **base_kwargs,
            tif=tif,
            post_only=post_only,
        )
    except TypeError as e:
        # Only fall back for the specific case: unexpected keyword arguments.
        msg = str(e) or ""
        if "unexpected keyword argument" in msg:
            return adapter.place_order(**base_kwargs)
        raise


def _cancel_order_safely(adapter, venue_order_id: str, dry_run: bool) -> bool:
    """
    Cross-adapter safety shim for cancel.

    Primary attempt matches your current adapters: cancel_order(<id>, dry_run=bool)
    Fallbacks cover:
      - adapters that don't accept dry_run kwarg
      - adapters that require keyword venue_order_id=
    """
    # 1) Preferred: positional id + dry_run kwarg
    try:
        return bool(adapter.cancel_order(venue_order_id, dry_run=dry_run))
    except TypeError as e:
        msg = str(e) or ""
        if "unexpected keyword argument" in msg:
            # 2) Positional id only
            return bool(adapter.cancel_order(venue_order_id))
        # 3) Try keyword venue_order_id=
        try:
            return bool(adapter.cancel_order(venue_order_id=venue_order_id, dry_run=dry_run))
        except TypeError as e2:
            msg2 = str(e2) or ""
            if "unexpected keyword argument" in msg2:
                return bool(adapter.cancel_order(venue_order_id=venue_order_id))
            raise


def create_order(db: Session, req: OrderCreate) -> Order:
    # Normalize venue for identity stability across LOCAL vs VENUE ingestion.
    venue = (req.venue or "").strip().lower()
    if not venue:
        raise ValueError("venue is required")

    symbol_canon, symbol_venue = resolve_symbol(venue, req.symbol)

    # Prefer caller-provided client_order_id if present (UI supports it),
    # otherwise generate a deterministic internal one.
    client_oid = _req_client_oid(req) or new_client_order_id()
    now = now_utc()

    order = Order(
        client_order_id=client_oid,
        venue=venue,
        symbol_canon=symbol_canon,
        symbol_venue=symbol_venue,
        side=req.side,
        type=req.type,
        qty=req.qty,
        limit_price=req.limit_price if req.type == "limit" else None,
        status="routed",
        filled_qty=0.0,
        avg_fill_price=None,
        venue_order_id=None,
        reject_reason=None,
        created_at=now,
        submitted_at=None,
        updated_at=now,
    )
    db.add(order)
    db.commit()
    db.refresh(order)

    # Compute policy once
    is_dry = _effective_dry_run()

    # DRY-RUN CONTRACT:
    if is_dry:
        order.status = "acked"
        order.reject_reason = f"dry-run: venue adapter not called ({_dry_run_reason()})"
        order.submitted_at = now_utc()
        order.updated_at = now_utc()
        db.add(order)
        db.commit()
        db.refresh(order)
        return order

    adapter = get_adapter(venue)

    try:
        tif = _req_tif(req)
        post_only = _req_post_only(req)

        # Server-side live validation (do not rely only on UI)
        norm_qty, norm_px, errs = _validate_and_normalize_live_order(
            adapter=adapter,
            symbol_venue=symbol_venue,
            side=req.side,
            type_=req.type,
            qty=req.qty,
            limit_price=order.limit_price,
            tif=tif,
            post_only=post_only,
        )

        if errs:
            order.status = "rejected"
            order.reject_reason = "pretrade_check_failed: " + " | ".join(errs)
            order.updated_at = now_utc()
            db.add(order)
            db.commit()
            db.refresh(order)
            return order

        # Apply normalized values (flooring to increments)
        order.qty = float(norm_qty) if norm_qty is not None else order.qty
        if req.type == "limit":
            order.limit_price = float(norm_px) if norm_px is not None else order.limit_price

        placed = _place_order_safely(
            adapter,
            symbol_venue=symbol_venue,
            side=req.side,
            type_=req.type,
            qty=order.qty,
            limit_price=order.limit_price,
            client_order_id=client_oid,
            dry_run=is_dry,  # False here, but keep explicit for adapter contract
            tif=tif,
            post_only=post_only,
        )

        vo = placed.get("venue_order_id") if isinstance(placed, dict) else None
        order.venue_order_id = str(vo).strip() if vo else None

        order.status = (placed.get("status", "acked") if isinstance(placed, dict) else "acked") or "acked"
        order.reject_reason = None
        order.submitted_at = now_utc()
        order.updated_at = now_utc()

    except Exception as e:
        order.status = "rejected"
        order.reject_reason = str(e)
        order.updated_at = now_utc()

    db.add(order)
    db.commit()
    db.refresh(order)
    return order


def _parse_cancel_ref(cancel_ref: str) -> tuple[str, Optional[str], str]:
    """
    Supported:
      - LOCAL:<order_id>
      - VENUE:<venue>:<venue_order_id>
      - <venue>:<venue_order_id>                  (shorthand; e.g., robinhood:<id>, kraken:<id>)
    """
    s = (cancel_ref or "").strip()
    if not s:
        raise ValueError("cancel_ref is required")

    parts = s.split(":")
    head = parts[0].strip().upper()

    if head == "LOCAL":
        if len(parts) != 2 or not parts[1].strip():
            raise ValueError("cancel_ref LOCAL form must be 'LOCAL:<order_id>'")
        return "LOCAL", None, parts[1].strip()

    if head == "VENUE":
        if len(parts) < 3:
            raise ValueError("cancel_ref VENUE form must be 'VENUE:<venue>:<venue_order_id>'")
        venue = parts[1].strip().lower()
        oid = ":".join(parts[2:]).strip()
        if not venue or not oid:
            raise ValueError("cancel_ref VENUE form must be 'VENUE:<venue>:<venue_order_id>'")
        return "VENUE", venue, oid

    # Shorthand venue form: "<venue>:<venue_order_id>"
    # This is what your Robinhood adapter now emits: "robinhood:<id>"
    if len(parts) >= 2:
        venue = parts[0].strip().lower()
        oid = ":".join(parts[1:]).strip()
        if venue and oid:
            return "VENUE", venue, oid

    raise ValueError("cancel_ref must be 'LOCAL:<id>' or 'VENUE:<venue>:<id>' or '<venue>:<id>'")


def cancel_order(db: Session, order_id: str) -> Order:
    """
    Cancel a LOCAL Order row and (when possible) cancel at the venue.

    Correctness rules:
      - If dry-run (or not armed): do not call venue; mark local row canceled (simulated).
      - In LIVE mode: do NOT mark local row canceled unless the venue confirms.
      - If venue_order_id missing in LIVE mode: keep status as-is, record reject_reason.
      - If cancel succeeds: also mark *all* matching rows for (venue, venue_order_id) as canceled,
        including venue_orders snapshot rows. This prevents the same order from showing in both
        Open and Terminal buckets.
    """
    order = db.get(Order, order_id)
    if not order:
        raise KeyError("Order not found")

    st = (order.status or "").strip().lower()
    if st in _TERMINAL:
        return order

    is_dry = _effective_dry_run()
    now = now_utc()

    # In dry-run, simulate cancel deterministically.
    if is_dry:
        order.status = "canceled"
        order.reject_reason = None
        order.updated_at = now
        if getattr(order, "closed_at", None) is None:
            order.closed_at = now
        db.add(order)
        db.commit()
        db.refresh(order)
        return order

    # LIVE mode: if we cannot identify the venue order id, we must not claim canceled.
    if not order.venue_order_id:
        order.reject_reason = "cancel failed: missing venue_order_id (cannot cancel at venue)"
        order.updated_at = now
        db.add(order)
        db.commit()
        db.refresh(order)
        return order

    adapter = get_adapter((order.venue or "").strip().lower())

    try:
        ok = _cancel_order_safely(adapter, str(order.venue_order_id), dry_run=is_dry)
    except Exception as e:
        # ✅ Surgical change: treat "already not open" as success
        if _is_already_closed_cancel_error(str(e)):
            ok = True
        else:
            order.reject_reason = f"cancel failed: {e}"
            order.updated_at = now_utc()
            db.add(order)
            db.commit()
            db.refresh(order)
            return order

    if ok:
        order.status = "canceled"
        order.reject_reason = None
        order.updated_at = now
        if getattr(order, "closed_at", None) is None:
            order.closed_at = now

        v = (order.venue or "").strip().lower()
        vo = str(order.venue_order_id)

        # Update ALL order rows for this same venue_order_id (prevents duplicates across buckets)
        try:
            matches = db.execute(
                select(Order).where(func.lower(Order.venue) == v).where(Order.venue_order_id == vo)
            ).scalars().all()
            for r in matches:
                if (r.status or "").strip().lower() not in _TERMINAL:
                    r.status = "canceled"
                r.reject_reason = None
                r.updated_at = now
                if getattr(r, "closed_at", None) is None:
                    r.closed_at = now
                db.add(r)
        except Exception:
            # best-effort; local order already marked canceled
            pass

        # Update ALL venue_orders snapshot rows too (if you keep multiple snapshots per order)
        try:
            snaps = db.execute(
                select(VenueOrderRow)
                .where(func.lower(VenueOrderRow.venue) == v)
                .where(VenueOrderRow.venue_order_id == vo)
            ).scalars().all()
            for s in snaps:
                s.status = "canceled"
                s.updated_at = now
                if getattr(s, "closed_at", None) is None:
                    s.closed_at = now
                db.add(s)
        except Exception:
            pass

    else:
        # Do not lie: leave status unchanged, but record the failure.
        order.reject_reason = "cancel rejected or not confirmed by venue"
        order.updated_at = now

    db.add(order)
    db.commit()
    db.refresh(order)
    return order


def cancel_by_ref(db: Session, cancel_ref: str) -> dict:
    """
    Cancels either:
      - a LOCAL Order (LOCAL:<order_id>)
      - a VENUE order snapshot row (VENUE:<venue>:<venue_order_id>) OR shorthand (<venue>:<venue_order_id>)

    Correctness rules:
      - In LIVE mode, do NOT mark the snapshot row canceled unless the adapter cancel returns ok=True.
      - Never allow adapter exceptions to bubble to the router; return ok=False + error text instead.
      - In DRY mode, simulate cancel by updating snapshot status only (best-effort).
    """
    kind, venue, key = _parse_cancel_ref(cancel_ref)
    is_dry = _effective_dry_run()

    if kind == "LOCAL":
        o = cancel_order(db, key)
        return {
            "kind": "LOCAL",
            "ok": (str(o.status or "").lower() == "canceled"),
            "id": o.id,
            "status": o.status,
            "venue": o.venue,
            "venue_order_id": o.venue_order_id,
            "reject_reason": o.reject_reason,
            "updated_at": o.updated_at,
        }

    # VENUE
    assert venue is not None
    venue = venue.strip().lower()
    venue_order_id = key

    ok = True
    error: Optional[str] = None
    simulated = False

    if is_dry:
        simulated = True
        ok = True
    else:
        try:
            adapter = get_adapter(venue)
            ok = bool(_cancel_order_safely(adapter, venue_order_id, dry_run=is_dry))
        except Exception as e:
            # ✅ Surgical change: treat "already not open" as success
            if _is_already_closed_cancel_error(str(e)):
                ok = True
                error = None
            else:
                ok = False
                error = str(e)

    row = db.execute(
        select(VenueOrderRow)
        .where(VenueOrderRow.venue == venue)
        .where(VenueOrderRow.venue_order_id == venue_order_id)
        .order_by(desc(VenueOrderRow.captured_at))
        .limit(1)
    ).scalars().first()

    did_update_snapshot = False
    snapshot_rows_updated = 0
    orders_rows_updated = 0
    if simulated or ok:
        now = now_utc()

        # IMPORTANT: venue_orders is a *snapshot* table (multiple rows per venue_order_id).
        # If we only update the latest snapshot row, older "open" snapshots can still leak into
        # the UI (showing the same order in both Open + Terminal buckets). Update them all.
        all_snapshots = db.execute(
            select(VenueOrderRow)
            .where(VenueOrderRow.venue == venue)
            .where(VenueOrderRow.venue_order_id == venue_order_id)
        ).scalars().all()

        for s in all_snapshots:
            if getattr(s, "status", None) != "canceled":
                s.status = "canceled"
                snapshot_rows_updated += 1
            s.updated_at = now
            if hasattr(s, "closed_at") and getattr(s, "closed_at", None) is None:
                s.closed_at = now
            db.add(s)

        if all_snapshots:
            did_update_snapshot = True

        # Also update any unified Order rows that match this venue_order_id (dedupe safety net).
        order_rows = db.execute(
            select(Order)
            .where(Order.venue == venue)
            .where(Order.venue_order_id == venue_order_id)
        ).scalars().all()

        for o in order_rows:
            if getattr(o, "status", None) != "canceled":
                o.status = "canceled"
                orders_rows_updated += 1
            if hasattr(o, "updated_at"):
                o.updated_at = now
            if hasattr(o, "closed_at") and getattr(o, "closed_at", None) is None:
                o.closed_at = now
            db.add(o)

        db.commit()
        if row:
            db.refresh(row)

    if row:
        return {
            "kind": "VENUE",
            "ok": bool(ok),
            "simulated": bool(simulated),
            "venue": row.venue,
            "venue_order_id": row.venue_order_id,
            "status": row.status,
            "captured_at": row.captured_at,
            "updated_at": row.updated_at,
            "snapshot_updated": bool(did_update_snapshot),
            "snapshot_rows_updated": int(snapshot_rows_updated),
            "orders_rows_updated": int(orders_rows_updated),
            "error": error,
        }

    return {
        "kind": "VENUE",
        "ok": bool(ok),
        "simulated": bool(simulated),
        "venue": venue,
        "venue_order_id": venue_order_id,
        "status": ("canceled" if (simulated or ok) else "unknown"),
        "snapshot_updated": False,
        "note": "no VenueOrderRow found to update",
        "error": error,
    }


def cancel_all(db: Session, venue: str, symbol_canon: Optional[str]) -> int:
    v = (venue or "").strip().lower()
    stmt = (
        select(Order)
        .where(Order.venue == v)
        .where(Order.status.in_(("new", "routed", "acked", "partial", "open")))
    )
    if symbol_canon:
        stmt = stmt.where(Order.symbol_canon == symbol_canon)

    orders = db.execute(stmt).scalars().all()
    n = 0
    for o in orders:
        try:
            before = (o.status or "").lower().strip()
            cancel_order(db, o.id)
            after = (db.get(Order, o.id).status or "").lower().strip()
            if before != after and after == "canceled":
                n += 1
        except Exception:
            pass
    return n


def list_orders(
    db: Session,
    venue: Optional[str],
    source_name: Optional[str],  # accepted by router, but not stored in DB schema; ignored
    symbol: Optional[str],
    status: Optional[str],
    side: Optional[str],
    type_: Optional[str],
    viewed_confirmed: Optional[bool],  # accepted by router, but not stored in DB schema; ignored
    dt_from: Optional[datetime],
    dt_to: Optional[datetime],
    sort: Optional[str],
    page: int,
    page_size: int
) -> Tuple[list[Order], int]:
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)

    stmt = select(Order)
    count_stmt = select(func.count()).select_from(Order)

    def apply_filters(s):
        if venue:
            s = s.where(Order.venue == venue.strip().lower())
        if symbol:
            s = s.where(Order.symbol_canon == symbol)
        if status:
            s = s.where(Order.status == status)
        if side:
            s = s.where(Order.side == side)
        if type_:
            s = s.where(Order.type == type_)
        if dt_from:
            s = s.where(Order.created_at >= dt_from)
        if dt_to:
            s = s.where(Order.created_at <= dt_to)
        return s

    stmt = apply_filters(stmt)
    count_stmt = apply_filters(count_stmt)

    total = db.execute(count_stmt).scalar_one()

    field, direction = parse_sort(sort, _ALLOWED_SORT, default=("created_at", "desc"))
    col = getattr(Order, field)
    stmt = stmt.order_by(desc(col) if direction == "desc" else asc(col))

    stmt = stmt.offset((page - 1) * page_size).limit(page_size)
    items = db.execute(stmt).scalars().all()
    return items, int(total)
