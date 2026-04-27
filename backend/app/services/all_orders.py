from __future__ import annotations

import os
from datetime import datetime
from typing import Optional, Tuple, Any, Dict, List

from sqlalchemy.orm import Session
from sqlalchemy import select, false, func, or_, Table, MetaData

from ..models import Order, VenueOrderRow, OrderView, RuntimeSetting

# NEW (3.5): realized sourcing from lot_journal
from ..models_lot_journal import LotJournal
import re

# Fill may not exist in some envs (defensive import)
try:
    from ..models import Fill  # type: ignore
except Exception:
    Fill = None  # type: ignore

# Design A: scope allow-list
ALLOWED_SCOPES = {"ALL", "LOCAL", "VENUES"}

# Exported allow-list for router-side validation
ALLOWED_SORT_FIELDS = {
    "created_at",
    "updated_at",
    "captured_at",
    "closed_at",
    "venue",
    "status",
    "status_bucket",
    "symbol",
    "symbol_canon",
    "symbol_venue",
    "side",
    "type",
    "qty",
    "filled_qty",
    "limit_price",
    "avg_fill_price",
    "fee",
    "total_after_fee",
    "source",
    "client_order_id",
    "venue_order_id",
    "viewed_confirmed",
    "viewed_at",
    # Note: can_cancel/cancel_ref intentionally not sortable until explicitly added.
}

_TERMINAL = {"filled", "canceled", "cancelled", "rejected", "done", "closed", "expired", "failed"}
_OPENISH = {"new", "open", "routed", "acked", "partial", "live", "pending"}


# ----------------------------
# Swap orders (DEX aggregators)
# ----------------------------


def _swap_orders_table(db: Session):
    """Best-effort reflection of the generic swap_orders table.

    The table is created lazily by routers (e.g., routers/solana_dex.py).
    This reflection must NEVER break existing CEX flows, so failures return None.
    """
    try:
        bind = db.get_bind()
        md = MetaData()
        return Table('swap_orders', md, autoload_with=bind)
    except Exception:
        return None


def _to_unified_swap(mp: dict) -> Dict[str, Any]:
    """Normalize a swap_orders row mapping into the All Orders unified shape.

    Keep this minimal + compatible with downstream enrichment steps.
    """
    ts = mp.get('ts')
    status = (str(mp.get('status') or '').strip().lower() or None)
    side = (str(mp.get('side') or '').strip().lower() or None)

    base_qty = mp.get('base_qty')
    quote_qty = mp.get('quote_qty')
    price = mp.get('price')
    fee = mp.get('fee_quote')

    status_bucket = 'terminal' if status in ('confirmed', 'failed') else _status_bucket(status)

    def _f(x):
        return float(x) if isinstance(x, (int, float)) else None

    return {
        'id': str(mp.get('signature') or ''),
        'source': 'SWAP',
        'venue': str(mp.get('venue') or ''),
        'view_key': _view_key_swap(str(mp.get('venue') or ''), str(mp.get('signature') or mp.get('tx_signature') or mp.get('tx_sig') or '')),
        'symbol': str(mp.get('resolved_symbol') or mp.get('raw_symbol') or ''),
        'symbol_canon': str(mp.get('resolved_symbol') or ''),
        'symbol_venue': str(mp.get('raw_symbol') or ''),
        'side': side,
        'type': 'swap',
        'status': status,
        'status_bucket': status_bucket,
        'qty': _f(base_qty),
        'filled_qty': _f(base_qty),
        'limit_price': None,
        'avg_fill_price': _f(price),
        'fee': _f(fee),
        'total_after_fee': (_f(quote_qty) - _f(fee)) if (side == 'sell' and _f(quote_qty) is not None and _f(fee) is not None) else (_f(quote_qty) if side == 'sell' else None),
        'client_order_id': None,
        'venue_order_id': str(mp.get('signature') or ''),
        'can_cancel': False,
        'cancel_ref': None,
        'created_at': ts,
        'updated_at': ts,
        'captured_at': ts,
        'closed_at': ts if status_bucket == 'terminal' else None,
        'viewed_confirmed': False,
        'viewed_at': None,
        # extra swap context (harmless to UI consumers that ignore)
        'swap_chain': mp.get('chain'),
        'swap_wallet': mp.get('wallet_address'),
        'swap_base_mint': mp.get('base_mint'),
        'swap_quote_mint': mp.get('quote_mint'),
        'swap_quote_qty': _f(quote_qty),
    }


def _norm_venue(v: Any) -> str:
    return str(v or "").strip().lower()


def _status_bucket(s: Optional[str]) -> str:
    """
    Canonical bucket semantics:
      - Terminal => terminal
      - Else => open
    """
    if not s:
        return "open"
    ss = str(s).lower()
    if ss in _TERMINAL:
        return "terminal"
    if ss in _OPENISH:
        return "open"
    return "open"


def _is_terminal(status: Optional[str]) -> bool:
    return bool(status) and str(status).lower() in _TERMINAL


def _closed_at_local(
    status: Optional[str],
    created_at: Optional[datetime],
    updated_at: Optional[datetime],
) -> Tuple[Optional[datetime], bool]:
    """
    Local (Order) semantics:
      - Terminal: prefer updated_at; else fallback to created_at (sortable)
      - Non-terminal: None

    Returns: (closed_at, inferred?)
    """
    if not _is_terminal(status):
        return None, False

    if updated_at:
        return updated_at, False

    if created_at:
        return created_at, True

    return None, True


def _closed_at_venue(
    status: Optional[str],
    created_at: Optional[datetime],
    updated_at: Optional[datetime],
) -> Tuple[Optional[datetime], bool]:
    """
    Venue (VenueOrderRow) semantics:

    Rules:
      - Non-terminal => (None, False)
      - Terminal:
          1) If updated_at exists and is >= created_at (or created_at missing): use updated_at (not inferred)
          2) Else fallback to created_at (inferred)
      - NEVER use captured_at as a fallback for close time.

    Returns: (closed_at, inferred?)
    """
    if not _is_terminal(status):
        return None, False

    if updated_at:
        if created_at:
            try:
                if updated_at >= created_at:
                    return updated_at, False
            except Exception:
                pass
        else:
            return updated_at, False

    if created_at:
        return created_at, True

    return None, True


def _view_key_local(o: Order) -> str:
    return f"LOCAL:{o.id}"


def _view_key_venue(v: VenueOrderRow) -> str:
    oid = v.venue_order_id or v.id
    return f"VENUE:{_norm_venue(v.venue)}:{oid}"



def _view_key_swap(venue: str, signature: str) -> str:
    # Stable key for swap (Solana/Jupiter etc) rows so they can participate in OrderView hydration.
    # Signature is the most stable identifier for swap-derived orders.
    return f"SWAP:{venue}:{signature or ''}"

def _to_unified_local(o: Order) -> Dict[str, Any]:
    closed_at, closed_at_inferred = _closed_at_local(o.status, o.created_at, o.updated_at)
    view_key = _view_key_local(o)
    return {
        "source": "LOCAL",
        "venue": o.venue,
        "id": o.id,
        "venue_order_id": o.venue_order_id,
        "client_order_id": o.client_order_id,
        "symbol": o.symbol_canon,
        "symbol_canon": o.symbol_canon,
        "symbol_venue": o.symbol_venue,
        "side": o.side,
        "type": o.type,
        "status": o.status,
        "status_bucket": _status_bucket(o.status),
        "qty": float(o.qty) if o.qty is not None else None,
        "filled_qty": float(o.filled_qty) if o.filled_qty is not None else None,
        "limit_price": float(o.limit_price) if o.limit_price is not None else None,
        "avg_fill_price": float(o.avg_fill_price) if o.avg_fill_price is not None else None,
        "fee": None,
        "fee_asset": None,
        "total_after_fee": None,
        "reject_reason": o.reject_reason,
        "created_at": o.created_at,
        "updated_at": o.updated_at,
        "captured_at": o.updated_at,
        "closed_at": closed_at,
        "closed_at_inferred": bool(closed_at_inferred),
        "view_key": view_key,
        "viewed_confirmed": False,
        "viewed_at": None,
        # cancelability (filled later, after enrichment/hydration)
        "can_cancel": False,
        "cancel_ref": None,
    }


def _to_unified_venue(v: VenueOrderRow) -> Dict[str, Any]:
    closed_at, closed_at_inferred = _closed_at_venue(v.status, v.created_at, v.updated_at)
    view_key = _view_key_venue(v)
    return {
        "source": v.venue,
        "venue": v.venue,
        "id": v.id,
        "venue_order_id": v.venue_order_id,
        "client_order_id": None,
        "symbol": v.symbol_canon or v.symbol_venue,
        "symbol_canon": v.symbol_canon,
        "symbol_venue": v.symbol_venue,
        "side": v.side,
        "type": v.type,
        "status": v.status,
        "status_bucket": _status_bucket(v.status),
        "qty": float(v.qty) if v.qty is not None else None,
        "filled_qty": float(v.filled_qty) if v.filled_qty is not None else None,
        "limit_price": float(v.limit_price) if v.limit_price is not None else None,
        "avg_fill_price": float(v.avg_fill_price) if v.avg_fill_price is not None else None,
        "fee": float(v.fee) if v.fee is not None else None,
        "fee_asset": v.fee_asset,
        "total_after_fee": float(v.total_after_fee) if v.total_after_fee is not None else None,
        "reject_reason": None,
        "created_at": v.created_at,
        "updated_at": v.updated_at,
        "captured_at": v.captured_at,
        "closed_at": closed_at,
        "closed_at_inferred": bool(closed_at_inferred),
        "view_key": view_key,
        "viewed_confirmed": False,
        "viewed_at": None,
        # cancelability (filled later, after enrichment/hydration)
        "can_cancel": False,
        "cancel_ref": None,
    }


def _normalize_dir(direction: str) -> str:
    d = str(direction or "desc").strip().lower()
    return "asc" if d == "asc" else "desc"


def _normalize_sort_value(val: Any, field: Optional[str] = None) -> Any:
    """Normalize values used for in-memory sorting.

    We combine rows from multiple sources (CEX orders, swap_orders, etc.). Different
    sources can represent time/numeric fields as datetime, epoch float/int, or strings.
    Python cannot compare mixed types (e.g., float vs str) during sort, so we coerce
    into stable sortable primitives.
    """
    if val is None:
        return None

    # Datetime -> epoch seconds (float)
    if isinstance(val, datetime):
        try:
            return float(val.timestamp())
        except Exception:
            return None

    # Numeric -> float
    if isinstance(val, (int, float)):
        try:
            return float(val)
        except Exception:
            return None

    # Strings: attempt time/number coercions, otherwise stable string sort
    if isinstance(val, str):
        s = val.strip()
        if s == "":
            return None

        # If this looks like a time field, try ISO parse -> epoch seconds
        is_time_field = False
        if field:
            f = field.lower()
            is_time_field = f.endswith("_at") or f in ("ts", "time", "timestamp", "created", "closed")
        if is_time_field or ("T" in s and ":" in s and "-" in s):
            try:
                iso = s.replace("Z", "+00:00")
                return float(datetime.fromisoformat(iso).timestamp())
            except Exception:
                pass

        # Pure numeric string -> float
        try:
            # Fast path: allow leading +/- and decimals
            if re.match(r"^[+-]?\d+(?:\.\d+)?$", s):
                return float(s)
        except Exception:
            pass

        return s.lower()

    # Fallback: stringify
    try:
        return str(val).lower()
    except Exception:
        return None


def _missing_rank_for_field(field: str, is_missing: bool, reverse: bool) -> int:
    """
    Controls where missing values land.

    Required behavior (per your UI intent):
      - For closed_at: OPEN orders have closed_at=None and must NOT float to the top.
        Missing values must always sort LAST regardless of direction.
      - For all other fields: missing also sorts LAST.
    """
    if reverse:
        # DESC: present first, missing last
        return 0 if is_missing else 1
    # ASC: present first, missing last
    return 1 if is_missing else 0


def _sort_with_tiebreakers(combined: List[Dict[str, Any]], field: str, direction: str) -> None:
    d = _normalize_dir(direction)
    reverse = d == "desc"
    f = str(field or "").strip()

    def key(it: Dict[str, Any]) -> tuple:
        raw_primary = it.get(f)
        primary_norm = _normalize_sort_value(raw_primary, f)

        is_missing = primary_norm is None or primary_norm == ""
        if is_missing:
            primary_norm = None

        missing_rank = _missing_rank_for_field(f, is_missing, reverse)
        primary = (missing_rank, primary_norm)

        if f in ("closed_at", "created_at", "updated_at", "captured_at"):
            ca = _normalize_sort_value(it.get("created_at"))
            cap = _normalize_sort_value(it.get("captured_at"))
            oid = str(it.get("venue_order_id") or "")
            return (primary, (0 if ca is not None else 1, ca), (0 if cap is not None else 1, cap), oid)

        oid = str(it.get("venue_order_id") or "")
        return (primary, oid)

    combined.sort(key=key, reverse=reverse)


def _chunked(seq, size: int = 500):
    if size <= 0:
        size = 500
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _reconcile_client_order_ids(db: Session, combined: List[Dict[str, Any]]) -> None:
    venue_oids = list({
        str(it.get("venue_order_id"))
        for it in combined
        if it.get("source") != "LOCAL" and it.get("venue_order_id")
    })
    if not venue_oids:
        return

    oid_to_client: Dict[str, str] = {}
    for oid_chunk in _chunked(venue_oids, 500):
        rows = db.execute(
            select(Order.venue_order_id, Order.client_order_id)
            .where(Order.venue_order_id.in_(oid_chunk))
        ).all()
        for oid, cid in rows:
            if oid and cid:
                oid_to_client[str(oid)] = str(cid)

    if not oid_to_client:
        return

    for it in combined:
        if it.get("source") == "LOCAL":
            continue
        oid = it.get("venue_order_id")
        if oid and str(oid) in oid_to_client:
            it["client_order_id"] = oid_to_client[str(oid)]


def _hydrate_views(db: Session, combined: List[Dict[str, Any]]) -> None:
    keys = list({str(it.get("view_key")) for it in combined if it.get("view_key")})
    if not keys:
        return

    m: Dict[str, Dict[str, Any]] = {}
    for key_chunk in _chunked(keys, 500):
        rows = db.execute(
            select(OrderView.view_key, OrderView.viewed_confirmed, OrderView.viewed_at)
            .where(OrderView.view_key.in_(key_chunk))
        ).all()
        for k, vc, va in rows:
            if not k:
                continue
            m[str(k)] = {"viewed_confirmed": bool(vc), "viewed_at": va}

    for it in combined:
        k = it.get("view_key")
        if k and k in m:
            it["viewed_confirmed"] = m[k]["viewed_confirmed"]
            it["viewed_at"] = m[k]["viewed_at"]


def _dedupe_and_enrich_local_with_venue(combined: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Prevent duplicate rows for the same real venue order.

    Rule:
      - If a LOCAL row has venue_order_id, it represents an order we routed.
        Drop the corresponding VENUE row for (venue, venue_order_id).

    Enhancement:
      - Before dropping the VENUE row, enrich the LOCAL unified dict with venue-derived
        economics/status/timestamps when they are strictly better.
    """
    venue_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    local_keys: set[Tuple[str, str]] = set()

    for it in combined:
        oid = it.get("venue_order_id")
        v = _norm_venue(it.get("venue"))
        if not v or not oid:
            continue

        if it.get("source") == "LOCAL":
            local_keys.add((v, str(oid)))
        else:
            venue_by_key[(v, str(oid))] = it

    def enrich(local_it: Dict[str, Any], ven_it: Dict[str, Any]) -> None:
        for k in ("fee", "fee_asset", "total_after_fee"):
            if local_it.get(k) is None and ven_it.get(k) is not None:
                local_it[k] = ven_it.get(k)

        try:
            lv = local_it.get("filled_qty")
            vv = ven_it.get("filled_qty")
            if vv is not None and (
                lv is None
                or (isinstance(lv, (int, float)) and isinstance(vv, (int, float)) and vv > lv)
            ):
                local_it["filled_qty"] = vv
        except Exception:
            pass

        if local_it.get("avg_fill_price") is None and ven_it.get("avg_fill_price") is not None:
            local_it["avg_fill_price"] = ven_it.get("avg_fill_price")

        vs = ven_it.get("status")
        if vs:
            local_it["status"] = vs
            local_it["status_bucket"] = _status_bucket(vs)

        lu = local_it.get("updated_at")
        vu = ven_it.get("updated_at")
        try:
            if isinstance(vu, datetime) and (lu is None or (isinstance(lu, datetime) and vu > lu)):
                local_it["updated_at"] = vu
        except Exception:
            pass

        ca, inferred = _closed_at_local(
            local_it.get("status"),
            local_it.get("created_at"),
            local_it.get("updated_at"),
        )
        local_it["closed_at"] = ca
        local_it["closed_at_inferred"] = bool(inferred)

    for it in combined:
        if it.get("source") != "LOCAL":
            continue
        oid = it.get("venue_order_id")
        v = _norm_venue(it.get("venue"))
        if not v or not oid:
            continue
        ven = venue_by_key.get((v, str(oid)))
        if ven:
            enrich(it, ven)

    out: List[Dict[str, Any]] = []
    for it in combined:
        if it.get("source") == "LOCAL":
            out.append(it)
            continue
        oid = it.get("venue_order_id")
        v = _norm_venue(it.get("venue"))
        if v and oid and (v, str(oid)) in local_keys:
            continue
        out.append(it)

    # Additional dedupe: collapse multiple VENUE snapshot rows that share the same
    # (venue, venue_order_id). Keep a single "best" row so an order cannot show
    # in both Open and Terminal buckets due to multiple historical snapshots.
    deduped: List[Dict[str, Any]] = []
    best_by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def _is_terminal(item: Dict[str, Any]) -> bool:
        st = (item.get("status") or "").lower()
        b = (item.get("bucket") or "").lower()
        return b == "terminal" or st in ("canceled", "cancelled", "filled", "rejected", "expired", "closed", "done")

    def _score(item: Dict[str, Any]) -> Tuple[int, str, str]:
        # Prefer terminal rows, then prefer rows with a closed_at, then updated_at.
        term = 1 if _is_terminal(item) else 0
        closed = item.get("closed_at") or ""
        updated = item.get("updated_at") or ""
        # Use strings (ISO) for ordering; empty sorts low.
        return (term, closed, updated)

    for it in out:
        if it.get("source") == "LOCAL":
            deduped.append(it)
            continue
        v = _norm_venue(it.get("venue"))
        oid = it.get("venue_order_id")
        if not (v and oid):
            deduped.append(it)
            continue
        k = (v, str(oid))
        cur = best_by_key.get(k)
        if cur is None or _score(it) > _score(cur):
            best_by_key[k] = it

    # Append the deduped venue rows in stable order by re-walking out
    seen = set()
    for it in out:
        if it.get("source") == "LOCAL":
            continue
        v = _norm_venue(it.get("venue"))
        oid = it.get("venue_order_id")
        if not (v and oid):
            continue
        k = (v, str(oid))
        if k in seen:
            continue
        seen.add(k)
        deduped.append(best_by_key[k])

    out = deduped


    return out


def _apply_cancelability(combined: List[Dict[str, Any]]) -> None:
    """
    Ensure cancel_ref matches the unified cancel parser conventions.

    Local rows:
      - cancel_ref = "LOCAL:<order_id>"

    Venue rows:
      - cancel_ref = "<venue>:<venue_order_id>"
    """
    for it in combined:
        sb = (it.get("status_bucket") or "").strip().lower()
        is_open = sb == "open"

        if it.get("source") == "LOCAL":
            oid = it.get("id")
            cref = f"LOCAL:{oid}" if oid else None
            it["cancel_ref"] = cref
            it["can_cancel"] = bool(is_open and cref)
            continue

        v = _norm_venue(it.get("venue"))
        vo = it.get("venue_order_id")
        cref = f"{v}:{vo}" if v and vo else None
        it["cancel_ref"] = cref
        it["can_cancel"] = bool(is_open and cref)


# ----------------------------
# NEW (3.5) realized enrichment
# ----------------------------

def _boolish(v: Any, fallback: bool) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v or "").strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    return fallback


def _realized_flag_enabled(db: Session) -> bool:
    env_raw = str(os.getenv("UTT_REALIZED_FIELDS_V1", "") or "").strip()
    env_default = _boolish(env_raw, True) if env_raw else True
    try:
        row = db.execute(
            select(RuntimeSetting).where(RuntimeSetting.key == "realized_fields_enabled")
        ).scalar_one_or_none()
        if row is not None:
            return _boolish(getattr(row, "value_json", None), env_default)
    except Exception:
        pass
    return env_default


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _infer_fee_usd_from_row(it: Dict[str, Any], existing_fee_usd: Optional[float]) -> Optional[float]:
    """
    Fast UI-layer heuristic:
      - If journal didn't populate fee_usd,
      - and unified row has fee numeric,
      - and fee_asset is null/empty,
      - and the symbol is USD/USDC quoted,
    then treat fee as USD/USDC.
    """
    if isinstance(existing_fee_usd, (int, float)):
        return float(existing_fee_usd)

    fee = it.get("fee")
    if not isinstance(fee, (int, float)):
        return existing_fee_usd

    fee_asset = it.get("fee_asset")
    if fee_asset is not None and str(fee_asset).strip() != "":
        return existing_fee_usd

    sym = str(it.get("symbol_canon") or it.get("symbol") or "").strip().upper()
    # Common canonical format in your app: BASE-QUOTE
    if sym.endswith("-USD") or sym.endswith("-USDC"):
        return float(fee)

    return existing_fee_usd


def _compute_net_pnl_usd(
    pnl: Optional[float],
    gross_gain: Optional[float],
    proceeds: Optional[float],
    basis_used: Optional[float],
    fee_usd: Optional[float],
) -> Optional[float]:
    """
    Compute realized pnl net-of-fee if not already provided.

    Priority:
      1) If pnl is provided by impact, treat it as authoritative (already net or intended by writer).
      2) Else derive gross_gain:
         - gross_gain if provided
         - else proceeds - basis_used if both exist
      3) If fee known, net = gross_gain - fee
         Else net = gross_gain (best-available)
    """
    if isinstance(pnl, (int, float)):
        return float(pnl)

    gg: Optional[float] = None
    if isinstance(gross_gain, (int, float)):
        gg = float(gross_gain)
    elif isinstance(proceeds, (int, float)) and isinstance(basis_used, (int, float)):
        gg = float(proceeds) - float(basis_used)

    if gg is None:
        return None

    if isinstance(fee_usd, (int, float)):
        return gg - float(fee_usd)

    return gg


def _extract_realized_from_impact(impact: Any) -> Dict[str, Optional[float] | Optional[str]]:
    """
    Defensive extraction: tolerates versioned impact payloads.

    Expected (typical) sell impact contains one or more of:
      - realized_pnl_usd / pnl_usd
      - realized_proceeds_usd / proceeds_usd
      - realized_basis_used_usd / basis_used_usd / total_basis_moved_usd / basis_consumed_usd
      - realized_fee_usd / fee_usd
      - realized_gain_usd (gross, pre-fee)
      - error
    """
    out: Dict[str, Optional[float] | Optional[str]] = {
        "pnl": None,
        "gross_gain": None,
        "proceeds": None,
        "basis_used": None,
        "fee": None,
        "error": None,
    }
    if not isinstance(impact, dict):
        return out

    err = impact.get("error") or impact.get("reason") or impact.get("realized_error")
    if isinstance(err, str) and err.strip():
        out["error"] = err.strip()

    # Net pnl (if writer ever provides it)
    out["pnl"] = _to_float(impact.get("realized_pnl_usd"))
    if out["pnl"] is None:
        out["pnl"] = _to_float(impact.get("pnl_usd"))

    # Gross gain (pre-fee) in your current sell_fifo_v1 impact
    out["gross_gain"] = _to_float(impact.get("realized_gain_usd"))

    # Proceeds
    out["proceeds"] = _to_float(impact.get("realized_proceeds_usd"))
    if out["proceeds"] is None:
        out["proceeds"] = _to_float(impact.get("proceeds_usd"))

    # Basis used / consumed
    out["basis_used"] = _to_float(impact.get("realized_basis_used_usd"))
    if out["basis_used"] is None:
        out["basis_used"] = _to_float(impact.get("basis_used_usd"))
    if out["basis_used"] is None:
        out["basis_used"] = _to_float(impact.get("basis_consumed_usd"))
    if out["basis_used"] is None:
        out["basis_used"] = _to_float(impact.get("total_basis_moved_usd"))

    # Fee
    out["fee"] = _to_float(impact.get("realized_fee_usd"))
    if out["fee"] is None:
        out["fee"] = _to_float(impact.get("fee_usd"))

    return out


def _seed_realized_fields(it: Dict[str, Any]) -> None:
    it.setdefault("realized_status", None)
    it.setdefault("realized_pnl_usd", None)
    it.setdefault("realized_proceeds_usd", None)
    it.setdefault("realized_basis_used_usd", None)
    it.setdefault("realized_fee_usd", None)
    it.setdefault("realized_error", None)


def _apply_realized_fields(db: Session, combined: List[Dict[str, Any]]) -> None:
    # Always seed keys (even if flag off)
    for it in combined:
        _seed_realized_fields(it)

    if not _realized_flag_enabled(db):
        return

    sell_local_order_ids: List[str] = []
    sell_venue_row_ids: List[str] = []

    # NEW: LOCAL fallback path uses venue_order_id -> VenueOrderRow.id -> VENUE_ORDER_AGG journal
    local_sell_venue_order_ids: List[str] = []

    for it in combined:
        side = str(it.get("side") or "").strip().lower()
        if side != "sell":
            continue

        if it.get("source") == "LOCAL":
            if it.get("id"):
                sell_local_order_ids.append(str(it.get("id")))
            if it.get("venue_order_id"):
                local_sell_venue_order_ids.append(str(it.get("venue_order_id")))
        else:
            if it.get("id"):
                sell_venue_row_ids.append(str(it.get("id")))

    # --- VENUE sells: origin_ref = VenueOrderRow.id ---
    venue_j_by_ref: Dict[str, LotJournal] = {}
    if sell_venue_row_ids:
        rows = db.execute(
            select(LotJournal)
            .where(
                LotJournal.action == "SELL_FIFO_CONSUME",
                LotJournal.origin_type == "VENUE_ORDER_AGG",
                LotJournal.origin_ref.in_(sell_venue_row_ids),
            )
        ).scalars().all()
        for j in rows:
            venue_j_by_ref[str(j.origin_ref)] = j

    # --- LOCAL sells: aggregate journals across Fill rows for the Order ---
    order_to_fill_ids: Dict[str, List[str]] = {}
    fill_j_by_ref: Dict[str, LotJournal] = {}

    if sell_local_order_ids and Fill is not None:
        fill_rows = db.execute(
            select(Fill.id, Fill.order_id)
            .where(Fill.order_id.in_(sell_local_order_ids))
        ).all()
        fill_ids: List[str] = []
        for fid, oid in fill_rows:
            if not fid or not oid:
                continue
            sid = str(fid)
            so = str(oid)
            order_to_fill_ids.setdefault(so, []).append(sid)
            fill_ids.append(sid)

        if fill_ids:
            jrows = db.execute(
                select(LotJournal)
                .where(
                    LotJournal.action == "SELL_FIFO_CONSUME",
                    LotJournal.origin_type == "LOCAL_FILL",
                    LotJournal.origin_ref.in_(fill_ids),
                )
            ).scalars().all()
            for j in jrows:
                fill_j_by_ref[str(j.origin_ref)] = j

    # --- NEW: LOCAL fallback via venue_order_id -> VenueOrderRow.id -> VENUE_ORDER_AGG journal ---
    local_vo_key_to_vrow_id: Dict[Tuple[str, str], str] = {}
    local_vrow_j_by_vrow_id: Dict[str, LotJournal] = {}

    if local_sell_venue_order_ids:
        # Find VenueOrderRow.id for these venue_order_id values
        vrows = db.execute(
            select(VenueOrderRow.id, VenueOrderRow.venue, VenueOrderRow.venue_order_id)
            .where(VenueOrderRow.venue_order_id.in_(local_sell_venue_order_ids))
        ).all()

        vrow_ids: List[str] = []
        for rid, v, vo in vrows:
            if rid is None or not v or not vo:
                continue
            key = (_norm_venue(v), str(vo))
            local_vo_key_to_vrow_id[key] = str(rid)
            vrow_ids.append(str(rid))

        if vrow_ids:
            jrows2 = db.execute(
                select(LotJournal)
                .where(
                    LotJournal.action == "SELL_FIFO_CONSUME",
                    LotJournal.origin_type == "VENUE_ORDER_AGG",
                    LotJournal.origin_ref.in_(vrow_ids),
                )
            ).scalars().all()
            for j in jrows2:
                local_vrow_j_by_vrow_id[str(j.origin_ref)] = j

    # Apply to combined
    for it in combined:
        side = str(it.get("side") or "").strip().lower()
        if side != "sell":
            continue

        if it.get("source") != "LOCAL":
            # VENUE sell row
            rid = str(it.get("id") or "")
            j = venue_j_by_ref.get(rid)

            if not j:
                it["realized_status"] = "no_journal"
                continue

            if not bool(getattr(j, "applied", False)):
                it["realized_status"] = "unapplied"
                imp = getattr(j, "impact", None)
                ex = _extract_realized_from_impact(imp)
                it["realized_error"] = ex.get("error")
                continue

            it["realized_status"] = "applied"
            imp = getattr(j, "impact", None)
            ex = _extract_realized_from_impact(imp)

            proceeds = ex.get("proceeds")
            basis_used = ex.get("basis_used")
            fee_usd = ex.get("fee")
            gross_gain = ex.get("gross_gain")
            pnl = ex.get("pnl")

            fee_usd2 = _infer_fee_usd_from_row(it, fee_usd if isinstance(fee_usd, (int, float)) else None)
            pnl_net = _compute_net_pnl_usd(
                pnl if isinstance(pnl, (int, float)) else None,
                gross_gain if isinstance(gross_gain, (int, float)) else None,
                proceeds if isinstance(proceeds, (int, float)) else None,
                basis_used if isinstance(basis_used, (int, float)) else None,
                fee_usd2,
            )

            it["realized_proceeds_usd"] = (float(proceeds) if isinstance(proceeds, (int, float)) else None)
            it["realized_basis_used_usd"] = (float(basis_used) if isinstance(basis_used, (int, float)) else None)
            it["realized_fee_usd"] = (float(fee_usd2) if isinstance(fee_usd2, (int, float)) else None)
            it["realized_pnl_usd"] = (float(pnl_net) if isinstance(pnl_net, (int, float)) else None)
            it["realized_error"] = ex.get("error")
            continue

        # LOCAL sell: first try aggregating LOCAL_FILL journals (per fill)
        oid = str(it.get("id") or "")
        fids = order_to_fill_ids.get(oid) or []

        journals: List[LotJournal] = []
        for fid in fids:
            j = fill_j_by_ref.get(fid)
            if j:
                journals.append(j)

        if journals:
            any_unapplied = any(not bool(getattr(j, "applied", False)) for j in journals)
            any_applied = any(bool(getattr(j, "applied", False)) for j in journals)

            if any_unapplied and not any_applied:
                it["realized_status"] = "unapplied"
            elif any_unapplied and any_applied:
                it["realized_status"] = "unapplied"
            else:
                it["realized_status"] = "applied"

            proceeds_sum = 0.0
            basis_sum = 0.0
            fee_sum = 0.0
            pnl_sum = 0.0

            proceeds_any = False
            basis_any = False
            fee_any = False
            pnl_any = False

            first_err: Optional[str] = None

            for j in journals:
                imp = getattr(j, "impact", None)
                ex = _extract_realized_from_impact(imp)

                if first_err is None and ex.get("error"):
                    try:
                        first_err = str(ex.get("error"))
                    except Exception:
                        first_err = None

                if bool(getattr(j, "applied", False)):
                    proceeds = ex.get("proceeds")
                    basis_used = ex.get("basis_used")
                    fee_usd = ex.get("fee")
                    gross_gain = ex.get("gross_gain")
                    pnl = ex.get("pnl")

                    # Fee inference: per-row (local unified dict has fee possibly filled via venue enrichment)
                    fee_usd2 = _infer_fee_usd_from_row(it, fee_usd if isinstance(fee_usd, (int, float)) else None)

                    pnl_net = _compute_net_pnl_usd(
                        pnl if isinstance(pnl, (int, float)) else None,
                        gross_gain if isinstance(gross_gain, (int, float)) else None,
                        proceeds if isinstance(proceeds, (int, float)) else None,
                        basis_used if isinstance(basis_used, (int, float)) else None,
                        fee_usd2,
                    )

                    if isinstance(proceeds, (int, float)):
                        proceeds_sum += float(proceeds)
                        proceeds_any = True
                    if isinstance(basis_used, (int, float)):
                        basis_sum += float(basis_used)
                        basis_any = True
                    if isinstance(fee_usd2, (int, float)):
                        fee_sum += float(fee_usd2)
                        fee_any = True
                    if isinstance(pnl_net, (int, float)):
                        pnl_sum += float(pnl_net)
                        pnl_any = True

            it["realized_proceeds_usd"] = (float(proceeds_sum) if proceeds_any else None)
            it["realized_basis_used_usd"] = (float(basis_sum) if basis_any else None)
            it["realized_fee_usd"] = (float(fee_sum) if fee_any else None)
            it["realized_pnl_usd"] = (float(pnl_sum) if pnl_any else None)
            it["realized_error"] = first_err
            continue

        # NEW: fallback to VENUE_ORDER_AGG journal via (venue, venue_order_id)
        v = _norm_venue(it.get("venue"))
        vo = str(it.get("venue_order_id") or "")
        if v and vo:
            vrow_id = local_vo_key_to_vrow_id.get((v, vo))
            if vrow_id:
                j = local_vrow_j_by_vrow_id.get(str(vrow_id))
                if j:
                    if not bool(getattr(j, "applied", False)):
                        it["realized_status"] = "unapplied"
                        ex = _extract_realized_from_impact(getattr(j, "impact", None))
                        it["realized_error"] = ex.get("error")
                        continue

                    it["realized_status"] = "applied"
                    ex = _extract_realized_from_impact(getattr(j, "impact", None))

                    proceeds = ex.get("proceeds")
                    basis_used = ex.get("basis_used")
                    fee_usd = ex.get("fee")
                    gross_gain = ex.get("gross_gain")
                    pnl = ex.get("pnl")

                    fee_usd2 = _infer_fee_usd_from_row(it, fee_usd if isinstance(fee_usd, (int, float)) else None)
                    pnl_net = _compute_net_pnl_usd(
                        pnl if isinstance(pnl, (int, float)) else None,
                        gross_gain if isinstance(gross_gain, (int, float)) else None,
                        proceeds if isinstance(proceeds, (int, float)) else None,
                        basis_used if isinstance(basis_used, (int, float)) else None,
                        fee_usd2,
                    )

                    it["realized_proceeds_usd"] = (float(proceeds) if isinstance(proceeds, (int, float)) else None)
                    it["realized_basis_used_usd"] = (float(basis_used) if isinstance(basis_used, (int, float)) else None)
                    it["realized_fee_usd"] = (float(fee_usd2) if isinstance(fee_usd2, (int, float)) else None)
                    it["realized_pnl_usd"] = (float(pnl_net) if isinstance(pnl_net, (int, float)) else None)
                    it["realized_error"] = ex.get("error")
                    continue

        # If neither fill journals nor venue fallback found anything:
        it["realized_status"] = "no_journal"


# ----------------------------
# NEW (4.x) tax withholding (env-only v1)
# ----------------------------

def _tax_withhold_enabled() -> bool:
    """
    ENV-only toggle so we can make the All Orders Tax / net-a/tx columns meaningful
    without introducing a full settings subsystem yet.

    Enable with:
      UTT_TAX_WITHHOLD_ENABLED=1   (or true/yes/on)

    Rate (0..1):
      UTT_TAX_WITHHOLD_RATE=0.24
    """
    v = str(os.getenv("UTT_TAX_WITHHOLD_ENABLED", "") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _tax_withhold_rate() -> float:
    raw = str(os.getenv("UTT_TAX_WITHHOLD_RATE", "") or "").strip()
    try:
        x = float(raw)
    except Exception:
        x = 0.0
    if not (x >= 0.0):
        x = 0.0
    if x > 1.0:
        x = 1.0
    return x


def _infer_net_usd_for_tax(it: Dict[str, Any]) -> Optional[float]:
    """
    Infer a USD net proceeds figure that matches your UI "Net" semantics for sells.

    Priority (best -> fallback):
      1) total_after_fee if numeric
         - For sells, your data already uses: net = gross - fee
      2) (filled_qty * avg_fill_price) - fee_usd (fee inferred as USD when possible)

    Returns None if we cannot infer a reasonable net proceeds.
    """
    taf = it.get("total_after_fee")
    if isinstance(taf, (int, float)):
        return float(taf)

    fq = it.get("filled_qty")
    ap = it.get("avg_fill_price")
    if not isinstance(fq, (int, float)) or not isinstance(ap, (int, float)):
        # Try qty if filled_qty missing
        q = it.get("qty")
        if not isinstance(q, (int, float)) or not isinstance(ap, (int, float)):
            return None
        fq = q

    gross = float(fq) * float(ap)

    # Fee inference: prefer realized_fee_usd if present; else infer from row fee if USD-quoted
    fee_usd = None
    rf = it.get("realized_fee_usd")
    if isinstance(rf, (int, float)):
        fee_usd = float(rf)
    else:
        fee_usd = _infer_fee_usd_from_row(it, None)

    if isinstance(fee_usd, (int, float)):
        return gross - float(fee_usd)

    return gross


def _apply_tax_withholding_fields(combined: List[Dict[str, Any]]) -> None:
    """
    Emits:
      - tax_withheld_usd
      - tax_usd

    Rule (v1):
      - Only filled/terminal sells get a non-zero value.
      - Everything else is 0.0 so the UI displays a numeric 0 instead of "—".

    This is a *withholding estimate*, not a final tax calculation.
    """
    # Always seed keys so schemas are stable and UI columns don't render as missing.
    for it in combined:
        it["tax_withheld_usd"] = 0.0
        it["tax_usd"] = 0.0

    if not _tax_withhold_enabled():
        return

    rate = _tax_withhold_rate()
    if rate <= 0.0:
        return

    for it in combined:
        side = str(it.get("side") or "").strip().lower()
        if side != "sell":
            continue

        # Only apply to terminal/filled sells (avoid reserving on open orders).
        sb = str(it.get("status_bucket") or "").strip().lower()
        st = str(it.get("status") or "").strip().lower()
        if sb != "terminal" and st != "filled":
            continue

        net_proceeds = _infer_net_usd_for_tax(it)
        if not isinstance(net_proceeds, (int, float)):
            continue

        base = float(net_proceeds)
        if base <= 0.0:
            continue

        tax = base * rate
        it["tax_withheld_usd"] = tax
        it["tax_usd"] = tax


def list_all_orders(
    db: Session,
    source: Optional[str],
    scope: str,
    venue: Optional[str],
    status: Optional[str],
    status_bucket: Optional[str],
    symbol: Optional[str],
    dt_from: Optional[datetime],
    dt_to: Optional[datetime],
    sort_field: str,
    sort_dir: str,
    page: int,
    page_size: int,
) -> Tuple[List[Dict[str, Any]], int]:
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)

    scope_norm = (scope or "ALL").strip().upper()
    if scope_norm not in ALLOWED_SCOPES:
        scope_norm = "ALL"

    sort_dir_norm = _normalize_dir(sort_dir)
    sort_field_norm = str(sort_field or "closed_at").strip()

    local_stmt = select(Order)
    venue_stmt = select(VenueOrderRow)

    if scope_norm == "LOCAL":
        venue_stmt = venue_stmt.where(false())
    elif scope_norm == "VENUES":
        local_stmt = local_stmt.where(false())

    if source:
        s = str(source).strip()
        if s.upper() == "LOCAL":
            local_stmt = select(Order)
            venue_stmt = select(VenueOrderRow).where(false())
        else:
            local_stmt = local_stmt.where(false())
            venue_stmt = select(VenueOrderRow).where(func.lower(VenueOrderRow.venue) == _norm_venue(s))

    if venue:
        vnorm = _norm_venue(venue)
        local_stmt = local_stmt.where(func.lower(Order.venue) == vnorm)
        venue_stmt = venue_stmt.where(func.lower(VenueOrderRow.venue) == vnorm)

    if status_bucket:
        sb = str(status_bucket).strip().lower()
        if sb == "terminal":
            local_stmt = local_stmt.where(func.lower(Order.status).in_(list(_TERMINAL)))
            venue_stmt = venue_stmt.where(func.lower(VenueOrderRow.status).in_(list(_TERMINAL)))
        elif sb == "open":
            local_stmt = local_stmt.where(or_(Order.status.is_(None), ~func.lower(Order.status).in_(list(_TERMINAL))))
            venue_stmt = venue_stmt.where(
                or_(VenueOrderRow.status.is_(None), ~func.lower(VenueOrderRow.status).in_(list(_TERMINAL)))
            )

    def apply_local_filters(s):
        if symbol:
            s = s.where((Order.symbol_canon == symbol) | (Order.symbol_venue == symbol))
        if dt_from:
            s = s.where(Order.created_at >= dt_from)
        if dt_to:
            s = s.where(Order.created_at <= dt_to)
        if status:
            s = s.where(Order.status == status)
        return s

    def apply_venue_filters(s):
        if symbol:
            s = s.where((VenueOrderRow.symbol_canon == symbol) | (VenueOrderRow.symbol_venue == symbol))
        if dt_from:
            s = s.where(VenueOrderRow.created_at >= dt_from)
        if dt_to:
            s = s.where(VenueOrderRow.created_at <= dt_to)
        if status:
            s = s.where(VenueOrderRow.status == status)
        return s

    local_stmt = apply_local_filters(local_stmt)
    venue_stmt = apply_venue_filters(venue_stmt)

    local_items = db.execute(local_stmt).scalars().all()
    venue_items = db.execute(venue_stmt).scalars().all()

    # SWAPS (DEX) — best-effort include if table exists (must not break CEX flows)
    swap_items: List[Dict[str, Any]] = []
    t_swaps = _swap_orders_table(db)
    if t_swaps is not None:
        try:
            include_swaps = scope_norm in ('ALL', 'VENUES')
            if source and str(source).strip().upper() == 'LOCAL':
                include_swaps = False
            if include_swaps:
                sstmt = select(t_swaps)

                # Mirror venue/source filtering semantics
                if source and str(source).strip().upper() != 'LOCAL':
                    sstmt = sstmt.where(func.lower(t_swaps.c.venue) == _norm_venue(source))
                if venue:
                    sstmt = sstmt.where(func.lower(t_swaps.c.venue) == _norm_venue(venue))

                if status_bucket:
                    sb = str(status_bucket).strip().lower()
                    if sb == 'terminal':
                        sstmt = sstmt.where(func.lower(t_swaps.c.status).in_(list(_TERMINAL)))
                    elif sb == 'open':
                        sstmt = sstmt.where(or_(t_swaps.c.status.is_(None), ~func.lower(t_swaps.c.status).in_(list(_TERMINAL))))

                if symbol:
                    # Some venues will only populate raw_symbol; prefer resolved_symbol when present
                    sstmt = sstmt.where((t_swaps.c.resolved_symbol == symbol) | (t_swaps.c.raw_symbol == symbol))
                if dt_from:
                    sstmt = sstmt.where(t_swaps.c.ts >= dt_from)
                if dt_to:
                    sstmt = sstmt.where(t_swaps.c.ts <= dt_to)
                if status:
                    sstmt = sstmt.where(t_swaps.c.status == status)

                for mp in db.execute(sstmt).mappings().all():
                    swap_items.append(_to_unified_swap(dict(mp)))
        except Exception:
            swap_items = []

    combined: List[Dict[str, Any]] = []
    for o in local_items:
        combined.append(_to_unified_local(o))
    for vrow in venue_items:
        combined.append(_to_unified_venue(vrow))

    for sw in swap_items:
        combined.append(sw)

    if status_bucket:
        sb = str(status_bucket).lower().strip()
        combined = [x for x in combined if (x.get("status_bucket") or "").lower() == sb]

    combined = _dedupe_and_enrich_local_with_venue(combined)

    _reconcile_client_order_ids(db, combined)
    _hydrate_views(db, combined)

    _apply_cancelability(combined)

    # NEW: realized enrichment (feature-flagged, but keys always present)
    _apply_realized_fields(db, combined)

    # NEW: tax withholding fields (env-only v1)
    _apply_tax_withholding_fields(combined)

    _sort_with_tiebreakers(combined, sort_field_norm, sort_dir_norm)

    total = len(combined)
    start = (page - 1) * page_size
    end = start + page_size
    return combined[start:end], total
