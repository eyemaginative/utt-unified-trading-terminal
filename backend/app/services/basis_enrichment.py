# backend/app/services/basis_enrichment.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session


BasisKey = Tuple[str, str, str]


@dataclass(frozen=True)
class BasisSummary:
    venue: str
    wallet_id: str
    asset: str
    lot_count: int
    basis_qty_remaining: float
    basis_known_qty_remaining: float
    basis_missing_qty_remaining: float
    basis_missing_lots: int
    cost_basis_usd: Optional[float]
    cost_avg_usd: Optional[float]


def _norm_venue(value: Any) -> str:
    return str(value or "").strip().lower()


def _norm_wallet_id(value: Any) -> str:
    s = str(value or "default").strip()
    return s if s else "default"


def _norm_asset(value: Any) -> str:
    return str(value or "").strip().upper()


def normalize_basis_key(venue: Any, wallet_id: Any, asset: Any) -> BasisKey:
    return (_norm_venue(venue), _norm_wallet_id(wallet_id), _norm_asset(asset))


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        out = float(value)
        if out != out or out in (float("inf"), float("-inf")):
            return None
        return out
    except Exception:
        return None


def _basis_status_for_balance(
    *,
    balance_qty: Any,
    summary: Optional[BasisSummary],
    eps: float = 1e-12,
) -> str:
    qty = _safe_float(balance_qty)
    if qty is not None and abs(qty) <= eps:
        return "basis_not_applicable"

    if summary is None or int(summary.lot_count or 0) <= 0:
        return "basis_missing"

    basis_qty = float(summary.basis_qty_remaining or 0.0)
    known_qty = float(summary.basis_known_qty_remaining or 0.0)
    missing_qty = float(summary.basis_missing_qty_remaining or 0.0)
    missing_lots = int(summary.basis_missing_lots or 0)
    lot_count = int(summary.lot_count or 0)

    # If the current displayed balance exceeds open lot inventory, the row is only partially covered.
    if qty is not None and abs(qty) > max(basis_qty, 0.0) + eps:
        return "basis_partial" if known_qty > eps else "basis_missing"

    if missing_lots >= lot_count or missing_qty > eps:
        return "basis_partial" if known_qty > eps else "basis_missing"

    if summary.cost_basis_usd is None:
        return "basis_missing"

    return "basis_ok"


def build_basis_summary_map(
    db: Session,
    keys: Iterable[BasisKey],
) -> Dict[BasisKey, BasisSummary]:
    """Return read-only remaining-basis summaries keyed by (venue, wallet_id, asset).

    Cost basis is prorated to qty_remaining, not the original lot total:
        remaining_basis = total_basis_usd * qty_remaining / qty_total

    No FIFO state is mutated here.
    """
    normalized = []
    seen = set()
    for venue, wallet_id, asset in keys or []:
        key = normalize_basis_key(venue, wallet_id, asset)
        if not key[0] or not key[1] or not key[2]:
            continue
        if key in seen:
            continue
        seen.add(key)
        normalized.append(key)

    if not normalized:
        return {}

    out: Dict[BasisKey, BasisSummary] = {}

    # SQLite commonly caps bound parameters at 999.  Each key uses 3 params,
    # so keep chunks comfortably below that limit.
    for chunk_start in range(0, len(normalized), 250):
        chunk = normalized[chunk_start:chunk_start + 250]
        params: Dict[str, Any] = {}
        clauses = []
        for idx, (venue, wallet_id, asset) in enumerate(chunk):
            vk = f"v{idx}"
            wk = f"w{idx}"
            ak = f"a{idx}"
            params[vk] = venue
            params[wk] = wallet_id
            params[ak] = asset
            clauses.append(f'(LOWER(venue) = :{vk} AND wallet_id = :{wk} AND UPPER(asset) = :{ak})')

        sql = f"""
            SELECT
                LOWER(venue) AS venue,
                wallet_id AS wallet_id,
                UPPER(asset) AS asset,
                COUNT(*) AS lot_count,
                SUM(CASE WHEN qty_remaining > 0 THEN qty_remaining ELSE 0 END) AS basis_qty_remaining,
                SUM(
                    CASE
                        WHEN qty_remaining > 0
                         AND COALESCE(basis_is_missing, 0) = 0
                         AND total_basis_usd IS NOT NULL
                         AND qty_total > 0
                        THEN qty_remaining
                        ELSE 0
                    END
                ) AS basis_known_qty_remaining,
                SUM(
                    CASE
                        WHEN qty_remaining > 0
                         AND (
                            COALESCE(basis_is_missing, 0) != 0
                            OR total_basis_usd IS NULL
                            OR qty_total <= 0
                         )
                        THEN qty_remaining
                        ELSE 0
                    END
                ) AS basis_missing_qty_remaining,
                SUM(
                    CASE
                        WHEN COALESCE(basis_is_missing, 0) != 0
                          OR total_basis_usd IS NULL
                          OR qty_total <= 0
                        THEN 1
                        ELSE 0
                    END
                ) AS basis_missing_lots,
                SUM(
                    CASE
                        WHEN qty_remaining > 0
                         AND COALESCE(basis_is_missing, 0) = 0
                         AND total_basis_usd IS NOT NULL
                         AND qty_total > 0
                        THEN total_basis_usd * (qty_remaining / qty_total)
                        ELSE 0
                    END
                ) AS cost_basis_usd
            FROM basis_lots
            WHERE qty_remaining > 0
              AND ({' OR '.join(clauses)})
            GROUP BY LOWER(venue), wallet_id, UPPER(asset)
        """

        rows = db.execute(text(sql), params).mappings().all()
        for row in rows:
            key = normalize_basis_key(row.get("venue"), row.get("wallet_id"), row.get("asset"))
            lot_count = int(row.get("lot_count") or 0)
            qty_remaining = float(row.get("basis_qty_remaining") or 0.0)
            known_qty = float(row.get("basis_known_qty_remaining") or 0.0)
            missing_qty = float(row.get("basis_missing_qty_remaining") or 0.0)
            missing_lots = int(row.get("basis_missing_lots") or 0)
            cost_basis = _safe_float(row.get("cost_basis_usd"))

            if cost_basis is not None and known_qty <= 0:
                cost_basis = None

            cost_avg = None
            if cost_basis is not None and known_qty > 0:
                cost_avg = float(cost_basis) / float(known_qty)

            out[key] = BasisSummary(
                venue=key[0],
                wallet_id=key[1],
                asset=key[2],
                lot_count=lot_count,
                basis_qty_remaining=qty_remaining,
                basis_known_qty_remaining=known_qty,
                basis_missing_qty_remaining=missing_qty,
                basis_missing_lots=missing_lots,
                cost_basis_usd=cost_basis,
                cost_avg_usd=cost_avg,
            )

    return out




def basis_fields_for_balance(
    basis_map: Dict[BasisKey, BasisSummary],
    *,
    venue: Any,
    wallet_id: Any = "default",
    asset: Any,
    balance_qty: Any,
) -> Dict[str, Any]:
    key = normalize_basis_key(venue, wallet_id, asset)
    summary = (basis_map or {}).get(key)

    status = _basis_status_for_balance(balance_qty=balance_qty, summary=summary)

    if summary is None:
        return {
            "wallet_id": key[1],
            "cost_basis_usd": None,
            "cost_avg_usd": None,
            "basis_status": status,
            "basis_qty_remaining": None,
            "basis_known_qty_remaining": None,
            "basis_missing_qty_remaining": None,
            "basis_missing_lots": 0,
            "basis_lot_count": 0,
            "basis_unmatched_qty": None,
        }

    qty = _safe_float(balance_qty)
    unmatched = None
    if qty is not None:
        unmatched = max(float(qty) - float(summary.basis_qty_remaining or 0.0), 0.0)

    return {
        "wallet_id": key[1],
        "cost_basis_usd": summary.cost_basis_usd,
        "cost_avg_usd": summary.cost_avg_usd,
        "basis_status": status,
        "basis_qty_remaining": float(summary.basis_qty_remaining or 0.0),
        "basis_known_qty_remaining": float(summary.basis_known_qty_remaining or 0.0),
        "basis_missing_qty_remaining": float(summary.basis_missing_qty_remaining or 0.0),
        "basis_missing_lots": int(summary.basis_missing_lots or 0),
        "basis_lot_count": int(summary.lot_count or 0),
        "basis_unmatched_qty": unmatched,
    }
