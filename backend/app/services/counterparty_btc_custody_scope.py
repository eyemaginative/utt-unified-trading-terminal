from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..models import BasisLot, WalletAddress


_NATIVE_BITCOIN_NETWORKS = {
    "bitcoin",
    "btc",
    "mainnet",
    "bitcoin_mainnet",
}
_NATIVE_BITCOIN_ASSETS = {
    "BTC",
    "ALL",
    "*",
    "WALLET",
}
_COUNTERPARTY_WALLET_IDS = {
    "counterparty",
    "counterparty_default",
    "counterparty_unisat",
}
_USER_OWNER_SCOPES = {
    "",
    "user",
    "self",
    "self_custody",
    "personal",
    "default",
}


def _norm(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _asset(value: Any) -> str:
    return str(value or "").strip().upper()


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _created_rank(row: Any) -> float:
    value = getattr(row, "created_at", None)
    if isinstance(value, datetime):
        try:
            return float(value.timestamp())
        except Exception:
            return 0.0
    return 0.0


def _owner_compatible(candidate: Any, expected_owner_scope: Any) -> bool:
    candidate_owner = _norm(getattr(candidate, "owner_scope", None))
    expected_owner = _norm(expected_owner_scope)

    if expected_owner and candidate_owner == expected_owner:
        return True
    return candidate_owner in _USER_OWNER_SCOPES and expected_owner in _USER_OWNER_SCOPES


def _is_native_bitcoin_candidate(
    row: Any,
    *,
    address: str,
    expected_owner_scope: Any,
    counterparty_wallet_address_id: Optional[str],
) -> bool:
    row_id = str(getattr(row, "id", "") or "")
    if counterparty_wallet_address_id and row_id == str(counterparty_wallet_address_id):
        return False

    if str(getattr(row, "address", "") or "").strip() != address:
        return False
    if not _owner_compatible(row, expected_owner_scope):
        return False
    if _norm(getattr(row, "network", None)) not in _NATIVE_BITCOIN_NETWORKS:
        return False
    if _asset(getattr(row, "asset", None)) not in _NATIVE_BITCOIN_ASSETS:
        return False

    wallet_id = _norm(getattr(row, "wallet_id", None))
    if wallet_id in _COUNTERPARTY_WALLET_IDS:
        return False
    return True


def _basis_scope_for_wallet_address(row: Any) -> Tuple[str, str]:
    venue = _norm(getattr(row, "wallet_id", None)) or "self_custody"
    return venue, "wallet_address"


def _candidate_summary(row: Any) -> Dict[str, Any]:
    venue, lot_wallet_id = _basis_scope_for_wallet_address(row)
    return {
        "wallet_address_id": str(getattr(row, "id", "") or "") or None,
        "wallet_id": str(getattr(row, "wallet_id", "") or "").strip() or None,
        "network": str(getattr(row, "network", "") or "").strip() or None,
        "asset_scope": _asset(getattr(row, "asset", None)) or None,
        "address": str(getattr(row, "address", "") or "").strip() or None,
        "label": str(getattr(row, "label", "") or "").strip() or None,
        "owner_scope": str(getattr(row, "owner_scope", "") or "").strip() or None,
        "created_at": getattr(row, "created_at", None),
        "basis_venue": venue,
        "basis_wallet_id": lot_wallet_id,
        "scope_key": f"{venue}:{lot_wallet_id}:BTC",
    }


def _resolve_native_candidates(rows: Sequence[Any]) -> Dict[str, Any]:
    ordered = sorted(
        list(rows or []),
        key=lambda row: (
            0 if _asset(getattr(row, "asset", None)) == "BTC" else 1,
            0 if _norm(getattr(row, "network", None)) in {"bitcoin", "btc"} else 1,
            -_created_rank(row),
            str(getattr(row, "id", "") or ""),
        ),
    )
    summaries = [_candidate_summary(row) for row in ordered]

    by_scope: Dict[str, List[Dict[str, Any]]] = {}
    for summary in summaries:
        by_scope.setdefault(str(summary["scope_key"]), []).append(summary)

    if not summaries:
        return {
            "status": "native_bitcoin_wallet_row_required",
            "resolved": False,
            "candidate_count": 0,
            "distinct_scope_count": 0,
            "candidates": [],
            "selected": None,
        }

    if len(by_scope) > 1:
        return {
            "status": "native_bitcoin_wallet_scope_ambiguous",
            "resolved": False,
            "candidate_count": len(summaries),
            "distinct_scope_count": len(by_scope),
            "candidates": summaries,
            "selected": None,
        }

    selected = next(iter(by_scope.values()))[0]
    return {
        "status": "resolved",
        "resolved": True,
        "candidate_count": len(summaries),
        "distinct_scope_count": 1,
        "candidates": summaries,
        "selected": selected,
    }


def _btc_lot_scope_rows(db: Session) -> List[Dict[str, Any]]:
    rows = list(
        db.execute(
            select(BasisLot)
            .where(func.upper(BasisLot.asset) == "BTC")
            .order_by(
                BasisLot.venue.asc(),
                BasisLot.wallet_id.asc(),
                BasisLot.acquired_at.asc(),
                BasisLot.created_at.asc(),
                BasisLot.id.asc(),
            )
        ).scalars().all()
    )

    grouped: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for lot in rows:
        venue = _norm(getattr(lot, "venue", None))
        wallet_id = str(getattr(lot, "wallet_id", "") or "default").strip() or "default"
        key = (venue, wallet_id)
        item = grouped.setdefault(
            key,
            {
                "venue": venue,
                "wallet_id": wallet_id,
                "asset": "BTC",
                "lot_count": 0,
                "positive_lot_count": 0,
                "qty_total": 0.0,
                "qty_remaining": 0.0,
                "basis_missing_lot_count": 0,
                "basis_known_lot_count": 0,
                "basis_total_usd_known": 0.0,
                "origin_types": set(),
                "basis_sources": set(),
                "earliest_acquired_at": None,
                "latest_acquired_at": None,
            },
        )

        qty_total = _safe_float(getattr(lot, "qty_total", None))
        qty_remaining = max(_safe_float(getattr(lot, "qty_remaining", None)), 0.0)
        basis_missing = bool(getattr(lot, "basis_is_missing", False)) or getattr(lot, "total_basis_usd", None) is None
        basis_value = _safe_float(getattr(lot, "total_basis_usd", None))
        acquired_at = getattr(lot, "acquired_at", None)

        item["lot_count"] += 1
        item["qty_total"] += qty_total
        item["qty_remaining"] += qty_remaining
        if qty_remaining > 0:
            item["positive_lot_count"] += 1
        if basis_missing:
            item["basis_missing_lot_count"] += 1
        else:
            item["basis_known_lot_count"] += 1
            item["basis_total_usd_known"] += basis_value

        origin_type = str(getattr(lot, "origin_type", "") or "").strip()
        basis_source = str(getattr(lot, "basis_source", "") or "").strip()
        if origin_type:
            item["origin_types"].add(origin_type)
        if basis_source:
            item["basis_sources"].add(basis_source)

        if isinstance(acquired_at, datetime):
            if item["earliest_acquired_at"] is None or acquired_at < item["earliest_acquired_at"]:
                item["earliest_acquired_at"] = acquired_at
            if item["latest_acquired_at"] is None or acquired_at > item["latest_acquired_at"]:
                item["latest_acquired_at"] = acquired_at

    out: List[Dict[str, Any]] = []
    for item in grouped.values():
        out.append(
            {
                **item,
                "origin_types": sorted(item["origin_types"]),
                "basis_sources": sorted(item["basis_sources"]),
                "earliest_acquired_at": (
                    item["earliest_acquired_at"].isoformat()
                    if isinstance(item["earliest_acquired_at"], datetime)
                    else None
                ),
                "latest_acquired_at": (
                    item["latest_acquired_at"].isoformat()
                    if isinstance(item["latest_acquired_at"], datetime)
                    else None
                ),
            }
        )
    out.sort(key=lambda row: (str(row["venue"]), str(row["wallet_id"])))
    return out


def _scope_inventory(
    lot_scopes: Iterable[Dict[str, Any]],
    *,
    venue: Optional[str],
    wallet_id: Optional[str],
) -> Dict[str, Any]:
    if not venue or not wallet_id:
        return {
            "venue": venue,
            "wallet_id": wallet_id,
            "lot_count": 0,
            "positive_lot_count": 0,
            "qty_total": 0.0,
            "qty_remaining": 0.0,
            "basis_missing_lot_count": 0,
            "basis_known_lot_count": 0,
            "basis_total_usd_known": 0.0,
        }

    venue_norm = _norm(venue)
    wallet_norm = str(wallet_id or "").strip()
    for row in lot_scopes:
        if _norm(row.get("venue")) == venue_norm and str(row.get("wallet_id") or "").strip() == wallet_norm:
            return dict(row)

    return {
        "venue": venue_norm,
        "wallet_id": wallet_norm,
        "lot_count": 0,
        "positive_lot_count": 0,
        "qty_total": 0.0,
        "qty_remaining": 0.0,
        "basis_missing_lot_count": 0,
        "basis_known_lot_count": 0,
        "basis_total_usd_known": 0.0,
    }


def resolve_counterparty_btc_custody_scope(
    *,
    db: Session,
    source_address: Dict[str, Any],
    required_btc: float,
    acquired_asset: str,
    counterparty_venue: str = "counterparty",
    counterparty_wallet_id: str = "counterparty",
) -> Dict[str, Any]:
    """Resolve the native Bitcoin wallet-address FIFO scope without mutations.

    Wallet-address transaction ingestion stores native self-custody accounting as:
      venue    = WalletAddress.wallet_id or "self_custody"
      wallet_id = "wallet_address"

    The Counterparty ALL row identifies the protocol account but is not accepted
    as native BTC custody inventory. A separate native Bitcoin WalletAddress row
    for the same address is required before BTC FIFO can be proposed.
    """
    address = str(source_address.get("address") or "").strip()
    owner_scope = source_address.get("owner_scope")
    counterparty_wallet_address_id = (
        str(source_address.get("wallet_address_id") or "").strip() or None
    )
    required_qty = max(_safe_float(required_btc), 0.0)

    all_same_address: List[Any] = []
    if address:
        all_same_address = list(
            db.execute(
                select(WalletAddress)
                .where(WalletAddress.address == address)
                .order_by(WalletAddress.created_at.desc(), WalletAddress.id.asc())
            ).scalars().all()
        )

    native_candidates = [
        row
        for row in all_same_address
        if _is_native_bitcoin_candidate(
            row,
            address=address,
            expected_owner_scope=owner_scope,
            counterparty_wallet_address_id=counterparty_wallet_address_id,
        )
    ]
    resolution = _resolve_native_candidates(native_candidates)
    selected = resolution.get("selected") if resolution.get("resolved") else None

    lot_scopes = _btc_lot_scope_rows(db)

    selected_venue = selected.get("basis_venue") if isinstance(selected, dict) else None
    selected_wallet_id = selected.get("basis_wallet_id") if isinstance(selected, dict) else None
    selected_inventory = _scope_inventory(
        lot_scopes,
        venue=selected_venue,
        wallet_id=selected_wallet_id,
    )
    protocol_inventory = _scope_inventory(
        lot_scopes,
        venue=counterparty_venue,
        wallet_id=counterparty_wallet_id,
    )

    available_qty = _safe_float(selected_inventory.get("qty_remaining"))
    shortfall_qty = max(required_qty - available_qty, 0.0)
    inventory_sufficient = bool(resolution.get("resolved")) and shortfall_qty <= 1e-18

    selected_scope_key = (
        f"{_norm(selected_venue)}:{str(selected_wallet_id or '').strip()}:BTC"
        if selected_venue and selected_wallet_id
        else None
    )
    protocol_scope_key = (
        f"{_norm(counterparty_venue)}:{str(counterparty_wallet_id or '').strip()}:BTC"
    )
    duplicate_inventory_risk = (
        selected_scope_key is not None
        and selected_scope_key != protocol_scope_key
        and _safe_float(selected_inventory.get("qty_remaining")) > 0
        and _safe_float(protocol_inventory.get("qty_remaining")) > 0
    )

    other_positive_scopes = [
        row
        for row in lot_scopes
        if _safe_float(row.get("qty_remaining")) > 0
        and (
            selected_scope_key is None
            or f"{_norm(row.get('venue'))}:{str(row.get('wallet_id') or '').strip()}:BTC"
            != selected_scope_key
        )
    ]

    blockers: List[str] = []
    warnings: List[str] = []

    if resolution.get("status") == "native_bitcoin_wallet_row_required":
        blockers.append("underlying_btc_wallet_scope_unresolved")
        warnings.append(
            "Add a separate Wallet Addresses row with Asset BTC, Network bitcoin, "
            "the same address, and the intended custody grouping. The Counterparty "
            "ALL row is protocol metadata and is not native BTC FIFO inventory."
        )
    elif resolution.get("status") == "native_bitcoin_wallet_scope_ambiguous":
        blockers.append("underlying_btc_wallet_scope_ambiguous")
        warnings.append(
            "Multiple native Bitcoin Wallet Addresses rows resolve to different FIFO scopes. "
            "Keep one authoritative custody grouping before BTC disposition review."
        )
    elif not inventory_sufficient:
        blockers.append("underlying_btc_fifo_inventory_missing")

    if _safe_float(protocol_inventory.get("qty_remaining")) > 0:
        warnings.append(
            "BTC lots exist inside the Counterparty protocol scope. Review them before "
            "enabling persistence so native Bitcoin inventory is not duplicated."
        )
    if duplicate_inventory_risk:
        blockers.append("duplicate_btc_inventory_scope_review_required")
    if other_positive_scopes:
        warnings.append(
            "Other positive BTC lot scopes exist but are excluded by strict wallet-address scope isolation."
        )

    counterparty_account = {
        "wallet_address_id": counterparty_wallet_address_id,
        "wallet_id": source_address.get("wallet_id") or counterparty_wallet_id,
        "network": source_address.get("network"),
        "asset_scope": source_address.get("asset_scope"),
        "address": address or None,
        "owner_scope": owner_scope,
        "address_source": source_address.get("address_source"),
        "role": "counterparty_protocol_account",
    }
    acquisition_scope = {
        "venue": _norm(counterparty_venue),
        "wallet_id": str(counterparty_wallet_id or "counterparty").strip() or "counterparty",
        "asset": _asset(acquired_asset),
        "address": address or None,
        "role": "counterparty_asset_custody",
    }
    disposition_scope = {
        "resolved": bool(resolution.get("resolved")),
        "venue": selected_venue,
        "wallet_id": selected_wallet_id,
        "asset": "BTC",
        "address": address or None,
        "wallet_address_id": selected.get("wallet_address_id") if isinstance(selected, dict) else None,
        "label": selected.get("label") if isinstance(selected, dict) else None,
        "network": selected.get("network") if isinstance(selected, dict) else None,
        "asset_scope": selected.get("asset_scope") if isinstance(selected, dict) else None,
        "role": "underlying_bitcoin_custody",
    }

    return {
        "version": "counterparty_btc_custody_scope_v1",
        "status": resolution.get("status"),
        "resolved": bool(resolution.get("resolved")),
        "read_only": True,
        "database_mutation": False,
        "lot_mutation": False,
        "fifo_mutation": False,
        "basis_mutation": False,
        "universal_pooling": False,
        "scope_policy": "underlying_bitcoin_wallet_address_scope_only",
        "wallet_address_ingest_policy": {
            "venue": "WalletAddress.wallet_id or self_custody",
            "wallet_id": "wallet_address",
        },
        "counterparty_account": counterparty_account,
        "native_bitcoin_candidate_count": int(resolution.get("candidate_count") or 0),
        "native_bitcoin_distinct_scope_count": int(resolution.get("distinct_scope_count") or 0),
        "native_bitcoin_candidates": list(resolution.get("candidates") or []),
        "selected_native_bitcoin_wallet": selected,
        "proposed_btc_disposition_scope": disposition_scope,
        "proposed_asset_acquisition_scope": acquisition_scope,
        "required_btc": required_qty,
        "available_btc_lot_qty": available_qty,
        "shortfall_btc": shortfall_qty,
        "inventory_sufficient": inventory_sufficient,
        "selected_scope_inventory": selected_inventory,
        "counterparty_protocol_btc_inventory": protocol_inventory,
        "current_btc_lot_scopes": lot_scopes,
        "other_positive_btc_lot_scopes": other_positive_scopes,
        "duplicate_inventory_risk": duplicate_inventory_risk,
        "blockers": sorted(set(blockers)),
        "warnings": warnings,
    }
