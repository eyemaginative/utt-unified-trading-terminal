from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from sqlalchemy import asc, func, or_
from sqlalchemy.orm import Session

from ..models import (
    AssetDeposit,
    AssetWithdrawal,
    BasisLot,
    BridgeTransferRecord,
    WalletAddress,
    WalletAddressTx,
)
from .evm_rpc import validate_evm_address
from .robinhood_chain_history import validate_transaction_hash


_VENUE = "robinhood_chain"
_NETWORK = "robinhood_chain"
_MAX_QUERY_ROWS = 100


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def _iso(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value or "").strip()
    return text or None


def _asset(value: Any) -> str:
    return str(value or "").strip().upper()


def _address(value: Any) -> Optional[str]:
    try:
        return validate_evm_address(str(value or "").strip())
    except ValueError:
        return None


def _row_payload(row: Any, fields: Sequence[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for field in fields:
        value = getattr(row, field, None)
        out[field] = _iso(value) if isinstance(value, datetime) else value
    return out


def _resolve_wallet(
    db: Session,
    *,
    normalized_address: str,
    wallet_address_id: Optional[str],
) -> WalletAddress:
    query = db.query(WalletAddress).filter(WalletAddress.network == _NETWORK)
    if wallet_address_id:
        row = query.filter(WalletAddress.id == str(wallet_address_id).strip()).first()
        if row is None:
            raise ValueError("wallet_address_id does not identify a Robinhood Chain wallet")
        if str(row.address or "").strip().lower() != normalized_address.lower():
            raise ValueError("wallet_address_id does not match the requested address")
        return row

    row = (
        query.filter(func.lower(WalletAddress.address) == normalized_address.lower())
        .order_by(WalletAddress.created_at.desc())
        .first()
    )
    if row is None:
        raise ValueError("requested address is not registered in Wallet Addresses for Robinhood Chain")
    return row


def _owned_wallets(db: Session, *, owner_scope: str) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    rows = (
        db.query(WalletAddress)
        .filter(
            WalletAddress.network == _NETWORK,
            WalletAddress.owner_scope == owner_scope,
        )
        .order_by(WalletAddress.created_at.asc())
        .limit(250)
        .all()
    )
    by_address: Dict[str, Dict[str, Any]] = {}
    payloads: List[Dict[str, Any]] = []
    for row in rows:
        normalized = _address(row.address)
        if not normalized:
            continue
        payload = {
            "id": str(row.id),
            "wallet_id": row.wallet_id,
            "owner_scope": row.owner_scope,
            "asset": row.asset,
            "network": row.network,
            "address": normalized,
            "label": row.label,
        }
        by_address[normalized.lower()] = payload
        payloads.append(payload)
    return by_address, payloads


def _activity_legs(
    items: Iterable[Mapping[str, Any]],
    *,
    owner_address: str,
    owned_by_address: Mapping[str, Mapping[str, Any]],
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    legs: List[Dict[str, Any]] = []
    fee_leg: Optional[Dict[str, Any]] = None
    seen = set()
    owner_lower = owner_address.lower()

    for index, item in enumerate(items or []):
        if not isinstance(item, Mapping):
            continue
        item_id = str(item.get("id") or f"row:{index}")
        if item_id in seen:
            continue
        seen.add(item_id)

        direction = str(item.get("direction") or "other").strip().lower()
        from_address = _address(item.get("from_address"))
        to_address = _address(item.get("to_address"))
        quantity = _safe_float(item.get("amount"))
        amount_atomic = str(item.get("amount_atomic") or "0")
        asset = _asset(item.get("asset"))

        counterparty = None
        if direction == "out":
            counterparty = to_address
        elif direction == "in":
            counterparty = from_address
        elif direction == "self":
            counterparty = to_address or from_address
        counterparty_wallet = owned_by_address.get(str(counterparty or "").lower())

        if quantity is not None and quantity > 0 and asset and direction in {"in", "out", "self"}:
            legs.append({
                "id": item_id,
                "asset": asset,
                "direction": direction,
                "quantity": float(quantity),
                "amount_atomic": amount_atomic,
                "decimals": item.get("decimals"),
                "from_address": from_address,
                "to_address": to_address,
                "counterparty": counterparty,
                "counterparty_owned": bool(counterparty_wallet),
                "counterparty_wallet": dict(counterparty_wallet) if counterparty_wallet else None,
                "contract_address": _address(item.get("contract_address")),
                "registered": bool(item.get("registered")),
                "registry_id": item.get("registry_id"),
                "registry_venue": item.get("registry_venue"),
                "registry_label": item.get("registry_label"),
                "classification": item.get("classification"),
                "method": item.get("method"),
                "timestamp": item.get("timestamp"),
                "source": item.get("source"),
            })

        fee = _safe_float(item.get("fee_eth"))
        sender = str(from_address or "").lower()
        if fee_leg is None and fee is not None and fee > 0 and sender == owner_lower:
            fee_leg = {
                "asset": "ETH",
                "direction": "out",
                "quantity": float(fee),
                "amount_atomic": str(item.get("fee_wei") or "0"),
                "decimals": 18,
                "classification": "network_fee",
                "from_address": owner_address,
                "to_address": None,
                "counterparty": "Robinhood Chain validators / sequencer",
                "registered": True,
            }

    return legs, fee_leg


def _classification(
    items: Sequence[Mapping[str, Any]],
    legs: Sequence[Mapping[str, Any]],
    *,
    owned_by_address: Mapping[str, Mapping[str, Any]],
    fee_leg: Optional[Mapping[str, Any]],
) -> Tuple[str, str, bool, List[str]]:
    classifications = {str(item.get("classification") or "").strip().lower() for item in items}
    statuses = {str(item.get("status") or "").strip().lower() for item in items}
    warnings: List[str] = []

    if "reverted" in classifications:
        return "reverted_no_accounting", "exact", False, ["The provider marks this transaction as reverted; no accounting candidate is produced."]
    if "failed" in classifications or "error" in statuses:
        return "failed_no_accounting", "exact", False, ["The provider marks this transaction as failed; no accounting candidate is produced."]
    if "approval" in classifications:
        return "approval_no_accounting", "exact", False, ["Token approval activity is not a deposit, withdrawal, transfer, or swap record."]

    incoming = [leg for leg in legs if leg.get("direction") == "in"]
    outgoing = [leg for leg in legs if leg.get("direction") == "out"]
    self_legs = [leg for leg in legs if leg.get("direction") == "self"]
    incoming_assets = {_asset(leg.get("asset")) for leg in incoming if _asset(leg.get("asset"))}
    outgoing_assets = {_asset(leg.get("asset")) for leg in outgoing if _asset(leg.get("asset"))}

    if "bridge_candidate" in classifications:
        warnings.append("Bridge treatment is provisional until the destination evidence and ownership path are reviewed.")
        return "bridge_candidate", "provider_candidate", True, warnings

    explicit_swap = "swap_candidate" in classifications
    heuristic_swap = bool(incoming and outgoing and (incoming_assets != outgoing_assets or len(incoming_assets | outgoing_assets) > 1))
    if explicit_swap or heuristic_swap:
        warnings.append("Swap legs are diagnostic. Taxable treatment, proceeds, and fee allocation are not finalized by this preview.")
        return "swap_candidate", "provider_candidate" if explicit_swap else "strong_heuristic", True, warnings

    owned_counterparty = any(bool(leg.get("counterparty_owned")) for leg in [*incoming, *outgoing, *self_legs])
    if owned_counterparty and (incoming or outgoing or self_legs):
        warnings.append("Both addresses appear inside the same Wallet Addresses owner scope; this is a non-taxable transfer candidate, not an automatic conclusion.")
        return "internal_transfer_candidate", "strong", True, warnings

    if incoming and not outgoing:
        warnings.append("Inbound provenance and original basis are unknown unless a matching owned source or prior withdrawal is found.")
        return "deposit_candidate", "strong", True, warnings
    if outgoing and not incoming:
        warnings.append("Outbound activity may be a transfer or disposition; destination ownership must be reviewed before tax treatment.")
        return "withdrawal_candidate", "strong", True, warnings
    if self_legs:
        return "internal_transfer_candidate", "exact", True, ["The transaction sends value from and to the same registered address."]
    if "contract_call" in classifications:
        return "contract_call_no_accounting", "provider_candidate", False, ["No positive asset movement involving the wallet was normalized."]
    if fee_leg:
        return "fee_only", "strong", True, ["Only the network fee is visible; fee treatment remains review-only."]
    return "unknown_review_required", "low", True, ["The normalized activity does not support a safe automatic accounting classification."]


def _candidate_records(
    classification: str,
    legs: Sequence[Mapping[str, Any]],
    *,
    tx_hash: str,
    wallet: WalletAddress,
    timestamp: Optional[str],
) -> Dict[str, List[Dict[str, Any]]]:
    deposits: List[Dict[str, Any]] = []
    withdrawals: List[Dict[str, Any]] = []
    transfer_links: List[Dict[str, Any]] = []
    wallet_id = str(wallet.wallet_id or "default")

    if classification == "deposit_candidate":
        for leg in legs:
            if leg.get("direction") != "in":
                continue
            deposits.append({
                "venue": _VENUE,
                "wallet_id": wallet_id,
                "asset": leg.get("asset"),
                "qty": leg.get("quantity"),
                "deposit_time": timestamp,
                "txid": tx_hash,
                "network": _NETWORK,
                "status": "PREVIEW",
                "source": "RH_CHAIN_ACCOUNTING_PREVIEW",
                "counterparty": leg.get("counterparty"),
                "basis_total_usd": None,
                "basis_is_missing": True,
                "will_create": False,
            })
    elif classification == "withdrawal_candidate":
        for leg in legs:
            if leg.get("direction") != "out":
                continue
            withdrawals.append({
                "venue": _VENUE,
                "wallet_id": wallet_id,
                "asset": leg.get("asset"),
                "qty": leg.get("quantity"),
                "withdraw_time": timestamp,
                "txid": tx_hash,
                "chain": _NETWORK,
                "network": _NETWORK,
                "status": "PREVIEW",
                "source": "RH_CHAIN_ACCOUNTING_PREVIEW",
                "destination": leg.get("counterparty"),
                "will_create": False,
            })
    elif classification in {"internal_transfer_candidate", "bridge_candidate"}:
        for leg in legs:
            if leg.get("direction") not in {"in", "out", "self"}:
                continue
            transfer_links.append({
                "asset": leg.get("asset"),
                "quantity": leg.get("quantity"),
                "direction": leg.get("direction"),
                "source_wallet_address_id": str(wallet.id),
                "source_wallet_id": wallet_id,
                "counterparty": leg.get("counterparty"),
                "counterparty_wallet": leg.get("counterparty_wallet"),
                "non_taxable_candidate": classification == "internal_transfer_candidate",
                "bridge_candidate": classification == "bridge_candidate",
                "will_create": False,
            })

    return {
        "deposits": deposits,
        "withdrawals": withdrawals,
        "transfer_links": transfer_links,
    }


def _fifo_preview(
    db: Session,
    *,
    wallet: WalletAddress,
    asset: str,
    quantity: float,
) -> Dict[str, Any]:
    wallet_id = str(wallet.wallet_id or "default")
    normalized_asset = _asset(asset)
    requested = max(0.0, float(quantity or 0.0))
    lots = (
        db.query(BasisLot)
        .filter(
            BasisLot.venue == _VENUE,
            BasisLot.wallet_id == wallet_id,
            BasisLot.asset == normalized_asset,
            BasisLot.qty_remaining > 0,
        )
        .order_by(asc(BasisLot.acquired_at), asc(BasisLot.created_at), asc(BasisLot.id))
        .all()
    )

    total_available = sum(max(0.0, _safe_float(getattr(lot, "qty_remaining", None)) or 0.0) for lot in lots)
    remaining = requested
    slices: List[Dict[str, Any]] = []
    total_basis = 0.0
    basis_defined = True
    any_basis_missing = False

    for lot in lots:
        if remaining <= 1e-18:
            break
        available = max(0.0, _safe_float(getattr(lot, "qty_remaining", None)) or 0.0)
        if available <= 0:
            continue
        take = min(available, remaining)
        lot_total_basis = _safe_float(getattr(lot, "total_basis_usd", None))
        lot_qty_total = _safe_float(getattr(lot, "qty_total", None)) or 0.0
        missing = bool(getattr(lot, "basis_is_missing", False)) or lot_total_basis is None or lot_qty_total <= 0
        basis_moved = None
        if missing:
            any_basis_missing = True
            basis_defined = False
        else:
            basis_moved = float((lot_total_basis / lot_qty_total) * take)
            total_basis += basis_moved
        slices.append({
            "lot_id": str(lot.id),
            "acquired_at": _iso(getattr(lot, "acquired_at", None)),
            "qty_available_before": available,
            "qty_previewed": float(take),
            "basis_moved_usd": basis_moved,
            "basis_is_missing": missing,
            "basis_source": getattr(lot, "basis_source", None),
            "origin_type": getattr(lot, "origin_type", None),
            "origin_ref": getattr(lot, "origin_ref", None),
            "would_mutate": False,
        })
        remaining -= take

    covered = requested - max(0.0, remaining)
    return {
        "version": "robinhood_chain_fifo_preview_v1",
        "scope": {
            "venue": _VENUE,
            "wallet_id": wallet_id,
            "wallet_address_id": str(wallet.id),
            "asset": normalized_asset,
            "cross_venue_pooling": False,
            "cross_wallet_pooling": False,
        },
        "selection_semantics": "current_utt_fifo_scope_and_order",
        "qty_requested": requested,
        "qty_available": float(total_available),
        "qty_covered": float(covered),
        "qty_missing": float(max(0.0, remaining)),
        "fully_covered": bool(remaining <= 1e-12),
        "total_basis_moved_usd": float(total_basis) if basis_defined else None,
        "any_basis_missing": bool(any_basis_missing),
        "slices": slices,
        "will_mutate": False,
    }


def _basis_previews(
    db: Session,
    *,
    wallet: WalletAddress,
    classification: str,
    legs: Sequence[Mapping[str, Any]],
    fee_leg: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    previewable = classification in {
        "withdrawal_candidate",
        "internal_transfer_candidate",
        "bridge_candidate",
        "swap_candidate",
    }
    outgoing = [leg for leg in legs if leg.get("direction") == "out"] if previewable else []
    quantity_by_asset: Dict[str, float] = {}
    for leg in outgoing:
        normalized_asset = _asset(leg.get("asset"))
        quantity = _safe_float(leg.get("quantity")) or 0.0
        if normalized_asset and quantity > 0:
            quantity_by_asset[normalized_asset] = quantity_by_asset.get(normalized_asset, 0.0) + float(quantity)
    previews = [
        _fifo_preview(
            db,
            wallet=wallet,
            asset=normalized_asset,
            quantity=quantity,
        )
        for normalized_asset, quantity in sorted(quantity_by_asset.items())
    ]
    return {
        "outgoing_assets": previews,
        "fee_leg": dict(fee_leg) if fee_leg else None,
        "fee_basis_previewed": False,
        "fee_note": "Network-fee basis and tax allocation are not finalized in RH-CHAIN.9.",
        "will_mutate": False,
    }


def _existing_state(db: Session, *, tx_hash: str) -> Dict[str, Any]:
    tx_key = tx_hash.lower()
    deposits = db.query(AssetDeposit).filter(func.lower(AssetDeposit.txid) == tx_key).limit(_MAX_QUERY_ROWS).all()
    withdrawals = db.query(AssetWithdrawal).filter(func.lower(AssetWithdrawal.txid) == tx_key).limit(_MAX_QUERY_ROWS).all()
    wallet_txs = db.query(WalletAddressTx).filter(func.lower(WalletAddressTx.txid) == tx_key).limit(_MAX_QUERY_ROWS).all()
    bridge_records = (
        db.query(BridgeTransferRecord)
        .filter(or_(func.lower(BridgeTransferRecord.source_txid) == tx_key, func.lower(BridgeTransferRecord.destination_txid) == tx_key))
        .limit(_MAX_QUERY_ROWS)
        .all()
    )
    return {
        "deposits": [_row_payload(row, ("id", "venue", "wallet_id", "asset", "qty", "deposit_time", "txid", "status", "source")) for row in deposits],
        "withdrawals": [_row_payload(row, ("id", "venue", "wallet_id", "asset", "qty", "withdraw_time", "txid", "status", "source")) for row in withdrawals],
        "wallet_address_txs": [_row_payload(row, ("id", "wallet_address_id", "asset", "network", "direction", "amount", "tx_time", "deposit_id", "withdrawal_id")) for row in wallet_txs],
        "bridge_transfer_records": [_row_payload(row, ("id", "asset", "amount", "source_chain", "destination_chain", "status", "bridge_mechanism", "source_withdrawal_id", "destination_deposit_id")) for row in bridge_records],
        "counts": {
            "deposits": len(deposits),
            "withdrawals": len(withdrawals),
            "wallet_address_txs": len(wallet_txs),
            "bridge_transfer_records": len(bridge_records),
        },
    }


def build_robinhood_chain_accounting_preview(
    db: Session,
    *,
    address: str,
    tx_hash: str,
    transaction_activity: Mapping[str, Any],
    wallet_address_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a query-only accounting preview for one Robinhood Chain transaction."""
    normalized_address = validate_evm_address(address)
    normalized_tx_hash = validate_transaction_hash(tx_hash)
    activity_hash = validate_transaction_hash(transaction_activity.get("transaction_hash"))
    activity_address = validate_evm_address(str(transaction_activity.get("address") or ""))
    if activity_hash.lower() != normalized_tx_hash.lower():
        raise ValueError("transaction activity hash does not match the requested transaction")
    if activity_address.lower() != normalized_address.lower():
        raise ValueError("transaction activity address does not match the requested wallet")

    wallet = _resolve_wallet(
        db,
        normalized_address=normalized_address,
        wallet_address_id=wallet_address_id,
    )
    owner_scope = str(wallet.owner_scope or "default")
    owned_by_address, owned_wallet_rows = _owned_wallets(db, owner_scope=owner_scope)
    items = [item for item in transaction_activity.get("items") or [] if isinstance(item, Mapping)]
    legs, fee_leg = _activity_legs(
        items,
        owner_address=normalized_address,
        owned_by_address=owned_by_address,
    )
    classification, confidence, review_required, warnings = _classification(
        items,
        legs,
        owned_by_address=owned_by_address,
        fee_leg=fee_leg,
    )

    timestamp = next((str(item.get("timestamp")) for item in items if item.get("timestamp")), None)
    candidate_records = _candidate_records(
        classification,
        legs,
        tx_hash=normalized_tx_hash,
        wallet=wallet,
        timestamp=timestamp,
    )
    basis_preview = _basis_previews(
        db,
        wallet=wallet,
        classification=classification,
        legs=legs,
        fee_leg=fee_leg,
    )
    existing_state = _existing_state(db, tx_hash=normalized_tx_hash)

    incoming = [dict(leg) for leg in legs if leg.get("direction") == "in"]
    outgoing = [dict(leg) for leg in legs if leg.get("direction") == "out"]
    return {
        "ok": True,
        "version": "robinhood_chain_accounting_preview_v1",
        "venue": _VENUE,
        "network": _NETWORK,
        "chain_id": 4663,
        "address": normalized_address,
        "wallet_address_id": str(wallet.id),
        "wallet_id": wallet.wallet_id,
        "owner_scope": owner_scope,
        "transaction_hash": normalized_tx_hash,
        "transaction_timestamp": timestamp,
        "classification": classification,
        "confidence": confidence,
        "review_required": bool(review_required),
        "warnings": warnings,
        "activity_legs": legs,
        "fee_leg": fee_leg,
        "candidate_records": candidate_records,
        "transfer_preview": {
            "source_wallet": {
                "id": str(wallet.id),
                "wallet_id": wallet.wallet_id,
                "owner_scope": owner_scope,
                "address": normalized_address,
                "label": wallet.label,
            },
            "owned_robinhood_chain_wallets": owned_wallet_rows,
            "owned_counterparty_legs": [dict(leg) for leg in legs if leg.get("counterparty_owned")],
            "non_taxable_candidate": classification == "internal_transfer_candidate",
            "bridge_candidate": classification == "bridge_candidate",
            "will_link": False,
        },
        "basis_preview": basis_preview,
        "swap_preview": {
            "disposed_legs": outgoing if classification == "swap_candidate" else [],
            "acquired_legs": incoming if classification == "swap_candidate" else [],
            "fee_leg": fee_leg if classification == "swap_candidate" else None,
            "taxable_review_required": classification == "swap_candidate",
            "realized_gain_finalized": False,
        },
        "existing_state": existing_state,
        "history_source": transaction_activity.get("source"),
        "history_cached": bool(transaction_activity.get("cached")),
        "history_stale": bool(transaction_activity.get("stale")),
        "history_partial": bool(transaction_activity.get("partial")),
        "read_only": True,
        "safety": {
            "read_only": True,
            "will_mutate": False,
            "can_apply": False,
            "history_ingestion": False,
            "wallet_address_tx_creation": False,
            "deposit_creation": False,
            "withdrawal_creation": False,
            "bridge_record_creation": False,
            "ledger_mutation": False,
            "fifo_mutation": False,
            "basis_mutation": False,
            "realized_gain_finalization": False,
            "metamask_request": False,
            "signing": False,
            "broadcast": False,
        },
    }
