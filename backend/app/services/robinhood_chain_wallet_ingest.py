from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, Iterable, List, Mapping, Optional, Set, Tuple

from sqlalchemy.orm import Session

from ..config import settings
from ..models import (
    RobinhoodChainBuyExecution,
    RobinhoodChainExecution,
    RobinhoodChainSwapExecution,
    RobinhoodChainWalletCheckpoint,
    RobinhoodChainWalletEvent,
    WalletAddress,
)
from .evm_rpc import validate_evm_address
from .robinhood_chain_history import get_robinhood_chain_history_service


_EXPECTED_CHAIN_ID = 4663
_NETWORK = "robinhood_chain"
_WALLET_ID = "robinhood_chain"


def _utc_naive() -> datetime:
    return datetime.utcnow()


def _parse_time(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _wallet_row(db: Session, wallet_address_id: str) -> WalletAddress:
    row = db.query(WalletAddress).filter(WalletAddress.id == str(wallet_address_id or "").strip()).first()
    if row is None:
        raise ValueError("robinhood_chain_wallet_address_not_found")
    if str(row.network or "").strip().lower() != _NETWORK or str(row.wallet_id or "").strip().lower() != _WALLET_ID:
        raise ValueError("robinhood_chain_wallet_address_scope_mismatch")
    if str(row.asset or "").strip().upper() not in {"ALL", "*"}:
        raise ValueError("robinhood_chain_wallet_address_all_scope_required")
    validate_evm_address(str(row.address or "").strip())
    return row


def _existing_utt_tx_hashes(db: Session) -> Set[str]:
    out: Set[str] = set()
    for value, in db.query(RobinhoodChainExecution.tx_hash).filter(RobinhoodChainExecution.tx_hash.is_not(None)).all():
        text = str(value or "").strip().lower()
        if text:
            out.add(text)
    for value, in db.query(RobinhoodChainBuyExecution.swap_tx_hash).filter(RobinhoodChainBuyExecution.swap_tx_hash.is_not(None)).all():
        text = str(value or "").strip().lower()
        if text:
            out.add(text)
    for value, in db.query(RobinhoodChainSwapExecution.swap_tx_hash).filter(RobinhoodChainSwapExecution.swap_tx_hash.is_not(None)).all():
        text = str(value or "").strip().lower()
        if text:
            out.add(text)
    return out


def _event_type(item: Mapping[str, Any]) -> str:
    event_id = str(item.get("id") or "")
    if ":erc20:" in event_id:
        return "erc20_transfer"
    if ":internal:" in event_id:
        return "internal_native"
    return "transaction"


def _event_log_index(item: Mapping[str, Any]) -> Optional[str]:
    raw = item.get("provider_raw") if isinstance(item.get("provider_raw"), Mapping) else {}
    value = raw.get("log_index") if raw.get("log_index") is not None else raw.get("internal_index")
    text = str(value or "").strip()
    return text or None


def _transaction_groups(items: Iterable[Mapping[str, Any]]) -> Dict[str, List[Mapping[str, Any]]]:
    grouped: Dict[str, List[Mapping[str, Any]]] = defaultdict(list)
    for item in items:
        tx_hash = str(item.get("transaction_hash") or "").strip().lower()
        if tx_hash:
            grouped[tx_hash].append(item)
    return dict(grouped)


def _group_shape(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    incoming: Counter[str] = Counter()
    outgoing: Counter[str] = Counter()
    approvals = 0
    failed = False
    unregistered = 0
    for row in rows:
        classification = str(row.get("classification") or "").strip().lower()
        if classification == "approval":
            approvals += 1
        if classification in {"failed", "reverted"} or str(row.get("status") or "").strip().lower() == "error":
            failed = True
        if row.get("contract_address") and row.get("registered") is not True:
            unregistered += 1
        asset = str(row.get("asset") or "").strip().upper()
        try:
            amount = int(str(row.get("amount_atomic") or "0"))
        except Exception:
            amount = 0
        if not asset or amount <= 0:
            continue
        direction = str(row.get("direction") or "").strip().lower()
        if direction == "in":
            incoming[asset] += amount
        elif direction == "out":
            outgoing[asset] += amount
    incoming_assets = sorted(asset for asset, amount in incoming.items() if amount > 0)
    outgoing_assets = sorted(asset for asset, amount in outgoing.items() if amount > 0)
    potential_swap = bool(not failed and incoming_assets and outgoing_assets and set(incoming_assets) != set(outgoing_assets))
    return {
        "incoming_assets": incoming_assets,
        "outgoing_assets": outgoing_assets,
        "approval_rows": approvals,
        "failed": failed,
        "unregistered_rows": unregistered,
        "potential_swap": potential_swap,
    }


def _summary(scan: Mapping[str, Any], *, known_utt_hashes: Set[str], checkpoint: Optional[RobinhoodChainWalletCheckpoint], mode: str) -> Dict[str, Any]:
    items = list(scan.get("items") or [])
    groups = _transaction_groups(items)
    classifications = Counter(str(item.get("classification") or "unknown").strip().lower() or "unknown" for item in items)
    assets = Counter(str(item.get("asset") or "NONE").strip().upper() or "NONE" for item in items)
    event_types = Counter(_event_type(item) for item in items)
    registered_items = sum(1 for item in items if item.get("contract_address") and item.get("registered") is True)
    unregistered_items = sum(1 for item in items if item.get("contract_address") and item.get("registered") is not True)
    unregistered_contracts = {
        str(item.get("contract_address") or "").strip().lower()
        for item in items
        if item.get("contract_address") and item.get("registered") is not True
    }
    known_matches = sum(1 for tx_hash in groups if tx_hash in known_utt_hashes)
    potential_swaps = sum(1 for rows in groups.values() if _group_shape(rows)["potential_swap"])
    return {
        "mode": mode,
        "pages_scanned": int(scan.get("pages_scanned") or 0),
        "max_pages": int(scan.get("max_pages") or 0),
        "provider_exhausted": scan.get("provider_exhausted") is True,
        "reached_checkpoint": scan.get("reached_checkpoint") is True,
        "truncated": scan.get("truncated") is True,
        "partial": scan.get("partial") is True,
        "provider_errors": list(scan.get("provider_errors") or []),
        "provider_counts": dict(scan.get("provider_counts") or {}),
        "event_count": len(items),
        "transaction_count": len(groups),
        "known_utt_transaction_matches": known_matches,
        "history_only_transactions": max(0, len(groups) - known_matches),
        "potential_swap_transactions": potential_swaps,
        "registered_contract_items": registered_items,
        "unregistered_contract_items": unregistered_items,
        "unregistered_unique_contracts": len(unregistered_contracts),
        "classifications": dict(sorted(classifications.items())),
        "assets": dict(sorted(assets.items())),
        "event_types": dict(sorted(event_types.items())),
        "newest_block_number": scan.get("newest_block_number"),
        "oldest_block_number": scan.get("oldest_block_number"),
        "checkpoint": None if checkpoint is None else {
            "newest_block_number": checkpoint.newest_block_number,
            "oldest_block_number": checkpoint.oldest_block_number,
            "fully_backfilled": bool(checkpoint.fully_backfilled),
            "last_scan_at": checkpoint.last_scan_at.isoformat() if checkpoint.last_scan_at else None,
        },
    }


def _registered_history_token_map(db: Session) -> Dict[str, Dict[str, Any]]:
    # Local import avoids a router/service import cycle while preserving the
    # same Token Registry authority used by the exact-current display history.
    from .robinhood_chain_registry_discovery import get_robinhood_chain_registry_discovery_service

    service = get_robinhood_chain_registry_discovery_service()
    rows = service.registry_rows(db)
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        try:
            identity = service.token_identity(db, row)
            if bool(identity.get("native")):
                continue
            contract = validate_evm_address(
                str(identity.get("registry_contract_address") or "").strip()
            )
            key = contract.lower()
            if key in out:
                continue
            out[key] = {
                "registry_id": int(identity["registry_id"]),
                "registry_venue": identity.get("registry_venue"),
                "symbol": str(identity.get("symbol") or "").strip().upper(),
                "decimals": int(identity["decimals"]),
                "label": identity.get("label"),
                "contract_address": contract,
            }
        except Exception:
            continue
    return out


async def preview_robinhood_chain_wallet_ingest(
    db: Session,
    *,
    wallet_address_id: str,
    force_full: bool = False,
    force_refresh: bool = True,
) -> Dict[str, Any]:
    wallet = _wallet_row(db, wallet_address_id)
    checkpoint = (
        db.query(RobinhoodChainWalletCheckpoint)
        .filter(RobinhoodChainWalletCheckpoint.wallet_address_id == wallet.id)
        .first()
    )
    full_scan = bool(force_full or checkpoint is None or not checkpoint.fully_backfilled)
    mode = "full_backfill" if full_scan else "incremental"
    stop_at_block: Optional[int] = None
    if not full_scan and checkpoint and checkpoint.newest_block_number is not None:
        stop_at_block = max(
            0,
            int(checkpoint.newest_block_number) - int(settings.robinhood_chain_ingest_overlap_blocks),
        )

    scan = await get_robinhood_chain_history_service().scan_address_history(
        validate_evm_address(str(wallet.address or "").strip()),
        force_refresh=bool(force_refresh),
        registry_tokens=_registered_history_token_map(db),
        max_pages=int(settings.robinhood_chain_ingest_max_pages),
        stop_at_block=stop_at_block,
    )
    if scan.get("ok") is not True:
        return scan
    summary = _summary(
        scan,
        known_utt_hashes=_existing_utt_tx_hashes(db),
        checkpoint=checkpoint,
        mode=mode,
    )
    return {
        "ok": True,
        "tranche": "RH-WALLET.INGEST.1C2",
        "wallet_address_id": str(wallet.id),
        "wallet_address": str(wallet.address),
        "summary": summary,
        "scan": scan,
        "read_only": True,
        "will_mutate": False,
        "event_persistence": False,
        "order_mutation": False,
        "ledger_mutation": False,
        "fifo_mutation": False,
        "basis_mutation": False,
    }


def _apply_event(row: RobinhoodChainWalletEvent, item: Mapping[str, Any], *, wallet_address_id: str) -> bool:
    values = {
        "wallet_address_id": wallet_address_id,
        "chain_id": _EXPECTED_CHAIN_ID,
        "event_key": str(item.get("id") or ""),
        "transaction_hash": str(item.get("transaction_hash") or "").strip().lower(),
        "event_type": _event_type(item),
        "log_index": _event_log_index(item),
        "block_number": int(item.get("block_number")) if item.get("block_number") is not None else None,
        "tx_time": _parse_time(item.get("timestamp")),
        "status": str(item.get("status") or "unknown")[:32],
        "classification": str(item.get("classification") or "unknown")[:48],
        "direction": str(item.get("direction") or "other")[:16],
        "asset": (str(item.get("asset") or "").strip().upper() or None),
        "amount_atomic": str(item.get("amount_atomic") or "0")[:96],
        "decimals": int(item.get("decimals")) if item.get("decimals") is not None else None,
        "fee_wei": str(item.get("fee_wei") or "0")[:96],
        "contract_address": (str(item.get("contract_address") or "").strip().lower() or None),
        "registry_id": int(item.get("registry_id")) if item.get("registry_id") is not None else None,
        "registered": bool(item.get("registered")),
        "from_address": (str(item.get("from_address") or "").strip().lower() or None),
        "to_address": (str(item.get("to_address") or "").strip().lower() or None),
        "method": (str(item.get("method") or "").strip()[:128] or None),
        "source": str(item.get("source") or "blockscout_v2")[:32],
        "raw": {
            "provider_raw": dict(item.get("provider_raw") or {}),
            "explorer_url": item.get("explorer_url"),
        },
    }
    changed = False
    for field, value in values.items():
        if getattr(row, field, None) != value:
            setattr(row, field, value)
            changed = True
    if changed:
        row.updated_at = _utc_naive()
    return changed



async def ingest_robinhood_chain_wallet_history(
    db: Session,
    *,
    wallet_address_id: str,
    force_full: bool = False,
    force_refresh: bool = True,
) -> Dict[str, Any]:
    preview = await preview_robinhood_chain_wallet_ingest(
        db,
        wallet_address_id=wallet_address_id,
        force_full=force_full,
        force_refresh=force_refresh,
    )
    if preview.get("ok") is not True:
        return preview
    scan = dict(preview.get("scan") or {})
    summary = dict(preview.get("summary") or {})
    mode = str(summary.get("mode") or "").strip().lower()
    incomplete = bool(
        summary.get("partial") is True
        or summary.get("truncated") is True
        or (mode == "full_backfill" and summary.get("provider_exhausted") is not True)
    )
    if incomplete:
        return {
            "ok": False,
            "error": "robinhood_chain_wallet_ingest_incomplete_scan",
            "message": "Refusing to persist partial, truncated, or non-exhausted full-backfill evidence.",
            "wallet_address_id": str(preview.get("wallet_address_id") or ""),
            "summary": summary,
            "event_persistence": False,
            "database_mutation": False,
            "order_mutation": False,
            "ledger_mutation": False,
            "fifo_mutation": False,
            "basis_mutation": False,
            "read_only": True,
            "will_mutate": False,
        }
    wallet_id = str(preview.get("wallet_address_id") or "")
    now = _utc_naive()

    created = 0
    updated = 0
    unchanged = 0
    for item in list(scan.get("items") or []):
        event_key = str(item.get("id") or "").strip()
        tx_hash = str(item.get("transaction_hash") or "").strip().lower()
        if not event_key or not tx_hash:
            continue
        row = (
            db.query(RobinhoodChainWalletEvent)
            .filter(
                RobinhoodChainWalletEvent.wallet_address_id == wallet_id,
                RobinhoodChainWalletEvent.event_key == event_key,
            )
            .first()
        )
        if row is None:
            row = RobinhoodChainWalletEvent(wallet_address_id=wallet_id, event_key=event_key, transaction_hash=tx_hash)
            db.add(row)
            _apply_event(row, item, wallet_address_id=wallet_id)
            created += 1
        else:
            if _apply_event(row, item, wallet_address_id=wallet_id):
                updated += 1
            else:
                unchanged += 1

    # The production SessionLocal uses autoflush=False. Flush the newly
    # normalized event rows before aggregate checkpoint queries so the
    # checkpoint reflects the evidence that this same ingest just staged.
    db.flush()

    checkpoint = (
        db.query(RobinhoodChainWalletCheckpoint)
        .filter(RobinhoodChainWalletCheckpoint.wallet_address_id == wallet_id)
        .first()
    )
    if checkpoint is None:
        checkpoint = RobinhoodChainWalletCheckpoint(wallet_address_id=wallet_id, chain_id=_EXPECTED_CHAIN_ID)
        db.add(checkpoint)

    newest = scan.get("newest_block_number")
    oldest = scan.get("oldest_block_number")
    if newest is not None:
        checkpoint.newest_block_number = max(int(newest), int(checkpoint.newest_block_number or 0))
    if oldest is not None:
        checkpoint.oldest_block_number = (
            int(oldest)
            if checkpoint.oldest_block_number is None
            else min(int(oldest), int(checkpoint.oldest_block_number))
        )
    checkpoint.fully_backfilled = bool(checkpoint.fully_backfilled or (scan.get("provider_exhausted") is True and scan.get("partial") is not True))
    checkpoint.event_count = int(
        db.query(RobinhoodChainWalletEvent)
        .filter(RobinhoodChainWalletEvent.wallet_address_id == wallet_id)
        .count()
    )
    checkpoint.transaction_count = len({
        str(value or "").strip().lower()
        for value, in db.query(RobinhoodChainWalletEvent.transaction_hash)
        .filter(RobinhoodChainWalletEvent.wallet_address_id == wallet_id)
        .distinct()
        .all()
        if value
    })
    checkpoint.last_scan_at = now
    if scan.get("provider_exhausted") is True and scan.get("partial") is not True:
        checkpoint.last_full_scan_at = now
    checkpoint.updated_at = now
    db.flush()

    summary["checkpoint"] = {
        "newest_block_number": checkpoint.newest_block_number,
        "oldest_block_number": checkpoint.oldest_block_number,
        "fully_backfilled": bool(checkpoint.fully_backfilled),
        "event_count": int(checkpoint.event_count),
        "transaction_count": int(checkpoint.transaction_count),
        "last_scan_at": checkpoint.last_scan_at.isoformat() if checkpoint.last_scan_at else None,
    }
    return {
        "ok": True,
        "tranche": "RH-WALLET.INGEST.1C2",
        "wallet_address_id": wallet_id,
        "summary": summary,
        "created_events": created,
        "updated_events": updated,
        "unchanged_events": unchanged,
        "event_persistence": True,
        "database_mutation": bool(created or updated),
        "order_mutation": False,
        "ledger_mutation": False,
        "fifo_mutation": False,
        "basis_mutation": False,
        "read_only": False,
        "will_mutate": True,
        "next_stage": "RH-WALLET.INGEST.1D",
    }
