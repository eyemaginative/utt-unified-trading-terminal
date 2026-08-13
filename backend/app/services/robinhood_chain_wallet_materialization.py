from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from sqlalchemy.orm import Session

from ..models import (
    RobinhoodChainBuyExecution,
    RobinhoodChainExecution,
    RobinhoodChainExternalSwap,
    RobinhoodChainSwapExecution,
    RobinhoodChainWalletCheckpoint,
    RobinhoodChainWalletEvent,
    TokenRegistry,
    WalletAddress,
)
from .evm_rpc import validate_evm_address


_EXPECTED_CHAIN_ID = 4663
_NETWORK = "robinhood_chain"
_WALLET_ID = "robinhood_chain"
_MATERIALIZATION_VERSION = "rh_wallet_materialization_v1"


def _utc_naive() -> datetime:
    return datetime.utcnow()


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


def _asset(value: Any) -> str:
    return str(value or "").strip().upper()


def _wallet_row(db: Session, wallet_address_id: str) -> WalletAddress:
    row = (
        db.query(WalletAddress)
        .filter(WalletAddress.id == str(wallet_address_id or "").strip())
        .first()
    )
    if row is None:
        raise ValueError("robinhood_chain_wallet_address_not_found")
    if _norm(row.network) != _NETWORK or _norm(row.wallet_id) != _WALLET_ID:
        raise ValueError("robinhood_chain_wallet_address_scope_mismatch")
    if _asset(row.asset) not in {"ALL", "*"}:
        raise ValueError("robinhood_chain_wallet_address_all_scope_required")
    validate_evm_address(str(row.address or "").strip())
    return row


def _known_lifecycle_hashes(db: Session) -> Tuple[Set[str], Set[str]]:
    swap_hashes: Set[str] = set()
    approval_hashes: Set[str] = set()

    for value, in (
        db.query(RobinhoodChainExecution.tx_hash)
        .filter(RobinhoodChainExecution.tx_hash.is_not(None))
        .all()
    ):
        text = _norm(value)
        if text:
            swap_hashes.add(text)

    for swap_hash, approval_hash in (
        db.query(
            RobinhoodChainBuyExecution.swap_tx_hash,
            RobinhoodChainBuyExecution.approval_tx_hash,
        )
        .filter(
            (RobinhoodChainBuyExecution.swap_tx_hash.is_not(None))
            | (RobinhoodChainBuyExecution.approval_tx_hash.is_not(None))
        )
        .all()
    ):
        swap_text = _norm(swap_hash)
        approval_text = _norm(approval_hash)
        if swap_text:
            swap_hashes.add(swap_text)
        if approval_text:
            approval_hashes.add(approval_text)

    for swap_hash, approval_hash in (
        db.query(
            RobinhoodChainSwapExecution.swap_tx_hash,
            RobinhoodChainSwapExecution.approval_tx_hash,
        )
        .filter(
            (RobinhoodChainSwapExecution.swap_tx_hash.is_not(None))
            | (RobinhoodChainSwapExecution.approval_tx_hash.is_not(None))
        )
        .all()
    ):
        swap_text = _norm(swap_hash)
        approval_text = _norm(approval_hash)
        if swap_text:
            swap_hashes.add(swap_text)
        if approval_text:
            approval_hashes.add(approval_text)

    return swap_hashes, approval_hashes


def _positive_atomic(row: RobinhoodChainWalletEvent) -> int:
    try:
        return int(str(row.amount_atomic or "0"))
    except Exception:
        return 0


def _decimal_text_from_atomic(value: Any, decimals: Any) -> Optional[str]:
    try:
        atomic = int(str(value or "0"))
        places = int(decimals)
    except Exception:
        return None
    if atomic < 0 or places < 0 or places > 36:
        return None
    try:
        amount = Decimal(atomic) / (Decimal(10) ** places)
    except (InvalidOperation, OverflowError):
        return None
    text = format(amount, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _fee_text(fee_wei: Any) -> Optional[str]:
    return _decimal_text_from_atomic(fee_wei, 18)


def _group_rows(rows: Iterable[RobinhoodChainWalletEvent]) -> Dict[str, List[RobinhoodChainWalletEvent]]:
    grouped: Dict[str, List[RobinhoodChainWalletEvent]] = defaultdict(list)
    for row in rows:
        tx_hash = _norm(row.transaction_hash)
        if tx_hash:
            grouped[tx_hash].append(row)
    return dict(grouped)


def _has_failed(rows: Sequence[RobinhoodChainWalletEvent]) -> bool:
    for row in rows:
        classification = _norm(row.classification)
        status = _norm(row.status)
        if classification in {"failed", "reverted"} or status in {"error", "failed", "reverted"}:
            return True
    return False


def _has_approval(rows: Sequence[RobinhoodChainWalletEvent]) -> bool:
    return any(_norm(row.classification) == "approval" for row in rows)


def _candidate_external_swap(
    db: Session,
    rows: Sequence[RobinhoodChainWalletEvent],
) -> Optional[Dict[str, Any]]:
    """Return a high-confidence external ETH->registered ERC-20 swap candidate.

    Asset identity is event-type aware:
      - top-level transaction + asset ETH is native ETH; contract_address is
        only destination/call-target metadata.
      - ERC-20 transfer contract_address is token identity and must already be
        resolved through Token Registry (registered=True + registry_id).
    """
    if not rows or _has_failed(rows) or _has_approval(rows):
        return None

    incoming = [row for row in rows if _norm(row.direction) == "in" and _positive_atomic(row) > 0 and _asset(row.asset)]
    outgoing = [row for row in rows if _norm(row.direction) == "out" and _positive_atomic(row) > 0 and _asset(row.asset)]

    if len(incoming) != 1 or len(outgoing) != 1:
        return None

    out_row = outgoing[0]
    in_row = incoming[0]

    if _norm(out_row.event_type) != "transaction":
        return None
    if _asset(out_row.asset) != "ETH":
        return None
    if int(out_row.decimals if out_row.decimals is not None else -1) != 18:
        return None
    if not str(out_row.contract_address or "").strip():
        return None

    if _norm(in_row.event_type) != "erc20_transfer":
        return None
    if in_row.registered is not True or in_row.registry_id is None:
        return None
    if not str(in_row.contract_address or "").strip():
        return None
    if in_row.decimals is None:
        return None

    # Persisted `registered=True` is useful evidence, but materialization also
    # re-validates the current Token Registry identity by primary key + chain +
    # exact token contract. This prevents stale symbol-only mappings from
    # becoming canonical historical orders.
    registry_row = (
        db.query(TokenRegistry)
        .filter(TokenRegistry.id == int(in_row.registry_id))
        .first()
    )
    if registry_row is None:
        return None
    if _norm(registry_row.chain) != _NETWORK:
        return None
    if _norm(registry_row.address) != _norm(in_row.contract_address):
        return None

    output_asset = _asset(registry_row.symbol)
    if not output_asset or output_asset == "ETH":
        return None
    if output_asset != _asset(in_row.asset):
        return None
    try:
        output_decimals = int(registry_row.decimals)
    except Exception:
        return None
    if output_decimals != int(in_row.decimals):
        return None

    input_amount = _decimal_text_from_atomic(out_row.amount_atomic, out_row.decimals)
    output_amount = _decimal_text_from_atomic(in_row.amount_atomic, output_decimals)
    network_fee = _fee_text(out_row.fee_wei)
    if input_amount is None or output_amount is None or network_fee is None:
        return None
    try:
        if Decimal(input_amount) <= 0 or Decimal(output_amount) <= 0:
            return None
    except Exception:
        return None

    tx_hash = _norm(out_row.transaction_hash or in_row.transaction_hash)
    if not tx_hash:
        return None

    block_values = [int(row.block_number) for row in rows if row.block_number is not None]
    tx_times = [row.tx_time for row in rows if row.tx_time is not None]
    fee_wei = str(out_row.fee_wei or "0")

    return {
        "transaction_hash": tx_hash,
        "symbol": f"{output_asset}-ETH",
        "side": "buy",
        "status": "confirmed",
        "input_asset": "ETH",
        "input_amount_atomic": str(out_row.amount_atomic or "0"),
        "input_amount": input_amount,
        "input_decimals": 18,
        "output_asset": output_asset,
        "output_registry_id": int(registry_row.id),
        "output_amount_atomic": str(in_row.amount_atomic or "0"),
        "output_amount": output_amount,
        "output_decimals": output_decimals,
        "network_fee_asset": "ETH",
        "network_fee_wei": fee_wei,
        "network_fee": network_fee,
        "block_number": max(block_values) if block_values else None,
        "tx_time": min(tx_times) if tx_times else None,
        "source": "wallet_history_external",
        "evidence_summary": {
            "version": _MATERIALIZATION_VERSION,
            "event_count": len(rows),
            "input_event_type": _norm(out_row.event_type),
            "output_event_type": _norm(in_row.event_type),
            "input_direction": _norm(out_row.direction),
            "output_direction": _norm(in_row.direction),
            "output_registered": True,
            "call_target_present": bool(out_row.contract_address),
            "network_fee_separate": True,
        },
    }


def _checkpoint_state(
    db: Session,
    *,
    wallet_id: str,
    actual_event_count: int,
    actual_tx_count: int,
) -> Tuple[Optional[RobinhoodChainWalletCheckpoint], List[str]]:
    checkpoint = (
        db.query(RobinhoodChainWalletCheckpoint)
        .filter(RobinhoodChainWalletCheckpoint.wallet_address_id == wallet_id)
        .first()
    )
    blockers: List[str] = []
    if checkpoint is None:
        blockers.append("checkpoint_missing")
        return checkpoint, blockers
    if checkpoint.fully_backfilled is not True:
        blockers.append("checkpoint_not_fully_backfilled")
    if int(checkpoint.event_count or 0) != int(actual_event_count):
        blockers.append("checkpoint_event_count_mismatch")
    if int(checkpoint.transaction_count or 0) != int(actual_tx_count):
        blockers.append("checkpoint_transaction_count_mismatch")
    return checkpoint, blockers


def _classification_snapshot(db: Session, wallet: WalletAddress) -> Dict[str, Any]:
    rows = (
        db.query(RobinhoodChainWalletEvent)
        .filter(RobinhoodChainWalletEvent.wallet_address_id == wallet.id)
        .all()
    )
    groups = _group_rows(rows)
    swap_hashes, approval_hashes = _known_lifecycle_hashes(db)

    counts: Counter[str] = Counter()
    pair_counts: Counter[str] = Counter()
    candidates: List[Dict[str, Any]] = []

    for tx_hash, tx_rows in groups.items():
        if tx_hash in swap_hashes:
            counts["known_utt_swap_groups"] += 1
            continue
        if tx_hash in approval_hashes:
            counts["known_utt_approval_groups"] += 1
            continue

        candidate = _candidate_external_swap(db, tx_rows)
        if candidate is not None:
            counts["external_swap_ready"] += 1
            pair_counts[str(candidate["symbol"])] += 1
            if _asset(candidate["output_asset"]) == "SPCX":
                counts["spcx_external_swap_ready"] += 1
            candidates.append(candidate)
            continue

        if _has_failed(tx_rows):
            counts["failed_or_reverted_groups"] += 1
            continue
        if _has_approval(tx_rows):
            counts["history_only_approval_groups"] += 1
            continue

        incoming = [row for row in tx_rows if _norm(row.direction) == "in" and _positive_atomic(row) > 0 and _asset(row.asset)]
        outgoing = [row for row in tx_rows if _norm(row.direction) == "out" and _positive_atomic(row) > 0 and _asset(row.asset)]
        if incoming and not outgoing:
            counts["deposit_candidate_groups"] += 1
        elif outgoing and not incoming:
            counts["withdrawal_candidate_groups"] += 1
        else:
            counts["quarantined_groups"] += 1

    actual_event_count = len(rows)
    actual_tx_count = len(groups)
    checkpoint, blockers = _checkpoint_state(
        db,
        wallet_id=str(wallet.id),
        actual_event_count=actual_event_count,
        actual_tx_count=actual_tx_count,
    )

    existing_rows = (
        db.query(RobinhoodChainExternalSwap)
        .filter(RobinhoodChainExternalSwap.wallet_address_id == wallet.id)
        .all()
    )
    existing_by_hash = {_norm(row.transaction_hash): row for row in existing_rows if _norm(row.transaction_hash)}
    ready_hashes = {_norm(item["transaction_hash"]) for item in candidates}
    counts["external_swap_existing"] = sum(1 for tx_hash in ready_hashes if tx_hash in existing_by_hash)
    counts["external_swap_new"] = max(0, int(counts["external_swap_ready"]) - int(counts["external_swap_existing"]))

    history_only = max(
        0,
        actual_tx_count
        - int(counts["known_utt_swap_groups"])
        - int(counts["known_utt_approval_groups"]),
    )

    return {
        "wallet": wallet,
        "rows": rows,
        "groups": groups,
        "candidates": candidates,
        "existing_by_hash": existing_by_hash,
        "checkpoint": checkpoint,
        "blockers": blockers,
        "summary": {
            "event_count": actual_event_count,
            "transaction_count": actual_tx_count,
            "known_utt_swap_groups": int(counts["known_utt_swap_groups"]),
            "known_utt_approval_groups": int(counts["known_utt_approval_groups"]),
            "known_utt_lifecycle_groups": int(counts["known_utt_swap_groups"] + counts["known_utt_approval_groups"]),
            "history_only_groups": history_only,
            "external_swap_ready": int(counts["external_swap_ready"]),
            "external_swap_existing": int(counts["external_swap_existing"]),
            "external_swap_new": int(counts["external_swap_new"]),
            "spcx_external_swap_ready": int(counts["spcx_external_swap_ready"]),
            "deposit_candidate_groups": int(counts["deposit_candidate_groups"]),
            "withdrawal_candidate_groups": int(counts["withdrawal_candidate_groups"]),
            "history_only_approval_groups": int(counts["history_only_approval_groups"]),
            "failed_or_reverted_groups": int(counts["failed_or_reverted_groups"]),
            "quarantined_groups": int(counts["quarantined_groups"]),
            "external_asset_pairs": dict(sorted(pair_counts.items())),
            "checkpoint": None if checkpoint is None else {
                "fully_backfilled": bool(checkpoint.fully_backfilled),
                "event_count": int(checkpoint.event_count or 0),
                "transaction_count": int(checkpoint.transaction_count or 0),
                "newest_block_number": checkpoint.newest_block_number,
                "oldest_block_number": checkpoint.oldest_block_number,
            },
            "ready_for_materialization": not blockers,
            "blockers": list(blockers),
        },
    }


def preview_robinhood_chain_wallet_materialization(
    db: Session,
    *,
    wallet_address_id: str,
) -> Dict[str, Any]:
    wallet = _wallet_row(db, wallet_address_id)
    snapshot = _classification_snapshot(db, wallet)
    return {
        "ok": True,
        "tranche": "RH-WALLET.INGEST.1D-R1",
        "wallet_address_id": str(wallet.id),
        "summary": snapshot["summary"],
        "read_only": True,
        "will_mutate": False,
        "safety": {
            "read_only": True,
            "external_swap_table_mutation": False,
            "orders_table_mutation": False,
            "all_orders_visibility_mutation": False,
            "ledger_mutation": False,
            "fifo_mutation": False,
            "basis_mutation": False,
            "token_registry_mutation": False,
            "metamask_request": False,
            "signing": False,
            "broadcast": False,
        },
    }


def _critical_tuple(row: RobinhoodChainExternalSwap) -> Tuple[Any, ...]:
    return (
        _asset(row.input_asset),
        str(row.input_amount_atomic or ""),
        int(row.input_decimals),
        _asset(row.output_asset),
        int(row.output_registry_id) if row.output_registry_id is not None else None,
        str(row.output_amount_atomic or ""),
        int(row.output_decimals),
    )


def _candidate_critical_tuple(candidate: Mapping[str, Any]) -> Tuple[Any, ...]:
    return (
        _asset(candidate.get("input_asset")),
        str(candidate.get("input_amount_atomic") or ""),
        int(candidate.get("input_decimals")),
        _asset(candidate.get("output_asset")),
        int(candidate.get("output_registry_id")) if candidate.get("output_registry_id") is not None else None,
        str(candidate.get("output_amount_atomic") or ""),
        int(candidate.get("output_decimals")),
    )


def materialize_robinhood_chain_wallet_history(
    db: Session,
    *,
    wallet_address_id: str,
) -> Dict[str, Any]:
    wallet = _wallet_row(db, wallet_address_id)
    snapshot = _classification_snapshot(db, wallet)
    summary = snapshot["summary"]

    if summary.get("ready_for_materialization") is not True:
        return {
            "ok": False,
            "error": "robinhood_chain_wallet_materialization_checkpoint_incomplete",
            "wallet_address_id": str(wallet.id),
            "summary": summary,
            "created_external_swaps": 0,
            "updated_external_swaps": 0,
            "unchanged_external_swaps": 0,
            "database_mutation": False,
            "will_mutate": False,
        }

    existing_by_hash: Dict[str, RobinhoodChainExternalSwap] = snapshot["existing_by_hash"]
    conflicts: List[Dict[str, Any]] = []
    for candidate in snapshot["candidates"]:
        tx_hash = _norm(candidate.get("transaction_hash"))
        existing = existing_by_hash.get(tx_hash)
        if existing is None:
            continue
        if _critical_tuple(existing) != _candidate_critical_tuple(candidate):
            conflicts.append({
                "transaction_hash_present": True,
                "symbol_existing": existing.symbol,
                "symbol_candidate": candidate.get("symbol"),
                "reason": "economics_changed_after_materialization",
            })

    if conflicts:
        return {
            "ok": False,
            "error": "robinhood_chain_wallet_materialization_conflict",
            "wallet_address_id": str(wallet.id),
            "summary": summary,
            "conflict_count": len(conflicts),
            "conflicts": conflicts,
            "created_external_swaps": 0,
            "updated_external_swaps": 0,
            "unchanged_external_swaps": 0,
            "database_mutation": False,
            "will_mutate": False,
        }

    created = 0
    updated = 0
    unchanged = 0
    now = _utc_naive()

    mutable_fields = (
        "symbol",
        "side",
        "status",
        "input_asset",
        "input_amount_atomic",
        "input_amount",
        "input_decimals",
        "output_asset",
        "output_registry_id",
        "output_amount_atomic",
        "output_amount",
        "output_decimals",
        "network_fee_asset",
        "network_fee_wei",
        "network_fee",
        "block_number",
        "tx_time",
        "source",
        "evidence_summary",
    )

    for candidate in snapshot["candidates"]:
        tx_hash = _norm(candidate["transaction_hash"])
        existing = existing_by_hash.get(tx_hash)
        if existing is None:
            row = RobinhoodChainExternalSwap(
                wallet_address_id=str(wallet.id),
                chain_id=_EXPECTED_CHAIN_ID,
                transaction_hash=tx_hash,
                **{field: candidate[field] for field in mutable_fields},
            )
            db.add(row)
            existing_by_hash[tx_hash] = row
            created += 1
            continue

        changed = False
        for field in mutable_fields:
            value = candidate[field]
            if getattr(existing, field) != value:
                setattr(existing, field, value)
                changed = True
        if changed:
            existing.updated_at = now
            updated += 1
        else:
            unchanged += 1

    db.flush()

    summary = dict(summary)
    summary["external_swap_existing"] = int(
        db.query(RobinhoodChainExternalSwap)
        .filter(RobinhoodChainExternalSwap.wallet_address_id == wallet.id)
        .count()
    )
    summary["external_swap_new"] = 0

    return {
        "ok": True,
        "tranche": "RH-WALLET.INGEST.1D-R1",
        "wallet_address_id": str(wallet.id),
        "summary": summary,
        "created_external_swaps": created,
        "updated_external_swaps": updated,
        "unchanged_external_swaps": unchanged,
        "conflict_count": 0,
        "database_mutation": bool(created or updated),
        "external_swap_table_mutation": bool(created or updated),
        "orders_table_mutation": False,
        "all_orders_visibility_mutation": bool(created or updated),
        "ledger_mutation": False,
        "fifo_mutation": False,
        "basis_mutation": False,
        "token_registry_mutation": False,
        "metamask_request": False,
        "signing": False,
        "broadcast": False,
        "will_mutate": bool(created or updated),
        "next_stage": "RH-WALLET.INGEST.1D-ACCEPTANCE",
    }
