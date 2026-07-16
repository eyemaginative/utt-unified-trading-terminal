from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import os
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

import httpx
from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from ..models import AssetDeposit, AssetWithdrawal, BasisLot, VenueOrderRow
from ..models_lot_journal import LotJournal
from .counterparty_historical_btc_usd import lookup_historical_btc_usd
from .counterparty_btc_custody_scope import resolve_counterparty_btc_custody_scope
from .counterparty_btc_source_lot_preview import build_counterparty_btc_source_lot_preview


_SATOSHIS_PER_BTC = Decimal("100000000")
_TXID_RE = re.compile(r"^[0-9a-f]{64}$")


class CounterpartyLedgerPreviewError(RuntimeError):
    """Machine-readable read-only preview failure."""

    def __init__(self, *, code: str, message: str, status_code: int = 400, details: Optional[Dict[str, Any]] = None) -> None:
        self.code = str(code or "counterparty_ledger_preview_failed")
        self.message = str(message or self.code)
        self.status_code = int(status_code or 400)
        self.details = dict(details or {})
        super().__init__(self.message)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "error": self.code,
            "message": self.message,
            **self.details,
            "read_only": True,
            "database_mutation": False,
            "ledger_mutation": False,
            "lot_mutation": False,
            "basis_mutation": False,
        }


def _finite_decimal(value: Any) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    try:
        out = Decimal(str(value))
    except Exception:
        return None
    if not out.is_finite():
        return None
    return out


def _positive_decimal(value: Any) -> Optional[Decimal]:
    out = _finite_decimal(value)
    return out if out is not None and out > 0 else None


def _as_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(str(value).replace(",", ""))
    except Exception:
        try:
            return int(Decimal(str(value)))
        except Exception:
            return None


def _btc_to_satoshis(value: Any) -> Optional[int]:
    btc = _positive_decimal(value)
    if btc is None:
        return None
    return int((btc * _SATOSHIS_PER_BTC).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def _satoshis_to_btc(value: Any) -> Optional[Decimal]:
    sats = _as_int(value)
    if sats is None or sats < 0:
        return None
    return Decimal(sats) / _SATOSHIS_PER_BTC


def _decimal_text(value: Optional[Decimal], *, max_places: int = 8) -> Optional[str]:
    if value is None:
        return None
    places = max(0, min(int(max_places), 18))
    text = format(value, f".{places}f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _dt_iso(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        dt = value
    elif value not in (None, ""):
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def _norm_txid(value: Any) -> str:
    txid = str(value or "").strip().lower()
    if not _TXID_RE.fullmatch(txid):
        raise CounterpartyLedgerPreviewError(
            code="counterparty_ledger_preview_invalid_txid",
            message="txid must be a 64-character hexadecimal Bitcoin transaction id",
            status_code=400,
        )
    return txid


def _fee_satoshis_from_transaction(raw: Any) -> Optional[int]:
    """Extract a positive Bitcoin miner fee without accepting fee-rate fields."""
    if not isinstance(raw, dict):
        return None

    atomic_keys = (
        "fee",
        "btc_fee",
        "network_fee",
        "miner_fee",
        "fee_satoshis",
        "btc_fee_satoshis",
        "network_fee_satoshis",
        "miner_fee_satoshis",
    )
    normalized_keys = (
        "fee_normalized",
        "btc_fee_normalized",
        "network_fee_btc",
        "miner_fee_btc",
    )

    for key in atomic_keys:
        sats = _as_int(raw.get(key))
        if sats is not None and sats > 0:
            return sats
    for key in normalized_keys:
        sats = _btc_to_satoshis(raw.get(key))
        if sats is not None and sats > 0:
            return sats

    for container_key in ("transaction", "tx", "bitcoin", "raw", "data", "result"):
        nested = raw.get(container_key)
        if isinstance(nested, dict):
            sats = _fee_satoshis_from_transaction(nested)
            if sats is not None and sats > 0:
                return sats
    return None


def _bitcoin_tx_api_base_url() -> str:
    raw = str(os.getenv("COUNTERPARTY_BITCOIN_TX_API_BASE_URL") or "https://mempool.space/api").strip()
    if not raw.startswith(("https://", "http://")):
        return "https://mempool.space/api"
    return raw.rstrip("/")


def _bitcoin_tx_lookup_timeout_s() -> float:
    try:
        value = float(os.getenv("COUNTERPARTY_BITCOIN_TX_LOOKUP_TIMEOUT_S") or "10")
    except Exception:
        value = 10.0
    return max(2.0, min(value, 30.0))


def _external_bitcoin_tx(txid: str) -> Dict[str, Any]:
    """Read a public Bitcoin transaction for fee/confirmation metadata only."""
    url = f"{_bitcoin_tx_api_base_url()}/tx/{txid}"
    try:
        with httpx.Client(
            timeout=_bitcoin_tx_lookup_timeout_s(),
            headers={"accept": "application/json", "user-agent": "UTT Counterparty ledger preview/1.0"},
            follow_redirects=True,
        ) as client:
            response = client.get(url)
        if response.status_code >= 400:
            return {
                "ok": False,
                "source": "bitcoin_tx_api",
                "url": url,
                "error": f"HTTP {response.status_code}",
                "body_preview": str(response.text or "")[:300],
            }
        data = response.json()
        if not isinstance(data, dict):
            return {"ok": False, "source": "bitcoin_tx_api", "url": url, "error": "unexpected_response"}
        status = data.get("status") if isinstance(data.get("status"), dict) else {}
        return {
            "ok": True,
            "source": "bitcoin_tx_api",
            "url": url,
            "fee_satoshis": _fee_satoshis_from_transaction(data),
            "confirmed": bool(status.get("confirmed")),
            "block_height": _as_int(status.get("block_height")),
            "block_hash": status.get("block_hash"),
            "block_time": status.get("block_time"),
            "raw": data,
        }
    except Exception as exc:
        return {
            "ok": False,
            "source": "bitcoin_tx_api",
            "url": url,
            "error": f"{type(exc).__name__}: {exc}"[:500],
        }


def _select_dispense_event(
    history: Dict[str, Any],
    *,
    txid: str,
    dispense_index: Optional[int],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    raw_events = history.get("events") if isinstance(history.get("events"), list) else history.get("items")
    events = [dict(row) for row in (raw_events or []) if isinstance(row, dict)]
    matches = [
        row
        for row in events
        if str(row.get("counterparty_txid") or row.get("venue_order_id") or "").strip().lower() == txid
    ]
    if dispense_index is not None:
        matches = [
            row
            for row in matches
            if _as_int(row.get("counterparty_dispense_index")) == int(dispense_index)
        ]
    if not matches:
        raise CounterpartyLedgerPreviewError(
            code="counterparty_confirmed_dispense_not_found",
            message="No confirmed Counterparty dispense matched the requested transaction and event index",
            status_code=404,
            details={"txid": txid, "dispense_index": dispense_index},
        )
    matches.sort(key=lambda row: (_as_int(row.get("counterparty_dispense_index")) or 0))
    return matches[0], matches


def _count_query(db: Session, stmt: Any) -> int:
    try:
        return int(db.execute(stmt).scalar_one() or 0)
    except Exception:
        return 0


def _sum_basis_qty(db: Session, *, venue: str, wallet_id: str, asset: str) -> float:
    try:
        value = db.execute(
            select(func.coalesce(func.sum(BasisLot.qty_remaining), 0.0)).where(
                func.lower(BasisLot.venue) == venue.lower(),
                BasisLot.wallet_id == wallet_id,
                func.upper(BasisLot.asset) == asset.upper(),
                BasisLot.qty_remaining > 0,
            )
        ).scalar_one()
        return float(value or 0.0)
    except Exception:
        return 0.0


def _existing_state(
    db: Session,
    *,
    txid: str,
    event_identity: str,
    venue: str,
    wallet_id: str,
    acquired_asset: str,
    btc_scope_venue: Optional[str] = None,
    btc_scope_wallet_id: Optional[str] = None,
) -> Dict[str, Any]:
    venue_order_rows = _count_query(
        db,
        select(func.count()).select_from(VenueOrderRow).where(
            func.lower(VenueOrderRow.venue) == venue.lower(),
            VenueOrderRow.venue_order_id == txid,
        ),
    )
    deposit_rows = _count_query(
        db,
        select(func.count()).select_from(AssetDeposit).where(
            func.lower(AssetDeposit.venue) == venue.lower(),
            AssetDeposit.wallet_id == wallet_id,
            AssetDeposit.txid == txid,
        ),
    )
    withdrawal_rows = _count_query(
        db,
        select(func.count()).select_from(AssetWithdrawal).where(
            func.lower(AssetWithdrawal.venue) == venue.lower(),
            AssetWithdrawal.wallet_id == wallet_id,
            AssetWithdrawal.txid == txid,
        ),
    )
    journal_rows = _count_query(
        db,
        select(func.count()).select_from(LotJournal).where(
            func.lower(LotJournal.venue) == venue.lower(),
            LotJournal.wallet_id == wallet_id,
            or_(
                LotJournal.origin_ref == txid,
                LotJournal.origin_ref == event_identity,
            ),
        ),
    )
    asset_lot_rows = _count_query(
        db,
        select(func.count()).select_from(BasisLot).where(
            func.lower(BasisLot.venue) == venue.lower(),
            BasisLot.wallet_id == wallet_id,
            func.upper(BasisLot.asset) == acquired_asset.upper(),
            BasisLot.qty_remaining > 0,
        ),
    )
    return {
        "venue_order_rows": venue_order_rows,
        "asset_deposit_rows": deposit_rows,
        "asset_withdrawal_rows": withdrawal_rows,
        "lot_journal_rows": journal_rows,
        "acquired_asset_open_lot_rows": asset_lot_rows,
        "acquired_asset_open_lot_qty": _sum_basis_qty(
            db,
            venue=venue,
            wallet_id=wallet_id,
            asset=acquired_asset,
        ),
        "btc_scope_venue": btc_scope_venue,
        "btc_scope_wallet_id": btc_scope_wallet_id,
        "btc_open_lot_qty": (
            _sum_basis_qty(
                db,
                venue=btc_scope_venue,
                wallet_id=btc_scope_wallet_id,
                asset="BTC",
            )
            if btc_scope_venue and btc_scope_wallet_id
            else 0.0
        ),
        "counterparty_protocol_btc_open_lot_qty": _sum_basis_qty(
            db,
            venue=venue,
            wallet_id=wallet_id,
            asset="BTC",
        ),
    }


def build_counterparty_ledger_preview(
    *,
    db: Session,
    adapter: Any,
    txid: str,
    dispense_index: Optional[int] = None,
    allow_external_fee_lookup: bool = True,
    allow_external_price_lookup: bool = True,
    force_historical_price_refresh: bool = False,
    history_limit: int = 200,
) -> Dict[str, Any]:
    """Build a deterministic Counterparty accounting preview without writes.

    The preview separates the acquired Counterparty asset, the BTC dispenser
    consideration, and the Bitcoin miner fee. It intentionally does not call
    the generic deposit/withdrawal or lot-sync mutation paths.
    """
    txid_norm = _norm_txid(txid)

    source = adapter.configured_source_address_info()
    address = str(source.get("address") or "").strip()
    if not source.get("ok") or not address:
        raise CounterpartyLedgerPreviewError(
            code=str(source.get("error") or "counterparty_wallet_address_missing"),
            message=str(source.get("message") or "Counterparty Wallet Addresses account row is required"),
            status_code=409 if source.get("error") == "counterparty_wallet_address_ambiguous" else 422,
            details={"source_address": source},
        )

    history = adapter.get_confirmed_dispense_orders(address=address, limit=max(1, min(int(history_limit), 500)))
    if not history.get("ok"):
        raise CounterpartyLedgerPreviewError(
            code="counterparty_confirmed_history_failed",
            message="Counterparty confirmed dispense history could not be read",
            status_code=502,
            details={"history": history},
        )

    event, tx_matches = _select_dispense_event(
        history,
        txid=txid_norm,
        dispense_index=dispense_index,
    )

    confirmed = bool(event.get("counterparty_confirmed"))
    block_index = _as_int(event.get("counterparty_block_index"))
    confirmed_at = _dt_iso(event.get("created_at") or event.get("updated_at"))
    if not confirmed or block_index is None:
        raise CounterpartyLedgerPreviewError(
            code="counterparty_dispense_not_confirmed",
            message="Counterparty accounting preview requires a confirmed dispense with a block index",
            status_code=409,
            details={"txid": txid_norm, "event": event},
        )

    event_index = _as_int(event.get("counterparty_dispense_index"))
    if event_index is None:
        event_index = 0
    event_identity = f"counterparty:dispense:{txid_norm}:{event_index}"

    asset = str(event.get("symbol_canon") or event.get("symbol_venue") or "").split("-", 1)[0].strip().upper()
    quantity = _positive_decimal(event.get("filled_qty") if event.get("filled_qty") is not None else event.get("qty"))
    unit_price_btc = _positive_decimal(event.get("avg_fill_price") if event.get("avg_fill_price") is not None else event.get("limit_price"))
    gross_satoshis = _as_int(event.get("counterparty_gross_satoshis"))
    if gross_satoshis is None and quantity is not None and unit_price_btc is not None:
        gross_satoshis = int((quantity * unit_price_btc * _SATOSHIS_PER_BTC).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    gross_btc = _satoshis_to_btc(gross_satoshis)

    if not asset or quantity is None or gross_satoshis is None or gross_satoshis <= 0 or gross_btc is None:
        raise CounterpartyLedgerPreviewError(
            code="counterparty_dispense_economics_incomplete",
            message="The confirmed dispense is missing required asset, quantity, or BTC payment fields",
            status_code=409,
            details={"txid": txid_norm, "event": event},
        )

    fee_satoshis = _as_int(event.get("counterparty_fee_satoshis"))
    fee_source = "counterparty_transaction_metadata" if fee_satoshis is not None and fee_satoshis > 0 else None
    raw_tx = event.get("counterparty_raw_transaction") if isinstance(event.get("counterparty_raw_transaction"), dict) else {}
    if fee_satoshis is None or fee_satoshis <= 0:
        fee_satoshis = _fee_satoshis_from_transaction(raw_tx)
        if fee_satoshis is not None and fee_satoshis > 0:
            fee_source = "counterparty_raw_transaction"

    external_tx: Dict[str, Any] = {"attempted": False}
    if (fee_satoshis is None or fee_satoshis <= 0) and allow_external_fee_lookup:
        lookup = _external_bitcoin_tx(txid_norm)
        external_tx = {"attempted": True, **lookup}
        candidate = _as_int(lookup.get("fee_satoshis")) if lookup.get("ok") else None
        if candidate is not None and candidate > 0:
            fee_satoshis = candidate
            fee_source = "bitcoin_tx_api"

    if fee_satoshis is not None and fee_satoshis <= 0:
        fee_satoshis = None
        fee_source = None
    fee_btc = _satoshis_to_btc(fee_satoshis)

    tx_event_count = len(tx_matches)
    fee_allocated_satoshis = fee_satoshis if fee_satoshis is not None and tx_event_count == 1 else None
    fee_allocation_policy = (
        "full_transaction_fee_to_single_dispense"
        if fee_allocated_satoshis is not None
        else "unallocated_multiple_dispenses"
        if fee_satoshis is not None and tx_event_count > 1
        else "fee_unresolved"
    )
    total_outflow_satoshis = gross_satoshis + fee_satoshis if fee_satoshis is not None else None
    total_outflow_btc = _satoshis_to_btc(total_outflow_satoshis)
    acquisition_basis_btc = gross_btc + fee_btc if fee_btc is not None and tx_event_count == 1 else gross_btc

    historical_price = lookup_historical_btc_usd(
        confirmed_at,
        allow_external_lookup=bool(allow_external_price_lookup),
        force_refresh=bool(force_historical_price_refresh),
    )
    historical_btc_usd = (
        _positive_decimal(
            historical_price.get("price_usd_exact")
            if historical_price.get("price_usd_exact") is not None
            else historical_price.get("price_usd")
        )
        if historical_price.get("ok")
        else None
    )
    total_basis_usd = acquisition_basis_btc * historical_btc_usd if historical_btc_usd is not None else None
    cost_average_usd = total_basis_usd / quantity if total_basis_usd is not None and quantity > 0 else None

    wallet_id = str(source.get("wallet_id") or "counterparty").strip() or "counterparty"
    btc_custody_scope = resolve_counterparty_btc_custody_scope(
        db=db,
        source_address=source,
        required_btc=float(total_outflow_btc or gross_btc),
        acquired_asset=asset,
        counterparty_venue="counterparty",
        counterparty_wallet_id=wallet_id,
    )
    proposed_btc_scope = btc_custody_scope.get("proposed_btc_disposition_scope") or {}
    source_lot_target_raw = (
        external_tx.get("raw")
        if isinstance(external_tx.get("raw"), dict)
        else raw_tx
        if isinstance(raw_tx, dict)
        else None
    )
    btc_source_lot_preview = build_counterparty_btc_source_lot_preview(
        db=db,
        txid=txid_norm,
        custody_scope=btc_custody_scope,
        custody_address=address,
        expected_gross_satoshis=gross_satoshis,
        expected_fee_satoshis=fee_satoshis,
        dispenser_address=event.get("counterparty_dispenser"),
        supplied_target_tx_raw=source_lot_target_raw,
        allow_external_lookup=bool(allow_external_fee_lookup),
    )
    existing = _existing_state(
        db,
        txid=txid_norm,
        event_identity=event_identity,
        venue="counterparty",
        wallet_id=wallet_id,
        acquired_asset=asset,
        btc_scope_venue=proposed_btc_scope.get("venue"),
        btc_scope_wallet_id=proposed_btc_scope.get("wallet_id"),
    )

    blockers: List[str] = [
        "storage_model_review_required",
        *[
            str(item)
            for item in (btc_custody_scope.get("blockers") or [])
            if str(item or "").strip()
        ],
    ]
    warnings: List[str] = [
        str(item)
        for item in (
            list(historical_price.get("warnings") or [])
            + list(btc_custody_scope.get("warnings") or [])
            + list(btc_source_lot_preview.get("warnings") or [])
        )
        if str(item or "").strip()
    ]
    blockers.extend(
        str(item)
        for item in (btc_source_lot_preview.get("blockers") or [])
        if str(item or "").strip()
    )
    if historical_btc_usd is None:
        blockers.append("historical_btc_usd_price_required")
    if fee_satoshis is None:
        blockers.append("bitcoin_miner_fee_unresolved")
    if tx_event_count > 1:
        blockers.append("bitcoin_fee_allocation_required")
    if confirmed_at is None:
        blockers.append("confirmed_timestamp_missing")
    if existing.get("asset_deposit_rows") or existing.get("asset_withdrawal_rows") or existing.get("lot_journal_rows"):
        warnings.append("Potential prior accounting rows exist for this transaction; CP-LEDGER.2 must reconcile rather than duplicate them.")
    if fee_source == "bitcoin_tx_api":
        warnings.append("Miner fee was recovered from the configured public Bitcoin transaction API because Counterparty transaction metadata did not provide a positive fee.")

    components = [
        {
            "identity": f"{event_identity}:asset_acquisition:{asset.lower()}",
            "kind": "counterparty_asset_acquisition",
            "direction": "in",
            "asset": asset,
            "quantity": float(quantity),
            "quantity_exact": _decimal_text(quantity, max_places=8),
            "venue": "counterparty",
            "wallet_id": wallet_id,
            "txid": txid_norm,
            "confirmed_at": confirmed_at,
            "block_index": block_index,
        },
        {
            "identity": f"{event_identity}:btc_consideration",
            "kind": "bitcoin_trade_consideration",
            "direction": "out",
            "asset": "BTC",
            "quantity": float(gross_btc),
            "quantity_exact": _decimal_text(gross_btc, max_places=8),
            "satoshis": int(gross_satoshis),
            "counterparty": event.get("counterparty_dispenser"),
            "venue": proposed_btc_scope.get("venue"),
            "wallet_id": proposed_btc_scope.get("wallet_id"),
            "wallet_address_id": proposed_btc_scope.get("wallet_address_id"),
            "custody_scope_status": btc_custody_scope.get("status"),
            "txid": txid_norm,
            "confirmed_at": confirmed_at,
            "block_index": block_index,
        },
        {
            "identity": f"bitcoin:tx:{txid_norm}:network_fee",
            "kind": "bitcoin_network_fee",
            "direction": "out",
            "asset": "BTC",
            "quantity": float(fee_btc) if fee_btc is not None else None,
            "quantity_exact": _decimal_text(fee_btc, max_places=8),
            "satoshis": int(fee_satoshis) if fee_satoshis is not None else None,
            "fee_source": fee_source,
            "allocation_policy": fee_allocation_policy,
            "allocated_to_this_dispense_satoshis": int(fee_allocated_satoshis) if fee_allocated_satoshis is not None else None,
            "venue": proposed_btc_scope.get("venue"),
            "wallet_id": proposed_btc_scope.get("wallet_id"),
            "wallet_address_id": proposed_btc_scope.get("wallet_address_id"),
            "custody_scope_status": btc_custody_scope.get("status"),
            "txid": txid_norm,
            "confirmed_at": confirmed_at,
            "block_index": block_index,
        },
    ]

    return {
        "ok": True,
        "version": "counterparty_ledger_preview_v4",
        "status": "review_required",
        "read_only": True,
        "dry_run": True,
        "database_mutation": False,
        "deposit_mutation": False,
        "withdrawal_mutation": False,
        "ledger_mutation": False,
        "lot_mutation": False,
        "fifo_mutation": False,
        "basis_mutation": False,
        "signing": False,
        "broadcast": False,
        "venue": "counterparty",
        "wallet_id": wallet_id,
        "wallet_address": address,
        "wallet_address_id": source.get("wallet_address_id"),
        "address_source": source.get("address_source"),
        "event": "DISPENSE",
        "event_identity": event_identity,
        "txid": txid_norm,
        "dispense_index": event_index,
        "transaction_dispense_count": tx_event_count,
        "confirmed": confirmed,
        "block_index": block_index,
        "confirmed_at": confirmed_at,
        "asset": asset,
        "quantity": float(quantity),
        "quantity_exact": _decimal_text(quantity, max_places=8),
        "unit_price_btc": float(unit_price_btc) if unit_price_btc is not None else None,
        "unit_price_btc_exact": _decimal_text(unit_price_btc, max_places=18),
        "gross_payment_satoshis": int(gross_satoshis),
        "gross_payment_btc": float(gross_btc),
        "gross_payment_btc_exact": _decimal_text(gross_btc, max_places=8),
        "network_fee_satoshis": int(fee_satoshis) if fee_satoshis is not None else None,
        "network_fee_btc": float(fee_btc) if fee_btc is not None else None,
        "network_fee_btc_exact": _decimal_text(fee_btc, max_places=8),
        "network_fee_source": fee_source,
        "network_fee_external_lookup": external_tx,
        "total_btc_outflow_satoshis": int(total_outflow_satoshis) if total_outflow_satoshis is not None else None,
        "total_btc_outflow": float(total_outflow_btc) if total_outflow_btc is not None else None,
        "total_btc_outflow_exact": _decimal_text(total_outflow_btc, max_places=8),
        "components": components,
        "historical_price_lookup": historical_price,
        "btc_custody_scope_preview": btc_custody_scope,
        "btc_source_lot_preview": btc_source_lot_preview,
        "basis_preview": {
            "asset": asset,
            "quantity": float(quantity),
            "basis_btc_before_historical_usd_conversion": float(acquisition_basis_btc),
            "basis_btc_before_historical_usd_conversion_exact": _decimal_text(acquisition_basis_btc, max_places=8),
            "historical_btc_usd": float(historical_btc_usd) if historical_btc_usd is not None else None,
            "historical_btc_usd_exact": _decimal_text(historical_btc_usd, max_places=12),
            "historical_price_status": historical_price.get("status"),
            "historical_price_source": historical_price.get("source"),
            "historical_price_requested_at": historical_price.get("requested_at"),
            "historical_price_observation_at": historical_price.get("observation_at"),
            "historical_price_distance_s": historical_price.get("distance_s"),
            "historical_price_max_distance_s": historical_price.get("max_distance_s"),
            "historical_price_cache": historical_price.get("cache"),
            "total_basis_usd": float(total_basis_usd) if total_basis_usd is not None else None,
            "total_basis_usd_exact": _decimal_text(total_basis_usd, max_places=12),
            "cost_average_usd": float(cost_average_usd) if cost_average_usd is not None else None,
            "cost_average_usd_exact": _decimal_text(cost_average_usd, max_places=12),
            "status": "historical_btc_usd_resolved" if historical_btc_usd is not None else "historical_btc_usd_price_required",
            "network_fee_allocation_policy": fee_allocation_policy,
        },
        "btc_disposition_preview": {
            "asset": "BTC",
            "quantity_required": float(total_outflow_btc or gross_btc),
            "quantity_required_exact": _decimal_text(total_outflow_btc or gross_btc, max_places=8),
            "custody_scope_status": btc_custody_scope.get("status"),
            "custody_scope_resolved": bool(btc_custody_scope.get("resolved")),
            "custody_address": proposed_btc_scope.get("address"),
            "custody_wallet_address_id": proposed_btc_scope.get("wallet_address_id"),
            "custody_label": proposed_btc_scope.get("label"),
            "custody_network": proposed_btc_scope.get("network"),
            "custody_asset_scope": proposed_btc_scope.get("asset_scope"),
            "custody_venue": proposed_btc_scope.get("venue"),
            "custody_wallet_id": proposed_btc_scope.get("wallet_id"),
            "native_wallet_candidate_count": btc_custody_scope.get("native_bitcoin_candidate_count"),
            "native_wallet_distinct_scope_count": btc_custody_scope.get("native_bitcoin_distinct_scope_count"),
            "available_btc_lot_qty": float(btc_custody_scope.get("available_btc_lot_qty") or 0.0),
            "shortfall_btc": float(btc_custody_scope.get("shortfall_btc") or 0.0),
            "inventory_sufficient": bool(btc_custody_scope.get("inventory_sufficient")),
            "duplicate_inventory_risk": bool(btc_custody_scope.get("duplicate_inventory_risk")),
            "source_lot_status": btc_source_lot_preview.get("status"),
            "source_lot_identity": btc_source_lot_preview.get("identity"),
            "source_lot_candidate_count": len((btc_source_lot_preview.get("db_candidates") or {}).get("all_candidates") or []),
            "transaction_input_count": (btc_source_lot_preview.get("transaction_reconstruction") or {}).get("input_count"),
            "transaction_output_count": (btc_source_lot_preview.get("transaction_reconstruction") or {}).get("output_count"),
            "wallet_change_satoshis": (btc_source_lot_preview.get("transaction_reconstruction") or {}).get("wallet_change_satoshis"),
            "net_wallet_outflow_satoshis": (btc_source_lot_preview.get("transaction_reconstruction") or {}).get("net_wallet_outflow_satoshis"),
            "net_outflow_matches_expected": (btc_source_lot_preview.get("transaction_reconstruction") or {}).get("net_outflow_matches_expected"),
            "classification": "crypto_purchase_disposition_review_required",
            "fifo_policy": "underlying_bitcoin_wallet_address_scope_only",
            "universal_pooling": False,
            "historical_btc_usd_required": historical_btc_usd is None,
            "historical_btc_usd": float(historical_btc_usd) if historical_btc_usd is not None else None,
            "fifo_consumption": False,
        },
        "idempotency_preview": {
            "event_identity": event_identity,
            "asset_acquisition_identity": components[0]["identity"],
            "btc_consideration_identity": components[1]["identity"],
            "network_fee_identity": components[2]["identity"],
            "existing": existing,
        },
        "storage_review": {
            "status": "blocked_pending_cp_ledger_2_design",
            "reason": "AssetWithdrawal currently represents non-taxable transfer-out activity; the BTC purchase consideration must not be silently stored as a generic transfer.",
            "recommended": [
                "Persist one dedicated Counterparty event identity per dispense.",
                "Persist acquired-asset basis only after historical BTC/USD is resolved.",
                "Model BTC consideration as a disposal under the resolved underlying Bitcoin Wallet Addresses FIFO scope.",
                "Preserve the Bitcoin miner fee separately and allocate it once per transaction.",
                "Reconstruct the native Bitcoin funding UTXOs and carry forward linked transfer basis before FIFO consumption.",
            ],
        },
        "blockers": sorted(set(blockers)),
        "warnings": warnings,
        "source_address": source,
        "history_source": {
            "dispense_source_path": history.get("dispense_source_path"),
            "transaction_source_path": history.get("transaction_source_path"),
            "transaction_detail_lookups": history.get("transaction_detail_lookups"),
        },
    }
