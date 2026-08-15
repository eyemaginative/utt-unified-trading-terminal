# backend/app/services/cexius_basis_tax.py

from __future__ import annotations

from datetime import datetime
import hashlib
import json
import math
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import delete, exists, func, or_, select
from sqlalchemy.orm import Session

from ..adapters.cexius import CexiusAdapter
from ..models import AssetDeposit, AssetWithdrawal, BasisLot, VenueOrderRow
from ..models_lot_journal import LotJournal
from .lot_sync import (
    _create_buy_lot_if_needed,
    _consume_sell_if_needed,
    _venue_order_accounting_values,
)
from .lots_ledger import create_missing_basis_lots_from_deposits


_VENUE = "cexius"
_SOURCE = "CEXIUS_API"
_WALLET = "default"
_MAX_PAGES = 25
_PAGE_SIZE = 200
_TERMINAL_FILLED = {"filled", "done", "closed", "complete", "completed", "settled"}
_ALLOWED_JOURNAL_ACTIONS = {"BUY_LOT_CREATE", "SELL_FIFO_CONSUME"}
_ALLOWED_LOT_ORIGINS = {"BUY_FILL", "DEPOSIT"}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _upper(value: Any) -> str:
    return _text(value).upper()


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        number = float(value)
        return number if math.isfinite(number) else None
    except Exception:
        return None


def _first(row: Dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in row:
            value = row.get(name)
            if value is not None and value != "":
                return value
    return None


def _row_signature(row: Dict[str, Any]) -> str:
    blob = json.dumps(row, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _hash_identifier(value: Any) -> str:
    return hashlib.sha256(_text(value).lower().encode("utf-8")).hexdigest()


def _currency_map(adapter: CexiusAdapter) -> Dict[str, Dict[str, Optional[str]]]:
    out: Dict[str, Dict[str, Optional[str]]] = {}
    for row in adapter.fetch_currencies():
        if not isinstance(row, dict):
            continue
        currency_id = _text(_first(row, "id", "currency_id", "currencyId", "uuid"))
        if not currency_id:
            continue
        asset = adapter._asset(_first(row, "symbol", "code", "ticker", "asset", "currency"))
        if not asset:
            candidate = currency_id.upper()
            if len(candidate) <= 16 and candidate.replace("-", "").replace("_", "").isalnum():
                asset = adapter._asset(candidate)
        out[currency_id] = {
            "asset": asset or None,
            "name": _text(_first(row, "name", "label", "title")) or None,
        }
    return out


def _fetch_history_rows(adapter: CexiusAdapter) -> List[Dict[str, Any]]:
    rows_out: List[Dict[str, Any]] = []
    seen_pages = set()
    seen_rows = set()
    for page in range(1, _MAX_PAGES + 1):
        rows = adapter.fetch_transaction_history(page=page, limit=_PAGE_SIZE)
        rows = [row for row in rows if isinstance(row, dict)]
        if not rows:
            break
        page_sig = tuple(_row_signature(row) for row in rows[:10])
        if page_sig in seen_pages:
            break
        seen_pages.add(page_sig)
        for row in rows:
            sig = _row_signature(row)
            if sig not in seen_rows:
                seen_rows.add(sig)
                rows_out.append(row)
        if len(rows) < _PAGE_SIZE:
            break
    return rows_out


def _fetch_cexius_deposit_records(adapter: Optional[CexiusAdapter] = None) -> List[Dict[str, Any]]:
    adapter = adapter or CexiusAdapter()
    currencies = _currency_map(adapter)
    out: List[Dict[str, Any]] = []
    for row in _fetch_history_rows(adapter):
        if _text(_first(row, "type", "transaction_type", "transactionType")).lower() != "deposit":
            continue
        status = _text(_first(row, "status", "state"))
        if status.lower() not in {"success", "successful", "confirmed", "complete", "completed"}:
            continue
        currency_id = _text(_first(row, "currency_id", "currencyId", "currency"))
        mapped = currencies.get(currency_id, {})
        asset = _upper(mapped.get("asset"))
        qty = _safe_float(_first(row, "final_amount", "finalAmount", "amount", "qty", "quantity"))
        txid = _text(_first(row, "txid", "tx_id", "transaction_id", "transactionId", "hash"))
        created = adapter._parse_datetime(_first(row, "created_at", "createdAt", "timestamp", "time"))
        if not asset or qty is None or qty <= 0 or not txid or created is None:
            continue
        network = _text(_first(row, "network_name", "networkName", "network", "chain"))
        if not network:
            network_id = _text(_first(row, "network_id", "networkId"))
            try:
                for net in adapter.fetch_currency_networks(currency_id):
                    if not isinstance(net, dict):
                        continue
                    nid = _text(_first(net, "id", "network_id", "networkId", "uuid"))
                    if network_id and nid != network_id:
                        continue
                    network = _text(_first(net, "name", "network_name", "networkName", "chain", "symbol", "code"))
                    if network:
                        break
            except Exception:
                network = network or ""
        out.append({
            "asset": asset,
            "qty": float(qty),
            "deposit_time": created,
            "txid": txid,
            "txid_sha256": _hash_identifier(txid),
            "network": (network[:32] if network else None),
            "status": "CONFIRMED",
            "source": _SOURCE,
            "raw": {
                "type": "deposit",
                "currency_id": currency_id or None,
                "network_id": _text(_first(row, "network_id", "networkId")) or None,
                "network_name": network or None,
                "status": status or None,
                "final_amount": float(qty),
                "fee": _safe_float(_first(row, "fee", "commission")),
                "is_internal": _first(row, "is_internal", "isInternal"),
                "confirmations": _first(row, "confirmations"),
                "required_confirmations": _first(row, "required_confirmations", "requiredConfirmations"),
            },
        })
    out.sort(key=lambda item: (item["deposit_time"], item["txid_sha256"]))
    return out


def _materialize_deposit_records(
    db: Session,
    records: Iterable[Dict[str, Any]],
    *,
    wallet_id: str,
    dry_run: bool,
) -> Dict[str, Any]:
    wallet = _text(wallet_id) or _WALLET
    considered = created = existing = 0
    ids: List[str] = []
    for record in records:
        considered += 1
        txid = _text(record.get("txid"))
        current = db.execute(select(AssetDeposit).where(
            AssetDeposit.venue == _VENUE,
            AssetDeposit.wallet_id == wallet,
            AssetDeposit.txid == txid,
        )).scalars().first()
        if current is not None:
            cq = _safe_float(current.qty)
            rq = _safe_float(record.get("qty"))
            if _upper(current.asset) != _upper(record.get("asset")) or cq is None or rq is None or not math.isclose(cq, rq, rel_tol=1e-10, abs_tol=1e-12):
                raise RuntimeError("Existing Cexius deposit conflicts with authoritative transaction history")
            existing += 1
            ids.append(str(current.id))
            continue
        if dry_run:
            created += 1
            continue
        dep = AssetDeposit(
            venue=_VENUE,
            wallet_id=wallet,
            asset=_upper(record.get("asset")),
            qty=float(record["qty"]),
            deposit_time=record["deposit_time"],
            txid=txid,
            network=record.get("network"),
            status="CONFIRMED",
            source=_SOURCE,
            note="Cexius API deposit; source cost basis unavailable",
            raw=record.get("raw"),
        )
        db.add(dep)
        db.flush()
        created += 1
        ids.append(str(dep.id))
    return {"considered": considered, "created": created, "existing": existing, "deposit_ids": ids}


def sync_cexius_deposits(
    db: Session,
    *,
    adapter: Optional[CexiusAdapter] = None,
    wallet_id: str = _WALLET,
    dry_run: bool = True,
    commit: bool = True,
) -> Dict[str, Any]:
    records = _fetch_cexius_deposit_records(adapter)
    try:
        materialized = _materialize_deposit_records(db, records, wallet_id=wallet_id, dry_run=dry_run)
        lot_result = create_missing_basis_lots_from_deposits(
            db,
            venue=_VENUE,
            wallet_id=wallet_id,
            status="CONFIRMED",
            source_contains=_SOURCE,
            limit=5000,
            dry_run=dry_run,
        )
        result = {
            "version": "cexius_deposit_sync_v1",
            "dry_run": bool(dry_run),
            "history_deposits": len(records),
            "materialized": materialized,
            "lots": lot_result,
        }
        if not dry_run and commit:
            db.commit()
        return result
    except Exception:
        if not dry_run and commit:
            db.rollback()
        raise


def _effective_at(row: VenueOrderRow) -> datetime:
    return row.updated_at or row.created_at or row.captured_at or datetime.utcnow()


def _cexius_fill_rows(db: Session) -> List[VenueOrderRow]:
    filled = func.coalesce(VenueOrderRow.filled_qty, 0.0)
    avg = func.coalesce(VenueOrderRow.avg_fill_price, 0.0)
    rows = db.execute(select(VenueOrderRow).where(
        func.lower(VenueOrderRow.venue) == _VENUE,
        or_(filled > 0.0, avg > 0.0),
    )).scalars().all()
    rows.sort(key=lambda row: (_effective_at(row), 0 if _text(row.side).lower() == "buy" else 1, str(row.id)))
    return rows


def _safety_snapshot(db: Session, rows: List[VenueOrderRow], wallet_id: str) -> Dict[str, Any]:
    wallet = _text(wallet_id) or _WALLET
    row_ids = {str(row.id) for row in rows}
    buy_ids = {str(row.id) for row in rows if _text(row.side).lower() == "buy"}
    statuses = sorted({_text(row.status).lower() for row in rows})
    journals = db.execute(select(LotJournal).where(
        func.lower(LotJournal.venue) == _VENUE,
        LotJournal.origin_type == "VENUE_ORDER_AGG",
    )).scalars().all()
    lots = db.execute(select(BasisLot).where(
        BasisLot.venue == _VENUE,
        BasisLot.wallet_id == wallet,
    )).scalars().all()
    deposits = db.execute(select(AssetDeposit).where(
        AssetDeposit.venue == _VENUE,
        AssetDeposit.wallet_id == wallet,
    )).scalars().all()
    withdrawals = db.execute(select(AssetWithdrawal).where(
        AssetWithdrawal.venue == _VENUE,
        AssetWithdrawal.wallet_id == wallet,
    )).scalars().all()
    deposit_ids = {str(dep.id) for dep in deposits}
    unexpected_journals = [
        str(j.id) for j in journals
        if str(j.origin_ref) not in row_ids or str(j.action) not in _ALLOWED_JOURNAL_ACTIONS
    ]
    unexpected_lots = [
        str(lot.id) for lot in lots
        if str(lot.origin_type) not in _ALLOWED_LOT_ORIGINS
        or (str(lot.origin_type) == "BUY_FILL" and str(lot.origin_ref) not in buy_ids)
        or (str(lot.origin_type) == "DEPOSIT" and str(lot.origin_ref) not in deposit_ids)
    ]
    nonfilled_candidates = [str(row.id) for row in rows if _text(row.status).lower() not in _TERMINAL_FILLED]
    return {
        "row_count": len(rows),
        "statuses": statuses,
        "journal_count": len(journals),
        "lot_count": len(lots),
        "deposit_count": len(deposits),
        "withdrawal_count": len(withdrawals),
        "unexpected_journal_ids": unexpected_journals,
        "unexpected_lot_ids": unexpected_lots,
        "nonfilled_candidate_ids": nonfilled_candidates,
        "safe_to_apply": not unexpected_journals and not unexpected_lots and not nonfilled_candidates and not withdrawals,
    }


def _simulate_cexius_replay(rows: Iterable[VenueOrderRow], deposits: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    events: List[Tuple[datetime, int, str, Dict[str, Any]]] = []
    for dep in deposits:
        events.append((dep["deposit_time"], 0, dep["txid_sha256"], {
            "kind": "deposit", "asset": dep["asset"], "qty": float(dep["qty"]), "basis": None,
        }))
    for row in rows:
        values = _venue_order_accounting_values(row)
        side = values.get("side")
        qty = _safe_float(values.get("qty")) or 0.0
        if qty <= 0 or not values.get("base"):
            continue
        if side == "buy":
            basis = None
            p = _safe_float(values.get("price_usd"))
            f = _safe_float(values.get("fee_usd"))
            if p is not None and p > 0:
                basis = qty * p + float(f or 0.0)
            payload = {"kind": "buy", "asset": values["base"], "qty": qty, "basis": basis}
            priority = 1
        elif side == "sell":
            payload = {
                "kind": "sell", "asset": values["base"], "qty": qty,
                "price_usd": _safe_float(values.get("price_usd")), "fee_usd": _safe_float(values.get("fee_usd")),
            }
            priority = 2
        else:
            continue
        events.append((_effective_at(row), priority, str(row.id), payload))
    events.sort(key=lambda item: (item[0], item[1], item[2]))
    lots: Dict[str, List[Dict[str, Any]]] = {}
    sells = complete = basis_missing = insufficient = 0
    by_asset: Dict[str, Dict[str, int]] = {}
    for _, _, _, event in events:
        asset = _upper(event["asset"])
        lots.setdefault(asset, [])
        by_asset.setdefault(asset, {"sells": 0, "complete": 0, "basis_missing": 0, "insufficient": 0})
        if event["kind"] in {"deposit", "buy"}:
            lots[asset].append({"remaining": float(event["qty"]), "qty_total": float(event["qty"]), "basis": event.get("basis")})
            continue
        sells += 1
        by_asset[asset]["sells"] += 1
        required = float(event["qty"])
        available = sum(max(0.0, lot["remaining"]) for lot in lots[asset])
        if available + 1e-12 < required:
            insufficient += 1
            by_asset[asset]["insufficient"] += 1
            continue
        rem = required
        missing = False
        for lot in lots[asset]:
            if rem <= 1e-12:
                break
            take = min(max(0.0, lot["remaining"]), rem)
            if take <= 0:
                continue
            if lot.get("basis") is None:
                missing = True
            lot["remaining"] -= take
            rem -= take
        if missing:
            basis_missing += 1
            by_asset[asset]["basis_missing"] += 1
        else:
            complete += 1
            by_asset[asset]["complete"] += 1
    return {
        "sell_total": sells,
        "realized_complete": complete,
        "basis_missing": basis_missing,
        "insufficient": insufficient,
        "by_asset": by_asset,
    }


def _preview_fingerprint(rows: List[VenueOrderRow], deposits: List[Dict[str, Any]], replay: Dict[str, Any]) -> str:
    payload = {
        "orders": [
            {
                "id": str(row.id), "status": _text(row.status), "side": _text(row.side),
                "symbol": _text(row.symbol_canon or row.symbol_venue),
                "filled_qty": _safe_float(row.filled_qty), "fee": _safe_float(row.fee),
                "fee_asset": _text(row.fee_asset), "total_after_fee": _safe_float(row.total_after_fee),
                "effective_at": _effective_at(row).isoformat(),
            }
            for row in rows
        ],
        "deposits": [
            {"txid_sha256": dep["txid_sha256"], "asset": dep["asset"], "qty": dep["qty"], "time": dep["deposit_time"].isoformat()}
            for dep in deposits
        ],
        "replay": replay,
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def preview_cexius_basis_rebuild(
    db: Session,
    *,
    adapter: Optional[CexiusAdapter] = None,
    wallet_id: str = _WALLET,
) -> Dict[str, Any]:
    rows = _cexius_fill_rows(db)
    deposits = _fetch_cexius_deposit_records(adapter)
    safety = _safety_snapshot(db, rows, wallet_id)
    replay = _simulate_cexius_replay(rows, deposits)
    fingerprint = _preview_fingerprint(rows, deposits, replay)
    return {
        "version": "cexius_basis_rebuild_preview_v1",
        "dry_run": True,
        "safe_to_apply": bool(safety["safe_to_apply"] and replay["insufficient"] == 0),
        "safety": safety,
        "history_deposits": len(deposits),
        "filled_buys": sum(1 for row in rows if _text(row.side).lower() == "buy"),
        "filled_sells": sum(1 for row in rows if _text(row.side).lower() == "sell"),
        "replay": replay,
        "fingerprint": fingerprint,
    }


def apply_cexius_basis_rebuild(
    db: Session,
    *,
    expected_fingerprint: str,
    confirm: bool = False,
    adapter: Optional[CexiusAdapter] = None,
    wallet_id: str = _WALLET,
) -> Dict[str, Any]:
    if not confirm:
        raise RuntimeError("Cexius historical rebuild requires confirm=True")
    preview = preview_cexius_basis_rebuild(db, adapter=adapter, wallet_id=wallet_id)
    if not preview.get("safe_to_apply"):
        raise RuntimeError("Cexius historical rebuild safety guard failed")
    if _text(expected_fingerprint).lower() != _text(preview.get("fingerprint")).lower():
        raise RuntimeError("Cexius historical rebuild fingerprint changed; rerun preview")

    wallet = _text(wallet_id) or _WALLET
    records = _fetch_cexius_deposit_records(adapter)
    rows = _cexius_fill_rows(db)
    apply_replay = _simulate_cexius_replay(rows, records)
    apply_fingerprint = _preview_fingerprint(rows, records, apply_replay)
    if _text(expected_fingerprint).lower() != apply_fingerprint.lower():
        raise RuntimeError("Cexius history changed after preview; rerun preview before apply")
    row_ids = [str(row.id) for row in rows]
    buy_ids = [str(row.id) for row in rows if _text(row.side).lower() == "buy"]

    try:
        materialized = _materialize_deposit_records(db, records, wallet_id=wallet, dry_run=False)
        deposit_lots = create_missing_basis_lots_from_deposits(
            db, venue=_VENUE, wallet_id=wallet, status="CONFIRMED", source_contains=_SOURCE,
            limit=5000, dry_run=False,
        )
        db.flush()

        # Re-check after adding the accepted Cexius deposit history.
        safety = _safety_snapshot(db, rows, wallet)
        if not safety.get("safe_to_apply"):
            raise RuntimeError("Cexius historical rebuild safety guard changed after deposit materialization")

        if row_ids:
            db.execute(delete(LotJournal).where(
                LotJournal.origin_type == "VENUE_ORDER_AGG",
                LotJournal.origin_ref.in_(row_ids),
                func.lower(LotJournal.venue) == _VENUE,
            ))
        if buy_ids:
            db.execute(delete(BasisLot).where(
                BasisLot.venue == _VENUE,
                BasisLot.wallet_id == wallet,
                BasisLot.origin_type == "BUY_FILL",
                BasisLot.origin_ref.in_(buy_ids),
            ))

        # Deposits are durable source inventory; reset only their quantities because D1-D4
        # proved no Cexius withdrawals exist. The safety guard refuses apply if that changes.
        dep_ids = [str(dep.id) for dep in db.execute(select(AssetDeposit).where(
            AssetDeposit.venue == _VENUE, AssetDeposit.wallet_id == wallet,
        )).scalars().all()]
        if dep_ids:
            for lot in db.execute(select(BasisLot).where(
                BasisLot.venue == _VENUE,
                BasisLot.wallet_id == wallet,
                BasisLot.origin_type == "DEPOSIT",
                BasisLot.origin_ref.in_(dep_ids),
            )).scalars().all():
                lot.qty_remaining = float(lot.qty_total)
                db.add(lot)
        db.flush()

        created = consumed = basis_missing = insufficient = 0
        for row in rows:
            values = _venue_order_accounting_values(row)
            qty = float(values.get("qty") or 0.0)
            price = _safe_float(values.get("price_usd"))
            fee_usd = _safe_float(values.get("fee_usd"))
            base = values.get("base")
            side = values.get("side")
            if qty <= 0 or not base or side not in {"buy", "sell"}:
                raise RuntimeError(f"Unsupported Cexius accounting row during rebuild: {row.id}")
            when = _effective_at(row)
            if side == "buy":
                result = _create_buy_lot_if_needed(
                    db, venue=_VENUE, wallet_id=wallet, asset=base, qty=qty,
                    price_usd=price, fee_usd=fee_usd, acquired_at=when,
                    origin_type="VENUE_ORDER_AGG", origin_ref=str(row.id),
                    note="auto lot from Cexius venue aggregate fill", dry_run=False,
                )
                if result.get("skipped"):
                    raise RuntimeError(f"Unexpected Cexius BUY rebuild skip: {result.get('reason')}")
                created += 1
            else:
                result = _consume_sell_if_needed(
                    db, venue=_VENUE, wallet_id=wallet, asset=base, qty=qty,
                    price_usd=price, fee_usd=fee_usd, effective_at=when,
                    origin_type="VENUE_ORDER_AGG", origin_ref=str(row.id), dry_run=False,
                )
                if result.get("skipped"):
                    insufficient += 1
                else:
                    consumed += 1
                    impact = result.get("impact") if isinstance(result.get("impact"), dict) else {}
                    if impact.get("realized_gain_usd") is None:
                        basis_missing += 1

        if insufficient:
            raise RuntimeError(f"Cexius historical rebuild produced {insufficient} insufficient-inventory sells")
        db.commit()
        return {
            "version": "cexius_basis_rebuild_apply_v1",
            "applied": True,
            "fingerprint": preview["fingerprint"],
            "deposits": materialized,
            "deposit_lots": deposit_lots,
            "buy_lots_created": created,
            "sell_total": consumed,
            "sell_realized_complete": consumed - basis_missing,
            "sell_basis_missing": basis_missing,
            "sell_insufficient": insufficient,
        }
    except Exception:
        db.rollback()
        raise
