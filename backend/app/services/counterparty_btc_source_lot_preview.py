from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
import os
import re
from typing import Any, Dict, List, Optional, Sequence

import httpx
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..models import AssetDeposit, AssetWithdrawal, BasisLot, WalletAddressTx


_SATOSHIS_PER_BTC = Decimal("100000000")
_TXID_RE = re.compile(r"^[0-9a-f]{64}$")
_TX_CACHE: Dict[str, Dict[str, Any]] = {}
_TX_CACHE_MAX = 256


def _norm_txid(value: Any) -> Optional[str]:
    txid = str(value or "").strip().lower()
    return txid if _TXID_RE.fullmatch(txid) else None


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


def _safe_float(value: Any) -> float:
    try:
        out = float(value or 0.0)
    except Exception:
        return 0.0
    return out if out == out and out not in (float("inf"), float("-inf")) else 0.0


def _btc_from_sats(value: Any) -> Optional[float]:
    sats = _as_int(value)
    if sats is None:
        return None
    return float(Decimal(sats) / _SATOSHIS_PER_BTC)


def _iso(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        dt = value
    elif value not in (None, ""):
        try:
            if isinstance(value, (int, float)) or str(value).isdigit():
                dt = datetime.fromtimestamp(int(value), tz=timezone.utc)
            else:
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


def _extract_tx_payload(raw: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        return None
    if isinstance(raw.get("vin"), list) and isinstance(raw.get("vout"), list):
        return raw
    for key in ("raw", "tx", "transaction", "result", "data"):
        nested = raw.get(key)
        if isinstance(nested, dict):
            found = _extract_tx_payload(nested)
            if found is not None:
                return found
    return None


def _cache_put(txid: str, payload: Dict[str, Any]) -> None:
    if len(_TX_CACHE) >= _TX_CACHE_MAX:
        try:
            _TX_CACHE.pop(next(iter(_TX_CACHE)))
        except Exception:
            _TX_CACHE.clear()
    _TX_CACHE[txid] = dict(payload)


def _fetch_bitcoin_tx(txid: str, *, supplied_raw: Optional[Dict[str, Any]] = None, allow_external_lookup: bool = True) -> Dict[str, Any]:
    txid_norm = _norm_txid(txid)
    if not txid_norm:
        return {"ok": False, "status": "invalid_txid", "txid": str(txid or ""), "cache": "none"}

    supplied = _extract_tx_payload(supplied_raw)
    if supplied is not None:
        _cache_put(txid_norm, supplied)
        return {
            "ok": True,
            "status": "resolved",
            "txid": txid_norm,
            "source": "supplied_transaction_payload",
            "cache": "supplied",
            "raw": supplied,
        }

    cached = _TX_CACHE.get(txid_norm)
    if isinstance(cached, dict):
        return {
            "ok": True,
            "status": "resolved",
            "txid": txid_norm,
            "source": "bitcoin_tx_api",
            "cache": "hit",
            "raw": dict(cached),
        }

    if not allow_external_lookup:
        return {
            "ok": False,
            "status": "external_lookup_disabled",
            "txid": txid_norm,
            "source": "bitcoin_tx_api",
            "cache": "miss",
        }

    url = f"{_bitcoin_tx_api_base_url()}/tx/{txid_norm}"
    try:
        with httpx.Client(
            timeout=_bitcoin_tx_lookup_timeout_s(),
            headers={"accept": "application/json", "user-agent": "UTT Counterparty BTC source-lot preview/1.0"},
            follow_redirects=True,
        ) as client:
            response = client.get(url)
        if response.status_code >= 400:
            return {
                "ok": False,
                "status": "lookup_failed",
                "txid": txid_norm,
                "source": "bitcoin_tx_api",
                "url": url,
                "cache": "miss",
                "error": f"HTTP {response.status_code}",
                "body_preview": str(response.text or "")[:300],
            }
        data = response.json()
        payload = _extract_tx_payload(data)
        if payload is None:
            return {
                "ok": False,
                "status": "unexpected_response",
                "txid": txid_norm,
                "source": "bitcoin_tx_api",
                "url": url,
                "cache": "miss",
            }
        _cache_put(txid_norm, payload)
        return {
            "ok": True,
            "status": "resolved",
            "txid": txid_norm,
            "source": "bitcoin_tx_api",
            "url": url,
            "cache": "miss",
            "raw": payload,
        }
    except Exception as exc:
        return {
            "ok": False,
            "status": "lookup_failed",
            "txid": txid_norm,
            "source": "bitcoin_tx_api",
            "url": url,
            "cache": "miss",
            "error": f"{type(exc).__name__}: {exc}"[:500],
        }


def _address_from_script(node: Any) -> Optional[str]:
    if not isinstance(node, dict):
        return None
    for key in ("scriptpubkey_address", "recipient", "address"):
        value = str(node.get(key) or "").strip()
        if value:
            return value
    addresses = node.get("addresses")
    if isinstance(addresses, list) and addresses:
        value = str(addresses[0] or "").strip()
        if value:
            return value
    return None


def _script_type(node: Any) -> Optional[str]:
    if not isinstance(node, dict):
        return None
    return str(node.get("scriptpubkey_type") or node.get("type") or "").strip() or None


def _script_asm(node: Any) -> Optional[str]:
    if not isinstance(node, dict):
        return None
    return str(node.get("scriptpubkey_asm") or node.get("asm") or "").strip() or None


def _fee_from_payload(payload: Dict[str, Any]) -> Optional[int]:
    direct = _as_int(payload.get("fee"))
    if direct is not None and direct >= 0:
        return direct
    vin = payload.get("vin") if isinstance(payload.get("vin"), list) else []
    vout = payload.get("vout") if isinstance(payload.get("vout"), list) else []
    values_in = [_as_int((row.get("prevout") or {}).get("value")) for row in vin if isinstance(row, dict)]
    values_out = [_as_int(row.get("value")) for row in vout if isinstance(row, dict)]
    if values_in and all(v is not None for v in values_in) and all(v is not None for v in values_out):
        fee = sum(int(v or 0) for v in values_in) - sum(int(v or 0) for v in values_out)
        return fee if fee >= 0 else None
    return None


def _parse_target_transaction(
    payload: Dict[str, Any],
    *,
    txid: str,
    custody_address: str,
    dispenser_address: Optional[str],
    expected_gross_satoshis: Optional[int],
    expected_fee_satoshis: Optional[int],
) -> Dict[str, Any]:
    vin = payload.get("vin") if isinstance(payload.get("vin"), list) else []
    vout = payload.get("vout") if isinstance(payload.get("vout"), list) else []
    expected_gross = _as_int(expected_gross_satoshis)
    expected_fee = _as_int(expected_fee_satoshis)
    dispenser = str(dispenser_address or "").strip()

    inputs: List[Dict[str, Any]] = []
    for index, row in enumerate(vin):
        if not isinstance(row, dict):
            continue
        prevout = row.get("prevout") if isinstance(row.get("prevout"), dict) else {}
        prev_txid = _norm_txid(row.get("txid"))
        prev_vout = _as_int(row.get("vout"))
        value_sats = _as_int(prevout.get("value"))
        address = _address_from_script(prevout)
        inputs.append(
            {
                "index": index,
                "identity": f"bitcoin:utxo:{prev_txid}:{prev_vout}" if prev_txid is not None and prev_vout is not None else f"bitcoin:input:{txid}:{index}",
                "prev_txid": prev_txid,
                "prev_vout": prev_vout,
                "address": address,
                "value_satoshis": value_sats,
                "value_btc": _btc_from_sats(value_sats),
                "owned_by_custody": bool(address and address == custody_address),
                "script_type": _script_type(prevout),
                "sequence": _as_int(row.get("sequence")),
                "coinbase": bool(row.get("is_coinbase") or row.get("coinbase")),
            }
        )

    outputs: List[Dict[str, Any]] = []
    value_match_indexes = []
    for index, row in enumerate(vout):
        if not isinstance(row, dict):
            continue
        value_sats = _as_int(row.get("value"))
        address = _address_from_script(row)
        script_type = _script_type(row)
        asm = _script_asm(row)
        if expected_gross is not None and value_sats == expected_gross:
            value_match_indexes.append(index)
        is_op_return = str(script_type or "").lower() in {"op_return", "nulldata"} or str(asm or "").upper().startswith("OP_RETURN")
        classification = "external_output"
        if address and address == custody_address:
            classification = "wallet_change"
        elif is_op_return:
            classification = "counterparty_data"
        elif dispenser and address == dispenser:
            classification = "dispenser_payment"
        outputs.append(
            {
                "index": index,
                "identity": f"bitcoin:output:{txid}:{index}",
                "classification": classification,
                "address": address,
                "value_satoshis": value_sats,
                "value_btc": _btc_from_sats(value_sats),
                "script_type": script_type,
                "script_asm": asm,
                "is_change": bool(address and address == custody_address),
                "is_counterparty_data": is_op_return,
            }
        )

    if not dispenser and len(value_match_indexes) == 1:
        match_index = value_match_indexes[0]
        for output in outputs:
            if output["index"] == match_index and output["classification"] == "external_output":
                output["classification"] = "dispenser_payment_value_match"

    input_values = [row.get("value_satoshis") for row in inputs]
    output_values = [row.get("value_satoshis") for row in outputs]
    input_value_complete = bool(inputs) and all(value is not None for value in input_values)
    output_value_complete = bool(outputs) and all(value is not None for value in output_values)
    total_input_sats = sum(int(value or 0) for value in input_values) if input_value_complete else None
    total_output_sats = sum(int(value or 0) for value in output_values) if output_value_complete else None
    fee_sats = _fee_from_payload(payload)
    owned_input_sats = sum(int(row.get("value_satoshis") or 0) for row in inputs if row.get("owned_by_custody"))
    change_sats = sum(int(row.get("value_satoshis") or 0) for row in outputs if row.get("is_change"))
    net_wallet_outflow_sats = owned_input_sats - change_sats if owned_input_sats > 0 else None
    expected_total_sats = expected_gross + expected_fee if expected_gross is not None and expected_fee is not None else None
    payment_sats = sum(
        int(row.get("value_satoshis") or 0)
        for row in outputs
        if row.get("classification") in {"dispenser_payment", "dispenser_payment_value_match"}
    )

    return {
        "ok": bool(inputs and outputs),
        "status": "reconstructed" if inputs and outputs else "transaction_shape_incomplete",
        "txid": txid,
        "confirmed": bool((payload.get("status") or {}).get("confirmed")) if isinstance(payload.get("status"), dict) else None,
        "block_height": _as_int((payload.get("status") or {}).get("block_height")) if isinstance(payload.get("status"), dict) else None,
        "block_time": _iso((payload.get("status") or {}).get("block_time")) if isinstance(payload.get("status"), dict) else None,
        "input_count": len(inputs),
        "output_count": len(outputs),
        "inputs": inputs,
        "outputs": outputs,
        "input_value_complete": input_value_complete,
        "output_value_complete": output_value_complete,
        "total_input_satoshis": total_input_sats,
        "total_input_btc": _btc_from_sats(total_input_sats),
        "total_output_satoshis": total_output_sats,
        "total_output_btc": _btc_from_sats(total_output_sats),
        "fee_satoshis": fee_sats,
        "fee_btc": _btc_from_sats(fee_sats),
        "expected_fee_satoshis": expected_fee,
        "fee_matches_preview": (fee_sats == expected_fee) if fee_sats is not None and expected_fee is not None else None,
        "owned_input_satoshis": owned_input_sats,
        "owned_input_btc": _btc_from_sats(owned_input_sats),
        "wallet_change_satoshis": change_sats,
        "wallet_change_btc": _btc_from_sats(change_sats),
        "net_wallet_outflow_satoshis": net_wallet_outflow_sats,
        "net_wallet_outflow_btc": _btc_from_sats(net_wallet_outflow_sats),
        "expected_gross_satoshis": expected_gross,
        "expected_total_outflow_satoshis": expected_total_sats,
        "expected_total_outflow_btc": _btc_from_sats(expected_total_sats),
        "net_outflow_matches_expected": (net_wallet_outflow_sats == expected_total_sats) if net_wallet_outflow_sats is not None and expected_total_sats is not None else None,
        "payment_satoshis_to_dispenser": payment_sats,
        "payment_btc_to_dispenser": _btc_from_sats(payment_sats),
        "payment_matches_preview": (payment_sats == expected_gross) if expected_gross is not None else None,
        "all_inputs_owned_by_custody": bool(inputs) and all(bool(row.get("owned_by_custody")) for row in inputs),
        "owned_input_count": sum(1 for row in inputs if row.get("owned_by_custody")),
    }


def _raw_contains_txid(raw: Any, txids: Sequence[str], *, depth: int = 0) -> bool:
    if depth > 6:
        return False
    wanted = set(txids)
    if isinstance(raw, str):
        return raw.strip().lower() in wanted
    if isinstance(raw, dict):
        return any(_raw_contains_txid(value, txids, depth=depth + 1) for value in raw.values())
    if isinstance(raw, (list, tuple)):
        return any(_raw_contains_txid(value, txids, depth=depth + 1) for value in raw)
    return False


def _row_summary(row: Any, *, kind: str, match_type: str) -> Dict[str, Any]:
    when = getattr(row, "deposit_time", None) or getattr(row, "withdraw_time", None) or getattr(row, "tx_time", None)
    return {
        "kind": kind,
        "match_type": match_type,
        "id": str(getattr(row, "id", "") or "") or None,
        "venue": str(getattr(row, "venue", "") or "") or None,
        "wallet_id": str(getattr(row, "wallet_id", "") or "") or None,
        "wallet_address_id": str(getattr(row, "wallet_address_id", "") or "") or None,
        "asset": str(getattr(row, "asset", "") or "") or None,
        "qty": _safe_float(getattr(row, "qty", None) if hasattr(row, "qty") else getattr(row, "amount", None)),
        "txid": str(getattr(row, "txid", "") or "") or None,
        "direction": str(getattr(row, "direction", "") or "") or None,
        "time": _iso(when),
        "status": str(getattr(row, "status", "") or "") or None,
        "source": str(getattr(row, "source", "") or "") or None,
        "deposit_id": str(getattr(row, "deposit_id", "") or "") or None,
        "withdrawal_id": str(getattr(row, "withdrawal_id", "") or "") or None,
        "transfer_withdrawal_id": str(getattr(row, "transfer_withdrawal_id", "") or "") or None,
        "transfer_deposit_id": str(getattr(row, "transfer_deposit_id", "") or "") or None,
        "ingested_to_ledger_at": _iso(getattr(row, "ingested_to_ledger_at", None)),
    }


def _find_db_candidates(
    db: Session,
    *,
    parent_txids: Sequence[str],
    custody_wallet_address_id: Optional[str],
) -> Dict[str, Any]:
    txids = [txid for txid in dict.fromkeys(parent_txids) if _norm_txid(txid)]
    if not txids:
        return {"wallet_address_txs": [], "deposits": [], "withdrawals": [], "all_candidates": []}

    wallet_rows: List[Any] = []
    if custody_wallet_address_id:
        try:
            wallet_rows = list(
                db.execute(
                    select(WalletAddressTx).where(
                        WalletAddressTx.wallet_address_id == str(custody_wallet_address_id),
                        WalletAddressTx.txid.in_(txids),
                    )
                ).scalars().all()
            )
        except Exception:
            wallet_rows = []

    try:
        deposit_pool = list(
            db.execute(
                select(AssetDeposit)
                .where(func.upper(AssetDeposit.asset) == "BTC")
                .order_by(AssetDeposit.deposit_time.desc())
                .limit(5000)
            ).scalars().all()
        )
    except Exception:
        deposit_pool = []

    try:
        withdrawal_pool = list(
            db.execute(
                select(AssetWithdrawal)
                .where(func.upper(AssetWithdrawal.asset) == "BTC")
                .order_by(AssetWithdrawal.withdraw_time.desc())
                .limit(5000)
            ).scalars().all()
        )
    except Exception:
        withdrawal_pool = []

    deposits: List[Dict[str, Any]] = []
    for row in deposit_pool:
        row_txid = _norm_txid(getattr(row, "txid", None))
        if row_txid in txids:
            deposits.append(_row_summary(row, kind="asset_deposit", match_type="exact_txid"))
        elif _raw_contains_txid(getattr(row, "raw", None), txids):
            deposits.append(_row_summary(row, kind="asset_deposit", match_type="raw_txid_reference"))

    withdrawals: List[Dict[str, Any]] = []
    for row in withdrawal_pool:
        row_txid = _norm_txid(getattr(row, "txid", None))
        if row_txid in txids:
            withdrawals.append(_row_summary(row, kind="asset_withdrawal", match_type="exact_txid"))
        elif _raw_contains_txid(getattr(row, "raw", None), txids):
            withdrawals.append(_row_summary(row, kind="asset_withdrawal", match_type="raw_txid_reference"))

    wallet_summaries = [_row_summary(row, kind="wallet_address_tx", match_type="exact_txid") for row in wallet_rows]
    all_candidates = [*wallet_summaries, *deposits, *withdrawals]
    return {
        "wallet_address_txs": wallet_summaries,
        "deposits": deposits,
        "withdrawals": withdrawals,
        "all_candidates": all_candidates,
    }


def _native_lots(
    db: Session,
    *,
    custody_venue: Optional[str],
    custody_wallet_id: Optional[str],
) -> List[Dict[str, Any]]:
    if not custody_venue or not custody_wallet_id:
        return []
    try:
        rows = list(
            db.execute(
                select(BasisLot)
                .where(
                    func.lower(BasisLot.venue) == str(custody_venue).lower(),
                    BasisLot.wallet_id == str(custody_wallet_id),
                    func.upper(BasisLot.asset) == "BTC",
                )
                .order_by(BasisLot.acquired_at.asc(), BasisLot.created_at.asc(), BasisLot.id.asc())
            ).scalars().all()
        )
    except Exception:
        rows = []
    return [
        {
            "id": str(getattr(row, "id", "") or "") or None,
            "venue": str(getattr(row, "venue", "") or "") or None,
            "wallet_id": str(getattr(row, "wallet_id", "") or "") or None,
            "asset": str(getattr(row, "asset", "") or "") or None,
            "acquired_at": _iso(getattr(row, "acquired_at", None)),
            "qty_total": _safe_float(getattr(row, "qty_total", None)),
            "qty_remaining": _safe_float(getattr(row, "qty_remaining", None)),
            "total_basis_usd": getattr(row, "total_basis_usd", None),
            "basis_is_missing": bool(getattr(row, "basis_is_missing", False) or getattr(row, "total_basis_usd", None) is None),
            "basis_source": str(getattr(row, "basis_source", "") or "") or None,
            "origin_type": str(getattr(row, "origin_type", "") or "") or None,
            "origin_ref": str(getattr(row, "origin_ref", "") or "") or None,
        }
        for row in rows
    ]


def _parent_summary(result: Dict[str, Any], *, input_row: Dict[str, Any], custody_address: str) -> Dict[str, Any]:
    raw = result.get("raw") if isinstance(result.get("raw"), dict) else {}
    parent_inputs = raw.get("vin") if isinstance(raw.get("vin"), list) else []
    source_addresses: List[str] = []
    for vin in parent_inputs:
        if not isinstance(vin, dict):
            continue
        address = _address_from_script(vin.get("prevout") if isinstance(vin.get("prevout"), dict) else {})
        if address and address not in source_addresses:
            source_addresses.append(address)
    status = raw.get("status") if isinstance(raw.get("status"), dict) else {}
    return {
        "input_identity": input_row.get("identity"),
        "txid": input_row.get("prev_txid"),
        "vout": input_row.get("prev_vout"),
        "value_satoshis": input_row.get("value_satoshis"),
        "value_btc": input_row.get("value_btc"),
        "lookup_ok": bool(result.get("ok")),
        "lookup_status": result.get("status"),
        "lookup_source": result.get("source"),
        "lookup_cache": result.get("cache"),
        "lookup_error": result.get("error"),
        "confirmed": bool(status.get("confirmed")) if status else None,
        "block_height": _as_int(status.get("block_height")) if status else None,
        "block_time": _iso(status.get("block_time")) if status else None,
        "source_addresses": source_addresses,
        "source_address_count": len(source_addresses),
        "funding_output_address": input_row.get("address"),
        "funding_output_is_custody": input_row.get("address") == custody_address,
        "parent_fee_satoshis": _fee_from_payload(raw) if raw else None,
    }


def _recovery_status(
    *,
    custody_resolved: bool,
    tx_ok: bool,
    tx_confirmed: Optional[bool],
    economics_match: Optional[bool],
    available_btc: float,
    required_btc: float,
    db_candidates: Dict[str, Any],
) -> str:
    if not custody_resolved:
        return "custody_scope_unresolved"
    if not tx_ok:
        return "bitcoin_transaction_lookup_required"
    if tx_confirmed is False:
        return "bitcoin_transaction_confirmation_required"
    if economics_match is False:
        return "transaction_economics_review_required"
    if available_btc + 1e-18 >= required_btc:
        return "resolved_existing_native_lots"
    if db_candidates.get("withdrawals"):
        return "transfer_link_review_required"
    if db_candidates.get("deposits"):
        return "native_deposit_basis_review_required"
    if db_candidates.get("wallet_address_txs"):
        return "wallet_history_review_required"
    return "basis_missing"


def build_counterparty_btc_source_lot_preview(
    *,
    db: Session,
    txid: str,
    custody_scope: Dict[str, Any],
    custody_address: str,
    expected_gross_satoshis: Optional[int],
    expected_fee_satoshis: Optional[int],
    dispenser_address: Optional[str],
    supplied_target_tx_raw: Optional[Dict[str, Any]] = None,
    allow_external_lookup: bool = True,
    max_parent_lookups: int = 12,
) -> Dict[str, Any]:
    """Reconstruct the native BTC funding path without any database writes."""
    txid_norm = _norm_txid(txid)
    if not txid_norm:
        return {
            "ok": False,
            "version": "counterparty_btc_source_lot_preview_v1",
            "status": "invalid_txid",
            "read_only": True,
            "database_mutation": False,
            "ledger_mutation": False,
            "lot_mutation": False,
            "fifo_mutation": False,
            "basis_mutation": False,
            "blockers": ["underlying_btc_transaction_reconstruction_unresolved"],
            "warnings": [],
        }

    scope = custody_scope if isinstance(custody_scope, dict) else {}
    proposed = scope.get("proposed_btc_disposition_scope")
    proposed = proposed if isinstance(proposed, dict) else {}
    custody_resolved = bool(scope.get("resolved"))
    required_sats = (_as_int(expected_gross_satoshis) or 0) + (_as_int(expected_fee_satoshis) or 0)
    required_btc = float(Decimal(required_sats) / _SATOSHIS_PER_BTC)

    target_lookup = _fetch_bitcoin_tx(
        txid_norm,
        supplied_raw=supplied_target_tx_raw,
        allow_external_lookup=bool(allow_external_lookup),
    )
    target_payload = target_lookup.get("raw") if isinstance(target_lookup.get("raw"), dict) else None
    target_reconstruction = (
        _parse_target_transaction(
            target_payload,
            txid=txid_norm,
            custody_address=custody_address,
            dispenser_address=dispenser_address,
            expected_gross_satoshis=expected_gross_satoshis,
            expected_fee_satoshis=expected_fee_satoshis,
        )
        if target_payload is not None
        else {
            "ok": False,
            "status": "transaction_lookup_required",
            "txid": txid_norm,
            "inputs": [],
            "outputs": [],
            "input_count": 0,
            "output_count": 0,
            "expected_total_outflow_satoshis": required_sats,
            "expected_total_outflow_btc": required_btc,
        }
    )

    owned_inputs = [row for row in (target_reconstruction.get("inputs") or []) if row.get("owned_by_custody")]
    parent_txids = [str(row.get("prev_txid") or "") for row in owned_inputs if _norm_txid(row.get("prev_txid"))]
    parent_summaries: List[Dict[str, Any]] = []
    max_lookups = max(0, min(int(max_parent_lookups or 0), 50))
    for input_row in owned_inputs[:max_lookups]:
        parent_txid = str(input_row.get("prev_txid") or "")
        result = _fetch_bitcoin_tx(parent_txid, allow_external_lookup=bool(allow_external_lookup))
        parent_summaries.append(_parent_summary(result, input_row=input_row, custody_address=custody_address))

    db_candidates = _find_db_candidates(
        db,
        parent_txids=parent_txids,
        custody_wallet_address_id=proposed.get("wallet_address_id"),
    )
    native_lots = _native_lots(
        db,
        custody_venue=proposed.get("venue"),
        custody_wallet_id=proposed.get("wallet_id"),
    )
    available_btc = sum(max(_safe_float(row.get("qty_remaining")), 0.0) for row in native_lots)
    recovery_status = _recovery_status(
        custody_resolved=custody_resolved,
        tx_ok=bool(target_reconstruction.get("ok")),
        tx_confirmed=target_reconstruction.get("confirmed"),
        economics_match=target_reconstruction.get("net_outflow_matches_expected"),
        available_btc=available_btc,
        required_btc=required_btc,
        db_candidates=db_candidates,
    )

    blockers: List[str] = []
    warnings: List[str] = []
    if not target_reconstruction.get("ok"):
        blockers.append("underlying_btc_transaction_reconstruction_unresolved")
    if target_reconstruction.get("confirmed") is False:
        blockers.append("underlying_btc_transaction_unconfirmed")
    if target_reconstruction.get("net_outflow_matches_expected") is False:
        blockers.append("underlying_btc_net_outflow_mismatch")
    if target_reconstruction.get("payment_matches_preview") is False:
        blockers.append("underlying_btc_dispenser_payment_mismatch")
    if recovery_status == "transfer_link_review_required":
        blockers.append("underlying_btc_source_transfer_review_required")
    elif recovery_status in {
        "native_deposit_basis_review_required",
        "wallet_history_review_required",
        "basis_missing",
    }:
        blockers.append("underlying_btc_source_basis_missing")
    if target_reconstruction.get("all_inputs_owned_by_custody") is False:
        warnings.append("The transaction contains one or more inputs not attributed to the resolved custody address; source-lot review must not assume single-wallet ownership.")
    if len(parent_txids) > max_lookups:
        warnings.append(f"Parent transaction lookups were capped at {max_lookups}; {len(parent_txids) - max_lookups} input parent(s) were not expanded.")
    if db_candidates.get("withdrawals"):
        warnings.append("Potential BTC withdrawal source records matched one or more funding transaction ids. Basis carry-forward requires explicit transfer-link review.")
    if db_candidates.get("deposits") and not db_candidates.get("withdrawals"):
        warnings.append("Native BTC deposit records matched the funding transaction, but no linked source withdrawal was found.")
    if not native_lots:
        warnings.append("No BTC BasisLot rows currently exist in the resolved self-custody / wallet_address scope.")

    input_candidate_map: Dict[str, Dict[str, int]] = {}
    for input_row in owned_inputs:
        txid_key = str(input_row.get("prev_txid") or "")
        input_candidate_map[txid_key] = {
            "wallet_address_tx_count": sum(1 for row in db_candidates.get("wallet_address_txs") or [] if str(row.get("txid") or "").lower() == txid_key.lower()),
            "deposit_count": sum(1 for row in db_candidates.get("deposits") or [] if str(row.get("txid") or "").lower() == txid_key.lower()),
            "withdrawal_count": sum(1 for row in db_candidates.get("withdrawals") or [] if str(row.get("txid") or "").lower() == txid_key.lower()),
        }
    enriched_inputs = []
    parent_by_identity = {str(row.get("input_identity")): row for row in parent_summaries}
    for input_row in target_reconstruction.get("inputs") or []:
        parent = parent_by_identity.get(str(input_row.get("identity"))) or {}
        counts = input_candidate_map.get(str(input_row.get("prev_txid") or "")) or {}
        enriched_inputs.append({**input_row, "parent": parent, **counts})
    target_reconstruction["inputs"] = enriched_inputs

    return {
        "ok": bool(target_reconstruction.get("ok")),
        "version": "counterparty_btc_source_lot_preview_v1",
        "status": recovery_status,
        "read_only": True,
        "dry_run": True,
        "database_mutation": False,
        "deposit_mutation": False,
        "withdrawal_mutation": False,
        "transfer_link_mutation": False,
        "ledger_mutation": False,
        "lot_mutation": False,
        "fifo_mutation": False,
        "basis_mutation": False,
        "txid": txid_norm,
        "identity": f"bitcoin:source-lot-preview:{txid_norm}:{custody_address}",
        "custody_scope_resolved": custody_resolved,
        "custody_address": custody_address,
        "custody_wallet_address_id": proposed.get("wallet_address_id"),
        "custody_venue": proposed.get("venue"),
        "custody_wallet_id": proposed.get("wallet_id"),
        "required_satoshis": required_sats,
        "required_btc": required_btc,
        "target_transaction_lookup": {key: value for key, value in target_lookup.items() if key != "raw"},
        "transaction_reconstruction": target_reconstruction,
        "funding_parent_transactions": parent_summaries,
        "funding_parent_count": len(parent_summaries),
        "funding_parent_txids": parent_txids,
        "db_candidates": db_candidates,
        "native_btc_lots": native_lots,
        "native_btc_lot_count": len(native_lots),
        "native_btc_available": available_btc,
        "native_btc_shortfall": max(required_btc - available_btc, 0.0),
        "recovery_proposal": {
            "status": recovery_status,
            "policy": "recover_underlying_wallet_basis_before_fifo",
            "transfer_basis_carryforward_required": bool(db_candidates.get("withdrawals")),
            "do_not_create_basis_at_spend_time": True,
            "do_not_treat_full_input_utxo_as_disposition": True,
            "net_disposition_satoshis": target_reconstruction.get("net_wallet_outflow_satoshis"),
            "net_disposition_btc": target_reconstruction.get("net_wallet_outflow_btc"),
            "fifo_scope": {
                "venue": proposed.get("venue"),
                "wallet_id": proposed.get("wallet_id"),
                "asset": "BTC",
            },
            "fifo_consumption": False,
            "basis_mutation": False,
        },
        "blockers": sorted(set(blockers)),
        "warnings": warnings,
    }
