from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from ..config import settings
from ..models import RobinhoodChainExecution
from .evm_rpc import decode_hex_quantity, validate_evm_address
from .robinhood_chain_transaction_planning import (
    EXPECTED_CHAIN_ID,
    ROBINHOOD_CHAIN_FIRM_QUOTE_SYMBOL,
    RobinhoodChainTransactionPlanningService,
    get_robinhood_chain_transaction_planning_service,
)


ROBINHOOD_CHAIN_EXECUTION_SYMBOL = "ETH-USDG"
ROBINHOOD_CHAIN_EXECUTION_SIDE = "sell"
# Backward-compatible names retained for callers that display the historical
# accepted amount. R5C.3B.1 treats this value as the maximum reviewed input,
# not as a mandatory fixed amount.
ROBINHOOD_CHAIN_EXECUTION_INPUT_ETH = Decimal("0.002")
ROBINHOOD_CHAIN_EXECUTION_INPUT_WEI = 2_000_000_000_000_000
ROBINHOOD_CHAIN_EXECUTION_MAX_INPUT_ETH = ROBINHOOD_CHAIN_EXECUTION_INPUT_ETH
ROBINHOOD_CHAIN_EXECUTION_MAX_INPUT_WEI = ROBINHOOD_CHAIN_EXECUTION_INPUT_WEI
ROBINHOOD_CHAIN_EXECUTION_STATUSES = frozenset(
    {
        "prepared",
        "send_claimed",
        "pending",
        "confirmed",
        "reverted",
        "verification_failed",
        "wallet_rejected",
        "submission_failed",
    }
)
ROBINHOOD_CHAIN_SUBMISSION_FAILURE_REASONS = frozenset(
    {"wallet_rejected", "wallet_request_failed"}
)

_TX_HASH_RE = re.compile(r"^0x[0-9a-fA-F]{64}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_QUOTE_ID_RE = re.compile(r"^[0-9a-f]{64}$")
_CLAIM_ID_RE = re.compile(r"^[0-9a-f]{64}$")
_ERC20_TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
_ROBINHOOD_CHAIN_USDG_CONTRACT = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
_ROBINHOOD_CHAIN_USDG_DECIMALS = 6
_WEI_PER_ETH = Decimal(10) ** 18


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso_or_none(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return _as_utc(value).isoformat()


def _parse_utc_datetime(value: Any, *, field: str) -> datetime:
    raw = str(value or "").strip()
    if not raw:
        raise ValueError(f"missing_{field}")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception as exc:
        raise ValueError(f"invalid_{field}") from exc
    return _as_utc(parsed)


def _decimal_text(value: Any, *, field: str) -> str:
    try:
        number = Decimal(str(value if value is not None else "").strip())
    except Exception as exc:
        raise ValueError(f"invalid_{field}") from exc
    if not number.is_finite() or number < 0:
        raise ValueError(f"invalid_{field}")
    text = format(number, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def normalize_robinhood_chain_execution_quantity(value: Any) -> tuple[Decimal, str, int]:
    """Normalize one positive native-ETH input bounded by the reviewed cap."""
    raw = str(value if value is not None else "").strip()
    try:
        amount = Decimal(raw)
    except Exception as exc:
        raise ValueError("invalid_robinhood_chain_execution_quantity") from exc
    if not amount.is_finite() or amount <= 0:
        raise ValueError("invalid_robinhood_chain_execution_quantity")

    normalized = amount.normalize()
    if normalized.as_tuple().exponent < -18:
        raise ValueError("robinhood_chain_execution_quantity_precision_exceeded")
    if amount > ROBINHOOD_CHAIN_EXECUTION_MAX_INPUT_ETH:
        raise ValueError("robinhood_chain_execution_quantity_exceeds_cap")

    atomic_decimal = amount * _WEI_PER_ETH
    integral_atomic = atomic_decimal.to_integral_value()
    if atomic_decimal != integral_atomic:
        raise ValueError("robinhood_chain_execution_quantity_precision_exceeded")
    atomic = int(integral_atomic)
    if atomic <= 0 or atomic > ROBINHOOD_CHAIN_EXECUTION_MAX_INPUT_WEI:
        raise ValueError("robinhood_chain_execution_quantity_exceeds_cap")

    text = format(amount, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return amount, text or "0", atomic


def _topic_address(value: Any, *, field: str) -> str:
    raw = str(value or "").strip().lower()
    if not re.fullmatch(r"0x[0-9a-f]{64}", raw):
        raise ValueError(f"invalid_{field}")
    return "0x" + raw[-40:]


def _execution_reconciliation(row: RobinhoodChainExecution) -> Optional[Dict[str, Any]]:
    route = row.route if isinstance(row.route, dict) else {}
    value = route.get("execution_reconciliation")
    return dict(value) if isinstance(value, dict) else None


def _decode_confirmed_execution_reconciliation(
    row: RobinhoodChainExecution,
    receipt: Dict[str, Any],
) -> Dict[str, Any]:
    logs = receipt.get("logs")
    if not isinstance(logs, list):
        raise ValueError("robinhood_chain_receipt_logs_missing")

    recipient = validate_evm_address(row.wallet_address).lower()
    output_atomic = 0
    matching_logs = 0

    for item in logs:
        if not isinstance(item, dict):
            continue
        try:
            contract = validate_evm_address(str(item.get("address") or "")).lower()
        except ValueError:
            continue
        if contract != _ROBINHOOD_CHAIN_USDG_CONTRACT:
            continue

        topics = item.get("topics")
        if not isinstance(topics, list) or len(topics) < 3:
            continue
        if str(topics[0] or "").strip().lower() != _ERC20_TRANSFER_TOPIC0:
            continue
        try:
            transfer_recipient = _topic_address(topics[2], field="usdg_transfer_recipient_topic")
        except ValueError:
            continue
        if transfer_recipient != recipient:
            continue

        try:
            amount_atomic = decode_hex_quantity(item.get("data"))
        except ValueError:
            continue
        if amount_atomic <= 0:
            continue

        output_atomic += int(amount_atomic)
        matching_logs += 1

    if output_atomic <= 0 or matching_logs <= 0:
        raise ValueError("robinhood_chain_usdg_transfer_to_wallet_not_found")

    gas_used = decode_hex_quantity(receipt.get("gasUsed")) if receipt.get("gasUsed") else 0
    effective_gas_price = (
        decode_hex_quantity(receipt.get("effectiveGasPrice"))
        if receipt.get("effectiveGasPrice")
        else 0
    )
    if gas_used <= 0 or effective_gas_price <= 0:
        raise ValueError("robinhood_chain_receipt_gas_fields_missing")

    output_amount = Decimal(output_atomic) / (Decimal(10) ** _ROBINHOOD_CHAIN_USDG_DECIMALS)
    input_amount = Decimal(str(row.input_amount or "0"))
    minimum_output = Decimal(str(row.minimum_output_amount or "0"))
    if input_amount <= 0:
        raise ValueError("invalid_robinhood_chain_execution_input_amount")

    network_fee_wei = int(gas_used) * int(effective_gas_price)
    network_fee_eth = Decimal(network_fee_wei) / _WEI_PER_ETH
    average_fill_price = output_amount / input_amount
    minimum_limit_price = minimum_output / input_amount

    return {
        "version": "RH-CHAIN.10D.1B",
        "reconciled": True,
        "reconciled_at": utc_now().isoformat(),
        "output_asset": "USDG",
        "output_contract": _ROBINHOOD_CHAIN_USDG_CONTRACT,
        "output_decimals": _ROBINHOOD_CHAIN_USDG_DECIMALS,
        "output_amount_atomic": str(output_atomic),
        "output_amount": _decimal_text(output_amount, field="actual_output_amount"),
        "average_fill_price": _decimal_text(average_fill_price, field="actual_average_fill_price"),
        "minimum_limit_price": _decimal_text(minimum_limit_price, field="minimum_limit_price"),
        "transfer_log_count": int(matching_logs),
        "recipient": recipient,
        "fee_asset": "ETH",
        "network_fee_wei": str(network_fee_wei),
        "network_fee": _decimal_text(network_fee_eth, field="actual_network_fee"),
        "gas_used": str(gas_used),
        "effective_gas_price_wei": str(effective_gas_price),
    }


def validate_transaction_hash(value: Any) -> str:
    tx_hash = str(value or "").strip()
    if not _TX_HASH_RE.fullmatch(tx_hash):
        raise ValueError("invalid_robinhood_chain_transaction_hash")
    return tx_hash.lower()


def validate_claim_id(value: Any) -> str:
    claim_id = str(value or "").strip().lower()
    if not _CLAIM_ID_RE.fullmatch(claim_id):
        raise ValueError("invalid_robinhood_chain_send_claim_id")
    return claim_id


def validate_execution_saved_wallet(saved_wallet: Any, requested_wallet: Any) -> str:
    saved = validate_evm_address(str(saved_wallet or "").strip()).lower()
    requested_raw = str(requested_wallet or "").strip()
    if not requested_raw:
        return saved
    requested = validate_evm_address(requested_raw).lower()
    if requested != saved:
        raise ValueError("robinhood_chain_execution_saved_wallet_mismatch")
    return saved


def _safe_route(plan: Dict[str, Any]) -> Dict[str, Any]:
    route = plan.get("route") if isinstance(plan.get("route"), dict) else {}
    fills = route.get("fills") if isinstance(route.get("fills"), list) else []
    out = []
    for fill in fills[:12]:
        if not isinstance(fill, dict):
            continue
        source = str(fill.get("source") or "").strip()
        if not source:
            continue
        out.append(
            {
                "source": source,
                "proportion_bps": str(fill.get("proportion_bps") or "").strip() or None,
            }
        )
    return {"fills": out, "fill_count": len(out)}


def _plan_identity(plan: Dict[str, Any]) -> Dict[str, Any]:
    tx = plan.get("unsigned_transaction_plan")
    if not isinstance(tx, dict):
        raise ValueError("missing_unsigned_transaction_plan")

    quote_id = str(plan.get("quote_id") or "").strip().lower()
    if not _QUOTE_ID_RE.fullmatch(quote_id):
        raise ValueError("invalid_firm_plan_quote_id")

    calldata_sha256 = str(tx.get("calldata_sha256") or "").strip().lower()
    if not _SHA256_RE.fullmatch(calldata_sha256):
        raise ValueError("invalid_firm_plan_calldata_sha256")

    wallet = validate_evm_address(str(tx.get("from") or "").strip()).lower()
    destination = validate_evm_address(str(tx.get("to") or "").strip()).lower()
    value_wei = str(tx.get("value_wei") or "").strip()
    if not value_wei.isdigit():
        raise ValueError("invalid_firm_plan_transaction_value")

    gas_limit = str(tx.get("gas_limit") or "").strip()
    gas_price_wei = str(tx.get("gas_price_wei") or "").strip()
    if not gas_limit.isdigit() or int(gas_limit) <= 0:
        raise ValueError("invalid_firm_plan_gas_limit")
    if not gas_price_wei.isdigit() or int(gas_price_wei) <= 0:
        raise ValueError("invalid_firm_plan_gas_price")

    calldata = str(tx.get("calldata") or "").strip()
    if not calldata.startswith("0x") or len(calldata) <= 2 or len(calldata[2:]) % 2 != 0:
        raise ValueError("invalid_firm_plan_calldata")
    try:
        calldata_bytes = bytes.fromhex(calldata[2:])
    except Exception as exc:
        raise ValueError("invalid_firm_plan_calldata") from exc
    if not calldata_bytes:
        raise ValueError("invalid_firm_plan_calldata")
    observed_calldata_hash = hashlib.sha256(calldata_bytes).hexdigest()
    if observed_calldata_hash != calldata_sha256:
        raise ValueError("firm_plan_calldata_hash_mismatch")

    declared_bytes = int(tx.get("calldata_bytes") or 0)
    if declared_bytes != len(calldata_bytes):
        raise ValueError("firm_plan_calldata_length_mismatch")

    expires_at = _parse_utc_datetime(plan.get("plan_expires_at"), field="firm_plan_expiration")
    fetched_at = _parse_utc_datetime(plan.get("fetched_at"), field="firm_plan_fetched_at")

    return {
        "quote_id": quote_id,
        "wallet_address": wallet,
        "transaction_to": destination,
        "transaction_value_wei": value_wei,
        "gas_limit": gas_limit,
        "gas_price_wei": gas_price_wei,
        "calldata": calldata,
        "calldata_sha256": calldata_sha256,
        "calldata_bytes": declared_bytes,
        "plan_expires_at": expires_at,
        "plan_fetched_at": fetched_at,
    }


def _plan_hash(plan: Dict[str, Any], identity: Dict[str, Any]) -> str:
    material = {
        "chain_id": int(plan.get("chain_id") or 0),
        "symbol": str(plan.get("symbol") or "").strip().upper(),
        "side": str(plan.get("side") or "").strip().lower(),
        "input_asset": str(plan.get("input_asset") or "").strip().upper(),
        "input_amount_atomic": str(plan.get("input_amount_atomic") or "").strip(),
        "output_asset": str(plan.get("output_asset") or "").strip().upper(),
        "minimum_received_atomic": str(plan.get("minimum_received_atomic") or "").strip(),
        "slippage_bps": int(plan.get("slippage_bps") or 0),
        "quote_id": identity["quote_id"],
        "wallet_address": identity["wallet_address"],
        "transaction_to": identity["transaction_to"],
        "transaction_value_wei": identity["transaction_value_wei"],
        "gas_limit": identity["gas_limit"],
        "gas_price_wei": identity["gas_price_wei"],
        "calldata_sha256": identity["calldata_sha256"],
        "plan_expires_at": identity["plan_expires_at"].isoformat(),
    }
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _execution_gate() -> Dict[str, Any]:
    dedicated = bool(getattr(settings, "robinhood_chain_live_execution_enabled", False))
    armed = bool(getattr(settings, "armed", False))
    dry_run = bool(getattr(settings, "dry_run", True))
    chain_ready = bool(settings.robinhood_chain_effective_enabled())
    send_enabled = bool(chain_ready and dedicated and armed and not dry_run)
    missing = []
    if not chain_ready:
        missing.append("robinhood_chain_effective_enabled")
    if not dedicated:
        missing.append("ROBINHOOD_CHAIN_LIVE_EXECUTION_ENABLED=1")
    if dry_run:
        missing.append("DRY_RUN=0")
    if not armed:
        missing.append("ARMED=1")
    return {
        "chain_id": EXPECTED_CHAIN_ID,
        "symbol": ROBINHOOD_CHAIN_EXECUTION_SYMBOL,
        "side": ROBINHOOD_CHAIN_EXECUTION_SIDE,
        "exact_input_policy": "custom_up_to_reviewed_maximum",
        "exact_input_eth": format(ROBINHOOD_CHAIN_EXECUTION_MAX_INPUT_ETH, "f"),
        "exact_input_wei": str(ROBINHOOD_CHAIN_EXECUTION_MAX_INPUT_WEI),
        "maximum_input_eth": format(ROBINHOOD_CHAIN_EXECUTION_MAX_INPUT_ETH, "f"),
        "maximum_input_wei": str(ROBINHOOD_CHAIN_EXECUTION_MAX_INPUT_WEI),
        "custom_input_enabled": True,
        "dedicated_execution_enabled": dedicated,
        "armed": armed,
        "dry_run": dry_run,
        "chain_ready": chain_ready,
        "send_enabled": send_enabled,
        "missing_requirements": missing,
        "backend_private_key": False,
        "backend_transaction_sender": False,
        "automatic_retry": False,
        "approval_required": False,
    }


def _validate_execution_row_lock(row: RobinhoodChainExecution) -> None:
    if int(row.chain_id or 0) != EXPECTED_CHAIN_ID:
        raise ValueError("robinhood_chain_execution_locked_chain_mismatch")
    if str(row.symbol or "").strip().upper() != ROBINHOOD_CHAIN_EXECUTION_SYMBOL:
        raise ValueError("robinhood_chain_execution_locked_symbol_mismatch")
    if str(row.side or "").strip().lower() != ROBINHOOD_CHAIN_EXECUTION_SIDE:
        raise ValueError("robinhood_chain_execution_locked_side_mismatch")
    if str(row.input_asset or "").strip().upper() != "ETH":
        raise ValueError("robinhood_chain_execution_locked_input_asset_mismatch")
    if str(row.expected_output_asset or "").strip().upper() != "USDG":
        raise ValueError("robinhood_chain_execution_locked_output_asset_mismatch")
    try:
        _, normalized_amount, expected_atomic = normalize_robinhood_chain_execution_quantity(row.input_amount)
    except ValueError as exc:
        raise ValueError("robinhood_chain_execution_locked_input_amount_mismatch") from exc
    if normalized_amount != str(row.input_amount or "").strip():
        raise ValueError("robinhood_chain_execution_locked_input_amount_mismatch")
    if str(row.input_amount_atomic or "").strip() != str(expected_atomic):
        raise ValueError("robinhood_chain_execution_locked_input_amount_mismatch")
    if str(row.transaction_value_wei or "").strip() != str(expected_atomic):
        raise ValueError("robinhood_chain_execution_locked_transaction_value_mismatch")


def serialize_execution(row: RobinhoodChainExecution) -> Dict[str, Any]:
    reconciliation = _execution_reconciliation(row)
    return {
        "id": str(row.id),
        "venue": "robinhood_chain",
        "chain_id": int(row.chain_id),
        "wallet_address": row.wallet_address,
        "symbol": row.symbol,
        "side": row.side,
        "input_asset": row.input_asset,
        "input_amount": row.input_amount,
        "input_amount_atomic": row.input_amount_atomic,
        "expected_output_asset": row.expected_output_asset,
        "expected_output_amount": row.expected_output_amount,
        "minimum_output_amount": row.minimum_output_amount,
        "slippage_bps": int(row.slippage_bps),
        "quote_id": row.quote_id,
        "plan_hash": row.plan_hash,
        "plan_fetched_at": _iso_or_none(row.plan_fetched_at),
        "plan_expires_at": _iso_or_none(row.plan_expires_at),
        "transaction_to": row.transaction_to,
        "transaction_value_wei": row.transaction_value_wei,
        "calldata_sha256": row.calldata_sha256,
        "calldata_bytes": int(row.calldata_bytes or 0),
        "gas_limit": row.gas_limit,
        "gas_price_wei": row.gas_price_wei,
        "route": row.route,
        "status": row.status,
        "send_claimed": bool(row.send_claim_id),
        "send_claimed_at": _iso_or_none(row.send_claimed_at),
        "submission_attempts": int(row.submission_attempts or 0),
        "submission_failure_at": _iso_or_none(row.submission_failure_at),
        "tx_hash": row.tx_hash,
        "submitted_at": _iso_or_none(row.submitted_at),
        "last_receipt_check_at": _iso_or_none(row.last_receipt_check_at),
        "confirmed_at": _iso_or_none(row.confirmed_at),
        "reverted_at": _iso_or_none(row.reverted_at),
        "block_number": row.block_number,
        "gas_used": row.gas_used,
        "effective_gas_price_wei": row.effective_gas_price_wei,
        "receipt_status": row.receipt_status,
        "reconciliation": reconciliation,
        "actual_output_asset": (reconciliation or {}).get("output_asset"),
        "actual_output_amount": (reconciliation or {}).get("output_amount"),
        "actual_output_amount_atomic": (reconciliation or {}).get("output_amount_atomic"),
        "actual_average_fill_price": (reconciliation or {}).get("average_fill_price"),
        "actual_network_fee": (reconciliation or {}).get("network_fee"),
        "actual_network_fee_asset": (reconciliation or {}).get("fee_asset"),
        "actual_network_fee_wei": (reconciliation or {}).get("network_fee_wei"),
        "error_code": row.error_code,
        "error_message": row.error_message,
        "created_at": _iso_or_none(row.created_at),
        "updated_at": _iso_or_none(row.updated_at),
    }


class RobinhoodChainExecutionService:
    """Dedicated, tightly bounded lifecycle for one explicit native-ETH swap.

    The browser wallet is the only transaction sender. The backend prepares and
    records a validated intent, grants one idempotent send claim, stores the
    returned transaction hash, and polls read-only transaction/receipt methods.
    It never holds a private key and never invokes an EVM send RPC method.
    """

    def __init__(
        self,
        *,
        planning_service: Optional[RobinhoodChainTransactionPlanningService] = None,
        rpc_client: Any = None,
    ) -> None:
        self.planning_service = planning_service or get_robinhood_chain_transaction_planning_service()
        self.rpc_client = rpc_client

    def status(self) -> Dict[str, Any]:
        return {
            "ok": True,
            "tranche": "RH-CHAIN.10D.1B",
            **_execution_gate(),
            "prepare_enabled": True,
            "send_claim_enabled": True,
            "submission_recording_enabled": True,
            "receipt_monitoring_enabled": True,
            "all_orders_inclusion_enabled": True,
            "ledger_mutation_enabled": False,
            "fifo_mutation_enabled": False,
            "basis_mutation_enabled": False,
        }

    async def prepare(
        self,
        db: Session,
        *,
        taker_address: str,
        eth_token: Dict[str, Any],
        usdg_token: Dict[str, Any],
        quantity: Any = ROBINHOOD_CHAIN_EXECUTION_INPUT_ETH,
        slippage_bps: int,
        confirm_prepare: bool,
    ) -> Dict[str, Any]:
        if confirm_prepare is not True:
            raise ValueError("confirm_prepare_required")

        taker = validate_evm_address(taker_address).lower()
        _, requested_amount_text, requested_amount_wei = normalize_robinhood_chain_execution_quantity(quantity)
        plan = await self.planning_service.firm_quote_plan(
            symbol=ROBINHOOD_CHAIN_FIRM_QUOTE_SYMBOL,
            side=ROBINHOOD_CHAIN_EXECUTION_SIDE,
            quantity=requested_amount_text,
            total_quote=None,
            taker_address=taker,
            eth_token=eth_token,
            usdg_token=usdg_token,
            slippage_bps=int(slippage_bps),
        )
        if not plan.get("ok"):
            return plan

        identity = _plan_identity(plan)
        now = utc_now()

        if int(plan.get("chain_id") or 0) != EXPECTED_CHAIN_ID:
            raise ValueError("execution_plan_chain_mismatch")
        if str(plan.get("symbol") or "").strip().upper() != ROBINHOOD_CHAIN_EXECUTION_SYMBOL:
            raise ValueError("execution_plan_symbol_mismatch")
        if str(plan.get("side") or "").strip().lower() != ROBINHOOD_CHAIN_EXECUTION_SIDE:
            raise ValueError("execution_plan_side_mismatch")
        if str(plan.get("input_asset") or "").strip().upper() != "ETH":
            raise ValueError("execution_plan_input_asset_mismatch")
        if str(plan.get("output_asset") or "").strip().upper() != "USDG":
            raise ValueError("execution_plan_output_asset_mismatch")
        try:
            _, planned_amount_text, planned_amount_wei = normalize_robinhood_chain_execution_quantity(
                plan.get("input_amount")
            )
        except ValueError as exc:
            raise ValueError("execution_plan_input_amount_mismatch") from exc
        if planned_amount_text != requested_amount_text:
            raise ValueError("execution_plan_input_amount_mismatch")
        if str(plan.get("input_amount_atomic") or "").strip() != str(requested_amount_wei):
            raise ValueError("execution_plan_input_amount_mismatch")
        if planned_amount_wei != requested_amount_wei:
            raise ValueError("execution_plan_input_amount_mismatch")
        if identity["transaction_value_wei"] != str(requested_amount_wei):
            raise ValueError("execution_plan_transaction_value_mismatch")
        if identity["wallet_address"] != taker:
            raise ValueError("execution_plan_wallet_mismatch")
        if plan.get("approval_required") is not False:
            raise ValueError("execution_plan_unexpected_approval")
        if plan.get("unsigned_transaction_plan", {}).get("native_input") is not True:
            raise ValueError("execution_plan_native_input_required")
        if plan.get("unsigned_transaction_plan", {}).get("destination_allowlisted") is not True:
            raise ValueError("execution_plan_destination_not_allowlisted")
        if identity["plan_expires_at"] <= now:
            raise ValueError("execution_plan_expired")

        plan_hash = _plan_hash(plan, identity)
        existing = (
            db.query(RobinhoodChainExecution)
            .filter(RobinhoodChainExecution.quote_id == identity["quote_id"])
            .first()
        )
        if existing is not None:
            if existing.plan_hash != plan_hash or existing.wallet_address.lower() != taker:
                raise ValueError("execution_quote_id_collision")
            return {
                "ok": True,
                "idempotent": True,
                "execution": serialize_execution(existing),
                "unsigned_transaction_plan": plan["unsigned_transaction_plan"],
                "plan_expires_at": plan["plan_expires_at"],
                "send_gate": self.status(),
            }

        row = RobinhoodChainExecution(
            chain_id=EXPECTED_CHAIN_ID,
            wallet_address=taker,
            symbol=ROBINHOOD_CHAIN_EXECUTION_SYMBOL,
            side=ROBINHOOD_CHAIN_EXECUTION_SIDE,
            input_asset="ETH",
            input_amount=requested_amount_text,
            input_amount_atomic=str(requested_amount_wei),
            expected_output_asset="USDG",
            expected_output_amount=_decimal_text(plan.get("output_amount"), field="expected_output"),
            minimum_output_amount=_decimal_text(plan.get("minimum_received"), field="minimum_output"),
            slippage_bps=int(plan.get("slippage_bps") or slippage_bps),
            quote_id=identity["quote_id"],
            plan_hash=plan_hash,
            plan_fetched_at=_utc_naive(identity["plan_fetched_at"]),
            plan_expires_at=_utc_naive(identity["plan_expires_at"]),
            transaction_to=identity["transaction_to"],
            transaction_value_wei=identity["transaction_value_wei"],
            calldata_sha256=identity["calldata_sha256"],
            calldata_bytes=identity["calldata_bytes"],
            gas_limit=identity["gas_limit"],
            gas_price_wei=identity["gas_price_wei"],
            route=_safe_route(plan),
            status="prepared",
            submission_attempts=0,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )
        db.add(row)
        db.commit()
        db.refresh(row)

        return {
            "ok": True,
            "idempotent": False,
            "execution": serialize_execution(row),
            "unsigned_transaction_plan": plan["unsigned_transaction_plan"],
            "plan_expires_at": plan["plan_expires_at"],
            "send_gate": self.status(),
        }

    def get(self, db: Session, execution_id: str) -> Dict[str, Any]:
        row = db.query(RobinhoodChainExecution).filter(RobinhoodChainExecution.id == str(execution_id)).first()
        if row is None:
            raise ValueError("robinhood_chain_execution_not_found")
        return {"ok": True, "execution": serialize_execution(row), "send_gate": self.status()}

    def claim_send(
        self,
        db: Session,
        *,
        execution_id: str,
        wallet_address: str,
        plan_hash: str,
        claim_id: str,
        confirm_send_claim: bool,
    ) -> Dict[str, Any]:
        if confirm_send_claim is not True:
            raise ValueError("confirm_send_claim_required")
        wallet = validate_evm_address(wallet_address).lower()
        normalized_plan_hash = str(plan_hash or "").strip().lower()
        if not _SHA256_RE.fullmatch(normalized_plan_hash):
            raise ValueError("invalid_robinhood_chain_execution_plan_hash")
        normalized_claim_id = validate_claim_id(claim_id)

        gate = self.status()
        if gate.get("send_enabled") is not True:
            raise ValueError("robinhood_chain_live_send_gate_blocked")

        row = db.query(RobinhoodChainExecution).filter(RobinhoodChainExecution.id == str(execution_id)).first()
        if row is None:
            raise ValueError("robinhood_chain_execution_not_found")
        _validate_execution_row_lock(row)
        if row.wallet_address.lower() != wallet:
            raise ValueError("robinhood_chain_execution_wallet_mismatch")
        if row.plan_hash.lower() != normalized_plan_hash:
            raise ValueError("robinhood_chain_execution_plan_hash_mismatch")
        if _as_utc(row.plan_expires_at) <= utc_now():
            raise ValueError("robinhood_chain_execution_plan_expired")
        if row.tx_hash:
            raise ValueError("robinhood_chain_execution_already_submitted")

        if row.status == "send_claimed":
            if str(row.send_claim_id or "").lower() != normalized_claim_id:
                raise ValueError("robinhood_chain_execution_send_already_claimed")
            return {
                "ok": True,
                "idempotent": True,
                "claim_id": normalized_claim_id,
                "execution": serialize_execution(row),
                "send_gate": gate,
            }
        if row.status != "prepared":
            raise ValueError("robinhood_chain_execution_not_prepared")
        if int(row.submission_attempts or 0) != 0:
            raise ValueError("robinhood_chain_execution_submission_already_attempted")

        duplicate = (
            db.query(RobinhoodChainExecution)
            .filter(RobinhoodChainExecution.send_claim_id == normalized_claim_id)
            .first()
        )
        if duplicate is not None and str(duplicate.id) != str(row.id):
            raise ValueError("robinhood_chain_send_claim_already_used")

        claimed_at = datetime.utcnow()
        updated = (
            db.query(RobinhoodChainExecution)
            .filter(
                RobinhoodChainExecution.id == str(execution_id),
                RobinhoodChainExecution.status == "prepared",
                RobinhoodChainExecution.send_claim_id.is_(None),
                RobinhoodChainExecution.submission_attempts == 0,
                RobinhoodChainExecution.tx_hash.is_(None),
            )
            .update(
                {
                    RobinhoodChainExecution.status: "send_claimed",
                    RobinhoodChainExecution.send_claim_id: normalized_claim_id,
                    RobinhoodChainExecution.send_claimed_at: claimed_at,
                    RobinhoodChainExecution.submission_attempts: 1,
                    RobinhoodChainExecution.error_code: None,
                    RobinhoodChainExecution.error_message: None,
                    RobinhoodChainExecution.updated_at: claimed_at,
                },
                synchronize_session=False,
            )
        )
        db.commit()
        db.expire_all()
        row = db.query(RobinhoodChainExecution).filter(RobinhoodChainExecution.id == str(execution_id)).first()
        if row is None:
            raise ValueError("robinhood_chain_execution_not_found")
        if updated != 1:
            if row.status == "send_claimed" and str(row.send_claim_id or "").lower() == normalized_claim_id:
                return {
                    "ok": True,
                    "idempotent": True,
                    "claim_id": normalized_claim_id,
                    "execution": serialize_execution(row),
                    "send_gate": gate,
                }
            raise ValueError("robinhood_chain_execution_send_already_claimed")
        return {
            "ok": True,
            "idempotent": False,
            "claim_id": normalized_claim_id,
            "execution": serialize_execution(row),
            "send_gate": gate,
        }

    def record_submission(
        self,
        db: Session,
        *,
        execution_id: str,
        tx_hash: str,
        wallet_address: str,
        claim_id: str,
        confirm_record: bool,
    ) -> Dict[str, Any]:
        if confirm_record is not True:
            raise ValueError("confirm_submission_record_required")
        txid = validate_transaction_hash(tx_hash)
        wallet = validate_evm_address(wallet_address).lower()
        normalized_claim_id = validate_claim_id(claim_id)
        row = db.query(RobinhoodChainExecution).filter(RobinhoodChainExecution.id == str(execution_id)).first()
        if row is None:
            raise ValueError("robinhood_chain_execution_not_found")
        if row.wallet_address.lower() != wallet:
            raise ValueError("robinhood_chain_execution_wallet_mismatch")
        if str(row.send_claim_id or "").lower() != normalized_claim_id:
            raise ValueError("robinhood_chain_execution_send_claim_mismatch")
        if row.tx_hash:
            if row.tx_hash.lower() != txid:
                raise ValueError("robinhood_chain_execution_already_has_different_tx_hash")
            return {"ok": True, "idempotent": True, "execution": serialize_execution(row)}
        if row.status != "send_claimed":
            raise ValueError("robinhood_chain_execution_not_send_claimed")

        duplicate = (
            db.query(RobinhoodChainExecution)
            .filter(RobinhoodChainExecution.tx_hash == txid)
            .first()
        )
        if duplicate is not None and str(duplicate.id) != str(row.id):
            raise ValueError("robinhood_chain_transaction_hash_already_recorded")

        submitted_at = datetime.utcnow()
        updated = (
            db.query(RobinhoodChainExecution)
            .filter(
                RobinhoodChainExecution.id == str(execution_id),
                RobinhoodChainExecution.status == "send_claimed",
                RobinhoodChainExecution.send_claim_id == normalized_claim_id,
                RobinhoodChainExecution.tx_hash.is_(None),
            )
            .update(
                {
                    RobinhoodChainExecution.tx_hash: txid,
                    RobinhoodChainExecution.status: "pending",
                    RobinhoodChainExecution.submitted_at: submitted_at,
                    RobinhoodChainExecution.last_receipt_check_at: submitted_at,
                    RobinhoodChainExecution.error_code: None,
                    RobinhoodChainExecution.error_message: None,
                    RobinhoodChainExecution.updated_at: submitted_at,
                },
                synchronize_session=False,
            )
        )
        db.commit()
        db.expire_all()
        row = db.query(RobinhoodChainExecution).filter(RobinhoodChainExecution.id == str(execution_id)).first()
        if row is None:
            raise ValueError("robinhood_chain_execution_not_found")
        if updated != 1:
            if row.tx_hash and row.tx_hash.lower() == txid:
                return {"ok": True, "idempotent": True, "execution": serialize_execution(row)}
            if row.tx_hash:
                raise ValueError("robinhood_chain_execution_already_has_different_tx_hash")
            raise ValueError("robinhood_chain_execution_not_send_claimed")
        return {"ok": True, "idempotent": False, "execution": serialize_execution(row)}

    def record_submission_failure(
        self,
        db: Session,
        *,
        execution_id: str,
        wallet_address: str,
        claim_id: str,
        reason: str,
        message: Optional[str],
        confirm_failure: bool,
    ) -> Dict[str, Any]:
        if confirm_failure is not True:
            raise ValueError("confirm_submission_failure_required")
        wallet = validate_evm_address(wallet_address).lower()
        normalized_claim_id = validate_claim_id(claim_id)
        normalized_reason = str(reason or "").strip().lower()
        if normalized_reason not in ROBINHOOD_CHAIN_SUBMISSION_FAILURE_REASONS:
            raise ValueError("invalid_robinhood_chain_submission_failure_reason")
        terminal_status = "wallet_rejected" if normalized_reason == "wallet_rejected" else "submission_failed"

        row = db.query(RobinhoodChainExecution).filter(RobinhoodChainExecution.id == str(execution_id)).first()
        if row is None:
            raise ValueError("robinhood_chain_execution_not_found")
        if row.wallet_address.lower() != wallet:
            raise ValueError("robinhood_chain_execution_wallet_mismatch")
        if str(row.send_claim_id or "").lower() != normalized_claim_id:
            raise ValueError("robinhood_chain_execution_send_claim_mismatch")
        if row.tx_hash:
            raise ValueError("robinhood_chain_execution_already_submitted")
        if row.status == terminal_status:
            if row.error_code != normalized_reason:
                raise ValueError("robinhood_chain_execution_has_different_failure")
            return {"ok": True, "idempotent": True, "execution": serialize_execution(row)}
        if row.status != "send_claimed":
            raise ValueError("robinhood_chain_execution_not_send_claimed")

        failed_at = datetime.utcnow()
        failure_message = str(message or "").strip()[:512] or normalized_reason.replace("_", " ")
        updated = (
            db.query(RobinhoodChainExecution)
            .filter(
                RobinhoodChainExecution.id == str(execution_id),
                RobinhoodChainExecution.status == "send_claimed",
                RobinhoodChainExecution.send_claim_id == normalized_claim_id,
                RobinhoodChainExecution.tx_hash.is_(None),
            )
            .update(
                {
                    RobinhoodChainExecution.status: terminal_status,
                    RobinhoodChainExecution.submission_failure_at: failed_at,
                    RobinhoodChainExecution.error_code: normalized_reason,
                    RobinhoodChainExecution.error_message: failure_message,
                    RobinhoodChainExecution.updated_at: failed_at,
                },
                synchronize_session=False,
            )
        )
        db.commit()
        db.expire_all()
        row = db.query(RobinhoodChainExecution).filter(RobinhoodChainExecution.id == str(execution_id)).first()
        if row is None:
            raise ValueError("robinhood_chain_execution_not_found")
        if updated != 1:
            if row.tx_hash:
                raise ValueError("robinhood_chain_execution_already_submitted")
            if row.status == terminal_status and row.error_code == normalized_reason:
                return {"ok": True, "idempotent": True, "execution": serialize_execution(row)}
            raise ValueError("robinhood_chain_execution_not_send_claimed")
        return {"ok": True, "idempotent": False, "execution": serialize_execution(row)}

    async def refresh_receipt(self, db: Session, *, execution_id: str) -> Dict[str, Any]:
        row = db.query(RobinhoodChainExecution).filter(RobinhoodChainExecution.id == str(execution_id)).first()
        if row is None:
            raise ValueError("robinhood_chain_execution_not_found")
        if not row.tx_hash:
            raise ValueError("robinhood_chain_execution_not_submitted")
        existing_reconciliation = _execution_reconciliation(row)
        if row.status == "confirmed" and existing_reconciliation:
            return {
                "ok": True,
                "terminal": True,
                "reconciled": True,
                "execution": serialize_execution(row),
            }
        if row.status in {"reverted", "verification_failed"}:
            return {"ok": True, "terminal": True, "execution": serialize_execution(row)}
        if row.status not in {"pending", "confirmed"}:
            raise ValueError("robinhood_chain_execution_not_pending")

        rpc = self.rpc_client
        if rpc is None:
            from .evm_rpc import get_robinhood_chain_client
            rpc = get_robinhood_chain_client()

        chain_result = await rpc.verify_expected_chain(force_refresh=True)
        if not chain_result.get("ok") or chain_result.get("chain_id_matches") is not True:
            raise ValueError("robinhood_chain_receipt_rpc_chain_mismatch")

        tx_result = await rpc.rpc_read(
            "eth_getTransactionByHash",
            [row.tx_hash],
            cache_namespace=f"rh-chain-execution-tx:{row.tx_hash}",
            force_refresh=True,
        )
        receipt_result = await rpc.rpc_read(
            "eth_getTransactionReceipt",
            [row.tx_hash],
            cache_namespace=f"rh-chain-execution-receipt:{row.tx_hash}",
            force_refresh=True,
        )
        if not tx_result.get("ok"):
            raise ValueError(str(tx_result.get("error") or "robinhood_chain_transaction_lookup_failed"))
        if not receipt_result.get("ok"):
            raise ValueError(str(receipt_result.get("error") or "robinhood_chain_receipt_lookup_failed"))

        tx = tx_result.get("result")
        receipt = receipt_result.get("result")
        row.last_receipt_check_at = datetime.utcnow()
        row.updated_at = datetime.utcnow()

        if tx is None:
            # Never mark a new receipt terminal until the submitted transaction
            # body is available and verified against the reviewed plan. A row
            # already confirmed by an earlier tranche remains confirmed while
            # reconciliation waits for a complete provider response.
            if row.status != "confirmed":
                row.status = "pending"
            db.commit()
            db.refresh(row)
            return {
                "ok": True,
                "terminal": row.status == "confirmed",
                "transaction_details_available": False,
                "reconciliation_pending": row.status == "confirmed",
                "execution": serialize_execution(row),
            }
        if not isinstance(tx, dict):
            raise ValueError("invalid_robinhood_chain_transaction_lookup")

        mismatches = []
        try:
            observed_tx_hash = validate_transaction_hash(tx.get("hash"))
            observed_from = validate_evm_address(str(tx.get("from") or "")).lower()
            observed_to = validate_evm_address(str(tx.get("to") or "")).lower()
            observed_value = decode_hex_quantity(tx.get("value"))
            calldata = str(tx.get("input") or tx.get("data") or "").strip()
            if not calldata.startswith("0x") or len(calldata[2:]) % 2 != 0:
                raise ValueError("invalid_transaction_calldata")
            observed_hash = hashlib.sha256(bytes.fromhex(calldata[2:])).hexdigest()
        except Exception:
            observed_tx_hash = ""
            observed_from = ""
            observed_to = ""
            observed_value = -1
            observed_hash = ""
            mismatches.append("malformed_transaction")

        if observed_tx_hash != row.tx_hash.lower():
            mismatches.append("hash")
        if observed_from != row.wallet_address.lower():
            mismatches.append("from")
        if observed_to != row.transaction_to.lower():
            mismatches.append("to")
        if observed_value != int(row.transaction_value_wei):
            mismatches.append("value")
        if observed_hash != row.calldata_sha256.lower():
            mismatches.append("calldata")
        if mismatches:
            row.status = "verification_failed"
            row.error_code = "onchain_transaction_mismatch"
            row.error_message = "Transaction fields differed from the reviewed plan: " + ", ".join(dict.fromkeys(mismatches))
            db.commit()
            db.refresh(row)
            return {"ok": True, "terminal": True, "execution": serialize_execution(row)}

        if receipt is None:
            if row.status != "confirmed":
                row.status = "pending"
            db.commit()
            db.refresh(row)
            return {
                "ok": True,
                "terminal": row.status == "confirmed",
                "transaction_details_available": True,
                "reconciliation_pending": row.status == "confirmed",
                "execution": serialize_execution(row),
            }
        if not isinstance(receipt, dict):
            raise ValueError("invalid_robinhood_chain_transaction_receipt")

        receipt_hash = validate_transaction_hash(receipt.get("transactionHash"))
        if receipt_hash != row.tx_hash.lower():
            raise ValueError("robinhood_chain_receipt_hash_mismatch")
        if receipt.get("from"):
            receipt_from = validate_evm_address(str(receipt.get("from"))).lower()
            if receipt_from != row.wallet_address.lower():
                raise ValueError("robinhood_chain_receipt_from_mismatch")
        if receipt.get("to"):
            receipt_to = validate_evm_address(str(receipt.get("to"))).lower()
            if receipt_to != row.transaction_to.lower():
                raise ValueError("robinhood_chain_receipt_to_mismatch")

        status_value = decode_hex_quantity(receipt.get("status"))
        if status_value not in {0, 1}:
            raise ValueError("invalid_robinhood_chain_receipt_status")

        row.receipt_status = status_value
        row.block_number = decode_hex_quantity(receipt.get("blockNumber")) if receipt.get("blockNumber") else None
        row.gas_used = str(decode_hex_quantity(receipt.get("gasUsed"))) if receipt.get("gasUsed") else None
        row.effective_gas_price_wei = (
            str(decode_hex_quantity(receipt.get("effectiveGasPrice")))
            if receipt.get("effectiveGasPrice")
            else None
        )
        if status_value == 1:
            try:
                reconciliation = _decode_confirmed_execution_reconciliation(row, receipt)
            except ValueError as exc:
                row.status = "verification_failed"
                row.error_code = "receipt_reconciliation_failed"
                row.error_message = str(exc)
                db.commit()
                db.refresh(row)
                return {
                    "ok": True,
                    "terminal": True,
                    "reconciled": False,
                    "execution": serialize_execution(row),
                }

            route = dict(row.route or {}) if isinstance(row.route, dict) else {}
            route["execution_reconciliation"] = reconciliation
            row.route = route
            row.status = "confirmed"
            row.confirmed_at = row.confirmed_at or datetime.utcnow()
            row.reverted_at = None
            row.error_code = None
            row.error_message = None
        else:
            row.status = "reverted"
            row.reverted_at = datetime.utcnow()
            row.confirmed_at = None
            row.error_code = "transaction_reverted"
            row.error_message = "Robinhood Chain transaction receipt returned status 0."
        db.commit()
        db.refresh(row)
        return {
            "ok": True,
            "terminal": True,
            "reconciled": bool(_execution_reconciliation(row)),
            "execution": serialize_execution(row),
        }


_service: Optional[RobinhoodChainExecutionService] = None


def get_robinhood_chain_execution_service() -> RobinhoodChainExecutionService:
    global _service
    if _service is None:
        _service = RobinhoodChainExecutionService()
    return _service
