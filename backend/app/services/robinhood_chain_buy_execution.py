from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from ..config import settings
from ..models import RobinhoodChainBuyExecution
from .evm_rpc import (
    decode_hex_quantity,
    encode_erc20_approve,
    get_robinhood_chain_client,
    validate_evm_address,
)
from .robinhood_chain_execution import (
    validate_claim_id,
    validate_execution_saved_wallet,
    validate_transaction_hash,
)
from .robinhood_chain_transaction_planning import (
    EXPECTED_CHAIN_ID,
    ROBINHOOD_CHAIN_ALLOWANCE_HOLDER_ALLOWLIST,
    RobinhoodChainTransactionPlanningService,
    get_robinhood_chain_transaction_planning_service,
)


ROBINHOOD_CHAIN_BUY_TRANCHE = "RH-CHAIN.10D.2"
ROBINHOOD_CHAIN_BUY_SYMBOL = "ETH-USDG"
ROBINHOOD_CHAIN_BUY_SIDE = "buy"
ROBINHOOD_CHAIN_BUY_EXACT_OUTPUT_ETH = Decimal("0.001")
ROBINHOOD_CHAIN_BUY_EXACT_OUTPUT_WEI = 1_000_000_000_000_000
ROBINHOOD_CHAIN_BUY_MAXIMUM_USDG = Decimal("2")
ROBINHOOD_CHAIN_BUY_MAXIMUM_USDG_ATOMIC = 2_000_000
ROBINHOOD_CHAIN_BUY_APPROVAL_USDG = Decimal("2")
ROBINHOOD_CHAIN_BUY_APPROVAL_ATOMIC = 2_000_000
ROBINHOOD_CHAIN_BUY_SLIPPAGE_BPS = 100
ROBINHOOD_CHAIN_USDG_CONTRACT = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
ROBINHOOD_CHAIN_USDG_DECIMALS = 6
ROBINHOOD_CHAIN_APPROVAL_GAS_LIMIT = 100_000
ROBINHOOD_CHAIN_MAX_APPROVAL_GAS_LIMIT = 200_000

ROBINHOOD_CHAIN_BUY_SUBMISSION_FAILURE_REASONS = frozenset(
    {"wallet_rejected", "wallet_request_failed"}
)
ROBINHOOD_CHAIN_BUY_TERMINAL_STATUSES = frozenset(
    {
        "confirmed",
        "approval_reverted",
        "swap_reverted",
        "approval_wallet_rejected",
        "approval_submission_failed",
        "swap_wallet_rejected",
        "swap_submission_failed",
        "verification_failed",
    }
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ERC20_TRANSFER_TOPIC0 = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
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


def _iso(value: Optional[datetime]) -> Optional[str]:
    return _as_utc(value).isoformat() if value is not None else None


def _decimal_text(value: Any) -> str:
    number = Decimal(str(value))
    text = format(number, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _topic_address(value: Any) -> Optional[str]:
    raw = str(value or "").strip().lower()
    if not re.fullmatch(r"0x[0-9a-f]{64}", raw):
        return None
    return "0x" + raw[-40:]


def _hash_payload(payload: Dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _buy_gate() -> Dict[str, Any]:
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
        "tranche": ROBINHOOD_CHAIN_BUY_TRANCHE,
        "chain_id": EXPECTED_CHAIN_ID,
        "symbol": ROBINHOOD_CHAIN_BUY_SYMBOL,
        "side": ROBINHOOD_CHAIN_BUY_SIDE,
        "exact_output_eth": _decimal_text(ROBINHOOD_CHAIN_BUY_EXACT_OUTPUT_ETH),
        "exact_output_wei": str(ROBINHOOD_CHAIN_BUY_EXACT_OUTPUT_WEI),
        "maximum_usdg_spend": _decimal_text(ROBINHOOD_CHAIN_BUY_MAXIMUM_USDG),
        "maximum_usdg_spend_atomic": str(ROBINHOOD_CHAIN_BUY_MAXIMUM_USDG_ATOMIC),
        "approval_amount_usdg": _decimal_text(ROBINHOOD_CHAIN_BUY_APPROVAL_USDG),
        "approval_amount_atomic": str(ROBINHOOD_CHAIN_BUY_APPROVAL_ATOMIC),
        "maximum_slippage_bps": ROBINHOOD_CHAIN_BUY_SLIPPAGE_BPS,
        "dedicated_execution_enabled": dedicated,
        "armed": armed,
        "dry_run": dry_run,
        "chain_ready": chain_ready,
        "send_enabled": send_enabled,
        "missing_requirements": missing,
        "unlimited_approval_enabled": False,
        "automatic_second_transaction": False,
        "backend_private_key": False,
        "backend_transaction_sender": False,
        "automatic_retry": False,
        "generic_live_venues_required": False,
        "ledger_mutation_enabled": False,
        "fifo_mutation_enabled": False,
        "basis_mutation_enabled": False,
    }


def _validate_locked_row(row: RobinhoodChainBuyExecution) -> None:
    if int(row.chain_id or 0) != EXPECTED_CHAIN_ID:
        raise ValueError("robinhood_chain_buy_locked_chain_mismatch")
    if str(row.symbol or "").strip().upper() != ROBINHOOD_CHAIN_BUY_SYMBOL:
        raise ValueError("robinhood_chain_buy_locked_symbol_mismatch")
    if str(row.side or "").strip().lower() != ROBINHOOD_CHAIN_BUY_SIDE:
        raise ValueError("robinhood_chain_buy_locked_side_mismatch")
    if str(row.exact_output_amount_atomic or "") != str(ROBINHOOD_CHAIN_BUY_EXACT_OUTPUT_WEI):
        raise ValueError("robinhood_chain_buy_locked_output_mismatch")
    if str(row.maximum_input_amount_atomic or "") != str(ROBINHOOD_CHAIN_BUY_MAXIMUM_USDG_ATOMIC):
        raise ValueError("robinhood_chain_buy_locked_maximum_input_mismatch")
    if str(row.approval_amount_atomic or "") != str(ROBINHOOD_CHAIN_BUY_APPROVAL_ATOMIC):
        raise ValueError("robinhood_chain_buy_locked_approval_mismatch")
    if int(row.slippage_bps or 0) != ROBINHOOD_CHAIN_BUY_SLIPPAGE_BPS:
        raise ValueError("robinhood_chain_buy_locked_slippage_mismatch")
    if validate_evm_address(row.approval_token_address).lower() != ROBINHOOD_CHAIN_USDG_CONTRACT:
        raise ValueError("robinhood_chain_buy_locked_token_mismatch")
    if row.approval_spender.lower() not in ROBINHOOD_CHAIN_ALLOWANCE_HOLDER_ALLOWLIST:
        raise ValueError("robinhood_chain_buy_locked_spender_mismatch")


def _reconciliation(row: RobinhoodChainBuyExecution) -> Dict[str, Any]:
    route = row.route if isinstance(row.route, dict) else {}
    value = route.get("execution_reconciliation")
    return dict(value) if isinstance(value, dict) else {}


def serialize_buy_execution(row: RobinhoodChainBuyExecution) -> Dict[str, Any]:
    reconciliation = _reconciliation(row)
    return {
        "id": str(row.id),
        "venue": "robinhood_chain",
        "tranche": ROBINHOOD_CHAIN_BUY_TRANCHE,
        "chain_id": int(row.chain_id),
        "wallet_address": row.wallet_address,
        "symbol": row.symbol,
        "side": row.side,
        "exact_output_asset": row.exact_output_asset,
        "exact_output_amount": row.exact_output_amount,
        "exact_output_amount_atomic": row.exact_output_amount_atomic,
        "maximum_input_asset": row.maximum_input_asset,
        "maximum_input_amount": row.maximum_input_amount,
        "maximum_input_amount_atomic": row.maximum_input_amount_atomic,
        "slippage_bps": int(row.slippage_bps),
        "status": row.status,
        "approval": {
            "token_address": row.approval_token_address,
            "spender": row.approval_spender,
            "amount": row.approval_amount,
            "amount_atomic": row.approval_amount_atomic,
            "allowance_before_atomic": row.allowance_before_atomic,
            "allowance_confirmed_atomic": row.allowance_confirmed_atomic,
            "allowance_confirmed_at": _iso(row.allowance_confirmed_at),
            "plan_hash": row.approval_plan_hash,
            "transaction_to": row.approval_transaction_to,
            "transaction_value_wei": row.approval_transaction_value_wei,
            "calldata_sha256": row.approval_calldata_sha256,
            "calldata_bytes": int(row.approval_calldata_bytes or 0),
            "gas_limit": row.approval_gas_limit,
            "gas_price_wei": row.approval_gas_price_wei,
            "send_claimed": bool(row.approval_send_claim_id),
            "send_claimed_at": _iso(row.approval_send_claimed_at),
            "submission_attempts": int(row.approval_submission_attempts or 0),
            "tx_hash": row.approval_tx_hash,
            "submitted_at": _iso(row.approval_submitted_at),
            "confirmed_at": _iso(row.approval_confirmed_at),
            "reverted_at": _iso(row.approval_reverted_at),
            "block_number": row.approval_block_number,
            "gas_used": row.approval_gas_used,
            "effective_gas_price_wei": row.approval_effective_gas_price_wei,
            "receipt_status": row.approval_receipt_status,
        },
        "swap": {
            "quote_id": row.swap_quote_id,
            "expected_input_amount": row.expected_input_amount,
            "expected_input_amount_atomic": row.expected_input_amount_atomic,
            "plan_hash": row.swap_plan_hash,
            "plan_fetched_at": _iso(row.swap_plan_fetched_at),
            "plan_expires_at": _iso(row.swap_plan_expires_at),
            "transaction_to": row.swap_transaction_to,
            "transaction_value_wei": row.swap_transaction_value_wei,
            "calldata_sha256": row.swap_calldata_sha256,
            "calldata_bytes": row.swap_calldata_bytes,
            "gas_limit": row.swap_gas_limit,
            "gas_price_wei": row.swap_gas_price_wei,
            "send_claimed": bool(row.swap_send_claim_id),
            "send_claimed_at": _iso(row.swap_send_claimed_at),
            "submission_attempts": int(row.swap_submission_attempts or 0),
            "tx_hash": row.swap_tx_hash,
            "submitted_at": _iso(row.swap_submitted_at),
            "confirmed_at": _iso(row.swap_confirmed_at),
            "reverted_at": _iso(row.swap_reverted_at),
            "block_number": row.swap_block_number,
            "gas_used": row.swap_gas_used,
            "effective_gas_price_wei": row.swap_effective_gas_price_wei,
            "receipt_status": row.swap_receipt_status,
        },
        "reconciliation": reconciliation or None,
        "actual_input_asset": reconciliation.get("input_asset"),
        "actual_input_amount": reconciliation.get("input_amount"),
        "actual_input_amount_atomic": reconciliation.get("input_amount_atomic"),
        "actual_output_asset": reconciliation.get("output_asset"),
        "actual_output_amount": reconciliation.get("output_amount"),
        "actual_output_amount_atomic": reconciliation.get("output_amount_atomic"),
        "actual_average_fill_price": reconciliation.get("average_fill_price"),
        "actual_network_fee": reconciliation.get("swap_network_fee"),
        "actual_network_fee_asset": reconciliation.get("fee_asset"),
        "actual_approval_network_fee": reconciliation.get("approval_network_fee"),
        "error_code": row.error_code,
        "error_message": row.error_message,
        "created_at": _iso(row.created_at),
        "updated_at": _iso(row.updated_at),
    }


class RobinhoodChainBuyExecutionService:
    """Two explicit browser-wallet transactions for one bounded exact-output BUY."""

    def __init__(
        self,
        *,
        planning_service: Optional[RobinhoodChainTransactionPlanningService] = None,
        rpc_client: Any = None,
    ) -> None:
        self.planning_service = planning_service or get_robinhood_chain_transaction_planning_service()
        self.rpc_client = rpc_client or get_robinhood_chain_client()

    def status(self) -> Dict[str, Any]:
        return {"ok": True, **_buy_gate()}

    def _get(self, db: Session, execution_id: str) -> RobinhoodChainBuyExecution:
        row = db.get(RobinhoodChainBuyExecution, str(execution_id or "").strip())
        if row is None:
            raise KeyError("robinhood_chain_buy_execution_not_found")
        _validate_locked_row(row)
        return row

    def get(self, db: Session, execution_id: str) -> Dict[str, Any]:
        row = self._get(db, execution_id)
        return {"ok": True, "execution": serialize_buy_execution(row), "send_gate": _buy_gate()}

    async def prepare_approval(
        self,
        db: Session,
        *,
        taker_address: str,
        eth_token: Dict[str, Any],
        usdg_token: Dict[str, Any],
        confirm_prepare: bool,
    ) -> Dict[str, Any]:
        if confirm_prepare is not True:
            raise ValueError("confirm_buy_approval_prepare_required")
        wallet = validate_evm_address(taker_address).lower()
        if validate_evm_address(str(usdg_token.get("contract_address") or "")).lower() != ROBINHOOD_CHAIN_USDG_CONTRACT:
            raise ValueError("robinhood_chain_buy_usdg_identity_mismatch")
        if int(usdg_token.get("decimals") or -1) != ROBINHOOD_CHAIN_USDG_DECIMALS:
            raise ValueError("robinhood_chain_buy_usdg_decimals_mismatch")

        existing = (
            db.query(RobinhoodChainBuyExecution)
            .filter(
                RobinhoodChainBuyExecution.wallet_address == wallet,
                RobinhoodChainBuyExecution.status.in_([
                    "approval_prepared", "approval_send_claimed", "approval_pending",
                    "approval_confirmed", "swap_prepared", "swap_send_claimed", "swap_pending",
                ]),
            )
            .order_by(RobinhoodChainBuyExecution.created_at.desc())
            .first()
        )
        if existing is not None:
            _validate_locked_row(existing)
            return {
                "ok": True,
                "idempotent": True,
                "execution": serialize_buy_execution(existing),
                "approval_transaction_plan": self._approval_plan(existing),
                "send_gate": _buy_gate(),
            }

        plan = await self.planning_service.firm_quote_plan(
            symbol=ROBINHOOD_CHAIN_BUY_SYMBOL,
            side="buy",
            quantity=None,
            total_quote=None,
            exact_output_quantity=_decimal_text(ROBINHOOD_CHAIN_BUY_EXACT_OUTPUT_ETH),
            maximum_total_quote=_decimal_text(ROBINHOOD_CHAIN_BUY_MAXIMUM_USDG),
            taker_address=wallet,
            eth_token=eth_token,
            usdg_token=usdg_token,
            slippage_bps=ROBINHOOD_CHAIN_BUY_SLIPPAGE_BPS,
        )
        if plan.get("ok") is not True:
            raise ValueError(str(plan.get("error") or "robinhood_chain_buy_firm_plan_failed"))
        if str(plan.get("amount_mode")) != "exact_output":
            raise ValueError("robinhood_chain_buy_plan_not_exact_output")
        if str(plan.get("output_amount_atomic")) != str(ROBINHOOD_CHAIN_BUY_EXACT_OUTPUT_WEI):
            raise ValueError("robinhood_chain_buy_plan_output_mismatch")
        if int(str(plan.get("input_amount_atomic") or "0")) > ROBINHOOD_CHAIN_BUY_MAXIMUM_USDG_ATOMIC:
            raise ValueError("robinhood_chain_buy_plan_exceeds_maximum")
        if str(plan.get("maximum_input_ceiling_atomic")) != str(ROBINHOOD_CHAIN_BUY_MAXIMUM_USDG_ATOMIC):
            raise ValueError("robinhood_chain_buy_plan_ceiling_mismatch")

        allowance = plan.get("allowance") if isinstance(plan.get("allowance"), dict) else {}
        if str(allowance.get("read_method")) != "eth_call":
            raise ValueError("robinhood_chain_buy_allowance_not_read_by_eth_call")
        spender = validate_evm_address(str(allowance.get("spender") or "")).lower()
        if spender not in ROBINHOOD_CHAIN_ALLOWANCE_HOLDER_ALLOWLIST or allowance.get("spender_allowlisted") is not True:
            raise ValueError("robinhood_chain_buy_spender_not_allowlisted")
        current_allowance = int(str(allowance.get("current_atomic") or "0"))

        calldata = encode_erc20_approve(spender, ROBINHOOD_CHAIN_BUY_APPROVAL_ATOMIC)
        calldata_hash = hashlib.sha256(bytes.fromhex(calldata[2:])).hexdigest()
        gas_price = str((plan.get("unsigned_transaction_plan") or {}).get("gas_price_wei") or "0")
        if not gas_price.isdigit() or int(gas_price) <= 0:
            raise ValueError("robinhood_chain_buy_approval_gas_price_missing")
        approval_plan_hash = _hash_payload({
            "chain_id": EXPECTED_CHAIN_ID,
            "wallet": wallet,
            "token": ROBINHOOD_CHAIN_USDG_CONTRACT,
            "spender": spender,
            "amount_atomic": str(ROBINHOOD_CHAIN_BUY_APPROVAL_ATOMIC),
            "calldata_sha256": calldata_hash,
        })
        now = utc_now()
        already_sufficient = current_allowance >= ROBINHOOD_CHAIN_BUY_APPROVAL_ATOMIC
        row = RobinhoodChainBuyExecution(
            chain_id=EXPECTED_CHAIN_ID,
            wallet_address=wallet,
            symbol=ROBINHOOD_CHAIN_BUY_SYMBOL,
            side=ROBINHOOD_CHAIN_BUY_SIDE,
            exact_output_asset="ETH",
            exact_output_amount=_decimal_text(ROBINHOOD_CHAIN_BUY_EXACT_OUTPUT_ETH),
            exact_output_amount_atomic=str(ROBINHOOD_CHAIN_BUY_EXACT_OUTPUT_WEI),
            maximum_input_asset="USDG",
            maximum_input_amount=_decimal_text(ROBINHOOD_CHAIN_BUY_MAXIMUM_USDG),
            maximum_input_amount_atomic=str(ROBINHOOD_CHAIN_BUY_MAXIMUM_USDG_ATOMIC),
            slippage_bps=ROBINHOOD_CHAIN_BUY_SLIPPAGE_BPS,
            approval_token_address=ROBINHOOD_CHAIN_USDG_CONTRACT,
            approval_spender=spender,
            approval_amount=_decimal_text(ROBINHOOD_CHAIN_BUY_APPROVAL_USDG),
            approval_amount_atomic=str(ROBINHOOD_CHAIN_BUY_APPROVAL_ATOMIC),
            allowance_before_atomic=str(current_allowance),
            allowance_confirmed_atomic=str(current_allowance) if already_sufficient else None,
            allowance_confirmed_at=_utc_naive(now) if already_sufficient else None,
            approval_plan_hash=approval_plan_hash,
            approval_transaction_to=ROBINHOOD_CHAIN_USDG_CONTRACT,
            approval_transaction_value_wei="0",
            approval_calldata_sha256=calldata_hash,
            approval_calldata_bytes=len(bytes.fromhex(calldata[2:])),
            approval_gas_limit=str(ROBINHOOD_CHAIN_APPROVAL_GAS_LIMIT),
            approval_gas_price_wei=gas_price,
            status="approval_confirmed" if already_sufficient else "approval_prepared",
            created_at=_utc_naive(now),
            updated_at=_utc_naive(now),
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        return {
            "ok": True,
            "idempotent": False,
            "approval_required": not already_sufficient,
            "execution": serialize_buy_execution(row),
            "approval_transaction_plan": None if already_sufficient else self._approval_plan(row),
            "source_firm_plan": plan,
            "send_gate": _buy_gate(),
        }

    def _approval_plan(self, row: RobinhoodChainBuyExecution) -> Dict[str, Any]:
        calldata = encode_erc20_approve(row.approval_spender, row.approval_amount_atomic)
        digest = hashlib.sha256(bytes.fromhex(calldata[2:])).hexdigest()
        if digest != row.approval_calldata_sha256:
            raise ValueError("robinhood_chain_buy_approval_calldata_hash_mismatch")
        return {
            "stage": "approval",
            "review_only": True,
            "chain_id": EXPECTED_CHAIN_ID,
            "chain_id_hex": hex(EXPECTED_CHAIN_ID),
            "from": row.wallet_address,
            "to": row.approval_transaction_to,
            "value_wei": "0",
            "gas_limit": row.approval_gas_limit,
            "gas_price_wei": row.approval_gas_price_wei,
            "calldata": calldata,
            "calldata_sha256": digest,
            "calldata_bytes": row.approval_calldata_bytes,
            "token": row.approval_token_address,
            "spender": row.approval_spender,
            "approval_amount": row.approval_amount,
            "approval_amount_atomic": row.approval_amount_atomic,
            "finite_approval": True,
            "unlimited_approval": False,
            "signing_requested": False,
            "broadcast_requested": False,
        }

    def claim_approval_send(
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
            raise ValueError("confirm_buy_approval_send_claim_required")
        gate = _buy_gate()
        if gate.get("send_enabled") is not True:
            raise ValueError("robinhood_chain_buy_send_gate_blocked")
        row = self._get(db, execution_id)
        wallet = validate_execution_saved_wallet(row.wallet_address, wallet_address)
        claim = validate_claim_id(claim_id)
        plan = str(plan_hash or "").strip().lower()
        if not _SHA256_RE.fullmatch(plan) or plan != row.approval_plan_hash:
            raise ValueError("robinhood_chain_buy_approval_plan_hash_mismatch")
        if row.status == "approval_send_claimed" and row.approval_send_claim_id == claim:
            return {"ok": True, "idempotent": True, "execution": serialize_buy_execution(row), "approval_transaction_plan": self._approval_plan(row)}
        if row.status != "approval_prepared" or row.approval_send_claim_id:
            raise ValueError("robinhood_chain_buy_approval_not_claimable")
        row.approval_send_claim_id = claim
        row.approval_send_claimed_at = _utc_naive(utc_now())
        row.status = "approval_send_claimed"
        row.updated_at = _utc_naive(utc_now())
        db.add(row); db.commit(); db.refresh(row)
        return {"ok": True, "idempotent": False, "wallet_address": wallet, "execution": serialize_buy_execution(row), "approval_transaction_plan": self._approval_plan(row), "send_gate": gate}

    def record_approval_submission(
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
            raise ValueError("confirm_buy_approval_submission_record_required")
        row = self._get(db, execution_id)
        validate_execution_saved_wallet(row.wallet_address, wallet_address)
        claim = validate_claim_id(claim_id)
        tx = validate_transaction_hash(tx_hash)
        if row.approval_tx_hash == tx and row.status in {"approval_pending", "approval_confirmed"}:
            return {"ok": True, "idempotent": True, "execution": serialize_buy_execution(row)}
        if row.status != "approval_send_claimed" or row.approval_send_claim_id != claim:
            raise ValueError("robinhood_chain_buy_approval_claim_mismatch")
        row.approval_tx_hash = tx
        row.approval_submitted_at = _utc_naive(utc_now())
        row.approval_submission_attempts = int(row.approval_submission_attempts or 0) + 1
        row.status = "approval_pending"
        row.updated_at = _utc_naive(utc_now())
        db.add(row); db.commit(); db.refresh(row)
        return {"ok": True, "idempotent": False, "execution": serialize_buy_execution(row)}

    def record_submission_failure(
        self,
        db: Session,
        *,
        execution_id: str,
        stage: str,
        wallet_address: str,
        claim_id: str,
        reason: str,
        message: Optional[str],
        confirm_failure: bool,
    ) -> Dict[str, Any]:
        if confirm_failure is not True:
            raise ValueError("confirm_buy_submission_failure_required")
        stage_n = str(stage or "").strip().lower()
        if stage_n not in {"approval", "swap"}:
            raise ValueError("invalid_robinhood_chain_buy_failure_stage")
        reason_n = str(reason or "").strip().lower()
        if reason_n not in ROBINHOOD_CHAIN_BUY_SUBMISSION_FAILURE_REASONS:
            raise ValueError("invalid_robinhood_chain_buy_failure_reason")
        row = self._get(db, execution_id)
        validate_execution_saved_wallet(row.wallet_address, wallet_address)
        claim = validate_claim_id(claim_id)
        if stage_n == "approval":
            if row.status != "approval_send_claimed" or row.approval_send_claim_id != claim or row.approval_tx_hash:
                raise ValueError("robinhood_chain_buy_approval_claim_mismatch")
            row.status = "approval_wallet_rejected" if reason_n == "wallet_rejected" else "approval_submission_failed"
            row.approval_submission_failure_at = _utc_naive(utc_now())
        else:
            if row.status != "swap_send_claimed" or row.swap_send_claim_id != claim or row.swap_tx_hash:
                raise ValueError("robinhood_chain_buy_swap_claim_mismatch")
            row.status = "swap_wallet_rejected" if reason_n == "wallet_rejected" else "swap_submission_failed"
            row.swap_submission_failure_at = _utc_naive(utc_now())
        row.error_code = reason_n
        row.error_message = str(message or "")[:512] or None
        row.updated_at = _utc_naive(utc_now())
        db.add(row); db.commit(); db.refresh(row)
        return {"ok": True, "execution": serialize_buy_execution(row)}

    async def _verified_transaction(self, row: RobinhoodChainBuyExecution, *, stage: str) -> Optional[Dict[str, Any]]:
        tx_hash = row.approval_tx_hash if stage == "approval" else row.swap_tx_hash
        result = await self.rpc_client.rpc_read("eth_getTransactionByHash", [tx_hash], cache_namespace=None, force_refresh=True)
        if result.get("ok") is not True:
            raise ValueError("robinhood_chain_buy_transaction_read_failed")
        tx = result.get("result")
        if tx is None:
            return None
        expected_to = row.approval_transaction_to if stage == "approval" else row.swap_transaction_to
        expected_value = row.approval_transaction_value_wei if stage == "approval" else row.swap_transaction_value_wei
        expected_hash = row.approval_calldata_sha256 if stage == "approval" else row.swap_calldata_sha256
        if validate_transaction_hash(tx.get("hash")) != tx_hash:
            raise ValueError("robinhood_chain_buy_transaction_hash_mismatch")
        if validate_evm_address(tx.get("from")).lower() != row.wallet_address.lower():
            raise ValueError("robinhood_chain_buy_transaction_sender_mismatch")
        if validate_evm_address(tx.get("to")).lower() != str(expected_to).lower():
            raise ValueError("robinhood_chain_buy_transaction_destination_mismatch")
        if decode_hex_quantity(tx.get("value")) != int(str(expected_value or "0")):
            raise ValueError("robinhood_chain_buy_transaction_value_mismatch")
        calldata = str(tx.get("input") or "").strip()
        if not re.fullmatch(r"0x[0-9a-fA-F]+", calldata) or len(calldata[2:]) % 2:
            raise ValueError("robinhood_chain_buy_transaction_calldata_invalid")
        if hashlib.sha256(bytes.fromhex(calldata[2:])).hexdigest() != expected_hash:
            raise ValueError("robinhood_chain_buy_transaction_calldata_mismatch")
        return tx

    async def refresh_approval(self, db: Session, *, execution_id: str) -> Dict[str, Any]:
        row = self._get(db, execution_id)
        if row.status == "approval_confirmed":
            return {"ok": True, "idempotent": True, "execution": serialize_buy_execution(row)}
        if row.status != "approval_pending" or not row.approval_tx_hash:
            raise ValueError("robinhood_chain_buy_approval_not_pending")
        chain = await self.rpc_client.verify_expected_chain(force_refresh=True)
        if chain.get("ok") is not True or chain.get("chain_id_matches") is not True:
            raise ValueError("robinhood_chain_buy_chain_mismatch")
        tx = await self._verified_transaction(row, stage="approval")
        receipt_record = await self.rpc_client.rpc_read("eth_getTransactionReceipt", [row.approval_tx_hash], cache_namespace=None, force_refresh=True)
        if receipt_record.get("ok") is not True:
            raise ValueError("robinhood_chain_buy_approval_receipt_read_failed")
        receipt = receipt_record.get("result")
        row.approval_last_receipt_check_at = _utc_naive(utc_now())
        if tx is None or receipt is None:
            db.add(row); db.commit(); db.refresh(row)
            return {"ok": True, "pending": True, "execution": serialize_buy_execution(row)}
        status = decode_hex_quantity(receipt.get("status"))
        row.approval_receipt_status = status
        row.approval_block_number = decode_hex_quantity(receipt.get("blockNumber")) if receipt.get("blockNumber") else None
        row.approval_gas_used = str(decode_hex_quantity(receipt.get("gasUsed"))) if receipt.get("gasUsed") else None
        row.approval_effective_gas_price_wei = str(decode_hex_quantity(receipt.get("effectiveGasPrice"))) if receipt.get("effectiveGasPrice") else None
        if status == 0:
            row.status = "approval_reverted"
            row.approval_reverted_at = _utc_naive(utc_now())
            row.error_code = "approval_reverted"
        elif status == 1:
            allowance = await self.rpc_client.get_erc20_allowance(
                owner_address=row.wallet_address,
                contract_address=row.approval_token_address,
                spender_address=row.approval_spender,
                decimals=ROBINHOOD_CHAIN_USDG_DECIMALS,
                force_refresh=True,
            )
            if allowance.get("ok") is not True:
                raise ValueError("robinhood_chain_buy_post_approval_allowance_read_failed")
            confirmed = int(str(allowance.get("allowance_atomic") or "0"))
            if confirmed < ROBINHOOD_CHAIN_BUY_APPROVAL_ATOMIC:
                raise ValueError("robinhood_chain_buy_post_approval_allowance_insufficient")
            row.allowance_confirmed_atomic = str(confirmed)
            row.allowance_confirmed_at = _utc_naive(utc_now())
            row.approval_confirmed_at = _utc_naive(utc_now())
            row.status = "approval_confirmed"
        else:
            raise ValueError("robinhood_chain_buy_invalid_approval_receipt_status")
        row.updated_at = _utc_naive(utc_now())
        db.add(row); db.commit(); db.refresh(row)
        return {"ok": True, "pending": False, "execution": serialize_buy_execution(row)}

    async def prepare_swap(
        self,
        db: Session,
        *,
        execution_id: str,
        wallet_address: str,
        eth_token: Dict[str, Any],
        usdg_token: Dict[str, Any],
        confirm_prepare: bool,
    ) -> Dict[str, Any]:
        if confirm_prepare is not True:
            raise ValueError("confirm_buy_swap_prepare_required")
        row = self._get(db, execution_id)
        validate_execution_saved_wallet(row.wallet_address, wallet_address)
        if row.status not in {"approval_confirmed", "swap_prepared"}:
            raise ValueError("robinhood_chain_buy_approval_not_confirmed")
        if row.swap_send_claim_id or row.swap_tx_hash:
            raise ValueError("robinhood_chain_buy_swap_already_claimed_or_submitted")
        # A review-only swap plan can be safely replaced before a one-time send
        # claim exists. This allows an expired plan or a browser-reloaded view to
        # obtain fresh calldata without reusing the old quote or sending anything.

        allowance = await self.rpc_client.get_erc20_allowance(
            owner_address=row.wallet_address,
            contract_address=row.approval_token_address,
            spender_address=row.approval_spender,
            decimals=ROBINHOOD_CHAIN_USDG_DECIMALS,
            force_refresh=True,
        )
        if allowance.get("ok") is not True or int(str(allowance.get("allowance_atomic") or "0")) < ROBINHOOD_CHAIN_BUY_APPROVAL_ATOMIC:
            raise ValueError("robinhood_chain_buy_fresh_allowance_insufficient")

        plan = await self.planning_service.firm_quote_plan(
            symbol=ROBINHOOD_CHAIN_BUY_SYMBOL,
            side="buy",
            quantity=None,
            total_quote=None,
            exact_output_quantity=_decimal_text(ROBINHOOD_CHAIN_BUY_EXACT_OUTPUT_ETH),
            maximum_total_quote=_decimal_text(ROBINHOOD_CHAIN_BUY_MAXIMUM_USDG),
            taker_address=row.wallet_address,
            eth_token=eth_token,
            usdg_token=usdg_token,
            slippage_bps=ROBINHOOD_CHAIN_BUY_SLIPPAGE_BPS,
        )
        if plan.get("ok") is not True:
            raise ValueError(str(plan.get("error") or "robinhood_chain_buy_fresh_plan_failed"))
        if str(plan.get("amount_mode")) != "exact_output" or str(plan.get("output_amount_atomic")) != str(ROBINHOOD_CHAIN_BUY_EXACT_OUTPUT_WEI):
            raise ValueError("robinhood_chain_buy_fresh_plan_output_mismatch")
        required_input = int(str(plan.get("input_amount_atomic") or "0"))
        if required_input <= 0 or required_input > ROBINHOOD_CHAIN_BUY_MAXIMUM_USDG_ATOMIC:
            raise ValueError("robinhood_chain_buy_fresh_plan_exceeds_maximum")
        plan_allowance = plan.get("allowance") if isinstance(plan.get("allowance"), dict) else {}
        if str(plan_allowance.get("read_method")) != "eth_call" or int(str(plan_allowance.get("current_atomic") or "0")) < required_input:
            raise ValueError("robinhood_chain_buy_fresh_plan_allowance_insufficient")
        if validate_evm_address(str(plan_allowance.get("spender") or "")).lower() != row.approval_spender.lower():
            raise ValueError("robinhood_chain_buy_spender_rotated")
        unsigned = plan.get("unsigned_transaction_plan") if isinstance(plan.get("unsigned_transaction_plan"), dict) else {}
        if str(unsigned.get("value_wei")) != "0" or unsigned.get("destination_allowlisted") is not True:
            raise ValueError("robinhood_chain_buy_swap_transaction_invalid")
        fetched = datetime.fromisoformat(str(plan.get("fetched_at")).replace("Z", "+00:00"))
        expires = datetime.fromisoformat(str(plan.get("plan_expires_at")).replace("Z", "+00:00"))
        calldata_hash = str(unsigned.get("calldata_sha256") or "").lower()
        if not _SHA256_RE.fullmatch(calldata_hash):
            raise ValueError("robinhood_chain_buy_swap_calldata_hash_invalid")
        swap_hash = _hash_payload({
            "chain_id": EXPECTED_CHAIN_ID,
            "wallet": row.wallet_address,
            "quote_id": str(plan.get("quote_id")),
            "output_atomic": str(plan.get("output_amount_atomic")),
            "maximum_input_atomic": str(plan.get("maximum_input_ceiling_atomic")),
            "transaction_to": str(unsigned.get("to")).lower(),
            "calldata_sha256": calldata_hash,
            "plan_expires_at": expires.isoformat(),
        })
        row.swap_quote_id = str(plan.get("quote_id"))
        row.expected_input_amount = str(plan.get("input_amount"))
        row.expected_input_amount_atomic = str(plan.get("input_amount_atomic"))
        row.swap_plan_hash = swap_hash
        row.swap_plan_fetched_at = _utc_naive(fetched)
        row.swap_plan_expires_at = _utc_naive(expires)
        row.swap_transaction_to = validate_evm_address(str(unsigned.get("to"))).lower()
        row.swap_transaction_value_wei = "0"
        row.swap_calldata_sha256 = calldata_hash
        row.swap_calldata_bytes = int(unsigned.get("calldata_bytes") or 0)
        row.swap_gas_limit = str(unsigned.get("gas_limit") or "")
        row.swap_gas_price_wei = str(unsigned.get("gas_price_wei") or "")
        row.route = {"fills": (plan.get("route") or {}).get("fills") or [], "firm_plan": {"quote_id": plan.get("quote_id"), "route_sources": plan.get("route_sources") or []}}
        row.status = "swap_prepared"
        row.error_code = None; row.error_message = None; row.updated_at = _utc_naive(utc_now())
        db.add(row); db.commit(); db.refresh(row)
        return {"ok": True, "idempotent": False, "execution": serialize_buy_execution(row), "unsigned_transaction_plan": {**self._swap_plan(row), "calldata": unsigned.get("calldata")}, "source_firm_plan": plan, "send_gate": _buy_gate()}

    def _swap_plan(self, row: RobinhoodChainBuyExecution) -> Dict[str, Any]:
        return {
            "stage": "swap",
            "review_only": True,
            "chain_id": EXPECTED_CHAIN_ID,
            "chain_id_hex": hex(EXPECTED_CHAIN_ID),
            "from": row.wallet_address,
            "to": row.swap_transaction_to,
            "value_wei": row.swap_transaction_value_wei,
            "gas_limit": row.swap_gas_limit,
            "gas_price_wei": row.swap_gas_price_wei,
            "calldata": None,
            "calldata_sha256": row.swap_calldata_sha256,
            "calldata_bytes": row.swap_calldata_bytes,
            "exact_output_eth": row.exact_output_amount,
            "maximum_input_usdg": row.maximum_input_amount,
            "expected_input_usdg": row.expected_input_amount,
            "signing_requested": False,
            "broadcast_requested": False,
        }

    def claim_swap_send(self, db: Session, *, execution_id: str, wallet_address: str, plan_hash: str, claim_id: str, confirm_send_claim: bool) -> Dict[str, Any]:
        if confirm_send_claim is not True:
            raise ValueError("confirm_buy_swap_send_claim_required")
        gate = _buy_gate()
        if gate.get("send_enabled") is not True:
            raise ValueError("robinhood_chain_buy_send_gate_blocked")
        row = self._get(db, execution_id)
        validate_execution_saved_wallet(row.wallet_address, wallet_address)
        claim = validate_claim_id(claim_id)
        if str(plan_hash or "").strip().lower() != row.swap_plan_hash:
            raise ValueError("robinhood_chain_buy_swap_plan_hash_mismatch")
        if row.swap_plan_expires_at is None or utc_now() >= _as_utc(row.swap_plan_expires_at):
            raise ValueError("robinhood_chain_buy_swap_plan_expired")
        if row.status == "swap_send_claimed" and row.swap_send_claim_id == claim:
            return {"ok": True, "idempotent": True, "execution": serialize_buy_execution(row), "unsigned_transaction_plan": self._swap_plan(row)}
        if row.status != "swap_prepared" or row.swap_send_claim_id:
            raise ValueError("robinhood_chain_buy_swap_not_claimable")
        row.swap_send_claim_id = claim; row.swap_send_claimed_at = _utc_naive(utc_now()); row.status = "swap_send_claimed"; row.updated_at = _utc_naive(utc_now())
        db.add(row); db.commit(); db.refresh(row)
        return {"ok": True, "idempotent": False, "execution": serialize_buy_execution(row), "unsigned_transaction_plan": self._swap_plan(row), "send_gate": gate}

    def record_swap_submission(self, db: Session, *, execution_id: str, tx_hash: str, wallet_address: str, claim_id: str, confirm_record: bool) -> Dict[str, Any]:
        if confirm_record is not True:
            raise ValueError("confirm_buy_swap_submission_record_required")
        row = self._get(db, execution_id)
        validate_execution_saved_wallet(row.wallet_address, wallet_address)
        claim = validate_claim_id(claim_id); tx = validate_transaction_hash(tx_hash)
        if row.swap_tx_hash == tx and row.status in {"swap_pending", "confirmed"}:
            return {"ok": True, "idempotent": True, "execution": serialize_buy_execution(row)}
        if row.status != "swap_send_claimed" or row.swap_send_claim_id != claim:
            raise ValueError("robinhood_chain_buy_swap_claim_mismatch")
        row.swap_tx_hash = tx; row.swap_submitted_at = _utc_naive(utc_now()); row.swap_submission_attempts = int(row.swap_submission_attempts or 0) + 1; row.status = "swap_pending"; row.updated_at = _utc_naive(utc_now())
        db.add(row); db.commit(); db.refresh(row)
        return {"ok": True, "idempotent": False, "execution": serialize_buy_execution(row)}

    def _decode_swap_reconciliation(self, row: RobinhoodChainBuyExecution, receipt: Dict[str, Any]) -> Dict[str, Any]:
        wallet = row.wallet_address.lower()
        net_input_atomic = 0
        transfer_log_count = 0
        for item in receipt.get("logs") or []:
            if not isinstance(item, dict):
                continue
            try:
                if validate_evm_address(str(item.get("address") or "")).lower() != ROBINHOOD_CHAIN_USDG_CONTRACT:
                    continue
            except ValueError:
                continue
            topics = item.get("topics")
            if not isinstance(topics, list) or len(topics) < 3 or str(topics[0]).lower() != _ERC20_TRANSFER_TOPIC0:
                continue
            source = _topic_address(topics[1]); destination = _topic_address(topics[2])
            try:
                amount = decode_hex_quantity(item.get("data"))
            except ValueError:
                continue
            if source == wallet:
                net_input_atomic += amount; transfer_log_count += 1
            if destination == wallet:
                net_input_atomic -= amount; transfer_log_count += 1
        if net_input_atomic <= 0 or net_input_atomic > ROBINHOOD_CHAIN_BUY_MAXIMUM_USDG_ATOMIC:
            raise ValueError("robinhood_chain_buy_usdg_spend_not_found_or_exceeds_cap")
        gas_used = decode_hex_quantity(receipt.get("gasUsed"))
        gas_price = decode_hex_quantity(receipt.get("effectiveGasPrice"))
        swap_fee_wei = gas_used * gas_price
        approval_fee_wei = int(row.approval_gas_used or "0") * int(row.approval_effective_gas_price_wei or "0")
        input_amount = Decimal(net_input_atomic) / (Decimal(10) ** ROBINHOOD_CHAIN_USDG_DECIMALS)
        output_amount = ROBINHOOD_CHAIN_BUY_EXACT_OUTPUT_ETH
        return {
            "version": ROBINHOOD_CHAIN_BUY_TRANCHE,
            "reconciled": True,
            "reconciled_at": utc_now().isoformat(),
            "input_asset": "USDG",
            "input_amount_atomic": str(net_input_atomic),
            "input_amount": _decimal_text(input_amount),
            "output_asset": "ETH",
            "output_amount_atomic": str(ROBINHOOD_CHAIN_BUY_EXACT_OUTPUT_WEI),
            "output_amount": _decimal_text(output_amount),
            "average_fill_price": _decimal_text(input_amount / output_amount),
            "maximum_fill_price": _decimal_text(ROBINHOOD_CHAIN_BUY_MAXIMUM_USDG / output_amount),
            "fee_asset": "ETH",
            "swap_network_fee_wei": str(swap_fee_wei),
            "swap_network_fee": _decimal_text(Decimal(swap_fee_wei) / _WEI_PER_ETH),
            "approval_network_fee_wei": str(approval_fee_wei),
            "approval_network_fee": _decimal_text(Decimal(approval_fee_wei) / _WEI_PER_ETH),
            "total_network_fee_wei": str(swap_fee_wei + approval_fee_wei),
            "total_network_fee": _decimal_text(Decimal(swap_fee_wei + approval_fee_wei) / _WEI_PER_ETH),
            "usdg_transfer_log_count": transfer_log_count,
            "approval_tx_hash": row.approval_tx_hash,
            "swap_tx_hash": row.swap_tx_hash,
        }

    async def refresh_swap(self, db: Session, *, execution_id: str) -> Dict[str, Any]:
        row = self._get(db, execution_id)
        if row.status == "confirmed":
            return {"ok": True, "idempotent": True, "execution": serialize_buy_execution(row)}
        if row.status != "swap_pending" or not row.swap_tx_hash:
            raise ValueError("robinhood_chain_buy_swap_not_pending")
        chain = await self.rpc_client.verify_expected_chain(force_refresh=True)
        if chain.get("ok") is not True or chain.get("chain_id_matches") is not True:
            raise ValueError("robinhood_chain_buy_chain_mismatch")
        tx = await self._verified_transaction(row, stage="swap")
        receipt_record = await self.rpc_client.rpc_read("eth_getTransactionReceipt", [row.swap_tx_hash], cache_namespace=None, force_refresh=True)
        if receipt_record.get("ok") is not True:
            raise ValueError("robinhood_chain_buy_swap_receipt_read_failed")
        receipt = receipt_record.get("result")
        row.swap_last_receipt_check_at = _utc_naive(utc_now())
        if tx is None or receipt is None:
            db.add(row); db.commit(); db.refresh(row)
            return {"ok": True, "pending": True, "execution": serialize_buy_execution(row)}
        status = decode_hex_quantity(receipt.get("status"))
        row.swap_receipt_status = status
        row.swap_block_number = decode_hex_quantity(receipt.get("blockNumber")) if receipt.get("blockNumber") else None
        row.swap_gas_used = str(decode_hex_quantity(receipt.get("gasUsed"))) if receipt.get("gasUsed") else None
        row.swap_effective_gas_price_wei = str(decode_hex_quantity(receipt.get("effectiveGasPrice"))) if receipt.get("effectiveGasPrice") else None
        if status == 0:
            row.status = "swap_reverted"; row.swap_reverted_at = _utc_naive(utc_now()); row.error_code = "swap_reverted"
        elif status == 1:
            reconciliation = self._decode_swap_reconciliation(row, receipt)
            route = dict(row.route or {}); route["execution_reconciliation"] = reconciliation; row.route = route
            row.status = "confirmed"; row.swap_confirmed_at = _utc_naive(utc_now()); row.error_code = None; row.error_message = None
        else:
            raise ValueError("robinhood_chain_buy_invalid_swap_receipt_status")
        row.updated_at = _utc_naive(utc_now())
        db.add(row); db.commit(); db.refresh(row)
        return {"ok": True, "pending": False, "execution": serialize_buy_execution(row)}


_BUY_EXECUTION_SERVICE: Optional[RobinhoodChainBuyExecutionService] = None


def get_robinhood_chain_buy_execution_service() -> RobinhoodChainBuyExecutionService:
    global _BUY_EXECUTION_SERVICE
    if _BUY_EXECUTION_SERVICE is None:
        _BUY_EXECUTION_SERVICE = RobinhoodChainBuyExecutionService()
    return _BUY_EXECUTION_SERVICE
