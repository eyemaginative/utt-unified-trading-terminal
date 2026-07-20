from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from ..config import settings
from ..models import RobinhoodChainSwapExecution
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
    ROBINHOOD_CHAIN_DEFAULT_SLIPPAGE_BPS,
    ROBINHOOD_CHAIN_MAX_SLIPPAGE_BPS,
    ROBINHOOD_CHAIN_MIN_SLIPPAGE_BPS,
    RobinhoodChainTransactionPlanningService,
    get_robinhood_chain_transaction_planning_service,
)


ROBINHOOD_CHAIN_SWAP_TRANCHE = "RH-CHAIN.10D.2-R5B"
ROBINHOOD_CHAIN_SWAP_FROM_ASSET = "USDG"
ROBINHOOD_CHAIN_SWAP_TO_ASSET = "ETH"
ROBINHOOD_CHAIN_SWAP_AMOUNT_MODE = "exact_input"
ROBINHOOD_CHAIN_SWAP_DISPLAY_MODE = "exact_spend"
ROBINHOOD_CHAIN_SWAP_SYMBOL = "ETH-USDG"
ROBINHOOD_CHAIN_SWAP_SIDE = "buy"
ROBINHOOD_CHAIN_SWAP_USDG_CONTRACT = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
ROBINHOOD_CHAIN_SWAP_USDG_DECIMALS = 6
ROBINHOOD_CHAIN_SWAP_MAX_USDG = Decimal("5")
ROBINHOOD_CHAIN_SWAP_DEFAULT_USDG = Decimal("2")
ROBINHOOD_CHAIN_SWAP_APPROVAL_GAS_LIMIT = 100_000
ROBINHOOD_CHAIN_SWAP_SUBMISSION_FAILURE_REASONS = frozenset({"wallet_rejected", "wallet_request_failed"})
ROBINHOOD_CHAIN_SWAP_TERMINAL_STATUSES = frozenset({
    "confirmed",
    "approval_reverted",
    "swap_reverted",
    "approval_wallet_rejected",
    "approval_submission_failed",
    "swap_wallet_rejected",
    "swap_submission_failed",
    "verification_failed",
})
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


def _display_to_atomic(value: Any, decimals: int) -> tuple[str, str]:
    try:
        amount = Decimal(str(value or "").strip())
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError("invalid_robinhood_chain_swap_input_amount") from exc
    if not amount.is_finite() or amount <= 0:
        raise ValueError("invalid_robinhood_chain_swap_input_amount")
    places = int(decimals)
    if max(0, -amount.as_tuple().exponent) > places:
        raise ValueError("invalid_robinhood_chain_swap_input_amount")
    atomic = int(amount * (Decimal(10) ** places))
    if atomic <= 0:
        raise ValueError("invalid_robinhood_chain_swap_input_amount")
    return str(atomic), _decimal_text(Decimal(atomic) / (Decimal(10) ** places))


def _hash_payload(payload: Dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _topic_address(value: Any) -> Optional[str]:
    raw = str(value or "").strip().lower()
    if not re.fullmatch(r"0x[0-9a-f]{64}", raw):
        return None
    return "0x" + raw[-40:]


def _route_copy(row: RobinhoodChainSwapExecution) -> Dict[str, Any]:
    return dict(row.route) if isinstance(row.route, dict) else {}


def _execution_lifecycle(row: RobinhoodChainSwapExecution) -> Dict[str, Any]:
    route = _route_copy(row)
    value = route.get("execution_lifecycle")
    return dict(value) if isinstance(value, dict) else {}


def _stage_lifecycle(row: RobinhoodChainSwapExecution, stage: str) -> Dict[str, Any]:
    lifecycle = _execution_lifecycle(row)
    value = lifecycle.get(str(stage))
    return dict(value) if isinstance(value, dict) else {}


def _set_stage_lifecycle(row: RobinhoodChainSwapExecution, stage: str, values: Dict[str, Any]) -> None:
    route = _route_copy(row)
    lifecycle = route.get("execution_lifecycle")
    lifecycle = dict(lifecycle) if isinstance(lifecycle, dict) else {}
    current = lifecycle.get(str(stage))
    current = dict(current) if isinstance(current, dict) else {}
    current.update(values)
    lifecycle[str(stage)] = current
    route["execution_lifecycle"] = lifecycle
    row.route = route


def _reconciliation(row: RobinhoodChainSwapExecution) -> Dict[str, Any]:
    route = _route_copy(row)
    value = route.get("execution_reconciliation")
    return dict(value) if isinstance(value, dict) else {}


def _swap_gate() -> Dict[str, Any]:
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
        "tranche": ROBINHOOD_CHAIN_SWAP_TRANCHE,
        "chain_id": EXPECTED_CHAIN_ID,
        "from_asset": ROBINHOOD_CHAIN_SWAP_FROM_ASSET,
        "to_asset": ROBINHOOD_CHAIN_SWAP_TO_ASSET,
        "amount_mode": ROBINHOOD_CHAIN_SWAP_AMOUNT_MODE,
        "display_mode": ROBINHOOD_CHAIN_SWAP_DISPLAY_MODE,
        "default_input_amount": _decimal_text(ROBINHOOD_CHAIN_SWAP_DEFAULT_USDG),
        "maximum_input_amount": _decimal_text(ROBINHOOD_CHAIN_SWAP_MAX_USDG),
        "finite_approval_only": True,
        "unlimited_approval_enabled": False,
        "dedicated_execution_enabled": dedicated,
        "armed": armed,
        "dry_run": dry_run,
        "chain_ready": chain_ready,
        "send_enabled": send_enabled,
        "missing_requirements": missing,
        "wallet_connection_requested": False,
        "signing_enabled": False,
        "broadcast_enabled": False,
        "execution_enabled": send_enabled,
        "automatic_second_transaction": False,
        "backend_private_key": False,
        "backend_transaction_sender": False,
        "generic_live_venues_required": False,
        "ledger_mutation_enabled": False,
        "fifo_mutation_enabled": False,
        "basis_mutation_enabled": False,
        "automatic_retry": False,
        "review_only": not send_enabled,
        "will_mutate": False,
    }


def _validate_row(row: RobinhoodChainSwapExecution) -> None:
    if int(row.chain_id or 0) != EXPECTED_CHAIN_ID:
        raise ValueError("robinhood_chain_swap_chain_mismatch")
    if str(row.from_asset or "").upper() != ROBINHOOD_CHAIN_SWAP_FROM_ASSET:
        raise ValueError("robinhood_chain_swap_from_asset_mismatch")
    if str(row.to_asset or "").upper() != ROBINHOOD_CHAIN_SWAP_TO_ASSET:
        raise ValueError("robinhood_chain_swap_to_asset_mismatch")
    if str(row.amount_mode or "").lower() != ROBINHOOD_CHAIN_SWAP_AMOUNT_MODE:
        raise ValueError("robinhood_chain_swap_amount_mode_mismatch")
    if validate_evm_address(row.from_contract_address).lower() != ROBINHOOD_CHAIN_SWAP_USDG_CONTRACT:
        raise ValueError("robinhood_chain_swap_usdg_identity_mismatch")
    if int(row.from_decimals or -1) != ROBINHOOD_CHAIN_SWAP_USDG_DECIMALS:
        raise ValueError("robinhood_chain_swap_usdg_decimals_mismatch")
    if row.from_native:
        raise ValueError("robinhood_chain_swap_input_must_be_erc20")
    if not row.to_native:
        raise ValueError("robinhood_chain_swap_output_must_be_native")
    if row.allowance_spender.lower() not in ROBINHOOD_CHAIN_ALLOWANCE_HOLDER_ALLOWLIST:
        raise ValueError("robinhood_chain_swap_spender_not_allowlisted")
    if row.swap_transaction_to.lower() not in ROBINHOOD_CHAIN_ALLOWANCE_HOLDER_ALLOWLIST:
        raise ValueError("robinhood_chain_swap_destination_not_allowlisted")
    if str(row.swap_transaction_value_wei or "") != "0":
        raise ValueError("robinhood_chain_swap_transaction_value_mismatch")
    if str(row.approval_amount_atomic or "") != str(row.exact_input_amount_atomic or ""):
        raise ValueError("robinhood_chain_swap_approval_amount_mismatch")


def serialize_swap_execution(row: RobinhoodChainSwapExecution) -> Dict[str, Any]:
    approval_lifecycle = _stage_lifecycle(row, "approval")
    swap_lifecycle = _stage_lifecycle(row, "swap")
    reconciliation = _reconciliation(row)
    confirmed_reconciled = str(row.status or "").lower() == "confirmed" and reconciliation.get("reconciled") is True
    return {
        "id": str(row.id),
        "venue": "robinhood_chain",
        "tranche": ROBINHOOD_CHAIN_SWAP_TRANCHE,
        "chain_id": int(row.chain_id),
        "wallet_address": row.wallet_address,
        "provider": row.provider,
        "symbol": row.symbol,
        "side": row.side,
        "from_asset": row.from_asset,
        "from_contract_address": row.from_contract_address,
        "from_decimals": int(row.from_decimals),
        "from_native": bool(row.from_native),
        "to_asset": row.to_asset,
        "to_contract_address": row.to_contract_address,
        "to_decimals": int(row.to_decimals),
        "to_native": bool(row.to_native),
        "amount_mode": row.amount_mode,
        "display_mode": ROBINHOOD_CHAIN_SWAP_DISPLAY_MODE,
        "exact_input_amount": row.exact_input_amount,
        "exact_input_amount_atomic": row.exact_input_amount_atomic,
        "expected_output_amount": row.expected_output_amount,
        "expected_output_amount_atomic": row.expected_output_amount_atomic,
        "minimum_output_amount": row.minimum_output_amount,
        "minimum_output_amount_atomic": row.minimum_output_amount_atomic,
        "slippage_bps": int(row.slippage_bps),
        "status": row.status,
        "approval_status": row.approval_status,
        "swap_status": row.swap_status,
        "allowance": {
            "read_method": row.allowance_read_method,
            "token_address": row.allowance_token_address,
            "spender": row.allowance_spender,
            "spender_allowlisted": True,
            "current_atomic": row.allowance_current_atomic,
            "required_atomic": row.allowance_required_atomic,
            "shortfall_atomic": row.allowance_shortfall_atomic,
            "approval_required": bool(row.approval_required),
        },
        "approval": {
            "amount": row.approval_amount,
            "amount_atomic": row.approval_amount_atomic,
            "finite_approval": True,
            "unlimited_approval": False,
            "plan_hash": row.approval_plan_hash,
            "transaction_to": row.approval_transaction_to,
            "transaction_value_wei": row.approval_transaction_value_wei,
            "calldata_sha256": row.approval_calldata_sha256,
            "calldata_bytes": int(row.approval_calldata_bytes or 0),
            "gas_limit": row.approval_gas_limit,
            "gas_price_wei": row.approval_gas_price_wei,
            "tx_hash": row.approval_tx_hash,
            "send_claimed": bool(approval_lifecycle.get("send_claim_id")),
            "send_claimed_at": approval_lifecycle.get("send_claimed_at"),
            "submission_attempts": int(approval_lifecycle.get("submission_attempts") or 0),
            "submitted_at": approval_lifecycle.get("submitted_at"),
            "last_receipt_check_at": approval_lifecycle.get("last_receipt_check_at"),
            "confirmed_at": approval_lifecycle.get("confirmed_at"),
            "reverted_at": approval_lifecycle.get("reverted_at"),
            "block_number": approval_lifecycle.get("block_number"),
            "gas_used": approval_lifecycle.get("gas_used"),
            "effective_gas_price_wei": approval_lifecycle.get("effective_gas_price_wei"),
            "receipt_status": approval_lifecycle.get("receipt_status"),
            "allowance_confirmed_atomic": approval_lifecycle.get("allowance_confirmed_atomic"),
            "allowance_confirmed_at": approval_lifecycle.get("allowance_confirmed_at"),
        },
        "swap": {
            "quote_id": row.quote_id,
            "plan_hash": row.swap_plan_hash,
            "plan_fetched_at": _iso(row.plan_fetched_at),
            "plan_expires_at": _iso(row.plan_expires_at),
            "transaction_to": row.swap_transaction_to,
            "transaction_value_wei": row.swap_transaction_value_wei,
            "calldata_sha256": row.swap_calldata_sha256,
            "calldata_bytes": int(row.swap_calldata_bytes or 0),
            "gas_limit": row.swap_gas_limit,
            "gas_price_wei": row.swap_gas_price_wei,
            "route": dict(row.route) if isinstance(row.route, dict) else {},
            "tx_hash": row.swap_tx_hash,
            "send_claimed": bool(swap_lifecycle.get("send_claim_id")),
            "send_claimed_at": swap_lifecycle.get("send_claimed_at"),
            "submission_attempts": int(swap_lifecycle.get("submission_attempts") or 0),
            "submitted_at": swap_lifecycle.get("submitted_at"),
            "last_receipt_check_at": swap_lifecycle.get("last_receipt_check_at"),
            "confirmed_at": swap_lifecycle.get("confirmed_at"),
            "reverted_at": swap_lifecycle.get("reverted_at"),
            "block_number": swap_lifecycle.get("block_number"),
            "gas_used": swap_lifecycle.get("gas_used"),
            "effective_gas_price_wei": swap_lifecycle.get("effective_gas_price_wei"),
            "receipt_status": swap_lifecycle.get("receipt_status"),
        },
        "actual_input_asset": reconciliation.get("input_asset") if confirmed_reconciled else None,
        "actual_input_amount": reconciliation.get("input_amount") if confirmed_reconciled else None,
        "actual_input_amount_atomic": reconciliation.get("input_amount_atomic") if confirmed_reconciled else None,
        "actual_output_asset": reconciliation.get("output_asset") if confirmed_reconciled else None,
        "actual_output_amount": reconciliation.get("output_amount") if confirmed_reconciled else None,
        "actual_output_amount_atomic": reconciliation.get("output_amount_atomic") if confirmed_reconciled else None,
        "actual_average_fill_price": reconciliation.get("average_fill_price") if confirmed_reconciled else None,
        "actual_network_fee": reconciliation.get("swap_network_fee") if confirmed_reconciled else None,
        "actual_network_fee_wei": reconciliation.get("swap_network_fee_wei") if confirmed_reconciled else None,
        "actual_approval_network_fee": reconciliation.get("approval_network_fee") if confirmed_reconciled else None,
        "actual_approval_network_fee_wei": reconciliation.get("approval_network_fee_wei") if confirmed_reconciled else None,
        "actual_total_network_fee": reconciliation.get("total_network_fee") if confirmed_reconciled else None,
        "actual_total_network_fee_wei": reconciliation.get("total_network_fee_wei") if confirmed_reconciled else None,
        "reconciliation": reconciliation if confirmed_reconciled else None,
        "automatic_second_transaction": False,
        "signing_enabled": False,
        "broadcast_enabled": False,
        "review_only": not _swap_gate()["send_enabled"],
        "will_mutate": False,
        "created_at": _iso(row.created_at),
        "updated_at": _iso(row.updated_at),
    }


class RobinhoodChainSwapExecutionService:
    """Generalized exact-spend browser-wallet lifecycle for USDG -> native ETH.

    The backend validates and persists plans, claims, hashes, receipts, and
    reconciliation. It never signs or broadcasts; MetaMask remains the only
    transaction sender and each stage requires a separate explicit claim.
    """

    def __init__(
        self,
        *,
        planning_service: Optional[RobinhoodChainTransactionPlanningService] = None,
        rpc_client=None,
    ) -> None:
        self.planning_service = planning_service or get_robinhood_chain_transaction_planning_service()
        self.rpc_client = rpc_client or get_robinhood_chain_client()

    def status(self) -> Dict[str, Any]:
        return {"ok": True, **_swap_gate()}

    def _get(self, db: Session, execution_id: str) -> RobinhoodChainSwapExecution:
        row = db.get(RobinhoodChainSwapExecution, str(execution_id or "").strip())
        if row is None:
            raise KeyError("robinhood_chain_swap_execution_not_found")
        _validate_row(row)
        return row

    def get(self, db: Session, execution_id: str) -> Dict[str, Any]:
        row = self._get(db, execution_id)
        return {
            "ok": True,
            "execution": serialize_swap_execution(row),
            "approval_transaction_plan": self._approval_plan(row) if row.approval_required and not row.approval_tx_hash else None,
            "unsigned_transaction_plan": self._swap_plan(row),
            "send_gate": _swap_gate(),
            "review_gate": _swap_gate(),
        }

    async def prepare(
        self,
        db: Session,
        *,
        taker_address: str,
        exact_input_amount: str,
        slippage_bps: int,
        eth_token: Dict[str, Any],
        usdg_token: Dict[str, Any],
        confirm_prepare: bool,
    ) -> Dict[str, Any]:
        if confirm_prepare is not True:
            raise ValueError("confirm_robinhood_chain_swap_prepare_required")
        wallet = validate_evm_address(taker_address).lower()
        input_atomic, input_display = _display_to_atomic(exact_input_amount, ROBINHOOD_CHAIN_SWAP_USDG_DECIMALS)
        if Decimal(input_display) > ROBINHOOD_CHAIN_SWAP_MAX_USDG:
            raise ValueError("robinhood_chain_swap_input_exceeds_cap")
        slippage = int(slippage_bps)
        if slippage < ROBINHOOD_CHAIN_MIN_SLIPPAGE_BPS or slippage > ROBINHOOD_CHAIN_MAX_SLIPPAGE_BPS:
            raise ValueError("invalid_slippage_bps")
        if validate_evm_address(str(usdg_token.get("contract_address") or "")).lower() != ROBINHOOD_CHAIN_SWAP_USDG_CONTRACT:
            raise ValueError("robinhood_chain_swap_usdg_identity_mismatch")
        if int(usdg_token.get("decimals") or -1) != ROBINHOOD_CHAIN_SWAP_USDG_DECIMALS:
            raise ValueError("robinhood_chain_swap_usdg_decimals_mismatch")
        if bool(usdg_token.get("native")):
            raise ValueError("robinhood_chain_swap_input_must_be_erc20")
        if str(eth_token.get("symbol") or "").upper() != "ETH" or bool(eth_token.get("native")) is not True:
            raise ValueError("robinhood_chain_swap_eth_identity_mismatch")

        now = utc_now()
        existing = (
            db.query(RobinhoodChainSwapExecution)
            .filter(
                RobinhoodChainSwapExecution.wallet_address == wallet,
                RobinhoodChainSwapExecution.from_asset == ROBINHOOD_CHAIN_SWAP_FROM_ASSET,
                RobinhoodChainSwapExecution.to_asset == ROBINHOOD_CHAIN_SWAP_TO_ASSET,
                RobinhoodChainSwapExecution.exact_input_amount_atomic == input_atomic,
                RobinhoodChainSwapExecution.slippage_bps == slippage,
                RobinhoodChainSwapExecution.status.in_([
                    "approval_prepared", "allowance_sufficient", "approval_send_claimed",
                    "approval_pending", "approval_confirmed", "swap_prepared",
                    "swap_send_claimed", "swap_pending", "confirmed",
                ]),
            )
            .order_by(RobinhoodChainSwapExecution.created_at.desc())
            .first()
        )
        if existing is not None:
            _validate_row(existing)
            active_after_claim = {
                "approval_send_claimed", "approval_pending", "approval_confirmed",
                "swap_prepared", "swap_send_claimed", "swap_pending", "confirmed",
            }
            if str(existing.status or "") in active_after_claim or _as_utc(existing.plan_expires_at) > now:
                return {
                    "ok": True,
                    "idempotent": True,
                    "approval_required": bool(existing.approval_required),
                    "execution": serialize_swap_execution(existing),
                    "approval_transaction_plan": self._approval_plan(existing) if existing.approval_required and not existing.approval_tx_hash else None,
                    "unsigned_transaction_plan": self._swap_plan(existing),
                    "send_gate": _swap_gate(),
                    "review_gate": _swap_gate(),
                }
            existing.status = "expired"
            existing.approval_status = "expired"
            existing.swap_status = "expired"
            existing.updated_at = _utc_naive(now)
            db.commit()

        plan = await self.planning_service.firm_quote_plan(
            symbol=ROBINHOOD_CHAIN_SWAP_SYMBOL,
            side=ROBINHOOD_CHAIN_SWAP_SIDE,
            quantity=None,
            total_quote=input_display,
            exact_output_quantity=None,
            maximum_total_quote=None,
            taker_address=wallet,
            eth_token=eth_token,
            usdg_token=usdg_token,
            slippage_bps=slippage,
        )
        if plan.get("ok") is not True:
            raise ValueError(str(plan.get("error") or "robinhood_chain_swap_firm_plan_failed"))
        if str(plan.get("amount_mode") or "") != ROBINHOOD_CHAIN_SWAP_AMOUNT_MODE:
            raise ValueError("robinhood_chain_swap_plan_not_exact_input")
        if str(plan.get("input_asset") or "") != ROBINHOOD_CHAIN_SWAP_FROM_ASSET:
            raise ValueError("robinhood_chain_swap_plan_input_asset_mismatch")
        if str(plan.get("output_asset") or "") != ROBINHOOD_CHAIN_SWAP_TO_ASSET:
            raise ValueError("robinhood_chain_swap_plan_output_asset_mismatch")
        if str(plan.get("input_amount_atomic") or "") != input_atomic:
            raise ValueError("robinhood_chain_swap_plan_input_amount_mismatch")
        if int(str(plan.get("output_amount_atomic") or "0")) <= 0:
            raise ValueError("robinhood_chain_swap_plan_output_missing")
        if int(str(plan.get("minimum_received_atomic") or "0")) <= 0:
            raise ValueError("robinhood_chain_swap_plan_minimum_missing")
        if int(str(plan.get("minimum_received_atomic") or "0")) > int(str(plan.get("output_amount_atomic") or "0")):
            raise ValueError("robinhood_chain_swap_plan_minimum_invalid")

        allowance = plan.get("allowance") if isinstance(plan.get("allowance"), dict) else {}
        if allowance.get("applicable") is not True or str(allowance.get("read_method") or "") != "eth_call":
            raise ValueError("robinhood_chain_swap_allowance_not_verified")
        spender = validate_evm_address(str(allowance.get("spender") or "")).lower()
        if allowance.get("spender_allowlisted") is not True or spender not in ROBINHOOD_CHAIN_ALLOWANCE_HOLDER_ALLOWLIST:
            raise ValueError("robinhood_chain_swap_spender_not_allowlisted")
        required_atomic = str(allowance.get("required_atomic") or "")
        if required_atomic != input_atomic:
            raise ValueError("robinhood_chain_swap_required_allowance_mismatch")
        current_atomic = str(allowance.get("current_atomic") or "0")
        shortfall_atomic = str(max(0, int(input_atomic) - int(current_atomic)))
        approval_required = int(shortfall_atomic) > 0

        swap_plan = plan.get("unsigned_transaction_plan") if isinstance(plan.get("unsigned_transaction_plan"), dict) else {}
        destination = validate_evm_address(str(swap_plan.get("to") or "")).lower()
        if swap_plan.get("destination_allowlisted") is not True or destination not in ROBINHOOD_CHAIN_ALLOWANCE_HOLDER_ALLOWLIST:
            raise ValueError("robinhood_chain_swap_destination_not_allowlisted")
        if str(swap_plan.get("value_wei") or "") != "0":
            raise ValueError("robinhood_chain_swap_transaction_value_mismatch")
        swap_calldata = str(swap_plan.get("calldata") or "")
        if not swap_calldata.startswith("0x") or len(swap_calldata) <= 2 or len(swap_calldata[2:]) % 2:
            raise ValueError("robinhood_chain_swap_calldata_invalid")
        swap_hash = hashlib.sha256(bytes.fromhex(swap_calldata[2:])).hexdigest()
        if swap_hash != str(swap_plan.get("calldata_sha256") or ""):
            raise ValueError("robinhood_chain_swap_calldata_hash_mismatch")
        gas_price = str(swap_plan.get("gas_price_wei") or "")
        if not gas_price.isdigit() or int(gas_price) <= 0:
            raise ValueError("robinhood_chain_swap_gas_price_missing")

        approval_calldata = encode_erc20_approve(spender, input_atomic)
        approval_hash = hashlib.sha256(bytes.fromhex(approval_calldata[2:])).hexdigest()
        approval_plan_hash = _hash_payload({
            "chain_id": EXPECTED_CHAIN_ID,
            "wallet": wallet,
            "token": ROBINHOOD_CHAIN_SWAP_USDG_CONTRACT,
            "spender": spender,
            "amount_atomic": input_atomic,
            "calldata_sha256": approval_hash,
            "quote_id": str(plan.get("quote_id") or ""),
        })
        swap_plan_hash = _hash_payload({
            "chain_id": EXPECTED_CHAIN_ID,
            "wallet": wallet,
            "quote_id": str(plan.get("quote_id") or ""),
            "input_atomic": input_atomic,
            "output_atomic": str(plan.get("output_amount_atomic") or ""),
            "minimum_atomic": str(plan.get("minimum_received_atomic") or ""),
            "destination": destination,
            "calldata_sha256": swap_hash,
        })

        fetched_at = datetime.fromisoformat(str(plan.get("fetched_at") or "").replace("Z", "+00:00"))
        expires_at = datetime.fromisoformat(str(plan.get("plan_expires_at") or "").replace("Z", "+00:00"))
        if expires_at <= now:
            raise ValueError("robinhood_chain_swap_plan_expired")

        row = RobinhoodChainSwapExecution(
            chain_id=EXPECTED_CHAIN_ID,
            wallet_address=wallet,
            provider="0x",
            symbol=ROBINHOOD_CHAIN_SWAP_SYMBOL,
            side=ROBINHOOD_CHAIN_SWAP_SIDE,
            from_asset=ROBINHOOD_CHAIN_SWAP_FROM_ASSET,
            from_contract_address=ROBINHOOD_CHAIN_SWAP_USDG_CONTRACT,
            from_decimals=ROBINHOOD_CHAIN_SWAP_USDG_DECIMALS,
            from_native=False,
            to_asset=ROBINHOOD_CHAIN_SWAP_TO_ASSET,
            to_contract_address=validate_evm_address(str(eth_token.get("contract_address") or "")),
            to_decimals=int(eth_token.get("decimals") or 18),
            to_native=True,
            amount_mode=ROBINHOOD_CHAIN_SWAP_AMOUNT_MODE,
            exact_input_amount=input_display,
            exact_input_amount_atomic=input_atomic,
            expected_output_amount=str(plan.get("output_amount")),
            expected_output_amount_atomic=str(plan.get("output_amount_atomic")),
            minimum_output_amount=str(plan.get("minimum_received")),
            minimum_output_amount_atomic=str(plan.get("minimum_received_atomic")),
            slippage_bps=slippage,
            quote_id=str(plan.get("quote_id")),
            plan_fetched_at=_utc_naive(fetched_at),
            plan_expires_at=_utc_naive(expires_at),
            allowance_read_method="eth_call",
            allowance_token_address=ROBINHOOD_CHAIN_SWAP_USDG_CONTRACT,
            allowance_spender=spender,
            allowance_current_atomic=current_atomic,
            allowance_required_atomic=input_atomic,
            allowance_shortfall_atomic=shortfall_atomic,
            approval_required=approval_required,
            approval_amount=input_display,
            approval_amount_atomic=input_atomic,
            approval_plan_hash=approval_plan_hash,
            approval_transaction_to=ROBINHOOD_CHAIN_SWAP_USDG_CONTRACT,
            approval_transaction_value_wei="0",
            approval_calldata_sha256=approval_hash,
            approval_calldata_bytes=len(bytes.fromhex(approval_calldata[2:])),
            approval_gas_limit=str(ROBINHOOD_CHAIN_SWAP_APPROVAL_GAS_LIMIT),
            approval_gas_price_wei=gas_price,
            approval_status="prepared" if approval_required else "not_required",
            swap_plan_hash=swap_plan_hash,
            swap_transaction_to=destination,
            swap_transaction_value_wei="0",
            swap_calldata_sha256=swap_hash,
            swap_calldata_bytes=int(swap_plan.get("calldata_bytes") or len(bytes.fromhex(swap_calldata[2:]))),
            swap_gas_limit=str(swap_plan.get("gas_limit") or ""),
            swap_gas_price_wei=gas_price,
            swap_status="review_only",
            route=dict(plan.get("route") or {}),
            status="approval_prepared" if approval_required else "allowance_sufficient",
            created_at=_utc_naive(now),
            updated_at=_utc_naive(now),
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        _validate_row(row)
        return {
            "ok": True,
            "idempotent": False,
            "approval_required": approval_required,
            "execution": serialize_swap_execution(row),
            "approval_transaction_plan": self._approval_plan(row) if approval_required else None,
            "source_firm_plan": plan,
            "send_gate": _swap_gate(),
            "review_gate": _swap_gate(),
        }

    def _approval_plan(self, row: RobinhoodChainSwapExecution) -> Dict[str, Any]:
        _validate_row(row)
        calldata = encode_erc20_approve(row.allowance_spender, row.approval_amount_atomic)
        digest = hashlib.sha256(bytes.fromhex(calldata[2:])).hexdigest()
        if digest != row.approval_calldata_sha256:
            raise ValueError("robinhood_chain_swap_approval_calldata_hash_mismatch")
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
            "calldata_bytes": int(row.approval_calldata_bytes),
            "token": row.allowance_token_address,
            "spender": row.allowance_spender,
            "approval_amount": row.approval_amount,
            "approval_amount_atomic": row.approval_amount_atomic,
            "finite_approval": True,
            "unlimited_approval": False,
            "wallet_connection_requested": False,
            "signing_requested": False,
            "broadcast_requested": False,
        }

    def _swap_plan(self, row: RobinhoodChainSwapExecution) -> Optional[Dict[str, Any]]:
        if not row.swap_plan_hash:
            return None
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
            "calldata_bytes": int(row.swap_calldata_bytes or 0),
            "exact_input_usdg": row.exact_input_amount,
            "expected_output_eth": row.expected_output_amount,
            "minimum_output_eth": row.minimum_output_amount,
            "signing_requested": False,
            "broadcast_requested": False,
        }

    def claim_approval_send(
        self, db: Session, *, execution_id: str, wallet_address: str,
        plan_hash: str, claim_id: str, confirm_send_claim: bool,
    ) -> Dict[str, Any]:
        if confirm_send_claim is not True:
            raise ValueError("confirm_robinhood_chain_swap_approval_send_claim_required")
        gate = _swap_gate()
        if gate.get("send_enabled") is not True:
            raise ValueError("robinhood_chain_swap_send_gate_blocked")
        row = self._get(db, execution_id)
        validate_execution_saved_wallet(row.wallet_address, wallet_address)
        claim = validate_claim_id(claim_id)
        plan = str(plan_hash or "").strip().lower()
        if not _SHA256_RE.fullmatch(plan) or plan != row.approval_plan_hash:
            raise ValueError("robinhood_chain_swap_approval_plan_hash_mismatch")
        if _as_utc(row.plan_expires_at) <= utc_now():
            raise ValueError("robinhood_chain_swap_approval_plan_expired")
        lifecycle = _stage_lifecycle(row, "approval")
        if row.status == "approval_send_claimed" and lifecycle.get("send_claim_id") == claim:
            return {"ok": True, "idempotent": True, "execution": serialize_swap_execution(row), "approval_transaction_plan": self._approval_plan(row), "send_gate": gate}
        if row.status != "approval_prepared" or lifecycle.get("send_claim_id") or row.approval_tx_hash:
            raise ValueError("robinhood_chain_swap_approval_not_claimable")
        now = utc_now()
        _set_stage_lifecycle(row, "approval", {"send_claim_id": claim, "send_claimed_at": now.isoformat()})
        row.status = "approval_send_claimed"
        row.approval_status = "send_claimed"
        row.updated_at = _utc_naive(now)
        db.add(row); db.commit(); db.refresh(row)
        return {"ok": True, "idempotent": False, "execution": serialize_swap_execution(row), "approval_transaction_plan": self._approval_plan(row), "send_gate": gate}

    def record_approval_submission(
        self, db: Session, *, execution_id: str, tx_hash: str,
        wallet_address: str, claim_id: str, confirm_record: bool,
    ) -> Dict[str, Any]:
        if confirm_record is not True:
            raise ValueError("confirm_robinhood_chain_swap_approval_submission_record_required")
        row = self._get(db, execution_id)
        validate_execution_saved_wallet(row.wallet_address, wallet_address)
        claim = validate_claim_id(claim_id)
        tx = validate_transaction_hash(tx_hash)
        lifecycle = _stage_lifecycle(row, "approval")
        if row.approval_tx_hash == tx and row.status in {"approval_pending", "approval_confirmed", "swap_prepared", "swap_send_claimed", "swap_pending", "confirmed"}:
            return {"ok": True, "idempotent": True, "execution": serialize_swap_execution(row)}
        if row.status != "approval_send_claimed" or lifecycle.get("send_claim_id") != claim:
            raise ValueError("robinhood_chain_swap_approval_claim_mismatch")
        attempts = int(lifecycle.get("submission_attempts") or 0) + 1
        now = utc_now()
        row.approval_tx_hash = tx
        row.status = "approval_pending"
        row.approval_status = "pending"
        _set_stage_lifecycle(row, "approval", {"submission_attempts": attempts, "submitted_at": now.isoformat()})
        row.updated_at = _utc_naive(now)
        db.add(row); db.commit(); db.refresh(row)
        return {"ok": True, "idempotent": False, "execution": serialize_swap_execution(row)}

    def record_submission_failure(
        self, db: Session, *, execution_id: str, stage: str, wallet_address: str,
        claim_id: str, reason: str, message: Optional[str], confirm_failure: bool,
    ) -> Dict[str, Any]:
        if confirm_failure is not True:
            raise ValueError("confirm_robinhood_chain_swap_submission_failure_required")
        stage_n = str(stage or "").strip().lower()
        if stage_n not in {"approval", "swap"}:
            raise ValueError("invalid_robinhood_chain_swap_failure_stage")
        reason_n = str(reason or "").strip().lower()
        if reason_n not in ROBINHOOD_CHAIN_SWAP_SUBMISSION_FAILURE_REASONS:
            raise ValueError("invalid_robinhood_chain_swap_failure_reason")
        row = self._get(db, execution_id)
        validate_execution_saved_wallet(row.wallet_address, wallet_address)
        claim = validate_claim_id(claim_id)
        lifecycle = _stage_lifecycle(row, stage_n)
        expected_status = f"{stage_n}_send_claimed"
        tx_hash = row.approval_tx_hash if stage_n == "approval" else row.swap_tx_hash
        if row.status != expected_status or lifecycle.get("send_claim_id") != claim or tx_hash:
            raise ValueError(f"robinhood_chain_swap_{stage_n}_claim_mismatch")
        now = utc_now()
        terminal = f"{stage_n}_wallet_rejected" if reason_n == "wallet_rejected" else f"{stage_n}_submission_failed"
        row.status = terminal
        if stage_n == "approval":
            row.approval_status = terminal
        else:
            row.swap_status = terminal
        _set_stage_lifecycle(row, stage_n, {"submission_failure_at": now.isoformat()})
        row.error_code = reason_n
        row.error_message = str(message or "")[:512] or None
        row.updated_at = _utc_naive(now)
        db.add(row); db.commit(); db.refresh(row)
        return {"ok": True, "execution": serialize_swap_execution(row)}

    async def _verified_transaction(self, row: RobinhoodChainSwapExecution, *, stage: str) -> Optional[Dict[str, Any]]:
        tx_hash = row.approval_tx_hash if stage == "approval" else row.swap_tx_hash
        result = await self.rpc_client.rpc_read("eth_getTransactionByHash", [tx_hash], cache_namespace=None, force_refresh=True)
        if result.get("ok") is not True:
            raise ValueError("robinhood_chain_swap_transaction_read_failed")
        tx = result.get("result")
        if tx is None:
            return None
        expected_to = row.approval_transaction_to if stage == "approval" else row.swap_transaction_to
        expected_value = row.approval_transaction_value_wei if stage == "approval" else row.swap_transaction_value_wei
        expected_hash = row.approval_calldata_sha256 if stage == "approval" else row.swap_calldata_sha256
        if validate_transaction_hash(tx.get("hash")) != tx_hash:
            raise ValueError("robinhood_chain_swap_transaction_hash_mismatch")
        if validate_evm_address(tx.get("from")).lower() != row.wallet_address.lower():
            raise ValueError("robinhood_chain_swap_transaction_sender_mismatch")
        if validate_evm_address(tx.get("to")).lower() != str(expected_to).lower():
            raise ValueError("robinhood_chain_swap_transaction_destination_mismatch")
        if decode_hex_quantity(tx.get("value")) != int(str(expected_value or "0")):
            raise ValueError("robinhood_chain_swap_transaction_value_mismatch")
        calldata = str(tx.get("input") or "").strip()
        if not re.fullmatch(r"0x[0-9a-fA-F]+", calldata) or len(calldata[2:]) % 2:
            raise ValueError("robinhood_chain_swap_transaction_calldata_invalid")
        if hashlib.sha256(bytes.fromhex(calldata[2:])).hexdigest() != expected_hash:
            raise ValueError("robinhood_chain_swap_transaction_calldata_mismatch")
        return tx

    async def refresh_approval(self, db: Session, *, execution_id: str) -> Dict[str, Any]:
        row = self._get(db, execution_id)
        if row.status in {"approval_confirmed", "swap_prepared", "swap_send_claimed", "swap_pending", "confirmed"}:
            return {"ok": True, "idempotent": True, "execution": serialize_swap_execution(row), "send_gate": _swap_gate()}
        if row.status != "approval_pending" or not row.approval_tx_hash:
            raise ValueError("robinhood_chain_swap_approval_not_pending")
        chain = await self.rpc_client.verify_expected_chain(force_refresh=True)
        if chain.get("ok") is not True or chain.get("chain_id_matches") is not True:
            raise ValueError("robinhood_chain_swap_chain_mismatch")
        tx = await self._verified_transaction(row, stage="approval")
        receipt_record = await self.rpc_client.rpc_read("eth_getTransactionReceipt", [row.approval_tx_hash], cache_namespace=None, force_refresh=True)
        if receipt_record.get("ok") is not True:
            raise ValueError("robinhood_chain_swap_approval_receipt_read_failed")
        receipt = receipt_record.get("result")
        now = utc_now()
        _set_stage_lifecycle(row, "approval", {"last_receipt_check_at": now.isoformat()})
        if tx is None or receipt is None:
            db.add(row); db.commit(); db.refresh(row)
            return {"ok": True, "pending": True, "execution": serialize_swap_execution(row), "send_gate": _swap_gate()}
        receipt_status = decode_hex_quantity(receipt.get("status"))
        lifecycle_values = {
            "receipt_status": receipt_status,
            "block_number": decode_hex_quantity(receipt.get("blockNumber")) if receipt.get("blockNumber") else None,
            "gas_used": str(decode_hex_quantity(receipt.get("gasUsed"))) if receipt.get("gasUsed") else None,
            "effective_gas_price_wei": str(decode_hex_quantity(receipt.get("effectiveGasPrice"))) if receipt.get("effectiveGasPrice") else None,
        }
        if receipt_status == 0:
            lifecycle_values["reverted_at"] = now.isoformat()
            row.status = "approval_reverted"
            row.approval_status = "reverted"
            row.error_code = "approval_reverted"
        elif receipt_status == 1:
            allowance = await self.rpc_client.get_erc20_allowance(
                owner_address=row.wallet_address, contract_address=row.allowance_token_address,
                spender_address=row.allowance_spender, decimals=ROBINHOOD_CHAIN_SWAP_USDG_DECIMALS, force_refresh=True,
            )
            if allowance.get("ok") is not True:
                raise ValueError("robinhood_chain_swap_post_approval_allowance_read_failed")
            confirmed = int(str(allowance.get("allowance_atomic") or "0"))
            if confirmed < int(row.exact_input_amount_atomic):
                raise ValueError("robinhood_chain_swap_post_approval_allowance_insufficient")
            lifecycle_values.update({
                "confirmed_at": now.isoformat(),
                "allowance_confirmed_atomic": str(confirmed),
                "allowance_confirmed_at": now.isoformat(),
            })
            row.allowance_current_atomic = str(confirmed)
            row.allowance_shortfall_atomic = "0"
            row.approval_required = False
            row.status = "approval_confirmed"
            row.approval_status = "confirmed"
            row.error_code = None
            row.error_message = None
        else:
            raise ValueError("robinhood_chain_swap_invalid_approval_receipt_status")
        _set_stage_lifecycle(row, "approval", lifecycle_values)
        row.updated_at = _utc_naive(now)
        db.add(row); db.commit(); db.refresh(row)
        return {"ok": True, "pending": False, "execution": serialize_swap_execution(row), "send_gate": _swap_gate()}

    async def prepare_swap(
        self, db: Session, *, execution_id: str, wallet_address: str,
        eth_token: Dict[str, Any], usdg_token: Dict[str, Any], confirm_prepare: bool,
    ) -> Dict[str, Any]:
        if confirm_prepare is not True:
            raise ValueError("confirm_robinhood_chain_swap_fresh_prepare_required")
        row = self._get(db, execution_id)
        validate_execution_saved_wallet(row.wallet_address, wallet_address)
        if row.status not in {"approval_confirmed", "allowance_sufficient", "swap_prepared"}:
            raise ValueError("robinhood_chain_swap_approval_not_confirmed")
        swap_lifecycle = _stage_lifecycle(row, "swap")
        if swap_lifecycle.get("send_claim_id") or row.swap_tx_hash:
            raise ValueError("robinhood_chain_swap_already_claimed_or_submitted")
        allowance = await self.rpc_client.get_erc20_allowance(
            owner_address=row.wallet_address, contract_address=row.allowance_token_address,
            spender_address=row.allowance_spender, decimals=ROBINHOOD_CHAIN_SWAP_USDG_DECIMALS, force_refresh=True,
        )
        if allowance.get("ok") is not True or int(str(allowance.get("allowance_atomic") or "0")) < int(row.exact_input_amount_atomic):
            raise ValueError("robinhood_chain_swap_fresh_allowance_insufficient")
        plan = await self.planning_service.firm_quote_plan(
            symbol=ROBINHOOD_CHAIN_SWAP_SYMBOL, side=ROBINHOOD_CHAIN_SWAP_SIDE, quantity=None,
            total_quote=row.exact_input_amount, exact_output_quantity=None, maximum_total_quote=None,
            taker_address=row.wallet_address, eth_token=eth_token, usdg_token=usdg_token, slippage_bps=int(row.slippage_bps),
        )
        if plan.get("ok") is not True:
            raise ValueError(str(plan.get("error") or "robinhood_chain_swap_fresh_plan_failed"))
        if str(plan.get("amount_mode")) != "exact_input" or str(plan.get("input_asset")) != "USDG" or str(plan.get("output_asset")) != "ETH":
            raise ValueError("robinhood_chain_swap_fresh_plan_identity_mismatch")
        if str(plan.get("input_amount_atomic")) != str(row.exact_input_amount_atomic):
            raise ValueError("robinhood_chain_swap_fresh_plan_input_mismatch")
        output_atomic = int(str(plan.get("output_amount_atomic") or "0"))
        minimum_atomic = int(str(plan.get("minimum_received_atomic") or "0"))
        if output_atomic <= 0 or minimum_atomic <= 0 or minimum_atomic > output_atomic:
            raise ValueError("robinhood_chain_swap_fresh_plan_output_invalid")
        plan_allowance = plan.get("allowance") if isinstance(plan.get("allowance"), dict) else {}
        if str(plan_allowance.get("read_method")) != "eth_call" or int(str(plan_allowance.get("current_atomic") or "0")) < int(row.exact_input_amount_atomic):
            raise ValueError("robinhood_chain_swap_fresh_plan_allowance_insufficient")
        if validate_evm_address(str(plan_allowance.get("spender") or "")).lower() != row.allowance_spender.lower():
            raise ValueError("robinhood_chain_swap_spender_rotated")
        unsigned = plan.get("unsigned_transaction_plan") if isinstance(plan.get("unsigned_transaction_plan"), dict) else {}
        destination = validate_evm_address(str(unsigned.get("to") or "")).lower()
        if unsigned.get("destination_allowlisted") is not True or destination not in ROBINHOOD_CHAIN_ALLOWANCE_HOLDER_ALLOWLIST:
            raise ValueError("robinhood_chain_swap_destination_not_allowlisted")
        if str(unsigned.get("value_wei")) != "0":
            raise ValueError("robinhood_chain_swap_transaction_value_mismatch")
        calldata = str(unsigned.get("calldata") or "")
        if not re.fullmatch(r"0x[0-9a-fA-F]+", calldata) or len(calldata[2:]) % 2:
            raise ValueError("robinhood_chain_swap_calldata_invalid")
        calldata_hash = hashlib.sha256(bytes.fromhex(calldata[2:])).hexdigest()
        if calldata_hash != str(unsigned.get("calldata_sha256") or "").lower():
            raise ValueError("robinhood_chain_swap_calldata_hash_mismatch")
        fetched = datetime.fromisoformat(str(plan.get("fetched_at") or "").replace("Z", "+00:00"))
        expires = datetime.fromisoformat(str(plan.get("plan_expires_at") or "").replace("Z", "+00:00"))
        if _as_utc(expires) <= utc_now():
            raise ValueError("robinhood_chain_swap_fresh_plan_expired")
        plan_hash = _hash_payload({
            "chain_id": EXPECTED_CHAIN_ID, "wallet": row.wallet_address, "quote_id": str(plan.get("quote_id")),
            "input_atomic": str(row.exact_input_amount_atomic), "output_atomic": str(output_atomic),
            "minimum_atomic": str(minimum_atomic), "destination": destination,
            "calldata_sha256": calldata_hash, "plan_expires_at": _as_utc(expires).isoformat(),
        })
        route = _route_copy(row)
        route["fills"] = (plan.get("route") or {}).get("fills") or []
        route["firm_plan"] = {"quote_id": plan.get("quote_id"), "route_sources": plan.get("route_sources") or []}
        row.route = route
        row.quote_id = str(plan.get("quote_id"))
        row.plan_fetched_at = _utc_naive(fetched)
        row.plan_expires_at = _utc_naive(expires)
        row.expected_output_amount = str(plan.get("output_amount"))
        row.expected_output_amount_atomic = str(output_atomic)
        row.minimum_output_amount = str(plan.get("minimum_received"))
        row.minimum_output_amount_atomic = str(minimum_atomic)
        row.allowance_current_atomic = str(plan_allowance.get("current_atomic"))
        row.allowance_shortfall_atomic = "0"
        row.approval_required = False
        row.swap_plan_hash = plan_hash
        row.swap_transaction_to = destination
        row.swap_transaction_value_wei = "0"
        row.swap_calldata_sha256 = calldata_hash
        row.swap_calldata_bytes = int(unsigned.get("calldata_bytes") or len(bytes.fromhex(calldata[2:])))
        row.swap_gas_limit = str(unsigned.get("gas_limit") or "")
        row.swap_gas_price_wei = str(unsigned.get("gas_price_wei") or "")
        row.swap_status = "prepared"
        row.status = "swap_prepared"
        row.error_code = None
        row.error_message = None
        row.updated_at = _utc_naive(utc_now())
        db.add(row); db.commit(); db.refresh(row)
        return {
            "ok": True, "idempotent": False, "execution": serialize_swap_execution(row),
            "unsigned_transaction_plan": {**self._swap_plan(row), "calldata": calldata},
            "source_firm_plan": plan, "send_gate": _swap_gate(),
        }

    async def claim_swap_send(
        self, db: Session, *, execution_id: str, wallet_address: str,
        plan_hash: str, claim_id: str, confirm_send_claim: bool,
    ) -> Dict[str, Any]:
        if confirm_send_claim is not True:
            raise ValueError("confirm_robinhood_chain_swap_send_claim_required")
        gate = _swap_gate()
        if gate.get("send_enabled") is not True:
            raise ValueError("robinhood_chain_swap_send_gate_blocked")
        row = self._get(db, execution_id)
        validate_execution_saved_wallet(row.wallet_address, wallet_address)
        claim = validate_claim_id(claim_id)
        if str(plan_hash or "").strip().lower() != row.swap_plan_hash:
            raise ValueError("robinhood_chain_swap_plan_hash_mismatch")
        if _as_utc(row.plan_expires_at) <= utc_now():
            raise ValueError("robinhood_chain_swap_plan_expired")
        lifecycle = _stage_lifecycle(row, "swap")
        if row.status == "swap_send_claimed" and lifecycle.get("send_claim_id") == claim:
            return {"ok": True, "idempotent": True, "execution": serialize_swap_execution(row), "unsigned_transaction_plan": self._swap_plan(row), "send_gate": gate}
        if row.status != "swap_prepared" or lifecycle.get("send_claim_id") or row.swap_tx_hash:
            raise ValueError("robinhood_chain_swap_not_claimable")
        eth_balance = await self.rpc_client.get_native_balance(row.wallet_address, block_tag="latest", force_refresh=True)
        usdg_balance = await self.rpc_client.get_erc20_balance(row.wallet_address, row.allowance_token_address, ROBINHOOD_CHAIN_SWAP_USDG_DECIMALS, block_tag="latest", force_refresh=True)
        if eth_balance.get("ok") is not True or usdg_balance.get("ok") is not True:
            raise ValueError("robinhood_chain_swap_pre_balance_snapshot_failed")
        now = utc_now()
        _set_stage_lifecycle(row, "swap", {
            "send_claim_id": claim, "send_claimed_at": now.isoformat(),
            "pre_balance_snapshot": {
                "captured_at": now.isoformat(),
                "eth_balance_wei": str(eth_balance.get("balance_wei") or "0"),
                "usdg_balance_atomic": str(usdg_balance.get("balance_atomic") or "0"),
            },
        })
        row.status = "swap_send_claimed"
        row.swap_status = "send_claimed"
        row.updated_at = _utc_naive(now)
        db.add(row); db.commit(); db.refresh(row)
        return {"ok": True, "idempotent": False, "execution": serialize_swap_execution(row), "unsigned_transaction_plan": self._swap_plan(row), "send_gate": gate}

    def record_swap_submission(
        self, db: Session, *, execution_id: str, tx_hash: str, wallet_address: str,
        claim_id: str, confirm_record: bool,
    ) -> Dict[str, Any]:
        if confirm_record is not True:
            raise ValueError("confirm_robinhood_chain_swap_submission_record_required")
        row = self._get(db, execution_id)
        validate_execution_saved_wallet(row.wallet_address, wallet_address)
        claim = validate_claim_id(claim_id)
        tx = validate_transaction_hash(tx_hash)
        lifecycle = _stage_lifecycle(row, "swap")
        if row.swap_tx_hash == tx and row.status in {"swap_pending", "confirmed"}:
            return {"ok": True, "idempotent": True, "execution": serialize_swap_execution(row)}
        if row.status != "swap_send_claimed" or lifecycle.get("send_claim_id") != claim:
            raise ValueError("robinhood_chain_swap_claim_mismatch")
        attempts = int(lifecycle.get("submission_attempts") or 0) + 1
        now = utc_now()
        row.swap_tx_hash = tx
        row.status = "swap_pending"
        row.swap_status = "pending"
        _set_stage_lifecycle(row, "swap", {"submission_attempts": attempts, "submitted_at": now.isoformat()})
        row.updated_at = _utc_naive(now)
        db.add(row); db.commit(); db.refresh(row)
        return {"ok": True, "idempotent": False, "execution": serialize_swap_execution(row)}

    def _decode_swap_input(self, row: RobinhoodChainSwapExecution, receipt: Dict[str, Any]) -> tuple[int, int]:
        wallet = row.wallet_address.lower()
        net_input_atomic = 0
        transfer_log_count = 0
        for item in receipt.get("logs") or []:
            if not isinstance(item, dict):
                continue
            try:
                if validate_evm_address(str(item.get("address") or "")).lower() != ROBINHOOD_CHAIN_SWAP_USDG_CONTRACT:
                    continue
            except ValueError:
                continue
            topics = item.get("topics")
            if not isinstance(topics, list) or len(topics) < 3 or str(topics[0]).lower() != _ERC20_TRANSFER_TOPIC0:
                continue
            source = _topic_address(topics[1])
            destination = _topic_address(topics[2])
            try:
                amount = decode_hex_quantity(item.get("data"))
            except ValueError:
                continue
            if source == wallet:
                net_input_atomic += amount; transfer_log_count += 1
            if destination == wallet:
                net_input_atomic -= amount; transfer_log_count += 1
        if net_input_atomic != int(row.exact_input_amount_atomic):
            raise ValueError("robinhood_chain_swap_usdg_spend_mismatch")
        return net_input_atomic, transfer_log_count

    async def refresh_swap(self, db: Session, *, execution_id: str) -> Dict[str, Any]:
        row = self._get(db, execution_id)
        if row.status == "confirmed":
            return {"ok": True, "idempotent": True, "execution": serialize_swap_execution(row), "send_gate": _swap_gate()}
        if row.status != "swap_pending" or not row.swap_tx_hash:
            raise ValueError("robinhood_chain_swap_not_pending")
        chain = await self.rpc_client.verify_expected_chain(force_refresh=True)
        if chain.get("ok") is not True or chain.get("chain_id_matches") is not True:
            raise ValueError("robinhood_chain_swap_chain_mismatch")
        tx = await self._verified_transaction(row, stage="swap")
        receipt_record = await self.rpc_client.rpc_read("eth_getTransactionReceipt", [row.swap_tx_hash], cache_namespace=None, force_refresh=True)
        if receipt_record.get("ok") is not True:
            raise ValueError("robinhood_chain_swap_receipt_read_failed")
        receipt = receipt_record.get("result")
        now = utc_now()
        _set_stage_lifecycle(row, "swap", {"last_receipt_check_at": now.isoformat()})
        if tx is None or receipt is None:
            db.add(row); db.commit(); db.refresh(row)
            return {"ok": True, "pending": True, "execution": serialize_swap_execution(row), "send_gate": _swap_gate()}
        receipt_status = decode_hex_quantity(receipt.get("status"))
        lifecycle = _stage_lifecycle(row, "swap")
        lifecycle_values = {
            "receipt_status": receipt_status,
            "block_number": decode_hex_quantity(receipt.get("blockNumber")) if receipt.get("blockNumber") else None,
            "gas_used": str(decode_hex_quantity(receipt.get("gasUsed"))) if receipt.get("gasUsed") else None,
            "effective_gas_price_wei": str(decode_hex_quantity(receipt.get("effectiveGasPrice"))) if receipt.get("effectiveGasPrice") else None,
        }
        if receipt_status == 0:
            lifecycle_values["reverted_at"] = now.isoformat()
            row.status = "swap_reverted"
            row.swap_status = "reverted"
            row.error_code = "swap_reverted"
        elif receipt_status == 1:
            input_atomic, transfer_log_count = self._decode_swap_input(row, receipt)
            block_number = int(lifecycle_values.get("block_number") or 0)
            if block_number <= 0:
                raise ValueError("robinhood_chain_swap_receipt_block_missing")
            before_tag = hex(block_number - 1)
            after_tag = hex(block_number)
            pre_eth_result = await self.rpc_client.get_native_balance(
                row.wallet_address, block_tag=before_tag, force_refresh=True
            )
            post_eth_result = await self.rpc_client.get_native_balance(
                row.wallet_address, block_tag=after_tag, force_refresh=True
            )
            pre_usdg_result = await self.rpc_client.get_erc20_balance(
                row.wallet_address, row.allowance_token_address, ROBINHOOD_CHAIN_SWAP_USDG_DECIMALS,
                block_tag=before_tag, force_refresh=True,
            )
            post_usdg_result = await self.rpc_client.get_erc20_balance(
                row.wallet_address, row.allowance_token_address, ROBINHOOD_CHAIN_SWAP_USDG_DECIMALS,
                block_tag=after_tag, force_refresh=True,
            )
            if (
                pre_eth_result.get("ok") is not True
                or post_eth_result.get("ok") is not True
                or pre_usdg_result.get("ok") is not True
                or post_usdg_result.get("ok") is not True
            ):
                raise ValueError("robinhood_chain_swap_receipt_block_balance_snapshot_failed")
            gas_used = int(lifecycle_values.get("gas_used") or 0)
            gas_price = int(lifecycle_values.get("effective_gas_price_wei") or 0)
            if gas_used <= 0 or gas_price <= 0:
                raise ValueError("robinhood_chain_swap_receipt_gas_fields_missing")
            swap_fee_wei = gas_used * gas_price
            pre_eth = int(str(pre_eth_result.get("balance_wei") or "0"))
            post_eth = int(str(post_eth_result.get("balance_wei") or "0"))
            pre_usdg = int(str(pre_usdg_result.get("balance_atomic") or "0"))
            post_usdg = int(str(post_usdg_result.get("balance_atomic") or "0"))
            if pre_usdg - post_usdg != input_atomic:
                raise ValueError("robinhood_chain_swap_usdg_balance_delta_mismatch")
            output_wei = post_eth - pre_eth + swap_fee_wei
            if output_wei <= 0 or output_wei < int(row.minimum_output_amount_atomic):
                raise ValueError("robinhood_chain_swap_native_output_missing_or_below_minimum")
            approval_lifecycle = _stage_lifecycle(row, "approval")
            approval_fee_wei = int(approval_lifecycle.get("gas_used") or 0) * int(approval_lifecycle.get("effective_gas_price_wei") or 0)
            input_amount = Decimal(input_atomic) / (Decimal(10) ** ROBINHOOD_CHAIN_SWAP_USDG_DECIMALS)
            output_amount = Decimal(output_wei) / _WEI_PER_ETH
            reconciliation = {
                "version": ROBINHOOD_CHAIN_SWAP_TRANCHE, "reconciled": True, "reconciled_at": now.isoformat(),
                "input_asset": "USDG", "input_amount_atomic": str(input_atomic), "input_amount": _decimal_text(input_amount),
                "output_asset": "ETH", "output_amount_atomic": str(output_wei), "output_amount": _decimal_text(output_amount),
                "minimum_output_amount_atomic": row.minimum_output_amount_atomic, "minimum_output_amount": row.minimum_output_amount,
                "average_fill_price": _decimal_text(input_amount / output_amount), "fee_asset": "ETH",
                "swap_network_fee_wei": str(swap_fee_wei), "swap_network_fee": _decimal_text(Decimal(swap_fee_wei) / _WEI_PER_ETH),
                "approval_network_fee_wei": str(approval_fee_wei), "approval_network_fee": _decimal_text(Decimal(approval_fee_wei) / _WEI_PER_ETH),
                "total_network_fee_wei": str(swap_fee_wei + approval_fee_wei), "total_network_fee": _decimal_text(Decimal(swap_fee_wei + approval_fee_wei) / _WEI_PER_ETH),
                "usdg_transfer_log_count": transfer_log_count, "approval_tx_hash": row.approval_tx_hash, "swap_tx_hash": row.swap_tx_hash,
            }
            route = _route_copy(row)
            route["execution_reconciliation"] = reconciliation
            row.route = route
            lifecycle_values.update({
                "confirmed_at": now.isoformat(),
                "receipt_block_balance_snapshot": {
                    "captured_at": now.isoformat(),
                    "before_block_tag": before_tag,
                    "after_block_tag": after_tag,
                    "pre_eth_balance_wei": str(pre_eth),
                    "post_eth_balance_wei": str(post_eth),
                    "pre_usdg_balance_atomic": str(pre_usdg),
                    "post_usdg_balance_atomic": str(post_usdg),
                },
            })
            row.status = "confirmed"
            row.swap_status = "confirmed"
            row.error_code = None
            row.error_message = None
        else:
            raise ValueError("robinhood_chain_swap_invalid_receipt_status")
        _set_stage_lifecycle(row, "swap", lifecycle_values)
        row.updated_at = _utc_naive(now)
        db.add(row); db.commit(); db.refresh(row)
        return {"ok": True, "pending": False, "execution": serialize_swap_execution(row), "send_gate": _swap_gate()}


_SERVICE: Optional[RobinhoodChainSwapExecutionService] = None


def get_robinhood_chain_swap_execution_service() -> RobinhoodChainSwapExecutionService:
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = RobinhoodChainSwapExecutionService()
    return _SERVICE
