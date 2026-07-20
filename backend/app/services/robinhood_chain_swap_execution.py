from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from ..models import RobinhoodChainSwapExecution
from .evm_rpc import encode_erc20_approve, validate_evm_address
from .robinhood_chain_execution import validate_execution_saved_wallet
from .robinhood_chain_transaction_planning import (
    EXPECTED_CHAIN_ID,
    ROBINHOOD_CHAIN_ALLOWANCE_HOLDER_ALLOWLIST,
    ROBINHOOD_CHAIN_DEFAULT_SLIPPAGE_BPS,
    ROBINHOOD_CHAIN_MAX_SLIPPAGE_BPS,
    ROBINHOOD_CHAIN_MIN_SLIPPAGE_BPS,
    RobinhoodChainTransactionPlanningService,
    get_robinhood_chain_transaction_planning_service,
)


ROBINHOOD_CHAIN_SWAP_TRANCHE = "RH-CHAIN.10D.2-R5A"
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


def _review_gate() -> Dict[str, Any]:
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
        "wallet_connection_requested": False,
        "signing_enabled": False,
        "broadcast_enabled": False,
        "execution_enabled": False,
        "automatic_second_transaction": False,
        "backend_private_key": False,
        "backend_transaction_sender": False,
        "generic_live_venues_required": False,
        "ledger_mutation_enabled": False,
        "fifo_mutation_enabled": False,
        "basis_mutation_enabled": False,
        "review_only": True,
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
        },
        "automatic_second_transaction": False,
        "signing_enabled": False,
        "broadcast_enabled": False,
        "review_only": True,
        "will_mutate": False,
        "created_at": _iso(row.created_at),
        "updated_at": _iso(row.updated_at),
    }


class RobinhoodChainSwapExecutionService:
    """Review-only exact-spend lifecycle preparation for USDG -> native ETH.

    R5A persists one generalized plan-bound record and returns a finite ERC-20
    approval transaction for operator review. It cannot claim, sign, broadcast,
    or record a transaction hash.
    """

    def __init__(
        self,
        *,
        planning_service: Optional[RobinhoodChainTransactionPlanningService] = None,
    ) -> None:
        self.planning_service = planning_service or get_robinhood_chain_transaction_planning_service()

    def status(self) -> Dict[str, Any]:
        return {"ok": True, **_review_gate()}

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
            "approval_transaction_plan": self._approval_plan(row) if row.approval_required else None,
            "review_gate": _review_gate(),
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
                RobinhoodChainSwapExecution.status.in_(["approval_prepared", "allowance_sufficient"]),
            )
            .order_by(RobinhoodChainSwapExecution.created_at.desc())
            .first()
        )
        if existing is not None:
            _validate_row(existing)
            if _as_utc(existing.plan_expires_at) > now:
                return {
                    "ok": True,
                    "idempotent": True,
                    "approval_required": bool(existing.approval_required),
                    "execution": serialize_swap_execution(existing),
                    "approval_transaction_plan": self._approval_plan(existing) if existing.approval_required else None,
                    "review_gate": _review_gate(),
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
            "review_gate": _review_gate(),
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


_SERVICE: Optional[RobinhoodChainSwapExecutionService] = None


def get_robinhood_chain_swap_execution_service() -> RobinhoodChainSwapExecutionService:
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = RobinhoodChainSwapExecutionService()
    return _SERVICE
