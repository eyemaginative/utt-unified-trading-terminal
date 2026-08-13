from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import hmac
import json
import os
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import httpx

from ..config import settings
from .evm_rpc import validate_evm_address


UNISWAP_PROVIDER = "uniswap_api"
UNISWAP_QUOTE_PATH = "/quote"
UNISWAP_CHECK_APPROVAL_PATH = "/check_approval"
UNISWAP_SWAP_PATH = "/swap"
UNISWAP_CHAIN_ID = 4663
UNISWAP_ROUTER_VERSION = "2.1.1"
UNISWAP_AMM_PROTOCOLS = ("V2", "V3", "V4")
UNISWAP_NATIVE_TOKEN = "0x0000000000000000000000000000000000000000"
_ALLOWED_ROUTING = frozenset({"CLASSIC", "WRAP", "UNWRAP"})
_MAX_PROVIDER_ERROR_TEXT = 1200
_MAX_UINT256 = (1 << 256) - 1
_APPROVE_SELECTOR = "0x095ea7b3"
_BOOK_MULTIPLIERS = (Decimal("0.25"), Decimal("0.5"), Decimal("1"), Decimal("2"), Decimal("4"))
_WALLET_APPROVAL_CAPABILITY_VERSION = "r5c5d2f2_wallet_approval_v1"
_WALLET_APPROVAL_RECEIPT_TTL_SECONDS = 30 * 60
_WALLET_APPROVAL_CAPABILITY_DOMAIN = b"UTT:R5C.5D.2F.2:wallet-approval-capability:v1"
_WALLET_SWAP_CAPABILITY_VERSION = "r5c5d2f3_wallet_swap_v1"
_WALLET_SWAP_RECEIPT_TTL_SECONDS = 30 * 60
_WALLET_SWAP_CAPABILITY_DOMAIN = b"UTT:R5C.5D.2F.3:wallet-swap-capability:v1"


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _decimal(value: Any, *, field: str) -> Decimal:
    text = str(value if value is not None else "").strip()
    try:
        number = Decimal(text)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid_{field}") from exc
    if not number.is_finite() or number <= 0:
        raise ValueError(f"invalid_{field}")
    return number


def _decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _atomic_to_display(value: Any, decimals: int, *, field: str) -> Tuple[str, str]:
    text = str(value if value is not None else "").strip()
    if not text.isdigit():
        raise ValueError(f"invalid_{field}")
    atomic = int(text)
    if atomic < 0 or atomic > _MAX_UINT256:
        raise ValueError(f"invalid_{field}")
    display = Decimal(atomic) / (Decimal(10) ** int(decimals))
    return str(atomic), _decimal_text(display)


def _display_to_atomic(value: Any, decimals: int) -> Tuple[str, str]:
    number = _decimal(value, field="requested_amount")
    places = int(decimals)
    if places < 0 or places > 18:
        raise ValueError("invalid_token_decimals")
    exponent = max(0, -number.as_tuple().exponent)
    if exponent > places:
        raise ValueError("requested_amount_exceeds_token_precision")
    atomic = int(number * (Decimal(10) ** places))
    if atomic <= 0 or atomic > _MAX_UINT256:
        raise ValueError("invalid_requested_amount")
    return str(atomic), _decimal_text(Decimal(atomic) / (Decimal(10) ** places))


def _scaled_display_amount(value: Any, multiplier: Decimal, decimals: int) -> str:
    number = _decimal(value, field="probe_amount") * multiplier
    places = max(0, min(int(decimals), 18))
    atomic = int(number * (Decimal(10) ** places))
    if atomic <= 0:
        atomic = 1
    return _decimal_text(Decimal(atomic) / (Decimal(10) ** places))


def _normalize_evm_quantity(
    value: Any,
    *,
    field: str,
    allow_zero: bool,
) -> str:
    text = str(value if value is not None else "").strip().lower()
    try:
        if text.startswith("0x"):
            digits = text[2:]
            if not digits or any(ch not in "0123456789abcdef" for ch in digits):
                raise ValueError
            number = int(digits, 16)
        elif text.isdigit():
            number = int(text, 10)
        else:
            raise ValueError
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid_{field}") from exc
    if number < 0 or number > _MAX_UINT256 or (not allow_zero and number == 0):
        raise ValueError(f"invalid_{field}")
    return str(number)


def _normalize_transaction(
    value: Any,
    *,
    expected_from: str,
    native_input: bool,
    expected_value: str,
    require_gas_limit: bool = True,
) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("invalid_uniswap_transaction")
    sender = validate_evm_address(str(value.get("from") or "").strip())
    if sender.lower() != expected_from.lower():
        raise ValueError("uniswap_transaction_sender_mismatch")
    destination = validate_evm_address(str(value.get("to") or "").strip())
    chain_id_text = _normalize_evm_quantity(
        value.get("chainId"),
        field="uniswap_transaction_chain_id",
        allow_zero=False,
    )
    chain_id = int(chain_id_text)
    if chain_id != UNISWAP_CHAIN_ID:
        raise ValueError("uniswap_transaction_chain_id_mismatch")
    data = str(value.get("data") or "").strip().lower()
    if not data.startswith("0x") or len(data) < 10 or any(ch not in "0123456789abcdef" for ch in data[2:]):
        raise ValueError("invalid_uniswap_transaction_calldata")
    value_wei = _normalize_evm_quantity(
        value.get("value") if value.get("value") is not None else "0",
        field="uniswap_transaction_value",
        allow_zero=True,
    )
    expected_value_wei = _normalize_evm_quantity(
        expected_value,
        field="uniswap_expected_transaction_value",
        allow_zero=True,
    )
    if native_input:
        if value_wei != expected_value_wei:
            raise ValueError("uniswap_native_transaction_value_mismatch")
    elif value_wei != "0":
        raise ValueError("uniswap_erc20_transaction_value_must_be_zero")
    raw_gas_limit = value.get("gasLimit")
    if raw_gas_limit is None or str(raw_gas_limit).strip() == "":
        if require_gas_limit:
            raise ValueError("invalid_uniswap_transaction_gas_limit")
        gas_limit: Optional[str] = None
    else:
        gas_limit = _normalize_evm_quantity(
            raw_gas_limit,
            field="uniswap_transaction_gas_limit",
            allow_zero=False,
        )

    def optional_quantity(key: str, field: str) -> Optional[str]:
        raw = value.get(key)
        if raw is None or str(raw).strip() == "":
            return None
        return _normalize_evm_quantity(raw, field=field, allow_zero=True)

    return {
        "from": sender,
        "to": destination,
        "data": data,
        "value_wei": value_wei,
        "gas_limit": gas_limit,
        "chain_id": chain_id,
        "max_fee_per_gas": optional_quantity("maxFeePerGas", "uniswap_transaction_max_fee_per_gas"),
        "max_priority_fee_per_gas": optional_quantity(
            "maxPriorityFeePerGas",
            "uniswap_transaction_max_priority_fee_per_gas",
        ),
        "gas_price": optional_quantity("gasPrice", "uniswap_transaction_gas_price"),
    }


def _normalize_exact_approval(
    value: Any,
    *,
    expected_from: str,
    input_token: Dict[str, Any],
    input_amount_atomic: str,
) -> Dict[str, Any]:
    tx = _normalize_transaction(
        value,
        expected_from=expected_from,
        native_input=False,
        expected_value="0",
    )
    token_address = validate_evm_address(str(input_token.get("provider_address") or "").strip())
    if tx["to"].lower() != token_address.lower():
        raise ValueError("uniswap_approval_token_mismatch")
    data = str(tx["data"] or "").lower()
    if not data.startswith(_APPROVE_SELECTOR) or len(data) < 138:
        raise ValueError("invalid_uniswap_approval_calldata")
    spender = validate_evm_address("0x" + data[34:74])
    provider_approved_atomic = str(int(data[74:138], 16))
    exact_approved_atomic = _normalize_evm_quantity(
        input_amount_atomic,
        field="uniswap_approval_input_amount",
        allow_zero=False,
    )
    exact_data = (
        _APPROVE_SELECTOR
        + ("0" * 24)
        + spender[2:].lower()
        + int(exact_approved_atomic).to_bytes(32, "big").hex()
    )
    tx.update({
        "data": exact_data,
        "token": token_address,
        "token_symbol": input_token.get("symbol"),
        "spender": spender,
        "approved_amount_atomic": exact_approved_atomic,
        "provider_approved_amount_atomic": provider_approved_atomic,
        "provider_approval_rewritten": provider_approved_atomic != exact_approved_atomic,
        "approval_exact": True,
        "unlimited_approval": False,
    })
    return tx


def validate_wallet_rejection_handoff(
    plan: Dict[str, Any],
    *,
    wallet_address: str,
    native_balance_wei: Any,
    input_balance_atomic: Any = None,
) -> Dict[str, Any]:
    """Validate one fresh Uniswap plan for the deliberate first-wallet rejection test.

    This helper never requests a wallet, signs, broadcasts, or mutates state. It
    converts an already-normalized firm plan into exactly one transaction request
    and fails closed unless live input/gas balances cover the reviewed request.
    """
    if not isinstance(plan, dict) or plan.get("ok") is not True:
        raise ValueError("wallet_rejection_firm_plan_required")
    if str(plan.get("provider") or "").strip().lower() != UNISWAP_PROVIDER:
        raise ValueError("wallet_rejection_provider_mismatch")
    if plan.get("read_only") is not True or plan.get("execution_enabled") is not False:
        raise ValueError("wallet_rejection_plan_safety_mismatch")
    if plan.get("signing_enabled") is not False or plan.get("broadcast_enabled") is not False:
        raise ValueError("wallet_rejection_plan_safety_mismatch")

    wallet = validate_evm_address(wallet_address)
    unsigned = plan.get("unsigned_transaction_plan")
    if not isinstance(unsigned, dict):
        raise ValueError("wallet_rejection_unsigned_plan_required")
    if validate_evm_address(str(unsigned.get("from") or "")).lower() != wallet.lower():
        raise ValueError("wallet_rejection_wallet_mismatch")
    if int(unsigned.get("chain_id") or 0) != UNISWAP_CHAIN_ID:
        raise ValueError("wallet_rejection_chain_mismatch")

    input_atomic = _normalize_evm_quantity(
        unsigned.get("input_amount_atomic") or plan.get("input_amount_atomic"),
        field="wallet_rejection_input_amount",
        allow_zero=False,
    )
    native_input = bool(unsigned.get("native_input"))
    approval_required = plan.get("approval_required") is True
    allowance = plan.get("allowance") if isinstance(plan.get("allowance"), dict) else {}

    if native_input and approval_required:
        raise ValueError("wallet_rejection_native_approval_forbidden")

    action = "swap"
    tx = unsigned
    approval_summary: Optional[Dict[str, Any]] = None
    if approval_required:
        if allowance.get("applicable") is not True:
            raise ValueError("wallet_rejection_allowance_mismatch")
        if allowance.get("approval_exact") is not True or allowance.get("unlimited_approval") is not False:
            raise ValueError("wallet_rejection_exact_approval_required")
        required_atomic = _normalize_evm_quantity(
            allowance.get("required_amount_atomic"),
            field="wallet_rejection_required_approval",
            allow_zero=False,
        )
        if required_atomic != input_atomic:
            raise ValueError("wallet_rejection_approval_amount_mismatch")
        approval = allowance.get("approval_transaction_plan")
        if not isinstance(approval, dict):
            raise ValueError("wallet_rejection_approval_plan_required")
        if approval.get("approval_exact") is not True or approval.get("unlimited_approval") is not False:
            raise ValueError("wallet_rejection_exact_approval_required")
        approved_atomic = _normalize_evm_quantity(
            approval.get("approved_amount_atomic"),
            field="wallet_rejection_approved_amount",
            allow_zero=False,
        )
        if approved_atomic != input_atomic:
            raise ValueError("wallet_rejection_approval_amount_mismatch")
        if _normalize_evm_quantity(
            approval.get("value_wei") if approval.get("value_wei") is not None else "0",
            field="wallet_rejection_approval_value",
            allow_zero=True,
        ) != "0":
            raise ValueError("wallet_rejection_approval_value_mismatch")
        if validate_evm_address(str(approval.get("from") or "")).lower() != wallet.lower():
            raise ValueError("wallet_rejection_wallet_mismatch")
        if int(approval.get("chain_id") or 0) != UNISWAP_CHAIN_ID:
            raise ValueError("wallet_rejection_chain_mismatch")
        action = "approval"
        tx = approval
        approval_summary = {
            "token": validate_evm_address(str(approval.get("token") or "")),
            "token_symbol": str(approval.get("token_symbol") or unsigned.get("input_asset") or "").strip().upper(),
            "spender": validate_evm_address(str(approval.get("spender") or "")),
            "approved_amount_atomic": approved_atomic,
            "provider_approved_amount_atomic": _normalize_evm_quantity(
                approval.get("provider_approved_amount_atomic"),
                field="wallet_rejection_provider_approval_amount",
                allow_zero=False,
            ),
            "provider_approval_rewritten": bool(approval.get("provider_approval_rewritten")),
            "unlimited_approval": False,
        }
    elif plan.get("requires_refresh_after_approval") is True:
        raise ValueError("wallet_rejection_stale_post_approval_plan")
    elif unsigned.get("provider_simulation_requested") is not True:
        raise ValueError("wallet_rejection_swap_simulation_required")

    tx_from = validate_evm_address(str(tx.get("from") or ""))
    tx_to = validate_evm_address(str(tx.get("to") or ""))
    if tx_from.lower() != wallet.lower():
        raise ValueError("wallet_rejection_wallet_mismatch")
    data = str(tx.get("data") or "").strip().lower()
    if not data.startswith("0x") or len(data) < 10 or len(data[2:]) % 2 != 0:
        raise ValueError("wallet_rejection_calldata_invalid")
    try:
        bytes.fromhex(data[2:])
    except ValueError as exc:
        raise ValueError("wallet_rejection_calldata_invalid") from exc

    value_wei = _normalize_evm_quantity(
        tx.get("value_wei") if tx.get("value_wei") is not None else "0",
        field="wallet_rejection_transaction_value",
        allow_zero=True,
    )
    if action == "swap":
        expected_value = input_atomic if native_input else "0"
        if value_wei != expected_value:
            raise ValueError("wallet_rejection_transaction_value_mismatch")
    elif value_wei != "0":
        raise ValueError("wallet_rejection_approval_value_mismatch")

    gas_limit = _normalize_evm_quantity(
        tx.get("gas_limit"),
        field="wallet_rejection_gas_limit",
        allow_zero=False,
    )
    max_fee_per_gas = None
    gas_price = None
    if tx.get("max_fee_per_gas") not in (None, ""):
        max_fee_per_gas = _normalize_evm_quantity(
            tx.get("max_fee_per_gas"),
            field="wallet_rejection_max_fee_per_gas",
            allow_zero=False,
        )
    if tx.get("gas_price") not in (None, ""):
        gas_price = _normalize_evm_quantity(
            tx.get("gas_price"),
            field="wallet_rejection_gas_price",
            allow_zero=False,
        )
    fee_per_gas = max_fee_per_gas or gas_price
    if fee_per_gas is None:
        raise ValueError("wallet_rejection_fee_cap_unavailable")
    max_priority_fee_per_gas = None
    if tx.get("max_priority_fee_per_gas") not in (None, ""):
        max_priority_fee_per_gas = _normalize_evm_quantity(
            tx.get("max_priority_fee_per_gas"),
            field="wallet_rejection_max_priority_fee_per_gas",
            allow_zero=True,
        )
        if max_fee_per_gas is not None and int(max_priority_fee_per_gas) > int(max_fee_per_gas):
            raise ValueError("wallet_rejection_fee_fields_invalid")

    maximum_fee_wei = int(gas_limit) * int(fee_per_gas)
    native_balance = int(_normalize_evm_quantity(
        native_balance_wei,
        field="wallet_rejection_native_balance",
        allow_zero=True,
    ))
    if native_input:
        required_native_wei = int(input_atomic) + maximum_fee_wei
        if native_balance < required_native_wei:
            raise ValueError("wallet_rejection_insufficient_native_balance")
        input_balance_text = str(native_balance)
    else:
        input_balance_text = _normalize_evm_quantity(
            input_balance_atomic,
            field="wallet_rejection_input_balance",
            allow_zero=True,
        )
        if int(input_balance_text) < int(input_atomic):
            raise ValueError("wallet_rejection_insufficient_input_balance")
        if native_balance < maximum_fee_wei:
            raise ValueError("wallet_rejection_insufficient_native_gas_balance")
        required_native_wei = maximum_fee_wei

    return {
        "ok": True,
        "provider": UNISWAP_PROVIDER,
        "chain_id": UNISWAP_CHAIN_ID,
        "wallet_address": wallet,
        "action": action,
        "input_asset": str(unsigned.get("input_asset") or "").strip().upper(),
        "output_asset": str(unsigned.get("output_asset") or "").strip().upper(),
        "input_amount": str(unsigned.get("input_amount") or plan.get("input_amount") or "").strip(),
        "input_amount_atomic": input_atomic,
        "input_registry_id": plan.get("input_registry_id"),
        "output_asset": str(unsigned.get("output_asset") or plan.get("output_asset") or "").strip().upper(),
        "output_registry_id": plan.get("output_registry_id"),
        "output_amount": str(plan.get("output_amount") or "").strip() or None,
        "output_amount_atomic": str(plan.get("output_amount_atomic") or "").strip() or None,
        "minimum_received": str(unsigned.get("minimum_received") or plan.get("minimum_received") or "").strip() or None,
        "minimum_received_atomic": str(unsigned.get("minimum_received_atomic") or plan.get("minimum_received_atomic") or "").strip() or None,
        "native_input": native_input,
        "approval_required": approval_required,
        "provider_simulation_requested": bool(unsigned.get("provider_simulation_requested")),
        "requires_refresh_after_approval": bool(plan.get("requires_refresh_after_approval")),
        "approval": approval_summary,
        "transaction": {
            "from": tx_from,
            "to": tx_to,
            "data": data,
            "value_wei": value_wei,
            "gas_limit": gas_limit,
            "gas_price": gas_price,
            "max_fee_per_gas": max_fee_per_gas,
            "max_priority_fee_per_gas": max_priority_fee_per_gas,
            "chain_id": UNISWAP_CHAIN_ID,
        },
        "balance_checks": {
            "input_balance_atomic": input_balance_text,
            "native_balance_wei": str(native_balance),
            "maximum_fee_wei": str(maximum_fee_wei),
            "required_native_wei": str(required_native_wei),
            "input_balance_sufficient": True,
            "native_balance_sufficient": True,
        },
        "successful_broadcast_authorized": False,
        "reject_only": True,
        "automatic_retry": False,
        "automatic_second_transaction": False,
    }


def validate_wallet_successful_approval_handoff(handoff: Dict[str, Any]) -> Dict[str, Any]:
    """Promote a validated reject-only approval handoff to one explicit approval send.

    The transaction itself is unchanged. This helper only changes the authority
    envelope after re-checking that the first action is one exact finite ERC-20
    approval. It never signs, broadcasts, or authorizes a swap request.
    """
    if not isinstance(handoff, dict) or handoff.get("ok") is not True:
        raise ValueError("wallet_approval_handoff_required")
    if str(handoff.get("provider") or "").strip().lower() != UNISWAP_PROVIDER:
        raise ValueError("wallet_approval_provider_mismatch")
    if str(handoff.get("action") or "").strip().lower() != "approval":
        raise ValueError("wallet_approval_exact_finite_approval_required")
    if handoff.get("approval_required") is not True or handoff.get("native_input") is True:
        raise ValueError("wallet_approval_exact_finite_approval_required")
    approval = handoff.get("approval") if isinstance(handoff.get("approval"), dict) else {}
    if approval.get("unlimited_approval") is not False:
        raise ValueError("wallet_approval_unlimited_approval_forbidden")
    approved_atomic = _normalize_evm_quantity(
        approval.get("approved_amount_atomic"),
        field="wallet_approval_approved_amount",
        allow_zero=False,
    )
    input_atomic = _normalize_evm_quantity(
        handoff.get("input_amount_atomic"),
        field="wallet_approval_input_amount",
        allow_zero=False,
    )
    if approved_atomic != input_atomic:
        raise ValueError("wallet_approval_amount_mismatch")
    transaction = handoff.get("transaction") if isinstance(handoff.get("transaction"), dict) else {}
    if _normalize_evm_quantity(
        transaction.get("value_wei") if transaction.get("value_wei") is not None else "0",
        field="wallet_approval_transaction_value",
        allow_zero=True,
    ) != "0":
        raise ValueError("wallet_approval_transaction_value_mismatch")
    data = str(transaction.get("data") or "").strip().lower()
    if not data.startswith("0x") or len(data) < 10 or len(data[2:]) % 2 != 0:
        raise ValueError("wallet_approval_calldata_invalid")
    try:
        bytes.fromhex(data[2:])
    except ValueError as exc:
        raise ValueError("wallet_approval_calldata_invalid") from exc

    promoted = copy.deepcopy(handoff)
    promoted["successful_broadcast_authorized"] = True
    promoted["reject_only"] = False
    promoted["approval_only"] = True
    promoted["swap_request_authorized"] = False
    promoted["automatic_retry"] = False
    promoted["automatic_second_transaction"] = False
    promoted["receipt_refresh_required"] = True
    return promoted


def _wallet_approval_capability_key() -> bytes:
    """Derive a stable, process-independent HMAC key from the existing UTT vault master key."""
    master = str(os.getenv("UTT_KMS_MASTER_KEY") or "").strip()
    if len(master) < 32:
        raise ValueError("wallet_approval_capability_key_unavailable")
    return hashlib.sha256(_WALLET_APPROVAL_CAPABILITY_DOMAIN + b"\x00" + master.encode("utf-8")).digest()


def _wallet_approval_b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _wallet_approval_b64decode(text: str) -> bytes:
    normalized = str(text or "").strip()
    if not normalized:
        raise ValueError("wallet_approval_capability_invalid")
    normalized += "=" * ((4 - len(normalized) % 4) % 4)
    try:
        return base64.urlsafe_b64decode(normalized.encode("ascii"))
    except Exception as exc:
        raise ValueError("wallet_approval_capability_invalid") from exc


def create_wallet_approval_capability(
    handoff: Dict[str, Any],
    *,
    symbol: str,
    side: str,
    requested_amount: str,
    ttl_seconds: int = _WALLET_APPROVAL_RECEIPT_TTL_SECONDS,
) -> Dict[str, Any]:
    promoted = validate_wallet_successful_approval_handoff(handoff)
    approval = promoted["approval"]
    transaction = promoted["transaction"]
    now = int(time.time())
    ttl = max(60, min(int(ttl_seconds), 60 * 60))
    data = str(transaction.get("data") or "").strip().lower()
    payload = {
        "version": _WALLET_APPROVAL_CAPABILITY_VERSION,
        "issued_at": now,
        "expires_at": now + ttl,
        "provider": UNISWAP_PROVIDER,
        "chain_id": UNISWAP_CHAIN_ID,
        "wallet_address": validate_evm_address(str(promoted.get("wallet_address") or "")),
        "symbol": str(symbol or "").strip().upper().replace("/", "-").replace("_", "-"),
        "side": str(side or "").strip().lower(),
        "requested_amount": str(requested_amount or "").strip(),
        "input_asset": str(promoted.get("input_asset") or "").strip().upper(),
        "output_asset": str(promoted.get("output_asset") or "").strip().upper(),
        "input_amount": str(promoted.get("input_amount") or "").strip(),
        "input_amount_atomic": _normalize_evm_quantity(
            promoted.get("input_amount_atomic"),
            field="wallet_approval_capability_input_amount",
            allow_zero=False,
        ),
        "token": validate_evm_address(str(approval.get("token") or "")),
        "token_symbol": str(approval.get("token_symbol") or promoted.get("input_asset") or "").strip().upper(),
        "spender": validate_evm_address(str(approval.get("spender") or "")),
        "approved_amount_atomic": _normalize_evm_quantity(
            approval.get("approved_amount_atomic"),
            field="wallet_approval_capability_approved_amount",
            allow_zero=False,
        ),
        "transaction_from": validate_evm_address(str(transaction.get("from") or "")),
        "transaction_to": validate_evm_address(str(transaction.get("to") or "")),
        "transaction_value_wei": _normalize_evm_quantity(
            transaction.get("value_wei") if transaction.get("value_wei") is not None else "0",
            field="wallet_approval_capability_transaction_value",
            allow_zero=True,
        ),
        "calldata_sha256": hashlib.sha256(bytes.fromhex(data[2:])).hexdigest(),
    }
    if not payload["symbol"] or payload["side"] not in {"buy", "sell"} or not payload["requested_amount"]:
        raise ValueError("wallet_approval_capability_context_invalid")
    if payload["approved_amount_atomic"] != payload["input_amount_atomic"]:
        raise ValueError("wallet_approval_capability_amount_mismatch")
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    encoded = _wallet_approval_b64encode(raw)
    signature = _wallet_approval_b64encode(hmac.new(_wallet_approval_capability_key(), encoded.encode("ascii"), hashlib.sha256).digest())
    return {
        "token": f"{encoded}.{signature}",
        "expires_at_epoch": payload["expires_at"],
        "ttl_seconds": ttl,
        "payload": dict(payload),
    }


def decode_wallet_approval_capability(token: str, *, now_epoch: Optional[int] = None) -> Dict[str, Any]:
    text = str(token or "").strip()
    parts = text.split(".")
    if len(parts) != 2:
        raise ValueError("wallet_approval_capability_invalid")
    encoded, signature = parts
    expected = _wallet_approval_b64encode(
        hmac.new(_wallet_approval_capability_key(), encoded.encode("ascii"), hashlib.sha256).digest()
    )
    if not hmac.compare_digest(signature, expected):
        raise ValueError("wallet_approval_capability_invalid")
    try:
        payload = json.loads(_wallet_approval_b64decode(encoded).decode("utf-8"))
    except Exception as exc:
        raise ValueError("wallet_approval_capability_invalid") from exc
    if not isinstance(payload, dict) or payload.get("version") != _WALLET_APPROVAL_CAPABILITY_VERSION:
        raise ValueError("wallet_approval_capability_invalid")
    now = int(time.time()) if now_epoch is None else int(now_epoch)
    expires = int(payload.get("expires_at") or 0)
    if expires <= now:
        raise ValueError("wallet_approval_capability_expired")
    return payload


def validate_wallet_approval_transaction(
    capability: Dict[str, Any],
    *,
    tx_hash: str,
    transaction: Dict[str, Any],
) -> Dict[str, Any]:
    if not isinstance(capability, dict):
        raise ValueError("wallet_approval_capability_invalid")
    tx_text = str(tx_hash or "").strip().lower()
    if len(tx_text) != 66 or not tx_text.startswith("0x"):
        raise ValueError("wallet_approval_transaction_hash_invalid")
    try:
        int(tx_text[2:], 16)
    except ValueError as exc:
        raise ValueError("wallet_approval_transaction_hash_invalid") from exc
    if not isinstance(transaction, dict):
        raise ValueError("wallet_approval_transaction_unavailable")
    actual_hash = str(transaction.get("hash") or "").strip().lower()
    if actual_hash != tx_text:
        raise ValueError("wallet_approval_transaction_hash_mismatch")
    actual_from = validate_evm_address(str(transaction.get("from") or ""))
    actual_to = validate_evm_address(str(transaction.get("to") or ""))
    if actual_from.lower() != validate_evm_address(str(capability.get("transaction_from") or "")).lower():
        raise ValueError("wallet_approval_transaction_sender_mismatch")
    if actual_to.lower() != validate_evm_address(str(capability.get("transaction_to") or "")).lower():
        raise ValueError("wallet_approval_transaction_destination_mismatch")
    value = _normalize_evm_quantity(
        transaction.get("value") if transaction.get("value") is not None else "0",
        field="wallet_approval_transaction_value",
        allow_zero=True,
    )
    if value != str(capability.get("transaction_value_wei") or "0"):
        raise ValueError("wallet_approval_transaction_value_mismatch")
    data = str(transaction.get("input") or transaction.get("data") or "").strip().lower()
    if not data.startswith("0x") or len(data[2:]) % 2 != 0:
        raise ValueError("wallet_approval_transaction_calldata_invalid")
    try:
        digest = hashlib.sha256(bytes.fromhex(data[2:])).hexdigest()
    except ValueError as exc:
        raise ValueError("wallet_approval_transaction_calldata_invalid") from exc
    if digest != str(capability.get("calldata_sha256") or "").strip().lower():
        raise ValueError("wallet_approval_transaction_calldata_mismatch")
    return {
        "tx_hash": tx_text,
        "from": actual_from,
        "to": actual_to,
        "value_wei": value,
        "calldata_sha256": digest,
    }


def validate_wallet_successful_swap_handoff(handoff: Dict[str, Any]) -> Dict[str, Any]:
    """Promote one freshly simulated swap handoff to one explicit wallet send.

    The input handoff must come from the same fresh provider/RPC/balance validation
    used by R5C.5D.2F.1. This helper refuses approval-required or stale
    post-approval plans and never authorizes an automatic second transaction.
    """
    if not isinstance(handoff, dict) or handoff.get("ok") is not True:
        raise ValueError("wallet_swap_handoff_required")
    if str(handoff.get("provider") or "").strip().lower() != UNISWAP_PROVIDER:
        raise ValueError("wallet_swap_provider_mismatch")
    if str(handoff.get("action") or "").strip().lower() != "swap":
        raise ValueError("wallet_swap_fresh_swap_required")
    if handoff.get("approval_required") is not False:
        raise ValueError("wallet_swap_approval_still_required")
    if handoff.get("requires_refresh_after_approval") is not False:
        raise ValueError("wallet_swap_stale_post_approval_plan")
    if handoff.get("provider_simulation_requested") is not True:
        raise ValueError("wallet_swap_simulation_required")
    transaction = handoff.get("transaction") if isinstance(handoff.get("transaction"), dict) else {}
    data = str(transaction.get("data") or "").strip().lower()
    if not data.startswith("0x") or len(data) < 10 or len(data[2:]) % 2 != 0:
        raise ValueError("wallet_swap_calldata_invalid")
    try:
        bytes.fromhex(data[2:])
    except ValueError as exc:
        raise ValueError("wallet_swap_calldata_invalid") from exc
    if not str(handoff.get("minimum_received") or "").strip():
        raise ValueError("wallet_swap_minimum_received_required")

    promoted = copy.deepcopy(handoff)
    promoted["successful_broadcast_authorized"] = True
    promoted["reject_only"] = False
    promoted["swap_only"] = True
    promoted["approval_request_authorized"] = False
    promoted["swap_request_authorized"] = True
    promoted["automatic_retry"] = False
    promoted["automatic_second_transaction"] = False
    promoted["receipt_refresh_required"] = True
    return promoted


def _wallet_swap_capability_key() -> bytes:
    master = str(os.getenv("UTT_KMS_MASTER_KEY") or "").strip()
    if len(master) < 32:
        raise ValueError("wallet_swap_capability_key_unavailable")
    return hashlib.sha256(_WALLET_SWAP_CAPABILITY_DOMAIN + b"\x00" + master.encode("utf-8")).digest()


def create_wallet_swap_capability(
    handoff: Dict[str, Any],
    *,
    symbol: str,
    side: str,
    requested_amount: str,
    approval_tx_hash: Optional[str] = None,
    ttl_seconds: int = _WALLET_SWAP_RECEIPT_TTL_SECONDS,
) -> Dict[str, Any]:
    promoted = validate_wallet_successful_swap_handoff(handoff)
    transaction = promoted["transaction"]
    now = int(time.time())
    ttl = max(60, min(int(ttl_seconds), 60 * 60))
    data = str(transaction.get("data") or "").strip().lower()
    approval_hash = str(approval_tx_hash or "").strip().lower() or None
    if approval_hash is not None:
        if len(approval_hash) != 66 or not approval_hash.startswith("0x"):
            raise ValueError("wallet_swap_approval_transaction_hash_invalid")
        try:
            int(approval_hash[2:], 16)
        except ValueError as exc:
            raise ValueError("wallet_swap_approval_transaction_hash_invalid") from exc
    payload = {
        "version": _WALLET_SWAP_CAPABILITY_VERSION,
        "issued_at": now,
        "expires_at": now + ttl,
        "provider": UNISWAP_PROVIDER,
        "chain_id": UNISWAP_CHAIN_ID,
        "wallet_address": validate_evm_address(str(promoted.get("wallet_address") or "")),
        "symbol": str(symbol or "").strip().upper().replace("/", "-").replace("_", "-"),
        "side": str(side or "").strip().lower(),
        "requested_amount": str(requested_amount or "").strip(),
        "approval_tx_hash": approval_hash,
        "input_asset": str(promoted.get("input_asset") or "").strip().upper(),
        "input_registry_id": int(promoted.get("input_registry_id") or 0),
        "output_asset": str(promoted.get("output_asset") or "").strip().upper(),
        "output_registry_id": int(promoted.get("output_registry_id") or 0),
        "input_amount": str(promoted.get("input_amount") or "").strip(),
        "input_amount_atomic": _normalize_evm_quantity(
            promoted.get("input_amount_atomic"),
            field="wallet_swap_capability_input_amount",
            allow_zero=False,
        ),
        "output_amount": str(promoted.get("output_amount") or "").strip(),
        "minimum_received": str(promoted.get("minimum_received") or "").strip(),
        "minimum_received_atomic": _normalize_evm_quantity(
            promoted.get("minimum_received_atomic"),
            field="wallet_swap_capability_minimum_received",
            allow_zero=False,
        ),
        "transaction_from": validate_evm_address(str(transaction.get("from") or "")),
        "transaction_to": validate_evm_address(str(transaction.get("to") or "")),
        "transaction_value_wei": _normalize_evm_quantity(
            transaction.get("value_wei") if transaction.get("value_wei") is not None else "0",
            field="wallet_swap_capability_transaction_value",
            allow_zero=True,
        ),
        "calldata_sha256": hashlib.sha256(bytes.fromhex(data[2:])).hexdigest(),
    }
    if (
        not payload["symbol"]
        or payload["side"] not in {"buy", "sell"}
        or not payload["requested_amount"]
        or not payload["input_asset"]
        or not payload["output_asset"]
        or payload["input_registry_id"] <= 0
        or payload["output_registry_id"] <= 0
        or not payload["minimum_received"]
    ):
        raise ValueError("wallet_swap_capability_context_invalid")
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    encoded = _wallet_approval_b64encode(raw)
    signature = _wallet_approval_b64encode(
        hmac.new(_wallet_swap_capability_key(), encoded.encode("ascii"), hashlib.sha256).digest()
    )
    return {
        "token": f"{encoded}.{signature}",
        "expires_at_epoch": payload["expires_at"],
        "ttl_seconds": ttl,
        "payload": dict(payload),
    }


def decode_wallet_swap_capability(token: str, *, now_epoch: Optional[int] = None) -> Dict[str, Any]:
    text = str(token or "").strip()
    parts = text.split(".")
    if len(parts) != 2:
        raise ValueError("wallet_swap_capability_invalid")
    encoded, signature = parts
    expected = _wallet_approval_b64encode(
        hmac.new(_wallet_swap_capability_key(), encoded.encode("ascii"), hashlib.sha256).digest()
    )
    if not hmac.compare_digest(signature, expected):
        raise ValueError("wallet_swap_capability_invalid")
    try:
        payload = json.loads(_wallet_approval_b64decode(encoded).decode("utf-8"))
    except Exception as exc:
        raise ValueError("wallet_swap_capability_invalid") from exc
    if not isinstance(payload, dict) or payload.get("version") != _WALLET_SWAP_CAPABILITY_VERSION:
        raise ValueError("wallet_swap_capability_invalid")
    now = int(time.time()) if now_epoch is None else int(now_epoch)
    expires = int(payload.get("expires_at") or 0)
    if expires <= now:
        raise ValueError("wallet_swap_capability_expired")
    return payload


def validate_wallet_swap_transaction(
    capability: Dict[str, Any],
    *,
    tx_hash: str,
    transaction: Dict[str, Any],
) -> Dict[str, Any]:
    if not isinstance(capability, dict):
        raise ValueError("wallet_swap_capability_invalid")
    tx_text = str(tx_hash or "").strip().lower()
    if len(tx_text) != 66 or not tx_text.startswith("0x"):
        raise ValueError("wallet_swap_transaction_hash_invalid")
    try:
        int(tx_text[2:], 16)
    except ValueError as exc:
        raise ValueError("wallet_swap_transaction_hash_invalid") from exc
    if not isinstance(transaction, dict):
        raise ValueError("wallet_swap_transaction_unavailable")
    actual_hash = str(transaction.get("hash") or "").strip().lower()
    if actual_hash != tx_text:
        raise ValueError("wallet_swap_transaction_hash_mismatch")
    actual_from = validate_evm_address(str(transaction.get("from") or ""))
    actual_to = validate_evm_address(str(transaction.get("to") or ""))
    if actual_from.lower() != validate_evm_address(str(capability.get("transaction_from") or "")).lower():
        raise ValueError("wallet_swap_transaction_sender_mismatch")
    if actual_to.lower() != validate_evm_address(str(capability.get("transaction_to") or "")).lower():
        raise ValueError("wallet_swap_transaction_destination_mismatch")
    value = _normalize_evm_quantity(
        transaction.get("value") if transaction.get("value") is not None else "0",
        field="wallet_swap_transaction_value",
        allow_zero=True,
    )
    if value != str(capability.get("transaction_value_wei") or "0"):
        raise ValueError("wallet_swap_transaction_value_mismatch")
    data = str(transaction.get("input") or transaction.get("data") or "").strip().lower()
    if not data.startswith("0x") or len(data[2:]) % 2 != 0:
        raise ValueError("wallet_swap_transaction_calldata_invalid")
    try:
        digest = hashlib.sha256(bytes.fromhex(data[2:])).hexdigest()
    except ValueError as exc:
        raise ValueError("wallet_swap_transaction_calldata_invalid") from exc
    if digest != str(capability.get("calldata_sha256") or "").strip().lower():
        raise ValueError("wallet_swap_transaction_calldata_mismatch")
    return {
        "tx_hash": tx_text,
        "from": actual_from,
        "to": actual_to,
        "value_wei": value,
        "calldata_sha256": digest,
    }


def _safe_provider_error(value: Any) -> Any:
    if isinstance(value, dict):
        out: Dict[str, Any] = {}
        for key in ("errorCode", "name", "message", "reason", "code", "detail"):
            if key in value and value.get(key) is not None:
                out[key] = value.get(key)
        validation = value.get("validationErrors")
        if isinstance(validation, list):
            out["validationErrors"] = copy.deepcopy(validation[:8])
        if out:
            return out
    text = str(value or "").strip()
    return text[:_MAX_PROVIDER_ERROR_TEXT] if text else None


def _normalize_token(token: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(token, dict):
        raise ValueError("invalid_uniswap_registry_identity")
    symbol = str(token.get("symbol") or "").strip().upper()
    registry_id = token.get("registry_id")
    identity_source = str(token.get("identity_source") or "").strip().lower()
    native = bool(token.get("native"))
    try:
        decimals = int(token.get("decimals"))
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid_uniswap_registry_identity") from exc
    if not symbol or registry_id is None or identity_source != "token_registry":
        raise ValueError("invalid_uniswap_registry_identity")
    if decimals < 0 or decimals > 18:
        raise ValueError("invalid_uniswap_registry_identity")
    if native:
        provider_address = UNISWAP_NATIVE_TOKEN
        registry_address = str(token.get("contract_address") or "").strip() or None
    else:
        try:
            provider_address = validate_evm_address(str(token.get("contract_address") or "").strip())
        except ValueError as exc:
            raise ValueError("invalid_uniswap_registry_identity") from exc
        registry_address = provider_address
    return {
        "symbol": symbol,
        "registry_id": int(registry_id),
        "decimals": decimals,
        "native": native,
        "contract_address": registry_address,
        "provider_address": provider_address,
        "identity_source": "token_registry",
        "registry_venue": token.get("registry_venue"),
        "registry_status": token.get("registry_status"),
    }


def _route_protocols(value: Any) -> List[str]:
    found: List[str] = []

    def visit(item: Any) -> None:
        if len(found) >= 12:
            return
        if isinstance(item, dict):
            for key, nested in item.items():
                key_text = str(key or "").strip().lower()
                if key_text in {"protocol", "protocolversion", "type", "version"}:
                    text = str(nested or "").strip().upper().replace("-", "_")
                    for protocol in UNISWAP_AMM_PROTOCOLS:
                        if protocol in text and protocol not in found:
                            found.append(protocol)
                visit(nested)
        elif isinstance(item, (list, tuple)):
            for nested in item:
                visit(nested)

    visit(value)
    return found


def _prohibited_artifact(body: Dict[str, Any]) -> Optional[str]:
    if body.get("permitData") is not None:
        return "permitData"
    if body.get("permitTransaction") is not None:
        return "permitTransaction"
    quote = body.get("quote") if isinstance(body.get("quote"), dict) else {}
    for key in (
        "encodedOrder",
        "orderInfo",
        "orderId",
        "transaction",
        "swap",
        "calldata",
        "methodParameters",
        "permitData",
        "permitTransaction",
    ):
        if quote.get(key) is not None:
            return f"quote.{key}"
    return None


def _failure(error: str, **extra: Any) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "ok": False,
        "error": str(error or "uniswap_quote_failed"),
        "provider": UNISWAP_PROVIDER,
        "chain_id": UNISWAP_CHAIN_ID,
        "read_only": True,
        "quote_only": True,
        "provider_contacted": False,
        "response_sanitized": True,
        "database_mutation": False,
        "will_mutate": False,
        "execution_authority": False,
        "execution_enabled": False,
        "wallet_connection_requested": False,
        "signing_enabled": False,
        "broadcast_enabled": False,
        "swap_endpoint_enabled": False,
        "order_endpoint_enabled": False,
        "transaction": None,
        "transaction_calldata": None,
        "permit_data": None,
        "permit_transaction": None,
        "encoded_order": None,
    }
    payload.update(extra)
    return payload


class RobinhoodChainUniswapQuoteService:
    """Backend-only Uniswap review planning for Robinhood Chain registry pairs.

    The service supports bounded AMM-only exact-input quotes, same-provider
    synthetic books, and unsigned review plans. It may call /check_approval and
    /swap to obtain exact approval and swap calldata, but it never requests a
    wallet connection, signs, broadcasts, writes database state, or exposes raw
    provider responses.
    """

    def __init__(
        self,
        *,
        api_base: str,
        timeout_s: float,
        max_concurrent: int,
        credential_getter: Callable[[], Optional[dict]],
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ) -> None:
        self.api_base = str(api_base or "").strip().rstrip("/")
        self.timeout_s = max(2.0, min(float(timeout_s), 30.0))
        self.max_concurrent = max(1, min(int(max_concurrent), 4))
        self.credential_getter = credential_getter
        self.transport = transport
        self._semaphore = asyncio.Semaphore(self.max_concurrent)
        self._last_good_at: Optional[str] = None
        self._last_error: Optional[str] = None

    def _credential_record(self) -> Optional[Dict[str, Any]]:
        try:
            raw = self.credential_getter()
        except Exception:
            return None
        return dict(raw) if isinstance(raw, dict) else None

    @staticmethod
    def _credential_usable(record: Optional[Dict[str, Any]]) -> bool:
        if not isinstance(record, dict):
            return False
        return bool(
            str(record.get("api_key") or "").strip()
            and bool(record.get("api_key_configured"))
            and bool(record.get("declared_read_only"))
            and not bool(record.get("dangerous_scope_present"))
            and str(record.get("venue") or "").strip().lower() == UNISWAP_PROVIDER
            and int(record.get("key_version") or 0) == 1
        )

    def status(self) -> Dict[str, Any]:
        credential = self._credential_record()
        base_configured = self.api_base.startswith("https://")
        usable = self._credential_usable(credential)
        return {
            "ok": True,
            "provider": UNISWAP_PROVIDER,
            "venue": "robinhood_chain",
            "network": "robinhood_chain",
            "chain_id": UNISWAP_CHAIN_ID,
            "mainnet_only": True,
            "api_base_configured": base_configured,
            "api_key_configured": bool((credential or {}).get("api_key_configured")),
            "credential_source": (credential or {}).get("source"),
            "credential_venue": (credential or {}).get("venue"),
            "credential_key_version": (credential or {}).get("key_version"),
            "declared_read_only": bool((credential or {}).get("declared_read_only")),
            "dangerous_scope_present": bool((credential or {}).get("dangerous_scope_present")),
            "scope_source": (credential or {}).get("scope_source"),
            "quote_endpoint_enabled": bool(base_configured and usable),
            "quote_path": UNISWAP_QUOTE_PATH,
            "check_approval_path": UNISWAP_CHECK_APPROVAL_PATH,
            "swap_calldata_path": UNISWAP_SWAP_PATH,
            "quote_type": "EXACT_INPUT",
            "routing_preference": "BEST_PRICE",
            "protocols": list(UNISWAP_AMM_PROTOCOLS),
            "universal_router_version": UNISWAP_ROUTER_VERSION,
            "permit2_disabled": True,
            "permit_amount": "EXACT",
            "generate_permit_as_transaction": False,
            "swap_endpoint_enabled": False,
            "swap_calldata_endpoint_enabled": bool(base_configured and usable),
            "check_approval_endpoint_enabled": bool(base_configured and usable),
            "order_endpoint_enabled": False,
            "database_persistence_enabled": False,
            "execution_authority": False,
            "wallet_connection_requested": False,
            "signing_enabled": False,
            "broadcast_enabled": False,
            "last_good_at": self._last_good_at,
            "last_error": self._last_error,
            "read_only": True,
        }

    async def quote(
        self,
        *,
        symbol: str,
        side: str,
        amount_mode: str,
        requested_amount: str,
        slippage_bps: int,
        swapper_address: str,
        input_token: Dict[str, Any],
        output_token: Dict[str, Any],
        confirm_quote: bool,
        _include_provider_response: bool = False,
    ) -> Dict[str, Any]:
        normalized_symbol = str(symbol or "").strip().upper().replace("/", "-").replace("_", "-")
        normalized_side = str(side or "").strip().lower()
        normalized_mode = str(amount_mode or "").strip().lower()
        if not confirm_quote:
            return _failure("uniswap_quote_confirmation_required", symbol=normalized_symbol)
        if normalized_side not in {"buy", "sell"}:
            return _failure("invalid_quote_side", symbol=normalized_symbol)
        if normalized_mode != "exact_input":
            return _failure("uniswap_quote_exact_input_only", symbol=normalized_symbol)
        try:
            slippage = int(slippage_bps)
        except (TypeError, ValueError):
            return _failure("invalid_slippage_bps", symbol=normalized_symbol)
        if slippage < 10 or slippage > 300:
            return _failure("invalid_slippage_bps", symbol=normalized_symbol)

        credential = self._credential_record()
        if not self._credential_usable(credential):
            return _failure(
                "uniswap_quote_not_configured",
                symbol=normalized_symbol,
                api_key_configured=bool((credential or {}).get("api_key_configured")),
                declared_read_only=bool((credential or {}).get("declared_read_only")),
                dangerous_scope_present=bool((credential or {}).get("dangerous_scope_present")),
            )
        if not self.api_base.startswith("https://"):
            return _failure("uniswap_quote_api_base_invalid", symbol=normalized_symbol)

        try:
            swapper = validate_evm_address(str(swapper_address or "").strip())
            input_identity = _normalize_token(input_token)
            output_identity = _normalize_token(output_token)
            if input_identity["registry_id"] == output_identity["registry_id"]:
                raise ValueError("uniswap_quote_same_asset")
            input_atomic, normalized_requested = _display_to_atomic(
                requested_amount,
                int(input_identity["decimals"]),
            )
        except ValueError as exc:
            return _failure(str(exc), symbol=normalized_symbol)

        request_body = {
            "type": "EXACT_INPUT",
            "amount": input_atomic,
            "tokenInChainId": UNISWAP_CHAIN_ID,
            "tokenOutChainId": UNISWAP_CHAIN_ID,
            "tokenIn": input_identity["provider_address"],
            "tokenOut": output_identity["provider_address"],
            "swapper": swapper,
            "slippageTolerance": float(Decimal(slippage) / Decimal(100)),
            "routingPreference": "BEST_PRICE",
            "protocols": list(UNISWAP_AMM_PROTOCOLS),
            "permitAmount": "EXACT",
            "generatePermitAsTransaction": False,
        }
        url = f"{self.api_base}{UNISWAP_QUOTE_PATH}"
        started = time.perf_counter()
        async with self._semaphore:
            try:
                async with httpx.AsyncClient(
                    timeout=httpx.Timeout(self.timeout_s),
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                        "x-api-key": str(credential.get("api_key") or ""),
                        "x-universal-router-version": UNISWAP_ROUTER_VERSION,
                        "x-permit2-disabled": "true",
                        "x-erc20eth-enabled": "false",
                        "User-Agent": "UTT-Robinhood-Chain-Uniswap-Quote/1.0",
                    },
                    transport=self.transport,
                ) as client:
                    response = await client.post(url, json=request_body)
                elapsed_ms = (time.perf_counter() - started) * 1000.0
                try:
                    body = response.json()
                except Exception:
                    body = {"message": response.text[:_MAX_PROVIDER_ERROR_TEXT]}

                if response.status_code in {401, 403}:
                    self._last_error = f"HTTP {response.status_code} from Uniswap API"
                    return _failure(
                        "uniswap_quote_authentication_failed",
                        symbol=normalized_symbol,
                        http_status=response.status_code,
                        provider_error=_safe_provider_error(body),
                        provider_contacted=True,
                    )
                if response.status_code == 429 or response.status_code >= 500:
                    self._last_error = f"HTTP {response.status_code} from Uniswap API"
                    return _failure(
                        "uniswap_quote_provider_transient_error",
                        symbol=normalized_symbol,
                        http_status=response.status_code,
                        retry_after=response.headers.get("Retry-After"),
                        provider_error=_safe_provider_error(body),
                        provider_contacted=True,
                    )
                if not response.is_success or not isinstance(body, dict):
                    self._last_error = f"HTTP {response.status_code} from Uniswap API"
                    return _failure(
                        "uniswap_quote_provider_error",
                        symbol=normalized_symbol,
                        http_status=response.status_code,
                        provider_error=_safe_provider_error(body),
                        provider_contacted=True,
                    )

                prohibited = _prohibited_artifact(body)
                if prohibited:
                    self._last_error = f"Prohibited provider artifact: {prohibited}"
                    return _failure(
                        "uniswap_quote_prohibited_artifact",
                        symbol=normalized_symbol,
                        prohibited_artifact=prohibited,
                        provider_contacted=True,
                        http_status=response.status_code,
                    )

                tx_failure = body.get("txFailureReason")
                quote_obj = body.get("quote") if isinstance(body.get("quote"), dict) else {}
                if not tx_failure:
                    tx_failure = quote_obj.get("txFailureReason")
                if tx_failure:
                    self._last_error = "Uniswap quote simulation failed"
                    return _failure(
                        "uniswap_quote_simulation_failed",
                        symbol=normalized_symbol,
                        provider_error=_safe_provider_error(tx_failure),
                        provider_contacted=True,
                        http_status=response.status_code,
                    )

                routing = str(body.get("routing") or "").strip().upper()
                if routing not in _ALLOWED_ROUTING:
                    self._last_error = f"Disallowed routing: {routing or 'missing'}"
                    return _failure(
                        "uniswap_quote_routing_not_allowed",
                        symbol=normalized_symbol,
                        routing=routing or None,
                        provider_contacted=True,
                        http_status=response.status_code,
                    )

                input_obj = quote_obj.get("input") if isinstance(quote_obj.get("input"), dict) else {}
                output_obj = quote_obj.get("output") if isinstance(quote_obj.get("output"), dict) else {}
                returned_input_token = str(input_obj.get("token") or "").strip().lower()
                returned_output_token = str(output_obj.get("token") or "").strip().lower()
                if returned_input_token != str(input_identity["provider_address"]).lower():
                    return _failure(
                        "uniswap_quote_provider_identity_mismatch",
                        symbol=normalized_symbol,
                        field="input.token",
                        provider_contacted=True,
                        http_status=response.status_code,
                    )
                if returned_output_token != str(output_identity["provider_address"]).lower():
                    return _failure(
                        "uniswap_quote_provider_identity_mismatch",
                        symbol=normalized_symbol,
                        field="output.token",
                        provider_contacted=True,
                        http_status=response.status_code,
                    )

                try:
                    returned_input_atomic, returned_input = _atomic_to_display(
                        input_obj.get("amount"),
                        int(input_identity["decimals"]),
                        field="provider_input_amount",
                    )
                    output_atomic, output_amount = _atomic_to_display(
                        output_obj.get("amount"),
                        int(output_identity["decimals"]),
                        field="provider_output_amount",
                    )
                    minimum_atomic, minimum_amount = _atomic_to_display(
                        output_obj.get("minimumAmount", output_obj.get("amount")),
                        int(output_identity["decimals"]),
                        field="provider_minimum_output_amount",
                    )
                except ValueError as exc:
                    self._last_error = str(exc)
                    return _failure(
                        "uniswap_quote_invalid_response",
                        symbol=normalized_symbol,
                        response_error=str(exc),
                        provider_contacted=True,
                        http_status=response.status_code,
                    )
                if returned_input_atomic != input_atomic:
                    return _failure(
                        "uniswap_quote_provider_amount_mismatch",
                        symbol=normalized_symbol,
                        expected_input_atomic=input_atomic,
                        returned_input_atomic=returned_input_atomic,
                        provider_contacted=True,
                        http_status=response.status_code,
                    )

                input_decimal = Decimal(returned_input)
                output_decimal = Decimal(output_amount)
                output_per_input = output_decimal / input_decimal
                if normalized_side == "buy":
                    quote_per_base = input_decimal / output_decimal
                else:
                    quote_per_base = output_decimal / input_decimal

                protocols = _route_protocols(quote_obj.get("route"))
                gas_estimate = quote_obj.get("gasUseEstimate")
                gas_estimate_usd = (
                    quote_obj.get("gasUseEstimateUSD")
                    or quote_obj.get("classicGasUseEstimateUSD")
                    or body.get("gasFeeUSD")
                )
                self._last_good_at = _utc_iso()
                self._last_error = None
                result = {
                    "ok": True,
                    "provider": UNISWAP_PROVIDER,
                    "provider_contacted": True,
                    "credential_source": credential.get("source"),
                    "credential_venue": credential.get("venue"),
                    "chain_id": UNISWAP_CHAIN_ID,
                    "symbol": normalized_symbol,
                    "side": normalized_side,
                    "amount_mode": "exact_input",
                    "routing": routing,
                    "routing_preference": "BEST_PRICE",
                    "route_protocols": protocols,
                    "requested_amount": normalized_requested,
                    "input_asset": input_identity["symbol"],
                    "input_registry_id": input_identity["registry_id"],
                    "input_amount": returned_input,
                    "input_amount_atomic": returned_input_atomic,
                    "output_asset": output_identity["symbol"],
                    "output_registry_id": output_identity["registry_id"],
                    "output_amount": output_amount,
                    "output_amount_atomic": output_atomic,
                    "minimum_received": minimum_amount,
                    "minimum_received_atomic": minimum_atomic,
                    "price_output_per_input": _decimal_text(output_per_input),
                    "price_quote_per_base": _decimal_text(quote_per_base),
                    "effective_price": _decimal_text(quote_per_base),
                    "base_quantity": output_amount if normalized_side == "buy" else returned_input,
                    "quote_quantity": returned_input if normalized_side == "buy" else output_amount,
                    "slippage_bps": slippage,
                    "slippage_percent": _decimal_text(Decimal(slippage) / Decimal(100)),
                    "is_token_approval_applicable": bool(body.get("isTokenApprovalApplicable", True)),
                    "gas_use_estimate": str(gas_estimate).strip() if gas_estimate is not None else None,
                    "gas_use_estimate_usd": str(gas_estimate_usd).strip() if gas_estimate_usd is not None else None,
                    "request_id": str(body.get("requestId") or "").strip() or None,
                    "elapsed_ms": round(elapsed_ms, 2),
                    "fetched_at": self._last_good_at,
                    "read_only": True,
                    "quote_only": True,
                    "response_sanitized": True,
                    "raw_provider_response_returned": False,
                    "database_mutation": False,
                    "will_mutate": False,
                    "execution_authority": False,
                    "execution_enabled": False,
                    "wallet_connection_requested": False,
                    "signing_enabled": False,
                    "broadcast_enabled": False,
                    "swap_endpoint_enabled": False,
                    "order_endpoint_enabled": False,
                    "transaction": None,
                    "transaction_calldata": None,
                    "permit_data": None,
                    "permit_transaction": None,
                    "encoded_order": None,
                }
                if _include_provider_response:
                    result["_provider_response"] = copy.deepcopy(body)
                return result
            except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
                self._last_error = f"{type(exc).__name__}: {exc}"
                return _failure(
                    "uniswap_quote_provider_transient_error",
                    symbol=normalized_symbol,
                    provider_error=type(exc).__name__,
                    provider_contacted=True,
                )
            except Exception as exc:
                self._last_error = f"{type(exc).__name__}: {exc}"
                return _failure(
                    "uniswap_quote_provider_error",
                    symbol=normalized_symbol,
                    provider_error=type(exc).__name__,
                    provider_contacted=True,
                )


    def _headers(self, credential: Dict[str, Any]) -> Dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "x-api-key": str(credential.get("api_key") or ""),
            "x-universal-router-version": UNISWAP_ROUTER_VERSION,
            "x-permit2-disabled": "true",
            "x-erc20eth-enabled": "false",
            "User-Agent": "UTT-Robinhood-Chain-Uniswap-Review/1.0",
        }

    async def _post_provider(
        self,
        *,
        path: str,
        body: Dict[str, Any],
        credential: Dict[str, Any],
    ) -> Tuple[Optional[httpx.Response], Any]:
        url = f"{self.api_base}{path}"
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout_s),
            headers=self._headers(credential),
            transport=self.transport,
        ) as client:
            response = await client.post(url, json=body)
        try:
            payload: Any = response.json()
        except Exception:
            payload = {"message": response.text[:_MAX_PROVIDER_ERROR_TEXT]}
        return response, payload

    async def probe(
        self,
        *,
        symbol: str,
        side: str,
        requested_amount: str,
        swapper_address: str,
        input_token: Dict[str, Any],
        output_token: Dict[str, Any],
        slippage_bps: int = 50,
    ) -> Dict[str, Any]:
        result = await self.quote(
            symbol=symbol,
            side=side,
            amount_mode="exact_input",
            requested_amount=requested_amount,
            slippage_bps=slippage_bps,
            swapper_address=swapper_address,
            input_token=input_token,
            output_token=output_token,
            confirm_quote=True,
        )
        if result.get("ok") is not True:
            return {
                **result,
                "liquidity_available": False,
                "sell_amount": requested_amount,
                "buy_amount": None,
                "price_buy_per_sell": None,
            }
        return {
            **result,
            "liquidity_available": True,
            "sell_amount": result.get("input_amount"),
            "buy_amount": result.get("output_amount"),
            "min_buy_amount": result.get("minimum_received"),
            "price_buy_per_sell": result.get("price_output_per_input"),
            "route": {
                "fills": [
                    {"source": f"UNISWAP_{protocol}"}
                    for protocol in (result.get("route_protocols") or [])
                ],
            },
            "provider_warnings": [],
        }

    async def synthetic_orderbook_for_pair(
        self,
        *,
        symbol: str,
        depth: int,
        taker_address: str,
        base_token: Dict[str, Any],
        quote_token: Dict[str, Any],
        base_to_quote_capability: Dict[str, Any],
        quote_to_base_capability: Dict[str, Any],
    ) -> Dict[str, Any]:
        normalized_symbol = str(symbol or "").strip().upper().replace("/", "-").replace("_", "-")
        base_identity = _normalize_token(base_token)
        quote_identity = _normalize_token(quote_token)
        if normalized_symbol != f"{base_identity['symbol']}-{quote_identity['symbol']}":
            return _failure("uniswap_orderbook_pair_identity_mismatch", symbol=normalized_symbol)
        for capability, from_symbol, to_symbol in (
            (base_to_quote_capability, base_identity["symbol"], quote_identity["symbol"]),
            (quote_to_base_capability, quote_identity["symbol"], base_identity["symbol"]),
        ):
            if (
                str(capability.get("provider") or "").strip().lower() != UNISWAP_PROVIDER
                or str(capability.get("from_asset") or "").strip().upper() != from_symbol
                or str(capability.get("to_asset") or "").strip().upper() != to_symbol
                or str(capability.get("amount_mode") or "").strip().lower() != "exact_input"
                or str(capability.get("indicative_status") or "").strip().lower() not in {"available", "live_verified"}
            ):
                return _failure(
                    "uniswap_orderbook_direction_unavailable",
                    symbol=normalized_symbol,
                    input_asset=from_symbol,
                    output_asset=to_symbol,
                )
        levels = max(1, min(int(depth), len(_BOOK_MULTIPLIERS)))
        bid_seed = str(base_to_quote_capability.get("probe_amount") or "").strip()
        ask_seed = str(quote_to_base_capability.get("probe_amount") or "").strip()
        try:
            bid_amounts = [
                _scaled_display_amount(bid_seed, multiplier, int(base_identity["decimals"]))
                for multiplier in _BOOK_MULTIPLIERS[:levels]
            ]
            ask_amounts = [
                _scaled_display_amount(ask_seed, multiplier, int(quote_identity["decimals"]))
                for multiplier in _BOOK_MULTIPLIERS[:levels]
            ]
        except ValueError as exc:
            return _failure(str(exc), symbol=normalized_symbol)

        bids: List[Dict[str, Any]] = []
        asks: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []
        for amount in bid_amounts:
            quote = await self.quote(
                symbol=normalized_symbol,
                side="sell",
                amount_mode="exact_input",
                requested_amount=amount,
                slippage_bps=50,
                swapper_address=taker_address,
                input_token=base_token,
                output_token=quote_token,
                confirm_quote=True,
            )
            if quote.get("ok"):
                bids.append({
                    "price": quote.get("price_quote_per_base"),
                    "size": quote.get("input_amount"),
                    "base_quantity": quote.get("input_amount"),
                    "quote_quantity": quote.get("output_amount"),
                    "input_asset": base_identity["symbol"],
                    "input_amount": quote.get("input_amount"),
                    "output_asset": quote_identity["symbol"],
                    "output_amount": quote.get("output_amount"),
                    "minimum_received": quote.get("minimum_received"),
                    "route_sources": [f"UNISWAP_{item}" for item in (quote.get("route_protocols") or [])],
                    "provider": UNISWAP_PROVIDER,
                    "synthetic": True,
                    "resting_order": False,
                    "fetched_at": quote.get("fetched_at"),
                })
            else:
                errors.append({"side": "bid", "sample_input_amount": amount, "error": quote.get("error")})
        for amount in ask_amounts:
            quote = await self.quote(
                symbol=normalized_symbol,
                side="buy",
                amount_mode="exact_input",
                requested_amount=amount,
                slippage_bps=50,
                swapper_address=taker_address,
                input_token=quote_token,
                output_token=base_token,
                confirm_quote=True,
            )
            if quote.get("ok"):
                asks.append({
                    "price": quote.get("price_quote_per_base"),
                    "size": quote.get("output_amount"),
                    "base_quantity": quote.get("output_amount"),
                    "quote_quantity": quote.get("input_amount"),
                    "input_asset": quote_identity["symbol"],
                    "input_amount": quote.get("input_amount"),
                    "output_asset": base_identity["symbol"],
                    "output_amount": quote.get("output_amount"),
                    "minimum_received": quote.get("minimum_received"),
                    "route_sources": [f"UNISWAP_{item}" for item in (quote.get("route_protocols") or [])],
                    "provider": UNISWAP_PROVIDER,
                    "synthetic": True,
                    "resting_order": False,
                    "fetched_at": quote.get("fetched_at"),
                })
            else:
                errors.append({"side": "ask", "sample_input_amount": amount, "error": quote.get("error")})
        bids.sort(key=lambda item: Decimal(str(item.get("price") or "0")), reverse=True)
        asks.sort(key=lambda item: Decimal(str(item.get("price") or "0")))
        best_bid = Decimal(str(bids[0]["price"])) if bids else None
        best_ask = Decimal(str(asks[0]["price"])) if asks else None
        spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None
        midpoint = (best_ask + best_bid) / Decimal(2) if spread is not None else None
        spread_bps = spread / midpoint * Decimal(10000) if midpoint and midpoint > 0 else None
        sources: List[str] = []
        for row in [*bids, *asks]:
            for source in row.get("route_sources") or []:
                if source not in sources:
                    sources.append(source)
        return {
            "ok": bool(bids and asks),
            "tranche": "R5C.5D.2E",
            "venue": "robinhood_chain",
            "network": "robinhood_chain",
            "chain_id": UNISWAP_CHAIN_ID,
            "chain_id_hex": hex(UNISWAP_CHAIN_ID),
            "mainnet_only": True,
            "provider": UNISWAP_PROVIDER,
            "router": UNISWAP_PROVIDER,
            "symbol": normalized_symbol,
            "resolvedSymbol": normalized_symbol,
            "base_asset": base_identity["symbol"],
            "quote_asset": quote_identity["symbol"],
            "base_token_registry_id": base_identity["registry_id"],
            "quote_token_registry_id": quote_identity["registry_id"],
            "identity_source": "token_registry",
            "capability_source": "database",
            "depth_requested": int(depth),
            "depth_returned": min(len(bids), len(asks)),
            "max_depth": len(_BOOK_MULTIPLIERS),
            "bids": bids,
            "asks": asks,
            "best_bid": _decimal_text(best_bid) if best_bid is not None else None,
            "best_ask": _decimal_text(best_ask) if best_ask is not None else None,
            "spread": _decimal_text(spread) if spread is not None else None,
            "spread_bps": _decimal_text(spread_bps) if spread_bps is not None else None,
            "midpoint": _decimal_text(midpoint) if midpoint is not None else None,
            "sources": sources,
            "route_sources": sources,
            "errors": errors[:20],
            "warning_count": len(errors),
            "liquidity_available": bool(bids and asks),
            "priceDecimals": max(6, min(12, int(quote_identity["decimals"]))),
            "sizeDecimals": max(0, min(18, int(base_identity["decimals"]))),
            "cached": False,
            "fetched_at": max([str(row.get("fetched_at") or "") for row in [*bids, *asks]] or [""]) or None,
            "snapshot_source": "uniswap_api_database_capability_samples",
            "stale": False,
            "synthetic": True,
            "resting_order": False,
            "quote_only": True,
            "read_only": True,
            "execution_enabled": False,
            "signing_enabled": False,
            "transaction_construction_enabled": False,
            "firm_quote": False,
            "transaction_calldata": None,
            "will_mutate": False,
            **({"error": "synthetic_orderbook_liquidity_incomplete"} if not (bids and asks) else {}),
        }

    async def firm_quote_plan(
        self,
        *,
        symbol: str,
        side: str,
        amount_mode: str,
        requested_amount: str,
        slippage_bps: int,
        swapper_address: str,
        input_token: Dict[str, Any],
        output_token: Dict[str, Any],
    ) -> Dict[str, Any]:
        credential = self._credential_record()
        if not self._credential_usable(credential):
            return _failure("uniswap_quote_not_configured", symbol=symbol)
        quote_result = await self.quote(
            symbol=symbol,
            side=side,
            amount_mode=amount_mode,
            requested_amount=requested_amount,
            slippage_bps=slippage_bps,
            swapper_address=swapper_address,
            input_token=input_token,
            output_token=output_token,
            confirm_quote=True,
            _include_provider_response=True,
        )
        raw_response = quote_result.pop("_provider_response", None)
        if quote_result.get("ok") is not True or not isinstance(raw_response, dict):
            return quote_result
        raw_quote = raw_response.get("quote") if isinstance(raw_response.get("quote"), dict) else None
        if raw_quote is None:
            return _failure("uniswap_firm_plan_quote_missing", symbol=symbol, provider_contacted=True)
        try:
            swapper = validate_evm_address(swapper_address)
            input_identity = _normalize_token(input_token)
            output_identity = _normalize_token(output_token)
            input_atomic = str(quote_result.get("input_amount_atomic") or "")
            if not input_atomic.isdigit() or int(input_atomic) <= 0:
                raise ValueError("invalid_uniswap_plan_input_amount")
            approval_required = False
            approval_plan: Optional[Dict[str, Any]] = None
            approval_request_id: Optional[str] = None
            if not bool(input_identity["native"]):
                approval_response, approval_body = await self._post_provider(
                    path=UNISWAP_CHECK_APPROVAL_PATH,
                    credential=credential,
                    body={
                        "walletAddress": swapper,
                        "token": input_identity["provider_address"],
                        "amount": input_atomic,
                        "chainId": UNISWAP_CHAIN_ID,
                        "tokenOut": output_identity["provider_address"],
                        "tokenOutChainId": UNISWAP_CHAIN_ID,
                        "includeGasInfo": True,
                    },
                )
                if approval_response is None or not approval_response.is_success or not isinstance(approval_body, dict):
                    return _failure(
                        "uniswap_approval_check_failed",
                        symbol=symbol,
                        provider_contacted=True,
                        http_status=getattr(approval_response, "status_code", None),
                        provider_error=_safe_provider_error(approval_body),
                    )
                if approval_body.get("cancel") is not None:
                    return _failure(
                        "uniswap_approval_reset_required",
                        symbol=symbol,
                        provider_contacted=True,
                        automatic_second_transaction=False,
                    )
                approval_request_id = str(approval_body.get("requestId") or "").strip() or None
                if approval_body.get("approval") is not None:
                    approval_plan = _normalize_exact_approval(
                        approval_body.get("approval"),
                        expected_from=swapper,
                        input_token=input_identity,
                        input_amount_atomic=input_atomic,
                    )
                    approval_required = True

            swap_simulation_requested = not approval_required
            swap_response, swap_body = await self._post_provider(
                path=UNISWAP_SWAP_PATH,
                credential=credential,
                body={
                    "quote": raw_quote,
                    "refreshGasPrice": True,
                    # A provider simulation cannot succeed until a required ERC-20
                    # approval is mined. Build review calldata without simulation,
                    # then require a fresh simulated plan after approval confirmation.
                    "simulateTransaction": swap_simulation_requested,
                    "safetyMode": "SAFE",
                },
            )
            if swap_response is None or not swap_response.is_success or not isinstance(swap_body, dict):
                return _failure(
                    "uniswap_swap_calldata_failed",
                    symbol=symbol,
                    provider_contacted=True,
                    http_status=getattr(swap_response, "status_code", None),
                    provider_error=_safe_provider_error(swap_body),
                )
            unsigned = _normalize_transaction(
                swap_body.get("swap"),
                expected_from=swapper,
                native_input=bool(input_identity["native"]),
                expected_value=input_atomic if bool(input_identity["native"]) else "0",
                require_gas_limit=swap_simulation_requested,
            )
            return {
                **quote_result,
                "ok": True,
                "tranche": "R5C.5D.2E",
                "provider": UNISWAP_PROVIDER,
                "firm_quote": True,
                "firm_plan_status": "available",
                "approval_required": approval_required,
                "allowance": {
                    "applicable": not bool(input_identity["native"]),
                    "approval_required": approval_required,
                    "approval_exact": bool(approval_plan),
                    "unlimited_approval": False,
                    "token": {
                        "symbol": input_identity["symbol"],
                        "registry_id": input_identity["registry_id"],
                        "contract_address": input_identity["contract_address"],
                    },
                    "spender": (approval_plan or {}).get("spender"),
                    "required_amount_atomic": input_atomic,
                    "approval_transaction_plan": approval_plan,
                    "request_id": approval_request_id,
                },
                "unsigned_transaction_plan": {
                    **unsigned,
                    "native_input": bool(input_identity["native"]),
                    "input_asset": input_identity["symbol"],
                    "output_asset": output_identity["symbol"],
                    "input_amount": quote_result.get("input_amount"),
                    "input_amount_atomic": input_atomic,
                    "minimum_received": quote_result.get("minimum_received"),
                    "minimum_received_atomic": quote_result.get("minimum_received_atomic"),
                    "provider_request_id": str(swap_body.get("requestId") or "").strip() or None,
                    "gas_fee_wei": str(swap_body.get("gasFee") or "").strip() or None,
                    "provider_bound": True,
                    "provider_simulation_requested": swap_simulation_requested,
                    "gas_limit_estimated": unsigned.get("gas_limit") is not None,
                    "requires_refresh_after_approval": approval_required,
                    "wallet_connection_requested": False,
                    "signing_enabled": False,
                    "broadcast_enabled": False,
                },
                "quote_id": str(swap_body.get("requestId") or raw_response.get("requestId") or "").strip() or None,
                "read_only": True,
                "quote_only": False,
                "transaction_constructed": True,
                "transaction_calldata_returned": True,
                "swap_simulation_requested": swap_simulation_requested,
                "swap_simulation_deferred_until_approval": approval_required,
                "requires_refresh_after_approval": approval_required,
                "database_mutation": False,
                "will_mutate": False,
                "execution_authority": False,
                "execution_enabled": False,
                "wallet_connection_requested": False,
                "signing_enabled": False,
                "broadcast_enabled": False,
                "automatic_wallet_request": False,
                "automatic_retry": False,
                "automatic_second_transaction": False,
                "raw_provider_response_returned": False,
                "transaction": None,
                "transaction_calldata": None,
            }
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
            return _failure(
                "uniswap_firm_plan_provider_transient_error",
                symbol=symbol,
                provider_contacted=True,
                provider_error=type(exc).__name__,
            )
        except ValueError as exc:
            return _failure(
                str(exc),
                symbol=symbol,
                provider_contacted=True,
            )
        except Exception as exc:
            return _failure(
                "uniswap_firm_plan_provider_error",
                symbol=symbol,
                provider_contacted=True,
                provider_error=type(exc).__name__,
            )


_SERVICE: Optional[RobinhoodChainUniswapQuoteService] = None


def get_robinhood_chain_uniswap_quote_service() -> RobinhoodChainUniswapQuoteService:
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = RobinhoodChainUniswapQuoteService(
            api_base=settings.robinhood_chain_effective_uniswap_api_base(),
            timeout_s=float(settings.robinhood_chain_uniswap_quote_timeout_s),
            max_concurrent=int(settings.robinhood_chain_uniswap_quote_max_concurrent),
            credential_getter=settings.robinhood_chain_uniswap_api_credential,
        )
    return _SERVICE
