from __future__ import annotations

import asyncio
import copy
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ..config import settings
from ..db import get_db
from ..models import TokenRegistry, WalletAddress, WalletAddressSnapshot
from ..services.evm_rpc import get_robinhood_chain_client, validate_evm_address
from ..services.robinhood_chain_accounting_preview import build_robinhood_chain_accounting_preview
from ..services.robinhood_chain_history import (
    get_robinhood_chain_history_service,
    validate_transaction_hash,
)
from ..services.robinhood_chain_execution_discovery import (
    ROBINHOOD_CHAIN_DISCOVERY_TOKENS,
    ROBINHOOD_CHAIN_ROUTE_CAPABILITIES,
    get_robinhood_chain_execution_discovery_service,
)
from ..services.robinhood_chain_quotes import (
    ROBINHOOD_CHAIN_QUOTE_PROVIDER,
    ROBINHOOD_CHAIN_QUOTE_SYMBOL,
    get_robinhood_chain_quote_service,
)
from ..services.robinhood_chain_transaction_planning import (
    ROBINHOOD_CHAIN_DEFAULT_SLIPPAGE_BPS,
    ROBINHOOD_CHAIN_FIRM_QUOTE_SYMBOL,
    ROBINHOOD_CHAIN_MAX_SLIPPAGE_BPS,
    ROBINHOOD_CHAIN_MIN_SLIPPAGE_BPS,
    get_robinhood_chain_transaction_planning_service,
)
from ..services.robinhood_chain_execution import (
    ROBINHOOD_CHAIN_EXECUTION_INPUT_ETH,
    ROBINHOOD_CHAIN_EXECUTION_SIDE,
    ROBINHOOD_CHAIN_EXECUTION_SYMBOL,
    ROBINHOOD_CHAIN_SUBMISSION_FAILURE_REASONS,
    get_robinhood_chain_execution_service,
    validate_execution_saved_wallet,
)
from ..services.robinhood_chain_buy_execution import (
    ROBINHOOD_CHAIN_BUY_APPROVAL_USDG,
    ROBINHOOD_CHAIN_BUY_EXACT_OUTPUT_ETH,
    ROBINHOOD_CHAIN_BUY_MAXIMUM_USDG,
    ROBINHOOD_CHAIN_BUY_SIDE,
    ROBINHOOD_CHAIN_BUY_SLIPPAGE_BPS,
    ROBINHOOD_CHAIN_BUY_SUBMISSION_FAILURE_REASONS,
    ROBINHOOD_CHAIN_BUY_SYMBOL,
    get_robinhood_chain_buy_execution_service,
)


router = APIRouter(prefix="/api/robinhood_chain", tags=["robinhood_chain"])

_EXPECTED_CHAIN_ID_DECIMAL = 4663
_EXPECTED_CHAIN_ID_HEX = hex(_EXPECTED_CHAIN_ID_DECIMAL)
_NATIVE_CURRENCY = "ETH"
_EXPLORER_URL = "https://robinhoodchain.blockscout.com"
_TOKEN_REGISTRY_CHAIN = "robinhood_chain"
_TOKEN_REGISTRY_VENUE = "robinhood_chain"


def _normalize_registry_symbol(symbol: str) -> str:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        raise HTTPException(status_code=400, detail="token symbol is required")
    if len(normalized) > 32:
        raise HTTPException(status_code=400, detail="token symbol exceeds Token Registry capacity")
    return normalized


def _resolve_registered_erc20(db: Session, symbol: str) -> Tuple[TokenRegistry, str, int]:
    normalized_symbol = _normalize_registry_symbol(symbol)
    if normalized_symbol == _NATIVE_CURRENCY:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "native_asset_not_erc20",
                "message": "ETH is the native Robinhood Chain asset; use the native balance endpoint.",
                "symbol": normalized_symbol,
            },
        )

    override = (
        db.query(TokenRegistry)
        .filter(
            TokenRegistry.chain == _TOKEN_REGISTRY_CHAIN,
            TokenRegistry.venue == _TOKEN_REGISTRY_VENUE,
            TokenRegistry.symbol == normalized_symbol,
        )
        .first()
    )
    global_row = (
        db.query(TokenRegistry)
        .filter(
            TokenRegistry.chain == _TOKEN_REGISTRY_CHAIN,
            ((TokenRegistry.venue.is_(None)) | (TokenRegistry.venue == "")),
            TokenRegistry.symbol == normalized_symbol,
        )
        .first()
    )
    row = override or global_row
    if row is None:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "registered_erc20_not_found",
                "chain": _TOKEN_REGISTRY_CHAIN,
                "symbol": normalized_symbol,
            },
        )

    try:
        contract = validate_evm_address(str(row.address or "").strip())
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "invalid_registered_erc20_contract",
                "chain": _TOKEN_REGISTRY_CHAIN,
                "symbol": normalized_symbol,
                "registry_id": row.id,
                "message": str(exc),
            },
        ) from exc

    try:
        decimals = int(row.decimals)
    except Exception as exc:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "invalid_registered_erc20_decimals",
                "chain": _TOKEN_REGISTRY_CHAIN,
                "symbol": normalized_symbol,
                "registry_id": row.id,
            },
        ) from exc
    if decimals < 0 or decimals > 18:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "invalid_registered_erc20_decimals",
                "chain": _TOKEN_REGISTRY_CHAIN,
                "symbol": normalized_symbol,
                "registry_id": row.id,
                "decimals": decimals,
            },
        )

    return row, contract, decimals


def _registered_history_token_map(db: Session) -> Dict[str, Dict[str, Any]]:
    """Return a bounded contract-keyed Token Registry view for history labels.

    Venue-specific Robinhood Chain rows win over global rows. Malformed rows are
    skipped rather than weakening the history endpoint's strict address rules.
    """
    overrides = (
        db.query(TokenRegistry)
        .filter(
            TokenRegistry.chain == _TOKEN_REGISTRY_CHAIN,
            TokenRegistry.venue == _TOKEN_REGISTRY_VENUE,
            TokenRegistry.symbol != _NATIVE_CURRENCY,
        )
        .order_by(TokenRegistry.symbol.asc())
        .limit(100)
        .all()
    )
    global_rows = (
        db.query(TokenRegistry)
        .filter(
            TokenRegistry.chain == _TOKEN_REGISTRY_CHAIN,
            ((TokenRegistry.venue.is_(None)) | (TokenRegistry.venue == "")),
            TokenRegistry.symbol != _NATIVE_CURRENCY,
        )
        .order_by(TokenRegistry.symbol.asc())
        .limit(100)
        .all()
    )

    selected: Dict[str, Dict[str, Any]] = {}
    for row in [*(overrides or []), *(global_rows or [])]:
        try:
            contract = validate_evm_address(str(row.address or "").strip())
            contract_key = contract.lower()
            if contract_key in selected:
                continue
            decimals = int(row.decimals)
            if decimals < 0 or decimals > 18:
                continue
            symbol = str(row.symbol or "").strip().upper()
            if not symbol or symbol == _NATIVE_CURRENCY:
                continue
            selected[contract_key] = {
                "registry_id": int(row.id),
                "registry_venue": row.venue,
                "symbol": symbol,
                "decimals": decimals,
                "label": row.label,
                "contract_address": contract,
            }
        except Exception:
            continue
    return selected


def _resolve_execution_discovery_token(db: Session, symbol: str) -> Dict[str, Any]:
    normalized = _normalize_registry_symbol(symbol)
    candidate = ROBINHOOD_CHAIN_DISCOVERY_TOKENS.get(normalized)
    if candidate is None:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "execution_discovery_symbol_not_allowlisted",
                "symbol": normalized,
                "allowed": sorted(ROBINHOOD_CHAIN_DISCOVERY_TOKENS.keys()),
            },
        )

    override = (
        db.query(TokenRegistry)
        .filter(
            TokenRegistry.chain == _TOKEN_REGISTRY_CHAIN,
            TokenRegistry.venue == _TOKEN_REGISTRY_VENUE,
            TokenRegistry.symbol == normalized,
        )
        .first()
    )
    global_row = (
        db.query(TokenRegistry)
        .filter(
            TokenRegistry.chain == _TOKEN_REGISTRY_CHAIN,
            ((TokenRegistry.venue.is_(None)) | (TokenRegistry.venue == "")),
            TokenRegistry.symbol == normalized,
        )
        .first()
    )
    row = override or global_row

    identity = dict(candidate)
    identity["symbol"] = normalized
    identity["registry_id"] = int(row.id) if row is not None else None
    identity["registry_venue"] = row.venue if row is not None else None
    identity["registry_status"] = "registered" if row is not None else "official_candidate"

    if normalized == _NATIVE_CURRENCY:
        if row is not None:
            if str(row.address or "").strip():
                raise HTTPException(
                    status_code=422,
                    detail={"error": "execution_discovery_native_registry_mismatch", "symbol": normalized},
                )
            if int(row.decimals) != int(candidate["decimals"]):
                raise HTTPException(
                    status_code=422,
                    detail={"error": "execution_discovery_decimals_mismatch", "symbol": normalized},
                )
        return identity

    expected_contract = validate_evm_address(str(candidate["contract_address"]))
    identity["contract_address"] = expected_contract
    if row is not None:
        try:
            registered_contract = validate_evm_address(str(row.address or "").strip())
            registered_decimals = int(row.decimals)
        except Exception as exc:
            raise HTTPException(
                status_code=422,
                detail={"error": "invalid_execution_discovery_registry_identity", "symbol": normalized},
            ) from exc
        if registered_contract.lower() != expected_contract.lower():
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "execution_discovery_contract_mismatch",
                    "symbol": normalized,
                    "registry_id": int(row.id),
                    "expected_contract": expected_contract,
                    "registered_contract": registered_contract,
                },
            )
        if registered_decimals != int(candidate["decimals"]):
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "execution_discovery_decimals_mismatch",
                    "symbol": normalized,
                    "registry_id": int(row.id),
                    "expected_decimals": int(candidate["decimals"]),
                    "registered_decimals": registered_decimals,
                },
            )
    return identity


# Callers select stable check names, never arbitrary JSON-RPC methods or params.
_PROBE_DEFINITIONS: Dict[str, Tuple[str, List[Any]]] = {
    "chain_id": ("eth_chainId", []),
    "net_version": ("net_version", []),
    "client_version": ("web3_clientVersion", []),
    "block_number": ("eth_blockNumber", []),
    "latest_block": ("eth_getBlockByNumber", ["latest", False]),
    "gas_price": ("eth_gasPrice", []),
    "max_priority_fee_per_gas": ("eth_maxPriorityFeePerGas", []),
    "fee_history": ("eth_feeHistory", ["0x5", "latest", [25, 50, 75]]),
}
_DEFAULT_PROBE_CHECKS = list(_PROBE_DEFINITIONS.keys())


class RobinhoodChainProbeRequest(BaseModel):
    checks: Optional[List[str]] = Field(
        default=None,
        description="Optional fixed check names; arbitrary RPC methods are not accepted.",
    )
    force_refresh: bool = False


class RobinhoodChainAccountingPreviewRequest(BaseModel):
    wallet_address_id: Optional[str] = Field(
        default=None,
        max_length=36,
        description="Optional exact Wallet Addresses row id for source-scope verification.",
    )
    force_refresh: bool = Field(
        default=False,
        description="Bypass the bounded transaction-detail cache for this explicit preview request.",
    )


class RobinhoodChainExecutionDiscoveryRequest(BaseModel):
    provider: str = Field(
        default="0x",
        min_length=1,
        max_length=16,
        description="Fixed discovery provider identifier; only 0x is accepted in RH-CHAIN.10A.",
    )
    sell_symbol: str = Field(min_length=1, max_length=32)
    buy_symbol: str = Field(min_length=1, max_length=32)
    sell_amount: Optional[str] = Field(
        default=None,
        max_length=80,
        description="Exact-input amount in display units. Mutually exclusive with buy_amount.",
    )
    buy_amount: Optional[str] = Field(
        default=None,
        max_length=80,
        description="Exact-output amount in display units. Mutually exclusive with sell_amount.",
    )
    taker_address: str = Field(
        min_length=42,
        max_length=42,
        description="Saved Robinhood Chain public address used only for provider diagnostics.",
    )
    force_refresh: bool = False


class RobinhoodChainIndicativeQuoteRequest(BaseModel):
    provider: str = Field(
        default="0x",
        min_length=1,
        max_length=16,
        description="Fixed quote provider identifier; only 0x is accepted in RH-CHAIN.10B.",
    )
    symbol: str = Field(
        default=ROBINHOOD_CHAIN_QUOTE_SYMBOL,
        min_length=1,
        max_length=32,
        description="Canonical quote-only market symbol. RH-CHAIN.10D.0 supports ETH-USDG only.",
    )
    side: str = Field(min_length=3, max_length=4, description="buy or sell")
    quantity: Optional[str] = Field(
        default=None,
        max_length=80,
        description="Exact ETH input for sell quotes.",
    )
    total_quote: Optional[str] = Field(
        default=None,
        max_length=80,
        description="Exact USDG input for buy quotes.",
    )
    taker_address: Optional[str] = Field(
        default=None,
        min_length=42,
        max_length=42,
        description="Optional public address override. When omitted, UTT uses the saved ALL / robinhood_chain wallet.",
    )
    force_refresh: bool = False


class RobinhoodChainFirmQuotePlanRequest(BaseModel):
    provider: str = Field(
        default="0x",
        min_length=1,
        max_length=16,
        description="Fixed provider identifier; only 0x AllowanceHolder is accepted in RH-CHAIN.10C.",
    )
    symbol: str = Field(
        default=ROBINHOOD_CHAIN_FIRM_QUOTE_SYMBOL,
        min_length=1,
        max_length=32,
        description="Canonical market symbol. RH-CHAIN.10D.0 supports ETH-USDG only.",
    )
    side: str = Field(min_length=3, max_length=4, description="buy or sell")
    quantity: Optional[str] = Field(
        default=None,
        max_length=80,
        description="Exact ETH input for sell plans.",
    )
    total_quote: Optional[str] = Field(
        default=None,
        max_length=80,
        description="Exact USDG input for exact-input buy plans.",
    )
    exact_output_quantity: Optional[str] = Field(
        default=None,
        max_length=80,
        description="Exact ETH output for the RH-CHAIN.10D.2 reverse BUY.",
    )
    maximum_total_quote: Optional[str] = Field(
        default=None,
        max_length=80,
        description="Strict maximum USDG spend for an exact-output BUY.",
    )
    slippage_bps: int = Field(
        default=ROBINHOOD_CHAIN_DEFAULT_SLIPPAGE_BPS,
        ge=ROBINHOOD_CHAIN_MIN_SLIPPAGE_BPS,
        le=ROBINHOOD_CHAIN_MAX_SLIPPAGE_BPS,
        description="Bounded exact-input slippage protection in basis points.",
    )
    taker_address: Optional[str] = Field(
        default=None,
        min_length=42,
        max_length=42,
        description="Optional public address override. When omitted, UTT uses the saved ALL / robinhood_chain wallet.",
    )


class RobinhoodChainExecutionPrepareRequest(BaseModel):
    symbol: str = Field(default=ROBINHOOD_CHAIN_EXECUTION_SYMBOL, min_length=1, max_length=32)
    side: str = Field(default=ROBINHOOD_CHAIN_EXECUTION_SIDE, min_length=3, max_length=4)
    quantity: str = Field(default=str(ROBINHOOD_CHAIN_EXECUTION_INPUT_ETH), min_length=1, max_length=80)
    slippage_bps: int = Field(
        default=ROBINHOOD_CHAIN_DEFAULT_SLIPPAGE_BPS,
        ge=ROBINHOOD_CHAIN_MIN_SLIPPAGE_BPS,
        le=ROBINHOOD_CHAIN_MAX_SLIPPAGE_BPS,
    )
    taker_address: Optional[str] = Field(default=None, min_length=42, max_length=42)
    confirm_prepare: bool = Field(
        default=False,
        description="Must be true to create the dedicated prepared execution lifecycle row.",
    )


class RobinhoodChainExecutionSendClaimRequest(BaseModel):
    wallet_address: str = Field(min_length=42, max_length=42)
    plan_hash: str = Field(min_length=64, max_length=64)
    claim_id: str = Field(min_length=64, max_length=64)
    confirm_send_claim: bool = Field(
        default=False,
        description="Must be true to atomically reserve this prepared plan for one wallet send attempt.",
    )


class RobinhoodChainExecutionSubmissionRequest(BaseModel):
    tx_hash: str = Field(min_length=66, max_length=66)
    wallet_address: str = Field(min_length=42, max_length=42)
    claim_id: str = Field(min_length=64, max_length=64)
    confirm_record: bool = Field(
        default=False,
        description="Must be true to record the MetaMask-returned transaction hash.",
    )


class RobinhoodChainExecutionSubmissionFailureRequest(BaseModel):
    wallet_address: str = Field(min_length=42, max_length=42)
    claim_id: str = Field(min_length=64, max_length=64)
    reason: str = Field(min_length=1, max_length=64)
    message: Optional[str] = Field(default=None, max_length=512)
    confirm_failure: bool = Field(
        default=False,
        description="Must be true to terminate a claimed send after MetaMask returned no transaction hash.",
    )


class RobinhoodChainBuyApprovalPrepareRequest(BaseModel):
    symbol: str = Field(default=ROBINHOOD_CHAIN_BUY_SYMBOL, min_length=1, max_length=32)
    side: str = Field(default=ROBINHOOD_CHAIN_BUY_SIDE, min_length=3, max_length=4)
    exact_output_quantity: str = Field(default=str(ROBINHOOD_CHAIN_BUY_EXACT_OUTPUT_ETH), min_length=1, max_length=80)
    maximum_total_quote: str = Field(default=str(ROBINHOOD_CHAIN_BUY_MAXIMUM_USDG), min_length=1, max_length=80)
    approval_amount: str = Field(default=str(ROBINHOOD_CHAIN_BUY_APPROVAL_USDG), min_length=1, max_length=80)
    slippage_bps: int = Field(default=ROBINHOOD_CHAIN_BUY_SLIPPAGE_BPS, ge=ROBINHOOD_CHAIN_BUY_SLIPPAGE_BPS, le=ROBINHOOD_CHAIN_BUY_SLIPPAGE_BPS)
    taker_address: Optional[str] = Field(default=None, min_length=42, max_length=42)
    confirm_prepare: bool = False


class RobinhoodChainBuySwapPrepareRequest(BaseModel):
    wallet_address: str = Field(min_length=42, max_length=42)
    confirm_prepare: bool = False


def _resolve_robinhood_chain_quote_taker(
    db: Session,
    requested_address: Optional[str] = None,
) -> str:
    requested = str(requested_address or "").strip()
    if requested:
        try:
            return validate_evm_address(requested)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    row = (
        db.query(WalletAddress)
        .filter(
            WalletAddress.network == _TOKEN_REGISTRY_CHAIN,
            WalletAddress.wallet_id == _TOKEN_REGISTRY_VENUE,
            WalletAddress.asset.in_(["ALL", "*"]),
        )
        .order_by(WalletAddress.created_at.desc())
        .first()
    )
    if row is None:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "robinhood_chain_quote_wallet_required",
                "message": "Save an ALL / robinhood_chain Wallet Addresses row before requesting quote-only market data.",
                "read_only": True,
                "will_mutate": False,
            },
        )
    try:
        return validate_evm_address(str(row.address or "").strip())
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "invalid_saved_robinhood_chain_quote_wallet",
                "wallet_address_id": row.id,
                "message": str(exc),
                "read_only": True,
                "will_mutate": False,
            },
        ) from exc


def _resolve_robinhood_chain_execution_taker(
    db: Session,
    requested_address: Optional[str] = None,
) -> str:
    """Resolve the saved RH Chain wallet and reject any execution override.

    Quote-only endpoints retain their optional public-address override. The live
    execution lifecycle does not: the connected MetaMask account must match the
    saved ALL / robinhood_chain wallet on the backend as well as in the UI.
    """
    saved = _resolve_robinhood_chain_quote_taker(db, None)
    try:
        return validate_execution_saved_wallet(saved, requested_address)
    except ValueError as exc:
        error = str(exc)
        status_code = 409 if error == "robinhood_chain_execution_saved_wallet_mismatch" else 400
        raise HTTPException(status_code=status_code, detail={"error": error}) from exc


async def _refresh_robinhood_chain_execution_balance_snapshots(
    db: Session,
    wallet_address: str,
) -> Dict[str, Any]:
    """Force-refresh the saved RH Chain account snapshots after confirmation.

    This is intentionally limited to native ETH and canonical registered USDG.
    A snapshot failure never changes the already-verified execution lifecycle.
    """
    try:
        normalized_address = validate_evm_address(wallet_address)
    except ValueError as exc:
        return {"ok": False, "refreshed": 0, "errors": [{"asset": "ALL", "error": str(exc)}]}

    saved_row = (
        db.query(WalletAddress)
        .filter(
            WalletAddress.network == _TOKEN_REGISTRY_CHAIN,
            WalletAddress.wallet_id == _TOKEN_REGISTRY_VENUE,
            WalletAddress.asset.in_(["ALL", "*"]),
        )
        .order_by(WalletAddress.created_at.desc())
        .first()
    )
    if saved_row is None:
        return {
            "ok": False,
            "refreshed": 0,
            "errors": [{"asset": "ALL", "error": "saved_robinhood_chain_wallet_not_found"}],
        }
    try:
        saved_address = validate_evm_address(str(saved_row.address or "").strip())
    except ValueError as exc:
        return {"ok": False, "refreshed": 0, "errors": [{"asset": "ALL", "error": str(exc)}]}
    if saved_address.lower() != normalized_address.lower():
        return {
            "ok": False,
            "refreshed": 0,
            "errors": [{"asset": "ALL", "error": "saved_robinhood_chain_wallet_mismatch"}],
        }

    client = get_robinhood_chain_client()
    refreshed = 0
    errors: List[Dict[str, Any]] = []
    items: List[Dict[str, Any]] = []

    try:
        eth_result = await client.get_native_balance(
            normalized_address,
            block_tag="latest",
            force_refresh=True,
        )
        if not eth_result.get("ok"):
            raise RuntimeError(str(eth_result.get("error") or eth_result))
        balance_eth = Decimal(str(eth_result.get("balance_eth") or "0"))
        snapshot = WalletAddressSnapshot(
            wallet_address_id=saved_row.id,
            asset="ETH",
            network=_TOKEN_REGISTRY_CHAIN,
            address=normalized_address,
            balance_qty=float(balance_eth),
            balance_raw={
                "read_only": True,
                "post_execution_refresh": True,
                "chain_id": _EXPECTED_CHAIN_ID_DECIMAL,
                "chain_id_hex": _EXPECTED_CHAIN_ID_HEX,
                "block_tag": eth_result.get("block_tag") or "latest",
                "balance_wei": str(eth_result.get("balance_wei") or "0"),
                "balance_eth": str(eth_result.get("balance_eth") or "0"),
                "cached": bool(eth_result.get("cached")),
                "fetched_at": eth_result.get("fetched_at"),
                "source": "robinhood_chain_rpc",
            },
            source="robinhood_chain_rpc",
            fetched_at=datetime.utcnow(),
        )
        db.add(snapshot)
        db.commit()
        refreshed += 1
        items.append({
            "asset": "ETH",
            "balance": str(eth_result.get("balance_eth") or "0"),
            "balance_atomic": str(eth_result.get("balance_wei") or "0"),
            "cached": bool(eth_result.get("cached")),
            "fetched_at": eth_result.get("fetched_at"),
        })
    except Exception as exc:
        db.rollback()
        errors.append({"asset": "ETH", "error": str(exc)})

    try:
        token_row, contract_address, token_decimals = _resolve_registered_erc20(db, "USDG")
        token_result = await client.get_erc20_balance(
            normalized_address,
            contract_address,
            token_decimals,
            block_tag="latest",
            force_refresh=True,
        )
        if not token_result.get("ok"):
            raise RuntimeError(str(token_result.get("error") or token_result))
        balance_token = Decimal(str(token_result.get("balance_token") or "0"))
        snapshot = WalletAddressSnapshot(
            wallet_address_id=saved_row.id,
            asset="USDG",
            network=_TOKEN_REGISTRY_CHAIN,
            address=normalized_address,
            balance_qty=float(balance_token),
            balance_raw={
                "read_only": True,
                "post_execution_refresh": True,
                "chain_id": _EXPECTED_CHAIN_ID_DECIMAL,
                "chain_id_hex": _EXPECTED_CHAIN_ID_HEX,
                "block_tag": token_result.get("block_tag") or "latest",
                "contract_address": contract_address,
                "decimals": token_decimals,
                "balance_atomic": str(token_result.get("balance_atomic") or "0"),
                "balance_token": str(token_result.get("balance_token") or "0"),
                "registry_id": int(token_row.id),
                "registry_venue": token_row.venue,
                "registry_label": token_row.label,
                "cached": bool(token_result.get("cached")),
                "fetched_at": token_result.get("fetched_at"),
                "source": "robinhood_chain_erc20_rpc",
            },
            source="robinhood_chain_erc20_rpc",
            fetched_at=datetime.utcnow(),
        )
        db.add(snapshot)
        db.commit()
        refreshed += 1
        items.append({
            "asset": "USDG",
            "balance": str(token_result.get("balance_token") or "0"),
            "balance_atomic": str(token_result.get("balance_atomic") or "0"),
            "contract_address": contract_address,
            "cached": bool(token_result.get("cached")),
            "fetched_at": token_result.get("fetched_at"),
        })
    except Exception as exc:
        db.rollback()
        errors.append({"asset": "USDG", "error": str(exc)})

    return {
        "ok": len(errors) == 0,
        "refreshed": refreshed,
        "items": items,
        "errors": errors,
        "force_refresh": True,
        "wallet_address_id": saved_row.id,
        "wallet_address": normalized_address,
    }


def _quote_failure_status(result: Dict[str, Any]) -> int:
    error = str(result.get("error") or "robinhood_chain_quote_failed")
    if error in {
        "unsupported_robinhood_chain_quote_symbol",
        "invalid_quote_side",
        "invalid_quantity",
        "invalid_quote_amount",
        "invalid_discovery_amount",
        "discovery_amount_exceeds_cap",
        "unsupported_discovery_pair",
        "invalid_firm_quote_amount",
        "firm_quote_amount_exceeds_cap",
        "invalid_slippage_bps",
    }:
        return 400
    if error in {
        "execution_discovery_route_mode_not_live_verified",
        "robinhood_chain_exact_receive_route_unavailable",
        "firm_quote_route_mode_not_live_verified",
    }:
        return 409
    if error in {
        "execution_discovery_not_configured",
        "execution_discovery_backoff_active",
        "chain_id_mismatch_or_unavailable",
        "firm_quote_planning_not_configured",
        "firm_quote_provider_transient_error",
    }:
        return 503
    return 502


_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_CACHE_LOCK = asyncio.Lock()
_RPC_SEMAPHORE = asyncio.Semaphore(max(1, int(settings.robinhood_chain_max_concurrent)))

_LAST_GOOD_AT: Optional[datetime] = None
_LAST_ERROR: Optional[str] = None
_LAST_OBSERVED_CHAIN_ID: Optional[str] = None
_BACKOFF_UNTIL_MONOTONIC = 0.0
_BACKOFF_UNTIL_UTC: Optional[datetime] = None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_or_none(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value is not None else None


def _configured_rpc_http() -> str:
    return settings.robinhood_chain_effective_rpc_http()


def _status_payload() -> Dict[str, Any]:
    configured_chain_id = int(settings.robinhood_chain_chain_id)
    configured_match = configured_chain_id == _EXPECTED_CHAIN_ID_DECIMAL
    observed = str(_LAST_OBSERVED_CHAIN_ID or "").strip().lower() or None
    observed_match = observed == _EXPECTED_CHAIN_ID_HEX if observed is not None else None

    return {
        "venue": "robinhood_chain",
        "network": "mainnet",
        "native_currency": _NATIVE_CURRENCY,
        "explorer_url": _EXPLORER_URL,
        "read_only": True,
        "enabled": bool(settings.robinhood_chain_enabled),
        "configured": bool(_configured_rpc_http()),
        "effective_enabled": bool(settings.robinhood_chain_effective_enabled()),
        "chain_id": configured_chain_id,
        "expected_chain_id": _EXPECTED_CHAIN_ID_DECIMAL,
        "expected_chain_id_hex": _EXPECTED_CHAIN_ID_HEX,
        "configured_chain_id_matches": configured_match,
        "rpc_chain_id": observed,
        "chain_id_matches": observed_match if observed_match is not None else configured_match,
        "rpc_http_configured": bool(_configured_rpc_http()),
        "rpc_ws_configured": bool(settings.robinhood_chain_effective_rpc_ws()),
        "timeout_s": float(settings.robinhood_chain_timeout_s),
        "cache_ttl_s": float(settings.robinhood_chain_cache_ttl_s),
        "error_backoff_s": float(settings.robinhood_chain_error_backoff_s),
        "max_concurrent": int(settings.robinhood_chain_max_concurrent),
        "last_good_at": _iso_or_none(_LAST_GOOD_AT),
        "last_error": _LAST_ERROR,
        "backoff_until": _iso_or_none(_BACKOFF_UNTIL_UTC),
        "allowed_probe_checks": list(_DEFAULT_PROBE_CHECKS),
    }


def _set_transient_backoff(message: str) -> None:
    global _LAST_ERROR, _BACKOFF_UNTIL_MONOTONIC, _BACKOFF_UNTIL_UTC
    seconds = max(0.0, float(settings.robinhood_chain_error_backoff_s))
    _LAST_ERROR = str(message or "Robinhood Chain RPC transient error")
    _BACKOFF_UNTIL_MONOTONIC = time.monotonic() + seconds
    _BACKOFF_UNTIL_UTC = _utc_now() + timedelta(seconds=seconds) if seconds > 0 else None


def _clear_backoff_after_success() -> None:
    global _LAST_GOOD_AT, _LAST_ERROR, _BACKOFF_UNTIL_MONOTONIC, _BACKOFF_UNTIL_UTC
    _LAST_GOOD_AT = _utc_now()
    _LAST_ERROR = None
    _BACKOFF_UNTIL_MONOTONIC = 0.0
    _BACKOFF_UNTIL_UTC = None


def _normalize_checks(checks: Optional[List[str]]) -> List[str]:
    requested = checks if checks is not None else _DEFAULT_PROBE_CHECKS
    out: List[str] = []
    seen = set()
    unknown: List[str] = []

    for raw in requested:
        key = str(raw or "").strip().lower()
        if not key:
            continue
        if key not in _PROBE_DEFINITIONS:
            unknown.append(key)
            continue
        if key not in seen:
            seen.add(key)
            out.append(key)

    if unknown:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "unsupported_probe_check",
                "unknown": unknown,
                "allowed": list(_DEFAULT_PROBE_CHECKS),
            },
        )

    if not out:
        out = list(_DEFAULT_PROBE_CHECKS)

    # Identity is always checked first before any other read.
    if "chain_id" in out:
        out.remove("chain_id")
    return ["chain_id", *out]


async def _cached_result(check: str) -> Optional[Dict[str, Any]]:
    ttl = max(0.0, float(settings.robinhood_chain_cache_ttl_s))
    if ttl <= 0:
        return None
    now = time.monotonic()
    async with _CACHE_LOCK:
        item = _CACHE.get(check)
        if item is None:
            return None
        expires_at, result = item
        if expires_at <= now:
            _CACHE.pop(check, None)
            return None
        cached = copy.deepcopy(result)
        cached["cached"] = True
        return cached


async def _store_cache(check: str, result: Dict[str, Any]) -> None:
    ttl = max(0.0, float(settings.robinhood_chain_cache_ttl_s))
    if ttl <= 0:
        return
    async with _CACHE_LOCK:
        _CACHE[check] = (time.monotonic() + ttl, copy.deepcopy(result))


async def _rpc_check(check: str, *, force_refresh: bool) -> Dict[str, Any]:
    global _LAST_ERROR, _LAST_OBSERVED_CHAIN_ID

    if not force_refresh:
        cached = await _cached_result(check)
        if cached is not None:
            return cached

    now = time.monotonic()
    if _BACKOFF_UNTIL_MONOTONIC > now:
        return {
            "ok": False,
            "check": check,
            "method": _PROBE_DEFINITIONS[check][0],
            "cached": False,
            "error": "rpc_backoff_active",
            "backoff_until": _iso_or_none(_BACKOFF_UNTIL_UTC),
        }

    rpc = _configured_rpc_http()
    if not rpc:
        return {
            "ok": False,
            "check": check,
            "method": _PROBE_DEFINITIONS[check][0],
            "cached": False,
            "error": "ROBINHOOD_CHAIN_RPC_HTTP is not configured",
        }

    method, params = _PROBE_DEFINITIONS[check]
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    started = time.perf_counter()

    async with _RPC_SEMAPHORE:
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(float(settings.robinhood_chain_timeout_s)),
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "User-Agent": "UTT-Robinhood-Chain-ReadOnly/1.0",
                },
            ) as client:
                response = await client.post(rpc, json=payload)

            elapsed_ms = round((time.perf_counter() - started) * 1000.0, 1)
            retry_after = response.headers.get("Retry-After")

            try:
                body = response.json()
            except Exception:
                body = {"non_json_body": response.text[:1000]}

            if response.status_code == 429 or response.status_code >= 500:
                message = f"HTTP {response.status_code} from Robinhood Chain RPC"
                _set_transient_backoff(message)
                return {
                    "ok": False,
                    "check": check,
                    "method": method,
                    "cached": False,
                    "http_status": response.status_code,
                    "elapsed_ms": elapsed_ms,
                    "retry_after": retry_after,
                    "error": body,
                }

            if not response.is_success:
                _LAST_ERROR = f"HTTP {response.status_code} from Robinhood Chain RPC"
                return {
                    "ok": False,
                    "check": check,
                    "method": method,
                    "cached": False,
                    "http_status": response.status_code,
                    "elapsed_ms": elapsed_ms,
                    "error": body,
                }

            if isinstance(body, dict) and body.get("error") is not None:
                _LAST_ERROR = str(body.get("error"))
                return {
                    "ok": False,
                    "check": check,
                    "method": method,
                    "cached": False,
                    "http_status": response.status_code,
                    "elapsed_ms": elapsed_ms,
                    "error": body.get("error"),
                }

            result = body.get("result") if isinstance(body, dict) else body
            record = {
                "ok": True,
                "check": check,
                "method": method,
                "cached": False,
                "http_status": response.status_code,
                "elapsed_ms": elapsed_ms,
                "result": result,
            }

            if check == "chain_id":
                _LAST_OBSERVED_CHAIN_ID = str(result or "").strip().lower() or None

            _clear_backoff_after_success()
            await _store_cache(check, record)
            return record

        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError) as exc:
            elapsed_ms = round((time.perf_counter() - started) * 1000.0, 1)
            message = f"{type(exc).__name__}: {exc}"
            _set_transient_backoff(message)
            return {
                "ok": False,
                "check": check,
                "method": method,
                "cached": False,
                "http_status": None,
                "elapsed_ms": elapsed_ms,
                "error": message,
            }
        except Exception as exc:
            elapsed_ms = round((time.perf_counter() - started) * 1000.0, 1)
            message = f"{type(exc).__name__}: {exc}"
            _LAST_ERROR = message
            return {
                "ok": False,
                "check": check,
                "method": method,
                "cached": False,
                "http_status": None,
                "elapsed_ms": elapsed_ms,
                "error": message,
            }


@router.get("/status")
def robinhood_chain_status() -> Dict[str, Any]:
    return _status_payload()


@router.post("/rpc_probe")
async def robinhood_chain_rpc_probe(
    payload: Optional[RobinhoodChainProbeRequest] = None,
) -> Dict[str, Any]:
    if not bool(settings.robinhood_chain_enabled):
        raise HTTPException(status_code=503, detail="Robinhood Chain is disabled")
    if not _configured_rpc_http():
        raise HTTPException(status_code=503, detail="ROBINHOOD_CHAIN_RPC_HTTP is not configured")

    request = payload or RobinhoodChainProbeRequest()
    checks = _normalize_checks(request.checks)
    results: Dict[str, Dict[str, Any]] = {}

    chain_record = await _rpc_check("chain_id", force_refresh=bool(request.force_refresh))
    results["chain_id"] = chain_record

    actual_chain_id = str(chain_record.get("result") or "").strip().lower()
    chain_matches = bool(chain_record.get("ok")) and actual_chain_id == _EXPECTED_CHAIN_ID_HEX

    if not chain_matches:
        for check in checks[1:]:
            method = _PROBE_DEFINITIONS[check][0]
            results[check] = {
                "ok": False,
                "check": check,
                "method": method,
                "cached": False,
                "skipped": True,
                "error": "chain_id_mismatch_or_unavailable",
            }
    else:
        for check in checks[1:]:
            results[check] = await _rpc_check(check, force_refresh=bool(request.force_refresh))

    overall_ok = chain_matches and all(bool(record.get("ok")) for record in results.values())

    return {
        "ok": overall_ok,
        "read_only": True,
        "expected_chain_id": _EXPECTED_CHAIN_ID_DECIMAL,
        "expected_chain_id_hex": _EXPECTED_CHAIN_ID_HEX,
        "actual_chain_id": actual_chain_id or None,
        "chain_id_matches": chain_matches,
        "requested_checks": checks,
        "results": results,
        "status": _status_payload(),
    }

@router.get("/address/{address}/balance")
async def robinhood_chain_address_balance(
    address: str,
    force_refresh: bool = Query(default=False),
) -> Dict[str, Any]:
    """Return the native ETH balance for one Robinhood Chain address.

    This endpoint is read-only. It verifies chain ID 4663 before reading the
    latest balance and never constructs, signs, or broadcasts a transaction.
    """
    if not bool(settings.robinhood_chain_enabled):
        raise HTTPException(status_code=503, detail="Robinhood Chain is disabled")
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")
    if not _configured_rpc_http():
        raise HTTPException(status_code=503, detail="ROBINHOOD_CHAIN_RPC_HTTP is not configured")

    try:
        normalized_address = validate_evm_address(address)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    result = await get_robinhood_chain_client().get_native_balance(
        normalized_address,
        block_tag="latest",
        force_refresh=bool(force_refresh),
    )
    if not result.get("ok"):
        error = str(result.get("error") or "Robinhood Chain native balance read failed")
        status_code = 503 if error == "native_balance_rpc_failed" and (result.get("rpc") or {}).get("error") == "rpc_backoff_active" else 502
        raise HTTPException(status_code=status_code, detail=result)

    return {
        "ok": True,
        "venue": "robinhood_chain",
        "network": "robinhood_chain",
        "chain_id": _EXPECTED_CHAIN_ID_DECIMAL,
        "chain_id_hex": _EXPECTED_CHAIN_ID_HEX,
        "address": result.get("address"),
        "asset": _NATIVE_CURRENCY,
        "balance_wei": result.get("balance_wei"),
        "balance_eth": result.get("balance_eth"),
        "block_tag": result.get("block_tag"),
        "cached": bool(result.get("cached")),
        "fetched_at": result.get("fetched_at"),
        "source": "robinhood_chain_rpc",
        "read_only": True,
    }

@router.get("/address/{address}/erc20/{symbol}/balance")
async def robinhood_chain_erc20_balance(
    address: str,
    symbol: str,
    force_refresh: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Return one registered ERC-20 balance through balanceOf(address).

    The token contract and decimals must come from Token Registry. This endpoint
    does not accept arbitrary contracts, calldata, block tags, or write methods.
    """
    if not bool(settings.robinhood_chain_enabled):
        raise HTTPException(status_code=503, detail="Robinhood Chain is disabled")
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")
    if not _configured_rpc_http():
        raise HTTPException(status_code=503, detail="ROBINHOOD_CHAIN_RPC_HTTP is not configured")

    try:
        normalized_address = validate_evm_address(address)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    registry_row, contract_address, decimals = _resolve_registered_erc20(db, symbol)
    asset = str(registry_row.symbol or "").strip().upper()

    result = await get_robinhood_chain_client().get_erc20_balance(
        normalized_address,
        contract_address,
        decimals,
        block_tag="latest",
        force_refresh=bool(force_refresh),
    )
    if not result.get("ok"):
        rpc_error = (result.get("rpc") or {}).get("error")
        status_code = 503 if rpc_error == "rpc_backoff_active" else 502
        raise HTTPException(status_code=status_code, detail=result)

    return {
        "ok": True,
        "venue": "robinhood_chain",
        "network": "robinhood_chain",
        "chain_id": _EXPECTED_CHAIN_ID_DECIMAL,
        "chain_id_hex": _EXPECTED_CHAIN_ID_HEX,
        "address": result.get("owner_address"),
        "asset": asset,
        "contract_address": result.get("contract_address"),
        "decimals": int(result.get("decimals")),
        "balance_atomic": result.get("balance_atomic"),
        "balance_token": result.get("balance_token"),
        "block_tag": result.get("block_tag"),
        "cached": bool(result.get("cached")),
        "fetched_at": result.get("fetched_at"),
        "registry_id": int(registry_row.id),
        "registry_venue": registry_row.venue,
        "source": "robinhood_chain_erc20_rpc",
        "read_only": True,
    }

@router.get("/address/{address}/history")
async def robinhood_chain_address_history(
    address: str,
    cursor: Optional[str] = Query(default=None, max_length=4096),
    force_refresh: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Return bounded, display-only Robinhood Chain address activity.

    The endpoint reads only fixed Blockscout address-history resources and the
    existing chain-identity RPC check. It does not cache transactions in the
    database, create deposits/withdrawals, or mutate ledger/FIFO/basis state.
    """
    if not bool(settings.robinhood_chain_enabled):
        raise HTTPException(status_code=503, detail="Robinhood Chain is disabled")
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")
    if not settings.robinhood_chain_effective_explorer_api_base():
        raise HTTPException(status_code=503, detail="ROBINHOOD_CHAIN_EXPLORER_API_BASE is not configured")

    try:
        normalized_address = validate_evm_address(address)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    result = await get_robinhood_chain_history_service().get_address_history(
        normalized_address,
        cursor=cursor,
        force_refresh=bool(force_refresh),
        registry_tokens=_registered_history_token_map(db),
    )
    if result.get("ok"):
        return result

    error = str(result.get("error") or "robinhood_chain_history_failed")
    if error == "invalid_history_request":
        status_code = 400
    elif error in {"history_backoff_active", "history_api_not_configured"}:
        status_code = 503
    else:
        status_code = 502
    raise HTTPException(status_code=status_code, detail=result)

@router.post("/address/{address}/transactions/{tx_hash}/accounting-preview")
async def robinhood_chain_transaction_accounting_preview(
    address: str,
    tx_hash: str,
    request: Optional[RobinhoodChainAccountingPreviewRequest] = None,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Return a transaction-group accounting preview without persisting anything."""
    if not bool(settings.robinhood_chain_enabled):
        raise HTTPException(status_code=503, detail="Robinhood Chain is disabled")
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")
    if not settings.robinhood_chain_effective_explorer_api_base():
        raise HTTPException(status_code=503, detail="ROBINHOOD_CHAIN_EXPLORER_API_BASE is not configured")

    try:
        normalized_address = validate_evm_address(address)
        normalized_tx_hash = validate_transaction_hash(tx_hash)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    payload = request or RobinhoodChainAccountingPreviewRequest()
    activity = await get_robinhood_chain_history_service().get_transaction_activity(
        normalized_address,
        normalized_tx_hash,
        force_refresh=bool(payload.force_refresh),
        registry_tokens=_registered_history_token_map(db),
    )
    if not activity.get("ok"):
        error = str(activity.get("error") or "transaction_activity_failed")
        if error in {"invalid_transaction_request", "transaction_not_related_to_address"}:
            status_code = 400
        elif error == "transaction_not_found":
            status_code = 404
        elif error in {"history_backoff_active", "history_api_not_configured", "chain_id_mismatch_or_unavailable"}:
            status_code = 503
        else:
            status_code = 502
        raise HTTPException(status_code=status_code, detail=activity)

    try:
        preview = build_robinhood_chain_accounting_preview(
            db,
            address=normalized_address,
            tx_hash=normalized_tx_hash,
            transaction_activity=activity,
            wallet_address_id=payload.wallet_address_id,
        )
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail={
                "error": "robinhood_chain_accounting_preview_failed",
                "message": str(exc),
                "exc": type(exc).__name__,
                "read_only": True,
                "will_mutate": False,
            },
        ) from exc

    # Defensive session reset: the preview service performs SELECTs only.
    db.rollback()
    return preview

@router.get("/execution-discovery/status")
async def robinhood_chain_execution_discovery_status() -> Dict[str, Any]:
    """Return secret-free, mainnet-only 0x discovery readiness."""
    if not bool(settings.robinhood_chain_enabled):
        raise HTTPException(status_code=503, detail="Robinhood Chain is disabled")
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")
    return get_robinhood_chain_execution_discovery_service().status()


@router.post("/execution-discovery/probe")
async def robinhood_chain_execution_discovery_probe(
    request: RobinhoodChainExecutionDiscoveryRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Run one bounded indicative-price probe without constructing a trade."""
    if not bool(settings.robinhood_chain_enabled):
        raise HTTPException(status_code=503, detail="Robinhood Chain is disabled")
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")

    try:
        taker_address = validate_evm_address(request.taker_address)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    provider = str(request.provider or "").strip().lower()
    if provider not in {"0x", "zerox"}:
        raise HTTPException(
            status_code=400,
            detail={"error": "unsupported_execution_discovery_provider", "provider": request.provider, "allowed": ["0x"]},
        )

    sell_token = _resolve_execution_discovery_token(db, request.sell_symbol)
    buy_token = _resolve_execution_discovery_token(db, request.buy_symbol)
    if sell_token["symbol"] == buy_token["symbol"]:
        raise HTTPException(status_code=400, detail={"error": "identical_execution_discovery_assets"})

    result = await get_robinhood_chain_execution_discovery_service().probe(
        sell_token=sell_token,
        buy_token=buy_token,
        sell_amount=request.sell_amount,
        buy_amount=request.buy_amount,
        taker_address=taker_address,
        force_refresh=bool(request.force_refresh),
    )
    if result.get("ok"):
        return result

    error = str(result.get("error") or "execution_discovery_probe_failed")
    if error in {
        "invalid_discovery_amount",
        "discovery_amount_mode_required",
        "discovery_amount_modes_mutually_exclusive",
        "discovery_amount_exceeds_cap",
        "unsupported_discovery_pair",
    }:
        status_code = 400
    elif error == "execution_discovery_route_mode_not_live_verified":
        status_code = 409
    elif error in {
        "execution_discovery_not_configured",
        "execution_discovery_backoff_active",
        "chain_id_mismatch_or_unavailable",
    }:
        status_code = 503
    elif error in {"contract_code_unavailable", "provider_authentication_failed"}:
        status_code = 502
    else:
        status_code = 502
    raise HTTPException(status_code=status_code, detail=result)


@router.get("/quotes/status")
async def robinhood_chain_quotes_status(
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Return secret-free quote-only readiness and canonical token identities."""
    if not bool(settings.robinhood_chain_enabled):
        raise HTTPException(status_code=503, detail="Robinhood Chain is disabled")
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")

    eth = _resolve_execution_discovery_token(db, "ETH")
    weth = _resolve_execution_discovery_token(db, "WETH")
    usdg = _resolve_execution_discovery_token(db, "USDG")
    payload = get_robinhood_chain_quote_service().status()
    payload["firm_planning"] = get_robinhood_chain_transaction_planning_service().status()
    payload["tokens"] = {
        "ETH": eth,
        "WETH": weth,
        "USDG": usdg,
    }
    payload["route_capabilities"] = [dict(item) for item in ROBINHOOD_CHAIN_ROUTE_CAPABILITIES]
    payload["swap_oriented"] = True
    payload["amount_modes"] = ["exact_spend", "exact_receive"]
    payload["exact_receive_provider"] = "direct_router_required"
    wallet_row = (
        db.query(WalletAddress)
        .filter(
            WalletAddress.network == _TOKEN_REGISTRY_CHAIN,
            WalletAddress.wallet_id == _TOKEN_REGISTRY_VENUE,
            WalletAddress.asset.in_(["ALL", "*"]),
        )
        .order_by(WalletAddress.created_at.desc())
        .first()
    )
    payload["wallet_configured"] = wallet_row is not None
    payload["wallet"] = (
        {
            "id": str(wallet_row.id),
            "wallet_id": wallet_row.wallet_id,
            "network": wallet_row.network,
            "asset": wallet_row.asset,
            "address": validate_evm_address(str(wallet_row.address or "").strip()),
            "label": wallet_row.label,
            "owner_scope": wallet_row.owner_scope,
            "wallet_type": "MetaMask",
        }
        if wallet_row is not None
        else None
    )
    db.rollback()
    return payload


@router.post("/quotes/indicative")
async def robinhood_chain_indicative_quote(
    request: RobinhoodChainIndicativeQuoteRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Return a bounded indicative quote without constructing or submitting a trade.

    RH-CHAIN.10D.2 uses BUY quantity=0.001 with total_quote omitted to request
    exact output under the fixed 2.00 USDG ceiling.
    """
    if not bool(settings.robinhood_chain_enabled):
        raise HTTPException(status_code=503, detail="Robinhood Chain is disabled")
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")

    provider = str(request.provider or "").strip().lower()
    if provider not in {"0x", "zerox"}:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "unsupported_robinhood_chain_quote_provider",
                "provider": request.provider,
                "allowed": [ROBINHOOD_CHAIN_QUOTE_PROVIDER],
            },
        )

    taker_address = _resolve_robinhood_chain_quote_taker(db, request.taker_address)
    eth = _resolve_execution_discovery_token(db, "ETH")
    usdg = _resolve_execution_discovery_token(db, "USDG")
    result = await get_robinhood_chain_quote_service().indicative_quote(
        symbol=request.symbol,
        side=request.side,
        quantity=request.quantity,
        total_quote=request.total_quote,
        taker_address=taker_address,
        eth_token=eth,
        usdg_token=usdg,
        force_refresh=bool(request.force_refresh),
    )
    db.rollback()
    if result.get("ok"):
        return result
    raise HTTPException(status_code=_quote_failure_status(result), detail=result)


@router.post("/quotes/firm-plan")
async def robinhood_chain_firm_quote_plan(
    request: RobinhoodChainFirmQuotePlanRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Return a bounded firm 0x quote and validated unsigned transaction plan.

    Native ETH input requires no allowance and must carry the exact input in
    transaction.value. ERC-20 input reads current allowance with eth_call. The
    endpoint never builds an approval transaction, prompts a wallet, signs,
    broadcasts, records an order, or mutates ledger/FIFO/basis state.
    """
    if not bool(settings.robinhood_chain_enabled):
        raise HTTPException(status_code=503, detail="Robinhood Chain is disabled")
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")

    provider = str(request.provider or "").strip().lower()
    if provider not in {"0x", "zerox"}:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "unsupported_robinhood_chain_quote_provider",
                "provider": request.provider,
                "allowed": [ROBINHOOD_CHAIN_QUOTE_PROVIDER],
            },
        )

    taker_address = _resolve_robinhood_chain_quote_taker(db, request.taker_address)
    eth = _resolve_execution_discovery_token(db, "ETH")
    usdg = _resolve_execution_discovery_token(db, "USDG")
    result = await get_robinhood_chain_transaction_planning_service().firm_quote_plan(
        symbol=request.symbol,
        side=request.side,
        quantity=request.quantity,
        total_quote=request.total_quote,
        exact_output_quantity=request.exact_output_quantity,
        maximum_total_quote=request.maximum_total_quote,
        taker_address=taker_address,
        eth_token=eth,
        usdg_token=usdg,
        slippage_bps=int(request.slippage_bps),
    )
    db.rollback()
    if result.get("ok"):
        return result
    raise HTTPException(status_code=_quote_failure_status(result), detail=result)


@router.get("/execution/status")
async def robinhood_chain_execution_status() -> Dict[str, Any]:
    """Return the dedicated RH-CHAIN.10D.1 browser-wallet execution gate."""
    return get_robinhood_chain_execution_service().status()


@router.post("/execution/prepare")
async def robinhood_chain_execution_prepare(
    request: RobinhoodChainExecutionPrepareRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Create one tightly bounded prepared lifecycle row and fresh transaction plan.

    This endpoint writes only the dedicated robinhood_chain_executions table. It
    does not connect MetaMask, sign, broadcast, create a generic order, or touch
    ledger/FIFO/basis state.
    """
    if not bool(settings.robinhood_chain_enabled):
        raise HTTPException(status_code=503, detail="Robinhood Chain is disabled")
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")

    if str(request.symbol or "").strip().upper() != ROBINHOOD_CHAIN_EXECUTION_SYMBOL:
        raise HTTPException(status_code=400, detail={"error": "robinhood_chain_execution_symbol_locked"})
    if str(request.side or "").strip().lower() != ROBINHOOD_CHAIN_EXECUTION_SIDE:
        raise HTTPException(status_code=400, detail={"error": "robinhood_chain_execution_side_locked"})
    if str(request.quantity or "").strip() != str(ROBINHOOD_CHAIN_EXECUTION_INPUT_ETH):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "robinhood_chain_execution_amount_locked",
                "required_quantity_eth": str(ROBINHOOD_CHAIN_EXECUTION_INPUT_ETH),
            },
        )

    taker = _resolve_robinhood_chain_execution_taker(db, request.taker_address)
    eth = _resolve_execution_discovery_token(db, "ETH")
    usdg = _resolve_execution_discovery_token(db, "USDG")
    try:
        result = await get_robinhood_chain_execution_service().prepare(
            db,
            taker_address=taker,
            eth_token=eth,
            usdg_token=usdg,
            slippage_bps=int(request.slippage_bps),
            confirm_prepare=bool(request.confirm_prepare),
        )
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc

    if result.get("ok"):
        return result
    db.rollback()
    raise HTTPException(status_code=_quote_failure_status(result), detail=result)


@router.get("/execution/{execution_id}")
async def robinhood_chain_execution_get(
    execution_id: str,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        payload = get_robinhood_chain_execution_service().get(db, execution_id)
        db.rollback()
        return payload
    except ValueError as exc:
        db.rollback()
        status_code = 404 if str(exc) == "robinhood_chain_execution_not_found" else 400
        raise HTTPException(status_code=status_code, detail={"error": str(exc)}) from exc


@router.post("/execution/{execution_id}/claim-send")
async def robinhood_chain_execution_claim_send(
    execution_id: str,
    request: RobinhoodChainExecutionSendClaimRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Atomically reserve one prepared plan for one explicit MetaMask request."""
    try:
        return get_robinhood_chain_execution_service().claim_send(
            db,
            execution_id=execution_id,
            wallet_address=request.wallet_address,
            plan_hash=request.plan_hash,
            claim_id=request.claim_id,
            confirm_send_claim=bool(request.confirm_send_claim),
        )
    except ValueError as exc:
        db.rollback()
        error = str(exc)
        status_code = 404 if error == "robinhood_chain_execution_not_found" else 409
        raise HTTPException(status_code=status_code, detail={"error": error}) from exc


@router.post("/execution/{execution_id}/submission")
async def robinhood_chain_execution_record_submission(
    execution_id: str,
    request: RobinhoodChainExecutionSubmissionRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Record exactly one transaction hash returned by explicit MetaMask send."""
    try:
        return get_robinhood_chain_execution_service().record_submission(
            db,
            execution_id=execution_id,
            tx_hash=request.tx_hash,
            wallet_address=request.wallet_address,
            claim_id=request.claim_id,
            confirm_record=bool(request.confirm_record),
        )
    except ValueError as exc:
        db.rollback()
        status_code = 404 if str(exc) == "robinhood_chain_execution_not_found" else 409
        raise HTTPException(status_code=status_code, detail={"error": str(exc)}) from exc


@router.post("/execution/{execution_id}/submission-failure")
async def robinhood_chain_execution_record_submission_failure(
    execution_id: str,
    request: RobinhoodChainExecutionSubmissionFailureRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Record a terminal MetaMask request failure only when no hash was returned."""
    reason = str(request.reason or "").strip().lower()
    if reason not in ROBINHOOD_CHAIN_SUBMISSION_FAILURE_REASONS:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "invalid_robinhood_chain_submission_failure_reason",
                "allowed": sorted(ROBINHOOD_CHAIN_SUBMISSION_FAILURE_REASONS),
            },
        )
    try:
        return get_robinhood_chain_execution_service().record_submission_failure(
            db,
            execution_id=execution_id,
            wallet_address=request.wallet_address,
            claim_id=request.claim_id,
            reason=reason,
            message=request.message,
            confirm_failure=bool(request.confirm_failure),
        )
    except ValueError as exc:
        db.rollback()
        error = str(exc)
        status_code = 404 if error == "robinhood_chain_execution_not_found" else 409
        raise HTTPException(status_code=status_code, detail={"error": error}) from exc


@router.post("/execution/{execution_id}/refresh")
async def robinhood_chain_execution_refresh(
    execution_id: str,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Refresh receipt state, realized output, and saved ETH/USDG snapshots."""
    try:
        result = await get_robinhood_chain_execution_service().refresh_receipt(
            db,
            execution_id=execution_id,
        )
        execution = result.get("execution") if isinstance(result, dict) else None
        if isinstance(execution, dict) and str(execution.get("status") or "").lower() == "confirmed":
            result["balance_refresh"] = await _refresh_robinhood_chain_execution_balance_snapshots(
                db,
                str(execution.get("wallet_address") or ""),
            )
        return result
    except ValueError as exc:
        db.rollback()
        error = str(exc)
        if error == "robinhood_chain_execution_not_found":
            status_code = 404
        elif error == "robinhood_chain_execution_not_submitted":
            status_code = 409
        else:
            status_code = 502
        raise HTTPException(status_code=status_code, detail={"error": error}) from exc


@router.get("/buy-execution/status")
async def robinhood_chain_buy_execution_status() -> Dict[str, Any]:
    """Return the RH-CHAIN.10D.2 bounded approval + exact-output BUY gate."""
    return get_robinhood_chain_buy_execution_service().status()


@router.post("/buy-execution/prepare-approval")
async def robinhood_chain_buy_prepare_approval(
    request: RobinhoodChainBuyApprovalPrepareRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")
    if str(request.symbol).strip().upper() != ROBINHOOD_CHAIN_BUY_SYMBOL or str(request.side).strip().lower() != ROBINHOOD_CHAIN_BUY_SIDE:
        raise HTTPException(status_code=400, detail={"error": "robinhood_chain_buy_identity_locked"})
    if str(request.exact_output_quantity).strip() != str(ROBINHOOD_CHAIN_BUY_EXACT_OUTPUT_ETH):
        raise HTTPException(status_code=400, detail={"error": "robinhood_chain_buy_output_locked"})
    if str(request.maximum_total_quote).strip() != str(ROBINHOOD_CHAIN_BUY_MAXIMUM_USDG):
        raise HTTPException(status_code=400, detail={"error": "robinhood_chain_buy_maximum_locked"})
    if str(request.approval_amount).strip() != str(ROBINHOOD_CHAIN_BUY_APPROVAL_USDG):
        raise HTTPException(status_code=400, detail={"error": "robinhood_chain_buy_approval_locked"})
    taker = _resolve_robinhood_chain_execution_taker(db, request.taker_address)
    try:
        return await get_robinhood_chain_buy_execution_service().prepare_approval(
            db,
            taker_address=taker,
            eth_token=_resolve_execution_discovery_token(db, "ETH"),
            usdg_token=_resolve_execution_discovery_token(db, "USDG"),
            confirm_prepare=bool(request.confirm_prepare),
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc


@router.get("/buy-execution/{execution_id}")
async def robinhood_chain_buy_execution_get(execution_id: str, db: Session = Depends(get_db)) -> Dict[str, Any]:
    try:
        payload = get_robinhood_chain_buy_execution_service().get(db, execution_id)
        db.rollback()
        return payload
    except (ValueError, KeyError) as exc:
        db.rollback()
        raise HTTPException(status_code=404 if "not_found" in str(exc) else 400, detail={"error": str(exc)}) from exc


@router.post("/buy-execution/{execution_id}/approval/claim-send")
async def robinhood_chain_buy_approval_claim_send(
    execution_id: str,
    request: RobinhoodChainExecutionSendClaimRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return get_robinhood_chain_buy_execution_service().claim_approval_send(
            db,
            execution_id=execution_id,
            wallet_address=request.wallet_address,
            plan_hash=request.plan_hash,
            claim_id=request.claim_id,
            confirm_send_claim=bool(request.confirm_send_claim),
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": str(exc)}) from exc


@router.post("/buy-execution/{execution_id}/approval/submission")
async def robinhood_chain_buy_approval_submission(
    execution_id: str,
    request: RobinhoodChainExecutionSubmissionRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return get_robinhood_chain_buy_execution_service().record_approval_submission(
            db,
            execution_id=execution_id,
            tx_hash=request.tx_hash,
            wallet_address=request.wallet_address,
            claim_id=request.claim_id,
            confirm_record=bool(request.confirm_record),
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": str(exc)}) from exc


@router.post("/buy-execution/{execution_id}/approval/submission-failure")
async def robinhood_chain_buy_approval_submission_failure(
    execution_id: str,
    request: RobinhoodChainExecutionSubmissionFailureRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    reason = str(request.reason or "").strip().lower()
    if reason not in ROBINHOOD_CHAIN_BUY_SUBMISSION_FAILURE_REASONS:
        raise HTTPException(status_code=400, detail={"error": "invalid_robinhood_chain_buy_failure_reason"})
    try:
        return get_robinhood_chain_buy_execution_service().record_submission_failure(
            db,
            execution_id=execution_id,
            stage="approval",
            wallet_address=request.wallet_address,
            claim_id=request.claim_id,
            reason=reason,
            message=request.message,
            confirm_failure=bool(request.confirm_failure),
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": str(exc)}) from exc


@router.post("/buy-execution/{execution_id}/approval/refresh")
async def robinhood_chain_buy_approval_refresh(execution_id: str, db: Session = Depends(get_db)) -> Dict[str, Any]:
    try:
        return await get_robinhood_chain_buy_execution_service().refresh_approval(db, execution_id=execution_id)
    except (ValueError, KeyError) as exc:
        db.rollback()
        raise HTTPException(status_code=502, detail={"error": str(exc)}) from exc


@router.post("/buy-execution/{execution_id}/prepare-swap")
async def robinhood_chain_buy_prepare_swap(
    execution_id: str,
    request: RobinhoodChainBuySwapPrepareRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return await get_robinhood_chain_buy_execution_service().prepare_swap(
            db,
            execution_id=execution_id,
            wallet_address=request.wallet_address,
            eth_token=_resolve_execution_discovery_token(db, "ETH"),
            usdg_token=_resolve_execution_discovery_token(db, "USDG"),
            confirm_prepare=bool(request.confirm_prepare),
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": str(exc)}) from exc


@router.post("/buy-execution/{execution_id}/swap/claim-send")
async def robinhood_chain_buy_swap_claim_send(
    execution_id: str,
    request: RobinhoodChainExecutionSendClaimRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return get_robinhood_chain_buy_execution_service().claim_swap_send(
            db,
            execution_id=execution_id,
            wallet_address=request.wallet_address,
            plan_hash=request.plan_hash,
            claim_id=request.claim_id,
            confirm_send_claim=bool(request.confirm_send_claim),
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": str(exc)}) from exc


@router.post("/buy-execution/{execution_id}/swap/submission")
async def robinhood_chain_buy_swap_submission(
    execution_id: str,
    request: RobinhoodChainExecutionSubmissionRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return get_robinhood_chain_buy_execution_service().record_swap_submission(
            db,
            execution_id=execution_id,
            tx_hash=request.tx_hash,
            wallet_address=request.wallet_address,
            claim_id=request.claim_id,
            confirm_record=bool(request.confirm_record),
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": str(exc)}) from exc


@router.post("/buy-execution/{execution_id}/swap/submission-failure")
async def robinhood_chain_buy_swap_submission_failure(
    execution_id: str,
    request: RobinhoodChainExecutionSubmissionFailureRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    reason = str(request.reason or "").strip().lower()
    if reason not in ROBINHOOD_CHAIN_BUY_SUBMISSION_FAILURE_REASONS:
        raise HTTPException(status_code=400, detail={"error": "invalid_robinhood_chain_buy_failure_reason"})
    try:
        return get_robinhood_chain_buy_execution_service().record_submission_failure(
            db,
            execution_id=execution_id,
            stage="swap",
            wallet_address=request.wallet_address,
            claim_id=request.claim_id,
            reason=reason,
            message=request.message,
            confirm_failure=bool(request.confirm_failure),
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": str(exc)}) from exc


@router.post("/buy-execution/{execution_id}/swap/refresh")
async def robinhood_chain_buy_swap_refresh(execution_id: str, db: Session = Depends(get_db)) -> Dict[str, Any]:
    try:
        result = await get_robinhood_chain_buy_execution_service().refresh_swap(db, execution_id=execution_id)
        execution = result.get("execution") if isinstance(result, dict) else None
        if isinstance(execution, dict) and str(execution.get("status") or "").lower() == "confirmed":
            result["balance_refresh"] = await _refresh_robinhood_chain_execution_balance_snapshots(
                db,
                str(execution.get("wallet_address") or ""),
            )
        return result
    except (ValueError, KeyError) as exc:
        db.rollback()
        raise HTTPException(status_code=502, detail={"error": str(exc)}) from exc


@router.get("/orderbook")
async def robinhood_chain_synthetic_orderbook(
    symbol: str = Query(default=ROBINHOOD_CHAIN_QUOTE_SYMBOL, min_length=1, max_length=32),
    depth: int = Query(default=5, ge=1, le=5),
    taker_address: Optional[str] = Query(default=None, min_length=42, max_length=42),
    force_refresh: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Return bounded synthetic bid/ask samples; these are not resting orders."""
    if not bool(settings.robinhood_chain_enabled):
        raise HTTPException(status_code=503, detail="Robinhood Chain is disabled")
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")

    taker = _resolve_robinhood_chain_quote_taker(db, taker_address)
    eth = _resolve_execution_discovery_token(db, "ETH")
    usdg = _resolve_execution_discovery_token(db, "USDG")
    result = await get_robinhood_chain_quote_service().synthetic_orderbook(
        symbol=symbol,
        depth=depth,
        taker_address=taker,
        eth_token=eth,
        usdg_token=usdg,
        force_refresh=bool(force_refresh),
    )
    db.rollback()
    if result.get("ok"):
        return result
    raise HTTPException(status_code=_quote_failure_status(result), detail=result)
