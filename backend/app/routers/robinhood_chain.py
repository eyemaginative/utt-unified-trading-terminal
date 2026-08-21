from __future__ import annotations

import asyncio
import copy
import hashlib
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
from ..models import (
    RobinhoodChainExternalSwap,
    RobinhoodChainSwapExecution,
    RobinhoodChainWalletEvent,
    TokenRegistry,
    WalletAddress,
    WalletAddressSnapshot,
)
from ..services.evm_rpc import get_robinhood_chain_client, validate_evm_address
from ..services.robinhood_chain_accounting_preview import build_robinhood_chain_accounting_preview
from ..services.robinhood_chain_wallet_ingest import (
    ingest_robinhood_chain_wallet_history,
    preview_robinhood_chain_wallet_ingest,
)
from ..services.robinhood_chain_wallet_materialization import (
    materialize_robinhood_chain_wallet_history,
    preview_robinhood_chain_wallet_materialization,
)
from ..services.robinhood_chain_wallet_sync import sync_robinhood_chain_wallet_incremental
from ..services.robinhood_chain_history import (
    get_robinhood_chain_history_service,
    validate_transaction_hash,
)
from ..services.robinhood_chain_execution_discovery import (
    get_robinhood_chain_execution_discovery_service,
)
from ..services.robinhood_chain_registry_discovery import (
    get_robinhood_chain_registry_discovery_service,
)
from ..services.robinhood_chain_registry_authority import (
    RobinhoodChainRegistryAuthorityError,
    assert_robinhood_chain_execution_amount,
    resolve_robinhood_chain_execution_authority,
)
from ..services.robinhood_chain_quotes import (
    ROBINHOOD_CHAIN_QUOTE_PROVIDER,
    get_robinhood_chain_quote_service,
)
from ..services.robinhood_chain_uniswap_quote import (
    UNISWAP_NATIVE_TOKEN,
    UNISWAP_PROVIDER,
    create_wallet_approval_capability,
    create_wallet_swap_capability,
    decode_wallet_approval_capability,
    decode_wallet_swap_capability,
    get_robinhood_chain_uniswap_quote_service,
    validate_wallet_approval_transaction,
    validate_wallet_rejection_handoff,
    validate_wallet_successful_approval_handoff,
    validate_wallet_successful_swap_handoff,
    validate_wallet_swap_transaction,
)
from ..services.robinhood_chain_uniswap_v3_quote import (
    UNISWAP_V3_RPC_PROVIDER,
    get_robinhood_chain_uniswap_v3_quote_service,
)
from ..services.robinhood_chain_transaction_planning import (
    ROBINHOOD_CHAIN_DEFAULT_SLIPPAGE_BPS,
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
    normalize_robinhood_chain_execution_quantity,
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
from ..services.robinhood_chain_swap_execution import (
    ROBINHOOD_CHAIN_SWAP_AMOUNT_MODE,
    ROBINHOOD_CHAIN_SWAP_APPROVAL_TO_ASSETS,
    ROBINHOOD_CHAIN_SWAP_DEFAULT_USDG,
    ROBINHOOD_CHAIN_SWAP_DISPLAY_MODE,
    ROBINHOOD_CHAIN_SWAP_FROM_ASSET,
    ROBINHOOD_CHAIN_SWAP_TO_ASSET,
    get_robinhood_chain_swap_execution_service,
)


router = APIRouter(prefix="/api/robinhood_chain", tags=["robinhood_chain"])

_EXPECTED_CHAIN_ID_DECIMAL = 4663
_EXPECTED_CHAIN_ID_HEX = hex(_EXPECTED_CHAIN_ID_DECIMAL)
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


def _resolve_native_registry_identity(db: Session) -> Dict[str, Any]:
    try:
        return get_robinhood_chain_registry_discovery_service().native_identity(db)
    except ValueError as exc:
        error = str(exc)
        raise HTTPException(
            status_code=404 if "not_found" in error else 409,
            detail={
                "error": error,
                "chain": _TOKEN_REGISTRY_CHAIN,
                "identity_source": "token_registry",
            },
        ) from exc


def _resolve_registered_erc20(db: Session, symbol: str) -> Tuple[TokenRegistry, str, int]:
    normalized_symbol = _normalize_registry_symbol(symbol)
    try:
        identity = get_robinhood_chain_registry_discovery_service().resolve_token(
            db,
            normalized_symbol,
        )
    except ValueError as exc:
        error = str(exc)
        raise HTTPException(
            status_code=404 if "not_found" in error else 422,
            detail={
                "error": error,
                "chain": _TOKEN_REGISTRY_CHAIN,
                "symbol": normalized_symbol,
                "identity_source": "token_registry",
            },
        ) from exc

    if bool(identity.get("native")):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "native_asset_not_erc20",
                "message": "The requested Token Registry identity is native; use the native balance endpoint.",
                "symbol": normalized_symbol,
                "registry_id": identity.get("registry_id"),
                "identity_source": "token_registry",
            },
        )

    registry_id = int(identity.get("registry_id"))
    row = db.query(TokenRegistry).filter(TokenRegistry.id == registry_id).first()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail={
                "error": "robinhood_chain_registry_token_not_found",
                "chain": _TOKEN_REGISTRY_CHAIN,
                "symbol": normalized_symbol,
                "registry_id": registry_id,
            },
        )
    contract = validate_evm_address(str(identity.get("registry_contract_address") or "").strip())
    decimals = int(identity.get("decimals"))
    return row, contract, decimals


def _registered_history_token_map(db: Session) -> Dict[str, Dict[str, Any]]:
    """Return the effective contract-keyed Token Registry view for history labels."""
    service = get_robinhood_chain_registry_discovery_service()
    try:
        rows = service.registry_rows(db)
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "error": str(exc),
                "identity_source": "token_registry",
            },
        ) from exc

    selected: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        try:
            identity = service.token_identity(db, row)
            if bool(identity.get("native")):
                continue
            contract = validate_evm_address(
                str(identity.get("registry_contract_address") or "").strip()
            )
            contract_key = contract.lower()
            if contract_key in selected:
                continue
            selected[contract_key] = {
                "registry_id": int(identity["registry_id"]),
                "registry_venue": identity.get("registry_venue"),
                "symbol": str(identity.get("symbol") or "").strip().upper(),
                "decimals": int(identity["decimals"]),
                "label": identity.get("label"),
                "contract_address": contract,
            }
        except Exception:
            continue
    return selected


def _resolve_execution_discovery_token(db: Session, symbol: str) -> Dict[str, Any]:
    """Resolve a Robinhood Chain identity exclusively from TokenRegistry."""
    try:
        return get_robinhood_chain_registry_discovery_service().resolve_token(db, symbol)
    except ValueError as exc:
        error = str(exc)
        status_code = 404 if "not_found" in error else 422
        raise HTTPException(
            status_code=status_code,
            detail={
                "error": error,
                "symbol": str(symbol or "").strip().upper(),
                "identity_source": "token_registry",
            },
        ) from exc


def _resolve_robinhood_chain_review_market(
    db: Session,
    *,
    symbol: str,
    side: str,
    amount_mode: str,
    capability_status_field: str,
    provider: str = "0x",
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Resolve one database market and fail closed before provider contact."""
    registry_service = get_robinhood_chain_registry_discovery_service()
    try:
        market = registry_service.objective_by_symbol(db, symbol)
    except ValueError as exc:
        raise HTTPException(
            status_code=404,
            detail={
                "error": str(exc),
                "symbol": str(symbol or "").strip().upper(),
                "identity_source": "token_registry",
                "capability_source": "database",
                "provider_contacted": False,
            },
        ) from exc

    if str(market.get("mechanism") or "").strip().lower() != "swap":
        raise HTTPException(
            status_code=409,
            detail={
                "error": "robinhood_chain_quote_mechanism_not_supported",
                "symbol": market.get("symbol"),
                "mechanism": market.get("mechanism"),
                "provider_contacted": False,
                "read_only": True,
                "execution_enabled": False,
            },
        )

    base = market.get("base") if isinstance(market.get("base"), dict) else {}
    quote = market.get("quote") if isinstance(market.get("quote"), dict) else {}
    base_symbol = str(base.get("symbol") or "").strip().upper()
    quote_symbol = str(quote.get("symbol") or "").strip().upper()
    normalized_side = str(side or "").strip().lower()
    if normalized_side not in {"buy", "sell"}:
        raise HTTPException(status_code=400, detail={"error": "invalid_quote_side", "provider_contacted": False})
    normalized_mode = str(amount_mode or "").strip().lower()
    if normalized_mode not in {"exact_input", "exact_output"}:
        raise HTTPException(status_code=400, detail={"error": "invalid_quote_amount_mode", "provider_contacted": False})
    from_asset = base_symbol if normalized_side == "sell" else quote_symbol
    to_asset = quote_symbol if normalized_side == "sell" else base_symbol

    normalized_provider = str(provider or "").strip().lower().replace("zerox", "0x")
    capability = next(
        (
            item for item in (market.get("capabilities") or [])
            if isinstance(item, dict)
            and str(item.get("provider") or "").strip().lower() == normalized_provider
            and str(item.get("from_asset") or "").strip().upper() == from_asset
            and str(item.get("to_asset") or "").strip().upper() == to_asset
            and str(item.get("amount_mode") or "").strip().lower() == normalized_mode
        ),
        None,
    )
    status_field = str(capability_status_field or "").strip()
    if status_field not in {"indicative_status", "firm_plan_status"}:
        raise HTTPException(status_code=500, detail={"error": "invalid_review_capability_status_field"})
    capability_status = str((capability or {}).get(status_field) or "").strip().lower()
    if capability_status not in {"available", "live_verified"}:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "robinhood_chain_quote_route_unavailable"
                if status_field == "indicative_status"
                else "firm_quote_route_capability_unavailable",
                "symbol": market.get("symbol"),
                "input_asset": from_asset,
                "output_asset": to_asset,
                "amount_mode": normalized_mode,
                "capability_status_field": status_field,
                "capability_status": capability_status or "missing",
                "route_capability": capability,
                "provider_contacted": False,
                "read_only": True,
                "execution_enabled": False,
            },
        )
    return market, base, quote, capability


def _execution_authority_http_detail(exc: RobinhoodChainRegistryAuthorityError) -> Dict[str, Any]:
    detail = {
        "error": exc.code,
        "message": exc.message,
        "provider_contacted": False,
        "automatic_execution_promotion": False,
    }
    detail.update(exc.context)
    return detail


def _resolve_robinhood_chain_execution_authority_or_http(
    db: Session,
    *,
    symbol: str,
    side: str,
    amount_mode: str = "exact_input",
    provider: str = "0x",
    require_execution: bool = False,
) -> Dict[str, Any]:
    try:
        return resolve_robinhood_chain_execution_authority(
            db,
            symbol=symbol,
            side=side,
            amount_mode=amount_mode,
            provider=provider,
            require_execution=require_execution,
        )
    except RobinhoodChainRegistryAuthorityError as exc:
        status_code = 404 if "not_found" in exc.code else 409
        if exc.code.startswith("invalid_") or "not_supported" in exc.code:
            status_code = 400
        raise HTTPException(status_code=status_code, detail=_execution_authority_http_detail(exc)) from exc


def _assert_robinhood_chain_execution_amount_or_http(
    authority: Dict[str, Any],
    amount: Any,
) -> str:
    try:
        return assert_robinhood_chain_execution_amount(authority, amount)
    except RobinhoodChainRegistryAuthorityError as exc:
        raise HTTPException(status_code=409, detail=_execution_authority_http_detail(exc)) from exc


def _assert_persisted_swap_execution_authority(
    db: Session,
    execution: Dict[str, Any],
    *,
    require_successful_broadcast: bool = False,
) -> Dict[str, Any]:
    authority = _resolve_robinhood_chain_execution_authority_or_http(
        db,
        symbol=str(execution.get("symbol") or ""),
        side=str(execution.get("side") or ""),
        amount_mode=str(execution.get("amount_mode") or "exact_input"),
        provider=str(execution.get("provider") or "0x"),
        require_execution=True,
    )
    if require_successful_broadcast:
        _resolve_robinhood_chain_execution_taker(
            db,
            str(execution.get("wallet_address") or ""),
        )
        if authority.get("successful_broadcast_authorized") is not True:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "robinhood_chain_successful_broadcast_not_authorized",
                    "authority": authority,
                    "provider_contacted": False,
                    "wallet_connection_requested": False,
                    "signing_enabled": False,
                    "broadcast_enabled": False,
                    "automatic_execution_promotion": False,
                },
            )
    return authority


def _r5c5a_controlled_buy_execution(execution: Dict[str, Any]) -> bool:
    return bool(
        str(execution.get("symbol") or "").strip().upper() == "WETH-USDG"
        and str(execution.get("side") or "").strip().lower() == "buy"
        and str(execution.get("from_asset") or "").strip().upper() == "USDG"
        and str(execution.get("to_asset") or "").strip().upper() == "WETH"
        and str(execution.get("amount_mode") or "").strip().lower() == "exact_input"
        and str(execution.get("exact_input_amount") or "").strip() == "1"
    )


def _r5c5b_controlled_sell_execution(execution: Dict[str, Any]) -> bool:
    return bool(
        str(execution.get("symbol") or "").strip().upper() == "WETH-USDG"
        and str(execution.get("side") or "").strip().lower() == "sell"
        and str(execution.get("from_asset") or "").strip().upper() == "WETH"
        and str(execution.get("to_asset") or "").strip().upper() == "USDG"
        and str(execution.get("amount_mode") or "").strip().lower() == "exact_input"
        and str(execution.get("exact_input_amount") or "").strip() == "0.0001"
    )


def _controlled_weth_usdg_execution(execution: Dict[str, Any]) -> bool:
    """Amount-specific live authorization is retired by RH-EXEC.AMT.1."""
    return False


def _resolve_robinhood_chain_review_identities(
    db: Session,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    registry_service = get_robinhood_chain_registry_discovery_service()
    registry_tokens = [
        item for item in registry_service.assets(db)
        if isinstance(item, dict)
        and not item.get("identity_error")
        and item.get("registry_id") is not None
    ]
    native_token = next((item for item in registry_tokens if item.get("native") is True), None)
    if native_token is None:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "robinhood_chain_native_registry_identity_not_found",
                "identity_source": "token_registry",
                "provider_contacted": False,
            },
        )
    return registry_tokens, native_token


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


class RobinhoodChainWalletIngestRequest(BaseModel):
    wallet_address_id: str = Field(min_length=1, max_length=64)
    force_full: bool = False
    force_refresh: bool = True
    confirm_ingest: bool = False


class RobinhoodChainWalletMaterializationRequest(BaseModel):
    wallet_address_id: str = Field(min_length=1, max_length=64)
    confirm_materialize: bool = False


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


class RobinhoodChainRegistryVerifyRequest(BaseModel):
    force_refresh: bool = False
    confirm_verify: bool = Field(
        default=False,
        description="Must be true to persist the read-only onchain identity verification result.",
    )


class RobinhoodChainPairObjectiveCreateRequest(BaseModel):
    base_token_registry_id: int = Field(gt=0)
    quote_token_registry_id: int = Field(gt=0)
    mechanism: str = Field(default="swap", max_length=32)
    notes: Optional[str] = Field(default=None, max_length=512)
    confirm_create: bool = Field(
        default=False,
        description="Must be true to create or update the database-backed pair objective.",
    )


class RobinhoodChainPairObjectiveDeleteRequest(BaseModel):
    confirm_delete: bool = Field(default=False)


class RobinhoodChainPairDiscoveryRequest(BaseModel):
    taker_address: str = Field(min_length=42, max_length=42)
    base_probe_amount: str = Field(min_length=1, max_length=80)
    quote_probe_amount: str = Field(min_length=1, max_length=80)
    force_refresh: bool = False
    confirm_discovery: bool = Field(
        default=False,
        description="Must be true to perform read-only provider/RPC calls and persist capability evidence.",
    )


class RobinhoodChainSelectedMarketRefreshRequest(BaseModel):
    taker_address: Optional[str] = Field(
        default=None,
        min_length=42,
        max_length=42,
        description="Optional public address override. When omitted, UTT uses the saved Robinhood Chain wallet.",
    )
    force_refresh: bool = True
    confirm_refresh: bool = Field(
        default=False,
        description="Must be true to refresh only the selected market's bounded review-only capability evidence.",
    )


class RobinhoodChainExecutionEvidenceSyncRequest(BaseModel):
    confirm_sync: bool = Field(
        default=False,
        description="Must be true to persist capabilities derived from confirmed local execution records.",
    )


class RobinhoodChainIndicativeQuoteRequest(BaseModel):
    provider: str = Field(
        default="0x",
        min_length=1,
        max_length=16,
        description="Fixed quote provider identifier; only 0x is accepted.",
    )
    symbol: str = Field(
        min_length=1,
        max_length=32,
        description="Explicit database-registered market symbol.",
    )
    side: str = Field(min_length=3, max_length=4, description="buy or sell")
    amount_mode: str = Field(
        min_length=10,
        max_length=12,
        description="Explicit provider amount mode: exact_input or exact_output.",
    )
    requested_amount: str = Field(
        min_length=1,
        max_length=80,
        description="Requested amount in the capability-defined input or output asset.",
    )
    taker_address: Optional[str] = Field(
        default=None,
        min_length=42,
        max_length=42,
        description="Optional public address override. When omitted, UTT uses the saved ALL / robinhood_chain wallet.",
    )
    force_refresh: bool = False


class RobinhoodChainUniswapQuoteRequest(BaseModel):
    symbol: str = Field(
        min_length=1,
        max_length=32,
        description="Explicit Token Registry-backed Robinhood Chain market symbol.",
    )
    side: str = Field(min_length=3, max_length=4, description="buy or sell")
    amount_mode: str = Field(
        default="exact_input",
        min_length=10,
        max_length=12,
        description="The canary accepts exact_input only.",
    )
    requested_amount: str = Field(
        min_length=1,
        max_length=80,
        description="Exact input amount in display units of the direction's input token.",
    )
    slippage_bps: int = Field(
        default=50,
        ge=ROBINHOOD_CHAIN_MIN_SLIPPAGE_BPS,
        le=ROBINHOOD_CHAIN_MAX_SLIPPAGE_BPS,
    )
    taker_address: Optional[str] = Field(
        default=None,
        min_length=42,
        max_length=42,
        description="Optional public address override; no wallet connection is requested.",
    )
    confirm_quote: bool = Field(
        default=False,
        description="Must be true to make one explicit read-only Uniswap /quote request.",
    )


class RobinhoodChainFirmQuotePlanRequest(BaseModel):
    provider: str = Field(
        default="0x",
        min_length=1,
        max_length=16,
        description="Fixed provider identifier; only 0x AllowanceHolder is accepted.",
    )
    symbol: str = Field(
        min_length=1,
        max_length=32,
        description="Explicit database-registered market symbol.",
    )
    side: str = Field(min_length=3, max_length=4, description="buy or sell")
    amount_mode: str = Field(
        min_length=10,
        max_length=12,
        description="Explicit provider amount mode: exact_input or exact_output.",
    )
    requested_amount: str = Field(
        min_length=1,
        max_length=80,
        description="Requested amount in the capability-defined input or output asset.",
    )
    maximum_input_amount: Optional[str] = Field(
        default=None,
        max_length=80,
        description="Required only for a database-verified exact-output capability.",
    )
    slippage_bps: int = Field(
        default=ROBINHOOD_CHAIN_DEFAULT_SLIPPAGE_BPS,
        ge=ROBINHOOD_CHAIN_MIN_SLIPPAGE_BPS,
        le=ROBINHOOD_CHAIN_MAX_SLIPPAGE_BPS,
        description="Bounded slippage protection in basis points.",
    )
    taker_address: Optional[str] = Field(
        default=None,
        min_length=42,
        max_length=42,
        description="Optional public address override. When omitted, UTT uses the saved ALL / robinhood_chain wallet.",
    )


class RobinhoodChainWalletRejectionPrepareRequest(BaseModel):
    provider: str = Field(default=UNISWAP_PROVIDER, min_length=1, max_length=16)
    symbol: str = Field(min_length=1, max_length=80)
    side: str = Field(min_length=3, max_length=4)
    amount_mode: str = Field(default="exact_input", min_length=1, max_length=32)
    requested_amount: str = Field(min_length=1, max_length=80)
    slippage_bps: int = Field(
        default=ROBINHOOD_CHAIN_DEFAULT_SLIPPAGE_BPS,
        ge=ROBINHOOD_CHAIN_MIN_SLIPPAGE_BPS,
        le=ROBINHOOD_CHAIN_MAX_SLIPPAGE_BPS,
    )
    taker_address: Optional[str] = Field(default=None, min_length=42, max_length=42)
    confirm_prepare: bool = Field(
        default=False,
        description="Must be true to prepare one reject-only MetaMask request. Successful broadcast is not authorized.",
    )


class RobinhoodChainWalletApprovalPrepareRequest(BaseModel):
    provider: str = Field(default=UNISWAP_PROVIDER, min_length=1, max_length=16)
    symbol: str = Field(min_length=1, max_length=80)
    side: str = Field(min_length=3, max_length=4)
    amount_mode: str = Field(default="exact_input", min_length=1, max_length=32)
    requested_amount: str = Field(min_length=1, max_length=80)
    slippage_bps: int = Field(
        default=ROBINHOOD_CHAIN_DEFAULT_SLIPPAGE_BPS,
        ge=ROBINHOOD_CHAIN_MIN_SLIPPAGE_BPS,
        le=ROBINHOOD_CHAIN_MAX_SLIPPAGE_BPS,
    )
    taker_address: Optional[str] = Field(default=None, min_length=42, max_length=42)
    confirm_prepare: bool = Field(
        default=False,
        description="Must be true to prepare one successful exact finite approval request. No swap request is authorized.",
    )


class RobinhoodChainWalletApprovalReceiptRequest(BaseModel):
    capability: str = Field(min_length=32, max_length=8192)
    tx_hash: str = Field(min_length=66, max_length=66)


class RobinhoodChainWalletSwapPrepareRequest(BaseModel):
    provider: str = Field(default=UNISWAP_PROVIDER, min_length=1, max_length=16)
    symbol: str = Field(min_length=1, max_length=80)
    side: str = Field(min_length=3, max_length=4)
    amount_mode: str = Field(default="exact_input", min_length=1, max_length=32)
    requested_amount: str = Field(min_length=1, max_length=80)
    slippage_bps: int = Field(
        default=ROBINHOOD_CHAIN_DEFAULT_SLIPPAGE_BPS,
        ge=ROBINHOOD_CHAIN_MIN_SLIPPAGE_BPS,
        le=ROBINHOOD_CHAIN_MAX_SLIPPAGE_BPS,
    )
    taker_address: Optional[str] = Field(default=None, min_length=42, max_length=42)
    approval_tx_hash: Optional[str] = Field(
        default=None,
        min_length=66,
        max_length=66,
        description="Optional confirmed finite-approval transaction hash to bind into later reconciliation evidence.",
    )
    confirm_prepare: bool = Field(
        default=False,
        description="Must be true to prepare one freshly simulated swap-only wallet request.",
    )


class RobinhoodChainWalletSwapReceiptRequest(BaseModel):
    capability: str = Field(min_length=32, max_length=8192)
    tx_hash: str = Field(min_length=66, max_length=66)
    execution_id: Optional[str] = Field(
        default=None,
        max_length=36,
        description="Optional durable generic lifecycle owner. When present, receipt refresh re-binds the MetaMask hash idempotently before verification.",
    )


class RobinhoodChainWalletSwapSubmissionRequest(BaseModel):
    capability: str = Field(min_length=32, max_length=8192)
    tx_hash: str = Field(min_length=66, max_length=66)
    confirm_record: bool = Field(
        default=False,
        description="Must be true to persist the MetaMask-returned swap hash against the prepared generic lifecycle.",
    )


class RobinhoodChainWalletSwapOrphanRecoveryRequest(BaseModel):
    tx_hash: str = Field(min_length=66, max_length=66)
    approval_tx_hash: str = Field(min_length=66, max_length=66)
    confirm_recovery: bool = Field(
        default=False,
        description="Must be true only for the explicit apply endpoint; preview is read-only.",
    )


class RobinhoodChainWalletSwapReconcileRequest(BaseModel):
    tx_hash: str = Field(min_length=66, max_length=66)
    symbol: str = Field(min_length=1, max_length=80)
    side: str = Field(min_length=3, max_length=4)
    requested_amount: str = Field(min_length=1, max_length=80)
    quoted_output_amount: str = Field(min_length=1, max_length=80)
    minimum_received: str = Field(min_length=1, max_length=80)
    approval_tx_hash: Optional[str] = Field(default=None, min_length=66, max_length=66)
    confirm_reconcile: bool = Field(
        default=False,
        description="Must be true to persist one receipt-verified generic swap into UTT accounting/order state.",
    )


class RobinhoodChainExecutionAuthorityRequest(BaseModel):
    symbol: str = Field(min_length=1, max_length=80)
    side: str = Field(min_length=3, max_length=4)
    amount_mode: str = Field(default="exact_input", min_length=1, max_length=32)
    provider: str = Field(default="0x", min_length=1, max_length=32)


class RobinhoodChainControlledLiveAuthorizationRequest(BaseModel):
    symbol: str = Field(default="WETH-USDG", min_length=1, max_length=80)
    side: str = Field(default="buy", min_length=3, max_length=4)
    amount_mode: str = Field(default="exact_input", min_length=1, max_length=32)
    requested_amount: str = Field(default="1", min_length=1, max_length=80)
    provider: str = Field(default="0x", min_length=1, max_length=32)
    taker_address: Optional[str] = Field(default=None, min_length=42, max_length=42)
    confirm_authorize: bool = Field(
        default=False,
        description="Must be true to authorize the bounded R5C.5A browser-wallet BUY pending confirmation.",
    )


class RobinhoodChainPreparationVerificationRequest(BaseModel):
    symbol: str = Field(default="WETH-USDG", min_length=1, max_length=80)
    side: str = Field(default="buy", min_length=3, max_length=4)
    amount_mode: str = Field(default="exact_input", min_length=1, max_length=32)
    requested_amount: str = Field(default="1", min_length=1, max_length=80)
    slippage_bps: int = Field(default=100, ge=100, le=100)
    taker_address: Optional[str] = Field(default=None, min_length=42, max_length=42)
    confirm_verify: bool = Field(
        default=False,
        description="Must be true to persist bounded R5C.4A or R5C.4B preparation-verification evidence.",
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


class RobinhoodChainSwapExecutionPrepareRequest(BaseModel):
    symbol: str = Field(default="ETH-USDG", min_length=1, max_length=80)
    side: str = Field(default="buy", min_length=3, max_length=4)
    from_asset: str = Field(default=ROBINHOOD_CHAIN_SWAP_FROM_ASSET, min_length=1, max_length=32)
    to_asset: str = Field(
        default=ROBINHOOD_CHAIN_SWAP_TO_ASSET,
        min_length=1,
        max_length=32,
        description="Registry-authoritative exact-input output asset. R5C.4B permits bounded WETH-to-USDG preparation only; successful broadcast remains unauthorized.",
    )
    amount_mode: str = Field(default=ROBINHOOD_CHAIN_SWAP_DISPLAY_MODE, min_length=1, max_length=32)
    exact_input_amount: str = Field(default=str(ROBINHOOD_CHAIN_SWAP_DEFAULT_USDG), min_length=1, max_length=80)
    slippage_bps: int = Field(
        default=ROBINHOOD_CHAIN_DEFAULT_SLIPPAGE_BPS,
        ge=ROBINHOOD_CHAIN_MIN_SLIPPAGE_BPS,
        le=ROBINHOOD_CHAIN_MAX_SLIPPAGE_BPS,
    )
    taker_address: Optional[str] = Field(default=None, min_length=42, max_length=42)
    confirm_prepare: bool = Field(
        default=False,
        description="Must be true to persist the generalized exact-spend lifecycle record.",
    )


class RobinhoodChainSwapFreshPrepareRequest(BaseModel):
    wallet_address: str = Field(min_length=42, max_length=42)
    confirm_prepare: bool = Field(
        default=False,
        description="Must be true to request and persist a fresh post-approval exact-spend swap plan.",
    )


_ROBINHOOD_CHAIN_WALLET_REJECTION_TTL_SECONDS = 60


def _robinhood_chain_wallet_rejection_gate() -> Dict[str, Any]:
    chain_ready = bool(settings.robinhood_chain_effective_enabled())
    dedicated = bool(getattr(settings, "robinhood_chain_live_execution_enabled", False))
    armed = bool(getattr(settings, "armed", False))
    dry_run = bool(getattr(settings, "dry_run", True))
    request_enabled = bool(chain_ready and dedicated and armed and not dry_run)
    missing: List[str] = []
    if not chain_ready:
        missing.append("robinhood_chain_effective_enabled")
    if not dedicated:
        missing.append("ROBINHOOD_CHAIN_LIVE_EXECUTION_ENABLED=1")
    if dry_run:
        missing.append("DRY_RUN=0")
    if not armed:
        missing.append("ARMED=1")
    return {
        "chain_id": _EXPECTED_CHAIN_ID_DECIMAL,
        "wallet_request_enabled": request_enabled,
        "reject_only": True,
        "successful_broadcast_authorized": False,
        "automatic_retry": False,
        "automatic_second_transaction": False,
        "missing_requirements": missing,
        "chain_ready": chain_ready,
        "dedicated_execution_enabled": dedicated,
        "armed": armed,
        "dry_run": dry_run,
    }


def _decode_robinhood_chain_rpc_quantity(value: Any, *, field: str) -> int:
    text = str(value if value is not None else "").strip().lower()
    try:
        number = int(text, 16) if text.startswith("0x") else int(text)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=502, detail={"error": f"invalid_{field}"}) from exc
    if number < 0:
        raise HTTPException(status_code=502, detail={"error": f"invalid_{field}"})
    return number


_ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
_ERC20_APPROVE_SELECTOR = "0x095ea7b3"
_WEI_PER_ETH_DECIMAL = Decimal(10) ** 18

# RH-ORDER.MISS.1B target-specific orphan guard.  Raw transaction hashes are
# intentionally not embedded in source; only irreversible SHA-256 fingerprints
# from the accepted D2/D3 diagnostics are retained.
_RH_ORDER_MISS_1B_TARGET_TX_SHA256 = "43545b67d2e62f240bd36613b17da68dad368d0e31e094c719f212bd617bc42a"
_RH_ORDER_MISS_1B_TARGET_APPROVAL_SHA256 = "5bef4b09b8345d550028392376fb3cf722df12c535ca39a6cd92c2eaf1723c11"
_RH_ORDER_MISS_1B_TARGET_SYMBOL = "INDEX-USDG"
_RH_ORDER_MISS_1B_TARGET_SIDE = "buy"
_RH_ORDER_MISS_1B_TARGET_INPUT_AMOUNT = "2"
_RH_ORDER_MISS_1B_TARGET_OUTPUT_AMOUNT = "273.765361829109662072"



def _decimal_text_exact(value: Decimal) -> str:
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _display_amount_to_atomic(value: Any, decimals: int, *, field: str) -> int:
    try:
        amount = Decimal(str(value or "").strip())
    except Exception as exc:
        raise HTTPException(status_code=400, detail={"error": f"invalid_{field}"}) from exc
    if not amount.is_finite() or amount <= 0:
        raise HTTPException(status_code=400, detail={"error": f"invalid_{field}"})
    scale = Decimal(10) ** int(decimals)
    atomic = amount * scale
    if atomic != atomic.to_integral_value():
        raise HTTPException(status_code=400, detail={"error": f"{field}_precision_exceeded"})
    return int(atomic)


def _atomic_amount_to_display(value: int, decimals: int) -> str:
    return _decimal_text_exact(Decimal(int(value)) / (Decimal(10) ** int(decimals)))


def _topic_address(topic: Any) -> Optional[str]:
    text = str(topic or "").strip().lower()
    if not text.startswith("0x") or len(text) != 66:
        return None
    tail = text[-40:]
    try:
        int(tail, 16)
    except ValueError:
        return None
    return "0x" + tail


def _erc20_wallet_flow(receipt: Dict[str, Any], *, contract: str, wallet: str) -> Dict[str, Any]:
    contract_key = validate_evm_address(contract).lower()
    wallet_key = validate_evm_address(wallet).lower()
    incoming = 0
    outgoing = 0
    incoming_logs = 0
    outgoing_logs = 0
    for log in receipt.get("logs") or []:
        if not isinstance(log, dict) or bool(log.get("removed")):
            continue
        try:
            log_contract = validate_evm_address(str(log.get("address") or "")).lower()
        except ValueError:
            continue
        if log_contract != contract_key:
            continue
        topics = log.get("topics") if isinstance(log.get("topics"), list) else []
        if len(topics) < 3 or str(topics[0] or "").strip().lower() != _ERC20_TRANSFER_TOPIC:
            continue
        from_address = _topic_address(topics[1])
        to_address = _topic_address(topics[2])
        try:
            amount = int(str(log.get("data") or "0x0"), 16)
        except ValueError:
            continue
        if amount < 0:
            continue
        if from_address and from_address.lower() == wallet_key:
            outgoing += amount
            outgoing_logs += 1
        if to_address and to_address.lower() == wallet_key:
            incoming += amount
            incoming_logs += 1
    return {
        "incoming_atomic": incoming,
        "outgoing_atomic": outgoing,
        "net_incoming_atomic": incoming - outgoing,
        "net_outgoing_atomic": outgoing - incoming,
        "incoming_log_count": incoming_logs,
        "outgoing_log_count": outgoing_logs,
    }


def _decode_exact_approval_transaction(
    transaction: Dict[str, Any],
    *,
    wallet: str,
    token_contract: str,
    required_atomic: int,
) -> Dict[str, Any]:
    tx_from = validate_evm_address(str(transaction.get("from") or ""))
    tx_to = validate_evm_address(str(transaction.get("to") or ""))
    if tx_from.lower() != validate_evm_address(wallet).lower():
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_approval_wallet_mismatch"})
    if tx_to.lower() != validate_evm_address(token_contract).lower():
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_approval_token_mismatch"})
    tx_value = _decode_robinhood_chain_rpc_quantity(
        transaction.get("value") if transaction.get("value") is not None else "0",
        field="wallet_swap_reconcile_approval_value",
    )
    if tx_value != 0:
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_approval_value_mismatch"})
    data = str(transaction.get("input") or transaction.get("data") or "").strip().lower()
    if not data.startswith(_ERC20_APPROVE_SELECTOR) or len(data) < 138:
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_approval_calldata_invalid"})
    spender_word = data[10:74]
    amount_word = data[74:138]
    try:
        spender = validate_evm_address("0x" + spender_word[-40:])
        approved_atomic = int(amount_word, 16)
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_approval_calldata_invalid"}) from exc
    if approved_atomic != int(required_atomic):
        raise HTTPException(
            status_code=409,
            detail={
                "error": "wallet_swap_reconcile_approval_amount_mismatch",
                "expected_atomic": str(required_atomic),
                "actual_atomic": str(approved_atomic),
            },
        )
    return {
        "spender": spender,
        "approved_amount_atomic": str(approved_atomic),
        "calldata": data,
        "calldata_sha256": hashlib.sha256(bytes.fromhex(data[2:])).hexdigest(),
        "calldata_bytes": len(bytes.fromhex(data[2:])),
    }



def _generic_wallet_swap_sha256_text(value: Any) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _generic_wallet_swap_utc_naive(value: Any, *, fallback: Optional[datetime] = None) -> datetime:
    text = str(value or "").strip()
    if text:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
            return parsed
        except Exception:
            pass
    return fallback or datetime.utcnow()


def _generic_wallet_swap_contract(identity: Dict[str, Any]) -> str:
    if bool(identity.get("native")):
        return validate_evm_address(UNISWAP_NATIVE_TOKEN)
    return validate_evm_address(str(identity.get("registry_contract_address") or ""))


def _generic_wallet_swap_calldata_evidence(transaction: Dict[str, Any]) -> Tuple[str, int]:
    calldata = str(transaction.get("data") or transaction.get("input") or "").strip().lower()
    if not calldata.startswith("0x") or len(calldata) <= 2 or len(calldata[2:]) % 2:
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_prepared_calldata_invalid"})
    try:
        payload = bytes.fromhex(calldata[2:])
    except ValueError as exc:
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_prepared_calldata_invalid"}) from exc
    return hashlib.sha256(payload).hexdigest(), len(payload)


def _persist_generic_wallet_swap_prepared_lifecycle(
    db: Session,
    *,
    fresh_preflight: Dict[str, Any],
    handoff: Dict[str, Any],
    capability_token: str,
    capability: Dict[str, Any],
    slippage_bps: int,
    approval_tx_hash: Optional[str],
) -> Tuple[RobinhoodChainSwapExecution, bool]:
    """Persist the generic exact-input owner before a browser wallet request.

    The complete accepted preflight/handoff remains in route JSON. Required ORM
    convenience fields are populated from the same capability evidence so a
    later MetaMask hash can be attached without reconstructing the plan.
    """
    wallet = _resolve_robinhood_chain_execution_taker(
        db,
        str(capability.get("wallet_address") or handoff.get("wallet_address") or ""),
    )
    symbol = str(capability.get("symbol") or fresh_preflight.get("symbol") or "").strip().upper().replace("/", "-").replace("_", "-")
    side = str(capability.get("side") or fresh_preflight.get("side") or "").strip().lower()
    if not symbol or side not in {"buy", "sell"}:
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_prepared_identity_invalid"})

    registry_service = get_robinhood_chain_registry_discovery_service()
    try:
        input_identity = registry_service.resolve_verified_token(db, str(capability.get("input_asset") or ""))
        output_identity = registry_service.resolve_verified_token(db, str(capability.get("output_asset") or ""))
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={"error": "wallet_swap_prepared_verified_registry_identity_required", "message": str(exc)},
        ) from exc
    if (
        int(input_identity.get("registry_id") or 0) != int(capability.get("input_registry_id") or 0)
        or int(output_identity.get("registry_id") or 0) != int(capability.get("output_registry_id") or 0)
    ):
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_prepared_registry_identity_changed"})

    # The signed wallet capability is an authorization/identity envelope and does
    # not duplicate every reviewed economic field.  The validated fresh handoff
    # remains the authoritative source for expected/minimum output, while Token
    # Registry decimals provide deterministic atomic conversion.  Any economic
    # duplicates that are present in the capability must still match exactly.
    input_decimals = int(input_identity.get("decimals"))
    output_decimals = int(output_identity.get("decimals"))
    input_amount = str(
        handoff.get("input_amount")
        or capability.get("input_amount")
        or capability.get("requested_amount")
        or fresh_preflight.get("requested_amount")
        or ""
    ).strip()
    output_amount = str(handoff.get("output_amount") or capability.get("output_amount") or "").strip()
    minimum_amount = str(handoff.get("minimum_received") or capability.get("minimum_received") or "").strip()
    if not all([input_amount, output_amount, minimum_amount]):
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_prepared_economics_missing"})

    input_atomic = str(
        _display_amount_to_atomic(
            input_amount,
            input_decimals,
            field="wallet_swap_prepared_input_amount",
        )
    )
    output_atomic = str(
        _display_amount_to_atomic(
            output_amount,
            output_decimals,
            field="wallet_swap_prepared_output_amount",
        )
    )
    minimum_atomic = str(
        _display_amount_to_atomic(
            minimum_amount,
            output_decimals,
            field="wallet_swap_prepared_minimum_received",
        )
    )

    capability_display_checks = (
        ("input_amount", input_decimals, input_atomic),
        ("requested_amount", input_decimals, input_atomic),
        ("output_amount", output_decimals, output_atomic),
        ("minimum_received", output_decimals, minimum_atomic),
    )
    for field_name, decimals, expected_atomic in capability_display_checks:
        capability_value = str(capability.get(field_name) or "").strip()
        if not capability_value:
            continue
        try:
            capability_atomic = _display_amount_to_atomic(
                capability_value,
                decimals,
                field=f"wallet_swap_prepared_capability_{field_name}",
            )
        except HTTPException as exc:
            raise HTTPException(
                status_code=409,
                detail={"error": "wallet_swap_prepared_economics_mismatch", "field": field_name},
            ) from exc
        if str(capability_atomic) != expected_atomic:
            raise HTTPException(
                status_code=409,
                detail={"error": "wallet_swap_prepared_economics_mismatch", "field": field_name},
            )

    capability_atomic_checks = (
        ("input_amount_atomic", input_atomic),
        ("output_amount_atomic", output_atomic),
        ("minimum_received_atomic", minimum_atomic),
    )
    for field_name, expected_atomic in capability_atomic_checks:
        capability_value = str(capability.get(field_name) or "").strip()
        if capability_value and capability_value != expected_atomic:
            raise HTTPException(
                status_code=409,
                detail={"error": "wallet_swap_prepared_economics_mismatch", "field": field_name},
            )

    transaction = handoff.get("transaction") if isinstance(handoff.get("transaction"), dict) else {}
    destination = validate_evm_address(str(transaction.get("to") or ""))
    calldata_sha, calldata_bytes = _generic_wallet_swap_calldata_evidence(transaction)
    tx_value_wei = str(transaction.get("value_wei") or "0").strip() or "0"
    gas_limit = str(transaction.get("gas_limit") or "0").strip() or "0"
    gas_price_wei = str(
        transaction.get("gas_price")
        or transaction.get("max_fee_per_gas")
        or handoff.get("gas_price_wei")
        or "0"
    ).strip() or "0"

    capability_sha = _generic_wallet_swap_sha256_text(capability_token)
    quote_id_source = str(handoff.get("quote_id") or fresh_preflight.get("quote_id") or "").strip()
    quote_id = quote_id_source or _generic_wallet_swap_sha256_text("generic-wallet-quote:" + capability_sha)
    swap_plan_hash = str(handoff.get("plan_hash") or "").strip() or _generic_wallet_swap_sha256_text(
        "|".join([
            "generic-wallet-plan-v1", wallet.lower(), symbol, side, input_atomic, output_atomic,
            minimum_atomic, destination.lower(), calldata_sha, capability_sha,
        ])
    )
    normalized_approval_hash = validate_transaction_hash(approval_tx_hash).lower() if approval_tx_hash else None
    approval_plan_hash = _generic_wallet_swap_sha256_text(
        "generic-wallet-approval:" + str(normalized_approval_hash or "not-required") + ":" + input_atomic
    )

    prepared_at = _generic_wallet_swap_utc_naive(
        handoff.get("fetched_at") or handoff.get("prepared_at") or fresh_preflight.get("prepared_at")
    )
    expires_at = _generic_wallet_swap_utc_naive(
        handoff.get("plan_expires_at") or handoff.get("expires_at") or fresh_preflight.get("expires_at"),
        fallback=prepared_at + timedelta(seconds=max(1, int(fresh_preflight.get("ttl_seconds") or 60))),
    )
    if expires_at <= prepared_at:
        expires_at = prepared_at + timedelta(seconds=max(1, int(fresh_preflight.get("ttl_seconds") or 60)))

    existing = db.query(RobinhoodChainSwapExecution).filter(RobinhoodChainSwapExecution.quote_id == quote_id).first()
    if existing is not None:
        existing_route = existing.route if isinstance(existing.route, dict) else {}
        durable = existing_route.get("generic_wallet_lifecycle") if isinstance(existing_route.get("generic_wallet_lifecycle"), dict) else {}
        if durable.get("swap_capability_sha256") != capability_sha:
            raise HTTPException(status_code=409, detail={"error": "wallet_swap_prepared_quote_id_conflict"})
        return existing, False

    input_native = bool(input_identity.get("native"))
    output_native = bool(output_identity.get("native"))
    input_contract = _generic_wallet_swap_contract(input_identity)
    output_contract = _generic_wallet_swap_contract(output_identity)
    approval_required = bool(handoff.get("approval_required"))
    if approval_required:
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_prepared_approval_still_required"})

    allowance_spender_raw = str(
        handoff.get("allowance_spender")
        or handoff.get("spender")
        or transaction.get("allowance_spender")
        or destination
    ).strip()
    allowance_spender = validate_evm_address(allowance_spender_raw)
    exact_allowance = str(
        handoff.get("allowance_current_atomic")
        or handoff.get("current_allowance_atomic")
        or ""
    ).strip()
    allowance_current = "0" if input_native else (exact_allowance if exact_allowance.isdigit() else input_atomic)
    allowance_semantics = "not_applicable_native" if input_native else (
        "exact_preflight" if exact_allowance.isdigit() else "known_sufficient_lower_bound"
    )

    now = datetime.utcnow()
    route = {
        "provider": UNISWAP_PROVIDER,
        "generic_wallet_lifecycle": {
            "version": "RH-ORDER.MISS.1B",
            "durable_before_wallet_request": True,
            "historical_preflight_available": True,
            "swap_capability_sha256": capability_sha,
            "quote_id_source": "provider_preflight" if quote_id_source else "capability_sha256_identity",
            "allowance_current_model_semantics": allowance_semantics,
            "source_preflight": copy.deepcopy(fresh_preflight),
            "wallet_request": copy.deepcopy(handoff),
        },
        "execution_lifecycle": {
            "approval": {
                "tx_hash": normalized_approval_hash,
                "status": "confirmed" if normalized_approval_hash else "not_required",
            },
            "swap": {
                "prepared_at": prepared_at.isoformat(),
                "tx_hash": None,
                "submission_recorded": False,
            },
        },
    }
    row = RobinhoodChainSwapExecution(
        chain_id=_EXPECTED_CHAIN_ID_DECIMAL,
        wallet_address=wallet,
        provider=UNISWAP_PROVIDER,
        symbol=symbol,
        side=side,
        from_asset=str(capability.get("input_asset") or "").strip().upper(),
        from_contract_address=input_contract,
        from_decimals=int(input_identity.get("decimals")),
        from_native=input_native,
        to_asset=str(capability.get("output_asset") or "").strip().upper(),
        to_contract_address=output_contract,
        to_decimals=int(output_identity.get("decimals")),
        to_native=output_native,
        amount_mode="exact_input",
        exact_input_amount=input_amount,
        exact_input_amount_atomic=input_atomic,
        expected_output_amount=output_amount,
        expected_output_amount_atomic=output_atomic,
        minimum_output_amount=minimum_amount,
        minimum_output_amount_atomic=minimum_atomic,
        slippage_bps=int(slippage_bps),
        quote_id=quote_id,
        plan_fetched_at=prepared_at,
        plan_expires_at=expires_at,
        allowance_read_method="not_applicable" if input_native else "eth_call",
        allowance_token_address=input_contract,
        allowance_spender=allowance_spender,
        allowance_current_atomic=allowance_current,
        allowance_required_atomic="0" if input_native else input_atomic,
        allowance_shortfall_atomic="0",
        approval_required=False,
        approval_amount="0" if input_native else input_amount,
        approval_amount_atomic="0" if input_native else input_atomic,
        approval_plan_hash=approval_plan_hash,
        approval_transaction_to=input_contract,
        approval_transaction_value_wei="0",
        approval_calldata_sha256=hashlib.sha256(b"").hexdigest(),
        approval_calldata_bytes=0,
        approval_gas_limit="0",
        approval_gas_price_wei="0",
        approval_status="confirmed" if normalized_approval_hash else "not_required",
        approval_tx_hash=normalized_approval_hash,
        swap_plan_hash=swap_plan_hash,
        swap_transaction_to=destination,
        swap_transaction_value_wei=tx_value_wei,
        swap_calldata_sha256=calldata_sha,
        swap_calldata_bytes=calldata_bytes,
        swap_gas_limit=gas_limit,
        swap_gas_price_wei=gas_price_wei,
        swap_status="prepared",
        swap_tx_hash=None,
        route=route,
        status="swap_prepared",
        error_code=None,
        error_message=None,
        created_at=now,
        updated_at=now,
    )
    db.add(row)
    db.flush()
    return row, True


def _record_generic_wallet_swap_submission(
    db: Session,
    *,
    execution_id: str,
    tx_hash: str,
    capability_token: str,
    capability: Dict[str, Any],
) -> Tuple[RobinhoodChainSwapExecution, bool]:
    execution_key = str(execution_id or "").strip()
    if not execution_key:
        raise HTTPException(status_code=400, detail={"error": "wallet_swap_execution_id_required"})
    row = db.get(RobinhoodChainSwapExecution, execution_key)
    if row is None:
        raise HTTPException(status_code=404, detail={"error": "wallet_swap_execution_not_found"})
    route = row.route if isinstance(row.route, dict) else {}
    durable = route.get("generic_wallet_lifecycle") if isinstance(route.get("generic_wallet_lifecycle"), dict) else {}
    if durable.get("version") != "RH-ORDER.MISS.1B" or durable.get("durable_before_wallet_request") is not True:
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_execution_not_generic_durable"})
    capability_sha = _generic_wallet_swap_sha256_text(capability_token)
    if durable.get("swap_capability_sha256") != capability_sha:
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_submission_capability_mismatch"})
    if str(row.wallet_address or "").strip().lower() != str(capability.get("wallet_address") or "").strip().lower():
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_submission_wallet_mismatch"})
    if str(row.symbol or "").strip().upper() != str(capability.get("symbol") or "").strip().upper():
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_submission_symbol_mismatch"})
    if str(row.side or "").strip().lower() != str(capability.get("side") or "").strip().lower():
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_submission_side_mismatch"})

    normalized_tx_hash = validate_transaction_hash(tx_hash).lower()
    owner = (
        db.query(RobinhoodChainSwapExecution)
        .filter(
            RobinhoodChainSwapExecution.swap_tx_hash == normalized_tx_hash,
            RobinhoodChainSwapExecution.id != row.id,
        )
        .first()
    )
    if owner is not None:
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_submission_hash_owned_by_other_execution"})
    current_hash = str(row.swap_tx_hash or "").strip().lower()
    if current_hash:
        if current_hash != normalized_tx_hash:
            raise HTTPException(status_code=409, detail={"error": "wallet_swap_submission_hash_conflict"})
        return row, False

    now = datetime.utcnow()
    lifecycle = route.get("execution_lifecycle") if isinstance(route.get("execution_lifecycle"), dict) else {}
    lifecycle = copy.deepcopy(lifecycle)
    swap_lifecycle = lifecycle.get("swap") if isinstance(lifecycle.get("swap"), dict) else {}
    swap_lifecycle = dict(swap_lifecycle)
    swap_lifecycle.update({
        "tx_hash": normalized_tx_hash,
        "submitted_at": now.isoformat(),
        "submission_recorded": True,
        "automatic_retry": False,
        "automatic_second_transaction": False,
    })
    lifecycle["swap"] = swap_lifecycle
    route = copy.deepcopy(route)
    route["execution_lifecycle"] = lifecycle
    row.route = route
    row.swap_tx_hash = normalized_tx_hash
    row.swap_status = "submitted"
    row.status = "swap_pending"
    row.updated_at = now
    db.flush()
    return row, True


def _decode_generic_wallet_swap_receipt_capability(
    db: Session,
    *,
    capability_token: str,
    execution_id: Optional[str],
    tx_hash: str,
) -> Tuple[Dict[str, Any], bool]:
    """Decode receipt authority without extending pre-broadcast send authority.

    Fresh capabilities follow the original strict TTL path. An expired
    capability is accepted only when its exact signed token and transaction
    hash are already bound to the durable RH-ORDER.MISS.1B execution owner.
    """
    try:
        return decode_wallet_swap_capability(capability_token), False
    except ValueError as exc:
        if str(exc) != "wallet_swap_capability_expired":
            raise

    execution_key = str(execution_id or "").strip()
    if not execution_key:
        raise ValueError("wallet_swap_expired_capability_execution_id_required")

    row = db.get(RobinhoodChainSwapExecution, execution_key)
    if row is None:
        raise ValueError("wallet_swap_expired_capability_execution_not_found")

    normalized_tx_hash = validate_transaction_hash(tx_hash).lower()
    route = row.route if isinstance(row.route, dict) else {}
    durable = route.get("generic_wallet_lifecycle") if isinstance(route.get("generic_wallet_lifecycle"), dict) else {}
    if durable.get("version") != "RH-ORDER.MISS.1B" or durable.get("durable_before_wallet_request") is not True:
        raise ValueError("wallet_swap_expired_capability_execution_not_durable")

    capability_sha = _generic_wallet_swap_sha256_text(capability_token)
    if str(durable.get("swap_capability_sha256") or "") != capability_sha:
        raise ValueError("wallet_swap_expired_capability_capability_mismatch")

    if str(row.swap_tx_hash or "").strip().lower() != normalized_tx_hash:
        raise ValueError("wallet_swap_expired_capability_hash_not_owned")

    lifecycle = route.get("execution_lifecycle") if isinstance(route.get("execution_lifecycle"), dict) else {}
    swap_lifecycle = lifecycle.get("swap") if isinstance(lifecycle.get("swap"), dict) else {}
    reconciliation = route.get("execution_reconciliation") if isinstance(route.get("execution_reconciliation"), dict) else {}
    row_status = str(row.status or "").strip().lower()
    if row_status == "swap_pending":
        if (
            swap_lifecycle.get("submission_recorded") is not True
            or str(swap_lifecycle.get("tx_hash") or "").strip().lower() != normalized_tx_hash
        ):
            raise ValueError("wallet_swap_expired_capability_submission_not_recorded")
    elif row_status == "confirmed":
        if (
            reconciliation.get("reconciled") is not True
            or str(reconciliation.get("swap_tx_hash") or "").strip().lower() != normalized_tx_hash
        ):
            raise ValueError("wallet_swap_expired_capability_confirmation_not_owned")
    elif row_status == "swap_reverted":
        if str(swap_lifecycle.get("tx_hash") or "").strip().lower() != normalized_tx_hash:
            raise ValueError("wallet_swap_expired_capability_revert_not_owned")
    else:
        raise ValueError("wallet_swap_expired_capability_execution_state_invalid")

    capability = decode_wallet_swap_capability(
        capability_token,
        allow_expired_for_bound_receipt=True,
    )

    plan_checks = (
        (int(row.chain_id or 0), int(capability.get("chain_id") or 0)),
        (str(row.wallet_address or "").strip().lower(), str(capability.get("wallet_address") or "").strip().lower()),
        (str(row.symbol or "").strip().upper(), str(capability.get("symbol") or "").strip().upper()),
        (str(row.side or "").strip().lower(), str(capability.get("side") or "").strip().lower()),
        (str(row.from_asset or "").strip().upper(), str(capability.get("input_asset") or "").strip().upper()),
        (str(row.to_asset or "").strip().upper(), str(capability.get("output_asset") or "").strip().upper()),
        (str(row.exact_input_amount_atomic or ""), str(capability.get("input_amount_atomic") or "")),
        (str(row.minimum_output_amount_atomic or ""), str(capability.get("minimum_received_atomic") or "")),
        (str(row.swap_transaction_to or "").strip().lower(), str(capability.get("transaction_to") or "").strip().lower()),
        (str(row.swap_transaction_value_wei or "0"), str(capability.get("transaction_value_wei") or "0")),
        (str(row.swap_calldata_sha256 or "").strip().lower(), str(capability.get("calldata_sha256") or "").strip().lower()),
    )
    if any(actual != expected for actual, expected in plan_checks):
        raise ValueError("wallet_swap_expired_capability_durable_plan_mismatch")

    return capability, True


async def _reconcile_pending_generic_wallet_swaps_for_sync_load(
    db: Session,
    *,
    limit: int = 50,
) -> Dict[str, Any]:
    """Best-effort receipt reconciliation for durable submitted UTT swaps.

    All Orders Sync+Load already invokes the Robinhood Chain incremental wallet
    path.  Known UTT lifecycle hashes are deliberately excluded from external
    wallet materialization, so a submitted generic UTT swap must be reconciled
    through its durable RobinhoodChainSwapExecution owner instead.

    This scanner never prepares, signs, broadcasts, retries, or creates a second
    wallet request. It only inspects rows that already own a submitted tx hash.
    """
    bounded_limit = max(1, min(int(limit or 50), 100))
    rows = (
        db.query(RobinhoodChainSwapExecution)
        .filter(
            RobinhoodChainSwapExecution.status == "swap_pending",
            RobinhoodChainSwapExecution.swap_tx_hash.is_not(None),
        )
        .order_by(RobinhoodChainSwapExecution.updated_at.asc())
        .limit(bounded_limit)
        .all()
    )

    result: Dict[str, Any] = {
        "ok": True,
        "tranche": "RH-RECEIPT.EXPIRY.1B",
        "source": "wallet_sync_incremental",
        "candidate_count": len(rows),
        "inspected": 0,
        "still_pending": 0,
        "confirmed": 0,
        "reverted": 0,
        "skipped_not_durable": 0,
        "errors": [],
        "order_mutation": False,
        "database_mutation": False,
        "wallet_request": False,
        "signing": False,
        "broadcast": False,
        "automatic_retry": False,
        "automatic_second_transaction": False,
    }
    if not rows:
        return result

    rpc = get_robinhood_chain_client()
    try:
        chain = await rpc.verify_expected_chain(force_refresh=True)
    except Exception as exc:
        result["ok"] = False
        result["errors"].append({
            "error": "wallet_sync_receipt_chain_read_failed",
            "message": str(exc),
        })
        return result
    if chain.get("ok") is not True or chain.get("chain_id_matches") is not True:
        result["ok"] = False
        result["errors"].append({
            "error": "wallet_sync_receipt_chain_mismatch",
        })
        return result

    for row in rows:
        result["inspected"] += 1
        execution_id = str(row.id)
        tx_hash_text = str(row.swap_tx_hash or "").strip()
        try:
            tx_hash = validate_transaction_hash(tx_hash_text).lower()
        except ValueError as exc:
            result["errors"].append({
                "execution_id": execution_id,
                "error": str(exc),
            })
            continue

        route = row.route if isinstance(row.route, dict) else {}
        durable = route.get("generic_wallet_lifecycle") if isinstance(route.get("generic_wallet_lifecycle"), dict) else {}
        lifecycle = route.get("execution_lifecycle") if isinstance(route.get("execution_lifecycle"), dict) else {}
        swap_lifecycle = lifecycle.get("swap") if isinstance(lifecycle.get("swap"), dict) else {}
        if durable.get("version") != "RH-ORDER.MISS.1B" or durable.get("durable_before_wallet_request") is not True:
            result["skipped_not_durable"] += 1
            continue
        if (
            swap_lifecycle.get("submission_recorded") is not True
            or str(swap_lifecycle.get("tx_hash") or "").strip().lower() != tx_hash
        ):
            result["errors"].append({
                "execution_id": execution_id,
                "error": "wallet_sync_receipt_submission_not_recorded",
            })
            continue

        try:
            receipt_record = await rpc.rpc_read(
                "eth_getTransactionReceipt",
                [tx_hash],
                cache_namespace=None,
                force_refresh=True,
            )
        except Exception as exc:
            result["errors"].append({
                "execution_id": execution_id,
                "error": "wallet_sync_receipt_read_failed",
                "message": str(exc),
            })
            continue
        if receipt_record.get("ok") is not True:
            result["errors"].append({
                "execution_id": execution_id,
                "error": "wallet_sync_receipt_read_failed",
            })
            continue
        receipt = receipt_record.get("result")
        if receipt is None:
            result["still_pending"] += 1
            continue
        if not isinstance(receipt, dict):
            result["errors"].append({
                "execution_id": execution_id,
                "error": "wallet_sync_receipt_invalid",
            })
            continue

        try:
            receipt_status = _decode_robinhood_chain_rpc_quantity(
                receipt.get("status"),
                field="wallet_sync_receipt_status",
            )
        except Exception as exc:
            result["errors"].append({
                "execution_id": execution_id,
                "error": "wallet_sync_receipt_status_invalid",
                "message": str(exc),
            })
            continue

        try:
            tx_record = await rpc.rpc_read(
                "eth_getTransactionByHash",
                [tx_hash],
                cache_namespace=None,
                force_refresh=True,
            )
        except Exception as exc:
            result["errors"].append({
                "execution_id": execution_id,
                "error": "wallet_sync_transaction_read_failed",
                "message": str(exc),
            })
            continue
        transaction = tx_record.get("result") if tx_record.get("ok") is True else None
        if not isinstance(transaction, dict):
            result["errors"].append({
                "execution_id": execution_id,
                "error": "wallet_sync_transaction_read_failed",
            })
            continue

        durable_transaction = {
            "transaction_from": str(row.wallet_address or ""),
            "transaction_to": str(row.swap_transaction_to or ""),
            "transaction_value_wei": str(row.swap_transaction_value_wei or "0"),
            "calldata_sha256": str(row.swap_calldata_sha256 or ""),
        }
        try:
            validate_wallet_swap_transaction(
                durable_transaction,
                tx_hash=tx_hash,
                transaction=transaction,
            )
        except ValueError as exc:
            result["errors"].append({
                "execution_id": execution_id,
                "error": "wallet_sync_durable_transaction_mismatch",
                "message": str(exc),
            })
            continue

        if receipt_status == 0:
            try:
                block_number = (
                    _decode_robinhood_chain_rpc_quantity(
                        receipt.get("blockNumber"),
                        field="wallet_sync_reverted_block_number",
                    )
                    if receipt.get("blockNumber") is not None
                    else None
                )
                gas_used = (
                    _decode_robinhood_chain_rpc_quantity(
                        receipt.get("gasUsed"),
                        field="wallet_sync_reverted_gas_used",
                    )
                    if receipt.get("gasUsed") is not None
                    else None
                )
                effective_gas_price = (
                    _decode_robinhood_chain_rpc_quantity(
                        receipt.get("effectiveGasPrice"),
                        field="wallet_sync_reverted_effective_gas_price",
                    )
                    if receipt.get("effectiveGasPrice") is not None
                    else None
                )
                with db.begin_nested():
                    changed = _mark_generic_wallet_swap_reverted(
                        db,
                        execution_id=execution_id,
                        tx_hash=tx_hash,
                        block_number=block_number,
                        gas_used=gas_used,
                        effective_gas_price=effective_gas_price,
                    )
                if changed:
                    result["reverted"] += 1
                    result["database_mutation"] = True
                else:
                    result["reverted"] += 1
            except Exception as exc:
                result["errors"].append({
                    "execution_id": execution_id,
                    "error": "wallet_sync_revert_reconciliation_failed",
                    "message": str(getattr(exc, "detail", None) or exc),
                })
            continue

        if receipt_status != 1:
            result["errors"].append({
                "execution_id": execution_id,
                "error": "wallet_sync_receipt_status_invalid",
                "receipt_status": receipt_status,
            })
            continue

        try:
            with db.begin_nested():
                reconciliation = await _persist_generic_wallet_swap_reconciliation(
                    db,
                    tx_hash=tx_hash,
                    symbol=str(row.symbol or ""),
                    side=str(row.side or ""),
                    requested_amount=str(row.exact_input_amount or ""),
                    quoted_output_amount=str(row.expected_output_amount or ""),
                    minimum_received=str(row.minimum_output_amount or ""),
                    approval_tx_hash=str(row.approval_tx_hash or "").strip() or None,
                    source="wallet_sync_incremental_receipt",
                    historical_preflight_available=(
                        durable.get("historical_preflight_available") is not False
                    ),
                )
            if reconciliation.get("ok") is True:
                result["confirmed"] += 1
                result["order_mutation"] = bool(
                    result["order_mutation"]
                    or reconciliation.get("order_mutation")
                )
                result["database_mutation"] = bool(
                    result["database_mutation"]
                    or reconciliation.get("database_mutation")
                )
            else:
                result["errors"].append({
                    "execution_id": execution_id,
                    "error": "wallet_sync_receipt_reconciliation_not_ok",
                })
        except Exception as exc:
            result["errors"].append({
                "execution_id": execution_id,
                "error": "wallet_sync_receipt_reconciliation_failed",
                "message": str(getattr(exc, "detail", None) or exc),
            })

    result["error_count"] = len(result["errors"])
    return result


def _mark_generic_wallet_swap_reverted(
    db: Session,
    *,
    execution_id: Optional[str],
    tx_hash: str,
    block_number: Optional[int],
    gas_used: Optional[int],
    effective_gas_price: Optional[int],
) -> bool:
    normalized_tx_hash = validate_transaction_hash(tx_hash).lower()
    row = db.get(RobinhoodChainSwapExecution, str(execution_id or "").strip()) if execution_id else None
    if row is None:
        row = db.query(RobinhoodChainSwapExecution).filter(RobinhoodChainSwapExecution.swap_tx_hash == normalized_tx_hash).first()
    if row is None or str(row.swap_tx_hash or "").strip().lower() != normalized_tx_hash:
        return False
    if row.status == "swap_reverted" and row.swap_status == "reverted":
        return False
    now = datetime.utcnow()
    route = copy.deepcopy(row.route if isinstance(row.route, dict) else {})
    lifecycle = route.get("execution_lifecycle") if isinstance(route.get("execution_lifecycle"), dict) else {}
    lifecycle = copy.deepcopy(lifecycle)
    swap = lifecycle.get("swap") if isinstance(lifecycle.get("swap"), dict) else {}
    swap = dict(swap)
    swap.update({
        "tx_hash": normalized_tx_hash,
        "receipt_status": 0,
        "reverted_at": now.isoformat(),
        "block_number": block_number,
        "gas_used": str(gas_used) if gas_used is not None else None,
        "effective_gas_price_wei": str(effective_gas_price) if effective_gas_price is not None else None,
    })
    lifecycle["swap"] = swap
    route["execution_lifecycle"] = lifecycle
    row.route = route
    row.swap_status = "reverted"
    row.status = "swap_reverted"
    row.updated_at = now
    db.flush()
    return True


def _rh_order_miss_1b_target_hash_guard(tx_hash: str, approval_tx_hash: str) -> Tuple[str, str]:
    normalized_tx_hash = validate_transaction_hash(tx_hash).lower()
    normalized_approval_hash = validate_transaction_hash(approval_tx_hash).lower()
    if _generic_wallet_swap_sha256_text(normalized_tx_hash) != _RH_ORDER_MISS_1B_TARGET_TX_SHA256:
        raise HTTPException(status_code=409, detail={"error": "rh_order_miss_1b_target_tx_mismatch"})
    if _generic_wallet_swap_sha256_text(normalized_approval_hash) != _RH_ORDER_MISS_1B_TARGET_APPROVAL_SHA256:
        raise HTTPException(status_code=409, detail={"error": "rh_order_miss_1b_target_approval_mismatch"})
    return normalized_tx_hash, normalized_approval_hash


def _rh_order_miss_1b_wallet_evidence(db: Session, tx_hash: str) -> Dict[str, Any]:
    wallet = (
        db.query(WalletAddress)
        .filter(
            WalletAddress.asset == "ALL",
            WalletAddress.network == _TOKEN_REGISTRY_CHAIN,
            WalletAddress.wallet_id == _TOKEN_REGISTRY_VENUE,
        )
        .order_by(WalletAddress.created_at.desc())
        .first()
    )
    if wallet is None:
        raise HTTPException(status_code=409, detail={"error": "rh_order_miss_1b_wallet_missing"})
    rows = (
        db.query(RobinhoodChainWalletEvent)
        .filter(
            RobinhoodChainWalletEvent.wallet_address_id == wallet.id,
            RobinhoodChainWalletEvent.transaction_hash == tx_hash,
        )
        .all()
    )
    positive = []
    for row in rows:
        try:
            atomic = int(str(row.amount_atomic or "0"))
        except Exception:
            atomic = 0
        if atomic <= 0:
            continue
        positive.append({
            "event_type": str(row.event_type or ""),
            "direction": str(row.direction or "").strip().lower(),
            "asset": str(row.asset or "").strip().upper(),
            "amount_atomic": str(atomic),
            "decimals": int(row.decimals) if row.decimals is not None else None,
            "registered": row.registered is True,
            "registry_id": int(row.registry_id) if row.registry_id is not None else None,
            "tx_time": row.tx_time.isoformat() if row.tx_time is not None else None,
        })
    expected_usdg = [item for item in positive if item["direction"] == "out" and item["asset"] == "USDG" and item["amount_atomic"] == "2000000"]
    expected_index = [item for item in positive if item["direction"] == "in" and item["asset"] == "INDEX" and item["amount_atomic"] == "273765361829109662072"]
    if len(expected_usdg) != 1 or len(expected_index) != 1:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "rh_order_miss_1b_wallet_evidence_mismatch",
                "event_rows": len(rows),
                "positive_rows": len(positive),
                "usdg_out_matches": len(expected_usdg),
                "index_in_matches": len(expected_index),
            },
        )
    lifecycle_count = db.query(RobinhoodChainSwapExecution).filter(RobinhoodChainSwapExecution.swap_tx_hash == tx_hash).count()
    external_count = (
        db.query(RobinhoodChainExternalSwap)
        .filter(
            RobinhoodChainExternalSwap.wallet_address_id == wallet.id,
            RobinhoodChainExternalSwap.transaction_hash == tx_hash,
        )
        .count()
    )
    return {
        "wallet_address_id": str(wallet.id),
        "event_rows": len(rows),
        "positive_rows": len(positive),
        "usdg_out_matches": len(expected_usdg),
        "index_in_matches": len(expected_index),
        "lifecycle_owner_count": int(lifecycle_count),
        "external_owner_count": int(external_count),
        "target_tx_time": expected_index[0].get("tx_time") or expected_usdg[0].get("tx_time"),
    }


async def _persist_generic_wallet_swap_reconciliation(
    db: Session,
    *,
    tx_hash: str,
    symbol: str,
    side: str,
    requested_amount: str,
    quoted_output_amount: Optional[str],
    minimum_received: Optional[str],
    approval_tx_hash: Optional[str] = None,
    source: str = "wallet_swap_receipt",
    historical_preflight_available: bool = True,
    expected_actual_output_amount: Optional[str] = None,
    persist: bool = True,
) -> Dict[str, Any]:
    normalized_tx_hash = validate_transaction_hash(tx_hash).lower()
    normalized_approval_hash = validate_transaction_hash(approval_tx_hash).lower() if approval_tx_hash else None
    normalized_symbol = str(symbol or "").strip().upper().replace("/", "-").replace("_", "-")
    normalized_side = str(side or "").strip().lower()
    if normalized_side not in {"buy", "sell"}:
        raise HTTPException(status_code=400, detail={"error": "wallet_swap_reconcile_side_invalid"})

    existing_row = (
        db.query(RobinhoodChainSwapExecution)
        .filter(RobinhoodChainSwapExecution.swap_tx_hash == normalized_tx_hash)
        .first()
    )
    if existing_row is not None:
        existing_route = existing_row.route if isinstance(existing_row.route, dict) else {}
        existing_reconciliation = existing_route.get("execution_reconciliation")
        if (
            existing_row.status == "confirmed"
            and isinstance(existing_reconciliation, dict)
            and existing_reconciliation.get("reconciled") is True
        ):
            if str(existing_row.symbol or "").strip().upper() != normalized_symbol:
                raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_existing_symbol_mismatch"})
            if str(existing_row.side or "").strip().lower() != normalized_side:
                raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_existing_side_mismatch"})
            existing_historical_preflight = existing_reconciliation.get("historical_preflight_available") is not False
            try:
                if Decimal(str(existing_row.exact_input_amount)) != Decimal(str(requested_amount)):
                    raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_existing_input_mismatch"})
                if existing_historical_preflight:
                    if Decimal(str(existing_row.expected_output_amount)) != Decimal(str(quoted_output_amount)):
                        raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_existing_quote_mismatch"})
                    if Decimal(str(existing_row.minimum_output_amount)) != Decimal(str(minimum_received)):
                        raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_existing_minimum_mismatch"})
                elif expected_actual_output_amount is not None:
                    if Decimal(str(existing_reconciliation.get("output_amount"))) != Decimal(str(expected_actual_output_amount)):
                        raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_existing_actual_output_mismatch"})
            except HTTPException:
                raise
            except Exception as exc:
                raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_existing_amount_invalid"}) from exc
            existing_approval_hash = str(existing_row.approval_tx_hash or "").strip().lower() or None
            if normalized_approval_hash is not None and existing_approval_hash != normalized_approval_hash:
                raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_existing_approval_hash_mismatch"})
            return {
                "ok": True,
                "tranche": "R5C.5D.2F.4",
                "created": False,
                "idempotent": True,
                "already_reconciled": True,
                "execution_id": str(existing_row.id),
                "symbol": normalized_symbol,
                "side": normalized_side,
                "tx_hash": normalized_tx_hash,
                "approval_tx_hash": existing_approval_hash,
                "receipt_status": 1,
                "block_number": existing_reconciliation.get("block_number"),
                "actual_input_asset": existing_reconciliation.get("input_asset"),
                "actual_input_amount": existing_reconciliation.get("input_amount"),
                "actual_input_amount_atomic": existing_reconciliation.get("input_amount_atomic"),
                "actual_output_asset": existing_reconciliation.get("output_asset"),
                "actual_output_amount": existing_reconciliation.get("output_amount"),
                "actual_output_amount_atomic": existing_reconciliation.get("output_amount_atomic"),
                "quoted_output_amount": existing_reconciliation.get("quoted_output_amount"),
                "minimum_received": existing_reconciliation.get("minimum_output_amount"),
                "minimum_received_satisfied": existing_reconciliation.get("minimum_received_satisfied"),
                "historical_preflight_available": existing_historical_preflight,
                "average_fill_price": existing_reconciliation.get("average_fill_price"),
                "swap_network_fee_wei": existing_reconciliation.get("swap_network_fee_wei"),
                "approval_network_fee_wei": existing_reconciliation.get("approval_network_fee_wei"),
                "total_network_fee_wei": existing_reconciliation.get("total_network_fee_wei"),
                "residual_allowance_atomic": existing_reconciliation.get("residual_allowance_atomic"),
                "order_mutation": False,
                "ledger_mutation": False,
                "fifo_mutation": False,
                "basis_mutation": False,
                "tax_mutation": False,
                "database_mutation": False,
            }

    saved_wallet = _resolve_robinhood_chain_execution_taker(db, None)
    registry_service = get_robinhood_chain_registry_discovery_service()
    try:
        market = registry_service.market_by_symbol(db, normalized_symbol)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_market_invalid", "message": str(exc)}) from exc
    base = market.get("base") if isinstance(market.get("base"), dict) else {}
    quote = market.get("quote") if isinstance(market.get("quote"), dict) else {}
    if normalized_symbol != f"{str(base.get('symbol') or '').upper()}-{str(quote.get('symbol') or '').upper()}":
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_market_identity_mismatch"})
    input_token = quote if normalized_side == "buy" else base
    output_token = base if normalized_side == "buy" else quote
    input_native = bool(input_token.get("native"))
    output_native = bool(output_token.get("native"))
    if output_native:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "wallet_swap_reconcile_native_output_not_supported",
                "message": (
                    "R5C.5D.2F.4-N1 supports exact-input native ETH spends "
                    "into an ERC-20 output. Native ETH output reconciliation "
                    "remains RH-WALLET.INGEST.1 because receipt-only net ETH "
                    "accounting must separate proceeds from gas."
                ),
            },
        )
    if input_native and normalized_approval_hash is not None:
        raise HTTPException(
            status_code=409,
            detail={"error": "wallet_swap_reconcile_native_input_approval_not_allowed"},
        )
    input_contract = (
        validate_evm_address(UNISWAP_NATIVE_TOKEN)
        if input_native
        else validate_evm_address(str(input_token.get("registry_contract_address") or ""))
    )
    output_contract = validate_evm_address(str(output_token.get("registry_contract_address") or ""))
    input_decimals = int(input_token.get("decimals"))
    output_decimals = int(output_token.get("decimals"))
    requested_atomic = _display_amount_to_atomic(requested_amount, input_decimals, field="wallet_swap_reconcile_requested_amount")
    if historical_preflight_available:
        quoted_output_atomic = _display_amount_to_atomic(quoted_output_amount, output_decimals, field="wallet_swap_reconcile_quoted_output")
        minimum_atomic = _display_amount_to_atomic(minimum_received, output_decimals, field="wallet_swap_reconcile_minimum_received")
    else:
        quoted_output_atomic = 0
        minimum_atomic = 0
    expected_actual_output_atomic = (
        _display_amount_to_atomic(expected_actual_output_amount, output_decimals, field="wallet_swap_reconcile_expected_actual_output")
        if expected_actual_output_amount is not None
        else None
    )

    rpc = get_robinhood_chain_client()
    chain = await rpc.verify_expected_chain(force_refresh=True)
    if chain.get("ok") is not True or chain.get("chain_id_matches") is not True:
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_rpc_chain_mismatch"})

    tx_record = await rpc.rpc_read("eth_getTransactionByHash", [normalized_tx_hash], cache_namespace=None, force_refresh=True)
    receipt_record = await rpc.rpc_read("eth_getTransactionReceipt", [normalized_tx_hash], cache_namespace=None, force_refresh=True)
    if tx_record.get("ok") is not True or not isinstance(tx_record.get("result"), dict):
        raise HTTPException(status_code=502, detail={"error": "wallet_swap_reconcile_transaction_read_failed"})
    if receipt_record.get("ok") is not True or not isinstance(receipt_record.get("result"), dict):
        raise HTTPException(status_code=502, detail={"error": "wallet_swap_reconcile_receipt_read_failed"})
    transaction = dict(tx_record["result"])
    receipt = dict(receipt_record["result"])
    if str(transaction.get("hash") or "").strip().lower() != normalized_tx_hash:
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_transaction_hash_mismatch"})
    tx_from = validate_evm_address(str(transaction.get("from") or ""))
    if tx_from.lower() != saved_wallet.lower():
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_wallet_mismatch"})
    tx_to = validate_evm_address(str(transaction.get("to") or ""))
    tx_value = _decode_robinhood_chain_rpc_quantity(
        transaction.get("value") if transaction.get("value") is not None else "0",
        field="wallet_swap_reconcile_transaction_value",
    )
    if input_native:
        if tx_value != requested_atomic:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "wallet_swap_reconcile_native_input_value_mismatch",
                    "expected_wei": str(requested_atomic),
                    "actual_wei": str(tx_value),
                },
            )
    elif tx_value != 0:
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_transaction_value_mismatch"})
    receipt_status = _decode_robinhood_chain_rpc_quantity(receipt.get("status"), field="wallet_swap_reconcile_receipt_status")
    if receipt_status != 1:
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_receipt_not_confirmed", "receipt_status": receipt_status})

    if input_native:
        input_flow = {
            "incoming_atomic": 0,
            "outgoing_atomic": 0,
            "net_incoming_atomic": 0,
            "net_outgoing_atomic": 0,
            "incoming_log_count": 0,
            "outgoing_log_count": 0,
        }
        actual_input_atomic = int(tx_value)
    else:
        input_flow = _erc20_wallet_flow(receipt, contract=input_contract, wallet=saved_wallet)
        actual_input_atomic = int(input_flow["net_outgoing_atomic"])
    output_flow = _erc20_wallet_flow(receipt, contract=output_contract, wallet=saved_wallet)
    actual_output_atomic = int(output_flow["net_incoming_atomic"])
    if actual_input_atomic != requested_atomic:
        raise HTTPException(
            status_code=409,
            detail={"error": "wallet_swap_reconcile_input_amount_mismatch", "expected_atomic": str(requested_atomic), "actual_atomic": str(actual_input_atomic)},
        )
    if actual_output_atomic <= 0:
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_output_missing"})
    if historical_preflight_available and actual_output_atomic < minimum_atomic:
        raise HTTPException(
            status_code=409,
            detail={"error": "wallet_swap_reconcile_output_below_minimum", "minimum_atomic": str(minimum_atomic), "actual_atomic": str(actual_output_atomic)},
        )
    if expected_actual_output_atomic is not None and actual_output_atomic != expected_actual_output_atomic:
        raise HTTPException(
            status_code=409,
            detail={"error": "wallet_swap_reconcile_actual_output_mismatch", "expected_atomic": str(expected_actual_output_atomic), "actual_atomic": str(actual_output_atomic)},
        )

    block_number = _decode_robinhood_chain_rpc_quantity(receipt.get("blockNumber"), field="wallet_swap_reconcile_block_number")
    block_tag = hex(block_number)
    block_record = await rpc.rpc_read("eth_getBlockByNumber", [block_tag, False], cache_namespace=None, force_refresh=True)
    if block_record.get("ok") is not True or not isinstance(block_record.get("result"), dict):
        raise HTTPException(status_code=502, detail={"error": "wallet_swap_reconcile_block_read_failed"})
    block_timestamp = _decode_robinhood_chain_rpc_quantity(block_record["result"].get("timestamp"), field="wallet_swap_reconcile_block_timestamp")
    confirmed_at = datetime.fromtimestamp(block_timestamp, tz=timezone.utc).replace(tzinfo=None)

    swap_gas_used = _decode_robinhood_chain_rpc_quantity(receipt.get("gasUsed"), field="wallet_swap_reconcile_gas_used")
    swap_effective_gas_price = _decode_robinhood_chain_rpc_quantity(receipt.get("effectiveGasPrice"), field="wallet_swap_reconcile_effective_gas_price")
    swap_fee_wei = swap_gas_used * swap_effective_gas_price
    swap_fee_eth = Decimal(swap_fee_wei) / _WEI_PER_ETH_DECIMAL
    swap_data = str(transaction.get("input") or transaction.get("data") or "").strip().lower()
    if not swap_data.startswith("0x") or len(swap_data) <= 2 or len(swap_data[2:]) % 2:
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_calldata_invalid"})
    try:
        swap_data_bytes = bytes.fromhex(swap_data[2:])
    except ValueError as exc:
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_calldata_invalid"}) from exc
    swap_calldata_sha = hashlib.sha256(swap_data_bytes).hexdigest()
    swap_gas_limit = _decode_robinhood_chain_rpc_quantity(transaction.get("gas"), field="wallet_swap_reconcile_gas_limit")

    approval_fee_wei = 0
    approval_gas_used = 0
    approval_gas_limit = 0
    approval_gas_price = 0
    approval_calldata = "0x"
    approval_calldata_sha = hashlib.sha256(b"").hexdigest()
    approval_calldata_bytes = 0
    spender = tx_to
    residual_allowance_atomic: Optional[str] = None
    if normalized_approval_hash:
        approval_tx_record = await rpc.rpc_read("eth_getTransactionByHash", [normalized_approval_hash], cache_namespace=None, force_refresh=True)
        approval_receipt_record = await rpc.rpc_read("eth_getTransactionReceipt", [normalized_approval_hash], cache_namespace=None, force_refresh=True)
        if approval_tx_record.get("ok") is not True or not isinstance(approval_tx_record.get("result"), dict):
            raise HTTPException(status_code=502, detail={"error": "wallet_swap_reconcile_approval_transaction_read_failed"})
        if approval_receipt_record.get("ok") is not True or not isinstance(approval_receipt_record.get("result"), dict):
            raise HTTPException(status_code=502, detail={"error": "wallet_swap_reconcile_approval_receipt_read_failed"})
        approval_transaction = dict(approval_tx_record["result"])
        approval_receipt = dict(approval_receipt_record["result"])
        approval_status = _decode_robinhood_chain_rpc_quantity(approval_receipt.get("status"), field="wallet_swap_reconcile_approval_receipt_status")
        if approval_status != 1:
            raise HTTPException(status_code=409, detail={"error": "wallet_swap_reconcile_approval_not_confirmed"})
        approval_evidence = _decode_exact_approval_transaction(
            approval_transaction,
            wallet=saved_wallet,
            token_contract=input_contract,
            required_atomic=requested_atomic,
        )
        spender = approval_evidence["spender"]
        approval_calldata = approval_evidence["calldata"]
        approval_calldata_sha = approval_evidence["calldata_sha256"]
        approval_calldata_bytes = int(approval_evidence["calldata_bytes"])
        approval_gas_used = _decode_robinhood_chain_rpc_quantity(approval_receipt.get("gasUsed"), field="wallet_swap_reconcile_approval_gas_used")
        approval_gas_price = _decode_robinhood_chain_rpc_quantity(approval_receipt.get("effectiveGasPrice"), field="wallet_swap_reconcile_approval_effective_gas_price")
        approval_fee_wei = approval_gas_used * approval_gas_price
        approval_gas_limit = _decode_robinhood_chain_rpc_quantity(approval_transaction.get("gas"), field="wallet_swap_reconcile_approval_gas_limit")
        allowance = await rpc.get_erc20_allowance(
            owner_address=saved_wallet,
            contract_address=input_contract,
            spender_address=spender,
            decimals=input_decimals,
            force_refresh=True,
        )
        if allowance.get("ok") is not True:
            raise HTTPException(status_code=502, detail={"error": "wallet_swap_reconcile_allowance_read_failed"})
        residual_allowance_atomic = str(allowance.get("allowance_atomic") or "0")

    actual_input = Decimal(actual_input_atomic) / (Decimal(10) ** input_decimals)
    actual_output = Decimal(actual_output_atomic) / (Decimal(10) ** output_decimals)
    average_fill_price = (
        actual_output / actual_input
        if normalized_side == "sell"
        else actual_input / actual_output
    )
    total_fee_wei = swap_fee_wei + approval_fee_wei
    total_fee_eth = Decimal(total_fee_wei) / _WEI_PER_ETH_DECIMAL
    approval_fee_eth = Decimal(approval_fee_wei) / _WEI_PER_ETH_DECIMAL
    now = datetime.utcnow()
    existing_route_base = copy.deepcopy(existing_row.route) if existing_row is not None and isinstance(existing_row.route, dict) else {}
    existing_generic = existing_route_base.get("generic_wallet_lifecycle") if isinstance(existing_route_base.get("generic_wallet_lifecycle"), dict) else {}
    preserve_durable_preflight = bool(
        historical_preflight_available
        and existing_row is not None
        and existing_generic.get("durable_before_wallet_request") is True
        and existing_generic.get("historical_preflight_available") is True
    )
    quote_id = (
        str(existing_row.quote_id)
        if preserve_durable_preflight
        else hashlib.sha256(("wallet-swap-reconcile:quote:" + normalized_tx_hash).encode("utf-8")).hexdigest()
    )
    swap_plan_hash = (
        str(existing_row.swap_plan_hash)
        if preserve_durable_preflight
        else hashlib.sha256(("wallet-swap-reconcile:plan:" + normalized_tx_hash + ":" + swap_calldata_sha).encode("utf-8")).hexdigest()
    )
    approval_plan_hash = (
        str(existing_row.approval_plan_hash)
        if preserve_durable_preflight
        else hashlib.sha256(("wallet-swap-reconcile:approval:" + str(normalized_approval_hash or "none") + ":" + approval_calldata_sha).encode("utf-8")).hexdigest()
    )
    reconciliation = {
        "version": "R5C.5D.2F.4",
        "reconciled": True,
        "reconciled_at": now.isoformat(),
        "reconciliation_mode": "receipt_transfer_logs",
        "source": source,
        "input_asset": str(input_token.get("symbol") or "").strip().upper(),
        "input_native": input_native,
        "input_amount_atomic": str(actual_input_atomic),
        "input_amount": _decimal_text_exact(actual_input),
        "output_asset": str(output_token.get("symbol") or "").strip().upper(),
        "output_native": output_native,
        "output_amount_atomic": str(actual_output_atomic),
        "output_amount": _decimal_text_exact(actual_output),
        "historical_preflight_available": bool(historical_preflight_available),
        "quoted_output_amount_atomic": str(quoted_output_atomic) if historical_preflight_available else None,
        "quoted_output_amount": _atomic_amount_to_display(quoted_output_atomic, output_decimals) if historical_preflight_available else None,
        "minimum_output_amount_atomic": str(minimum_atomic) if historical_preflight_available else None,
        "minimum_output_amount": _atomic_amount_to_display(minimum_atomic, output_decimals) if historical_preflight_available else None,
        "minimum_received_satisfied": (actual_output_atomic >= minimum_atomic) if historical_preflight_available else None,
        "average_fill_price": _decimal_text_exact(average_fill_price),
        "fee_asset": "ETH",
        "swap_network_fee_wei": str(swap_fee_wei),
        "swap_network_fee": _decimal_text_exact(swap_fee_eth),
        "approval_network_fee_wei": str(approval_fee_wei),
        "approval_network_fee": _decimal_text_exact(approval_fee_eth),
        "total_network_fee_wei": str(total_fee_wei),
        "total_network_fee": _decimal_text_exact(total_fee_eth),
        "residual_allowance_atomic": residual_allowance_atomic,
        "allowance_spender": spender,
        "input_transfer_log_count": int(input_flow["outgoing_log_count"] + input_flow["incoming_log_count"]),
        "output_transfer_log_count": int(output_flow["outgoing_log_count"] + output_flow["incoming_log_count"]),
        "approval_tx_hash": normalized_approval_hash,
        "swap_tx_hash": normalized_tx_hash,
        "block_number": block_number,
    }
    route = {
        **existing_route_base,
        "provider": UNISWAP_PROVIDER,
        "execution_reconciliation": reconciliation,
        "historical_preflight": {
            "available": bool(historical_preflight_available),
            "reason": None if historical_preflight_available else "original_preflight_lost_before_durable_lifecycle",
            "model_schema_placeholders": None if historical_preflight_available else {
                "expected_output_amount": "0",
                "expected_output_amount_atomic": "0",
                "minimum_output_amount": "0",
                "minimum_output_amount_atomic": "0",
                "plan_fetched_at": "confirmed_at",
                "plan_expires_at": "confirmed_at",
            },
        },
        "execution_lifecycle": {
            "approval": {
                "tx_hash": normalized_approval_hash,
                "confirmed_at": confirmed_at.isoformat() if normalized_approval_hash else None,
                "gas_used": str(approval_gas_used) if normalized_approval_hash else None,
                "effective_gas_price_wei": str(approval_gas_price) if normalized_approval_hash else None,
            },
            "swap": {
                "tx_hash": normalized_tx_hash,
                "submitted_at": confirmed_at.isoformat(),
                "confirmed_at": confirmed_at.isoformat(),
                "gas_used": str(swap_gas_used),
                "effective_gas_price_wei": str(swap_effective_gas_price),
            },
        },
    }

    if not persist:
        return {
            "ok": True,
            "tranche": "RH-ORDER.MISS.1B",
            "preview": True,
            "created": False,
            "idempotent": False,
            "execution_id": None,
            "symbol": normalized_symbol,
            "side": normalized_side,
            "tx_hash": normalized_tx_hash,
            "approval_tx_hash": normalized_approval_hash,
            "receipt_status": 1,
            "block_number": block_number,
            "actual_input_asset": reconciliation["input_asset"],
            "actual_input_amount": reconciliation["input_amount"],
            "actual_input_amount_atomic": str(actual_input_atomic),
            "actual_output_asset": reconciliation["output_asset"],
            "actual_output_amount": reconciliation["output_amount"],
            "actual_output_amount_atomic": str(actual_output_atomic),
            "quoted_output_amount": reconciliation["quoted_output_amount"],
            "minimum_received": reconciliation["minimum_output_amount"],
            "minimum_received_satisfied": reconciliation["minimum_received_satisfied"],
            "historical_preflight_available": bool(historical_preflight_available),
            "average_fill_price": reconciliation["average_fill_price"],
            "swap_network_fee_wei": str(swap_fee_wei),
            "approval_network_fee_wei": str(approval_fee_wei),
            "total_network_fee_wei": str(total_fee_wei),
            "residual_allowance_atomic": residual_allowance_atomic,
            "order_mutation": False,
            "ledger_mutation": False,
            "fifo_mutation": False,
            "basis_mutation": False,
            "tax_mutation": False,
            "database_mutation": False,
            "will_mutate": False,
        }

    row = db.query(RobinhoodChainSwapExecution).filter(RobinhoodChainSwapExecution.swap_tx_hash == normalized_tx_hash).first()
    created = row is None
    if row is None:
        row = RobinhoodChainSwapExecution()
        db.add(row)
    row.chain_id = _EXPECTED_CHAIN_ID_DECIMAL
    row.wallet_address = saved_wallet
    row.provider = UNISWAP_PROVIDER
    row.symbol = normalized_symbol
    row.side = normalized_side
    row.from_asset = reconciliation["input_asset"]
    row.from_contract_address = input_contract
    row.from_decimals = input_decimals
    row.from_native = input_native
    row.to_asset = reconciliation["output_asset"]
    row.to_contract_address = output_contract
    row.to_decimals = output_decimals
    row.to_native = output_native
    row.amount_mode = "exact_input"
    row.exact_input_amount = reconciliation["input_amount"]
    row.exact_input_amount_atomic = str(actual_input_atomic)
    row.expected_output_amount = _atomic_amount_to_display(quoted_output_atomic, output_decimals) if historical_preflight_available else "0"
    row.expected_output_amount_atomic = str(quoted_output_atomic) if historical_preflight_available else "0"
    row.minimum_output_amount = _atomic_amount_to_display(minimum_atomic, output_decimals) if historical_preflight_available else "0"
    row.minimum_output_amount_atomic = str(minimum_atomic) if historical_preflight_available else "0"
    row.slippage_bps = 100
    row.quote_id = quote_id
    row.plan_fetched_at = existing_row.plan_fetched_at if preserve_durable_preflight else confirmed_at
    row.plan_expires_at = (
        existing_row.plan_expires_at
        if preserve_durable_preflight
        else (confirmed_at + timedelta(seconds=60) if historical_preflight_available else confirmed_at)
    )
    row.allowance_read_method = "not_applicable" if input_native else "eth_call"
    row.allowance_token_address = input_contract
    row.allowance_spender = spender
    row.allowance_current_atomic = (
        "0" if input_native
        else (residual_allowance_atomic if residual_allowance_atomic is not None else "0")
    )
    row.allowance_required_atomic = "0" if input_native else str(actual_input_atomic)
    row.allowance_shortfall_atomic = "0"
    row.approval_required = bool(normalized_approval_hash)
    row.approval_amount = "0" if input_native else reconciliation["input_amount"]
    row.approval_amount_atomic = "0" if input_native else str(actual_input_atomic)
    row.approval_plan_hash = approval_plan_hash
    row.approval_transaction_to = input_contract
    row.approval_transaction_value_wei = "0"
    row.approval_calldata_sha256 = approval_calldata_sha
    row.approval_calldata_bytes = approval_calldata_bytes
    row.approval_gas_limit = str(approval_gas_limit)
    row.approval_gas_price_wei = str(approval_gas_price)
    row.approval_status = "confirmed" if normalized_approval_hash else "not_required"
    row.approval_tx_hash = normalized_approval_hash
    row.swap_plan_hash = swap_plan_hash
    row.swap_transaction_to = tx_to
    row.swap_transaction_value_wei = str(tx_value)
    row.swap_calldata_sha256 = swap_calldata_sha
    row.swap_calldata_bytes = len(swap_data_bytes)
    row.swap_gas_limit = str(swap_gas_limit)
    row.swap_gas_price_wei = str(swap_effective_gas_price)
    row.swap_status = "confirmed"
    row.swap_tx_hash = normalized_tx_hash
    row.route = route
    row.status = "confirmed"
    row.error_code = None
    row.error_message = None
    if created:
        row.created_at = confirmed_at
    row.updated_at = now
    db.flush()
    return {
        "ok": True,
        "tranche": "R5C.5D.2F.4",
        "created": created,
        "idempotent": False,
        "execution_id": str(row.id),
        "symbol": normalized_symbol,
        "side": normalized_side,
        "tx_hash": normalized_tx_hash,
        "approval_tx_hash": normalized_approval_hash,
        "receipt_status": 1,
        "block_number": block_number,
        "actual_input_asset": reconciliation["input_asset"],
        "actual_input_amount": reconciliation["input_amount"],
        "actual_input_amount_atomic": str(actual_input_atomic),
        "actual_output_asset": reconciliation["output_asset"],
        "actual_output_amount": reconciliation["output_amount"],
        "actual_output_amount_atomic": str(actual_output_atomic),
        "quoted_output_amount": reconciliation["quoted_output_amount"],
        "minimum_received": reconciliation["minimum_output_amount"],
        "minimum_received_satisfied": reconciliation["minimum_received_satisfied"],
        "historical_preflight_available": bool(historical_preflight_available),
        "average_fill_price": reconciliation["average_fill_price"],
        "swap_network_fee_wei": str(swap_fee_wei),
        "approval_network_fee_wei": str(approval_fee_wei),
        "total_network_fee_wei": str(total_fee_wei),
        "residual_allowance_atomic": residual_allowance_atomic,
        "order_mutation": True,
        "ledger_mutation": False,
        "fifo_mutation": False,
        "basis_mutation": False,
        "tax_mutation": False,
        "database_mutation": True,
    }


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
    additional_erc20_assets: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Force-refresh the saved RH Chain account snapshots after confirmation.

    The Token Registry native identity and canonical USDG are always refreshed. Confirmed ERC-20 output
    assets, such as WETH, may be added explicitly without changing execution state.
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

    try:
        native_identity = _resolve_native_registry_identity(db)
    except HTTPException as exc:
        detail = exc.detail if isinstance(exc.detail, dict) else {"error": str(exc.detail)}
        return {"ok": False, "refreshed": 0, "errors": [{"asset": "native", **detail}]}
    native_symbol = str(native_identity.get("symbol") or "").strip().upper()

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
            asset=native_symbol,
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
            "asset": native_symbol,
            "balance": str(eth_result.get("balance_eth") or "0"),
            "balance_atomic": str(eth_result.get("balance_wei") or "0"),
            "cached": bool(eth_result.get("cached")),
            "fetched_at": eth_result.get("fetched_at"),
        })
    except Exception as exc:
        db.rollback()
        errors.append({"asset": native_symbol, "error": str(exc)})

    requested_erc20_assets = ["USDG"]
    for raw_symbol in additional_erc20_assets or []:
        symbol = str(raw_symbol or "").strip().upper()
        if symbol and symbol not in {native_symbol, *requested_erc20_assets}:
            requested_erc20_assets.append(symbol)

    for symbol in requested_erc20_assets:
        try:
            token_row, contract_address, token_decimals = _resolve_registered_erc20(db, symbol)
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
                asset=symbol,
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
                "asset": symbol,
                "balance": str(token_result.get("balance_token") or "0"),
                "balance_atomic": str(token_result.get("balance_atomic") or "0"),
                "contract_address": contract_address,
                "cached": bool(token_result.get("cached")),
                "fetched_at": token_result.get("fetched_at"),
            })
        except Exception as exc:
            db.rollback()
            errors.append({"asset": symbol, "error": str(exc)})

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
        "invalid_quote_amount_mode",
        "invalid_requested_amount",
        "invalid_quantity",
        "invalid_quote_amount",
        "invalid_discovery_amount",
        "discovery_amount_exceeds_cap",
        "unsupported_discovery_pair",
        "invalid_firm_quote_amount",
        "firm_quote_amount_exceeds_capability_probe",
        "firm_quote_maximum_input_exceeds_capability_ceiling",
        "robinhood_chain_quote_amount_exceeds_indicative_ceiling",
        "robinhood_chain_quote_exact_output_exceeds_probe_evidence",
        "maximum_input_amount_requires_exact_output",
        "invalid_slippage_bps",
        "uniswap_quote_exact_input_only",
        "requested_amount_exceeds_token_precision",
        "invalid_token_decimals",
        "uniswap_quote_same_asset",
        "invalid_uniswap_registry_identity",
    }:
        return 400
    if error in {
        "execution_discovery_route_mode_not_live_verified",
        "robinhood_chain_exact_receive_route_unavailable",
        "firm_quote_route_mode_not_live_verified",
        "robinhood_chain_quote_route_unavailable",
        "firm_quote_route_capability_unavailable",
        "firm_quote_probe_evidence_missing",
        "firm_quote_exact_output_ceiling_unavailable",
        "robinhood_chain_quote_probe_evidence_missing",
        "robinhood_chain_quote_mechanism_not_supported",
        "uniswap_approval_reset_required",
        "uniswap_orderbook_direction_unavailable",
        "uniswap_orderbook_pair_identity_mismatch",
        "robinhood_chain_same_provider_orderbook_unavailable",
    }:
        return 409
    if error in {
        "execution_discovery_not_configured",
        "execution_discovery_backoff_active",
        "chain_id_mismatch_or_unavailable",
        "firm_quote_planning_not_configured",
        "firm_quote_provider_transient_error",
        "uniswap_quote_not_configured",
        "uniswap_quote_authentication_failed",
        "uniswap_quote_provider_transient_error",
        "uniswap_firm_plan_provider_transient_error",
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


def _status_payload(db: Optional[Session] = None) -> Dict[str, Any]:
    configured_chain_id = int(settings.robinhood_chain_chain_id)
    configured_match = configured_chain_id == _EXPECTED_CHAIN_ID_DECIMAL
    observed = str(_LAST_OBSERVED_CHAIN_ID or "").strip().lower() or None
    observed_match = observed == _EXPECTED_CHAIN_ID_HEX if observed is not None else None

    native_identity = None
    native_identity_error = None
    if db is not None and hasattr(db, "query"):
        try:
            native_identity = _resolve_native_registry_identity(db)
        except HTTPException as exc:
            detail = exc.detail if isinstance(exc.detail, dict) else {"error": str(exc.detail)}
            native_identity_error = detail.get("error")

    return {
        "venue": "robinhood_chain",
        "network": "mainnet",
        "native_currency": (native_identity or {}).get("symbol"),
        "native_currency_source": "token_registry",
        "native_identity_ready": native_identity_error is None and native_identity is not None,
        "native_identity_error": native_identity_error,
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
def robinhood_chain_status(
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    return _status_payload(db)


@router.post("/rpc_probe")
async def robinhood_chain_rpc_probe(
    payload: Optional[RobinhoodChainProbeRequest] = None,
    db: Session = Depends(get_db),
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
        "status": _status_payload(db),
    }

@router.get("/address/{address}/balance")
async def robinhood_chain_address_balance(
    address: str,
    force_refresh: bool = Query(default=False),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Return the registry-defined native balance for one Robinhood Chain address.

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

    native_identity = _resolve_native_registry_identity(db)
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
        "asset": native_identity.get("symbol"),
        "decimals": int(native_identity.get("decimals")),
        "registry_id": int(native_identity.get("registry_id")),
        "registry_venue": native_identity.get("registry_venue"),
        "balance_wei": result.get("balance_wei"),
        "balance_native": result.get("balance_eth"),
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

@router.post("/wallet-ingest/preview")
async def robinhood_chain_wallet_ingest_preview(
    request: RobinhoodChainWalletIngestRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Scan saved RH Chain wallet history without persisting or accounting."""
    if not bool(settings.robinhood_chain_enabled) or not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")
    try:
        result = await preview_robinhood_chain_wallet_ingest(
            db,
            wallet_address_id=request.wallet_address_id,
            force_full=bool(request.force_full),
            force_refresh=bool(request.force_refresh),
        )
        db.rollback()
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=422, detail={"error": str(exc), "will_mutate": False}) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail={"error": "robinhood_chain_wallet_ingest_preview_failed", "message": str(exc), "will_mutate": False}) from exc
    if result.get("ok") is not True:
        raise HTTPException(status_code=502, detail=result)
    result.pop("scan", None)
    result.pop("wallet_address", None)
    return result


@router.post("/wallet-ingest/commit")
async def robinhood_chain_wallet_ingest_commit(
    request: RobinhoodChainWalletIngestRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Persist normalized RH Chain history evidence after explicit confirmation.

    This tranche intentionally stops before All Orders/Ledger/FIFO materialization;
    transaction-level reconciliation belongs to RH-WALLET.INGEST.1D.
    """
    if request.confirm_ingest is not True:
        raise HTTPException(status_code=400, detail={"error": "robinhood_chain_wallet_ingest_confirmation_required"})
    if not bool(settings.robinhood_chain_enabled) or not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")
    try:
        result = await ingest_robinhood_chain_wallet_history(
            db,
            wallet_address_id=request.wallet_address_id,
            force_full=bool(request.force_full),
            force_refresh=bool(request.force_refresh),
        )
        if result.get("ok") is not True:
            db.rollback()
            raise HTTPException(status_code=502, detail=result)
        db.commit()
        return result
    except HTTPException:
        db.rollback()
        raise
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=422, detail={"error": str(exc)}) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail={"error": "robinhood_chain_wallet_ingest_commit_failed", "message": str(exc)}) from exc


@router.post("/wallet-materialization/preview")
def robinhood_chain_wallet_materialization_preview(
    request: RobinhoodChainWalletMaterializationRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Classify persisted RH Chain evidence without creating canonical swap rows."""
    if not bool(settings.robinhood_chain_enabled) or not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")
    try:
        result = preview_robinhood_chain_wallet_materialization(
            db,
            wallet_address_id=request.wallet_address_id,
        )
        db.rollback()
        return result
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=422, detail={"error": str(exc), "will_mutate": False}) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail={
                "error": "robinhood_chain_wallet_materialization_preview_failed",
                "message": str(exc),
                "will_mutate": False,
            },
        ) from exc


@router.post("/wallet-materialization/commit")
def robinhood_chain_wallet_materialization_commit(
    request: RobinhoodChainWalletMaterializationRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Persist only high-confidence external RH Chain swap rows after confirmation.

    This endpoint intentionally does not run Ledger/FIFO.  The resulting
    canonical external-swap rows become visible to All Orders, while lot sync
    remains a separate acceptance step with its existing dry-run safety gate.
    """
    if request.confirm_materialize is not True:
        raise HTTPException(
            status_code=400,
            detail={"error": "robinhood_chain_wallet_materialization_confirmation_required"},
        )
    if not bool(settings.robinhood_chain_enabled) or not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")
    try:
        result = materialize_robinhood_chain_wallet_history(
            db,
            wallet_address_id=request.wallet_address_id,
        )
        if result.get("ok") is not True:
            db.rollback()
            raise HTTPException(status_code=409, detail=result)
        db.commit()
        return result
    except HTTPException:
        db.rollback()
        raise
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=422, detail={"error": str(exc)}) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail={
                "error": "robinhood_chain_wallet_materialization_commit_failed",
                "message": str(exc),
            },
        ) from exc


@router.post("/wallet-sync/incremental")
async def robinhood_chain_wallet_sync_incremental(
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Incrementally ingest/materialize saved RH Chain history for Sync+Load.

    This route never initiates a historical full backfill. A completed
    Wallet Addresses backfill/checkpoint is required first.
    """
    if not bool(settings.robinhood_chain_enabled) or not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")
    try:
        result = await sync_robinhood_chain_wallet_incremental(db)
        if result.get("ok") is not True:
            db.rollback()
            error = str(result.get("error") or "robinhood_chain_wallet_incremental_sync_failed")
            status_code = (
                409
                if error == "robinhood_chain_wallet_sync_requires_completed_backfill"
                else 502
            )
            raise HTTPException(status_code=status_code, detail=result)

        # RH-RECEIPT.EXPIRY.1B: All Orders Sync+Load must not depend on the
        # Order Ticket's short-lived receipt capability.  Reconcile already
        # submitted durable UTT lifecycle owners from their persisted tx hash.
        # This never prepares/signs/broadcasts or creates a second wallet request.
        receipt_reconciliation = await _reconcile_pending_generic_wallet_swaps_for_sync_load(
            db,
            limit=50,
        )
        result = dict(result)
        result["utt_swap_receipt_reconciliation"] = receipt_reconciliation
        result["orders_table_mutation"] = bool(
            result.get("orders_table_mutation")
            or receipt_reconciliation.get("order_mutation")
        )
        result["all_orders_visibility_mutation"] = bool(
            result.get("all_orders_visibility_mutation")
            or receipt_reconciliation.get("order_mutation")
        )
        result["database_mutation"] = bool(
            result.get("database_mutation")
            or receipt_reconciliation.get("database_mutation")
        )
        result["wallet_request"] = False
        result["signing"] = False
        result["broadcast"] = False
        result["automatic_retry"] = False
        result["automatic_second_transaction"] = False
        db.commit()
        return result
    except HTTPException:
        db.rollback()
        raise
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=422, detail={"error": str(exc)}) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail={
                "error": "robinhood_chain_wallet_incremental_sync_failed",
                "message": str(exc),
            },
        ) from exc


@router.get("/registry-discovery/status")
async def robinhood_chain_registry_discovery_status(
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")
    return get_robinhood_chain_registry_discovery_service().status(db)


@router.get("/registry-discovery/assets")
async def robinhood_chain_registry_discovery_assets(
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    service = get_robinhood_chain_registry_discovery_service()
    return {
        "ok": True,
        "items": service.assets(db),
        "blockchain_read_only": True,
        "execution_enabled": False,
    }


@router.get("/registry-discovery/unregistered-wallet-assets")
async def robinhood_chain_registry_discovery_unregistered_wallet_assets(
    wallet_address_id: Optional[str] = Query(
        None,
        description="Optional saved ALL / robinhood_chain Wallet Addresses row ID.",
    ),
    limit: int = Query(50, ge=1, le=100),
    positive_only: bool = Query(
        True,
        description="When true, return only contracts with a positive current wallet balance.",
    ),
    force_refresh: bool = Query(
        False,
        description="Bypass bounded RPC read caches for code, metadata, and balance reads.",
    ),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(
            status_code=503,
            detail="Robinhood Chain configuration is not effective for chain ID 4663",
        )
    try:
        return await get_robinhood_chain_registry_discovery_service().unregistered_wallet_assets(
            db,
            wallet_address_id=wallet_address_id,
            limit=int(limit),
            positive_only=bool(positive_only),
            force_refresh=bool(force_refresh),
        )
    except ValueError as exc:
        error = str(exc)
        status_code = 404 if "not_found" in error else 409
        raise HTTPException(status_code=status_code, detail={"error": error}) from exc


@router.post("/registry-discovery/assets/{token_registry_id}/verify")
async def robinhood_chain_registry_discovery_verify_asset(
    token_registry_id: int,
    request: RobinhoodChainRegistryVerifyRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return await get_robinhood_chain_registry_discovery_service().verify_asset(
            db,
            token_registry_id=token_registry_id,
            force_refresh=bool(request.force_refresh),
            confirm_verify=bool(request.confirm_verify),
        )
    except ValueError as exc:
        db.rollback()
        error = str(exc)
        raise HTTPException(status_code=404 if "not_found" in error else 409, detail={"error": error}) from exc


@router.get("/registry-discovery/objectives")
async def robinhood_chain_registry_discovery_objectives(
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    return {
        "ok": True,
        "items": get_robinhood_chain_registry_discovery_service().objectives(db),
        "review_only": True,
        "execution_enabled": False,
    }


@router.get("/registry-discovery/markets")
async def robinhood_chain_registry_discovery_markets(
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    service = get_robinhood_chain_registry_discovery_service()
    return {
        "ok": True,
        "tranche": "RH-CHAIN.10D.2-R5C.2",
        "items": service.market_catalog(db),
        "token_identity_source": "token_registry",
        "pair_capability_source": "database",
        "hardcoded_markets": False,
        "automatic_execution_promotion": False,
        "review_only": True,
        "execution_enabled": False,
    }


@router.post("/registry-discovery/markets/{symbol}/refresh")
async def robinhood_chain_registry_discovery_refresh_market(
    symbol: str,
    request: RobinhoodChainSelectedMarketRefreshRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Explicitly refresh one selected market's bounded review-only directions."""
    taker = _resolve_robinhood_chain_quote_taker(db, request.taker_address)
    try:
        return await get_robinhood_chain_registry_discovery_service().refresh_selected_market(
            db,
            symbol=symbol,
            taker_address=taker,
            force_refresh=bool(request.force_refresh),
            confirm_refresh=bool(request.confirm_refresh),
        )
    except ValueError as exc:
        db.rollback()
        error = str(exc)
        status_code = 404 if "not_found" in error else 409
        raise HTTPException(
            status_code=status_code,
            detail={
                "error": error,
                "symbol": str(symbol or "").strip().upper(),
                "provider_contacted": False,
                "execution_enabled": False,
                "automatic_execution_promotion": False,
            },
        ) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(
            status_code=502,
            detail={
                "error": "selected_market_refresh_failed",
                "failure_type": type(exc).__name__,
                "message": str(exc)[:1000],
                "symbol": str(symbol or "").strip().upper(),
                "provider_contacted": False,
                "execution_enabled": False,
                "automatic_execution_promotion": False,
            },
        ) from exc


@router.post("/registry-discovery/objectives")
async def robinhood_chain_registry_discovery_create_objective(
    request: RobinhoodChainPairObjectiveCreateRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return get_robinhood_chain_registry_discovery_service().create_objective(
            db,
            base_token_registry_id=request.base_token_registry_id,
            quote_token_registry_id=request.quote_token_registry_id,
            mechanism=request.mechanism,
            notes=request.notes,
            confirm_create=bool(request.confirm_create),
            require_verified_registry_identities=True,
        )
    except ValueError as exc:
        db.rollback()
        error = str(exc)
        raise HTTPException(status_code=404 if "not_found" in error else 409, detail={"error": error}) from exc


@router.delete("/registry-discovery/objectives/{objective_id}")
async def robinhood_chain_registry_discovery_delete_objective(
    objective_id: str,
    request: RobinhoodChainPairObjectiveDeleteRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return get_robinhood_chain_registry_discovery_service().delete_objective(
            db,
            objective_id=objective_id,
            confirm_delete=bool(request.confirm_delete),
        )
    except ValueError as exc:
        db.rollback()
        error = str(exc)
        raise HTTPException(status_code=404 if "not_found" in error else 409, detail={"error": error}) from exc


@router.post("/registry-discovery/objectives/{objective_id}/discover")
async def robinhood_chain_registry_discovery_discover_objective(
    objective_id: str,
    request: RobinhoodChainPairDiscoveryRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return await get_robinhood_chain_registry_discovery_service().discover_objective(
            db,
            objective_id=objective_id,
            taker_address=request.taker_address,
            base_probe_amount=request.base_probe_amount,
            quote_probe_amount=request.quote_probe_amount,
            force_refresh=bool(request.force_refresh),
            confirm_discovery=bool(request.confirm_discovery),
        )
    except ValueError as exc:
        db.rollback()
        error = str(exc)
        status_code = 404 if "not_found" in error else 409
        raise HTTPException(status_code=status_code, detail={"error": error}) from exc


@router.post("/registry-discovery/sync-execution-evidence")
async def robinhood_chain_registry_discovery_sync_execution_evidence(
    request: RobinhoodChainExecutionEvidenceSyncRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return get_robinhood_chain_registry_discovery_service().sync_execution_evidence(
            db,
            confirm_sync=bool(request.confirm_sync),
        )
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": str(exc)}) from exc


@router.get("/execution-discovery/status")
async def robinhood_chain_execution_discovery_status(
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Return secret-free, mainnet-only 0x discovery readiness."""
    if not bool(settings.robinhood_chain_enabled):
        raise HTTPException(status_code=503, detail="Robinhood Chain is disabled")
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")
    capabilities = get_robinhood_chain_registry_discovery_service().route_capabilities(db)
    return get_robinhood_chain_execution_discovery_service().status(
        route_capabilities=capabilities,
    )


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
    amount_mode = "exact_input" if request.sell_amount is not None and str(request.sell_amount).strip() else "exact_output"
    capability = get_robinhood_chain_registry_discovery_service().route_capability(
        db,
        from_token_registry_id=int(sell_token["registry_id"]),
        to_token_registry_id=int(buy_token["registry_id"]),
        amount_mode=amount_mode,
    )

    result = await get_robinhood_chain_execution_discovery_service().probe(
        sell_token=sell_token,
        buy_token=buy_token,
        sell_amount=request.sell_amount,
        buy_amount=request.buy_amount,
        taker_address=taker_address,
        force_refresh=bool(request.force_refresh),
        route_capability=capability,
        require_live_verified=True,
        max_probe_amount=(request.sell_amount if amount_mode == "exact_input" else request.buy_amount),
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


@router.get("/uniswap/status")
async def robinhood_chain_uniswap_status(
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Return secret-free readiness for the backend-only Uniswap /quote canary."""
    if not bool(settings.robinhood_chain_enabled):
        raise HTTPException(status_code=503, detail="Robinhood Chain is disabled")
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(
            status_code=503,
            detail="Robinhood Chain configuration is not effective for chain ID 4663",
        )

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
    payload = get_robinhood_chain_uniswap_quote_service().status()
    payload["wallet_configured"] = wallet_row is not None
    payload["provider"] = UNISWAP_PROVIDER
    payload["execution_enabled"] = False
    payload["will_mutate"] = False
    db.rollback()
    return payload


@router.post("/uniswap/quote")
async def robinhood_chain_uniswap_quote(
    request: RobinhoodChainUniswapQuoteRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Make one explicit AMM-only Uniswap quote without persistence or execution."""
    if not bool(settings.robinhood_chain_enabled):
        raise HTTPException(status_code=503, detail="Robinhood Chain is disabled")
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(
            status_code=503,
            detail="Robinhood Chain configuration is not effective for chain ID 4663",
        )
    if not bool(request.confirm_quote):
        raise HTTPException(
            status_code=400,
            detail={
                "error": "uniswap_quote_confirmation_required",
                "provider_contacted": False,
                "read_only": True,
                "will_mutate": False,
            },
        )

    registry_service = get_robinhood_chain_registry_discovery_service()
    try:
        market = registry_service.objective_by_symbol(db, request.symbol)
    except ValueError as exc:
        db.rollback()
        raise HTTPException(
            status_code=404,
            detail={
                "error": str(exc),
                "symbol": str(request.symbol or "").strip().upper(),
                "identity_source": "token_registry",
                "provider_contacted": False,
                "read_only": True,
                "will_mutate": False,
            },
        ) from exc

    if str(market.get("mechanism") or "").strip().lower() != "swap":
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail={
                "error": "uniswap_quote_mechanism_not_supported",
                "symbol": market.get("symbol"),
                "mechanism": market.get("mechanism"),
                "provider_contacted": False,
                "read_only": True,
                "will_mutate": False,
            },
        )

    normalized_side = str(request.side or "").strip().lower()
    if normalized_side not in {"buy", "sell"}:
        db.rollback()
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_quote_side", "provider_contacted": False},
        )
    if str(request.amount_mode or "").strip().lower() != "exact_input":
        db.rollback()
        raise HTTPException(
            status_code=400,
            detail={
                "error": "uniswap_quote_exact_input_only",
                "provider_contacted": False,
            },
        )

    base = market.get("base") if isinstance(market.get("base"), dict) else {}
    quote = market.get("quote") if isinstance(market.get("quote"), dict) else {}
    input_token = quote if normalized_side == "buy" else base
    output_token = base if normalized_side == "buy" else quote
    taker = _resolve_robinhood_chain_quote_taker(db, request.taker_address)
    result = await get_robinhood_chain_uniswap_quote_service().quote(
        symbol=market.get("symbol") or request.symbol,
        side=normalized_side,
        amount_mode=request.amount_mode,
        requested_amount=request.requested_amount,
        slippage_bps=int(request.slippage_bps),
        swapper_address=taker,
        input_token=input_token,
        output_token=output_token,
        confirm_quote=True,
    )
    db.rollback()
    if result.get("ok"):
        return result

    error = str(result.get("error") or "uniswap_quote_failed")
    if error in {
        "uniswap_quote_confirmation_required",
        "invalid_quote_side",
        "uniswap_quote_exact_input_only",
        "invalid_requested_amount",
        "requested_amount_exceeds_token_precision",
        "invalid_token_decimals",
        "invalid_uniswap_registry_identity",
        "uniswap_quote_same_asset",
        "invalid_slippage_bps",
        "uniswap_quote_exact_input_only",
        "requested_amount_exceeds_token_precision",
        "invalid_token_decimals",
        "uniswap_quote_same_asset",
        "invalid_uniswap_registry_identity",
    }:
        status_code = 400
    elif error in {"uniswap_quote_not_configured", "uniswap_quote_api_base_invalid"}:
        status_code = 503
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

    registry_service = get_robinhood_chain_registry_discovery_service()
    registry_tokens, native_token = _resolve_robinhood_chain_review_identities(db)
    tokens = {
        str(item.get("symbol") or "").strip().upper(): item
        for item in registry_tokens
        if item.get("symbol")
    }
    markets = registry_service.market_catalog(db)
    route_capabilities = registry_service.route_capabilities(db)
    indicative_symbols = sorted({
        str(item.get("symbol") or "").strip().upper()
        for item in route_capabilities
        if str(item.get("mechanism") or "swap").strip().lower() == "swap"
        and str(item.get("indicative_status") or "").strip().lower() in {"available", "live_verified"}
    })
    firm_symbols = sorted({
        str(item.get("symbol") or "").strip().upper()
        for item in route_capabilities
        if str(item.get("mechanism") or "swap").strip().lower() == "swap"
        and str(item.get("firm_plan_status") or "").strip().lower() in {"available", "live_verified"}
    })
    payload = get_robinhood_chain_quote_service().status()
    payload["supported_symbols"] = indicative_symbols
    payload["review_markets"] = markets
    payload["firm_planning"] = get_robinhood_chain_transaction_planning_service().status()
    payload["firm_planning"]["supported_symbols"] = firm_symbols
    payload["firm_planning"]["route_capabilities"] = route_capabilities
    payload["firm_planning"]["exact_output_enabled"] = any(
        str(item.get("amount_mode") or "").strip().lower() == "exact_output"
        and str(item.get("firm_plan_status") or "").strip().lower() in {"available", "live_verified"}
        for item in route_capabilities
    )
    payload["tokens"] = tokens
    payload["native_fee_identity"] = native_token
    payload["route_capabilities"] = route_capabilities
    payload["token_identity_source"] = "token_registry"
    payload["pair_capability_source"] = "database"
    payload["swap_oriented"] = True
    payload["amount_modes"] = sorted({
        str(item.get("display_mode") or item.get("amount_mode") or "").strip().lower()
        for item in route_capabilities
        if item.get("display_mode") or item.get("amount_mode")
    })
    payload["exact_output_enabled"] = any(
        str(item.get("amount_mode") or "").strip().lower() == "exact_output"
        and str(item.get("indicative_status") or "").strip().lower() in {"available", "live_verified"}
        for item in route_capabilities
    )
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

    The requested market, identities, direction, and amount mode must be present
    in database-backed capability evidence. Exact-input review uses an explicit
    direction ceiling when present, otherwise the configured read-only value cap.
    """
    if not bool(settings.robinhood_chain_enabled):
        raise HTTPException(status_code=503, detail="Robinhood Chain is disabled")
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")

    provider = str(request.provider or "").strip().lower().replace("zerox", "0x")
    if provider not in {
        ROBINHOOD_CHAIN_QUOTE_PROVIDER,
        UNISWAP_PROVIDER,
        UNISWAP_V3_RPC_PROVIDER,
    }:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "unsupported_robinhood_chain_quote_provider",
                "provider": request.provider,
                "allowed": [
                    ROBINHOOD_CHAIN_QUOTE_PROVIDER,
                    UNISWAP_PROVIDER,
                    UNISWAP_V3_RPC_PROVIDER,
                ],
            },
        )

    taker_address = _resolve_robinhood_chain_quote_taker(db, request.taker_address)
    market, base, quote, capability = _resolve_robinhood_chain_review_market(
        db,
        symbol=request.symbol,
        side=request.side,
        amount_mode=request.amount_mode,
        capability_status_field="indicative_status",
        provider=provider,
    )
    if provider == UNISWAP_PROVIDER:
        normalized_side = str(request.side or "").strip().lower()
        input_token = quote if normalized_side == "buy" else base
        output_token = base if normalized_side == "buy" else quote
        result = await get_robinhood_chain_uniswap_quote_service().quote(
            symbol=market.get("symbol") or request.symbol,
            side=normalized_side,
            amount_mode=request.amount_mode,
            requested_amount=request.requested_amount,
            slippage_bps=50,
            swapper_address=taker_address,
            input_token=input_token,
            output_token=output_token,
            confirm_quote=True,
        )
    elif provider == UNISWAP_V3_RPC_PROVIDER:
        try:
            weth = get_robinhood_chain_registry_discovery_service().resolve_verified_token(db, "WETH")
        except ValueError as exc:
            db.rollback()
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "verified_weth_registry_identity_required",
                    "message": str(exc),
                    "symbol": market.get("symbol") or request.symbol,
                    "provider": UNISWAP_V3_RPC_PROVIDER,
                    "provider_contacted": False,
                },
            ) from exc
        result = await get_robinhood_chain_uniswap_v3_quote_service().quote_for_pair(
            symbol=market.get("symbol") or request.symbol,
            side=request.side,
            amount_mode=request.amount_mode,
            requested_amount=request.requested_amount,
            base_token=base,
            quote_token=quote,
            weth_token=weth,
            force_refresh=bool(request.force_refresh),
        )
    else:
        registry_tokens, native_token = _resolve_robinhood_chain_review_identities(db)
        result = await get_robinhood_chain_quote_service().indicative_quote(
            symbol=market.get("symbol") or request.symbol,
            side=request.side,
            amount_mode=request.amount_mode,
            requested_amount=request.requested_amount,
            taker_address=taker_address,
            base_token=base,
            quote_token=quote,
            native_token=native_token,
            registry_tokens=registry_tokens,
            route_capability=capability,
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
    """Return a bounded provider quote and validated unsigned transaction plan.

    Native registry input requires no allowance and must carry the exact input in
    transaction.value. ERC-20 input may return one separate exact finite approval
    transaction when allowance is insufficient. The endpoint never prompts a wallet,
    signs, broadcasts, records an order, or mutates ledger/FIFO/basis state.
    """
    if not bool(settings.robinhood_chain_enabled):
        raise HTTPException(status_code=503, detail="Robinhood Chain is disabled")
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")

    provider = str(request.provider or "").strip().lower().replace("zerox", "0x")
    if provider not in {ROBINHOOD_CHAIN_QUOTE_PROVIDER, UNISWAP_PROVIDER}:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "unsupported_robinhood_chain_quote_provider",
                "provider": request.provider,
                "allowed": [ROBINHOOD_CHAIN_QUOTE_PROVIDER, UNISWAP_PROVIDER],
            },
        )

    taker_address = _resolve_robinhood_chain_quote_taker(db, request.taker_address)
    market, base, quote, capability = _resolve_robinhood_chain_review_market(
        db,
        symbol=request.symbol,
        side=request.side,
        amount_mode=request.amount_mode,
        capability_status_field="indicative_status" if provider == UNISWAP_PROVIDER else "firm_plan_status",
        provider=provider,
    )
    if provider == UNISWAP_PROVIDER:
        if str(request.amount_mode or "").strip().lower() != "exact_input":
            raise HTTPException(status_code=400, detail={"error": "uniswap_quote_exact_input_only"})
        normalized_side = str(request.side or "").strip().lower()
        input_token = quote if normalized_side == "buy" else base
        output_token = base if normalized_side == "buy" else quote
        result = await get_robinhood_chain_uniswap_quote_service().firm_quote_plan(
            symbol=market.get("symbol") or request.symbol,
            side=normalized_side,
            amount_mode=request.amount_mode,
            requested_amount=request.requested_amount,
            slippage_bps=int(request.slippage_bps),
            swapper_address=taker_address,
            input_token=input_token,
            output_token=output_token,
        )
    else:
        registry_tokens, native_token = _resolve_robinhood_chain_review_identities(db)
        result = await get_robinhood_chain_transaction_planning_service().firm_quote_plan(
            symbol=market.get("symbol") or request.symbol,
            side=request.side,
            amount_mode=request.amount_mode,
            requested_amount=request.requested_amount,
            maximum_input_amount=request.maximum_input_amount,
            taker_address=taker_address,
            base_token=base,
            quote_token=quote,
            native_token=native_token,
            registry_tokens=registry_tokens,
            route_capability=capability,
            slippage_bps=int(request.slippage_bps),
        )
    db.rollback()
    if result.get("ok"):
        return result
    raise HTTPException(status_code=_quote_failure_status(result), detail=result)


@router.post("/wallet-rejection/prepare")
async def robinhood_chain_wallet_rejection_prepare(
    request: RobinhoodChainWalletRejectionPrepareRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Prepare one fresh transaction solely for the deliberate first-wallet rejection test.

    The endpoint performs provider/RPC preflight and returns one exact transaction
    request. It never opens MetaMask, signs, broadcasts, writes an order, or mutates
    ledger/FIFO/basis/tax state. Successful broadcast is explicitly unauthorized.
    """
    if request.confirm_prepare is not True:
        raise HTTPException(status_code=400, detail={"error": "confirm_wallet_rejection_prepare_required"})
    provider = str(request.provider or "").strip().lower()
    if provider != UNISWAP_PROVIDER:
        raise HTTPException(
            status_code=400,
            detail={"error": "wallet_rejection_provider_not_supported", "provider": provider, "allowed": [UNISWAP_PROVIDER]},
        )
    if str(request.amount_mode or "").strip().lower() != "exact_input":
        raise HTTPException(status_code=400, detail={"error": "wallet_rejection_exact_input_only"})

    gate = _robinhood_chain_wallet_rejection_gate()
    if gate.get("wallet_request_enabled") is not True:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "robinhood_chain_wallet_rejection_gate_blocked",
                "gate": gate,
                "provider_contacted": False,
                "rpc_contacted": False,
                "wallet_connection_requested": False,
                "successful_broadcast_authorized": False,
            },
        )

    taker = _resolve_robinhood_chain_execution_taker(db, request.taker_address)
    market, base, quote, _capability = _resolve_robinhood_chain_review_market(
        db,
        symbol=request.symbol,
        side=request.side,
        amount_mode="exact_input",
        capability_status_field="indicative_status",
        provider=UNISWAP_PROVIDER,
    )
    normalized_side = str(request.side or "").strip().lower()
    input_token = quote if normalized_side == "buy" else base
    output_token = base if normalized_side == "buy" else quote

    registry_service = get_robinhood_chain_registry_discovery_service()
    try:
        verified_input = registry_service.resolve_verified_token(db, str(input_token.get("symbol") or ""))
        verified_output = registry_service.resolve_verified_token(db, str(output_token.get("symbol") or ""))
    except ValueError as exc:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail={
                "error": "wallet_rejection_verified_registry_identity_required",
                "message": str(exc),
                "provider_contacted": False,
                "rpc_contacted": False,
            },
        ) from exc
    if (
        int(verified_input.get("registry_id") or 0) != int(input_token.get("registry_id") or 0)
        or int(verified_output.get("registry_id") or 0) != int(output_token.get("registry_id") or 0)
    ):
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": "wallet_rejection_registry_identity_mismatch"})

    firm_plan = await get_robinhood_chain_uniswap_quote_service().firm_quote_plan(
        symbol=market.get("symbol") or request.symbol,
        side=normalized_side,
        amount_mode="exact_input",
        requested_amount=request.requested_amount,
        slippage_bps=int(request.slippage_bps),
        swapper_address=taker,
        input_token=input_token,
        output_token=output_token,
    )
    if firm_plan.get("ok") is not True:
        db.rollback()
        raise HTTPException(status_code=_quote_failure_status(firm_plan), detail=firm_plan)

    rpc = get_robinhood_chain_client()
    chain = await rpc.verify_expected_chain(force_refresh=True)
    if chain.get("ok") is not True or chain.get("chain_id_matches") is not True:
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": "wallet_rejection_rpc_chain_mismatch"})
    native_balance = await rpc.get_native_balance(taker, block_tag="latest", force_refresh=True)
    if native_balance.get("ok") is not True:
        db.rollback()
        raise HTTPException(status_code=502, detail={"error": "wallet_rejection_native_balance_read_failed"})

    input_balance_atomic: Optional[str] = None
    if not bool(input_token.get("native")):
        contract = validate_evm_address(
            str(input_token.get("registry_contract_address") or input_token.get("contract_address") or "").strip()
        )
        input_balance = await rpc.get_erc20_balance(
            taker,
            contract,
            int(input_token.get("decimals")),
            block_tag="latest",
            force_refresh=True,
        )
        if input_balance.get("ok") is not True:
            db.rollback()
            raise HTTPException(status_code=502, detail={"error": "wallet_rejection_input_balance_read_failed"})
        input_balance_atomic = str(input_balance.get("balance_atomic") or "0")

    try:
        handoff = validate_wallet_rejection_handoff(
            firm_plan,
            wallet_address=taker,
            native_balance_wei=str(native_balance.get("balance_wei") or "0"),
            input_balance_atomic=input_balance_atomic,
        )
    except ValueError as exc:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail={
                "error": str(exc),
                "provider_contacted": True,
                "rpc_contacted": True,
                "wallet_connection_requested": False,
                "successful_broadcast_authorized": False,
            },
        ) from exc

    prepared_at = datetime.now(timezone.utc)
    expires_at = prepared_at + timedelta(seconds=_ROBINHOOD_CHAIN_WALLET_REJECTION_TTL_SECONDS)
    db.rollback()
    return {
        "ok": True,
        "tranche": "R5C.5D.2F.1",
        "symbol": market.get("symbol") or request.symbol,
        "side": normalized_side,
        "amount_mode": "exact_input",
        "requested_amount": str(request.requested_amount),
        "provider": UNISWAP_PROVIDER,
        "prepared_at": prepared_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "ttl_seconds": _ROBINHOOD_CHAIN_WALLET_REJECTION_TTL_SECONDS,
        "wallet_request": handoff,
        "gate": gate,
        "provider_contacted": True,
        "rpc_contacted": True,
        "database_mutation": False,
        "wallet_connection_requested": False,
        "signing_enabled": False,
        "broadcast_enabled": False,
        "successful_broadcast_authorized": False,
        "reject_only": True,
        "automatic_retry": False,
        "automatic_second_transaction": False,
        "order_mutation": False,
        "ledger_mutation": False,
        "fifo_mutation": False,
        "basis_mutation": False,
        "tax_mutation": False,
        "will_mutate": False,
    }


@router.post("/wallet-approval/prepare")
async def robinhood_chain_wallet_approval_prepare(
    request: RobinhoodChainWalletApprovalPrepareRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Prepare one successful exact finite approval MetaMask request.

    This endpoint deliberately reuses the accepted R5C.5D.2F.1 fresh provider/RPC/
    balance preflight and only promotes an exact finite ERC-20 approval. It never
    signs, broadcasts, opens MetaMask, or authorizes a swap request.
    """
    if request.confirm_prepare is not True:
        raise HTTPException(status_code=400, detail={"error": "confirm_wallet_approval_prepare_required"})
    if str(request.provider or "").strip().lower() != UNISWAP_PROVIDER:
        raise HTTPException(status_code=400, detail={"error": "wallet_approval_provider_not_supported"})
    if str(request.amount_mode or "").strip().lower().replace("exact_spend", "exact_input") != "exact_input":
        raise HTTPException(status_code=400, detail={"error": "wallet_approval_exact_input_only"})

    reject_preflight = await robinhood_chain_wallet_rejection_prepare(
        RobinhoodChainWalletRejectionPrepareRequest(
            provider=UNISWAP_PROVIDER,
            symbol=request.symbol,
            side=request.side,
            amount_mode="exact_input",
            requested_amount=request.requested_amount,
            slippage_bps=int(request.slippage_bps),
            taker_address=request.taker_address,
            confirm_prepare=True,
        ),
        db,
    )
    try:
        handoff = validate_wallet_successful_approval_handoff(dict(reject_preflight.get("wallet_request") or {}))
        capability = create_wallet_approval_capability(
            handoff,
            symbol=str(reject_preflight.get("symbol") or request.symbol),
            side=str(reject_preflight.get("side") or request.side),
            requested_amount=str(reject_preflight.get("requested_amount") or request.requested_amount),
        )
    except ValueError as exc:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail={
                "error": str(exc),
                "successful_broadcast_authorized": False,
                "swap_request_authorized": False,
                "automatic_second_transaction": False,
            },
        ) from exc

    gate = dict(reject_preflight.get("gate") or {})
    gate.update({
        "reject_only": False,
        "approval_only": True,
        "successful_broadcast_authorized": True,
        "swap_request_authorized": False,
        "automatic_retry": False,
        "automatic_second_transaction": False,
    })
    db.rollback()
    return {
        **reject_preflight,
        "tranche": "R5C.5D.2F.2",
        "wallet_request": handoff,
        "gate": gate,
        "approval_capability": capability["token"],
        "approval_receipt_expires_at_epoch": capability["expires_at_epoch"],
        "approval_receipt_ttl_seconds": capability["ttl_seconds"],
        "successful_broadcast_authorized": True,
        "reject_only": False,
        "approval_only": True,
        "swap_request_authorized": False,
        "automatic_retry": False,
        "automatic_second_transaction": False,
        "order_mutation": False,
        "ledger_mutation": False,
        "fifo_mutation": False,
        "basis_mutation": False,
        "tax_mutation": False,
        "will_mutate": False,
    }


@router.post("/wallet-approval/receipt")
async def robinhood_chain_wallet_approval_receipt(
    request: RobinhoodChainWalletApprovalReceiptRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Verify one submitted approval transaction and its current allowance.

    The capability is integrity-protected and approval-only. Receipt refresh is
    read-only with respect to UTT state: it does not create an order, ledger row,
    FIFO/basis mutation, or any follow-on wallet request.
    """
    try:
        capability = decode_wallet_approval_capability(request.capability)
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": str(exc)}) from exc

    try:
        tx_hash = validate_transaction_hash(request.tx_hash)
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc

    saved_wallet = _resolve_robinhood_chain_execution_taker(db, str(capability.get("wallet_address") or ""))
    if saved_wallet.lower() != str(capability.get("wallet_address") or "").lower():
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": "wallet_approval_saved_wallet_mismatch"})

    registry_service = get_robinhood_chain_registry_discovery_service()
    try:
        verified_token = registry_service.resolve_verified_token(db, str(capability.get("token_symbol") or ""))
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": "wallet_approval_verified_registry_identity_required", "message": str(exc)}) from exc
    current_contract = validate_evm_address(
        str(verified_token.get("registry_contract_address") or verified_token.get("contract_address") or "")
    )
    if current_contract.lower() != validate_evm_address(str(capability.get("token") or "")).lower():
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": "wallet_approval_registry_identity_changed"})

    rpc = get_robinhood_chain_client()
    chain = await rpc.verify_expected_chain(force_refresh=True)
    if chain.get("ok") is not True or chain.get("chain_id_matches") is not True:
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": "wallet_approval_rpc_chain_mismatch"})

    tx_record = await rpc.rpc_read(
        "eth_getTransactionByHash",
        [tx_hash],
        cache_namespace=None,
        force_refresh=True,
    )
    if tx_record.get("ok") is not True:
        db.rollback()
        raise HTTPException(status_code=502, detail={"error": "wallet_approval_transaction_read_failed"})
    transaction = tx_record.get("result")
    if transaction is None:
        db.rollback()
        return {
            "ok": True,
            "tranche": "R5C.5D.2F.2",
            "status": "submitted_pending_visibility",
            "pending": True,
            "confirmed": False,
            "tx_hash": tx_hash,
            "successful_broadcast_authorized": True,
            "approval_only": True,
            "swap_request_authorized": False,
            "automatic_second_transaction": False,
            "database_mutation": False,
        }
    try:
        verified_tx = validate_wallet_approval_transaction(
            capability,
            tx_hash=tx_hash,
            transaction=transaction,
        )
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": str(exc)}) from exc

    receipt_record = await rpc.rpc_read(
        "eth_getTransactionReceipt",
        [tx_hash],
        cache_namespace=None,
        force_refresh=True,
    )
    if receipt_record.get("ok") is not True:
        db.rollback()
        raise HTTPException(status_code=502, detail={"error": "wallet_approval_receipt_read_failed"})
    receipt = receipt_record.get("result")
    if receipt is None:
        db.rollback()
        return {
            "ok": True,
            "tranche": "R5C.5D.2F.2",
            "status": "approval_pending",
            "pending": True,
            "confirmed": False,
            "tx_hash": tx_hash,
            "transaction": verified_tx,
            "successful_broadcast_authorized": True,
            "approval_only": True,
            "swap_request_authorized": False,
            "automatic_second_transaction": False,
            "database_mutation": False,
        }

    receipt_status = _decode_robinhood_chain_rpc_quantity(receipt.get("status"), field="wallet_approval_receipt_status")
    block_number = _decode_robinhood_chain_rpc_quantity(receipt.get("blockNumber"), field="wallet_approval_block_number") if receipt.get("blockNumber") is not None else None
    gas_used = _decode_robinhood_chain_rpc_quantity(receipt.get("gasUsed"), field="wallet_approval_gas_used") if receipt.get("gasUsed") is not None else None
    effective_gas_price = _decode_robinhood_chain_rpc_quantity(receipt.get("effectiveGasPrice"), field="wallet_approval_effective_gas_price") if receipt.get("effectiveGasPrice") is not None else None
    network_fee_wei = gas_used * effective_gas_price if gas_used is not None and effective_gas_price is not None else None
    if receipt_status == 0:
        db.rollback()
        return {
            "ok": True,
            "tranche": "R5C.5D.2F.2",
            "status": "approval_reverted",
            "pending": False,
            "confirmed": False,
            "reverted": True,
            "tx_hash": tx_hash,
            "transaction": verified_tx,
            "receipt_status": 0,
            "block_number": block_number,
            "gas_used": str(gas_used) if gas_used is not None else None,
            "effective_gas_price_wei": str(effective_gas_price) if effective_gas_price is not None else None,
            "network_fee_wei": str(network_fee_wei) if network_fee_wei is not None else None,
            "swap_request_authorized": False,
            "automatic_second_transaction": False,
            "database_mutation": False,
        }
    if receipt_status != 1:
        db.rollback()
        raise HTTPException(status_code=502, detail={"error": "wallet_approval_receipt_status_invalid"})

    decimals = int(verified_token.get("decimals"))
    allowance = await rpc.get_erc20_allowance(
        owner_address=str(capability.get("wallet_address") or ""),
        contract_address=current_contract,
        spender_address=validate_evm_address(str(capability.get("spender") or "")),
        decimals=decimals,
        force_refresh=True,
    )
    if allowance.get("ok") is not True:
        db.rollback()
        raise HTTPException(status_code=502, detail={"error": "wallet_approval_post_receipt_allowance_read_failed"})
    allowance_atomic = str(allowance.get("allowance_atomic") or "0")
    approved_atomic = str(capability.get("approved_amount_atomic") or "0")
    if allowance_atomic != approved_atomic:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail={
                "error": "wallet_approval_post_receipt_allowance_not_exact",
                "expected_atomic": approved_atomic,
                "current_atomic": allowance_atomic,
            },
        )

    db.rollback()
    return {
        "ok": True,
        "tranche": "R5C.5D.2F.2",
        "status": "approval_confirmed",
        "pending": False,
        "confirmed": True,
        "reverted": False,
        "tx_hash": tx_hash,
        "symbol": capability.get("symbol"),
        "side": capability.get("side"),
        "input_asset": capability.get("input_asset"),
        "output_asset": capability.get("output_asset"),
        "requested_amount": capability.get("requested_amount"),
        "approved_amount_atomic": approved_atomic,
        "allowance_confirmed_atomic": allowance_atomic,
        "allowance_exact": True,
        "unlimited_approval": False,
        "transaction": verified_tx,
        "receipt_status": 1,
        "block_number": block_number,
        "gas_used": str(gas_used) if gas_used is not None else None,
        "effective_gas_price_wei": str(effective_gas_price) if effective_gas_price is not None else None,
        "network_fee_wei": str(network_fee_wei) if network_fee_wei is not None else None,
        "successful_broadcast_authorized": True,
        "approval_only": True,
        "swap_request_authorized": False,
        "automatic_retry": False,
        "automatic_second_transaction": False,
        "order_mutation": False,
        "ledger_mutation": False,
        "fifo_mutation": False,
        "basis_mutation": False,
        "tax_mutation": False,
        "database_mutation": False,
        "will_mutate": False,
    }



@router.post("/wallet-swap/prepare")
async def robinhood_chain_wallet_swap_prepare(
    request: RobinhoodChainWalletSwapPrepareRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Prepare one fresh, simulated, swap-only MetaMask request.

    The endpoint deliberately re-runs the accepted provider/RPC/balance preflight.
    It only promotes a plan whose current allowance is sufficient and whose
    provider simulation has already succeeded. No approval request is authorized.
    """
    if request.confirm_prepare is not True:
        raise HTTPException(status_code=400, detail={"error": "confirm_wallet_swap_prepare_required"})
    if str(request.provider or "").strip().lower() != UNISWAP_PROVIDER:
        raise HTTPException(status_code=400, detail={"error": "wallet_swap_provider_not_supported"})
    if str(request.amount_mode or "").strip().lower().replace("exact_spend", "exact_input") != "exact_input":
        raise HTTPException(status_code=400, detail={"error": "wallet_swap_exact_input_only"})

    fresh_preflight = await robinhood_chain_wallet_rejection_prepare(
        RobinhoodChainWalletRejectionPrepareRequest(
            provider=UNISWAP_PROVIDER,
            symbol=request.symbol,
            side=request.side,
            amount_mode="exact_input",
            requested_amount=request.requested_amount,
            slippage_bps=int(request.slippage_bps),
            taker_address=request.taker_address,
            confirm_prepare=True,
        ),
        db,
    )
    try:
        handoff = validate_wallet_successful_swap_handoff(dict(fresh_preflight.get("wallet_request") or {}))
        capability = create_wallet_swap_capability(
            handoff,
            symbol=str(fresh_preflight.get("symbol") or request.symbol),
            side=str(fresh_preflight.get("side") or request.side),
            requested_amount=str(fresh_preflight.get("requested_amount") or request.requested_amount),
            approval_tx_hash=request.approval_tx_hash,
        )
    except ValueError as exc:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail={
                "error": str(exc),
                "successful_broadcast_authorized": False,
                "approval_request_authorized": False,
                "swap_request_authorized": False,
                "automatic_second_transaction": False,
            },
        ) from exc

    try:
        decoded_capability = decode_wallet_swap_capability(capability["token"])
        lifecycle_row, lifecycle_created = _persist_generic_wallet_swap_prepared_lifecycle(
            db,
            fresh_preflight=fresh_preflight,
            handoff=handoff,
            capability_token=capability["token"],
            capability=decoded_capability,
            slippage_bps=int(request.slippage_bps),
            approval_tx_hash=request.approval_tx_hash,
        )
        db.commit()
    except HTTPException:
        db.rollback()
        raise
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": str(exc)}) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail={"error": "wallet_swap_durable_lifecycle_create_failed", "message": str(exc)},
        ) from exc

    gate = dict(fresh_preflight.get("gate") or {})
    gate.update({
        "reject_only": False,
        "swap_only": True,
        "successful_broadcast_authorized": True,
        "approval_request_authorized": False,
        "swap_request_authorized": True,
        "automatic_retry": False,
        "automatic_second_transaction": False,
        "durable_lifecycle_required": True,
        "durable_lifecycle_created": True,
    })
    return {
        **fresh_preflight,
        "tranche": "RH-ORDER.MISS.1B",
        "wallet_request": handoff,
        "gate": gate,
        "swap_capability": capability["token"],
        "swap_receipt_expires_at_epoch": capability["expires_at_epoch"],
        "swap_receipt_ttl_seconds": capability["ttl_seconds"],
        "execution_id": str(lifecycle_row.id),
        "durable_lifecycle_created": True,
        "durable_lifecycle_idempotent": not lifecycle_created,
        "successful_broadcast_authorized": True,
        "reject_only": False,
        "swap_only": True,
        "approval_request_authorized": False,
        "swap_request_authorized": True,
        "provider_simulation_required": True,
        "provider_simulation_complete": True,
        "fresh_post_approval_plan": True,
        "automatic_retry": False,
        "automatic_second_transaction": False,
        "order_mutation": False,
        "ledger_mutation": False,
        "fifo_mutation": False,
        "basis_mutation": False,
        "tax_mutation": False,
        "database_mutation": bool(lifecycle_created),
        "will_mutate": False,
    }


@router.post("/wallet-swap/execution/{execution_id}/submission")
def robinhood_chain_wallet_swap_submission(
    execution_id: str,
    request: RobinhoodChainWalletSwapSubmissionRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Persist the MetaMask-returned hash immediately against the durable owner."""
    if request.confirm_record is not True:
        raise HTTPException(status_code=400, detail={"error": "wallet_swap_submission_confirmation_required"})
    try:
        capability = decode_wallet_swap_capability(request.capability)
        row, changed = _record_generic_wallet_swap_submission(
            db,
            execution_id=execution_id,
            tx_hash=request.tx_hash,
            capability_token=request.capability,
            capability=capability,
        )
        db.commit()
        return {
            "ok": True,
            "tranche": "RH-ORDER.MISS.1B",
            "execution_id": str(row.id),
            "tx_hash": str(row.swap_tx_hash or ""),
            "status": str(row.status or ""),
            "submission_recorded": True,
            "idempotent": not changed,
            "automatic_retry": False,
            "automatic_second_transaction": False,
            "order_mutation": False,
            "ledger_mutation": False,
            "fifo_mutation": False,
            "basis_mutation": False,
            "tax_mutation": False,
            "database_mutation": bool(changed),
        }
    except HTTPException:
        db.rollback()
        raise
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": str(exc)}) from exc
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail={"error": "wallet_swap_submission_record_failed", "message": str(exc)}) from exc


async def _rh_order_miss_1b_orphan_recovery(
    db: Session,
    *,
    tx_hash: str,
    approval_tx_hash: str,
    persist: bool,
) -> Dict[str, Any]:
    normalized_tx_hash, normalized_approval_hash = _rh_order_miss_1b_target_hash_guard(tx_hash, approval_tx_hash)
    evidence = _rh_order_miss_1b_wallet_evidence(db, normalized_tx_hash)
    if evidence["external_owner_count"] != 0:
        raise HTTPException(status_code=409, detail={"error": "rh_order_miss_1b_external_owner_present"})
    if evidence["lifecycle_owner_count"] > 1:
        raise HTTPException(status_code=409, detail={"error": "rh_order_miss_1b_multiple_lifecycle_owners"})
    if evidence["lifecycle_owner_count"] == 1:
        existing = (
            db.query(RobinhoodChainSwapExecution)
            .filter(RobinhoodChainSwapExecution.swap_tx_hash == normalized_tx_hash)
            .first()
        )
        route = existing.route if existing is not None and isinstance(existing.route, dict) else {}
        historical = route.get("historical_preflight") if isinstance(route.get("historical_preflight"), dict) else {}
        reconciliation = route.get("execution_reconciliation") if isinstance(route.get("execution_reconciliation"), dict) else {}
        if not (
            existing is not None
            and existing.status == "confirmed"
            and historical.get("available") is False
            and reconciliation.get("reconciled") is True
            and reconciliation.get("source") == "rh_order_miss_1b_orphan_recovery"
        ):
            raise HTTPException(status_code=409, detail={"error": "rh_order_miss_1b_unexpected_lifecycle_owner"})

    reconciliation = await _persist_generic_wallet_swap_reconciliation(
        db,
        tx_hash=normalized_tx_hash,
        symbol=_RH_ORDER_MISS_1B_TARGET_SYMBOL,
        side=_RH_ORDER_MISS_1B_TARGET_SIDE,
        requested_amount=_RH_ORDER_MISS_1B_TARGET_INPUT_AMOUNT,
        quoted_output_amount=None,
        minimum_received=None,
        approval_tx_hash=normalized_approval_hash,
        source="rh_order_miss_1b_orphan_recovery",
        historical_preflight_available=False,
        expected_actual_output_amount=_RH_ORDER_MISS_1B_TARGET_OUTPUT_AMOUNT,
        persist=persist,
    )
    return {
        "ok": True,
        "tranche": "RH-ORDER.MISS.1B",
        "target": "INDEX-USDG",
        "preview": not persist,
        "apply": bool(persist),
        "target_hash_guard": True,
        "wallet_evidence": evidence,
        "historical_preflight_available": False,
        "original_quote_available": False,
        "original_minimum_available": False,
        "original_plan_timestamps_available": False,
        "recovery_provenance": "utt_orphan",
        "canonical_source": "RHCHAINSWAP",
        "external_materialization": False,
        "reconciliation": reconciliation,
        "order_mutation": bool(reconciliation.get("order_mutation")) if persist else False,
        "ledger_mutation": False,
        "fifo_mutation": False,
        "basis_mutation": False,
        "tax_mutation": False,
        "database_mutation": bool(reconciliation.get("database_mutation")) if persist else False,
        "will_mutate": False if not persist else bool(reconciliation.get("database_mutation")),
    }


@router.post("/wallet-swap/orphan-recovery/preview")
async def robinhood_chain_wallet_swap_orphan_recovery_preview(
    request: RobinhoodChainWalletSwapOrphanRecoveryRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Target-specific read-only preview for the accepted August 14 INDEX-USDG orphan."""
    try:
        result = await _rh_order_miss_1b_orphan_recovery(
            db,
            tx_hash=request.tx_hash,
            approval_tx_hash=request.approval_tx_hash,
            persist=False,
        )
        db.rollback()
        result["database_mutation"] = False
        result["will_mutate"] = False
        return result
    except HTTPException:
        db.rollback()
        raise
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail={"error": "rh_order_miss_1b_preview_failed", "message": str(exc)}) from exc


@router.post("/wallet-swap/orphan-recovery/apply")
async def robinhood_chain_wallet_swap_orphan_recovery_apply(
    request: RobinhoodChainWalletSwapOrphanRecoveryRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Explicitly recover only the fingerprint-guarded accepted INDEX-USDG orphan."""
    if request.confirm_recovery is not True:
        raise HTTPException(status_code=400, detail={"error": "rh_order_miss_1b_recovery_confirmation_required"})
    try:
        result = await _rh_order_miss_1b_orphan_recovery(
            db,
            tx_hash=request.tx_hash,
            approval_tx_hash=request.approval_tx_hash,
            persist=True,
        )
        db.commit()
        return result
    except HTTPException:
        db.rollback()
        raise
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail={"error": "rh_order_miss_1b_recovery_failed", "message": str(exc)}) from exc


@router.post("/wallet-swap/reconcile-confirmed")
async def robinhood_chain_wallet_swap_reconcile_confirmed(
    request: RobinhoodChainWalletSwapReconcileRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Explicitly backfill/reconcile one confirmed generic UTT wallet swap.

    This is intentionally not a general external-wallet scanner. RH-WALLET.INGEST.1
    remains responsible for arbitrary MetaMask activity outside the UTT lifecycle.
    """
    if request.confirm_reconcile is not True:
        raise HTTPException(status_code=400, detail={"error": "wallet_swap_reconcile_confirmation_required"})
    try:
        result = await _persist_generic_wallet_swap_reconciliation(
            db,
            tx_hash=request.tx_hash,
            symbol=request.symbol,
            side=request.side,
            requested_amount=request.requested_amount,
            quoted_output_amount=request.quoted_output_amount,
            minimum_received=request.minimum_received,
            approval_tx_hash=request.approval_tx_hash,
            source="explicit_confirmed_backfill",
        )
        db.commit()
        return result
    except HTTPException:
        db.rollback()
        raise
    except Exception as exc:
        db.rollback()
        raise HTTPException(status_code=500, detail={"error": "wallet_swap_reconcile_failed", "message": str(exc)}) from exc


@router.post("/wallet-swap/receipt")
async def robinhood_chain_wallet_swap_receipt(
    request: RobinhoodChainWalletSwapReceiptRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Verify one submitted swap transaction and persist receipt-verified order reconciliation only."""
    try:
        tx_hash = validate_transaction_hash(request.tx_hash)
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc

    try:
        capability, expired_capability_recovery = _decode_generic_wallet_swap_receipt_capability(
            db,
            capability_token=request.capability,
            execution_id=request.execution_id,
            tx_hash=tx_hash,
        )
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": str(exc)}) from exc

    saved_wallet = _resolve_robinhood_chain_execution_taker(db, str(capability.get("wallet_address") or ""))
    if saved_wallet.lower() != str(capability.get("wallet_address") or "").lower():
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_saved_wallet_mismatch"})

    registry_service = get_robinhood_chain_registry_discovery_service()
    try:
        verified_input = registry_service.resolve_verified_token(db, str(capability.get("input_asset") or ""))
        verified_output = registry_service.resolve_verified_token(db, str(capability.get("output_asset") or ""))
    except ValueError as exc:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail={"error": "wallet_swap_verified_registry_identity_required", "message": str(exc)},
        ) from exc
    if (
        int(verified_input.get("registry_id") or 0) != int(capability.get("input_registry_id") or 0)
        or int(verified_output.get("registry_id") or 0) != int(capability.get("output_registry_id") or 0)
    ):
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_registry_identity_changed"})

    submission_changed = False
    if request.execution_id:
        try:
            _, submission_changed = _record_generic_wallet_swap_submission(
                db,
                execution_id=request.execution_id,
                tx_hash=tx_hash,
                capability_token=request.capability,
                capability=capability,
            )
            db.commit()
        except HTTPException:
            db.rollback()
            raise

    rpc = get_robinhood_chain_client()
    chain = await rpc.verify_expected_chain(force_refresh=True)
    if chain.get("ok") is not True or chain.get("chain_id_matches") is not True:
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": "wallet_swap_rpc_chain_mismatch"})

    tx_record = await rpc.rpc_read(
        "eth_getTransactionByHash",
        [tx_hash],
        cache_namespace=None,
        force_refresh=True,
    )
    if tx_record.get("ok") is not True:
        db.rollback()
        raise HTTPException(status_code=502, detail={"error": "wallet_swap_transaction_read_failed"})
    transaction = tx_record.get("result")
    if transaction is None:
        db.rollback()
        return {
            "ok": True,
            "tranche": "R5C.5D.2F.3",
            "status": "submitted_pending_visibility",
            "pending": True,
            "confirmed": False,
            "tx_hash": tx_hash,
            "execution_id": request.execution_id,
            "swap_only": True,
            "approval_request_authorized": False,
            "automatic_second_transaction": False,
            "receipt_authority_source": (
                "durable_execution_bound_expired_capability"
                if expired_capability_recovery
                else "fresh_signed_capability"
            ),
            "expired_capability_recovery": bool(expired_capability_recovery),
            "new_wallet_request_authorized": False,
            "database_mutation": bool(submission_changed),
        }
    try:
        verified_tx = validate_wallet_swap_transaction(
            capability,
            tx_hash=tx_hash,
            transaction=transaction,
        )
    except ValueError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail={"error": str(exc)}) from exc

    receipt_record = await rpc.rpc_read(
        "eth_getTransactionReceipt",
        [tx_hash],
        cache_namespace=None,
        force_refresh=True,
    )
    if receipt_record.get("ok") is not True:
        db.rollback()
        raise HTTPException(status_code=502, detail={"error": "wallet_swap_receipt_read_failed"})
    receipt = receipt_record.get("result")
    if receipt is None:
        db.rollback()
        return {
            "ok": True,
            "tranche": "R5C.5D.2F.3",
            "status": "swap_pending",
            "pending": True,
            "confirmed": False,
            "tx_hash": tx_hash,
            "execution_id": request.execution_id,
            "transaction": verified_tx,
            "swap_only": True,
            "approval_request_authorized": False,
            "automatic_second_transaction": False,
            "receipt_authority_source": (
                "durable_execution_bound_expired_capability"
                if expired_capability_recovery
                else "fresh_signed_capability"
            ),
            "expired_capability_recovery": bool(expired_capability_recovery),
            "new_wallet_request_authorized": False,
            "database_mutation": bool(submission_changed),
        }

    receipt_status = _decode_robinhood_chain_rpc_quantity(receipt.get("status"), field="wallet_swap_receipt_status")
    block_number = _decode_robinhood_chain_rpc_quantity(receipt.get("blockNumber"), field="wallet_swap_block_number") if receipt.get("blockNumber") is not None else None
    gas_used = _decode_robinhood_chain_rpc_quantity(receipt.get("gasUsed"), field="wallet_swap_gas_used") if receipt.get("gasUsed") is not None else None
    effective_gas_price = _decode_robinhood_chain_rpc_quantity(receipt.get("effectiveGasPrice"), field="wallet_swap_effective_gas_price") if receipt.get("effectiveGasPrice") is not None else None
    network_fee_wei = gas_used * effective_gas_price if gas_used is not None and effective_gas_price is not None else None
    if receipt_status == 0:
        reverted_changed = _mark_generic_wallet_swap_reverted(
            db,
            execution_id=request.execution_id,
            tx_hash=tx_hash,
            block_number=block_number,
            gas_used=gas_used,
            effective_gas_price=effective_gas_price,
        )
        if reverted_changed:
            db.commit()
        else:
            db.rollback()
        return {
            "ok": True,
            "tranche": "R5C.5D.2F.3",
            "status": "swap_reverted",
            "pending": False,
            "confirmed": False,
            "reverted": True,
            "tx_hash": tx_hash,
            "execution_id": request.execution_id,
            "transaction": verified_tx,
            "receipt_status": 0,
            "block_number": block_number,
            "gas_used": str(gas_used) if gas_used is not None else None,
            "effective_gas_price_wei": str(effective_gas_price) if effective_gas_price is not None else None,
            "network_fee_wei": str(network_fee_wei) if network_fee_wei is not None else None,
            "automatic_second_transaction": False,
            "receipt_authority_source": (
                "durable_execution_bound_expired_capability"
                if expired_capability_recovery
                else "fresh_signed_capability"
            ),
            "expired_capability_recovery": bool(expired_capability_recovery),
            "new_wallet_request_authorized": False,
            "database_mutation": bool(submission_changed or reverted_changed),
        }
    if receipt_status != 1:
        db.rollback()
        raise HTTPException(status_code=502, detail={"error": "wallet_swap_receipt_status_invalid"})

    reconciliation_result: Optional[Dict[str, Any]] = None
    reconciliation_error: Optional[str] = None
    try:
        reconciliation_result = await _persist_generic_wallet_swap_reconciliation(
            db,
            tx_hash=tx_hash,
            symbol=str(capability.get("symbol") or ""),
            side=str(capability.get("side") or ""),
            requested_amount=str(capability.get("input_amount") or capability.get("requested_amount") or ""),
            quoted_output_amount=str(capability.get("output_amount") or ""),
            minimum_received=str(capability.get("minimum_received") or ""),
            approval_tx_hash=str(capability.get("approval_tx_hash") or "").strip() or None,
            source="wallet_swap_receipt",
        )
        db.commit()
    except Exception as exc:
        db.rollback()
        reconciliation_error = str(getattr(exc, "detail", None) or exc)

    return {
        "ok": True,
        "tranche": "R5C.5D.2F.4",
        "status": "swap_confirmed",
        "pending": False,
        "confirmed": True,
        "reverted": False,
        "tx_hash": tx_hash,
        "execution_id": request.execution_id or (reconciliation_result or {}).get("execution_id"),
        "submission_recorded": bool(request.execution_id),
        "symbol": capability.get("symbol"),
        "side": capability.get("side"),
        "input_asset": capability.get("input_asset"),
        "output_asset": capability.get("output_asset"),
        "requested_amount": capability.get("requested_amount"),
        "input_amount": capability.get("input_amount"),
        "input_amount_atomic": capability.get("input_amount_atomic"),
        "quoted_output_amount": capability.get("output_amount"),
        "minimum_received": capability.get("minimum_received"),
        "minimum_received_atomic": capability.get("minimum_received_atomic"),
        "transaction": verified_tx,
        "receipt_status": 1,
        "block_number": block_number,
        "gas_used": str(gas_used) if gas_used is not None else None,
        "effective_gas_price_wei": str(effective_gas_price) if effective_gas_price is not None else None,
        "network_fee_wei": str(network_fee_wei) if network_fee_wei is not None else None,
        "successful_broadcast_authorized": True,
        "swap_only": True,
        "approval_request_authorized": False,
        "swap_request_authorized": True,
        "automatic_retry": False,
        "automatic_second_transaction": False,
        "receipt_authority_source": (
            "durable_execution_bound_expired_capability"
            if expired_capability_recovery
            else "fresh_signed_capability"
        ),
        "expired_capability_recovery": bool(expired_capability_recovery),
        "new_wallet_request_authorized": False,
        "reconciliation": reconciliation_result,
        "reconciliation_error": reconciliation_error,
        "order_mutation": bool(reconciliation_result and reconciliation_result.get("order_mutation")),
        "ledger_mutation": False,
        "fifo_mutation": False,
        "basis_mutation": False,
        "tax_mutation": False,
        "database_mutation": bool(submission_changed or (reconciliation_result and reconciliation_result.get("database_mutation"))),
        "will_mutate": False,
    }


@router.post("/execution-authority/resolve")
async def robinhood_chain_execution_authority_resolve(
    request: RobinhoodChainExecutionAuthorityRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Resolve persisted execution authority without provider contact or mutation."""
    payload = _resolve_robinhood_chain_execution_authority_or_http(
        db,
        symbol=request.symbol,
        side=request.side,
        amount_mode=request.amount_mode,
        provider=request.provider,
        require_execution=False,
    )
    db.rollback()
    return payload


@router.post("/execution-authority/authorize-controlled-buy")
async def robinhood_chain_execution_authority_authorize_controlled_buy(
    request: RobinhoodChainControlledLiveAuthorizationRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Authorize one exact 1 USDG WETH BUY without requesting or sending a wallet transaction."""
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")
    taker = _resolve_robinhood_chain_execution_taker(db, request.taker_address)
    try:
        return get_robinhood_chain_registry_discovery_service().authorize_controlled_live_buy(
            db,
            symbol=request.symbol,
            side=request.side,
            amount_mode=request.amount_mode,
            requested_amount=request.requested_amount,
            provider=request.provider,
            wallet_address=taker,
            confirm_authorize=bool(request.confirm_authorize),
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        error = str(exc)
        raise HTTPException(
            status_code=409,
            detail={
                "error": error,
                "wallet_connection_requested": False,
                "signing_enabled": False,
                "broadcast_enabled": False,
                "successful_broadcast": False,
                "automatic_second_transaction": False,
                "automatic_retry": False,
                "automatic_execution_promotion": False,
            },
        ) from exc


@router.post("/execution-authority/authorize-controlled-sell")
async def robinhood_chain_execution_authority_authorize_controlled_sell(
    request: RobinhoodChainControlledLiveAuthorizationRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Authorize one exact 0.0001 WETH SELL without requesting or sending a wallet transaction."""
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")
    taker = _resolve_robinhood_chain_execution_taker(db, request.taker_address)
    try:
        return get_robinhood_chain_registry_discovery_service().authorize_controlled_live_sell(
            db,
            symbol=request.symbol,
            side=request.side,
            amount_mode=request.amount_mode,
            requested_amount=request.requested_amount,
            provider=request.provider,
            wallet_address=taker,
            confirm_authorize=bool(request.confirm_authorize),
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        error = str(exc)
        raise HTTPException(
            status_code=409,
            detail={
                "error": error,
                "wallet_connection_requested": False,
                "signing_enabled": False,
                "broadcast_enabled": False,
                "successful_broadcast": False,
                "automatic_second_transaction": False,
                "automatic_retry": False,
                "automatic_execution_promotion": False,
            },
        ) from exc


@router.post("/execution-authority/verify-preparation")
async def robinhood_chain_execution_authority_verify_preparation(
    request: RobinhoodChainPreparationVerificationRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Persist bounded WETH preparation evidence after a validated firm plan.

    R5C.4A remains locked to WETH-USDG BUY at 1 USDG. R5C.4B adds
    WETH-USDG SELL at exactly 0.0001 WETH. This endpoint never requests
    MetaMask, signs, broadcasts, or claims live execution verification.
    """
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")
    taker = _resolve_robinhood_chain_execution_taker(db, request.taker_address)
    try:
        return await get_robinhood_chain_registry_discovery_service().verify_preparation_authority(
            db,
            symbol=request.symbol,
            side=request.side,
            amount_mode=request.amount_mode,
            requested_amount=request.requested_amount,
            taker_address=taker,
            slippage_bps=int(request.slippage_bps),
            confirm_verify=bool(request.confirm_verify),
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        error = str(exc)
        if "not_found" in error or "missing" in error:
            status_code = 404
        elif "provider" in error or "firm_plan" in error or "allowance" in error:
            status_code = 502
        else:
            status_code = 409
        raise HTTPException(
            status_code=status_code,
            detail={
                "error": error,
                "wallet_connection_requested": False,
                "signing_enabled": False,
                "broadcast_enabled": False,
                "successful_broadcast": False,
                "automatic_execution_promotion": False,
            },
        ) from exc


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

    authority = _resolve_robinhood_chain_execution_authority_or_http(
        db,
        symbol=request.symbol,
        side=request.side,
        amount_mode="exact_input",
        provider="0x",
        require_execution=True,
    )
    if str(authority.get("execution_adapter") or "") != "native_exact_input":
        raise HTTPException(
            status_code=409,
            detail={
                "error": "robinhood_chain_native_execution_adapter_required",
                "authority": authority,
                "provider_contacted": False,
            },
        )
    normalized_authority_amount = _assert_robinhood_chain_execution_amount_or_http(
        authority,
        request.quantity,
    )
    try:
        _, normalized_quantity, _ = normalize_robinhood_chain_execution_quantity(normalized_authority_amount)
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "error": str(exc),
                "historical_amount_evidence": authority.get("execution_ceiling"),
                "provider_contacted": False,
            },
        ) from exc

    taker = _resolve_robinhood_chain_execution_taker(db, request.taker_address)
    eth = dict(authority.get("input") or {})
    usdg = dict(authority.get("output") or {})
    try:
        result = await get_robinhood_chain_execution_service().prepare(
            db,
            taker_address=taker,
            eth_token=eth,
            usdg_token=usdg,
            quantity=normalized_quantity,
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
        return await get_robinhood_chain_execution_service().claim_send(
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


@router.get("/swap-execution/status")
async def robinhood_chain_swap_execution_status() -> Dict[str, Any]:
    """Return the exact-spend approval gate and the separately scoped swap gate."""
    return get_robinhood_chain_swap_execution_service().status()


@router.post("/swap-execution/prepare")
async def robinhood_chain_swap_execution_prepare(
    request: RobinhoodChainSwapExecutionPrepareRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")
    authority = _resolve_robinhood_chain_execution_authority_or_http(
        db,
        symbol=request.symbol,
        side=request.side,
        amount_mode=request.amount_mode,
        provider="0x",
        require_execution=True,
    )
    if str(authority.get("execution_adapter") or "") != "erc20_exact_input":
        raise HTTPException(
            status_code=409,
            detail={
                "error": "robinhood_chain_erc20_execution_adapter_required",
                "authority": authority,
                "provider_contacted": False,
            },
        )
    from_asset = str(request.from_asset or "").strip().upper()
    to_asset = str(request.to_asset or "").strip().upper()
    input_token = dict(authority.get("input") or {})
    output_token = dict(authority.get("output") or {})
    if (
        from_asset != str(input_token.get("symbol") or "").strip().upper()
        or to_asset != str(output_token.get("symbol") or "").strip().upper()
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "error": "robinhood_chain_swap_market_identity_mismatch",
                "symbol": authority.get("symbol"),
                "input_asset": input_token.get("symbol"),
                "output_asset": output_token.get("symbol"),
                "provider_contacted": False,
            },
        )
    normalized_input_amount = _assert_robinhood_chain_execution_amount_or_http(
        authority,
        request.exact_input_amount,
    )
    capability = dict(authority.get("capability") or {})
    capability.update({
        "from_asset": input_token.get("symbol"),
        "to_asset": output_token.get("symbol"),
        "amount_mode": authority.get("amount_mode"),
        "mechanism": authority.get("mechanism"),
        "execution_authority": authority,
    })
    taker = _resolve_robinhood_chain_execution_taker(db, request.taker_address)
    try:
        return await get_robinhood_chain_swap_execution_service().prepare(
            db,
            taker_address=taker,
            exact_input_amount=normalized_input_amount,
            slippage_bps=int(request.slippage_bps),
            eth_token=output_token if bool(output_token.get("native")) else None,
            usdg_token=input_token,
            side=str(authority.get("side") or request.side),
            symbol=str(authority.get("symbol") or request.symbol),
            from_asset=from_asset,
            from_token=input_token,
            to_asset=to_asset,
            to_token=output_token,
            route_capability=capability,
            confirm_prepare=bool(request.confirm_prepare),
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        raise HTTPException(status_code=400, detail={"error": str(exc)}) from exc


@router.get("/swap-execution/latest")
async def robinhood_chain_swap_execution_latest(
    symbol: str = Query(..., min_length=1, max_length=80),
    side: str = Query(..., min_length=3, max_length=4),
    amount_mode: str = Query(default=ROBINHOOD_CHAIN_SWAP_AMOUNT_MODE, min_length=1, max_length=32),
    wallet_address: Optional[str] = Query(default=None, min_length=42, max_length=42),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Restore the newest matching lifecycle by read-only database lookup."""
    try:
        payload = get_robinhood_chain_swap_execution_service().latest(
            db,
            symbol=symbol,
            side=side,
            amount_mode=amount_mode,
            wallet_address=wallet_address,
        )
        db.rollback()
        return payload
    except (ValueError, KeyError) as exc:
        db.rollback()
        raise HTTPException(
            status_code=404 if "not_found" in str(exc) else 400,
            detail={
                "error": str(exc),
                "read_only": True,
                "will_mutate": False,
                "wallet_connection_requested": False,
                "signing_enabled": False,
                "broadcast_enabled": False,
            },
        ) from exc


@router.get("/swap-execution/{execution_id}")
async def robinhood_chain_swap_execution_get(execution_id: str, db: Session = Depends(get_db)) -> Dict[str, Any]:
    try:
        payload = get_robinhood_chain_swap_execution_service().get(db, execution_id)
        db.rollback()
        return payload
    except (ValueError, KeyError) as exc:
        db.rollback()
        raise HTTPException(status_code=404 if "not_found" in str(exc) else 400, detail={"error": str(exc)}) from exc


@router.post("/swap-execution/{execution_id}/approval/claim-send")
async def robinhood_chain_swap_execution_claim_approval_send(
    execution_id: str, request: RobinhoodChainExecutionSendClaimRequest, db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        service = get_robinhood_chain_swap_execution_service()
        current = service.get(db, execution_id)
        execution = current.get("execution") if isinstance(current, dict) else None
        if not isinstance(execution, dict):
            raise ValueError("robinhood_chain_swap_execution_not_found")
        authority = _assert_persisted_swap_execution_authority(
            db, execution,
            require_successful_broadcast=False,
        )
        return service.claim_approval_send(
            db, execution_id=execution_id, wallet_address=request.wallet_address,
            plan_hash=request.plan_hash, claim_id=request.claim_id,
            confirm_send_claim=bool(request.confirm_send_claim),
            execution_authority=authority,
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        error = str(exc)
        raise HTTPException(status_code=404 if "not_found" in error else 409, detail={"error": error}) from exc


@router.post("/swap-execution/{execution_id}/approval/submission")
async def robinhood_chain_swap_execution_record_approval_submission(
    execution_id: str, request: RobinhoodChainExecutionSubmissionRequest, db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return get_robinhood_chain_swap_execution_service().record_approval_submission(
            db, execution_id=execution_id, tx_hash=request.tx_hash,
            wallet_address=request.wallet_address, claim_id=request.claim_id,
            confirm_record=bool(request.confirm_record),
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        error = str(exc)
        raise HTTPException(status_code=404 if "not_found" in error else 409, detail={"error": error}) from exc


@router.post("/swap-execution/{execution_id}/approval/submission-failure")
async def robinhood_chain_swap_execution_record_approval_failure(
    execution_id: str, request: RobinhoodChainExecutionSubmissionFailureRequest, db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return get_robinhood_chain_swap_execution_service().record_submission_failure(
            db, execution_id=execution_id, stage="approval", wallet_address=request.wallet_address,
            claim_id=request.claim_id, reason=request.reason, message=request.message,
            confirm_failure=bool(request.confirm_failure),
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        error = str(exc)
        raise HTTPException(status_code=404 if "not_found" in error else 409, detail={"error": error}) from exc


@router.post("/swap-execution/{execution_id}/approval/refresh")
async def robinhood_chain_swap_execution_refresh_approval(
    execution_id: str, db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return await get_robinhood_chain_swap_execution_service().refresh_approval(db, execution_id=execution_id)
    except (ValueError, KeyError) as exc:
        db.rollback()
        error = str(exc)
        status_code = 404 if "not_found" in error else 409 if "not_pending" in error else 502
        raise HTTPException(status_code=status_code, detail={"error": error}) from exc


@router.post("/swap-execution/{execution_id}/prepare-swap")
async def robinhood_chain_swap_execution_prepare_fresh_swap(
    execution_id: str, request: RobinhoodChainSwapFreshPrepareRequest, db: Session = Depends(get_db),
) -> Dict[str, Any]:
    if not bool(settings.robinhood_chain_effective_enabled()):
        raise HTTPException(status_code=503, detail="Robinhood Chain configuration is not effective for chain ID 4663")
    try:
        service = get_robinhood_chain_swap_execution_service()
        current = service.get(db, execution_id)
        execution = current.get("execution") if isinstance(current, dict) else None
        if not isinstance(execution, dict):
            raise ValueError("robinhood_chain_swap_execution_not_found")
        authority = _assert_persisted_swap_execution_authority(
            db, execution,
            require_successful_broadcast=False,
        )
        if str(authority.get("execution_adapter") or "") != "erc20_exact_input":
            raise ValueError("robinhood_chain_erc20_execution_adapter_required")
        input_token = dict(authority.get("input") or {})
        output_token = dict(authority.get("output") or {})
        capability = dict(authority.get("capability") or {})
        capability.update({
            "from_asset": input_token.get("symbol"),
            "to_asset": output_token.get("symbol"),
            "amount_mode": authority.get("amount_mode"),
            "mechanism": authority.get("mechanism"),
            "execution_authority": authority,
        })
        return await service.prepare_swap(
            db, execution_id=execution_id, wallet_address=request.wallet_address,
            eth_token=output_token if bool(output_token.get("native")) else None,
            usdg_token=input_token,
            output_token=output_token,
            route_capability=capability,
            confirm_prepare=bool(request.confirm_prepare),
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        error = str(exc)
        raise HTTPException(status_code=404 if "not_found" in error else 409, detail={"error": error}) from exc


@router.post("/swap-execution/{execution_id}/swap/claim-send")
async def robinhood_chain_swap_execution_claim_swap_send(
    execution_id: str, request: RobinhoodChainExecutionSendClaimRequest, db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        service = get_robinhood_chain_swap_execution_service()
        current = service.get(db, execution_id)
        execution = current.get("execution") if isinstance(current, dict) else None
        if not isinstance(execution, dict):
            raise ValueError("robinhood_chain_swap_execution_not_found")
        authority = _assert_persisted_swap_execution_authority(
            db, execution,
            require_successful_broadcast=False,
        )
        return await service.claim_swap_send(
            db, execution_id=execution_id, wallet_address=request.wallet_address,
            plan_hash=request.plan_hash, claim_id=request.claim_id,
            confirm_send_claim=bool(request.confirm_send_claim),
            execution_authority=authority,
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        error = str(exc)
        raise HTTPException(status_code=404 if "not_found" in error else 409, detail={"error": error}) from exc


@router.post("/swap-execution/{execution_id}/swap/submission")
async def robinhood_chain_swap_execution_record_swap_submission(
    execution_id: str, request: RobinhoodChainExecutionSubmissionRequest, db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return get_robinhood_chain_swap_execution_service().record_swap_submission(
            db, execution_id=execution_id, tx_hash=request.tx_hash,
            wallet_address=request.wallet_address, claim_id=request.claim_id,
            confirm_record=bool(request.confirm_record),
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        error = str(exc)
        raise HTTPException(status_code=404 if "not_found" in error else 409, detail={"error": error}) from exc


@router.post("/swap-execution/{execution_id}/swap/submission-failure")
async def robinhood_chain_swap_execution_record_swap_failure(
    execution_id: str, request: RobinhoodChainExecutionSubmissionFailureRequest, db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        return get_robinhood_chain_swap_execution_service().record_submission_failure(
            db, execution_id=execution_id, stage="swap", wallet_address=request.wallet_address,
            claim_id=request.claim_id, reason=request.reason, message=request.message,
            confirm_failure=bool(request.confirm_failure),
        )
    except (ValueError, KeyError) as exc:
        db.rollback()
        error = str(exc)
        raise HTTPException(status_code=404 if "not_found" in error else 409, detail={"error": error}) from exc


@router.post("/swap-execution/{execution_id}/swap/refresh")
async def robinhood_chain_swap_execution_refresh_swap(
    execution_id: str, db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        result = await get_robinhood_chain_swap_execution_service().refresh_swap(db, execution_id=execution_id)
        execution = result.get("execution") if isinstance(result, dict) else None
        if isinstance(execution, dict) and str(execution.get("status") or "").lower() == "confirmed":
            output_asset = str(execution.get("actual_output_asset") or execution.get("to_asset") or "").strip().upper()
            result["balance_refresh"] = await _refresh_robinhood_chain_execution_balance_snapshots(
                db,
                str(execution.get("wallet_address") or ""),
                additional_erc20_assets=[output_asset] if output_asset not in {"", "ETH", "USDG"} else None,
            )
        return result
    except (ValueError, KeyError) as exc:
        db.rollback()
        error = str(exc)
        status_code = 404 if "not_found" in error else 409 if "not_pending" in error else 502
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
    symbol: str = Query(..., min_length=1, max_length=32),
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
    registry_service = get_robinhood_chain_registry_discovery_service()
    try:
        market = registry_service.market_by_symbol(db, symbol)
    except ValueError as exc:
        db.rollback()
        raise HTTPException(
            status_code=404,
            detail={
                "error": str(exc),
                "symbol": str(symbol or "").strip().upper(),
                "identity_source": "token_registry",
                "capability_source": "database",
                "provider_contacted": False,
            },
        ) from exc

    if str(market.get("mechanism") or "").strip().lower() != "swap":
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail={
                "error": "robinhood_chain_orderbook_mechanism_not_supported",
                "symbol": market.get("symbol"),
                "mechanism": market.get("mechanism"),
                "presentation": "wrap_unwrap",
                "provider_contacted": False,
                "read_only": True,
                "execution_enabled": False,
            },
        )

    base = market.get("base") if isinstance(market.get("base"), dict) else {}
    quote = market.get("quote") if isinstance(market.get("quote"), dict) else {}
    capabilities = [
        item for item in (market.get("capabilities") or [])
        if isinstance(item, dict)
        and str(item.get("amount_mode") or "").strip().lower() == "exact_input"
    ]
    base_symbol = str(base.get("symbol") or "").strip().upper()
    quote_symbol = str(quote.get("symbol") or "").strip().upper()
    preferred_provider = str(market.get("preferred_orderbook_provider") or "").strip().lower()
    if preferred_provider not in {
        ROBINHOOD_CHAIN_QUOTE_PROVIDER,
        UNISWAP_PROVIDER,
        UNISWAP_V3_RPC_PROVIDER,
    }:
        db.rollback()
        raise HTTPException(
            status_code=409,
            detail={
                "error": "robinhood_chain_same_provider_orderbook_unavailable",
                "symbol": market.get("symbol"),
                "orderbook_providers": market.get("orderbook_providers") or [],
                "provider_contacted": False,
            },
        )

    def direction_capability(from_asset: str, to_asset: str) -> Optional[Dict[str, Any]]:
        return next(
            (
                item for item in capabilities
                if str(item.get("provider") or "").strip().lower() == preferred_provider
                and str(item.get("from_asset") or "").strip().upper() == from_asset
                and str(item.get("to_asset") or "").strip().upper() == to_asset
            ),
            None,
        )

    base_to_quote = direction_capability(base_symbol, quote_symbol)
    quote_to_base = direction_capability(quote_symbol, base_symbol)
    if preferred_provider == UNISWAP_PROVIDER:
        result = await get_robinhood_chain_uniswap_quote_service().synthetic_orderbook_for_pair(
            symbol=market.get("symbol") or symbol,
            depth=depth,
            taker_address=taker,
            base_token=base,
            quote_token=quote,
            base_to_quote_capability=base_to_quote or {},
            quote_to_base_capability=quote_to_base or {},
        )
    elif preferred_provider == UNISWAP_V3_RPC_PROVIDER:
        try:
            weth = registry_service.resolve_verified_token(db, "WETH")
        except ValueError as exc:
            db.rollback()
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "verified_weth_registry_identity_required",
                    "message": str(exc),
                    "symbol": market.get("symbol"),
                    "provider": UNISWAP_V3_RPC_PROVIDER,
                    "provider_contacted": False,
                },
            ) from exc
        base_provider = weth if bool(base.get("native")) else base
        quote_provider = weth if bool(quote.get("native")) else quote
        result = await get_robinhood_chain_uniswap_v3_quote_service().synthetic_orderbook_for_pair(
            symbol=market.get("symbol") or symbol,
            depth=depth,
            base_token=base,
            quote_token=quote,
            base_provider_token=base_provider,
            quote_provider_token=quote_provider,
            bridge_token=weth,
            base_to_quote_capability=base_to_quote or {},
            quote_to_base_capability=quote_to_base or {},
            force_refresh=bool(force_refresh),
        )
    else:
        registry_tokens, native_token = _resolve_robinhood_chain_review_identities(db)
        result = await get_robinhood_chain_quote_service().synthetic_orderbook_for_pair(
            symbol=market.get("symbol") or symbol,
            depth=depth,
            taker_address=taker,
            base_token=base,
            quote_token=quote,
            base_to_quote_capability=base_to_quote or {},
            quote_to_base_capability=quote_to_base or {},
            native_token=native_token,
            registry_tokens=registry_tokens,
            force_refresh=bool(force_refresh),
        )
    db.rollback()
    if result.get("ok"):
        result["market"] = {
            "id": market.get("id"),
            "symbol": market.get("symbol"),
            "mechanism": market.get("mechanism"),
            "review_only": market.get("review_only"),
        }
        return result
    raise HTTPException(status_code=_quote_failure_status(result), detail=result)
