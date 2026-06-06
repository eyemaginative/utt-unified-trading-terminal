# backend/app/routers/bridge.py
from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import AssetDeposit, AssetWithdrawal, BasisLot, BridgeTransferRecord, TokenRegistry, WalletAddress

router = APIRouter(prefix="/api/bridge", tags=["bridge"])


def _env_float(name: str, default: Optional[float] = None) -> Optional[float]:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return float(str(raw).replace(",", "").strip())
    except Exception:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_str(name: str, default: Optional[str] = None) -> Optional[str]:
    raw = os.getenv(name)
    if raw is None:
        return default
    text = str(raw).strip()
    return text or default


def _token_registry_rows(db: Optional[Session], symbol: str) -> List[TokenRegistry]:
    if db is None:
        return []
    sym = str(symbol or "").strip().upper()
    if not sym:
        return []
    try:
        return (
            db.query(TokenRegistry)
            .filter(TokenRegistry.symbol == sym)
            .order_by(TokenRegistry.chain.asc(), TokenRegistry.venue.asc().nullsfirst())
            .all()
        )
    except Exception:
        return []


def _registry_row_payload(row: Optional[TokenRegistry]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    return {
        "chain": row.chain,
        "venue": row.venue,
        "symbol": row.symbol,
        "address": row.address,
        "decimals": row.decimals,
        "label": row.label,
    }


def _pick_registry_row(
    rows: List[TokenRegistry],
    *,
    chain_aliases: List[str],
    venue_aliases: Optional[List[str]] = None,
) -> Optional[TokenRegistry]:
    chain_set = {str(x or "").strip().lower() for x in chain_aliases if str(x or "").strip()}
    venue_set = {str(x or "").strip().lower() for x in (venue_aliases or []) if str(x or "").strip()}

    scored: List[tuple[int, TokenRegistry]] = []
    for row in rows or []:
        ch = str(row.chain or "").strip().lower()
        venue = str(row.venue or "").strip().lower()
        score = 999
        if ch in chain_set and venue_set and venue in venue_set:
            score = 0
        elif ch in chain_set and not venue:
            score = 1
        elif ch in chain_set:
            score = 2
        elif venue_set and venue in venue_set:
            score = 3
        if score < 999:
            scored.append((score, row))
    scored.sort(key=lambda x: x[0])
    return scored[0][1] if scored else None


def _display_asset_id(row: Optional[TokenRegistry], fallback: Optional[str] = None) -> Optional[str]:
    value = str(getattr(row, "address", "") or "").strip()
    if value:
        return value
    return fallback




_BRIDGE_TRANSFER_STATUSES = {
    "PLANNED",
    "SOURCE_SENT",
    "DESTINATION_RECEIVED",
    "LINKED",
    "RECONCILED",
    "CANCELLED",
}

_BRIDGE_TRANSFER_MECHANISMS = {
    "manual",
    "treasury_mediated",
    "burn_mint",
    "lock_release",
    "lock_mint",
    "vault_deposit_mint_xcm",
    "xcm_transfer",
    "external_bridge",
}


_UTTT_TREASURY_ROLE_DEFINITIONS: List[Dict[str, Any]] = [
    {
        "role": "solana_bridge_reserve",
        "label": "UTTT Solana Bridge Reserve",
        "chain": "solana",
        "asset": "UTTT",
        "env": "UTT_UTTT_SOLANA_BRIDGE_RESERVE_ADDRESS",
        "defaultAddress": "4zW3sGbsrCVYYAbuDM2QgtU1Xe9qnpYPFxZSprnkTPDJ",
        "requiredTerms": ["solana", "bridge", "reserve"],
        "avoidTerms": ["mixed", "trading", "pool", "lp", "initial"],
        "purpose": "Canonical Solana-side bridge reserve backing the current Hydration bridge tranche.",
    },
    {
        "role": "hydration_bridge_treasury",
        "label": "UTTT Hydration Bridge Treasury",
        "chain": "hydration",
        "asset": "UTTT",
        "env": "UTT_UTTT_HYDRATION_BRIDGE_TREASURY_ADDRESS",
        "defaultAddress": None,
        "requiredTerms": ["hydration", "bridge", "treasury"],
        "avoidTerms": ["initial", "allocation", "mixed", "trading", "pool", "lp"],
        "purpose": "Hydration-side bridge treasury for the reconciled vault/mint/XCM tranche.",
    },
    {
        "role": "hydration_initial_allocation_treasury",
        "label": "UTTT Hydration Initial Allocation Treasury",
        "chain": "hydration",
        "asset": "UTTT",
        "env": "UTT_UTTT_HYDRATION_INITIAL_ALLOCATION_TREASURY_ADDRESS",
        "defaultAddress": None,
        "requiredTerms": ["hydration", "initial", "allocation", "treasury"],
        "avoidTerms": ["bridge", "pool", "lp", "mixed", "trading"],
        "purpose": "Hydration treasury holding the deferred 29M initial allocation tranche.",
    },
    {
        "role": "asset_hub_issuer_staging",
        "label": "UTTT Asset Hub Issuer / Staging",
        "chain": "polkadot_asset_hub",
        "asset": "UTTT",
        "env": "UTT_UTTT_ASSET_HUB_ISSUER_STAGING_ADDRESS",
        "defaultAddress": None,
        "requiredTerms": ["asset", "hub", "issuer"],
        "avoidTerms": ["mixed", "trading", "pool", "lp"],
        "purpose": "Polkadot Asset Hub issuer/staging account used for mint/XCM evidence.",
    },
]


class BridgeTransferPreviewRequest(BaseModel):
    asset: str = Field("UTTT", description="Asset symbol being moved across chains.")
    amount: float = Field(..., gt=0, description="Human-unit asset quantity.")
    source_chain: str = Field(..., description="Source chain/network, e.g. solana or hydration.")
    destination_chain: str = Field(..., description="Destination chain/network, e.g. hydration or solana.")
    source_address: Optional[str] = Field(None, description="Source wallet/address, if known.")
    destination_address: Optional[str] = Field(None, description="Destination wallet/address, if known.")
    source_wallet_id: Optional[str] = Field(None, description="Optional local wallet/account grouping.")
    destination_wallet_id: Optional[str] = Field(None, description="Optional local wallet/account grouping.")
    bridge_mechanism: str = Field("manual", description="manual|treasury_mediated|burn_mint|lock_release|lock_mint|vault_deposit_mint_xcm|xcm_transfer|external_bridge")
    gross_amount: Optional[float] = Field(None, description="Optional gross amount before destination fees/dust, in human units.")
    destination_received_amount: Optional[float] = Field(None, description="Optional destination received amount, in human units.")
    xcm_delta_amount: Optional[float] = Field(None, description="Optional gross-minus-received delta, in human units.")
    source_vault_address: Optional[str] = Field(None, description="Optional source bridge reserve/vault address.")
    asset_hub_mint_txid: Optional[str] = Field(None, description="Optional Asset Hub mint extrinsic/hash.")
    asset_hub_xcm_txid: Optional[str] = Field(None, description="Optional Asset Hub XCM/send extrinsic/hash.")
    hydration_receive_txid: Optional[str] = Field(None, description="Optional Hydration receive/XCM message/extrinsic/hash.")
    source_proof_url: Optional[str] = Field(None, description="Optional source proof/explorer URL.")
    destination_proof_url: Optional[str] = Field(None, description="Optional destination proof/explorer URL.")
    note: Optional[str] = Field(None, description="Optional planning note. Preview does not persist this.")


class BridgeTransferCreateRequest(BridgeTransferPreviewRequest):
    create_from_preview: bool = Field(
        True,
        description="Safety flag: this endpoint only creates a local PLANNED transfer record from preview-shaped data.",
    )


class BridgeTransferLinkSourceRequest(BaseModel):
    source_withdrawal_id: Optional[str] = Field(None, description="Existing AssetWithdrawal id to link as the source-side outflow.")
    source_txid: Optional[str] = Field(None, description="Optional source-chain transaction id/signature/hash.")
    source_evidence_type: Optional[str] = Field(None, description="Optional source evidence type, e.g. solana_vault_deposit.")
    source_vault_address: Optional[str] = Field(None, description="Optional source bridge reserve/vault address.")
    source_amount: Optional[float] = Field(None, description="Optional source-side amount in human units.")
    source_proof_url: Optional[str] = Field(None, description="Optional source proof/explorer URL.")
    note: Optional[str] = Field(None, description="Optional linkage note. Appended to the transfer record raw audit trail.")


class BridgeTransferLinkDestinationRequest(BaseModel):
    destination_deposit_id: Optional[str] = Field(None, description="Existing AssetDeposit id to link as the destination-side inflow.")
    destination_txid: Optional[str] = Field(None, description="Optional destination-chain transaction id/signature/hash.")
    destination_evidence_type: Optional[str] = Field(None, description="Optional destination evidence type, e.g. asset_hub_mint_xcm_receive.")
    asset_hub_mint_txid: Optional[str] = Field(None, description="Optional Asset Hub mint extrinsic/hash.")
    asset_hub_mint_amount: Optional[float] = Field(None, description="Optional Asset Hub minted amount in human units.")
    asset_hub_xcm_txid: Optional[str] = Field(None, description="Optional Asset Hub XCM/send extrinsic/hash.")
    hydration_receive_txid: Optional[str] = Field(None, description="Optional Hydration receive/XCM message/extrinsic/hash.")
    hydration_received_amount: Optional[float] = Field(None, description="Optional Hydration received amount in human units.")
    xcm_delta_amount: Optional[float] = Field(None, description="Optional gross-minus-received delta in human units.")
    destination_proof_url: Optional[str] = Field(None, description="Optional destination proof/explorer URL.")
    note: Optional[str] = Field(None, description="Optional linkage note. Appended to the transfer record raw audit trail.")


class BridgeTransferAmendEvidenceRequest(BaseModel):
    source_txid: Optional[str] = Field(None, description="Optional corrected source-chain transaction id/signature/hash.")
    source_evidence_type: Optional[str] = Field(None, description="Optional corrected source evidence type.")
    source_vault_address: Optional[str] = Field(None, description="Optional corrected source bridge reserve/vault address.")
    source_amount: Optional[float] = Field(None, description="Optional corrected source-side amount in human units.")
    source_proof_url: Optional[str] = Field(None, description="Optional corrected source proof/explorer URL.")
    destination_txid: Optional[str] = Field(None, description="Optional corrected destination-chain transaction id/signature/hash.")
    destination_evidence_type: Optional[str] = Field(None, description="Optional corrected destination evidence type.")
    asset_hub_mint_txid: Optional[str] = Field(None, description="Optional corrected Asset Hub mint extrinsic/hash.")
    asset_hub_mint_amount: Optional[float] = Field(None, description="Optional corrected Asset Hub minted amount in human units.")
    asset_hub_xcm_txid: Optional[str] = Field(None, description="Optional corrected Asset Hub XCM/send extrinsic/hash.")
    hydration_receive_txid: Optional[str] = Field(None, description="Optional corrected Hydration receive/XCM message/extrinsic/hash.")
    hydration_received_amount: Optional[float] = Field(None, description="Optional corrected Hydration received amount in human units.")
    xcm_delta_amount: Optional[float] = Field(None, description="Optional corrected gross-minus-received delta in human units.")
    destination_proof_url: Optional[str] = Field(None, description="Optional corrected destination proof/explorer URL.")
    note: Optional[str] = Field(None, description="Optional amendment note. Appended to raw audit trail without changing reconciliation status.")


class BridgeTransferReconcileRequest(BaseModel):
    note: Optional[str] = Field(None, description="Optional reconciliation note. This does not mutate ledger/FIFO state.")


class BridgeTransferCancelRequest(BaseModel):
    note: Optional[str] = Field(None, description="Optional cancellation note. This does not mutate ledger/FIFO state.")
    allow_reconciled_manual_cancel: bool = Field(
        False,
        description="Safety flag for cancelling old reconciled manual/evidence-only records that were local test artifacts.",
    )


class BridgeTransferApplyBasisPreviewRequest(BaseModel):
    note: Optional[str] = Field(None, description="Optional dry-run note. This preview never mutates ledger/FIFO state.")


def _bridge_norm_asset(value: Any) -> str:
    return str(value or "UTTT").strip().upper() or "UTTT"


def _bridge_norm_chain(value: Any) -> str:
    raw = str(value or "").strip().lower()
    aliases = {
        "sol": "solana",
        "solana_jupiter": "solana",
        "polkadot_hydration": "hydration",
        "hyd": "hydration",
        "hydradx": "hydration",
        "assethub": "polkadot_asset_hub",
        "asset_hub": "polkadot_asset_hub",
        "polkadot": "polkadot_asset_hub",
        "polkadot_assethub": "polkadot_asset_hub",
    }
    return aliases.get(raw, raw)


def _bridge_chain_label(chain: str) -> str:
    c = _bridge_norm_chain(chain)
    if c == "solana":
        return "Solana"
    if c == "hydration":
        return "Hydration"
    if c == "polkadot_asset_hub":
        return "Polkadot / Asset Hub"
    return c or "Unknown"


def _bridge_wallet_network_aliases(chain: str) -> List[str]:
    c = _bridge_norm_chain(chain)
    if c == "solana":
        return ["solana"]
    if c == "hydration":
        return ["hydration", "polkadot_hydration", "polkadot"]
    if c == "polkadot_asset_hub":
        return ["polkadot_asset_hub", "asset_hub", "polkadot"]
    return [c] if c else []


def _bridge_registered_wallet(
    db: Session,
    *,
    chain: str,
    asset: str,
    address: Optional[str] = None,
) -> Optional[WalletAddress]:
    aliases = _bridge_wallet_network_aliases(chain)
    if not aliases:
        return None
    try:
        q = db.query(WalletAddress).filter(WalletAddress.network.in_(aliases))
        if address and str(address).strip():
            q = q.filter(WalletAddress.address == str(address).strip())
        asset_u = _bridge_norm_asset(asset)
        rows = q.order_by(WalletAddress.created_at.desc()).limit(50).all()
        if not rows:
            return None
        for row in rows:
            if str(row.asset or "").strip().upper() in {asset_u, "ALL", "*"}:
                return row
        return rows[0]
    except Exception:
        return None


def _bridge_wallet_payload(row: Optional[WalletAddress]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    return {
        "id": row.id,
        "asset": row.asset,
        "network": row.network,
        "wallet_id": row.wallet_id,
        "address": row.address,
        "label": row.label,
    }


def _bridge_wallet_haystack(row: Optional[WalletAddress]) -> str:
    if row is None:
        return ""
    return " ".join(
        str(x or "").strip().lower()
        for x in [
            row.label,
            row.wallet_id,
            row.owner_scope,
            row.network,
            row.asset,
            row.address,
        ]
        if str(x or "").strip()
    )


def _bridge_wallet_role_match_score(row: WalletAddress, definition: Dict[str, Any]) -> Optional[int]:
    asset_u = _bridge_norm_asset(definition.get("asset") or "UTTT")
    row_asset = str(row.asset or "").strip().upper()
    if row_asset and row_asset not in {asset_u, "ALL", "*"}:
        return None

    aliases = set(_bridge_wallet_network_aliases(str(definition.get("chain") or "")))
    row_network = str(row.network or "").strip().lower()
    if aliases and row_network and row_network not in aliases:
        return None

    configured_address = _bridge_clean_str(definition.get("address"))
    row_address = _bridge_clean_str(row.address)
    haystack = _bridge_wallet_haystack(row)

    score = 100
    if configured_address and row_address and row_address == configured_address:
        score -= 100
    elif configured_address and row_address and row_address != configured_address:
        score += 45

    if row_asset == asset_u:
        score -= 35
    elif row_asset in {"ALL", "*"}:
        score -= 8

    if aliases and row_network in aliases:
        score -= 20

    for term in definition.get("requiredTerms") or []:
        text = str(term or "").strip().lower()
        if text and text in haystack:
            score -= 12

    for term in definition.get("avoidTerms") or []:
        text = str(term or "").strip().lower()
        if text and text in haystack:
            score += 18

    return score


def _bridge_pick_treasury_wallet(db: Session, definition: Dict[str, Any]) -> tuple[Optional[WalletAddress], Optional[int]]:
    try:
        aliases = _bridge_wallet_network_aliases(str(definition.get("chain") or ""))
        q = db.query(WalletAddress)
        if aliases:
            q = q.filter(WalletAddress.network.in_(aliases))
        configured_address = _bridge_clean_str(definition.get("address"))
        if configured_address:
            direct = q.filter(WalletAddress.address == configured_address).order_by(WalletAddress.created_at.desc()).first()
            if direct is not None:
                return direct, _bridge_wallet_role_match_score(direct, definition)
        rows = q.order_by(WalletAddress.created_at.desc()).limit(250).all()
    except Exception:
        return None, None

    ranked: List[tuple[int, WalletAddress]] = []
    for row in rows:
        score = _bridge_wallet_role_match_score(row, definition)
        if score is not None:
            ranked.append((score, row))
    ranked.sort(key=lambda x: x[0])
    return ranked[0] if ranked else (None, None)


def _bridge_treasury_registry_payload(db: Session, *, asset: str = "UTTT") -> Dict[str, Any]:
    asset_u = _bridge_norm_asset(asset)
    roles: List[Dict[str, Any]] = []
    warnings: List[str] = []

    for base_definition in _UTTT_TREASURY_ROLE_DEFINITIONS:
        definition = dict(base_definition)
        configured_address = _env_str(str(definition.get("env") or ""), definition.get("defaultAddress"))
        definition["address"] = configured_address

        wallet, score = _bridge_pick_treasury_wallet(db, definition)
        wallet_payload = _bridge_wallet_payload(wallet)
        registered_address = _bridge_clean_str(wallet_payload.get("address") if wallet_payload else None)
        expected_address = _bridge_clean_str(configured_address)
        address_matches = bool(expected_address and registered_address and expected_address == registered_address)
        inferred_from_wallet = bool(wallet_payload and not expected_address)
        configured = bool(expected_address)
        registered = bool(wallet_payload)
        ready = bool(registered and (address_matches or inferred_from_wallet or not expected_address))

        if not registered:
            warnings.append(f"{definition.get('label')} is not registered in Wallet Addresses yet.")
        elif expected_address and not address_matches:
            warnings.append(f"{definition.get('label')} registered address does not match configured official address.")

        roles.append({
            "role": definition.get("role"),
            "label": definition.get("label"),
            "asset": asset_u,
            "chain": _bridge_norm_chain(definition.get("chain")),
            "chainLabel": _bridge_chain_label(str(definition.get("chain") or "")),
            "env": definition.get("env"),
            "configuredAddress": expected_address,
            "registeredAddress": registered_address,
            "address": expected_address or registered_address,
            "registered": registered,
            "configured": configured,
            "addressMatches": address_matches,
            "inferredFromWallet": inferred_from_wallet,
            "ready": ready,
            "wallet": wallet_payload,
            "matchScore": score,
            "purpose": definition.get("purpose"),
            "requiredTerms": definition.get("requiredTerms") or [],
            "avoidTerms": definition.get("avoidTerms") or [],
        })

    ready_count = sum(1 for r in roles if r.get("ready"))
    configured_count = sum(1 for r in roles if r.get("configured"))
    registered_count = sum(1 for r in roles if r.get("registered"))

    return {
        "ok": True,
        "asset": asset_u,
        "model": "official_uttt_treasury_registry_v1",
        "roles": roles,
        "count": len(roles),
        "readyCount": ready_count,
        "configuredCount": configured_count,
        "registeredCount": registered_count,
        "ready": ready_count >= 3,
        "warnings": warnings,
        "sync": {
            "autoDetectEnabled": False,
            "candidateBuilderEnabled": False,
            "reviewRequired": True,
            "message": "Treasury registry is read-only. Address sync/candidate creation remains review-only and is not yet enabled.",
            "next": [
                "Use these official treasury roles as the source of truth for bridge detection.",
                "Detect Solana reserve movements, Asset Hub mint/XCM events, and Hydration treasury receives.",
                "Build review-only candidate transfer records before enabling any automated reconciliation.",
            ],
        },
        "execution": {
            "bridgeExecutionEnabled": False,
            "ledgerFifoMutation": False,
        },
    }


def _bridge_count_source_candidates(
    db: Session,
    *,
    asset: str,
    source_chain: str,
    source_address: Optional[str],
    amount: float,
) -> Dict[str, Any]:
    chain = _bridge_norm_chain(source_chain)
    asset_u = _bridge_norm_asset(asset)
    try:
        q = db.query(AssetWithdrawal).filter(AssetWithdrawal.asset == asset_u)
        if chain:
            q = q.filter((AssetWithdrawal.chain == chain) | (AssetWithdrawal.network == chain))
        if source_address and str(source_address).strip():
            q = q.filter(AssetWithdrawal.destination == str(source_address).strip())
        rows = q.order_by(AssetWithdrawal.withdraw_time.desc()).limit(25).all()
        close = [
            r.id for r in rows
            if r.qty is not None and abs(float(r.qty) - float(amount)) <= max(0.000001, abs(float(amount)) * 0.000001)
        ]
        return {"table": "asset_withdrawals", "count": len(rows), "closeAmountIds": close[:10]}
    except Exception as e:
        return {"table": "asset_withdrawals", "count": None, "error": type(e).__name__}


def _bridge_count_destination_candidates(
    db: Session,
    *,
    asset: str,
    destination_chain: str,
    amount: float,
) -> Dict[str, Any]:
    chain = _bridge_norm_chain(destination_chain)
    asset_u = _bridge_norm_asset(asset)
    try:
        q = db.query(AssetDeposit).filter(AssetDeposit.asset == asset_u)
        if chain:
            q = q.filter((AssetDeposit.network == chain) | (AssetDeposit.venue == chain))
        rows = q.order_by(AssetDeposit.deposit_time.desc()).limit(25).all()
        close = [
            r.id for r in rows
            if r.qty is not None and abs(float(r.qty) - float(amount)) <= max(0.000001, abs(float(amount)) * 0.000001)
        ]
        return {"table": "asset_deposits", "count": len(rows), "closeAmountIds": close[:10]}
    except Exception as e:
        return {"table": "asset_deposits", "count": None, "error": type(e).__name__}


def _bridge_transfer_record_payload(row: BridgeTransferRecord) -> Dict[str, Any]:
    return {
        "id": row.id,
        "asset": row.asset,
        "amount": row.amount,
        "source_chain": row.source_chain,
        "destination_chain": row.destination_chain,
        "source_wallet_id": row.source_wallet_id,
        "destination_wallet_id": row.destination_wallet_id,
        "source_address": row.source_address,
        "destination_address": row.destination_address,
        "source_txid": row.source_txid,
        "destination_txid": row.destination_txid,
        "status": row.status,
        "bridge_mechanism": row.bridge_mechanism,
        "source_withdrawal_id": row.source_withdrawal_id,
        "destination_deposit_id": row.destination_deposit_id,
        "note": row.note,
        "raw": row.raw,
        "evidenceSummary": (row.raw or {}).get("bridgeEvidence") if isinstance(row.raw, dict) else None,
        "created_at": row.created_at.isoformat() if isinstance(row.created_at, datetime) else row.created_at,
        "updated_at": row.updated_at.isoformat() if isinstance(row.updated_at, datetime) else row.updated_at,
    }


def _bridge_get_transfer_record(db: Session, record_id: str) -> BridgeTransferRecord:
    rid = str(record_id or "").strip()
    if not rid:
        raise HTTPException(status_code=422, detail={"error": "bridge_transfer_record_id_required"})
    row = db.query(BridgeTransferRecord).filter(BridgeTransferRecord.id == rid).first()
    if row is None:
        raise HTTPException(status_code=404, detail={"error": "bridge_transfer_record_not_found", "id": rid})
    return row


def _bridge_raw_with_event(row: BridgeTransferRecord, event: Dict[str, Any]) -> Dict[str, Any]:
    raw = row.raw if isinstance(row.raw, dict) else {}
    events = raw.get("events")
    if not isinstance(events, list):
        events = []
    event_payload = {
        "ts": datetime.utcnow().isoformat(),
        **(event or {}),
    }
    return {
        **raw,
        "events": [*events, event_payload],
    }


def _bridge_clean_str(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _bridge_clean_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def _bridge_raw_merge_evidence(row: BridgeTransferRecord, *, section: str, evidence: Dict[str, Any]) -> Dict[str, Any]:
    raw = row.raw if isinstance(row.raw, dict) else {}
    bridge_evidence = raw.get("bridgeEvidence")
    if not isinstance(bridge_evidence, dict):
        bridge_evidence = {}
    current = bridge_evidence.get(section)
    if not isinstance(current, dict):
        current = {}
    cleaned = {k: v for k, v in (evidence or {}).items() if v is not None and v != ""}
    return {
        **raw,
        "bridgeEvidence": {
            **bridge_evidence,
            section: {
                **current,
                **cleaned,
                "updatedAtUtc": datetime.utcnow().isoformat(),
            },
        },
    }


def _bridge_amount_close(a: Any, b: Any) -> bool:
    try:
        aa = float(a)
        bb = float(b)
        return abs(aa - bb) <= max(0.000001, abs(bb) * 0.000001)
    except Exception:
        return False


def _bridge_validate_source_withdrawal(
    db: Session,
    *,
    row: BridgeTransferRecord,
    source_withdrawal_id: Optional[str],
) -> tuple[Optional[AssetWithdrawal], List[str]]:
    warnings: List[str] = []
    wid = str(source_withdrawal_id or "").strip()
    if not wid:
        return None, warnings

    withdrawal = db.query(AssetWithdrawal).filter(AssetWithdrawal.id == wid).first()
    if withdrawal is None:
        raise HTTPException(status_code=404, detail={"error": "source_withdrawal_not_found", "source_withdrawal_id": wid})

    if _bridge_norm_asset(withdrawal.asset) != _bridge_norm_asset(row.asset):
        raise HTTPException(
            status_code=422,
            detail={
                "error": "source_withdrawal_asset_mismatch",
                "record_asset": row.asset,
                "withdrawal_asset": withdrawal.asset,
            },
        )

    if not _bridge_amount_close(withdrawal.qty, row.amount):
        warnings.append(
            f"Source withdrawal amount {float(withdrawal.qty or 0):,.6f} differs from transfer amount {float(row.amount or 0):,.6f}."
        )

    chain = _bridge_norm_chain(row.source_chain)
    withdrawal_chain = _bridge_norm_chain(withdrawal.chain or withdrawal.network)
    if chain and withdrawal_chain and chain != withdrawal_chain:
        warnings.append(f"Source withdrawal chain/network '{withdrawal.chain or withdrawal.network}' differs from transfer source '{row.source_chain}'.")

    return withdrawal, warnings


def _bridge_validate_destination_deposit(
    db: Session,
    *,
    row: BridgeTransferRecord,
    destination_deposit_id: Optional[str],
) -> tuple[Optional[AssetDeposit], List[str]]:
    warnings: List[str] = []
    did = str(destination_deposit_id or "").strip()
    if not did:
        return None, warnings

    deposit = db.query(AssetDeposit).filter(AssetDeposit.id == did).first()
    if deposit is None:
        raise HTTPException(status_code=404, detail={"error": "destination_deposit_not_found", "destination_deposit_id": did})

    if _bridge_norm_asset(deposit.asset) != _bridge_norm_asset(row.asset):
        raise HTTPException(
            status_code=422,
            detail={
                "error": "destination_deposit_asset_mismatch",
                "record_asset": row.asset,
                "deposit_asset": deposit.asset,
            },
        )

    if not _bridge_amount_close(deposit.qty, row.amount):
        warnings.append(
            f"Destination deposit amount {float(deposit.qty or 0):,.6f} differs from transfer amount {float(row.amount or 0):,.6f}."
        )

    chain = _bridge_norm_chain(row.destination_chain)
    deposit_chain = _bridge_norm_chain(deposit.network or deposit.venue)
    if chain and deposit_chain and chain != deposit_chain:
        warnings.append(f"Destination deposit network/venue '{deposit.network or deposit.venue}' differs from transfer destination '{row.destination_chain}'.")

    return deposit, warnings


def _bridge_status_after_link(row: BridgeTransferRecord) -> str:
    has_source = bool(row.source_withdrawal_id or row.source_txid)
    has_destination = bool(row.destination_deposit_id or row.destination_txid)
    if has_source and has_destination:
        return "LINKED"
    if has_source:
        return "SOURCE_SENT"
    if has_destination:
        return "DESTINATION_RECEIVED"
    return "PLANNED"


def _bridge_withdrawal_payload(row: Optional[AssetWithdrawal]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    return {
        "id": row.id,
        "venue": row.venue,
        "wallet_id": row.wallet_id,
        "asset": row.asset,
        "qty": row.qty,
        "withdraw_time": row.withdraw_time.isoformat() if isinstance(row.withdraw_time, datetime) else row.withdraw_time,
        "txid": row.txid,
        "chain": row.chain,
        "network": row.network,
        "status": row.status,
        "source": row.source,
        "destination": row.destination,
    }


def _bridge_deposit_payload(row: Optional[AssetDeposit]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    return {
        "id": row.id,
        "venue": row.venue,
        "wallet_id": row.wallet_id,
        "asset": row.asset,
        "qty": row.qty,
        "deposit_time": row.deposit_time.isoformat() if isinstance(row.deposit_time, datetime) else row.deposit_time,
        "txid": row.txid,
        "network": row.network,
        "status": row.status,
        "source": row.source,
        "transfer_withdrawal_id": row.transfer_withdrawal_id,
    }


def _bridge_find_source_withdrawal(db: Session, row: BridgeTransferRecord) -> Optional[AssetWithdrawal]:
    if row.source_withdrawal_id:
        return db.query(AssetWithdrawal).filter(AssetWithdrawal.id == row.source_withdrawal_id).first()
    txid = str(row.source_txid or "").strip()
    if not txid:
        return None
    return (
        db.query(AssetWithdrawal)
        .filter(AssetWithdrawal.asset == _bridge_norm_asset(row.asset))
        .filter(AssetWithdrawal.txid == txid)
        .order_by(AssetWithdrawal.withdraw_time.desc())
        .first()
    )


def _bridge_find_destination_deposit(db: Session, row: BridgeTransferRecord) -> Optional[AssetDeposit]:
    if row.destination_deposit_id:
        return db.query(AssetDeposit).filter(AssetDeposit.id == row.destination_deposit_id).first()
    txid = str(row.destination_txid or "").strip()
    if not txid:
        return None
    return (
        db.query(AssetDeposit)
        .filter(AssetDeposit.asset == _bridge_norm_asset(row.asset))
        .filter(AssetDeposit.txid == txid)
        .order_by(AssetDeposit.deposit_time.desc())
        .first()
    )


def _bridge_lot_payload(lot: BasisLot, qty_used: float, basis_used_usd: Optional[float]) -> Dict[str, Any]:
    return {
        "id": lot.id,
        "venue": lot.venue,
        "wallet_id": lot.wallet_id,
        "asset": lot.asset,
        "acquired_at": lot.acquired_at.isoformat() if isinstance(lot.acquired_at, datetime) else lot.acquired_at,
        "qty_total": lot.qty_total,
        "qty_remaining": lot.qty_remaining,
        "qty_used": qty_used,
        "total_basis_usd": lot.total_basis_usd,
        "basis_used_usd": basis_used_usd,
        "basis_is_missing": bool(lot.basis_is_missing),
        "basis_source": lot.basis_source,
        "origin_type": lot.origin_type,
        "origin_ref": lot.origin_ref,
    }


def _bridge_fifo_lot_preview(
    db: Session,
    *,
    asset: str,
    amount: float,
    source_venue: Optional[str],
    source_wallet_id: Optional[str],
) -> Dict[str, Any]:
    asset_u = _bridge_norm_asset(asset)
    wallet = str(source_wallet_id or "").strip()
    venue = str(source_venue or "").strip()

    def _query_lots(strict_venue: bool, strict_wallet: bool) -> List[BasisLot]:
        q = (
            db.query(BasisLot)
            .filter(BasisLot.asset == asset_u)
            .filter(BasisLot.qty_remaining > 0)
        )
        if strict_venue and venue:
            q = q.filter(BasisLot.venue == venue)
        if strict_wallet and wallet:
            q = q.filter(BasisLot.wallet_id == wallet)
        return q.order_by(BasisLot.acquired_at.asc(), BasisLot.created_at.asc()).limit(100).all()

    filter_mode = "asset_only"
    lots = []
    if venue and wallet:
        lots = _query_lots(strict_venue=True, strict_wallet=True)
        filter_mode = "venue_wallet"
    if not lots and wallet:
        lots = _query_lots(strict_venue=False, strict_wallet=True)
        filter_mode = "wallet_only"
    if not lots and venue:
        lots = _query_lots(strict_venue=True, strict_wallet=False)
        filter_mode = "venue_only"
    if not lots:
        lots = _query_lots(strict_venue=False, strict_wallet=False)
        filter_mode = "asset_only"

    remaining = max(0.0, float(amount or 0.0))
    qty_available = 0.0
    selected: List[Dict[str, Any]] = []
    total_basis_used = 0.0
    basis_known_any = False
    basis_missing = False

    for lot in lots:
        try:
            lot_remaining = max(0.0, float(lot.qty_remaining or 0.0))
        except Exception:
            lot_remaining = 0.0
        qty_available += lot_remaining
        if remaining <= 0:
            continue

        qty_used = min(lot_remaining, remaining)
        if qty_used <= 0:
            continue

        basis_used: Optional[float] = None
        try:
            total_basis = lot.total_basis_usd
            qty_total = float(lot.qty_total or 0.0)
            if total_basis is not None and qty_total > 0:
                basis_used = float(total_basis) * (qty_used / qty_total)
                total_basis_used += basis_used
                basis_known_any = True
            else:
                basis_missing = True
        except Exception:
            basis_missing = True
            basis_used = None

        if bool(lot.basis_is_missing):
            basis_missing = True

        selected.append(_bridge_lot_payload(lot, qty_used, basis_used))
        remaining -= qty_used

    quantity_shortfall = max(0.0, remaining)
    enough_quantity = quantity_shortfall <= max(0.000001, abs(float(amount or 0.0)) * 0.000001)

    return {
        "filterMode": filter_mode,
        "sourceVenue": venue or None,
        "sourceWalletId": wallet or None,
        "asset": asset_u,
        "requiredQty": float(amount or 0.0),
        "availableQty": qty_available,
        "selectedQty": max(0.0, float(amount or 0.0) - quantity_shortfall),
        "quantityShortfall": quantity_shortfall,
        "enoughQuantity": bool(enough_quantity),
        "basisKnownAny": bool(basis_known_any),
        "basisMissing": bool(basis_missing or not basis_known_any),
        "estimatedCarriedBasisUsd": total_basis_used if basis_known_any else None,
        "selectedLots": selected,
        "candidateLotCount": len(lots),
    }


def _bridge_planned_source_consumptions(fifo: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for lot in fifo.get("selectedLots") or []:
        if not isinstance(lot, dict):
            continue
        qty_used = float(lot.get("qty_used") or 0.0)
        qty_before = float(lot.get("qty_remaining") or 0.0)
        out.append({
            "action": "TRANSFER_OUT_FIFO_CONSUME_PREVIEW",
            "lot_id": lot.get("id"),
            "asset": lot.get("asset"),
            "venue": lot.get("venue"),
            "wallet_id": lot.get("wallet_id"),
            "qty": qty_used,
            "qty_remaining_before": qty_before,
            "qty_remaining_after": max(0.0, qty_before - qty_used),
            "basis_used_usd": lot.get("basis_used_usd"),
            "basis_is_missing": bool(lot.get("basis_is_missing")),
            "origin_type": lot.get("origin_type"),
            "origin_ref": lot.get("origin_ref"),
        })
    return out


def _bridge_destination_lot_preview(
    *,
    row: BridgeTransferRecord,
    destination_deposit: Optional[AssetDeposit],
    fifo: Dict[str, Any],
) -> Dict[str, Any]:
    basis_missing = bool(fifo.get("basisMissing"))
    acquired_at = None
    if destination_deposit is not None:
        acquired_at = destination_deposit.deposit_time
    acquired_at_payload = acquired_at.isoformat() if isinstance(acquired_at, datetime) else acquired_at

    return {
        "action": "TRANSFER_IN_INHERITED_LOT_PREVIEW",
        "venue": destination_deposit.venue if destination_deposit is not None else row.destination_chain,
        "wallet_id": destination_deposit.wallet_id if destination_deposit is not None else (row.destination_wallet_id or "default"),
        "asset": row.asset,
        "acquired_at": acquired_at_payload,
        "qty_total": float(row.amount or 0.0),
        "qty_remaining": float(row.amount or 0.0),
        "total_basis_usd": fifo.get("estimatedCarriedBasisUsd"),
        "basis_is_missing": basis_missing,
        "basis_source": "BRIDGE_TRANSFER_INHERITED",
        "origin_type": "BRIDGE_TRANSFER",
        "origin_ref": row.id,
        "source_transfer_record_id": row.id,
        "source_withdrawal_id": row.source_withdrawal_id,
        "destination_deposit_id": row.destination_deposit_id,
    }


def _bridge_basis_journal_preview(
    *,
    row: BridgeTransferRecord,
    source_consumptions: List[Dict[str, Any]],
    destination_lot: Dict[str, Any],
    basis_missing: bool,
) -> List[Dict[str, Any]]:
    total_qty = sum(float(x.get("qty") or 0.0) for x in source_consumptions)
    total_basis = destination_lot.get("total_basis_usd")
    return [
        {
            "action": "TRANSFER_OUT_FIFO_CONSUME",
            "origin_type": "BRIDGE_TRANSFER",
            "origin_ref": row.id,
            "venue": row.source_chain,
            "wallet_id": row.source_wallet_id or "default",
            "asset": row.asset,
            "qty": total_qty,
            "price_usd": None,
            "fee_usd": None,
            "applied": False,
            "wouldWrite": False,
            "impact": {
                "type": "bridge_transfer_out_preview",
                "source_withdrawal_id": row.source_withdrawal_id,
                "source_txid": row.source_txid,
                "selected_lot_count": len(source_consumptions),
                "total_basis_moved_usd": total_basis,
                "basis_missing": basis_missing,
                "taxable_disposition": False,
            },
        },
        {
            "action": "TRANSFER_IN_INHERITED_LOT_CREATE",
            "origin_type": "BRIDGE_TRANSFER",
            "origin_ref": row.id,
            "venue": destination_lot.get("venue"),
            "wallet_id": destination_lot.get("wallet_id"),
            "asset": row.asset,
            "qty": float(row.amount or 0.0),
            "price_usd": None,
            "fee_usd": None,
            "applied": False,
            "wouldWrite": False,
            "impact": {
                "type": "bridge_transfer_in_preview",
                "destination_deposit_id": row.destination_deposit_id,
                "destination_txid": row.destination_txid,
                "inherited_basis_usd": total_basis,
                "basis_missing": basis_missing,
                "taxable_acquisition": False,
            },
        },
    ]


def _bridge_transfer_record_min_payload(row: BridgeTransferRecord) -> Dict[str, Any]:
    return {
        "id": row.id,
        "asset": row.asset,
        "amount": row.amount,
        "source_chain": row.source_chain,
        "destination_chain": row.destination_chain,
        "source_txid": row.source_txid,
        "destination_txid": row.destination_txid,
        "status": row.status,
        "bridge_mechanism": row.bridge_mechanism,
        "source_withdrawal_id": row.source_withdrawal_id,
        "destination_deposit_id": row.destination_deposit_id,
        "note": row.note,
        "created_at": row.created_at.isoformat() if isinstance(row.created_at, datetime) else row.created_at,
        "updated_at": row.updated_at.isoformat() if isinstance(row.updated_at, datetime) else row.updated_at,
    }


def _bridge_transfer_record_supply_summary(db: Session, *, asset: str, limit: int = 100) -> Dict[str, Any]:
    asset_u = _bridge_norm_asset(asset)
    try:
        rows = (
            db.query(BridgeTransferRecord)
            .filter(BridgeTransferRecord.asset == asset_u)
            .order_by(BridgeTransferRecord.created_at.desc())
            .limit(int(limit))
            .all()
        )
    except Exception as e:
        return {
            "ok": False,
            "error": "bridge_transfer_record_supply_summary_failed",
            "exc": type(e).__name__,
            "message": str(e),
            "items": [],
        }

    summary: Dict[str, Any] = {
        "ok": True,
        "asset": asset_u,
        "count": len(rows),
        "pendingAmount": 0.0,
        "linkedAmount": 0.0,
        "reconciledAmount": 0.0,
        "solanaToHydrationPendingAmount": 0.0,
        "solanaToHydrationLinkedAmount": 0.0,
        "solanaToHydrationReconciledAmount": 0.0,
        "vaultMintXcmReconciledGrossAmount": 0.0,
        "vaultMintXcmReconciledHydrationReceivedAmount": 0.0,
        "vaultMintXcmReconciledXcmDeltaAmount": 0.0,
        "solanaToHydrationVaultMintXcmReconciledGrossAmount": 0.0,
        "solanaToHydrationVaultMintXcmReconciledHydrationReceivedAmount": 0.0,
        "solanaToHydrationVaultMintXcmReconciledXcmDeltaAmount": 0.0,
        "items": [],
    }

    for row in rows:
        status = str(row.status or "").strip().upper()
        mechanism = str(row.bridge_mechanism or "").strip().lower()
        amount = float(row.amount or 0.0)
        source_chain = _bridge_norm_chain(row.source_chain)
        destination_chain = _bridge_norm_chain(row.destination_chain)
        is_sol_to_hyd = source_chain == "solana" and destination_chain == "hydration"
        item = _bridge_transfer_record_min_payload(row)
        item["sourceLabel"] = _bridge_chain_label(source_chain)
        item["destinationLabel"] = _bridge_chain_label(destination_chain)
        item["isSolanaToHydration"] = bool(is_sol_to_hyd)
        bridge_evidence = (row.raw or {}).get("bridgeEvidence") if isinstance(row.raw, dict) else None
        destination_evidence = bridge_evidence.get("destination") if isinstance(bridge_evidence, dict) and isinstance(bridge_evidence.get("destination"), dict) else {}
        hydration_received_amount = _bridge_clean_float(destination_evidence.get("hydrationReceivedAmount"))
        xcm_delta_amount = _bridge_clean_float(destination_evidence.get("xcmDeltaAmount"))
        item["evidence"] = {
            "sourceLinked": bool(row.source_withdrawal_id or row.source_txid),
            "destinationLinked": bool(row.destination_deposit_id or row.destination_txid),
            "lockMintWorkflow": mechanism == "lock_mint",
            "vaultMintXcmWorkflow": mechanism == "vault_deposit_mint_xcm",
            "bridgeEvidence": bridge_evidence,
            "hydrationReceivedAmount": hydration_received_amount,
            "xcmDeltaAmount": xcm_delta_amount,
        }
        summary["items"].append(item)

        if status == "RECONCILED":
            summary["reconciledAmount"] += amount
            if mechanism == "vault_deposit_mint_xcm":
                summary["vaultMintXcmReconciledGrossAmount"] += amount
                summary["vaultMintXcmReconciledHydrationReceivedAmount"] += hydration_received_amount if hydration_received_amount is not None else amount
                summary["vaultMintXcmReconciledXcmDeltaAmount"] += xcm_delta_amount if xcm_delta_amount is not None else 0.0
            if is_sol_to_hyd:
                summary["solanaToHydrationReconciledAmount"] += amount
                if mechanism == "vault_deposit_mint_xcm":
                    summary["solanaToHydrationVaultMintXcmReconciledGrossAmount"] += amount
                    summary["solanaToHydrationVaultMintXcmReconciledHydrationReceivedAmount"] += hydration_received_amount if hydration_received_amount is not None else amount
                    summary["solanaToHydrationVaultMintXcmReconciledXcmDeltaAmount"] += xcm_delta_amount if xcm_delta_amount is not None else 0.0
        elif status == "LINKED":
            summary["linkedAmount"] += amount
            if is_sol_to_hyd:
                summary["solanaToHydrationLinkedAmount"] += amount
        elif status not in {"CANCELLED"}:
            summary["pendingAmount"] += amount
            if is_sol_to_hyd:
                summary["solanaToHydrationPendingAmount"] += amount

    return summary


@router.get("/transfer_records/status")
def bridge_transfer_records_status() -> Dict[str, Any]:
    return {
        "ok": True,
        "support": "apply_basis_preview",
        "model": "BridgeTransferRecord",
        "table": "bridge_transfer_records",
        "execution": {
            "bridgeExecutionEnabled": False,
            "message": "Transfer records are planning/linkage records only. This endpoint does not execute bridge transactions.",
        },
        "endpoints": {
            "status": "GET /api/bridge/transfer_records/status",
            "treasury_registry": "GET /api/bridge/uttt_treasury_registry",
            "list": "GET /api/bridge/transfer_records",
            "preview": "POST /api/bridge/transfer_records/preview",
            "create": "POST /api/bridge/transfer_records",
            "link_source": "POST /api/bridge/transfer_records/{id}/link_source",
            "link_destination": "POST /api/bridge/transfer_records/{id}/link_destination",
            "amend_evidence": "POST /api/bridge/transfer_records/{id}/amend_evidence",
            "reconcile": "POST /api/bridge/transfer_records/{id}/reconcile",
            "cancel": "POST /api/bridge/transfer_records/{id}/cancel",
            "basis_preview": "GET /api/bridge/transfer_records/{id}/basis_preview",
            "apply_basis_transfer_preview": "POST /api/bridge/transfer_records/{id}/apply_basis_transfer_preview",
        },
        "allowedStatuses": sorted(_BRIDGE_TRANSFER_STATUSES),
        "allowedMechanisms": sorted(_BRIDGE_TRANSFER_MECHANISMS),
        "workflows": {
            "utttSolanaToHydration10mVaultMintXcm": {
                "asset": "UTTT",
                "amount": 10_000_000.0,
                "source_chain": "solana",
                "destination_chain": "hydration",
                "bridge_mechanism": "vault_deposit_mint_xcm",
                "executionEnabled": False,
                "evidencePlan": [
                    "Create a local PLANNED transfer record.",
                    "Link the Solana bridge-reserve deposit transaction as source evidence.",
                    "Link the Asset Hub mint and Asset Hub → Hydration receive/XCM evidence as destination evidence.",
                    "Reconcile after both sides are linked.",
                ],
            },
            "initial30mDeferred": {
                "asset": "UTTT",
                "amount": 30_000_000.0,
                "status": "deferred_until_solana_side_reserve_or_equivalent_evidence_exists",
                "message": "Do not record the initial 30M as reconciled until Solana-side reserve/lock/burn/equivalent evidence is available.",
            },
        },
        "nextRequired": "Use vault_deposit_mint_xcm for the 10M Solana-to-Hydration UTTT record. Apply-basis-transfer preview remains read-only and the actual apply endpoint remains disabled until a real bridge transfer is ready for testing.",
    }


@router.get("/uttt_treasury_registry")
def bridge_uttt_treasury_registry(
    asset: str = Query("UTTT", description="Asset symbol. Currently optimized for UTTT."),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Read-only official UTTT treasury registry for bridge/spread detection.

    This endpoint does not scan chains, create transfer records, execute bridge
    actions, or mutate ledger/FIFO state. It resolves the configured/registered
    treasury roles that later auto-detection should use as source-of-truth.
    """
    return _bridge_treasury_registry_payload(db, asset=asset)


@router.get("/transfer_records")
def bridge_transfer_records_list(
    asset: Optional[str] = Query(None, description="Optional asset symbol filter."),
    status: Optional[str] = Query(None, description="Optional status filter."),
    limit: int = Query(50, ge=1, le=250),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    try:
        q = db.query(BridgeTransferRecord)
        if asset and str(asset).strip():
            q = q.filter(BridgeTransferRecord.asset == _bridge_norm_asset(asset))
        if status and str(status).strip():
            q = q.filter(BridgeTransferRecord.status == str(status).strip().upper())
        rows = q.order_by(BridgeTransferRecord.created_at.desc()).limit(int(limit)).all()
        return {
            "ok": True,
            "items": [_bridge_transfer_record_payload(r) for r in rows],
            "count": len(rows),
            "execution": {"bridgeExecutionEnabled": False},
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"error": "bridge_transfer_records_list_failed", "exc": type(e).__name__, "message": str(e)},
        )


@router.post("/transfer_records")
def bridge_transfer_record_create(
    req: BridgeTransferCreateRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Create a local PLANNED bridge transfer record from accepted preview data.

    This endpoint only persists the planning/linkage record. It does not execute,
    sign, submit, reconcile, or mutate ledger/FIFO/deposit/withdrawal state.
    """
    preview = bridge_transfer_record_preview(req, db)
    planned = dict(preview.get("plannedRecord") or {})

    if not planned:
        raise HTTPException(status_code=422, detail={"error": "bridge_transfer_preview_missing_planned_record"})
    if not bool(req.create_from_preview):
        raise HTTPException(
            status_code=422,
            detail={
                "error": "create_from_preview_required",
                "message": "Set create_from_preview=true to confirm this is a local PLANNED transfer-record creation only.",
            },
        )

    now = datetime.utcnow()
    row = BridgeTransferRecord(
        asset=_bridge_norm_asset(planned.get("asset")),
        amount=float(planned.get("amount") or 0.0),
        source_chain=_bridge_norm_chain(planned.get("source_chain")),
        destination_chain=_bridge_norm_chain(planned.get("destination_chain")),
        source_wallet_id=planned.get("source_wallet_id"),
        destination_wallet_id=planned.get("destination_wallet_id"),
        source_address=planned.get("source_address"),
        destination_address=planned.get("destination_address"),
        source_txid=None,
        destination_txid=None,
        status="PLANNED",
        bridge_mechanism=str(planned.get("bridge_mechanism") or req.bridge_mechanism or "manual").strip().lower(),
        source_withdrawal_id=None,
        destination_deposit_id=None,
        note=planned.get("note"),
        raw={
            "createdFrom": "bridge_transfer_record_preview",
            "preview": preview,
            "bridgeEvidence": {
                "planned": {
                    "workflow": planned.get("workflow"),
                    "evidencePlan": planned.get("evidence_plan"),
                    "sourceVaultAddress": planned.get("source_vault_address"),
                    "assetHubMintTxid": planned.get("asset_hub_mint_txid"),
                    "assetHubXcmTxid": planned.get("asset_hub_xcm_txid"),
                    "hydrationReceiveTxid": planned.get("hydration_receive_txid"),
                    "grossAmount": planned.get("gross_amount"),
                    "destinationReceivedAmount": planned.get("destination_received_amount"),
                    "xcmDeltaAmount": planned.get("xcm_delta_amount"),
                    "sourceProofUrl": planned.get("source_proof_url"),
                    "destinationProofUrl": planned.get("destination_proof_url"),
                },
            },
            "safety": {
                "bridgeExecutionEnabled": False,
                "ledgerFifoMutation": False,
                "createdAtUtc": now.isoformat(),
            },
        },
        created_at=now,
        updated_at=now,
    )

    db.add(row)
    try:
        db.commit()
        db.refresh(row)
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail={"error": "bridge_transfer_record_create_failed", "exc": type(e).__name__, "message": str(e)},
        )

    return {
        "ok": True,
        "mode": "create",
        "created": True,
        "willMutate": True,
        "mutationScope": "bridge_transfer_records_only",
        "item": _bridge_transfer_record_payload(row),
        "preview": preview,
        "warnings": preview.get("warnings") or [],
        "execution": {
            "bridgeExecutionEnabled": False,
            "message": "Created a local PLANNED transfer record only. No bridge transaction was executed and ledger/FIFO state was not mutated.",
        },
        "nextRequired": "Link the source outflow and destination inflow when they are available, then reconcile the transfer record.",
    }


@router.post("/transfer_records/{record_id}/link_source")
def bridge_transfer_record_link_source(
    record_id: str,
    req: BridgeTransferLinkSourceRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Link an existing source-side withdrawal/outflow to a bridge transfer record.

    This updates only the bridge_transfer_records row. It does not mutate ledger,
    FIFO, deposit, or withdrawal accounting state.
    """
    row = _bridge_get_transfer_record(db, record_id)
    withdrawal, warnings = _bridge_validate_source_withdrawal(
        db,
        row=row,
        source_withdrawal_id=req.source_withdrawal_id,
    )

    source_txid = str(req.source_txid or "").strip() or None
    if withdrawal is not None and not source_txid:
        source_txid = str(withdrawal.txid or "").strip() or None

    if withdrawal is None and not source_txid:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "source_link_required",
                "message": "Provide source_withdrawal_id and/or source_txid.",
            },
        )

    now = datetime.utcnow()
    if withdrawal is not None:
        row.source_withdrawal_id = withdrawal.id
    if source_txid:
        row.source_txid = source_txid
    row.status = _bridge_status_after_link(row)
    row.updated_at = now
    source_evidence = {
        "evidenceType": _bridge_clean_str(req.source_evidence_type) or ("solana_vault_deposit" if _bridge_norm_chain(row.source_chain) == "solana" else "source_outflow"),
        "sourceTxid": row.source_txid,
        "sourceWithdrawalId": row.source_withdrawal_id,
        "sourceVaultAddress": _bridge_clean_str(req.source_vault_address),
        "sourceAmount": _bridge_clean_float(req.source_amount),
        "sourceProofUrl": _bridge_clean_str(req.source_proof_url),
    }
    row.raw = _bridge_raw_merge_evidence(row, section="source", evidence=source_evidence)
    row.raw = _bridge_raw_with_event(row, {
        "type": "link_source",
        "source_withdrawal_id": row.source_withdrawal_id,
        "source_txid": row.source_txid,
        "sourceEvidence": source_evidence,
        "status": row.status,
        "note": req.note,
        "mutationScope": "bridge_transfer_records_only",
    })

    db.add(row)
    try:
        db.commit()
        db.refresh(row)
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail={"error": "bridge_transfer_record_link_source_failed", "exc": type(e).__name__, "message": str(e)},
        )

    return {
        "ok": True,
        "mode": "link_source",
        "linked": True,
        "willMutate": True,
        "mutationScope": "bridge_transfer_records_only",
        "item": _bridge_transfer_record_payload(row),
        "linkedSource": {
            "source_withdrawal_id": row.source_withdrawal_id,
            "source_txid": row.source_txid,
        },
        "warnings": warnings,
        "execution": {
            "bridgeExecutionEnabled": False,
            "message": "Linked source-side evidence to the local transfer record only. Ledger/FIFO state was not mutated.",
        },
        "nextRequired": "Link the destination inflow when available, then reconcile the transfer record.",
    }


@router.post("/transfer_records/{record_id}/link_destination")
def bridge_transfer_record_link_destination(
    record_id: str,
    req: BridgeTransferLinkDestinationRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Link an existing destination-side deposit/inflow to a bridge transfer record.

    This updates only the bridge_transfer_records row. It does not mutate ledger,
    FIFO, deposit, or withdrawal accounting state.
    """
    row = _bridge_get_transfer_record(db, record_id)
    deposit, warnings = _bridge_validate_destination_deposit(
        db,
        row=row,
        destination_deposit_id=req.destination_deposit_id,
    )

    destination_txid = (
        _bridge_clean_str(req.destination_txid)
        or _bridge_clean_str(req.hydration_receive_txid)
        or _bridge_clean_str(req.asset_hub_xcm_txid)
        or _bridge_clean_str(req.asset_hub_mint_txid)
    )
    if deposit is not None and not destination_txid:
        destination_txid = str(deposit.txid or "").strip() or None

    if deposit is None and not destination_txid:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "destination_link_required",
                "message": "Provide destination_deposit_id, destination_txid, Asset Hub XCM txid, Hydration receive txid, or Asset Hub mint txid.",
            },
        )

    now = datetime.utcnow()
    if deposit is not None:
        row.destination_deposit_id = deposit.id
    if destination_txid:
        row.destination_txid = destination_txid
    row.status = _bridge_status_after_link(row)
    row.updated_at = now
    destination_evidence = {
        "evidenceType": _bridge_clean_str(req.destination_evidence_type) or ("asset_hub_mint_xcm_receive" if str(row.bridge_mechanism or "").strip().lower() == "vault_deposit_mint_xcm" else "destination_inflow"),
        "destinationTxid": row.destination_txid,
        "destinationDepositId": row.destination_deposit_id,
        "assetHubMintTxid": _bridge_clean_str(req.asset_hub_mint_txid),
        "assetHubMintAmount": _bridge_clean_float(req.asset_hub_mint_amount),
        "assetHubXcmTxid": _bridge_clean_str(req.asset_hub_xcm_txid),
        "hydrationReceiveTxid": _bridge_clean_str(req.hydration_receive_txid),
        "hydrationReceivedAmount": _bridge_clean_float(req.hydration_received_amount),
        "xcmDeltaAmount": _bridge_clean_float(req.xcm_delta_amount),
        "destinationProofUrl": _bridge_clean_str(req.destination_proof_url),
    }
    row.raw = _bridge_raw_merge_evidence(row, section="destination", evidence=destination_evidence)
    row.raw = _bridge_raw_with_event(row, {
        "type": "link_destination",
        "destination_deposit_id": row.destination_deposit_id,
        "destination_txid": row.destination_txid,
        "destinationEvidence": destination_evidence,
        "status": row.status,
        "note": req.note,
        "mutationScope": "bridge_transfer_records_only",
    })

    db.add(row)
    try:
        db.commit()
        db.refresh(row)
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail={"error": "bridge_transfer_record_link_destination_failed", "exc": type(e).__name__, "message": str(e)},
        )

    return {
        "ok": True,
        "mode": "link_destination",
        "linked": True,
        "willMutate": True,
        "mutationScope": "bridge_transfer_records_only",
        "item": _bridge_transfer_record_payload(row),
        "linkedDestination": {
            "destination_deposit_id": row.destination_deposit_id,
            "destination_txid": row.destination_txid,
        },
        "warnings": warnings,
        "execution": {
            "bridgeExecutionEnabled": False,
            "message": "Linked destination-side evidence to the local transfer record only. Ledger/FIFO state was not mutated.",
        },
        "nextRequired": "Reconcile the transfer record after both sides are linked.",
    }


@router.post("/transfer_records/{record_id}/amend_evidence")
def bridge_transfer_record_amend_evidence(
    record_id: str,
    req: BridgeTransferAmendEvidenceRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Amend source/destination evidence on a local bridge transfer record.

    This is an audit correction endpoint. It updates only bridgeEvidence/raw
    fields and optional top-level txid mirrors. It intentionally preserves the
    existing transfer-record status, including RECONCILED, and does not mutate
    ledger, FIFO, deposit, withdrawal, or bridge execution state.
    """
    row = _bridge_get_transfer_record(db, record_id)
    status = str(row.status or "").strip().upper()
    if status == "CANCELLED":
        raise HTTPException(
            status_code=422,
            detail={
                "error": "bridge_transfer_record_cancelled_amend_blocked",
                "message": "Cancelled bridge records cannot be amended. Create a new correcting record instead.",
                "item": _bridge_transfer_record_payload(row),
            },
        )

    source_evidence = {
        "evidenceType": _bridge_clean_str(req.source_evidence_type),
        "sourceTxid": _bridge_clean_str(req.source_txid),
        "sourceVaultAddress": _bridge_clean_str(req.source_vault_address),
        "sourceAmount": _bridge_clean_float(req.source_amount),
        "sourceProofUrl": _bridge_clean_str(req.source_proof_url),
    }
    destination_evidence = {
        "evidenceType": _bridge_clean_str(req.destination_evidence_type),
        "destinationTxid": _bridge_clean_str(req.destination_txid),
        "assetHubMintTxid": _bridge_clean_str(req.asset_hub_mint_txid),
        "assetHubMintAmount": _bridge_clean_float(req.asset_hub_mint_amount),
        "assetHubXcmTxid": _bridge_clean_str(req.asset_hub_xcm_txid),
        "hydrationReceiveTxid": _bridge_clean_str(req.hydration_receive_txid),
        "hydrationReceivedAmount": _bridge_clean_float(req.hydration_received_amount),
        "xcmDeltaAmount": _bridge_clean_float(req.xcm_delta_amount),
        "destinationProofUrl": _bridge_clean_str(req.destination_proof_url),
    }

    source_clean = {k: v for k, v in source_evidence.items() if v is not None and v != ""}
    destination_clean = {k: v for k, v in destination_evidence.items() if v is not None and v != ""}
    if not source_clean and not destination_clean:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "bridge_transfer_amendment_empty",
                "message": "Provide at least one source or destination evidence field to amend.",
            },
        )

    original_status = row.status
    now = datetime.utcnow()
    if source_clean:
        row.raw = _bridge_raw_merge_evidence(row, section="source", evidence=source_clean)
        if source_clean.get("sourceTxid"):
            row.source_txid = source_clean.get("sourceTxid")
    if destination_clean:
        row.raw = _bridge_raw_merge_evidence(row, section="destination", evidence=destination_clean)
        if destination_clean.get("destinationTxid"):
            row.destination_txid = destination_clean.get("destinationTxid")
    row.status = original_status
    row.updated_at = now
    row.raw = _bridge_raw_with_event(row, {
        "type": "amend_evidence",
        "status": row.status,
        "sourceEvidenceAmended": source_clean or None,
        "destinationEvidenceAmended": destination_clean or None,
        "note": req.note,
        "mutationScope": "bridge_transfer_records_only",
        "ledgerFifoMutation": False,
    })

    db.add(row)
    try:
        db.commit()
        db.refresh(row)
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail={"error": "bridge_transfer_record_amend_evidence_failed", "exc": type(e).__name__, "message": str(e)},
        )

    return {
        "ok": True,
        "mode": "amend_evidence",
        "amended": True,
        "willMutate": True,
        "mutationScope": "bridge_transfer_records_only",
        "item": _bridge_transfer_record_payload(row),
        "amendedSections": {
            "source": bool(source_clean),
            "destination": bool(destination_clean),
        },
        "execution": {
            "bridgeExecutionEnabled": False,
            "message": "Amended local bridge evidence only. Transfer-record status was preserved and ledger/FIFO state was not mutated.",
        },
    }


@router.post("/transfer_records/{record_id}/reconcile")
def bridge_transfer_record_reconcile(
    record_id: str,
    req: BridgeTransferReconcileRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Mark a bridge transfer record as reconciled.

    This updates only the bridge_transfer_records row. Non-taxable transfer/FIFO
    basis preservation remains a later explicit step.
    """
    row = _bridge_get_transfer_record(db, record_id)

    has_source = bool(row.source_withdrawal_id or row.source_txid)
    has_destination = bool(row.destination_deposit_id or row.destination_txid)
    warnings: List[str] = []
    if not has_source:
        warnings.append("Source-side withdrawal/outflow evidence is not linked yet.")
    if not has_destination:
        warnings.append("Destination-side deposit/inflow evidence is not linked yet.")
    if not has_source or not has_destination:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "bridge_transfer_record_not_fully_linked",
                "message": "Link both source and destination evidence before reconciliation.",
                "warnings": warnings,
                "item": _bridge_transfer_record_payload(row),
            },
        )

    now = datetime.utcnow()
    row.status = "RECONCILED"
    row.updated_at = now
    row.raw = _bridge_raw_with_event(row, {
        "type": "reconcile",
        "status": row.status,
        "note": req.note,
        "mutationScope": "bridge_transfer_records_only",
        "ledgerFifoMutation": False,
    })

    db.add(row)
    try:
        db.commit()
        db.refresh(row)
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail={"error": "bridge_transfer_record_reconcile_failed", "exc": type(e).__name__, "message": str(e)},
        )

    return {
        "ok": True,
        "mode": "reconcile",
        "reconciled": True,
        "willMutate": True,
        "mutationScope": "bridge_transfer_records_only",
        "item": _bridge_transfer_record_payload(row),
        "warnings": warnings,
        "execution": {
            "bridgeExecutionEnabled": False,
            "message": "Marked the local transfer record RECONCILED only. Ledger/FIFO basis preservation is still intentionally deferred.",
        },
        "nextRequired": "Connect reconciled transfer records to non-taxable transfer handling and FIFO basis preservation.",
    }


@router.post("/transfer_records/{record_id}/cancel")
def bridge_transfer_record_cancel(
    record_id: str,
    req: BridgeTransferCancelRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Cancel a local bridge transfer record without deleting it.

    This updates only the bridge_transfer_records row. It does not mutate ledger,
    FIFO, deposit, withdrawal, or bridge execution state. Reconciled records remain
    protected except for explicitly confirmed manual/evidence-only records, which is
    intended for stale local test artifacts.
    """
    row = _bridge_get_transfer_record(db, record_id)
    status = str(row.status or "").strip().upper()
    mechanism = str(row.bridge_mechanism or "").strip().lower()
    if status == "RECONCILED":
        can_cancel_reconciled_manual = bool(req.allow_reconciled_manual_cancel) and mechanism == "manual" and not row.source_withdrawal_id and not row.destination_deposit_id
        if not can_cancel_reconciled_manual:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "bridge_transfer_record_reconciled_cancel_blocked",
                    "message": "Reconciled bridge records cannot be cancelled unless this is an explicitly confirmed manual/evidence-only local test artifact. Create an explicit correcting record instead.",
                    "item": _bridge_transfer_record_payload(row),
                },
            )
    if status == "CANCELLED":
        return {
            "ok": True,
            "mode": "cancel",
            "cancelled": True,
            "willMutate": False,
            "mutationScope": "none_already_cancelled",
            "item": _bridge_transfer_record_payload(row),
            "execution": {
                "bridgeExecutionEnabled": False,
                "message": "Transfer record was already CANCELLED. No ledger/FIFO state was mutated.",
            },
        }

    now = datetime.utcnow()
    row.status = "CANCELLED"
    row.updated_at = now
    row.raw = _bridge_raw_with_event(row, {
        "type": "cancel_reconciled_manual_record" if status == "RECONCILED" else "cancel",
        "previousStatus": status,
        "status": row.status,
        "allowReconciledManualCancel": bool(req.allow_reconciled_manual_cancel),
        "note": req.note,
        "mutationScope": "bridge_transfer_records_only",
        "ledgerFifoMutation": False,
    })

    db.add(row)
    try:
        db.commit()
        db.refresh(row)
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail={"error": "bridge_transfer_record_cancel_failed", "exc": type(e).__name__, "message": str(e)},
        )

    return {
        "ok": True,
        "mode": "cancel",
        "cancelled": True,
        "willMutate": True,
        "mutationScope": "bridge_transfer_records_only",
        "item": _bridge_transfer_record_payload(row),
        "execution": {
            "bridgeExecutionEnabled": False,
            "message": "Marked the local transfer record CANCELLED only. Ledger/FIFO, deposit, withdrawal, and bridge execution state were not mutated.",
        },
    }


@router.get("/transfer_records/{record_id}/basis_preview")
def bridge_transfer_record_basis_preview(
    record_id: str,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Read-only tax/basis preview for a bridge transfer record.

    This endpoint classifies a reconciled bridge-transfer record as a
    TRANSFER_OUT / TRANSFER_IN candidate and previews FIFO lot availability.
    It does not create lots, consume lots, write lot_journal rows, mutate deposits,
    mutate withdrawals, or change ledger/FIFO state.
    """
    row = _bridge_get_transfer_record(db, record_id)
    source_withdrawal = _bridge_find_source_withdrawal(db, row)
    destination_deposit = _bridge_find_destination_deposit(db, row)

    has_source_evidence = bool(row.source_withdrawal_id or row.source_txid)
    has_destination_evidence = bool(row.destination_deposit_id or row.destination_txid)
    is_reconciled = str(row.status or "").strip().upper() == "RECONCILED"

    source_venue = None
    source_wallet_id = None
    if source_withdrawal is not None:
        source_venue = source_withdrawal.venue
        source_wallet_id = source_withdrawal.wallet_id
    else:
        source_venue = row.source_chain
        source_wallet_id = row.source_wallet_id or "default"

    fifo = _bridge_fifo_lot_preview(
        db,
        asset=row.asset,
        amount=float(row.amount or 0.0),
        source_venue=source_venue,
        source_wallet_id=source_wallet_id,
    )

    ready_for_apply = bool(
        is_reconciled
        and has_source_evidence
        and has_destination_evidence
        and source_withdrawal is not None
        and destination_deposit is not None
        and fifo.get("enoughQuantity")
    )

    readiness = [
        {
            "key": "record_reconciled",
            "label": "Transfer record reconciled",
            "status": "ready" if is_reconciled else "missing",
            "message": "Record is RECONCILED." if is_reconciled else f"Record status is {row.status or 'unknown'}; reconcile both sides first.",
        },
        {
            "key": "source_evidence",
            "label": "Source evidence",
            "status": "ready" if has_source_evidence else "missing",
            "message": "Source txid/withdrawal evidence is linked." if has_source_evidence else "Link source txid or source withdrawal first.",
        },
        {
            "key": "destination_evidence",
            "label": "Destination evidence",
            "status": "ready" if has_destination_evidence else "missing",
            "message": "Destination txid/deposit evidence is linked." if has_destination_evidence else "Link destination txid or destination deposit first.",
        },
        {
            "key": "source_withdrawal_row",
            "label": "Source withdrawal row",
            "status": "ready" if source_withdrawal is not None else "preview",
            "message": "Linked/cached AssetWithdrawal row found." if source_withdrawal is not None else "Only txid evidence is linked; no AssetWithdrawal row is linked/cached yet.",
        },
        {
            "key": "destination_deposit_row",
            "label": "Destination deposit row",
            "status": "ready" if destination_deposit is not None else "preview",
            "message": "Linked/cached AssetDeposit row found." if destination_deposit is not None else "Only txid evidence is linked; no AssetDeposit row is linked/cached yet.",
        },
        {
            "key": "source_lot_quantity",
            "label": "Source FIFO quantity",
            "status": "ready" if fifo.get("enoughQuantity") else "missing",
            "message": (
                f"{float(fifo.get('availableQty') or 0):,.6f} {row.asset} available for {float(row.amount or 0):,.6f} {row.asset} transfer preview."
                if fifo.get("enoughQuantity")
                else f"Insufficient source lots for preview; short {float(fifo.get('quantityShortfall') or 0):,.6f} {row.asset}."
            ),
        },
        {
            "key": "basis_known",
            "label": "Basis availability",
            "status": "ready" if not fifo.get("basisMissing") else "warning",
            "message": "Selected source lots have usable basis." if not fifo.get("basisMissing") else "Some or all selected source lots have missing basis.",
        },
        {
            "key": "apply_basis_transfer_preview",
            "label": "Apply basis preview",
            "status": "preview",
            "message": "Apply-basis-transfer preview is wired read-only. The actual apply endpoint remains intentionally disabled.",
        },
    ]

    warnings: List[str] = []
    if not is_reconciled:
        warnings.append("Transfer record is not RECONCILED yet; basis transfer can only be considered after both sides are reconciled.")
    if source_withdrawal is None:
        warnings.append("No linked/cached AssetWithdrawal row found. This preview can classify txid evidence but cannot safely apply FIFO basis movement yet.")
    if destination_deposit is None:
        warnings.append("No linked/cached AssetDeposit row found. This preview can classify txid evidence but cannot safely create destination basis inheritance yet.")
    if not fifo.get("enoughQuantity"):
        warnings.append("Source FIFO lots do not currently cover the planned transfer amount.")
    if fifo.get("basisMissing"):
        warnings.append("Basis is missing on one or more selected source lots; inherited destination basis would require review.")

    return {
        "ok": True,
        "mode": "basis_preview",
        "willMutate": False,
        "mutationScope": "none_read_only",
        "item": _bridge_transfer_record_payload(row),
        "source": {
            "treatment": "TRANSFER_OUT_CANDIDATE",
            "taxableDisposition": False,
            "withdrawal": _bridge_withdrawal_payload(source_withdrawal),
            "evidence": {
                "source_withdrawal_id": row.source_withdrawal_id,
                "source_txid": row.source_txid,
            },
        },
        "destination": {
            "treatment": "TRANSFER_IN_CANDIDATE",
            "taxableAcquisition": False,
            "deposit": _bridge_deposit_payload(destination_deposit),
            "evidence": {
                "destination_deposit_id": row.destination_deposit_id,
                "destination_txid": row.destination_txid,
            },
        },
        "fifoPreview": fifo,
        "basisTreatment": {
            "status": "ready_for_apply_preview" if ready_for_apply else "preview_only",
            "readyForApply": ready_for_apply,
            "sourceTreatment": "TRANSFER_OUT_CANDIDATE",
            "destinationTreatment": "TRANSFER_IN_CANDIDATE",
            "estimatedCarriedBasisUsd": fifo.get("estimatedCarriedBasisUsd"),
            "basisMissing": bool(fifo.get("basisMissing")),
            "ledgerFifoMutation": False,
            "applyPreviewEndpointWired": True,
            "applyEndpointWired": False,
        },
        "readiness": readiness,
        "warnings": warnings,
        "execution": {
            "bridgeExecutionEnabled": False,
            "message": "Basis preview is read-only. No ledger/FIFO, lot_journal, deposit, withdrawal, or bridge execution state was mutated.",
        },
        "nextRequired": "When a real bridge transfer is ready and deposit/withdrawal rows are linked, add a guarded apply_basis_transfer endpoint with explicit confirmation.",
    }


@router.post("/transfer_records/{record_id}/apply_basis_transfer_preview")
def bridge_transfer_record_apply_basis_transfer_preview(
    record_id: str,
    req: BridgeTransferApplyBasisPreviewRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Read-only preview of the eventual bridge basis-transfer mutation.

    This models the FIFO source-lot consumption, destination inherited-basis lot,
    and lot_journal rows that a future guarded apply endpoint would create.
    It does not mutate ledger/FIFO, deposits, withdrawals, lot_journal, or bridge
    execution state.
    """
    basis = bridge_transfer_record_basis_preview(record_id, db)
    row = _bridge_get_transfer_record(db, record_id)
    source_withdrawal = _bridge_find_source_withdrawal(db, row)
    destination_deposit = _bridge_find_destination_deposit(db, row)
    fifo = basis.get("fifoPreview") or {}

    source_consumptions = _bridge_planned_source_consumptions(fifo)
    destination_lot = _bridge_destination_lot_preview(
        row=row,
        destination_deposit=destination_deposit,
        fifo=fifo,
    )
    basis_missing = bool(fifo.get("basisMissing"))
    journal_preview = _bridge_basis_journal_preview(
        row=row,
        source_consumptions=source_consumptions,
        destination_lot=destination_lot,
        basis_missing=basis_missing,
    )

    blocked_reasons: List[str] = []
    if str(row.status or "").strip().upper() != "RECONCILED":
        blocked_reasons.append("Transfer record must be RECONCILED before basis transfer can be applied.")
    if source_withdrawal is None:
        blocked_reasons.append("A linked/cached AssetWithdrawal row is required before applying basis transfer.")
    if destination_deposit is None:
        blocked_reasons.append("A linked/cached AssetDeposit row is required before applying destination inherited basis.")
    if not fifo.get("enoughQuantity"):
        blocked_reasons.append("Source FIFO lots do not cover the planned transfer amount.")
    if basis_missing:
        blocked_reasons.append("Selected source lots have missing basis; apply should remain blocked until basis is reviewed.")

    ready_if_confirmed = len(blocked_reasons) == 0

    readiness = [
        {
            "key": "record_reconciled",
            "label": "Transfer record reconciled",
            "status": "ready" if str(row.status or "").strip().upper() == "RECONCILED" else "missing",
            "message": f"Record status is {row.status or 'unknown'}.",
        },
        {
            "key": "source_withdrawal_row",
            "label": "Source withdrawal row",
            "status": "ready" if source_withdrawal is not None else "missing",
            "message": "AssetWithdrawal row is linked/cached." if source_withdrawal is not None else "Link a real source withdrawal row before applying.",
        },
        {
            "key": "destination_deposit_row",
            "label": "Destination deposit row",
            "status": "ready" if destination_deposit is not None else "missing",
            "message": "AssetDeposit row is linked/cached." if destination_deposit is not None else "Link a real destination deposit row before applying.",
        },
        {
            "key": "source_lot_quantity",
            "label": "Source FIFO quantity",
            "status": "ready" if fifo.get("enoughQuantity") else "missing",
            "message": f"{float(fifo.get('selectedQty') or 0.0):,.6f} of {float(row.amount or 0.0):,.6f} {row.asset} selected.",
        },
        {
            "key": "basis_known",
            "label": "Basis known",
            "status": "ready" if not basis_missing else "warning",
            "message": "Selected source lots have usable basis." if not basis_missing else "Missing basis blocks automatic apply until reviewed.",
        },
        {
            "key": "apply_endpoint",
            "label": "Actual apply endpoint",
            "status": "disabled",
            "message": "Actual mutation endpoint is intentionally not wired yet.",
        },
    ]

    warnings = list(basis.get("warnings") or [])
    warnings.extend(blocked_reasons)

    return {
        "ok": True,
        "mode": "apply_basis_transfer_preview",
        "willMutate": False,
        "mutationScope": "none_read_only",
        "item": _bridge_transfer_record_payload(row),
        "basisPreview": basis,
        "plan": {
            "sourceTreatment": "TRANSFER_OUT",
            "destinationTreatment": "TRANSFER_IN",
            "taxableDisposition": False,
            "taxableAcquisition": False,
            "sourceLotConsumptions": source_consumptions,
            "destinationInheritedLot": destination_lot,
            "lotJournalPreview": journal_preview,
            "selectedLotCount": len(source_consumptions),
            "estimatedCarriedBasisUsd": fifo.get("estimatedCarriedBasisUsd"),
            "basisMissing": basis_missing,
        },
        "applyReadiness": {
            "readyIfConfirmed": ready_if_confirmed,
            "blocked": not ready_if_confirmed,
            "blockedReasons": blocked_reasons,
            "actualApplyEndpointWired": False,
            "requiredFutureConfirmation": "confirm_apply_basis_transfer=true",
        },
        "readiness": readiness,
        "warnings": warnings,
        "execution": {
            "bridgeExecutionEnabled": False,
            "message": "Apply-basis-transfer preview is read-only. No ledger/FIFO, lot_journal, deposit, withdrawal, or bridge execution state was mutated.",
        },
        "nextRequired": "When a real bridge transfer is ready, link real deposit/withdrawal rows, review basis, then add a separate guarded apply_basis_transfer endpoint with explicit confirmation.",
    }


@router.post("/transfer_records/preview")
def bridge_transfer_record_preview(
    req: BridgeTransferPreviewRequest,
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    asset = _bridge_norm_asset(req.asset)
    source_chain = _bridge_norm_chain(req.source_chain)
    destination_chain = _bridge_norm_chain(req.destination_chain)
    mechanism = str(req.bridge_mechanism or "manual").strip().lower()

    if not source_chain or not destination_chain:
        raise HTTPException(status_code=422, detail={"error": "source_and_destination_chain_required"})
    if source_chain == destination_chain:
        raise HTTPException(status_code=422, detail={"error": "source_and_destination_must_differ", "chain": source_chain})
    if mechanism not in _BRIDGE_TRANSFER_MECHANISMS:
        raise HTTPException(
            status_code=422,
            detail={"error": "invalid_bridge_mechanism", "bridge_mechanism": mechanism, "allowed": sorted(_BRIDGE_TRANSFER_MECHANISMS)},
        )

    amount = float(req.amount)
    source_wallet = _bridge_registered_wallet(db, chain=source_chain, asset=asset, address=req.source_address)
    dest_wallet = _bridge_registered_wallet(db, chain=destination_chain, asset=asset, address=req.destination_address)

    source_address = str(req.source_address or getattr(source_wallet, "address", "") or "").strip() or None
    destination_address = str(req.destination_address or getattr(dest_wallet, "address", "") or "").strip() or None

    source_candidates = _bridge_count_source_candidates(
        db,
        asset=asset,
        source_chain=source_chain,
        source_address=source_address,
        amount=amount,
    )
    destination_candidates = _bridge_count_destination_candidates(
        db,
        asset=asset,
        destination_chain=destination_chain,
        amount=amount,
    )

    readiness = [
        {
            "key": "valid_amount",
            "label": "Amount",
            "status": "ready" if amount > 0 else "missing",
            "message": f"{amount:,.6f} {asset}" if amount > 0 else "Enter a positive amount.",
        },
        {
            "key": "source_wallet_registered",
            "label": "Source wallet registered",
            "status": "ready" if source_wallet is not None else "missing",
            "message": "Registered wallet found." if source_wallet is not None else f"No registered {_bridge_chain_label(source_chain)} wallet matched this preview.",
        },
        {
            "key": "destination_wallet_registered",
            "label": "Destination wallet registered",
            "status": "ready" if dest_wallet is not None else "missing",
            "message": "Registered wallet found." if dest_wallet is not None else f"No registered {_bridge_chain_label(destination_chain)} wallet matched this preview.",
        },
        {
            "key": "transfer_record_support",
            "label": "Transfer record support",
            "status": "preview",
            "message": "Preview endpoint is wired. Create/link/reconcile endpoints remain intentionally disabled.",
        },
        {
            "key": "transfer_execution",
            "label": "Transfer execution",
            "status": "disabled",
            "message": "Bridge execution remains intentionally disabled.",
        },
    ]

    if mechanism in {"lock_mint", "vault_deposit_mint_xcm"}:
        if mechanism == "vault_deposit_mint_xcm":
            readiness.extend([
                {
                    "key": "source_vault_deposit_evidence",
                    "label": "Source vault deposit evidence",
                    "status": "planned",
                    "message": "Link the Solana bridge-reserve deposit transaction as source evidence.",
                },
                {
                    "key": "asset_hub_mint_evidence",
                    "label": "Asset Hub mint evidence",
                    "status": "planned",
                    "message": "Record the Asset Hub mint extrinsic/hash and minted amount.",
                },
                {
                    "key": "hydration_receive_evidence",
                    "label": "Hydration receive/XCM evidence",
                    "status": "planned",
                    "message": "Record the Asset Hub → Hydration XCM evidence and exact Hydration received amount.",
                },
            ])
        else:
            readiness.extend([
                {
                    "key": "source_lock_evidence",
                    "label": "Source lock evidence",
                    "status": "planned",
                    "message": "After the Solana-side lock transaction exists, link its signature as source evidence.",
                },
                {
                    "key": "destination_mint_evidence",
                    "label": "Destination mint evidence",
                    "status": "planned",
                    "message": "After the Hydration-side mint/receive transaction exists, link its hash as destination evidence.",
                },
            ])

    warnings: List[str] = []
    if source_wallet is None:
        warnings.append("Source wallet is not registered locally yet.")
    if dest_wallet is None:
        warnings.append("Destination wallet is not registered locally yet.")
    if source_candidates.get("count") == 0:
        warnings.append("No matching source withdrawal/outflow candidates are currently cached.")
    if destination_candidates.get("count") == 0:
        warnings.append("No matching destination deposit/inflow candidates are currently cached.")
    if mechanism == "lock_mint":
        warnings.append("Lock/mint workflow is record-only: link Solana lock evidence and Hydration mint evidence before reconciliation. Bridge execution remains disabled.")
    if mechanism == "vault_deposit_mint_xcm":
        warnings.append("Vault/mint/XCM workflow is record-only: link Solana vault deposit, Asset Hub mint, and Hydration receive evidence before reconciliation. Bridge execution remains disabled.")

    return {
        "ok": True,
        "mode": "preview",
        "willMutate": False,
        "asset": asset,
        "amount": amount,
        "sourceChain": source_chain,
        "destinationChain": destination_chain,
        "sourceLabel": _bridge_chain_label(source_chain),
        "destinationLabel": _bridge_chain_label(destination_chain),
        "bridgeMechanism": mechanism,
        "plannedRecord": {
            "asset": asset,
            "amount": amount,
            "source_chain": source_chain,
            "destination_chain": destination_chain,
            "source_wallet_id": req.source_wallet_id or getattr(source_wallet, "wallet_id", None),
            "destination_wallet_id": req.destination_wallet_id or getattr(dest_wallet, "wallet_id", None),
            "source_address": source_address,
            "destination_address": destination_address,
            "status": "PLANNED",
            "bridge_mechanism": mechanism,
            "workflow": (
                "solana_to_hydration_vault_deposit_mint_xcm"
                if mechanism == "vault_deposit_mint_xcm" and source_chain == "solana" and destination_chain == "hydration"
                else ("solana_to_hydration_lock_mint" if mechanism == "lock_mint" and source_chain == "solana" and destination_chain == "hydration" else mechanism)
            ),
            "evidence_plan": (
                {
                    "source": "solana_vault_deposit_txid",
                    "asset_hub_mint": "asset_hub_mint_txid",
                    "asset_hub_xcm": "asset_hub_xcm_txid",
                    "destination": "hydration_receive_txid",
                    "amounts": {
                        "gross_amount": req.gross_amount or amount,
                        "destination_received_amount": req.destination_received_amount,
                        "xcm_delta_amount": req.xcm_delta_amount,
                    },
                }
                if mechanism == "vault_deposit_mint_xcm"
                else {
                    "source": "solana_lock_txid" if mechanism == "lock_mint" else "source_outflow",
                    "destination": "hydration_mint_txid" if mechanism == "lock_mint" else "destination_inflow",
                }
            ),
            "source_vault_address": _bridge_clean_str(req.source_vault_address),
            "asset_hub_mint_txid": _bridge_clean_str(req.asset_hub_mint_txid),
            "asset_hub_xcm_txid": _bridge_clean_str(req.asset_hub_xcm_txid),
            "hydration_receive_txid": _bridge_clean_str(req.hydration_receive_txid),
            "gross_amount": _bridge_clean_float(req.gross_amount) or amount,
            "destination_received_amount": _bridge_clean_float(req.destination_received_amount),
            "xcm_delta_amount": _bridge_clean_float(req.xcm_delta_amount),
            "source_proof_url": _bridge_clean_str(req.source_proof_url),
            "destination_proof_url": _bridge_clean_str(req.destination_proof_url),
            "note": req.note,
        },
        "wallets": {
            "source": _bridge_wallet_payload(source_wallet),
            "destination": _bridge_wallet_payload(dest_wallet),
        },
        "candidateLinks": {
            "source": source_candidates,
            "destination": destination_candidates,
        },
        "readiness": readiness,
        "warnings": warnings,
        "execution": {
            "bridgeExecutionEnabled": False,
            "message": "This preview does not execute, sign, submit, reconcile, or mutate ledger/FIFO state.",
        },
        "nextRequired": "If preview shape is accepted, add create/link-source/link-destination/reconcile endpoints next.",
    }


@router.get("/uttt_supply")
def bridge_uttt_supply(
    asset: str = Query("UTTT", description="Asset symbol. Currently optimized for UTTT."),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Read-only multichain UTTT supply summary for the Spread / Bridge dashboard.

    This endpoint intentionally does not execute bridge actions and does not mutate
    ledger, withdrawal, deposit, or FIFO state. It is safe for UI polling.

    Current v1 behavior:
    - Prefer Token Registry for chain/asset metadata.
    - Use env-configurable supply values with conservative defaults matching the
      current UTTT multichain design.
    - Show Hydration as route/liquidity metadata by default, not separately
      counted, so the Asset Hub-side supply is not double-counted.
    """

    sym = str(asset or "UTTT").strip().upper() or "UTTT"
    rows = _token_registry_rows(db, sym)

    sol_row = _pick_registry_row(rows, chain_aliases=["solana"], venue_aliases=["solana_jupiter"])
    asset_hub_row = _pick_registry_row(
        rows,
        chain_aliases=["polkadot_asset_hub", "asset_hub", "polkadot"],
        venue_aliases=["polkadot_asset_hub", "asset_hub"],
    )
    hyd_row = _pick_registry_row(
        rows,
        chain_aliases=["hydration", "polkadot"],
        venue_aliases=["polkadot_hydration", "hydration"],
    )

    transfer_record_summary = _bridge_transfer_record_supply_summary(db, asset=sym)
    bridge_backed_gross_amount = float(
        (transfer_record_summary or {}).get("solanaToHydrationVaultMintXcmReconciledGrossAmount")
        or (transfer_record_summary or {}).get("vaultMintXcmReconciledGrossAmount")
        or 0.0
    )
    bridge_backed_received_amount = float(
        (transfer_record_summary or {}).get("solanaToHydrationVaultMintXcmReconciledHydrationReceivedAmount")
        or (transfer_record_summary or {}).get("vaultMintXcmReconciledHydrationReceivedAmount")
        or 0.0
    )
    bridge_backed_delta_amount = float(
        (transfer_record_summary or {}).get("solanaToHydrationVaultMintXcmReconciledXcmDeltaAmount")
        or (transfer_record_summary or {}).get("vaultMintXcmReconciledXcmDeltaAmount")
        or 0.0
    )

    default_asset_hub_supply = _env_float("UTT_UTTT_POLKADOT_ASSET_HUB_SUPPLY", None)
    asset_hub_supply_source = "env:UTT_UTTT_POLKADOT_ASSET_HUB_SUPPLY" if os.getenv("UTT_UTTT_POLKADOT_ASSET_HUB_SUPPLY") else None
    if default_asset_hub_supply is None:
        default_asset_hub_supply = _env_float("UTT_UTTT_ASSET_HUB_SUPPLY", None)
        asset_hub_supply_source = "env:UTT_UTTT_ASSET_HUB_SUPPLY" if os.getenv("UTT_UTTT_ASSET_HUB_SUPPLY") else None
    if default_asset_hub_supply is None:
        initial_asset_hub_supply = _env_float("UTT_UTTT_INITIAL_POLKADOT_ALLOCATION_SUPPLY", 30_000_000.0)
        include_bridge_records = _env_bool("UTT_UTTT_INCLUDE_RECONCILED_BRIDGE_IN_ASSET_HUB_SUPPLY", True)
        default_asset_hub_supply = float(initial_asset_hub_supply or 0.0) + (bridge_backed_gross_amount if include_bridge_records else 0.0)
        asset_hub_supply_source = "derived:initial_allocation_plus_reconciled_bridge_records" if include_bridge_records else "derived:initial_allocation_only"

    target_supply = _env_float("UTT_UTTT_CANONICAL_TOTAL_SUPPLY", None)
    if target_supply is None:
        target_supply = _env_float("UTT_UTTT_TOTAL_SUPPLY", 1_000_000_000.0)

    solana_supply = _env_float("UTT_UTTT_SOLANA_SUPPLY", None)
    if solana_supply is None and target_supply is not None and default_asset_hub_supply is not None:
        solana_supply = max(0.0, float(target_supply) - float(default_asset_hub_supply))

    hydration_supply = _env_float("UTT_UTTT_HYDRATION_SUPPLY", None)
    hydration_counted = _env_bool("UTT_UTTT_HYDRATION_SUPPLY_COUNTED", False)

    chains: List[Dict[str, Any]] = [
        {
            "chain": "solana",
            "venue": "solana_jupiter",
            "label": "Solana",
            "assetId": _display_asset_id(sol_row),
            "supply": solana_supply,
            "counted": True,
            "source": "env:UTT_UTTT_SOLANA_SUPPLY" if os.getenv("UTT_UTTT_SOLANA_SUPPLY") else "derived:canonical_minus_asset_hub",
            "status": "configured" if solana_supply is not None else "missing_supply",
            "registry": _registry_row_payload(sol_row),
        },
        {
            "chain": "polkadot_asset_hub",
            "venue": "polkadot_asset_hub",
            "label": "Polkadot / Asset Hub",
            "assetId": _display_asset_id(asset_hub_row, "50000456"),
            "supply": default_asset_hub_supply,
            "counted": True,
            "source": asset_hub_supply_source or ("env:UTT_UTTT_POLKADOT_ASSET_HUB_SUPPLY" if os.getenv("UTT_UTTT_POLKADOT_ASSET_HUB_SUPPLY") else "default:known_asset_hub_mint"),
            "status": "configured" if default_asset_hub_supply is not None else "missing_supply",
            "registry": _registry_row_payload(asset_hub_row),
            "decimals": int(getattr(asset_hub_row, "decimals", 6) or 6),
        },
        {
            "chain": "hydration",
            "venue": "polkadot_hydration",
            "label": "Hydration route asset",
            "assetId": _display_asset_id(hyd_row, "1001331"),
            "supply": hydration_supply,
            "counted": bool(hydration_counted and hydration_supply is not None),
            "source": "env:UTT_UTTT_HYDRATION_SUPPLY" if hydration_supply is not None else "token_registry_or_route_metadata",
            "status": "configured_counted" if hydration_counted and hydration_supply is not None else "metadata_only_not_counted",
            "registry": _registry_row_payload(hyd_row),
            "note": "Hydration is treated as route/liquidity presence by default. It is not counted separately unless UTT_UTTT_HYDRATION_SUPPLY_COUNTED=1.",
        },
    ]

    counted_supply = 0.0
    for row in chains:
        if row.get("counted"):
            try:
                counted_supply += float(row.get("supply") or 0.0)
            except Exception:
                pass

    warnings: List[str] = []
    if target_supply is not None and counted_supply > 0:
        delta = abs(float(target_supply) - float(counted_supply))
        if delta > 0.000001:
            warnings.append(
                f"Counted supply {counted_supply:,.6f} differs from configured target supply {float(target_supply):,.6f}."
            )
    if hyd_row is not None and not hydration_counted:
        warnings.append("Hydration UTTT metadata is shown but not double-counted against Asset Hub-side canonical supply.")

    bridge_treasury = {
        "ok": True,
        "asset": sym,
        "model": "record_derived_until_live_treasury_balance_sync",
        "sourceReserveAmount": bridge_backed_gross_amount,
        "destinationTreasuryAmount": bridge_backed_received_amount,
        "xcmDeltaAmount": bridge_backed_delta_amount,
        "sourceReserveLabel": "Solana Bridge Reserve",
        "destinationTreasuryLabel": "Hydration Bridge Treasury",
        "source": "bridge_transfer_records:vault_deposit_mint_xcm:reconciled",
        "note": "These amounts are derived from reconciled vault/mint/XCM bridge records. Live treasury balance sync is a later bridge registry task.",
    }

    return {
        "ok": True,
        "asset": sym,
        "decimals": 6,
        "totalSupply": counted_supply,
        "totalCanonicalSupply": counted_supply,
        # Backward-compatible alias for the earlier misspelling; UI should prefer totalCanonicalSupply.
        "totalConicalSupply": counted_supply,
        "targetSupply": target_supply,
        "chains": chains,
        "transferRecords": transfer_record_summary,
        "bridgeTreasury": bridge_treasury,
        "pendingBridgeAmount": transfer_record_summary.get("pendingAmount") if isinstance(transfer_record_summary, dict) else None,
        "reconciledBridgeAmount": transfer_record_summary.get("reconciledAmount") if isinstance(transfer_record_summary, dict) else None,
        "solanaToHydrationPendingAmount": transfer_record_summary.get("solanaToHydrationPendingAmount") if isinstance(transfer_record_summary, dict) else None,
        "solanaToHydrationReconciledAmount": transfer_record_summary.get("solanaToHydrationReconciledAmount") if isinstance(transfer_record_summary, dict) else None,
        "warnings": warnings,
        "execution": {
            "bridgeExecutionEnabled": False,
            "message": "Supply endpoint is read-only and does not execute or plan bridge transactions.",
        },
    }
