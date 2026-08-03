from __future__ import annotations

import copy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy.orm import Session

from ..models import RobinhoodChainPairCapability, RobinhoodChainPairObjective, TokenRegistry
from .evm_rpc import validate_evm_address


ROBINHOOD_CHAIN = "robinhood_chain"
ROBINHOOD_CHAIN_VENUE = "robinhood_chain"
ASSET_KIND_NATIVE = "native"
ASSET_KIND_ERC20 = "erc20"


class RobinhoodChainRegistryAuthorityError(ValueError):
    def __init__(self, code: str, message: str, **context: Any) -> None:
        self.code = str(code or "robinhood_chain_registry_authority_error")
        self.message = str(message or self.code)
        self.context = dict(context)
        super().__init__(self.code)


def normalize_registry_symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper()
    if not symbol:
        raise RobinhoodChainRegistryAuthorityError(
            "robinhood_chain_registry_symbol_required",
            "A Robinhood Chain Token Registry symbol is required.",
        )
    if len(symbol) > 32:
        raise RobinhoodChainRegistryAuthorityError(
            "robinhood_chain_registry_symbol_too_long",
            "The Robinhood Chain Token Registry symbol exceeds 32 characters.",
            symbol=symbol,
        )
    return symbol


def normalize_registry_venue(value: Any) -> Optional[str]:
    venue = str(value or "").strip().lower()
    return venue or None


def normalize_registry_decimals(value: Any) -> int:
    try:
        decimals = int(value)
    except Exception as exc:
        raise RobinhoodChainRegistryAuthorityError(
            "invalid_robinhood_chain_registry_decimals",
            "Robinhood Chain Token Registry decimals must be an integer between 0 and 18.",
            decimals=value,
        ) from exc
    if decimals < 0 or decimals > 18:
        raise RobinhoodChainRegistryAuthorityError(
            "invalid_robinhood_chain_registry_decimals",
            "Robinhood Chain Token Registry decimals must be between 0 and 18.",
            decimals=decimals,
        )
    return decimals


def normalize_asset_kind(value: Any, *, address: Any) -> str:
    raw = str(value or "").strip().lower().replace("-", "_")
    if raw in {"contract", "contract_backed", "token", "erc_20"}:
        raw = ASSET_KIND_ERC20
    if raw:
        if raw not in {ASSET_KIND_NATIVE, ASSET_KIND_ERC20}:
            raise RobinhoodChainRegistryAuthorityError(
                "invalid_robinhood_chain_asset_kind",
                "Robinhood Chain asset_kind must be native or erc20.",
                asset_kind=value,
            )
        return raw
    return ASSET_KIND_NATIVE if not str(address or "").strip() else ASSET_KIND_ERC20


def normalize_identity_input(
    *,
    symbol: Any,
    address: Any,
    decimals: Any,
    asset_kind: Any = None,
) -> Dict[str, Any]:
    normalized_symbol = normalize_registry_symbol(symbol)
    normalized_decimals = normalize_registry_decimals(decimals)
    raw_address = str(address or "").strip()
    normalized_kind = normalize_asset_kind(asset_kind, address=raw_address)

    if normalized_kind == ASSET_KIND_NATIVE:
        if raw_address:
            raise RobinhoodChainRegistryAuthorityError(
                "robinhood_chain_native_address_must_be_blank",
                "A Robinhood Chain native asset must use a blank Token Registry address.",
                symbol=normalized_symbol,
                address=raw_address,
                asset_kind=normalized_kind,
            )
        normalized_address: Optional[str] = None
    else:
        if not raw_address:
            raise RobinhoodChainRegistryAuthorityError(
                "robinhood_chain_erc20_contract_required",
                "A Robinhood Chain ERC-20 asset requires a Token Registry contract address.",
                symbol=normalized_symbol,
                asset_kind=normalized_kind,
            )
        try:
            normalized_address = validate_evm_address(raw_address)
        except ValueError as exc:
            raise RobinhoodChainRegistryAuthorityError(
                "invalid_robinhood_chain_contract_address",
                str(exc),
                symbol=normalized_symbol,
                address=raw_address,
                asset_kind=normalized_kind,
            ) from exc

    return {
        "symbol": normalized_symbol,
        "address": normalized_address,
        "decimals": normalized_decimals,
        "asset_kind": normalized_kind,
        "native": normalized_kind == ASSET_KIND_NATIVE,
    }


def identity_fields_from_row(row: TokenRegistry) -> Dict[str, Any]:
    return normalize_identity_input(
        symbol=getattr(row, "symbol", None),
        address=getattr(row, "address", None),
        decimals=getattr(row, "decimals", None),
        asset_kind=None,
    )


def row_is_native(row: TokenRegistry) -> bool:
    return not str(getattr(row, "address", None) or "").strip()


def row_asset_kind(row: TokenRegistry) -> str:
    return ASSET_KIND_NATIVE if row_is_native(row) else ASSET_KIND_ERC20


def assert_unambiguous_effective_native_rows(rows: Sequence[TokenRegistry]) -> None:
    native_rows = [row for row in rows if row_is_native(row)]
    if len(native_rows) <= 1:
        return
    raise RobinhoodChainRegistryAuthorityError(
        "ambiguous_robinhood_chain_native_registry_identity",
        "More than one effective Robinhood Chain Token Registry row has a blank address.",
        registry_ids=[int(row.id) for row in native_rows],
        symbols=[normalize_registry_symbol(row.symbol) for row in native_rows],
    )


def select_effective_registry_rows(
    db: Session,
    *,
    venue: str = ROBINHOOD_CHAIN_VENUE,
    limit: int = 250,
) -> List[TokenRegistry]:
    normalized_venue = normalize_registry_venue(venue) or ROBINHOOD_CHAIN_VENUE
    bounded_limit = max(1, min(int(limit), 1000))
    overrides = (
        db.query(TokenRegistry)
        .filter(
            TokenRegistry.chain == ROBINHOOD_CHAIN,
            TokenRegistry.venue == normalized_venue,
        )
        .order_by(TokenRegistry.symbol.asc())
        .limit(bounded_limit)
        .all()
    )
    globals_ = (
        db.query(TokenRegistry)
        .filter(
            TokenRegistry.chain == ROBINHOOD_CHAIN,
            ((TokenRegistry.venue.is_(None)) | (TokenRegistry.venue == "")),
        )
        .order_by(TokenRegistry.symbol.asc())
        .limit(bounded_limit)
        .all()
    )

    for scope_name, scope_rows in ((normalized_venue, overrides or []), (None, globals_ or [])):
        native_rows = [row for row in scope_rows if row_is_native(row)]
        if len(native_rows) > 1:
            raise RobinhoodChainRegistryAuthorityError(
                "duplicate_robinhood_chain_native_registry_scope",
                "More than one Robinhood Chain native Token Registry row exists in the same venue scope.",
                venue=scope_name,
                registry_ids=[int(row.id) for row in native_rows],
                symbols=[normalize_registry_symbol(row.symbol) for row in native_rows],
            )

    selected: Dict[str, TokenRegistry] = {}
    for row in [*(overrides or []), *(globals_ or [])]:
        symbol = normalize_registry_symbol(getattr(row, "symbol", None))
        if symbol not in selected:
            selected[symbol] = row

    rows = [selected[key] for key in sorted(selected)]
    assert_unambiguous_effective_native_rows(rows)
    return rows


def effective_row_by_symbol(
    db: Session,
    symbol: Any,
    *,
    venue: str = ROBINHOOD_CHAIN_VENUE,
) -> TokenRegistry:
    normalized_symbol = normalize_registry_symbol(symbol)
    for row in select_effective_registry_rows(db, venue=venue):
        if normalize_registry_symbol(row.symbol) == normalized_symbol:
            return row
    raise RobinhoodChainRegistryAuthorityError(
        "robinhood_chain_registry_token_not_found",
        "The requested Robinhood Chain token is not present in the effective Token Registry view.",
        symbol=normalized_symbol,
        venue=normalize_registry_venue(venue),
    )


def effective_native_row(
    db: Session,
    *,
    venue: str = ROBINHOOD_CHAIN_VENUE,
) -> TokenRegistry:
    rows = select_effective_registry_rows(db, venue=venue)
    native_rows = [row for row in rows if row_is_native(row)]
    if not native_rows:
        raise RobinhoodChainRegistryAuthorityError(
            "robinhood_chain_native_registry_identity_not_found",
            "No effective Robinhood Chain Token Registry row has a blank native address.",
            venue=normalize_registry_venue(venue),
        )
    return native_rows[0]


def assert_native_write_unambiguous(
    db: Session,
    *,
    venue: Any,
    symbol: Any,
    exclude_token_id: Optional[int] = None,
) -> None:
    normalized_venue = normalize_registry_venue(venue)
    normalized_symbol = normalize_registry_symbol(symbol)
    query = db.query(TokenRegistry).filter(
        TokenRegistry.chain == ROBINHOOD_CHAIN,
        ((TokenRegistry.address.is_(None)) | (TokenRegistry.address == "")),
    )
    if exclude_token_id is not None:
        query = query.filter(TokenRegistry.id != int(exclude_token_id))

    for row in query.all() or []:
        existing_venue = normalize_registry_venue(getattr(row, "venue", None))
        existing_symbol = normalize_registry_symbol(getattr(row, "symbol", None))

        if existing_venue == normalized_venue:
            raise RobinhoodChainRegistryAuthorityError(
                "duplicate_robinhood_chain_native_registry_scope",
                "Only one Robinhood Chain native Token Registry row is allowed in the same venue scope.",
                symbol=normalized_symbol,
                venue=normalized_venue,
                conflicting_registry_id=int(row.id),
                conflicting_symbol=existing_symbol,
            )

        cross_scope_overlap = (
            normalized_venue is None
            or existing_venue is None
        )
        if cross_scope_overlap and existing_symbol != normalized_symbol:
            raise RobinhoodChainRegistryAuthorityError(
                "ambiguous_robinhood_chain_native_registry_identity",
                "Global and venue-specific Robinhood Chain native rows must use the same symbol.",
                symbol=normalized_symbol,
                venue=normalized_venue,
                conflicting_registry_id=int(row.id),
                conflicting_symbol=existing_symbol,
                conflicting_venue=existing_venue,
            )


EXECUTION_STATUS_LIVE_VERIFIED = "live_verified"
EXECUTION_STATUS_PREPARATION_VERIFIED = "preparation_verified"
EXECUTION_STATUS_LIVE_AUTHORIZED_PENDING_CONFIRMATION = "live_authorized_pending_confirmation"
EXECUTION_MECHANISM_SWAP = "swap"
EXECUTION_AMOUNT_MODE_EXACT_INPUT = "exact_input"
EXECUTION_PROVIDER_ZEROX = "0x"
ZEROX_NATIVE_TOKEN = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"


def normalize_execution_symbol(value: Any) -> str:
    symbol = str(value or "").strip().upper().replace("/", "-")
    parts = [part.strip() for part in symbol.split("-") if part.strip()]
    if len(parts) != 2:
        raise RobinhoodChainRegistryAuthorityError(
            "invalid_robinhood_chain_execution_symbol",
            "Robinhood Chain execution requires one BASE-QUOTE market symbol.",
            symbol=symbol,
        )
    return f"{normalize_registry_symbol(parts[0])}-{normalize_registry_symbol(parts[1])}"


def normalize_execution_side(value: Any) -> str:
    side = str(value or "").strip().lower()
    if side not in {"buy", "sell"}:
        raise RobinhoodChainRegistryAuthorityError(
            "invalid_robinhood_chain_execution_side",
            "Robinhood Chain execution side must be buy or sell.",
            side=value,
        )
    return side


def normalize_execution_amount_mode(value: Any) -> str:
    amount_mode = str(value or "").strip().lower().replace("exact_spend", "exact_input")
    if amount_mode != EXECUTION_AMOUNT_MODE_EXACT_INPUT:
        raise RobinhoodChainRegistryAuthorityError(
            "robinhood_chain_execution_amount_mode_not_supported",
            "RH-REG.AUTH.1C authorizes exact-input execution only.",
            amount_mode=value,
        )
    return amount_mode


def normalize_execution_provider(value: Any) -> str:
    provider = str(value or EXECUTION_PROVIDER_ZEROX).strip().lower()
    if provider == "zerox":
        provider = EXECUTION_PROVIDER_ZEROX
    if provider != EXECUTION_PROVIDER_ZEROX:
        raise RobinhoodChainRegistryAuthorityError(
            "robinhood_chain_execution_provider_not_supported",
            "RH-REG.AUTH.1C authorizes the persisted 0x swap provider only.",
            provider=value,
        )
    return provider


def _execution_identity(row: TokenRegistry) -> Dict[str, Any]:
    identity = identity_fields_from_row(row)
    registry_contract_address = identity["address"]
    return {
        "registry_id": int(row.id),
        "registry_venue": normalize_registry_venue(getattr(row, "venue", None)),
        "identity_source": "token_registry",
        "symbol": identity["symbol"],
        "contract_address": ZEROX_NATIVE_TOKEN if identity["native"] else registry_contract_address,
        "registry_contract_address": registry_contract_address,
        "decimals": identity["decimals"],
        "native": identity["native"],
        "asset_kind": identity["asset_kind"],
    }


def _preparation_authority_matches(
    *,
    objective: RobinhoodChainPairObjective,
    capability: RobinhoodChainPairCapability,
    symbol: str,
    side: str,
    amount_mode: str,
    provider: str,
    input_identity: Dict[str, Any],
    output_identity: Dict[str, Any],
) -> bool:
    evidence = capability.evidence if isinstance(capability.evidence, dict) else {}
    ceiling = str(capability.probe_amount or "").strip()
    return bool(
        capability.enabled
        and str(capability.indicative_status or "").strip().lower() in {"available", EXECUTION_STATUS_LIVE_VERIFIED}
        and str(capability.firm_plan_status or "").strip().lower() == "available"
        and str(capability.execution_status or "").strip().lower() == EXECUTION_STATUS_PREPARATION_VERIFIED
        and evidence.get("preparation_verified") is True
        and evidence.get("live_accepted") is not True
        and evidence.get("successful_broadcast") is not True
        and normalize_execution_symbol(evidence.get("symbol")) == symbol
        and normalize_execution_side(evidence.get("side")) == side
        and normalize_execution_amount_mode(evidence.get("amount_mode")) == amount_mode
        and normalize_execution_provider(evidence.get("provider")) == provider
        and normalize_registry_symbol(evidence.get("from_asset")) == input_identity["symbol"]
        and normalize_registry_symbol(evidence.get("to_asset")) == output_identity["symbol"]
        and str(evidence.get("verified_input_amount") or "").strip() == ceiling
        and str(evidence.get("firm_plan_input_ceiling") or "").strip() == ceiling
        and bool(ceiling)
        and str(objective.symbol or "").strip().upper() == symbol
    )


def _parse_authorization_time(value: Any) -> Optional[datetime]:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _live_authorization_matches(
    *,
    objective: RobinhoodChainPairObjective,
    capability: RobinhoodChainPairCapability,
    symbol: str,
    side: str,
    amount_mode: str,
    provider: str,
    input_identity: Dict[str, Any],
    output_identity: Dict[str, Any],
) -> bool:
    evidence = capability.evidence if isinstance(capability.evidence, dict) else {}
    authorization = evidence.get("live_authorization")
    if not isinstance(authorization, dict):
        return False
    expires_at = _parse_authorization_time(authorization.get("expires_at"))
    now = datetime.now(timezone.utc)
    ceiling = str(capability.probe_amount or "").strip()
    return bool(
        _preparation_authority_matches(
            objective=objective,
            capability=capability,
            symbol=symbol,
            side=side,
            amount_mode=amount_mode,
            provider=provider,
            input_identity=input_identity,
            output_identity=output_identity,
        )
        and evidence.get("successful_broadcast_authorized") is True
        and str(authorization.get("status") or "").strip().lower()
        == EXECUTION_STATUS_LIVE_AUTHORIZED_PENDING_CONFIRMATION
        and authorization.get("operator_confirmed") is True
        and authorization.get("automatic_execution_promotion") is False
        and str(authorization.get("symbol") or "").strip().upper() == symbol
        and str(authorization.get("side") or "").strip().lower() == side
        and str(authorization.get("amount_mode") or "").strip().lower() == amount_mode
        and str(authorization.get("provider") or "").strip().lower() == provider
        and str(authorization.get("input_asset") or "").strip().upper() == input_identity["symbol"]
        and str(authorization.get("output_asset") or "").strip().upper() == output_identity["symbol"]
        and str(authorization.get("exact_input_amount") or "").strip() == ceiling
        and str(authorization.get("approval_model") or "").strip().lower()
        == ("none" if bool(input_identity["native"]) else "finite_exact_input")
        and authorization.get("unlimited_approval_enabled") is False
        and str(authorization.get("approval_transaction_value_wei") or "").strip() == "0"
        and str(authorization.get("swap_transaction_value_wei") or "").strip() == "0"
        and authorization.get("separate_wallet_requests_required") is True
        and authorization.get("automatic_second_transaction") is False
        and authorization.get("automatic_retry") is False
        and bool(str(authorization.get("wallet_address") or "").strip())
        and bool(str(authorization.get("authorization_id") or "").strip())
        and expires_at is not None
        and expires_at > now
    )


def _execution_blocking_reasons(
    *,
    objective: RobinhoodChainPairObjective,
    capability: Optional[RobinhoodChainPairCapability],
    preparation_authority: bool,
) -> List[str]:
    reasons: List[str] = []
    if not bool(objective.enabled):
        reasons.append("objective_disabled")
    if str(objective.mechanism or "").strip().lower() != EXECUTION_MECHANISM_SWAP:
        reasons.append("mechanism_not_supported")
    if capability is None:
        reasons.append("direction_capability_missing")
        return reasons
    if not bool(capability.enabled):
        reasons.append("capability_disabled")
    if preparation_authority:
        if str(capability.indicative_status or "").strip().lower() not in {"available", EXECUTION_STATUS_LIVE_VERIFIED}:
            reasons.append("indicative_not_preparation_verified")
        if str(capability.firm_plan_status or "").strip().lower() != "available":
            reasons.append("firm_plan_not_preparation_verified")
        if str(capability.execution_status or "").strip().lower() != EXECUTION_STATUS_PREPARATION_VERIFIED:
            reasons.append("execution_not_preparation_verified")
    else:
        if str(capability.indicative_status or "").strip().lower() != EXECUTION_STATUS_LIVE_VERIFIED:
            reasons.append("indicative_not_live_verified")
        if str(capability.firm_plan_status or "").strip().lower() != EXECUTION_STATUS_LIVE_VERIFIED:
            reasons.append("firm_plan_not_live_verified")
        if str(capability.execution_status or "").strip().lower() != EXECUTION_STATUS_LIVE_VERIFIED:
            reasons.append("execution_not_live_verified")
    return reasons


def resolve_robinhood_chain_execution_authority(
    db: Session,
    *,
    symbol: Any,
    side: Any,
    amount_mode: Any = EXECUTION_AMOUNT_MODE_EXACT_INPUT,
    provider: Any = EXECUTION_PROVIDER_ZEROX,
    require_execution: bool = False,
) -> Dict[str, Any]:
    """Resolve one execution direction from database capability and Token Registry identity.

    This resolver never contacts a provider and never mutates capability state.
    Persisted probe and prior acceptance amounts remain historical capability
    evidence only; each explicit transaction uses the operator's current exact
    input, a fresh plan, live balance checks, and current allowance checks.
    """
    normalized_symbol = normalize_execution_symbol(symbol)
    normalized_side = normalize_execution_side(side)
    normalized_mode = normalize_execution_amount_mode(amount_mode)
    normalized_provider = normalize_execution_provider(provider)

    objective = (
        db.query(RobinhoodChainPairObjective)
        .filter(RobinhoodChainPairObjective.symbol == normalized_symbol)
        .order_by(RobinhoodChainPairObjective.updated_at.desc())
        .first()
    )
    if objective is None:
        raise RobinhoodChainRegistryAuthorityError(
            "robinhood_chain_execution_objective_not_found",
            "The requested market is not present in the Robinhood Chain pair-objective database.",
            symbol=normalized_symbol,
            provider_contacted=False,
        )

    token_ids = {
        int(objective.base_token_registry_id),
        int(objective.quote_token_registry_id),
    }
    token_rows = (
        db.query(TokenRegistry)
        .filter(TokenRegistry.id.in_(token_ids))
        .all()
    )
    token_by_id = {int(row.id): row for row in token_rows}
    base_row = token_by_id.get(int(objective.base_token_registry_id))
    quote_row = token_by_id.get(int(objective.quote_token_registry_id))
    if base_row is None or quote_row is None:
        raise RobinhoodChainRegistryAuthorityError(
            "robinhood_chain_execution_registry_identity_missing",
            "The requested market does not resolve to both Token Registry identities.",
            symbol=normalized_symbol,
            base_token_registry_id=int(objective.base_token_registry_id),
            quote_token_registry_id=int(objective.quote_token_registry_id),
            provider_contacted=False,
        )

    base_identity = _execution_identity(base_row)
    quote_identity = _execution_identity(quote_row)
    expected_symbol = f"{base_identity['symbol']}-{quote_identity['symbol']}"
    if expected_symbol != normalized_symbol:
        raise RobinhoodChainRegistryAuthorityError(
            "robinhood_chain_execution_objective_identity_mismatch",
            "The pair-objective symbol does not match its Token Registry identities.",
            symbol=normalized_symbol,
            expected_symbol=expected_symbol,
            provider_contacted=False,
        )

    input_identity = base_identity if normalized_side == "sell" else quote_identity
    output_identity = quote_identity if normalized_side == "sell" else base_identity
    capability = (
        db.query(RobinhoodChainPairCapability)
        .filter(
            RobinhoodChainPairCapability.objective_id == objective.id,
            RobinhoodChainPairCapability.from_token_registry_id == int(input_identity["registry_id"]),
            RobinhoodChainPairCapability.to_token_registry_id == int(output_identity["registry_id"]),
            RobinhoodChainPairCapability.amount_mode == normalized_mode,
            RobinhoodChainPairCapability.provider == normalized_provider,
        )
        .order_by(RobinhoodChainPairCapability.updated_at.desc())
        .first()
    )

    live_execution_verified = bool(
        capability is not None
        and bool(capability.enabled)
        and str(capability.indicative_status or "").strip().lower() == EXECUTION_STATUS_LIVE_VERIFIED
        and str(capability.firm_plan_status or "").strip().lower() == EXECUTION_STATUS_LIVE_VERIFIED
        and str(capability.execution_status or "").strip().lower() == EXECUTION_STATUS_LIVE_VERIFIED
    )
    preparation_authority = False
    live_authorized_pending_confirmation = False
    if capability is not None and not live_execution_verified:
        try:
            preparation_authority = _preparation_authority_matches(
                objective=objective,
                capability=capability,
                symbol=normalized_symbol,
                side=normalized_side,
                amount_mode=normalized_mode,
                provider=normalized_provider,
                input_identity=input_identity,
                output_identity=output_identity,
            )
            if preparation_authority:
                live_authorized_pending_confirmation = _live_authorization_matches(
                    objective=objective,
                    capability=capability,
                    symbol=normalized_symbol,
                    side=normalized_side,
                    amount_mode=normalized_mode,
                    provider=normalized_provider,
                    input_identity=input_identity,
                    output_identity=output_identity,
                )
        except RobinhoodChainRegistryAuthorityError:
            preparation_authority = False
            live_authorized_pending_confirmation = False
    reasons = _execution_blocking_reasons(
        objective=objective,
        capability=capability,
        preparation_authority=preparation_authority,
    )
    ceiling = str(capability.probe_amount or "").strip() if capability is not None else ""
    adapter = "native_exact_input" if bool(input_identity["native"]) else "erc20_exact_input"
    authority_level = (
        EXECUTION_STATUS_LIVE_VERIFIED
        if live_execution_verified
        else EXECUTION_STATUS_LIVE_AUTHORIZED_PENDING_CONFIRMATION
        if live_authorized_pending_confirmation
        else EXECUTION_STATUS_PREPARATION_VERIFIED
        if preparation_authority
        else "blocked"
    )
    approval = {
        "applicable": not bool(input_identity["native"]),
        "model": "none" if bool(input_identity["native"]) else "finite_exact_input",
        "token": input_identity,
        "unlimited_approval_enabled": False,
    }
    payload = {
        "ok": True,
        "venue": ROBINHOOD_CHAIN_VENUE,
        "network": ROBINHOOD_CHAIN,
        "symbol": normalized_symbol,
        "side": normalized_side,
        "amount_mode": normalized_mode,
        "provider": normalized_provider,
        "mechanism": str(objective.mechanism or "").strip().lower(),
        "objective": {
            "id": str(objective.id),
            "enabled": bool(objective.enabled),
            "review_only": bool(objective.review_only),
        },
        "capability": None if capability is None else {
            "id": str(capability.id),
            "enabled": bool(capability.enabled),
            "indicative_status": str(capability.indicative_status or "").strip().lower(),
            "firm_plan_status": str(capability.firm_plan_status or "").strip().lower(),
            "execution_status": str(capability.execution_status or "").strip().lower(),
            "probe_amount": ceiling or None,
            "evidence": copy.deepcopy(capability.evidence) if isinstance(capability.evidence, dict) else {},
            "last_verified_at": capability.last_verified_at.isoformat() if capability.last_verified_at else None,
        },
        "input": input_identity,
        "output": output_identity,
        "approval": approval,
        "execution_adapter": adapter,
        "execution_ceiling": {
            "amount": ceiling or None,
            "asset": input_identity["symbol"],
            "enforced": False,
            "role": "historical_capability_evidence",
            "source": (
                "database_confirmed_execution_evidence"
                if live_execution_verified
                else "database_live_authorization_evidence"
                if live_authorized_pending_confirmation
                else "database_preparation_verification_evidence"
                if preparation_authority
                else "database_direction_capability_probe_evidence"
            ),
        },
        "authority_level": authority_level,
        "live_execution_verified": live_execution_verified,
        "live_authorized_pending_confirmation": live_authorized_pending_confirmation,
        "preparation_verified": preparation_authority,
        "initial_acceptance_wallet_reject_only": bool(
            preparation_authority and not live_authorized_pending_confirmation
        ),
        "successful_broadcast_authorized": bool(
            live_execution_verified or live_authorized_pending_confirmation
        ),
        "live_authorization": (
            copy.deepcopy((capability.evidence or {}).get("live_authorization"))
            if capability is not None
            and isinstance(capability.evidence, dict)
            and isinstance(capability.evidence.get("live_authorization"), dict)
            else None
        ),
        "execution_permitted": not reasons,
        "blocking_reasons": reasons,
        "provider_contacted": False,
        "automatic_execution_promotion": False,
        "will_mutate": False,
    }

    if require_execution and reasons:
        raise RobinhoodChainRegistryAuthorityError(
            "robinhood_chain_execution_authority_blocked",
            "The persisted direction capability does not authorize execution preparation.",
            authority=payload,
            blocking_reasons=reasons,
            provider_contacted=False,
        )
    return payload


def assert_robinhood_chain_execution_amount(
    authority: Dict[str, Any],
    amount: Any,
) -> str:
    """Validate the operator's current exact input without enforcing probe evidence."""
    raw_amount = str(amount or "").strip()
    try:
        requested = Decimal(raw_amount)
    except (InvalidOperation, ValueError) as exc:
        raise RobinhoodChainRegistryAuthorityError(
            "invalid_robinhood_chain_execution_amount",
            "Execution input amount must be a positive decimal.",
            amount=amount,
            provider_contacted=False,
        ) from exc
    if not requested.is_finite() or requested <= 0:
        raise RobinhoodChainRegistryAuthorityError(
            "invalid_robinhood_chain_execution_amount",
            "Execution input amount must be a positive decimal.",
            amount=amount,
            provider_contacted=False,
        )

    try:
        decimals = int(((authority or {}).get("input") or {}).get("decimals"))
    except (TypeError, ValueError) as exc:
        raise RobinhoodChainRegistryAuthorityError(
            "robinhood_chain_execution_input_decimals_missing",
            "Execution input decimals are unavailable from Token Registry authority.",
            authority=authority,
            provider_contacted=False,
        ) from exc
    if decimals < 0 or decimals > 18:
        raise RobinhoodChainRegistryAuthorityError(
            "robinhood_chain_execution_input_decimals_missing",
            "Execution input decimals are outside the supported Token Registry range.",
            authority=authority,
            provider_contacted=False,
        )
    if max(0, -requested.as_tuple().exponent) > decimals:
        raise RobinhoodChainRegistryAuthorityError(
            "robinhood_chain_execution_amount_precision_exceeded",
            "Execution input exceeds the Token Registry decimal precision.",
            requested_amount=raw_amount,
            input_decimals=decimals,
            input_asset=((authority or {}).get("input") or {}).get("symbol"),
            provider_contacted=False,
        )

    normalized = format(requested, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized or "0"
