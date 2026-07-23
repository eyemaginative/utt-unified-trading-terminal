from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy.orm import Session

from ..models import TokenRegistry
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
