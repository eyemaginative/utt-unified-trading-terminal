from __future__ import annotations

import copy
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import text
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
            normalized_address = validate_evm_address(raw_address).lower()
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



def token_registry_contract_identity_schema_state(db: Session) -> Dict[str, Any]:
    """Inspect the Token Registry identity schema without mutating it."""
    bind = db.get_bind()
    dialect = str(getattr(getattr(bind, "dialect", None), "name", "") or "").lower()
    if dialect != "sqlite":
        return {
            "checked": False,
            "ready": True,
            "dialect": dialect or None,
            "legacy_symbol_unique": False,
            "contract_identity_unique": True,
        }

    indexes = db.execute(text("PRAGMA index_list('token_registry')")).mappings().all()
    legacy_columns = ("chain", "venue", "symbol")
    legacy_present = False
    current_present = False
    unique_indexes: List[Dict[str, Any]] = []
    for index in indexes or []:
        if not bool(index.get("unique")):
            continue
        index_name = str(index.get("name") or "")
        safe_name = index_name.replace("'", "''")
        columns = db.execute(text(f"PRAGMA index_info('{safe_name}')")).mappings().all()
        column_names = tuple(str(item.get("name") or "") for item in columns)
        sql_row = db.execute(
            text("SELECT sql FROM sqlite_master WHERE type='index' AND name=:name"),
            {"name": index_name},
        ).first()
        sql = str(sql_row[0] or "") if sql_row else ""
        unique_indexes.append({"name": index_name, "columns": list(column_names), "sql": sql})
        if column_names == legacy_columns:
            legacy_present = True
        if index_name == "uq_token_registry_rh_chain_contract_norm":
            current_present = True

    return {
        "checked": True,
        "ready": bool(current_present and not legacy_present),
        "dialect": "sqlite",
        "legacy_symbol_unique": bool(legacy_present),
        "contract_identity_unique": bool(current_present),
        "unique_indexes": unique_indexes,
    }


def require_token_registry_contract_identity_schema(db: Session) -> Dict[str, Any]:
    """Fail closed unless the operator-controlled contract-identity migration is complete."""
    state = token_registry_contract_identity_schema_state(db)
    if bool(state.get("ready")):
        return state
    raise RobinhoodChainRegistryAuthorityError(
        "robinhood_chain_registry_schema_migration_required",
        (
            "The Token Registry contract-identity schema is not ready. "
            "Run the operator-controlled Token Registry identity migration before using Registry runtime paths."
        ),
        legacy_symbol_unique=bool(state.get("legacy_symbol_unique")),
        contract_identity_unique=bool(state.get("contract_identity_unique")),
        dialect=state.get("dialect"),
    )


def ensure_token_registry_contract_identity_schema(db: Session) -> Dict[str, Any]:
    """Remove the legacy Robinhood Chain symbol-uniqueness boundary in SQLite.

    Existing UTT SQLite databases predate contract-qualified Robinhood Chain
    identity and can carry UNIQUE(chain, venue, symbol). SQLite create_all cannot
    remove that constraint. This bounded compatibility migration preserves every
    token_registry numeric ID so verification/objective/capability foreign keys
    remain stable, while replacing symbol uniqueness with a partial unique index
    on chain + normalized contract for Robinhood Chain ERC-20 rows.
    """
    bind = db.get_bind()
    dialect = str(getattr(getattr(bind, "dialect", None), "name", "") or "").lower()
    if dialect != "sqlite":
        return {"checked": False, "migrated": False, "dialect": dialect or None}

    indexes = db.execute(text("PRAGMA index_list('token_registry')")).mappings().all()
    legacy_columns = ("chain", "venue", "symbol")
    legacy_present = False
    current_present = False
    unique_indexes: List[Dict[str, Any]] = []
    for index in indexes or []:
        if not bool(index.get("unique")):
            continue
        index_name = str(index.get("name") or "")
        safe_name = index_name.replace("'", "''")
        columns = db.execute(text(f"PRAGMA index_info('{safe_name}')")).mappings().all()
        column_names = tuple(str(item.get("name") or "") for item in columns)
        sql_row = db.execute(
            text("SELECT sql FROM sqlite_master WHERE type='index' AND name=:name"),
            {"name": index_name},
        ).first()
        sql = str(sql_row[0] or "") if sql_row else ""
        unique_indexes.append({"name": index_name, "columns": list(column_names), "sql": sql})
        if column_names == legacy_columns:
            legacy_present = True
        if index_name == "uq_token_registry_rh_chain_contract_norm":
            current_present = True

    if current_present and not legacy_present:
        return {
            "checked": True,
            "migrated": False,
            "dialect": "sqlite",
            "legacy_symbol_unique": False,
            "contract_identity_unique": True,
        }

    if not legacy_present:
        return {
            "checked": True,
            "migrated": False,
            "dialect": "sqlite",
            "legacy_symbol_unique": False,
            "contract_identity_unique": current_present,
            "unique_indexes": unique_indexes,
        }

    duplicate_contracts = int(db.execute(text(
        """
        SELECT COUNT(*)
        FROM (
            SELECT LOWER(TRIM(address)) AS contract
            FROM token_registry
            WHERE LOWER(TRIM(chain)) = 'robinhood_chain'
              AND address IS NOT NULL
              AND TRIM(address) <> ''
            GROUP BY LOWER(TRIM(address))
            HAVING COUNT(*) > 1
        )
        """
    )).scalar_one() or 0)
    duplicate_native_scopes = int(db.execute(text(
        """
        SELECT COUNT(*)
        FROM (
            SELECT COALESCE(NULLIF(LOWER(TRIM(venue)), ''), '<global>') AS scope
            FROM token_registry
            WHERE LOWER(TRIM(chain)) = 'robinhood_chain'
              AND (address IS NULL OR TRIM(address) = '')
            GROUP BY COALESCE(NULLIF(LOWER(TRIM(venue)), ''), '<global>')
            HAVING COUNT(*) > 1
        )
        """
    )).scalar_one() or 0)
    if duplicate_contracts or duplicate_native_scopes:
        raise RobinhoodChainRegistryAuthorityError(
            "robinhood_chain_registry_identity_migration_unsafe",
            "The Token Registry contains duplicate contract or native identities and cannot be migrated automatically.",
            duplicate_contracts=duplicate_contracts,
            duplicate_native_scopes=duplicate_native_scopes,
        )

    column_rows = db.execute(text("PRAGMA table_info('token_registry')")).mappings().all()
    source_columns = {str(row.get("name") or "") for row in column_rows}
    source_row_count = int(db.execute(text("SELECT COUNT(*) FROM token_registry")).scalar_one() or 0)
    db.commit()

    temp_table = "token_registry_rhrg1_contract_identity"
    raw_connection = bind.raw_connection()
    cursor = raw_connection.cursor()
    initial_foreign_keys = 1
    try:
        cursor.execute("PRAGMA foreign_keys")
        row = cursor.fetchone()
        initial_foreign_keys = int(row[0] if row else 0)
        raw_connection.commit()
        cursor.execute("PRAGMA foreign_keys=OFF")
        cursor.execute("PRAGMA foreign_keys")
        disabled_row = cursor.fetchone()
        if int(disabled_row[0] if disabled_row else 1) != 0:
            raise RuntimeError("token_registry_identity_migration_foreign_keys_not_disabled")

        cursor.execute("BEGIN IMMEDIATE")
        cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
        cursor.execute(f"""
            CREATE TABLE {temp_table} (
                id INTEGER NOT NULL PRIMARY KEY,
                chain VARCHAR(16) NOT NULL,
                venue VARCHAR(32),
                symbol VARCHAR(32) NOT NULL,
                address VARCHAR(128),
                decimals INTEGER NOT NULL,
                label VARCHAR(64),
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                external_price_source TEXT,
                external_price_id TEXT
            )
        """)

        price_source_expr = "external_price_source" if "external_price_source" in source_columns else "NULL"
        price_id_expr = "external_price_id" if "external_price_id" in source_columns else "NULL"
        cursor.execute(f"""
            INSERT INTO {temp_table} (
                id, chain, venue, symbol, address, decimals, label,
                created_at, updated_at, external_price_source, external_price_id
            )
            SELECT
                id, chain, venue, symbol, address, decimals, label,
                created_at, updated_at, {price_source_expr}, {price_id_expr}
            FROM token_registry
        """)
        cursor.execute(f"SELECT COUNT(*) FROM {temp_table}")
        copied_row_count = int(cursor.fetchone()[0])
        if copied_row_count != source_row_count:
            raise RuntimeError("token_registry_identity_migration_copy_count_mismatch")

        cursor.execute("DROP TABLE token_registry")
        cursor.execute(f"ALTER TABLE {temp_table} RENAME TO token_registry")
        cursor.execute(
            "CREATE UNIQUE INDEX uq_token_registry_rh_chain_contract_norm "
            "ON token_registry (chain, LOWER(TRIM(address))) "
            "WHERE LOWER(TRIM(chain)) = 'robinhood_chain' "
            "AND address IS NOT NULL AND TRIM(address) <> ''"
        )
        cursor.execute(
            "CREATE INDEX ix_token_registry_chain_symbol ON token_registry (chain, symbol)"
        )
        cursor.execute(
            "CREATE INDEX ix_token_registry_chain_venue ON token_registry (chain, venue)"
        )
        cursor.execute(
            "CREATE INDEX ix_token_registry_chain_address ON token_registry (chain, address)"
        )

        cursor.execute("PRAGMA foreign_key_check")
        foreign_key_issues = list(cursor.fetchall() or [])
        if foreign_key_issues:
            raise RuntimeError(
                f"token_registry_identity_migration_foreign_key_check_failed:{len(foreign_key_issues)}"
            )
        raw_connection.commit()
    except Exception:
        try:
            raw_connection.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            cursor.execute(f"PRAGMA foreign_keys={'ON' if initial_foreign_keys else 'OFF'}")
            raw_connection.commit()
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            raw_connection.close()

    db.expire_all()
    return {
        "checked": True,
        "migrated": True,
        "dialect": "sqlite",
        "legacy_symbol_unique": True,
        "contract_identity_unique": True,
        "preserved_row_count": source_row_count,
        "preserved_ids": True,
        "foreign_keys_restored": bool(initial_foreign_keys),
    }


def assert_erc20_contract_write_unambiguous(
    db: Session,
    *,
    address: Any,
    exclude_token_id: Optional[int] = None,
) -> None:
    try:
        normalized_address = validate_evm_address(str(address or "").strip()).lower()
    except ValueError as exc:
        raise RobinhoodChainRegistryAuthorityError(
            "invalid_robinhood_chain_contract_address",
            str(exc),
            address=address,
        ) from exc

    query = db.query(TokenRegistry).filter(
        TokenRegistry.chain == ROBINHOOD_CHAIN,
        text("LOWER(TRIM(address)) = :normalized_contract"),
    ).params(normalized_contract=normalized_address)
    if exclude_token_id is not None:
        query = query.filter(TokenRegistry.id != int(exclude_token_id))
    row = query.first()
    if row is not None:
        raise RobinhoodChainRegistryAuthorityError(
            "duplicate_robinhood_chain_contract_registry_identity",
            "The Robinhood Chain ERC-20 contract already has a Token Registry identity.",
            address=normalized_address,
            conflicting_registry_id=int(row.id),
            conflicting_symbol=normalize_registry_symbol(row.symbol),
        )


def effective_row_by_contract(
    db: Session,
    address: Any,
    *,
    venue: str = ROBINHOOD_CHAIN_VENUE,
) -> TokenRegistry:
    require_token_registry_contract_identity_schema(db)
    try:
        normalized_address = validate_evm_address(str(address or "").strip()).lower()
    except ValueError as exc:
        raise RobinhoodChainRegistryAuthorityError(
            "invalid_robinhood_chain_contract_address",
            str(exc),
            address=address,
        ) from exc

    matches = [
        row
        for row in select_effective_registry_rows(db, venue=venue)
        if not row_is_native(row)
        and str(getattr(row, "address", None) or "").strip().lower() == normalized_address
    ]
    if not matches:
        raise RobinhoodChainRegistryAuthorityError(
            "robinhood_chain_registry_token_not_found",
            "The requested Robinhood Chain contract is not present in the effective Token Registry view.",
            address=normalized_address,
            venue=normalize_registry_venue(venue),
        )
    if len(matches) > 1:
        raise RobinhoodChainRegistryAuthorityError(
            "ambiguous_robinhood_chain_registry_contract",
            "More than one effective Robinhood Chain Token Registry row resolves to the same contract.",
            address=normalized_address,
            registry_ids=[int(row.id) for row in matches],
        )
    return matches[0]


def select_effective_registry_rows(
    db: Session,
    *,
    venue: str = ROBINHOOD_CHAIN_VENUE,
    limit: int = 250,
) -> List[TokenRegistry]:
    require_token_registry_contract_identity_schema(db)
    normalized_venue = normalize_registry_venue(venue) or ROBINHOOD_CHAIN_VENUE
    bounded_limit = max(1, min(int(limit), 1000))
    overrides = (
        db.query(TokenRegistry)
        .filter(
            TokenRegistry.chain == ROBINHOOD_CHAIN,
            TokenRegistry.venue == normalized_venue,
        )
        .order_by(TokenRegistry.symbol.asc(), TokenRegistry.id.asc())
        .limit(bounded_limit)
        .all()
    )
    globals_ = (
        db.query(TokenRegistry)
        .filter(
            TokenRegistry.chain == ROBINHOOD_CHAIN,
            ((TokenRegistry.venue.is_(None)) | (TokenRegistry.venue == "")),
        )
        .order_by(TokenRegistry.symbol.asc(), TokenRegistry.id.asc())
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

    # Preserve the established venue-override contract before applying the new
    # duplicate-symbol rules: a venue-scoped row shadows global rows with the
    # same symbol. Duplicate symbols remain visible only when they coexist in
    # the same effective scope (for example, two global contracts with one
    # ticker, or two venue-scoped contracts with one ticker). Symbol-only
    # resolvers can then fail closed without treating a legitimate venue
    # override as ambiguous.
    override_symbols = {
        normalize_registry_symbol(getattr(row, "symbol", None))
        for row in overrides or []
    }

    rows: List[TokenRegistry] = [*(overrides or [])]
    for row in globals_ or []:
        symbol = normalize_registry_symbol(getattr(row, "symbol", None))
        if symbol in override_symbols:
            continue
        rows.append(row)

    # Native identity retains the pre-RHRG1 semantics: a same-symbol venue
    # native row shadows its global row through the rule above, but different
    # global/venue native symbols remain simultaneously effective and are
    # therefore rejected as ambiguous.
    assert_unambiguous_effective_native_rows(rows)

    # Contract identity is canonical for ERC-20s. The database migration adds
    # the normalized-contract unique index, but keep the runtime guard so a
    # malformed/non-SQLite database still fails closed.
    seen_contracts: Dict[str, TokenRegistry] = {}
    for row in rows:
        if row_is_native(row):
            continue
        try:
            contract = validate_evm_address(
                str(getattr(row, "address", None) or "").strip()
            ).lower()
        except ValueError as exc:
            raise RobinhoodChainRegistryAuthorityError(
                "invalid_robinhood_chain_contract_address",
                str(exc),
                registry_id=int(row.id),
                symbol=normalize_registry_symbol(row.symbol),
            ) from exc
        if contract in seen_contracts:
            other = seen_contracts[contract]
            raise RobinhoodChainRegistryAuthorityError(
                "ambiguous_robinhood_chain_registry_contract",
                "More than one effective Robinhood Chain Token Registry row resolves to the same contract.",
                address=contract,
                registry_ids=[int(other.id), int(row.id)],
            )
        seen_contracts[contract] = row

    rows.sort(
        key=lambda row: (
            normalize_registry_symbol(row.symbol),
            0 if normalize_registry_venue(getattr(row, "venue", None)) == normalized_venue else 1,
            str(getattr(row, "address", None) or "").strip().lower(),
            int(row.id),
        )
    )
    return rows[:bounded_limit]

def effective_row_by_symbol(
    db: Session,
    symbol: Any,
    *,
    venue: str = ROBINHOOD_CHAIN_VENUE,
) -> TokenRegistry:
    normalized_symbol = normalize_registry_symbol(symbol)
    matches = [
        row
        for row in select_effective_registry_rows(db, venue=venue)
        if normalize_registry_symbol(row.symbol) == normalized_symbol
    ]
    if not matches:
        raise RobinhoodChainRegistryAuthorityError(
            "robinhood_chain_registry_token_not_found",
            "The requested Robinhood Chain token is not present in the effective Token Registry view.",
            symbol=normalized_symbol,
            venue=normalize_registry_venue(venue),
        )
    if len(matches) > 1:
        raise RobinhoodChainRegistryAuthorityError(
            "ambiguous_robinhood_chain_registry_symbol",
            "The Robinhood Chain symbol resolves to more than one effective Token Registry identity; use an exact Registry ID or contract.",
            symbol=normalized_symbol,
            venue=normalize_registry_venue(venue),
            registry_ids=[int(row.id) for row in matches],
            contract_addresses=[str(getattr(row, "address", None) or "").strip().lower() or None for row in matches],
        )
    return matches[0]

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
    objective_id: Any = None,
    expected_input_contract_address: Any = None,
    expected_output_contract_address: Any = None,
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

    objective_key = str(objective_id or "").strip()
    if objective_key:
        exact_row = (
            db.query(RobinhoodChainPairObjective)
            .filter(RobinhoodChainPairObjective.id == objective_key)
            .first()
        )
        if exact_row is None:
            candidates: List[RobinhoodChainPairObjective] = []
        elif str(exact_row.symbol or "").strip().upper() != normalized_symbol:
            raise RobinhoodChainRegistryAuthorityError(
                "robinhood_chain_execution_objective_symbol_mismatch",
                "The selected pair-objective ID does not match the requested market symbol.",
                symbol=normalized_symbol,
                objective_id=objective_key,
                objective_symbol=str(exact_row.symbol or "").strip().upper(),
                provider_contacted=False,
            )
        else:
            candidates = [exact_row]
    else:
        candidates = (
            db.query(RobinhoodChainPairObjective)
            .filter(
                RobinhoodChainPairObjective.symbol == normalized_symbol,
                RobinhoodChainPairObjective.enabled.is_(True),
            )
            .order_by(RobinhoodChainPairObjective.id.asc())
            .all()
        )

    if not candidates:
        raise RobinhoodChainRegistryAuthorityError(
            "robinhood_chain_execution_objective_not_found",
            "The requested market is not present in the enabled Robinhood Chain pair-objective database.",
            symbol=normalized_symbol,
            objective_id=objective_key or None,
            provider_contacted=False,
        )

    expected_input_contract = str(expected_input_contract_address or "").strip().lower()
    expected_output_contract = str(expected_output_contract_address or "").strip().lower()
    if expected_input_contract and expected_output_contract:
        exact_candidates: List[RobinhoodChainPairObjective] = []
        for candidate in candidates:
            candidate_token_ids = {
                int(candidate.base_token_registry_id),
                int(candidate.quote_token_registry_id),
            }
            candidate_rows = (
                db.query(TokenRegistry)
                .filter(TokenRegistry.id.in_(candidate_token_ids))
                .all()
            )
            candidate_by_id = {int(row.id): row for row in candidate_rows}
            candidate_base = candidate_by_id.get(int(candidate.base_token_registry_id))
            candidate_quote = candidate_by_id.get(int(candidate.quote_token_registry_id))
            if candidate_base is None or candidate_quote is None:
                continue
            candidate_base_identity = _execution_identity(candidate_base)
            candidate_quote_identity = _execution_identity(candidate_quote)
            candidate_input = candidate_base_identity if normalized_side == "sell" else candidate_quote_identity
            candidate_output = candidate_quote_identity if normalized_side == "sell" else candidate_base_identity
            if (
                str(candidate_input.get("contract_address") or "").strip().lower() == expected_input_contract
                and str(candidate_output.get("contract_address") or "").strip().lower() == expected_output_contract
            ):
                exact_candidates.append(candidate)
        candidates = exact_candidates
        if not candidates:
            raise RobinhoodChainRegistryAuthorityError(
                "robinhood_chain_execution_objective_identity_mismatch",
                "No same-symbol pair objective matches the persisted exact input/output contract identities.",
                symbol=normalized_symbol,
                expected_input_contract_address=expected_input_contract,
                expected_output_contract_address=expected_output_contract,
                provider_contacted=False,
            )

    if len(candidates) > 1:
        raise RobinhoodChainRegistryAuthorityError(
            "robinhood_chain_execution_objective_ambiguous",
            "More than one pair objective uses this market symbol. Select one exact objective ID before review or execution.",
            symbol=normalized_symbol,
            objective_ids=[str(row.id) for row in candidates],
            base_token_registry_ids=[int(row.base_token_registry_id) for row in candidates],
            quote_token_registry_ids=[int(row.quote_token_registry_id) for row in candidates],
            provider_contacted=False,
        )

    objective = candidates[0]

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
            "base_token_registry_id": int(objective.base_token_registry_id),
            "quote_token_registry_id": int(objective.quote_token_registry_id),
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
