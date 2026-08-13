from __future__ import annotations

import copy
import re
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from ..models import (
    RobinhoodChainBuyExecution,
    RobinhoodChainExecution,
    RobinhoodChainPairCapability,
    RobinhoodChainPairObjective,
    RobinhoodChainRegistryVerification,
    RobinhoodChainSwapExecution,
    TokenRegistry,
)
from .evm_rpc import decode_abi_uint256, get_robinhood_chain_client, validate_evm_address
from .robinhood_chain_execution_discovery import (
    ZEROX_NATIVE_TOKEN,
    get_robinhood_chain_execution_discovery_service,
)
from .robinhood_chain_registry_authority import (
    ROBINHOOD_CHAIN,
    ROBINHOOD_CHAIN_VENUE,
    RobinhoodChainRegistryAuthorityError,
    effective_native_row,
    effective_row_by_symbol,
    identity_fields_from_row,
    resolve_robinhood_chain_execution_authority,
    select_effective_registry_rows,
)
from .robinhood_chain_transaction_planning import (
    ROBINHOOD_CHAIN_ALLOWANCE_HOLDER_ALLOWLIST,
    get_robinhood_chain_transaction_planning_service,
)
from .robinhood_chain_uniswap_quote import (
    UNISWAP_PROVIDER,
    get_robinhood_chain_uniswap_quote_service,
)
from .robinhood_chain_uniswap_v3_quote import (
    UNISWAP_V3_RPC_PROVIDER,
    get_robinhood_chain_uniswap_v3_quote_service,
)


ROBINHOOD_CHAIN_ID = 4663
AMOUNT_MODE_EXACT_INPUT = "exact_input"
MECHANISM_SWAP = "swap"
MECHANISM_WRAP_UNWRAP = "wrap_unwrap"
PROVIDER_ZEROX = "0x"
PROVIDER_UNISWAP = UNISWAP_PROVIDER
PROVIDER_UNISWAP_V3_RPC = UNISWAP_V3_RPC_PROVIDER
PROVIDER_NATIVE_WRAP = "native_wrap"
PREPARATION_STATUS = "preparation_verified"
INDICATIVE_AVAILABLE_STATUSES = {"available", "live_verified"}
INDICATIVE_UNAVAILABLE_PRIORITY = (
    "identity_invalid",
    "legal_restriction",
    "provider_authentication_failed",
    "provider_not_configured",
    "backoff_active",
    "provider_transient_error",
    "unsupported",
    "no_liquidity",
    "provider_error",
    "not_yet_probed",
    "not_tested",
)
LIVE_AUTHORIZED_PENDING_CONFIRMATION = "live_authorized_pending_confirmation"
R5C5A_AUTHORIZATION_TTL_MINUTES = 60
R5C5B_AUTHORIZATION_TTL_MINUTES = 60
R5C4A_SYMBOL = "WETH-USDG"
R5C4A_SIDE = "buy"
R5C4A_INPUT_ASSET = "USDG"
R5C4A_OUTPUT_ASSET = "WETH"
R5C4A_INPUT_AMOUNT = "1"
R5C4A_SLIPPAGE_BPS = 100
R5C4B_SYMBOL = "WETH-USDG"
R5C4B_SIDE = "sell"
R5C4B_INPUT_ASSET = "WETH"
R5C4B_OUTPUT_ASSET = "USDG"
R5C4B_INPUT_AMOUNT = "0.0001"
R5C4B_SLIPPAGE_BPS = 100

_ERC20_SYMBOL_SELECTOR = "0x95d89b41"
_ERC20_NAME_SELECTOR = "0x06fdde03"
_ERC20_DECIMALS_SELECTOR = "0x313ce567"
_DECIMAL_RE = re.compile(r"^(?:0|[1-9]\d*)(?:\.\d+)?$")


def _normalize_market_symbol(value: Any) -> str:
    raw = str(value or "").strip().upper().replace("/", "-").replace("_", "-")
    parts = [part.strip() for part in raw.split("-") if part.strip()]
    if len(parts) != 2:
        raise ValueError("invalid_robinhood_chain_market_symbol")
    return f"{parts[0]}-{parts[1]}"


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def iso_or_none(value: Optional[datetime]) -> Optional[str]:
    return value.replace(tzinfo=timezone.utc).isoformat() if value is not None else None


def _clean_text(value: Any, max_length: int = 512) -> Optional[str]:
    text_value = str(value or "").strip()
    return text_value[:max_length] if text_value else None


def _json_safe_error(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        safe: Dict[str, Any] = {}
        for key in (
            "error",
            "message",
            "name",
            "reason",
            "code",
            "http_status",
            "retry_after",
            "backoff_until",
            "provider_error",
        ):
            if value.get(key) is not None:
                safe[key] = copy.deepcopy(value.get(key))
        if safe:
            return safe
    text_value = _clean_text(value, 1000)
    return {"message": text_value} if text_value else {}


def _provider_error_text(value: Any, *, max_length: int = 4000) -> str:
    parts: List[str] = []

    def collect(item: Any) -> None:
        if len(" ".join(parts)) >= max_length:
            return
        if isinstance(item, dict):
            for key, nested in item.items():
                parts.append(str(key))
                collect(nested)
            return
        if isinstance(item, (list, tuple, set)):
            for nested in item:
                collect(nested)
            return
        text_value = str(item or "").strip()
        if text_value:
            parts.append(text_value)

    collect(value)
    return " ".join(parts)[:max_length].lower()


def _probe_provider_contacted(result: Dict[str, Any]) -> bool:
    if result.get("provider_contacted") is not None:
        return bool(result.get("provider_contacted"))
    error = str(result.get("error") or "").strip().lower()
    return error in {
        "execution_discovery_provider_transient_error",
        "provider_authentication_failed",
        "execution_discovery_provider_error",
    }


def _classify_probe_result(result: Dict[str, Any]) -> str:
    if result.get("ok") is True:
        return "available" if result.get("liquidity_available") is True else "no_liquidity"

    error = str(result.get("error") or "").strip().lower()
    exact = {
        "execution_discovery_not_configured": "provider_not_configured",
        "uniswap_quote_not_configured": "provider_not_configured",
        "uniswap_quote_api_base_invalid": "provider_not_configured",
        "execution_discovery_backoff_active": "backoff_active",
        "execution_discovery_provider_transient_error": "provider_transient_error",
        "uniswap_quote_provider_transient_error": "provider_transient_error",
        "provider_transient_error": "provider_transient_error",
        "provider_authentication_failed": "provider_authentication_failed",
        "uniswap_quote_authentication_failed": "provider_authentication_failed",
        "unsupported_discovery_pair": "unsupported",
        "uniswap_quote_routing_not_allowed": "unsupported",
        "uniswap_v3_pool_not_found": "no_liquidity",
        "uniswap_v3_no_quotable_route": "no_liquidity",
        "uniswap_v3_same_token_pair": "unsupported",
        "uniswap_v3_rpc_requires_wrapped_native_identity": "identity_invalid",
        "invalid_uniswap_v3_registry_identity": "identity_invalid",
        "invalid_registry_token_identity": "identity_invalid",
        "execution_discovery_provider_identity_mismatch": "identity_invalid",
        "chain_id_mismatch_or_unavailable": "identity_invalid",
        "contract_code_unavailable": "identity_invalid",
        "pair_discovery_requires_verified_registry_identity": "identity_invalid",
        "not_yet_probed": "not_yet_probed",
    }
    if error in exact:
        return exact[error]

    detail = _provider_error_text(result)
    legal_restriction_terms = (
        "buy_token_not_authorized_for_trade",
        "sell_token_not_authorized_for_trade",
        "token_not_authorized_for_trade",
        "not authorized for trade due to legal restrictions",
        "not authorized for trade",
        "legal restriction",
        "legal restrictions",
    )
    unsupported_terms = (
        "unsupported",
        "not supported",
        "token_not_tradable",
        "token not tradable",
        "invalid token pair",
        "pair not supported",
    )
    no_liquidity_terms = (
        "insufficient_asset_liquidity",
        "insufficient asset liquidity",
        "no liquidity",
        "liquidity unavailable",
        "no route",
        "no routes",
        "no quote",
        "cannot find a route",
    )
    if any(term in detail for term in legal_restriction_terms):
        return "legal_restriction"
    if any(term in detail for term in unsupported_terms):
        return "unsupported"
    if any(term in detail for term in no_liquidity_terms):
        return "no_liquidity"
    return "provider_error"


def _decode_abi_string(value: Any) -> Optional[str]:
    raw = str(value or "").strip()
    if not raw.startswith("0x"):
        return None
    body = raw[2:]
    if not body or len(body) % 2 != 0 or not re.fullmatch(r"[0-9a-fA-F]+", body):
        return None
    data = bytes.fromhex(body)

    # Standard ABI dynamic string: offset -> length -> bytes.
    if len(data) >= 64:
        offset = int.from_bytes(data[:32], "big")
        if offset + 32 <= len(data):
            length = int.from_bytes(data[offset : offset + 32], "big")
            start = offset + 32
            end = start + length
            if 0 <= length <= 512 and end <= len(data):
                try:
                    decoded = data[start:end].decode("utf-8", errors="strict").strip("\x00").strip()
                    return decoded or None
                except Exception:
                    pass

    # Some legacy ERC-20s return bytes32 for symbol/name.
    if len(data) >= 32:
        try:
            decoded = data[:32].rstrip(b"\x00").decode("utf-8", errors="strict").strip()
            return decoded or None
        except Exception:
            return None
    return None


def _parse_probe_amount(value: Any, decimals: int) -> str:
    text_value = str(value or "").strip()
    if not text_value or not _DECIMAL_RE.fullmatch(text_value):
        raise ValueError("invalid_probe_amount")
    try:
        amount = Decimal(text_value)
    except InvalidOperation as exc:
        raise ValueError("invalid_probe_amount") from exc
    if not amount.is_finite() or amount <= 0:
        raise ValueError("invalid_probe_amount")
    if max(0, -amount.as_tuple().exponent) > int(decimals):
        raise ValueError("probe_amount_exceeds_token_precision")
    # Review-only discovery is intentionally bounded to 25 display units per request.
    if amount > Decimal("25"):
        raise ValueError("probe_amount_exceeds_review_cap")
    normalized = format(amount, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    return normalized


def _registry_external_price_meta(db: Session, token_id: int) -> Dict[str, Optional[str]]:
    try:
        row = db.execute(
            text(
                """
                SELECT external_price_source, external_price_id
                FROM token_registry
                WHERE id = :id
                """
            ),
            {"id": int(token_id)},
        ).mappings().first()
        if not row:
            return {"external_price_source": None, "external_price_id": None}
        return {
            "external_price_source": _clean_text(row.get("external_price_source"), 64),
            "external_price_id": _clean_text(row.get("external_price_id"), 128),
        }
    except Exception:
        return {"external_price_source": None, "external_price_id": None}


def _select_registry_rows(db: Session) -> List[TokenRegistry]:
    return select_effective_registry_rows(
        db,
        venue=ROBINHOOD_CHAIN_VENUE,
        limit=250,
    )


def _provider_exception_result(provider: str, exc: Exception) -> Dict[str, Any]:
    return {
        "ok": False,
        "error": "selected_market_provider_exception",
        "provider": str(provider or "").strip().lower(),
        "provider_contacted": False,
        "liquidity_available": False,
        "provider_error": {
            "name": type(exc).__name__,
            "message": _clean_text(exc, 1000),
        },
        "read_only": True,
        "transaction_constructed": False,
        "will_mutate": False,
    }


def _safe_http_status(value: Any) -> Optional[int]:
    try:
        status = int(value)
    except (TypeError, ValueError):
        return None
    return status if 100 <= status <= 599 else None


def _ensure_provider_scoped_capability_schema(db: Session) -> Dict[str, Any]:
    """Repair the legacy SQLite uniqueness boundary that omitted provider.

    Early RH-REG databases could contain a unique constraint on objective,
    direction, and amount mode without the provider column. The model now
    requires provider-scoped evidence, but SQLite create_all cannot alter an
    existing unique constraint. This bounded compatibility migration preserves
    every row and rebuilds only this local review-evidence table.
    """
    bind = db.get_bind()
    dialect = str(getattr(getattr(bind, "dialect", None), "name", "") or "").lower()
    if dialect != "sqlite":
        return {"checked": False, "migrated": False, "dialect": dialect or None}

    db.commit()
    indexes = db.execute(text("PRAGMA index_list('robinhood_chain_pair_capabilities')")).mappings().all()
    unique_column_sets: List[Tuple[str, ...]] = []
    for index in indexes:
        if not bool(index.get("unique")):
            continue
        index_name = str(index.get("name") or "").replace("'", "''")
        columns = db.execute(text(f"PRAGMA index_info('{index_name}')")).mappings().all()
        unique_column_sets.append(tuple(str(item.get("name") or "") for item in columns))

    current_columns = (
        "objective_id",
        "from_token_registry_id",
        "to_token_registry_id",
        "amount_mode",
        "provider",
    )
    legacy_columns = current_columns[:-1]
    if current_columns in unique_column_sets:
        return {"checked": True, "migrated": False, "dialect": "sqlite"}
    if legacy_columns not in unique_column_sets:
        # No incompatible unique boundary was found. A later insert can still
        # surface any unrelated schema defect through the sanitized endpoint.
        return {
            "checked": True,
            "migrated": False,
            "dialect": "sqlite",
            "unique_columns": [list(item) for item in unique_column_sets],
        }

    temp_table = "robinhood_chain_pair_capabilities_r5c5d2er1"
    try:
        source_row_count = int(
            db.execute(text("SELECT COUNT(*) FROM robinhood_chain_pair_capabilities")).scalar_one()
        )
        db.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))
        db.execute(text(f"""
            CREATE TABLE {temp_table} (
                id VARCHAR(36) NOT NULL PRIMARY KEY,
                objective_id VARCHAR(36) NOT NULL,
                from_token_registry_id INTEGER NOT NULL,
                to_token_registry_id INTEGER NOT NULL,
                amount_mode VARCHAR(24) NOT NULL,
                provider VARCHAR(32) NOT NULL,
                indicative_status VARCHAR(32) NOT NULL,
                firm_plan_status VARCHAR(32) NOT NULL,
                execution_status VARCHAR(32) NOT NULL,
                enabled BOOLEAN NOT NULL,
                route_sources JSON,
                probe_amount VARCHAR(80),
                price_impact_bps FLOAT,
                provider_error JSON,
                backoff_until DATETIME,
                evidence JSON,
                last_verified_at DATETIME,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                CONSTRAINT uq_rh_chain_pair_capability_direction
                    UNIQUE (objective_id, from_token_registry_id, to_token_registry_id, amount_mode, provider),
                FOREIGN KEY(objective_id) REFERENCES robinhood_chain_pair_objectives (id) ON DELETE CASCADE,
                FOREIGN KEY(from_token_registry_id) REFERENCES token_registry (id) ON DELETE CASCADE,
                FOREIGN KEY(to_token_registry_id) REFERENCES token_registry (id) ON DELETE CASCADE
            )
        """))
        db.execute(text(f"""
            INSERT INTO {temp_table} (
                id, objective_id, from_token_registry_id, to_token_registry_id,
                amount_mode, provider, indicative_status, firm_plan_status,
                execution_status, enabled, route_sources, probe_amount,
                price_impact_bps, provider_error, backoff_until, evidence,
                last_verified_at, created_at, updated_at
            )
            SELECT
                id, objective_id, from_token_registry_id, to_token_registry_id,
                amount_mode, COALESCE(NULLIF(provider, ''), '0x'),
                indicative_status, firm_plan_status, execution_status, enabled,
                route_sources, probe_amount, price_impact_bps, provider_error,
                backoff_until, evidence, last_verified_at, created_at, updated_at
            FROM robinhood_chain_pair_capabilities
        """))
        copied_row_count = int(
            db.execute(text(f"SELECT COUNT(*) FROM {temp_table}")).scalar_one()
        )
        if copied_row_count != source_row_count:
            raise RuntimeError("provider_scoped_capability_schema_copy_count_mismatch")
        db.execute(text("DROP TABLE robinhood_chain_pair_capabilities"))
        db.execute(text(
            f"ALTER TABLE {temp_table} RENAME TO robinhood_chain_pair_capabilities"
        ))
        db.execute(text(
            "CREATE INDEX ix_robinhood_chain_pair_capabilities_objective_id "
            "ON robinhood_chain_pair_capabilities (objective_id)"
        ))
        db.execute(text(
            "CREATE INDEX ix_robinhood_chain_pair_capabilities_from_token_registry_id "
            "ON robinhood_chain_pair_capabilities (from_token_registry_id)"
        ))
        db.execute(text(
            "CREATE INDEX ix_robinhood_chain_pair_capabilities_to_token_registry_id "
            "ON robinhood_chain_pair_capabilities (to_token_registry_id)"
        ))
        db.execute(text(
            "CREATE INDEX ix_rh_chain_pair_capability_status "
            "ON robinhood_chain_pair_capabilities (indicative_status, last_verified_at)"
        ))
        db.execute(text(
            "CREATE INDEX ix_rh_chain_pair_capability_enabled "
            "ON robinhood_chain_pair_capabilities (enabled, execution_status)"
        ))
        db.commit()
    except Exception:
        db.rollback()
        raise
    return {
        "checked": True,
        "migrated": True,
        "dialect": "sqlite",
        "legacy_unique_columns": list(legacy_columns),
        "current_unique_columns": list(current_columns),
        "preserved_row_count": source_row_count,
    }


class RobinhoodChainRegistryDiscoveryService:
    """TokenRegistry-backed, review-only Robinhood Chain discovery service.

    The service may write local identity/objective/capability evidence. It never
    signs, broadcasts, constructs an executable transaction, changes an
    allowance, or enables execution automatically.
    """

    def __init__(
        self,
        *,
        rpc_client: Any = None,
        discovery_service: Any = None,
        planning_service: Any = None,
        uniswap_service: Any = None,
        uniswap_v3_service: Any = None,
    ) -> None:
        self.rpc_client = rpc_client or get_robinhood_chain_client()
        self.discovery_service = discovery_service or get_robinhood_chain_execution_discovery_service()
        self.planning_service = planning_service or get_robinhood_chain_transaction_planning_service()
        self.uniswap_service = uniswap_service or get_robinhood_chain_uniswap_quote_service()
        self.uniswap_v3_service = uniswap_v3_service or get_robinhood_chain_uniswap_v3_quote_service()

    def status(self, db: Session) -> Dict[str, Any]:
        try:
            native_identity = self.native_identity(db)
            native_identity_error = None
        except ValueError as exc:
            native_identity = None
            native_identity_error = str(exc)
        return {
            "ok": True,
            "tranche": "RH-CHAIN.10D.2-R5C.1",
            "chain": ROBINHOOD_CHAIN,
            "chain_id": ROBINHOOD_CHAIN_ID,
            "token_registry_authority": True,
            "native_identity": native_identity,
            "native_identity_ready": native_identity_error is None,
            "native_identity_error": native_identity_error,
            "hardcoded_native_symbol": False,
            "hardcoded_native_decimals": False,
            "hardcoded_token_contracts": False,
            "hardcoded_pair_contracts": False,
            "asset_verification_count": db.query(RobinhoodChainRegistryVerification).count(),
            "objective_count": db.query(RobinhoodChainPairObjective).count(),
            "capability_count": db.query(RobinhoodChainPairCapability).count(),
            "supported_mechanisms": [MECHANISM_SWAP, MECHANISM_WRAP_UNWRAP],
            "supported_amount_modes": [AMOUNT_MODE_EXACT_INPUT],
            "review_providers": [PROVIDER_ZEROX, PROVIDER_UNISWAP, PROVIDER_UNISWAP_V3_RPC],
            "selected_pair_provider_probing": True,
            "database_writes_require_confirmation": True,
            "blockchain_read_only": True,
            "execution_enabled": False,
            "signing_enabled": False,
            "broadcast_enabled": False,
            "automatic_execution_promotion": False,
            "generic_live_venues_required": False,
            "ledger_mutation_enabled": False,
            "fifo_mutation_enabled": False,
            "basis_mutation_enabled": False,
            "will_mutate_chain": False,
        }

    def registry_rows(self, db: Session) -> List[TokenRegistry]:
        return _select_registry_rows(db)

    def _registry_row_by_id(self, db: Session, token_registry_id: int) -> TokenRegistry:
        row = (
            db.query(TokenRegistry)
            .filter(
                TokenRegistry.id == int(token_registry_id),
                TokenRegistry.chain == ROBINHOOD_CHAIN,
            )
            .first()
        )
        if row is None:
            raise ValueError("robinhood_chain_registry_token_not_found")
        return row

    def _registry_row_by_symbol(self, db: Session, symbol: str) -> TokenRegistry:
        try:
            return effective_row_by_symbol(
                db,
                symbol,
                venue=ROBINHOOD_CHAIN_VENUE,
            )
        except RobinhoodChainRegistryAuthorityError as exc:
            raise ValueError(exc.code) from exc

    def token_identity(self, db: Session, row: TokenRegistry) -> Dict[str, Any]:
        try:
            identity_fields = identity_fields_from_row(row)
        except RobinhoodChainRegistryAuthorityError as exc:
            raise ValueError(exc.code) from exc

        symbol = str(identity_fields["symbol"])
        decimals = int(identity_fields["decimals"])
        native = bool(identity_fields["native"])
        registry_contract_address = identity_fields["address"]
        contract_address = ZEROX_NATIVE_TOKEN if native else registry_contract_address
        asset_kind = str(identity_fields["asset_kind"])

        price_meta = _registry_external_price_meta(db, int(row.id))
        return {
            "registry_id": int(row.id),
            "registry_venue": row.venue,
            "registry_status": "registered",
            "identity_source": "token_registry",
            "symbol": symbol,
            "label": row.label,
            "contract_address": contract_address,
            "registry_contract_address": registry_contract_address,
            "decimals": decimals,
            "native": native,
            "asset_kind": asset_kind,
            **price_meta,
        }

    def resolve_token(self, db: Session, symbol: str) -> Dict[str, Any]:
        return self.token_identity(db, self._registry_row_by_symbol(db, symbol))

    def resolve_verified_token(self, db: Session, symbol: str) -> Dict[str, Any]:
        row = self._registry_row_by_symbol(db, symbol)
        self._verified_identity_required(db, int(row.id))
        return self.token_identity(db, row)

    def native_identity(self, db: Session) -> Dict[str, Any]:
        try:
            row = effective_native_row(db, venue=ROBINHOOD_CHAIN_VENUE)
        except RobinhoodChainRegistryAuthorityError as exc:
            raise ValueError(exc.code) from exc
        return self.token_identity(db, row)

    def _verification_row(self, db: Session, token_registry_id: int) -> Optional[RobinhoodChainRegistryVerification]:
        return (
            db.query(RobinhoodChainRegistryVerification)
            .filter(RobinhoodChainRegistryVerification.token_registry_id == int(token_registry_id))
            .first()
        )

    def _verification_dict(
        self,
        row: Optional[RobinhoodChainRegistryVerification],
    ) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        return {
            "id": row.id,
            "token_registry_id": int(row.token_registry_id),
            "chain_id": int(row.chain_id),
            "asset_kind": row.asset_kind,
            "code_present": row.code_present,
            "onchain_symbol": row.onchain_symbol,
            "onchain_name": row.onchain_name,
            "onchain_decimals": row.onchain_decimals,
            "registry_match": bool(row.registry_match),
            "canonical_status": row.canonical_status,
            "verification_error": row.verification_error,
            "evidence": copy.deepcopy(row.evidence) if isinstance(row.evidence, dict) else {},
            "verified_at": iso_or_none(row.verified_at),
            "updated_at": iso_or_none(row.updated_at),
        }

    def _verification_matches_current_registry_identity(
        self,
        verification: Optional[RobinhoodChainRegistryVerification],
        identity: Dict[str, Any],
    ) -> bool:
        if verification is None:
            return False
        evidence = verification.evidence if isinstance(verification.evidence, dict) else {}
        try:
            evidence_decimals = int(evidence.get("registry_decimals"))
            current_decimals = int(identity.get("decimals"))
        except Exception:
            return False
        evidence_symbol = str(evidence.get("registry_symbol") or "").strip().upper()
        current_symbol = str(identity.get("symbol") or "").strip().upper()
        evidence_address = str(evidence.get("registry_contract_address") or "").strip().lower()
        current_address = str(identity.get("registry_contract_address") or "").strip().lower()
        current_kind = str(identity.get("asset_kind") or "").strip().lower()
        return bool(
            int(verification.chain_id) == ROBINHOOD_CHAIN_ID
            and str(verification.asset_kind or "").strip().lower() == current_kind
            and evidence_symbol
            and evidence_symbol == current_symbol
            and evidence_decimals == current_decimals
            and evidence_address == current_address
        )

    def assets(self, db: Session) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for row in self.registry_rows(db):
            try:
                identity = self.token_identity(db, row)
                identity_error = None
            except Exception as exc:
                identity = {
                    "registry_id": int(row.id),
                    "registry_venue": row.venue,
                    "symbol": str(row.symbol or "").strip().upper(),
                    "label": row.label,
                    "registry_contract_address": row.address,
                    "decimals": row.decimals,
                }
                identity_error = str(exc)
            identity["identity_error"] = identity_error
            verification_row = self._verification_row(db, int(row.id))
            verification = self._verification_dict(verification_row)
            if (
                identity_error is None
                and verification is not None
                and str(verification.get("canonical_status") or "").strip().lower() == "verified"
                and not self._verification_matches_current_registry_identity(verification_row, identity)
            ):
                verification["canonical_status"] = "registry_changed_since_verification"
                verification["registry_match"] = False
                verification["verification_error"] = "registry_changed_since_verification"
            identity["verification"] = verification
            out.append(identity)
        return out

    async def verify_asset(
        self,
        db: Session,
        *,
        token_registry_id: int,
        force_refresh: bool,
        confirm_verify: bool,
    ) -> Dict[str, Any]:
        if confirm_verify is not True:
            raise ValueError("confirm_registry_verification_required")
        registry_row = self._registry_row_by_id(db, token_registry_id)
        identity = self.token_identity(db, registry_row)
        verified_at = utc_now()
        evidence: Dict[str, Any] = {
            "registry_symbol": identity["symbol"],
            "registry_decimals": identity["decimals"],
            "registry_contract_address": identity["registry_contract_address"],
            "rpc_read_only": True,
        }
        code_present: Optional[bool] = None
        onchain_symbol: Optional[str] = None
        onchain_name: Optional[str] = None
        onchain_decimals: Optional[int] = None
        registry_match = False
        canonical_status = "verification_failed"
        verification_error: Optional[str] = None

        chain = await self.rpc_client.verify_expected_chain(force_refresh=force_refresh)
        evidence["chain"] = {
            "ok": bool(chain.get("ok")),
            "actual_chain_id": chain.get("actual_chain_id"),
            "expected_chain_id": chain.get("expected_chain_id"),
        }
        if not chain.get("ok"):
            verification_error = "chain_id_mismatch_or_unavailable"
        elif identity["native"]:
            code_present = None
            onchain_symbol = identity["symbol"]
            onchain_name = registry_row.label or identity["symbol"]
            onchain_decimals = identity["decimals"]
            registry_match = True
            canonical_status = "verified"
            verification_error = None
        else:
            contract = identity["registry_contract_address"]
            code_result = await self.rpc_client.rpc_read(
                "eth_getCode",
                [contract, "latest"],
                cache_namespace=f"rh_registry_code:{int(registry_row.id)}",
                force_refresh=force_refresh,
            )
            raw_code = str(code_result.get("result") or "").strip()
            code_present = bool(code_result.get("ok") and raw_code not in {"", "0x", "0x0"})
            evidence["code"] = {
                "ok": bool(code_result.get("ok")),
                "present": code_present,
                "cached": bool(code_result.get("cached")),
                "fetched_at": code_result.get("fetched_at"),
                "error": _json_safe_error(code_result.get("error")),
            }

            async def _call(selector: str, namespace: str) -> Dict[str, Any]:
                return await self.rpc_client.rpc_read(
                    "eth_call",
                    [{"to": contract, "data": selector}, "latest"],
                    cache_namespace=f"rh_registry_{namespace}:{int(registry_row.id)}",
                    force_refresh=force_refresh,
                )

            symbol_result = await _call(_ERC20_SYMBOL_SELECTOR, "symbol")
            name_result = await _call(_ERC20_NAME_SELECTOR, "name")
            decimals_result = await _call(_ERC20_DECIMALS_SELECTOR, "decimals")
            onchain_symbol = _decode_abi_string(symbol_result.get("result")) if symbol_result.get("ok") else None
            onchain_name = _decode_abi_string(name_result.get("result")) if name_result.get("ok") else None
            if decimals_result.get("ok"):
                try:
                    onchain_decimals = int(decode_abi_uint256(decimals_result.get("result")))
                except Exception:
                    onchain_decimals = None
            evidence["metadata_calls"] = {
                "symbol_ok": bool(symbol_result.get("ok")),
                "name_ok": bool(name_result.get("ok")),
                "decimals_ok": bool(decimals_result.get("ok")),
                "symbol_error": _json_safe_error(symbol_result.get("error")),
                "name_error": _json_safe_error(name_result.get("error")),
                "decimals_error": _json_safe_error(decimals_result.get("error")),
            }

            registry_match = bool(
                code_present
                and onchain_symbol
                and onchain_symbol.strip().upper() == identity["symbol"]
                and onchain_decimals == identity["decimals"]
            )
            if registry_match:
                canonical_status = "verified"
                verification_error = None
            elif not code_present:
                canonical_status = "contract_code_missing"
                verification_error = "contract_code_missing"
            elif onchain_symbol is None or onchain_decimals is None:
                canonical_status = "metadata_unavailable"
                verification_error = "erc20_metadata_unavailable"
            else:
                canonical_status = "registry_mismatch"
                verification_error = "registry_onchain_identity_mismatch"

        record = self._verification_row(db, int(registry_row.id))
        if record is None:
            record = RobinhoodChainRegistryVerification(token_registry_id=int(registry_row.id))
            db.add(record)
        record.chain_id = ROBINHOOD_CHAIN_ID
        record.asset_kind = identity["asset_kind"]
        record.code_present = code_present
        record.onchain_symbol = onchain_symbol
        record.onchain_name = onchain_name
        record.onchain_decimals = onchain_decimals
        record.registry_match = bool(registry_match)
        record.canonical_status = canonical_status
        record.verification_error = verification_error
        record.evidence = evidence
        record.verified_at = verified_at
        record.updated_at = verified_at
        db.commit()
        db.refresh(record)
        return {
            "ok": canonical_status == "verified",
            "asset": identity,
            "verification": self._verification_dict(record),
            "database_mutated": True,
            "blockchain_read_only": True,
            "execution_enabled": False,
            "will_mutate_chain": False,
        }

    def _objective_row(self, db: Session, objective_id: str) -> RobinhoodChainPairObjective:
        row = (
            db.query(RobinhoodChainPairObjective)
            .filter(RobinhoodChainPairObjective.id == str(objective_id))
            .first()
        )
        if row is None:
            raise ValueError("robinhood_chain_pair_objective_not_found")
        return row

    def _objective_tokens(
        self,
        db: Session,
        objective: RobinhoodChainPairObjective,
    ) -> Tuple[TokenRegistry, TokenRegistry]:
        return (
            self._registry_row_by_id(db, int(objective.base_token_registry_id)),
            self._registry_row_by_id(db, int(objective.quote_token_registry_id)),
        )

    def _objective_dict(self, db: Session, row: RobinhoodChainPairObjective) -> Dict[str, Any]:
        base_row, quote_row = self._objective_tokens(db, row)
        capabilities = (
            db.query(RobinhoodChainPairCapability)
            .filter(RobinhoodChainPairCapability.objective_id == row.id)
            .order_by(RobinhoodChainPairCapability.from_token_registry_id.asc())
            .all()
        )
        return {
            "id": row.id,
            "symbol": row.symbol,
            "mechanism": row.mechanism,
            "enabled": bool(row.enabled),
            "review_only": bool(row.review_only),
            "notes": row.notes,
            "base": self.token_identity(db, base_row),
            "quote": self.token_identity(db, quote_row),
            "capabilities": [self._capability_dict(db, item) for item in capabilities],
            "created_at": iso_or_none(row.created_at),
            "updated_at": iso_or_none(row.updated_at),
        }

    def objectives(self, db: Session) -> List[Dict[str, Any]]:
        rows = (
            db.query(RobinhoodChainPairObjective)
            .order_by(RobinhoodChainPairObjective.symbol.asc())
            .all()
        )
        return [self._objective_dict(db, row) for row in rows]

    def objective_by_symbol(
        self,
        db: Session,
        symbol: str,
    ) -> Dict[str, Any]:
        normalized = _normalize_market_symbol(symbol)
        row = (
            db.query(RobinhoodChainPairObjective)
            .filter(
                RobinhoodChainPairObjective.symbol == normalized,
                RobinhoodChainPairObjective.enabled.is_(True),
            )
            .first()
        )
        if row is None:
            raise ValueError("robinhood_chain_pair_objective_not_found")
        return self._objective_dict(db, row)

    def market_by_symbol(
        self,
        db: Session,
        symbol: str,
    ) -> Dict[str, Any]:
        """Return one enabled market with derived provider/order-book state."""
        normalized = _normalize_market_symbol(symbol)
        market = next(
            (
                item
                for item in self.market_catalog(db)
                if str(item.get("symbol") or "").strip().upper() == normalized
            ),
            None,
        )
        if market is None:
            raise ValueError("robinhood_chain_pair_objective_not_found")
        return market

    @staticmethod
    def _market_indicative_state(capabilities: List[Dict[str, Any]]) -> str:
        statuses = {
            str(item.get("indicative_status") or "").strip().lower()
            for item in capabilities
            if isinstance(item, dict)
        }
        if statuses and statuses.issubset({"live_verified"}):
            return "live_verified"
        if statuses and statuses.issubset(INDICATIVE_AVAILABLE_STATUSES):
            return "available"
        if "mechanism_configured" in statuses:
            return "mechanism_configured"
        for status in INDICATIVE_UNAVAILABLE_PRIORITY:
            if status in statuses:
                return status
        return "not_tested"

    def market_catalog(self, db: Session) -> List[Dict[str, Any]]:
        markets: List[Dict[str, Any]] = []
        for objective in self.objectives(db):
            capabilities = [
                item for item in (objective.get("capabilities") or [])
                if isinstance(item, dict)
                and str(item.get("amount_mode") or "").strip().lower() == AMOUNT_MODE_EXACT_INPUT
            ]
            mechanism = str(objective.get("mechanism") or "").strip().lower()
            expected_directions = {
                (
                    str(objective.get("base", {}).get("symbol") or "").strip().upper(),
                    str(objective.get("quote", {}).get("symbol") or "").strip().upper(),
                ),
                (
                    str(objective.get("quote", {}).get("symbol") or "").strip().upper(),
                    str(objective.get("base", {}).get("symbol") or "").strip().upper(),
                ),
            }
            available_statuses = INDICATIVE_AVAILABLE_STATUSES
            available_by_provider: Dict[str, set] = {}
            for item in capabilities:
                if str(item.get("indicative_status") or "").strip().lower() not in available_statuses:
                    continue
                provider = str(item.get("provider") or "").strip().lower()
                if not provider:
                    continue
                available_by_provider.setdefault(provider, set()).add((
                    str(item.get("from_asset") or "").strip().upper(),
                    str(item.get("to_asset") or "").strip().upper(),
                ))
            complete_orderbook_providers = sorted(
                provider
                for provider, directions in available_by_provider.items()
                if expected_directions.issubset(directions)
            )
            live_execution_by_provider: Dict[str, set] = {}
            for item in capabilities:
                if not (
                    item.get("enabled") is True
                    and str(item.get("execution_status") or "").strip().lower() == "live_verified"
                ):
                    continue
                provider = str(item.get("provider") or "").strip().lower()
                if not provider:
                    continue
                live_execution_by_provider.setdefault(provider, set()).add((
                    str(item.get("from_asset") or "").strip().upper(),
                    str(item.get("to_asset") or "").strip().upper(),
                ))
            complete_live_execution_providers = sorted(
                provider
                for provider, directions in live_execution_by_provider.items()
                if expected_directions.issubset(directions)
                and provider in complete_orderbook_providers
            )
            preferred_orderbook_provider = (
                complete_live_execution_providers[0]
                if complete_live_execution_providers
                else (
                    PROVIDER_UNISWAP
                    if PROVIDER_UNISWAP in complete_orderbook_providers
                    else (complete_orderbook_providers[0] if complete_orderbook_providers else None)
                )
            )
            available_directions = set().union(*available_by_provider.values()) if available_by_provider else set()
            provider_errors = [
                item for item in capabilities
                if str(item.get("indicative_status") or "").strip().lower() == "provider_error"
            ]
            legal_restrictions = [
                item for item in capabilities
                if str(item.get("indicative_status") or "").strip().lower() == "legal_restriction"
            ]
            unavailable_capabilities = [
                item for item in capabilities
                if str(item.get("indicative_status") or "").strip().lower() not in available_statuses
            ]
            direction_statuses = {
                f"{str(item.get('provider') or '').strip().lower()}:{str(item.get('from_asset') or '').strip().upper()}->{str(item.get('to_asset') or '').strip().upper()}":
                    str(item.get("indicative_status") or "not_yet_probed").strip().lower()
                for item in capabilities
            }
            live_verified = [
                item for item in capabilities
                if str(item.get("execution_status") or "").strip().lower() == "live_verified"
                and item.get("enabled") is True
            ]
            mechanism_configured = bool(
                mechanism == MECHANISM_WRAP_UNWRAP
                and len(capabilities) == 2
                and all(
                    str(item.get("indicative_status") or "").strip().lower() == "mechanism_configured"
                    for item in capabilities
                )
            )
            orderbook_enabled = bool(
                mechanism == MECHANISM_SWAP
                and complete_orderbook_providers
            )
            if orderbook_enabled:
                orderbook_reason = None
            elif mechanism == MECHANISM_WRAP_UNWRAP:
                orderbook_reason = "wrap_unwrap_uses_dedicated_mechanism_view"
            else:
                unavailable_state = self._market_indicative_state(unavailable_capabilities)
                if expected_directions.issubset(available_directions):
                    orderbook_reason = "same_provider_both_exact_input_directions_not_available"
                else:
                    orderbook_reason = (
                        unavailable_state
                        if unavailable_state not in {"available", "live_verified", "mechanism_configured"}
                        else "same_provider_both_exact_input_directions_not_available"
                    )

            providers = sorted({
                str(item.get("provider") or "").strip().lower()
                for item in capabilities
                if str(item.get("provider") or "").strip()
            })
            verified_times = sorted(
                str(item.get("last_verified_at") or "").strip()
                for item in capabilities
                if str(item.get("last_verified_at") or "").strip()
            )
            indicative_state = self._market_indicative_state(capabilities)
            markets.append({
                **objective,
                "tranche": "RH-CHAIN.10D.2-R5C.2",
                "identity_source": "token_registry",
                "capability_source": "database",
                "indicative_state": indicative_state,
                "providers": providers,
                "orderbook_providers": complete_orderbook_providers,
                "live_execution_orderbook_providers": complete_live_execution_providers,
                "preferred_orderbook_provider": preferred_orderbook_provider,
                "orderbook_enabled": orderbook_enabled,
                "orderbook_reason": orderbook_reason,
                "mechanism_configured": mechanism_configured,
                "execution_enabled": bool(live_verified),
                "automatic_execution_promotion": False,
                "available_direction_count": len(available_directions),
                "live_verified_direction_count": len(live_verified),
                "provider_error_direction_count": len(provider_errors),
                "legal_restriction_direction_count": len(legal_restrictions),
                "unavailable_direction_count": len(unavailable_capabilities),
                "direction_statuses": direction_statuses,
                "refresh_supported": bool(mechanism == MECHANISM_SWAP and objective.get("enabled") is True),
                "explicit_refresh_required": bool(mechanism == MECHANISM_SWAP and not orderbook_enabled),
                "last_verified_at": verified_times[-1] if verified_times else None,
            })
        return markets

    def create_objective(
        self,
        db: Session,
        *,
        base_token_registry_id: int,
        quote_token_registry_id: int,
        mechanism: str,
        notes: Optional[str],
        confirm_create: bool,
        require_verified_registry_identities: bool = False,
    ) -> Dict[str, Any]:
        if confirm_create is not True:
            raise ValueError("confirm_pair_objective_create_required")

        base_row = self._registry_row_by_id(db, base_token_registry_id)
        quote_row = self._registry_row_by_id(db, quote_token_registry_id)
        if int(base_row.id) == int(quote_row.id):
            raise ValueError("pair_objective_assets_must_differ")

        effective_registry_ids = {
            int(row.id)
            for row in self.registry_rows(db)
        }
        requested_registry_ids = {
            int(base_row.id),
            int(quote_row.id),
        }
        if not requested_registry_ids.issubset(effective_registry_ids):
            raise ValueError("pair_objective_requires_effective_registry_identity")

        if require_verified_registry_identities:
            self._verified_identity_required(db, int(base_row.id))
            self._verified_identity_required(db, int(quote_row.id))

        normalized_mechanism = str(mechanism or MECHANISM_SWAP).strip().lower()
        if normalized_mechanism not in {MECHANISM_SWAP, MECHANISM_WRAP_UNWRAP}:
            raise ValueError("unsupported_pair_objective_mechanism")
        if normalized_mechanism == MECHANISM_WRAP_UNWRAP:
            base_identity = self.token_identity(db, base_row)
            quote_identity = self.token_identity(db, quote_row)
            if bool(base_identity["native"]) == bool(quote_identity["native"]):
                raise ValueError("wrap_unwrap_requires_one_native_and_one_erc20_asset")

        symbol = f"{str(base_row.symbol).strip().upper()}-{str(quote_row.symbol).strip().upper()}"
        row = (
            db.query(RobinhoodChainPairObjective)
            .filter(
                RobinhoodChainPairObjective.base_token_registry_id == int(base_row.id),
                RobinhoodChainPairObjective.quote_token_registry_id == int(quote_row.id),
            )
            .first()
        )

        created = row is None
        mutated = False
        requested_notes = _clean_text(notes)
        if row is None:
            row = RobinhoodChainPairObjective(
                base_token_registry_id=int(base_row.id),
                quote_token_registry_id=int(quote_row.id),
                symbol=symbol,
                mechanism=normalized_mechanism,
                enabled=True,
                review_only=True,
                notes=requested_notes,
                updated_at=utc_now(),
            )
            db.add(row)
            mutated = True
        else:
            desired_notes = row.notes if requested_notes is None else requested_notes
            desired_values = {
                "symbol": symbol,
                "mechanism": normalized_mechanism,
                "enabled": True,
                "review_only": True,
                "notes": desired_notes,
            }
            for field_name, desired_value in desired_values.items():
                if getattr(row, field_name) != desired_value:
                    setattr(row, field_name, desired_value)
                    mutated = True
            if mutated:
                row.updated_at = utc_now()

        if mutated:
            db.commit()
            db.refresh(row)

        return {
            "ok": True,
            "objective": self._objective_dict(db, row),
            "created": bool(created),
            "updated": bool(mutated and not created),
            "idempotent": bool(not mutated),
            "selected_pair_only": True,
            "database_mutated": bool(mutated),
            "blockchain_read_only": True,
            "provider_contacted": False,
            "rpc_contacted": False,
            "wallet_connection_requested": False,
            "signing_enabled": False,
            "broadcast_enabled": False,
            "automatic_execution_promotion": False,
            "execution_enabled": False,
            "registry_verification_required": bool(require_verified_registry_identities),
            "registry_verified": bool(require_verified_registry_identities),
            "will_mutate_chain": False,
        }

    def delete_objective(
        self,
        db: Session,
        *,
        objective_id: str,
        confirm_delete: bool,
    ) -> Dict[str, Any]:
        if confirm_delete is not True:
            raise ValueError("confirm_pair_objective_delete_required")
        row = self._objective_row(db, objective_id)
        db.query(RobinhoodChainPairCapability).filter(
            RobinhoodChainPairCapability.objective_id == row.id
        ).delete(synchronize_session=False)
        db.delete(row)
        db.commit()
        return {
            "ok": True,
            "deleted": 1,
            "database_mutated": True,
            "blockchain_read_only": True,
            "execution_enabled": False,
        }

    def _capability_row(
        self,
        db: Session,
        *,
        objective_id: str,
        from_token_registry_id: int,
        to_token_registry_id: int,
        amount_mode: str,
        provider: str,
    ) -> RobinhoodChainPairCapability:
        """Atomically get or create one provider-scoped capability row.

        Selected-market refreshes can overlap with foreground Order Book or
        Order Ticket refreshes. A query-then-insert sequence allows two
        requests to observe the same missing row and race on the provider-
        scoped unique constraint. SQLite/PostgreSQL therefore use a single
        conflict-safe insert before reading the canonical row.
        """
        objective_key = str(objective_id or "").strip()
        amount_mode_key = str(amount_mode or "").strip().lower()
        provider_key = str(provider or "").strip().lower()
        from_id = int(from_token_registry_id)
        to_id = int(to_token_registry_id)
        now = utc_now()

        if not objective_key or not amount_mode_key or not provider_key:
            raise ValueError("invalid_robinhood_chain_capability_identity")

        identity_filters = (
            RobinhoodChainPairCapability.objective_id == objective_key,
            RobinhoodChainPairCapability.from_token_registry_id == from_id,
            RobinhoodChainPairCapability.to_token_registry_id == to_id,
            RobinhoodChainPairCapability.amount_mode == amount_mode_key,
            RobinhoodChainPairCapability.provider == provider_key,
        )
        table = RobinhoodChainPairCapability.__table__
        identity_columns = (
            table.c.objective_id,
            table.c.from_token_registry_id,
            table.c.to_token_registry_id,
            table.c.amount_mode,
            table.c.provider,
        )
        insert_values = {
            "id": str(uuid.uuid4()),
            "objective_id": objective_key,
            "from_token_registry_id": from_id,
            "to_token_registry_id": to_id,
            "amount_mode": amount_mode_key,
            "provider": provider_key,
            "indicative_status": "not_tested",
            "firm_plan_status": "not_tested",
            "execution_status": "disabled",
            "enabled": False,
            "created_at": now,
            "updated_at": now,
        }
        bind = db.get_bind()
        dialect = str(getattr(getattr(bind, "dialect", None), "name", "") or "").lower()

        if dialect == "sqlite":
            statement = (
                sqlite_insert(table)
                .values(**insert_values)
                .on_conflict_do_nothing(index_elements=identity_columns)
            )
            db.execute(statement)
            db.flush()
        elif dialect == "postgresql":
            statement = (
                postgresql_insert(table)
                .values(**insert_values)
                .on_conflict_do_nothing(index_elements=identity_columns)
            )
            db.execute(statement)
            db.flush()
        else:
            row = db.query(RobinhoodChainPairCapability).filter(*identity_filters).first()
            if row is None:
                try:
                    with db.begin_nested():
                        row = RobinhoodChainPairCapability(**insert_values)
                        db.add(row)
                        db.flush()
                except IntegrityError:
                    db.expire_all()
            row = db.query(RobinhoodChainPairCapability).filter(*identity_filters).first()
            if row is not None:
                return row

        row = (
            db.query(RobinhoodChainPairCapability)
            .filter(*identity_filters)
            .populate_existing()
            .first()
        )
        if row is None:
            raise RuntimeError("robinhood_chain_capability_atomic_upsert_failed")
        return row

    def _capability_dict(self, db: Session, row: RobinhoodChainPairCapability) -> Dict[str, Any]:
        from_row = self._registry_row_by_id(db, int(row.from_token_registry_id))
        to_row = self._registry_row_by_id(db, int(row.to_token_registry_id))
        objective = self._objective_row(db, row.objective_id)
        from_symbol = str(from_row.symbol or "").strip().upper()
        to_symbol = str(to_row.symbol or "").strip().upper()
        display_mode = "exact_spend" if row.amount_mode == AMOUNT_MODE_EXACT_INPUT else row.amount_mode
        persisted_indicative_status = str(row.indicative_status or "not_tested").strip().lower()
        provider_error = copy.deepcopy(row.provider_error) if isinstance(row.provider_error, dict) else {}
        evidence = copy.deepcopy(row.evidence) if isinstance(row.evidence, dict) else {}
        effective_indicative_status = persisted_indicative_status
        classification_source = "persisted_status"
        if persisted_indicative_status == "provider_error" and provider_error:
            reclassified = _classify_probe_result({
                "ok": False,
                "error": provider_error.get("error") or "execution_discovery_provider_error",
                "http_status": provider_error.get("http_status"),
                "provider_error": provider_error.get("provider_error") or provider_error,
            })
            if reclassified != "provider_error":
                effective_indicative_status = reclassified
                classification_source = "persisted_provider_error_envelope"
                provider_error["classification"] = reclassified
                evidence["classification"] = reclassified
        reason = None
        if row.execution_status == PREPARATION_STATUS:
            reason = "Bounded preparation is verified; live execution remains unverified."
        elif row.execution_status != "live_verified":
            reason = "Review-only discovery does not automatically enable execution."
        return {
            "id": row.id,
            "objective_id": row.objective_id,
            "symbol": objective.symbol,
            "mechanism": objective.mechanism,
            "from_token_registry_id": int(row.from_token_registry_id),
            "to_token_registry_id": int(row.to_token_registry_id),
            "from_asset": from_symbol,
            "to_asset": to_symbol,
            "amount_mode": row.amount_mode,
            "display_mode": display_mode,
            "provider": row.provider,
            "indicative_status": effective_indicative_status,
            "persisted_indicative_status": persisted_indicative_status,
            "classification_source": classification_source,
            "firm_plan_status": row.firm_plan_status,
            "execution_status": row.execution_status,
            "enabled": bool(row.enabled),
            "route_sources": copy.deepcopy(row.route_sources) if isinstance(row.route_sources, dict) else {},
            "probe_amount": row.probe_amount,
            "price_impact_bps": row.price_impact_bps,
            "provider_error": provider_error,
            "backoff_until": iso_or_none(row.backoff_until),
            "evidence": evidence,
            "last_verified_at": iso_or_none(row.last_verified_at),
            "reason": reason,
            "review_only": True,
            "execution_enabled": bool(row.enabled and row.execution_status == "live_verified"),
            "preparation_enabled": bool(row.enabled and row.execution_status == PREPARATION_STATUS),
        }

    def route_capabilities(self, db: Session) -> List[Dict[str, Any]]:
        rows = (
            db.query(RobinhoodChainPairCapability)
            .join(
                RobinhoodChainPairObjective,
                RobinhoodChainPairObjective.id == RobinhoodChainPairCapability.objective_id,
            )
            .filter(RobinhoodChainPairObjective.enabled.is_(True))
            .order_by(RobinhoodChainPairObjective.symbol.asc(), RobinhoodChainPairCapability.from_token_registry_id.asc())
            .all()
        )
        return [self._capability_dict(db, row) for row in rows]

    def route_capability(
        self,
        db: Session,
        *,
        from_token_registry_id: int,
        to_token_registry_id: int,
        amount_mode: str,
    ) -> Optional[Dict[str, Any]]:
        row = (
            db.query(RobinhoodChainPairCapability)
            .join(
                RobinhoodChainPairObjective,
                RobinhoodChainPairObjective.id == RobinhoodChainPairCapability.objective_id,
            )
            .filter(
                RobinhoodChainPairObjective.enabled.is_(True),
                RobinhoodChainPairCapability.from_token_registry_id == int(from_token_registry_id),
                RobinhoodChainPairCapability.to_token_registry_id == int(to_token_registry_id),
                RobinhoodChainPairCapability.amount_mode == str(amount_mode),
            )
            .order_by(RobinhoodChainPairCapability.last_verified_at.desc().nullslast())
            .first()
        )
        return self._capability_dict(db, row) if row is not None else None

    def _verified_identity_required(self, db: Session, token_registry_id: int) -> None:
        record = self._verification_row(db, token_registry_id)
        if record is None or record.canonical_status != "verified" or not bool(record.registry_match):
            raise ValueError("pair_discovery_requires_verified_registry_identity")
        registry_row = self._registry_row_by_id(db, token_registry_id)
        identity = self.token_identity(db, registry_row)
        if not self._verification_matches_current_registry_identity(record, identity):
            raise ValueError("pair_discovery_requires_verified_registry_identity")

    def _persist_probe_result(
        self,
        db: Session,
        *,
        objective: RobinhoodChainPairObjective,
        from_row: TokenRegistry,
        to_row: TokenRegistry,
        provider: str,
        probe_amount: str,
        result: Dict[str, Any],
    ) -> RobinhoodChainPairCapability:
        now = utc_now()
        row = self._capability_row(
            db,
            objective_id=objective.id,
            from_token_registry_id=int(from_row.id),
            to_token_registry_id=int(to_row.id),
            amount_mode=AMOUNT_MODE_EXACT_INPUT,
            provider=provider,
        )
        route = result.get("route") if isinstance(result.get("route"), dict) else {}
        fills = route.get("fills") if isinstance(route.get("fills"), list) else []
        sources = sorted(
            {
                str(item.get("source") or "").strip()
                for item in fills
                if isinstance(item, dict) and str(item.get("source") or "").strip()
            }
        )
        price_impact = result.get("price_impact_bps")
        try:
            price_impact_float = float(price_impact) if price_impact is not None else None
        except Exception:
            price_impact_float = None
        classification = _classify_probe_result(result)
        row.indicative_status = classification
        authority_already_verified = bool(
            row.enabled
            or str(row.execution_status or "").strip().lower()
            in {PREPARATION_STATUS, "live_verified", LIVE_AUTHORIZED_PENDING_CONFIRMATION}
        )
        if not authority_already_verified:
            row.firm_plan_status = "not_tested"
            row.execution_status = "disabled"
            row.enabled = False
        row.route_sources = {"sources": sources, "fill_count": len(fills)}
        row.probe_amount = probe_amount
        row.price_impact_bps = price_impact_float
        if classification in INDICATIVE_AVAILABLE_STATUSES:
            row.provider_error = {}
        else:
            row.provider_error = {
                **_json_safe_error(result),
                "classification": classification,
            }
        backoff_raw = _clean_text(result.get("backoff_until"), 128)
        row.backoff_until = None
        if backoff_raw:
            try:
                row.backoff_until = datetime.fromisoformat(backoff_raw.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                row.backoff_until = None
        row.evidence = {
            "classification": classification,
            "liquidity_available": result.get("liquidity_available") is True,
            "sell_amount": result.get("sell_amount"),
            "buy_amount": result.get("buy_amount"),
            "price_buy_per_sell": result.get("price_buy_per_sell"),
            "provider_warnings": list(result.get("provider_warnings") or [])[:20],
            "provider_contacted": _probe_provider_contacted(result),
            "read_only": True,
            "transaction_constructed": False,
            "automatic_execution_promotion": False,
        }
        row.last_verified_at = now
        row.updated_at = now
        return row

    async def discover_objective(
        self,
        db: Session,
        *,
        objective_id: str,
        taker_address: str,
        base_probe_amount: str,
        quote_probe_amount: str,
        force_refresh: bool,
        confirm_discovery: bool,
    ) -> Dict[str, Any]:
        if confirm_discovery is not True:
            raise ValueError("confirm_pair_discovery_required")
        objective = self._objective_row(db, objective_id)
        if not bool(objective.enabled):
            raise ValueError("pair_objective_disabled")
        base_row, quote_row = self._objective_tokens(db, objective)
        base_identity = self.token_identity(db, base_row)
        quote_identity = self.token_identity(db, quote_row)
        self._verified_identity_required(db, int(base_row.id))
        self._verified_identity_required(db, int(quote_row.id))
        base_amount = _parse_probe_amount(base_probe_amount, int(base_row.decimals))
        quote_amount = _parse_probe_amount(quote_probe_amount, int(quote_row.decimals))
        taker = validate_evm_address(taker_address)
        results: List[Dict[str, Any]] = []

        if objective.mechanism == MECHANISM_WRAP_UNWRAP:
            for from_row, to_row, amount in (
                (base_row, quote_row, base_amount),
                (quote_row, base_row, quote_amount),
            ):
                row = self._capability_row(
                    db,
                    objective_id=objective.id,
                    from_token_registry_id=int(from_row.id),
                    to_token_registry_id=int(to_row.id),
                    amount_mode=AMOUNT_MODE_EXACT_INPUT,
                    provider=PROVIDER_NATIVE_WRAP,
                )
                row.indicative_status = "mechanism_configured"
                row.firm_plan_status = "not_tested"
                row.execution_status = "disabled"
                row.enabled = False
                row.route_sources = {"sources": [PROVIDER_NATIVE_WRAP], "fill_count": 0}
                row.probe_amount = amount
                row.provider_error = {}
                row.evidence = {
                    "mechanism": MECHANISM_WRAP_UNWRAP,
                    "provider_contacted": False,
                    "transaction_constructed": False,
                    "read_only": True,
                }
                row.last_verified_at = utc_now()
                row.updated_at = utc_now()
                db.flush()
                results.append(self._capability_dict(db, row))
            db.commit()
        else:
            for from_row, to_row, from_identity, to_identity, amount in (
                (base_row, quote_row, base_identity, quote_identity, base_amount),
                (quote_row, base_row, quote_identity, base_identity, quote_amount),
            ):
                result = await self.discovery_service.probe(
                    sell_token=from_identity,
                    buy_token=to_identity,
                    sell_amount=amount,
                    buy_amount=None,
                    taker_address=taker,
                    force_refresh=force_refresh,
                    route_capability=None,
                    require_live_verified=False,
                    max_probe_amount=amount,
                )
                row = self._persist_probe_result(
                    db,
                    objective=objective,
                    from_row=from_row,
                    to_row=to_row,
                    provider=PROVIDER_ZEROX,
                    probe_amount=amount,
                    result=result,
                )
                db.flush()
                results.append(self._capability_dict(db, row))
            db.commit()

        return {
            "ok": True,
            "objective": self._objective_dict(db, objective),
            "results": results,
            "database_mutated": True,
            "blockchain_read_only": True,
            "provider_read_only": True,
            "execution_enabled": False,
            "signing_enabled": False,
            "broadcast_enabled": False,
            "automatic_execution_promotion": False,
            "will_mutate_chain": False,
        }

    @staticmethod
    def _default_selected_probe_amount(identity: Dict[str, Any]) -> str:
        symbol = str(identity.get("symbol") or "").strip().upper()
        if bool(identity.get("native")):
            return "0.0001"
        if symbol == "USDG":
            return "1"
        return "1"

    async def refresh_selected_market(
        self,
        db: Session,
        *,
        symbol: str,
        taker_address: str,
        force_refresh: bool,
        confirm_refresh: bool,
    ) -> Dict[str, Any]:
        """Refresh provider-scoped exact-input evidence for one selected market.

        Every provider/direction is isolated. Provider and persistence failures
        are returned as sanitized capability diagnostics and cannot erase a
        successful result from another provider or direction. Direct Uniswap v3
        Factory + QuoterV2 reads are an independent fallback to the Trading API.
        """
        if confirm_refresh is not True:
            raise ValueError("confirm_selected_market_refresh_required")

        schema_compatibility = _ensure_provider_scoped_capability_schema(db)
        normalized_symbol = _normalize_market_symbol(symbol)
        objective = (
            db.query(RobinhoodChainPairObjective)
            .filter(
                RobinhoodChainPairObjective.symbol == normalized_symbol,
                RobinhoodChainPairObjective.enabled.is_(True),
            )
            .first()
        )
        if objective is None:
            raise ValueError("robinhood_chain_pair_objective_not_found")
        if str(objective.mechanism or "").strip().lower() != MECHANISM_SWAP:
            raise ValueError("selected_market_refresh_requires_swap_objective")

        base_row, quote_row = self._objective_tokens(db, objective)
        base_identity = self.token_identity(db, base_row)
        quote_identity = self.token_identity(db, quote_row)
        taker = validate_evm_address(taker_address)
        directions = (
            (base_row, quote_row, base_identity, quote_identity),
            (quote_row, base_row, quote_identity, base_identity),
        )
        results: List[Dict[str, Any]] = []
        provider_contacted = False
        persistence_errors: List[Dict[str, Any]] = []

        identity_error: Optional[str] = None
        try:
            self._verified_identity_required(db, int(base_row.id))
            self._verified_identity_required(db, int(quote_row.id))
        except ValueError as exc:
            identity_error = str(exc)

        weth_identity: Optional[Dict[str, Any]] = None
        weth_identity_error: Optional[str] = None
        try:
            weth_row = self._registry_row_by_symbol(db, "WETH")
            self._verified_identity_required(db, int(weth_row.id))
            weth_identity = self.token_identity(db, weth_row)
        except ValueError as exc:
            weth_identity_error = str(exc)

        providers = (PROVIDER_ZEROX, PROVIDER_UNISWAP, PROVIDER_UNISWAP_V3_RPC)
        for from_row, to_row, from_identity, to_identity in directions:
            side = "sell" if int(from_row.id) == int(base_row.id) else "buy"
            direction_probe_row = (
                db.query(RobinhoodChainPairCapability)
                .filter(
                    RobinhoodChainPairCapability.objective_id == objective.id,
                    RobinhoodChainPairCapability.from_token_registry_id == int(from_row.id),
                    RobinhoodChainPairCapability.to_token_registry_id == int(to_row.id),
                    RobinhoodChainPairCapability.amount_mode == AMOUNT_MODE_EXACT_INPUT,
                    RobinhoodChainPairCapability.probe_amount.isnot(None),
                    RobinhoodChainPairCapability.probe_amount != "",
                )
                .order_by(RobinhoodChainPairCapability.last_verified_at.desc().nullslast())
                .first()
            )
            direction_probe_amount = str(getattr(direction_probe_row, "probe_amount", "") or "").strip()
            for provider in providers:
                capability = self._capability_row(
                    db,
                    objective_id=objective.id,
                    from_token_registry_id=int(from_row.id),
                    to_token_registry_id=int(to_row.id),
                    amount_mode=AMOUNT_MODE_EXACT_INPUT,
                    provider=provider,
                )
                probe_amount = str(capability.probe_amount or "").strip()
                if not probe_amount:
                    probe_amount = direction_probe_amount or self._default_selected_probe_amount(from_identity)
                classification_result: Optional[Dict[str, Any]] = None

                if identity_error:
                    classification_result = {
                        "ok": False,
                        "error": identity_error,
                        "provider": provider,
                        "provider_contacted": False,
                        "liquidity_available": False,
                        "read_only": True,
                        "will_mutate": False,
                    }
                else:
                    try:
                        probe_amount = _parse_probe_amount(probe_amount, int(from_row.decimals))
                    except ValueError as exc:
                        classification_result = {
                            "ok": False,
                            "error": str(exc),
                            "provider": provider,
                            "provider_contacted": False,
                            "liquidity_available": False,
                            "read_only": True,
                            "will_mutate": False,
                        }

                if classification_result is None:
                    try:
                        if provider == PROVIDER_ZEROX:
                            classification_result = await self.discovery_service.probe(
                                sell_token=from_identity,
                                buy_token=to_identity,
                                sell_amount=probe_amount,
                                buy_amount=None,
                                taker_address=taker,
                                force_refresh=force_refresh,
                                route_capability=None,
                                require_live_verified=False,
                                max_probe_amount=probe_amount,
                            )
                        elif provider == PROVIDER_UNISWAP:
                            classification_result = await self.uniswap_service.probe(
                                symbol=normalized_symbol,
                                side=side,
                                requested_amount=probe_amount,
                                swapper_address=taker,
                                input_token=from_identity,
                                output_token=to_identity,
                                slippage_bps=50,
                            )
                        else:
                            native_route = bool(from_identity.get("native")) or bool(to_identity.get("native"))
                            if native_route and weth_identity is None:
                                classification_result = {
                                    "ok": False,
                                    "error": weth_identity_error or "verified_weth_registry_identity_required",
                                    "provider": provider,
                                    "provider_contacted": False,
                                    "liquidity_available": False,
                                    "read_only": True,
                                    "will_mutate": False,
                                }
                            else:
                                provider_from = (
                                    weth_identity
                                    if bool(from_identity.get("native"))
                                    else from_identity
                                )
                                provider_to = (
                                    weth_identity
                                    if bool(to_identity.get("native"))
                                    else to_identity
                                )
                                classification_result = await self.uniswap_v3_service.probe(
                                    requested_amount=probe_amount,
                                    input_token=provider_from,
                                    output_token=provider_to,
                                    bridge_token=weth_identity,
                                    display_input_symbol=from_identity.get("symbol"),
                                    display_output_symbol=to_identity.get("symbol"),
                                    force_refresh=force_refresh,
                                )
                    except Exception as exc:
                        classification_result = _provider_exception_result(provider, exc)

                provider_contacted = provider_contacted or _probe_provider_contacted(classification_result)
                try:
                    row = self._persist_probe_result(
                        db,
                        objective=objective,
                        from_row=from_row,
                        to_row=to_row,
                        provider=provider,
                        probe_amount=probe_amount,
                        result=classification_result,
                    )
                    db.flush()
                    db.commit()
                    db.refresh(row)
                    results.append(self._capability_dict(db, row))
                except Exception as exc:
                    db.rollback()
                    persistence_error = {
                        "provider": provider,
                        "from_asset": str(from_identity.get("symbol") or "").strip().upper(),
                        "to_asset": str(to_identity.get("symbol") or "").strip().upper(),
                        "error": "capability_persistence_failed",
                        "failure_type": type(exc).__name__,
                        "message": _clean_text(exc, 1000),
                    }
                    persistence_errors.append(persistence_error)
                    results.append({
                        "id": None,
                        "objective_id": objective.id,
                        "symbol": normalized_symbol,
                        "mechanism": objective.mechanism,
                        "from_token_registry_id": int(from_row.id),
                        "to_token_registry_id": int(to_row.id),
                        "from_asset": persistence_error["from_asset"],
                        "to_asset": persistence_error["to_asset"],
                        "amount_mode": AMOUNT_MODE_EXACT_INPUT,
                        "display_mode": "exact_spend",
                        "provider": provider,
                        "indicative_status": "provider_error",
                        "persisted_indicative_status": "not_persisted",
                        "classification_source": "persistence_exception",
                        "firm_plan_status": "not_tested",
                        "execution_status": "disabled",
                        "enabled": False,
                        "route_sources": {},
                        "probe_amount": probe_amount,
                        "price_impact_bps": None,
                        "provider_error": persistence_error,
                        "evidence": {
                            "classification": "provider_error",
                            "provider_contacted": _probe_provider_contacted(classification_result),
                            "read_only": True,
                            "transaction_constructed": False,
                        },
                        "last_verified_at": None,
                        "reason": "Capability evidence could not be persisted; other providers remain isolated.",
                        "review_only": True,
                        "execution_enabled": False,
                        "preparation_enabled": False,
                    })

        market = next(
            (item for item in self.market_catalog(db) if item.get("symbol") == normalized_symbol),
            None,
        )
        if market is None:
            raise ValueError("robinhood_chain_pair_objective_not_found")

        provider_contacted_direction_count = sum(
            1
            for item in results
            if isinstance(item, dict)
            and isinstance(item.get("evidence"), dict)
            and item["evidence"].get("provider_contacted") is True
        )
        provider_http_statuses = sorted({
            status
            for item in results
            if isinstance(item, dict)
            and isinstance(item.get("provider_error"), dict)
            for status in [_safe_http_status(item["provider_error"].get("http_status"))]
            if status is not None
        })
        provider_error_names = sorted({
            str(item["provider_error"]["provider_error"].get("name") or "").strip()
            for item in results
            if isinstance(item, dict)
            and isinstance(item.get("provider_error"), dict)
            and isinstance(item["provider_error"].get("provider_error"), dict)
            and str(item["provider_error"]["provider_error"].get("name") or "").strip()
        })
        provider_error_messages = sorted({
            str(item["provider_error"]["provider_error"].get("message") or "").strip()
            for item in results
            if isinstance(item, dict)
            and isinstance(item.get("provider_error"), dict)
            and isinstance(item["provider_error"].get("provider_error"), dict)
            and str(item["provider_error"]["provider_error"].get("message") or "").strip()
        })

        return {
            "ok": True,
            "tranche": "R5C.5D.2E-R1",
            "symbol": normalized_symbol,
            "market": market,
            "results": results,
            "provider_contacted": provider_contacted,
            "provider_contacted_direction_count": provider_contacted_direction_count,
            "provider_http_statuses": provider_http_statuses,
            "provider_error_names": provider_error_names,
            "provider_error_messages": provider_error_messages,
            "persistence_errors": persistence_errors,
            "schema_compatibility": schema_compatibility,
            "selected_pair_only": True,
            "probe_direction_count": len(results),
            "provider_count": len(providers),
            "providers": list(providers),
            "database_mutated": True,
            "blockchain_read_only": True,
            "provider_read_only": True,
            "execution_enabled": False,
            "signing_enabled": False,
            "broadcast_enabled": False,
            "automatic_execution_promotion": False,
            "transaction_constructed": False,
            "transaction_calldata": None,
            "will_mutate_chain": False,
        }

    async def verify_preparation_authority(
        self,
        db: Session,
        *,
        symbol: str,
        side: str,
        amount_mode: str,
        requested_amount: str,
        taker_address: str,
        slippage_bps: int,
        confirm_verify: bool,
    ) -> Dict[str, Any]:
        """Verify one explicitly bounded generic WETH preparation direction.

        R5C.4A remains locked to USDG -> WETH exact input at 1 USDG.
        R5C.4B adds WETH -> USDG exact input at 0.0001 WETH. The method
        may persist validated firm-plan evidence only after explicit confirmation.
        It never requests a wallet, signs, broadcasts, or claims live execution.
        """
        normalized_symbol = _normalize_market_symbol(symbol)
        normalized_side = str(side or "").strip().lower()
        normalized_mode = str(amount_mode or "").strip().lower().replace("exact_spend", AMOUNT_MODE_EXACT_INPUT)
        normalized_slippage = int(slippage_bps)

        if normalized_symbol == R5C4A_SYMBOL and normalized_side == R5C4A_SIDE:
            target = {
                "tranche": "R5C.4A",
                "error_prefix": "r5c4a",
                "input_asset": R5C4A_INPUT_ASSET,
                "output_asset": R5C4A_OUTPUT_ASSET,
                "input_amount": R5C4A_INPUT_AMOUNT,
                "slippage_bps": R5C4A_SLIPPAGE_BPS,
            }
        elif normalized_symbol == R5C4B_SYMBOL and normalized_side == R5C4B_SIDE:
            target = {
                "tranche": "R5C.4B",
                "error_prefix": "r5c4b",
                "input_asset": R5C4B_INPUT_ASSET,
                "output_asset": R5C4B_OUTPUT_ASSET,
                "input_amount": R5C4B_INPUT_AMOUNT,
                "slippage_bps": R5C4B_SLIPPAGE_BPS,
            }
        else:
            prefix = "r5c4a" if normalized_side == R5C4A_SIDE else "r5c4b"
            raise ValueError(f"{prefix}_preparation_target_locked")

        error_prefix = str(target["error_prefix"])
        if confirm_verify is not True:
            raise ValueError(f"confirm_{error_prefix}_preparation_verification_required")
        if normalized_mode != AMOUNT_MODE_EXACT_INPUT or normalized_slippage != int(target["slippage_bps"]):
            raise ValueError(f"{error_prefix}_preparation_target_locked")

        taker = validate_evm_address(taker_address)
        objective = (
            db.query(RobinhoodChainPairObjective)
            .filter(
                RobinhoodChainPairObjective.symbol == normalized_symbol,
                RobinhoodChainPairObjective.enabled.is_(True),
            )
            .first()
        )
        if objective is None:
            raise ValueError("robinhood_chain_pair_objective_not_found")
        if str(objective.mechanism or "").strip().lower() != MECHANISM_SWAP:
            raise ValueError(f"{error_prefix}_swap_mechanism_required")

        base_row, quote_row = self._objective_tokens(db, objective)
        base_identity = self.token_identity(db, base_row)
        quote_identity = self.token_identity(db, quote_row)
        input_row, input_identity = (
            (base_row, base_identity) if normalized_side == "sell" else (quote_row, quote_identity)
        )
        output_row, output_identity = (
            (quote_row, quote_identity) if normalized_side == "sell" else (base_row, base_identity)
        )
        if (
            str(input_identity.get("symbol") or "").strip().upper() != str(target["input_asset"])
            or bool(input_identity.get("native"))
            or str(output_identity.get("symbol") or "").strip().upper() != str(target["output_asset"])
            or bool(output_identity.get("native"))
        ):
            raise ValueError(f"{error_prefix}_token_registry_identity_mismatch")
        normalized_amount = _parse_probe_amount(requested_amount, int(input_identity["decimals"]))
        if normalized_amount != str(target["input_amount"]):
            raise ValueError(f"{error_prefix}_preparation_target_locked")
        self._verified_identity_required(db, int(base_row.id))
        self._verified_identity_required(db, int(quote_row.id))

        capability = (
            db.query(RobinhoodChainPairCapability)
            .filter(
                RobinhoodChainPairCapability.objective_id == objective.id,
                RobinhoodChainPairCapability.from_token_registry_id == int(input_row.id),
                RobinhoodChainPairCapability.to_token_registry_id == int(output_row.id),
                RobinhoodChainPairCapability.amount_mode == AMOUNT_MODE_EXACT_INPUT,
                RobinhoodChainPairCapability.provider == PROVIDER_ZEROX,
            )
            .first()
        )
        if capability is None:
            raise ValueError(f"{error_prefix}_direction_capability_missing")
        if str(capability.indicative_status or "").strip().lower() not in {"available", "live_verified"}:
            raise ValueError(f"{error_prefix}_indicative_capability_unavailable")
        if str(capability.probe_amount or "").strip() != str(target["input_amount"]):
            raise ValueError(f"{error_prefix}_probe_amount_mismatch")

        existing_evidence = copy.deepcopy(capability.evidence) if isinstance(capability.evidence, dict) else {}
        if (
            bool(capability.enabled)
            and str(capability.firm_plan_status or "").strip().lower() == "available"
            and str(capability.execution_status or "").strip().lower() == PREPARATION_STATUS
            and existing_evidence.get("preparation_verified") is True
            and str(existing_evidence.get("verified_input_amount") or "").strip() == str(target["input_amount"])
            and str(existing_evidence.get("side") or "").strip().lower() == normalized_side
            and str(existing_evidence.get("from_asset") or "").strip().upper() == str(target["input_asset"])
            and str(existing_evidence.get("to_asset") or "").strip().upper() == str(target["output_asset"])
        ):
            authority = resolve_robinhood_chain_execution_authority(
                db,
                symbol=normalized_symbol,
                side=normalized_side,
                amount_mode=normalized_mode,
                provider=PROVIDER_ZEROX,
                require_execution=True,
            )
            return {
                "ok": True,
                "idempotent": True,
                "tranche": target["tranche"],
                "capability": self._capability_dict(db, capability),
                "execution_authority": authority,
                "firm_plan": None,
                "database_mutated": False,
                "blockchain_read_only": True,
                "wallet_connection_requested": False,
                "signing_enabled": False,
                "broadcast_enabled": False,
                "successful_broadcast": False,
                "automatic_execution_promotion": False,
                "will_mutate_chain": False,
            }

        transient_capability = self._capability_dict(db, capability)
        transient_evidence = copy.deepcopy(transient_capability.get("evidence") or {})
        transient_evidence.update({
            "firm_plan_input_ceiling": target["input_amount"],
            "preparation_verification_requested": True,
            "live_accepted": False,
            "successful_broadcast": False,
        })
        transient_capability.update({
            "firm_plan_status": "available",
            "firm_plan_input_ceiling": target["input_amount"],
            "evidence": transient_evidence,
        })
        registry_tokens = [
            self.token_identity(db, row)
            for row in self.registry_rows(db)
        ]
        native_token = self.native_identity(db)
        plan = await self.planning_service.firm_quote_plan(
            symbol=normalized_symbol,
            side=normalized_side,
            amount_mode=normalized_mode,
            requested_amount=normalized_amount,
            maximum_input_amount=None,
            taker_address=taker,
            base_token=base_identity,
            quote_token=quote_identity,
            native_token=native_token,
            registry_tokens=registry_tokens,
            route_capability=transient_capability,
            slippage_bps=normalized_slippage,
        )
        if plan.get("ok") is not True:
            raise ValueError(str(plan.get("error") or f"{error_prefix}_firm_plan_verification_failed"))
        allowance = plan.get("allowance") if isinstance(plan.get("allowance"), dict) else {}
        unsigned = plan.get("unsigned_transaction_plan") if isinstance(plan.get("unsigned_transaction_plan"), dict) else {}
        if (
            str(plan.get("symbol") or "").strip().upper() != normalized_symbol
            or str(plan.get("side") or "").strip().lower() != normalized_side
            or str(plan.get("amount_mode") or "").strip().lower() != normalized_mode
            or str(plan.get("input_asset") or "").strip().upper() != str(target["input_asset"])
            or str(plan.get("output_asset") or "").strip().upper() != str(target["output_asset"])
            or str(plan.get("input_amount") or "").strip() != str(target["input_amount"])
            or allowance.get("applicable") is not True
            or str((allowance.get("token") or {}).get("symbol") or "").strip().upper() != str(target["input_asset"])
            or allowance.get("spender_allowlisted") is not True
            or validate_evm_address(str(allowance.get("spender") or "")).lower() not in ROBINHOOD_CHAIN_ALLOWANCE_HOLDER_ALLOWLIST
            or unsigned.get("destination_allowlisted") is not True
            or validate_evm_address(str(unsigned.get("to") or "")).lower() not in ROBINHOOD_CHAIN_ALLOWANCE_HOLDER_ALLOWLIST
            or str(unsigned.get("value_wei") or "") != "0"
            or unsigned.get("native_input") is not False
            or not str(unsigned.get("calldata_sha256") or "").strip()
            or int(str(unsigned.get("gas_limit") or "0")) <= 0
            or int(str(plan.get("minimum_received_atomic") or "0")) <= 0
        ):
            raise ValueError(f"{error_prefix}_firm_plan_identity_or_safety_mismatch")

        now = utc_now()
        evidence = existing_evidence
        evidence.update({
            "preparation_verified": True,
            "preparation_status": PREPARATION_STATUS,
            "tranche": target["tranche"],
            "symbol": normalized_symbol,
            "side": normalized_side,
            "amount_mode": normalized_mode,
            "provider": PROVIDER_ZEROX,
            "from_asset": target["input_asset"],
            "to_asset": target["output_asset"],
            "verified_input_amount": target["input_amount"],
            "firm_plan_input_ceiling": target["input_amount"],
            "quote_id": plan.get("quote_id"),
            "calldata_sha256": unsigned.get("calldata_sha256"),
            "transaction_destination": unsigned.get("to"),
            "allowance_spender": allowance.get("spender"),
            "approval_required": bool(plan.get("approval_required")),
            "minimum_received": plan.get("minimum_received"),
            "verified_at": iso_or_none(now),
            "provider_contacted": True,
            "read_only": True,
            "wallet_connection_requested": False,
            "signing_requested": False,
            "broadcast_requested": False,
            "live_accepted": False,
            "successful_broadcast": False,
            "automatic_execution_promotion": False,
        })
        capability.firm_plan_status = "available"
        capability.execution_status = PREPARATION_STATUS
        capability.enabled = True
        capability.provider_error = {}
        capability.evidence = evidence
        capability.last_verified_at = now
        capability.updated_at = now
        db.add(capability)
        db.flush()

        authority = resolve_robinhood_chain_execution_authority(
            db,
            symbol=normalized_symbol,
            side=normalized_side,
            amount_mode=normalized_mode,
            provider=PROVIDER_ZEROX,
            require_execution=True,
        )
        if authority.get("authority_level") != PREPARATION_STATUS or authority.get("live_execution_verified") is not False:
            raise ValueError(f"{error_prefix}_preparation_authority_resolution_failed")
        db.commit()
        db.refresh(capability)
        return {
            "ok": True,
            "idempotent": False,
            "tranche": target["tranche"],
            "capability": self._capability_dict(db, capability),
            "execution_authority": authority,
            "firm_plan": plan,
            "database_mutated": True,
            "blockchain_read_only": True,
            "provider_read_only": True,
            "wallet_connection_requested": False,
            "signing_enabled": False,
            "broadcast_enabled": False,
            "successful_broadcast": False,
            "initial_acceptance_wallet_reject_only": True,
            "automatic_execution_promotion": False,
            "will_mutate_chain": False,
        }

    def authorize_controlled_live_buy(
        self,
        db: Session,
        *,
        symbol: str,
        side: str,
        amount_mode: str,
        requested_amount: str,
        provider: str,
        wallet_address: str,
        confirm_authorize: bool,
    ) -> Dict[str, Any]:
        """Authorize one bounded R5C.5A browser-wallet BUY before live verification.

        This mutates database capability evidence only. It never contacts a provider,
        requests a wallet, signs, or broadcasts. The authorization expires and does
        not promote the capability to live_verified.
        """
        if confirm_authorize is not True:
            raise ValueError("confirm_r5c5a_live_authorization_required")
        normalized_symbol = _normalize_market_symbol(symbol)
        normalized_side = str(side or "").strip().lower()
        normalized_mode = str(amount_mode or "").strip().lower().replace("exact_spend", AMOUNT_MODE_EXACT_INPUT)
        normalized_provider = str(provider or "").strip().lower().replace("zerox", PROVIDER_ZEROX)
        authorized_wallet = validate_evm_address(wallet_address).lower()
        if (
            normalized_symbol != R5C4A_SYMBOL
            or normalized_side != R5C4A_SIDE
            or normalized_mode != AMOUNT_MODE_EXACT_INPUT
            or normalized_provider != PROVIDER_ZEROX
            or str(requested_amount or "").strip() != R5C4A_INPUT_AMOUNT
        ):
            raise ValueError("r5c5a_live_authorization_target_locked")

        objective = (
            db.query(RobinhoodChainPairObjective)
            .filter(
                RobinhoodChainPairObjective.symbol == normalized_symbol,
                RobinhoodChainPairObjective.enabled.is_(True),
            )
            .first()
        )
        if objective is None:
            raise ValueError("robinhood_chain_pair_objective_not_found")
        base_row, quote_row = self._objective_tokens(db, objective)
        base_identity = self.token_identity(db, base_row)
        quote_identity = self.token_identity(db, quote_row)
        input_row, input_identity = quote_row, quote_identity
        output_row, output_identity = base_row, base_identity
        if (
            str(input_identity.get("symbol") or "").strip().upper() != R5C4A_INPUT_ASSET
            or bool(input_identity.get("native"))
            or str(output_identity.get("symbol") or "").strip().upper() != R5C4A_OUTPUT_ASSET
            or bool(output_identity.get("native"))
        ):
            raise ValueError("r5c5a_token_registry_identity_mismatch")

        capability = (
            db.query(RobinhoodChainPairCapability)
            .filter(
                RobinhoodChainPairCapability.objective_id == objective.id,
                RobinhoodChainPairCapability.from_token_registry_id == int(input_row.id),
                RobinhoodChainPairCapability.to_token_registry_id == int(output_row.id),
                RobinhoodChainPairCapability.amount_mode == AMOUNT_MODE_EXACT_INPUT,
                RobinhoodChainPairCapability.provider == PROVIDER_ZEROX,
            )
            .first()
        )
        if capability is None:
            raise ValueError("r5c5a_direction_capability_missing")
        evidence = copy.deepcopy(capability.evidence) if isinstance(capability.evidence, dict) else {}
        if (
            not bool(capability.enabled)
            or str(capability.indicative_status or "").strip().lower() not in {"available", "live_verified"}
            or str(capability.firm_plan_status or "").strip().lower() != "available"
            or str(capability.execution_status or "").strip().lower() != PREPARATION_STATUS
            or str(capability.probe_amount or "").strip() != R5C4A_INPUT_AMOUNT
            or evidence.get("preparation_verified") is not True
            or evidence.get("live_accepted") is True
            or evidence.get("successful_broadcast") is True
            or str(evidence.get("symbol") or "").strip().upper() != R5C4A_SYMBOL
            or str(evidence.get("side") or "").strip().lower() != R5C4A_SIDE
            or str(evidence.get("amount_mode") or "").strip().lower() != AMOUNT_MODE_EXACT_INPUT
            or str(evidence.get("provider") or "").strip().lower() != PROVIDER_ZEROX
            or str(evidence.get("from_asset") or "").strip().upper() != R5C4A_INPUT_ASSET
            or str(evidence.get("to_asset") or "").strip().upper() != R5C4A_OUTPUT_ASSET
            or str(evidence.get("verified_input_amount") or "").strip() != R5C4A_INPUT_AMOUNT
            or str(evidence.get("firm_plan_input_ceiling") or "").strip() != R5C4A_INPUT_AMOUNT
        ):
            raise ValueError("r5c5a_preparation_authority_required")

        now = utc_now()
        existing = evidence.get("live_authorization") if isinstance(evidence.get("live_authorization"), dict) else {}
        existing_expires = None
        try:
            existing_expires = datetime.fromisoformat(str(existing.get("expires_at") or "").replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            existing_expires = None
        if (
            str(existing.get("status") or "").strip().lower() == LIVE_AUTHORIZED_PENDING_CONFIRMATION
            and existing.get("operator_confirmed") is True
            and existing_expires is not None
            and existing_expires > now
            and evidence.get("successful_broadcast_authorized") is True
            and str(existing.get("wallet_address") or "").strip().lower() == authorized_wallet
        ):
            authority = resolve_robinhood_chain_execution_authority(
                db, symbol=normalized_symbol, side=normalized_side,
                amount_mode=normalized_mode, provider=normalized_provider,
                require_execution=True,
            )
            return {
                "ok": True,
                "idempotent": True,
                "tranche": "R5C.5A",
                "execution_authority": authority,
                "capability": self._capability_dict(db, capability),
                "database_mutated": False,
                "wallet_connection_requested": False,
                "signing_enabled": False,
                "broadcast_enabled": False,
                "successful_broadcast_authorized": True,
                "live_execution_verified": False,
                "automatic_execution_promotion": False,
                "will_mutate_chain": False,
            }

        expires_at = now + timedelta(minutes=R5C5A_AUTHORIZATION_TTL_MINUTES)
        authorization = {
            "status": LIVE_AUTHORIZED_PENDING_CONFIRMATION,
            "tranche": "R5C.5A",
            "authorization_id": uuid.uuid4().hex,
            "operator_confirmed": True,
            "authorized_at": iso_or_none(now),
            "expires_at": iso_or_none(expires_at),
            "symbol": R5C4A_SYMBOL,
            "side": R5C4A_SIDE,
            "amount_mode": AMOUNT_MODE_EXACT_INPUT,
            "provider": PROVIDER_ZEROX,
            "input_asset": R5C4A_INPUT_ASSET,
            "output_asset": R5C4A_OUTPUT_ASSET,
            "exact_input_amount": R5C4A_INPUT_AMOUNT,
            "wallet_address": authorized_wallet,
            "approval_model": "finite_exact_input",
            "unlimited_approval_enabled": False,
            "approval_transaction_value_wei": "0",
            "swap_transaction_value_wei": "0",
            "separate_wallet_requests_required": True,
            "automatic_second_transaction": False,
            "automatic_retry": False,
            "automatic_execution_promotion": False,
        }
        evidence.update({
            "successful_broadcast_authorized": True,
            "live_authorized_pending_confirmation": True,
            "live_authorization": authorization,
        })
        capability.evidence = evidence
        capability.updated_at = now
        db.add(capability)
        db.flush()
        authority = resolve_robinhood_chain_execution_authority(
            db, symbol=normalized_symbol, side=normalized_side,
            amount_mode=normalized_mode, provider=normalized_provider,
            require_execution=True,
        )
        if (
            authority.get("authority_level") != LIVE_AUTHORIZED_PENDING_CONFIRMATION
            or authority.get("successful_broadcast_authorized") is not True
            or authority.get("live_execution_verified") is not False
        ):
            raise ValueError("r5c5a_live_authorization_resolution_failed")
        db.commit()
        db.refresh(capability)
        return {
            "ok": True,
            "idempotent": False,
            "tranche": "R5C.5A",
            "execution_authority": authority,
            "capability": self._capability_dict(db, capability),
            "database_mutated": True,
            "blockchain_read_only": True,
            "provider_contacted": False,
            "wallet_connection_requested": False,
            "signing_enabled": False,
            "broadcast_enabled": False,
            "successful_broadcast_authorized": True,
            "live_execution_verified": False,
            "automatic_second_transaction": False,
            "automatic_retry": False,
            "automatic_execution_promotion": False,
            "will_mutate_chain": False,
        }

    def authorize_controlled_live_sell(
        self,
        db: Session,
        *,
        symbol: str,
        side: str,
        amount_mode: str,
        requested_amount: str,
        provider: str,
        wallet_address: str,
        confirm_authorize: bool,
    ) -> Dict[str, Any]:
        """Authorize one bounded R5C.5B browser-wallet SELL before live verification.

        This mutates database capability evidence only. It never contacts a provider,
        requests a wallet, signs, or broadcasts. The authorization expires and does
        not promote the capability to live_verified.
        """
        if confirm_authorize is not True:
            raise ValueError("confirm_r5c5b_live_authorization_required")
        normalized_symbol = _normalize_market_symbol(symbol)
        normalized_side = str(side or "").strip().lower()
        normalized_mode = str(amount_mode or "").strip().lower().replace("exact_spend", AMOUNT_MODE_EXACT_INPUT)
        normalized_provider = str(provider or "").strip().lower().replace("zerox", PROVIDER_ZEROX)
        authorized_wallet = validate_evm_address(wallet_address).lower()
        if (
            normalized_symbol != R5C4B_SYMBOL
            or normalized_side != R5C4B_SIDE
            or normalized_mode != AMOUNT_MODE_EXACT_INPUT
            or normalized_provider != PROVIDER_ZEROX
            or str(requested_amount or "").strip() != R5C4B_INPUT_AMOUNT
        ):
            raise ValueError("r5c5b_live_authorization_target_locked")

        objective = (
            db.query(RobinhoodChainPairObjective)
            .filter(
                RobinhoodChainPairObjective.symbol == normalized_symbol,
                RobinhoodChainPairObjective.enabled.is_(True),
            )
            .first()
        )
        if objective is None:
            raise ValueError("robinhood_chain_pair_objective_not_found")
        base_row, quote_row = self._objective_tokens(db, objective)
        base_identity = self.token_identity(db, base_row)
        quote_identity = self.token_identity(db, quote_row)
        input_row, input_identity = base_row, base_identity
        output_row, output_identity = quote_row, quote_identity
        if (
            str(input_identity.get("symbol") or "").strip().upper() != R5C4B_INPUT_ASSET
            or bool(input_identity.get("native"))
            or str(output_identity.get("symbol") or "").strip().upper() != R5C4B_OUTPUT_ASSET
            or bool(output_identity.get("native"))
        ):
            raise ValueError("r5c5b_token_registry_identity_mismatch")

        capability = (
            db.query(RobinhoodChainPairCapability)
            .filter(
                RobinhoodChainPairCapability.objective_id == objective.id,
                RobinhoodChainPairCapability.from_token_registry_id == int(input_row.id),
                RobinhoodChainPairCapability.to_token_registry_id == int(output_row.id),
                RobinhoodChainPairCapability.amount_mode == AMOUNT_MODE_EXACT_INPUT,
                RobinhoodChainPairCapability.provider == PROVIDER_ZEROX,
            )
            .first()
        )
        if capability is None:
            raise ValueError("r5c5b_direction_capability_missing")
        evidence = copy.deepcopy(capability.evidence) if isinstance(capability.evidence, dict) else {}
        if (
            not bool(capability.enabled)
            or str(capability.indicative_status or "").strip().lower() not in {"available", "live_verified"}
            or str(capability.firm_plan_status or "").strip().lower() != "available"
            or str(capability.execution_status or "").strip().lower() != PREPARATION_STATUS
            or str(capability.probe_amount or "").strip() != R5C4B_INPUT_AMOUNT
            or evidence.get("preparation_verified") is not True
            or evidence.get("live_accepted") is True
            or evidence.get("successful_broadcast") is True
            or str(evidence.get("symbol") or "").strip().upper() != R5C4B_SYMBOL
            or str(evidence.get("side") or "").strip().lower() != R5C4B_SIDE
            or str(evidence.get("amount_mode") or "").strip().lower() != AMOUNT_MODE_EXACT_INPUT
            or str(evidence.get("provider") or "").strip().lower() != PROVIDER_ZEROX
            or str(evidence.get("from_asset") or "").strip().upper() != R5C4B_INPUT_ASSET
            or str(evidence.get("to_asset") or "").strip().upper() != R5C4B_OUTPUT_ASSET
            or str(evidence.get("verified_input_amount") or "").strip() != R5C4B_INPUT_AMOUNT
            or str(evidence.get("firm_plan_input_ceiling") or "").strip() != R5C4B_INPUT_AMOUNT
        ):
            raise ValueError("r5c5b_preparation_authority_required")

        now = utc_now()
        existing = evidence.get("live_authorization") if isinstance(evidence.get("live_authorization"), dict) else {}
        existing_expires = None
        try:
            existing_expires = datetime.fromisoformat(str(existing.get("expires_at") or "").replace("Z", "+00:00")).replace(tzinfo=None)
        except ValueError:
            existing_expires = None
        if (
            str(existing.get("status") or "").strip().lower() == LIVE_AUTHORIZED_PENDING_CONFIRMATION
            and existing.get("operator_confirmed") is True
            and existing_expires is not None
            and existing_expires > now
            and evidence.get("successful_broadcast_authorized") is True
            and str(existing.get("wallet_address") or "").strip().lower() == authorized_wallet
        ):
            authority = resolve_robinhood_chain_execution_authority(
                db, symbol=normalized_symbol, side=normalized_side,
                amount_mode=normalized_mode, provider=normalized_provider,
                require_execution=True,
            )
            return {
                "ok": True,
                "idempotent": True,
                "tranche": "R5C.5B",
                "execution_authority": authority,
                "capability": self._capability_dict(db, capability),
                "database_mutated": False,
                "wallet_connection_requested": False,
                "signing_enabled": False,
                "broadcast_enabled": False,
                "successful_broadcast_authorized": True,
                "live_execution_verified": False,
                "automatic_execution_promotion": False,
                "will_mutate_chain": False,
            }

        expires_at = now + timedelta(minutes=R5C5B_AUTHORIZATION_TTL_MINUTES)
        authorization = {
            "status": LIVE_AUTHORIZED_PENDING_CONFIRMATION,
            "tranche": "R5C.5B",
            "authorization_id": uuid.uuid4().hex,
            "operator_confirmed": True,
            "authorized_at": iso_or_none(now),
            "expires_at": iso_or_none(expires_at),
            "symbol": R5C4B_SYMBOL,
            "side": R5C4B_SIDE,
            "amount_mode": AMOUNT_MODE_EXACT_INPUT,
            "provider": PROVIDER_ZEROX,
            "input_asset": R5C4B_INPUT_ASSET,
            "output_asset": R5C4B_OUTPUT_ASSET,
            "exact_input_amount": R5C4B_INPUT_AMOUNT,
            "wallet_address": authorized_wallet,
            "approval_model": "finite_exact_input",
            "unlimited_approval_enabled": False,
            "approval_transaction_value_wei": "0",
            "swap_transaction_value_wei": "0",
            "separate_wallet_requests_required": True,
            "automatic_second_transaction": False,
            "automatic_retry": False,
            "automatic_execution_promotion": False,
        }
        evidence.update({
            "successful_broadcast_authorized": True,
            "live_authorized_pending_confirmation": True,
            "live_authorization": authorization,
        })
        capability.evidence = evidence
        capability.updated_at = now
        db.add(capability)
        db.flush()
        authority = resolve_robinhood_chain_execution_authority(
            db, symbol=normalized_symbol, side=normalized_side,
            amount_mode=normalized_mode, provider=normalized_provider,
            require_execution=True,
        )
        if (
            authority.get("authority_level") != LIVE_AUTHORIZED_PENDING_CONFIRMATION
            or authority.get("successful_broadcast_authorized") is not True
            or authority.get("live_execution_verified") is not False
        ):
            raise ValueError("r5c5b_live_authorization_resolution_failed")
        db.commit()
        db.refresh(capability)
        return {
            "ok": True,
            "idempotent": False,
            "tranche": "R5C.5B",
            "execution_authority": authority,
            "capability": self._capability_dict(db, capability),
            "database_mutated": True,
            "blockchain_read_only": True,
            "provider_contacted": False,
            "wallet_connection_requested": False,
            "signing_enabled": False,
            "broadcast_enabled": False,
            "successful_broadcast_authorized": True,
            "live_execution_verified": False,
            "automatic_second_transaction": False,
            "automatic_retry": False,
            "automatic_execution_promotion": False,
            "will_mutate_chain": False,
        }

    def _upsert_historical_capability(
        self,
        db: Session,
        *,
        symbol: str,
        from_symbol: str,
        to_symbol: str,
        amount_mode: str,
        probe_amount: Optional[str],
        provider: str,
        evidence: Dict[str, Any],
    ) -> Optional[RobinhoodChainPairCapability]:
        try:
            from_row = self._registry_row_by_symbol(db, from_symbol)
            to_row = self._registry_row_by_symbol(db, to_symbol)
        except ValueError:
            return None
        parts = [part.strip().upper() for part in str(symbol or "").split("-") if part.strip()]
        try:
            base_row = self._registry_row_by_symbol(db, parts[0]) if len(parts) == 2 else to_row
            quote_row = self._registry_row_by_symbol(db, parts[1]) if len(parts) == 2 else from_row
        except ValueError:
            base_row, quote_row = to_row, from_row
        objective = (
            db.query(RobinhoodChainPairObjective)
            .filter(
                RobinhoodChainPairObjective.base_token_registry_id == int(base_row.id),
                RobinhoodChainPairObjective.quote_token_registry_id == int(quote_row.id),
            )
            .first()
        )
        if objective is None:
            objective = RobinhoodChainPairObjective(
                base_token_registry_id=int(base_row.id),
                quote_token_registry_id=int(quote_row.id),
                symbol=f"{str(base_row.symbol).strip().upper()}-{str(quote_row.symbol).strip().upper()}",
                mechanism=MECHANISM_SWAP,
                enabled=True,
                review_only=True,
                notes="Synced from confirmed Robinhood Chain execution evidence.",
            )
            db.add(objective)
            db.flush()
        row = self._capability_row(
            db,
            objective_id=objective.id,
            from_token_registry_id=int(from_row.id),
            to_token_registry_id=int(to_row.id),
            amount_mode=amount_mode,
            provider=provider or PROVIDER_ZEROX,
        )
        row.indicative_status = "live_verified"
        row.firm_plan_status = "live_verified"
        row.execution_status = "live_verified"
        row.enabled = True
        row.probe_amount = _clean_text(probe_amount, 80)
        row.provider_error = {}
        row.evidence = copy.deepcopy(evidence)
        row.last_verified_at = utc_now()
        row.updated_at = utc_now()
        return row

    def sync_execution_evidence(self, db: Session, *, confirm_sync: bool) -> Dict[str, Any]:
        if confirm_sync is not True:
            raise ValueError("confirm_execution_evidence_sync_required")
        synced: List[str] = []

        legacy_rows = (
            db.query(RobinhoodChainExecution)
            .filter(RobinhoodChainExecution.status == "confirmed")
            .all()
        )
        for row in legacy_rows:
            capability = self._upsert_historical_capability(
                db,
                symbol=row.symbol,
                from_symbol=row.input_asset,
                to_symbol=row.expected_output_asset,
                amount_mode=AMOUNT_MODE_EXACT_INPUT,
                probe_amount=row.input_amount,
                provider=PROVIDER_ZEROX,
                evidence={
                    "source_table": row.__tablename__,
                    "execution_id": row.id,
                    "transaction_hash": row.tx_hash,
                    "live_accepted": True,
                },
            )
            if capability is not None:
                synced.append(capability.id)

        swap_rows = (
            db.query(RobinhoodChainSwapExecution)
            .filter(RobinhoodChainSwapExecution.status == "confirmed")
            .all()
        )
        for row in swap_rows:
            capability = self._upsert_historical_capability(
                db,
                symbol=row.symbol,
                from_symbol=row.from_asset,
                to_symbol=row.to_asset,
                amount_mode=row.amount_mode,
                probe_amount=row.exact_input_amount,
                provider=row.provider,
                evidence={
                    "source_table": row.__tablename__,
                    "execution_id": row.id,
                    "transaction_hash": row.swap_tx_hash,
                    "approval_transaction_hash": row.approval_tx_hash,
                    "live_accepted": True,
                },
            )
            if capability is not None:
                synced.append(capability.id)

        buy_rows = (
            db.query(RobinhoodChainBuyExecution)
            .filter(RobinhoodChainBuyExecution.status == "confirmed")
            .all()
        )
        for row in buy_rows:
            capability = self._upsert_historical_capability(
                db,
                symbol=row.symbol,
                from_symbol=row.maximum_input_asset,
                to_symbol=row.exact_output_asset,
                amount_mode="exact_output",
                probe_amount=row.exact_output_amount,
                provider=PROVIDER_ZEROX,
                evidence={
                    "source_table": row.__tablename__,
                    "execution_id": row.id,
                    "transaction_hash": row.swap_tx_hash,
                    "approval_transaction_hash": row.approval_tx_hash,
                    "live_accepted": True,
                },
            )
            if capability is not None:
                synced.append(capability.id)

        db.commit()
        return {
            "ok": True,
            "synced_capability_ids": sorted(set(synced)),
            "synced_count": len(set(synced)),
            "route_capabilities": self.route_capabilities(db),
            "database_mutated": True,
            "blockchain_read_only": True,
            "historical_live_capabilities_synced": bool(synced),
            "execution_enabled_by_sync": False,
            "automatic_execution_promotion": False,
            "evidence_source": "confirmed_local_execution_records",
            "will_mutate_chain": False,
        }


_SERVICE: Optional[RobinhoodChainRegistryDiscoveryService] = None


def get_robinhood_chain_registry_discovery_service() -> RobinhoodChainRegistryDiscoveryService:
    global _SERVICE
    if _SERVICE is None:
        _SERVICE = RobinhoodChainRegistryDiscoveryService()
    return _SERVICE
