from __future__ import annotations

import copy
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional, Tuple

from sqlalchemy import text
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


ROBINHOOD_CHAIN = "robinhood_chain"
ROBINHOOD_CHAIN_VENUE = "robinhood_chain"
ROBINHOOD_CHAIN_ID = 4663
NATIVE_SYMBOL = "ETH"
AMOUNT_MODE_EXACT_INPUT = "exact_input"
MECHANISM_SWAP = "swap"
MECHANISM_WRAP_UNWRAP = "wrap_unwrap"
PROVIDER_ZEROX = "0x"
PROVIDER_NATIVE_WRAP = "native_wrap"

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
        ):
            if value.get(key) is not None:
                safe[key] = copy.deepcopy(value.get(key))
        if safe:
            return safe
    text_value = _clean_text(value, 1000)
    return {"message": text_value} if text_value else {}


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
    overrides = (
        db.query(TokenRegistry)
        .filter(
            TokenRegistry.chain == ROBINHOOD_CHAIN,
            TokenRegistry.venue == ROBINHOOD_CHAIN_VENUE,
        )
        .order_by(TokenRegistry.symbol.asc())
        .limit(250)
        .all()
    )
    globals_ = (
        db.query(TokenRegistry)
        .filter(
            TokenRegistry.chain == ROBINHOOD_CHAIN,
            ((TokenRegistry.venue.is_(None)) | (TokenRegistry.venue == "")),
        )
        .order_by(TokenRegistry.symbol.asc())
        .limit(250)
        .all()
    )
    selected: Dict[str, TokenRegistry] = {}
    for row in [*(overrides or []), *(globals_ or [])]:
        symbol = str(row.symbol or "").strip().upper()
        if symbol and symbol not in selected:
            selected[symbol] = row
    return [selected[key] for key in sorted(selected)]


class RobinhoodChainRegistryDiscoveryService:
    """TokenRegistry-backed, review-only Robinhood Chain discovery service.

    The service may write local identity/objective/capability evidence. It never
    signs, broadcasts, constructs an executable transaction, changes an
    allowance, or enables execution automatically.
    """

    def __init__(self, *, rpc_client: Any = None, discovery_service: Any = None) -> None:
        self.rpc_client = rpc_client or get_robinhood_chain_client()
        self.discovery_service = discovery_service or get_robinhood_chain_execution_discovery_service()

    def status(self, db: Session) -> Dict[str, Any]:
        return {
            "ok": True,
            "tranche": "RH-CHAIN.10D.2-R5C.1",
            "chain": ROBINHOOD_CHAIN,
            "chain_id": ROBINHOOD_CHAIN_ID,
            "token_registry_authority": True,
            "hardcoded_token_contracts": False,
            "hardcoded_pair_contracts": False,
            "asset_verification_count": db.query(RobinhoodChainRegistryVerification).count(),
            "objective_count": db.query(RobinhoodChainPairObjective).count(),
            "capability_count": db.query(RobinhoodChainPairCapability).count(),
            "supported_mechanisms": [MECHANISM_SWAP, MECHANISM_WRAP_UNWRAP],
            "supported_amount_modes": [AMOUNT_MODE_EXACT_INPUT],
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
        normalized = str(symbol or "").strip().upper()
        if not normalized:
            raise ValueError("robinhood_chain_registry_symbol_required")
        for row in self.registry_rows(db):
            if str(row.symbol or "").strip().upper() == normalized:
                return row
        raise ValueError("robinhood_chain_registry_token_not_found")

    def token_identity(self, db: Session, row: TokenRegistry) -> Dict[str, Any]:
        symbol = str(row.symbol or "").strip().upper()
        if not symbol:
            raise ValueError("robinhood_chain_registry_symbol_required")
        try:
            decimals = int(row.decimals)
        except Exception as exc:
            raise ValueError("invalid_robinhood_chain_registry_decimals") from exc
        if decimals < 0 or decimals > 18:
            raise ValueError("invalid_robinhood_chain_registry_decimals")

        raw_address = str(row.address or "").strip()
        native = not raw_address
        if native:
            if symbol != NATIVE_SYMBOL or decimals != 18:
                raise ValueError("invalid_robinhood_chain_native_registry_identity")
            contract_address = ZEROX_NATIVE_TOKEN
            asset_kind = "native"
        else:
            contract_address = validate_evm_address(raw_address)
            asset_kind = "erc20"

        price_meta = _registry_external_price_meta(db, int(row.id))
        return {
            "registry_id": int(row.id),
            "registry_venue": row.venue,
            "registry_status": "registered",
            "identity_source": "token_registry",
            "symbol": symbol,
            "label": row.label,
            "contract_address": contract_address,
            "registry_contract_address": None if native else contract_address,
            "decimals": decimals,
            "native": native,
            "asset_kind": asset_kind,
            **price_meta,
        }

    def resolve_token(self, db: Session, symbol: str) -> Dict[str, Any]:
        return self.token_identity(db, self._registry_row_by_symbol(db, symbol))

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
            identity["verification"] = self._verification_dict(self._verification_row(db, int(row.id)))
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
            registry_match = identity["symbol"] == NATIVE_SYMBOL and identity["decimals"] == 18
            canonical_status = "verified" if registry_match else "registry_mismatch"
            verification_error = None if registry_match else "native_registry_identity_mismatch"
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

    @staticmethod
    def _market_indicative_state(capabilities: List[Dict[str, Any]]) -> str:
        statuses = {
            str(item.get("indicative_status") or "").strip().lower()
            for item in capabilities
            if isinstance(item, dict)
        }
        if "live_verified" in statuses:
            return "live_verified"
        if "available" in statuses:
            return "available"
        if "mechanism_configured" in statuses:
            return "mechanism_configured"
        if "provider_error" in statuses:
            return "provider_error"
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
            available_statuses = {"available", "live_verified"}
            available_directions = {
                (
                    str(item.get("from_asset") or "").strip().upper(),
                    str(item.get("to_asset") or "").strip().upper(),
                )
                for item in capabilities
                if str(item.get("indicative_status") or "").strip().lower() in available_statuses
            }
            provider_errors = [
                item for item in capabilities
                if str(item.get("indicative_status") or "").strip().lower() == "provider_error"
            ]
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
                and expected_directions
                and expected_directions.issubset(available_directions)
            )
            if orderbook_enabled:
                orderbook_reason = None
            elif mechanism == MECHANISM_WRAP_UNWRAP:
                orderbook_reason = "wrap_unwrap_uses_dedicated_mechanism_view"
            elif provider_errors:
                orderbook_reason = "provider_error"
            else:
                orderbook_reason = "both_exact_input_directions_not_available"

            providers = sorted({
                str(item.get("provider") or "").strip()
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
                "orderbook_enabled": orderbook_enabled,
                "orderbook_reason": orderbook_reason,
                "mechanism_configured": mechanism_configured,
                "execution_enabled": bool(live_verified),
                "automatic_execution_promotion": False,
                "available_direction_count": len(available_directions),
                "live_verified_direction_count": len(live_verified),
                "provider_error_direction_count": len(provider_errors),
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
    ) -> Dict[str, Any]:
        if confirm_create is not True:
            raise ValueError("confirm_pair_objective_create_required")
        base_row = self._registry_row_by_id(db, base_token_registry_id)
        quote_row = self._registry_row_by_id(db, quote_token_registry_id)
        if int(base_row.id) == int(quote_row.id):
            raise ValueError("pair_objective_assets_must_differ")
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
        if row is None:
            row = RobinhoodChainPairObjective(
                base_token_registry_id=int(base_row.id),
                quote_token_registry_id=int(quote_row.id),
                symbol=symbol,
            )
            db.add(row)
        row.symbol = symbol
        row.mechanism = normalized_mechanism
        row.enabled = True
        row.review_only = True
        row.notes = _clean_text(notes)
        row.updated_at = utc_now()
        db.commit()
        db.refresh(row)
        return {
            "ok": True,
            "objective": self._objective_dict(db, row),
            "database_mutated": True,
            "blockchain_read_only": True,
            "execution_enabled": False,
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
        row = (
            db.query(RobinhoodChainPairCapability)
            .filter(
                RobinhoodChainPairCapability.objective_id == objective_id,
                RobinhoodChainPairCapability.from_token_registry_id == int(from_token_registry_id),
                RobinhoodChainPairCapability.to_token_registry_id == int(to_token_registry_id),
                RobinhoodChainPairCapability.amount_mode == amount_mode,
                RobinhoodChainPairCapability.provider == provider,
            )
            .first()
        )
        if row is None:
            row = RobinhoodChainPairCapability(
                objective_id=objective_id,
                from_token_registry_id=int(from_token_registry_id),
                to_token_registry_id=int(to_token_registry_id),
                amount_mode=amount_mode,
                provider=provider,
            )
            db.add(row)
        return row

    def _capability_dict(self, db: Session, row: RobinhoodChainPairCapability) -> Dict[str, Any]:
        from_row = self._registry_row_by_id(db, int(row.from_token_registry_id))
        to_row = self._registry_row_by_id(db, int(row.to_token_registry_id))
        objective = self._objective_row(db, row.objective_id)
        from_symbol = str(from_row.symbol or "").strip().upper()
        to_symbol = str(to_row.symbol or "").strip().upper()
        display_mode = "exact_spend" if row.amount_mode == AMOUNT_MODE_EXACT_INPUT else row.amount_mode
        reason = None
        if row.execution_status != "live_verified":
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
            "indicative_status": row.indicative_status,
            "firm_plan_status": row.firm_plan_status,
            "execution_status": row.execution_status,
            "enabled": bool(row.enabled),
            "route_sources": copy.deepcopy(row.route_sources) if isinstance(row.route_sources, dict) else {},
            "probe_amount": row.probe_amount,
            "price_impact_bps": row.price_impact_bps,
            "provider_error": copy.deepcopy(row.provider_error) if isinstance(row.provider_error, dict) else {},
            "backoff_until": iso_or_none(row.backoff_until),
            "evidence": copy.deepcopy(row.evidence) if isinstance(row.evidence, dict) else {},
            "last_verified_at": iso_or_none(row.last_verified_at),
            "reason": reason,
            "review_only": True,
            "execution_enabled": bool(row.enabled and row.execution_status == "live_verified"),
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
        row.indicative_status = "available" if result.get("ok") else "provider_error"
        row.firm_plan_status = "not_tested"
        row.execution_status = "disabled"
        row.enabled = False
        row.route_sources = {"sources": sources, "fill_count": len(fills)}
        row.probe_amount = probe_amount
        row.price_impact_bps = price_impact_float
        row.provider_error = {} if result.get("ok") else _json_safe_error(result)
        backoff_raw = _clean_text(result.get("backoff_until"), 128)
        row.backoff_until = None
        if backoff_raw:
            try:
                row.backoff_until = datetime.fromisoformat(backoff_raw.replace("Z", "+00:00")).replace(tzinfo=None)
            except Exception:
                row.backoff_until = None
        row.evidence = {
            "liquidity_available": bool(result.get("liquidity_available")),
            "sell_amount": result.get("sell_amount"),
            "buy_amount": result.get("buy_amount"),
            "price_buy_per_sell": result.get("price_buy_per_sell"),
            "provider_warnings": list(result.get("provider_warnings") or [])[:20],
            "provider_contacted": result.get("provider_contacted", True),
            "read_only": True,
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
