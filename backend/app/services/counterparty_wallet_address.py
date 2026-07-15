from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ..config import settings
from ..db import SessionLocal
from ..models import WalletAddress


_COUNTERPARTY_WALLET_IDS = {
    "counterparty",
    "counterparty_default",
    "counterparty_unisat",
}
_COUNTERPARTY_NETWORKS = {
    "counterparty",
    "bitcoin_counterparty",
    "btc_counterparty",
    "xcp",
}
_LEGACY_BITCOIN_NETWORKS = {
    "bitcoin",
    "btc",
    "mainnet",
    "bitcoin_mainnet",
}
_ACCOUNT_ASSETS = {"ALL", "*", "WALLET"}
_LEGACY_ACCOUNT_ASSETS = {*_ACCOUNT_ASSETS, "BTC"}
_USER_OWNER_SCOPES = {"user", "self", "self_custody", "personal", "default"}


def _norm(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _asset(value: Any) -> str:
    return str(value or "").strip().upper()


def _environment_counterparty_address() -> str:
    """Preserve the pre-CP-WALLET.1 environment fallback during migration."""
    for name in (
        "counterparty_effective_source_address",
        "counterparty_effective_wallet_address",
    ):
        fn = getattr(settings, name, None)
        if callable(fn):
            try:
                value = str(fn() or "").strip()
                if value:
                    return value
            except Exception:
                pass

    for name in ("counterparty_source_address", "counterparty_wallet_address"):
        try:
            value = str(getattr(settings, name, None) or "").strip()
            if value:
                return value
        except Exception:
            pass

    for name in (
        "COUNTERPARTY_SOURCE_ADDRESS",
        "COUNTERPARTY_WALLET_ADDRESS",
        "COUNTERPARTY_ADDRESS",
    ):
        value = str(os.getenv(name) or "").strip()
        if value:
            return value
    return ""


def _row_value(row: Any, name: str) -> Any:
    if isinstance(row, dict):
        return row.get(name)
    return getattr(row, name, None)


def _created_rank(row: Any) -> float:
    value = _row_value(row, "created_at")
    if isinstance(value, datetime):
        try:
            return value.timestamp()
        except Exception:
            return 0.0
    return 0.0


def _row_summary(row: Any) -> Dict[str, Any]:
    return {
        "wallet_address_id": str(_row_value(row, "id") or "") or None,
        "wallet_id": str(_row_value(row, "wallet_id") or "").strip() or None,
        "network": str(_row_value(row, "network") or "").strip() or None,
        "asset_scope": _asset(_row_value(row, "asset")) or None,
        "address": str(_row_value(row, "address") or "").strip() or None,
        "label": str(_row_value(row, "label") or "").strip() or None,
        "owner_scope": str(_row_value(row, "owner_scope") or "").strip() or None,
        "created_at": _row_value(row, "created_at"),
    }


def _is_user_row(row: Any) -> bool:
    owner = _norm(_row_value(row, "owner_scope"))
    return not owner or owner in _USER_OWNER_SCOPES


def _is_exact_counterparty_row(row: Any) -> bool:
    return (
        _is_user_row(row)
        and _norm(_row_value(row, "wallet_id")) == "counterparty"
        and _norm(_row_value(row, "network")) in _COUNTERPARTY_NETWORKS
        and _asset(_row_value(row, "asset")) in _ACCOUNT_ASSETS
        and bool(str(_row_value(row, "address") or "").strip())
    )


def _is_legacy_counterparty_row(row: Any) -> bool:
    return (
        _is_user_row(row)
        and _norm(_row_value(row, "wallet_id")) in _COUNTERPARTY_WALLET_IDS
        and _norm(_row_value(row, "network")) in _LEGACY_BITCOIN_NETWORKS
        and _asset(_row_value(row, "asset")) in _LEGACY_ACCOUNT_ASSETS
        and bool(str(_row_value(row, "address") or "").strip())
    )


def _resolve_selected_rows(
    rows: Iterable[Any],
    *,
    tier: str,
    environment_address: str,
) -> Optional[Dict[str, Any]]:
    candidates = [row for row in rows if (_is_exact_counterparty_row(row) if tier == "exact" else _is_legacy_counterparty_row(row))]
    if not candidates:
        return None

    candidates.sort(key=_created_rank, reverse=True)
    by_address: Dict[str, List[Any]] = {}
    for row in candidates:
        address = str(_row_value(row, "address") or "").strip()
        by_address.setdefault(address.lower(), []).append(row)

    if len(by_address) > 1:
        summaries = [_row_summary(group[0]) for group in by_address.values()]
        return {
            "ok": False,
            "error": "counterparty_wallet_address_ambiguous",
            "message": (
                "Multiple distinct Counterparty account addresses are configured in Wallet Addresses. "
                "Keep one authoritative account-level row before refreshing balances or All Orders."
            ),
            "address": None,
            "address_source": "wallet_addresses",
            "priority_tier": tier,
            "matching_row_count": len(candidates),
            "distinct_address_count": len(by_address),
            "candidates": summaries,
            "environment_fallback": False,
            "environment_address_configured": bool(environment_address),
            "environment_address_matches": None,
            "read_only": True,
            "database_mutation": False,
        }

    selected_group = next(iter(by_address.values()))
    selected = selected_group[0]
    summary = _row_summary(selected)
    address = str(summary.get("address") or "").strip()
    environment_matches = (
        address.lower() == environment_address.lower()
        if environment_address
        else None
    )
    warnings: List[str] = []
    if environment_address and environment_matches is False:
        warnings.append(
            "Wallet Addresses is authoritative and differs from the legacy Counterparty environment address."
        )

    return {
        "ok": True,
        "error": None,
        "message": None,
        "address": address,
        "address_source": "wallet_addresses",
        "priority_tier": tier,
        **summary,
        "matching_row_count": len(candidates),
        "distinct_address_count": 1,
        "environment_fallback": False,
        "environment_address_configured": bool(environment_address),
        "environment_address_matches": environment_matches,
        "warnings": warnings,
        "resolution_key": f"wallet_addresses:{summary.get('wallet_address_id') or address.lower()}",
        "read_only": True,
        "database_mutation": False,
    }


def resolve_counterparty_wallet_address(
    db: Optional[Session] = None,
    *,
    allow_environment_fallback: bool = True,
) -> Dict[str, Any]:
    """Resolve the authoritative Counterparty account address.

    Priority:
      1. Wallet Addresses: wallet_id=counterparty, network=counterparty,
         account-level asset scope (ALL/*/WALLET).
      2. Compatible legacy Counterparty wallet rows on a Bitcoin network.
      3. Temporary environment fallback during migration.

    The resolver is read-only and fails closed when a selected priority tier
    contains more than one distinct address.
    """
    environment_address = _environment_counterparty_address()
    owns_session = db is None
    session = db or SessionLocal()
    try:
        stmt = (
            select(WalletAddress)
            .where(
                func.lower(func.coalesce(WalletAddress.wallet_id, "")).in_(
                    sorted(_COUNTERPARTY_WALLET_IDS)
                )
            )
            .order_by(WalletAddress.created_at.desc())
            .limit(2000)
        )
        rows = list(session.execute(stmt).scalars().all())
    except Exception as exc:
        if allow_environment_fallback and environment_address:
            return {
                "ok": True,
                "error": None,
                "message": None,
                "address": environment_address,
                "address_source": "environment",
                "priority_tier": "environment_fallback_after_wallet_lookup_error",
                "wallet_address_id": None,
                "wallet_id": None,
                "network": None,
                "asset_scope": None,
                "label": None,
                "owner_scope": None,
                "matching_row_count": 0,
                "distinct_address_count": 0,
                "environment_fallback": True,
                "environment_address_configured": True,
                "environment_address_matches": True,
                "warnings": [f"Wallet Addresses lookup failed; legacy environment fallback used: {str(exc)[:300]}"],
                "resolution_key": f"environment:{environment_address.lower()}",
                "read_only": True,
                "database_mutation": False,
            }
        return {
            "ok": False,
            "error": "counterparty_wallet_address_lookup_failed",
            "message": f"Wallet Addresses lookup failed: {str(exc)[:300]}",
            "address": None,
            "address_source": "wallet_addresses",
            "priority_tier": None,
            "matching_row_count": 0,
            "distinct_address_count": 0,
            "environment_fallback": False,
            "environment_address_configured": bool(environment_address),
            "environment_address_matches": None,
            "read_only": True,
            "database_mutation": False,
        }
    finally:
        if owns_session:
            session.close()

    exact = _resolve_selected_rows(rows, tier="exact", environment_address=environment_address)
    if exact is not None:
        return exact

    legacy = _resolve_selected_rows(rows, tier="legacy_bitcoin", environment_address=environment_address)
    if legacy is not None:
        return legacy

    if allow_environment_fallback and environment_address:
        return {
            "ok": True,
            "error": None,
            "message": None,
            "address": environment_address,
            "address_source": "environment",
            "priority_tier": "environment_fallback",
            "wallet_address_id": None,
            "wallet_id": None,
            "network": None,
            "asset_scope": None,
            "label": None,
            "owner_scope": None,
            "matching_row_count": 0,
            "distinct_address_count": 0,
            "environment_fallback": True,
            "environment_address_configured": True,
            "environment_address_matches": True,
            "warnings": [
                "No compatible Counterparty Wallet Addresses row was found; the legacy environment address is still in use."
            ],
            "resolution_key": f"environment:{environment_address.lower()}",
            "read_only": True,
            "database_mutation": False,
        }

    return {
        "ok": False,
        "error": "counterparty_wallet_address_missing",
        "message": (
            "Add one Wallet Addresses row with Asset ALL, Venue counterparty, "
            "Network counterparty, and the Bitcoin/UniSat address."
        ),
        "address": None,
        "address_source": "missing",
        "priority_tier": None,
        "wallet_address_id": None,
        "wallet_id": None,
        "network": None,
        "asset_scope": None,
        "label": None,
        "owner_scope": None,
        "matching_row_count": 0,
        "distinct_address_count": 0,
        "environment_fallback": False,
        "environment_address_configured": False,
        "environment_address_matches": None,
        "warnings": [],
        "resolution_key": "missing",
        "read_only": True,
        "database_mutation": False,
    }
