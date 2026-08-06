# backend/app/routers/venues.py

from __future__ import annotations

from fastapi import APIRouter, Query
import os

from ..config import settings
from ..venues.registry import list_venues

router = APIRouter(prefix="/api/venues", tags=["venues"])


def _env_bool(name: str) -> bool:
    return str(os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _live_venues_set() -> set[str]:
    try:
        return {str(x or "").strip().lower() for x in settings.live_venues_set()}
    except Exception:
        raw = getattr(settings, "live_venues", None) or os.getenv("LIVE_VENUES") or ""
        return {x.strip().lower() for x in str(raw).split(",") if x.strip()}


def _cexius_trade_scope_enabled() -> bool:
    """Return only a non-secret boolean for the Cexius vault trade scope."""
    fn = getattr(settings, "_vault_latest_credential_record", None)
    if not callable(fn):
        return False
    try:
        record = fn("cexius")
    except Exception:
        return False
    if not isinstance(record, dict):
        return False
    bundle = record.get("bundle")
    token_present = bool(
        isinstance(bundle, dict)
        and str(bundle.get("api_secret") or bundle.get("api_key") or "").strip()
    )
    return bool(record.get("scope_read") and record.get("scope_trade") and token_present)


def _trade_gate_for_venue(spec, *, enabled: bool) -> dict:
    """Return non-secret live-trading gate status for UI display.

    This only reports booleans/config state that the operator already controls.
    It never returns API keys, passphrases, account identifiers, balances, or
    exchange private data.
    """
    venue = str(getattr(spec, "key", "") or "").strip().lower()
    supports_trading = bool(getattr(spec, "supports_trading", False))
    dry_run = bool(getattr(settings, "dry_run", True))
    armed = bool(getattr(settings, "armed", False))
    live_venues = _live_venues_set()
    live_venues_includes_venue = bool(venue and venue in live_venues)

    is_okx = venue == "okx"
    okx_enable_trading = _env_bool("OKX_ENABLE_TRADING") if is_okx else None
    is_cexius = venue == "cexius"
    cexius_trade_scope_enabled = _cexius_trade_scope_enabled() if is_cexius else None

    effective_live_submit_enabled = bool(
        supports_trading
        and bool(enabled)
        and not dry_run
        and armed
        and live_venues_includes_venue
        and ((not is_okx) or bool(okx_enable_trading))
        and ((not is_cexius) or bool(cexius_trade_scope_enabled))
    )

    missing = []
    if not supports_trading:
        missing.append("venue_supports_trading")
    if not enabled:
        missing.append("venue_enabled")
    if dry_run:
        missing.append("DRY_RUN=false")
    if not armed:
        missing.append("ARMED=true")
    if not live_venues_includes_venue:
        missing.append(f"LIVE_VENUES includes {venue}")
    if is_okx and not okx_enable_trading:
        missing.append("OKX_ENABLE_TRADING=1")
    if is_cexius and not cexius_trade_scope_enabled:
        missing.append("Cexius credential read=true and trade=true")

    return {
        "version": "trade_gate_v1",
        "venue": venue,
        "supports_trading": supports_trading,
        "venue_enabled": bool(enabled),
        "dry_run": dry_run,
        "armed": armed,
        "live_venues_includes_venue": live_venues_includes_venue,
        "okx_enable_trading": okx_enable_trading,
        "cexius_trade_scope_enabled": cexius_trade_scope_enabled,
        "effective_live_submit_enabled": effective_live_submit_enabled,
        "missing_requirements": missing,
    }


@router.get("")
@router.get("/")
def get_venues(
    include_disabled: bool = Query(True, description="If true, include disabled venues in the response."),
):
    """
    Frontend should call this instead of hardcoding venue lists.

    Returns:
      [
        {
          "venue": "gemini",
          "display_name": "Gemini",
          "enabled": true,
          "supports": { ... }
        },
        ...
      ]
    """
    reg = list_venues(include_disabled=include_disabled)

    out = []
    for k, spec in sorted(reg.items(), key=lambda x: x[0]):
        enabled = False
        try:
            enabled = bool(spec.enabled())
        except Exception:
            enabled = False

        out.append(
            {
                "venue": spec.key,
                "display_name": spec.display_name,
                "enabled": enabled,
                "supports": {
                    "trading": bool(spec.supports_trading),
                    "balances": bool(spec.supports_balances),
                    "orderbook": bool(spec.supports_orderbook),
                    "markets": bool(spec.supports_markets),
                },
                "trade_gate": _trade_gate_for_venue(spec, enabled=enabled),
            }
        )

    return out
