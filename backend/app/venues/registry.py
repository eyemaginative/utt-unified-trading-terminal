# backend/app/venues/registry.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, Optional, Any

from ..config import settings


@dataclass(frozen=True)
class VenueSpec:
    """
    Canonical registry entry for a venue.

    - key: canonical venue id used everywhere ("gemini", "kraken", "coinbase", "robinhood", "dex_trade")
    - display_name: human readable
    - enabled: function returning bool (guards whether it should appear in UI / be usable)
    - adapter_factory: zero-arg callable returning an ExchangeAdapter instance
    - supports_*: capability flags for UI/feature gating (non-breaking defaults)
    """
    key: str
    display_name: str
    enabled: Callable[[], bool]
    adapter_factory: Callable[[], Any]

    supports_trading: bool = True
    supports_balances: bool = True
    supports_orderbook: bool = True
    supports_markets: bool = True  # for /api/market/markets style listings, discovery, etc.


def _always_enabled() -> bool:
    return True


def _always_disabled() -> bool:
    """
    Hard-disable helper for closure testing.
    Wire a venue's `enabled=` to this temporarily when you want to prove
    the registry drives the dropdown.
    """
    return False


def _robinhood_enabled() -> bool:
    # Guarded by your config method (won’t show unless properly configured)
    try:
        return bool(settings.robinhood_effective_enabled())
    except Exception:
        return False


def _dex_trade_enabled() -> bool:
    # Guarded by your config method (won’t show unless properly configured)
    try:
        return bool(settings.dex_trade_effective_enabled())
    except Exception:
        return False


def _cryptocom_enabled() -> bool:
    """
    Crypto.com Exchange is enabled if credentials are present.
    We do NOT require a separate CRYPTOCOM_ENABLED flag to keep onboarding simple.
    """
    try:
        k = (getattr(settings, "cryptocom_exchange_api_key", None) or "").strip()
        s = (getattr(settings, "cryptocom_exchange_api_secret", None) or "").strip()
        if k and s:
            return True
    except Exception:
        pass

    # Fallback to env vars directly (works even if config.py doesn't declare these yet)
    import os
    k2 = (os.getenv("CRYPTOCOM_EXCHANGE_API_KEY") or "").strip()
    s2 = (os.getenv("CRYPTOCOM_EXCHANGE_API_SECRET") or "").strip()
    return bool(k2 and s2)


def _make_registry() -> Dict[str, VenueSpec]:
    """
    Build the registry lazily to avoid import-time circular dependencies
    between adapters and services.
    """

    def gemini_factory():
        from ..adapters.gemini import GeminiAdapter
        return GeminiAdapter()

    def kraken_factory():
        from ..adapters.kraken import KrakenAdapter
        return KrakenAdapter()

    def coinbase_factory():
        from ..adapters.coinbase import CoinbaseAdapter
        return CoinbaseAdapter()

    def robinhood_factory():
        from ..adapters.robinhood import RobinhoodAdapter
        return RobinhoodAdapter()

    def dex_trade_factory():
        from ..adapters.dex_trade import DexTradeAdapter
        return DexTradeAdapter()

    def cryptocom_factory():
        from ..adapters.cryptocom_exchange import CryptoComExchangeAdapter
        return CryptoComExchangeAdapter()

    def solana_dex_factory():
        # On-chain Solana DEX / aggregator execution + reads
        from ..adapters.solana_onchain import SolanaDexAdapter
        return SolanaDexAdapter()

    reg: Dict[str, VenueSpec] = {
        "gemini": VenueSpec(
            key="gemini",
            display_name="Gemini",
            enabled=_always_enabled,
            adapter_factory=gemini_factory,
            supports_trading=True,
            supports_balances=True,
            supports_orderbook=True,
            supports_markets=True,
        ),
        "kraken": VenueSpec(
            key="kraken",
            display_name="Kraken",
            enabled=_always_enabled,
            adapter_factory=kraken_factory,
            supports_trading=True,
            supports_balances=True,
            supports_orderbook=True,
            supports_markets=True,
        ),
        "coinbase": VenueSpec(
            key="coinbase",
            display_name="Coinbase",
            enabled=_always_enabled,
            adapter_factory=coinbase_factory,
            supports_trading=True,
            supports_balances=True,
            supports_orderbook=True,
            supports_markets=True,
        ),

        # Optional/guarded venues
        "robinhood": VenueSpec(
            key="robinhood",
            display_name="Robinhood",
            enabled=_robinhood_enabled,
            adapter_factory=robinhood_factory,
            supports_trading=True,
            supports_balances=True,
            supports_orderbook=True,  # your market.py already supports a pricing fallback for robinhood
            supports_markets=False,
        ),
        "dex_trade": VenueSpec(
            key="dex_trade",
            display_name="Dex-Trade",
            enabled=_dex_trade_enabled,
            adapter_factory=dex_trade_factory,
            supports_trading=True,
            supports_balances=True,
            supports_orderbook=True,
            supports_markets=True,
        ),

        # NEW: Crypto.com Exchange (REST)
        "cryptocom": VenueSpec(
            key="cryptocom",
            display_name="Crypto.com",
            enabled=_cryptocom_enabled,
            adapter_factory=cryptocom_factory,
            supports_trading=True,
            supports_balances=True,
            supports_orderbook=True,  # public/get-book
            supports_markets=True,    # instruments discovery via public/get-instruments
        ),

        # NEW: Solana on-chain DEX (reads first; trading wired later)
        "solana_dex": VenueSpec(
            key="solana_dex",
            display_name="Solana DEX",
            enabled=_always_enabled,
            adapter_factory=solana_dex_factory,
            supports_trading=False,   # flip to True once swap execution is wired end-to-end
            supports_balances=True,
            supports_orderbook=False, # not via your centralized orderbook path initially
            supports_markets=False,   # add later if we want /api/market/markets integration
        ),

        # NEW: Solana Jupiter (DEX execution venue; initially mirrors Solana DEX read capabilities)
        # NOTE: We intentionally keep orderbook/trading disabled until the Jupiter quote + sign/send
        # + ack/reconcile plumbing is wired, so selecting this venue cannot break existing widgets.
        "solana_jupiter": VenueSpec(
            key="solana_jupiter",
            display_name="Solana-Jupiter",
            enabled=_always_enabled,
            adapter_factory=solana_dex_factory,
            supports_trading=False,
            supports_balances=True,
            supports_orderbook=False,
            supports_markets=False,
        ),
    }

    return reg


# module-level singleton (built on first use)
_REGISTRY: Optional[Dict[str, VenueSpec]] = None


def venue_registry() -> Dict[str, VenueSpec]:
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = _make_registry()
    return _REGISTRY


def normalize_venue(v: str) -> str:
    return (v or "").strip().lower()


def get_venue_spec(venue: str) -> VenueSpec:
    v = normalize_venue(venue)
    reg = venue_registry()
    if v not in reg:
        raise KeyError(f"Unknown venue '{venue}'. Known: {', '.join(sorted(reg.keys()))}")
    return reg[v]


def is_venue_enabled(venue: str) -> bool:
    try:
        spec = get_venue_spec(venue)
        return bool(spec.enabled())
    except Exception:
        return False


def list_venues(include_disabled: bool = True) -> Dict[str, VenueSpec]:
    """
    Returns {venue_key: spec}. Optionally filter to enabled-only.
    """
    reg = venue_registry()
    if include_disabled:
        return dict(reg)

    out: Dict[str, VenueSpec] = {}
    for k, spec in reg.items():
        try:
            if spec.enabled():
                out[k] = spec
        except Exception:
            continue
    return out
