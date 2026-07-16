from __future__ import annotations

from typing import List, Optional

from .base import BalanceItem, ExchangeAdapter, OrderBook, PlacedOrder, VenueOrder


class RobinhoodChainAdapter(ExchangeAdapter):
    """Fail-closed registry adapter for the Robinhood Chain read-only foundation.

    Chain diagnostics and RPC reads are intentionally exposed through the
    dedicated /api/robinhood_chain router.  Generic exchange trading, balance,
    order-book, and order-history paths remain disabled in RH-CHAIN.0A/1.
    """

    venue = "robinhood_chain"
    chain_id = 4663
    native_currency = "ETH"
    explorer_url = "https://robinhoodchain.blockscout.com"

    def resolve_symbol(self, symbol_canon: str) -> str:
        return str(symbol_canon or "").strip().upper()

    def place_order(
        self,
        symbol_venue: str,
        side: str,
        type_: str,
        qty: float,
        limit_price: Optional[float],
        client_order_id: str,
        dry_run: bool,
        tif: Optional[str] = None,
        post_only: bool = False,
    ) -> PlacedOrder:
        _ = (symbol_venue, side, type_, qty, limit_price, client_order_id, dry_run, tif, post_only)
        raise RuntimeError("Robinhood Chain trading is not enabled")

    def cancel_order(self, venue_order_id: str, dry_run: bool) -> bool:
        _ = (venue_order_id, dry_run)
        return False

    def fetch_balances(self, dry_run: bool) -> List[BalanceItem]:
        _ = dry_run
        raise RuntimeError("Robinhood Chain generic balances are not enabled; use the dedicated read-only EVM path")

    def fetch_orderbook(self, symbol_venue: str, depth: int, dry_run: bool) -> OrderBook:
        _ = (symbol_venue, depth, dry_run)
        raise RuntimeError("Robinhood Chain order books are not enabled")

    def fetch_orders(self, dry_run: bool) -> List[VenueOrder]:
        _ = dry_run
        return []
