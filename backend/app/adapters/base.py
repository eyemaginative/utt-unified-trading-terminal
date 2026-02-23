from abc import ABC, abstractmethod
from typing import List, Optional, TypedDict, Dict, Any
from datetime import datetime


class PlacedOrder(TypedDict, total=False):
    """
    Minimal normalized response for an attempted order placement.
    Adapters may add extra fields (total=False).
    """
    venue_order_id: str
    status: str  # acked/open/filled/etc.
    reject_reason: str
    raw: dict

    # Optional convenience for unified cancel-by-ref flows.
    cancel_ref: str
    status_raw: str


class BalanceItem(TypedDict):
    asset: str
    total: float
    available: float
    hold: Optional[float]


class OrderBookLevel(TypedDict):
    price: float
    qty: float


class OrderBook(TypedDict):
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]


class VenueOrder(TypedDict, total=False):
    """
    Normalized venue order record (read-only).
    Adapters should return a list of these for fetch_orders().

    total=False so adapters can omit fields they don't have.
    """
    venue: str
    venue_order_id: str

    symbol_canon: str
    symbol_venue: str

    side: str
    type: str
    status: str

    # For debugging / reconciliation
    status_raw: str

    # Used by unified cancel routing (e.g., "coinbase:...", "kraken:...", "robinhood:...")
    cancel_ref: str

    qty: float
    filled_qty: float

    limit_price: Optional[float]
    avg_fill_price: Optional[float]

    fee: Optional[float]
    fee_asset: Optional[str]

    # Prefer datetime objects internally; serializers can convert to ISO strings.
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


class OrderRules(TypedDict, total=False):
    """
    Normalized, best-effort venue trading rules for a specific symbol_venue.
    Used by the frontend to warn users before submitting.

    Not all venues provide all fields; omit when unknown.
    """
    symbol_venue: str

    # Increments / precision
    base_increment: Optional[float]
    price_increment: Optional[float]
    qty_decimals: Optional[int]
    price_decimals: Optional[int]

    # Limits
    min_qty: Optional[float]
    max_qty: Optional[float]
    min_notional: Optional[float]
    max_notional: Optional[float]

    # Capabilities
    supports_post_only: bool
    supported_tifs: List[str]
    supported_order_types: List[str]

    # Optional raw payload for debugging
    raw: Dict[str, Any]


class ExchangeAdapter(ABC):
    venue: str

    @abstractmethod
    def resolve_symbol(self, symbol_canon: str) -> str:
        """Map canonical symbol to venue symbol"""

    @abstractmethod
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
        """
        Place an order at the venue.

        tif/post_only are optional so venues/adapters that do not support them can ignore.
        """
        ...

    @abstractmethod
    def cancel_order(self, venue_order_id: str, dry_run: bool) -> bool:
        ...

    @abstractmethod
    def fetch_balances(self, dry_run: bool) -> List[BalanceItem]:
        ...

    @abstractmethod
    def fetch_orderbook(self, symbol_venue: str, depth: int, dry_run: bool) -> OrderBook:
        ...

    @abstractmethod
    def fetch_orders(self, dry_run: bool) -> List[VenueOrder]:
        """
        Read-only venue orders ingestion.
        Returns a list of normalized VenueOrder dicts.
        """
        ...

    # ─────────────────────────────────────────────────────────────
    # Phase 1: Discovery + Rules normalization (best-effort)
    # ─────────────────────────────────────────────────────────────
    def list_symbols(self) -> List[str]:
        """
        Returns canonical symbols (BASE-QUOTE) supported by the venue (best-effort).
        Default: empty list (adapters should override).
        """
        return []

    def get_order_rules(self, symbol_venue: str) -> OrderRules:
        """
        Returns normalized venue rules for symbol_venue (best-effort).
        Default: empty dict (adapters should override).
        """
        _ = symbol_venue
        return {}
