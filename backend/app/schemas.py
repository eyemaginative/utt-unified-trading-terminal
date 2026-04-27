from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import datetime

Venue = Literal["gemini", "kraken", "coinbase", "robinhood", "dex_trade", "cryptocom"]
Side = Literal["buy", "sell"]
OrderType = Literal["market", "limit"]


# ─────────────────────────────────────────────────────────────
# Orders (LOCAL DB)
# ─────────────────────────────────────────────────────────────
class OrderCreate(BaseModel):
    venue: Venue
    symbol: str = Field(..., description="Canonical symbol, e.g., USDT-USD")
    side: Side
    type: OrderType
    qty: float = Field(..., gt=0)
    limit_price: Optional[float] = Field(default=None, gt=0)


class OrderOut(BaseModel):
    id: str
    client_order_id: str
    venue: str
    symbol_canon: str
    symbol_venue: str
    side: Side
    type: OrderType
    qty: float
    limit_price: Optional[float]
    status: str
    filled_qty: float
    avg_fill_price: Optional[float]
    venue_order_id: Optional[str]
    reject_reason: Optional[str]
    created_at: datetime
    submitted_at: Optional[datetime]
    updated_at: datetime


class OrdersPage(BaseModel):
    items: List[OrderOut]
    page: int
    page_size: int
    total: int


class CancelAllRequest(BaseModel):
    venue: Venue
    symbol: Optional[str] = Field(default=None, description="Optional canonical symbol filter")


# ─────────────────────────────────────────────────────────────
# Fills (LOCAL DB)
# ─────────────────────────────────────────────────────────────
class FillOut(BaseModel):
    id: str
    order_id: str
    venue: Venue
    symbol_canon: str
    venue_trade_id: str
    qty: float
    price: float
    fee: Optional[float]
    fee_asset: Optional[str]
    ts: datetime


class FillsPage(BaseModel):
    items: List[FillOut]
    page: int
    page_size: int
    total: int


# ─────────────────────────────────────────────────────────────
# Balances (snapshots)
# ─────────────────────────────────────────────────────────────
class BalanceRow(BaseModel):
    venue: Venue
    asset: str
    total: float
    available: float
    hold: Optional[float]
    captured_at: datetime

    # NEW (Phase 2)
    px_usd: Optional[float] = None
    total_usd: Optional[float] = None
    available_usd: Optional[float] = None
    hold_usd: Optional[float] = None
    usd_source_symbol: Optional[str] = None


class BalancesLatestResponse(BaseModel):
    items: List[BalanceRow]
    as_of: datetime

    # NEW (Phase 2)
    portfolio_total_usd: Optional[float] = None


class BalanceRefreshRequest(BaseModel):
    venue: Venue


# ─────────────────────────────────────────────────────────────
# Symbols / Market Data
# ─────────────────────────────────────────────────────────────
class SymbolListResponse(BaseModel):
    venue: Venue
    symbols: List[str]


class SymbolResolveResponse(BaseModel):
    venue: Venue
    symbol_canon: str
    symbol_venue: str


class OrderBookLevel(BaseModel):
    price: float
    qty: float


class OrderBookResponse(BaseModel):
    venue: Venue
    symbol_canon: str
    bids: List[OrderBookLevel]
    asks: List[OrderBookLevel]
    ts: datetime


# ─────────────────────────────────────────────────────────────
# NEW: Order Rules (Phase 1)
# ─────────────────────────────────────────────────────────────
class OrderRulesResponse(BaseModel):
    venue: Venue
    symbol_canon: str
    symbol_venue: str

    base_increment: Optional[float] = None
    price_increment: Optional[float] = None
    qty_decimals: Optional[int] = None
    price_decimals: Optional[int] = None

    min_qty: Optional[float] = None
    max_qty: Optional[float] = None
    min_notional: Optional[float] = None
    max_notional: Optional[float] = None

    supports_post_only: bool = False
    supported_tifs: List[str] = []
    supported_order_types: List[str] = []


# ─────────────────────────────────────────────────────────────
# Venue Orders (VENUE ingestion table)
# ─────────────────────────────────────────────────────────────
class VenueOrderRefreshRequest(BaseModel):
    venue: Venue


class VenueOrderRowOut(BaseModel):
    venue: Venue
    venue_order_id: str

    # IMPORTANT: used by UI cancel-by-ref
    cancel_ref: Optional[str] = None

    symbol_canon: Optional[str] = None
    symbol_venue: str

    side: Optional[str] = None
    type: Optional[str] = None
    status: Optional[str] = None

    qty: Optional[float] = None
    filled_qty: Optional[float] = None

    limit_price: Optional[float] = None
    avg_fill_price: Optional[float] = None

    fee: Optional[float] = None
    fee_asset: Optional[str] = None
    total_after_fee: Optional[float] = None

    # NEW (4.x): tax withholding (backend-emitted, optional)
    tax_withheld_usd: Optional[float] = None
    tax_usd: Optional[float] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    captured_at: datetime


class VenueOrdersLatestResponse(BaseModel):
    items: List[VenueOrderRowOut]
    page: int
    page_size: int
    total: int
    as_of: Optional[datetime] = None


# ─────────────────────────────────────────────────────────────
# Unified "ALL orders" output (LOCAL + VENUE)
# ─────────────────────────────────────────────────────────────
class AllOrderRow(BaseModel):
    id: Optional[str] = None
    source: str
    venue: str

    symbol: Optional[str] = None
    symbol_canon: Optional[str] = None
    symbol_venue: Optional[str] = None

    side: Optional[str] = None
    type: Optional[str] = None
    status: Optional[str] = None
    status_bucket: Optional[str] = None

    qty: Optional[float] = None
    limit_price: Optional[float] = None

    filled_qty: Optional[float] = None
    avg_fill_price: Optional[float] = None

    fee: Optional[float] = None
    fee_asset: Optional[str] = None
    total_after_fee: Optional[float] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    captured_at: Optional[datetime] = None

    venue_order_id: Optional[str] = None
    client_order_id: Optional[str] = None

    reject_reason: Optional[str] = None

    # NEW: workflow fields
    closed_at: Optional[datetime] = None
    closed_at_inferred: Optional[bool] = None

    view_key: Optional[str] = None
    viewed_confirmed: bool = False
    viewed_at: Optional[datetime] = None

    # NEW: cancel behavior for UI
    can_cancel: bool = False
    cancel_ref: Optional[str] = None

    # NEW (3.5): realized P&L fields (feature-flagged in services, but schema must allow them)
    realized_status: Optional[str] = None               # applied | unapplied | no_journal
    realized_pnl_usd: Optional[float] = None
    realized_proceeds_usd: Optional[float] = None
    realized_basis_used_usd: Optional[float] = None
    realized_fee_usd: Optional[float] = None
    realized_error: Optional[str] = None               # e.g. insufficient_inventory


class AllOrdersPage(BaseModel):
    items: List[AllOrderRow]
    page: int
    page_size: int
    total: int
    total_pages: int


# ─────────────────────────────────────────────────────────────
# Order Views (confirm you viewed an order)
# ─────────────────────────────────────────────────────────────
class OrderViewConfirmRequest(BaseModel):
    view_key: str = Field(..., min_length=3, description="e.g. LOCAL:<uuid> or VENUE:gemini:<order_id>")
    viewed_confirmed: bool = Field(default=True)


class OrderViewOut(BaseModel):
    view_key: str
    viewed_confirmed: bool
    viewed_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


class OrderViewsPage(BaseModel):
    items: List[OrderViewOut]
    page: int
    page_size: int
    total: int
    total_pages: int
