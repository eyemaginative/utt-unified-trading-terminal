# backend/app/schemas_wallet_addresses.py

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class WalletAddressCreate(BaseModel):
    asset: str = Field(min_length=1)
    network: str = Field(min_length=1)
    address: str = Field(min_length=3)

    label: Optional[str] = None
    wallet_id: Optional[str] = None  # None => self-custody; non-None => venue group
    owner_scope: str = "default"


class WalletAddressUpdate(BaseModel):
    asset: Optional[str] = None
    network: Optional[str] = None
    address: Optional[str] = None
    label: Optional[str] = None
    wallet_id: Optional[str] = None
    owner_scope: Optional[str] = None


class WalletAddressOut(BaseModel):
    # Allow either int or str to stay compatible with older UI payloads.
    # NOTE: your DB is currently emitting UUID-like strings.
    id: int | str
    asset: str
    network: str
    address: str
    label: Optional[str] = None
    wallet_id: Optional[str] = None
    owner_scope: str
    created_at: datetime


class WalletAddressDeleteOut(BaseModel):
    ok: bool = True
    id: int | str


class WalletBalancesRefreshIn(BaseModel):
    # Optional subset refresh; if omitted, refresh all in scope
    ids: Optional[List[int | str]] = None
    owner_scope: str = "default"


class WalletBalanceItem(BaseModel):
    # Balance-like row compatible with your balances UI conventions
    venue: str = "wallet"
    wallet_id: int | str = "default"

    owner_scope: str = "default"
    asset: str
    network: str
    address: str
    label: Optional[str] = None

    total: float = 0.0
    available: float = 0.0
    captured_at: datetime

    # Optional enrichment
    px_usd: Optional[float] = None
    total_usd: Optional[float] = None


class WalletBalancesLatestOut(BaseModel):
    items: List[WalletBalanceItem]
    as_of: datetime
    portfolio_total_usd: Optional[float] = None


# ---------------------------------------------------------------------------
# Back-compat schema names expected by routers/wallet_addresses.py
# ---------------------------------------------------------------------------

class WalletAddressRefreshRequest(BaseModel):
    """Request body for POST /api/wallet_addresses/balances/refresh."""

    # Optional subset refresh; if omitted, refresh all in scope
    # NOTE: WalletAddress.id is UUID-like in your runtime logs, so accept str.
    ids: Optional[List[int | str]] = None
    owner_scope: str = "default"


class WalletAddressBalanceOut(BaseModel):
    """
    Response row for GET /api/wallet_addresses/balances/latest

    IMPORTANT:
      - Snapshot ids are UUID strings in your DB -> id must NOT be int.
      - Endpoint returns fetched_at + usd_price/usd_value/balance (not captured_at/total_usd).
      - Keep created_at/captured_at as OPTIONAL for back-compat with any older callers.
    """

    # Primary row id (we use snapshot id in the router for uniqueness)
    id: int | str

    # Helpful linkage (address record id)
    wallet_address_id: Optional[int | str] = None

    # Venue grouping (None => self-custody)
    wallet_id: Optional[str] = None
    owner_scope: Optional[str] = None

    asset: str
    network: Optional[str] = None
    address: Optional[str] = None
    label: Optional[str] = None

    # New “balances latest” fields
    balance: Optional[float] = None
    usd_price: Optional[float] = None
    usd_value: Optional[float] = None
    fetched_at: Optional[datetime] = None

    # Back-compat (optional)
    created_at: Optional[datetime] = None
    captured_at: Optional[datetime] = None
