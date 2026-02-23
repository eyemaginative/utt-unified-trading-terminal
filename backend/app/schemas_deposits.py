# backend/app/schemas_deposits.py

from datetime import datetime
from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field


class DepositCreate(BaseModel):
    venue: str
    asset: str
    qty: float = Field(gt=0)

    deposit_time: Optional[datetime] = None
    wallet_id: str = "default"

    txid: Optional[str] = None
    network: Optional[str] = None
    note: Optional[str] = None

    # basis inputs (normal deposits only)
    basis_total_usd: Optional[float] = Field(default=None, ge=0)
    basis_usd_per_coin: Optional[float] = Field(default=None, ge=0)
    acquired_at_override: Optional[datetime] = None

    # NEW: if set, treat this deposit as the TRANSFER_IN paired to an existing withdrawal.
    # We will create one lot per slice in withdrawal.raw.lot_impact.
    transfer_withdrawal_id: Optional[str] = None


class DepositUpdate(BaseModel):
    qty: Optional[float] = Field(default=None, gt=0)
    deposit_time: Optional[datetime] = None
    wallet_id: Optional[str] = None

    txid: Optional[str] = None
    network: Optional[str] = None
    status: Optional[str] = None
    note: Optional[str] = None

    # Match-only transfer link (no lot impact required).
    transfer_withdrawal_id: Optional[str] = None


class LotUpdate(BaseModel):
    total_basis_usd: Optional[float] = Field(default=None, ge=0)
    acquired_at: Optional[datetime] = None
    note: Optional[str] = None


class DepositOut(BaseModel):
    id: str
    venue: str
    wallet_id: str
    asset: str
    qty: float
    deposit_time: datetime

    txid: Optional[str] = None
    network: Optional[str] = None
    status: str
    source: str
    note: Optional[str] = None

    # lot linkage / computed flags
    lot_id: Optional[str] = None
    lot_ids: Optional[List[str]] = None  # NEW: supports transfer-in (multiple lots)
    needs_basis: bool = True
    total_basis_usd: Optional[float] = None
    basis_is_missing: bool = True
    acquired_at: Optional[datetime] = None  # earliest acquired_at across linked lots

    # transfer linking visibility
    transfer_withdrawal_id: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None


class LotOut(BaseModel):
    id: str
    venue: str
    wallet_id: str
    asset: str

    acquired_at: datetime

    qty_total: float
    qty_remaining: float

    total_basis_usd: Optional[float] = None
    basis_is_missing: bool

    basis_source: str
    origin_type: str
    origin_ref: Optional[str] = None

    note: Optional[str] = None

    created_at: datetime
    updated_at: datetime
