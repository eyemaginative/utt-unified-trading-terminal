# backend/app/schemas_withdrawals.py

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class WithdrawalCreate(BaseModel):
    venue: str
    wallet_id: str = "default"
    asset: str
    qty: float = Field(gt=0)

    withdraw_time: Optional[datetime] = None

    txid: Optional[str] = None
    chain: Optional[str] = None
    network: Optional[str] = None

    status: str = "MANUAL"
    source: str = "UI_MANUAL"

    destination: Optional[str] = None
    note: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None


class WithdrawalPatch(BaseModel):
    # IMPORTANT: changing qty after lot-impact is applied is blocked in the router for safety.
    qty: Optional[float] = Field(default=None, gt=0)

    withdraw_time: Optional[datetime] = None

    txid: Optional[str] = None
    chain: Optional[str] = None
    network: Optional[str] = None

    status: Optional[str] = None
    source: Optional[str] = None

    destination: Optional[str] = None
    note: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None


class WithdrawalOut(BaseModel):
    id: str
    venue: str
    wallet_id: str
    asset: str
    qty: float

    withdraw_time: datetime

    txid: Optional[str] = None
    chain: Optional[str] = None
    network: Optional[str] = None

    status: str
    source: str

    destination: Optional[str] = None
    note: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None

    created_at: datetime
