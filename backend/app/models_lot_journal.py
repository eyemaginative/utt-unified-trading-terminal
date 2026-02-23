# backend/app/models_lot_journal.py

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Column, String, DateTime, Float, Boolean, UniqueConstraint
from sqlalchemy.types import JSON

from .db import Base  # FIX: was ..db (invalid when app is the top-level package)


class LotJournal(Base):
    __tablename__ = "lot_journal"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))

    # idempotency
    action = Column(String, nullable=False)         # e.g. BUY_LOT_CREATE, SELL_FIFO_CONSUME
    origin_type = Column(String, nullable=False)    # e.g. LOCAL_FILL, VENUE_ORDER_AGG
    origin_ref = Column(String, nullable=False)     # fill.id or venue_order_row.id

    venue = Column(String, nullable=True)
    wallet_id = Column(String, nullable=True)
    asset = Column(String, nullable=True)

    qty = Column(Float, nullable=True)
    price_usd = Column(Float, nullable=True)
    fee_usd = Column(Float, nullable=True)

    effective_at = Column(DateTime, nullable=True)

    applied = Column(Boolean, nullable=False, default=False)

    # impact payload for audit/debug/recompute
    impact = Column(JSON, nullable=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("action", "origin_type", "origin_ref", name="uq_lot_journal_action_origin"),
    )
