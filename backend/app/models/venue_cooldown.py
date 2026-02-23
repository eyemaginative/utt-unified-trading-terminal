# backend/app/models/venue_cooldown.py

from __future__ import annotations

from datetime import datetime
from sqlalchemy import Column, String, DateTime, Text

from ..db import Base


class VenueCooldown(Base):
    """
    Stores per-venue cooldowns so backoff is shared across processes.
    One row per venue.
    """
    __tablename__ = "venue_cooldowns"

    venue = Column(String(64), primary_key=True)  # e.g. "coinbase"
    blocked_until = Column(DateTime, nullable=True)
    reason = Column(Text, nullable=True)

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
