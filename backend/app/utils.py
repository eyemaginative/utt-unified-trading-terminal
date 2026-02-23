# backend/app/utils.py

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional, Tuple, Set

from .parse_sort import parse_sort  # single source of truth


def new_client_order_id() -> str:
    return uuid.uuid4().hex


def now_utc() -> datetime:
    return datetime.utcnow()


__all__ = ["new_client_order_id", "now_utc", "parse_sort"]
