# backend/app/parse_sort.py

from __future__ import annotations

from typing import Optional, Tuple, Set


def parse_sort(
    sort: Optional[str],
    allowed: Set[str],
    default: Tuple[str, str],
    *,
    raise_on_invalid: bool = False,
) -> Tuple[str, str]:
    if not sort:
        return default

    raw = str(sort).strip()
    if not raw:
        return default

    parts = raw.split(":")
    if len(parts) != 2:
        if raise_on_invalid:
            raise ValueError("Invalid sort format; expected 'field:asc' or 'field:desc'")
        return default

    field = parts[0].strip()
    direction = parts[1].strip().lower()

    if field not in allowed:
        if raise_on_invalid:
            raise ValueError(f"Invalid sort field '{field}'")
        return default

    if direction not in ("asc", "desc"):
        if raise_on_invalid:
            raise ValueError(f"Invalid sort direction '{direction}'")
        return default

    return field, direction
