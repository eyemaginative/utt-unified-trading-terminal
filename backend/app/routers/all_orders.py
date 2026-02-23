from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from ..db import get_db
from ..schemas import AllOrdersPage
from ..services.all_orders import list_all_orders, ALLOWED_SORT_FIELDS, ALLOWED_SCOPES
from ..utils import parse_sort

router = APIRouter(prefix="/api/all_orders", tags=["all_orders"])

_ALLOWED_BUCKETS = {"open", "terminal"}


def _normalize_status_bucket(sb: Optional[str]) -> Optional[str]:
    """
    status_bucket semantics:
      - None / "" / "all" => ALL (no bucket restriction)
      - "open"           => open-only
      - "terminal"       => terminal-only
    """
    if sb is None:
        return None
    s = sb.strip().lower()
    if s == "" or s == "all":
        return None
    if s in _ALLOWED_BUCKETS:
        return s
    raise HTTPException(status_code=400, detail="status_bucket must be one of: open, terminal (or omit/all for ALL)")


def _normalize_scope(scope: Optional[str]) -> str:
    """
    Design A: scope model for inclusion
      - ALL (default) => include LOCAL + VENUES
      - LOCAL        => include only local Order rows
      - VENUES       => include only VenueOrderRow rows
    """
    if scope is None:
        return "ALL"
    s = scope.strip().upper()
    if s == "":
        return "ALL"
    if s in ALLOWED_SCOPES:
        return s
    raise HTTPException(status_code=400, detail="scope must be one of: ALL, LOCAL, VENUES")


def _ceil_div(a: int, b: int) -> int:
    if b <= 0:
        return 0
    return (a + b - 1) // b


@router.get("", response_model=AllOrdersPage)
def get_all_orders(
    # Back-compat: retained, but Design A prefers `scope` + `venue`.
    source: Optional[str] = Query(default=None, description="(legacy) LOCAL or venue name (kraken/gemini/coinbase)"),
    # Design A: new scope param (None => ALL)
    scope: Optional[str] = Query(default=None, description="ALL | LOCAL | VENUES"),
    venue: Optional[str] = Query(default=None, description="Filter by venue (works for both LOCAL and VENUE rows)"),
    symbol: Optional[str] = Query(default=None, description="Symbol (canon or venue symbol)"),
    status: Optional[str] = Query(default=None, description="Exact status filter"),
    status_bucket: Optional[str] = Query(default=None, description="open | terminal (or omit/all for ALL)"),
    from_: Optional[datetime] = Query(default=None, alias="from"),
    to: Optional[datetime] = Query(default=None),
    sort: Optional[str] = Query(default="closed_at:desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    sb_norm = _normalize_status_bucket(status_bucket)

    # Normalize scope; if omitted, treat as ALL.
    scope_norm = _normalize_scope(scope)

    # Legacy mapping (only when caller didn't explicitly set scope):
    # - source=LOCAL  => scope=LOCAL
    # - source=<venue> => scope=VENUES and venue=<source> (if venue not already provided)
    if scope is None and source:
        src = source.strip().upper()
        if src == "LOCAL":
            scope_norm = "LOCAL"
        else:
            # treat as venue name
            if venue is None or str(venue).strip() == "":
                venue = source.strip().lower()
            scope_norm = "VENUES"

    try:
        sort_field, sort_dir = parse_sort(
            sort,
            ALLOWED_SORT_FIELDS,
            default=("closed_at", "desc"),
            raise_on_invalid=True,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    items, total = list_all_orders(
        db=db,
        # legacy param retained
        source=source,
        # new design-A param
        scope=scope_norm,
        venue=venue,
        status=status,
        status_bucket=sb_norm,
        symbol=symbol,
        dt_from=from_,
        dt_to=to,
        sort_field=sort_field,
        sort_dir=sort_dir,
        page=page,
        page_size=page_size,
    )

    total_pages = _ceil_div(int(total), int(page_size))
    return {
        "items": items,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
    }
