from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import select, func

from ..db import get_db
from ..models import OrderView
from ..schemas import OrderViewConfirmRequest, OrderViewOut, OrderViewsPage
from ..utils import now_utc

router = APIRouter(prefix="/api/order_views", tags=["order_views"])


def _ceil_div(a: int, b: int) -> int:
    if b <= 0:
        return 0
    return (a + b - 1) // b


class OrderViewConfirmResponse(OrderViewOut):
    ok: bool


@router.post("/confirm", response_model=OrderViewConfirmResponse)
def confirm_view(req: OrderViewConfirmRequest, db: Session = Depends(get_db)):
    """
    Upsert a view record keyed by view_key.

    Semantics:
      - viewed_confirmed=true  => viewed_at set to now_utc()
      - viewed_confirmed=false => viewed_at set to null

    Idempotency:
      - Re-sending the same request updates the same row (no duplicates).
    """
    key = (req.view_key or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="view_key is required")

    confirmed = bool(req.viewed_confirmed)
    ts = now_utc() if confirmed else None

    row = db.execute(select(OrderView).where(OrderView.view_key == key)).scalar_one_or_none()

    if row is None:
        row = OrderView(
            view_key=key,
            viewed_confirmed=1 if confirmed else 0,
            viewed_at=ts,
        )
        db.add(row)
    else:
        row.viewed_confirmed = 1 if confirmed else 0
        row.viewed_at = ts
        db.add(row)

    db.commit()
    db.refresh(row)

    return OrderViewConfirmResponse(
        ok=True,
        view_key=row.view_key,
        viewed_confirmed=bool(row.viewed_confirmed == 1),
        viewed_at=row.viewed_at,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


@router.get("", response_model=OrderViewsPage)
def list_order_views(
    view_key: Optional[str] = Query(default=None, description="Filter by exact view_key"),
    confirmed: Optional[bool] = Query(default=None, description="Filter by viewed_confirmed true/false"),
    sort: str = Query(default="updated_at:desc", description="updated_at:desc|created_at:desc|viewed_at:desc"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    page = max(page, 1)
    page_size = min(max(page_size, 1), 200)

    stmt = select(OrderView)
    count_stmt = select(func.count()).select_from(OrderView)

    if view_key:
        stmt = stmt.where(OrderView.view_key == view_key)
        count_stmt = count_stmt.where(OrderView.view_key == view_key)

    if confirmed is not None:
        v = 1 if confirmed else 0
        stmt = stmt.where(OrderView.viewed_confirmed == v)
        count_stmt = count_stmt.where(OrderView.viewed_confirmed == v)

    sort = (sort or "updated_at:desc").strip().lower()
    if sort == "created_at:asc":
        stmt = stmt.order_by(OrderView.created_at.asc())
    elif sort == "created_at:desc":
        stmt = stmt.order_by(OrderView.created_at.desc())
    elif sort == "viewed_at:asc":
        stmt = stmt.order_by(OrderView.viewed_at.asc().nullslast())
    elif sort == "viewed_at:desc":
        stmt = stmt.order_by(OrderView.viewed_at.desc().nullslast())
    elif sort == "updated_at:asc":
        stmt = stmt.order_by(OrderView.updated_at.asc())
    else:
        stmt = stmt.order_by(OrderView.updated_at.desc())

    total = int(db.execute(count_stmt).scalar_one())
    total_pages = _ceil_div(total, page_size)

    stmt = stmt.offset((page - 1) * page_size).limit(page_size)
    rows = db.execute(stmt).scalars().all()

    items: List[OrderViewOut] = []
    for r in rows:
        items.append(
            OrderViewOut(
                view_key=r.view_key,
                viewed_confirmed=bool(r.viewed_confirmed == 1),
                viewed_at=r.viewed_at,
                created_at=r.created_at,
                updated_at=r.updated_at,
            )
        )

    return {"items": items, "page": page, "page_size": page_size, "total": total, "total_pages": total_pages}
