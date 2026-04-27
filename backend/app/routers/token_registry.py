# backend/app/routers/token_registry.py

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import TokenRegistry

router = APIRouter(prefix="/api/token_registry", tags=["token_registry"])


def _norm_chain(chain: str) -> str:
    c = (chain or "").strip().lower()
    return c or "solana"


def _norm_symbol(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if not s:
        raise HTTPException(status_code=422, detail="symbol is required")
    return s


def _norm_venue(venue: Optional[str]) -> Optional[str]:
    v = (venue or "").strip().lower() if isinstance(venue, str) else None
    return v or None


def _validate_decimals(decimals: int) -> int:
    try:
        d = int(decimals)
    except Exception:
        raise HTTPException(status_code=422, detail="decimals must be an integer")
    if d < 0 or d > 18:
        raise HTTPException(status_code=422, detail="decimals must be between 0 and 18")
    return d


def _validate_address(addr: Optional[str]) -> Optional[str]:
    if addr is None:
        return None
    a = (addr or "").strip()
    return a or None


class TokenRegistryCreate(BaseModel):
    chain: str = Field(default="solana")
    venue: Optional[str] = Field(default=None, description="Optional venue override (e.g. coinbase); omit for global mapping")
    symbol: str = Field(..., description="Symbol ticker, e.g. UTTT")
    address: Optional[str] = Field(default=None, description="Contract/mint address (chain-specific)")
    decimals: int = Field(..., ge=0, le=18)
    label: Optional[str] = Field(default=None, description="Optional display label/name")


class TokenRegistryUpdate(BaseModel):
    venue: Optional[str] = Field(default=None)
    symbol: Optional[str] = Field(default=None)
    address: Optional[str] = Field(default=None)
    decimals: Optional[int] = Field(default=None, ge=0, le=18)
    label: Optional[str] = Field(default=None)


def _row_to_dict(r: TokenRegistry) -> Dict[str, Any]:
    return {
        "id": r.id,
        "chain": r.chain,
        "venue": r.venue,
        "symbol": r.symbol,
        "address": r.address,
        "decimals": int(r.decimals),
        "label": r.label,
        "created_at": getattr(r, "created_at", None),
        "updated_at": getattr(r, "updated_at", None),
    }


@router.get("")
def list_tokens(
    chain: str = Query("solana"),
    venue: Optional[str] = Query(None, description="If set, returns that venue override plus global rows when include_global=1"),
    include_global: int = Query(1, ge=0, le=1),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    c = _norm_chain(chain)
    v = _norm_venue(venue)

    q = db.query(TokenRegistry).filter(TokenRegistry.chain == c)

    if v:
        if include_global:
            q = q.filter((TokenRegistry.venue == v) | (TokenRegistry.venue.is_(None)))
        else:
            q = q.filter(TokenRegistry.venue == v)
    else:
        q = q.filter(TokenRegistry.venue.is_(None))

    rows = q.order_by(TokenRegistry.symbol.asc(), TokenRegistry.venue.asc().nullsfirst()).all()
    return {"ok": True, "items": [_row_to_dict(r) for r in rows]}


@router.post("")
def create_token(req: TokenRegistryCreate, db: Session = Depends(get_db)) -> Dict[str, Any]:
    c = _norm_chain(req.chain)
    v = _norm_venue(req.venue)
    s = _norm_symbol(req.symbol)
    a = _validate_address(req.address)
    d = _validate_decimals(req.decimals)
    label = (req.label or "").strip() or None

    # Upsert-ish behavior: if a row exists for (chain, venue, symbol), update it.
    row = (
        db.query(TokenRegistry)
        .filter(TokenRegistry.chain == c, TokenRegistry.venue.is_(v) if v is None else TokenRegistry.venue == v, TokenRegistry.symbol == s)
        .first()
    )

    if row is None:
        row = TokenRegistry(chain=c, venue=v, symbol=s, address=a, decimals=d, label=label)
        db.add(row)
    else:
        row.address = a
        row.decimals = d
        row.label = label

    db.commit()
    db.refresh(row)
    return {"ok": True, "item": _row_to_dict(row)}


@router.put("/{token_id}")
def update_token(token_id: int, req: TokenRegistryUpdate, db: Session = Depends(get_db)) -> Dict[str, Any]:
    row = db.query(TokenRegistry).filter(TokenRegistry.id == int(token_id)).first()
    if row is None:
        raise HTTPException(status_code=404, detail="token not found")

    if req.venue is not None:
        row.venue = _norm_venue(req.venue)
    if req.symbol is not None:
        row.symbol = _norm_symbol(req.symbol)
    if req.address is not None:
        row.address = _validate_address(req.address)
    if req.decimals is not None:
        row.decimals = _validate_decimals(req.decimals)
    if req.label is not None:
        row.label = (req.label or "").strip() or None

    db.commit()
    db.refresh(row)
    return {"ok": True, "item": _row_to_dict(row)}


@router.delete("/{token_id}")
def delete_token(token_id: int, db: Session = Depends(get_db)) -> Dict[str, Any]:
    row = db.query(TokenRegistry).filter(TokenRegistry.id == int(token_id)).first()
    if row is None:
        return {"ok": True, "deleted": 0}

    db.delete(row)
    db.commit()
    return {"ok": True, "deleted": 1}
