from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Body, HTTPException, Query
from pydantic import BaseModel

from ..adapters.counterparty import CounterpartyAdapter

router = APIRouter(prefix="/api/counterparty", tags=["counterparty"])


class CounterpartyComposePreviewRequest(BaseModel):
    source_address: str
    symbol: str
    side: str
    quantity: float | str
    limit_price: float | str
    selected_level: Optional[Dict[str, Any]] = None
    attempt_upstream: bool = True
    fee_tier: str = "normal"
    execution_mode: str = "auto"
    expiration_blocks: int | str | None = None


def _adapter() -> CounterpartyAdapter:
    return CounterpartyAdapter()


def _raise_if_failed(result: Dict[str, Any], *, label: str) -> Dict[str, Any]:
    if result.get("ok"):
        return result
    raise HTTPException(status_code=502, detail={"error": label, **result})


@router.get("/diagnostics")
def counterparty_diagnostics() -> Dict[str, Any]:
    return _adapter().diagnostics()


@router.get("/wallet_provider/unisat")
def counterparty_unisat_provider() -> Dict[str, Any]:
    return _adapter().wallet_provider_info("unisat")


@router.get("/orderbook")
def counterparty_orderbook(
    symbol: str = Query(..., description="Counterparty market symbol, e.g. XCP-BTC or BITCRYSTALS-XCP"),
    depth: int = Query(default=25, ge=1, le=200),
    open_only: bool = Query(default=True, description="If true, return only open locally-filtered order/dispenser rows."),
) -> Dict[str, Any]:
    try:
        return _adapter().get_orderbook(symbol=symbol, depth=depth, open_only=open_only)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=502, detail={"error": "counterparty_orderbook_failed", "message": str(e)}) from e




@router.post("/compose/preview")
def counterparty_compose_preview(req: CounterpartyComposePreviewRequest = Body(...)) -> Dict[str, Any]:
    """Return an unsigned Counterparty compose preview only.

    This route does not sign, submit, broadcast, write orders, mutate balances,
    or touch FIFO/basis/ledger state.
    """
    try:
        return _adapter().preview_compose(
            source_address=req.source_address,
            symbol=req.symbol,
            side=req.side,
            quantity=req.quantity,
            limit_price=req.limit_price,
            selected_level=req.selected_level,
            attempt_upstream=bool(req.attempt_upstream),
            fee_tier=req.fee_tier,
            execution_mode=req.execution_mode,
            expiration_blocks=req.expiration_blocks,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=502, detail={"error": "counterparty_compose_preview_failed", "message": str(e)}) from e


@router.get("/assets/metadata")
def counterparty_assets_metadata(
    assets: str = Query(default="", description="Comma-separated Counterparty asset names"),
    limit: int = Query(default=100, ge=1, le=200),
) -> Dict[str, Any]:
    asset_list = [a.strip() for a in str(assets or "").split(",") if a.strip()]
    return _adapter().get_assets_metadata(asset_list, limit=limit)


@router.get("/assets/{asset}/orders")
def counterparty_asset_orders(
    asset: str,
    limit: int = Query(default=50, ge=1, le=500),
    open_only: bool = Query(default=False, description="If true, return only locally-open order rows when possible."),
) -> Dict[str, Any]:
    return _raise_if_failed(_adapter().get_asset_orders(asset=asset, limit=limit, open_only=open_only), label="counterparty_asset_orders_failed")


@router.get("/assets/{asset}/dispensers")
def counterparty_asset_dispensers(
    asset: str,
    limit: int = Query(default=50, ge=1, le=500),
    open_only: bool = Query(default=False, description="If true, return only locally-open dispenser rows when possible."),
) -> Dict[str, Any]:
    return _raise_if_failed(_adapter().get_asset_dispensers(asset=asset, limit=limit, open_only=open_only), label="counterparty_asset_dispensers_failed")


@router.get("/assets/{asset}/market_context")
def counterparty_asset_market_context(
    asset: str,
    limit: int = Query(default=25, ge=1, le=200),
    open_only: bool = Query(default=True, description="Default true for fast trading context; set false for historical rows."),
) -> Dict[str, Any]:
    return _adapter().get_asset_market_context(asset=asset, limit=limit, open_only=open_only)


@router.get("/assets/{asset}")
def counterparty_asset(asset: str) -> Dict[str, Any]:
    return _raise_if_failed(_adapter().get_asset(asset), label="counterparty_asset_lookup_failed")


@router.get("/address/{address}/balances/audit")
def counterparty_address_balances_audit(
    address: str,
    assets: str = Query(default="XCP,BITCRYSTALS", description="Comma-separated assets to audit from one Counterparty balance snapshot."),
) -> Dict[str, Any]:
    asset_list = [a.strip() for a in str(assets or "").split(",") if a.strip()]
    return _adapter().get_address_balances_audit(address=address, assets=asset_list)


@router.get("/address/{address}/balances")
def counterparty_address_balances(address: str) -> Dict[str, Any]:
    return _raise_if_failed(_adapter().get_address_balances(address), label="counterparty_address_balances_failed")


@router.get("/address/{address}/balance/{asset}")
def counterparty_address_asset_balance(address: str, asset: str) -> Dict[str, Any]:
    result = _adapter().get_address_asset_balance(address=address, asset=asset)
    if not result.get("ok"):
        raise HTTPException(status_code=502, detail={"error": "counterparty_address_asset_balance_failed", **result})
    return result


@router.get("/address/{address}/sends")
def counterparty_address_sends(
    address: str,
    limit: int = Query(default=50, ge=1, le=500),
) -> Dict[str, Any]:
    return _raise_if_failed(_adapter().get_address_sends(address=address, limit=limit), label="counterparty_address_sends_failed")


@router.get("/orders")
def counterparty_orders(
    asset: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
) -> Dict[str, Any]:
    return _raise_if_failed(_adapter().get_orders(asset=asset, limit=limit), label="counterparty_orders_failed")
