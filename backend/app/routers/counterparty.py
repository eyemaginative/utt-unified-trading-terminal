from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, Query

from ..adapters.counterparty import CounterpartyAdapter

router = APIRouter(prefix="/api/counterparty", tags=["counterparty"])


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


@router.get("/assets/{asset}")
def counterparty_asset(asset: str) -> Dict[str, Any]:
    return _raise_if_failed(_adapter().get_asset(asset), label="counterparty_asset_lookup_failed")


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
