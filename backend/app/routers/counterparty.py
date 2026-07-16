from __future__ import annotations

from datetime import datetime, timezone
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..adapters.counterparty import CounterpartyAdapter
from ..db import get_db
from ..services.counterparty_ledger_preview import (
    CounterpartyLedgerPreviewError,
    build_counterparty_ledger_preview,
)
from ..services.market_metrics import get_market_metrics_summary

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


def _finite_number(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if out != out or out in (float("inf"), float("-inf")):
        return None
    return out


def _positive_number(value: Any) -> Optional[float]:
    out = _finite_number(value)
    return out if out is not None and out > 0 else None


def _counterparty_balance_cache_ttl_s() -> int:
    try:
        return max(10, min(int(os.getenv("COUNTERPARTY_BALANCE_PORTFOLIO_CACHE_TTL_S") or "60"), 900))
    except Exception:
        return 60


def _counterparty_balance_cache_stale_max_s() -> int:
    try:
        return max(60, min(int(os.getenv("COUNTERPARTY_BALANCE_PORTFOLIO_STALE_MAX_S") or "3600"), 86400))
    except Exception:
        return 3600


def _counterparty_balance_derived_asset_cap(value: Any) -> int:
    try:
        requested = int(value)
    except Exception:
        requested = 12
    try:
        configured = int(os.getenv("COUNTERPARTY_BALANCE_DERIVED_PRICE_ASSET_CAP") or "12")
    except Exception:
        configured = 12
    return max(0, min(requested, configured, 50))


def _market_metric_price_rows(assets: List[str], *, force_refresh: bool) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    requested = []
    seen = set()
    for asset in ["BTC", *(assets or [])]:
        symbol = str(asset or "").strip().upper()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        requested.append(symbol)

    summary = get_market_metrics_summary(
        assets=",".join(requested),
        include_assets=None,
        limit=max(1, min(len(requested), 1000)),
        ttl_s=300,
        force_refresh=bool(force_refresh),
    )
    rows: Dict[str, Dict[str, Any]] = {}
    for row in summary.get("items") or []:
        if not isinstance(row, dict):
            continue
        symbol = str(row.get("asset") or "").strip().upper()
        if symbol:
            rows[symbol] = row
    return rows, summary


def _metric_price_candidate(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    price = _positive_number(row.get("price_usd"))
    if price is None:
        return None

    warnings = [str(x or "").strip() for x in (row.get("warnings") or []) if str(x or "").strip()]
    warning_text = " ".join(warnings).lower()
    # Ticker-only CoinGecko matches are not authoritative enough for accounting
    # or balance valuation of legacy Counterparty assets.  Require an explicit
    # Token Registry / mapped source or fall through to the Counterparty book.
    if "matched by ticker symbol" in warning_text or "ambiguous" in warning_text:
        return None

    stale = "stale" in warning_text
    return {
        "price_usd": price,
        "price_source": str(row.get("price_source") or row.get("source") or "market_metrics"),
        "price_status": "stale" if stale else "cached",
        "price_updated_at": row.get("updated_at"),
        "price_warnings": warnings,
        "price_basis": "direct_usd",
    }


def _counterparty_book_price_btc(book: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(book, dict) or book.get("ok") is False:
        return None

    best_bid = _positive_number(book.get("best_bid"))
    best_ask = _positive_number(book.get("best_ask"))
    if best_bid is None:
        bids = book.get("bids") if isinstance(book.get("bids"), list) else []
        if bids:
            best_bid = _positive_number((bids[0] or {}).get("price") if isinstance(bids[0], dict) else None)
    if best_ask is None:
        asks = book.get("asks") if isinstance(book.get("asks"), list) else []
        if asks:
            best_ask = _positive_number((asks[0] or {}).get("price") if isinstance(asks[0], dict) else None)

    if best_bid is not None:
        return {
            "price_btc": best_bid,
            "price_basis": "best_bid",
            "price_warning": None,
        }
    if best_ask is not None:
        return {
            "price_btc": best_ask,
            "price_basis": "best_ask_reference",
            "price_warning": "No executable bid was available; valuation uses the lowest visible ask as a reference.",
        }
    return None


_COUNTERPARTY_BALANCE_PORTFOLIO_CACHE: Dict[str, Dict[str, Any]] = {}


def _counterparty_balance_cache_get(key: str, *, allow_stale: bool = False) -> Optional[Dict[str, Any]]:
    item = _COUNTERPARTY_BALANCE_PORTFOLIO_CACHE.get(str(key or ""))
    if not isinstance(item, dict):
        return None
    age_s = max(0.0, time.time() - float(item.get("ts") or 0.0))
    max_age_s = _counterparty_balance_cache_stale_max_s() if allow_stale else _counterparty_balance_cache_ttl_s()
    if age_s > float(max_age_s):
        return None
    payload = item.get("payload")
    if not isinstance(payload, dict):
        return None

    if allow_stale and age_s > float(_counterparty_balance_cache_ttl_s()):
        stale_items = []
        for row in payload.get("items") or []:
            if isinstance(row, dict):
                stale_items.append({**row, "balance_status": "stale_fallback", "balance_stale": True})
        return {
            **payload,
            "items": stale_items,
            "cache": "stale_fallback",
            "cache_age_s": int(age_s),
            "stale": True,
            "stale_reason": "counterparty_balance_refresh_failed",
        }

    return {**payload, "cache": "hit", "cache_age_s": int(age_s), "stale": False}


def _counterparty_balance_cache_put(key: str, payload: Dict[str, Any]) -> None:
    _COUNTERPARTY_BALANCE_PORTFOLIO_CACHE[str(key or "")] = {
        "ts": time.time(),
        "payload": dict(payload or {}),
    }


@router.get("/diagnostics")
def counterparty_diagnostics() -> Dict[str, Any]:
    return _adapter().diagnostics()


@router.get("/address/source")
def counterparty_address_source() -> Dict[str, Any]:
    """Report the read-only Counterparty address resolution provenance."""
    return _adapter().configured_source_address_info()


@router.get("/wallet_provider/unisat")
def counterparty_unisat_provider() -> Dict[str, Any]:
    return _adapter().wallet_provider_info("unisat")


@router.get("/ledger/preview")
def counterparty_ledger_preview(
    txid: str = Query(..., min_length=64, max_length=64, description="Confirmed Bitcoin transaction id"),
    dispense_index: Optional[int] = Query(default=None, ge=0, description="Optional Counterparty dispense event index"),
    allow_external_fee_lookup: bool = Query(default=True, description="Read a public Bitcoin transaction API only when Counterparty metadata lacks a positive miner fee"),
    allow_external_price_lookup: bool = Query(default=True, description="Read an auditable historical BTC/USD observation when the preview lacks historical basis pricing"),
    force_historical_price_refresh: bool = Query(default=False, description="Refresh the immutable historical BTC/USD cache instead of using an existing observation"),
    history_limit: int = Query(default=200, ge=1, le=500),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Return a read-only Counterparty accounting preview.

    The preview separates the acquired asset, BTC dispenser consideration, and
    Bitcoin miner fee, then optionally resolves an auditable historical BTC/USD
    observation for basis review. It performs no deposit, withdrawal, ledger,
    lot, FIFO, basis, signing, or broadcast mutation.
    """
    try:
        return build_counterparty_ledger_preview(
            db=db,
            adapter=_adapter(),
            txid=txid,
            dispense_index=dispense_index,
            allow_external_fee_lookup=bool(allow_external_fee_lookup),
            allow_external_price_lookup=bool(allow_external_price_lookup),
            force_historical_price_refresh=bool(force_historical_price_refresh),
            history_limit=history_limit,
        )
    except CounterpartyLedgerPreviewError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.as_dict()) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "counterparty_ledger_preview_failed",
                "message": str(exc),
                "read_only": True,
                "database_mutation": False,
                "ledger_mutation": False,
                "lot_mutation": False,
                "basis_mutation": False,
            },
        ) from exc


@router.get("/balances/portfolio")
def counterparty_configured_balance_portfolio(
    force_refresh: bool = Query(default=False, description="Refresh configured-address balances and price context instead of using the short last-good cache."),
    derive_btc_prices: bool = Query(default=True, description="Derive missing asset USD prices from Counterparty ASSET-BTC liquidity and BTC/USD."),
    max_derived_assets: int = Query(default=12, ge=0, le=50),
    include_zero: bool = Query(default=False),
) -> Dict[str, Any]:
    """Return configured-address Counterparty balances for unified Balances.

    Pricing hierarchy:
      1. Token Registry / Market Metrics direct USD price.
      2. Counterparty ASSET-BTC best bid, or lowest ask reference when no bid exists.
      3. BTC/USD from Market Metrics.
      4. Explicit unavailable state.

    This endpoint is read-only. It performs no signing, broadcast, database
    writes, ledger writes, lot creation, FIFO consumption, or basis mutation.
    """
    adapter = _adapter()
    source_address = adapter.configured_source_address_info()
    address = str(source_address.get("address") or "").strip()
    if not source_address.get("ok") or not address:
        status_code = 409 if source_address.get("error") == "counterparty_wallet_address_ambiguous" else 422
        raise HTTPException(
            status_code=status_code,
            detail={
                "error": source_address.get("error") or "counterparty_configured_balances_failed",
                "message": source_address.get("message") or "Counterparty Wallet Addresses account row is required",
                "source_address": source_address,
                "read_only": True,
                "database_mutation": False,
                "browser_state_required": False,
            },
        )

    resolution_key = str(source_address.get("resolution_key") or address.lower())
    cache_key = f"{resolution_key}|derive={int(bool(derive_btc_prices))}|cap={int(max_derived_assets)}|zero={int(bool(include_zero))}"
    if not force_refresh:
        cached = _counterparty_balance_cache_get(cache_key)
        if cached is not None:
            return cached

    configured = adapter.get_configured_address_balances(source_resolution=source_address)
    if not configured.get("ok"):
        stale = _counterparty_balance_cache_get(cache_key, allow_stale=True)
        if stale is not None:
            stale_items = [
                {**row, "balance_status": "stale_fallback", "balance_stale": True}
                for row in (stale.get("items") or [])
                if isinstance(row, dict)
            ]
            return {
                **stale,
                "items": stale_items,
                "cache": "stale_fallback",
                "stale": True,
                "stale_reason": "counterparty_balance_refresh_failed",
                "refresh_error": configured.get("errors") or configured.get("error") or configured.get("message"),
            }
        raise HTTPException(
            status_code=502,
            detail={"error": "counterparty_configured_balances_failed", **configured},
        )

    raw_items = [dict(row) for row in (configured.get("items") or []) if isinstance(row, dict)]
    normalized: List[Dict[str, Any]] = []
    assets: List[str] = []
    for row in raw_items:
        asset = str(row.get("asset") or "").strip().upper()
        quantity = _finite_number(row.get("quantity_normalized") if row.get("quantity_normalized") is not None else row.get("quantity"))
        quantity = quantity if quantity is not None else 0.0
        if not asset or (not include_zero and abs(quantity) <= 1e-18):
            continue
        assets.append(asset)
        normalized.append({
            **row,
            "venue": "counterparty",
            "source_type": "Counterparty Wallet Addresses",
            "network": "bitcoin",
            "chain": "bitcoin",
            "wallet_id": source_address.get("wallet_id") or "counterparty",
            "address": address,
            "wallet_address": address,
            "wallet_address_id": source_address.get("wallet_address_id"),
            "address_source": source_address.get("address_source"),
            "wallet_network": source_address.get("network"),
            "asset_scope": source_address.get("asset_scope"),
            "asset": asset,
            "symbol": asset,
            "total": float(quantity),
            "available": float(quantity),
            "hold": 0.0,
            "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "balance_status": "live",
            "balance_stale": False,
            "basis_status": "basis_missing",
            "basis_lot_count": 0,
            "cost_basis_usd": None,
            "cost_avg_usd": None,
        })

    metric_rows, metric_summary = _market_metric_price_rows(assets, force_refresh=bool(force_refresh))
    btc_metric = _metric_price_candidate(metric_rows.get("BTC"))
    btc_usd = _positive_number((btc_metric or {}).get("price_usd"))
    derived_cap = _counterparty_balance_derived_asset_cap(max_derived_assets)
    derived_count = 0
    derived_attempt_count = 0
    derivation_stopped_reason: Optional[str] = None
    price_errors: List[Dict[str, Any]] = []
    out_items: List[Dict[str, Any]] = []
    portfolio_total_usd = 0.0
    has_portfolio_total = False

    for row in normalized:
        asset = str(row.get("asset") or "").strip().upper()
        direct = _metric_price_candidate(metric_rows.get(asset))
        price_info = direct
        price_btc = None

        if (
            price_info is None
            and derive_btc_prices
            and asset != "BTC"
            and btc_usd is not None
            and derived_attempt_count < derived_cap
            and derivation_stopped_reason is None
        ):
            try:
                derived_attempt_count += 1
                book = adapter.get_orderbook(symbol=f"{asset}-BTC", depth=1, open_only=True)
                if book.get("rate_limited") is True:
                    derivation_stopped_reason = "counterparty_rate_limited"
                book_price = _counterparty_book_price_btc(book)
                if book_price is not None:
                    derived_count += 1
                    price_btc = _positive_number(book_price.get("price_btc"))
                    if price_btc is not None:
                        warnings = []
                        if book_price.get("price_warning"):
                            warnings.append(str(book_price.get("price_warning")))
                        if book.get("stale") is True:
                            warnings.append("Counterparty OrderBook price uses the last-good stale snapshot.")
                        price_info = {
                            "price_usd": price_btc * btc_usd,
                            "price_source": f"derived:{asset}-BTC:{book_price.get('price_basis')}×{(btc_metric or {}).get('price_source') or 'BTC-USD'}",
                            "price_status": "stale" if book.get("stale") is True or (btc_metric or {}).get("price_status") == "stale" else ("cached" if (btc_metric or {}).get("price_status") == "cached" else "live"),
                            "price_updated_at": book.get("snapshot_cached_at") or (btc_metric or {}).get("price_updated_at"),
                            "price_warnings": warnings,
                            "price_basis": book_price.get("price_basis"),
                        }
            except Exception as e:
                price_errors.append({"asset": asset, "error": str(e)[:500]})

        px_usd = _positive_number((price_info or {}).get("price_usd"))
        quantity = float(row.get("total") or 0.0)
        total_usd = quantity * px_usd if px_usd is not None else None
        if total_usd is not None:
            portfolio_total_usd += total_usd
            has_portfolio_total = True

        out_items.append({
            **row,
            "px_usd": px_usd,
            "total_usd": total_usd,
            "available_usd": total_usd,
            "hold_usd": 0.0 if px_usd is not None else None,
            "usd_source_symbol": (price_info or {}).get("price_source") or "—",
            "price_status": (price_info or {}).get("price_status") or "unavailable",
            "price_basis": (price_info or {}).get("price_basis") or "unavailable",
            "price_btc": price_btc,
            "price_updated_at": (price_info or {}).get("price_updated_at"),
            "price_warnings": (price_info or {}).get("price_warnings") or [],
        })

    payload = {
        "ok": True,
        "venue": "counterparty",
        "address": address,
        "address_source": source_address.get("address_source"),
        "wallet_address_id": source_address.get("wallet_address_id"),
        "wallet_id": source_address.get("wallet_id"),
        "wallet_network": source_address.get("network"),
        "asset_scope": source_address.get("asset_scope"),
        "environment_fallback": bool(source_address.get("environment_fallback")),
        "environment_address_configured": bool(source_address.get("environment_address_configured")),
        "environment_address_matches": source_address.get("environment_address_matches"),
        "source_address": source_address,
        "count": len(out_items),
        "items": out_items,
        "portfolio_total_usd": portfolio_total_usd if has_portfolio_total else None,
        "btc_usd": btc_usd,
        "btc_usd_source": (btc_metric or {}).get("price_source"),
        "pricing_hierarchy": [
            "token_registry_or_market_metrics_direct_usd",
            "counterparty_asset_btc_best_bid_or_ask_reference",
            "btc_usd_market_metrics",
            "unavailable",
        ],
        "market_metrics_cache": metric_summary.get("cache"),
        "market_metrics_updated_at": metric_summary.get("updated_at"),
        "derived_asset_count": int(derived_count),
        "derived_asset_attempt_count": int(derived_attempt_count),
        "derived_asset_cap": int(derived_cap),
        "derivation_stopped_reason": derivation_stopped_reason,
        "price_errors": price_errors,
        "captured_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "cache": "miss",
        "cache_age_s": 0,
        "read_only": True,
        "database_mutation": False,
        "ledger_mutation": False,
        "lot_mutation": False,
        "basis_mutation": False,
        "browser_state_required": False,
        "signing": False,
        "broadcast": False,
    }
    _counterparty_balance_cache_put(cache_key, payload)
    return payload


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


@router.get("/address/{address}/dispense_orders")
def counterparty_address_dispense_orders(
    address: str,
    limit: int = Query(default=200, ge=1, le=500),
) -> Dict[str, Any]:
    """Return confirmed buyer-side dispenser purchases without DB mutation."""
    return _raise_if_failed(
        _adapter().get_confirmed_dispense_orders(address=address, limit=limit),
        label="counterparty_confirmed_dispense_orders_failed",
    )


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
