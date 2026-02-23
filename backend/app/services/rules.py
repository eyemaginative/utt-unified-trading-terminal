# backend/app/services/rules.py

from __future__ import annotations

from typing import Dict, Any, Optional, List, Tuple
from decimal import Decimal, InvalidOperation
from threading import Lock
import time

import httpx

from .symbols import get_adapter, resolve_symbol
from .symbol_policy import canonicalize_symbol

# ─────────────────────────────────────────────────────────────
# Gemini public symbol details cache
# ─────────────────────────────────────────────────────────────

_GEMINI_DETAILS_TTL_S = 300  # 5 minutes
_gemini_details_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_gemini_details_lock = Lock()


def _get_gemini_symbol_details(symbol_venue: str) -> Optional[Dict[str, Any]]:
    """
    Fetch https://api.gemini.com/v1/symbols/details/{symbol}
    Cached to avoid hammering Gemini.

    Returns dict containing (at least) tick_size, quote_increment, min_order_size when available.
    """
    sym = (symbol_venue or "").strip().lower()
    if not sym:
        return None

    now = time.time()
    with _gemini_details_lock:
        hit = _gemini_details_cache.get(sym)
        if hit:
            ts, data = hit
            if now - ts <= _GEMINI_DETAILS_TTL_S:
                return data

    url = f"https://api.gemini.com/v1/symbols/details/{sym}"
    try:
        r = httpx.get(url, timeout=8.0, headers={"Accept": "application/json"})
        if r.status_code != 200:
            return None
        data = r.json()
        if not isinstance(data, dict):
            return None
    except Exception:
        return None

    with _gemini_details_lock:
        _gemini_details_cache[sym] = (now, data)

    return data


def _decimals_from_increment(x: Any) -> Optional[int]:
    """
    Infer decimal places from an increment string/number.
    Examples:
      "0.01" -> 2
      "0.00010000" -> 4
      1 -> 0
      "1e-9" -> 9
    """
    if x is None:
        return None
    try:
        d = Decimal(str(x))
    except (InvalidOperation, ValueError, TypeError):
        return None

    d = d.normalize()
    exp = d.as_tuple().exponent
    if exp >= 0:
        return 0
    return int(-exp)


def _should_override_decimals(adapter_dec: Any, inferred_dec: Optional[int]) -> bool:
    """
    Decide whether to override adapter-reported decimals with inferred decimals from increment.

    Rationale:
    - Some adapters default decimals to 0 (noteably on crypto-crypto pairs) even when increments imply more.
    - Increments are authoritative for UI precision and step validation.
    """
    if inferred_dec is None:
        return False

    # Missing/unknown from adapter
    if adapter_dec is None:
        return True

    # Non-int adapter values: treat as unreliable
    try:
        ad = int(adapter_dec)
    except Exception:
        return True

    # If adapter says 0 but increment clearly requires more, override
    if ad == 0 and inferred_dec > 0:
        return True

    # If adapter disagrees with increment-derived precision, override (increment wins)
    if ad != inferred_dec:
        return True

    return False


def order_rules_for_symbol(
    venue: str,
    symbol_canon: str,
    side: Optional[str] = None,
    order_type: str = "limit",
    tif: Optional[str] = None,
    post_only: Optional[bool] = None,
) -> Dict[str, Any]:
    """
    Service wrapper used by the rules router.

    Contract:
      - Never throws due to adapter capability gaps.
      - Returns a stable payload shape for the UI.
      - Populates errors/warnings arrays when rules are unknown/unavailable.
    """
    v = (venue or "").strip().lower()
    sym_in = (symbol_canon or "").strip().upper()

    errors: List[str] = []
    warnings: List[str] = []

    if not v:
        return {
            "venue": "",
            "symbol_canon": "",
            "symbol_venue": "",
            "base_increment": None,
            "price_increment": None,
            "qty_decimals": None,
            "price_decimals": None,
            "min_qty": None,
            "max_qty": None,
            "min_notional": None,
            "max_notional": None,
            "supports_post_only": False,
            "supported_tifs": [],
            "supported_order_types": [],
            "suggested_symbol": None,
            "errors": ["venue is required"],
            "warnings": [],
        }

    if not sym_in:
        return {
            "venue": v,
            "symbol_canon": "",
            "symbol_venue": "",
            "base_increment": None,
            "price_increment": None,
            "qty_decimals": None,
            "price_decimals": None,
            "min_qty": None,
            "max_qty": None,
            "min_notional": None,
            "max_notional": None,
            "supports_post_only": False,
            "supported_tifs": [],
            "supported_order_types": [],
            "suggested_symbol": None,
            "errors": ["symbol is required"],
            "warnings": [],
        }

    side_n = (side or "").strip().lower() or None
    ot_n = (order_type or "").strip().lower() or "limit"
    tif_n = (tif or "").strip().lower() or None
    post_only_b = bool(post_only) if post_only is not None else None

    suggested_symbol = None

    # Resolve symbol venue mapping
    try:
        sym_canon_norm, sym_venue = resolve_symbol(v, sym_in)
    except Exception as e:
        sym_canon_norm = canonicalize_symbol(sym_in)
        sym_venue = sym_canon_norm
        errors.append(f"symbol resolution failed for venue '{v}': {e}")
        suggested_symbol = sym_canon_norm

    # Load adapter
    try:
        adapter = get_adapter(v)
    except Exception as e:
        errors.append(str(e))
        return {
            "venue": v,
            "symbol_canon": sym_canon_norm,
            "symbol_venue": sym_venue,
            "base_increment": None,
            "price_increment": None,
            "qty_decimals": None,
            "price_decimals": None,
            "min_qty": None,
            "max_qty": None,
            "min_notional": None,
            "max_notional": None,
            "supports_post_only": False,
            "supported_tifs": [],
            "supported_order_types": [],
            "suggested_symbol": suggested_symbol,
            "errors": errors,
            "warnings": warnings,
        }

    rules: Dict[str, Any] = {}

    fn = getattr(adapter, "get_order_rules", None)
    if callable(fn):
        try:
            rules = fn(symbol_venue=sym_venue) or {}
        except Exception as e:
            warnings.append(f"adapter.get_order_rules failed for venue '{v}': {e}")
            rules = {}
    else:
        warnings.append(f"adapter for venue '{v}' does not implement get_order_rules(); constraints unknown")

    # ─────────────────────────────────────────────────────────────
    # Gemini correctness:
    # - Use Gemini public symbol details as authoritative for:
    #   tick_size (base increment), quote_increment (price tick), min_order_size (min qty)
    # - IMPORTANT: override qty_decimals/price_decimals even if adapter provided 0 defaults,
    #   because those defaults cause false failures and bad UI formatting.
    # ─────────────────────────────────────────────────────────────
    if v == "gemini":
        det = _get_gemini_symbol_details(sym_venue)
        if det:
            if det.get("tick_size") is not None:
                rules["base_increment"] = det.get("tick_size")
            if det.get("quote_increment") is not None:
                rules["price_increment"] = det.get("quote_increment")
            if det.get("min_order_size") is not None:
                rules["min_qty"] = det.get("min_order_size")

            # Force decimals to be derived from the authoritative increments
            rules.pop("qty_decimals", None)
            rules.pop("price_decimals", None)

    base_inc = rules.get("base_increment")
    price_inc = rules.get("price_increment")

    adapter_qty_dec = rules.get("qty_decimals")
    adapter_px_dec = rules.get("price_decimals")

    inferred_qty_dec = _decimals_from_increment(base_inc) if base_inc is not None else None
    inferred_px_dec = _decimals_from_increment(price_inc) if price_inc is not None else None

    # For Gemini: always infer if increments exist
    # For other venues (Coinbase/Kraken/etc): infer when adapter decimals are missing or suspicious.
    if v == "gemini":
        qty_dec = inferred_qty_dec
        px_dec = inferred_px_dec
    else:
        qty_dec = adapter_qty_dec
        px_dec = adapter_px_dec

        if _should_override_decimals(adapter_qty_dec, inferred_qty_dec):
            if adapter_qty_dec is not None and inferred_qty_dec is not None and str(adapter_qty_dec) != str(inferred_qty_dec):
                warnings.append(
                    f"qty_decimals overridden by base_increment (adapter={adapter_qty_dec}, inferred={inferred_qty_dec})"
                )
            qty_dec = inferred_qty_dec

        if _should_override_decimals(adapter_px_dec, inferred_px_dec):
            if adapter_px_dec is not None and inferred_px_dec is not None and str(adapter_px_dec) != str(inferred_px_dec):
                warnings.append(
                    f"price_decimals overridden by price_increment (adapter={adapter_px_dec}, inferred={inferred_px_dec})"
                )
            px_dec = inferred_px_dec

    out: Dict[str, Any] = {
        "venue": v,
        "symbol_canon": sym_canon_norm,
        "symbol_venue": sym_venue,
        "base_increment": base_inc,
        "price_increment": price_inc,
        "qty_decimals": qty_dec,
        "price_decimals": px_dec,
        "min_qty": rules.get("min_qty"),
        "max_qty": rules.get("max_qty"),
        "min_notional": rules.get("min_notional"),
        "max_notional": rules.get("max_notional"),
        "supports_post_only": bool(rules.get("supports_post_only", False)),
        "supported_tifs": list(rules.get("supported_tifs") or []),
        "supported_order_types": list(rules.get("supported_order_types") or []),
        "suggested_symbol": suggested_symbol,
        "errors": errors,
        "warnings": warnings,
    }

    out["supported_tifs"] = [str(x).lower().strip() for x in out["supported_tifs"] if str(x).strip()]
    out["supported_order_types"] = [str(x).lower().strip() for x in out["supported_order_types"] if str(x).strip()]

    if post_only_b is True and out["supports_post_only"] is False:
        warnings.append("post-only requested but adapter reports post-only unsupported")

    if tif_n and out["supported_tifs"] and tif_n not in out["supported_tifs"]:
        warnings.append(f"tif '{tif_n}' not in supported_tifs: {', '.join(out['supported_tifs'])}")

    if ot_n and out["supported_order_types"] and ot_n not in out["supported_order_types"]:
        warnings.append(f"order type '{ot_n}' not in supported_order_types: {', '.join(out['supported_order_types'])}")

    if side_n and side_n not in ("buy", "sell"):
        warnings.append(f"unrecognized side '{side_n}' (expected buy/sell)")

    out["warnings"] = warnings
    return out
