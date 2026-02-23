# backend/app/services/symbol_policy.py

from __future__ import annotations

from typing import Optional, Tuple

# Keep your original constant intact
ALLOWED_QUOTES = {"USD", "BTC", "USDT", "USDC"}

# Common venue-specific synonyms
_ASSET_SYNONYMS = {
    "XBT": "BTC",
    "XXBT": "BTC",
    "XDG": "DOGE",
    "XXDG": "DOGE",
    "ZUSD": "USD",
    "USDT": "USDT",
    "USDC": "USDC",
}


def normalize_asset(asset: Optional[str]) -> Optional[str]:
    if asset is None:
        return None
    a = str(asset).strip().upper()
    if not a:
        return None
    # Normalize Kraken-style prefixes
    if a in _ASSET_SYNONYMS:
        return _ASSET_SYNONYMS[a]
    # Strip single leading X/Z often used by Kraken internal codes
    if len(a) >= 2 and (a[0] in {"X", "Z"}) and a[1:].isalpha():
        aa = a[1:]
        return _ASSET_SYNONYMS.get(aa, aa)
    return a


def split_symbol(symbol: str) -> Tuple[Optional[str], Optional[str]]:
    s = str(symbol or "").strip().upper()
    if not s:
        return None, None

    if "-" in s:
        a, b = s.split("-", 1)
        return normalize_asset(a), normalize_asset(b)
    if "/" in s:
        a, b = s.split("/", 1)
        return normalize_asset(a), normalize_asset(b)

    # No delimiter => treat as base-only
    return normalize_asset(s), None


def canonicalize_symbol(symbol: str) -> str:
    base, quote = split_symbol(symbol)
    if not base:
        return ""
    if not quote:
        return base
    return f"{base}-{quote}"


def is_allowed_quote(quote: Optional[str]) -> bool:
    q = normalize_asset(quote)
    return bool(q) and q in ALLOWED_QUOTES
