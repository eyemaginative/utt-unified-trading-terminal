from typing import Dict, List, Optional
import requests

from ..services.symbol_policy import ALLOWED_QUOTES

# Public endpoint (no auth) for Coinbase Exchange products
COINBASE_PRODUCTS_URL = "https://api.exchange.coinbase.com/products"


class CoinbaseSymbolDiscovery:
    venue = "coinbase"

    @staticmethod
    def _to_canonical_from_product(p: Dict) -> Optional[Dict[str, str]]:
        """
        Coinbase Exchange product shape (typical):
          {
            "id": "BTC-USD",
            "base_currency": "BTC",
            "quote_currency": "USD",
            "status": "online",
            ...
          }
        """
        if not isinstance(p, dict):
            return None

        product_id = str(p.get("id") or "").strip()
        base = str(p.get("base_currency") or "").strip().upper()
        quote = str(p.get("quote_currency") or "").strip().upper()

        if not product_id or not base or not quote:
            return None

        # Enforce quote policy
        if quote not in ALLOWED_QUOTES:
            return None

        # Basic sanity
        if not base.isalnum():
            return None

        # Normalize venue symbol and canonical:
        # - venue symbol: keep the exchange's product id in lowercase (stable & readable)
        # - canonical: BASE-QUOTE uppercase
        sym_venue = product_id.strip().lower()
        sym_canon = f"{base}-{quote}"

        # Determine active status (Coinbase uses "online"/"offline"/etc.)
        status = str(p.get("status") or "").strip().lower()
        is_active = status == "online" if status else True

        return {
            "symbol_venue": sym_venue,
            "symbol_canon": sym_canon,
            "base_asset": base,
            "quote_asset": quote,
            "is_active": bool(is_active),
        }

    def list_symbols(self) -> List[Dict]:
        """
        Public discovery: list products from Coinbase Exchange and map to canonical symbols.
        This requires NO API keys and avoids the 401 from /api/v3/brokerage/products.
        """
        headers = {
            # Helps Coinbase identify client; not required but polite.
            "User-Agent": "unified-trading-terminal/1.0",
            "Accept": "application/json",
        }

        resp = requests.get(COINBASE_PRODUCTS_URL, headers=headers, timeout=15)
        resp.raise_for_status()

        raw = resp.json()
        if not isinstance(raw, list):
            # Defensive: if Coinbase changes response shape
            return []

        out: List[Dict] = []
        for p in raw:
            row = self._to_canonical_from_product(p)
            if row:
                out.append(row)

        # Stable ordering helps diffs and debugging
        out.sort(key=lambda r: (r.get("symbol_canon") or ""))
        return out
