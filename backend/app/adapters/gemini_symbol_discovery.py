from typing import Dict, List, Optional
import requests

from ..services.symbol_policy import ALLOWED_QUOTES

GEMINI_SYMBOLS_URL = "https://api.gemini.com/v1/symbols"


class GeminiSymbolDiscovery:
    venue = "gemini"

    @staticmethod
    def _to_canonical(symbol_venue: str) -> Optional[Dict[str, str]]:
        s = (symbol_venue or "").strip().lower()
        if len(s) < 6:
            return None

        quote = s[-3:].upper()
        base = s[:-3].upper()

        if quote not in ALLOWED_QUOTES:
            return None

        if not base or not base.isalnum():
            return None

        return {
            "symbol_venue": s,
            "symbol_canon": f"{base}-{quote}",
            "base_asset": base,
            "quote_asset": quote,
            "is_active": True,
        }

    def list_symbols(self) -> List[Dict]:
        resp = requests.get(GEMINI_SYMBOLS_URL, timeout=10)
        resp.raise_for_status()

        raw = resp.json()
        out: List[Dict] = []
        for sym in raw:
            row = self._to_canonical(sym)
            if row:
                out.append(row)
        return out
