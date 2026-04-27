# backend/app/routers/solana_dex.py

from __future__ import annotations

import os
import time
import asyncio
import random
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from sqlalchemy.orm import Session

from ..db import get_db
from ..models import TokenRegistry, VenueOrderRow

from ..config import settings  # vault-first provider keys (jupiter)

router = APIRouter(prefix="/api/solana_dex", tags=["solana_dex"])

@router.get("/_debug")
async def solana_dex_debug() -> Dict[str, Any]:
    """Return diagnostic info to confirm which solana_dex module is running."""
    return {
        "ok": True,
        "module_file": __file__,
        "quote_like": sorted(list(_QUOTE_LIKE)),
        "jup_quote_url": _JUP_QUOTE_URL,
        "jup_swap_url": _JUP_SWAP_URL,
        "jup_ultra_order_url": _JUP_ULTRA_ORDER_URL,
        "jup_ultra_execute_url": _JUP_ULTRA_EXECUTE_URL,
        "jup_token_search_url": _JUP_TOKEN_SEARCH_URL,
        "used_api_key": bool(_JUP_API_KEY),
        "timeout_s": _JUP_TIMEOUT_S,
    }


# Jupiter quote endpoint (public). Used for pseudo-orderbook (best bid/ask via effective execution).
# Jupiter Swap API v1 (Quote). Keep backward-compatible env name support.
# Prefer UTT_JUP_QUOTE_URL, fall back to legacy JUPITER_QUOTE_URL.
_JUP_QUOTE_URL = (
    os.getenv("UTT_JUP_QUOTE_URL")
    or os.getenv("JUPITER_QUOTE_URL")
    or "https://api.jup.ag/swap/v1/quote"
).strip()

# Jupiter swap endpoint (builds serialized tx). We return it UNSIGNED.
_JUP_SWAP_URL = (
    os.getenv("UTT_JUP_SWAP_URL")
    or os.getenv("JUPITER_SWAP_URL")
    or "https://api.jup.ag/swap/v1/swap"
).strip()

# Jupiter Ultra endpoints (unsigned tx order + execute signed tx).
_JUP_ULTRA_ORDER_URL = (
    os.getenv("UTT_JUP_ULTRA_ORDER_URL")
    or os.getenv("JUPITER_ULTRA_ORDER_URL")
    or "https://api.jup.ag/ultra/v1/order"
).strip()
_JUP_ULTRA_EXECUTE_URL = (
    os.getenv("UTT_JUP_ULTRA_EXECUTE_URL")
    or os.getenv("JUPITER_ULTRA_EXECUTE_URL")
    or "https://api.jup.ag/ultra/v1/execute"
).strip()

# Jupiter Trigger (limit orders) API base / endpoints.
_JUP_TRIGGER_BASE_URL = (
    os.getenv("UTT_JUP_TRIGGER_BASE_URL")
    or os.getenv("JUPITER_TRIGGER_BASE_URL")
    or "https://api.jup.ag/trigger/v1"
).strip().rstrip("/")
_JUP_TRIGGER_CREATE_URL = f"{_JUP_TRIGGER_BASE_URL}/createOrder"
_JUP_TRIGGER_OPEN_URL = f"{_JUP_TRIGGER_BASE_URL}/getTriggerOrders"
_JUP_TRIGGER_CANCEL_URL = f"{_JUP_TRIGGER_BASE_URL}/cancelOrder"

# Jupiter token discovery (symbol→mint/decimals) via Tokens API (cached per symbol)
_JUP_TOKEN_SEARCH_URL = (
    os.getenv("UTT_JUP_TOKEN_SEARCH_URL")
    or os.getenv("JUPITER_TOKEN_SEARCH_URL")
    or "https://lite-api.jup.ag/tokens/v2/search"
).strip()

_TOKEN_CACHE: Dict[str, List[Dict[str, Any]]] = {}
_TOKEN_CACHE_AT: Dict[str, float] = {}
_TOKEN_TTL_S: int = int(os.getenv("UTT_JUP_TOKEN_TTL_S") or "3600")

_QUOTE_LIKE = {"SOL", "USDC", "USDT"}

def _jup_api_key_effective() -> str:
    """Provider key resolution (vault-first without venue gating).

    Resolution order:
      1) env: UTT_JUP_API_KEY or JUPITER_API_KEY
      2) vault: venue='jupiter' api_key
    """
    k = (os.getenv("UTT_JUP_API_KEY") or os.getenv("JUPITER_API_KEY") or "").strip()
    if k:
        return k
    try:
        b = settings._vault_latest_bundle("jupiter")  # type: ignore[attr-defined]
        if isinstance(b, dict):
            k2 = (b.get("api_key") or "").strip()
            if k2:
                return k2
    except Exception:
        pass
    return ""

# Optional Jupiter API key (Swap API v1). Sent as header `x-api-key` when present.
_JUP_API_KEY = _jup_api_key_effective()

# Timeout (seconds) for Jupiter HTTP requests.
try:
    _JUP_TIMEOUT_S = float(os.getenv("UTT_JUP_TIMEOUT_S") or os.getenv("JUPITER_TIMEOUT_S") or "10")
except Exception:
    _JUP_TIMEOUT_S = 10.0

_JUP_PRICE_URL = os.getenv("UTT_JUP_PRICE_URL", "https://api.jup.ag/price/v3").strip()

_RAY_SWAP_HOST = (
    os.getenv("UTT_RAYDIUM_SWAP_HOST")
    or "https://transaction-v1.raydium.io"
).strip().rstrip("/")
_RAY_SWAP_BASE_IN_URL = f"{_RAY_SWAP_HOST}/compute/swap-base-in"
_RAY_SWAP_TX_BASE_IN_URL = f"{_RAY_SWAP_HOST}/transaction/swap-base-in"
_RAY_AUTO_FEE_URL = (
    os.getenv("UTT_RAYDIUM_AUTO_FEE_URL")
    or "https://api-v3.raydium.io/main/auto-fee"
).strip()
_RAY_TIMEOUT_S = float(os.getenv("UTT_RAYDIUM_TIMEOUT_S") or "15")
_RAY_TX_VERSION = (os.getenv("UTT_RAYDIUM_TX_VERSION") or "V0").strip().upper()

_ATA_PROGRAM_ID = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
_TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
_TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"


_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_IDX = {c: i for i, c in enumerate(_B58_ALPHABET)}


def _b58decode_raw(s: str) -> bytes:
    v = (s or "").strip()
    if not v:
        raise ValueError("empty base58")
    num = 0
    for ch in v:
        if ch not in _B58_IDX:
            raise ValueError(f"invalid base58 character: {ch!r}")
        num = num * 58 + _B58_IDX[ch]
    full = num.to_bytes((num.bit_length() + 7) // 8, "big") if num > 0 else b""
    pad = 0
    for ch in v:
        if ch == "1":
            pad += 1
        else:
            break
    out = (b"\x00" * pad) + full
    if len(out) > 32:
        raise ValueError("base58 value too long for pubkey")
    return out


def _b58encode_raw(data: bytes) -> str:
    b = bytes(data or b"")
    if not b:
        return ""
    zeros = 0
    for by in b:
        if by == 0:
            zeros += 1
        else:
            break
    num = int.from_bytes(b, "big")
    chars = []
    while num > 0:
        num, rem = divmod(num, 58)
        chars.append(_B58_ALPHABET[rem])
    return ("1" * zeros) + ("".join(reversed(chars)) if chars else "")


def _is_on_ed25519_curve(pubkey_bytes: bytes) -> bool:
    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey  # type: ignore
        Ed25519PublicKey.from_public_bytes(bytes(pubkey_bytes))
        return True
    except Exception:
        return False


# Jupiter pricing cache (in-process) to reduce upstream rate limits / RPC bursts.
_JUP_PRICES_TTL_S = float(os.getenv('UTT_JUP_PRICES_TTL_S', '12'))
_JUP_PRICES_CACHE: Dict[str, Any] = {'ts': 0.0, 'key': '', 'items': {}}
_JUP_PRICES_LOCK = asyncio.Lock()

# Minimal curated mint map (MVP). We can expand / move to DB later.
_SOL_MINTS = {
    # Wrapped SOL mint (used by most Solana programs / aggregators)
    "SOL": "So11111111111111111111111111111111111111112",
    "WSOL": "So11111111111111111111111111111111111111112",
    # USDC (Solana)
    "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    # USDT (Solana)
    "USDT": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    # PYUSD (Solana)
    "PYUSD": "2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo",
    # Convenience: treat USD as USDC for Solana venues
    "USD": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
}

_SOL_DECIMALS = {
    "SOL": 9,
    "WSOL": 9,
    "USDC": 6,
    "USDT": 6,
    "PYUSD": 6,
    "USD": 6,
}

_JUP_TRIGGER_MIN_USD = float(os.getenv("UTT_JUP_TRIGGER_MIN_USD") or "10.10")
_STABLE_MINTS = {
    _SOL_MINTS.get("USDC"),
    _SOL_MINTS.get("USDT"),
    _SOL_MINTS.get("PYUSD"),
}

# Optional overrides/extends (for tokens we haven't curated yet):
# - UTT_SOLANA_MINTS_JSON: JSON object mapping symbol -> mint
# - UTT_SOLANA_DECIMALS_JSON: JSON object mapping symbol -> decimals
# This lets you add tokens/stables like USD1 without code changes.
def _merge_env_token_maps():
    import os, json
    try:
        mints_json = os.getenv("UTT_SOLANA_MINTS_JSON")
        if mints_json:
            extra = json.loads(mints_json)
            if isinstance(extra, dict):
                for k, v in extra.items():
                    if isinstance(k, str) and isinstance(v, str) and v:
                        _SOL_MINTS[k.upper()] = v
    except Exception:
        pass
    try:
        dec_json = os.getenv("UTT_SOLANA_DECIMALS_JSON")
        if dec_json:
            extra = json.loads(dec_json)
            if isinstance(extra, dict):
                for k, v in extra.items():
                    if isinstance(k, str) and isinstance(v, (int, float)):
                        _SOL_DECIMALS[k.upper()] = int(v)
    except Exception:
        pass

_merge_env_token_maps()



def _parse_symbol(symbol: str) -> tuple[str, str]:
    s = (symbol or "").strip()
    if "-" not in s:
        raise HTTPException(status_code=422, detail=f"Invalid symbol '{symbol}' (expected BASE-QUOTE)")
    left, right = s.split("-", 1)
    left = left.strip()
    right = right.strip()
    if not left or not right:
        raise HTTPException(status_code=422, detail=f"Invalid symbol '{symbol}' (expected BASE-QUOTE)")

    def _norm_side(x: str) -> str:
        return x if x.lower().startswith("mint:") else x.upper()

    return _norm_side(left), _norm_side(right)


def _normalize_symbol(symbol: str) -> str:
    """DEX convenience: allow SOL-FOO or USDC-FOO and normalize to FOO-SOL/FOO-USDC
    so that quote-like assets become QUOTE when paired with a non-quote asset.
    For non-quote pairs (e.g. WIF-UTTT), we do not guess.
    """
    a, b = _parse_symbol(symbol)
    # a/b can be 'mint:...' too; only normalize when both are tickers and one is quote-like
    if not a.lower().startswith("mint:") and not b.lower().startswith("mint:"):
        if a in _QUOTE_LIKE and b not in _QUOTE_LIKE:
            return f"{b}-{a}"
    return f"{a}-{b}"






async def _fetch_tokens(query: str) -> List[Dict[str, Any]]:
    q = (query or "").strip()
    if not q:
        return []
    now = time.time()
    if q in _TOKEN_CACHE and (now - _TOKEN_CACHE_AT.get(q, 0.0)) < _TOKEN_TTL_S:
        return _TOKEN_CACHE[q]

    async with httpx.AsyncClient(timeout=_JUP_TIMEOUT_S) as client:
        resp = await client.get(_JUP_TOKEN_SEARCH_URL, params={"query": q}, headers={"accept": "application/json"})
    if resp.status_code >= 400:
        raise HTTPException(status_code=502, detail={"error": "jupiter_token_search_failed", "status": resp.status_code, "body": resp.text})

    data = resp.json()
    if not isinstance(data, list):
        raise HTTPException(status_code=502, detail={"error": "jupiter_token_search_unexpected", "body": data})

    _TOKEN_CACHE[q] = data
    _TOKEN_CACHE_AT[q] = now
    return data


def _db_lookup_registry(
    db: Optional[Session],
    chain: str,
    symbol: str,
    venue: Optional[str] = None,
) -> Optional[TokenRegistry]:
    """Lookup token metadata in DB token_registry.

    Resolution order:
      1) venue-specific override (if venue provided)
      2) global row (venue is NULL)
    """
    if db is None:
        return None
    ch = (chain or "").strip().lower()
    sym = (symbol or "").strip().upper()
    if not ch or not sym:
        return None

    try:
        if venue:
            row = (
                db.query(TokenRegistry)
                .filter(TokenRegistry.chain == ch)
                .filter(TokenRegistry.venue == venue)
                .filter(TokenRegistry.symbol == sym)
                .first()
            )
            if row is not None:
                return row

        return (
            db.query(TokenRegistry)
            .filter(TokenRegistry.chain == ch)
            .filter(TokenRegistry.venue.is_(None))
            .filter(TokenRegistry.symbol == sym)
            .first()
        )
    except Exception:
        return None



async def _resolve_mint_and_decimals(asset: str, db: Optional[Session] = None, venue: Optional[str] = None) -> tuple[str, int]:
    """Resolve a ticker symbol (e.g. 'WIF') or a forced mint ('mint:<ADDRESS>') to (mint, decimals).

    Resolution order:
      1) Env overrides: UTT_SOLANA_MINTS_JSON + UTT_SOLANA_DECIMALS_JSON
      2) Jupiter token search (cached per query), with collision detection
    """
    s = (asset or "").strip()
    if not s:
        raise HTTPException(status_code=422, detail="Empty asset")

    # Forced mint syntax
    if s.lower().startswith("mint:"):
        mint = s.split(":", 1)[1].strip()

        # decimals override by mint
        dec = _SOL_DECIMALS.get(mint) or _SOL_DECIMALS.get(mint.upper())
        if dec is not None:
            return mint, int(dec)

        toks = await _fetch_tokens(mint)
        for t in toks:
            addr = t.get("address") or t.get("mint")
            if addr == mint:
                dec2 = t.get("decimals")
                if dec2 is None:
                    break
                return mint, int(dec2)

        raise HTTPException(status_code=422, detail={"error": "unknown_mint_decimals", "mint": mint})

    sym = s.upper()


    # DB registry (preferred): allows UI-managed symbol/mint/decimals without env JSON.
    row = _db_lookup_registry(db=db, chain="solana", symbol=sym, venue=venue)
    if row is not None:
        mint = (row.address or "").strip()
        dec = row.decimals
        if not mint or dec is None:
            raise HTTPException(status_code=422, detail={"error": "registry_row_incomplete", "symbol": sym})
        return mint, int(dec)

    # Env override (deterministic)
    if sym in _SOL_MINTS:
        mint = _SOL_MINTS[sym]
        dec = _SOL_DECIMALS.get(sym)
        if dec is None:
            dec = _SOL_DECIMALS.get(mint) or _SOL_DECIMALS.get(mint.upper())
        if dec is None:
            raise HTTPException(status_code=422, detail={"error": "missing_decimals", "symbol": sym, "mint": mint})
        return mint, int(dec)

    toks = await _fetch_tokens(sym)

    # Filter to exact symbol match (Jupiter search can return partials)
    exact = []
    for t in toks:
        if (t.get("symbol") or "").upper() == sym:
            addr = t.get("address") or t.get("mint")
            if addr and t.get("decimals") is not None:
                exact.append(t)

    if len(exact) == 1:
        tok = exact[0]
        return str(tok.get("address") or tok.get("mint")), int(tok.get("decimals"))

    if len(exact) > 1:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "symbol_ambiguous",
                "symbol": sym,
                "hint": "Use mint:<ADDRESS> in market string or set UTT_SOLANA_MINTS_JSON override",
                "exampleMints": [t.get("address") or t.get("mint") for t in exact[:10]],
            },
        )

    raise HTTPException(status_code=422, detail={"error": "unknown_symbol", "symbol": sym})



async def _jup_quote(input_mint: str, output_mint: str, amount_atomic: int) -> Dict[str, Any]:
    """Get a best route quote from Jupiter Swap API v1.

    IMPORTANT:
      - Jupiter v6 quote endpoint has been sunset; Swap API v1 is the supported path.
      - Some deployments require an API key. If you have one, set:
            UTT_JUP_API_KEY=...
      - Override URL if needed:
            UTT_JUP_QUOTE_URL=https://api.jup.ag/swap/v1/quote
    """
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(int(amount_atomic)),
        # keep defaults; this is for indicative pricing / pseudo-book
    }
    headers = {"x-api-key": _JUP_API_KEY} if _JUP_API_KEY else {}

    try:
        async with httpx.AsyncClient(timeout=_JUP_TIMEOUT_S) as client:
            r = await client.get(_JUP_QUOTE_URL, params=params, headers=headers)
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "jupiter_quote_request_failed",
                "exc": type(e).__name__,
                "message": str(e),
                "url": _JUP_QUOTE_URL,
            },
        )

    if r.status_code in (401, 403):
        raise HTTPException(
            status_code=502,
            detail={
                "error": "jupiter_quote_unauthorized",
                "message": "Set UTT_JUP_API_KEY in backend env if your Jupiter Swap API requires it.",
                "url": _JUP_QUOTE_URL,
            },
        )

    if r.status_code >= 400:
        body = (r.text or "")[:300]
        raise HTTPException(
            status_code=502,
            detail={"error": "jupiter_quote_http_error", "status": r.status_code, "body": body},
        )

    try:
        data = r.json() or {}
    except Exception:
        raise HTTPException(status_code=502, detail={"error": "jupiter_quote_non_json"})
    routes = data.get("data")
    # Jupiter "swap/v1/quote" commonly returns a single route object (not wrapped in {"data":[...]}).
    if isinstance(routes, list) and len(routes) > 0:
        return routes[0]
    if isinstance(data, dict) and (data.get("routePlan") is not None or data.get("outAmount") is not None):
        return data

    raise HTTPException(status_code=502, detail={"error": "no_routes", "jupiter": data})



def _jup_ultra_default_taker() -> str:
    return (os.getenv("UTT_JUP_ULTRA_TAKER") or os.getenv("JUPITER_ULTRA_TAKER") or "BQ72nSv9f3PRyRKCBnHLVrerrv37CYTHm5h3s9VSGQDV").strip()


async def _jup_ultra_order(input_mint: str, output_mint: str, amount_atomic: int, taker: Optional[str] = None) -> Dict[str, Any]:
    """Get an indicative Ultra order response for pseudo-orderbook sampling."""
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(int(amount_atomic)),
        "taker": (taker or _jup_ultra_default_taker()),
    }
    headers = {"accept": "application/json"}
    if _JUP_API_KEY:
        headers["x-api-key"] = _JUP_API_KEY

    try:
        async with httpx.AsyncClient(timeout=_JUP_TIMEOUT_S) as client:
            r = await client.get(_JUP_ULTRA_ORDER_URL, params=params, headers=headers)
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "jupiter_ultra_order_request_failed",
                "exc": type(e).__name__,
                "message": str(e),
                "url": _JUP_ULTRA_ORDER_URL,
            },
        )

    if r.status_code in (401, 403):
        raise HTTPException(
            status_code=502,
            detail={
                "error": "jupiter_ultra_order_unauthorized",
                "message": "Set UTT_JUP_API_KEY in backend env if your Jupiter Ultra API requires it.",
                "url": _JUP_ULTRA_ORDER_URL,
            },
        )

    if r.status_code >= 400:
        body = (r.text or "")[:500]
        raise HTTPException(
            status_code=502,
            detail={"error": "jupiter_ultra_order_http_error", "status": r.status_code, "body": body},
        )

    try:
        data = r.json() or {}
    except Exception:
        raise HTTPException(status_code=502, detail={"error": "jupiter_ultra_order_non_json"})

    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail={"error": "jupiter_ultra_order_unexpected", "body": data})

    in_amt = int(data.get("inAmount") or 0)
    out_amt = int(data.get("outAmount") or 0)
    if in_amt <= 0 or out_amt <= 0:
        raise HTTPException(status_code=502, detail={"error": "no_routes", "jupiterUltra": data})

    return data

def _solana_rpc_url() -> str:
    # Default to public mainnet-beta RPC (free, rate-limited).
    return (os.getenv("SOLANA_RPC_URL") or "https://api.mainnet-beta.solana.com").strip()


async def _rpc(method: str, params: Optional[list] = None) -> Dict[str, Any]:
    url = _solana_rpc_url()
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}

    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.post(url, json=payload)
        if r.status_code == 429:
            raise HTTPException(status_code=429, detail="Solana RPC rate-limited (429). Set SOLANA_RPC_URL to a private RPC.")
        r.raise_for_status()
        data = r.json()

    if "error" in data:
        raise HTTPException(status_code=502, detail={"rpc_error": data["error"], "rpc_url": url})
    return data


async def _token_account_by_mint(owner_pubkey: str, mint: str, *, require: bool) -> Optional[str]:
    """Return an existing token account owned by `owner_pubkey` for `mint`.

    For Raydium swap build:
      - non-SOL input should provide a real existing source token account
      - non-SOL output can provide an existing destination token account if one already exists
    """
    try:
        data = await _rpc(
            "getTokenAccountsByOwner",
            [
                owner_pubkey,
                {"mint": mint},
                {"encoding": "jsonParsed", "commitment": "confirmed"},
            ],
        )
        items = ((data.get("result") or {}).get("value") or [])
        for it in items:
            try:
                pubkey = str(it.get("pubkey") or "").strip()
                if pubkey:
                    return pubkey
            except Exception:
                continue
    except HTTPException:
        raise
    except Exception as e:
        if require:
            raise HTTPException(
                status_code=500,
                detail={"error": "token_account_lookup_failed", "mint": mint, "message": str(e)},
            )
        return None

    if require:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "missing_input_token_account",
                "mint": mint,
                "message": "No existing token account found for non-SOL input mint on this wallet",
            },
        )
    return None




async def _jup_price_items_for_mints(mint_ids: List[str]) -> Dict[str, Dict[str, float]]:
    mint_ids = [str(m).strip() for m in (mint_ids or []) if str(m).strip()]
    if not mint_ids:
        return {}

    seen: set[str] = set()
    uniq: List[str] = []
    for mid in mint_ids:
        if mid in seen:
            continue
        seen.add(mid)
        uniq.append(mid)
    mint_ids = uniq[:50]

    cache_key = ",".join(sorted(mint_ids))
    now = time.time()
    async with _JUP_PRICES_LOCK:
        try:
            ts = float(_JUP_PRICES_CACHE.get("ts") or 0.0)
            key = str(_JUP_PRICES_CACHE.get("key") or "")
            if key == cache_key and (now - ts) <= float(_JUP_PRICES_TTL_S):
                items = _JUP_PRICES_CACHE.get("items") or {}
                if isinstance(items, dict):
                    return items
        except Exception:
            pass

        headers: Dict[str, str] = {}
        if _JUP_API_KEY:
            headers["x-api-key"] = _JUP_API_KEY

        last_err = None
        for attempt in range(4):
            try:
                async with httpx.AsyncClient(timeout=_JUP_TIMEOUT_S) as client:
                    r = await client.get(_JUP_PRICE_URL, params={"ids": ",".join(mint_ids)}, headers=headers)
                if r.status_code in (429, 503):
                    await asyncio.sleep((0.35 * (2 ** attempt)) + random.random() * 0.15)
                    continue
                r.raise_for_status()
                data = r.json() or {}
                raw = data.get("data") or data.get("prices") or data.get("items") or {}
                if not raw and isinstance(data, dict):
                    flat = {k: v for k, v in data.items() if isinstance(v, dict)}
                    if any(isinstance(v, dict) and (v.get("price") is not None or v.get("usdPrice") is not None or v.get("priceUsd") is not None or v.get("usd_price") is not None or v.get("price_usd") is not None) for v in flat.values()):
                        raw = flat
                items: Dict[str, Dict[str, float]] = {}
                if isinstance(raw, dict):
                    for mint, info in raw.items():
                        try:
                            if isinstance(info, (int, float, str)):
                                items[str(mint)] = {"price": float(info)}
                                continue
                            if not isinstance(info, dict):
                                continue
                            p = info.get("price") or info.get("priceUsd") or info.get("usdPrice") or info.get("usd")
                            if p is None:
                                continue
                            items[str(mint)] = {"price": float(p)}
                        except Exception:
                            continue
                _JUP_PRICES_CACHE["ts"] = now
                _JUP_PRICES_CACHE["key"] = cache_key
                _JUP_PRICES_CACHE["items"] = items
                return items
            except Exception as e:
                last_err = str(e)[:200]
                if attempt < 3:
                    await asyncio.sleep((0.25 * (2 ** attempt)) + random.random() * 0.15)
                    continue
        raise HTTPException(status_code=502, detail={"error": "jupiter_price_fetch_failed", "message": last_err or "unknown"})


async def _estimate_usd_value(mint: str, amount_ui: float) -> float:
    try:
        amt = float(amount_ui)
    except Exception:
        amt = 0.0
    if amt <= 0:
        return 0.0
    if mint in _STABLE_MINTS:
        return amt
    items = await _jup_price_items_for_mints([mint])
    px = float(((items.get(mint) or {}).get("price") or 0.0))
    if px <= 0:
        raise HTTPException(status_code=502, detail={"error": "missing_input_mint_usd_price", "mint": mint})
    return amt * px


async def _jup_trigger_request(method: str, url: str, *, params: Optional[Dict[str, Any]] = None, json_body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    headers = {"accept": "application/json"}
    if json_body is not None:
        headers["content-type"] = "application/json"
    if _JUP_API_KEY:
        headers["x-api-key"] = _JUP_API_KEY
    try:
        async with httpx.AsyncClient(timeout=_JUP_TIMEOUT_S) as client:
            resp = await client.request(method.upper(), url, params=params, json=json_body, headers=headers)
    except httpx.RequestError as e:
        raise HTTPException(status_code=502, detail={"error": "jupiter_trigger_request_failed", "exc": type(e).__name__, "message": str(e), "url": url})

    if resp.status_code in (401, 403):
        raise HTTPException(status_code=502, detail={"error": "jupiter_trigger_unauthorized", "message": "Set UTT_JUP_API_KEY in backend env / vault for Trigger API access.", "url": url})
    if resp.status_code >= 400:
        body: Any
        try:
            body = resp.json()
        except Exception:
            body = (resp.text or "")[:500]
        raise HTTPException(status_code=502, detail={"error": "jupiter_trigger_http_error", "status": resp.status_code, "url": url, "body": body})
    try:
        data = resp.json() or {}
    except Exception:
        raise HTTPException(status_code=502, detail={"error": "jupiter_trigger_non_json", "url": url})
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail={"error": "jupiter_trigger_unexpected", "url": url, "body": data})
    return data


class TriggerCreateOrderRequest(BaseModel):
    symbol: str = Field(..., description="BASE-QUOTE, e.g. UTTT-USDC")
    side: str = Field(..., description="buy|sell")
    quantity: float = Field(..., gt=0, description="Base-asset quantity in human units")
    limit_price: float = Field(..., gt=0, description="Limit price in QUOTE per BASE")
    user_pubkey: str = Field(..., description="User public key (wallet address)")
    payer: Optional[str] = Field(None, description="Optional alternate payer; defaults to user_pubkey")
    expired_at: Optional[str] = Field(None, description="Optional unix timestamp expiry (stringified unix seconds)")
    slippage_bps: int = Field(0, ge=0, le=5000, description="0 = exact mode")
    compute_unit_price: str = Field("auto", description="Jupiter computeUnitPrice; default auto")
    wrap_and_unwrap_sol: bool = Field(True, description="Wrap/unwrap native SOL when needed")


class TriggerOpenOrdersRequest(BaseModel):
    user_pubkey: str = Field(..., description="Wallet address to query")
    order_status: str = Field("active", description="active|history")
    page: int = Field(1, ge=1, le=1000)
    symbol: Optional[str] = Field(None, description="Optional BASE-QUOTE filter")


class TriggerCancelOrderRequest(BaseModel):
    user_pubkey: str = Field(..., description="Wallet address / maker")
    order: str = Field(..., description="Trigger order account to cancel")
    compute_unit_price: str = Field("auto", description="Jupiter computeUnitPrice; default auto")


class TriggerRegisterOpenOrderRequest(BaseModel):
    symbol: str = Field(..., description="BASE-QUOTE, e.g. UTTT-USDC")
    side: str = Field(..., description="buy|sell")
    quantity: float = Field(..., gt=0, description="Base-asset quantity in human units")
    limit_price: float = Field(..., gt=0, description="Limit price in QUOTE per BASE")
    user_pubkey: str = Field(..., description="User public key (wallet address)")
    signature: str = Field(..., description="Signed Solana transaction signature")
    request_id: Optional[str] = Field(None, description="Jupiter createOrder requestId")
    order: str = Field(..., description="Jupiter Trigger order account")
    expired_at: Optional[str] = Field(None, description="Optional unix timestamp expiry (stringified unix seconds)")



class TriggerMarkCanceledRequest(BaseModel):
    order: str = Field(..., description="Jupiter Trigger order account to mark canceled")
    signature: Optional[str] = Field(None, description="Cancel transaction signature")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _upsert_jupiter_open_order_row(
    db: Session,
    *,
    venue: str,
    venue_order_id: str,
    symbol_venue: str,
    symbol_canon: str,
    side: str,
    type_: str,
    status: str,
    qty: float,
    filled_qty: float,
    limit_price: float,
    created_at: Optional[datetime] = None,
    updated_at: Optional[datetime] = None,
) -> VenueOrderRow:
    now = _utcnow()
    created = created_at or now
    updated = updated_at or created

    existing = (
        db.query(VenueOrderRow)
        .filter(VenueOrderRow.venue == venue)
        .filter(VenueOrderRow.venue_order_id == venue_order_id)
        .first()
    )

    if existing is None:
        row = VenueOrderRow(
            venue=venue,
            venue_order_id=venue_order_id,
            symbol_venue=symbol_venue,
            symbol_canon=symbol_canon,
            side=side,
            type=type_,
            status=status,
            qty=qty,
            filled_qty=filled_qty,
            limit_price=limit_price,
            avg_fill_price=None,
            fee=None,
            fee_asset=None,
            total_after_fee=None,
            created_at=created,
            updated_at=updated,
            captured_at=now,
        )
        db.add(row)
        db.flush()
        return row

    existing.symbol_venue = symbol_venue or existing.symbol_venue
    if symbol_canon:
        existing.symbol_canon = symbol_canon
    if side:
        existing.side = side
    if type_:
        existing.type = type_
    if status:
        existing.status = status
    existing.qty = qty
    existing.filled_qty = filled_qty
    existing.limit_price = limit_price
    if existing.created_at is None:
        existing.created_at = created
    existing.updated_at = updated
    existing.captured_at = now
    db.add(existing)
    db.flush()
    return existing


async def _ray_auto_fee() -> str:
    try:
        async with httpx.AsyncClient(timeout=_RAY_TIMEOUT_S) as client:
            r = await client.get(_RAY_AUTO_FEE_URL, headers={"accept": "application/json"})
        if r.status_code >= 400:
            return "10000"
        data = r.json() or {}
        root = data.get("data") if isinstance(data, dict) else {}
        default = root.get("default") if isinstance(root, dict) else {}
        for key in ("h", "m", "vh"):
            val = default.get(key) if isinstance(default, dict) else None
            if val is not None:
                return str(val)
    except Exception:
        pass
    return "10000"


async def _ray_swap_quote(input_mint: str, output_mint: str, amount_atomic: int, slippage_bps: int) -> Dict[str, Any]:
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(int(amount_atomic)),
        "slippageBps": int(slippage_bps),
        "txVersion": _RAY_TX_VERSION,
    }
    try:
        async with httpx.AsyncClient(timeout=_RAY_TIMEOUT_S) as client:
            r = await client.get(_RAY_SWAP_BASE_IN_URL, params=params, headers={"accept": "application/json"})
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502,
            detail={"error": "raydium_quote_request_failed", "exc": type(e).__name__, "message": str(e), "url": _RAY_SWAP_BASE_IN_URL},
        )

    if r.status_code >= 400:
        body = (r.text or "")[:500]
        raise HTTPException(status_code=502, detail={"error": "raydium_quote_http_error", "status": r.status_code, "body": body})

    data = r.json() or {}
    if not isinstance(data, dict) or not data.get("success") or not isinstance(data.get("data"), dict):
        raise HTTPException(status_code=502, detail={"error": "raydium_quote_unexpected", "body": data})
    return data


async def _ray_swap_tx_build(
    quote_response: Dict[str, Any],
    wallet: str,
    *,
    wrap_sol: bool,
    unwrap_sol: bool,
    input_account: Optional[str] = None,
    output_account: Optional[str] = None,
) -> Dict[str, Any]:
    body = {
        "swapResponse": quote_response,
        "wallet": wallet,
        "txVersion": _RAY_TX_VERSION,
        "wrapSol": bool(wrap_sol),
        "unwrapSol": bool(unwrap_sol),
        "computeUnitPriceMicroLamports": await _ray_auto_fee(),
    }
    if input_account:
        body["inputAccount"] = input_account
    if output_account:
        body["outputAccount"] = output_account
    try:
        async with httpx.AsyncClient(timeout=_RAY_TIMEOUT_S) as client:
            r = await client.post(_RAY_SWAP_TX_BASE_IN_URL, json=body, headers={"content-type": "application/json", "accept": "application/json"})
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502,
            detail={"error": "raydium_swap_request_failed", "exc": type(e).__name__, "message": str(e), "url": _RAY_SWAP_TX_BASE_IN_URL},
        )

    if r.status_code >= 400:
        body_txt = (r.text or "")[:500]
        raise HTTPException(status_code=502, detail={"error": "raydium_swap_http_error", "status": r.status_code, "body": body_txt})

    data = r.json() or {}
    if not isinstance(data, dict) or not data.get("success") or not isinstance(data.get("data"), list) or not data.get("data"):
        raise HTTPException(status_code=502, detail={"error": "raydium_swap_unexpected", "body": data})
    return data


class SwapTxRequest(BaseModel):
    # Terminal symbol convention: BASE-QUOTE (e.g. UTTT-SOL)
    symbol: str = Field(..., description="BASE-QUOTE, e.g. UTTT-SOL")
    # BUY means buy BASE using QUOTE (spend QUOTE)
    # SELL means sell BASE for QUOTE (spend BASE)
    side: str = Field(..., description="buy|sell")
    # Amount in HUMAN units of the INPUT token:
    # - BUY  => QUOTE spend ("Total")
    # - SELL => BASE qty ("Qty")
    amount: float = Field(..., gt=0)
    slippage_bps: int = Field(100, ge=1, le=5000)
    user_pubkey: str = Field(..., description="User public key (wallet address)")


class RecordSubmitRequest(BaseModel):
    mode: Optional[str] = None
    symbol: Optional[str] = None
    side: Optional[str] = None
    signature: Optional[str] = None
    request_id: Optional[str] = None
    requestId: Optional[str] = None
    order: Optional[str] = None
    quantity: Optional[float] = None
    qty: Optional[float] = None
    limit_price: Optional[float] = None
    price: Optional[float] = None
    raw: Optional[Dict[str, Any]] = None



class UltraExecuteRequest(BaseModel):
    signed_transaction: Optional[str] = None
    signedTransaction: Optional[str] = None
    request_id: Optional[str] = None
    requestId: Optional[str] = None


@router.post("/jupiter/ultra_order")
async def jupiter_ultra_order(req: SwapTxRequest, db: Session = Depends(get_db)):
    base, quote = _parse_symbol(_normalize_symbol(req.symbol))

    side = (req.side or "").strip().lower()
    if side not in ("buy", "sell"):
        raise HTTPException(status_code=422, detail=f"Invalid side '{req.side}' (expected buy|sell)")

    base_mint, base_dec = await _resolve_mint_and_decimals(base, db=db)
    quote_mint, quote_dec = await _resolve_mint_and_decimals(quote, db=db)

    if side == "buy":
        input_mint, input_dec = quote_mint, quote_dec
        output_mint = base_mint
    else:
        input_mint, input_dec = base_mint, base_dec
        output_mint = quote_mint

    try:
        amount_atomic = int(round(float(req.amount) * (10 ** int(input_dec))))
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid amount")
    if amount_atomic <= 0:
        raise HTTPException(status_code=422, detail="Amount too small after decimal conversion")

    headers = {"accept": "application/json"}
    if _JUP_API_KEY:
        headers["x-api-key"] = _JUP_API_KEY

    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(amount_atomic),
        "taker": req.user_pubkey,
    }

    async with httpx.AsyncClient(timeout=_JUP_TIMEOUT_S) as client:
        resp = await client.get(_JUP_ULTRA_ORDER_URL, params=params, headers=headers)

    if resp.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={"error": "jupiter_ultra_order_failed", "status": resp.status_code, "body": resp.text},
        )

    data = resp.json()
    swap_tx = data.get("transaction")
    if not swap_tx:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "jupiter_ultra_missing_transaction",
                "body": data,
                "errorCode": data.get("errorCode"),
                "errorMessage": data.get("errorMessage"),
            },
        )

    return {
        "ok": True,
        "provider": "jupiter_ultra",
        "symbol": req.symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base,
        "quote": quote,
        "baseMint": base_mint,
        "quoteMint": quote_mint,
        "baseDecimals": int(base_dec),
        "quoteDecimals": int(quote_dec),
        "side": side,
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amountAtomic": amount_atomic,
        "transaction": swap_tx,
        "swapTransaction": swap_tx,
        "requestId": data.get("requestId"),
        "router": data.get("router"),
        "order": data,
    }


@router.post("/jupiter/ultra_execute")
async def jupiter_ultra_execute(req: UltraExecuteRequest):
    signed_tx = (req.signed_transaction or req.signedTransaction or "").strip()
    request_id = (req.request_id or req.requestId or "").strip()

    if not signed_tx:
        raise HTTPException(status_code=422, detail="Missing signedTransaction")
    if not request_id:
        raise HTTPException(status_code=422, detail="Missing requestId")

    headers = {"accept": "application/json", "content-type": "application/json"}
    if _JUP_API_KEY:
        headers["x-api-key"] = _JUP_API_KEY

    payload = {"signedTransaction": signed_tx, "requestId": request_id}

    async with httpx.AsyncClient(timeout=_JUP_TIMEOUT_S) as client:
        resp = await client.post(_JUP_ULTRA_EXECUTE_URL, json=payload, headers=headers)

    if resp.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={"error": "jupiter_ultra_execute_failed", "status": resp.status_code, "body": resp.text},
        )

    data = resp.json()
    return {"ok": True, "provider": "jupiter_ultra", **data}


@router.post("/jupiter/swap_tx")
async def jupiter_swap_tx(req: SwapTxRequest, db: Session = Depends(get_db)):
    base, quote = _parse_symbol(_normalize_symbol(req.symbol))

    side = (req.side or "").strip().lower()
    if side not in ("buy", "sell"):
        raise HTTPException(status_code=422, detail=f"Invalid side '{req.side}' (expected buy|sell)")

    base_mint, base_dec = await _resolve_mint_and_decimals(base, db=db)
    quote_mint, quote_dec = await _resolve_mint_and_decimals(quote, db=db)

    # BUY BASE using QUOTE => input=QUOTE, output=BASE
    # SELL BASE for QUOTE  => input=BASE,  output=QUOTE
    if side == "buy":
        input_mint, input_dec = quote_mint, quote_dec
        output_mint = base_mint
    else:
        input_mint, input_dec = base_mint, base_dec
        output_mint = quote_mint

    # Convert human -> atomic
    try:
        amount_atomic = int(round(float(req.amount) * (10 ** int(input_dec))))
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid amount")
    if amount_atomic <= 0:
        raise HTTPException(status_code=422, detail="Amount too small after decimal conversion")

    # 1) Quote
    quote_resp = await _jup_quote(input_mint=input_mint, output_mint=output_mint, amount_atomic=amount_atomic)

    # 2) Build swap tx (UNSIGNED)
    headers = {"accept": "application/json"}
    if _JUP_API_KEY:
        headers["x-api-key"] = _JUP_API_KEY

    payload = {
        "quoteResponse": quote_resp,
        "userPublicKey": req.user_pubkey,
        "wrapAndUnwrapSol": True,
    }

    async with httpx.AsyncClient(timeout=_JUP_TIMEOUT_S) as client:
        resp = await client.post(_JUP_SWAP_URL, json=payload, headers=headers)
    if resp.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={"error": "jupiter_swap_failed", "status": resp.status_code, "body": resp.text},
        )

    data = resp.json()
    swap_tx = data.get("swapTransaction")
    if not swap_tx:
        raise HTTPException(status_code=502, detail={"error": "missing_swapTransaction", "body": data})

    return {
        "ok": True,
        "symbol": req.symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base,
        "quote": quote,
        "baseMint": base_mint,
        "quoteMint": quote_mint,
        "baseDecimals": int(base_dec),
        "quoteDecimals": int(quote_dec),
        "side": side,
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amountAtomic": amount_atomic,
        "slippageBps": req.slippage_bps,
        "swapTransaction": swap_tx,  # base64 (UNSIGNED)
        "quote": quote_resp,
    }






@router.post("/raydium/swap_tx")
async def raydium_swap_tx(req: SwapTxRequest, db: Session = Depends(get_db)):
    base, quote = _parse_symbol(_normalize_symbol(req.symbol))

    side = (req.side or "").strip().lower()
    if side not in ("buy", "sell"):
        raise HTTPException(status_code=422, detail=f"Invalid side '{req.side}' (expected buy|sell)")

    base_mint, base_dec = await _resolve_mint_and_decimals(base, db=db)
    quote_mint, quote_dec = await _resolve_mint_and_decimals(quote, db=db)

    if side == "buy":
        input_mint, input_dec = quote_mint, quote_dec
        output_mint = base_mint
    else:
        input_mint, input_dec = base_mint, base_dec
        output_mint = quote_mint

    try:
        amount_atomic = int(round(float(req.amount) * (10 ** int(input_dec))))
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid amount")
    if amount_atomic <= 0:
        raise HTTPException(status_code=422, detail="Amount too small after decimal conversion")

    quote_resp = await _ray_swap_quote(
        input_mint=input_mint,
        output_mint=output_mint,
        amount_atomic=amount_atomic,
        slippage_bps=int(req.slippage_bps),
    )
    quote_data = quote_resp.get("data") or {}

    wrap_sol = input_mint == _SOL_MINTS.get("SOL")
    unwrap_sol = output_mint == _SOL_MINTS.get("SOL")

    input_account = None if wrap_sol else await _token_account_by_mint(req.user_pubkey, input_mint, require=True)
    output_account = None if unwrap_sol else await _token_account_by_mint(req.user_pubkey, output_mint, require=False)

    swap_resp = await _ray_swap_tx_build(
        quote_response=quote_resp,
        wallet=req.user_pubkey,
        wrap_sol=wrap_sol,
        unwrap_sol=unwrap_sol,
        input_account=input_account,
        output_account=output_account,
    )
    tx_list = swap_resp.get("data") or []
    swap_tx = None
    if tx_list and isinstance(tx_list[0], dict):
        swap_tx = tx_list[0].get("transaction")
    if not swap_tx:
        raise HTTPException(status_code=502, detail={"error": "missing_raydium_transaction", "body": swap_resp})

    return {
        "ok": True,
        "provider": "raydium",
        "symbol": req.symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base,
        "quote": quote,
        "baseMint": base_mint,
        "quoteMint": quote_mint,
        "baseDecimals": int(base_dec),
        "quoteDecimals": int(quote_dec),
        "side": side,
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amountAtomic": amount_atomic,
        "slippageBps": req.slippage_bps,
        "swapTransaction": swap_tx,
        "transactions": [t.get("transaction") for t in tx_list if isinstance(t, dict) and t.get("transaction")],
        "inputAccount": input_account,
        "outputAccount": output_account,
        "quote": quote_data,
        "raw": swap_resp,
    }


@router.post("/jupiter/trigger/create_order")
async def jupiter_trigger_create_order(req: TriggerCreateOrderRequest, db: Session = Depends(get_db)):
    base, quote = _parse_symbol(_normalize_symbol(req.symbol))
    side = (req.side or "").strip().lower()
    if side not in ("buy", "sell"):
        raise HTTPException(status_code=422, detail=f"Invalid side '{req.side}' (expected buy|sell)")

    base_mint, base_dec = await _resolve_mint_and_decimals(base, db=db)
    quote_mint, quote_dec = await _resolve_mint_and_decimals(quote, db=db)

    qty_base_ui = float(req.quantity)
    px_quote_per_base = float(req.limit_price)
    making_amount_ui = qty_base_ui * px_quote_per_base if side == "buy" else qty_base_ui
    taking_amount_ui = qty_base_ui if side == "buy" else qty_base_ui * px_quote_per_base

    input_mint = quote_mint if side == "buy" else base_mint
    output_mint = base_mint if side == "buy" else quote_mint
    input_dec = quote_dec if side == "buy" else base_dec
    output_dec = base_dec if side == "buy" else quote_dec

    making_amount_atomic = int(round(making_amount_ui * (10 ** int(input_dec))))
    taking_amount_atomic = int(round(taking_amount_ui * (10 ** int(output_dec))))
    if making_amount_atomic <= 0 or taking_amount_atomic <= 0:
        raise HTTPException(status_code=422, detail={"error": "amount_too_small_after_decimal_conversion"})

    input_notional_usd = await _estimate_usd_value(input_mint, making_amount_ui)
    if input_notional_usd + 1e-12 < float(_JUP_TRIGGER_MIN_USD):
        raise HTTPException(status_code=422, detail={
            "error": "jupiter_trigger_min_notional",
            "message": f"Jupiter limit orders require at least ${_JUP_TRIGGER_MIN_USD:.2f} of current input-token value.",
            "minimumUsd": float(_JUP_TRIGGER_MIN_USD),
            "inputMint": input_mint,
            "inputAmount": making_amount_ui,
            "inputValueUsd": input_notional_usd,
            "symbol": f"{base}-{quote}",
            "side": side,
        })

    params: Dict[str, Any] = {
        "makingAmount": str(making_amount_atomic),
        "takingAmount": str(taking_amount_atomic),
    }
    if int(req.slippage_bps or 0) > 0:
        params["slippageBps"] = int(req.slippage_bps)
    if req.expired_at is not None:
        try:
            params["expiredAt"] = str(int(str(req.expired_at).strip()))
        except Exception:
            raise HTTPException(status_code=422, detail={"error": "invalid_expired_at", "message": "expired_at must be a unix timestamp in seconds"})

    payload: Dict[str, Any] = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "maker": req.user_pubkey,
        "payer": req.payer or req.user_pubkey,
        "params": params,
        "computeUnitPrice": req.compute_unit_price or "auto",
        "wrapAndUnwrapSol": bool(req.wrap_and_unwrap_sol),
    }

    data = await _jup_trigger_request("POST", _JUP_TRIGGER_CREATE_URL, json_body=payload)
    transaction = data.get("transaction") or data.get("tx")
    request_id = data.get("requestId")
    order = data.get("order")
    if not transaction or not request_id:
        raise HTTPException(status_code=502, detail={"error": "jupiter_trigger_create_unexpected", "body": data})

    return {
        "ok": True,
        "symbol": req.symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "side": side,
        "base": base,
        "quote": quote,
        "baseMint": base_mint,
        "quoteMint": quote_mint,
        "baseDecimals": int(base_dec),
        "quoteDecimals": int(quote_dec),
        "inputMint": input_mint,
        "outputMint": output_mint,
        "inputDecimals": int(input_dec),
        "outputDecimals": int(output_dec),
        "quantity": qty_base_ui,
        "limitPrice": px_quote_per_base,
        "makingAmount": str(making_amount_atomic),
        "takingAmount": str(taking_amount_atomic),
        "minimumUsd": float(_JUP_TRIGGER_MIN_USD),
        "inputValueUsd": input_notional_usd,
        "requestId": request_id,
        "order": order,
        "transaction": transaction,
        "raw": data,
    }


@router.get("/jupiter/trigger/open_orders")
async def jupiter_trigger_open_orders(
    user_pubkey: str = Query(..., min_length=32, max_length=64, description="Wallet address to query"),
    order_status: str = Query("active", description="active|history"),
    page: int = Query(1, ge=1, le=1000),
    symbol: Optional[str] = Query(None, description="Optional BASE-QUOTE filter"),
    db: Session = Depends(get_db),
):
    status_norm = (order_status or "active").strip().lower()
    if status_norm not in ("active", "history"):
        raise HTTPException(status_code=422, detail="order_status must be active or history")

    params: Dict[str, Any] = {"user": user_pubkey, "orderStatus": status_norm, "page": int(page)}
    base = quote = None
    if symbol:
        base, quote = _parse_symbol(_normalize_symbol(symbol))
        base_mint, _ = await _resolve_mint_and_decimals(base, db=db)
        quote_mint, _ = await _resolve_mint_and_decimals(quote, db=db)
        params["inputMint"] = quote_mint
        params["outputMint"] = base_mint

    data = await _jup_trigger_request("GET", _JUP_TRIGGER_OPEN_URL, params=params)
    return {
        "ok": True,
        "user": user_pubkey,
        "orderStatus": status_norm,
        "page": int(page),
        "symbol": f"{base}-{quote}" if base and quote else None,
        "items": data.get("orders") or data.get("items") or data.get("data") or [],
        "hasMoreData": bool(data.get("hasMoreData")),
        "raw": data,
    }


@router.post("/jupiter/trigger/cancel_order")
async def jupiter_trigger_cancel_order(req: TriggerCancelOrderRequest):
    payload = {
        "maker": req.user_pubkey,
        "order": req.order,
        "computeUnitPrice": req.compute_unit_price or "auto",
    }
    data = await _jup_trigger_request("POST", _JUP_TRIGGER_CANCEL_URL, json_body=payload)
    transaction = data.get("transaction") or data.get("tx")
    request_id = data.get("requestId")
    if not transaction or not request_id:
        raise HTTPException(status_code=502, detail={"error": "jupiter_trigger_cancel_unexpected", "body": data})
    return {
        "ok": True,
        "order": req.order,
        "requestId": request_id,
        "transaction": transaction,
        "raw": data,
    }


@router.post("/jupiter/trigger/register_open_order")
async def jupiter_trigger_register_open_order(req: TriggerRegisterOpenOrderRequest, db: Session = Depends(get_db)):
    base, quote = _parse_symbol(_normalize_symbol(req.symbol))
    side = (req.side or "").strip().lower()
    if side not in ("buy", "sell"):
        raise HTTPException(status_code=422, detail=f"Invalid side '{req.side}' (expected buy|sell)")

    qty_base_ui = float(req.quantity)
    px_quote_per_base = float(req.limit_price)

    if not req.order or not str(req.order).strip():
        raise HTTPException(status_code=422, detail={"error": "missing_order_account"})
    if not req.signature or not str(req.signature).strip():
        raise HTTPException(status_code=422, detail={"error": "missing_signature"})

    now = _utcnow()
    row = _upsert_jupiter_open_order_row(
        db,
        venue="solana_jupiter",
        venue_order_id=str(req.order).strip(),
        symbol_venue=str(req.symbol).strip(),
        symbol_canon=f"{base}-{quote}",
        side=side,
        type_="limit",
        status="open",
        qty=qty_base_ui,
        filled_qty=0.0,
        limit_price=px_quote_per_base,
        created_at=now,
        updated_at=now,
    )
    db.commit()

    return {
        "ok": True,
        "mode": "limit",
        "registered": True,
        "source": "venue_row",
        "id": row.id,
        "venue": row.venue,
        "venue_order_id": row.venue_order_id,
        "symbol": row.symbol_canon or row.symbol_venue,
        "side": row.side,
        "type": row.type,
        "status": row.status,
        "qty": float(row.qty) if row.qty is not None else None,
        "filled_qty": float(row.filled_qty) if row.filled_qty is not None else None,
        "limit_price": float(row.limit_price) if row.limit_price is not None else None,
        "signature": str(req.signature).strip(),
        "requestId": (str(req.request_id).strip() if req.request_id is not None else None),
        "order": str(req.order).strip(),
        "expiredAt": (str(req.expired_at).strip() if req.expired_at is not None else None),
        "created_at": row.created_at,
        "updated_at": row.updated_at,
        "captured_at": row.captured_at,
    }


@router.post("/jupiter/record_submit")
async def jupiter_record_submit(req: RecordSubmitRequest, db: Session = Depends(get_db)):
    mode = str(req.mode or "swap").strip().lower() or "swap"
    signature = str(req.signature or "").strip() or None
    request_id = str(req.request_id or req.requestId or "").strip() or None
    order_id = str(req.order or "").strip() or None
    symbol = str(req.symbol or "").strip() or None
    side = str(req.side or "").strip().lower() or None
    qty = req.quantity if req.quantity is not None else req.qty
    limit_price = req.limit_price if req.limit_price is not None else req.price

    recorded = False
    row = None

    # Limit orders already have a dedicated registration route.
    # Keep this endpoint backward-compatible for older submit callers and
    # optionally touch an existing open-order row if the frontend includes an order id.
    if order_id:
        existing = (
            db.query(VenueOrderRow)
            .filter(VenueOrderRow.venue == "solana_jupiter")
            .filter(VenueOrderRow.venue_order_id == order_id)
            .first()
        )
        if existing is not None:
            existing.updated_at = _utcnow()
            existing.captured_at = _utcnow()
            db.add(existing)
            db.commit()
            row = existing
            recorded = True

    return {
        "ok": True,
        "recorded": recorded,
        "source": ("venue_row" if recorded else "ack_only"),
        "mode": mode,
        "venue": "solana_jupiter",
        "symbol": symbol or None,
        "side": side,
        "quantity": float(qty) if qty is not None else None,
        "limit_price": float(limit_price) if limit_price is not None else None,
        "signature": signature,
        "requestId": request_id,
        "order": order_id,
        "id": (row.id if row is not None else None),
    }


@router.get("/balances")
async def solana_balances(
    address: str = Query(..., min_length=32, max_length=64, description="Solana public key (base58)"),
) -> Dict[str, Any]:
    """
    Returns SOL balance + SPL token balances for `address`.

    This is a READ-ONLY endpoint. It uses SOLANA_RPC_URL if set, else public mainnet-beta RPC.
    """
    # SOL balance (lamports)
    b = await _rpc("getBalance", [address, {"commitment": "confirmed"}])
    lamports = int((b.get("result") or {}).get("value") or 0)
    sol = lamports / 1_000_000_000

    
    # SPL tokens (query BOTH classic SPL Token + Token-2022, aggregate by mint)
    PROGRAM_TOKEN = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
    PROGRAM_TOKEN_2022 = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"

    ta_legacy = await _rpc(
        "getTokenAccountsByOwner",
        [
            address,
            {"programId": PROGRAM_TOKEN},
            {"encoding": "jsonParsed", "commitment": "confirmed"},
        ],
    )
    ta_2022 = await _rpc(
        "getTokenAccountsByOwner",
        [
            address,
            {"programId": PROGRAM_TOKEN_2022},
            {"encoding": "jsonParsed", "commitment": "confirmed"},
        ],
    )

    by_mint: Dict[str, Dict[str, Any]] = {}

    def _add_token_row(mint: str, amount_str: Any, decimals: Any) -> None:
        if not mint:
            return
        try:
            base = int(amount_str) if amount_str is not None else 0
        except Exception:
            base = 0
        try:
            dec = int(decimals) if decimals is not None else None
        except Exception:
            dec = None

        row = by_mint.get(mint)
        if row is None:
            row = {"mint": mint, "amount_atomic": 0, "decimals": dec}
            by_mint[mint] = row

        row["amount_atomic"] = int(row.get("amount_atomic") or 0) + base
        if row.get("decimals") is None and dec is not None:
            row["decimals"] = dec

    for ta in (ta_legacy, ta_2022):
        for it in ((ta.get("result") or {}).get("value") or []):
            try:
                acc = (it.get("account") or {}).get("data", {})
                parsed = (acc.get("parsed") or {}).get("info") or {}
                mint = parsed.get("mint")
                token_amount = (parsed.get("tokenAmount") or {})
                _add_token_row(
                    mint=mint,
                    amount_str=token_amount.get("amount"),
                    decimals=token_amount.get("decimals"),
                )
            except Exception:
                continue

    # Build token rows (uiAmount is computed from atomic+decimals to avoid uiAmount=null cases)
    tokens: List[Dict[str, Any]] = []
    for mint, row in by_mint.items():
        d = row.get("decimals")
        amt_int = int(row.get("amount_atomic") or 0)
        ui_amt = None
        if d is not None:
            try:
                ui_amt = amt_int / (10 ** int(d))
            except Exception:
                ui_amt = None
        tokens.append(
            {
                "mint": mint,
                "uiAmount": ui_amt,
                "amount": str(amt_int),
                "decimals": d,
            }
        )

    # Attach symbols from Token Registry (match on any mint-like field; blank venue = global)
    try:
        mints = [t.get("mint") for t in tokens if t.get("mint")]
        if mints:
            mint_cols = []
            for col in ("mint", "address", "contract_address", "mint_address"):
                if hasattr(TokenRegistry, col):
                    mint_cols.append(getattr(TokenRegistry, col))

            venue_col = getattr(TokenRegistry, "venue", None)
            sym_col = getattr(TokenRegistry, "symbol", None)

            if mint_cols and sym_col is not None:
                from sqlalchemy import or_, and_

                mint_match = or_(*[c.in_(mints) for c in mint_cols])

                if venue_col is not None:
                    venue_match = or_(
                        venue_col.is_(None),
                        venue_col == "",
                        venue_col == "solana",
                        venue_col == "solana_jupiter",
                    )
                    q = db.query(TokenRegistry).filter(and_(mint_match, venue_match))
                else:
                    q = db.query(TokenRegistry).filter(mint_match)

                rows = q.all()
                mint_to_sym: Dict[str, str] = {}
                for r in rows:
                    sym = getattr(r, "symbol", None)
                    if not sym:
                        continue
                    for colname in ("mint", "address", "contract_address", "mint_address"):
                        if hasattr(r, colname):
                            v = getattr(r, colname, None)
                            if v:
                                mint_to_sym[str(v)] = sym

                for t in tokens:
                    sym = mint_to_sym.get(t.get("mint"))
                    if sym:
                        t["symbol"] = sym
    except Exception:
        pass

    # Prefer showing symbol-known assets first
    tokens.sort(key=lambda x: (0 if x.get("symbol") else 1, -(float(x.get("uiAmount") or 0.0))))

    return {
        "ok": True,
        "rpc_url": _solana_rpc_url(),
        "address": address,
        "sol": sol,
        "lamports": lamports,
        "tokens": tokens,
    }




@router.get("/resolve")
async def solana_resolve_asset(
    asset: str = Query(..., description="Ticker (e.g. SOL, USDC, UTTT) or mint:<ADDRESS>"),
    db: Session = Depends(get_db),
) -> Dict[str, Any]:
    """Resolve a symbol or mint override to (mint, decimals).

    DEX-only helper for frontend (balances mapping, debug, etc.).
    """
    mint, dec = await _resolve_mint_and_decimals(asset, db=db)
    return {"ok": True, "asset": asset, "mint": mint, "decimals": int(dec)}
@router.get("/token_balance")
async def solana_token_balance(owner: str, mint: str):
    """Return SPL token balance for a specific owner+mimnt (watchlist helper)."""
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenAccountsByOwner",
        "params": [
            owner,
            {"mint": mint},
            {"encoding": "jsonParsed"},
        ],
    }
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(_solana_rpc_url(), json=payload)
        r.raise_for_status()
        data = r.json()
    result = (data or {}).get("result") or {}
    value = result.get("value") or []
    total_atomic = 0
    decimals = 0
    for acct in value:
        try:
            info = acct.get("account", {}).get("data", {}).get("parsed", {}).get("info", {})
            token_amt = info.get("tokenAmount") or {}
            amt_str = token_amt.get("amount") or "0"
            dec = token_amt.get("decimals")
            if dec is not None:
                decimals = int(dec)
            total_atomic += int(amt_str)
        except Exception:
            continue
    return {"owner": owner, "mint": mint, "amount_atomic": total_atomic, "decimals": decimals, "accounts": len(value)}


@router.get("/jupiter/orderbook")
async def jupiter_pseudo_orderbook(
    symbol: str = Query(..., description="Symbol pair, e.g. UTTT-SOL (BASE-QUOTE)"),
    depth: int = Query(10, ge=1, le=50, description="Requested depth; we cap to a small sample for quotes"),
    db: Session = Depends(get_db),
):
    """Pseudo-orderbook for Solana-Jupiter using Jupiter quotes.

    Not a real CLOB ladder. We synthesize levels by sampling quotes at a few sizes.

    Semantics (BASE-QUOTE) match a traditional orderbook:
      - asks: BUY BASE using QUOTE  => quote QUOTE -> BASE (these are the prices a buyer pays)
      - bids: SELL BASE for QUOTE   => quote BASE  -> QUOTE (these are the prices a seller receives)

    We return sizes in BASE units.
    """

    base, quote = _parse_symbol(_normalize_symbol(symbol))
    base_mint, base_dec = await _resolve_mint_and_decimals(base, db=db)
    quote_mint, quote_dec = await _resolve_mint_and_decimals(quote, db=db)

    # Keep quote load low: cap to <=10 sampled levels.
    n = max(1, min(int(depth), 10))

    # Helpers
    def _atomic_from_ui(ui_amt: float, dec: int) -> int:
        return int(round(float(ui_amt) * (10 ** int(dec))))

    def _ui_from_atomic(amt: int, dec: int) -> float:
        return float(amt) / (10 ** int(dec))


    def _suggest_price_decimals(levels: List[Dict[str, Any]], fallback: int) -> int:
        """Suggest display decimals for QUOTE-per-BASE prices."""
        best = int(fallback)
        for lvl in levels or []:
            try:
                px = float(lvl.get("price"))
            except Exception:
                continue
            if not (px > 0):
                continue
            s = f"{px:.12f}".rstrip("0").rstrip(".")
            if "." not in s:
                continue
            dec = len(s.split(".", 1)[1])
            if dec > best:
                best = dec
        # Preserve enough precision for order entry, but do not advertise more than 9 decimals.
        return max(int(fallback), min(best, 9))

    sample_errors: List[Dict[str, Any]] = []

    # ── Asks (buyers): spend QUOTE to receive BASE ─────────────────────────────
    # Sample sizes in QUOTE.
    if quote in ("SOL", "WSOL") or quote_mint == _SOL_MINTS.get("SOL"):
        ask_sizes_quote = [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0][:n]
    else:
        # For stable-like quotes, try small-to-medium.
        ask_sizes_quote = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000][:n]

    asks: List[Dict[str, Any]] = []
    for qsz in ask_sizes_quote:
        amt_in = _atomic_from_ui(qsz, quote_dec)
        try:
            qt = await _jup_quote(quote_mint, base_mint, amt_in)
            in_amt = int(qt.get("inAmount") or amt_in)
            out_amt = int(qt.get("outAmount") or 0)
            if in_amt <= 0 or out_amt <= 0:
                continue
            quote_ui = _ui_from_atomic(in_amt, quote_dec)
            base_ui = _ui_from_atomic(out_amt, base_dec)
            if base_ui <= 0:
                continue
            price = quote_ui / base_ui  # QUOTE per BASE
            asks.append({"price": price, "size": base_ui})
        except HTTPException as e:
            if len(sample_errors) < 8:
                sample_errors.append({"side": "ask", "amount_ui": qsz, "denom": quote, "detail": e.detail})
            continue

    # ── Bids (sellers): spend BASE to receive QUOTE ───────────────────────────
    # Sample sizes in BASE.
    if base in ("SOL", "WSOL") or base_mint == _SOL_MINTS.get("SOL"):
        bid_sizes_base = [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0][:n]
    elif int(base_dec) <= 6:
        bid_sizes_base = [1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000, 200_000, 500_000, 1_000_000][:n]
    else:
        bid_sizes_base = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000][:n]

    bids: List[Dict[str, Any]] = []
    for bsz in bid_sizes_base:
        amt_in = _atomic_from_ui(bsz, base_dec)
        try:
            qt = await _jup_quote(base_mint, quote_mint, amt_in)
            in_amt = int(qt.get("inAmount") or amt_in)
            out_amt = int(qt.get("outAmount") or 0)
            if in_amt <= 0 or out_amt <= 0:
                continue
            base_ui = _ui_from_atomic(in_amt, base_dec)
            quote_ui = _ui_from_atomic(out_amt, quote_dec)
            if base_ui <= 0:
                continue
            price = quote_ui / base_ui  # QUOTE per BASE
            bids.append({"price": price, "size": base_ui})
        except HTTPException as e:
            if len(sample_errors) < 8:
                sample_errors.append({"side": "bid", "amount_ui": bsz, "denom": base, "detail": e.detail})
            continue

    # If both sides empty, surface why (don’t silently return empties).
    if not bids and not asks:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "no_quote_levels",
                "message": "No routable Jupiter quotes for sampled sizes",
                "rawSymbol": symbol,
                "resolvedSymbol": f"{base}-{quote}",
                "quoteUrl": _JUP_QUOTE_URL,
                "swapUrl": _JUP_SWAP_URL,
                "usedApiKey": bool(_JUP_API_KEY),
                "sampleErrors": sample_errors,
            },
        )

    # Typical orderbook convention:
    # - asks sorted low→high
    # - bids sorted high→low
    asks.sort(key=lambda x: x.get("price") or 0)
    bids.sort(key=lambda x: -(x.get("price") or 0))

    # Suggested formatting hints for UI.
    # Price precision is QUOTE-per-BASE precision, not quote-token decimals.
    # Low-priced assets quoted in USDC/USDT can require >6 decimals.
    price_dec_suggested = _suggest_price_decimals(asks + bids, int(quote_dec))

    # UI currently displays a compact capped precision, so the non-crossing guardrail
    # must operate at that visible precision, not only at hidden extra decimals.
    display_price_decimals = max(1, min(int(price_dec_suggested or 0), 8))
    display_tick = 10 ** (-display_price_decimals)

    def _round_up_to_tick(px: float, tick: float) -> float:
        import math
        return math.ceil(px / tick) * tick

    def _round_down_to_tick(px: float, tick: float) -> float:
        import math
        return math.floor(px / tick) * tick

    # Normalize visible ladder monotonicity at display precision first.
    # This prevents duplicate visible prices and ensures a clean displayed ladder.
    if bids:
        prev_bid = None
        norm_bids = []
        for lvl in bids:
            try:
                px = float(lvl.get("price") or 0.0)
                sz = float(lvl.get("size") or 0.0)
            except Exception:
                continue
            if px <= 0 or sz <= 0:
                continue
            px = _round_down_to_tick(px, display_tick)
            if prev_bid is not None and px >= prev_bid:
                px = max(display_tick, prev_bid - display_tick)
            norm_bids.append({"price": px, "size": sz})
            prev_bid = px
        if norm_bids:
            bids = norm_bids

    if asks:
        prev_ask = None
        norm_asks = []
        for lvl in asks:
            try:
                px = float(lvl.get("price") or 0.0)
                sz = float(lvl.get("size") or 0.0)
            except Exception:
                continue
            if px <= 0 or sz <= 0:
                continue
            px = _round_up_to_tick(px, display_tick)
            if prev_ask is not None and px <= prev_ask:
                px = prev_ask + display_tick
            norm_asks.append({"price": px, "size": sz})
            prev_ask = px
        if norm_asks:
            asks = norm_asks

    # Synthetic-book sanity pass:
    # independent ask/bid route samples can occasionally cross.
    # Displayed book must always satisfy best_bid < best_ask at visible precision.
    crossedBookCorrected = False
    crossedBy = None
    if asks and bids:
        try:
            best_ask = float(asks[0].get("price") or 0.0)
            best_bid = float(bids[0].get("price") or 0.0)
            if best_ask > 0 and best_bid > 0 and best_ask <= best_bid:
                crossedBookCorrected = True
                crossedBy = best_bid - best_ask

                floor_ask = best_bid + display_tick
                prev_px = floor_ask
                corrected_asks = []
                for lvl in asks:
                    try:
                        px = float(lvl.get("price") or 0.0)
                        sz = float(lvl.get("size") or 0.0)
                    except Exception:
                        continue
                    if px <= 0 or sz <= 0:
                        continue
                    adj = px if px >= prev_px else prev_px
                    # Keep corrected asks aligned to visible tick and strictly increasing.
                    adj = _round_up_to_tick(adj, display_tick)
                    if corrected_asks and adj <= corrected_asks[-1]["price"]:
                        adj = corrected_asks[-1]["price"] + display_tick
                    corrected_asks.append({"price": adj, "size": sz})
                    prev_px = adj + display_tick

                if corrected_asks:
                    asks = corrected_asks
        except Exception:
            # Never fail the endpoint because of the guardrail itself.
            pass

    size_dec_suggested = int(base_dec)
    # Avoid ultra-long SOL sizes in UI; keep precision but readable.
    if size_dec_suggested > 6:
        size_dec_suggested = 6

    return {
        "ok": True,
        "venue": "solana_jupiter",
        "rawSymbol": symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base,
        "quote": quote,
        "baseMint": base_mint,
        "quoteMint": quote_mint,
        "baseDecimals": int(base_dec),
        "quoteDecimals": int(quote_dec),
        "priceDecimals": price_dec_suggested,
        "displayPriceDecimals": display_price_decimals,
        "sizeDecimals": size_dec_suggested,
        "bids": bids,
        "asks": asks,
        "crossedBookCorrected": crossedBookCorrected,
        "crossedBy": crossedBy,
    }






@router.get("/jupiter/ultra_orderbook")
async def jupiter_ultra_pseudo_orderbook(
    symbol: str = Query(..., description="Symbol pair, e.g. UTTT-SOL (BASE-QUOTE)"),
    depth: int = Query(10, ge=1, le=50, description="Requested depth; we cap to a small sample for quotes"),
    db: Session = Depends(get_db),
):
    """Pseudo-orderbook for Solana-Jupiter Ultra using sampled Ultra orders.

    Not a real CLOB ladder. We synthesize levels by sampling Ultra order responses.
    """
    base, quote = _parse_symbol(_normalize_symbol(symbol))
    base_mint, base_dec = await _resolve_mint_and_decimals(base, db=db)
    quote_mint, quote_dec = await _resolve_mint_and_decimals(quote, db=db)

    n = max(1, min(int(depth), 10))

    def _atomic_from_ui(ui_amt: float, dec: int) -> int:
        return int(round(float(ui_amt) * (10 ** int(dec))))

    def _ui_from_atomic(amt: int, dec: int) -> float:
        return float(amt) / (10 ** int(dec))

    def _suggest_price_decimals(levels: List[Dict[str, Any]], fallback: int) -> int:
        best = int(fallback)
        for lvl in levels or []:
            try:
                px = float(lvl.get("price"))
            except Exception:
                continue
            if not (px > 0):
                continue
            s = f"{px:.12f}".rstrip("0").rstrip(".")
            if "." not in s:
                continue
            dec = len(s.split(".", 1)[1])
            if dec > best:
                best = dec
        return max(int(fallback), min(best, 9))

    sample_errors: List[Dict[str, Any]] = []

    if quote in ("SOL", "WSOL") or quote_mint == _SOL_MINTS.get("SOL"):
        ask_sizes_quote = [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0][:n]
    else:
        ask_sizes_quote = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000][:n]

    asks: List[Dict[str, Any]] = []
    ultra_router = None
    for qsz in ask_sizes_quote:
        amt_in = _atomic_from_ui(qsz, quote_dec)
        try:
            qt = await _jup_ultra_order(quote_mint, base_mint, amt_in)
            in_amt = int(qt.get("inAmount") or amt_in)
            out_amt = int(qt.get("outAmount") or 0)
            if in_amt <= 0 or out_amt <= 0:
                continue
            quote_ui = _ui_from_atomic(in_amt, quote_dec)
            base_ui = _ui_from_atomic(out_amt, base_dec)
            if base_ui <= 0:
                continue
            ultra_router = ultra_router or qt.get("router")
            asks.append({"price": quote_ui / base_ui, "size": base_ui})
        except HTTPException as e:
            if len(sample_errors) < 8:
                sample_errors.append({"side": "ask", "amount_ui": qsz, "denom": quote, "detail": e.detail})
            continue

    if base in ("SOL", "WSOL") or base_mint == _SOL_MINTS.get("SOL"):
        bid_sizes_base = [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0][:n]
    elif int(base_dec) <= 6:
        bid_sizes_base = [1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000][:n]
    else:
        bid_sizes_base = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000][:n]

    bids: List[Dict[str, Any]] = []
    for bsz in bid_sizes_base:
        amt_in = _atomic_from_ui(bsz, base_dec)
        try:
            qt = await _jup_ultra_order(base_mint, quote_mint, amt_in)
            in_amt = int(qt.get("inAmount") or amt_in)
            out_amt = int(qt.get("outAmount") or 0)
            if in_amt <= 0 or out_amt <= 0:
                continue
            base_ui = _ui_from_atomic(in_amt, base_dec)
            quote_ui = _ui_from_atomic(out_amt, quote_dec)
            if base_ui <= 0:
                continue
            ultra_router = ultra_router or qt.get("router")
            bids.append({"price": quote_ui / base_ui, "size": base_ui})
        except HTTPException as e:
            if len(sample_errors) < 8:
                sample_errors.append({"side": "bid", "amount_ui": bsz, "denom": base, "detail": e.detail})
            continue

    if not bids and not asks:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "no_quote_levels",
                "message": "No routable Jupiter Ultra orders for sampled sizes",
                "rawSymbol": symbol,
                "resolvedSymbol": f"{base}-{quote}",
                "orderUrl": _JUP_ULTRA_ORDER_URL,
                "usedApiKey": bool(_JUP_API_KEY),
                "sampleErrors": sample_errors,
            },
        )

    asks.sort(key=lambda x: x.get("price") or 0)
    bids.sort(key=lambda x: -(x.get("price") or 0))

    price_dec_suggested = _suggest_price_decimals(asks + bids, int(quote_dec))
    display_price_decimals = max(1, min(int(price_dec_suggested or 0), 8))
    display_tick = 10 ** (-display_price_decimals)

    def _round_up_to_tick(px: float, tick: float) -> float:
        import math
        return math.ceil(px / tick) * tick

    def _round_down_to_tick(px: float, tick: float) -> float:
        import math
        return math.floor(px / tick) * tick

    if bids:
        prev_bid = None
        norm_bids = []
        for lvl in bids:
            try:
                px = float(lvl.get("price") or 0.0)
                sz = float(lvl.get("size") or 0.0)
            except Exception:
                continue
            if px <= 0 or sz <= 0:
                continue
            px = _round_down_to_tick(px, display_tick)
            if prev_bid is not None and px >= prev_bid:
                px = max(display_tick, prev_bid - display_tick)
            norm_bids.append({"price": px, "size": sz})
            prev_bid = px
        if norm_bids:
            bids = norm_bids

    if asks:
        prev_ask = None
        norm_asks = []
        for lvl in asks:
            try:
                px = float(lvl.get("price") or 0.0)
                sz = float(lvl.get("size") or 0.0)
            except Exception:
                continue
            if px <= 0 or sz <= 0:
                continue
            px = _round_up_to_tick(px, display_tick)
            if prev_ask is not None and px <= prev_ask:
                px = prev_ask + display_tick
            norm_asks.append({"price": px, "size": sz})
            prev_ask = px
        if norm_asks:
            asks = norm_asks

    crossedBookCorrected = False
    crossedBy = None
    if asks and bids:
        try:
            best_ask = float(asks[0].get("price") or 0.0)
            best_bid = float(bids[0].get("price") or 0.0)
            if best_ask > 0 and best_bid > 0 and best_ask <= best_bid:
                crossedBookCorrected = True
                crossedBy = best_bid - best_ask
                floor_ask = best_bid + display_tick
                prev_px = floor_ask
                corrected_asks = []
                for lvl in asks:
                    try:
                        px = float(lvl.get("price") or 0.0)
                        sz = float(lvl.get("size") or 0.0)
                    except Exception:
                        continue
                    if px <= 0 or sz <= 0:
                        continue
                    adj = px if px >= prev_px else prev_px
                    adj = _round_up_to_tick(adj, display_tick)
                    if corrected_asks and adj <= corrected_asks[-1]["price"]:
                        adj = corrected_asks[-1]["price"] + display_tick
                    corrected_asks.append({"price": adj, "size": sz})
                    prev_px = adj + display_tick
                if corrected_asks:
                    asks = corrected_asks
        except Exception:
            pass

    size_dec_suggested = int(base_dec)
    if size_dec_suggested > 6:
        size_dec_suggested = 6

    return {
        "ok": True,
        "venue": "solana_jupiter",
        "router": "ultra",
        "ultraRouter": ultra_router,
        "rawSymbol": symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base,
        "quote": quote,
        "baseMint": base_mint,
        "quoteMint": quote_mint,
        "baseDecimals": int(base_dec),
        "quoteDecimals": int(quote_dec),
        "priceDecimals": price_dec_suggested,
        "displayPriceDecimals": display_price_decimals,
        "sizeDecimals": size_dec_suggested,
        "bids": bids,
        "asks": asks,
        "crossedBookCorrected": crossedBookCorrected,
        "crossedBy": crossedBy,
    }

@router.get("/raydium/orderbook")
async def raydium_pseudo_orderbook(
    symbol: str = Query(..., description="Symbol pair, e.g. UTTT-USDC (BASE-QUOTE)"),
    depth: int = Query(10, ge=1, le=50, description="Requested depth; we cap to a small sample for quotes"),
    db: Session = Depends(get_db),
):
    """Pseudo-orderbook for Solana-Raydium using sampled Raydium quotes."""

    base, quote = _parse_symbol(_normalize_symbol(symbol))
    base_mint, base_dec = await _resolve_mint_and_decimals(base, db=db)
    quote_mint, quote_dec = await _resolve_mint_and_decimals(quote, db=db)

    n = max(1, min(int(depth), 10))

    def _atomic_from_ui(ui_amt: float, dec: int) -> int:
        return int(round(float(ui_amt) * (10 ** int(dec))))

    def _ui_from_atomic(amt: int, dec: int) -> float:
        return float(amt) / (10 ** int(dec))

    def _suggest_price_decimals(levels: List[Dict[str, Any]], fallback: int) -> int:
        best = int(fallback)
        for lvl in levels or []:
            try:
                px = float(lvl.get("price"))
            except Exception:
                continue
            if not (px > 0):
                continue
            s = f"{px:.12f}".rstrip("0").rstrip(".")
            if "." not in s:
                continue
            dec = len(s.split(".", 1)[1])
            if dec > best:
                best = dec
        return max(int(fallback), min(best, 9))

    sample_errors: List[Dict[str, Any]] = []

    if quote in ("SOL", "WSOL") or quote_mint == _SOL_MINTS.get("SOL"):
        ask_sizes_quote = [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0][:n]
    else:
        ask_sizes_quote = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000][:n]

    asks: List[Dict[str, Any]] = []
    for qsz in ask_sizes_quote:
        amt_in = _atomic_from_ui(qsz, quote_dec)
        try:
            qt = await _ray_swap_quote(quote_mint, base_mint, amt_in, 100)
            qd = qt.get("data") or {}
            in_amt = int(qd.get("inputAmount") or amt_in)
            out_amt = int(qd.get("outputAmount") or 0)
            if in_amt <= 0 or out_amt <= 0:
                continue
            quote_ui = _ui_from_atomic(in_amt, quote_dec)
            base_ui = _ui_from_atomic(out_amt, base_dec)
            if base_ui <= 0:
                continue
            asks.append({"price": quote_ui / base_ui, "size": base_ui})
        except HTTPException as e:
            if len(sample_errors) < 8:
                sample_errors.append({"side": "ask", "amount_ui": qsz, "denom": quote, "detail": e.detail})
            continue

    if base in ("SOL", "WSOL") or base_mint == _SOL_MINTS.get("SOL"):
        bid_sizes_base = [0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0][:n]
    elif int(base_dec) <= 6:
        bid_sizes_base = [1000, 2000, 5000, 10000, 20000, 50000, 100000, 200000, 500000, 1000000][:n]
    else:
        bid_sizes_base = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000][:n]

    bids: List[Dict[str, Any]] = []
    for bsz in bid_sizes_base:
        amt_in = _atomic_from_ui(bsz, base_dec)
        try:
            qt = await _ray_swap_quote(base_mint, quote_mint, amt_in, 100)
            qd = qt.get("data") or {}
            in_amt = int(qd.get("inputAmount") or amt_in)
            out_amt = int(qd.get("outputAmount") or 0)
            if in_amt <= 0 or out_amt <= 0:
                continue
            base_ui = _ui_from_atomic(in_amt, base_dec)
            quote_ui = _ui_from_atomic(out_amt, quote_dec)
            if base_ui <= 0:
                continue
            bids.append({"price": quote_ui / base_ui, "size": base_ui})
        except HTTPException as e:
            if len(sample_errors) < 8:
                sample_errors.append({"side": "bid", "amount_ui": bsz, "denom": base, "detail": e.detail})
            continue

    if not bids and not asks:
        raise HTTPException(
            status_code=502,
            detail={
                "error": "no_quote_levels",
                "message": "No routable Raydium quotes for sampled sizes",
                "rawSymbol": symbol,
                "resolvedSymbol": f"{base}-{quote}",
                "sampleErrors": sample_errors,
            },
        )

    asks.sort(key=lambda x: x.get("price") or 0)
    bids.sort(key=lambda x: -(x.get("price") or 0))

    price_dec_suggested = _suggest_price_decimals(asks + bids, int(quote_dec))
    display_price_decimals = max(1, min(int(price_dec_suggested or 0), 8))
    display_tick = 10 ** (-display_price_decimals)

    def _round_up_to_tick(px: float, tick: float) -> float:
        import math
        return math.ceil(px / tick) * tick

    def _round_down_to_tick(px: float, tick: float) -> float:
        import math
        return math.floor(px / tick) * tick

    if bids:
        prev_bid = None
        norm_bids = []
        for lvl in bids:
            try:
                px = float(lvl.get("price") or 0.0)
                sz = float(lvl.get("size") or 0.0)
            except Exception:
                continue
            if px <= 0 or sz <= 0:
                continue
            px = _round_down_to_tick(px, display_tick)
            if prev_bid is not None and px >= prev_bid:
                px = max(display_tick, prev_bid - display_tick)
            norm_bids.append({"price": px, "size": sz})
            prev_bid = px
        if norm_bids:
            bids = norm_bids

    if asks:
        prev_ask = None
        norm_asks = []
        for lvl in asks:
            try:
                px = float(lvl.get("price") or 0.0)
                sz = float(lvl.get("size") or 0.0)
            except Exception:
                continue
            if px <= 0 or sz <= 0:
                continue
            px = _round_up_to_tick(px, display_tick)
            if prev_ask is not None and px <= prev_ask:
                px = prev_ask + display_tick
            norm_asks.append({"price": px, "size": sz})
            prev_ask = px
        if norm_asks:
            asks = norm_asks

    crossedBookCorrected = False
    crossedBy = None
    if asks and bids:
        try:
            best_ask = float(asks[0].get("price") or 0.0)
            best_bid = float(bids[0].get("price") or 0.0)
            if best_ask > 0 and best_bid > 0 and best_ask <= best_bid:
                crossedBookCorrected = True
                crossedBy = best_bid - best_ask
                floor_ask = best_bid + display_tick
                prev_px = floor_ask
                corrected_asks = []
                for lvl in asks:
                    try:
                        px = float(lvl.get("price") or 0.0)
                        sz = float(lvl.get("size") or 0.0)
                    except Exception:
                        continue
                    if px <= 0 or sz <= 0:
                        continue
                    adj = px if px >= prev_px else prev_px
                    adj = _round_up_to_tick(adj, display_tick)
                    if corrected_asks and adj <= corrected_asks[-1]["price"]:
                        adj = corrected_asks[-1]["price"] + display_tick
                    corrected_asks.append({"price": adj, "size": sz})
                    prev_px = adj + display_tick
                if corrected_asks:
                    asks = corrected_asks
        except Exception:
            pass

    size_dec_suggested = int(base_dec)
    if size_dec_suggested > 6:
        size_dec_suggested = 6

    return {
        "ok": True,
        "venue": "solana_raydium",
        "rawSymbol": symbol,
        "resolvedSymbol": f"{base}-{quote}",
        "base": base,
        "quote": quote,
        "baseMint": base_mint,
        "quoteMint": quote_mint,
        "baseDecimals": int(base_dec),
        "quoteDecimals": int(quote_dec),
        "priceDecimals": price_dec_suggested,
        "displayPriceDecimals": display_price_decimals,
        "sizeDecimals": size_dec_suggested,
        "bids": bids,
        "asks": asks,
        "crossedBookCorrected": crossedBookCorrected,
        "crossedBy": crossedBy,
    }

@router.get("/jupiter/prices")
async def jupiter_prices(
    ids: str = Query(..., description="Comma-separated mint addresses (max 50)"),
) -> Dict[str, Any]:
    """Fetch USD prices for Solana mints via Jupiter Price API.

    Normalized for frontend:
      { ok: true, items: { <mint>: { price: <float> } } }

    Notes:
      - Uses a small in-process TTL cache + single-flight lock to reduce upstream bursts.
      - Retries briefly on 429/503 with exponential backoff + jitter.
    """
    mint_ids = [s.strip() for s in (ids or "").split(",") if s.strip()]
    # de-dupe but preserve order
    seen: set[str] = set()
    uniq: List[str] = []
    for mid in mint_ids:
        if mid in seen:
            continue
        seen.add(mid)
        uniq.append(mid)
    mint_ids = uniq[:50]

    if not mint_ids:
        return {"ok": True, "items": {}}

    cache_key = ",".join(sorted(mint_ids))
    now = time.time()

    # Single-flight + TTL cache
    async with _JUP_PRICES_LOCK:
        try:
            ts = float(_JUP_PRICES_CACHE.get("ts") or 0.0)
            key = str(_JUP_PRICES_CACHE.get("key") or "")
            if key == cache_key and (now - ts) <= float(_JUP_PRICES_TTL_S):
                items = _JUP_PRICES_CACHE.get("items") or {}
                if items:
                    return {"ok": True, "items": items, "cached": True}
            # cached empty -> treat as miss
        except Exception:
            # ignore cache corruption and proceed to fetch
            pass

        headers: Dict[str, str] = {}
        if _JUP_API_KEY:
            headers["x-api-key"] = _JUP_API_KEY

        last_status: Optional[int] = None
        last_text: str = ""

        for attempt in range(4):
            try:
                async with httpx.AsyncClient(timeout=_JUP_TIMEOUT_S) as client:
                    r = await client.get(
                        _JUP_PRICE_URL,
                        params={"ids": ",".join(mint_ids)},
                        headers=headers,
                    )
                last_status = r.status_code
                if r.status_code in (429, 503):
                    last_text = (r.text or "")[:200]
                    # backoff with jitter
                    await asyncio.sleep((0.35 * (2 ** attempt)) + random.random() * 0.15)
                    continue
                r.raise_for_status()
                data = r.json() or {}

                raw = data.get("data") or data.get("prices") or data.get("items") or {}
                if not raw and isinstance(data, dict):
                    # Jupiter v3 can return a flat dict keyed by mint:
                    # { "<mint>": {"usdPrice": 1.23, ...}, ... }
                    flat = {k: v for k, v in data.items() if isinstance(v, dict)}
                    if any(
                        isinstance(v, dict)
                        and (
                            v.get("price") is not None
                            or v.get("usdPrice") is not None
                            or v.get("priceUsd") is not None
                            or v.get("usd_price") is not None
                            or v.get("price_usd") is not None
                        )
                        for v in flat.values()
                    ):
                        raw = flat

                items: Dict[str, Dict[str, float]] = {}

                if isinstance(raw, dict):
                    for mint, info in raw.items():
                        try:
                            if isinstance(info, (int, float, str)):
                                p = float(info)
                                items[str(mint)] = {"price": p}
                                continue
                            if not isinstance(info, dict):
                                continue
                            p = (
                                info.get("price")
                                or info.get("priceUsd")
                                or info.get("usdPrice")
                                or info.get("usd")
                            )
                            if p is None:
                                continue
                            items[str(mint)] = {"price": float(p)}
                        except Exception:
                            continue
                if items:
                    _JUP_PRICES_CACHE["ts"] = now
                    _JUP_PRICES_CACHE["key"] = cache_key
                    _JUP_PRICES_CACHE["items"] = items

                return {"ok": True, "items": items, "cached": False}

            except httpx.HTTPStatusError as e:
                # non-retriable HTTP error
                raise HTTPException(
                    status_code=e.response.status_code,
                    detail=f"Jupiter price error: {e.response.text[:200]}",
                )
            except Exception as e:
                last_text = str(e)[:200]
                # retry on transient network errors
                if attempt < 3:
                    await asyncio.sleep((0.25 * (2 ** attempt)) + random.random() * 0.15)
                    continue
                raise HTTPException(status_code=500, detail=f"Jupiter price error: {last_text}")

        # should not reach
        raise HTTPException(status_code=502, detail=f"Jupiter price error: status={last_status} {last_text}")


@router.post("/jupiter/trigger/mark_canceled")
async def jupiter_trigger_mark_canceled(req: TriggerMarkCanceledRequest, db: Session = Depends(get_db)):
    order_id = str(req.order or "").strip()
    row = (
        db.query(VenueOrderRow)
        .filter(VenueOrderRow.venue == "solana_jupiter")
        .filter(VenueOrderRow.venue_order_id == order_id)
        .one_or_none()
    )
    if row is None:
        return {"ok": True, "updated": False, "reason": "not_found", "order": order_id}
    now = _utcnow()
    row.status = "canceled"
    row.updated_at = now
    db.add(row)
    db.commit()
    return {
        "ok": True,
        "updated": True,
        "order": order_id,
        "venue": row.venue,
        "venue_order_id": row.venue_order_id,
        "status": row.status,
        "updated_at": row.updated_at,
        "signature": req.signature,
    }
