# backend/app/routers/solana_dex.py

from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/solana_dex", tags=["solana_dex"])

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

# Optional Jupiter API key (Swap API v1). Sent as header `x-api-key` when present.
_JUP_API_KEY = (os.getenv("UTT_JUP_API_KEY") or os.getenv("JUPITER_API_KEY") or "").strip()

# Timeout (seconds) for Jupiter HTTP requests.
try:
    _JUP_TIMEOUT_S = float(os.getenv("UTT_JUP_TIMEOUT_S") or os.getenv("JUPITER_TIMEOUT_S") or "10")
except Exception:
    _JUP_TIMEOUT_S = 10.0

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


async def _resolve_mint_and_decimals(asset: str) -> tuple[str, int]:
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

    routes = data.get("data") or []
    if not routes:
        raise HTTPException(status_code=502, detail={"error": "no_routes", "jupiter": data})

    return routes[0]

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


@router.post("/jupiter/swap_tx")
async def jupiter_swap_tx(req: SwapTxRequest):
    base, quote = _parse_symbol(_normalize_symbol(req.symbol))

    side = (req.side or "").strip().lower()
    if side not in ("buy", "sell"):
        raise HTTPException(status_code=422, detail=f"Invalid side '{req.side}' (expected buy|sell)")

    base_mint, base_dec = await _resolve_mint_and_decimals(base)
    quote_mint, quote_dec = await _resolve_mint_and_decimals(quote)

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

    # SPL tokens (jsonParsed makes this easy)
    ta = await _rpc(
        "getTokenAccountsByOwner",
        [
            address,
            {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
            {"encoding": "jsonParsed", "commitment": "confirmed"},
        ],
    )

    tokens: List[Dict[str, Any]] = []
    for it in ((ta.get("result") or {}).get("value") or []):
        try:
            acc = (it.get("account") or {}).get("data", {})
            parsed = (acc.get("parsed") or {}).get("info") or {}
            mint = parsed.get("mint")
            token_amount = (parsed.get("tokenAmount") or {})
            ui_amt = token_amount.get("uiAmount")
            decimals = token_amount.get("decimals")
            amount = token_amount.get("amount")
            if mint:
                tokens.append(
                    {
                        "mint": mint,
                        "uiAmount": ui_amt,
                        "amount": amount,
                        "decimals": decimals,
                    }
                )
        except Exception:
            continue

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
) -> Dict[str, Any]:
    """Resolve a symbol or mint override to (mint, decimals).

    DEX-only helper for frontend (balances mapping, debug, etc.).
    """
    mint, dec = await _resolve_mint_and_decimals(asset)
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
):
    """Pseudo-orderbook for Solana-Jupiter using Jupiter quotes.

    Not a real CLOB ladder. We synthesize levels by sampling quotes at a few sizes.

    Semantics (BASE-QUOTE) match a traditional orderbook:
      - asks: BUY BASE using QUOTE  => quote QUOTE -> BASE (these are the prices a buyer pays)
      - bids: SELL BASE for QUOTE   => quote BASE  -> QUOTE (these are the prices a seller receives)

    We return sizes in BASE units.
    """

    base, quote = _parse_symbol(_normalize_symbol(symbol))
    base_mint, base_dec = await _resolve_mint_and_decimals(base)
    quote_mint, quote_dec = await _resolve_mint_and_decimals(quote)

    # Keep quote load low: cap to <=10 sampled levels.
    n = max(1, min(int(depth), 10))

    # Helpers
    def _atomic_from_ui(ui_amt: float, dec: int) -> int:
        return int(round(float(ui_amt) * (10 ** int(dec))))

    def _ui_from_atomic(amt: int, dec: int) -> float:
        return float(amt) / (10 ** int(dec))

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
    price_dec_suggested = int(quote_dec)
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
        "sizeDecimals": size_dec_suggested,
        "bids": bids,
        "asks": asks,
    }
