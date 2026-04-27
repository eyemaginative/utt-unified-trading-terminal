from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import httpx
from pydantic import BaseModel
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..db import get_db
from ..models import WalletAddress, WalletAddressSnapshot, WalletAddressTx, AssetDeposit, AssetWithdrawal
from ..schemas_wallet_addresses import (
    WalletAddressCreate,
    WalletAddressOut,
    WalletAddressRefreshRequest,
    WalletAddressBalanceOut,
)

# Reuse existing USD pricing helper so on-chain balances display like venue balances
from ..services.market import prices_usd_from_assets
from ..config import settings

# Pricing venue used for USD valuation (market data venue, not where funds are held)
_PRICING_VENUE = (os.getenv("UTT_PRICING_VENUE", "coinbase") or "coinbase").strip().lower()

router = APIRouter(prefix="/api/wallet_addresses", tags=["wallet_addresses"])


# ------------------------------------------------------------------------------
# Explorer adapters (free, no API keys)
# ------------------------------------------------------------------------------

# Blockchair supports BTC + DOGE + DOT in a single consistent API surface.
_BLOCKCHAIR_CHAIN = {
    "BTC": "bitcoin",
    "DOGE": "dogecoin",
    "DOT": "polkadot",
}

_DECIMALS = {
    "BTC": 8,
    "DOGE": 8,
    "DOT": 10,  # Planck -> DOT
    "SOL": 9,   # lamports -> SOL
}

# BlockCypher chains for balances/txs (token-supported)
_BLOCKCYPHER_CHAIN = {
    "BTC": "btc",
    "DOGE": "doge",
}

# Env var names (either works). Vault fallback uses venue='blockcypher' (api_key).
def _blockcypher_token() -> str:
    tok = (os.getenv("BLOCKCYPHER_TOKEN") or os.getenv("UTT_blockcypher_token()") or "").strip()
    if tok:
        return tok
    # Vault-first (provider-style): do not gate features if missing; return "" to allow fallbacks.
    try:
        bundle = settings._vault_latest_bundle("blockcypher")
        if isinstance(bundle, dict):
            tok2 = (bundle.get("api_key") or "").strip()
            if tok2:
                return tok2
    except Exception:
        pass
    return ""



def _norm_asset(x: str) -> str:
    return str(x or "").strip().upper()


def _norm_network(x: str, asset: str) -> str:
    s = str(x or "").strip().lower()
    return s if s else asset.lower()


# ------------------------------------------------------------------------------
# Solana JSON-RPC helpers (public RPC by default)
# ------------------------------------------------------------------------------

def _solana_rpc_url() -> str:
    # Prefer env var (local dev + deployments).
    url = (os.getenv("SOLANA_RPC_URL") or "").strip()
    if url:
        return url

    # Fallback: read from DB-backed API Key Vault (write-only) if present.
    # Expectation: store the FULL RPC URL as api_key under venue "solana_rpc".
    try:
        bundle = settings._vault_latest_bundle("solana_rpc")
        if isinstance(bundle, dict):
            v = (bundle.get("api_key") or "").strip()
            if v:
                return v
    except Exception:
        pass

    # Last resort: public mainnet-beta RPC (free, rate-limited).
    return "https://api.mainnet-beta.solana.com"


async def _solana_rpc(method: str, params: Optional[list] = None) -> Dict:
    url = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com").strip()
    timeout_s = float(os.getenv("SOLANA_RPC_TIMEOUT_S", "20") or 20)
    # Public RPCs rate-limit aggressively; retry with exponential backoff on 429.
    max_retries = int(os.getenv("SOLANA_RPC_RETRIES", "5") or 5)

    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}

    last_err: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=timeout_s) as client:
                r = await client.post(url, json=payload)
            # Handle rate-limit with backoff (+ optional Retry-After)
            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                if ra:
                    try:
                        sleep_s = float(ra)
                    except Exception:
                        sleep_s = 0.0
                else:
                    # 0.5, 1, 2, 4, 8 ... capped
                    sleep_s = min(8.0, 0.5 * (2 ** attempt))
                await asyncio.sleep(max(0.25, sleep_s))
                continue

            js = r.json()
            if not r.is_success:
                raise RuntimeError(f"Solana RPC HTTP {r.status_code}: {js}")
            if isinstance(js, dict) and js.get("error"):
                raise RuntimeError(f"Solana RPC error: {js.get('error')}")
            return js
        except Exception as e:
            last_err = e
            # Backoff a bit on transient failures too.
            await asyncio.sleep(min(2.0, 0.25 * (attempt + 1)))

    # Exhausted retries
    raise RuntimeError(f"Solana RPC failed after {max_retries} attempts: {last_err}")


async def _fetch_solana_balance_lamports(address: str) -> Tuple[int, Dict]:
    js = await _solana_rpc("getBalance", [address, {"commitment": "confirmed"}])
    lamports = int(((js or {}).get("result") or {}).get("value") or 0)
    return lamports, js


async def _fetch_solana_signatures(address: str, limit: int) -> List[Dict]:
    js = await _solana_rpc("getSignaturesForAddress", [address, {"limit": int(limit or 50)}])
    out = (js or {}).get("result") or []
    return list(out)


async def _fetch_solana_transaction(signature: str) -> Dict:
    # jsonParsed is easiest to work with (still includes meta.preBalances/postBalances)
    js = await _solana_rpc(
        "getTransaction",
        [
            signature,
            {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 0,
            },
        ],
    )
    return js



def _fetch_solana_tx_dashboard(signature: str, *, cached_raw: Optional[Dict] = None) -> Dict:
    """Compatibility helper.

    Earlier iterations referenced `_fetch_solana_tx_dashboard`. This project now uses
    `_fetch_solana_transaction` (async). The ingest endpoints are sync (threadpool),
    so we provide a small shim.

    If a cached `raw` dict already contains a `result` payload (or a nested tx object),
    we reuse it to avoid unnecessary RPC calls.
    """
    try:
        if isinstance(cached_raw, dict) and cached_raw:
            # Some caches store the full RPC response directly in raw
            if isinstance(cached_raw.get("result"), dict):
                return cached_raw
            # Or nest it under a known key
            for k in ("solana_tx", "tx", "transaction", "rpc"):
                v = cached_raw.get(k)
                if isinstance(v, dict):
                    if isinstance(v.get("result"), dict):
                        return v
                    # Sometimes it's the `result` object itself
                    if any(x in v for x in ("meta", "transaction", "slot", "blockTime")):
                        return {"result": v}
    except Exception:
        pass

    import asyncio
    try:
        return asyncio.run(_fetch_solana_transaction(signature))
    except RuntimeError:
        # Fallback in the unlikely case we're invoked from a running loop.
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(_fetch_solana_transaction(signature))
        finally:
            loop.close()



def _parse_solana_tx_netflow(js: Dict, address: str) -> Tuple[str, float, Optional[float], Optional[datetime]]:
    """
    Compute net SOL delta for `address` using meta.preBalances/postBalances.

    Returns (direction, amount_sol, fee_sol, tx_time).
    """
    result = (js or {}).get("result")
    if not result:
        raise ValueError("tx result missing (not found or pruned)")

    meta = (result.get("meta") or {})
    tx = (result.get("transaction") or {})
    msg = (tx.get("message") or {})

    # accountKeys may be list[str] or list[dict] depending on node version
    keys_raw = msg.get("accountKeys") or []
    keys: List[str] = []
    for k in keys_raw:
        if isinstance(k, str):
            keys.append(k)
        elif isinstance(k, dict):
            # jsonParsed often uses {"pubkey": "...", "signer":..., "writable":...}
            pk = k.get("pubkey")
            if pk:
                keys.append(str(pk))
    if not keys:
        raise ValueError("accountKeys missing")

    try:
        idx = keys.index(address)
    except ValueError:
        # If address isn't a top-level key, we can't compute netflow reliably here.
        raise ValueError("address not found in tx accountKeys")

    pre = meta.get("preBalances") or []
    post = meta.get("postBalances") or []
    if idx >= len(pre) or idx >= len(post):
        raise ValueError("pre/post balance arrays missing index")

    pre_lamports = int(pre[idx] or 0)
    post_lamports = int(post[idx] or 0)
    delta = post_lamports - pre_lamports

    fee_lamports = meta.get("fee")
    fee_sol = (int(fee_lamports) / 1_000_000_000) if fee_lamports is not None else None

    block_time = result.get("blockTime")
    tx_time = None
    if block_time is not None:
        try:
            tx_time = datetime.utcfromtimestamp(int(block_time))
        except Exception:
            tx_time = None

    if delta >= 0:
        return "in", (delta / 1_000_000_000), fee_sol, tx_time
    else:
        return "out", (abs(delta) / 1_000_000_000), fee_sol, tx_time


def _solana_fee_only(amount_sol: float, fee_sol: Optional[float], eps: float = 1e-12) -> bool:
    return (fee_sol is not None) and (abs(float(amount_sol) - float(fee_sol)) <= float(eps))


def _solana_native_sol_delta_from_tx(js: Dict, address: str) -> Optional[float]:
    """
    Best-effort signed native SOL delta for `address`, adjusted to exclude the network fee.

    Returns signed SOL delta where:
      - negative => net SOL spent (excluding fee)
      - positive => net SOL received (excluding fee)

    If the tx only moved the fee, returns None.
    """
    try:
        result = (js or {}).get("result") or {}
        meta = result.get("meta") or {}
        tx = result.get("transaction") or {}
        msg = tx.get("message") or {}

        keys_raw = msg.get("accountKeys") or []
        keys: List[str] = []
        for k in keys_raw:
            if isinstance(k, str):
                keys.append(k)
            elif isinstance(k, dict):
                pk = k.get("pubkey")
                if pk:
                    keys.append(str(pk))
        if not keys:
            return None

        idx = keys.index(address)
        pre = meta.get("preBalances") or []
        post = meta.get("postBalances") or []
        if idx >= len(pre) or idx >= len(post):
            return None

        signed_delta = (int(post[idx] or 0) - int(pre[idx] or 0)) / 1_000_000_000.0
        fee_lamports = meta.get("fee")
        fee_sol = (int(fee_lamports) / 1_000_000_000.0) if fee_lamports is not None else 0.0

        # Remove the network fee contribution from the wallet netflow.
        signed_delta_ex_fee = signed_delta + fee_sol

        if abs(signed_delta_ex_fee) <= 1e-12:
            return None
        return float(signed_delta_ex_fee)
    except Exception:
        return None


def _solana_tx_is_swap_like(js: Dict, address: str) -> bool:
    """
    Robust heuristic for Solana "complex" txs (swaps/unwrap/ATA/rent/etc).

    IMPORTANT: jsonParsed getTransaction commonly uses programIdIndex (not programId),
    especially in innerInstructions, so we must resolve program IDs via accountKeys.
    """
    result = (js or {}).get("result")
    if not result:
        return False

    meta = result.get("meta") or {}

    # 1) Fast-path: any token balance arrays => complex (swap/ATA/wSOL/etc)
    pre_tb = meta.get("preTokenBalances") or []
    post_tb = meta.get("postTokenBalances") or []
    if pre_tb or post_tb:
        return True

    # 2) Build accountKeys list so we can resolve programIdIndex -> program id string
    tx = (result.get("transaction") or {})
    msg = (tx.get("message") or {})

    keys_raw = msg.get("accountKeys") or []
    keys: List[str] = []
    for k in keys_raw:
        if isinstance(k, str):
            keys.append(k)
        elif isinstance(k, dict):
            pk = k.get("pubkey")
            if pk:
                keys.append(str(pk))

    TOKEN_PROGRAM_IDS = {
        # SPL Token Program
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
        # Associated Token Account Program
        "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
    }

    def _pid_from_index(ix: Optional[int]) -> Optional[str]:
        if ix is None:
            return None
        try:
            i = int(ix)
            if 0 <= i < len(keys):
                return keys[i]
        except Exception:
            pass
        return None

    def _iter_program_ids() -> List[str]:
        ids: List[str] = []

        # Outer instructions
        for ins in (msg.get("instructions") or []):
            if not isinstance(ins, dict):
                continue

            # Some nodes include programId directly
            pid = ins.get("programId")
            if isinstance(pid, str):
                ids.append(pid)

            # Most include programIdIndex instead
            pid_ix = ins.get("programIdIndex")
            pid2 = _pid_from_index(pid_ix)
            if pid2:
                ids.append(pid2)

            # jsonParsed may include parsed.program as string ("spl-token", etc.)
            parsed = ins.get("parsed")
            if isinstance(parsed, dict):
                program = parsed.get("program")
                if isinstance(program, str):
                    ids.append(program)

        # Inner instructions (almost always programIdIndex)
        for ii in (meta.get("innerInstructions") or []):
            if not isinstance(ii, dict):
                continue
            for ins in (ii.get("instructions") or []):
                if not isinstance(ins, dict):
                    continue
                pid = ins.get("programId")
                if isinstance(pid, str):
                    ids.append(pid)

                pid_ix = ins.get("programIdIndex")
                pid2 = _pid_from_index(pid_ix)
                if pid2:
                    ids.append(pid2)

                parsed = ins.get("parsed")
                if isinstance(parsed, dict):
                    program = parsed.get("program")
                    if isinstance(program, str):
                        ids.append(program)

        return ids

    # 3) Log-message fallback: swaps usually show token/ATA programs invoked in logs
    # logMessages look like: "Program <PROGRAM_ID> invoke [1]"
    for lm in (meta.get("logMessages") or []):
        if not isinstance(lm, str):
            continue
        for pid in TOKEN_PROGRAM_IDS:
            if pid in lm:
                return True
        if "spl-token" in lm.lower() or "associated token account" in lm.lower():
            return True

    # 4) Program id scan
    for pid in _iter_program_ids():
        if pid in TOKEN_PROGRAM_IDS:
            return True
        if isinstance(pid, str) and pid.lower() in {"spl-token", "spl-associated-token-account"}:
            return True

    return False


async def _fetch_blockchair_balance_atomic(chain: str, address: str) -> Tuple[int, Dict]:
    """Return (atomic_balance, raw_json)."""
    url = f"https://api.blockchair.com/{chain}/dashboards/address/{address}"
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(url)
        r.raise_for_status()
        js = r.json()

    data = (js or {}).get("data") or {}
    node = data.get(address) or {}

    # Blockchair's dashboards endpoint typically exposes balance under:
    # data[address].address.balance (atomic units)
    addr_node = node.get("address") or {}
    bal = addr_node.get("balance")

    if bal is None:
        # Some chains may nest differently; attempt fallbacks.
        bal = node.get("balance")

    if bal is None:
        raise ValueError("balance field not found in explorer response")

    return int(bal), js


async def _fetch_blockcypher_balance_atomic(chain: str, address: str) -> Tuple[int, Dict]:
    """
    Return (atomic_balance, raw_json) from BlockCypher.

    Uses /addrs/{address}/balance which returns:
      - balance / final_balance (in atomic units; satoshis for BTC, 1e-8 DOGE)
    """
    base = "https://api.blockcypher.com"
    url = f"{base}/v1/{chain}/main/addrs/{address}/balance"

    params: Dict = {}
    if _blockcypher_token():
        params["token"] = _blockcypher_token()

    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(url, params=params)
        # keep body for better error detail if needed
        raw_bytes = await r.aread()
        body = raw_bytes.decode("utf-8", errors="replace") if isinstance(raw_bytes, (bytes, bytearray)) else str(raw_bytes)

        if not r.is_success:
            raise RuntimeError(f"HTTP {r.status_code} from BlockCypher. url={url} body={body[:600]}")

        try:
            js = r.json()
        except Exception:
            raise RuntimeError(f"Non-JSON from BlockCypher. url={url} body={body[:600]}")

    bal = (js or {}).get("final_balance")
    if bal is None:
        bal = (js or {}).get("balance")

    if bal is None:
        raise ValueError("final_balance/balance field not found in BlockCypher response")

    return int(bal), js


async def _get_balance_atomic(asset: str, network: str, address: str) -> Tuple[int, int, Dict, str]:
    """
    Return (atomic_balance, decimals, raw_json, source).

    Source is "blockcypher" for BTC/DOGE when used, otherwise "blockchair".
    For SOL this is "solana_rpc".
    """
    a = _norm_asset(asset)
    decimals = _DECIMALS.get(a, 8)

    # Solana native SOL
    if a == "SOL":
        lamports, raw = await _fetch_solana_balance_lamports(address)
        return lamports, decimals, raw, "solana_rpc"

    # Prefer BlockCypher for BTC/DOGE (current strategy)
    if a in ("BTC", "DOGE"):
        chain_bc = _BLOCKCYPHER_CHAIN.get(a)
        if chain_bc:
            try:
                atomic, raw = await _fetch_blockcypher_balance_atomic(chain_bc, address)
                return atomic, decimals, raw, "blockcypher"
            except Exception:
                # Fall back to Blockchair below
                pass

    # Fallback (and DOT support)
    chain = _BLOCKCHAIR_CHAIN.get(a)
    if not chain:
        raise ValueError(f"unsupported asset for on-chain balances: {a}")

    atomic, raw = await _fetch_blockchair_balance_atomic(chain, address)
    return atomic, decimals, raw, "blockchair"


# ------------------------------------------------------------------------------
# CRUD: wallet addresses
# ------------------------------------------------------------------------------


@router.get("", response_model=List[WalletAddressOut])
def list_wallet_addresses(
    db: Session = Depends(get_db),
    asset: Optional[str] = Query(default=None),
    network: Optional[str] = Query(default=None),
    wallet_id: Optional[str] = Query(default=None),
    limit: int = Query(default=500, ge=1, le=2000),
):
    stmt = select(WalletAddress).order_by(WalletAddress.created_at.desc()).limit(limit)

    if asset:
        stmt = stmt.where(WalletAddress.asset == _norm_asset(asset))
    if network:
        stmt = stmt.where(WalletAddress.network == str(network).strip().lower())
    if wallet_id:
        stmt = stmt.where(WalletAddress.wallet_id == str(wallet_id).strip())

    rows = db.execute(stmt).scalars().all()
    return [
        WalletAddressOut(
            id=r.id,
            asset=r.asset,
            network=r.network,
            address=r.address,
            label=r.label,
            wallet_id=r.wallet_id,
            owner_scope=r.owner_scope,
            created_at=r.created_at,
        )
        for r in rows
    ]


@router.post("", response_model=WalletAddressOut)
def create_wallet_address(payload: WalletAddressCreate, db: Session = Depends(get_db)):
    asset = _norm_asset(payload.asset)
    network = _norm_network(payload.network, asset)
    address = str(payload.address or "").strip()
    if not address:
        raise HTTPException(status_code=400, detail="address is required")

    wallet_id = None
    if payload.wallet_id is not None:
        s = str(payload.wallet_id).strip()
        wallet_id = s if s else None

    row = WalletAddress(
        wallet_id=wallet_id,
        asset=asset,
        network=network,
        address=address,
        label=str(payload.label or "").strip() or None,
        owner_scope=str(payload.owner_scope or "user").strip().lower(),
        created_at=datetime.utcnow(),
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    return WalletAddressOut(
        id=row.id,
        asset=row.asset,
        network=row.network,
        address=row.address,
        label=row.label,
        wallet_id=row.wallet_id,
        owner_scope=row.owner_scope,
        created_at=row.created_at,
    )


@router.delete("/{address_id}")
def delete_wallet_address(address_id: str, db: Session = Depends(get_db)):
    row = db.get(WalletAddress, address_id)
    if not row:
        raise HTTPException(status_code=404, detail="wallet address not found")
    db.delete(row)
    db.commit()
    return {"ok": True}


# ------------------------------------------------------------------------------
# Cached balances (latest) + refresh
# ------------------------------------------------------------------------------


@router.get("/balances/latest", response_model=List[WalletAddressBalanceOut])
def wallet_balances_latest(
    db: Session = Depends(get_db),
    with_prices: int = Query(default=1, ge=0, le=1),
    limit: int = Query(default=2000, ge=1, le=5000),
):
    # latest snapshot per address (simple approach: order and pick first per address_id)
    stmt = (
        select(WalletAddressSnapshot, WalletAddress)
        .join(WalletAddress, WalletAddress.id == WalletAddressSnapshot.wallet_address_id)
        .order_by(WalletAddressSnapshot.fetched_at.desc())
        .limit(limit)
    )
    rows = db.execute(stmt).all()

    latest: Dict[str, Tuple[WalletAddressSnapshot, WalletAddress]] = {}
    for snap, addr in rows:
        # addr.id may be UUID-like; normalize key as str for stability
        addr_key = str(addr.id)
        if addr_key not in latest:
            latest[addr_key] = (snap, addr)

    # Build pricing map (optional)
    prices_usd = {}
    if with_prices:
        assets = sorted({addr.asset for _, addr in latest.values()})
        try:
            prices_usd = prices_usd_from_assets(_PRICING_VENUE, assets) or {}
        except Exception:
            prices_usd = {}

    out: List[WalletAddressBalanceOut] = []
    for snap, addr in latest.values():
        qty = float(snap.balance_qty)
        usd_price = float(prices_usd.get(addr.asset) or 0.0)
        usd_value = qty * usd_price

        # IMPORTANT: make UUID-ish ids strings defensively + include fetched_at
        fetched_at = snap.fetched_at

        out.append(
            WalletAddressBalanceOut(
                # Use snapshot id as the row id (unique per refresh)
                id=str(snap.id),
                wallet_address_id=str(addr.id),

                wallet_id=addr.wallet_id,
                owner_scope=addr.owner_scope,
                asset=addr.asset,
                network=addr.network,
                address=addr.address,
                label=addr.label,

                balance=qty,
                usd_price=usd_price if with_prices else None,
                usd_value=usd_value if with_prices else None,
                fetched_at=fetched_at,

                # Back-compat (optional in schema)
                created_at=fetched_at,
                captured_at=fetched_at,
            )
        )

    # Stable sort: biggest USD value first if priced, otherwise by fetched_at
    if with_prices:
        out.sort(key=lambda r: (r.usd_value or 0.0), reverse=True)
    else:
        out.sort(key=lambda r: r.fetched_at or datetime.min, reverse=True)

    return out


@router.post("/balances/refresh")
async def wallet_balances_refresh(payload: WalletAddressRefreshRequest, db: Session = Depends(get_db)):
    # Determine which addresses to refresh
    if payload.ids:
        stmt = select(WalletAddress).where(WalletAddress.id.in_(payload.ids))
    else:
        stmt = select(WalletAddress)

    addrs = db.execute(stmt).scalars().all()
    if not addrs:
        return {"ok": True, "refreshed": 0}

    refreshed = 0
    errors: List[Dict] = []

    for a in addrs:
        try:
            atomic, decimals, raw, source = await _get_balance_atomic(a.asset, a.network, a.address)
            units = atomic / (10 ** decimals)

            snap = WalletAddressSnapshot(
                wallet_address_id=a.id,
                asset=a.asset,
                network=a.network,
                address=a.address,
                balance_qty=float(units),
                balance_raw=raw,
                source=source,  # "blockcypher" for BTC/DOGE when used, else "blockchair" or "solana_rpc"
                fetched_at=datetime.utcnow(),
            )
            db.add(snap)
            db.commit()
            refreshed += 1
        except Exception as e:
            db.rollback()
            errors.append({"id": a.id, "asset": a.asset, "address": a.address, "error": str(e)})

    return {"ok": True, "refreshed": refreshed, "errors": errors}


# ------------------------------------------------------------------------------
# Tx ingest (on-chain) -> cache txs -> optionally write ledger deposits/withdrawals
# ------------------------------------------------------------------------------


class WalletAddressTxIngestRequest(BaseModel):
    ids: Optional[List[str]] = None
    limit_per_address: int = 50
    write_ledger: bool = True
    # Solana-specific: minimum SOL amount to treat as a real external transfer.
    # Smaller "micro" SOL movements are typically fee/rent/swap legs and should
    # be resolved (ingested_to_ledger_at stamped) without creating deposit/
    # withdrawal ledger rows.
    solana_min_transfer_sol: float = 0.001


_SKIP_WALLET_IDS = {"coinbase"}
_DEPOSITS_ONLY_WALLET_IDS = {"robinhood", "dex-trade"}


def _blockchair_key() -> Optional[str]:
    """Return optional Blockchair API key from environment."""
    for k in ("BLOCKCHAIR_API_KEY", "UTT_BLOCKCHAIR_API_KEY"):
        v = os.getenv(k)
        if v and v.strip():
            return v.strip()
    return None


async def _blockchair_get_json(url: str, params: Optional[Dict] = None) -> Dict:
    """HTTP GET wrapper with friendly errors + basic backoff for 429/430."""
    params = dict(params or {})
    key = _blockchair_key()
    if key and "key" not in params:
        params["key"] = key

    headers = {
        "Accept": "application/json",
        "User-Agent": "UTT/WalletAddressTxIngest (+https://example.invalid)",
    }

    # Blockchair free tier can return 429 or sometimes 430 (non-standard) when throttled.
    backoffs = [0.5, 1.5, 3.0]
    last_err = None

    async with httpx.AsyncClient(timeout=20.0, headers=headers) as client:
        for i, delay in enumerate([0.0] + backoffs):
            if delay:
                await asyncio.sleep(delay)
            try:
                r = await client.get(url, params=params)
                if r.status_code in (429, 430):
                    # If the user is blacklisted and we have no API key configured, be explicit.
                    # Blockchair returns a JSON body with context.error.
                    context_err = None
                    try:
                        j = r.json()
                        context_err = (j or {}).get("context", {}).get("error")
                    except Exception:
                        context_err = None

                    hint = ""
                    if not key:
                        hint = (
                            " Set env var BLOCKCHAIR_API_KEY (or UTT_BLOCKCHAIR_API_KEY) and restart backend."
                        )

                    msg = (
                        f"HTTP {r.status_code} from Blockchair (throttle/blocked). url={url}."
                        + (f" error={context_err!r}." if context_err else "")
                        + hint
                    )
                    last_err = RuntimeError(msg)

                    # If we have no key and we're blocked, do NOT retry (retries just extend the blacklist).
                    if r.status_code == 430 and not key:
                        break

                    continue
                # Treat other non-2xx as fatal with body preview
                if r.status_code < 200 or r.status_code >= 300:
                    body = (r.text or "").strip()
                    body_preview = body[:500]
                    raise RuntimeError(f"HTTP {r.status_code} from Blockchair: {body_preview}")
                return r.json()
            except Exception as e:
                last_err = e
                # Retry only on the known throttling statuses or transient client errors.
                if isinstance(e, httpx.RequestError) and i < len(backoffs):
                    continue
                break

    raise RuntimeError(str(last_err) if last_err else "Blockchair request failed")


async def _fetch_blockchair_address_dashboard(chain: str, address: str) -> Dict:
    url = f"https://api.blockchair.com/{chain}/dashboards/address/{address}"
    return await _blockchair_get_json(url, params={"transaction_details": "true"})


async def _fetch_blockchair_tx_dashboard(chain: str, txid: str) -> Dict:
    url = f"https://api.blockchair.com/{chain}/dashboards/transaction/{txid}"
    return await _blockchair_get_json(url)


# ------------------------------------------------------------------------------
# BlockCypher (token-supported) — DOGE/BTC address tx refs
# ------------------------------------------------------------------------------

# NOTE: _BLOCKCYPHER_CHAIN + _blockcypher_token() are defined above (used for balances too)


async def _blockcypher_get_json(path: str, params: Optional[Dict] = None) -> Dict:
    """GET JSON from BlockCypher; raises with detail on failure."""
    base = "https://api.blockcypher.com"
    url = f"{base}{path}"

    q = dict(params or {})
    if _blockcypher_token():
        q.setdefault("token", _blockcypher_token())

    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(url, params=q)
        text = await r.aread()
        body = text.decode("utf-8", errors="replace") if isinstance(text, (bytes, bytearray)) else str(text)
        if not r.is_success:
            raise RuntimeError(f"HTTP {r.status_code} from BlockCypher. url={url} body={body[:600]}")
        try:
            return r.json()
        except Exception:
            raise RuntimeError(f"Non-JSON from BlockCypher. url={url} body={body[:600]}")


def _parse_blockcypher_txref(txref: Dict, decimals: int) -> Tuple[str, float, Optional[datetime]]:
    """
    BlockCypher TXRef direction rules:
      - tx_input_n < 0  => this address received an output (in)
      - tx_output_n < 0 => this address spent an input (out)
    """
    tx_input_n = txref.get("tx_input_n")
    tx_output_n = txref.get("tx_output_n")
    val_atomic = int(txref.get("value") or 0)

    direction = None
    if isinstance(tx_input_n, int) and tx_input_n < 0:
        direction = "in"
    elif isinstance(tx_output_n, int) and tx_output_n < 0:
        direction = "out"
    else:
        # fallback: treat positive value as inbound (best-effort)
        direction = "in"

    tx_time = None
    t = txref.get("confirmed") or txref.get("received")
    if t:
        try:
            tx_time = datetime.fromisoformat(str(t).replace("Z", "+00:00"))
        except Exception:
            tx_time = None

    return direction, (val_atomic / (10 ** decimals)), tx_time


async def _fetch_blockcypher_txrefs(asset: str, address: str, limit: int) -> List[Dict]:
    """Return a list of normalized tx items: {txid, direction, amount, fee, tx_time, raw, provider}."""
    a = _norm_asset(asset)
    chain = _BLOCKCYPHER_CHAIN.get(a)
    if not chain:
        raise ValueError(f"unsupported asset for BlockCypher: {a}")

    # Address endpoint returns txrefs (limited). This is MVP-friendly and avoids per-tx fetches.
    js = await _blockcypher_get_json(f"/v1/{chain}/main/addrs/{address}")
    txrefs = (js or {}).get("txrefs") or []
    unconfirmed = (js or {}).get("unconfirmed_txrefs") or []

    # Most recent first (BlockCypher usually returns newest first; we still enforce limit)
    refs = list(txrefs) + list(unconfirmed)
    refs = refs[: int(limit or 50)]

    decimals = _DECIMALS.get(a, 8)

    out: List[Dict] = []
    for r in refs:
        txid = str(r.get("tx_hash") or "").strip()
        if not txid:
            continue
        direction, amount, tx_time = _parse_blockcypher_txref(r, decimals)
        out.append(
            {
                "txid": txid,
                "direction": direction,
                "amount": float(amount),
                "fee": None,
                "tx_time": tx_time,
                "raw": r,
                "provider": "blockcypher",
            }
        )
    return out


def _parse_blockchair_utxo_netflow(js: Dict, address: str, decimals: int) -> Tuple[str, float, float | None, Optional[datetime]]:
    """Return (direction, amount, fee, tx_time) for UTXO-style chains (BTC/DOGE)."""
    data = (js or {}).get("data") or {}
    node = None
    # blockchair uses txid as key
    for k, v in data.items():
        node = v
        break
    if not node:
        raise ValueError("tx data missing")

    tx = (node.get("transaction") or {})
    inputs = node.get("inputs") or []
    outputs = node.get("outputs") or []

    in_sum = 0
    out_sum = 0
    for inp in inputs:
        if str(inp.get("recipient") or "").strip() == address:
            in_sum += int(inp.get("value") or 0)
    for out in outputs:
        if str(out.get("recipient") or "").strip() == address:
            out_sum += int(out.get("value") or 0)

    # net for address: outputs-to-address minus inputs-from-address
    net_atomic = out_sum - in_sum
    fee_atomic = tx.get("fee")
    fee = (int(fee_atomic) / (10 ** decimals)) if fee_atomic is not None else None

    tx_time = None
    t = tx.get("time") or tx.get("time_utc") or tx.get("block_time")
    if t:
        try:
            tx_time = datetime.fromisoformat(str(t).replace("Z", "+00:00"))
        except Exception:
            tx_time = None

    if net_atomic >= 0:
        return "in", (net_atomic / (10 ** decimals)), fee, tx_time
    else:
        return "out", (abs(net_atomic) / (10 ** decimals)), fee, tx_time


@router.post("/tx/ingest")
async def wallet_addresses_tx_ingest(payload: WalletAddressTxIngestRequest, db: Session = Depends(get_db)):
    if payload.ids:
        stmt = select(WalletAddress).where(WalletAddress.id.in_(payload.ids))
    else:
        stmt = select(WalletAddress)
    addrs = db.execute(stmt).scalars().all()
    if not addrs:
        return {"ok": True, "addresses": 0, "cached": 0, "ledger_written": 0, "errors": []}

    cached = 0
    ledger_written = 0

    def _wa_tx_skip_reason(raw) -> Optional[str]:
        try:
            if isinstance(raw, dict):
                utt = (raw or {}).get("utt") or {}
                r = utt.get("skip_ledger_reason")
                return str(r) if r else None
        except Exception:
            return None
        return None

    def _find_existing_ledger_id(_direction: str, _asset: str, _network: str, _txid: str) -> Optional[str]:
        _txid_s = str(_txid or "").strip()
        if not _txid_s:
            return None
        if _direction == "in":
            return db.execute(
                select(AssetDeposit.id).where(
                    AssetDeposit.venue == venue,
                    AssetDeposit.wallet_id == wallet_id,
                    AssetDeposit.asset == _asset,
                    AssetDeposit.network == _network,
                    AssetDeposit.txid == _txid_s,
                )
            ).scalar_one_or_none()
        if _direction == "out":
            return db.execute(
                select(AssetWithdrawal.id).where(
                    AssetWithdrawal.venue == venue,
                    AssetWithdrawal.wallet_id == wallet_id,
                    AssetWithdrawal.asset == _asset,
                    AssetWithdrawal.network == _network,
                    AssetWithdrawal.txid == _txid_s,
                )
            ).scalar_one_or_none()
        return None

    errors: List[Dict] = []

    for a in addrs:
        asset = _norm_asset(a.asset)

        # enforce skip policy early
        wa_wallet_id = (a.wallet_id or "").strip().lower() or None
        if wa_wallet_id in _SKIP_WALLET_IDS:
            continue

        decimals = _DECIMALS.get(asset, 8)

        # Discover tx candidates (provider selection + fallback).
        tx_items: List[Dict] = []
        last_discovery_err: Optional[Exception] = None

        # SOL: use Solana RPC signatures
        if asset == "SOL":
            try:
                sigs = await _fetch_solana_signatures(a.address, int(payload.limit_per_address or 50))
                for s in sigs:
                    sig = str(s.get("signature") or "").strip()
                    if not sig:
                        continue
                    tx_items.append({"txid": sig, "provider": "solana_rpc"})
            except Exception as e:
                last_discovery_err = e

        # Non-SOL paths: Blockchair/BlockCypher
        if asset != "SOL":
            chain = _BLOCKCHAIR_CHAIN.get(asset)
            if not chain:
                errors.append({"id": a.id, "asset": a.asset, "address": a.address, "error": f"unsupported asset for tx ingest: {asset}"})
                continue

            # Prefer BlockCypher for DOGE/BTC to avoid Blockchair blacklist/WAF issues.
            if asset in ("DOGE", "BTC"):
                try:
                    tx_items = await _fetch_blockcypher_txrefs(asset, a.address, int(payload.limit_per_address or 50))
                except Exception as e:
                    last_discovery_err = e

            # Fallback to Blockchair (also supports DOT).
            if not tx_items:
                try:
                    dash = await _fetch_blockchair_address_dashboard(chain, a.address)
                    node = ((dash.get("data") or {}).get(a.address) or {})
                    txids = node.get("transactions") or []
                    txids = list(txids)[: int(payload.limit_per_address or 50)]

                    # NOTE: this keeps your existing shape (provider=blockchair with txid only)
                    for txid_s in txids:
                        txid_s = str(txid_s or "").strip()
                        if not txid_s:
                            continue
                        tx_items.append(
                            {
                                "txid": txid_s,
                                "provider": "blockchair",
                            }
                        )
                except Exception as e:
                    last_discovery_err = e

        if not tx_items:
            msg = f"discovery failed"
            if last_discovery_err:
                msg = f"discovery failed: {last_discovery_err}"
            errors.append({"id": a.id, "asset": a.asset, "address": a.address, "error": msg})
            continue

        for item in tx_items:
            txid_s = str(item.get("txid") or "").strip()
            if not txid_s:
                continue

            existing = db.execute(
                select(WalletAddressTx).where(
                    WalletAddressTx.wallet_address_id == a.id,
                    WalletAddressTx.txid == txid_s,
                )
            ).scalars().all()

            provider = str(item.get("provider") or "blockchair")
            txdash = None
            solana_swap_like = False
            fee_only = False
            try:
                if provider == "solana_rpc":
                    txdash = None
                    # If we've already cached this tx (raw contains the RPC result), reuse it to avoid extra RPC calls.
                    if existing:
                        for _r in existing:
                            if isinstance(getattr(_r, "raw", None), dict) and _r.raw.get("result") is not None:
                                txdash = _r.raw
                                break
                    if txdash is None:
                        txdash = await _fetch_solana_transaction(txid_s)

                    solana_swap_like = _solana_tx_is_swap_like(txdash, a.address)
                    direction, amount, fee, tx_time = _parse_solana_tx_netflow(txdash, a.address)
                    fee_only = (direction == "out") and _solana_fee_only(float(amount), fee, eps=1e-12)
                elif provider == "blockcypher":
                    direction = str(item.get("direction") or "")
                    amount = float(item.get("amount") or 0.0)
                    fee = None
                    tx_time = item.get("tx_time")
                    txdash = {"provider": "blockcypher", "txref": item.get("raw")}
                else:
                    chain = _BLOCKCHAIR_CHAIN.get(asset)
                    if not chain:
                        raise ValueError(f"unsupported asset for blockchair tx parse: {asset}")
                    txdash = await _fetch_blockchair_tx_dashboard(chain, txid_s)
                    direction, amount, fee, tx_time = _parse_blockchair_utxo_netflow(txdash, a.address, decimals)
            except Exception as e:
                errors.append({"id": a.id, "txid": txid_s, "error": f"tx fetch/parse failed ({provider}): {e}"})
                continue

            tx_row = None
            if existing:
                for r in existing:
                    if r.direction == direction:
                        tx_row = r
                        break

            if not tx_row:
                tx_row = WalletAddressTx(
                    wallet_address_id=a.id,
                    asset=asset,
                    network=a.network,
                    address=a.address,
                    txid=txid_s,
                    direction=direction,
                    amount=float(amount),
                    fee=float(fee) if fee is not None else None,
                    tx_time=tx_time,
                    raw=txdash,
                )
                db.add(tx_row)
                try:
                    db.commit()
                    cached += 1
                except IntegrityError:
                    db.rollback()
                    tx_row = db.execute(
                        select(WalletAddressTx).where(
                            WalletAddressTx.wallet_address_id == a.id,
                            WalletAddressTx.txid == txid_s,
                            WalletAddressTx.direction == direction,
                        )
                    ).scalar_one_or_none()
                except Exception as e:
                    db.rollback()
                    errors.append({"id": a.id, "txid": txid_s, "error": f"cache insert failed: {e}"})
                    continue

            # SOL metadata-only tagging (runs even if already ingested),
            # so your DB stays debuggable without writing ledger rows.
            if provider == "solana_rpc" and tx_row and isinstance(tx_row.raw, dict):
                try:
                    existing_utt = dict((tx_row.raw or {}).get("utt") or {})
                    existing_reason = existing_utt.get("skip_ledger_reason")

                    # Only backfill if missing (don’t overwrite prior reason)
                    if not existing_reason:
                        reason = None
                        extra = {}

                        if fee_only and direction == "out":
                            reason = "solana_fee_only"
                            extra = {"fee_only": True}
                        elif solana_swap_like:
                            reason = "solana_swap_like"
                        elif float(amount) <= 0.000001:
                            reason = "solana_dust"

                        if reason:
                            existing_utt.update({"complex": True, "skip_ledger_reason": reason, **extra})
                            tx_row.raw = {**tx_row.raw, "utt": existing_utt}
                        db.add(tx_row)
                        db.commit()
                except Exception:
                    db.rollback()

            # Fee-only must take precedence over swap-like labeling.
            if provider == "solana_rpc" and fee_only and direction == "out":
                # Cache-only for fee-only SOL txs (program interactions where SOL delta == tx fee)
                try:
                    if tx_row and isinstance(tx_row.raw, dict):
                        prev_utt = dict((tx_row.raw or {}).get("utt") or {})
                        prev_utt.update({"complex": True, "skip_ledger_reason": "solana_fee_only", "fee_only": True})
                        tx_row.raw = {**tx_row.raw, "utt": prev_utt}
                    db.add(tx_row)
                    db.commit()
                    if payload.write_ledger:
                        tx_row.ingested_to_ledger_at = datetime.utcnow()
                        db.add(tx_row)
                        db.commit()
                except Exception:
                    db.rollback()
                continue

            if provider == "solana_rpc" and solana_swap_like:
                # Cache-only for swap-like / complex SOL txs to avoid polluting Deposits/Withdrawals.
                # (Swaps are economically trades; we normalize them separately under solana_dex.)
                try:
                    if tx_row and isinstance(tx_row.raw, dict):
                        prev_utt = dict((tx_row.raw or {}).get("utt") or {})
                        prev_utt.update(
                            {
                                "complex": True,
                                "skip_ledger_reason": "solana_swap_like",
                            }
                        )
                        tx_row.raw = {
                            **tx_row.raw,
                            "utt": prev_utt,
                        }
                    db.add(tx_row)
                    db.commit()
                    if payload.write_ledger:
                        tx_row.ingested_to_ledger_at = datetime.utcnow()
                        db.add(tx_row)
                        db.commit()
                except Exception:
                    db.rollback()
                continue

            if not payload.write_ledger:
                continue

            if tx_row and tx_row.ingested_to_ledger_at:
                if tx_row.deposit_id or tx_row.withdrawal_id or _wa_tx_skip_reason(tx_row.raw):
                    continue

            if direction == "out" and wa_wallet_id in _DEPOSITS_ONLY_WALLET_IDS:
                continue

            venue = wa_wallet_id or "self_custody"
            wallet_id = "wallet_address"

            # Per your request: put the actual wallet address in `source`.
            # To preserve stable grouping/filtering, also persist `source_type` in raw.
            source = f"WALLET_ADDR:{a.address}"

            try:
                # SOL fee-only guard:
                # If the address only paid the tx fee (no other SOL outflow), do NOT materialize a withdrawal.
                # This is common for token/program interactions where SOL delta == meta.fee.
                if provider == "solana_rpc" and direction == "out":
                    try:
                        if _solana_fee_only(float(amount), fee, eps=1e-12):
                            try:
                                if tx_row and isinstance(tx_row.raw, dict):
                                    prev_utt = dict((tx_row.raw or {}).get("utt") or {})
                                    prev_utt.update(
                                        {
                                            "complex": True,
                                            "skip_ledger_reason": "solana_fee_only",
                                            "fee_only": True,
                                        }
                                    )
                                    tx_row.raw = {**tx_row.raw, "utt": prev_utt}
                                db.add(tx_row)
                                db.commit()
                                if payload.write_ledger:
                                    tx_row.ingested_to_ledger_at = datetime.utcnow()
                                    db.add(tx_row)
                                    db.commit()
                            except Exception:
                                db.rollback()
                            continue
                    except Exception:
                        pass

                                # SOL "dust" guard:
                # 1 lamport = 0.000000001 SOL. (Swap legs + rent + fees can produce dust rows.)
                if payload.solana_min_transfer_sol and provider == "solana_rpc":
                    try:
                        if float(amount) < float(payload.solana_min_transfer_sol):
                            try:
                                if tx_row and isinstance(tx_row.raw, dict):
                                    prev_utt = dict((tx_row.raw or {}).get("utt") or {})
                                    prev_utt.update(
                                        {
                                            "complex": True,
                                            "skip_ledger_reason": "solana_dust",
                                        }
                                    )
                                    tx_row.raw = {**tx_row.raw, "utt": prev_utt}
                                db.add(tx_row)
                                db.commit()
                                if payload.write_ledger:
                                    tx_row.ingested_to_ledger_at = datetime.utcnow()
                                    db.add(tx_row)
                                    db.commit()
                            except Exception:
                                db.rollback()
                            continue
                    except Exception:
                        pass

                if direction == "in":
                    dep = AssetDeposit(
                        venue=venue,
                        wallet_id=wallet_id,
                        asset=asset,
                        qty=float(amount),
                        deposit_time=tx_time or datetime.utcnow(),
                        txid=txid_s,
                        network=a.network,
                        status="DETECTED",
                        source=source,
                        raw={
                            "source_type": "WALLET_ADDR",
                            "wallet_address": a.address,
                            "wallet_address_label": a.label,
                            "wallet_address_id": a.id,
                            "wallet_address_tx_id": tx_row.id if tx_row else None,
                            "direction": direction,
                            "txid": txid_s,
                            "provider": provider,
                        },
                    )
                    db.add(dep)
                    db.commit()
                    db.refresh(dep)
                    tx_row.deposit_id = dep.id
                else:
                    wd = AssetWithdrawal(
                        venue=venue,
                        wallet_id=wallet_id,
                        asset=asset,
                        qty=float(amount),
                        withdraw_time=tx_time or datetime.utcnow(),
                        txid=txid_s,
                        network=a.network,
                        status="DETECTED",
                        source=source,
                        raw={
                            "source_type": "WALLET_ADDR",
                            "wallet_address": a.address,
                            "wallet_address_label": a.label,
                            "wallet_address_id": a.id,
                            "wallet_address_tx_id": tx_row.id if tx_row else None,
                            "direction": direction,
                            "txid": txid_s,
                            "fee": float(fee) if fee is not None else None,
                            "provider": provider,
                        },
                    )
                    db.add(wd)
                    db.commit()
                    db.refresh(wd)
                    tx_row.withdrawal_id = wd.id

                tx_row.ingested_to_ledger_at = datetime.utcnow()
                db.add(tx_row)
                db.commit()
                ledger_written += 1
            except IntegrityError:
                db.rollback()
                try:
                    existing_id2 = _find_existing_ledger_id(direction, asset, a.network, txid_s)
                    if existing_id2:
                        if direction == "in":
                            tx_row.deposit_id = existing_id2
                        else:
                            tx_row.withdrawal_id = existing_id2
                    tx_row.ingested_to_ledger_at = datetime.utcnow()
                    db.add(tx_row)
                    db.commit()
                except Exception:
                    db.rollback()
            except Exception as e:
                db.rollback()
                errors.append({"id": a.id, "txid": txid_s, "error": f"ledger write failed: {e}"})
                continue

        # Backlog pass: if write_ledger=true, also process cached rows for this address that are still pending
        # (or that have ingested_to_ledger_at set but never got linked).
        if payload.write_ledger:
            try:
                backlog_pending_rows = db.execute(
                    select(WalletAddressTx).where(
                        WalletAddressTx.wallet_address_id == a.id,
                        (
                            WalletAddressTx.ingested_to_ledger_at.is_(None)
                            | (
                                WalletAddressTx.ingested_to_ledger_at.is_not(None)
                                & WalletAddressTx.deposit_id.is_(None)
                                & WalletAddressTx.withdrawal_id.is_(None)
                                & (WalletAddressTx.raw.is_not(None))
                            )
                        ),
                    )
                ).scalars().all()

                for pr in backlog_pending_rows:
                    if pr.deposit_id or pr.withdrawal_id or _wa_tx_skip_reason(pr.raw):
                        if pr.ingested_to_ledger_at is None:
                            pr.ingested_to_ledger_at = datetime.utcnow()
                            db.add(pr)
                            db.commit()
                        continue

                    # Solana noise guardrail: treat tiny SOL movements as non-transfer activity (fees/rent/swap legs).
                    if pr.asset == "SOL" and pr.network == "solana" and payload.solana_min_transfer_sol:
                        try:
                            if (pr.amount or 0.0) < float(payload.solana_min_transfer_sol):
                                # keep cache row, but resolve it so it doesn't stay pending
                                if isinstance(pr.raw, dict):
                                    utt = (pr.raw or {}).get("utt") or {}
                                    utt["skip_ledger_reason"] = "solana_dust"
                                    pr.raw = {**pr.raw, "utt": utt}
                                pr.ingested_to_ledger_at = datetime.utcnow()
                                db.add(pr)
                                db.commit()
                                continue
                        except Exception:
                            pass

                    existing_id3 = _find_existing_ledger_id(pr.direction, pr.asset, pr.network, pr.txid)
                    if existing_id3:
                        if pr.direction == "in":
                            pr.deposit_id = existing_id3
                        elif pr.direction == "out":
                            pr.withdrawal_id = existing_id3
                        pr.ingested_to_ledger_at = datetime.utcnow()
                        db.add(pr)
                        db.commit()
                        ledger_written += 1
                        continue

                    # Materialize backlog item into ledger
                    if pr.direction == "in":
                        dep = AssetDeposit(
                            venue=venue,
                            wallet_id=wallet_id,
                            asset=pr.asset,
                            qty=float(pr.amount or 0.0),
                            deposit_time=pr.tx_time or datetime.utcnow(),
                            txid=pr.txid,
                            network=pr.network,
                            status="DETECTED",
                            source=source,
                            raw={
                                "source_type": "WALLET_ADDR",
                                "wallet_address": a.address,
                                "wallet_address_label": a.label,
                                "wallet_address_id": a.id,
                                "wallet_address_tx_id": pr.id,
                                "direction": pr.direction,
                                "txid": pr.txid,
                                "provider": (pr.raw or {}).get("provider") if isinstance(pr.raw, dict) else None,
                            },
                        )
                        db.add(dep)
                        db.commit()
                        db.refresh(dep)
                        pr.deposit_id = dep.id
                    elif pr.direction == "out":
                        wd = AssetWithdrawal(
                            venue=venue,
                            wallet_id=wallet_id,
                            asset=pr.asset,
                            qty=float(pr.amount or 0.0),
                            withdraw_time=pr.tx_time or datetime.utcnow(),
                            txid=pr.txid,
                            network=pr.network,
                            status="DETECTED",
                            source=source,
                            raw={
                                "source_type": "WALLET_ADDR",
                                "wallet_address": a.address,
                                "wallet_address_label": a.label,
                                "wallet_address_id": a.id,
                                "wallet_address_tx_id": pr.id,
                                "direction": pr.direction,
                                "txid": pr.txid,
                                "fee": float(pr.fee) if pr.fee is not None else None,
                                "provider": (pr.raw or {}).get("provider") if isinstance(pr.raw, dict) else None,
                            },
                        )
                        db.add(wd)
                        db.commit()
                        db.refresh(wd)
                        pr.withdrawal_id = wd.id
                    else:
                        # resolve bad direction so it doesn't stay pending
                        if isinstance(pr.raw, dict):
                            utt = (pr.raw or {}).get("utt") or {}
                            utt["skip_ledger_reason"] = "bad_direction"
                            pr.raw = {**pr.raw, "utt": utt}
                        pr.ingested_to_ledger_at = datetime.utcnow()
                        db.add(pr)
                        db.commit()
                        continue

                    pr.ingested_to_ledger_at = datetime.utcnow()
                    db.add(pr)
                    db.commit()
                    ledger_written += 1
            except Exception:
                db.rollback()
                pass

    # ------------------------------------------------------------------
    # Tx ingest outcome counters (UI debuggability)
    #
    # NOTE: We compute these from DB state for the addresses in-scope,
    # so the UI gets useful counters even if the ingest response did not
    # track per-run deltas.
    # ------------------------------------------------------------------
    addr_ids = [x.id for x in addrs]
    linked_deposits = 0
    linked_withdrawals = 0
    pending = 0
    skipped_by_reason: Dict[str, int] = {}

    try:
        rows = db.execute(
            select(
                WalletAddressTx.deposit_id,
                WalletAddressTx.withdrawal_id,
                WalletAddressTx.ingested_to_ledger_at,
                WalletAddressTx.raw,
            ).where(WalletAddressTx.wallet_address_id.in_(addr_ids))
        ).all()

        for dep_id, wd_id, ing_at, raw in rows:
            if dep_id:
                linked_deposits += 1
            if wd_id:
                linked_withdrawals += 1
            if ing_at is None:
                pending += 1
            r = _wa_tx_skip_reason(raw)
            if r:
                skipped_by_reason[r] = skipped_by_reason.get(r, 0) + 1
    except Exception:
        # Counters are best-effort; never fail the ingest response.
        pass

    skipped_total = sum(skipped_by_reason.values())

    return {
        "ok": True,
        "addresses": len(addrs),
        "cached": cached,
        "ledger_written": ledger_written,
        "linked_deposits": linked_deposits,
        "linked_withdrawals": linked_withdrawals,
        "skipped_total": skipped_total,
        "skipped_by_reason": skipped_by_reason,
        "pending": pending,
        "errors": errors,
    }

# --- Solana/Jupiter: swap-like txs -> swap_orders ingestion (venue-gated) ---

def _ensure_swap_orders_table(db: Session) -> None:
    """
    Ensure swap_orders table exists. Keep minimal schema required by all_orders.py normalizer.
    """
    from sqlalchemy import text, inspect

    insp = inspect(db.bind)
    if "swap_orders" in insp.get_table_names():
        return

    db.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS swap_orders (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              venue TEXT NOT NULL,
              chain TEXT NOT NULL,
              wallet_address TEXT,
              signature TEXT NOT NULL,
              raw_symbol TEXT,
              resolved_symbol TEXT,
              side TEXT,
              type TEXT,
              status TEXT,
              base_qty REAL,
              quote_qty REAL,
              price REAL,
              fee_quote REAL,
              base_mint TEXT,
              quote_mint TEXT,
              ts TEXT,
              raw JSON
            );
            """
        )
    )
    db.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS ux_swap_orders_venue_sig ON swap_orders(venue, signature);"))
    db.commit()


def _solana_token_deltas_from_tx(js: Dict, owner_addr: str) -> Dict[str, float]:
    """
    Return {mint: delta_ui_amount} for token accounts attributable to owner_addr using
    meta.preTokenBalances/postTokenBalances. This is heuristic: many swaps touch ATAs/wSOL.
    """
    result = (js or {}).get("result") or {}
    meta = result.get("meta") or {}

    # accountKeys indices let us map token balances to owners
    tx = (result.get("transaction") or {})
    msg = (tx.get("message") or {})
    keys_raw = msg.get("accountKeys") or []
    keys: List[str] = []
    for k in keys_raw:
        if isinstance(k, str):
            keys.append(k)
        elif isinstance(k, dict):
            pk = k.get("pubkey")
            if pk:
                keys.append(str(pk))

    def _owner_for(tb: Dict) -> Optional[str]:
        # token balance objects may include 'owner' on some nodes. Do NOT try to derive
        # owner from accountIndex -> accountKeys; that key is typically the token account,
        # not the wallet owner, which causes false owner mismatches.
        o = tb.get("owner")
        if isinstance(o, str) and o:
            return o
        return None

    def _ui_amount(tb: Dict) -> float:
        ui = ((tb.get("uiTokenAmount") or {}).get("uiAmount"))
        if ui is None:
            # fallback: amount/decimals
            amt = (tb.get("uiTokenAmount") or {}).get("amount")
            dec = (tb.get("uiTokenAmount") or {}).get("decimals")
            try:
                return float(int(amt)) / (10 ** int(dec))
            except Exception:
                return 0.0
        try:
            return float(ui)
        except Exception:
            return 0.0

    pre = meta.get("preTokenBalances") or []
    post = meta.get("postTokenBalances") or []

    # key by (mint, owner, accountIndex) to align pre/post
    pre_map: Dict[Tuple[str, str, int], float] = {}
    for tb in pre:
        if not isinstance(tb, dict):
            continue
        mint = tb.get("mint")
        if not isinstance(mint, str) or not mint:
            continue
        owner = _owner_for(tb) or ""
        if owner != owner_addr:
            continue
        try:
            ai = int(tb.get("accountIndex"))
        except Exception:
            ai = -1
        pre_map[(mint, owner, ai)] = _ui_amount(tb)

    deltas: Dict[str, float] = {}
    seen_keys = set()

    for tb in post:
        if not isinstance(tb, dict):
            continue
        mint = tb.get("mint")
        if not isinstance(mint, str) or not mint:
            continue
        owner = _owner_for(tb) or ""
        if owner != owner_addr:
            continue
        try:
            ai = int(tb.get("accountIndex"))
        except Exception:
            ai = -1
        k = (mint, owner, ai)
        seen_keys.add(k)
        post_amt = _ui_amount(tb)
        pre_amt = pre_map.get(k, 0.0)
        d = post_amt - pre_amt
        if abs(d) > 0:
            deltas[mint] = deltas.get(mint, 0.0) + float(d)

    # include pre-only entries that went to zero / closed
    for k, pre_amt in pre_map.items():
        if k in seen_keys:
            continue
        mint = k[0]
        d = 0.0 - float(pre_amt)
        if abs(d) > 0:
            deltas[mint] = deltas.get(mint, 0.0) + d


    # Normalize wrapped SOL (wSOL) to native SOL so multi-route swaps can pair legs.
    _WSOL_MINT = "So11111111111111111111111111111111111111112"
    if _WSOL_MINT in deltas:
        deltas["SOL"] = deltas.get("SOL", 0.0) + deltas.pop(_WSOL_MINT)

    # Add native SOL delta (lamports) for this owner when present.
    # Jupiter swaps often have one leg in native SOL, which is not represented in pre/postTokenBalances.
    try:
        result = (js or {}).get("result") or {}
        meta = result.get("meta") or {}
        pre_bal = meta.get("preBalances") or []
        post_bal = meta.get("postBalances") or []
        fee_lamports = int(meta.get("fee") or 0)

        tx = (result.get("transaction") or {})
        msg = (tx.get("message") or {})
        keys_raw = msg.get("accountKeys") or []
        keys: List[str] = []
        for k in keys_raw:
            if isinstance(k, str):
                keys.append(k)
            elif isinstance(k, dict):
                pk = k.get("pubkey")
                if pk:
                    keys.append(str(pk))

        if owner_addr in keys:
            idx = keys.index(owner_addr)
            if idx < len(pre_bal) and idx < len(post_bal):
                pre_l = int(pre_bal[idx] or 0)
                post_l = int(post_bal[idx] or 0)
                d_l = post_l - pre_l

                # Remove fee from the payer's delta so swaps don't get misclassified as one-sided.
                # pre/post balances already include the fee deduction, so add the fee back.
                if keys and keys[0] == owner_addr and fee_lamports:
                    d_l += fee_lamports

                d_sol = float(d_l) / 1e9
                if abs(d_sol) > 0:
                    deltas["SOL"] = deltas.get("SOL", 0.0) + d_sol
    except Exception:
        pass

    return deltas


def _solana_token_deltas_from_tx_relaxed(js: Dict, owner_addr: str) -> Dict[str, float]:
    """
    Relaxed fallback used when owner-attributed token deltas are empty or one-sided.

    Aggregates token balance deltas by mint across the tx even if token balance rows do
    not expose `owner` (common on some Solana/Jupiter routes). This is only used after a
    tx has already been classified as swap-like, so it is a best-effort recovery path for
    missed Jupiter fills rather than the primary attribution path.
    """
    result = (js or {}).get("result") or {}
    meta = result.get("meta") or {}

    def _ui_amount(tb: Dict) -> float:
        ui = ((tb.get("uiTokenAmount") or {}).get("uiAmount"))
        if ui is None:
            amt = (tb.get("uiTokenAmount") or {}).get("amount")
            dec = (tb.get("uiTokenAmount") or {}).get("decimals")
            try:
                return float(int(amt)) / (10 ** int(dec))
            except Exception:
                return 0.0
        try:
            return float(ui)
        except Exception:
            return 0.0

    pre = meta.get("preTokenBalances") or []
    post = meta.get("postTokenBalances") or []

    pre_map: Dict[Tuple[str, int], float] = {}
    for tb in pre:
        if not isinstance(tb, dict):
            continue
        mint = tb.get("mint")
        if not isinstance(mint, str) or not mint:
            continue
        try:
            ai = int(tb.get("accountIndex"))
        except Exception:
            ai = -1
        pre_map[(mint, ai)] = _ui_amount(tb)

    deltas: Dict[str, float] = {}
    seen_keys = set()
    for tb in post:
        if not isinstance(tb, dict):
            continue
        mint = tb.get("mint")
        if not isinstance(mint, str) or not mint:
            continue
        try:
            ai = int(tb.get("accountIndex"))
        except Exception:
            ai = -1
        k = (mint, ai)
        seen_keys.add(k)
        post_amt = _ui_amount(tb)
        pre_amt = pre_map.get(k, 0.0)
        d = post_amt - pre_amt
        if abs(d) > 0:
            deltas[mint] = deltas.get(mint, 0.0) + float(d)

    for k, pre_amt in pre_map.items():
        if k in seen_keys:
            continue
        mint = k[0]
        d = 0.0 - float(pre_amt)
        if abs(d) > 0:
            deltas[mint] = deltas.get(mint, 0.0) + d

    _WSOL_MINT = "So11111111111111111111111111111111111111112"
    if _WSOL_MINT in deltas:
        deltas["SOL"] = deltas.get("SOL", 0.0) + deltas.pop(_WSOL_MINT)

    # Add the owner's native SOL delta as the non-token leg when present.
    try:
        tx = (result.get("transaction") or {})
        msg = (tx.get("message") or {})
        keys_raw = msg.get("accountKeys") or []
        keys: List[str] = []
        for k in keys_raw:
            if isinstance(k, str):
                keys.append(k)
            elif isinstance(k, dict):
                pk = k.get("pubkey")
                if pk:
                    keys.append(str(pk))

        pre_bal = meta.get("preBalances") or []
        post_bal = meta.get("postBalances") or []
        fee_lamports = int(meta.get("fee") or 0)

        if owner_addr in keys:
            idx = keys.index(owner_addr)
            if idx < len(pre_bal) and idx < len(post_bal):
                pre_l = int(pre_bal[idx] or 0)
                post_l = int(post_bal[idx] or 0)
                d_l = post_l - pre_l
                if keys and keys[0] == owner_addr and fee_lamports:
                    d_l += fee_lamports
                d_sol = float(d_l) / 1e9
                if abs(d_sol) > 0:
                    deltas["SOL"] = deltas.get("SOL", 0.0) + d_sol
    except Exception:
        pass

    return deltas





def _solana_token_meta_from_tx(js: Dict) -> Dict[str, Dict[str, Optional[int]]]:
    """Best-effort mint -> metadata map from pre/post token balances.

    Returns { mint: {"decimals": int|None} }. Symbols are not reliably present in Solana RPC
    token balance payloads, so decimals are the dependable field we preserve.
    """
    result = (js or {}).get("result") or {}
    meta = result.get("meta") or {}
    out: Dict[str, Dict[str, Optional[int]]] = {}
    for tb in [*(meta.get("preTokenBalances") or []), *(meta.get("postTokenBalances") or [])]:
        if not isinstance(tb, dict):
            continue
        mint = str(tb.get("mint") or "").strip()
        if not mint:
            continue
        dec = (tb.get("uiTokenAmount") or {}).get("decimals")
        try:
            dec_i = int(dec) if dec is not None else None
        except Exception:
            dec_i = None
        cur = out.get(mint) or {}
        if cur.get("decimals") is None and dec_i is not None:
            cur["decimals"] = dec_i
        out[mint] = cur
    return out


_RAYDIUM_PROGRAM_ID_HINTS = {
    # Raydium AMM v4
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
    # Raydium CLMM
    "CAMMCzo5YL8w4VFF8KVHrK22GGUQ1Jj3D5m2wNw7wTzM",
    # Raydium CPMM / router ecosystems vary across deployments; keep broad textual fallbacks too.
}


def _solana_program_ids_from_tx(js: Dict) -> List[str]:
    result = (js or {}).get("result") or {}
    meta = result.get("meta") or {}
    tx = (result.get("transaction") or {})
    msg = (tx.get("message") or {})

    keys_raw = msg.get("accountKeys") or []
    keys: List[str] = []
    for k in keys_raw:
        if isinstance(k, str):
            keys.append(k)
        elif isinstance(k, dict):
            pk = k.get("pubkey")
            if pk:
                keys.append(str(pk))

    def _pid_from_index(ix: Optional[int]) -> Optional[str]:
        if ix is None:
            return None
        try:
            i = int(ix)
            if 0 <= i < len(keys):
                return keys[i]
        except Exception:
            return None
        return None

    out: List[str] = []
    for ins in (msg.get("instructions") or []):
        if not isinstance(ins, dict):
            continue
        pid = ins.get("programId")
        if isinstance(pid, str) and pid:
            out.append(pid)
        pid2 = _pid_from_index(ins.get("programIdIndex"))
        if pid2:
            out.append(pid2)
    for ii in (meta.get("innerInstructions") or []):
        if not isinstance(ii, dict):
            continue
        for ins in (ii.get("instructions") or []):
            if not isinstance(ins, dict):
                continue
            pid = ins.get("programId")
            if isinstance(pid, str) and pid:
                out.append(pid)
            pid2 = _pid_from_index(ins.get("programIdIndex"))
            if pid2:
                out.append(pid2)
    return out


def _solana_is_probable_raydium_liquidity_event(js: Dict, deltas: Dict[str, float]) -> Tuple[bool, Optional[str]]:
    """
    Best-effort filter for Raydium add/remove liquidity events so they do not surface as swaps.

    Heuristics:
      - Raydium program/log involvement must be present.
      - Liquidity events usually have 3+ material mint legs (two assets + LP mint/burn),
        or at least 2 positive + 1 negative / 2 negative + 1 positive token deltas.
      - We intentionally keep this conservative to avoid suppressing normal swaps.
    """
    result = (js or {}).get("result") or {}
    meta = result.get("meta") or {}

    program_ids = _solana_program_ids_from_tx(js)
    log_messages = [str(x) for x in (meta.get("logMessages") or []) if isinstance(x, str)]
    raydium_seen = False
    for pid in program_ids:
        if pid in _RAYDIUM_PROGRAM_ID_HINTS:
            raydium_seen = True
            break
    if not raydium_seen:
        joined = "\n".join(log_messages).lower()
        if "raydium" in joined or "ray_log" in joined:
            raydium_seen = True
    if not raydium_seen:
        return False, None

    material = [(m, float(d)) for m, d in (deltas or {}).items() if abs(float(d)) > 1e-9]
    pos = [(m, d) for m, d in material if d > 0]
    neg = [(m, d) for m, d in material if d < 0]
    token_only_material = [(m, d) for m, d in material if str(m or "").strip() != "SOL"]
    token_only_pos = [(m, d) for m, d in pos if str(m or "").strip() != "SOL"]
    token_only_neg = [(m, d) for m, d in neg if str(m or "").strip() != "SOL"]

    if len(token_only_material) >= 3 and ((len(token_only_pos) >= 2 and len(token_only_neg) >= 1) or (len(token_only_neg) >= 2 and len(token_only_pos) >= 1)):
        return True, "solana_liquidity_event"

    return False, None


# Prefer stable quote legs over incidental SOL/wSOL settlement legs when a swap/fill tx
# exposes multiple negative deltas. This is especially important for Jupiter limit-order
# fills where the settlement tx can include a SOL leg even though the actual quote spent
# was USDC.
_SOLANA_STABLE_QUOTE_MINTS = {
    # USDC
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    # Symbol-like fallbacks if a caller ever resolves names upstream
    "USDC",
    "USDT",
    "PYUSD",
}

def _solana_pick_base_quote_legs(
    pos: List[Tuple[str, float]],
    neg: List[Tuple[str, float]],
    token_meta: Optional[Dict[str, Dict[str, Optional[int]]]] = None,
) -> Tuple[str, float, str, float, Optional[str]]:
    """
    Pick (base_mint, base_qty, quote_mint, quote_delta, selection_hint).

    Heuristics:
      1) Prefer the largest positive NON-SOL leg as base when available.
      2) Prefer a stable-quote negative leg (USDC/USDT/PYUSD or 6-dec non-SOL token)
         over an incidental SOL leg when both are present.
      3) Fall back to the old heuristic otherwise.
    """
    token_meta = token_meta or {}

    # Base: if we have multiple positive legs and at least one is non-SOL, use the largest non-SOL leg.
    non_sol_pos = [(m, d) for m, d in pos if str(m or "").strip() != "SOL"]
    if non_sol_pos:
        base_mint, base_qty = max(non_sol_pos, key=lambda x: x[1])
        base_hint = "prefer_non_sol_positive"
    else:
        base_mint, base_qty = max(pos, key=lambda x: x[1])
        base_hint = None

    # Quote: old heuristic baseline
    default_quote_mint, default_quote_delta = min(neg, key=lambda x: x[1])

    def _is_known_stable(mint: str) -> bool:
        m = str(mint or "").strip()
        return m in _SOLANA_STABLE_QUOTE_MINTS

    def _decimals_for(mint: str) -> Optional[int]:
        try:
            return (token_meta.get(str(mint or "").strip()) or {}).get("decimals")
        except Exception:
            return None

    # Strong preference: explicit stable quote mint(s)
    stable_neg = [(m, d) for m, d in neg if _is_known_stable(m)]
    if stable_neg and str(base_mint or "").strip() != "SOL":
        quote_mint, quote_delta = min(stable_neg, key=lambda x: x[1])  # largest spend among stable legs
        return base_mint, base_qty, quote_mint, quote_delta, "prefer_known_stable_quote"

    # Fallback preference: if SOL is one of several negative legs, and another non-SOL leg has 6 decimals,
    # prefer the 6-decimal token as quote. This catches USDC-like fills even when only mint addresses are present.
    has_sol_neg = any(str(m or "").strip() == "SOL" for m, _ in neg)
    six_dec_non_sol_neg = [
        (m, d) for m, d in neg
        if str(m or "").strip() != "SOL" and _decimals_for(m) == 6
    ]
    if has_sol_neg and six_dec_non_sol_neg and str(base_mint or "").strip() != "SOL":
        quote_mint, quote_delta = min(six_dec_non_sol_neg, key=lambda x: x[1])
        return base_mint, base_qty, quote_mint, quote_delta, "prefer_6dec_quote_over_sol"

    return base_mint, base_qty, default_quote_mint, default_quote_delta, base_hint



def _solana_pick_canonical_swap_legs(
    deltas: Dict[str, float],
    token_meta: Optional[Dict[str, Dict[str, Optional[int]]]] = None,
) -> Optional[Tuple[str, float, str, float, str, Optional[str]]]:
    """
    Return a canonicalized pair orientation:
      (base_mint, base_qty_abs, quote_mint, quote_qty_abs, side, selection_hint)

    For token/SOL and token/stable pairs, keep the non-SOL / non-stable asset as BASE so
    both buys and sells resolve to a consistent symbol like UTTT-SOL or UTTT-USDC.

    side semantics:
      - buy  => base delta > 0 (received base, spent quote)
      - sell => base delta < 0 (spent base, received quote)
    """
    token_meta = token_meta or {}
    material = [(str(m or "").strip(), float(d)) for m, d in (deltas or {}).items() if abs(float(d)) > 1e-9]
    if len(material) < 2:
        return None

    pos = [(m, d) for m, d in material if d > 0]
    neg = [(m, d) for m, d in material if d < 0]
    if not pos or not neg:
        return None

    material_mints = [m for m, _d in material]
    unique_mints = list(dict.fromkeys(material_mints))

    def _is_known_stable(mint: str) -> bool:
        m = str(mint or "").strip()
        return m in _SOLANA_STABLE_QUOTE_MINTS

    # Strong canonicalization: exactly one non-SOL mint paired with SOL.
    if "SOL" in unique_mints:
        non_sol = [m for m in unique_mints if m != "SOL"]
        if len(non_sol) == 1:
            base_mint = non_sol[0]
            quote_mint = "SOL"
            base_delta = float(deltas.get(base_mint) or 0.0)
            quote_delta = float(deltas.get(quote_mint) or 0.0)
            if abs(base_delta) > 1e-9 and abs(quote_delta) > 1e-9:
                side = "buy" if base_delta > 0 else "sell"
                return base_mint, abs(base_delta), quote_mint, abs(quote_delta), side, "canonical_non_sol_vs_sol"

    # Strong canonicalization: exactly one stable mint and one non-stable mint.
    stable_mints = [m for m in unique_mints if _is_known_stable(m)]
    non_stable = [m for m in unique_mints if not _is_known_stable(m)]
    if len(stable_mints) == 1 and len(non_stable) == 1:
        base_mint = non_stable[0]
        quote_mint = stable_mints[0]
        base_delta = float(deltas.get(base_mint) or 0.0)
        quote_delta = float(deltas.get(quote_mint) or 0.0)
        if abs(base_delta) > 1e-9 and abs(quote_delta) > 1e-9:
            side = "buy" if base_delta > 0 else "sell"
            return base_mint, abs(base_delta), quote_mint, abs(quote_delta), side, "canonical_non_stable_vs_stable"

    # Fallback to existing positive/negative leg picker.
    base_mint, base_qty, quote_mint, quote_delta, selection_hint = _solana_pick_base_quote_legs(
        pos, neg, token_meta=token_meta
    )
    side = "buy"
    try:
        base_delta = float(deltas.get(base_mint) or 0.0)
        if base_delta < 0:
            side = "sell"
    except Exception:
        pass
    return base_mint, abs(float(base_qty)), quote_mint, abs(float(quote_delta)), side, selection_hint

def _solana_owner_mint_deltas_from_tx(js: Dict) -> Dict[Tuple[str, str], float]:
    """
    Return {(mint, owner_key): delta_ui_amount} using pre/post token balances.
    We intentionally keep owner scope because Jupiter limit-order settlement txs can expose
    the true received base leg on one owner and the spent quote leg on another related owner.
    """
    result = (js or {}).get("result") or {}
    meta = result.get("meta") or {}

    def _ui_amount(tb: Dict) -> float:
        ui = ((tb.get("uiTokenAmount") or {}).get("uiAmount"))
        if ui is None:
            amt = (tb.get("uiTokenAmount") or {}).get("amount")
            dec = (tb.get("uiTokenAmount") or {}).get("decimals")
            try:
                return float(int(amt)) / (10 ** int(dec))
            except Exception:
                return 0.0
        try:
            return float(ui)
        except Exception:
            return 0.0

    pre = meta.get("preTokenBalances") or []
    post = meta.get("postTokenBalances") or []

    def _owner_key(tb: Dict) -> str:
        owner = str(tb.get("owner") or "").strip()
        if owner:
            return owner
        try:
            ai = int(tb.get("accountIndex"))
        except Exception:
            ai = -1
        return f"acct:{ai}"

    pre_map: Dict[Tuple[str, str, int], float] = {}
    for tb in pre:
        if not isinstance(tb, dict):
            continue
        mint = str(tb.get("mint") or "").strip()
        if not mint:
            continue
        owner_key = _owner_key(tb)
        try:
            ai = int(tb.get("accountIndex"))
        except Exception:
            ai = -1
        pre_map[(mint, owner_key, ai)] = _ui_amount(tb)

    deltas: Dict[Tuple[str, str], float] = {}
    seen = set()
    for tb in post:
        if not isinstance(tb, dict):
            continue
        mint = str(tb.get("mint") or "").strip()
        if not mint:
            continue
        owner_key = _owner_key(tb)
        try:
            ai = int(tb.get("accountIndex"))
        except Exception:
            ai = -1
        k = (mint, owner_key, ai)
        seen.add(k)
        post_amt = _ui_amount(tb)
        pre_amt = pre_map.get(k, 0.0)
        d = post_amt - pre_amt
        if abs(d) > 0:
            deltas[(mint, owner_key)] = deltas.get((mint, owner_key), 0.0) + float(d)

    for k, pre_amt in pre_map.items():
        if k in seen:
            continue
        mint, owner_key, _ai = k
        d = 0.0 - float(pre_amt)
        if abs(d) > 0:
            deltas[(mint, owner_key)] = deltas.get((mint, owner_key), 0.0) + d

    _WSOL_MINT = "So11111111111111111111111111111111111111112"
    out: Dict[Tuple[str, str], float] = {}
    for (mint, owner_key), delta in deltas.items():
        mint2 = "SOL" if mint == _WSOL_MINT else mint
        out[(mint2, owner_key)] = out.get((mint2, owner_key), 0.0) + float(delta)
    return out


def _solana_pick_base_quote_legs_from_owner_extrema(
    js: Dict,
    token_meta: Optional[Dict[str, Dict[str, Optional[int]]]] = None,
) -> Optional[Tuple[str, float, str, float, Optional[str]]]:
    """
    Recover legs from the strongest per-owner token-balance deltas in the tx.

    This is designed for Jupiter limit-order settlement fills where:
      - the user-facing received base leg may land on one owned token account
      - the spent quote leg may be reflected on a different related owner/account
      - naive owner-specific aggregation can therefore collapse to a tiny dust delta
    """
    token_meta = token_meta or {}
    owner_deltas = _solana_owner_mint_deltas_from_tx(js)
    if not owner_deltas:
        return None

    pos = [(mint, delta, owner) for (mint, owner), delta in owner_deltas.items() if delta > 0]
    neg = [(mint, delta, owner) for (mint, owner), delta in owner_deltas.items() if delta < 0]
    if not pos or not neg:
        return None

    non_sol_pos = [(m, d, o) for (m, d, o) in pos if str(m or "").strip() != "SOL"]
    if non_sol_pos:
        base_mint, base_qty, _base_owner = max(non_sol_pos, key=lambda x: x[1])
    else:
        base_mint, base_qty, _base_owner = max(pos, key=lambda x: x[1])

    def _is_known_stable(mint: str) -> bool:
        m = str(mint or "").strip()
        return m in _SOLANA_STABLE_QUOTE_MINTS

    def _decimals_for(mint: str) -> Optional[int]:
        try:
            return (token_meta.get(str(mint or "").strip()) or {}).get("decimals")
        except Exception:
            return None

    stable_neg = [(m, d, o) for (m, d, o) in neg if _is_known_stable(m)]
    if stable_neg and str(base_mint or "").strip() != "SOL":
        quote_mint, quote_delta, _quote_owner = min(stable_neg, key=lambda x: x[1])
        return base_mint, float(base_qty), quote_mint, float(quote_delta), "owner_extrema_prefer_known_stable_quote"

    has_sol_neg = any(str(m or "").strip() == "SOL" for m, _d, _o in neg)
    six_dec_non_sol_neg = [
        (m, d, o) for (m, d, o) in neg
        if str(m or "").strip() != "SOL" and _decimals_for(m) == 6
    ]
    if has_sol_neg and six_dec_non_sol_neg and str(base_mint or "").strip() != "SOL":
        quote_mint, quote_delta, _quote_owner = min(six_dec_non_sol_neg, key=lambda x: x[1])
        return base_mint, float(base_qty), quote_mint, float(quote_delta), "owner_extrema_prefer_6dec_quote_over_sol"

    quote_mint, quote_delta, _quote_owner = min(neg, key=lambda x: x[1])
    return base_mint, float(base_qty), quote_mint, float(quote_delta), "owner_extrema_fallback"


@router.post("/solana/tx/ingest")
async def wallet_addresses_solana_tx_ingest(
    limit_per_address: int = Query(50, ge=1, le=500),
    write_ledger: bool = Query(False),
    db: Session = Depends(get_db),
):
    """Discover + cache Solana txs for SOL wallet addresses only.

    This is a Solana-only variant of /tx/ingest. It intentionally defaults to
    write_ledger=False because swap materialization is handled by /solana/ingest_swap_orders.
    """
    wallet_addresses = db.execute(select(WalletAddress)).scalars().all()
    sol_ids = [str(wa.id) for wa in wallet_addresses if _norm_asset(wa.asset) == "SOL"]
    if not sol_ids:
        return {"ok": True, "cached": 0, "ledger_written": 0, "errors": []}

    payload = WalletAddressTxIngestRequest(
        ids=sol_ids,
        limit_per_address=limit_per_address,
        write_ledger=write_ledger,
    )
    return await wallet_addresses_tx_ingest(payload=payload, db=db)


@router.post("/solana/ingest_swap_orders")
def ingest_solana_swap_orders(
    wallet_address: Optional[str] = None,
    max_rows: int = 5000,
    venue: str = "solana_jupiter",
    db: Session = Depends(get_db),
):
    """
    Reads cached wallet_address_txs rows tagged utt.skip_ledger_reason == solana_swap_like,
    re-fetches tx jsonParsed from Solana RPC, derives base/quote legs, and writes idempotently
    into swap_orders for All Orders history.
    """
    _ensure_swap_orders_table(db)

    q = select(WalletAddressTx).where(WalletAddressTx.network == "solana")
    if wallet_address:
        q = q.where(WalletAddressTx.address == wallet_address)
    q = q.order_by(WalletAddressTx.tx_time.desc()).limit(int(max_rows))

    rows = db.execute(q).scalars().all()

    inserted = 0
    ignored = 0
    skipped = 0
    errors: List[Dict] = []

    # Diagnostics: why swap-like txs fail to materialize into swap_orders.
    skip_reasons: Dict[str, int] = {}
    skipped_examples: List[Dict] = []
    _SKIP_EXAMPLES_MAX = 10

    from sqlalchemy import text

    for tx in rows:
        raw = tx.raw or {}
        utt = (raw.get("utt") or {})
        sig = tx.txid
        addr = tx.address

        tagged_swap_like = str(utt.get("skip_ledger_reason") or "") == "solana_swap_like"
        second_chance_swap_like = False
        js = None

        if not tagged_swap_like:
            try:
                js = _fetch_solana_tx_dashboard(sig, cached_raw=raw)
                meta = ((js.get("result") or {}).get("meta") or {})
                pre_tb = meta.get("preTokenBalances") or []
                post_tb = meta.get("postTokenBalances") or []
                if pre_tb or post_tb:
                    second_chance_swap_like = True
                if not second_chance_swap_like and _solana_tx_is_swap_like(js, addr):
                    second_chance_swap_like = True
                if not second_chance_swap_like:
                    deltas_probe = _solana_token_deltas_from_tx(js, addr)
                    pos_probe = [(m, d) for m, d in deltas_probe.items() if d > 0]
                    neg_probe = [(m, d) for m, d in deltas_probe.items() if d < 0]
                    if pos_probe and neg_probe:
                        second_chance_swap_like = True
                if not second_chance_swap_like:
                    relaxed_probe = _solana_token_deltas_from_tx_relaxed(js, addr)
                    pos_probe = [(m, d) for m, d in relaxed_probe.items() if d > 0]
                    neg_probe = [(m, d) for m, d in relaxed_probe.items() if d < 0]
                    if pos_probe and neg_probe:
                        second_chance_swap_like = True
            except Exception:
                second_chance_swap_like = False

            if not second_chance_swap_like:
                skip_reasons["not_swap_like"] = skip_reasons.get("not_swap_like", 0) + 1
                continue

        # Track existing row, but do not skip: corrected materialization logic should be able
        # to repair previously misclassified rows on re-ingest.
        exists = db.execute(
            text("SELECT 1 FROM swap_orders WHERE venue = :v AND signature = :s LIMIT 1"),
            {"v": venue, "s": sig},
        ).first()

        try:
            if js is None:
                js = _fetch_solana_tx_dashboard(sig, cached_raw=raw)
            # token deltas for owner
            deltas = _solana_token_deltas_from_tx(js, addr)
            # Diagnostics helpers (owner may be missing on some token balance nodes)
            _owner_missing = 0
            try:
                meta = ((js.get("result") or {}).get("meta") or {})
                for tb in (meta.get("preTokenBalances") or []):
                    if not (tb.get("owner") or ""):
                        _owner_missing += 1
                for tb in (meta.get("postTokenBalances") or []):
                    if not (tb.get("owner") or ""):
                        _owner_missing += 1
            except Exception:
                _owner_missing = 0

            # Fallback for newer Jupiter routes / limit-order settlements where token balance
            # rows may omit owner or produce one-sided owner-attributed deltas.
            pos = [(m, d) for m, d in deltas.items() if d > 0]
            neg = [(m, d) for m, d in deltas.items() if d < 0]
            if (not deltas) or (not pos) or (not neg):
                relaxed = _solana_token_deltas_from_tx_relaxed(js, addr)
                pos_r = [(m, d) for m, d in relaxed.items() if d > 0]
                neg_r = [(m, d) for m, d in relaxed.items() if d < 0]
                if relaxed and pos_r and neg_r:
                    deltas = relaxed
                    pos = pos_r
                    neg = neg_r

            if not deltas:
                skipped += 1
                reason = "no_token_deltas_owner_missing" if _owner_missing else "no_token_deltas"
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                if len(skipped_examples) < _SKIP_EXAMPLES_MAX:
                    skipped_examples.append({"sig": sig, "reason": reason})
                continue

            is_liquidity_event, liquidity_reason = _solana_is_probable_raydium_liquidity_event(js, deltas)
            if is_liquidity_event:
                skipped += 1
                reason = liquidity_reason or "solana_liquidity_event"
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                try:
                    if isinstance(tx.raw, dict):
                        prev_utt = dict((tx.raw or {}).get("utt") or {})
                        prev_utt.update({"complex": True, "skip_ledger_reason": reason})
                        tx.raw = {**tx.raw, "utt": prev_utt}
                        db.add(tx)
                except Exception:
                    pass
                if len(skipped_examples) < _SKIP_EXAMPLES_MAX:
                    skipped_examples.append({"sig": sig, "reason": reason})
                continue

            # pick legs with stable-quote preference:
            # - prefer non-SOL positive leg as base when available
            # - prefer stable quote / 6-dec quote token over incidental SOL settlement leg
            pos = [(m, d) for m, d in deltas.items() if d > 0]
            neg = [(m, d) for m, d in deltas.items() if d < 0]

            if not pos or not neg:
                # Native SOL legs may only appear in lamport balance deltas rather than token balance deltas.
                native_sol_delta = _solana_native_sol_delta_from_tx(js, addr)
                has_non_sol_material = any(str(m or "").strip() != "SOL" and abs(float(d)) > 1e-9 for m, d in deltas.items())
                if native_sol_delta is not None and has_non_sol_material:
                    if abs(float(native_sol_delta)) > 1e-9:
                        deltas = dict(deltas)
                        deltas["SOL"] = float(deltas.get("SOL") or 0.0) + float(native_sol_delta)
                        pos = [(m, d) for m, d in deltas.items() if d > 0]
                        neg = [(m, d) for m, d in deltas.items() if d < 0]

                if not pos or not neg:
                    # can't form 2-leg swap; skip
                    skipped += 1
                    reason = "no_pos_leg" if not pos else "no_neg_leg"
                    skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                    if len(skipped_examples) < _SKIP_EXAMPLES_MAX:
                        skipped_examples.append({"sig": sig, "reason": reason})
                    continue

            token_meta = _solana_token_meta_from_tx(js)

            canonical = _solana_pick_canonical_swap_legs(deltas, token_meta=token_meta)
            if canonical is not None:
                base_mint, base_qty, quote_mint, quote_qty, side, selection_hint = canonical
                quote_delta = -float(quote_qty)
            else:
                base_mint, base_qty, quote_mint, quote_delta, selection_hint = _solana_pick_base_quote_legs(
                    pos,
                    neg,
                    token_meta=token_meta,
                )
                quote_qty = abs(float(quote_delta))
                side = "buy" if float(deltas.get(base_mint) or 0.0) >= 0 else "sell"

            # Second-pass recovery for Jupiter limit-order settlement fills:
            # if owner-specific / relaxed deltas produced a tiny dust-sized base or quote leg,
            # try strongest per-owner pre/post token-balance deltas across the tx.
            extrema = _solana_pick_base_quote_legs_from_owner_extrema(js, token_meta=token_meta)
            if extrema is not None:
                ex_base_mint, ex_base_qty, ex_quote_mint, ex_quote_delta, ex_hint = extrema
                current_quote_qty = abs(float(quote_delta))
                extrema_quote_qty = abs(float(ex_quote_delta))

                # Promote the owner-extrema candidate when it is materially larger than the current
                # owner-specific result. This preserves existing behavior for normal swaps while
                # fixing mis-reconciled Jupiter settlement fills like UTTT-USDC -> dust.
                if (
                    (float(base_qty) <= 0.0)
                    or (current_quote_qty <= 0.0)
                    or (abs(float(ex_base_qty)) > abs(float(base_qty)) * 1000.0)
                    or (extrema_quote_qty > current_quote_qty * 1000.0)
                ):
                    base_mint, base_qty, quote_mint, quote_delta, selection_hint = extrema
                    quote_qty = abs(float(ex_quote_delta))
                    try:
                        if float(deltas.get(base_mint) or 0.0) < 0:
                            side = "sell"
                        else:
                            side = "buy"
                    except Exception:
                        side = "buy"

            quote_qty = abs(float(quote_delta))

            # Native-SOL recovery for real TOKEN-SOL swaps that temporarily collapse into
            # a fake self-pair before the self-pair guard runs. If both legs resolve to the
            # same non-SOL mint, but the tx still has a material SOL delta, restore SOL as
            # the quote leg and continue through normal materialization.
            try:
                _bm = str(base_mint or "").strip()
                _qm = str(quote_mint or "").strip()
                _sol_delta = float(deltas.get("SOL") or 0.0)
                if _bm and (_bm == _qm) and (_bm != "SOL") and (abs(_sol_delta) > 0.0):
                    quote_mint = "SOL"
                    quote_delta = _sol_delta
                    quote_qty = abs(_sol_delta)
            except Exception:
                pass

            if str(base_mint or "").strip() == str(quote_mint or "").strip():
                skipped += 1
                reason = "solana_self_pair"
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                try:
                    if isinstance(tx.raw, dict):
                        prev_utt = dict((tx.raw or {}).get("utt") or {})
                        prev_utt.update({"complex": True, "skip_ledger_reason": reason})
                        tx.raw = {**tx.raw, "utt": prev_utt}
                        db.add(tx)
                except Exception:
                    pass
                if len(skipped_examples) < _SKIP_EXAMPLES_MAX:
                    skipped_examples.append({"sig": sig, "reason": reason})
                continue

            price = (quote_qty / float(base_qty)) if float(base_qty) != 0 else None

            # Determine side in UTT semantics: buy = spend quote, receive base; sell = spend base, receive quote
            status = "filled"
            typ = "market"

            # Timestamp: try from js.blockTime else tx.tx_time
            ts = None
            try:
                bt = (js.get("result") or {}).get("blockTime")
                if bt is not None:
                    ts = datetime.utcfromtimestamp(int(bt)).isoformat()
            except Exception:
                ts = None
            if not ts and tx.tx_time:
                ts = tx.tx_time.isoformat()

            raw_symbol = f"{base_mint}-{quote_mint}"

            payload = {
                "venue": venue,
                "chain": "solana",
                "wallet_address": addr,
                "signature": sig,
                "raw_symbol": raw_symbol,
                "resolved_symbol": None,
                "side": side,
                "type": typ,
                "status": status,
                "base_qty": float(base_qty),
                "quote_qty": float(quote_qty),
                "price": float(price) if price is not None else None,
                "fee_quote": None,
                "base_mint": base_mint,
                "quote_mint": quote_mint,
                "ts": ts,
                "raw": json.dumps({
                    "tx": raw,
                    "deltas": deltas,
                    "token_meta": token_meta,
                    "selection_hint": selection_hint,
                }),
            }

            db.execute(
                text(
                    """
                    INSERT INTO swap_orders
                    (venue, chain, wallet_address, signature, raw_symbol, resolved_symbol, side, type, status,
                     base_qty, quote_qty, price, fee_quote, base_mint, quote_mint, ts, raw)
                    VALUES
                    (:venue, :chain, :wallet_address, :signature, :raw_symbol, :resolved_symbol, :side, :type, :status,
                     :base_qty, :quote_qty, :price, :fee_quote, :base_mint, :quote_mint, :ts, :raw)
                    ON CONFLICT(venue, signature) DO UPDATE SET
                      chain=excluded.chain,
                      wallet_address=excluded.wallet_address,
                      raw_symbol=excluded.raw_symbol,
                      resolved_symbol=excluded.resolved_symbol,
                      side=excluded.side,
                      type=excluded.type,
                      status=excluded.status,
                      base_qty=excluded.base_qty,
                      quote_qty=excluded.quote_qty,
                      price=excluded.price,
                      fee_quote=excluded.fee_quote,
                      base_mint=excluded.base_mint,
                      quote_mint=excluded.quote_mint,
                      ts=excluded.ts,
                      raw=excluded.raw
                    """
                ),
                payload,
            )
            if exists:
                ignored += 1
                skip_reasons["updated_existing"] = skip_reasons.get("updated_existing", 0) + 1
            else:
                inserted += 1

        except Exception as e:
            errors.append({"signature": sig, "error": str(e)})
            skipped += 1
            skip_reasons["exception"] = skip_reasons.get("exception", 0) + 1
            if len(skipped_examples) < _SKIP_EXAMPLES_MAX:
                skipped_examples.append({"sig": sig, "reason": "exception"})

    db.commit()
    return {
        "ok": True,
        "inserted": inserted,
        "ignored": ignored,
        "skipped": skipped,
        "skip_reasons": skip_reasons,
        "skipped_examples": skipped_examples,
        "errors": errors,
    }
