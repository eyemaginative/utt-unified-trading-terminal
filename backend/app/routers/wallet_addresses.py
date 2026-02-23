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

# Env var names (either works)
_BLOCKCYPHER_TOKEN = os.getenv("BLOCKCYPHER_TOKEN") or os.getenv("UTT_BLOCKCYPHER_TOKEN")


def _norm_asset(x: str) -> str:
    return str(x or "").strip().upper()


def _norm_network(x: str, asset: str) -> str:
    s = str(x or "").strip().lower()
    return s if s else asset.lower()


# ------------------------------------------------------------------------------
# Solana JSON-RPC helpers (public RPC by default)
# ------------------------------------------------------------------------------

def _solana_rpc_url() -> str:
    # Default to public mainnet-beta RPC (free, rate-limited).
    return (os.getenv("SOLANA_RPC_URL") or "https://api.mainnet-beta.solana.com").strip()


async def _solana_rpc(method: str, params: Optional[list] = None) -> Dict:
    url = _solana_rpc_url()
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or []}

    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.post(url, json=payload)
        if r.status_code == 429:
            raise RuntimeError("Solana RPC rate-limited (429). Set SOLANA_RPC_URL to a private RPC.")
        r.raise_for_status()
        js = r.json()

    if (js or {}).get("error"):
        raise RuntimeError(f"Solana RPC error: {(js or {}).get('error')}")
    return js


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
    if _BLOCKCYPHER_TOKEN:
        params["token"] = _BLOCKCYPHER_TOKEN

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

# NOTE: _BLOCKCYPHER_CHAIN + _BLOCKCYPHER_TOKEN are defined above (used for balances too)


async def _blockcypher_get_json(path: str, params: Optional[Dict] = None) -> Dict:
    """GET JSON from BlockCypher; raises with detail on failure."""
    base = "https://api.blockcypher.com"
    url = f"{base}{path}"

    q = dict(params or {})
    if _BLOCKCYPHER_TOKEN:
        q.setdefault("token", _BLOCKCYPHER_TOKEN)

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

            if not payload.write_ledger:
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

