# backend/app/services/market_metrics.py
from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


DEFAULT_ASSETS = ("UTTT", "HDX", "DOT", "SOL", "DOGE", "BTC", "ETH")

COINGECKO_BY_SYMBOL: Dict[str, Dict[str, str]] = {
    "BTC": {"id": "bitcoin", "name": "Bitcoin", "chain": "bitcoin"},
    "ETH": {"id": "ethereum", "name": "Ethereum", "chain": "ethereum"},
    "SOL": {"id": "solana", "name": "Solana", "chain": "solana"},
    "DOT": {"id": "polkadot", "name": "Polkadot", "chain": "polkadot"},
    "DOGE": {"id": "dogecoin", "name": "Dogecoin", "chain": "dogecoin"},
    "HDX": {"id": "hydradx", "name": "HydraDX", "chain": "hydration"},
    "BCH": {"id": "bitcoin-cash", "name": "Bitcoin Cash", "chain": "bitcoin_cash"},
    "LTC": {"id": "litecoin", "name": "Litecoin", "chain": "litecoin"},
    "XRP": {"id": "ripple", "name": "XRP", "chain": "xrp"},
    "ADA": {"id": "cardano", "name": "Cardano", "chain": "cardano"},
    "AVAX": {"id": "avalanche-2", "name": "Avalanche", "chain": "avalanche"},
    "LINK": {"id": "chainlink", "name": "Chainlink", "chain": "ethereum"},
    "UNI": {"id": "uniswap", "name": "Uniswap", "chain": "ethereum"},
    "AAVE": {"id": "aave", "name": "Aave", "chain": "ethereum"},
    "ATOM": {"id": "cosmos", "name": "Cosmos Hub", "chain": "cosmos"},
    "XLM": {"id": "stellar", "name": "Stellar", "chain": "stellar"},
    "ETC": {"id": "ethereum-classic", "name": "Ethereum Classic", "chain": "ethereum_classic"},
    "FIL": {"id": "filecoin", "name": "Filecoin", "chain": "filecoin"},
    "TRX": {"id": "tron", "name": "TRON", "chain": "tron"},
    "SHIB": {"id": "shiba-inu", "name": "Shiba Inu", "chain": "ethereum"},
    "PEPE": {"id": "pepe", "name": "Pepe", "chain": "ethereum"},
    "BONK": {"id": "bonk", "name": "Bonk", "chain": "solana"},
    "SUI": {"id": "sui", "name": "Sui", "chain": "sui"},
    "HBAR": {"id": "hedera-hashgraph", "name": "Hedera", "chain": "hedera"},
    "NEAR": {"id": "near", "name": "NEAR Protocol", "chain": "near"},
    "APT": {"id": "aptos", "name": "Aptos", "chain": "aptos"},
    "ARB": {"id": "arbitrum", "name": "Arbitrum", "chain": "arbitrum"},
    "OP": {"id": "optimism", "name": "Optimism", "chain": "optimism"},
    "ICP": {"id": "internet-computer", "name": "Internet Computer", "chain": "internet_computer"},
    "INJ": {"id": "injective-protocol", "name": "Injective", "chain": "injective"},
    "MKR": {"id": "maker", "name": "Maker", "chain": "ethereum"},
    "CRV": {"id": "curve-dao-token", "name": "Curve DAO", "chain": "ethereum"},
    "COMP": {"id": "compound-governance-token", "name": "Compound", "chain": "ethereum"},
    "XTZ": {"id": "tezos", "name": "Tezos", "chain": "tezos"},
    "ALGO": {"id": "algorand", "name": "Algorand", "chain": "algorand"},
    "DASH": {"id": "dash", "name": "Dash", "chain": "dash"},
    "ZEC": {"id": "zcash", "name": "Zcash", "chain": "zcash"},
    "EOS": {"id": "eos", "name": "EOS", "chain": "eos"},
    "MANA": {"id": "decentraland", "name": "Decentraland", "chain": "ethereum"},
    "SAND": {"id": "the-sandbox", "name": "The Sandbox", "chain": "ethereum"},
    "USDC": {"id": "usd-coin", "name": "USD Coin", "chain": "multi"},
    "USDT": {"id": "tether", "name": "Tether", "chain": "multi"},
}

_CACHE: Dict[str, Dict[str, Any]] = {}

# CoinGecko raw-market cache.
# The summary cache is keyed by the exact asset list, but the raw cache is keyed
# by CoinGecko ID so one successful fetch can serve the AppHeader chip, MarketCap
# window, and Volume window without repeatedly hitting CoinGecko.
_CG_RAW_BY_ID: Dict[str, Dict[str, Any]] = {}
_CG_RAW_CACHE_LOADED = False
_CG_BACKOFF_UNTIL = 0.0

# Token Registry external price metadata cache.
# Token Registry is the preferred resolver for market metrics when a row has:
#   external_price_source = coingecko / coingecko_simple / blank
#   external_price_id     = CoinGecko coin id
# The hardcoded COINGECKO_BY_SYMBOL map remains only as a local bootstrap fallback.
_TOKEN_REGISTRY_CG_META_BY_SYMBOL: Dict[str, Dict[str, str]] = {}
_TOKEN_REGISTRY_CG_META_LOADED_AT = 0.0

# CoinGecko symbol discovery cache.
# This lets held CEX assets such as MATH resolve automatically from CoinGecko
# when Token Registry has no explicit external_price_id yet.  It is deliberately
# used only for explicit/selected symbols, not for broad owned-asset filtering.
_CG_SYMBOL_META_BY_SYMBOL: Dict[str, Dict[str, str]] = {}
_CG_SYMBOL_NEGATIVE_CACHE: Dict[str, float] = {}
_CG_SYMBOL_META_LOADED = False
_CG_SYMBOL_DISCOVERY_BACKOFF_UNTIL = 0.0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _num(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        n = float(v)
    except Exception:
        return None
    return n if n == n and n not in (float("inf"), float("-inf")) else None


def _clamp_int(v: Any, lo: int, hi: int, default: int) -> int:
    try:
        n = int(v)
    except Exception:
        n = default
    return max(lo, min(hi, n))


def _parse_assets(assets: Optional[Any]) -> List[str]:
    if assets is None:
        return list(DEFAULT_ASSETS)

    if isinstance(assets, str):
        parts = assets.replace(";", ",").split(",")
    elif isinstance(assets, Iterable):
        parts = []
        for item in assets:
            parts.extend(str(item or "").replace(";", ",").split(","))
    else:
        parts = [str(assets)]

    out: List[str] = []
    seen = set()
    for p in parts:
        s = str(p or "").strip().upper()
        if not s:
            continue
        if s in seen:
            continue
        seen.add(s)
        out.append(s)

    return out or list(DEFAULT_ASSETS)


# "owned" means current/likely current holdings.
# "db/known/tracked" means the broader local market universe the app knows about.
# PORT-METRICS.1 needs these split so owned/unowned rows are not both sourced from
# the same owned-only set.
_OWNED_ASSET_SENTINELS = {"OWNED", "DB_OWNED", "HELD"}
_KNOWN_ASSET_SENTINELS = {"DB", "KNOWN", "TRACKED", "DB_KNOWN", "ALL_DB"}
_METRIC_ASSET_EXCLUDE = {"", "USD", "FIAT"}


def _is_owned_asset_request(assets: Optional[Any]) -> bool:
    if not isinstance(assets, str):
        return False
    return assets.strip().upper() in _OWNED_ASSET_SENTINELS


def _is_known_asset_request(assets: Optional[Any]) -> bool:
    if not isinstance(assets, str):
        return False
    return assets.strip().upper() in _KNOWN_ASSET_SENTINELS


def _clean_asset_symbol(value: Any) -> str:
    s = str(value or "").strip().upper()
    if not s:
        return ""
    s = s.replace("\x00", "").strip()
    allowed = []
    for ch in s:
        if ch.isalnum() or ch in {"-", "_", "/", "."}:
            allowed.append(ch)
    return "".join(allowed).strip()


def _asset_symbols_from_value(value: Any) -> List[str]:
    """Extract likely asset symbols from asset columns or canonical symbols.

    Examples:
      DOGE-USD -> DOGE
      UTTT-HDX -> UTTT, HDX
      BCHUSD   -> BCH
    """
    raw = _clean_asset_symbol(value)
    if not raw:
        return []

    stable_quotes = {"USD", "USDT", "USDC", "DAI", "TUSD", "GUSD", "PYUSD", "USDP", "FDUSD"}

    if "-" in raw or "/" in raw or "_" in raw:
        parts = [p for p in raw.replace("/", "-").replace("_", "-").split("-") if p]
        out: List[str] = []
        for p in parts:
            if p in stable_quotes:
                continue
            if p not in _METRIC_ASSET_EXCLUDE and p not in out:
                out.append(p)
        return out

    # Compact venue symbols such as DOGEUSD/BCHUSD.
    suffixes = ["USDT", "USDC", "USD", "DAI", "HDX", "SOL", "DOT", "BTC", "ETH", "DOGE"]
    for suffix in suffixes:
        if raw.endswith(suffix) and len(raw) > len(suffix):
            base = raw[: -len(suffix)]
            out = []
            if base and base not in _METRIC_ASSET_EXCLUDE:
                out.append(base)
            if suffix not in stable_quotes and suffix not in _METRIC_ASSET_EXCLUDE:
                out.append(suffix)
            return out

    return [] if raw in _METRIC_ASSET_EXCLUDE else [raw]


def _merge_assets(*groups: Iterable[Any]) -> List[str]:
    out: List[str] = []
    seen = set()
    for group in groups:
        for item in group or []:
            for sym in _asset_symbols_from_value(item):
                if not sym or sym in seen:
                    continue
                seen.add(sym)
                out.append(sym)
    return out


def _db_table_columns(db: Any, table_name: str) -> set:
    try:
        from sqlalchemy import text

        rows = db.execute(text(f'PRAGMA table_info("{table_name}")')).mappings().all()
        return {str(r.get("name") or "") for r in rows or []}
    except Exception:
        return set()


def _db_scalar_values(
    db: Any,
    *,
    table_name: str,
    column_name: str,
    where_sql: str = "",
    params: Optional[Dict[str, Any]] = None,
    limit: int = 500,
) -> List[str]:
    cols = _db_table_columns(db, table_name)
    if column_name not in cols:
        return []
    try:
        from sqlalchemy import text

        sql = f'SELECT DISTINCT "{column_name}" AS v FROM "{table_name}" WHERE "{column_name}" IS NOT NULL AND TRIM("{column_name}") <> ""'
        if where_sql:
            sql += f" AND ({where_sql})"
        sql += " LIMIT :limit"
        rows = db.execute(text(sql), {**(params or {}), "limit": int(limit)}).mappings().all()
        return [str(r.get("v") or "").strip() for r in rows if str(r.get("v") or "").strip()]
    except Exception:
        return []


def _db_owned_query_limit(default: int = 1000) -> int:
    raw = _env_first("UTT_MARKET_METRICS_DB_QUERY_LIMIT", "MARKET_METRICS_DB_QUERY_LIMIT")
    try:
        n = int(float(raw)) if str(raw or "").strip() else int(default)
    except Exception:
        n = int(default)
    return max(50, min(5000, n))


def _db_owned_asset_cap(default: int = 1000) -> int:
    raw = _env_first("UTT_MARKET_METRICS_OWNED_ASSET_CAP", "MARKET_METRICS_OWNED_ASSET_CAP")
    try:
        n = int(float(raw)) if str(raw or "").strip() else int(default)
    except Exception:
        n = int(default)
    return max(5, min(1000, n))


def _db_known_asset_cap(default: int = 1000) -> int:
    """Maximum local known/tracked symbols surfaced to market metric windows.

    Defaults above the old 250-row route ceiling so each window can show the
    full local venue/token-registry universe available to one response instead
    of truncating to a small discovery subset.
    """
    raw = _env_first("UTT_MARKET_METRICS_KNOWN_ASSET_CAP", "MARKET_METRICS_KNOWN_ASSET_CAP")
    try:
        n = int(float(raw)) if str(raw or "").strip() else int(default)
    except Exception:
        n = int(default)
    return max(10, min(1000, n))


def _db_scalar_values_recent(
    db: Any,
    *,
    table_name: str,
    column_name: str,
    where_sql: str = "",
    params: Optional[Dict[str, Any]] = None,
    order_column: str = "",
    limit: int = 1000,
) -> List[str]:
    cols = _db_table_columns(db, table_name)
    if column_name not in cols:
        return []
    try:
        from sqlalchemy import text

        sql = f'SELECT "{column_name}" AS v FROM "{table_name}" WHERE "{column_name}" IS NOT NULL AND TRIM(CAST("{column_name}" AS TEXT)) <> ""'
        if where_sql:
            sql += f" AND ({where_sql})"
        if order_column and order_column in cols:
            sql += f' ORDER BY "{order_column}" DESC'
        sql += " LIMIT :limit"
        rows = db.execute(text(sql), {**(params or {}), "limit": int(limit)}).mappings().all()
        return [str(r.get("v") or "").strip() for r in rows if str(r.get("v") or "").strip()]
    except Exception:
        return []


def _db_owned_metric_assets(limit: int = 1000) -> Tuple[List[str], str]:
    """Fast local DB asset discovery for MarketCap/Volume windows.

    Keep this intentionally conservative:
      - true/non-zero holdings from balance snapshots
      - open basis lots
      - positive/manual deposit inventory
      - tracked wallet addresses and non-zero wallet snapshots
      - Token Registry only as fallback

    Avoid scanning all historical orders/fills/venue order rows here. Those tables
    can be large and represent *traded/listed* assets, not necessarily currently
    owned assets, and they can make the window request look stuck.
    """
    eps = _num(_env_first("UTT_MARKET_METRICS_OWNED_EPS", "MARKET_METRICS_OWNED_EPS"))
    if eps is None:
        eps = 1e-12

    asset_cap = min(max(1, int(limit)), _db_owned_asset_cap(1000))
    query_limit = _db_owned_query_limit(1000)

    try:
        from ..db import SessionLocal
    except Exception:
        return [], "db_unavailable"

    db = SessionLocal()
    try:
        raw_assets: List[str] = []

        # Balance snapshots are the primary source. Query recent rows only and
        # de-dupe in Python so SQLite never has to DISTINCT/order a large table.
        raw_assets.extend(
            _db_scalar_values_recent(
                db,
                table_name="balance_snapshots",
                column_name="asset",
                where_sql='ABS(COALESCE("total", 0)) > :eps OR ABS(COALESCE("available", 0)) > :eps OR ABS(COALESCE("hold", 0)) > :eps',
                params={"eps": float(eps)},
                order_column="captured_at",
                limit=query_limit,
            )
        )

        # Open cost-basis inventory.
        raw_assets.extend(
            _db_scalar_values_recent(
                db,
                table_name="basis_lots",
                column_name="asset",
                where_sql='ABS(COALESCE("qty_remaining", 0)) > :eps',
                params={"eps": float(eps)},
                order_column="acquired_at",
                limit=query_limit,
            )
        )

        # Deposits can represent opening inventory. Withdrawals are intentionally
        # not included as an ownership source.
        raw_assets.extend(
            _db_scalar_values_recent(
                db,
                table_name="asset_deposits",
                column_name="asset",
                where_sql='ABS(COALESCE("qty", 0)) > :eps AND COALESCE(UPPER("status"), \'\') <> \'IGNORED\'',
                params={"eps": float(eps)},
                order_column="deposit_time",
                limit=query_limit,
            )
        )

        # On-chain wallet tracking: explicit tracked assets plus recent non-zero snapshots.
        raw_assets.extend(
            _db_scalar_values_recent(
                db,
                table_name="wallet_addresses",
                column_name="asset",
                order_column="created_at",
                limit=query_limit,
            )
        )
        raw_assets.extend(
            _db_scalar_values_recent(
                db,
                table_name="wallet_address_snapshots",
                column_name="asset",
                where_sql='ABS(COALESCE("balance_qty", 0)) > :eps',
                params={"eps": float(eps)},
                order_column="fetched_at",
                limit=query_limit,
            )
        )

        assets = _merge_assets(raw_assets)
        if assets:
            return assets[:asset_cap], "db_owned"

        # Last local fallback: token registry. This is user-managed and safer
        # than a backend default list when no current owned rows exist.
        registry_assets = _db_scalar_values_recent(
            db,
            table_name="token_registry",
            column_name="symbol",
            order_column="updated_at",
            limit=query_limit,
        )
        assets = _merge_assets(registry_assets)
        if assets:
            return assets[:asset_cap], "token_registry_fallback"

        return [], "db_empty"
    except Exception:
        return [], "db_error"
    finally:
        try:
            db.close()
        except Exception:
            pass



def _metric_context_blank() -> Dict[str, Any]:
    return {
        "owned": False,
        "owned_venues": set(),
        "tracked_venues": set(),
        "chains": set(),
        "sources": set(),
    }


def _metric_context_add(
    ctx: Dict[str, Dict[str, Any]],
    value: Any,
    *,
    venue: Optional[Any] = None,
    chain: Optional[Any] = None,
    source: Optional[str] = None,
    owned: bool = False,
) -> None:
    venue_s = str(venue or "").strip().lower()
    chain_s = str(chain or "").strip().lower()
    source_s = str(source or "").strip().lower()

    for sym in _asset_symbols_from_value(value):
        if not sym or sym in _METRIC_ASSET_EXCLUDE:
            continue

        row = ctx.setdefault(sym, _metric_context_blank())
        if owned:
            row["owned"] = True

        if venue_s:
            if owned:
                row["owned_venues"].add(venue_s)
            row["tracked_venues"].add(venue_s)

        # Chain is informational.  Only promote DEX-like chains into the filter
        # universe when there is no explicit venue id available.
        if chain_s:
            row["chains"].add(chain_s)
            if chain_s in {"hydration", "polkadot_hydration", "solana", "jupiter", "raydium"} and not venue_s:
                row["tracked_venues"].add(chain_s)

        if source_s:
            row["sources"].add(source_s)


def _db_metric_rows(
    db: Any,
    *,
    table_name: str,
    column_names: Sequence[str],
    where_sql: str = "",
    params: Optional[Dict[str, Any]] = None,
    order_column: str = "",
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    cols = _db_table_columns(db, table_name)
    selected = [c for c in column_names if c in cols]
    if not selected:
        return []

    try:
        from sqlalchemy import text

        col_sql = ", ".join(f'"{c}"' for c in selected)
        sql = f'SELECT {col_sql} FROM "{table_name}" WHERE 1=1'
        if where_sql:
            sql += f" AND ({where_sql})"
        if order_column and order_column in cols:
            sql += f' ORDER BY "{order_column}" DESC'
        sql += " LIMIT :limit"
        rows = db.execute(text(sql), {**(params or {}), "limit": int(limit)}).mappings().all()
        return [dict(r) for r in rows or []]
    except Exception:
        return []


def _db_latest_venue_symbol_rows(db: Any) -> List[Dict[str, Any]]:
    """Return the latest active listing snapshot for every venue.

    venue_symbols is append-only. A single global ORDER BY captured_at DESC
    LIMIT can let one recently refreshed venue consume the entire context query,
    which makes older Coinbase/Gemini/Crypto.com/Robinhood snapshots disappear
    from Market Cap / Volume venue filters. Read each venue's latest snapshot
    instead, while keeping the query read-only and schema-tolerant.
    """
    cols = _db_table_columns(db, "venue_symbols")
    required = {"venue", "captured_at"}
    if not required.issubset(cols):
        return _db_metric_rows(
            db,
            table_name="venue_symbols",
            column_names=("base_asset", "quote_asset", "symbol_canon", "venue", "captured_at", "is_active"),
            order_column="captured_at",
            limit=_db_owned_query_limit(1000),
        )

    selected = [
        c
        for c in ("base_asset", "quote_asset", "symbol_canon", "venue", "captured_at", "is_active")
        if c in cols
    ]
    if not selected:
        return []

    raw_cap = _env_first(
        "UTT_MARKET_METRICS_VENUE_SYMBOL_SNAPSHOT_ROW_CAP",
        "MARKET_METRICS_VENUE_SYMBOL_SNAPSHOT_ROW_CAP",
    )
    try:
        row_cap = int(float(raw_cap)) if str(raw_cap or "").strip() else 20000
    except Exception:
        row_cap = 20000
    row_cap = max(1000, min(100000, row_cap))

    try:
        from sqlalchemy import text

        select_sql = ", ".join(f'v."{c}" AS "{c}"' for c in selected)
        active_sql = ' AND COALESCE(v."is_active", 1) = 1' if "is_active" in cols else ""
        order_parts = ['LOWER(TRIM(CAST(v."venue" AS TEXT)))']
        if "base_asset" in cols:
            order_parts.append('UPPER(TRIM(CAST(v."base_asset" AS TEXT)))')
        if "quote_asset" in cols:
            order_parts.append('UPPER(TRIM(CAST(v."quote_asset" AS TEXT)))')
        if "symbol_canon" in cols:
            order_parts.append('UPPER(TRIM(CAST(v."symbol_canon" AS TEXT)))')
        order_sql = ", ".join(order_parts)

        sql = f"""
            WITH latest AS (
                SELECT
                    LOWER(TRIM(CAST("venue" AS TEXT))) AS venue_key,
                    MAX("captured_at") AS latest_captured_at
                FROM "venue_symbols"
                WHERE "venue" IS NOT NULL
                  AND TRIM(CAST("venue" AS TEXT)) <> ''
                  AND "captured_at" IS NOT NULL
                GROUP BY LOWER(TRIM(CAST("venue" AS TEXT)))
            )
            SELECT {select_sql}
            FROM "venue_symbols" v
            JOIN latest l
              ON LOWER(TRIM(CAST(v."venue" AS TEXT))) = l.venue_key
             AND v."captured_at" = l.latest_captured_at
            WHERE 1=1{active_sql}
            ORDER BY {order_sql}
            LIMIT :row_cap
        """
        rows = db.execute(text(sql), {"row_cap": int(row_cap)}).mappings().all()
        return [dict(r) for r in rows or []]
    except Exception:
        return []


def _db_market_metric_asset_context(limit: int = 1000) -> Dict[str, Dict[str, Any]]:
    """Return local asset context for owned/tracked source filtering.

    This is read-only and deliberately bounded.  It does not mutate balances,
    token registry, ledger, or basis lots.
    """
    eps = _num(_env_first("UTT_MARKET_METRICS_OWNED_EPS", "MARKET_METRICS_OWNED_EPS"))
    if eps is None:
        eps = 1e-12

    query_limit = _db_owned_query_limit(1000)
    try:
        from ..db import SessionLocal
    except Exception:
        return {}

    ctx: Dict[str, Dict[str, Any]] = {}
    db = SessionLocal()
    try:
        for r in _db_metric_rows(
            db,
            table_name="balance_snapshots",
            column_names=("asset", "venue", "captured_at", "total", "available", "hold"),
            where_sql='ABS(COALESCE("total", 0)) > :eps OR ABS(COALESCE("available", 0)) > :eps OR ABS(COALESCE("hold", 0)) > :eps',
            params={"eps": float(eps)},
            order_column="captured_at",
            limit=query_limit,
        ):
            _metric_context_add(ctx, r.get("asset"), venue=r.get("venue"), source="balance_snapshots", owned=True)

        for r in _db_metric_rows(
            db,
            table_name="basis_lots",
            column_names=("asset", "venue", "qty_remaining", "acquired_at"),
            where_sql='ABS(COALESCE("qty_remaining", 0)) > :eps',
            params={"eps": float(eps)},
            order_column="acquired_at",
            limit=query_limit,
        ):
            _metric_context_add(ctx, r.get("asset"), venue=r.get("venue"), source="basis_lots", owned=True)

        for r in _db_metric_rows(
            db,
            table_name="asset_deposits",
            column_names=("asset", "venue", "qty", "status", "deposit_time"),
            where_sql='ABS(COALESCE("qty", 0)) > :eps AND COALESCE(UPPER("status"), \'\') <> \'IGNORED\'',
            params={"eps": float(eps)},
            order_column="deposit_time",
            limit=query_limit,
        ):
            _metric_context_add(ctx, r.get("asset"), venue=r.get("venue"), source="asset_deposits", owned=True)

        for r in _db_metric_rows(
            db,
            table_name="wallet_addresses",
            column_names=("asset", "network", "wallet_id", "created_at"),
            order_column="created_at",
            limit=query_limit,
        ):
            wallet_id = str(r.get("wallet_id") or "").strip().lower()
            venue = wallet_id or "self_custody"
            _metric_context_add(ctx, r.get("asset"), venue=venue, chain=r.get("network"), source="wallet_addresses", owned=True)

        for r in _db_metric_rows(
            db,
            table_name="wallet_address_snapshots",
            column_names=("asset", "network", "balance_qty", "fetched_at"),
            where_sql='ABS(COALESCE("balance_qty", 0)) > :eps',
            params={"eps": float(eps)},
            order_column="fetched_at",
            limit=query_limit,
        ):
            _metric_context_add(ctx, r.get("asset"), venue="self_custody", chain=r.get("network"), source="wallet_address_snapshots", owned=True)

        for r in _db_metric_rows(
            db,
            table_name="token_registry",
            column_names=("symbol", "chain", "venue", "updated_at"),
            order_column="updated_at",
            limit=query_limit,
        ):
            _metric_context_add(ctx, r.get("symbol"), venue=r.get("venue"), chain=r.get("chain"), source="token_registry", owned=False)

        # venue_symbols is append-only. Read the latest complete snapshot for
        # every venue instead of applying one global recent-row limit.
        for r in _db_latest_venue_symbol_rows(db):
            venue = r.get("venue")
            _metric_context_add(ctx, r.get("base_asset") or r.get("symbol_canon"), venue=venue, source="venue_symbols", owned=False)
            _metric_context_add(ctx, r.get("quote_asset"), venue=venue, source="venue_symbols", owned=False)

    except Exception:
        return ctx
    finally:
        try:
            db.close()
        except Exception:
            pass

    return ctx


def _db_known_metric_assets(limit: int = 1000) -> Tuple[List[str], str]:
    """Local DB universe for owned + unowned market metric rows."""
    asset_cap = min(max(1, int(limit)), _db_known_asset_cap(1000))
    ctx = _db_market_metric_asset_context(limit=limit)
    owned_assets, owned_source = _db_owned_metric_assets(limit=limit)

    assets = _merge_assets(owned_assets, ctx.keys())
    if assets:
        return assets[:asset_cap], "db_known"

    if owned_assets:
        return owned_assets[:asset_cap], owned_source

    return [], "db_known_empty"


def _metric_context_public(ctx: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "owned": bool(ctx.get("owned")),
        "owned_venues": sorted(str(x) for x in (ctx.get("owned_venues") or set()) if str(x or "").strip()),
        "tracked_venues": sorted(str(x) for x in (ctx.get("tracked_venues") or set()) if str(x or "").strip()),
        "chains": sorted(str(x) for x in (ctx.get("chains") or set()) if str(x or "").strip()),
        "sources": sorted(str(x) for x in (ctx.get("sources") or set()) if str(x or "").strip()),
    }


def _venue_filter_options_from_context(asset_context: Dict[str, Dict[str, Any]]) -> List[str]:
    """Return stable venue/source filter keys from current local asset context.

    This is read-only. It gives Market Cap / Volume windows a compact source
    option list even when the broad summary itself is loaded from disk cache.
    """
    keys: List[str] = []
    seen = set()

    def add(value: Any) -> None:
        s = str(value or "").strip().lower()
        if not s or s in {"global", "unknown", "none", "n/a", "na", "all"}:
            return
        if s == "self-custody" or s == "selfcustody":
            s = "self_custody"
        elif s in {"crypto_com", "crypto.com", "crypto-com"}:
            s = "cryptocom"
        elif s == "dex-trade":
            s = "dex_trade"
        elif s in {"polkadot_hydration", "hydration_dex"}:
            s = "hydration"
        elif s in {"solana_jupiter", "jupiter", "raydium", "solana_dex"}:
            s = "solana"
        if s and s not in seen:
            seen.add(s)
            keys.append(s)

    for ctx in (asset_context or {}).values():
        if not isinstance(ctx, dict):
            continue
        for group_name in ("owned_venues", "tracked_venues"):
            for v in (ctx.get(group_name) or set()):
                add(v)

    return sorted(keys)


def _venue_asset_counts_from_context(asset_context: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    counts: Dict[str, Dict[str, int]] = {}
    for ctx in (asset_context or {}).values():
        if not isinstance(ctx, dict):
            continue
        tracked = {str(v or "").strip().lower() for v in (ctx.get("tracked_venues") or set()) if str(v or "").strip()}
        owned = {str(v or "").strip().lower() for v in (ctx.get("owned_venues") or set()) if str(v or "").strip()}
        for venue in tracked | owned:
            row = counts.setdefault(venue, {"tracked": 0, "owned": 0, "unowned": 0})
            if venue in tracked:
                row["tracked"] += 1
            if venue in owned:
                row["owned"] += 1
    for row in counts.values():
        row["unowned"] = max(0, int(row.get("tracked", 0)) - int(row.get("owned", 0)))
    return {k: counts[k] for k in sorted(counts)}


def _summary_snapshot_refresh_current_context(
    payload: Dict[str, Any],
    *,
    limit: int,
) -> Dict[str, Any]:
    """Re-annotate a cached summary with current local owned/source context.

    The summary disk cache intentionally avoids live CoinGecko work on window
    open. However, local venues/balances can change after the snapshot is saved.
    Re-reading only local DB context keeps the window fast while letting new
    venues such as OKX appear in source filters immediately after balances are
    refreshed.
    """
    if not isinstance(payload, dict):
        return payload

    try:
        asset_context = _db_market_metric_asset_context(limit=limit)
        owned_assets_raw, owned_asset_source = _db_owned_metric_assets(limit=limit)
        owned_assets_payload = _merge_assets(owned_assets_raw)

        items = payload.get("items")
        if isinstance(items, list):
            payload["items"] = _annotate_market_metric_rows(
                [dict(r) for r in items if isinstance(r, dict)],
                asset_context=asset_context,
                owned_assets=owned_assets_payload,
            )

        payload["owned_assets"] = owned_assets_payload
        payload["owned_asset_count"] = len(owned_assets_payload)
        payload["owned_asset_source"] = owned_asset_source
        payload["asset_context"] = {k: _metric_context_public(v) for k, v in sorted(asset_context.items())}
        payload["venue_filter_options"] = _venue_filter_options_from_context(asset_context)
        payload["venue_asset_counts"] = _venue_asset_counts_from_context(asset_context)
        payload["venue_symbol_context_mode"] = "latest_snapshot_per_venue"
        payload["summary_snapshot_context_refreshed"] = True
    except Exception:
        # Snapshot context refresh is best-effort; never block window open.
        payload["summary_snapshot_context_refreshed"] = False

    return payload


def _annotate_market_metric_rows(
    items: List[Dict[str, Any]],
    *,
    asset_context: Dict[str, Dict[str, Any]],
    owned_assets: Sequence[str],
) -> List[Dict[str, Any]]:
    owned_set = set(_merge_assets(owned_assets))
    out: List[Dict[str, Any]] = []

    for row in items or []:
        if not isinstance(row, dict):
            continue
        next_row = dict(row)
        sym_list = _merge_assets([next_row.get("asset") or next_row.get("symbol") or next_row.get("pair")])
        sym = sym_list[0] if sym_list else str(next_row.get("asset") or "").strip().upper()
        ctx = asset_context.get(sym, _metric_context_blank())
        public_ctx = _metric_context_public(ctx)

        is_owned = bool(public_ctx.get("owned")) or (sym in owned_set)
        next_row["is_owned"] = is_owned
        next_row["owned_venues"] = public_ctx["owned_venues"]
        next_row["tracked_venues"] = public_ctx["tracked_venues"]
        next_row["chains"] = public_ctx["chains"]
        next_row["asset_context_sources"] = public_ctx["sources"]

        venue_keys: List[str] = []
        for group in (public_ctx["owned_venues"], public_ctx["tracked_venues"]):
            for v in group:
                vv = str(v or "").strip().lower()
                if vv and vv not in venue_keys:
                    venue_keys.append(vv)

        # Preserve real venue/dex rows if a future backend adds them; do not
        # promote CoinGecko IDs into venue filters.
        for v in (next_row.get("dex"), next_row.get("venue")):
            vv = str(v or "").strip().lower()
            if vv and vv not in {"global", "unknown", "none", "n/a", "na"} and vv not in venue_keys:
                venue_keys.append(vv)

        next_row["venue_filter_keys"] = venue_keys
        out.append(next_row)

    return out

def _env_first(*keys: str) -> str:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.getenv(key)
    if raw is None:
        return bool(default)
    s = str(raw).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _cg_api_key() -> str:
    return _env_first("COINGECKO_DEMO_API_KEY", "CG_DEMO_API_KEY", "COINGECKO_API_KEY", "CG_API_KEY")


def _cg_backoff_s() -> int:
    return _clamp_int(_env_first("UTT_MARKET_METRICS_CG_BACKOFF_S"), 30, 1800, 300)


def _cg_live_id_cap() -> int:
    """Max CoinGecko IDs to fetch live during one market-metrics refresh.

    The local row universe may be much larger than CoinGecko's practical
    per-request market endpoint size.  Keep live fetches bounded so the window
    can still display all owned/tracked assets as rows while filling live market
    data for a safe subset and using cache/placeholders for the rest.
    """
    return _clamp_int(_env_first("UTT_MARKET_METRICS_LIVE_CG_ID_CAP"), 1, 250, 250)


def _cg_market_page_cap() -> int:
    """Number of CoinGecko market-cap pages to cache during manual refresh.

    This is a small, bounded page sweep used to populate market data for broad
    local universes without doing one /search request per token symbol.
    """
    return _clamp_int(_env_first("UTT_MARKET_METRICS_CG_MARKET_PAGE_CAP"), 0, 10, 4)


def _cg_market_page_per_page() -> int:
    return _clamp_int(_env_first("UTT_MARKET_METRICS_CG_MARKET_PAGE_SIZE"), 50, 250, 250)


def _market_row_symbol(row: Dict[str, Any]) -> str:
    return str(row.get("symbol") or "").strip().upper()


def _market_row_rank_key(row: Dict[str, Any]) -> Tuple[int, int, str]:
    rank_raw = row.get("market_cap_rank")
    try:
        rank = int(rank_raw) if rank_raw is not None else 999999
    except Exception:
        rank = 999999
    market_cap = _num(row.get("market_cap")) or 0.0
    coin_id = str(row.get("id") or "").strip().lower()
    # Lower rank is better; for unranked rows, higher market cap wins.
    return (rank, int(-market_cap), coin_id)


def _cg_rows_by_symbol(rows: Sequence[Dict[str, Any]], assets: Sequence[Any]) -> Dict[str, Dict[str, Any]]:
    wanted = {str(a or "").strip().upper() for a in _merge_assets(assets)}
    if not wanted:
        return {}

    out: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        sym = _market_row_symbol(row)
        if not sym or sym not in wanted:
            continue
        prev = out.get(sym)
        if prev is None or _market_row_rank_key(row) < _market_row_rank_key(prev):
            next_row = dict(row)
            next_row["_utt_symbol_match"] = True
            next_row["_utt_symbol_match_symbol"] = sym
            out[sym] = next_row
    return out


def _cg_raw_cache_symbol_rows(
    assets: Sequence[Any],
    *,
    max_age_s: int,
    allow_stale: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """Return cached CoinGecko market rows matched by ticker symbol.

    This lets broad Market Cap / Volume windows show market data for assets that
    do not yet have explicit Token Registry external_price_id mappings.  Rows are
    matched by CoinGecko ticker symbol and ranked by market_cap_rank, so exact
    Token Registry mappings remain preferred when available.
    """
    _cg_raw_cache_load_once()

    wanted = {str(a or "").strip().upper() for a in _merge_assets(assets)}
    if not wanted:
        return {}

    now = time.time()
    candidates: List[Dict[str, Any]] = []
    for rec in list(_CG_RAW_BY_ID.values()):
        if not isinstance(rec, dict):
            continue
        row = rec.get("row")
        cached_at = _num(rec.get("cached_at"))
        if not isinstance(row, dict) or cached_at is None:
            continue

        sym = _market_row_symbol(row)
        if not sym or sym not in wanted:
            continue

        age_s = max(0.0, now - float(cached_at))
        if not allow_stale and age_s > max(10, int(max_age_s)):
            continue

        next_row = dict(row)
        if age_s > max(10, int(max_age_s)):
            next_row["_utt_cache_stale"] = True
            next_row["_utt_cache_age_s"] = age_s
        next_row["_utt_symbol_match"] = True
        next_row["_utt_symbol_match_symbol"] = sym
        next_row["_utt_symbol_match_source"] = "coingecko_market_cache"
        candidates.append(next_row)

    return _cg_rows_by_symbol(candidates, list(wanted))


def _summary_error_ttl_s() -> int:
    return _clamp_int(_env_first("UTT_MARKET_METRICS_ERROR_TTL_S"), 10, 300, 30)


def _summary_snapshot_cache_enabled() -> bool:
    """Whether broad market-metrics windows may load the last full summary from disk.

    This makes Market Cap / Volume windows open immediately from the last good
    asset/metric snapshot. Manual Refresh still rebuilds local rows and refreshes
    live market data.
    """
    return _env_bool("UTT_MARKET_METRICS_SUMMARY_SNAPSHOT_CACHE", True)


def _summary_snapshot_max_age_s() -> int:
    return _clamp_int(_env_first("UTT_MARKET_METRICS_SUMMARY_SNAPSHOT_MAX_AGE_S"), 60, 604800, 86400)


def _summary_snapshot_cache_file() -> Path:
    raw = _env_first("UTT_MARKET_METRICS_SUMMARY_CACHE_FILE")
    if raw:
        return Path(raw).expanduser()
    return Path(__file__).resolve().parents[2] / "data" / "market_metrics_summary_cache.json"


def _summary_snapshot_key(
    *,
    assets: Optional[Any],
    include_assets: Optional[Any],
    limit: int,
) -> str:
    """Stable cache key for the broad window summary.

    For assets=db/known, ignore include_assets so a selected symbol does not
    fragment the broad Market Cap / Volume cache. The selected asset should be a
    frontend highlight/filter concern, not a reason to rebuild 700+ local rows.
    """
    mode = str(assets or "default").strip().lower() or "default"
    include_clean: List[str] = []
    if mode not in {s.lower() for s in _KNOWN_ASSET_SENTINELS}:
        include_clean = _parse_assets(include_assets) if str(include_assets or "").strip() else []
    raw = json.dumps(
        {
            "schema_version": "port_metrics_2_venue_universe",
            "mode": mode,
            "include_assets": include_clean,
            "limit": int(limit),
        },
        sort_keys=True,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _summary_snapshot_load(snapshot_key: str, *, limit: int = 1000) -> Optional[Dict[str, Any]]:
    if not snapshot_key or not _summary_snapshot_cache_enabled():
        return None

    path = _summary_snapshot_cache_file()
    try:
        if not path.exists():
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        snapshots = data.get("snapshots") if isinstance(data, dict) else None
        rec = snapshots.get(snapshot_key) if isinstance(snapshots, dict) else None
        if not isinstance(rec, dict):
            return None
        payload = rec.get("payload")
        cached_at = _num(rec.get("cached_at"))
        if not isinstance(payload, dict) or cached_at is None:
            return None
        age_s = max(0.0, time.time() - float(cached_at))
        if age_s > _summary_snapshot_max_age_s():
            return None
        out = dict(payload)
        out["cache"] = "summary_disk_hit"
        out["summary_snapshot"] = True
        out["summary_snapshot_cached_at"] = rec.get("cached_at_iso") or ""
        out["summary_snapshot_age_s"] = age_s
        out["refresh_mode"] = "summary_snapshot"
        out["market_data_cache_snapshot_only"] = True
        out = _summary_snapshot_refresh_current_context(out, limit=limit)
        return out
    except Exception:
        return None


def _summary_snapshot_save(snapshot_key: str, payload: Dict[str, Any]) -> None:
    if not snapshot_key or not _summary_snapshot_cache_enabled() or not isinstance(payload, dict):
        return

    try:
        path = _summary_snapshot_cache_file()
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        except Exception:
            data = {}
        if not isinstance(data, dict):
            data = {}
        snapshots = data.get("snapshots")
        if not isinstance(snapshots, dict):
            snapshots = {}

        clean_payload = dict(payload)
        clean_payload.pop("cache", None)
        clean_payload.pop("summary_snapshot", None)
        clean_payload.pop("summary_snapshot_cached_at", None)
        clean_payload.pop("summary_snapshot_age_s", None)

        now = time.time()
        snapshots[snapshot_key] = {
            "cached_at": now,
            "cached_at_iso": _utc_now_iso(),
            "payload": clean_payload,
        }
        data["updated_at"] = _utc_now_iso()
        data["snapshots"] = snapshots
        path.write_text(json.dumps(data, sort_keys=True), encoding="utf-8")
    except Exception:
        return


def _cg_raw_cache_file() -> Path:
    raw = _env_first("UTT_MARKET_METRICS_CACHE_FILE")
    if raw:
        return Path(raw).expanduser()
    # backend/app/services/market_metrics.py -> backend/data/market_metrics_coingecko_cache.json
    return Path(__file__).resolve().parents[2] / "data" / "market_metrics_coingecko_cache.json"


def _cg_raw_cache_load_once() -> None:
    global _CG_RAW_CACHE_LOADED
    if _CG_RAW_CACHE_LOADED:
        return
    _CG_RAW_CACHE_LOADED = True

    path = _cg_raw_cache_file()
    try:
        if not path.exists():
            return
        data = json.loads(path.read_text(encoding="utf-8"))
        rows = data.get("rows") if isinstance(data, dict) else None
        if not isinstance(rows, dict):
            return
        now = time.time()
        max_disk_age = _clamp_int(_env_first("UTT_MARKET_METRICS_DISK_CACHE_MAX_AGE_S"), 300, 604800, 86400)
        for cg_id, rec in rows.items():
            if not isinstance(rec, dict):
                continue
            row = rec.get("row")
            cached_at = _num(rec.get("cached_at"))
            if not isinstance(row, dict) or cached_at is None:
                continue
            if now - float(cached_at) > max_disk_age:
                continue
            _CG_RAW_BY_ID[str(cg_id)] = {"row": row, "cached_at": float(cached_at)}
    except Exception:
        # Disk cache is strictly best-effort; never block the terminal on it.
        return


def _cg_raw_cache_save() -> None:
    try:
        path = _cg_raw_cache_file()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "updated_at": _utc_now_iso(),
            "rows": _CG_RAW_BY_ID,
        }
        path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    except Exception:
        return


def _cg_raw_cache_update(rows: Sequence[Dict[str, Any]]) -> None:
    if not rows:
        return
    _cg_raw_cache_load_once()
    now = time.time()
    changed = False
    for row in rows:
        if not isinstance(row, dict):
            continue
        cg_id = str(row.get("id") or "").strip()
        if not cg_id:
            continue
        clean_row = dict(row)
        clean_row.pop("_utt_cache_stale", None)
        clean_row.pop("_utt_cache_age_s", None)
        _CG_RAW_BY_ID[cg_id] = {"row": clean_row, "cached_at": now}
        changed = True
    if changed:
        _cg_raw_cache_save()


def _cg_raw_cache_rows(ids: Sequence[str], *, max_age_s: int, allow_stale: bool = False) -> Dict[str, Dict[str, Any]]:
    _cg_raw_cache_load_once()
    now = time.time()
    out: Dict[str, Dict[str, Any]] = {}
    for cg_id in ids:
        key = str(cg_id or "").strip()
        if not key:
            continue
        rec = _CG_RAW_BY_ID.get(key)
        if not isinstance(rec, dict):
            continue
        row = rec.get("row")
        cached_at = _num(rec.get("cached_at"))
        if not isinstance(row, dict) or cached_at is None:
            continue
        age_s = max(0.0, now - float(cached_at))
        if not allow_stale and age_s > max(10, int(max_age_s)):
            continue
        next_row = dict(row)
        if age_s > max(10, int(max_age_s)):
            next_row["_utt_cache_stale"] = True
            next_row["_utt_cache_age_s"] = age_s
        out[key] = next_row
    return out



def _token_registry_cg_meta_ttl_s() -> int:
    return _clamp_int(_env_first("UTT_MARKET_METRICS_TOKEN_REGISTRY_TTL_S"), 10, 3600, 300)


def _token_registry_cg_meta_by_symbol(force_refresh: bool = False) -> Dict[str, Dict[str, str]]:
    """Return symbol -> CoinGecko metadata from Token Registry.

    This is intentionally read-only and schema-tolerant: if older local DBs do
    not yet have external_price_source/external_price_id, market_metrics falls
    back to env overrides and COINGECKO_BY_SYMBOL without mutating the DB.
    """
    global _TOKEN_REGISTRY_CG_META_BY_SYMBOL, _TOKEN_REGISTRY_CG_META_LOADED_AT

    now = time.time()
    ttl_s = _token_registry_cg_meta_ttl_s()
    if (
        not force_refresh
        and _TOKEN_REGISTRY_CG_META_LOADED_AT > 0
        and (now - float(_TOKEN_REGISTRY_CG_META_LOADED_AT)) <= ttl_s
    ):
        return dict(_TOKEN_REGISTRY_CG_META_BY_SYMBOL)

    out: Dict[str, Dict[str, str]] = {}
    try:
        from sqlalchemy import text
        from ..db import SessionLocal
    except Exception:
        _TOKEN_REGISTRY_CG_META_BY_SYMBOL = {}
        _TOKEN_REGISTRY_CG_META_LOADED_AT = now
        return {}

    db = SessionLocal()
    try:
        cols = _db_table_columns(db, "token_registry")
        needed = {"symbol", "external_price_id"}
        if not needed.issubset(cols):
            _TOKEN_REGISTRY_CG_META_BY_SYMBOL = {}
            _TOKEN_REGISTRY_CG_META_LOADED_AT = now
            return {}

        select_cols = ["symbol", "external_price_id"]
        for optional_col in ("external_price_source", "chain", "venue", "label"):
            if optional_col in cols:
                select_cols.append(optional_col)
        col_sql = ", ".join(f'"{c}"' for c in select_cols)
        sql = (
            f'SELECT {col_sql} FROM "token_registry" '
            'WHERE "symbol" IS NOT NULL '
            'AND TRIM(CAST("symbol" AS TEXT)) <> "" '
            'AND "external_price_id" IS NOT NULL '
            'AND TRIM(CAST("external_price_id" AS TEXT)) <> ""'
        )
        rows = db.execute(text(sql)).mappings().all()

        def _priority(row: Dict[str, Any]) -> Tuple[int, int, int]:
            src = str(row.get("external_price_source") or "").strip().lower()
            chain = str(row.get("chain") or "").strip().lower()
            venue = str(row.get("venue") or "").strip().lower()
            source_rank = 0 if src in {"coingecko", "coingecko_simple", "coin_gecko", "cg"} else (1 if not src else 9)
            venue_rank = 0 if venue else 1
            chain_rank = 0 if chain in {"global", "multi", "coingecko"} else 1
            return (source_rank, venue_rank, chain_rank)

        picked: Dict[str, Tuple[Tuple[int, int, int], Dict[str, Any]]] = {}
        for row in rows or []:
            sym = str(row.get("symbol") or "").strip().upper()
            price_id = str(row.get("external_price_id") or "").strip()
            src = str(row.get("external_price_source") or "").strip().lower()
            if not sym or not price_id:
                continue
            # Only CoinGecko-compatible rows are usable by this service today.
            # Blank source + price id is accepted as CoinGecko for backward compatibility.
            if src and src not in {"coingecko", "coingecko_simple", "coin_gecko", "cg"}:
                continue
            rank = _priority(row)
            prev = picked.get(sym)
            if prev is None or rank < prev[0]:
                picked[sym] = (rank, dict(row))

        for sym, (_, row) in picked.items():
            chain = str(row.get("chain") or "global").strip().lower() or "global"
            label = str(row.get("label") or sym).strip() or sym
            price_id = str(row.get("external_price_id") or "").strip()
            if price_id:
                out[sym] = {"id": price_id, "name": label, "chain": chain, "source": "token_registry"}
    except Exception:
        out = {}
    finally:
        try:
            db.close()
        except Exception:
            pass

    _TOKEN_REGISTRY_CG_META_BY_SYMBOL = dict(out)
    _TOKEN_REGISTRY_CG_META_LOADED_AT = now
    return dict(out)



def _cg_symbol_cache_file() -> Path:
    raw = _env_first("UTT_MARKET_METRICS_SYMBOL_CACHE_FILE")
    if raw:
        return Path(raw).expanduser()
    return Path(__file__).resolve().parents[2] / "data" / "market_metrics_coingecko_symbol_cache.json"


def _cg_symbol_discovery_enabled() -> bool:
    return _env_bool("UTT_MARKET_METRICS_AUTO_DISCOVER_COINGECKO_SYMBOLS", True)


def _cg_symbol_discovery_ttl_s() -> int:
    return _clamp_int(_env_first("UTT_MARKET_METRICS_SYMBOL_DISCOVERY_TTL_S"), 300, 604800, 86400)


def _cg_symbol_negative_ttl_s() -> int:
    return _clamp_int(_env_first("UTT_MARKET_METRICS_SYMBOL_NEGATIVE_TTL_S"), 60, 86400, 1800)


def _cg_symbol_cache_load_once() -> None:
    global _CG_SYMBOL_META_LOADED
    if _CG_SYMBOL_META_LOADED:
        return
    _CG_SYMBOL_META_LOADED = True
    path = _cg_symbol_cache_file()
    try:
        if not path.exists():
            return
        data = json.loads(path.read_text(encoding="utf-8"))
        now = time.time()
        max_age = _cg_symbol_discovery_ttl_s()
        rows = data.get("rows") if isinstance(data, dict) else None
        negatives = data.get("negatives") if isinstance(data, dict) else None
        if isinstance(rows, dict):
            for sym, rec in rows.items():
                if not isinstance(rec, dict):
                    continue
                cached_at = _num(rec.get("cached_at"))
                meta = rec.get("meta")
                if cached_at is None or not isinstance(meta, dict):
                    continue
                if now - float(cached_at) > max_age:
                    continue
                symbol = str(sym or "").strip().upper()
                coin_id = str(meta.get("id") or "").strip()
                if symbol and coin_id:
                    _CG_SYMBOL_META_BY_SYMBOL[symbol] = {
                        "id": coin_id,
                        "name": str(meta.get("name") or symbol).strip() or symbol,
                        "chain": str(meta.get("chain") or "global").strip() or "global",
                        "source": str(meta.get("source") or "coingecko_search_cache").strip() or "coingecko_search_cache",
                    }
        if isinstance(negatives, dict):
            neg_ttl = _cg_symbol_negative_ttl_s()
            for sym, ts in negatives.items():
                cached_at = _num(ts)
                if cached_at is None:
                    continue
                if now - float(cached_at) <= neg_ttl:
                    _CG_SYMBOL_NEGATIVE_CACHE[str(sym or "").strip().upper()] = float(cached_at)
    except Exception:
        return


def _cg_symbol_cache_save() -> None:
    try:
        path = _cg_symbol_cache_file()
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "updated_at": _utc_now_iso(),
            "rows": {
                sym: {"meta": meta, "cached_at": time.time()}
                for sym, meta in sorted(_CG_SYMBOL_META_BY_SYMBOL.items())
            },
            "negatives": dict(_CG_SYMBOL_NEGATIVE_CACHE),
        }
        path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    except Exception:
        return


def _coingecko_search_base_url() -> str:
    return (_env_first("UTT_MARKET_METRICS_COINGECKO_BASE_URL") or "https://api.coingecko.com/api/v3").rstrip("/")


def _cg_search_json(query: str, timeout_s: float = 8.0) -> Any:
    q = str(query or "").strip()
    if not q:
        return None
    url = _coingecko_search_base_url() + "/search?" + urllib.parse.urlencode({"query": q})
    return _http_json(url, timeout_s=timeout_s)


def _choose_cg_search_coin_for_symbol(symbol: str, coins: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    sym = str(symbol or "").strip().upper()
    if not sym:
        return None
    exact = []
    for coin in coins or []:
        if not isinstance(coin, dict):
            continue
        if str(coin.get("symbol") or "").strip().upper() == sym and str(coin.get("id") or "").strip():
            exact.append(coin)
    if not exact:
        return None

    def _rank(coin: Dict[str, Any]) -> Tuple[int, int, int, str]:
        coin_id = str(coin.get("id") or "").strip().lower()
        api_symbol = str(coin.get("api_symbol") or "").strip().lower()
        name = str(coin.get("name") or "").strip().lower()
        lower_sym = sym.lower()
        # Prefer direct id/api-symbol/name matches, then ranked coins, then shorter IDs.
        direct = 0 if coin_id == lower_sym else (1 if api_symbol == lower_sym else (2 if name == lower_sym else 3))
        rank_raw = coin.get("market_cap_rank")
        try:
            mcap_rank = int(rank_raw) if rank_raw is not None else 999999
        except Exception:
            mcap_rank = 999999
        return (direct, mcap_rank, len(coin_id), coin_id)

    exact.sort(key=_rank)
    return exact[0]


def _coingecko_discover_meta_for_symbol(symbol: str) -> Optional[Dict[str, str]]:
    """Best-effort symbol -> CoinGecko ID discovery for explicit/selected assets.

    Owned CEX assets give us a symbol, but CoinGecko market endpoints require a
    coin ID.  Token Registry remains the deterministic override.  This resolver
    only runs for explicitly requested symbols to avoid a burst of searches for
    every dusty/legacy DB asset in the owned-assets list.
    """
    global _CG_SYMBOL_DISCOVERY_BACKOFF_UNTIL

    sym = str(symbol or "").strip().upper()
    if not sym or not _cg_symbol_discovery_enabled():
        return None

    _cg_symbol_cache_load_once()
    cached = _CG_SYMBOL_META_BY_SYMBOL.get(sym)
    if cached:
        return cached

    neg_ts = _CG_SYMBOL_NEGATIVE_CACHE.get(sym)
    if neg_ts is not None and (time.time() - float(neg_ts)) <= _cg_symbol_negative_ttl_s():
        return None

    now = time.time()
    if now < float(_CG_SYMBOL_DISCOVERY_BACKOFF_UNTIL or 0):
        return None

    try:
        data = _cg_search_json(sym, timeout_s=float(os.getenv("UTT_MARKET_METRICS_SYMBOL_DISCOVERY_TIMEOUT_S", "8") or 8))
    except urllib.error.HTTPError as e:
        if int(getattr(e, "code", 0) or 0) == 429:
            _CG_SYMBOL_DISCOVERY_BACKOFF_UNTIL = time.time() + _cg_backoff_s()
        return None
    except Exception:
        return None

    coins = data.get("coins") if isinstance(data, dict) else None
    if not isinstance(coins, list):
        _CG_SYMBOL_NEGATIVE_CACHE[sym] = time.time()
        _cg_symbol_cache_save()
        return None

    picked = _choose_cg_search_coin_for_symbol(sym, [c for c in coins if isinstance(c, dict)])
    if not picked:
        _CG_SYMBOL_NEGATIVE_CACHE[sym] = time.time()
        _cg_symbol_cache_save()
        return None

    meta = {
        "id": str(picked.get("id") or "").strip(),
        "name": str(picked.get("name") or sym).strip() or sym,
        "chain": "global",
        "source": "coingecko_search",
    }
    if not meta["id"]:
        _CG_SYMBOL_NEGATIVE_CACHE[sym] = time.time()
        _cg_symbol_cache_save()
        return None

    _CG_SYMBOL_META_BY_SYMBOL[sym] = meta
    _CG_SYMBOL_NEGATIVE_CACHE.pop(sym, None)
    _cg_symbol_cache_save()
    return meta


def _coingecko_meta_for_symbol(asset: str, *, allow_discovery: bool = False) -> Optional[Dict[str, str]]:
    sym = str(asset or "").strip().upper()
    if not sym:
        return None

    # Preferred: UI-managed Token Registry external price metadata.
    # This allows market metrics mappings to be maintained from the app instead
    # of adding every symbol->CoinGecko id to this backend file.
    registry_meta = _token_registry_cg_meta_by_symbol().get(sym)
    if registry_meta:
        return registry_meta

    # Optional env escape hatch for assets not yet in Token Registry:
    #   UTT_MARKET_METRICS_CG_IDS=BCH=bitcoin-cash,LTC=litecoin
    # or JSON:
    #   UTT_MARKET_METRICS_CG_IDS_JSON={"BCH":{"id":"bitcoin-cash","name":"Bitcoin Cash","chain":"bitcoin_cash"}}
    raw_json = _env_first("UTT_MARKET_METRICS_CG_IDS_JSON")
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            entry = parsed.get(sym) if isinstance(parsed, dict) else None
            if isinstance(entry, str) and entry.strip():
                return {"id": entry.strip(), "name": sym, "chain": "global"}
            if isinstance(entry, dict) and str(entry.get("id") or "").strip():
                return {
                    "id": str(entry.get("id") or "").strip(),
                    "name": str(entry.get("name") or sym).strip() or sym,
                    "chain": str(entry.get("chain") or "global").strip() or "global",
                }
        except Exception:
            pass

    raw_pairs = _env_first("UTT_MARKET_METRICS_CG_IDS")
    if raw_pairs:
        for part in raw_pairs.replace(";", ",").split(","):
            if "=" not in part:
                continue
            k, v = part.split("=", 1)
            if k.strip().upper() == sym and v.strip():
                return {"id": v.strip(), "name": sym, "chain": "global", "source": "env"}

    # Final bootstrap fallback. Keep this map small enough to ship the app, but
    # let Token Registry override it whenever external_price_id exists.
    meta = COINGECKO_BY_SYMBOL.get(sym)
    if meta:
        return {**meta, "source": "hardcoded_fallback"}

    if allow_discovery:
        discovered = _coingecko_discover_meta_for_symbol(sym)
        if discovered:
            return discovered

    return None


def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    hit = _CACHE.get(key)
    if not hit:
        return None
    if time.time() >= float(hit.get("expires_at", 0) or 0):
        return None
    payload = hit.get("payload")
    return payload if isinstance(payload, dict) else None


def _cache_set(key: str, payload: Dict[str, Any], ttl_s: int) -> None:
    _CACHE[key] = {
        "expires_at": time.time() + max(10, int(ttl_s)),
        "payload": payload,
    }


def _http_json(url: str, timeout_s: float = 10.0) -> Any:
    headers = {
        "accept": "application/json",
        "user-agent": "UTT-local-market-metrics/1.0",
    }
    key = _cg_api_key()
    if key:
        # CoinGecko Demo keys use x-cg-demo-api-key. Pro keys generally accept
        # x-cg-pro-api-key; sending only the Demo header keeps this free-safe.
        headers["x-cg-demo-api-key"] = key

    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read()
    if not raw:
        return None
    return json.loads(raw.decode("utf-8"))


def _fetch_coingecko_markets(ids: Sequence[str], limit: int, page: int = 1) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    global _CG_BACKOFF_UNTIL

    now = time.time()
    if now < float(_CG_BACKOFF_UNTIL or 0):
        wait_s = max(1, int(float(_CG_BACKOFF_UNTIL) - now))
        return [], [{
            "source": "coingecko",
            "error": "rate_limited_backoff",
            "message": f"CoinGecko backoff active for {wait_s}s; using cached/stale rows when available.",
        }]

    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": str(_clamp_int(limit, 1, 250, 250)),
        "page": str(_clamp_int(page, 1, 100, 1)),
        "sparkline": "false",
        "price_change_percentage": "24h",
        "locale": "en",
        "precision": "full",
    }
    if ids:
        params["ids"] = ",".join(ids)

    url = "https://api.coingecko.com/api/v3/coins/markets?" + urllib.parse.urlencode(params)
    errors: List[Dict[str, Any]] = []

    try:
        data = _http_json(url, timeout_s=float(os.getenv("UTT_MARKET_METRICS_HTTP_TIMEOUT_S", "10") or 10))
    except urllib.error.HTTPError as e:
        if int(getattr(e, "code", 0) or 0) == 429:
            _CG_BACKOFF_UNTIL = time.time() + _cg_backoff_s()
        errors.append({"source": "coingecko", "error": f"HTTP {e.code}", "message": str(e)})
        return [], errors
    except Exception as e:
        errors.append({"source": "coingecko", "error": type(e).__name__, "message": str(e)})
        return [], errors

    if not isinstance(data, list):
        errors.append({"source": "coingecko", "error": "unexpected_response", "message": "CoinGecko did not return a list"})
        return [], errors

    return [x for x in data if isinstance(x, dict)], errors



def _fetch_coingecko_market_pages(page_count: int, per_page: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Fetch top CoinGecko market pages for broad symbol-cache seeding.

    This is used only on manual live refresh for db/known market metric windows.
    It avoids hundreds of per-symbol /search requests and fills the raw market
    cache with ranked rows that can be matched back to local symbols.
    """
    pages = _clamp_int(page_count, 0, 10, 4)
    size = _clamp_int(per_page, 50, 250, 250)
    if pages <= 0:
        return [], []

    all_rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    seen_ids = set()

    for page in range(1, pages + 1):
        rows, page_errors = _fetch_coingecko_markets([], size, page=page)
        if page_errors:
            errors.extend(page_errors)
            # Stop paging on rate-limit/backoff/transport errors.
            break

        clean_rows = [r for r in rows or [] if isinstance(r, dict)]
        if not clean_rows:
            break

        for row in clean_rows:
            cg_id = str(row.get("id") or "").strip()
            if cg_id and cg_id in seen_ids:
                continue
            if cg_id:
                seen_ids.add(cg_id)
            all_rows.append(row)

        if len(clean_rows) < size:
            break

    return all_rows, errors

def _row_from_cg(asset: str, meta: Dict[str, str], raw: Dict[str, Any], updated_at: str) -> Dict[str, Any]:
    warnings: List[str] = []
    if raw.get("_utt_cache_stale"):
        age_s = _num(raw.get("_utt_cache_age_s")) or 0
        warnings.append(f"Using stale CoinGecko cache ({int(age_s // 60)}m old).")
    if raw.get("_utt_symbol_match"):
        warnings.append("CoinGecko data matched by ticker symbol; verify if this symbol is ambiguous.")

    price = _num(raw.get("current_price"))
    market_cap = _num(raw.get("market_cap"))
    fdv = _num(raw.get("fully_diluted_valuation"))
    total_volume = _num(raw.get("total_volume"))
    circulating = _num(raw.get("circulating_supply"))
    total_supply = _num(raw.get("total_supply"))
    max_supply = _num(raw.get("max_supply"))
    volume_base = total_volume / price if total_volume is not None and price and price > 0 else None

    return {
        "asset": asset,
        "name": raw.get("name") or meta.get("name") or asset,
        "pair": None,
        "chain": meta.get("chain") or "global",
        "venue": "global",
        "rank": raw.get("market_cap_rank"),
        "price_usd": price,
        "market_cap_usd": market_cap,
        "fdv_usd": fdv,
        "volume_24h_usd": total_volume,
        "volume_24h_base": volume_base,
        "liquidity_usd": None,
        "change_24h_pct": _num(raw.get("price_change_percentage_24h")),
        "circulating_supply": circulating,
        "total_supply": total_supply,
        "max_supply": max_supply,
        "supply_source": "coingecko:circulating_supply",
        "price_source": f"coingecko:{meta.get('id')}",
        "source": f"coingecko:{meta.get('id')}",
        "source_kind": "external_global",
        "updated_at": raw.get("last_updated") or updated_at,
        "warnings": warnings,
    }


def _uttt_placeholder(updated_at: str) -> Dict[str, Any]:
    price = _num(_env_first("UTTT_PRICE_USD", "UTTT_DERIVED_PRICE_USD"))
    circ = _num(_env_first("UTTT_CIRCULATING_SUPPLY"))
    total = _num(_env_first("UTTT_TOTAL_SUPPLY", "UTTT_MAX_SUPPLY"))
    if total is None:
        total = 1_000_000_000.0

    market_cap = circ * price if circ is not None and price is not None else None
    fdv = total * price if total is not None and price is not None else None

    warnings = []
    if price is None:
        warnings.append("UTTT price is not wired into market_metrics yet; set UTTT_PRICE_USD or wire derived UTTT/USD next.")
    if circ is None:
        warnings.append("UTTT circulating supply is not configured; market_cap_usd is blank and fdv_usd uses total supply only when price exists.")

    return {
        "asset": "UTTT",
        "name": "Unified Trading Terminal Token",
        "pair": "UTTT-HDX",
        "chain": "multi",
        "venue": "derived",
        "rank": None,
        "price_usd": price,
        "market_cap_usd": market_cap,
        "fdv_usd": fdv,
        "volume_24h_usd": None,
        "volume_24h_base": None,
        "liquidity_usd": None,
        "change_24h_pct": None,
        "circulating_supply": circ,
        "total_supply": total,
        "max_supply": total,
        "supply_source": "env:UTTT_CIRCULATING_SUPPLY/UTTT_TOTAL_SUPPLY",
        "price_source": "env:UTTT_PRICE_USD" if price is not None else "derived:pending",
        "source": "derived:registry_supply*UTTT/USD",
        "source_kind": "derived_project_asset",
        "updated_at": updated_at,
        "warnings": warnings,
    }


def _missing_placeholder(asset: str, updated_at: str, reason: str) -> Dict[str, Any]:
    return {
        "asset": asset,
        "name": asset,
        "pair": None,
        "chain": "unknown",
        "venue": "unknown",
        "rank": None,
        "price_usd": None,
        "market_cap_usd": None,
        "fdv_usd": None,
        "volume_24h_usd": None,
        "volume_24h_base": None,
        "liquidity_usd": None,
        "change_24h_pct": None,
        "circulating_supply": None,
        "total_supply": None,
        "max_supply": None,
        "supply_source": "unavailable",
        "price_source": "unavailable",
        "source": "unavailable",
        "source_kind": "missing",
        "updated_at": updated_at,
        "warnings": [reason],
    }


def get_market_metrics_summary(
    *,
    assets: Optional[Any] = None,
    include_assets: Optional[Any] = None,
    limit: int = 1000,
    ttl_s: int = 300,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    clean_limit = _clamp_int(limit, 1, 1000, 1000)
    clean_ttl = _clamp_int(ttl_s, 10, 3600, 300)

    is_owned_request = _is_owned_asset_request(assets)
    is_known_request = _is_known_asset_request(assets)
    summary_snapshot_key = _summary_snapshot_key(
        assets=assets,
        include_assets=include_assets,
        limit=clean_limit,
    )

    # PORT-METRICS.1 v9:
    # Let broad Market Cap / Volume window opens return immediately from the
    # last full summary snapshot. Manual Refresh uses force_refresh=true and
    # rebuilds the local universe / live market cache.
    if is_known_request and not force_refresh:
        snapshot_payload = _summary_snapshot_load(summary_snapshot_key, limit=clean_limit)
        if snapshot_payload is not None:
            return snapshot_payload

    owned_assets_raw, owned_asset_source = _db_owned_metric_assets(limit=clean_limit)

    if is_owned_request:
        clean_assets = list(owned_assets_raw)
        asset_source = owned_asset_source
        if not clean_assets:
            clean_assets = list(DEFAULT_ASSETS)
            asset_source = f"{asset_source}_fallback_default"
    elif is_known_request:
        clean_assets, asset_source = _db_known_metric_assets(limit=clean_limit)
        if not clean_assets:
            clean_assets = list(DEFAULT_ASSETS)
            asset_source = f"{asset_source}_fallback_default"
    else:
        clean_assets = _parse_assets(assets)
        asset_source = "default" if assets is None else "explicit"

    include_clean = _parse_assets(include_assets) if str(include_assets or "").strip() else []
    clean_assets = _merge_assets(clean_assets, include_clean)
    if not clean_assets:
        clean_assets = list(DEFAULT_ASSETS)
        asset_source = f"{asset_source}_fallback_default"

    # PORT-METRICS.1 v5:
    # Do not hide unmapped owned assets by default.  The Market Cap / Volume
    # windows are discovery/visibility tools; an owned token without a
    # CoinGecko/registry mapping should still display as an owned placeholder so
    # venue holdings are not silently under-counted.  The old mapped-only mode is
    # preserved behind an explicit opt-in env flag for users who want a shorter
    # market-data-only list.
    if is_owned_request and _env_bool("UTT_MARKET_METRICS_HIDE_UNMAPPED_OWNED", False):
        keep_explicit = set(include_clean)
        mapped_assets: List[str] = []
        for asset in clean_assets:
            sym = str(asset or "").strip().upper()
            if not sym:
                continue
            if sym in keep_explicit or sym == "UTTT" or _coingecko_meta_for_symbol(sym, allow_discovery=False):
                mapped_assets.append(sym)
        if mapped_assets:
            clean_assets = mapped_assets
            if not str(asset_source or "").endswith("_mapped"):
                asset_source = f"{asset_source}_mapped"

    asset_context = _db_market_metric_asset_context(limit=clean_limit)
    context_known_assets = _merge_assets(asset_context.keys())
    owned_assets_payload = _merge_assets(owned_assets_raw)

    cache_key = (
        "summary:"
        + asset_source
        + ":"
        + ",".join(clean_assets)
        + ":owned:"
        + ",".join(owned_assets_payload)
        + f":{clean_limit}"
    )
    if not force_refresh:
        cached = _cache_get(cache_key)
        if cached is not None:
            return {**cached, "cache": "hit"}

    updated_at = _utc_now_iso()
    ids: List[str] = []
    id_to_symbol: Dict[str, str] = {}

    # Avoid broad CoinGecko /search fan-out for assets=db/known window loads.
    # Known/unowned rows should return quickly from Token Registry, local
    # hardcoded mappings, env mappings, and placeholders.  Symbol search is kept
    # only for explicit one-off asset requests so the Market Cap / Volume windows
    # do not time out or rate-limit when many unowned candidates are present.
    explicit_discovery_assets = set()
    if not is_owned_request and not is_known_request:
        explicit_discovery_assets.update(clean_assets)
        explicit_discovery_assets.update(include_clean)

    for asset in clean_assets:
        meta = _coingecko_meta_for_symbol(asset, allow_discovery=str(asset or "").strip().upper() in explicit_discovery_assets)
        cg_id = meta.get("id") if meta else ""
        if cg_id:
            ids.append(cg_id)
            id_to_symbol[cg_id] = asset

    # PORT-METRICS.1 v7 cache-first behavior:
    # Market Cap / Volume windows use assets=db/known and may include hundreds
    # of local rows.  Do not block window-open/auto-refresh on live CoinGecko
    # fetches for that broad universe.  Return local rows plus any cached/stale
    # market data immediately.  A manual frontend Refresh sets force_refresh=1
    # and performs the bounded live fill/update path.
    cache_snapshot_only = bool(is_known_request and not force_refresh and _env_bool("UTT_MARKET_METRICS_DB_CACHE_FIRST", True))
    raw_cache_allow_stale = bool(cache_snapshot_only)

    fresh_cached = _cg_raw_cache_rows(ids, max_age_s=clean_ttl, allow_stale=raw_cache_allow_stale)
    missing_ids = [cg_id for cg_id in ids if cg_id not in fresh_cached]

    if cache_snapshot_only:
        live_missing_ids = []
        skipped_live_ids = list(missing_ids)
    else:
        live_missing_ids = missing_ids[: _cg_live_id_cap()]
        skipped_live_ids = missing_ids[len(live_missing_ids) :]

    markets: List[Dict[str, Any]] = []
    market_page_rows: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    if live_missing_ids:
        markets, live_errors = _fetch_coingecko_markets(live_missing_ids, min(clean_limit, len(live_missing_ids), 250))
        errors.extend(live_errors)
        if markets:
            _cg_raw_cache_update(markets)

    # PORT-METRICS.1 v8:
    # A broad local universe can contain hundreds of symbols that do not yet
    # have explicit Token Registry/CoinGecko ID mappings. On manual Refresh,
    # seed the raw CoinGecko market cache from bounded market-cap pages, then
    # match local assets by ticker symbol. Cache-first window loads reuse those
    # rows without live HTTP calls.
    if (
        is_known_request
        and force_refresh
        and _env_bool("UTT_MARKET_METRICS_ENABLE_MARKET_PAGE_REFRESH", True)
    ):
        market_page_rows, page_errors = _fetch_coingecko_market_pages(
            _cg_market_page_cap(),
            _cg_market_page_per_page(),
        )
        errors.extend(page_errors)
        if market_page_rows:
            _cg_raw_cache_update(market_page_rows)

    by_id: Dict[str, Dict[str, Any]] = {str(row.get("id") or ""): row for row in fresh_cached.values() if row.get("id")}
    by_id.update({str(row.get("id") or ""): row for row in markets if row.get("id")})
    by_id.update({str(row.get("id") or ""): row for row in market_page_rows if row.get("id")})

    if errors:
        stale_cached = _cg_raw_cache_rows(ids, max_age_s=clean_ttl, allow_stale=True)
        for cg_id, row in stale_cached.items():
            by_id.setdefault(cg_id, row)

    symbol_cached = _cg_raw_cache_symbol_rows(
        clean_assets,
        max_age_s=clean_ttl,
        allow_stale=bool(raw_cache_allow_stale or cache_snapshot_only),
    )
    symbol_live = _cg_rows_by_symbol(market_page_rows, clean_assets)
    symbol_rows: Dict[str, Dict[str, Any]] = dict(symbol_cached)
    symbol_rows.update(symbol_live)

    items: List[Dict[str, Any]] = []
    for asset in clean_assets:
        if asset == "UTTT":
            items.append(_uttt_placeholder(updated_at))
            continue

        asset_sym = str(asset or "").strip().upper()
        meta = _coingecko_meta_for_symbol(asset, allow_discovery=asset_sym in explicit_discovery_assets)
        cg_id = meta.get("id") if meta else ""
        raw = by_id.get(cg_id)
        if meta and raw:
            items.append(_row_from_cg(asset, meta, raw, updated_at))
            continue

        # Fallback: broad CoinGecko market-page cache matched by ticker symbol.
        # This fills market data for assets that do not yet have explicit
        # external_price_id mappings in Token Registry.
        symbol_raw = symbol_rows.get(asset_sym)
        if symbol_raw:
            symbol_meta = {
                "id": str(symbol_raw.get("id") or cg_id or asset_sym.lower()).strip(),
                "name": str(symbol_raw.get("name") or (meta or {}).get("name") or asset_sym).strip() or asset_sym,
                "chain": str((meta or {}).get("chain") or "global").strip() or "global",
                "source": "coingecko_market_symbol_match",
            }
            items.append(_row_from_cg(asset, symbol_meta, symbol_raw, updated_at))
            continue

        if meta and not raw:
            items.append(_missing_placeholder(asset, updated_at, f"CoinGecko row not returned for {cg_id}."))
        else:
            items.append(_missing_placeholder(asset, updated_at, "No market_metrics source mapping exists for this asset."))

    items = _annotate_market_metric_rows(
        items,
        asset_context=asset_context,
        owned_assets=owned_assets_payload,
    )

    has_real_market_rows = any(
        _num(row.get("price_usd")) is not None
        or _num(row.get("market_cap_usd")) is not None
        or _num(row.get("volume_24h_usd")) is not None
        for row in items
        if isinstance(row, dict) and row.get("source_kind") != "missing"
    )

    payload = {
        "ok": bool(has_real_market_rows) or not bool(errors),
        "updated_at": updated_at,
        "ttl_s": clean_ttl,
        "asset_source": asset_source,
        "owned_asset_source": owned_asset_source,
        "assets": clean_assets,
        "asset_count": len(clean_assets),
        "known_assets": _merge_assets(clean_assets, context_known_assets),
        "known_asset_count": len(_merge_assets(clean_assets, context_known_assets)),
        "owned_assets": owned_assets_payload,
        "owned_asset_count": len(owned_assets_payload),
        "market_data_id_count": len(ids),
        "market_data_cached_id_count": len(fresh_cached),
        "market_data_live_fetch_id_count": len(live_missing_ids),
        "market_data_skipped_live_id_count": len(skipped_live_ids),
        "market_data_symbol_match_count": len(symbol_rows),
        "market_data_symbol_cache_match_count": len(symbol_cached),
        "market_data_symbol_live_match_count": len(symbol_live),
        "market_data_market_page_live_fetch_count": len(market_page_rows),
        "market_data_cache_snapshot_only": cache_snapshot_only,
        "refresh_mode": "cache_snapshot" if cache_snapshot_only else ("live_refresh" if force_refresh else "normal"),
        "include_assets": include_clean,
        "asset_context": {k: _metric_context_public(v) for k, v in sorted(asset_context.items())},
        "venue_filter_options": _venue_filter_options_from_context(asset_context),
        "venue_asset_counts": _venue_asset_counts_from_context(asset_context),
        "venue_symbol_context_mode": "latest_snapshot_per_venue",
        "items": items,
        "errors": errors,
        "cache": "miss",
    }

    _cache_set(cache_key, payload, clean_ttl if has_real_market_rows else _summary_error_ttl_s())
    if is_known_request:
        _summary_snapshot_save(summary_snapshot_key, payload)
    return payload

