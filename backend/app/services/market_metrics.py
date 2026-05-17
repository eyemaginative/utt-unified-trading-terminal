# backend/app/services/market_metrics.py
from __future__ import annotations

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


_OWNED_ASSET_SENTINELS = {"OWNED", "DB", "DB_OWNED", "KNOWN", "HELD"}
_METRIC_ASSET_EXCLUDE = {"", "USD", "FIAT"}


def _is_owned_asset_request(assets: Optional[Any]) -> bool:
    if not isinstance(assets, str):
        return False
    return assets.strip().upper() in _OWNED_ASSET_SENTINELS


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


def _db_owned_asset_cap(default: int = 80) -> int:
    raw = _env_first("UTT_MARKET_METRICS_OWNED_ASSET_CAP", "MARKET_METRICS_OWNED_ASSET_CAP")
    try:
        n = int(float(raw)) if str(raw or "").strip() else int(default)
    except Exception:
        n = int(default)
    return max(5, min(250, n))


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


def _db_owned_metric_assets(limit: int = 250) -> Tuple[List[str], str]:
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

    asset_cap = min(max(1, int(limit)), _db_owned_asset_cap(80))
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


def _summary_error_ttl_s() -> int:
    return _clamp_int(_env_first("UTT_MARKET_METRICS_ERROR_TTL_S"), 10, 300, 30)


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


def _fetch_coingecko_markets(ids: Sequence[str], limit: int) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
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
        "page": "1",
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


def _row_from_cg(asset: str, meta: Dict[str, str], raw: Dict[str, Any], updated_at: str) -> Dict[str, Any]:
    warnings: List[str] = []
    if raw.get("_utt_cache_stale"):
        age_s = _num(raw.get("_utt_cache_age_s")) or 0
        warnings.append(f"Using stale CoinGecko cache ({int(age_s // 60)}m old).")

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
    limit: int = 250,
    ttl_s: int = 300,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    clean_limit = _clamp_int(limit, 1, 250, 250)
    clean_ttl = _clamp_int(ttl_s, 10, 3600, 300)

    if _is_owned_asset_request(assets):
        clean_assets, asset_source = _db_owned_metric_assets(limit=clean_limit)
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

    # Window-owned requests can discover many legacy/dust/tracked symbols. Keep
    # the default window payload fast/useful by hiding unmapped rows unless an
    # unmapped asset was explicitly requested through include_assets.
    if _is_owned_asset_request(assets) and not _env_bool("UTT_MARKET_METRICS_SHOW_UNMAPPED_OWNED", False):
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

    cache_key = "summary:" + asset_source + ":" + ",".join(clean_assets) + f":{clean_limit}"
    if not force_refresh:
        cached = _cache_get(cache_key)
        if cached is not None:
            return {**cached, "cache": "hit"}

    updated_at = _utc_now_iso()
    ids: List[str] = []
    id_to_symbol: Dict[str, str] = {}

    explicit_discovery_assets = set(include_clean)
    if not _is_owned_asset_request(assets):
        explicit_discovery_assets.update(clean_assets)

    for asset in clean_assets:
        meta = _coingecko_meta_for_symbol(asset, allow_discovery=str(asset or "").strip().upper() in explicit_discovery_assets)
        cg_id = meta.get("id") if meta else ""
        if cg_id:
            ids.append(cg_id)
            id_to_symbol[cg_id] = asset

    fresh_cached = _cg_raw_cache_rows(ids, max_age_s=clean_ttl, allow_stale=False)
    missing_ids = [cg_id for cg_id in ids if cg_id not in fresh_cached]

    markets: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    if missing_ids:
        markets, errors = _fetch_coingecko_markets(missing_ids, clean_limit)
        if markets:
            _cg_raw_cache_update(markets)

    by_id: Dict[str, Dict[str, Any]] = {str(row.get("id") or ""): row for row in fresh_cached.values() if row.get("id")}
    by_id.update({str(row.get("id") or ""): row for row in markets if row.get("id")})

    if errors:
        stale_cached = _cg_raw_cache_rows(ids, max_age_s=clean_ttl, allow_stale=True)
        for cg_id, row in stale_cached.items():
            by_id.setdefault(cg_id, row)

    items: List[Dict[str, Any]] = []
    for asset in clean_assets:
        if asset == "UTTT":
            items.append(_uttt_placeholder(updated_at))
            continue

        meta = _coingecko_meta_for_symbol(asset, allow_discovery=str(asset or "").strip().upper() in explicit_discovery_assets)
        cg_id = meta.get("id") if meta else ""
        raw = by_id.get(cg_id)
        if meta and raw:
            items.append(_row_from_cg(asset, meta, raw, updated_at))
            continue

        if meta and not raw:
            items.append(_missing_placeholder(asset, updated_at, f"CoinGecko row not returned for {cg_id}."))
        else:
            items.append(_missing_placeholder(asset, updated_at, "No market_metrics source mapping exists for this asset."))

    payload = {
        "ok": not bool(errors),
        "updated_at": updated_at,
        "ttl_s": clean_ttl,
        "asset_source": asset_source,
        "assets": clean_assets,
        "include_assets": include_clean,
        "items": items,
        "errors": errors,
        "cache": "miss",
    }

    has_real_market_rows = any(
        _num(row.get("price_usd")) is not None
        or _num(row.get("market_cap_usd")) is not None
        or _num(row.get("volume_24h_usd")) is not None
        for row in items
        if isinstance(row, dict) and row.get("source_kind") != "missing"
    )
    _cache_set(cache_key, payload, clean_ttl if has_real_market_rows else _summary_error_ttl_s())
    return payload
