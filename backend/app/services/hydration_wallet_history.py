# backend/app/services/hydration_wallet_history.py

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx
from sqlalchemy import select, or_, func
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..config import settings
from ..models import WalletAddress, WalletAddressTx, AssetDeposit, AssetWithdrawal, TokenRegistry


_HYDRATION_HISTORY_ASSETS = {"HDX", "DOT", "UTTT", "USDT", "USDC", "HOLLAR"}
_HYDRATION_HISTORY_NETWORK_HINTS = {
    "hydration",
    "hydradx",
    "polkadot_hydration",
    "polkadot-hydration",
    "hydration_mainnet",
    "hydration-mainnet",
}
_DEFAULT_SUBSCAN_TRANSFERS_URL = "https://hydration.api.subscan.io/api/v2/scan/transfers"
_DEFAULT_TIMEOUT_S = 20.0

# Conservative fallback used only for diagnostics/previews. Token Registry and
# env JSON are preferred before these defaults. The cache/write path still keeps
# integer-only provider amounts untrusted unless explicitly overridden.
_HYDRATION_HISTORY_DECIMALS_FALLBACK = {
    "HDX": 12,
    "DOT": 10,
    "UTTT": 6,
    "USDT": 6,
    "USDC": 6,
    "HOLLAR": 12,
}


def _json_map_safe(raw: Any) -> Dict[str, Any]:
    try:
        if isinstance(raw, dict):
            return dict(raw)
        s = str(raw or "").strip()
        if not s:
            return {}
        data = json.loads(s)
        return dict(data) if isinstance(data, dict) else {}
    except Exception:
        return {}


def _clean_str(x: Any) -> str:
    return str(x or "").strip()


def _norm_asset(x: Any) -> str:
    return _clean_str(x).upper()


def _norm_network(x: Any) -> str:
    return _clean_str(x).lower().replace("_", "-")


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip().replace(",", "")
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _parse_dt(x: Any) -> Optional[datetime]:
    if x is None:
        return None
    try:
        if isinstance(x, datetime):
            return x.replace(tzinfo=None)
        if isinstance(x, (int, float)):
            # Substrate explorers commonly use seconds, sometimes milliseconds.
            v = float(x)
            if v > 10_000_000_000:
                v = v / 1000.0
            return datetime.utcfromtimestamp(v)
        s = str(x).strip()
        if not s:
            return None
        if s.isdigit():
            return _parse_dt(int(s))
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _provider_name(provider: Optional[str]) -> str:
    p = _clean_str(provider or os.getenv("UTT_HYDRATION_HISTORY_PROVIDER") or "none").lower()
    aliases = {
        "": "none",
        "off": "none",
        "disabled": "none",
        "subscan_hydration": "subscan",
        "hydration_subscan": "subscan",
    }
    return aliases.get(p, p)


def _subscan_url() -> str:
    return _clean_str(
        os.getenv("UTT_HYDRATION_HISTORY_SUBSCAN_TRANSFERS_URL")
        or os.getenv("UTT_HYDRATION_SUBSCAN_TRANSFERS_URL")
        or _DEFAULT_SUBSCAN_TRANSFERS_URL
    )


def _history_api_key() -> str:
    # Env first for explicit local testing.
    for k in (
        "UTT_HYDRATION_HISTORY_API_KEY",
        "UTT_HYDRATION_SUBSCAN_API_KEY",
        "UTT_SUBSCAN_API_KEY",
        "SUBSCAN_API_KEY",
    ):
        v = _clean_str(os.getenv(k))
        if v:
            return v

    # Profile/API Keys fallback. These are provider-style app keys, not exchange trading keys.
    for venue in ("subscan_hydration", "hydration_subscan", "subscan"):
        try:
            bundle = settings._vault_latest_bundle(venue)
            if not bundle:
                bundle = settings._vault_latest_bundle_any_username(venue)
            if isinstance(bundle, dict):
                v = _clean_str(bundle.get("api_key"))
                if v:
                    return v
        except Exception:
            pass
    return ""


def hydration_wallet_history_status(*, provider: Optional[str] = None) -> Dict[str, Any]:
    p = _provider_name(provider)
    api_key = _history_api_key() if p == "subscan" else ""
    configured = p in {"subscan"}
    return {
        "ok": True,
        "provider": p,
        "configured": bool(configured),
        "dry_run_default": True,
        "mutates_by_default": False,
        "subscan": {
            "url": _subscan_url(),
            "has_api_key": bool(api_key),
            "api_key_sources": [
                "UTT_HYDRATION_HISTORY_API_KEY",
                "UTT_HYDRATION_SUBSCAN_API_KEY",
                "UTT_SUBSCAN_API_KEY",
                "SUBSCAN_API_KEY",
                "Profile/API Keys: subscan_hydration | hydration_subscan | subscan",
            ],
        } if p == "subscan" else None,
        "supported_assets": sorted(_HYDRATION_HISTORY_ASSETS),
        "supported_network_hints": sorted(_HYDRATION_HISTORY_NETWORK_HINTS),
        "note": (
            "Set UTT_HYDRATION_HISTORY_PROVIDER=subscan to enable the optional indexer path. "
            "The default provider=none performs no outbound indexer calls."
        ),
    }


def _is_hydration_wallet_address(row: WalletAddress) -> Tuple[bool, str]:
    asset = _norm_asset(getattr(row, "asset", None))
    network = _norm_network(getattr(row, "network", None))
    network_raw = _clean_str(getattr(row, "network", None)).lower()

    if network in _HYDRATION_HISTORY_NETWORK_HINTS or network_raw in _HYDRATION_HISTORY_NETWORK_HINTS:
        return True, "network_hint"
    if "hydration" in network or "hydradx" in network or "hydration" in network_raw or "hydradx" in network_raw:
        return True, "network_contains_hydration"
    if asset in {"HDX", "HOLLAR"}:
        return True, "asset_hint"
    # UTTT/USDT/DOT may also exist outside Hydration, so require a network hint for those.
    return False, "not_hydration_network"


def _registered_hydration_addresses(db: Session, *, address_id: Optional[str]) -> Tuple[List[WalletAddress], List[Dict[str, Any]]]:
    stmt = select(WalletAddress).order_by(WalletAddress.created_at.desc())
    if address_id:
        stmt = stmt.where(WalletAddress.id == str(address_id).strip())
    rows = db.execute(stmt).scalars().all()

    supported: List[WalletAddress] = []
    skipped: List[Dict[str, Any]] = []
    for r in rows:
        ok, reason = _is_hydration_wallet_address(r)
        if ok:
            supported.append(r)
        else:
            skipped.append({
                "id": str(getattr(r, "id", "")),
                "asset": getattr(r, "asset", None),
                "network": getattr(r, "network", None),
                "address": getattr(r, "address", None),
                "reason": reason,
            })
    return supported, skipped


def _subscan_headers() -> Dict[str, str]:
    headers = {"accept": "application/json", "content-type": "application/json"}
    api_key = _history_api_key()
    if api_key:
        headers["X-API-Key"] = api_key
    return headers


def _extract_transfer_rows(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    data = payload.get("data") if isinstance(payload.get("data"), (dict, list)) else payload
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if not isinstance(data, dict):
        return []
    for key in ("transfers", "list", "rows", "items"):
        v = data.get(key)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]
    return []


def _pick_first(row: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in row and row.get(k) not in (None, ""):
            return row.get(k)
    return None


def _nested_symbol(row: Dict[str, Any]) -> Optional[str]:
    for k in ("asset", "asset_info", "token", "token_info", "currency"):
        v = row.get(k)
        if isinstance(v, dict):
            s = _pick_first(v, ["symbol", "asset_symbol", "token_symbol", "currency", "name"])
            if s:
                return _norm_asset(s)
        elif isinstance(v, str) and v.strip() and len(v.strip()) <= 16:
            return _norm_asset(v)
    return None


def _row_raw_amount(row: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    for k in ("amount", "balance", "value", "quantity"):
        if k in row and row.get(k) not in (None, ""):
            return str(row.get(k)).strip(), k
    return None, None


def _row_amount_v2(row: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """Return likely atomic/raw amount companion fields from Subscan rows.

    Subscan transfer rows commonly expose:
      - amount: display/UI amount, sometimes as an integer string like "1"
      - amount_v2: atomic/base-unit amount

    The original conservative parser marks integer `amount` values untrusted.
    This diagnostic helper lets us verify whether integer-looking `amount`
    is actually a display amount by checking it against amount_v2 + known decimals.
    """
    for k in ("amount_v2", "amount_raw", "raw_amount", "amount_atomic", "balance_v2", "value_v2", "quantity_v2"):
        if k in row and row.get(k) not in (None, ""):
            return str(row.get(k)).strip(), k
    return None, None


def _amount_v2_validation_preview(
    *,
    raw_amount: Optional[str],
    amount_v2: Optional[str],
    decimals: Optional[int],
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "amount_v2_scaled_preview": None,
        "amount_matches_amount_v2_scaled": None,
        "recommended_amount": None,
        "recommended_amount_source": None,
        "recommended_interpretation": "unresolved_integer_amount",
    }
    if amount_v2 is None or decimals is None:
        return out

    scaled_v2 = _scaled_preview_amount(amount_v2, decimals)
    out["amount_v2_scaled_preview"] = scaled_v2

    raw_f = _safe_float(raw_amount)
    if raw_f is None or scaled_v2 is None:
        if scaled_v2 is not None:
            out["recommended_amount"] = scaled_v2
            out["recommended_amount_source"] = "amount_v2_scaled"
            out["recommended_interpretation"] = "use_amount_v2_scaled_if_importing"
        return out

    try:
        tolerance = max(1e-12, abs(float(raw_f)) * 1e-9, abs(float(scaled_v2)) * 1e-9)
        matches = abs(float(raw_f) - float(scaled_v2)) <= tolerance
    except Exception:
        matches = False

    out["amount_matches_amount_v2_scaled"] = bool(matches)
    if matches:
        out["recommended_amount"] = float(raw_f)
        out["recommended_amount_source"] = "amount_display_integer_validated_by_amount_v2"
        out["recommended_interpretation"] = "amount_field_is_display_units_even_though_integer"
    else:
        out["recommended_amount"] = scaled_v2
        out["recommended_amount_source"] = "amount_v2_scaled"
        out["recommended_interpretation"] = "amount_field_does_not_match_amount_v2_scaled"
    return out



def _trusted_amount_from_amount_v2(
    *,
    db: Optional[Session],
    row: Dict[str, Any],
    asset: Any,
) -> Tuple[Optional[float], Optional[str], Optional[Dict[str, Any]]]:
    """Return a trusted amount for integer-looking Subscan amount rows.

    Subscan transfer rows can expose:
      - amount: display amount, sometimes integer-looking ("1", "50", "100")
      - amount_v2: atomic/base-unit amount

    We only trust this path when `amount_v2` plus verified decimals produces a
    usable amount.  If amount == amount_v2 / 10^decimals, keep amount as display
    units.  If amount does not match but amount_v2 can be scaled, use the scaled
    amount_v2 value.  This is intentionally opt-in from the ingest endpoint.
    """
    raw_amount, raw_amount_key = _row_raw_amount(row)
    raw_amount_v2, raw_amount_v2_key = _row_amount_v2(row)
    if not raw_amount_v2:
        return None, None, None

    row_decimals, row_decimal_key = _row_decimal_hint(row)
    known_decimals, known_source, known_meta = _known_decimals_for_asset(db, asset)
    chosen_decimals = row_decimals if row_decimals is not None else known_decimals
    chosen_source = f"provider_row:{row_decimal_key}" if row_decimals is not None else known_source
    if chosen_decimals is None:
        return None, None, None

    preview = _amount_v2_validation_preview(
        raw_amount=raw_amount,
        amount_v2=raw_amount_v2,
        decimals=chosen_decimals,
    )
    rec_amount = _safe_float(preview.get("recommended_amount"))
    rec_source = _clean_str(preview.get("recommended_amount_source"))
    if rec_amount is None or rec_amount <= 0:
        return None, None, None
    if rec_source not in {"amount_display_integer_validated_by_amount_v2", "amount_v2_scaled"}:
        return None, None, None

    meta = {
        "raw_amount": raw_amount,
        "raw_amount_key": raw_amount_key,
        "raw_amount_v2": raw_amount_v2,
        "raw_amount_v2_key": raw_amount_v2_key,
        "known_decimals": known_decimals,
        "known_decimals_source": known_source,
        "known_decimals_meta": known_meta,
        "provider_row_decimals": row_decimals,
        "provider_row_decimal_key": row_decimal_key,
        "chosen_decimals": chosen_decimals,
        "chosen_decimals_source": chosen_source,
        **preview,
    }
    return float(rec_amount), rec_source, meta


def _row_decimal_hint(row: Dict[str, Any]) -> Tuple[Optional[int], Optional[str]]:
    for k in ("decimals", "decimal", "asset_decimals", "token_decimals"):
        if k not in row or row.get(k) in (None, ""):
            continue
        try:
            d = int(row.get(k))
            if 0 <= d <= 30:
                return d, k
        except Exception:
            continue
    for parent in ("asset", "asset_info", "token", "token_info", "currency"):
        v = row.get(parent)
        if not isinstance(v, dict):
            continue
        for k in ("decimals", "decimal", "asset_decimals", "token_decimals"):
            if k not in v or v.get(k) in (None, ""):
                continue
            try:
                d = int(v.get(k))
                if 0 <= d <= 30:
                    return d, f"{parent}.{k}"
            except Exception:
                continue
    return None, None


def _registry_decimals_for_asset(db: Optional[Session], asset: Any) -> Tuple[Optional[int], Optional[str], Optional[Dict[str, Any]]]:
    if db is None:
        return None, None, None
    sym = _norm_asset(asset)
    if not sym:
        return None, None, None
    try:
        rows = db.query(TokenRegistry).filter(TokenRegistry.symbol == sym).all()
    except Exception:
        return None, None, None

    def score(row: TokenRegistry) -> int:
        chain = _clean_str(getattr(row, "chain", None)).lower()
        venue = _clean_str(getattr(row, "venue", None)).lower()
        if chain == "polkadot" and venue == "polkadot_hydration":
            return 0
        if chain in {"polkadot", "hydration"} and venue in {"polkadot_hydration", "hydration"}:
            return 1
        if chain in {"polkadot", "hydration"} and not venue:
            return 2
        if chain == "global" and not venue:
            return 3
        return 9

    best = None
    best_score = 999
    for row in rows or []:
        try:
            d = int(getattr(row, "decimals", None))
        except Exception:
            continue
        if d < 0 or d > 30:
            continue
        sc = score(row)
        if sc < best_score:
            best_score = sc
            best = row

    if best is None:
        return None, None, None

    try:
        d = int(getattr(best, "decimals", None))
    except Exception:
        return None, None, None
    return d, "token_registry", {
        "chain": getattr(best, "chain", None),
        "venue": getattr(best, "venue", None),
        "address": getattr(best, "address", None),
        "label": getattr(best, "label", None),
    }


def _known_decimals_for_asset(db: Optional[Session], asset: Any) -> Tuple[Optional[int], Optional[str], Optional[Dict[str, Any]]]:
    sym = _norm_asset(asset)
    if not sym:
        return None, None, None

    dec, src, meta = _registry_decimals_for_asset(db, sym)
    if dec is not None:
        return dec, src, meta

    env_map = _json_map_safe(os.getenv("UTT_HYDRATION_DECIMALS_JSON") or os.getenv("HYDRATION_DECIMALS_JSON") or "")
    if sym in env_map:
        try:
            d = int(env_map.get(sym))
            if 0 <= d <= 30:
                return d, "env:UTT_HYDRATION_DECIMALS_JSON", None
        except Exception:
            pass

    if sym in _HYDRATION_HISTORY_DECIMALS_FALLBACK:
        return int(_HYDRATION_HISTORY_DECIMALS_FALLBACK[sym]), "builtin_diagnostic_fallback", None

    return None, None, None


def _scaled_preview_amount(raw_amount: Optional[str], decimals: Optional[int]) -> Optional[float]:
    if raw_amount is None or decimals is None:
        return None
    try:
        s = str(raw_amount).strip().replace(",", "")
        if not s or "." in s:
            return None
        return float(int(s) / (10 ** int(decimals)))
    except Exception:
        return None


def _event_hints_from_row(row: Dict[str, Any]) -> Dict[str, Any]:
    keys = [
        "module", "pallet", "section", "method", "event", "event_id", "event_index",
        "call_module", "call_name", "extrinsic_module", "extrinsic_call", "extrinsic_index",
        "event_module", "event_name", "success",
    ]
    out: Dict[str, Any] = {}
    for k in keys:
        if k in row and row.get(k) not in (None, ""):
            out[k] = row.get(k)
    for parent in ("event", "extrinsic", "call", "asset", "token", "currency"):
        v = row.get(parent)
        if isinstance(v, dict):
            small = {kk: vv for kk, vv in list(v.items())[:12] if vv not in (None, "")}
            if small:
                out[parent] = small
    return out


def _classify_hydration_history_row(row: Dict[str, Any], cand: Dict[str, Any]) -> str:
    try:
        hay = json.dumps(row, default=str).lower()[:6000]
    except Exception:
        hay = str(row).lower()[:6000]
    if any(x in hay for x in ("staking", "reward", "claim_rewards", "claimrewards", "paidout")):
        return "possible_staking_or_reward"
    if any(x in hay for x in ("transfer", "tokens.transfer", "balances.transfer")):
        return "transfer"
    if _clean_str(cand.get("direction")).lower() in {"in", "out"}:
        return "wallet_transfer_unknown_event"
    return "unknown"


def _record_untrusted_amount_example(
    out: Dict[str, Any],
    *,
    db: Optional[Session],
    candidate: Dict[str, Any],
    raw_row: Dict[str, Any],
    address_id: str,
    page: int,
    limit: int,
) -> None:
    diag = out.setdefault("untrusted_amount_diagnostics", {})
    diag.setdefault("note", "These rows are diagnostic only. They are not cached/materialized unless trust_provider_amounts=true or a later parser patch explicitly trusts amount_v2-validated integer display amounts.")
    diag.setdefault("by_asset", {})
    examples = diag.setdefault("examples", [])

    asset = _norm_asset(candidate.get("asset")) or "UNKNOWN"
    bucket = diag["by_asset"].setdefault(asset, {"rows": 0, "in": 0, "out": 0, "amount_sources": {}})
    bucket["rows"] = int(bucket.get("rows") or 0) + 1
    direction = _clean_str(candidate.get("direction")).lower()
    if direction == "in":
        bucket["in"] = int(bucket.get("in") or 0) + 1
    elif direction == "out":
        bucket["out"] = int(bucket.get("out") or 0) + 1
    amount_source = _clean_str(candidate.get("amount_source") or "unknown") or "unknown"
    bucket["amount_sources"][amount_source] = int(bucket["amount_sources"].get(amount_source) or 0) + 1

    if len(examples) >= max(1, int(limit or 20)):
        return

    raw_amount, raw_amount_key = _row_raw_amount(raw_row)
    raw_amount_v2, raw_amount_v2_key = _row_amount_v2(raw_row)
    row_decimals, row_decimal_key = _row_decimal_hint(raw_row)
    known_decimals, known_source, known_meta = _known_decimals_for_asset(db, asset)
    chosen_decimals = row_decimals if row_decimals is not None else known_decimals
    chosen_source = f"provider_row:{row_decimal_key}" if row_decimals is not None else known_source
    scaled_preview = _scaled_preview_amount(raw_amount, chosen_decimals)
    amount_v2_preview = _amount_v2_validation_preview(
        raw_amount=raw_amount,
        amount_v2=raw_amount_v2,
        decimals=chosen_decimals,
    )
    txid = _clean_str(candidate.get("txid"))

    examples.append({
        "kind": "untrusted_amount_scaling_example",
        "address_id": str(address_id),
        "page": int(page),
        "asset": asset,
        "direction": direction,
        "raw_integer_amount": raw_amount,
        "raw_amount_key": raw_amount_key,
        "raw_amount_v2": raw_amount_v2,
        "raw_amount_v2_key": raw_amount_v2_key,
        "current_untrusted_amount": candidate.get("amount"),
        "amount_source": candidate.get("amount_source"),
        "provider_row_decimals": row_decimals,
        "provider_row_decimal_key": row_decimal_key,
        "known_decimals": known_decimals,
        "known_decimals_source": known_source,
        "known_decimals_meta": known_meta,
        "scaled_preview_amount": scaled_preview,
        "scale_preview_source": chosen_source,
        **amount_v2_preview,
        "classification": _classify_hydration_history_row(raw_row, candidate),
        "event_hints": _event_hints_from_row(raw_row),
        "row_keys": list(raw_row.keys())[:40],
        "txid_preview": (f"{txid[:10]}…{txid[-8:]}" if len(txid) > 24 else txid),
    })


def _amount_from_row(row: Dict[str, Any]) -> Tuple[Optional[float], bool, Optional[str]]:
    # Prefer explicitly human/display decimal fields.
    for k in ("amount_decimal", "amount_human", "amount_ui", "amount_display", "display_amount", "amount_displayed"):
        v = _safe_float(row.get(k))
        if v is not None:
            return v, True, k

    raw = _pick_first(row, ["amount", "balance", "value", "quantity"])
    raw_f = _safe_float(raw)
    if raw_f is None:
        return None, False, None

    raw_s = str(raw).strip()
    if "." in raw_s:
        return raw_f, True, "amount_decimal_string"

    # If provider gives decimals in the row, scale integer amounts.
    decimals_raw = _pick_first(row, ["decimals", "decimal", "asset_decimals", "token_decimals"])
    try:
        decimals = int(decimals_raw) if decimals_raw is not None else None
    except Exception:
        decimals = None
    if decimals is not None and 0 <= decimals <= 30:
        return float(raw_f / (10 ** decimals)), True, "amount_scaled_by_row_decimals"

    # Conservative default: do not assume integer units are already UI units.
    return raw_f, False, "amount_unscaled_integer"


def _parse_transfer_candidate(
    row: Dict[str, Any],
    *,
    address: str,
    fallback_asset: str,
    raw_debug: bool,
    db: Optional[Session] = None,
    trust_amount_v2_validated: bool = False,
) -> Optional[Dict[str, Any]]:
    addr = _clean_str(address)
    if not addr:
        return None

    from_addr = _clean_str(_pick_first(row, ["from", "from_address", "sender", "src", "source"]))
    to_addr = _clean_str(_pick_first(row, ["to", "to_address", "recipient", "dest", "destination"]))
    from_l = from_addr.lower()
    to_l = to_addr.lower()
    addr_l = addr.lower()

    if to_l == addr_l:
        direction = "in"
        counterparty = from_addr or None
    elif from_l == addr_l:
        direction = "out"
        counterparty = to_addr or None
    else:
        return None

    txid = _clean_str(_pick_first(row, ["hash", "tx_hash", "extrinsic_hash", "extrinsic_index", "event_index", "id"]))
    if not txid:
        return None

    asset = _nested_symbol(row) or _norm_asset(_pick_first(row, ["symbol", "asset_symbol", "token_symbol", "currency"]) or fallback_asset)
    amount, amount_trusted, amount_source = _amount_from_row(row)
    amount_v2_validation = None
    if (
        trust_amount_v2_validated
        and not amount_trusted
        and amount_source == "amount_unscaled_integer"
    ):
        v2_amount, v2_source, v2_meta = _trusted_amount_from_amount_v2(db=db, row=row, asset=asset)
        if v2_amount is not None and v2_source:
            amount = float(v2_amount)
            amount_trusted = True
            amount_source = v2_source
            amount_v2_validation = v2_meta
    tx_time = _parse_dt(_pick_first(row, ["block_timestamp", "block_time", "timestamp", "time", "datetime", "created_at"]))
    fee = _safe_float(_pick_first(row, ["fee", "fee_amount", "fee_ui"]))

    raw_small = None
    if raw_debug:
        # Keep examples useful but bounded.
        raw_small = {k: row.get(k) for k in list(row.keys())[:30]}

    return {
        "txid": txid,
        "direction": direction,
        "asset": asset,
        "amount": amount,
        "amount_trusted": bool(amount_trusted),
        "amount_source": amount_source,
        "amount_v2_validation": amount_v2_validation,
        "fee": fee,
        "tx_time": tx_time,
        "counterparty": counterparty,
        "from": from_addr or None,
        "to": to_addr or None,
        "raw_debug": raw_small,
        "raw": row,
    }


async def _fetch_subscan_transfers(address: str, *, limit: int, page: int = 0) -> Dict[str, Any]:
    url = _subscan_url()
    timeout_s = float(os.getenv("UTT_HYDRATION_HISTORY_TIMEOUT_S") or _DEFAULT_TIMEOUT_S)
    page_i = max(0, int(page or 0))
    body = {
        "address": str(address),
        "row": int(limit),
        "page": page_i,
    }
    # Optional user overrides for indexer variants without code changes.
    extra_raw = os.getenv("UTT_HYDRATION_HISTORY_SUBSCAN_EXTRA_JSON") or ""
    if extra_raw.strip():
        try:
            import json
            extra = json.loads(extra_raw)
            if isinstance(extra, dict):
                body.update(extra)
        except Exception:
            pass

    async with httpx.AsyncClient(timeout=timeout_s) as client:
        r = await client.post(url, json=body, headers=_subscan_headers())
    try:
        data = r.json() if r.content else {}
    except Exception:
        data = {"non_json_body": (r.text or "")[:1000]}
    return {
        "ok": bool(r.status_code < 400),
        "status": int(r.status_code),
        "url": url,
        "request": {"address": str(address), "row": int(limit), "page": page_i},
        "data": data,
    }


def _skip_inc(out: Dict[str, Any], reason: str) -> None:
    out.setdefault("skip_reasons", {})[reason] = int(out.setdefault("skip_reasons", {}).get(reason) or 0) + 1


def _example(out: Dict[str, Any], item: Dict[str, Any], limit: int = 20) -> None:
    examples = out.setdefault("examples", [])
    if len(examples) < limit:
        examples.append(item)


def _coverage_asset_bucket(out: Dict[str, Any], asset: Any) -> Dict[str, Any]:
    cov = out.setdefault("coverage", {})
    asset_summary = cov.setdefault("asset_summary", {})
    a = _norm_asset(asset) or "UNKNOWN"
    row = asset_summary.setdefault(
        a,
        {
            "rows": 0,
            "candidates": 0,
            "in": 0,
            "out": 0,
            "trusted": 0,
            "untrusted": 0,
            "cached": 0,
            "existing_cached": 0,
            "cache_skipped": 0,
        },
    )
    return row


def _coverage_touch_candidate(out: Dict[str, Any], cand: Dict[str, Any]) -> None:
    row = _coverage_asset_bucket(out, cand.get("asset"))
    row["rows"] = int(row.get("rows") or 0) + 1
    row["candidates"] = int(row.get("candidates") or 0) + 1

    direction = _clean_str(cand.get("direction")).lower()
    if direction == "in":
        row["in"] = int(row.get("in") or 0) + 1
    elif direction == "out":
        row["out"] = int(row.get("out") or 0) + 1

    if cand.get("amount_trusted"):
        row["trusted"] = int(row.get("trusted") or 0) + 1
    else:
        row["untrusted"] = int(row.get("untrusted") or 0) + 1

    source = _clean_str(cand.get("amount_source") or "unknown") or "unknown"
    sources = out.setdefault("coverage", {}).setdefault("amount_sources", {})
    sources[source] = int(sources.get(source) or 0) + 1


def _coverage_cache_state(out: Dict[str, Any], cand: Dict[str, Any], state: str, reason: Optional[str] = None) -> None:
    row = _coverage_asset_bucket(out, cand.get("asset"))
    if state == "cached":
        row["cached"] = int(row.get("cached") or 0) + 1
    elif state == "existing":
        row["existing_cached"] = int(row.get("existing_cached") or 0) + 1
    else:
        row["cache_skipped"] = int(row.get("cache_skipped") or 0) + 1
        r = _clean_str(reason or "cache_skipped") or "cache_skipped"
        skipped = out.setdefault("coverage", {}).setdefault("skipped_by_asset", {})
        asset = _norm_asset(cand.get("asset")) or "UNKNOWN"
        asset_row = skipped.setdefault(asset, {})
        asset_row[r] = int(asset_row.get(r) or 0) + 1


def _candidate_to_raw(candidate: Dict[str, Any], provider: str, address_row: WalletAddress) -> Dict[str, Any]:
    return {
        "source_type": "HYDRATION_WALLET_HISTORY",
        "provider": provider,
        "wallet_address": getattr(address_row, "address", None),
        "wallet_address_id": str(getattr(address_row, "id", "")),
        "wallet_address_label": getattr(address_row, "label", None),
        "network": getattr(address_row, "network", None),
        "asset_detected": candidate.get("asset"),
        "direction": candidate.get("direction"),
        "counterparty": candidate.get("counterparty"),
        "amount_source": candidate.get("amount_source"),
        "amount_trusted": candidate.get("amount_trusted"),
        "amount_v2_validation": candidate.get("amount_v2_validation"),
        "raw_provider_row": candidate.get("raw"),
    }


def _existing_wallet_tx(db: Session, address_id: str, txid: str, direction: str) -> Optional[WalletAddressTx]:
    return db.execute(
        select(WalletAddressTx).where(
            WalletAddressTx.wallet_address_id == str(address_id),
            WalletAddressTx.txid == str(txid),
            WalletAddressTx.direction == str(direction),
        )
    ).scalars().first()


def _cache_wallet_tx(
    db: Session,
    *,
    address_row: WalletAddress,
    candidate: Dict[str, Any],
    provider: str,
    trust_provider_amounts: bool,
) -> Tuple[str, Optional[str]]:
    txid = _clean_str(candidate.get("txid"))
    direction = _clean_str(candidate.get("direction"))
    if not txid or direction not in {"in", "out"}:
        return "skipped", "invalid_txid_or_direction"

    existing = _existing_wallet_tx(db, str(address_row.id), txid, direction)
    if existing:
        return "existing", str(existing.id)

    amount = _safe_float(candidate.get("amount"))
    if amount is None or amount <= 0:
        return "skipped", "invalid_amount"
    if not candidate.get("amount_trusted") and not trust_provider_amounts:
        return "skipped", "untrusted_amount_scaling"

    row = WalletAddressTx(
        wallet_address_id=str(address_row.id),
        asset=_norm_asset(candidate.get("asset") or address_row.asset),
        network=getattr(address_row, "network", None) or "hydration",
        address=getattr(address_row, "address", None) or "",
        txid=txid,
        direction=direction,
        amount=float(amount),
        fee=_safe_float(candidate.get("fee")),
        tx_time=candidate.get("tx_time") if isinstance(candidate.get("tx_time"), datetime) else None,
        counterparty=candidate.get("counterparty"),
        raw=_candidate_to_raw(candidate, provider, address_row),
    )
    db.add(row)
    try:
        db.commit()
        db.refresh(row)
    except IntegrityError:
        db.rollback()
        existing2 = _existing_wallet_tx(db, str(address_row.id), txid, direction)
        if existing2:
            return "existing", str(existing2.id)
        return "skipped", "integrity_error"
    except Exception as e:
        db.rollback()
        return "skipped", f"cache_error:{type(e).__name__}"
    return "cached", str(row.id)




def _wallet_materialization_fields(address_row: WalletAddress) -> Tuple[str, str, str]:
    """Return (venue, wallet_id, source) for deposit/withdrawal materialization."""
    wa_wallet_id = _clean_str(getattr(address_row, "wallet_id", None)).lower()
    venue = wa_wallet_id or "self_custody"
    wallet_id = "wallet_address"
    source = f"WALLET_ADDR:{_clean_str(getattr(address_row, 'address', None))}"
    return venue, wallet_id, source


def _wallet_tx_is_hydration_history(tx: WalletAddressTx) -> bool:
    raw = getattr(tx, "raw", None) or {}
    if isinstance(raw, dict) and raw.get("source_type") == "HYDRATION_WALLET_HISTORY":
        return True
    net = _norm_network(getattr(tx, "network", None))
    return ("hydration" in net) or ("hydradx" in net)


def _existing_deposit_for_tx(db: Session, *, venue: str, wallet_id: str, txid: str) -> Optional[AssetDeposit]:
    if not txid:
        return None
    return db.execute(
        select(AssetDeposit).where(
            AssetDeposit.venue == venue,
            AssetDeposit.wallet_id == wallet_id,
            AssetDeposit.txid == txid,
        )
    ).scalars().first()


def _existing_withdrawal_for_tx(db: Session, *, venue: str, wallet_id: str, txid: str) -> Optional[AssetWithdrawal]:
    if not txid:
        return None
    return db.execute(
        select(AssetWithdrawal).where(
            AssetWithdrawal.venue == venue,
            AssetWithdrawal.wallet_id == wallet_id,
            AssetWithdrawal.txid == txid,
        )
    ).scalars().first()


def _materialization_raw(tx: WalletAddressTx, address_row: WalletAddress, *, kind: str) -> Dict[str, Any]:
    prev_raw = getattr(tx, "raw", None) if isinstance(getattr(tx, "raw", None), dict) else {}
    return {
        "source_type": "HYDRATION_WALLET_HISTORY",
        "materialized_as": kind,
        "wallet_address": getattr(address_row, "address", None),
        "wallet_address_label": getattr(address_row, "label", None),
        "wallet_address_id": str(getattr(address_row, "id", "")),
        "wallet_address_tx_id": str(getattr(tx, "id", "")),
        "direction": getattr(tx, "direction", None),
        "txid": getattr(tx, "txid", None),
        "provider": (prev_raw or {}).get("provider"),
        "counterparty": getattr(tx, "counterparty", None),
        "amount_source": (prev_raw or {}).get("amount_source"),
        "amount_trusted": (prev_raw or {}).get("amount_trusted"),
        "raw_wallet_tx": prev_raw,
    }


def _materialize_cached_hydration_txs(
    db: Session,
    *,
    address_id: Optional[str] = None,
    limit: int = 100,
    dry_run: bool = True,
) -> Dict[str, Any]:
    """Materialize cached Hydration wallet tx rows into deposits/withdrawals."""
    lim = max(1, min(int(limit or 100), 500))
    # Important: the generic wallet_address_txs table may contain many BTC/SOL/DOGE/etc.
    # rows before the Hydration rows.  Pre-filter by Hydration-like tx/address network
    # hints before applying the materialize limit.
    hydration_filter = or_(
        func.lower(WalletAddressTx.network).like("%hydration%"),
        func.lower(WalletAddressTx.network).like("%hydradx%"),
        func.lower(WalletAddress.network).like("%hydration%"),
        func.lower(WalletAddress.network).like("%hydradx%"),
        func.upper(WalletAddressTx.asset).in_(["HDX", "HOLLAR"]),
        func.upper(WalletAddress.asset).in_(["HDX", "HOLLAR"]),
    )
    stmt = (
        select(WalletAddressTx)
        .join(WalletAddress, WalletAddress.id == WalletAddressTx.wallet_address_id)
        .where(WalletAddressTx.ingested_to_ledger_at.is_(None))
        .where(hydration_filter)
        .order_by(WalletAddressTx.tx_time.asc().nulls_last(), WalletAddressTx.id.asc())
        .limit(lim * 5)
    )
    if address_id:
        stmt = stmt.where(WalletAddressTx.wallet_address_id == str(address_id).strip())

    rows = db.execute(stmt).scalars().all()

    out: Dict[str, Any] = {
        "enabled": True,
        "dry_run": bool(dry_run),
        "considered": 0,
        "eligible": 0,
        "would_write_deposits": 0,
        "would_write_withdrawals": 0,
        "written_deposits": 0,
        "written_withdrawals": 0,
        "existing_deposits": 0,
        "existing_withdrawals": 0,
        "linked_existing": 0,
        "marked_ingested": 0,
        "skip_reasons": {},
        "examples": [],
        "query_filter": "hydration_wallet_history_pre_filtered",
    }

    def minc(reason: str) -> None:
        out["skip_reasons"][reason] = int(out["skip_reasons"].get(reason) or 0) + 1

    def mex(item: Dict[str, Any], cap: int = 12) -> None:
        if len(out["examples"]) < cap:
            out["examples"].append(item)

    processed = 0
    for tx in rows:
        if processed >= lim:
            break
        out["considered"] += 1

        if not _wallet_tx_is_hydration_history(tx):
            minc("not_hydration_wallet_history")
            continue

        direction = _clean_str(getattr(tx, "direction", None)).lower()
        if direction not in {"in", "out"}:
            minc("invalid_direction")
            continue

        amount = _safe_float(getattr(tx, "amount", None))
        if amount is None or amount <= 0:
            minc("invalid_amount")
            continue

        address_row = db.get(WalletAddress, str(getattr(tx, "wallet_address_id", "")))
        if not address_row:
            minc("wallet_address_missing")
            continue

        venue, wallet_id, source = _wallet_materialization_fields(address_row)
        txid = _clean_str(getattr(tx, "txid", None))
        if not txid:
            minc("missing_txid")
            continue

        asset = _norm_asset(getattr(tx, "asset", None))
        if not asset:
            minc("missing_asset")
            continue

        processed += 1
        out["eligible"] += 1
        tx_time = getattr(tx, "tx_time", None) if isinstance(getattr(tx, "tx_time", None), datetime) else datetime.utcnow()

        if direction == "in":
            existing = _existing_deposit_for_tx(db, venue=venue, wallet_id=wallet_id, txid=txid)
            if existing:
                out["existing_deposits"] += 1
                if not dry_run:
                    tx.deposit_id = existing.id
                    tx.ingested_to_ledger_at = datetime.utcnow()
                    db.add(tx)
                    db.commit()
                    out["linked_existing"] += 1
                    out["marked_ingested"] += 1
                mex({"kind": "existing_deposit", "asset": asset, "amount": amount, "tx_time": tx_time.isoformat() if tx_time else None})
                continue

            out["would_write_deposits"] += 1
            mex({"kind": "deposit_candidate", "asset": asset, "amount": amount, "tx_time": tx_time.isoformat() if tx_time else None})
            if dry_run:
                continue

            dep = AssetDeposit(
                venue=venue,
                wallet_id=wallet_id,
                asset=asset,
                qty=float(amount),
                deposit_time=tx_time or datetime.utcnow(),
                txid=txid,
                network=getattr(tx, "network", None) or getattr(address_row, "network", None) or "hydration",
                status="DETECTED",
                source=source,
                raw=_materialization_raw(tx, address_row, kind="deposit"),
            )
            db.add(dep)
            try:
                db.commit()
                db.refresh(dep)
                tx.deposit_id = dep.id
                tx.ingested_to_ledger_at = datetime.utcnow()
                db.add(tx)
                db.commit()
                out["written_deposits"] += 1
                out["marked_ingested"] += 1
            except IntegrityError:
                db.rollback()
                existing2 = _existing_deposit_for_tx(db, venue=venue, wallet_id=wallet_id, txid=txid)
                if existing2:
                    tx.deposit_id = existing2.id
                    tx.ingested_to_ledger_at = datetime.utcnow()
                    db.add(tx)
                    db.commit()
                    out["existing_deposits"] += 1
                    out["linked_existing"] += 1
                    out["marked_ingested"] += 1
                else:
                    minc("deposit_integrity_error")
            except Exception as e:
                db.rollback()
                minc(f"deposit_error:{type(e).__name__}")

        else:
            existing = _existing_withdrawal_for_tx(db, venue=venue, wallet_id=wallet_id, txid=txid)
            if existing:
                out["existing_withdrawals"] += 1
                if not dry_run:
                    tx.withdrawal_id = existing.id
                    tx.ingested_to_ledger_at = datetime.utcnow()
                    db.add(tx)
                    db.commit()
                    out["linked_existing"] += 1
                    out["marked_ingested"] += 1
                mex({"kind": "existing_withdrawal", "asset": asset, "amount": amount, "tx_time": tx_time.isoformat() if tx_time else None})
                continue

            out["would_write_withdrawals"] += 1
            mex({"kind": "withdrawal_candidate", "asset": asset, "amount": amount, "tx_time": tx_time.isoformat() if tx_time else None})
            if dry_run:
                continue

            wd = AssetWithdrawal(
                venue=venue,
                wallet_id=wallet_id,
                asset=asset,
                qty=float(amount),
                withdraw_time=tx_time or datetime.utcnow(),
                txid=txid,
                chain="hydration",
                network=getattr(tx, "network", None) or getattr(address_row, "network", None) or "hydration",
                status="DETECTED",
                source=source,
                destination=getattr(tx, "counterparty", None),
                raw=_materialization_raw(tx, address_row, kind="withdrawal"),
            )
            db.add(wd)
            try:
                db.commit()
                db.refresh(wd)
                tx.withdrawal_id = wd.id
                tx.ingested_to_ledger_at = datetime.utcnow()
                db.add(tx)
                db.commit()
                out["written_withdrawals"] += 1
                out["marked_ingested"] += 1
            except IntegrityError:
                db.rollback()
                existing2 = _existing_withdrawal_for_tx(db, venue=venue, wallet_id=wallet_id, txid=txid)
                if existing2:
                    tx.withdrawal_id = existing2.id
                    tx.ingested_to_ledger_at = datetime.utcnow()
                    db.add(tx)
                    db.commit()
                    out["existing_withdrawals"] += 1
                    out["linked_existing"] += 1
                    out["marked_ingested"] += 1
                else:
                    minc("withdrawal_integrity_error")
            except Exception as e:
                db.rollback()
                minc(f"withdrawal_error:{type(e).__name__}")

    out["status"] = "preview" if dry_run else "applied"
    out["next"] = "Review materialized rows before applying lot/FIFO impacts."
    return out


async def ingest_hydration_wallet_history(
    db: Session,
    *,
    address_id: Optional[str] = None,
    limit_per_address: int = 25,
    dry_run: bool = True,
    provider: Optional[str] = None,
    cache_txs: bool = True,
    trust_provider_amounts: bool = False,
    raw_debug: bool = False,
    materialize: bool = False,
    materialize_limit: int = 100,
    page_start: int = 0,
    max_pages: int = 1,
    coverage_only: bool = False,
    untrusted_examples: bool = False,
    untrusted_example_limit: int = 20,
    trust_amount_v2_validated: bool = False,
) -> Dict[str, Any]:
    p = _provider_name(provider)
    limit = max(1, min(int(limit_per_address or 25), 100))
    page0 = max(0, int(page_start or 0))
    pages_n = max(1, min(int(max_pages or 1), 25))
    status = hydration_wallet_history_status(provider=p)

    supported, skipped_addresses = _registered_hydration_addresses(db, address_id=address_id)
    out: Dict[str, Any] = {
        "ok": True,
        "dry_run": bool(dry_run),
        "provider": p,
        "provider_status": status,
        "address_id": address_id,
        "limit_per_address": limit,
        "page_start": int(page0),
        "max_pages": int(pages_n),
        "coverage_only": bool(coverage_only),
        "untrusted_examples": bool(untrusted_examples),
        "untrusted_example_limit": int(max(1, min(int(untrusted_example_limit or 20), 100))),
        "trust_amount_v2_validated": bool(trust_amount_v2_validated),
        "addresses_considered": int(len(supported) + len(skipped_addresses)),
        "addresses_supported": int(len(supported)),
        "addresses_skipped": int(len(skipped_addresses)),
        "pages_fetched": 0,
        "discovered": 0,
        "candidates": 0,
        "deposits_candidates": 0,
        "withdrawals_candidates": 0,
        "cached": 0,
        "existing_cached": 0,
        "written_deposits": 0,
        "written_withdrawals": 0,
        "materialization": {
            "enabled": bool(materialize),
            "status": "disabled" if not materialize else "pending",
            "next": "Set materialize=true to preview/apply cached Hydration wallet tx rows into AssetDeposit / AssetWithdrawal rows.",
        },
        "coverage": {
            "asset_summary": {},
            "skipped_by_asset": {},
            "amount_sources": {},
            "page_summaries": [],
        },
        "untrusted_amount_diagnostics": {
            "enabled": bool(untrusted_examples),
            "by_asset": {},
            "examples": [],
        },
        "skip_reasons": {},
        "examples": [],
    }

    for item in skipped_addresses[:20]:
        _skip_inc(out, item.get("reason") or "address_skipped")
        _example(out, {"kind": "address_skipped", **item})

    if p == "none":
        _skip_inc(out, "provider_not_configured")
        if materialize and not coverage_only:
            out["materialization"] = _materialize_cached_hydration_txs(
                db,
                address_id=address_id,
                limit=materialize_limit,
                dry_run=dry_run,
            )
            out["written_deposits"] = int((out.get("materialization") or {}).get("written_deposits") or 0)
            out["written_withdrawals"] = int((out.get("materialization") or {}).get("written_withdrawals") or 0)
        elif materialize and coverage_only:
            out["materialization"] = {
                "enabled": True,
                "status": "skipped_coverage_only",
                "dry_run": bool(dry_run),
            }
        return out
    if p != "subscan":
        _skip_inc(out, "unsupported_provider")
        out["ok"] = False
        out["error"] = "unsupported_provider"
        return out

    if not supported:
        _skip_inc(out, "no_supported_hydration_addresses")
        return out

    for a in supported:
        addr = _clean_str(getattr(a, "address", None))
        if not addr:
            _skip_inc(out, "empty_address")
            continue

        for page in range(page0, page0 + pages_n):
            try:
                fetched = await _fetch_subscan_transfers(addr, limit=limit, page=page)
            except httpx.TimeoutException:
                _skip_inc(out, "provider_timeout")
                _example(out, {"kind": "provider_error", "address_id": str(a.id), "page": int(page), "error": "timeout"})
                break
            except Exception as e:
                _skip_inc(out, "provider_request_failed")
                _example(out, {"kind": "provider_error", "address_id": str(a.id), "page": int(page), "error": f"{type(e).__name__}: {e}"})
                break

            if not fetched.get("ok"):
                code = int(fetched.get("status") or 0)
                reason = "provider_rate_limited" if code in {420, 429} else "provider_http_error"
                _skip_inc(out, reason)
                _example(out, {"kind": "provider_error", "address_id": str(a.id), "page": int(page), "status": code, "provider": p})
                break

            rows = _extract_transfer_rows(fetched.get("data"))
            out["pages_fetched"] += 1
            out.setdefault("coverage", {}).setdefault("page_summaries", []).append({
                "address_id": str(a.id),
                "page": int(page),
                "rows": int(len(rows)),
            })
            out["discovered"] += len(rows)
            if not rows:
                _skip_inc(out, "no_transfer_rows")
                _example(out, {"kind": "no_transfer_rows", "address_id": str(a.id), "page": int(page), "provider": p})
                break

            for raw in rows:
                cand = _parse_transfer_candidate(
                    raw,
                    address=addr,
                    fallback_asset=getattr(a, "asset", None) or "HDX",
                    raw_debug=raw_debug,
                    db=db,
                    trust_amount_v2_validated=trust_amount_v2_validated,
                )
                if not cand:
                    _skip_inc(out, "not_wallet_transfer")
                    continue

                out["candidates"] += 1
                _coverage_touch_candidate(out, cand)
                if untrusted_examples and not cand.get("amount_trusted"):
                    _record_untrusted_amount_example(
                        out,
                        db=db,
                        candidate=cand,
                        raw_row=raw,
                        address_id=str(a.id),
                        page=int(page),
                        limit=int(max(1, min(int(untrusted_example_limit or 20), 100))),
                    )

                if cand["direction"] == "in":
                    out["deposits_candidates"] += 1
                else:
                    out["withdrawals_candidates"] += 1

                ex = {
                    "kind": "candidate",
                    "address_id": str(a.id),
                    "page": int(page),
                    "asset": cand.get("asset"),
                    "direction": cand.get("direction"),
                    "amount": cand.get("amount"),
                    "amount_trusted": cand.get("amount_trusted"),
                    "amount_source": cand.get("amount_source"),
                    "txid": cand.get("txid"),
                    "tx_time": cand.get("tx_time").isoformat() if isinstance(cand.get("tx_time"), datetime) else None,
                }
                if cand.get("amount_v2_validation") is not None:
                    v2_meta = cand.get("amount_v2_validation") or {}
                    ex["amount_v2_validation"] = {
                        "recommended_amount_source": v2_meta.get("recommended_amount_source"),
                        "recommended_interpretation": v2_meta.get("recommended_interpretation"),
                        "amount_matches_amount_v2_scaled": v2_meta.get("amount_matches_amount_v2_scaled"),
                    }
                if raw_debug and cand.get("raw_debug") is not None:
                    ex["raw_debug"] = cand.get("raw_debug")
                _example(out, ex)

                if dry_run or coverage_only or not cache_txs:
                    continue

                state, ref = _cache_wallet_tx(
                    db,
                    address_row=a,
                    candidate=cand,
                    provider=p,
                    trust_provider_amounts=trust_provider_amounts,
                )
                _coverage_cache_state(out, cand, state, ref)
                if state == "cached":
                    out["cached"] += 1
                elif state == "existing":
                    out["existing_cached"] += 1
                else:
                    _skip_inc(out, str(ref or "cache_skipped"))

    if materialize and not coverage_only:
        out["materialization"] = _materialize_cached_hydration_txs(
            db,
            address_id=address_id,
            limit=materialize_limit,
            dry_run=dry_run,
        )
        out["written_deposits"] = int((out.get("materialization") or {}).get("written_deposits") or 0)
        out["written_withdrawals"] = int((out.get("materialization") or {}).get("written_withdrawals") or 0)
    elif materialize and coverage_only:
        out["materialization"] = {
            "enabled": True,
            "status": "skipped_coverage_only",
            "dry_run": bool(dry_run),
        }

    return out
