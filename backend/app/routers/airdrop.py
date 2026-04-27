from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, Query

try:
    from nacl.signing import VerifyKey  # type: ignore
except Exception:  # pragma: no cover
    VerifyKey = None  # type: ignore

router = APIRouter(prefix="/api/airdrop", tags=["airdrop"])


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "")).strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _snapshot_path() -> Path:
    raw = str(os.getenv("UTT_AIRDROP_SNAPSHOT_PATH", "")).strip()
    if raw:
        return Path(raw)
    return Path("backend/data/airdrop_snapshot.json")


def _registrations_path() -> Path:
    raw = str(os.getenv("UTT_AIRDROP_REGISTRATIONS_PATH", "")).strip()
    if raw:
        return Path(raw)
    return Path("backend/data/airdrop_registrations.json")


def _challenges_path() -> Path:
    raw = str(os.getenv("UTT_AIRDROP_CHALLENGES_PATH", "")).strip()
    if raw:
        return Path(raw)
    return Path("backend/data/airdrop_challenges.json")


def _campaign_id() -> str:
    return str(os.getenv("UTT_AIRDROP_CAMPAIGN_ID", "uttt-airdrop-v1")).strip() or "uttt-airdrop-v1"


def _campaign_name() -> str:
    return str(os.getenv("UTT_AIRDROP_CAMPAIGN_NAME", "UTTT Airdrop")).strip() or "UTTT Airdrop"


def _token_symbol() -> str:
    return str(os.getenv("UTT_AIRDROP_TOKEN_SYMBOL", "UTTT")).strip() or "UTTT"


def _registration_open() -> bool:
    return _env_bool("UTT_AIRDROP_REGISTRATION_OPEN", True)


def _challenge_ttl_seconds() -> int:
    raw = str(os.getenv("UTT_AIRDROP_CHALLENGE_TTL_SECONDS", "900")).strip()
    try:
        n = int(raw)
    except Exception:
        n = 900
    return max(60, min(n, 86400))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json_file(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return dict(default)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return dict(default)


def _write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")
    tmp.replace(path)


def _load_snapshot() -> dict[str, Any]:
    p = _snapshot_path()
    if not p.exists():
        return {"items": []}
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"items": []}
    if isinstance(data, list):
        return {"items": data}
    if isinstance(data, dict):
        items = data.get("items")
        if isinstance(items, list):
            return data
        wallets = data.get("wallets")
        if isinstance(wallets, list):
            data["items"] = wallets
            return data
    return {"items": []}


def _find_wallet_record(items: list[dict[str, Any]], wallet: str) -> dict[str, Any] | None:
    target = str(wallet or "").strip()
    if not target:
        return None
    for item in items:
        if not isinstance(item, dict):
            continue
        w = str(item.get("wallet") or item.get("address") or "").strip()
        if w and w == target:
            return item
    return None


def _to_str_amount(v: Any) -> str:
    if v is None or v == "":
        return "0"
    try:
        return format(Decimal(str(v)), "f")
    except Exception:
        return str(v)


def _load_registrations() -> dict[str, Any]:
    data = _read_json_file(_registrations_path(), {"items": []})
    items = data.get("items")
    if not isinstance(items, list):
        data["items"] = []
    return data


def _save_registrations(data: dict[str, Any]) -> None:
    _write_json_file(_registrations_path(), data)


def _load_challenges() -> dict[str, Any]:
    data = _read_json_file(_challenges_path(), {"items": []})
    items = data.get("items")
    if not isinstance(items, list):
        data["items"] = []
    return data


def _save_challenges(data: dict[str, Any]) -> None:
    _write_json_file(_challenges_path(), data)


def _challenge_key(wallet: str, campaign_id: str) -> str:
    return f"{campaign_id}:{wallet}"


def _find_registration(items: list[dict[str, Any]], wallet: str, campaign_id: str) -> dict[str, Any] | None:
    target = str(wallet or "").strip()
    if not target:
        return None
    for item in items:
        if not isinstance(item, dict):
            continue
        if str(item.get("campaign_id") or "").strip() != campaign_id:
            continue
        w = str(item.get("wallet") or "").strip()
        if w == target:
            return item
    return None


def _prune_expired_challenges(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    now = int(time.time())
    out: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        exp = int(item.get("expires_at") or 0)
        if exp and exp < now:
            continue
        out.append(item)
    return out


_B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
_B58_IDX = {c: i for i, c in enumerate(_B58_ALPHABET)}


def _b58decode(s: str) -> bytes:
    raw = str(s or "").strip()
    if not raw:
        return b""
    num = 0
    for ch in raw:
        if ch not in _B58_IDX:
            raise ValueError("invalid base58 character")
        num = num * 58 + _B58_IDX[ch]
    out = bytearray()
    while num > 0:
        num, rem = divmod(num, 256)
        out.append(rem)
    out.reverse()
    leading = 0
    for ch in raw:
        if ch == "1":
            leading += 1
        else:
            break
    return b"\x00" * leading + bytes(out)


def _server_binding(wallet: str, campaign_id: str) -> str | None:
    secret = str(os.getenv("UTT_KMS_MASTER_KEY", "")).strip() or str(os.getenv("UTT_AIRDROP_BIND_SECRET", "")).strip()
    if not secret:
        return None
    msg = f"{campaign_id}|{wallet}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()


def _build_status(wallet: str) -> dict[str, Any]:
    campaign_active = _env_bool("UTT_AIRDROP_CAMPAIGN_ACTIVE", False)
    registration_open = _registration_open()
    campaign_id = _campaign_id()
    campaign_name = _campaign_name()
    token_symbol = _token_symbol()
    w = str(wallet or "").strip()

    snapshot = _load_snapshot()
    items = snapshot.get("items") if isinstance(snapshot, dict) else []
    items = items if isinstance(items, list) else []
    rec = _find_wallet_record(items, w)

    regs = _load_registrations()
    reg_items = regs.get("items") if isinstance(regs, dict) else []
    reg_items = reg_items if isinstance(reg_items, list) else []
    reg = _find_registration(reg_items, w, campaign_id)

    connected = bool(w)
    registered = reg is not None
    registered_at = reg.get("registered_at") if isinstance(reg, dict) else None
    eligible = False
    claimed = False
    claimed_at = None
    claim_tx = None
    amount = "0"
    notes = None
    reason = "Connect or paste a Solana wallet to check eligibility."

    if not connected:
        reason = "No wallet provided."
    elif not registered:
        reason = "Wallet not registered for this campaign."
    elif not campaign_active:
        reason = "Registered. Airdrop not active yet."
    elif rec is None:
        reason = "Registered — pending operator review/snapshot."
    else:
        eligible = bool(rec.get("eligible", True))
        claimed = bool(rec.get("claimed", False)) or bool(rec.get("claimed_at"))
        claimed_at = rec.get("claimed_at")
        claim_tx = rec.get("claim_tx") or rec.get("tx_sig")
        amount = _to_str_amount(rec.get("amount") or rec.get("claimable_amount") or "0")
        notes = rec.get("notes")
        if claimed:
            reason = "Airdrop already claimed."
        elif eligible:
            reason = f"Eligible for {amount} {token_symbol}."
        else:
            reason = str(rec.get("reason") or "This wallet is not currently eligible.")

    return {
        "ok": True,
        "campaignActive": campaign_active,
        "registrationOpen": registration_open,
        "campaignId": campaign_id,
        "campaignName": campaign_name,
        "tokenSymbol": token_symbol,
        "wallet": w,
        "connected": connected,
        "registered": registered,
        "registeredAt": registered_at,
        "eligible": eligible,
        "claimableAmount": amount,
        "claimed": claimed,
        "claimedAt": claimed_at,
        "claimTx": claim_tx,
        "reason": reason,
        "notes": notes,
        "snapshotPath": str(_snapshot_path()),
        "registrationsPath": str(_registrations_path()),
        "checkedAt": _utc_now_iso(),
    }


@router.get("/status")
def get_airdrop_status(wallet: str | None = Query(default=None, description="Solana wallet to check")):
    return _build_status(str(wallet or "").strip())


@router.post("/register_challenge")
def register_challenge(payload: dict[str, Any] = Body(default={})):
    campaign_id = _campaign_id()
    campaign_name = _campaign_name()
    wallet = str(payload.get("wallet") or "").strip()

    if not _registration_open():
        return {
            "ok": False,
            "detail": "Airdrop registration is not open.",
        }
    if not wallet:
        return {
            "ok": False,
            "detail": "Wallet is required.",
        }

    regs = _load_registrations()
    reg_items = regs.get("items") if isinstance(regs, dict) else []
    reg_items = reg_items if isinstance(reg_items, list) else []
    existing = _find_registration(reg_items, wallet, campaign_id)
    if existing is not None:
        return {
            "ok": True,
            "alreadyRegistered": True,
            "campaignId": campaign_id,
            "wallet": wallet,
            "message": None,
            "nonce": None,
            "reason": "Wallet already registered for this campaign.",
            "status": _build_status(wallet),
        }

    nonce = secrets.token_urlsafe(24)
    message = f"Register wallet {wallet} for {campaign_name} ({campaign_id}) with nonce {nonce}"
    ttl = _challenge_ttl_seconds()
    now = int(time.time())
    exp = now + ttl

    challenges = _load_challenges()
    items = challenges.get("items") if isinstance(challenges, dict) else []
    items = items if isinstance(items, list) else []
    items = _prune_expired_challenges(items)
    key = _challenge_key(wallet, campaign_id)
    items = [it for it in items if str(it.get("key") or "") != key]
    items.append({
        "key": key,
        "wallet": wallet,
        "campaign_id": campaign_id,
        "nonce": nonce,
        "message": message,
        "created_at": now,
        "expires_at": exp,
    })
    challenges["items"] = items
    _save_challenges(challenges)

    return {
        "ok": True,
        "alreadyRegistered": False,
        "campaignId": campaign_id,
        "campaignName": campaign_name,
        "wallet": wallet,
        "nonce": nonce,
        "message": message,
        "expiresAt": exp,
    }


@router.post("/register_verify")
def register_verify(payload: dict[str, Any] = Body(default={})):
    campaign_id = _campaign_id()
    wallet = str(payload.get("wallet") or "").strip()
    nonce = str(payload.get("nonce") or "").strip()
    message = str(payload.get("message") or "").strip()
    signature_b64 = str(payload.get("signature") or "").strip()
    auth_user = str(payload.get("authUser") or "").strip()

    if not _registration_open():
        return {"ok": False, "detail": "Airdrop registration is not open."}
    if not wallet or not nonce or not message or not signature_b64:
        return {"ok": False, "detail": "wallet, nonce, message, and signature are required."}

    challenges = _load_challenges()
    items = challenges.get("items") if isinstance(challenges, dict) else []
    items = items if isinstance(items, list) else []
    items = _prune_expired_challenges(items)
    key = _challenge_key(wallet, campaign_id)
    ch = None
    for item in items:
        if str(item.get("key") or "") == key:
            ch = item
            break
    if ch is None:
        challenges["items"] = items
        _save_challenges(challenges)
        return {"ok": False, "detail": "Registration challenge not found or expired."}

    if str(ch.get("nonce") or "") != nonce or str(ch.get("message") or "") != message:
        return {"ok": False, "detail": "Challenge mismatch."}

    if VerifyKey is None:
        return {"ok": False, "detail": "PyNaCl is required on the backend for wallet-signature verification."}

    try:
        verify_key = VerifyKey(_b58decode(wallet))
        verify_key.verify(message.encode("utf-8"), base64.b64decode(signature_b64))
    except Exception:
        return {"ok": False, "detail": "Wallet signature verification failed."}

    regs = _load_registrations()
    reg_items = regs.get("items") if isinstance(regs, dict) else []
    reg_items = reg_items if isinstance(reg_items, list) else []

    existing = _find_registration(reg_items, wallet, campaign_id)
    if existing is None:
        rec = {
            "wallet": wallet,
            "campaign_id": campaign_id,
            "registered_at": _utc_now_iso(),
            "signature_b64": signature_b64,
            "nonce": nonce,
            "message": message,
            "auth_user": auth_user or None,
            "server_binding": _server_binding(wallet, campaign_id),
        }
        reg_items.append(rec)
        regs["items"] = reg_items
        _save_registrations(regs)

    items = [it for it in items if str(it.get("key") or "") != key]
    challenges["items"] = items
    _save_challenges(challenges)

    return {
        "ok": True,
        "campaignId": campaign_id,
        "wallet": wallet,
        "registered": True,
        "alreadyRegistered": existing is not None,
        "status": _build_status(wallet),
    }
