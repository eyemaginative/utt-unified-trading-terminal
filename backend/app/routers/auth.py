# app/routers/auth.py

import base64
import hashlib
import hmac
import secrets
import time
import os
from typing import Optional

from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel

from ..config import settings

router = APIRouter(prefix="/api/auth", tags=["auth"])


def _auth_password() -> str:
    return (getattr(settings, "auth_password", None) or os.getenv("UTT_AUTH_PASSWORD") or "").strip()


def _auth_secret() -> str:
    # Separate signing secret. If unset, fall back to password (dev only).
    return (os.getenv("UTT_AUTH_SECRET") or _auth_password() or "utt-dev-secret").strip()


def _auth_required() -> bool:
    # Auth is required when a password is configured and UTT_AUTH_DISABLE is not set.
    if str(os.getenv("UTT_AUTH_DISABLE") or "").strip() in ("1", "true", "TRUE", "yes", "YES"):
        return False
    return bool(_auth_password())


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("utf-8").rstrip("=")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("utf-8"))


def _sign(msg: bytes) -> str:
    return _b64url(hmac.new(_auth_secret().encode("utf-8"), msg, hashlib.sha256).digest())


def _issue_token(user: str, ttl_s: int = 12 * 60 * 60) -> str:
    # token = b64(user).b64(exp).b64(nonce).sig
    exp = int(time.time()) + int(ttl_s)
    nonce = secrets.token_urlsafe(12)
    part_user = _b64url(user.encode("utf-8"))
    part_exp = _b64url(str(exp).encode("utf-8"))
    part_nonce = _b64url(nonce.encode("utf-8"))
    unsigned = f"{part_user}.{part_exp}.{part_nonce}".encode("utf-8")
    sig = _sign(unsigned)
    return f"{part_user}.{part_exp}.{part_nonce}.{sig}"


def _verify_token(token: str) -> Optional[dict]:
    try:
        parts = (token or "").split(".")
        if len(parts) != 4:
            return None
        part_user, part_exp, part_nonce, sig = parts
        unsigned = f"{part_user}.{part_exp}.{part_nonce}".encode("utf-8")
        if not hmac.compare_digest(sig, _sign(unsigned)):
            return None
        user = _b64url_decode(part_user).decode("utf-8", errors="ignore")
        exp = int(_b64url_decode(part_exp).decode("utf-8", errors="ignore"))
        if time.time() > exp:
            return None
        return {"user": user, "exp": exp}
    except Exception:
        return None


class LoginRequest(BaseModel):
    username: str = "local"
    password: str
    # Optional TOTP code; if UTT_AUTH_TOTP_SECRET is set, this becomes required.
    totp: Optional[str] = None


def _totp_secret() -> str:
    return (os.getenv("UTT_AUTH_TOTP_SECRET") or "").strip()


def _totp_now(secret_b32: str, step_s: int = 30, digits: int = 6, skew: int = 1) -> set[str]:
    # Minimal RFC6238 TOTP implementation (base32 secret).
    if not secret_b32:
        return set()
    # normalize base32 padding
    s = secret_b32.strip().replace(" ", "").upper()
    pad = "=" * (-len(s) % 8)
    key = base64.b32decode((s + pad).encode("utf-8"))
    t = int(time.time() // step_s)
    codes = set()
    for off in range(-skew, skew + 1):
        counter = (t + off).to_bytes(8, "big")
        hm = hmac.new(key, counter, hashlib.sha1).digest()
        o = hm[-1] & 0x0F
        dbc = int.from_bytes(hm[o:o+4], "big") & 0x7FFFFFFF
        code = str(dbc % (10 ** digits)).zfill(digits)
        codes.add(code)
    return codes


@router.post("/login")
def auth_login(req: LoginRequest):
    if not _auth_required():
        raise HTTPException(status_code=501, detail="Auth is not configured (set UTT_AUTH_PASSWORD).")
    pw = _auth_password()
    if not hmac.compare_digest((req.password or "").strip(), pw):
        raise HTTPException(status_code=401, detail="Invalid credentials.")
    secret = _totp_secret()
    if secret:
        code = (req.totp or "").strip()
        if code not in _totp_now(secret):
            raise HTTPException(status_code=401, detail="Invalid 2FA code.")
    token = _issue_token((req.username or "local").strip() or "local")
    return {"ok": True, "token": token, "user": (req.username or "local").strip() or "local"}


def require_auth(authorization: Optional[str] = Header(default=None)) -> dict:
    # If auth not configured, allow through.
    if not _auth_required():
        return {"user": "anonymous", "auth": False}
    auth = (authorization or "").strip()
    if auth.lower().startswith("bearer "):
        tok = auth.split(" ", 1)[1].strip()
    else:
        tok = ""
    info = _verify_token(tok)
    if not info:
        raise HTTPException(status_code=401, detail="Unauthorized (login required).")
    return {"user": info.get("user"), "auth": True, "exp": info.get("exp")}
