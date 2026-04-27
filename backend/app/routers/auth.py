# app/routers/auth.py
#
# NOTE: This router supports TWO modes:
#   (A) Shared-password mode (current default) via UTT_AUTH_PASSWORD (+ optional UTT_AUTH_TOTP_SECRET)
#   (B) DB-backed user accounts (opt-in) via UTT_AUTH_DB=1 (no credentials in env)
#
# Shared-password mode remains the default for safety while we incrementally roll out Profile/Signup.

import base64
import hashlib
import hmac
import secrets
import time
import os
import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Any

from fastapi import APIRouter, HTTPException, Header, Depends
from pydantic import BaseModel, Field

from sqlalchemy import text

from ..config import settings
from ..db import SessionLocal, engine

try:
    from cryptography.fernet import Fernet
except Exception:  # pragma: no cover
    Fernet = None  # type: ignore

router = APIRouter(prefix="/api/auth", tags=["auth"])


# -----------------------------
# Mode selection
# -----------------------------

def _auth_db_enabled() -> bool:
    return str(os.getenv("UTT_AUTH_DB") or "").strip() in ("1", "true", "TRUE", "yes", "YES")


# -----------------------------
# Shared-password mode helpers
# -----------------------------

def _auth_password() -> str:
    return (getattr(settings, "auth_password", None) or os.getenv("UTT_AUTH_PASSWORD") or "").strip()


def _totp_secret_shared() -> str:
    return (os.getenv("UTT_AUTH_TOTP_SECRET") or "").strip()


def _auth_required_shared() -> bool:
    # Auth is required when a password is configured and UTT_AUTH_DISABLE is not set.
    if str(os.getenv("UTT_AUTH_DISABLE") or "").strip() in ("1", "true", "TRUE", "yes", "YES"):
        return False
    return bool(_auth_password())


# -----------------------------
# Token signing / verification
# -----------------------------

def _auth_secret() -> str:
    # Separate signing secret. If unset, fall back to password (dev only).
    return (os.getenv("UTT_AUTH_SECRET") or _auth_password() or "utt-dev-secret").strip()


def _auth_ttl_short_s() -> int:
    try:
        return max(300, int(os.getenv("UTT_AUTH_TTL_S") or 12 * 60 * 60))
    except Exception:
        return 12 * 60 * 60


def _auth_ttl_remember_s() -> int:
    try:
        return max(_auth_ttl_short_s(), int(os.getenv("UTT_AUTH_TTL_REMEMBER_S") or 30 * 24 * 60 * 60))
    except Exception:
        return 30 * 24 * 60 * 60



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


# -----------------------------
# TOTP (RFC6238 minimal)
# -----------------------------

def _totp_now(secret_b32: str, step_s: int = 30, digits: int = 6, skew: int = 1) -> set[str]:
    # Minimal RFC6238 TOTP implementation (base32 secret).
    if not secret_b32:
        return set()
    s = secret_b32.strip().replace(" ", "").upper()
    pad = "=" * (-len(s) % 8)
    key = base64.b32decode((s + pad).encode("utf-8"))
    t = int(time.time() // step_s)
    codes = set()
    for off in range(-skew, skew + 1):
        counter = (t + off).to_bytes(8, "big")
        hm = hmac.new(key, counter, hashlib.sha1).digest()
        o = hm[-1] & 0x0F
        dbc = int.from_bytes(hm[o:o + 4], "big") & 0x7FFFFFFF
        code = str(dbc % (10 ** digits)).zfill(digits)
        codes.add(code)
    return codes


# -----------------------------
# DB-backed auth (opt-in)
# -----------------------------

def _kms_master_key() -> str:
    # For encrypting per-user TOTP secrets (and later API keys).
    # Keep in utt-secrets/backend.env (server-side only).
    return (os.getenv("UTT_KMS_MASTER_KEY") or "").strip()


def _fernet() -> Optional[Any]:
    if Fernet is None:
        return None
    mk = _kms_master_key()
    if not mk:
        # Dev fallback: derive from auth secret (still server-side). Prefer UTT_KMS_MASTER_KEY.
        mk = _auth_secret()
    # Fernet key must be 32 urlsafe-base64 bytes.
    key = base64.urlsafe_b64encode(hashlib.sha256(mk.encode("utf-8")).digest())
    return Fernet(key)


def _pw_hash(password: str, salt: bytes) -> bytes:
    # PBKDF2-HMAC-SHA256 (stdlib; no external deps). Adjustable later.
    return hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200_000, dklen=32)


def _ensure_auth_tables() -> None:
    # Minimal table (no migrations yet). Safe to call repeatedly.
    ddl = """
    CREATE TABLE IF NOT EXISTS utt_users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL UNIQUE,
      pw_salt_b64 TEXT NOT NULL,
      pw_hash_b64 TEXT NOT NULL,
      totp_secret_enc TEXT,
      totp_enabled INTEGER NOT NULL DEFAULT 0,
      auto_backup_on_logout INTEGER NOT NULL DEFAULT 1,
      created_at INTEGER NOT NULL
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
        try:
            cols = conn.execute(text("PRAGMA table_info(utt_users)")).mappings().all()
            names = {str(c.get("name") or "").strip().lower() for c in cols}
            if "auto_backup_on_logout" not in names:
                conn.execute(text("ALTER TABLE utt_users ADD COLUMN auto_backup_on_logout INTEGER NOT NULL DEFAULT 1"))
            if "remember_login" not in names:
                conn.execute(text("ALTER TABLE utt_users ADD COLUMN remember_login INTEGER NOT NULL DEFAULT 0"))
        except Exception:
            pass

# -----------------------------
# API Key Vault (write-only UI; encrypted at rest)
# Stored per user; UI can list metadata only.
# -----------------------------

def _ensure_api_keys_table() -> None:
    _ensure_auth_tables()
    ddl = """
    CREATE TABLE IF NOT EXISTS utt_api_keys (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT NOT NULL,
      venue TEXT NOT NULL,
      label TEXT,
      key_hint TEXT,
      secret_enc TEXT NOT NULL,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def _api_keys_encrypt(payload: dict) -> str:
    f = _fernet()
    if f is None:
        raise HTTPException(status_code=500, detail="Server crypto is unavailable (install cryptography).")
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return f.encrypt(raw).decode("utf-8")


def _api_keys_decrypt(token: str) -> dict:
    f = _fernet()
    if f is None:
        raise HTTPException(status_code=500, detail="Server crypto is unavailable (install cryptography).")
    try:
        raw = f.decrypt((token or "").encode("utf-8"))
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return {}


def _db_api_keys_list(username: str) -> list[dict]:
    _ensure_api_keys_table()
    u = (username or "").strip()
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT id, venue, label, key_hint, created_at, updated_at
                FROM utt_api_keys
                WHERE username = :u
                ORDER BY updated_at DESC, id DESC
                """
            ),
            {"u": u},
        ).mappings().all()
        out = []
        for r in rows:
            d = dict(r)
            # UI expects 'hint' field name
            d["hint"] = d.pop("key_hint", None)
            # Keep timestamps as ints for now; UI can render raw or format later
            out.append(d)
        return out


def _db_api_keys_upsert(username: str, venue: str, label: Optional[str], api_key: str, api_secret: Optional[str], passphrase: Optional[str]) -> dict:
    _ensure_api_keys_table()
    u = (username or "").strip()
    v = (venue or "").strip()
    if not v:
        raise HTTPException(status_code=400, detail="Venue is required.")
    if not (api_key or "").strip():
        raise HTTPException(status_code=400, detail="API key is required.")
    hint = ""
    ak = (api_key or "").strip()
    if len(ak) >= 4:
        hint = f"...{ak[-4:]}"
    payload = {"api_key": ak, "api_secret": (api_secret or "").strip() or None, "passphrase": (passphrase or "").strip() or None}
    enc = _api_keys_encrypt(payload)
    now = int(time.time())
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO utt_api_keys (username, venue, label, key_hint, secret_enc, created_at, updated_at)
                VALUES (:u, :v, :l, :h, :e, :c, :m)
                """
            ),
            {"u": u, "v": v, "l": (label or "").strip() or None, "h": hint or None, "e": enc, "c": now, "m": now},
        )
        rid = conn.execute(text("SELECT last_insert_rowid() AS id")).mappings().first()
        key_id = int(rid["id"]) if rid and rid.get("id") is not None else None
    return {"id": key_id, "venue": v, "label": (label or "").strip() or None, "hint": hint or None, "created_at": now, "updated_at": now}


def _db_api_keys_delete(username: str, key_id: int) -> None:
    _ensure_api_keys_table()
    u = (username or "").strip()
    with engine.begin() as conn:
        res = conn.execute(
            text("DELETE FROM utt_api_keys WHERE username = :u AND id = :id"),
            {"u": u, "id": int(key_id)},
        )
        # sqlite rowcount works with SQLAlchemy execute result
        if hasattr(res, "rowcount") and int(res.rowcount or 0) <= 0:
            raise HTTPException(status_code=404, detail="API key not found.")
# (removed stray text(ddl) evaluation)


def _db_get_user(username: str) -> Optional[dict]:
    _ensure_auth_tables()
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT username, pw_salt_b64, pw_hash_b64, totp_secret_enc, totp_enabled, auto_backup_on_logout, remember_login
                FROM utt_users
                WHERE username = :u
                """
            ),
            {"u": username},
        ).mappings().first()
        return dict(row) if row else None


def _db_verify_password(username: str, password: str) -> bool:
    row = _db_get_user(username)
    if not row:
        return False
    try:
        salt = _b64url_decode(row.get("pw_salt_b64") or "")
        want = _b64url_decode(row.get("pw_hash_b64") or "")
        got = _pw_hash((password or "").strip(), salt)
        return bool(hmac.compare_digest(got, want))
    except Exception:
        return False


def _db_set_password(username: str, new_password: str) -> None:
    # Rotate salt + hash; requires user to already exist.
    _ensure_auth_tables()
    u = (username or "").strip()
    if not u:
        raise HTTPException(status_code=400, detail="Username is required.")
    if len((new_password or "").strip()) < 6:
        raise HTTPException(status_code=400, detail="New password must be at least 6 characters.")
    salt = secrets.token_bytes(16)
    ph = _pw_hash((new_password or "").strip(), salt)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE utt_users
                SET pw_salt_b64 = :s, pw_hash_b64 = :h
                WHERE username = :u
                """
            ),
            {"u": u, "s": _b64url(salt), "h": _b64url(ph)},
        )


def _db_totp_provisioned(username: str) -> bool:
    row = _db_get_user(username) or {}
    return bool((row.get("totp_secret_enc") or "").strip())


def _db_count_users() -> int:
    _ensure_auth_tables()
    with engine.begin() as conn:
        row = conn.execute(text("SELECT COUNT(*) AS n FROM utt_users")).mappings().first()
        return int(row["n"]) if row else 0


def _db_create_user(username: str, password: str) -> dict:
    _ensure_auth_tables()
    u = username.strip()
    if not u:
        raise HTTPException(status_code=400, detail="Username is required.")
    if len(password or "") < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters.")
    if _db_get_user(u):
        raise HTTPException(status_code=409, detail="User already exists.")
    salt = secrets.token_bytes(16)
    ph = _pw_hash(password, salt)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO utt_users (username, pw_salt_b64, pw_hash_b64, totp_secret_enc, totp_enabled, created_at)
                VALUES (:u, :s, :h, NULL, 0, :ts)
                """
            ),
            {
                "u": u,
                "s": _b64url(salt),
                "h": _b64url(ph),
                "ts": int(time.time()),
            },
        )
    return {"username": u, "totp_enabled": False}


def _db_set_totp_secret(username: str, secret_b32: str, enabled: bool) -> None:
    _ensure_auth_tables()
    f = _fernet()
    if f is None:
        raise HTTPException(status_code=500, detail="Server crypto is not available.")
    s = secret_b32.strip().replace(" ", "").upper()
    if not s:
        raise HTTPException(status_code=400, detail="TOTP secret is required.")
    enc = f.encrypt(s.encode("utf-8")).decode("utf-8")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE utt_users
                SET totp_secret_enc = :enc, totp_enabled = :en
                WHERE username = :u
                """
            ),
            {"enc": enc, "en": 1 if enabled else 0, "u": username},
        )


def _db_get_totp_secret(username: str) -> Optional[str]:
    u = _db_get_user(username)
    if not u:
        return None
    enc = (u.get("totp_secret_enc") or "").strip()
    if not enc:
        return None
    f = _fernet()
    if f is None:
        return None
    try:
        return f.decrypt(enc.encode("utf-8")).decode("utf-8")
    except Exception:
        return None


def _db_totp_enabled(username: str) -> bool:
    u = _db_get_user(username)
    if not u:
        return False
    return bool(int(u.get("totp_enabled") or 0))


def _db_auto_backup_on_logout(username: str) -> bool:
    u = _db_get_user(username)
    if not u:
        return True
    try:
        return bool(int(u.get("auto_backup_on_logout") if u.get("auto_backup_on_logout") is not None else 1))
    except Exception:
        return True


def _db_set_auto_backup_on_logout(username: str, enabled: bool) -> None:
    _ensure_auth_tables()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE utt_users
                SET auto_backup_on_logout = :v
                WHERE username = :u
                """
            ),
            {"u": (username or "").strip(), "v": 1 if enabled else 0},
        )


def _db_remember_login(username: str) -> bool:
    u = _db_get_user(username)
    if not u:
        return False
    try:
        return bool(int(u.get("remember_login") if u.get("remember_login") is not None else 0))
    except Exception:
        return False


def _db_set_remember_login(username: str, enabled: bool) -> None:
    _ensure_auth_tables()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE utt_users
                SET remember_login = :v
                WHERE username = :u
                """
            ),
            {"u": (username or "").strip(), "v": 1 if enabled else 0},
        )


def _token_ttl_for_login(username: str = "", remember_me: Optional[bool] = None) -> int:
    remember = bool(remember_me) if remember_me is not None else False
    if _auth_db_enabled() and (username or "").strip():
        if remember_me is None:
            remember = _db_remember_login((username or "").strip())
    return _auth_ttl_remember_s() if remember else _auth_ttl_short_s()


def _issue_token_for_login(user: str, remember_me: Optional[bool] = None) -> str:
    return _issue_token(user, ttl_s=_token_ttl_for_login(user, remember_me))


def _create_sqlite_backup(requested_by: str = "") -> dict:
    db_path = settings.resolved_sqlite_path() if hasattr(settings, "resolved_sqlite_path") else Path((getattr(settings, "sqlite_path", "") or "./data/app.db")).expanduser()
    if not db_path.is_absolute():
        db_path = (Path(__file__).resolve().parents[2] / db_path).resolve()
    if not db_path.exists() or not db_path.is_file():
        raise HTTPException(status_code=404, detail=f"SQLite database not found: {db_path}")

    backup_dir = settings.resolved_backup_dir() if hasattr(settings, "resolved_backup_dir") else (db_path.parent / "backups").resolve()
    backup_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_user = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(requested_by or "anonymous").strip()) if 're' in globals() else str(requested_by or 'anonymous').strip()
    if not safe_user:
        safe_user = "anonymous"
    out_name = f"{db_path.stem}.{stamp}.{safe_user}.backup{db_path.suffix or '.sqlite'}"
    out_path = backup_dir / out_name

    try:
        src = __import__('sqlite3').connect(str(db_path))
        try:
            dst = __import__('sqlite3').connect(str(out_path))
            try:
                src.backup(dst)
            finally:
                dst.close()
        finally:
            src.close()
    except Exception:
        try:
            shutil.copy2(str(db_path), str(out_path))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Backup failed: {e}")

    try:
        st = out_path.stat()
        size = int(st.st_size)
        created_at = int(st.st_mtime)
    except Exception:
        size = None
        created_at = int(time.time())

    return {
        "ok": True,
        "db_path": str(db_path),
        "backup_dir": str(backup_dir),
        "backup_path": str(out_path),
        "filename": out_path.name,
        "size_bytes": size,
        "created_at": created_at,
        "requested_by": str(requested_by or "").strip() or "anonymous",
    }


# -----------------------------
# API models
# -----------------------------

class LoginRequest(BaseModel):
    username: str = "local"
    password: str
    totp: Optional[str] = None
    remember_me: Optional[bool] = None


class SignupRequest(BaseModel):
    username: str = Field(..., min_length=1)
    password: str = Field(..., min_length=6)
    # If true, return a provisioning secret + otpauth URL (2FA not enabled until /2fa/enable).
    want_2fa: bool = True


class Enable2FARequest(BaseModel):
    totp: str = Field(..., min_length=6, max_length=8)


class Reset2FARequest(BaseModel):
    # When 2FA is already enabled, reset must be step-up gated.
    # Keep these permissive to avoid 422 blobs; enforce in-route.
    password: str = Field("", min_length=0)
    totp: Optional[str] = None


class ChangePasswordRequest(BaseModel):
    # Keep permissive to avoid 422 blobs; enforce in-route.
    current_password: str = Field("", min_length=0)
    new_password: str = Field("", min_length=0)
    totp: Optional[str] = None

class ApiKeyUpsertRequest(BaseModel):
    venue: str = Field("", min_length=0)
    label: Optional[str] = None
    api_key: str = Field("", min_length=0)
    api_secret: Optional[str] = None
    passphrase: Optional[str] = None
    totp: Optional[str] = None


class BackupPrefsRequest(BaseModel):
    auto_backup_on_logout: bool = True


class SessionPrefsRequest(BaseModel):
    remember_login: bool = False


# -----------------------------
# Routes
# -----------------------------

@router.get("/bootstrap_status")
def auth_bootstrap_status():
    """Public bootstrap status for first-run local account setup UI."""
    db_mode = _auth_db_enabled()
    shared_required = _auth_required_shared()
    user_count = _db_count_users() if db_mode else 0
    signup_open = bool(db_mode and user_count == 0)
    return {
        "ok": True,
        "auth_db_enabled": bool(db_mode),
        "shared_auth_required": bool(shared_required),
        "required": bool(db_mode or shared_required),
        "user_count": int(user_count),
        "signup_open": bool(signup_open),
        "first_user_bootstrap_required": bool(signup_open),
    }


@router.post("/signup")
def auth_signup(req: SignupRequest):
    if not _auth_db_enabled():
        raise HTTPException(status_code=501, detail="Signup is not enabled (set UTT_AUTH_DB=1).")

    # Safety: only allow open signup when there are no users yet.
    if _db_count_users() > 0:
        raise HTTPException(status_code=403, detail="Signup is closed (admin required).")

    user = _db_create_user(req.username, req.password)

    if req.want_2fa:
        # Generate a new base32 secret; enable after user verifies a code.
        secret_b32 = base64.b32encode(secrets.token_bytes(20)).decode("utf-8").rstrip("=")
        _db_set_totp_secret(user["username"], secret_b32, enabled=False)
        otpauth = f"otpauth://totp/UTT:{user['username']}?secret={secret_b32}&issuer=UTT"
        return {"ok": True, "user": user["username"], "totp_provisioning": {"secret": secret_b32, "otpauth_url": otpauth}}

    return {"ok": True, "user": user["username"]}


@router.post("/login")
def auth_login(req: LoginRequest):
    # Mode B: DB-backed accounts
    if _auth_db_enabled():
        u = (req.username or "").strip()
        if not u:
            raise HTTPException(status_code=400, detail="Username is required.")
        row = _db_get_user(u)
        if not row:
            raise HTTPException(status_code=401, detail="Invalid credentials.")
        salt = _b64url_decode(row["pw_salt_b64"])
        want = _b64url_decode(row["pw_hash_b64"])
        got = _pw_hash((req.password or "").strip(), salt)
        if not hmac.compare_digest(got, want):
            raise HTTPException(status_code=401, detail="Invalid credentials.")

        if _db_totp_enabled(u):
            secret = _db_get_totp_secret(u) or ""
            code = (req.totp or "").strip()
            if (not secret) or (code not in _totp_now(secret)):
                raise HTTPException(status_code=401, detail="Invalid 2FA code.")

        token = _issue_token_for_login(u, req.remember_me)
        return {"ok": True, "token": token, "user": u}

    # Mode A: shared-password
    if not _auth_required_shared():
        raise HTTPException(status_code=501, detail="Auth is not configured (set UTT_AUTH_PASSWORD).")
    pw = _auth_password()
    if not hmac.compare_digest((req.password or "").strip(), pw):
        raise HTTPException(status_code=401, detail="Invalid credentials.")
    secret = _totp_secret_shared()
    if secret:
        code = (req.totp or "").strip()
        if code not in _totp_now(secret):
            raise HTTPException(status_code=401, detail="Invalid 2FA code.")
    user = (req.username or "local").strip() or "local"
    token = _issue_token_for_login(user, req.remember_me)
    return {"ok": True, "token": token, "user": user}


def require_auth(
    authorization: Optional[str] = Header(default=None),
    x_authorization: Optional[str] = Header(default=None, alias="X-Authorization"),
) -> dict:
    # If shared-auth is not configured and DB-auth is not enabled, allow through.
    if (not _auth_db_enabled()) and (not _auth_required_shared()):
        return {"user": "anonymous", "auth": False}

    auth = (authorization or "").strip()
    if (not auth) and x_authorization:
        auth = (x_authorization or "").strip()
    if auth.lower().startswith("bearer "):
        tok = auth.split(" ", 1)[1].strip()
    else:
        tok = ""
    info = _verify_token(tok)
    if not info:
        raise HTTPException(status_code=401, detail="Unauthorized (login required).")
    return {"user": info.get("user"), "auth": True, "exp": info.get("exp")}


@router.get("/me")
def auth_me(
    authorization: Optional[str] = Header(default=None),
    x_authorization: Optional[str] = Header(default=None, alias="X-Authorization"),
):
    required = _auth_db_enabled() or _auth_required_shared()
    auth = (authorization or "").strip()
    if (not auth) and x_authorization:
        auth = (x_authorization or "").strip()
    tok = auth.split(" ", 1)[1].strip() if auth.lower().startswith("bearer ") else ""
    if not tok:
        resp = {"ok": True, "required": bool(required), "auth": False, "user": "anonymous"}
        if _auth_db_enabled():
            try:
                n = _db_count_users()
                resp["user_count"] = int(n)
                resp["signup_open"] = bool(n == 0)
                resp["first_user_bootstrap_required"] = bool(n == 0)
            except Exception:
                resp["user_count"] = 0
                resp["signup_open"] = False
                resp["first_user_bootstrap_required"] = False
        return resp
    info = _verify_token(tok)
    if not info:
        raise HTTPException(status_code=401, detail="Unauthorized (invalid token).")
    user = (info.get("user") or "").strip() or "local"
    resp = {"ok": True, "required": bool(required), "auth": True, "user": user, "exp": info.get("exp")}
    if _auth_db_enabled():
        try:
            resp["totp_enabled"] = bool(_db_totp_enabled(user))
            resp["totp_provisioned"] = bool(_db_totp_provisioned(user))
            resp["auto_backup_on_logout"] = bool(_db_auto_backup_on_logout(user))
            resp["remember_login"] = bool(_db_remember_login(user))
        except Exception:
            resp["totp_enabled"] = False
            resp["totp_provisioned"] = False
            resp["auto_backup_on_logout"] = True
            resp["remember_login"] = False
    return resp


@router.post("/backup_db")
def auth_backup_db(ident: dict = Depends(require_auth)):
    """Create a timestamped backup copy of the active SQLite DB."""
    user = (ident.get("user") or "").strip() or "anonymous"
    return _create_sqlite_backup(requested_by=user)


@router.get("/backup_prefs")
def auth_backup_prefs_get(ident: dict = Depends(require_auth)):
    user = (ident.get("user") or "").strip() or "local"
    enabled = True
    if _auth_db_enabled():
        enabled = bool(_db_auto_backup_on_logout(user))
    return {"ok": True, "auto_backup_on_logout": bool(enabled)}


@router.post("/backup_prefs")
def auth_backup_prefs_set(req: BackupPrefsRequest, ident: dict = Depends(require_auth)):
    user = (ident.get("user") or "").strip() or "local"
    enabled = bool(req.auto_backup_on_logout)
    if _auth_db_enabled():
        _db_set_auto_backup_on_logout(user, enabled)
    return {"ok": True, "auto_backup_on_logout": bool(enabled)}


@router.get("/session_prefs")
def auth_session_prefs_get(ident: dict = Depends(require_auth)):
    user = (ident.get("user") or "").strip() or "local"
    remember = False
    if _auth_db_enabled():
        remember = bool(_db_remember_login(user))
    return {
        "ok": True,
        "remember_login": bool(remember),
        "token_ttl_s": int(_token_ttl_for_login(user, remember)),
        "remember_ttl_s": int(_auth_ttl_remember_s()),
        "default_ttl_s": int(_auth_ttl_short_s()),
    }


@router.post("/session_prefs")
def auth_session_prefs_set(req: SessionPrefsRequest, ident: dict = Depends(require_auth)):
    user = (ident.get("user") or "").strip() or "local"
    remember = bool(req.remember_login)
    if _auth_db_enabled():
        _db_set_remember_login(user, remember)
    token = _issue_token_for_login(user, remember)
    return {
        "ok": True,
        "remember_login": bool(remember),
        "token": token,
        "user": user,
        "token_ttl_s": int(_token_ttl_for_login(user, remember)),
    }


@router.post("/logout")
def auth_logout():
    # Stateless logout; client clears token.
    return {"ok": True}


@router.post("/password/change")
def auth_password_change(req: ChangePasswordRequest, ident: dict = Depends(require_auth)):
    """Change the current user's password.

    DB mode: requires current password; if 2FA is enabled, also requires a valid current TOTP (step-up).
    Shared-password mode: not supported.
    """
    if not _auth_db_enabled():
        raise HTTPException(status_code=501, detail="Password change is not available (set UTT_AUTH_DB=1).")

    user = (ident.get("user") or "").strip() or "local"
    cur_pw = (req.current_password or "").strip()
    new_pw = (req.new_password or "").strip()

    if not cur_pw:
        raise HTTPException(status_code=400, detail="Current password is required.")
    if not _db_verify_password(user, cur_pw):
        raise HTTPException(status_code=401, detail="Invalid credentials.")
    if len(new_pw) < 6:
        raise HTTPException(status_code=400, detail="New password must be at least 6 characters.")

    if _db_totp_enabled(user):
        code = ((req.totp or "").strip() if req else "")
        if len(code) < 6:
            raise HTTPException(status_code=400, detail="Current 2FA code is required.")
        secret = _db_get_totp_secret(user) or ""
        if (not secret) or (code not in _totp_now(secret)):
            raise HTTPException(status_code=401, detail="Invalid 2FA code.")

    _db_set_password(user, new_pw)
    return {"ok": True, "user": user}



@router.get("/api_keys")
def api_keys_list(ident: dict = Depends(require_auth)):
    """Return API key metadata for the current user (never secrets)."""
    if not _auth_db_enabled():
        raise HTTPException(status_code=501, detail="API keys are only available in DB auth mode (set UTT_AUTH_DB=1).")
    user = (ident.get("user") or "").strip() or "local"
    items = _db_api_keys_list(user)
    return {"ok": True, "items": items}


@router.post("/api_keys")
def api_keys_upsert(req: ApiKeyUpsertRequest, ident: dict = Depends(require_auth)):
    """Write-only create of an API key bundle (encrypted at rest)."""
    if not _auth_db_enabled():
        raise HTTPException(status_code=501, detail="API keys are only available in DB auth mode (set UTT_AUTH_DB=1).")
    user = (ident.get("user") or "").strip() or "local"

    # Step-up: require a current TOTP code when 2FA is enabled.
    if _db_totp_enabled(user):
        code = ((req.totp or "").strip() if req else "")
        if len(code) < 6:
            raise HTTPException(status_code=400, detail="Current 2FA code is required.")
        secret = _db_get_totp_secret(user) or ""
        if (not secret) or (code not in _totp_now(secret)):
            raise HTTPException(status_code=401, detail="Invalid 2FA code.")

    meta = _db_api_keys_upsert(
        username=user,
        venue=(req.venue or "").strip(),
        label=(req.label or None),
        api_key=(req.api_key or "").strip(),
        api_secret=(req.api_secret or None),
        passphrase=(req.passphrase or None),
    )
    return {"ok": True, "item": meta}


@router.delete("/api_keys/{key_id}")
def api_keys_delete(key_id: int, x_utt_totp: Optional[str] = Header(default=None, alias="X-UTT-TOTP"), ident: dict = Depends(require_auth)):
    """Delete an API key bundle by id. Metadata-only listing means delete by id is safest."""
    if not _auth_db_enabled():
        raise HTTPException(status_code=501, detail="API keys are only available in DB auth mode (set UTT_AUTH_DB=1).")
    user = (ident.get("user") or "").strip() or "local"

    if _db_totp_enabled(user):
        code = (x_utt_totp or "").strip()
        if len(code) < 6:
            raise HTTPException(status_code=400, detail="Current 2FA code is required.")
        secret = _db_get_totp_secret(user) or ""
        if (not secret) or (code not in _totp_now(secret)):
            raise HTTPException(status_code=401, detail="Invalid 2FA code.")

    _db_api_keys_delete(user, int(key_id))
    return {"ok": True}



@router.post("/2fa/setup")
def auth_2fa_setup(ident: dict = Depends(require_auth)):
    """Provision a new TOTP secret for the current user.

    DB mode: stores the secret encrypted in DB (totp_enabled stays False until /2fa/enable).
    Shared mode: returns a secret only; operator must copy it into UTT_AUTH_TOTP_SECRET and restart.
    """
    user = (ident.get("user") or "local").strip() or "local"

    # DB-backed: one-time setup (idempotent until enabled)
    if _auth_db_enabled():
        if _db_totp_enabled(user):
            raise HTTPException(status_code=409, detail="2FA is already enabled. Use Reset 2FA to rotate the secret.")

        # If a secret already exists (but not enabled), return it instead of rotating.
        existing = _db_get_totp_secret(user) or ""
        if existing.strip():
            otpauth = f"otpauth://totp/UTT:{user}?secret={existing}&issuer=UTT"
            return {
                "ok": True,
                "mode": "db",
                "user": user,
                "totp_provisioning": {"secret": existing, "otpauth_url": otpauth},
                "note": "2FA is provisioned but not enabled yet. Complete /api/auth/2fa/enable with a valid code to enforce 2FA.",
            }

        secret_b32 = base64.b32encode(secrets.token_bytes(20)).decode("utf-8").rstrip("=")
        _db_set_totp_secret(user, secret_b32, enabled=False)
        otpauth = f"otpauth://totp/UTT:{user}?secret={secret_b32}&issuer=UTT"
        return {
            "ok": True,
            "mode": "db",
            "user": user,
            "totp_provisioning": {"secret": secret_b32, "otpauth_url": otpauth},
            "note": "Run /api/auth/2fa/enable with a valid 6-digit code to turn on 2FA enforcement for this user.",
        }

    # Shared-password mode: generate only (not persisted server-side)
    if not _auth_required_shared():
        raise HTTPException(status_code=501, detail="Auth is not configured (set UTT_AUTH_PASSWORD).")

    secret_b32 = base64.b32encode(secrets.token_bytes(20)).decode("utf-8").rstrip("=")
    otpauth = f"otpauth://totp/UTT:{user}?secret={secret_b32}&issuer=UTT"
    return {
        "ok": True,
        "mode": "shared",
        "user": user,
        "totp_provisioning": {"secret": secret_b32, "otpauth_url": otpauth},
        "warning": "Shared-password mode cannot persist per-user 2FA. Copy the secret into UTT_AUTH_TOTP_SECRET in backend.env and restart the backend to enforce 2FA.",
    }


@router.post("/2fa/reset")
def auth_2fa_reset(req: Optional[Reset2FARequest] = None, ident: dict = Depends(require_auth)):
    """Rotate the TOTP secret.

    DB mode: replaces the stored secret and disables 2FA until /2fa/enable is completed again.
    Shared mode: returns a new secret; operator must update UTT_AUTH_TOTP_SECRET and restart.
    """
    user = (ident.get("user") or "local").strip() or "local"

    if _auth_db_enabled():
        already_enabled = bool(_db_totp_enabled(user))
        if already_enabled:
            pw = (req.password if req else "") or ""
            code = (req.totp if req else "") or ""
            if not pw.strip():
                raise HTTPException(status_code=400, detail="Password is required to reset 2FA.")
            if not _db_verify_password(user, pw):
                raise HTTPException(status_code=401, detail="Invalid credentials.")
            cur_secret = _db_get_totp_secret(user) or ""
            if not cur_secret:
                raise HTTPException(status_code=400, detail="No current 2FA secret is set.")
            if (code or "").strip() not in _totp_now(cur_secret):
                raise HTTPException(status_code=401, detail="Invalid 2FA code.")

        secret_b32 = base64.b32encode(secrets.token_bytes(20)).decode("utf-8").rstrip("=")
        _db_set_totp_secret(user, secret_b32, enabled=False)
        otpauth = f"otpauth://totp/UTT:{user}?secret={secret_b32}&issuer=UTT"
        return {
            "ok": True,
            "mode": "db",
            "user": user,
            "totp_provisioning": {"secret": secret_b32, "otpauth_url": otpauth},
            "note": "2FA has been reset and is now disabled until you complete /api/auth/2fa/enable with a valid code from the new secret.",
        }

    if not _auth_required_shared():
        raise HTTPException(status_code=501, detail="Auth is not configured (set UTT_AUTH_PASSWORD).")

    secret_b32 = base64.b32encode(secrets.token_bytes(20)).decode("utf-8").rstrip("=")
    otpauth = f"otpauth://totp/UTT:{user}?secret={secret_b32}&issuer=UTT"
    return {
        "ok": True,
        "mode": "shared",
        "user": user,
        "totp_provisioning": {"secret": secret_b32, "otpauth_url": otpauth},
        "warning": "Copy the secret into UTT_AUTH_TOTP_SECRET in backend.env and restart the backend to enforce 2FA.",
    }



@router.post("/2fa/enable")
def auth_2fa_enable(req: Enable2FARequest, ident: dict = Depends(require_auth)):
    if not _auth_db_enabled():
        raise HTTPException(status_code=501, detail="2FA enable is not available (set UTT_AUTH_DB=1).")
    user = ident.get("user") or ""
    secret = _db_get_totp_secret(user) or ""
    if not secret:
        raise HTTPException(status_code=400, detail="No TOTP secret provisioned. Call /api/auth/signup or setup first.")
    code = (req.totp or "").strip()
    if code not in _totp_now(secret):
        raise HTTPException(status_code=401, detail="Invalid 2FA code.")
    _db_set_totp_secret(user, secret, enabled=True)
    return {"ok": True, "user": user, "totp_enabled": True}
