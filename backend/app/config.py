from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator
from typing import List, Set, Optional
import base64
import os
import sqlite3
import json
import hashlib

try:
    from cryptography.fernet import Fernet
except Exception:  # pragma: no cover
    Fernet = None  # type: ignore


# config.py lives at: backend/app/config.py
# We want env_file to reliably point to backend/.env regardless of where uvicorn is launched from.
_BACKEND_DIR = Path(__file__).resolve().parents[1]  # .../backend
_DEFAULT_ENV_PATH = _BACKEND_DIR / ".env"


def _hydrate_os_environ_from_env_file(env_path: Path) -> None:
    """
    Best-effort: load key=value pairs from backend/.env into os.environ *only if*
    the key is not already present in the process environment.

    Why: Pydantic Settings reads env_file into the Settings object, but modules
    that use os.getenv(...) will not see backend/.env unless the shell exported
    vars. This keeps existing runtime env vars authoritative.
    """
    try:
        if not env_path.exists() or not env_path.is_file():
            return

        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            if "=" not in line:
                continue

            k, v = line.split("=", 1)
            key = k.strip()
            if not key:
                continue

            # Keep already-exported env vars authoritative
            if key in os.environ and str(os.environ.get(key) or "").strip() != "":
                continue

            val = v.strip()

            # Strip surrounding quotes if present
            if len(val) >= 2 and ((val[0] == val[-1] == '"') or (val[0] == val[-1] == "'")):
                val = val[1:-1]

            # Do not inject empty values
            if val == "":
                continue

            os.environ[key] = val
    except Exception:
        return


# 1) First pass: hydrate from default backend/.env so UTT_ENV_PATH can be discovered.
_hydrate_os_environ_from_env_file(_DEFAULT_ENV_PATH)

# 2) Compute selected env path (supports absolute paths or paths relative to backend/.)
_raw_env_override = (os.getenv("UTT_ENV_PATH") or "").strip()
if _raw_env_override:
    p = Path(_raw_env_override)
    _ENV_PATH = p if p.is_absolute() else (_BACKEND_DIR / p)
else:
    _ENV_PATH = _DEFAULT_ENV_PATH

# 3) Second pass: hydrate from selected env file (no-op if same).
if _ENV_PATH != _DEFAULT_ENV_PATH:
    _hydrate_os_environ_from_env_file(_ENV_PATH)


def _is_valid_b64_nonempty(s: Optional[str]) -> bool:
    """
    True if s is valid base64 and decodes to non-empty bytes.
    Never raises.
    """
    try:
        if not s or not isinstance(s, str):
            return False
        raw = base64.b64decode(s.encode("utf-8"), validate=True)
        return bool(raw)
    except Exception:
        return False


class Settings(BaseSettings):
    # IMPORTANT:
    # extra="ignore" allows backend/.env to contain env vars used by other modules via os.getenv
    # (e.g., services/market.py, services/balances.py) without crashing Settings validation.
    model_config = SettingsConfigDict(
        env_file=str(_ENV_PATH),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_host: str = Field(default="127.0.0.1", alias="APP_HOST")
    app_port: int = Field(default=8000, alias="APP_PORT")
    cors_origins: str = Field(default="http://localhost:3000", alias="CORS_ORIGINS")

    sqlite_path: str = Field(default="./data/app.db", alias="SQLITE_PATH")
    backup_dir: str = Field(default="./data/backups", alias="BACKUP_DIR")

    # Safety toggles
    dry_run: bool = Field(default=True, alias="DRY_RUN")
    armed: bool = Field(default=False, alias="ARMED")

    # LIVE venue allow-list (comma-separated). Example: "gemini"
    # When DRY_RUN=false and ARMED=true, routers/trade.py requires this to be set.
    live_venues: str = Field(default="", alias="LIVE_VENUES")

    # Venue credentials (leave blank until you wire each venue)
    gemini_api_key: str = Field(default="", alias="GEMINI_API_KEY")
    gemini_api_secret: str = Field(default="", alias="GEMINI_API_SECRET")

    kraken_api_key: str = Field(default="", alias="KRAKEN_API_KEY")
    kraken_api_secret: str = Field(default="", alias="KRAKEN_API_SECRET")

    coinbase_api_key: str = Field(default="", alias="COINBASE_API_KEY")
    coinbase_api_secret: str = Field(default="", alias="COINBASE_API_SECRET")
    coinbase_api_secret_path: str = Field(default="", alias="COINBASE_API_SECRET_PATH")

    # Coinbase adapter cooldown tuning (used by backend/app/adapters/coinbase.py via os.getenv)
    # Adding this prevents Pydantic Settings from rejecting the env var as an "extra" input.
    coinbase_too_many_errors_cooldown_s: int = Field(default=900, alias="COINBASE_TOO_MANY_ERRORS_COOLDOWN_S")

    # ─────────────────────────────────────────────────────────────
    # Crypto.com Exchange integration — optional (enabled when creds present)
    # ─────────────────────────────────────────────────────────────
    cryptocom_exchange_api_key: str = Field(default="", alias="CRYPTOCOM_EXCHANGE_API_KEY")
    cryptocom_exchange_api_secret: str = Field(default="", alias="CRYPTOCOM_EXCHANGE_API_SECRET")
    cryptocom_exchange_base_url: str = Field(default="https://api.crypto.com/exchange/v1", alias="CRYPTOCOM_EXCHANGE_BASE_URL")

    # Optional venue enable toggle for routers that require explicit enable.
    cryptocom_enabled: bool = Field(default=False, alias="CRYPTOCOM_ENABLED")

    # ─────────────────────────────────────────────────────────────
    # Robinhood (Crypto) integration — optional & guarded
    # ─────────────────────────────────────────────────────────────
    robinhood_enabled: bool = Field(default=False, alias="ROBINHOOD_ENABLED")

    robinhood_crypto_api_key_id: Optional[str] = Field(default=None, alias="ROBINHOOD_CRYPTO_API_KEY_ID")
    robinhood_crypto_public_key_b64: Optional[str] = Field(default=None, alias="ROBINHOOD_CRYPTO_PUBLIC_KEY_B64")
    robinhood_crypto_private_key_b64: Optional[str] = Field(default=None, alias="ROBINHOOD_CRYPTO_PRIVATE_KEY_B64")

    # Keep as string (not URL-typed) so blank/None never crashes Settings validation.
    # Adapter can apply defaults and/or validate later.
    robinhood_crypto_base_url: Optional[str] = Field(default=None, alias="ROBINHOOD_CRYPTO_BASE_URL")

    # ─────────────────────────────────────────────────────────────
    # Dex-Trade integration — optional & guarded
    #
    # Docs: token header "login-token", signature "X-Auth-Sign"
    # We still name the env vars in our own namespace: DEX_TRADE_*
    # ─────────────────────────────────────────────────────────────
    dex_trade_enabled: bool = Field(default=False, alias="DEX_TRADE_ENABLED")

    # Dex-Trade calls this "login-token" in headers; we store it as DEX_TRADE_LOGIN_TOKEN.
    dex_trade_login_token: Optional[str] = Field(default=None, alias="DEX_TRADE_LOGIN_TOKEN")
    dex_trade_secret: Optional[str] = Field(default=None, alias="DEX_TRADE_SECRET")

    # Optional; adapter can fall back to these defaults if unset.
    dex_trade_base_url: Optional[str] = Field(default=None, alias="DEX_TRADE_BASE_URL")
    dex_trade_socket_base_url: Optional[str] = Field(default=None, alias="DEX_TRADE_SOCKET_BASE_URL")

    def model_post_init(self, __context) -> None:
        # Vault-first hydration for Crypto.com: some routers/guards check the Settings fields
        # directly (cryptocom_exchange_api_key/secret). If env secrets are removed, mirror vault
        # values into these fields after model init.
        try:
            if (not (self.cryptocom_exchange_api_key or "").strip()) or (not (self.cryptocom_exchange_api_secret or "").strip()):
                vc = None
                try:
                    vc = self.cryptocom_private_creds()
                except Exception:
                    vc = None
                if isinstance(vc, (list, tuple)) and len(vc) >= 2:
                    k = (vc[0] or "").strip()
                    s = (vc[1] or "").strip()
                    if k and s:
                        self.cryptocom_exchange_api_key = k
                        self.cryptocom_exchange_api_secret = s
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────
# API Key Vault credential resolution (DB-backed auth mode)
#
# NOTE:
# - Stored by /api/auth/api_keys in auth.py into sqlite table `utt_api_keys`
# - secret_enc contains encrypted JSON: {"api_key":..., "api_secret":..., "passphrase":...}
#
# This is intentionally lightweight and avoids importing routers (prevents circular imports).
# ─────────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────────────────────
    # API Key Vault (DB) — lightweight reader for adapters
    # (avoids importing routers/services to prevent circular imports)
    #
    # Vault secret bundle is JSON like: {"api_key":..., "api_secret":..., "passphrase":...}
    # Encrypted with Fernet key derived from UTT_KMS_MASTER_KEY.
    # ─────────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────────────────────
    # API Key Vault (DB) — lightweight reader for adapters
    # (avoids importing routers/services to prevent circular imports)
    #
    # Vault secret bundle is JSON like: {"api_key":..., "api_secret":..., "passphrase":...}
    # Encrypted with Fernet key derived from UTT_KMS_MASTER_KEY.
    # ─────────────────────────────────────────────────────────────

    def _vault_fernet(self):
        if Fernet is None:
            return None

        mk = (os.getenv("UTT_KMS_MASTER_KEY") or "").strip()
        if not mk:
            # Dev fallback. Prefer UTT_KMS_MASTER_KEY.
            mk = (os.getenv("UTT_AUTH_SECRET") or os.getenv("UTT_AUTH_PASSWORD") or "utt-dev-secret").strip()

        key = base64.urlsafe_b64encode(hashlib.sha256(mk.encode("utf-8")).digest())
        try:
            return Fernet(key)
        except Exception:
            return None

    def _vault_decrypt(self, secret_enc: str) -> dict:
        f = self._vault_fernet()
        if f is None:
            return {}
        try:
            raw = f.decrypt((secret_enc or "").encode("utf-8"))
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return {}

    def _vault_latest_bundle(self, venue: str, username: Optional[str] = None) -> Optional[dict]:
        # Username resolution:
        #  - explicit param first
        #  - UTT_VAULT_USERNAME env override next
        #  - fall back to "local" (legacy single-user default)
        cands: List[str] = []
        if isinstance(username, str) and username.strip():
            cands.append(username.strip())
        env_u = (os.getenv("UTT_VAULT_USERNAME") or "").strip()
        if env_u:
            cands.append(env_u)
        cands.append("local")

        # De-dupe while preserving order
        seen_u: set[str] = set()
        users: List[str] = []
        for u in cands:
            if u not in seen_u:
                seen_u.add(u)
                users.append(u)

        v = (venue or "").strip()
        if not v:
            return None

        # DB path resolution:
        # Prefer the same SQLite file your app is using (SQLITE_PATH / Settings.sqlite_path).
        db_path = (self.sqlite_path or "").strip()
        if not db_path:
            db_path = (os.getenv("SQLITE_PATH") or "").strip()

        # Backward/compat fallbacks (older env var names)
        if not db_path:
            db_path = (os.getenv("UTT_DB_PATH") or os.getenv("UTT_AUTH_DB_PATH") or os.getenv("UTT_DB") or "").strip()

        if not db_path:
            # fallback to repo-local sqlite if present
            db_path = str(Path(__file__).resolve().parents[1] / "utt.sqlite")

        for u in users:
            try:
                conn = sqlite3.connect(db_path)
                try:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT secret_enc FROM utt_api_keys WHERE username=? AND venue=? ORDER BY created_at DESC LIMIT 1",
                        (u, v),
                    )
                    row = cur.fetchone()
                    if not row:
                        continue
                    secret_enc = row[0]
                    if not secret_enc:
                        continue
                    bundle = self._vault_decrypt(str(secret_enc))
                    if isinstance(bundle, dict):
                        return bundle
                finally:
                    conn.close()
            except Exception:
                continue

        return None


    def _normalize_pem(self, s: str) -> str:
        """Normalize PEM-ish text coming from env/vault.

        Handles common cases:
          - literal '\\n' sequences pasted into a single-line field
          - surrounding quotes
          - base64-wrapped PEM blocks
          - CRLF normalization
          - header/footer glued onto one line
          - payload re-wrapping (64-col) to satisfy OpenSSL/cryptography PEM parser
        """
        if not isinstance(s, str):
            return ""
        v = s.strip()

        # Strip surrounding quotes
        if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
            v = v[1:-1].strip()

        # If pasted with literal \n sequences (single-line), convert to real newlines.
        if "\\n" in v and "\n" not in v:
            v = v.replace("\\n", "\n")

        # Sometimes secrets are stored as base64 of the PEM text.
        if "BEGIN" not in v and _is_valid_b64_nonempty(v):
            try:
                decoded = base64.b64decode(v.encode("utf-8"), validate=True)
                cand = decoded.decode("utf-8", errors="ignore").strip()
                if "BEGIN" in cand and "END" in cand:
                    v = cand
            except Exception:
                pass

        # Normalize line endings
        v = v.replace("\r\n", "\n").replace("\r", "\n")

        # If header/footer are present but everything is glued together, add newlines around them.
        if "-----BEGIN" in v and "-----END" in v and "\n" not in v:
            v = v.replace("-----BEGIN", "\n-----BEGIN").replace("-----END", "\n-----END").strip()

        # Final hardening: if a PEM block exists, rebuild it with clean header/footer + 64-col payload.
        try:
            import re as _re

            m_begin = _re.search(r"-----BEGIN ([A-Z0-9 ]+?)-----", v)
            m_end = _re.search(r"-----END ([A-Z0-9 ]+?)-----", v)
            if m_begin and m_end:
                begin_type = m_begin.group(1).strip()
                end_type = m_end.group(1).strip()
                if begin_type == end_type:
                    # Extract everything between header and footer, remove whitespace, keep base64 charset only.
                    payload = v[m_begin.end() : m_end.start()]
                    payload = _re.sub(r"\s+", "", payload)
                    payload = _re.sub(r"[^A-Za-z0-9+/=]", "", payload)

                    # Rewrap into 64-character lines (PEM conventional)
                    lines = [payload[i : i + 64] for i in range(0, len(payload), 64) if payload[i : i + 64]]
                    if lines:
                        v = "\n".join(
                            [f"-----BEGIN {begin_type}-----"]
                            + lines
                            + [f"-----END {end_type}-----"]
                        )
        except Exception:
            pass

        return v.strip()


    def gemini_private_creds(self):
        """Optional callable for adapters: returns (api_key, api_secret) if present in DB vault."""
        bundle = self._vault_latest_bundle("gemini")
        if not bundle:
            return None
        api_key = (bundle.get("api_key") or "").strip()
        api_secret = self._normalize_pem((bundle.get("api_secret") or ""))
        if not api_key or not api_secret:
            return None
        return (api_key, api_secret)

    def kraken_private_creds(self):
        """Optional callable for adapters: returns (api_key, api_secret) if present in DB vault."""
        bundle = self._vault_latest_bundle("kraken")
        if not bundle:
            return None
        api_key = (bundle.get("api_key") or "").strip()
        api_secret = self._normalize_pem((bundle.get("api_secret") or ""))
        if not api_key or not api_secret:
            return None
        return (api_key, api_secret)




    def cryptocom_private_creds(self):
        """Optional callable for adapters: returns (api_key, api_secret) if present in DB vault.

        Stored under venue='cryptocom' in Profile → API Keys.
        Fallback: also checks venue='cryptocom_exchange' for backward compatibility.
        """
        bundle = self._vault_latest_bundle("cryptocom")
        if not bundle:
            bundle = self._vault_latest_bundle("cryptocom_exchange")
        if not bundle:
            return None
        api_key = (bundle.get("api_key") or "").strip()
        api_secret = (bundle.get("api_secret") or "").strip()
        if not api_key or not api_secret:
            return None
        return (api_key, api_secret)

    def cryptocom_effective_enabled(self) -> bool:
        """Guardrail for routers that require venue to be enabled/configured.

        Returns True when either:
          - vault creds exist (venue='cryptocom' or 'cryptocom_exchange'), OR
          - CRYPTOCOM_ENABLED is True and env/settings creds exist, OR
          - env/settings creds exist (legacy behavior).
        """
        # Env/settings creds
        k = (getattr(self, "cryptocom_exchange_api_key", "") or "").strip()
        s = (getattr(self, "cryptocom_exchange_api_secret", "") or "").strip()
        has_env = bool(k) and bool(s)

        # Vault creds
        vc = None
        try:
            vc = self.cryptocom_private_creds()
        except Exception:
            vc = None
        has_vault = (
            isinstance(vc, (list, tuple))
            and len(vc) >= 2
            and bool((vc[0] or "").strip())
            and bool((vc[1] or "").strip())
        )

        if has_vault:
            return True

        explicit = False
        try:
            explicit = bool(self.cryptocom_enabled)
        except Exception:
            explicit = False

        if explicit and has_env:
            return True
        if has_env:
            return True
        return False


    def coinbase_trade_private_creds(self):
        """Optional callable for adapters: returns (api_key_name, api_secret_pem) if present in DB vault.

        Stored under venue='coinbase' in Profile → API Keys:
          - api_key: Coinbase key name (organizations/{org_id}/apiKeys/{key_id})
          - api_secret: EC private key PEM (multiline)
        """
        bundle = self._vault_latest_bundle("coinbase")
        if not bundle:
            return None
        api_key = (bundle.get("api_key") or "").strip()
        api_secret = self._normalize_pem((bundle.get("api_secret") or ""))
        if not api_key or not api_secret:
            return None
        return (api_key, api_secret)

    def coinbase_transfers_private_creds(self):
        """Optional callable for adapters: returns (api_key_name, api_secret_pem) for transfers scope.

        Stored under venue='coinbase_transfers' in Profile → API Keys.
        """
        bundle = self._vault_latest_bundle("coinbase_transfers")
        if not bundle:
            return None
        api_key = (bundle.get("api_key") or "").strip()
        api_secret = self._normalize_pem((bundle.get("api_secret") or ""))
        if not api_key or not api_secret:
            return None
        return (api_key, api_secret)

    def dex_trade_private_creds(self):
        """Optional callable for adapters: returns (token, secret) if present in DB vault."""
        bundle = self._vault_latest_bundle("dex_trade")
        if not bundle:
            return None
        token = (bundle.get("api_key") or "").strip()
        secret = (bundle.get("api_secret") or "").strip()
        if not token or not secret:
            return None
        return (token, secret)



    @field_validator(
        # Robinhood fields
        "robinhood_crypto_api_key_id",
        "robinhood_crypto_public_key_b64",
        "robinhood_crypto_private_key_b64",
        "robinhood_crypto_base_url",
        # Dex-Trade fields
        "dex_trade_login_token",
        "dex_trade_secret",
        "dex_trade_base_url",
        "dex_trade_socket_base_url",
        mode="before",
    )
    @classmethod
    def _blank_to_none(cls, v):
        if v is None:
            return None
        if isinstance(v, str) and v.strip() == "":
            return None
        return v

    def robinhood_private_creds(self):
        """Optional callable for adapters: returns (api_key_id, private_key_b64, public_key_b64_or_empty) from DB vault.

        Stored under venue='robinhood' in Profile → API Keys:
          - api_key: Robinhood Crypto API key id (x-api-key)
          - api_secret: private key b64
          - passphrase: optional public key b64 (not required for signing today)
        """
        bundle = self._vault_latest_bundle("robinhood")
        if not bundle:
            return None
        api_key_id = (bundle.get("api_key") or "").strip()
        priv_b64 = (bundle.get("api_secret") or "").strip()
        pub_b64 = (bundle.get("passphrase") or "").strip()
        if not api_key_id or not priv_b64:
            return None
        return (api_key_id, priv_b64, pub_b64)

    def robinhood_effective_enabled(self) -> bool:
        """Guardrail:
          - If ROBINHOOD_ENABLED is not explicitly enabled -> False
          - Require API key id + PRIVATE key b64 (public key is optional)
          - Require base_url to be http(s)
          - Validate base64 only for the private key; validate public key only if present
        """
        if not self.robinhood_enabled:
            return False

        key_id = (self.robinhood_crypto_api_key_id or "").strip() if isinstance(self.robinhood_crypto_api_key_id, str) or self.robinhood_crypto_api_key_id is not None else ""
        priv_b64 = (self.robinhood_crypto_private_key_b64 or "").strip() if isinstance(self.robinhood_crypto_private_key_b64, str) or self.robinhood_crypto_private_key_b64 is not None else ""
        pub_b64 = (self.robinhood_crypto_public_key_b64 or "").strip() if isinstance(self.robinhood_crypto_public_key_b64, str) or self.robinhood_crypto_public_key_b64 is not None else ""
        base_url = (self.robinhood_crypto_base_url or "").strip() if isinstance(self.robinhood_crypto_base_url, str) or self.robinhood_crypto_base_url is not None else ""

        # Vault fallback when env creds are removed.
        if (not key_id) or (not priv_b64):
            try:
                vc = self.robinhood_private_creds()
            except Exception:
                vc = None
            if isinstance(vc, (list, tuple)) and len(vc) >= 2:
                k2 = vc[0] if len(vc) >= 1 else None
                s2 = vc[1] if len(vc) >= 2 else None
                p2 = vc[2] if len(vc) >= 3 else None
                if not key_id and isinstance(k2, str) and k2.strip():
                    key_id = k2.strip()
                if not priv_b64 and isinstance(s2, str) and s2.strip():
                    priv_b64 = s2.strip()
                if (not pub_b64) and isinstance(p2, str) and p2.strip():
                    pub_b64 = p2.strip()

        if not key_id or not priv_b64 or not base_url:
            return False

        if not (base_url.startswith("https://") or base_url.startswith("http://")):
            return False

        if not _is_valid_b64_nonempty(priv_b64):
            return False
        if pub_b64 and (not _is_valid_b64_nonempty(pub_b64)):
            return False

        return True


    def dex_trade_effective_enabled(self) -> bool:
        """
        Guardrail:
          - If Dex-Trade is not explicitly enabled -> False
          - If token/secret are missing -> False

        Base URLs may be omitted; adapters can apply defaults.
        """
        if not self.dex_trade_enabled:
            return False

        token = self.dex_trade_login_token
        secret = self.dex_trade_secret

        # DB-vault fallback (when env vars are removed). This keeps venue hidden until creds exist.
        if not (isinstance(token, str) and token.strip()) or not (isinstance(secret, str) and secret.strip()):
            vc = None
            try:
                vc = self.dex_trade_private_creds()
            except Exception:
                vc = None
            if isinstance(vc, (list, tuple)) and len(vc) >= 2:
                t2, s2 = (vc[0] or ""), (vc[1] or "")
                if isinstance(t2, str) and t2.strip() and isinstance(s2, str) and s2.strip():
                    return True
            return False

        return True

    def dex_trade_effective_base_url(self) -> str:
        """
        Convenience: returns configured base url or the Dex-Trade default.
        """
        return (self.dex_trade_base_url or "https://api.dex-trade.com").strip()

    def dex_trade_effective_socket_base_url(self) -> str:
        """
        Convenience: returns configured socket base url or the Dex-Trade default.
        """
        return (self.dex_trade_socket_base_url or "https://socket.dex-trade.com").strip()

    def resolved_sqlite_path(self) -> Path:
        """Return the active SQLite path as an absolute filesystem path."""
        raw = (self.sqlite_path or "").strip()
        if not raw:
            raw = (os.getenv("SQLITE_PATH") or os.getenv("UTT_DB_PATH") or os.getenv("UTT_AUTH_DB_PATH") or os.getenv("UTT_DB") or "./data/app.db").strip()
        p = Path(raw).expanduser()
        return p if p.is_absolute() else (_BACKEND_DIR / p).resolve()

    def resolved_backup_dir(self) -> Path:
        """Return the backup directory as an absolute filesystem path."""
        raw = (getattr(self, "backup_dir", "") or os.getenv("BACKUP_DIR") or "").strip()
        if raw:
            p = Path(raw).expanduser()
            return p if p.is_absolute() else (_BACKEND_DIR / p).resolve()
        dbp = self.resolved_sqlite_path()
        return (dbp.parent / "backups").resolve()

    def cors_list(self) -> List[str]:
        # comma-separated list
        return [o.strip() for o in self.cors_origins.split(",") if o.strip()]

    def live_venues_set(self) -> Set[str]:
        """
        Normalized set of enabled LIVE venues, derived from LIVE_VENUES.
        Example LIVE_VENUES="gemini,coinbase" -> {"gemini","coinbase"}
        """
        s = (self.live_venues or "").strip()
        if not s:
            return set()
        return {p.strip().lower() for p in s.split(",") if p.strip()}


settings = Settings()
