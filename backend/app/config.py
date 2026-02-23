from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator
from typing import List, Set, Optional
import base64
import os


# config.py lives at: backend/app/config.py
# We want env_file to reliably point to backend/.env regardless of where uvicorn is launched from.
_BACKEND_DIR = Path(__file__).resolve().parents[1]  # .../backend
_ENV_PATH = _BACKEND_DIR / ".env"


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


# Hydrate os.environ early so os.getenv() in other modules sees backend/.env.
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

    def robinhood_effective_enabled(self) -> bool:
        """
        Guardrail:
          - If Robinhood is not explicitly enabled -> False
          - If any required crypto credential is missing/blank -> False
          - If base_url is missing or not http(s) -> False
          - If public/private keys are not valid base64 -> False

        This prevents Robinhood from appearing in the UI venues list until configured.
        """
        if not self.robinhood_enabled:
            return False

        key_id = self.robinhood_crypto_api_key_id
        pub_b64 = self.robinhood_crypto_public_key_b64
        priv_b64 = self.robinhood_crypto_private_key_b64
        base_url = self.robinhood_crypto_base_url

        # Required presence
        if not (isinstance(key_id, str) and key_id.strip()):
            return False
        if not (isinstance(base_url, str) and base_url.strip()):
            return False

        # Minimal URL sanity
        bu = base_url.strip()
        if not (bu.startswith("https://") or bu.startswith("http://")):
            return False

        # Base64 validity (prevents “enabled but broken”)
        if not _is_valid_b64_nonempty(pub_b64):
            return False
        if not _is_valid_b64_nonempty(priv_b64):
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

        if not (isinstance(token, str) and token.strip()):
            return False
        if not (isinstance(secret, str) and secret.strip()):
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
