from __future__ import annotations

import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException
from sqlalchemy import create_engine, text

from app.config import Settings
from app.routers import auth


_TEST_KMS = "K" * 64


@contextmanager
def _temporary_auth_engine(db_path: Path):
    old_engine = auth.engine
    engine = create_engine(f"sqlite:///{db_path}", future=True)
    auth.engine = engine
    try:
        yield engine
    finally:
        auth.engine = old_engine
        engine.dispose()


@contextmanager
def _vault_environment(*, owner: str | None = "admin", kms: str | None = _TEST_KMS):
    keys = (
        "UTT_KMS_MASTER_KEY",
        "UTT_VAULT_USERNAME",
        "UTT_AUTH_SECRET",
        "UTT_AUTH_PASSWORD",
        "UTT_AUTH_DB",
        "UNISWAP_API_KEY",
    )
    original = {key: os.environ.get(key) for key in keys}
    try:
        for key in keys:
            os.environ.pop(key, None)
        if kms is not None:
            os.environ["UTT_KMS_MASTER_KEY"] = kms
        if owner is not None:
            os.environ["UTT_VAULT_USERNAME"] = owner
        os.environ["UTT_AUTH_DB"] = "1"
        yield
    finally:
        for key in keys:
            os.environ.pop(key, None)
        for key, value in original.items():
            if value is not None:
                os.environ[key] = value


class CredentialVaultSecurityTests(unittest.TestCase):
    def test_missing_kms_fails_closed_and_token_signing_has_no_public_default(self):
        with _vault_environment(owner="admin", kms=None):
            self.assertIsNone(auth._fernet())
            self.assertIsNone(auth._api_keys_fernet())
            with self.assertRaises(HTTPException) as ctx:
                auth._api_keys_encrypt({"api_key": "test"})
            self.assertEqual(ctx.exception.status_code, 503)

            signing_secret = auth._auth_secret()
            self.assertEqual(signing_secret, auth._PROCESS_AUTH_SECRET)
            self.assertGreaterEqual(len(signing_secret), 48)
            self.assertNotEqual(signing_secret, "utt-dev-secret")

            settings = Settings(_env_file=None, SQLITE_PATH="unused.db")
            self.assertIsNone(settings._vault_fernet())

    def test_legacy_schema_migrates_without_reencrypting_existing_rows(self):
        with tempfile.TemporaryDirectory() as tmp, _vault_environment():
            db_path = Path(tmp) / "vault.db"
            with _temporary_auth_engine(db_path) as engine:
                ciphertext = auth._api_keys_encrypt(
                    {"api_key": "legacy-key", "api_secret": "legacy-secret", "passphrase": None}
                )
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                            CREATE TABLE utt_api_keys (
                              id INTEGER PRIMARY KEY AUTOINCREMENT,
                              username TEXT NOT NULL,
                              venue TEXT NOT NULL,
                              label TEXT,
                              key_hint TEXT,
                              secret_enc TEXT NOT NULL,
                              created_at INTEGER NOT NULL,
                              updated_at INTEGER NOT NULL
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            """
                            INSERT INTO utt_api_keys (
                                username, venue, label, key_hint, secret_enc, created_at, updated_at
                            )
                            VALUES (
                                'admin', 'gemini', 'legacy', '...-key', :secret, 10, 10
                            )
                            """
                        ),
                        {"secret": ciphertext},
                    )

                auth.ensure_api_key_vault_schema()

                with engine.begin() as conn:
                    columns = {
                        row["name"]
                        for row in conn.execute(text("PRAGMA table_info(utt_api_keys)")).mappings().all()
                    }
                    row = conn.execute(
                        text(
                            """
                            SELECT secret_enc, enabled, key_version, scope_read,
                                   scope_trade, scope_transfer, scope_withdraw, scope_source
                            FROM utt_api_keys
                            WHERE username = 'admin' AND venue = 'gemini'
                            """
                        )
                    ).mappings().one()
                    indexes = {
                        item["name"]
                        for item in conn.execute(text("PRAGMA index_list(utt_api_keys)")).mappings().all()
                    }

                self.assertTrue(
                    {
                        "enabled",
                        "disabled_at",
                        "disabled_reason",
                        "key_version",
                        "scope_read",
                        "scope_trade",
                        "scope_transfer",
                        "scope_withdraw",
                        "scope_source",
                    }.issubset(columns)
                )
                self.assertEqual(row["secret_enc"], ciphertext)
                self.assertEqual(int(row["enabled"]), 1)
                self.assertEqual(int(row["key_version"]), 1)
                self.assertEqual(int(row["scope_read"]), 1)
                self.assertEqual(int(row["scope_trade"]), 0)
                self.assertEqual(int(row["scope_transfer"]), 0)
                self.assertEqual(int(row["scope_withdraw"]), 0)
                self.assertEqual(row["scope_source"], "migration_default_unverified")
                self.assertIn("uq_utt_api_keys_active_user_venue", indexes)

                settings = Settings(_env_file=None, SQLITE_PATH=str(db_path))
                bundle = settings._vault_latest_bundle("gemini")
                self.assertEqual(bundle["api_key"], "legacy-key")
                self.assertEqual(bundle["api_secret"], "legacy-secret")

    def test_migration_disables_older_active_duplicates_before_unique_index(self):
        with tempfile.TemporaryDirectory() as tmp, _vault_environment():
            db_path = Path(tmp) / "vault.db"
            with _temporary_auth_engine(db_path) as engine:
                first = auth._api_keys_encrypt({"api_key": "first", "api_secret": None, "passphrase": None})
                second = auth._api_keys_encrypt({"api_key": "second", "api_secret": None, "passphrase": None})
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                            CREATE TABLE utt_api_keys (
                              id INTEGER PRIMARY KEY AUTOINCREMENT,
                              username TEXT NOT NULL,
                              venue TEXT NOT NULL,
                              label TEXT,
                              key_hint TEXT,
                              secret_enc TEXT NOT NULL,
                              created_at INTEGER NOT NULL,
                              updated_at INTEGER NOT NULL
                            )
                            """
                        )
                    )
                    conn.execute(
                        text(
                            """
                            INSERT INTO utt_api_keys
                              (username, venue, secret_enc, created_at, updated_at)
                            VALUES
                              ('admin', 'zerox', :first, 10, 10),
                              ('admin', 'zerox', :second, 20, 20)
                            """
                        ),
                        {"first": first, "second": second},
                    )

                auth.ensure_api_key_vault_schema()

                with engine.begin() as conn:
                    rows = conn.execute(
                        text(
                            """
                            SELECT id, enabled, disabled_reason, updated_at
                            FROM utt_api_keys
                            WHERE username = 'admin' AND venue = 'zerox'
                            ORDER BY updated_at DESC
                            """
                        )
                    ).mappings().all()

                self.assertEqual(len(rows), 2)
                self.assertEqual(int(rows[0]["enabled"]), 1)
                self.assertEqual(int(rows[1]["enabled"]), 0)
                self.assertEqual(rows[1]["disabled_reason"], "superseded_during_migration")

    def test_replacement_and_disable_cannot_resurrect_an_older_credential(self):
        with tempfile.TemporaryDirectory() as tmp, _vault_environment():
            db_path = Path(tmp) / "vault.db"
            with _temporary_auth_engine(db_path) as engine:
                auth.ensure_api_key_vault_schema()
                first = auth._db_api_keys_upsert(
                    "admin", "gemini", "first", "key-one", "secret-one", None
                )
                second = auth._db_api_keys_upsert(
                    "admin",
                    "gemini",
                    "second",
                    "key-two",
                    "secret-two",
                    None,
                    scope_read=True,
                    scope_trade=True,
                )

                with engine.begin() as conn:
                    rows = conn.execute(
                        text(
                            """
                            SELECT id, enabled, disabled_reason
                            FROM utt_api_keys
                            WHERE username = 'admin' AND venue = 'gemini'
                            ORDER BY id
                            """
                        )
                    ).mappings().all()

                self.assertEqual(first["id"], rows[0]["id"])
                self.assertEqual(second["id"], rows[1]["id"])
                self.assertEqual(int(rows[0]["enabled"]), 0)
                self.assertEqual(rows[0]["disabled_reason"], "replaced")
                self.assertEqual(int(rows[1]["enabled"]), 1)

                settings = Settings(_env_file=None, SQLITE_PATH=str(db_path))
                self.assertEqual(settings._vault_latest_bundle("gemini")["api_key"], "key-two")

                result = auth._db_api_keys_disable("admin", second["id"])
                self.assertTrue(result["disabled"])
                self.assertIsNone(settings._vault_latest_bundle("gemini"))

                items = auth._db_api_keys_list("admin")
                self.assertEqual(len(items), 2)
                self.assertTrue(all(not item["enabled"] for item in items))
                self.assertTrue(all("secret_enc" not in item for item in items))
                self.assertTrue(all("api_key" not in item for item in items))
                self.assertEqual(auth._db_api_keys_summary("admin")["active_count"], 0)

    def test_panic_disable_is_user_scoped_and_no_any_user_fallback_exists(self):
        with tempfile.TemporaryDirectory() as tmp, _vault_environment():
            db_path = Path(tmp) / "vault.db"
            with _temporary_auth_engine(db_path):
                auth.ensure_api_key_vault_schema()
                auth._db_api_keys_upsert("admin", "gemini", None, "admin-gemini", "s1", None)
                auth._db_api_keys_upsert("admin", "zerox", None, "admin-zerox", None, None)
                auth._db_api_keys_upsert("other", "gemini", None, "other-gemini", "s2", None)

                settings = Settings(_env_file=None, SQLITE_PATH=str(db_path))
                self.assertEqual(settings._vault_latest_bundle("gemini")["api_key"], "admin-gemini")

                result = auth._db_api_keys_panic_disable("admin")
                self.assertEqual(result["disabled_count"], 2)
                self.assertEqual(auth._db_api_keys_summary("admin")["active_count"], 0)
                self.assertEqual(auth._db_api_keys_summary("other")["active_count"], 1)
                self.assertIsNone(settings._vault_latest_bundle("gemini"))
                self.assertEqual(
                    settings._vault_latest_bundle("gemini", username="other")["api_key"],
                    "other-gemini",
                )

                os.environ.pop("UTT_VAULT_USERNAME", None)
                self.assertIsNone(settings._vault_latest_bundle("gemini"))

    def test_withdrawal_scope_requires_acknowledgement_and_responses_are_metadata_only(self):
        with tempfile.TemporaryDirectory() as tmp, _vault_environment():
            db_path = Path(tmp) / "vault.db"
            with _temporary_auth_engine(db_path):
                auth.ensure_api_key_vault_schema()

                rejected = auth.ApiKeyUpsertRequest(
                    venue="okx",
                    api_key="key",
                    api_secret="secret",
                    passphrase="phrase",
                    scope_withdraw=True,
                    withdrawal_risk_acknowledged=False,
                )
                with self.assertRaises(HTTPException) as ctx:
                    auth.api_keys_upsert(rejected, ident={"user": "admin"})
                self.assertEqual(ctx.exception.status_code, 400)

                accepted = auth.ApiKeyUpsertRequest(
                    venue="okx",
                    label="explicit-danger-test",
                    api_key="key",
                    api_secret="secret",
                    passphrase="phrase",
                    scope_read=True,
                    scope_trade=True,
                    scope_transfer=False,
                    scope_withdraw=True,
                    withdrawal_risk_acknowledged=True,
                )
                response = auth.api_keys_upsert(accepted, ident={"user": "admin"})
                self.assertTrue(response["ok"])
                self.assertTrue(response["item"]["scope_withdraw"])
                self.assertEqual(response["summary"]["withdrawal_scope_count"], 1)

                listed = auth.api_keys_list(ident={"user": "admin"})
                encoded = repr(listed)
                self.assertNotIn("secret_enc", encoded)
                self.assertNotIn("api_secret", encoded)
                self.assertNotIn("passphrase", encoded)
                self.assertEqual(listed["summary"]["active_count"], 1)
                self.assertEqual(listed["items"][0]["scope_source"], "operator_declared")
                self.assertTrue(listed["vault"]["owner_matches_user"])

                os.environ["UTT_VAULT_USERNAME"] = "other"
                with self.assertRaises(HTTPException) as owner_ctx:
                    auth.api_keys_upsert(
                        auth.ApiKeyUpsertRequest(venue="gemini", api_key="key"),
                        ident={"user": "admin"},
                    )
                self.assertEqual(owner_ctx.exception.status_code, 403)

    def test_uniswap_api_credential_requires_operator_declared_read_only_scope(self):
        with tempfile.TemporaryDirectory() as tmp, _vault_environment():
            db_path = Path(tmp) / "vault.db"
            with _temporary_auth_engine(db_path):
                auth.ensure_api_key_vault_schema()
                safe = auth._db_api_keys_upsert(
                    "admin",
                    "uniswap_api",
                    "read-only",
                    "uniswap-key",
                    None,
                    None,
                    scope_read=True,
                    scope_trade=False,
                    scope_transfer=False,
                    scope_withdraw=False,
                )

                settings = Settings(_env_file=None, SQLITE_PATH=str(db_path))
                credential = settings.robinhood_chain_uniswap_api_credential()
                self.assertIsNotNone(credential)
                self.assertEqual(credential["api_key"], "uniswap-key")
                self.assertTrue(credential["api_key_configured"])
                self.assertTrue(credential["declared_read_only"])
                self.assertFalse(credential["dangerous_scope_present"])
                self.assertEqual(credential["scope_source"], "operator_declared")

                auth._db_api_keys_disable("admin", safe["id"])
                os.environ["UNISWAP_API_KEY"] = "environment-key-must-not-be-used"
                self.assertIsNone(settings.robinhood_chain_uniswap_api_credential())

                auth._db_api_keys_upsert(
                    "admin",
                    "uniswap_api",
                    "unsafe",
                    "unsafe-key",
                    None,
                    None,
                    scope_read=True,
                    scope_trade=True,
                    scope_transfer=False,
                    scope_withdraw=False,
                )
                unsafe = settings.robinhood_chain_uniswap_api_credential()
                self.assertIsNotNone(unsafe)
                self.assertTrue(unsafe["api_key_configured"])
                self.assertIsNone(unsafe["api_key"])
                self.assertFalse(unsafe["declared_read_only"])
                self.assertTrue(unsafe["dangerous_scope_present"])

    def test_vault_mirrored_cryptocom_fields_clear_after_disable_but_env_fields_remain(self):
        with tempfile.TemporaryDirectory() as tmp, _vault_environment():
            db_path = Path(tmp) / "vault.db"
            with _temporary_auth_engine(db_path):
                auth.ensure_api_key_vault_schema()
                created = auth._db_api_keys_upsert(
                    "admin", "cryptocom", None, "vault-key", "vault-secret", None
                )

                settings = Settings(_env_file=None, SQLITE_PATH=str(db_path))
                settings.refresh_vault_backed_fields()
                self.assertTrue(settings._cryptocom_vault_mirrored)
                self.assertEqual(settings.cryptocom_exchange_api_key, "vault-key")

                auth._db_api_keys_disable("admin", created["id"])
                settings.refresh_vault_backed_fields()
                self.assertFalse(settings._cryptocom_vault_mirrored)
                self.assertEqual(settings.cryptocom_exchange_api_key, "")
                self.assertEqual(settings.cryptocom_exchange_api_secret, "")

                env_settings = Settings(
                    _env_file=None,
                    SQLITE_PATH=str(db_path),
                    CRYPTOCOM_EXCHANGE_API_KEY="env-key",
                    CRYPTOCOM_EXCHANGE_API_SECRET="env-secret",
                )
                env_settings.refresh_vault_backed_fields()
                self.assertFalse(env_settings._cryptocom_vault_mirrored)
                self.assertEqual(env_settings.cryptocom_exchange_api_key, "env-key")
                self.assertEqual(env_settings.cryptocom_exchange_api_secret, "env-secret")


if __name__ == "__main__":
    unittest.main()
