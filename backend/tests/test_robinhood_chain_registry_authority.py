from __future__ import annotations

import unittest
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.models import RobinhoodChainPairCapability, RobinhoodChainPairObjective, TokenRegistry
from app.routers.token_registry import (
    TokenRegistryCreate,
    TokenRegistryUpdate,
    create_token,
    list_tokens,
    update_token,
)
from app.services.robinhood_chain_registry_authority import (
    ASSET_KIND_ERC20,
    ASSET_KIND_NATIVE,
    ROBINHOOD_CHAIN_VENUE,
    RobinhoodChainRegistryAuthorityError,
    effective_row_by_symbol,
    ensure_token_registry_contract_identity_schema,
    require_token_registry_contract_identity_schema,
    select_effective_registry_rows,
    resolve_robinhood_chain_execution_authority,
)


class RobinhoodChainRegistryAuthorityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite+pysqlite:///:memory:")
        TokenRegistry.__table__.create(self.engine)
        with self.engine.begin() as connection:
            connection.execute(text("ALTER TABLE token_registry ADD COLUMN external_price_source TEXT"))
            connection.execute(text("ALTER TABLE token_registry ADD COLUMN external_price_id TEXT"))
        self.SessionLocal = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.db: Session = self.SessionLocal()

    def tearDown(self) -> None:
        self.db.close()
        self.engine.dispose()

    @staticmethod
    def _contract(byte_pair: str) -> str:
        return "0x" + str(byte_pair) * 20

    def _create(
        self,
        *,
        symbol: str,
        address: str | None,
        decimals: int,
        asset_kind: str,
        venue: str | None = None,
    ) -> dict:
        request = TokenRegistryCreate(
            chain="robinhood_chain",
            venue=venue,
            symbol=symbol,
            address=address,
            asset_kind=asset_kind,
            decimals=decimals,
            label=f"{symbol} test identity",
        )
        return create_token(request, self.db)["item"]

    def test_arbitrary_native_symbol_and_decimals_are_persisted(self) -> None:
        item = self._create(
            symbol="GASX",
            address=None,
            decimals=9,
            asset_kind=ASSET_KIND_NATIVE,
        )
        self.assertEqual(item["symbol"], "GASX")
        self.assertEqual(item["decimals"], 9)
        self.assertIsNone(item["address"])
        self.assertEqual(item["asset_kind"], ASSET_KIND_NATIVE)
        self.assertTrue(item["native"])

    def test_arbitrary_erc20_identity_uses_registry_contract_and_decimals(self) -> None:
        contract = self._contract("ab")
        item = self._create(
            symbol="ALPHA",
            address=contract,
            decimals=7,
            asset_kind=ASSET_KIND_ERC20,
        )
        self.assertEqual(item["symbol"], "ALPHA")
        self.assertEqual(item["address"].lower(), contract.lower())
        self.assertEqual(item["decimals"], 7)
        self.assertEqual(item["asset_kind"], ASSET_KIND_ERC20)
        self.assertFalse(item["native"])

    def test_second_native_in_same_scope_is_rejected(self) -> None:
        self._create(
            symbol="GASX",
            address=None,
            decimals=9,
            asset_kind=ASSET_KIND_NATIVE,
        )
        with self.assertRaises(HTTPException) as caught:
            self._create(
                symbol="FUEL",
                address=None,
                decimals=6,
                asset_kind=ASSET_KIND_NATIVE,
            )
        self.assertEqual(
            caught.exception.detail["error"],
            "duplicate_robinhood_chain_native_registry_scope",
        )

    def test_matching_symbol_global_and_venue_native_override_is_allowed(self) -> None:
        global_item = self._create(
            symbol="GASX",
            address=None,
            decimals=9,
            asset_kind=ASSET_KIND_NATIVE,
        )
        venue_item = self._create(
            symbol="GASX",
            address=None,
            decimals=8,
            asset_kind=ASSET_KIND_NATIVE,
            venue=ROBINHOOD_CHAIN_VENUE,
        )
        rows = select_effective_registry_rows(self.db)
        native_rows = [row for row in rows if not str(row.address or "").strip()]
        self.assertEqual(len(native_rows), 1)
        self.assertEqual(int(native_rows[0].id), int(venue_item["id"]))
        self.assertNotEqual(int(native_rows[0].id), int(global_item["id"]))
        self.assertEqual(int(native_rows[0].decimals), 8)

    def test_cross_scope_native_override_with_different_symbol_is_rejected(self) -> None:
        self._create(
            symbol="GASX",
            address=None,
            decimals=9,
            asset_kind=ASSET_KIND_NATIVE,
        )
        with self.assertRaises(HTTPException) as caught:
            self._create(
                symbol="FUEL",
                address=None,
                decimals=9,
                asset_kind=ASSET_KIND_NATIVE,
                venue=ROBINHOOD_CHAIN_VENUE,
            )
        self.assertEqual(
            caught.exception.detail["error"],
            "ambiguous_robinhood_chain_native_registry_identity",
        )

    def test_update_contract_to_native_obeys_scope_conflict_guard(self) -> None:
        self._create(
            symbol="GASX",
            address=None,
            decimals=9,
            asset_kind=ASSET_KIND_NATIVE,
        )
        contract_item = self._create(
            symbol="ALPHA",
            address=self._contract("cd"),
            decimals=7,
            asset_kind=ASSET_KIND_ERC20,
        )
        request = TokenRegistryUpdate(
            address="",
            asset_kind=ASSET_KIND_NATIVE,
            decimals=7,
        )
        with self.assertRaises(HTTPException) as caught:
            update_token(int(contract_item["id"]), request, self.db)
        self.assertEqual(
            caught.exception.detail["error"],
            "duplicate_robinhood_chain_native_registry_scope",
        )

    def test_list_response_derives_asset_kind_without_schema_column(self) -> None:
        self._create(
            symbol="GASX",
            address=None,
            decimals=9,
            asset_kind=ASSET_KIND_NATIVE,
        )
        self._create(
            symbol="ALPHA",
            address=self._contract("ef"),
            decimals=7,
            asset_kind=ASSET_KIND_ERC20,
        )
        payload = list_tokens(
            chain="robinhood_chain",
            venue=None,
            include_global=1,
            db=self.db,
        )
        by_symbol = {item["symbol"]: item for item in payload["items"]}
        self.assertEqual(by_symbol["GASX"]["asset_kind"], ASSET_KIND_NATIVE)
        self.assertTrue(by_symbol["GASX"]["native"])
        self.assertEqual(by_symbol["ALPHA"]["asset_kind"], ASSET_KIND_ERC20)
        self.assertFalse(by_symbol["ALPHA"]["native"])

    def test_same_symbol_different_contracts_coexist_and_symbol_lookup_fails_closed(self) -> None:
        first = self._create(
            symbol="DUP",
            address=self._contract("11"),
            decimals=18,
            asset_kind=ASSET_KIND_ERC20,
        )
        second = self._create(
            symbol="DUP",
            address=self._contract("22"),
            decimals=6,
            asset_kind=ASSET_KIND_ERC20,
        )

        self.assertNotEqual(int(first["id"]), int(second["id"]))
        rows = [
            row for row in select_effective_registry_rows(self.db)
            if str(row.symbol or "").strip().upper() == "DUP"
        ]
        self.assertEqual(len(rows), 2)
        self.assertEqual(
            {str(row.address or "").lower() for row in rows},
            {self._contract("11").lower(), self._contract("22").lower()},
        )

        with self.assertRaises(RobinhoodChainRegistryAuthorityError) as caught:
            effective_row_by_symbol(self.db, "DUP")
        self.assertEqual(
            caught.exception.code,
            "ambiguous_robinhood_chain_registry_symbol",
        )
        self.assertEqual(
            sorted(caught.exception.context.get("registry_ids") or []),
            sorted([int(first["id"]), int(second["id"])]),
        )

    def test_same_contract_different_symbol_upserts_exact_identity(self) -> None:
        contract = self._contract("33")
        first = self._create(
            symbol="OLD",
            address=contract,
            decimals=18,
            asset_kind=ASSET_KIND_ERC20,
        )
        second = self._create(
            symbol="NEW",
            address=contract.upper().replace("0X", "0x"),
            decimals=9,
            asset_kind=ASSET_KIND_ERC20,
        )
        self.assertEqual(int(first["id"]), int(second["id"]))
        self.assertEqual(second["symbol"], "NEW")
        self.assertEqual(second["address"], contract.lower())
        self.assertEqual(second["decimals"], 9)
        self.assertEqual(
            self.db.query(TokenRegistry).filter(TokenRegistry.chain == "robinhood_chain").count(),
            1,
        )

    def test_update_to_existing_contract_is_rejected(self) -> None:
        first = self._create(
            symbol="AAA",
            address=self._contract("44"),
            decimals=18,
            asset_kind=ASSET_KIND_ERC20,
        )
        second = self._create(
            symbol="BBB",
            address=self._contract("55"),
            decimals=6,
            asset_kind=ASSET_KIND_ERC20,
        )
        request = TokenRegistryUpdate(
            address=first["address"],
            asset_kind=ASSET_KIND_ERC20,
        )
        with self.assertRaises(HTTPException) as caught:
            update_token(int(second["id"]), request, self.db)
        self.assertEqual(
            caught.exception.detail["error"],
            "duplicate_robinhood_chain_contract_registry_identity",
        )

    def test_runtime_schema_guard_fails_closed_without_migrating_legacy_sqlite(self) -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        with engine.connect() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys=ON")
            connection.exec_driver_sql(
                """
                CREATE TABLE token_registry (
                    id INTEGER NOT NULL PRIMARY KEY,
                    chain VARCHAR(16) NOT NULL,
                    venue VARCHAR(32),
                    symbol VARCHAR(32) NOT NULL,
                    address VARCHAR(128),
                    decimals INTEGER NOT NULL,
                    label VARCHAR(64),
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL,
                    external_price_source TEXT,
                    external_price_id TEXT,
                    CONSTRAINT uq_token_registry_chain_venue_symbol
                        UNIQUE (chain, venue, symbol)
                )
                """
            )
            connection.exec_driver_sql(
                """
                INSERT INTO token_registry
                    (id, chain, venue, symbol, address, decimals, label, created_at, updated_at)
                VALUES
                    (7, 'robinhood_chain', NULL, 'AAA', :a, 18, 'AAA', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                {"a": self._contract("66")},
            )
            connection.commit()

        Session2 = sessionmaker(bind=engine, expire_on_commit=False)
        db2: Session = Session2()
        try:
            with self.assertRaises(RobinhoodChainRegistryAuthorityError) as caught:
                require_token_registry_contract_identity_schema(db2)
            self.assertEqual(
                caught.exception.code,
                "robinhood_chain_registry_schema_migration_required",
            )
            with self.assertRaises(RobinhoodChainRegistryAuthorityError) as caught_read:
                select_effective_registry_rows(db2)
            self.assertEqual(
                caught_read.exception.code,
                "robinhood_chain_registry_schema_migration_required",
            )

            indexes = db2.execute(text("PRAGMA index_list('token_registry')")).mappings().all()
            names = {str(row.get("name") or "") for row in indexes}
            self.assertIn("sqlite_autoindex_token_registry_1", names)
            self.assertNotIn("uq_token_registry_rh_chain_contract_norm", names)
            self.assertEqual(
                int(db2.execute(text("SELECT COUNT(*) FROM token_registry")).scalar_one()),
                1,
            )
        finally:
            db2.close()
            engine.dispose()

    def test_legacy_symbol_unique_schema_migrates_preserving_ids_and_foreign_keys(self) -> None:
        engine = create_engine("sqlite+pysqlite:///:memory:")
        with engine.connect() as connection:
            connection.exec_driver_sql("PRAGMA foreign_keys=ON")
            connection.exec_driver_sql(
                """
                CREATE TABLE token_registry (
                    id INTEGER NOT NULL PRIMARY KEY,
                    chain VARCHAR(16) NOT NULL,
                    venue VARCHAR(32),
                    symbol VARCHAR(32) NOT NULL,
                    address VARCHAR(128),
                    decimals INTEGER NOT NULL,
                    label VARCHAR(64),
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL,
                    external_price_source TEXT,
                    external_price_id TEXT,
                    CONSTRAINT uq_token_registry_chain_venue_symbol
                        UNIQUE (chain, venue, symbol)
                )
                """
            )
            connection.exec_driver_sql(
                """
                CREATE TABLE robinhood_chain_registry_verifications (
                    id VARCHAR(36) PRIMARY KEY,
                    token_registry_id INTEGER NOT NULL,
                    FOREIGN KEY(token_registry_id) REFERENCES token_registry(id) ON DELETE CASCADE
                )
                """
            )
            connection.exec_driver_sql(
                """
                INSERT INTO token_registry
                    (id, chain, venue, symbol, address, decimals, label, created_at, updated_at)
                VALUES
                    (7, 'robinhood_chain', NULL, 'AAA', :a, 18, 'AAA', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP),
                    (9, 'robinhood_chain', NULL, 'BBB', :b, 6, 'BBB', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                {"a": self._contract("66"), "b": self._contract("77")},
            )
            connection.exec_driver_sql(
                "INSERT INTO robinhood_chain_registry_verifications (id, token_registry_id) VALUES ('v1', 7)"
            )
            connection.commit()

        Session2 = sessionmaker(bind=engine, expire_on_commit=False)
        db2: Session = Session2()
        try:
            result = ensure_token_registry_contract_identity_schema(db2)
            self.assertTrue(result["migrated"])
            self.assertTrue(result["preserved_ids"])
            ids = [
                int(row[0])
                for row in db2.execute(text("SELECT id FROM token_registry ORDER BY id")).all()
            ]
            self.assertEqual(ids, [7, 9])
            self.assertEqual(
                int(db2.execute(text("SELECT token_registry_id FROM robinhood_chain_registry_verifications")).scalar_one()),
                7,
            )
            self.assertEqual(db2.execute(text("PRAGMA foreign_key_check")).all(), [])
            indexes = db2.execute(text("PRAGMA index_list('token_registry')")).mappings().all()
            names = {str(row.get("name") or "") for row in indexes}
            self.assertIn("uq_token_registry_rh_chain_contract_norm", names)
            self.assertNotIn("sqlite_autoindex_token_registry_1", names)

            db2.execute(
                text(
                    """
                    INSERT INTO token_registry
                        (id, chain, venue, symbol, address, decimals, label, created_at, updated_at)
                    VALUES
                        (10, 'robinhood_chain', NULL, 'AAA', :address, 8, 'AAA duplicate symbol', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """
                ),
                {"address": self._contract("88")},
            )
            db2.commit()
            self.assertEqual(
                int(db2.execute(text("SELECT COUNT(*) FROM token_registry WHERE symbol='AAA'")).scalar_one()),
                2,
            )
        finally:
            db2.close()
            engine.dispose()

    def test_runtime_paths_do_not_call_parent_table_migration_helper(self) -> None:
        test_path = Path(__file__).resolve()
        backend_root = test_path.parents[1]
        sources = [
            backend_root / "app" / "routers" / "token_registry.py",
            backend_root / "app" / "services" / "robinhood_chain_registry_discovery.py",
        ]
        for source_path in sources:
            source = source_path.read_text(encoding="utf-8")
            self.assertNotIn("ensure_token_registry_contract_identity_schema(db)", source)
            self.assertIn("require_token_registry_contract_identity_schema", source)

    def test_frontend_native_identity_ui_has_no_token_specific_preset(self) -> None:
        test_path = Path(__file__).resolve()
        candidates = [
            test_path.parents[2] / "frontend" / "src" / "features" / "registry" / "TokenRegistryWindow.jsx",
            test_path.parents[1] / "frontend" / "src" / "features" / "registry" / "TokenRegistryWindow.jsx",
        ]
        frontend_path = next((candidate for candidate in candidates if candidate.exists()), candidates[0])
        source = frontend_path.read_text(encoding="utf-8")
        self.assertNotIn("ROBINHOOD_CHAIN_NATIVE_SYMBOL", source)
        self.assertNotIn("ROBINHOOD_CHAIN_NATIVE_DECIMALS", source)
        self.assertNotIn("Load native ETH preset", source)
        self.assertIn("ROBINHOOD_CHAIN_ASSET_KINDS", source)
        self.assertIn("Only one effective Native identity is allowed per scope", source)


class RobinhoodChainExecutionAuthorityExactSelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite+pysqlite:///:memory:")
        for table in (
            TokenRegistry.__table__,
            RobinhoodChainPairObjective.__table__,
            RobinhoodChainPairCapability.__table__,
        ):
            table.create(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.db: Session = self.SessionLocal()

    def tearDown(self) -> None:
        self.db.close()
        self.engine.dispose()

    @staticmethod
    def _contract(byte_pair: str) -> str:
        return "0x" + str(byte_pair) * 20

    def _token(self, symbol: str, byte_pair: str, decimals: int = 18) -> TokenRegistry:
        row = TokenRegistry(
            chain="robinhood_chain",
            venue="robinhood_chain",
            symbol=symbol,
            address=self._contract(byte_pair),
            decimals=decimals,
            label=f"{symbol} {byte_pair}",
        )
        self.db.add(row)
        self.db.commit()
        self.db.refresh(row)
        return row

    def _objective(self, base: TokenRegistry, quote: TokenRegistry, objective_id: str) -> RobinhoodChainPairObjective:
        row = RobinhoodChainPairObjective(
            id=objective_id,
            base_token_registry_id=int(base.id),
            quote_token_registry_id=int(quote.id),
            symbol=f"{base.symbol}-{quote.symbol}",
            mechanism="swap",
            enabled=True,
            review_only=True,
        )
        self.db.add(row)
        self.db.commit()
        return row

    def test_symbol_only_execution_authority_fails_closed_when_objective_symbol_is_ambiguous(self) -> None:
        base_a = self._token("GME", "31")
        base_b = self._token("GME", "32")
        quote = self._token("USDG", "33", 6)
        self._objective(base_a, quote, "objective-a")
        self._objective(base_b, quote, "objective-b")

        with self.assertRaises(RobinhoodChainRegistryAuthorityError) as ctx:
            resolve_robinhood_chain_execution_authority(
                self.db,
                symbol="GME-USDG",
                side="sell",
            )

        self.assertEqual(ctx.exception.code, "robinhood_chain_execution_objective_ambiguous")
        self.assertEqual(set(ctx.exception.context.get("objective_ids") or []), {"objective-a", "objective-b"})

    def test_exact_objective_id_selects_one_same_symbol_execution_identity(self) -> None:
        base_a = self._token("GME", "41")
        base_b = self._token("GME", "42")
        quote = self._token("USDG", "43", 6)
        self._objective(base_a, quote, "objective-a")
        self._objective(base_b, quote, "objective-b")

        authority = resolve_robinhood_chain_execution_authority(
            self.db,
            symbol="GME-USDG",
            side="sell",
            objective_id="objective-b",
        )

        self.assertEqual(authority["objective"]["id"], "objective-b")
        self.assertEqual(authority["objective"]["base_token_registry_id"], int(base_b.id))
        self.assertEqual(authority["input"]["registry_id"], int(base_b.id))
        self.assertEqual(authority["output"]["registry_id"], int(quote.id))

    def test_persisted_exact_contracts_rebind_same_symbol_objective_without_first_match(self) -> None:
        base_a = self._token("GME", "51")
        base_b = self._token("GME", "52")
        quote = self._token("USDG", "53", 6)
        self._objective(base_a, quote, "objective-a")
        self._objective(base_b, quote, "objective-b")

        authority = resolve_robinhood_chain_execution_authority(
            self.db,
            symbol="GME-USDG",
            side="sell",
            expected_input_contract_address=str(base_a.address),
            expected_output_contract_address=str(quote.address),
        )

        self.assertEqual(authority["objective"]["id"], "objective-a")
        self.assertEqual(authority["input"]["registry_id"], int(base_a.id))
        self.assertEqual(authority["output"]["registry_id"], int(quote.id))

    def test_persisted_exact_contracts_reject_wrong_single_same_symbol_objective(self) -> None:
        persisted_base = self._token("GME", "61")
        current_base = self._token("GME", "62")
        quote = self._token("USDG", "63", 6)
        self._objective(current_base, quote, "objective-current")

        with self.assertRaises(RobinhoodChainRegistryAuthorityError) as ctx:
            resolve_robinhood_chain_execution_authority(
                self.db,
                symbol="GME-USDG",
                side="sell",
                expected_input_contract_address=str(persisted_base.address),
                expected_output_contract_address=str(quote.address),
            )

        self.assertEqual(ctx.exception.code, "robinhood_chain_execution_objective_identity_mismatch")


if __name__ == "__main__":
    unittest.main()
