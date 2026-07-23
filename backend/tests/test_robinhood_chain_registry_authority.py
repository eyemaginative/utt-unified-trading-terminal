from __future__ import annotations

import unittest
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.models import TokenRegistry
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
    select_effective_registry_rows,
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


if __name__ == "__main__":
    unittest.main()
