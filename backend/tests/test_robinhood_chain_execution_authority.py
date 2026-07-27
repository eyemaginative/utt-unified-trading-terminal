from __future__ import annotations

import unittest
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.models import (
    RobinhoodChainPairCapability,
    RobinhoodChainPairObjective,
    TokenRegistry,
)
from app.services.robinhood_chain_registry_authority import (
    RobinhoodChainRegistryAuthorityError,
    assert_robinhood_chain_execution_amount,
    resolve_robinhood_chain_execution_authority,
)
from app.services.robinhood_chain_transaction_planning import (
    RobinhoodChainTransactionPlanningService,
)


class RobinhoodChainExecutionAuthorityTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite+pysqlite:///:memory:")
        TokenRegistry.__table__.create(self.engine)
        RobinhoodChainPairObjective.__table__.create(self.engine)
        RobinhoodChainPairCapability.__table__.create(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine, expire_on_commit=False)
        self.db: Session = self.SessionLocal()

    def tearDown(self) -> None:
        self.db.close()
        self.engine.dispose()

    @staticmethod
    def _contract(byte_pair: str) -> str:
        return "0x" + str(byte_pair) * 20

    def _token(
        self,
        *,
        token_id: int,
        symbol: str,
        decimals: int,
        address: str | None,
    ) -> TokenRegistry:
        row = TokenRegistry(
            id=token_id,
            chain="robinhood_chain",
            venue=None,
            symbol=symbol,
            address=address,
            decimals=decimals,
            label=f"{symbol} test identity",
        )
        self.db.add(row)
        self.db.flush()
        return row

    def _execution_pair(
        self,
        *,
        base_symbol: str = "GASX",
        base_address: str | None = None,
        base_decimals: int = 9,
        quote_symbol: str = "ALPHA",
        quote_address: str | None = None,
        quote_decimals: int = 7,
        side: str = "sell",
        indicative_status: str = "live_verified",
        firm_plan_status: str = "live_verified",
        execution_status: str = "live_verified",
        capability_enabled: bool = True,
        objective_enabled: bool = True,
        mechanism: str = "swap",
        probe_amount: str | None = "1.25",
        objective_symbol: str | None = None,
        evidence: dict | None = None,
    ) -> tuple[RobinhoodChainPairObjective, RobinhoodChainPairCapability]:
        if quote_address is None:
            quote_address = self._contract("ef")
        base = self._token(
            token_id=1,
            symbol=base_symbol,
            decimals=base_decimals,
            address=base_address,
        )
        quote = self._token(
            token_id=2,
            symbol=quote_symbol,
            decimals=quote_decimals,
            address=quote_address,
        )
        objective = RobinhoodChainPairObjective(
            id="objective-1",
            base_token_registry_id=int(base.id),
            quote_token_registry_id=int(quote.id),
            symbol=objective_symbol or f"{base_symbol}-{quote_symbol}",
            mechanism=mechanism,
            enabled=objective_enabled,
            review_only=True,
        )
        self.db.add(objective)
        self.db.flush()
        from_id = int(base.id) if side == "sell" else int(quote.id)
        to_id = int(quote.id) if side == "sell" else int(base.id)
        capability = RobinhoodChainPairCapability(
            id="capability-1",
            objective_id=objective.id,
            from_token_registry_id=from_id,
            to_token_registry_id=to_id,
            amount_mode="exact_input",
            provider="0x",
            indicative_status=indicative_status,
            firm_plan_status=firm_plan_status,
            execution_status=execution_status,
            enabled=capability_enabled,
            probe_amount=probe_amount,
            evidence=(
                dict(evidence)
                if evidence is not None
                else {"live_accepted": execution_status == "live_verified"}
            ),
        )
        self.db.add(capability)
        self.db.commit()
        return objective, capability

    def test_arbitrary_native_execution_authority_requires_no_approval(self) -> None:
        self._execution_pair()
        authority = resolve_robinhood_chain_execution_authority(
            self.db,
            symbol="GASX-ALPHA",
            side="sell",
            require_execution=True,
        )
        self.assertTrue(authority["execution_permitted"])
        self.assertEqual(authority["execution_adapter"], "native_exact_input")
        self.assertEqual(authority["input"]["symbol"], "GASX")
        self.assertEqual(authority["input"]["decimals"], 9)
        self.assertTrue(authority["input"]["native"])
        self.assertFalse(authority["approval"]["applicable"])
        self.assertEqual(authority["execution_ceiling"]["amount"], "1.25")

    def test_arbitrary_erc20_input_derives_finite_approval_and_decimals(self) -> None:
        self._execution_pair(side="buy")
        authority = resolve_robinhood_chain_execution_authority(
            self.db,
            symbol="GASX-ALPHA",
            side="buy",
            require_execution=True,
        )
        self.assertEqual(authority["execution_adapter"], "erc20_exact_input")
        self.assertEqual(authority["input"]["symbol"], "ALPHA")
        self.assertEqual(authority["input"]["decimals"], 7)
        self.assertFalse(authority["input"]["native"])
        self.assertTrue(authority["approval"]["applicable"])
        self.assertEqual(authority["approval"]["model"], "finite_exact_input")
        self.assertFalse(authority["approval"]["unlimited_approval_enabled"])

    def test_execution_authority_preserves_verified_firm_plan_ceiling_evidence(self) -> None:
        self._execution_pair(side="buy", probe_amount="1.25")
        authority = resolve_robinhood_chain_execution_authority(
            self.db,
            symbol="GASX-ALPHA",
            side="buy",
            require_execution=True,
        )
        capability = authority["capability"]
        self.assertTrue(capability["evidence"]["live_accepted"])
        ceiling, source = (
            RobinhoodChainTransactionPlanningService._firm_plan_exact_input_ceiling(
                capability
            )
        )
        self.assertEqual(ceiling, Decimal("1.25"))
        self.assertEqual(source, "verified_execution_evidence")

    def test_not_tested_firm_plan_blocks_execution(self) -> None:
        self._execution_pair(firm_plan_status="not_tested")
        authority = resolve_robinhood_chain_execution_authority(
            self.db,
            symbol="GASX-ALPHA",
            side="sell",
        )
        self.assertFalse(authority["execution_permitted"])
        self.assertIn("firm_plan_not_live_verified", authority["blocking_reasons"])
        with self.assertRaises(RobinhoodChainRegistryAuthorityError) as caught:
            resolve_robinhood_chain_execution_authority(
                self.db,
                symbol="GASX-ALPHA",
                side="sell",
                require_execution=True,
            )
        self.assertEqual(caught.exception.code, "robinhood_chain_execution_authority_blocked")

    def test_disabled_execution_status_never_auto_promotes(self) -> None:
        self._execution_pair(execution_status="disabled", capability_enabled=False)
        authority = resolve_robinhood_chain_execution_authority(
            self.db,
            symbol="GASX-ALPHA",
            side="sell",
        )
        self.assertFalse(authority["execution_permitted"])
        self.assertIn("capability_disabled", authority["blocking_reasons"])
        self.assertIn("execution_not_live_verified", authority["blocking_reasons"])
        self.assertFalse(authority["automatic_execution_promotion"])

    def test_provider_error_fails_closed_without_provider_contact(self) -> None:
        self._execution_pair(
            indicative_status="provider_error",
            firm_plan_status="not_tested",
            execution_status="disabled",
            capability_enabled=False,
        )
        authority = resolve_robinhood_chain_execution_authority(
            self.db,
            symbol="GASX-ALPHA",
            side="sell",
        )
        self.assertFalse(authority["execution_permitted"])
        self.assertFalse(authority["provider_contacted"])
        self.assertIn("indicative_not_live_verified", authority["blocking_reasons"])

    def test_non_swap_mechanism_is_recognized_but_execution_blocked(self) -> None:
        self._execution_pair(mechanism="wrap_unwrap")
        authority = resolve_robinhood_chain_execution_authority(
            self.db,
            symbol="GASX-ALPHA",
            side="sell",
        )
        self.assertFalse(authority["execution_permitted"])
        self.assertIn("mechanism_not_supported", authority["blocking_reasons"])

    def test_execution_amount_uses_persisted_live_verified_ceiling(self) -> None:
        self._execution_pair(probe_amount="1.25")
        authority = resolve_robinhood_chain_execution_authority(
            self.db,
            symbol="GASX-ALPHA",
            side="sell",
            require_execution=True,
        )
        self.assertEqual(assert_robinhood_chain_execution_amount(authority, "1.25"), "1.25")
        with self.assertRaises(RobinhoodChainRegistryAuthorityError) as caught:
            assert_robinhood_chain_execution_amount(authority, "1.2500001")
        self.assertEqual(caught.exception.code, "robinhood_chain_execution_amount_exceeds_ceiling")

    def test_objective_symbol_must_match_registry_identities(self) -> None:
        self._execution_pair(objective_symbol="WRONG-PAIR")
        with self.assertRaises(RobinhoodChainRegistryAuthorityError) as caught:
            resolve_robinhood_chain_execution_authority(
                self.db,
                symbol="WRONG-PAIR",
                side="sell",
            )
        self.assertEqual(caught.exception.code, "robinhood_chain_execution_objective_identity_mismatch")

    def test_preparation_verified_authority_is_bounded_and_not_live(self) -> None:
        self._execution_pair(
            base_symbol="WETH",
            base_address=self._contract("ab"),
            base_decimals=18,
            quote_symbol="USDG",
            quote_address=self._contract("cd"),
            quote_decimals=6,
            side="buy",
            indicative_status="available",
            firm_plan_status="available",
            execution_status="preparation_verified",
            capability_enabled=True,
            probe_amount="1",
            evidence={
                "preparation_verified": True,
                "live_accepted": False,
                "successful_broadcast": False,
                "symbol": "WETH-USDG",
                "side": "buy",
                "amount_mode": "exact_input",
                "provider": "0x",
                "from_asset": "USDG",
                "to_asset": "WETH",
                "verified_input_amount": "1",
                "firm_plan_input_ceiling": "1",
            },
        )
        authority = resolve_robinhood_chain_execution_authority(
            self.db,
            symbol="WETH-USDG",
            side="buy",
            require_execution=True,
        )
        self.assertTrue(authority["execution_permitted"])
        self.assertEqual(authority["authority_level"], "preparation_verified")
        self.assertTrue(authority["preparation_verified"])
        self.assertFalse(authority["live_execution_verified"])
        self.assertTrue(authority["initial_acceptance_wallet_reject_only"])
        self.assertFalse(authority["successful_broadcast_authorized"])
        self.assertEqual(authority["execution_ceiling"]["amount"], "1")
        self.assertEqual(authority["input"]["symbol"], "USDG")
        self.assertEqual(authority["output"]["symbol"], "WETH")
        self.assertFalse(authority["output"]["native"])
        self.assertEqual(assert_robinhood_chain_execution_amount(authority, "1"), "1")
        with self.assertRaises(RobinhoodChainRegistryAuthorityError) as caught:
            assert_robinhood_chain_execution_amount(authority, "1.000001")
        self.assertEqual(caught.exception.code, "robinhood_chain_execution_amount_exceeds_ceiling")

    def test_r5c4b_sell_preparation_authority_is_finite_and_not_live(self) -> None:
        self._execution_pair(
            base_symbol="WETH",
            base_address=self._contract("ab"),
            base_decimals=18,
            quote_symbol="USDG",
            quote_address=self._contract("cd"),
            quote_decimals=6,
            side="sell",
            indicative_status="available",
            firm_plan_status="available",
            execution_status="preparation_verified",
            capability_enabled=True,
            probe_amount="0.0001",
            evidence={
                "preparation_verified": True,
                "live_accepted": False,
                "successful_broadcast": False,
                "tranche": "R5C.4B",
                "symbol": "WETH-USDG",
                "side": "sell",
                "amount_mode": "exact_input",
                "provider": "0x",
                "from_asset": "WETH",
                "to_asset": "USDG",
                "verified_input_amount": "0.0001",
                "firm_plan_input_ceiling": "0.0001",
            },
        )
        authority = resolve_robinhood_chain_execution_authority(
            self.db,
            symbol="WETH-USDG",
            side="sell",
            require_execution=True,
        )
        self.assertTrue(authority["execution_permitted"])
        self.assertEqual(authority["execution_adapter"], "erc20_exact_input")
        self.assertEqual(authority["authority_level"], "preparation_verified")
        self.assertTrue(authority["preparation_verified"])
        self.assertFalse(authority["live_execution_verified"])
        self.assertTrue(authority["initial_acceptance_wallet_reject_only"])
        self.assertFalse(authority["successful_broadcast_authorized"])
        self.assertEqual(authority["execution_ceiling"]["amount"], "0.0001")
        self.assertEqual(authority["execution_ceiling"]["asset"], "WETH")
        self.assertEqual(authority["input"]["symbol"], "WETH")
        self.assertFalse(authority["input"]["native"])
        self.assertEqual(authority["input"]["decimals"], 18)
        self.assertEqual(authority["output"]["symbol"], "USDG")
        self.assertTrue(authority["approval"]["applicable"])
        self.assertEqual(authority["approval"]["model"], "finite_exact_input")
        self.assertFalse(authority["approval"]["unlimited_approval_enabled"])
        self.assertEqual(assert_robinhood_chain_execution_amount(authority, "0.0001"), "0.0001")
        with self.assertRaises(RobinhoodChainRegistryAuthorityError) as caught:
            assert_robinhood_chain_execution_amount(authority, "0.000100000000000001")
        self.assertEqual(caught.exception.code, "robinhood_chain_execution_amount_exceeds_ceiling")

    def test_preparation_status_without_exact_evidence_fails_closed(self) -> None:
        self._execution_pair(
            base_symbol="WETH",
            base_address=self._contract("ab"),
            base_decimals=18,
            quote_symbol="USDG",
            quote_address=self._contract("cd"),
            quote_decimals=6,
            side="buy",
            indicative_status="available",
            firm_plan_status="available",
            execution_status="preparation_verified",
            capability_enabled=True,
            probe_amount="1",
            evidence={
                "preparation_verified": True,
                "live_accepted": False,
                "successful_broadcast": False,
                "symbol": "WETH-USDG",
                "side": "buy",
                "amount_mode": "exact_input",
                "provider": "0x",
                "from_asset": "USDG",
                "to_asset": "ETH",
                "verified_input_amount": "1",
                "firm_plan_input_ceiling": "1",
            },
        )
        authority = resolve_robinhood_chain_execution_authority(
            self.db,
            symbol="WETH-USDG",
            side="buy",
        )
        self.assertFalse(authority["execution_permitted"])
        self.assertEqual(authority["authority_level"], "blocked")
        self.assertFalse(authority["preparation_verified"])
        with self.assertRaises(RobinhoodChainRegistryAuthorityError):
            resolve_robinhood_chain_execution_authority(
                self.db, symbol="WETH-USDG", side="buy", require_execution=True
            )

    def test_router_api_and_frontend_use_resolved_execution_authority(self) -> None:
        backend_root = Path(__file__).resolve().parents[1]
        project_root = Path(__file__).resolve().parents[2]
        router_source = (backend_root / "app" / "routers" / "robinhood_chain.py").read_text(encoding="utf-8")
        api_source = (project_root / "frontend" / "src" / "lib" / "api.js").read_text(encoding="utf-8")
        ticket_source = (project_root / "frontend" / "src" / "OrderTicketWidget.jsx").read_text(encoding="utf-8")

        self.assertIn('/execution-authority/resolve', router_source)
        self.assertIn('/execution-authority/verify-preparation', router_source)
        self.assertIn('_resolve_robinhood_chain_execution_authority_or_http', router_source)
        self.assertNotIn('robinhood_chain_execution_symbol_locked', router_source)
        self.assertNotIn('robinhood_chain_swap_from_asset_locked', router_source)
        self.assertIn('getRobinhoodChainExecutionAuthority', api_source)
        self.assertIn('getRobinhoodChainExecutionAuthority', ticket_source)
        self.assertIn('robinhoodChainExecutionAuthority?.execution_permitted', ticket_source)
        self.assertIn('robinhoodChainExecutionAdapter', ticket_source)
        self.assertIn('R5C.4A', ticket_source)
        self.assertIn('R5C.4B', ticket_source)
        self.assertIn('robinhoodChainR5C4BSell', ticket_source)
        self.assertIn('expectedApprovalToken', ticket_source)
        self.assertIn('Successful WETH to USDG broadcast is not authorized', ticket_source)
        self.assertIn('PREP VERIFIED', ticket_source)


if __name__ == "__main__":
    unittest.main(verbosity=2)
