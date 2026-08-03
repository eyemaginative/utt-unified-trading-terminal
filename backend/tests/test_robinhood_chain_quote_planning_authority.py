from __future__ import annotations

import ast
import inspect
import unittest
from pathlib import Path
import sys


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services import robinhood_chain_quotes as quotes_module  # noqa: E402
from app.services import robinhood_chain_transaction_planning as planning_module  # noqa: E402
from app.services.robinhood_chain_transaction_planning import (  # noqa: E402
    RobinhoodChainTransactionPlanningService,
)


def _synthetic_address(seed: int) -> str:
    return "0x" + int(seed).to_bytes(20, "big").hex()


def _identity(symbol: str, *, decimals: int, native: bool, seed: int) -> dict:
    registry_address = None if native else _synthetic_address(seed)
    return {
        "symbol": symbol,
        "contract_address": (
            "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
            if native
            else registry_address
        ),
        "registry_contract_address": registry_address,
        "decimals": decimals,
        "native": native,
        "asset_kind": "native" if native else "erc20",
        "identity_source": "token_registry",
        "registry_status": "registered",
        "registry_id": seed,
    }


NATIVE = _identity("FUEL", decimals=9, native=True, seed=101)
BASE = _identity("ALPHA", decimals=7, native=False, seed=202)
QUOTE = _identity("CRED", decimals=5, native=False, seed=303)


def _capability(
    from_asset: str,
    to_asset: str,
    *,
    amount_mode: str = "exact_input",
    probe_amount: str = "2",
    maximum_input_amount: str | None = None,
    firm_plan_status: str = "available",
    firm_plan_max_input_amount: str | None = None,
) -> dict:
    row = {
        "symbol": f"{BASE['symbol']}-{QUOTE['symbol']}",
        "mechanism": "swap",
        "from_asset": from_asset,
        "to_asset": to_asset,
        "amount_mode": amount_mode,
        "display_mode": "exact_receive" if amount_mode == "exact_output" else "exact_spend",
        "indicative_status": "available",
        "firm_plan_status": firm_plan_status,
        "execution_status": "disabled",
        "enabled": False,
        "probe_amount": probe_amount,
    }
    if maximum_input_amount is not None:
        row["maximum_input_amount"] = maximum_input_amount
    if firm_plan_max_input_amount is not None:
        row["firm_plan_max_input_amount"] = firm_plan_max_input_amount
    elif amount_mode == "exact_input":
        row["firm_plan_max_input_amount"] = probe_amount
    return row


class RobinhoodChainQuotePlanningAuthorityTests(unittest.TestCase):
    def make_planning_service(self) -> RobinhoodChainTransactionPlanningService:
        return RobinhoodChainTransactionPlanningService(
            api_base="https://example.invalid",
            timeout_s=2,
            max_concurrent=1,
            credential_getter=lambda: {"api_key": "synthetic-test-key"},
            rpc_client=object(),
        )

    def test_quote_service_contains_no_fixed_token_identity_literals(self) -> None:
        source = inspect.getsource(quotes_module)
        for literal in ('"ETH-USDG"', '"WETH-USDG"', '"ETH"', '"WETH"', '"USDG"'):
            self.assertNotIn(literal, source)
        for name in (
            "ROBINHOOD_CHAIN_QUOTE_SYMBOL",
            "ROBINHOOD_CHAIN_REVIEW_QUOTE_SYMBOLS",
            "ROBINHOOD_CHAIN_BID_INPUT_AMOUNTS",
            "ROBINHOOD_CHAIN_ASK_INPUT_AMOUNTS",
        ):
            self.assertNotIn(name, source)

    def test_planning_service_contains_no_fixed_token_identity_literals(self) -> None:
        source = inspect.getsource(planning_module)
        for literal in ('"ETH-USDG"', '"WETH-USDG"', '"ETH"', '"WETH"', '"USDG"'):
            self.assertNotIn(literal, source)
        for name in (
            "ROBINHOOD_CHAIN_FIRM_QUOTE_SYMBOL",
            "ROBINHOOD_CHAIN_REVIEW_FIRM_QUOTE_SYMBOLS",
            "ROBINHOOD_CHAIN_MAX_ETH_INPUT",
            "ROBINHOOD_CHAIN_MAX_USDG_INPUT",
        ):
            self.assertNotIn(name, source)

    def test_router_review_models_require_explicit_market_and_amount_fields(self) -> None:
        router_path = BACKEND_ROOT / "app" / "routers" / "robinhood_chain.py"
        tree = ast.parse(router_path.read_text(encoding="utf-8"), filename=str(router_path))
        classes = {
            node.name: node
            for node in tree.body
            if isinstance(node, ast.ClassDef)
        }
        for class_name in (
            "RobinhoodChainIndicativeQuoteRequest",
            "RobinhoodChainFirmQuotePlanRequest",
        ):
            model = classes[class_name]
            fields = {
                node.target.id: node
                for node in model.body
                if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
            }
            for field in ("symbol", "side", "amount_mode", "requested_amount"):
                self.assertIn(field, fields)
                call = fields[field].value
                self.assertIsInstance(call, ast.Call)
                default_keywords = {
                    keyword.arg
                    for keyword in call.keywords
                    if keyword.arg in {"default", "default_factory"}
                }
                self.assertEqual(default_keywords, set())

    def test_frontend_exact_spend_summary_uses_direction_input_asset_and_amount(self) -> None:
        frontend_path = BACKEND_ROOT.parent / "frontend" / "src" / "OrderTicketWidget.jsx"
        source = frontend_path.read_text(encoding="utf-8")

        self.assertIn("const robinhoodChainReviewSpendLabel = useMemo", source)
        self.assertIn("return inputAsset || totalLabel;", source)
        self.assertIn("const robinhoodChainReviewSpendAmount = useMemo", source)
        self.assertIn('side === "buy"', source)
        self.assertIn(") ? totalQuote : qty;", source)
        self.assertIn(
            "({isRobinhoodChainVenue ? robinhoodChainReviewSpendLabel : totalLabel})",
            source,
        )
        self.assertIn(": robinhoodChainReviewSpendAmount", source)
        self.assertNotIn("} ({totalLabel}): <b>{", source)

    def test_frontend_firm_plan_ui_requires_persisted_capability_status(self) -> None:
        frontend_path = BACKEND_ROOT.parent / "frontend" / "src" / "OrderTicketWidget.jsx"
        source = frontend_path.read_text(encoding="utf-8")

        self.assertIn("const robinhoodChainFirmPlanStatus = String(", source)
        self.assertIn(
            'robinhoodChainSelectedCapability?.firm_plan_status || "not_tested"',
            source,
        )
        self.assertIn(
            "robinhoodChainQuoteReviewEnabled && robinhoodChainFirmPlanAvailable",
            source,
        )
        self.assertIn("robinhoodChainFirmPlanReviewEnabled", source)
        self.assertIn(
            "Unsigned firm-plan review remains disabled while firm_plan_status is",
            source,
        )

    def test_frontend_does_not_treat_probe_as_exact_input_quote_ceiling(self) -> None:
        frontend_path = BACKEND_ROOT.parent / "frontend" / "src" / "OrderTicketWidget.jsx"
        source = frontend_path.read_text(encoding="utf-8")

        self.assertIn(
            "The persisted probe amount is historical capability evidence and the "
            "synthetic Order Book seed. The current manually entered exact-input "
            "amount is used for each fresh unsigned plan.",
            source,
        )
        self.assertIn("robinhoodChainConfiguredReviewValueCeilingUsd", source)
        self.assertIn("robinhoodChainExplicitIndicativeInputCeiling", source)
        self.assertNotIn("robinhood_chain_quote_capability_probe", source)

    def test_exact_input_planning_uses_firm_plan_ceiling_not_probe(self) -> None:
        service = self.make_planning_service()
        trade = service._resolve_trade(
            symbol="ALPHA-CRED",
            side="sell",
            amount_mode="exact_input",
            requested_amount="0.5",
            maximum_input_amount=None,
            base_token=BASE,
            quote_token=QUOTE,
            route_capability=_capability(
                "ALPHA",
                "CRED",
                probe_amount="0.1",
                firm_plan_max_input_amount="1",
            ),
        )
        self.assertEqual(trade["probe_amount"], "0.1")
        self.assertEqual(trade["firm_plan_input_ceiling"], "1")
        self.assertEqual(trade["firm_plan_ceiling_source"], "database_direction_capability")

    def test_live_accepted_execution_evidence_supplies_legacy_firm_plan_ceiling(self) -> None:
        service = self.make_planning_service()
        capability = _capability(
            "CRED",
            "FUEL",
            probe_amount="2",
            firm_plan_status="live_verified",
        )
        capability.pop("firm_plan_max_input_amount", None)
        capability["symbol"] = "FUEL-CRED"
        capability["evidence"] = {"live_accepted": True, "source_table": "synthetic"}

        trade = service._resolve_trade(
            symbol="FUEL-CRED",
            side="buy",
            amount_mode="exact_input",
            requested_amount="1.5",
            maximum_input_amount=None,
            base_token=NATIVE,
            quote_token=QUOTE,
            route_capability=capability,
        )

        self.assertEqual(trade["firm_plan_input_ceiling"], "2")
        self.assertEqual(trade["firm_plan_ceiling_source"], "verified_execution_evidence")
        self.assertEqual(trade["probe_amount_role"], "evidence_and_orderbook_seed")

    def test_native_to_erc20_exact_input_uses_registry_decimals(self) -> None:
        service = self.make_planning_service()
        trade = service._resolve_trade(
            symbol="FUEL-CRED",
            side="sell",
            amount_mode="exact_input",
            requested_amount="0.0001",
            maximum_input_amount=None,
            base_token=NATIVE,
            quote_token=QUOTE,
            route_capability={
                **_capability("FUEL", "CRED", probe_amount="0.002"),
                "symbol": "FUEL-CRED",
            },
        )
        self.assertTrue(trade["sell_token"]["native"])
        self.assertEqual(trade["sell_amount_atomic"], "100000")
        self.assertIsNone(trade["review_input_ceiling"])
        self.assertEqual(trade["probe_amount"], "0.002")
        self.assertEqual(trade["probe_amount_role"], "evidence_and_orderbook_seed")
        self.assertEqual(trade["current_amount_policy"], "user_selected_exact_input")
        self.assertFalse(trade["firm_plan_ceiling_enforced"])

    def test_erc20_to_native_exact_input_uses_direction_authority(self) -> None:
        service = self.make_planning_service()
        trade = service._resolve_trade(
            symbol="FUEL-CRED",
            side="buy",
            amount_mode="exact_input",
            requested_amount="1.25",
            maximum_input_amount=None,
            base_token=NATIVE,
            quote_token=QUOTE,
            route_capability={
                **_capability("CRED", "FUEL", probe_amount="2"),
                "symbol": "FUEL-CRED",
            },
        )
        self.assertEqual(trade["sell_token"]["symbol"], "CRED")
        self.assertEqual(trade["buy_token"]["symbol"], "FUEL")
        self.assertEqual(trade["sell_amount_atomic"], "125000")

    def test_erc20_to_erc20_exact_input_preserves_arbitrary_decimals(self) -> None:
        service = self.make_planning_service()
        trade = service._resolve_trade(
            symbol="ALPHA-CRED",
            side="sell",
            amount_mode="exact_input",
            requested_amount="0.1234567",
            maximum_input_amount=None,
            base_token=BASE,
            quote_token=QUOTE,
            route_capability=_capability("ALPHA", "CRED", probe_amount="1"),
        )
        self.assertEqual(trade["sell_amount_atomic"], "1234567")
        self.assertEqual(trade["sell_token"]["decimals"], 7)
        self.assertEqual(trade["buy_token"]["decimals"], 5)

    def test_missing_direction_capability_fails_closed(self) -> None:
        service = self.make_planning_service()
        with self.assertRaisesRegex(ValueError, "firm_quote_route_capability_unavailable"):
            service._resolve_trade(
                symbol="ALPHA-CRED",
                side="sell",
                amount_mode="exact_input",
                requested_amount="0.1",
                maximum_input_amount=None,
                base_token=BASE,
                quote_token=QUOTE,
                route_capability=_capability("CRED", "ALPHA"),
            )

    def test_wrong_amount_mode_fails_closed(self) -> None:
        service = self.make_planning_service()
        with self.assertRaisesRegex(ValueError, "invalid_quote_amount_mode"):
            service._resolve_trade(
                symbol="ALPHA-CRED",
                side="sell",
                amount_mode="floating_input",
                requested_amount="0.1",
                maximum_input_amount=None,
                base_token=BASE,
                quote_token=QUOTE,
                route_capability=_capability("ALPHA", "CRED"),
            )

    def test_exact_output_requires_persisted_input_ceiling(self) -> None:
        service = self.make_planning_service()
        with self.assertRaisesRegex(ValueError, "firm_quote_exact_output_ceiling_unavailable"):
            service._resolve_trade(
                symbol="ALPHA-CRED",
                side="buy",
                amount_mode="exact_output",
                requested_amount="0.5",
                maximum_input_amount="1",
                base_token=BASE,
                quote_token=QUOTE,
                route_capability=_capability(
                    "CRED",
                    "ALPHA",
                    amount_mode="exact_output",
                    probe_amount="1",
                ),
            )

    def test_exact_output_accepts_only_verified_persisted_ceiling(self) -> None:
        service = self.make_planning_service()
        trade = service._resolve_trade(
            symbol="ALPHA-CRED",
            side="buy",
            amount_mode="exact_output",
            requested_amount="0.5",
            maximum_input_amount="1.25",
            base_token=BASE,
            quote_token=QUOTE,
            route_capability=_capability(
                "CRED",
                "ALPHA",
                amount_mode="exact_output",
                probe_amount="1",
                maximum_input_amount="1.5",
            ),
        )
        self.assertEqual(trade["buy_amount_atomic"], "5000000")
        self.assertEqual(trade["maximum_sell_amount_atomic"], "125000")
        self.assertEqual(trade["review_input_ceiling"], "1.5")

    def test_identity_substitution_fails_closed(self) -> None:
        service = self.make_planning_service()
        with self.assertRaisesRegex(ValueError, "firm_quote_token_identity_mismatch"):
            service._resolve_trade(
                symbol="ALPHA-CRED",
                side="sell",
                amount_mode="exact_input",
                requested_amount="0.1",
                maximum_input_amount=None,
                base_token=NATIVE,
                quote_token=QUOTE,
                route_capability=_capability("ALPHA", "CRED"),
            )


if __name__ == "__main__":
    unittest.main()
