from __future__ import annotations

import ast
import inspect
import json
import unittest
from decimal import Decimal
from pathlib import Path
import sys


BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from app.services import robinhood_chain_execution_discovery as discovery_module  # noqa: E402
from app.services.robinhood_chain_quotes import (  # noqa: E402
    ROBINHOOD_CHAIN_MAX_BOOK_LEVELS,
    RobinhoodChainQuoteService,
)



def _synthetic_address(seed: int) -> str:
    return "0x" + int(seed).to_bytes(20, "big").hex()


GASX = {
    "symbol": "GASX",
    "contract_address": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
    "registry_contract_address": None,
    "decimals": 9,
    "native": True,
    "asset_kind": "native",
    "identity_source": "token_registry",
    "registry_status": "registered",
    "registry_id": 101,
}
CRED = {
    "symbol": "CRED",
    "contract_address": _synthetic_address(202),
    "registry_contract_address": _synthetic_address(202),
    "decimals": 5,
    "native": False,
    "asset_kind": "erc20",
    "identity_source": "token_registry",
    "registry_status": "registered",
    "registry_id": 202,
}
WGAS = {
    "symbol": "WGAS",
    "contract_address": _synthetic_address(303),
    "registry_contract_address": _synthetic_address(303),
    "decimals": 7,
    "native": False,
    "asset_kind": "erc20",
    "identity_source": "token_registry",
    "registry_status": "registered",
    "registry_id": 303,
}
TAKER = _synthetic_address(404)
WGAS_TO_CRED = {
    "symbol": "WGAS-CRED",
    "mechanism": "swap",
    "from_asset": "WGAS",
    "to_asset": "CRED",
    "amount_mode": "exact_input",
    "display_mode": "exact_spend",
    "indicative_status": "available",
    "firm_plan_status": "not_tested",
    "execution_status": "disabled",
    "enabled": False,
    "probe_amount": "0.004",
}
CRED_TO_WGAS = {
    "symbol": "WGAS-CRED",
    "mechanism": "swap",
    "from_asset": "CRED",
    "to_asset": "WGAS",
    "amount_mode": "exact_input",
    "display_mode": "exact_spend",
    "indicative_status": "available",
    "firm_plan_status": "not_tested",
    "execution_status": "disabled",
    "enabled": False,
    "probe_amount": "2",
}
GASX_TO_CRED = {**WGAS_TO_CRED, "symbol": "GASX-CRED", "from_asset": "GASX", "probe_amount": "0.004"}
CRED_TO_GASX = {**CRED_TO_WGAS, "symbol": "GASX-CRED", "to_asset": "GASX"}


def decimal_text(value: Decimal) -> str:
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


class FakeDiscoveryService:
    def __init__(self, *, fail_inputs: set[str] | None = None) -> None:
        self.calls: list[dict] = []
        self.fail_inputs = set(fail_inputs or set())

    def status(self) -> dict:
        return {
            "provider_configured": True,
            "api_key_configured": True,
            "credential_source": "profile_vault",
            "cache_ttl_s": 15.0,
            "error_backoff_s": 120.0,
            "discovery_max_sell_usd": 5.0,
        }

    async def probe(
        self,
        *,
        sell_token: dict,
        buy_token: dict,
        sell_amount: str | None,
        buy_amount: str | None,
        taker_address: str,
        force_refresh: bool,
        route_capability: dict | None = None,
        require_live_verified: bool = True,
        max_probe_amount: str | None = None,
    ) -> dict:
        amount_mode = "exact_output" if buy_amount is not None else "exact_input"
        amount_text = str(buy_amount if buy_amount is not None else sell_amount or "")
        self.calls.append(
            {
                "sell": sell_token["symbol"],
                "buy": buy_token["symbol"],
                "amount": amount_text,
                "amount_mode": amount_mode,
                "taker": taker_address,
                "force_refresh": force_refresh,
                "max_probe_amount": max_probe_amount,
            }
        )
        if amount_text in self.fail_inputs:
            return {
                "ok": False,
                "provider": "0x",
                "error": "fake_level_failure",
                "http_status": 502,
            }

        amount = Decimal(amount_text)
        sell_symbol = sell_token["symbol"]
        buy_symbol = buy_token["symbol"]
        if sell_symbol in {"GASX", "WGAS"} and buy_symbol == "CRED":
            price = Decimal("1800") - amount * Decimal("100")
            sell_value = amount
            buy_value = amount * price
            fee_token = CRED["contract_address"].lower()
            fee_atomic = "275"
        elif amount_mode == "exact_output":
            price = Decimal("1850")
            sell_value = amount * price
            buy_value = amount
            fee_token = buy_token["contract_address"].lower()
            fee_atomic = "100000000000"
        elif sell_symbol == "CRED" and buy_symbol in {"GASX", "WGAS"}:
            price = Decimal("1820") + amount * Decimal("0.1")
            sell_value = amount
            buy_value = amount / price
            fee_token = buy_token["contract_address"].lower()
            fee_atomic = "100000000000"
        else:
            price = Decimal("2")
            sell_value = amount
            buy_value = amount / price
            fee_token = buy_token["contract_address"].lower()
            fee_atomic = "1"

        min_output = buy_value * Decimal("0.99")
        return {
            "ok": True,
            "provider": "0x",
            "sell_amount": decimal_text(sell_value),
            "buy_amount": decimal_text(buy_value),
            "min_buy_amount": decimal_text(min_output),
            "liquidity_available": True,
            "block_number": "12345678",
            "gas": "280000",
            "gas_price": "80000000",
            "total_network_fee": "22400000",
            "fees": {
                "zeroExFee": {
                    "amount": fee_atomic,
                    "token": fee_token,
                    "type": "volume",
                }
            },
            "allowance_required": True,
            "allowance_spender": "0x0000000000001ff3684f28c67538d4d072c22734",
            "provider_warnings": ["allowance_required"],
            "route": {
                "fill_count": 1,
                "fills": [
                    {
                        "source": "Uniswap_V3",
                        "proportion_bps": "10000",
                    }
                ],
            },
            "cached": False,
            "elapsed_ms": 12.3,
            "fetched_at": "2026-07-17T20:48:39+00:00",
            # Deliberately include executable-looking provider fields. The quote
            # service must never copy these through to a 10B response.
            "transaction_destination": _synthetic_address(505),
            "transaction_calldata": "0xdeadbeef",
        }


async def _quote_request(
    service: RobinhoodChainQuoteService,
    *,
    symbol: str,
    side: str,
    quantity: str | None = None,
    total_quote: str | None = None,
    taker_address: str,
    base_token: dict | None = None,
    quote_token: dict | None = None,
    route_capability: dict | None = None,
    force_refresh: bool = False,
    amount_mode: str | None = None,
) -> dict:
    base = dict(base_token or GASX)
    quote = dict(quote_token or CRED)
    normalized_side = str(side).lower()
    normalized_mode = amount_mode or (
        "exact_output" if normalized_side == "buy" and quantity is not None and total_quote is None else "exact_input"
    )
    requested_amount = quantity if normalized_mode == "exact_output" or normalized_side == "sell" else total_quote
    if route_capability is None:
        if normalized_mode == "exact_output":
            from_asset = quote["symbol"] if normalized_side == "buy" else base["symbol"]
            to_asset = base["symbol"] if normalized_side == "buy" else quote["symbol"]
            route_capability = {
                "symbol": f"{base['symbol']}-{quote['symbol']}",
                "mechanism": "swap",
                "from_asset": from_asset,
                "to_asset": to_asset,
                "amount_mode": "exact_output",
                "display_mode": "exact_receive",
                "indicative_status": "provider_error",
                "probe_amount": str(requested_amount or "1"),
            }
        else:
            route_capability = (
                {**GASX_TO_CRED, "symbol": f"{base['symbol']}-{quote['symbol']}", "from_asset": base["symbol"], "to_asset": quote["symbol"]}
                if normalized_side == "sell"
                else {**CRED_TO_GASX, "symbol": f"{base['symbol']}-{quote['symbol']}", "from_asset": quote["symbol"], "to_asset": base["symbol"]}
            )
    registry_tokens = [GASX, CRED, WGAS, base, quote]
    return await service.indicative_quote(
        symbol=symbol,
        side=side,
        amount_mode=normalized_mode,
        requested_amount=str(requested_amount or ""),
        taker_address=taker_address,
        base_token=base,
        quote_token=quote,
        native_token=GASX,
        registry_tokens=registry_tokens,
        route_capability=route_capability,
        force_refresh=force_refresh,
    )


async def _pair_book_request(
    service: RobinhoodChainQuoteService,
    *,
    symbol: str,
    depth: int,
    taker_address: str,
    base_token: dict,
    quote_token: dict,
    base_to_quote_capability: dict,
    quote_to_base_capability: dict,
    force_refresh: bool = False,
) -> dict:
    return await service.synthetic_orderbook_for_pair(
        symbol=symbol,
        depth=depth,
        taker_address=taker_address,
        base_token=base_token,
        quote_token=quote_token,
        base_to_quote_capability=base_to_quote_capability,
        quote_to_base_capability=quote_to_base_capability,
        native_token=GASX,
        registry_tokens=[GASX, CRED, WGAS, base_token, quote_token],
        force_refresh=force_refresh,
    )


async def _book_request(
    service: RobinhoodChainQuoteService,
    *,
    symbol: str,
    depth: int,
    taker_address: str,
    base_token: dict | None = None,
    quote_token: dict | None = None,
    force_refresh: bool = False,
) -> dict:
    base = dict(base_token or GASX)
    quote = dict(quote_token or CRED)
    return await _pair_book_request(
        service,
        symbol=symbol,
        depth=depth,
        taker_address=taker_address,
        base_token=base,
        quote_token=quote,
        base_to_quote_capability={**GASX_TO_CRED, "symbol": symbol, "from_asset": base["symbol"], "to_asset": quote["symbol"]},
        quote_to_base_capability={**CRED_TO_GASX, "symbol": symbol, "from_asset": quote["symbol"], "to_asset": base["symbol"]},
        force_refresh=force_refresh,
    )


class RobinhoodChainQuoteServiceTests(unittest.IsolatedAsyncioTestCase):
    def test_router_indicative_quote_keywords_match_service_signature(self) -> None:
        router_path = BACKEND_ROOT / "app" / "routers" / "robinhood_chain.py"
        tree = ast.parse(router_path.read_text(encoding="utf-8"), filename=str(router_path))
        endpoint = next(
            node
            for node in tree.body
            if isinstance(node, ast.AsyncFunctionDef)
            and node.name == "robinhood_chain_indicative_quote"
        )
        call = next(
            node
            for node in ast.walk(endpoint)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "indicative_quote"
        )
        endpoint_keywords = {keyword.arg for keyword in call.keywords if keyword.arg}
        service_keywords = set(
            inspect.signature(RobinhoodChainQuoteService.indicative_quote).parameters
        ) - {"self"}

        self.assertTrue(endpoint_keywords <= service_keywords)
        self.assertNotIn("exact_output_quantity", endpoint_keywords)
        self.assertNotIn("maximum_total_quote", endpoint_keywords)
        self.assertIn("amount_mode", endpoint_keywords)
        self.assertIn("requested_amount", endpoint_keywords)
        self.assertIn("base_token", endpoint_keywords)
        self.assertIn("quote_token", endpoint_keywords)
        self.assertIn("route_capability", endpoint_keywords)

    def make_service(self, *, fail_inputs: set[str] | None = None):
        discovery = FakeDiscoveryService(fail_inputs=fail_inputs)
        return RobinhoodChainQuoteService(discovery_service=discovery), discovery

    async def test_sell_quote_is_safe_exact_input(self) -> None:
        service, discovery = self.make_service()
        quote = await _quote_request(service,
            symbol="GASX-CRED",
            side="sell",
            quantity="0.0001",
            total_quote=None,
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            force_refresh=True,
        )

        self.assertTrue(quote["ok"])
        self.assertEqual(quote["input_asset"], "GASX")
        self.assertEqual(quote["output_asset"], "CRED")
        self.assertEqual(quote["amount_mode"], "exact_input")
        self.assertEqual(quote["route_source"], "Uniswap_V3")
        self.assertEqual(quote["zero_x_fee"]["asset"], "CRED")
        self.assertEqual(quote["total_network_fee"], "0.0224")
        self.assertEqual(quote["total_network_fee_asset"], "GASX")
        self.assertFalse(quote["allowance_required"])
        self.assertIsNone(quote["allowance_spender"])
        self.assertNotIn("allowance_required", quote["provider_warnings"])
        self.assertEqual(quote["transaction_calldata"], None)
        self.assertEqual(quote["transaction_destination"], None)
        self.assertFalse(quote["transaction_data_present"])
        self.assertTrue(quote["read_only"])
        self.assertTrue(quote["quote_only"])
        self.assertFalse(quote["execution_enabled"])
        self.assertFalse(quote["signing_enabled"])
        self.assertFalse(quote["transaction_construction_enabled"])
        self.assertFalse(quote["will_mutate"])
        self.assertGreaterEqual(len(discovery.calls), 1)

    async def test_buy_quote_maps_quote_asset_to_native_asset(self) -> None:
        service, _ = self.make_service()
        quote = await _quote_request(service,
            symbol="gasx/cred",
            side="buy",
            quantity=None,
            total_quote="1.25",
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            force_refresh=False,
        )

        self.assertTrue(quote["ok"])
        self.assertEqual(quote["input_amount"], "1.25")
        self.assertEqual(quote["output_asset"], "GASX")
        self.assertEqual(quote["minimum_received_asset"], "GASX")
        self.assertEqual(quote["zero_x_fee"]["asset"], "GASX")
        self.assertTrue(quote["allowance_required"])
        self.assertIsNotNone(quote["allowance_spender"])
        self.assertIsNotNone(quote["price_impact_bps"])

    async def test_exact_input_above_probe_uses_configured_review_ceiling(self) -> None:
        service, discovery = self.make_service()
        quote = await _quote_request(
            service,
            symbol="GASX-CRED",
            side="sell",
            quantity="0.0041",
            total_quote=None,
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            route_capability=GASX_TO_CRED,
            force_refresh=True,
        )

        self.assertTrue(quote["ok"])
        self.assertEqual(quote["probe_amount"], "0.004")
        self.assertEqual(quote["probe_amount_role"], "evidence_and_orderbook_seed")
        self.assertEqual(discovery.calls[-1]["amount"], "0.004")
        self.assertIsNone(discovery.calls[0]["max_probe_amount"])

    async def test_explicit_indicative_input_ceiling_fails_before_provider(self) -> None:
        service, discovery = self.make_service()
        quote = await _quote_request(
            service,
            symbol="GASX-CRED",
            side="sell",
            quantity="0.0041",
            total_quote=None,
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            route_capability={**GASX_TO_CRED, "indicative_max_input_amount": "0.004"},
            force_refresh=True,
        )

        self.assertFalse(quote["ok"])
        self.assertEqual(quote["error"], "robinhood_chain_quote_amount_exceeds_indicative_ceiling")
        self.assertEqual(quote["maximum_review_amount"], "0.004")
        self.assertFalse(quote["provider_contacted"])
        self.assertEqual(discovery.calls, [])

    async def test_exact_output_buy_is_blocked_before_provider_contact(self) -> None:
        service, discovery = self.make_service()
        quote = await _quote_request(service,
            symbol="GASX-CRED",
            side="buy",
            quantity="0.001",
            total_quote=None,
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            force_refresh=True,
        )

        self.assertFalse(quote["ok"])
        self.assertEqual(quote["error"], "robinhood_chain_quote_route_unavailable")
        self.assertFalse(quote["provider_contacted"])
        self.assertEqual(discovery.calls, [])

    async def test_database_capability_erc20_pair_synthetic_orderbook(self) -> None:
        service, discovery = self.make_service()
        base_to_quote = {
            "symbol": "WGAS-CRED",
            "mechanism": "swap",
            "from_asset": "WGAS",
            "to_asset": "CRED",
            "amount_mode": "exact_input",
            "indicative_status": "available",
            "execution_status": "disabled",
            "enabled": False,
            "probe_amount": "0.0005",
        }
        quote_to_base = {
            "symbol": "WGAS-CRED",
            "mechanism": "swap",
            "from_asset": "CRED",
            "to_asset": "WGAS",
            "amount_mode": "exact_input",
            "indicative_status": "available",
            "execution_status": "disabled",
            "enabled": False,
            "probe_amount": "1",
        }

        book = await _pair_book_request(service,
            symbol="WGAS-CRED",
            depth=3,
            taker_address=TAKER,
            base_token=WGAS,
            quote_token={**CRED, "registry_id": 45},
            base_to_quote_capability=base_to_quote,
            quote_to_base_capability=quote_to_base,
            force_refresh=True,
        )

        self.assertTrue(book["ok"])
        self.assertEqual(book["tranche"], "RH-CHAIN.10D.2-R5C.2")
        self.assertEqual(book["symbol"], "WGAS-CRED")
        self.assertEqual(book["base_asset"], "WGAS")
        self.assertEqual(book["quote_asset"], "CRED")
        self.assertEqual(book["identity_source"], "token_registry")
        self.assertEqual(book["capability_source"], "database")
        self.assertEqual(len(book["bids"]), 3)
        self.assertEqual(len(book["asks"]), 3)
        self.assertTrue(all(row["synthetic"] for row in [*book["bids"], *book["asks"]]))
        self.assertTrue(all(row["quote_only"] for row in [*book["bids"], *book["asks"]]))
        self.assertIsNone(book["transaction_calldata"])
        self.assertFalse(book["execution_enabled"])
        self.assertEqual(len(discovery.calls), 6)

    async def test_generic_orderbook_blocks_provider_error_before_probe(self) -> None:
        service, discovery = self.make_service()
        blocked = {
            "symbol": "ALPHA-CRED",
            "mechanism": "swap",
            "from_asset": "ALPHA",
            "to_asset": "CRED",
            "amount_mode": "exact_input",
            "indicative_status": "provider_error",
            "execution_status": "disabled",
            "enabled": False,
            "probe_amount": "0.01",
        }
        available = {
            "symbol": "ALPHA-CRED",
            "mechanism": "swap",
            "from_asset": "CRED",
            "to_asset": "ALPHA",
            "amount_mode": "exact_input",
            "indicative_status": "available",
            "execution_status": "disabled",
            "enabled": False,
            "probe_amount": "1",
        }
        spcx = {
            "symbol": "ALPHA",
            "contract_address": "0x" + "4a" * 20,
            "decimals": 18,
            "native": False,
            "asset_kind": "erc20",
            "identity_source": "token_registry",
            "registry_status": "registered",
            "registry_id": 404,
        }

        book = await _pair_book_request(service,
            symbol="ALPHA-CRED",
            depth=3,
            taker_address=TAKER,
            base_token=spcx,
            quote_token={**CRED, "registry_id": 45},
            base_to_quote_capability=blocked,
            quote_to_base_capability=available,
            force_refresh=True,
        )

        self.assertFalse(book["ok"])
        self.assertEqual(book["error"], "robinhood_chain_bid_direction_unavailable")
        self.assertFalse(book["provider_contacted"])
        self.assertEqual(discovery.calls, [])

    def test_route_capabilities_are_not_embedded_in_provider_service(self) -> None:
        source = inspect.getsource(discovery_module)

        self.assertNotIn("ROBINHOOD_CHAIN_ROUTE_CAPABILITIES", source)
        self.assertNotIn("ROBINHOOD_CHAIN_DISCOVERY_TOKENS", source)
        self.assertIn("route_capabilities", source)
        self.assertIn("token_contracts_hardcoded", source)
        self.assertIn("pair_capabilities_hardcoded", source)

    async def test_erc20_pair_buy_quote_uses_database_capability_and_token_registry_identity(self) -> None:
        service, discovery = self.make_service()
        quote = await _quote_request(service,
            symbol="WGAS-CRED",
            side="buy",
            quantity=None,
            total_quote="1.25",
            taker_address=TAKER,
            base_token=WGAS,
            quote_token=CRED,
            route_capability=CRED_TO_WGAS,
            force_refresh=True,
        )

        self.assertTrue(quote["ok"])
        self.assertEqual(quote["symbol"], "WGAS-CRED")
        self.assertEqual(quote["base_asset"], "WGAS")
        self.assertEqual(quote["quote_asset"], "CRED")
        self.assertEqual(quote["input_amount"], "1.25")
        self.assertEqual(quote["minimum_received_asset"], "WGAS")
        self.assertEqual(quote["token_identity_source"], "token_registry")
        self.assertEqual(quote["pair_capability_source"], "database")
        self.assertEqual(quote["route_capability"]["from_asset"], "CRED")
        self.assertTrue(quote["allowance_required"])
        self.assertFalse(quote["execution_enabled"])
        self.assertIsNone(quote["transaction_calldata"])
        self.assertGreaterEqual(len(discovery.calls), 1)
        self.assertEqual(discovery.calls[0]["sell"], "CRED")
        self.assertEqual(discovery.calls[0]["buy"], "WGAS")

    async def test_erc20_pair_sell_quote_retains_allowance_requirement(self) -> None:
        service, discovery = self.make_service()
        quote = await _quote_request(service,
            symbol="WGAS-CRED",
            side="sell",
            quantity="0.0005",
            total_quote=None,
            taker_address=TAKER,
            base_token=WGAS,
            quote_token=CRED,
            route_capability=WGAS_TO_CRED,
            force_refresh=True,
        )

        self.assertTrue(quote["ok"])
        self.assertEqual(quote["input_asset"], "WGAS")
        self.assertEqual(quote["output_asset"], "CRED")
        self.assertTrue(quote["allowance_required"])
        self.assertIsNotNone(quote["allowance_spender"])
        self.assertIn("allowance_required", quote["provider_warnings"])
        self.assertEqual(discovery.calls[0]["sell"], "WGAS")
        self.assertEqual(discovery.calls[0]["buy"], "CRED")

    async def test_erc20_pair_blocked_capability_fails_before_provider_contact(self) -> None:
        service, discovery = self.make_service()
        blocked = {**CRED_TO_WGAS, "indicative_status": "provider_error"}
        quote = await _quote_request(service,
            symbol="WGAS-CRED",
            side="buy",
            quantity=None,
            total_quote="1",
            taker_address=TAKER,
            base_token=WGAS,
            quote_token=CRED,
            route_capability=blocked,
            force_refresh=True,
        )

        self.assertFalse(quote["ok"])
        self.assertEqual(quote["error"], "robinhood_chain_quote_route_unavailable")
        self.assertFalse(quote["provider_contacted"])
        self.assertEqual(discovery.calls, [])

    async def test_erc20_exact_receive_custom_quantity_is_blocked_without_provider_contact(self) -> None:
        service, discovery = self.make_service()
        quote = await _quote_request(service,
            symbol="WGAS-CRED",
            side="buy",
            quantity="0.0007",
            total_quote=None,
            taker_address=TAKER,
            base_token=WGAS,
            quote_token=CRED,
            route_capability=None,
            force_refresh=True,
        )

        self.assertFalse(quote["ok"])
        self.assertEqual(quote["error"], "robinhood_chain_quote_route_unavailable")
        self.assertFalse(quote["provider_contacted"])
        self.assertEqual(discovery.calls, [])

    async def test_unsupported_symbol_fails_closed_without_provider_call(self) -> None:
        service, discovery = self.make_service()
        quote = await _quote_request(service,
            symbol="ALPHA-CRED",
            side="sell",
            quantity="0.0001",
            total_quote=None,
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
        )

        self.assertFalse(quote["ok"])
        self.assertEqual(quote["error"], "robinhood_chain_pair_identity_mismatch")
        self.assertEqual(discovery.calls, [])
        self.assertIsNone(quote["transaction_calldata"])

    async def test_synthetic_orderbook_labels_sorting_and_cache(self) -> None:
        service, discovery = self.make_service()
        book = await _book_request(service,
            symbol="GASX-CRED",
            depth=3,
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            force_refresh=True,
        )

        self.assertTrue(book["ok"])
        self.assertTrue(book["synthetic"])
        self.assertTrue(book["quote_only"])
        self.assertFalse(book["resting_order"])
        self.assertFalse(book["execution_enabled"])
        self.assertFalse(book["signing_enabled"])
        self.assertFalse(book["transaction_construction_enabled"])
        self.assertIsNone(book["transaction_calldata"])
        self.assertEqual(len(book["bids"]), 3)
        self.assertEqual(len(book["asks"]), 3)
        self.assertEqual(len(discovery.calls), 6)

        bid_prices = [Decimal(row["price"]) for row in book["bids"]]
        ask_prices = [Decimal(row["price"]) for row in book["asks"]]
        self.assertEqual(bid_prices, sorted(bid_prices, reverse=True))
        self.assertEqual(ask_prices, sorted(ask_prices))
        for row in [*book["bids"], *book["asks"]]:
            self.assertTrue(row["synthetic"])
            self.assertTrue(row["quote_only"])
            self.assertFalse(row["resting_order"])
            self.assertEqual(row["liquidity_label"], "SYNTH")
            self.assertEqual(row["source_type"], "robinhood_chain_0x_indicative")

        provider_call_count = len(discovery.calls)
        cached = await _book_request(service,
            symbol="GASX-CRED",
            depth=3,
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            force_refresh=False,
        )
        self.assertTrue(cached["ok"])
        self.assertTrue(cached["cached"])
        self.assertEqual(cached["snapshot_source"], "synthetic_book_cache")
        self.assertTrue(all(row["cached"] for row in [*cached["bids"], *cached["asks"]]))
        self.assertEqual(len(discovery.calls), provider_call_count)

    async def test_one_level_failure_does_not_erase_other_levels(self) -> None:
        service, _ = self.make_service(fail_inputs={"0.0005"})
        book = await _book_request(service,
            symbol="GASX-CRED",
            depth=3,
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            force_refresh=True,
        )

        self.assertTrue(book["ok"])
        self.assertEqual(len(book["bids"]), 2)
        self.assertEqual(len(book["asks"]), 3)
        self.assertEqual(book["warning_count"], 1)
        self.assertEqual(book["errors"][0]["side"], "bid")

    async def test_book_depth_is_bounded_to_five(self) -> None:
        service, discovery = self.make_service()
        book = await _book_request(service,
            symbol="GASX-CRED",
            depth=200,
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
            force_refresh=True,
        )
        self.assertEqual(len(book["bids"]), ROBINHOOD_CHAIN_MAX_BOOK_LEVELS)
        self.assertEqual(len(book["asks"]), ROBINHOOD_CHAIN_MAX_BOOK_LEVELS)
        self.assertEqual(len(discovery.calls), 10)

    async def test_specific_unavailable_capability_blocks_book_without_provider(self) -> None:
        service, discovery = self.make_service()
        unavailable = {
            "symbol": "WGAS-CRED",
            "mechanism": "swap",
            "from_asset": "WGAS",
            "to_asset": "CRED",
            "amount_mode": "exact_input",
            "indicative_status": "no_liquidity",
            "execution_status": "disabled",
            "enabled": False,
            "probe_amount": "0.004",
            "provider_error": {"classification": "no_liquidity"},
        }
        book = await _pair_book_request(
            service,
            symbol="WGAS-CRED",
            depth=3,
            taker_address=TAKER,
            base_token=WGAS,
            quote_token=CRED,
            base_to_quote_capability=unavailable,
            quote_to_base_capability=CRED_TO_WGAS,
            force_refresh=True,
        )
        self.assertFalse(book["ok"])
        self.assertEqual(book["error"], "robinhood_chain_bid_direction_unavailable")
        self.assertEqual(book["route_capability"]["indicative_status"], "no_liquidity")
        self.assertFalse(book["provider_contacted"])
        self.assertIsNone(book["transaction_calldata"])
        self.assertEqual(discovery.calls, [])

    def test_r5c5d_frontend_refresh_and_manual_field_guards_are_present(self) -> None:
        repo_root = Path(__file__).resolve().parents[2]
        orderbook_source = (repo_root / "frontend" / "src" / "OrderBookWidget.jsx").read_text(encoding="utf-8")
        ticket_source = (repo_root / "frontend" / "src" / "OrderTicketWidget.jsx").read_text(encoding="utf-8")

        self.assertIn("/registry-discovery/markets/${encodeURIComponent(sym)}/refresh", orderbook_source)
        self.assertIn("confirm_refresh: true", orderbook_source)
        self.assertIn("robinhoodChainMarket?.orderbook_enabled !== true && !opts.force", orderbook_source)
        self.assertIn("selected_pair_capability_refresh", orderbook_source)
        self.assertIn("SELECTED PAIR CHECKED", orderbook_source)
        self.assertIn("robinhoodChainRefreshSummaryRef", orderbook_source)
        self.assertIn("providerContactedDirectionCount", orderbook_source)
        self.assertIn("providerHttpStatuses", orderbook_source)
        self.assertIn("PERSISTED PROVIDER EVIDENCE", orderbook_source)
        self.assertIn("persisted_provider_evidence", orderbook_source)
        self.assertIn('legal_restriction: "LEGAL RESTRICTION"', orderbook_source)

        self.assertIn('placeholder={robinhoodChainTicketFieldsUnavailable ? "Manual amount" : totalLabel}', ticket_source)
        self.assertIn('value={qty}', ticket_source)
        self.assertIn('value={totalQuote}', ticket_source)
        self.assertIn('checked={autoCalc}', ticket_source)
        self.assertIn('Auto-calc waiting for quote', ticket_source)
        self.assertIn('state === "legal_restriction"', ticket_source)
        self.assertIn('Provider Legal Restriction', ticket_source)
        self.assertNotIn('value={robinhoodChainTicketFieldsUnavailable ? "" : totalQuote}', ticket_source)
        self.assertNotIn('setQty("");\n        setLimitPrice("");\n        setTotalQuote("");', ticket_source)

    async def test_safe_json_contains_no_provider_calldata(self) -> None:
        service, _ = self.make_service()
        quote = await _quote_request(service,
            symbol="GASX-CRED",
            side="sell",
            quantity="0.0001",
            total_quote=None,
            taker_address=TAKER,
            base_token=GASX,
            quote_token=CRED,
        )
        encoded = json.dumps(quote, sort_keys=True)
        self.assertNotIn("deadbeef", encoded)
        self.assertNotIn(_synthetic_address(505)[2:], encoded)
        self.assertIn('"transaction_calldata": null', encoded)


if __name__ == "__main__":
    unittest.main(verbosity=2)
