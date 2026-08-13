from __future__ import annotations

import unittest
from typing import Any, Dict, List, Tuple

from app.services.robinhood_chain_uniswap_v3_quote import (
    UNISWAP_V3_FACTORY,
    UNISWAP_V3_QUOTER_V2,
    RobinhoodChainUniswapV3QuoteService,
    decode_abi_address,
    encode_factory_get_pool,
    encode_quote_exact_input,
    encode_quote_exact_input_single,
    encode_v3_path,
)


def _word(value: int) -> str:
    return f"{int(value):064x}"


def _abi_address(address: str) -> str:
    return "0x" + address[2:].lower().rjust(64, "0")


def _abi_quote(amount_out: int) -> str:
    return "0x" + _word(amount_out) + _word(0) + _word(0) + _word(120000)


class _FakeRpc:
    def __init__(self) -> None:
        self.calls: List[Tuple[str, list]] = []
        self.pools: Dict[Tuple[str, str, int], str] = {}
        self.single_rates: Dict[Tuple[str, str, int], Tuple[int, int]] = {}
        self.path_output: int = 0

    def status(self) -> Dict[str, Any]:
        return {"configured": True}

    async def verify_expected_chain(self, *, force_refresh: bool = False) -> Dict[str, Any]:
        return {"ok": True, "actual_chain_id": 4663, "expected_chain_id": 4663}

    async def rpc_read(
        self,
        method: str,
        params: list,
        *,
        cache_namespace: str,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        self.calls.append((method, list(params)))
        call = params[0]
        to = str(call.get("to") or "").lower()
        data = str(call.get("data") or "").lower()
        if to == UNISWAP_V3_FACTORY:
            body = data[10:]
            token_a = "0x" + body[24:64]
            token_b = "0x" + body[64 + 24:128]
            fee = int(body[128:192], 16)
            pool = self.pools.get((token_a, token_b, fee)) or self.pools.get((token_b, token_a, fee))
            return {"ok": True, "result": _abi_address(pool) if pool else "0x" + ("0" * 64)}
        if to == UNISWAP_V3_QUOTER_V2 and data.startswith("0xc6a5026a"):
            body = data[10:]
            token_in = "0x" + body[24:64]
            token_out = "0x" + body[64 + 24:128]
            amount_in = int(body[128:192], 16)
            fee = int(body[192:256], 16)
            numerator, denominator = self.single_rates[(token_in, token_out, fee)]
            return {"ok": True, "result": _abi_quote(amount_in * numerator // denominator)}
        if to == UNISWAP_V3_QUOTER_V2 and data.startswith("0xcdca1753"):
            return {"ok": True, "result": _abi_quote(self.path_output)}
        return {"ok": False, "error": {"message": "unsupported fake call"}}


class RobinhoodChainUniswapV3QuoteTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.rpc = _FakeRpc()
        self.service = RobinhoodChainUniswapV3QuoteService(rpc_client=self.rpc)
        self.token_a = "0x" + "11" * 20
        self.token_b = "0x" + "22" * 20
        self.weth = "0x" + "33" * 20
        self.pool_a = "0x" + "aa" * 20
        self.pool_b = "0x" + "bb" * 20

    @staticmethod
    def _token(symbol: str, address: str, decimals: int, registry_id: int) -> Dict[str, Any]:
        return {
            "symbol": symbol,
            "registry_id": registry_id,
            "identity_source": "token_registry",
            "native": False,
            "decimals": decimals,
            "contract_address": address,
            "registry_contract_address": address,
        }

    def test_abi_encoders_match_v3_selectors_and_shapes(self) -> None:
        get_pool = encode_factory_get_pool(self.token_a, self.token_b, 3000)
        self.assertTrue(get_pool.startswith("0x1698ee82"))
        self.assertEqual(len(get_pool), 2 + 8 + (64 * 3))
        single = encode_quote_exact_input_single(self.token_a, self.token_b, 10**18, 3000)
        self.assertTrue(single.startswith("0xc6a5026a"))
        self.assertEqual(len(single), 2 + 8 + (64 * 5))
        path = encode_v3_path([self.token_a, self.weth, self.token_b], [500, 3000])
        self.assertEqual(len(path), 20 + 3 + 20 + 3 + 20)
        multi = encode_quote_exact_input(path, 10**18)
        self.assertTrue(multi.startswith("0xcdca1753"))
        self.assertEqual(decode_abi_address(_abi_address(self.pool_a)), self.pool_a)

    async def test_direct_quote_selects_best_fee_tier(self) -> None:
        self.rpc.pools[(self.token_a, self.token_b, 500)] = self.pool_a
        self.rpc.pools[(self.token_a, self.token_b, 3000)] = self.pool_b
        self.rpc.single_rates[(self.token_a, self.token_b, 500)] = (2, 1)
        self.rpc.single_rates[(self.token_a, self.token_b, 3000)] = (3, 1)
        result = await self.service.probe(
            requested_amount="1",
            input_token=self._token("AAA", self.token_a, 18, 1),
            output_token=self._token("BBB", self.token_b, 18, 2),
            force_refresh=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["provider"], "uniswap_v3_rpc")
        self.assertEqual(result["route_type"], "direct")
        self.assertEqual(result["route_fees"], [3000])
        self.assertEqual(result["buy_amount"], "3")
        self.assertEqual(result["price_buy_per_sell"], "3")
        self.assertFalse(result["transaction_constructed"])

    async def test_weth_bridge_quote_is_used_when_direct_pool_missing(self) -> None:
        self.rpc.pools[(self.token_a, self.weth, 500)] = self.pool_a
        self.rpc.pools[(self.weth, self.token_b, 3000)] = self.pool_b
        self.rpc.path_output = 2500000
        result = await self.service.probe(
            requested_amount="1",
            input_token=self._token("AAA", self.token_a, 18, 1),
            output_token=self._token("USDG", self.token_b, 6, 2),
            bridge_token=self._token("WETH", self.weth, 18, 3),
            force_refresh=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["route_type"], "weth_bridge")
        self.assertEqual(result["bridge_symbol"], "WETH")
        self.assertEqual(result["route_fees"], [500, 3000])
        self.assertEqual(result["buy_amount"], "2.5")

    async def test_native_eth_buy_quote_maps_to_weth_and_preserves_display_identity(self) -> None:
        self.rpc.pools[(self.weth, self.token_a, 500)] = self.pool_a
        self.rpc.single_rates[(self.weth, self.token_a, 500)] = (2, 1)
        eth = {
            "symbol": "ETH",
            "registry_id": 2,
            "identity_source": "token_registry",
            "native": True,
            "decimals": 18,
            "contract_address": None,
            "registry_contract_address": None,
        }
        result = await self.service.quote_for_pair(
            symbol="AAA-ETH",
            side="buy",
            amount_mode="exact_input",
            requested_amount="0.1",
            base_token=self._token("AAA", self.token_a, 18, 1),
            quote_token=eth,
            weth_token=self._token("WETH", self.weth, 18, 3),
            force_refresh=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["input_asset"], "ETH")
        self.assertEqual(result["output_asset"], "AAA")
        self.assertEqual(result["input_amount"], "0.1")
        self.assertEqual(result["output_amount"], "0.2")
        self.assertEqual(result["effective_price"], "0.5")
        self.assertEqual(result["base_quantity"], "0.2")
        self.assertEqual(result["quote_quantity"], "0.1")
        self.assertFalse(result["transaction_constructed"])

    async def test_native_eth_sell_quote_maps_to_weth_and_preserves_display_identity(self) -> None:
        self.rpc.pools[(self.token_a, self.weth, 500)] = self.pool_a
        self.rpc.single_rates[(self.token_a, self.weth, 500)] = (1, 2)
        eth = {
            "symbol": "ETH",
            "registry_id": 2,
            "identity_source": "token_registry",
            "native": True,
            "decimals": 18,
            "contract_address": None,
            "registry_contract_address": None,
        }
        result = await self.service.quote_for_pair(
            symbol="AAA-ETH",
            side="sell",
            amount_mode="exact_input",
            requested_amount="1",
            base_token=self._token("AAA", self.token_a, 18, 1),
            quote_token=eth,
            weth_token=self._token("WETH", self.weth, 18, 3),
            force_refresh=True,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["input_asset"], "AAA")
        self.assertEqual(result["output_asset"], "ETH")
        self.assertEqual(result["input_amount"], "1")
        self.assertEqual(result["output_amount"], "0.5")
        self.assertEqual(result["effective_price"], "0.5")
        self.assertEqual(result["base_quantity"], "1")
        self.assertEqual(result["quote_quantity"], "0.5")
        self.assertFalse(result["transaction_constructed"])

    async def test_no_pool_returns_read_only_no_liquidity_result(self) -> None:
        result = await self.service.probe(
            requested_amount="1",
            input_token=self._token("AAA", self.token_a, 18, 1),
            output_token=self._token("BBB", self.token_b, 18, 2),
            force_refresh=True,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "uniswap_v3_pool_not_found")
        self.assertTrue(result["provider_contacted"])
        self.assertFalse(result["will_mutate"])


if __name__ == "__main__":
    unittest.main()
