from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from app.adapters.cryptocom_exchange import CryptoComExchangeAdapter
from app.adapters.gemini import GeminiAdapter


class AllOrdersFeeUnitTests(unittest.TestCase):
    def test_cryptocom_base_fee_is_not_mixed_into_quote_total(self):
        adapter = CryptoComExchangeAdapter()
        row = {
            "order_id": "cdc-buy",
            "instrument_name": "UNI_USD",
            "side": "BUY",
            "order_type": "LIMIT",
            "status": "FILLED",
            "quantity": "1",
            "cumulative_quantity": "1",
            "price": "10",
            "cumulative_value": "10",
            "cumulative_fee": "0.001",
            "fee_instrument_name": "UNI",
        }
        with patch.object(adapter, "_canon_from_instrument", return_value="UNI-USD"):
            mapped = adapter._map_order_row(row)
        self.assertEqual(mapped["fee_asset"], "UNI")
        self.assertEqual(mapped["total_after_fee"], 10.0)

    def test_cryptocom_quote_fee_preserves_existing_side_behavior(self):
        adapter = CryptoComExchangeAdapter()
        buy = {
            "order_id": "cdc-buy-usd-fee",
            "instrument_name": "UNI_USD",
            "side": "BUY",
            "order_type": "LIMIT",
            "status": "FILLED",
            "quantity": "1",
            "cumulative_quantity": "1",
            "price": "10",
            "cumulative_value": "10",
            "cumulative_fee": "0.01",
            "fee_instrument_name": "USD",
        }
        sell = dict(buy, order_id="cdc-sell-usd-fee", side="SELL")
        with patch.object(adapter, "_canon_from_instrument", return_value="UNI-USD"):
            buy_mapped = adapter._map_order_row(buy)
            sell_mapped = adapter._map_order_row(sell)
        self.assertAlmostEqual(buy_mapped["total_after_fee"], 10.01, places=12)
        self.assertAlmostEqual(sell_mapped["total_after_fee"], 9.99, places=12)

    def test_gemini_only_subtracts_fee_when_fee_asset_matches_quote(self):
        adapter = GeminiAdapter()
        self.assertEqual(
            adapter._quote_total_after_fee(
                notional=10.0, fee=0.001, fee_asset="UNI", symbol_canon="UNI-USD"
            ),
            10.0,
        )
        self.assertEqual(
            adapter._quote_total_after_fee(
                notional=10.0, fee=0.01, fee_asset="USD", symbol_canon="UNI-USD"
            ),
            9.99,
        )
        self.assertEqual(
            adapter._quote_total_after_fee(
                notional=10.0, fee=0.01, fee_asset=None, symbol_canon="UNI-USD"
            ),
            10.0,
        )

    def test_all_orders_ui_has_qty_after_fee_and_compact_timestamp_contract(self):
        repo_root = Path(__file__).resolve().parents[2]
        source = (repo_root / "frontend" / "src" / "TerminalTablesWidget.jsx").read_text(encoding="utf-8")

        self.assertIn('qtyAfterFee: "qty_after_fee"', source)
        self.assertIn('>Qty a/fee</th>', source)
        self.assertIn('feeAsset === baseAssetForFee', source)
        self.assertIn('numericFee <= filledQtyForFee', source)
        self.assertIn('qtyAfterFeeUseQty', source)
        self.assertIn('qtyAfterFeeText = "Use Qty"', source)
        self.assertIn('qtyAfterFeeText = "N/A"', source)
        self.assertIn('Fee asset unknown; quantity after fee cannot be confirmed.', source)
        self.assertIn('Quantity after fee\\nAsset:', source)
        self.assertIn('function formatAllOrdersTimeCompact(value)', source)
        self.assertIn('d.getFullYear() % 100', source)
        self.assertIn('title={hideTableDataGlobal ? undefined : String(o?.created_at || "")}', source)
        self.assertIn('title={hideTableDataGlobal ? undefined : String(o?.closed_at || "")}', source)
        self.assertIn('utt_all_orders_columns_mig_v3_qty_after_fee_v1', source)


if __name__ == "__main__":
    unittest.main()
