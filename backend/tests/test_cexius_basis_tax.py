from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from app.models import BasisLot
from app.services.cexius_basis_tax import _fetch_cexius_deposit_records, _simulate_cexius_replay
from app.services.lot_sync import _fee_usd_estimate, _venue_order_accounting_values
from app.services.all_orders import _compute_net_pnl_usd
from app.services.lots_ledger import fifo_consume_sell_fifo


class _FakeCexiusAdapter:
    @staticmethod
    def _asset(value):
        return str(value or "").strip().upper()

    @staticmethod
    def _parse_datetime(value):
        if not value:
            return None
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)

    def fetch_currencies(self):
        return [
            {"id": "DOGE", "symbol": "DOGE", "name": "Dogecoin"},
            {"id": "SOL", "symbol": "SOL", "name": "Solana"},
        ]

    def fetch_transaction_history(self, **params):
        if int(params.get("page", 1)) > 1:
            return []
        return [
            {
                "type": "deposit", "status": "Success", "currency_id": "DOGE",
                "final_amount": 5.0, "created_at": "2026-01-01T00:00:00Z",
                "txid": "synthetic-doge-deposit", "network_name": "Dogecoin Mainnet",
                "fee": 0, "is_internal": False,
            }
        ]

    def fetch_currency_networks(self, currency_id=None):
        return []


class CexiusBasisTaxTests(unittest.TestCase):
    def test_usdt_and_usdg_are_usd_like_fee_assets(self):
        self.assertEqual(_fee_usd_estimate(0.25, "USDT"), 0.25)
        self.assertEqual(_fee_usd_estimate(0.5, "USDG"), 0.5)
        self.assertIsNone(_fee_usd_estimate(0.5, "DOGE"))

    def test_cexius_buy_uses_post_fee_quantity_and_quote_basis(self):
        row = SimpleNamespace(
            venue="cexius", side="buy", symbol_canon="DOGE-USDT", symbol_venue="DOGE-USDT",
            filled_qty=14.6666, qty=14.6666, avg_fill_price=None,
            fee=0.0146666, fee_asset="DOGE", total_after_fee=0.99,
        )
        values = _venue_order_accounting_values(row)
        self.assertAlmostEqual(values["qty"], 14.6519334, places=10)
        self.assertIsNone(values["fee_usd"])
        self.assertAlmostEqual(values["qty"] * values["price_usd"], 0.99, places=12)

    def test_all_orders_does_not_double_subtract_fifo_fee(self):
        pnl = _compute_net_pnl_usd(
            None,
            gross_gain=4.9,
            proceeds=9.9,
            basis_used=5.0,
            fee_usd=0.1,
        )
        self.assertAlmostEqual(pnl, 4.9, places=12)

    def test_cexius_sell_reconstructs_gross_and_net_proceeds(self):
        row = SimpleNamespace(
            venue="cexius", side="sell", symbol_canon="DOGE-USDT", symbol_venue="DOGE-USDT",
            filled_qty=10.0, qty=10.0, avg_fill_price=None,
            fee=0.1, fee_asset="USDT", total_after_fee=9.9,
        )
        values = _venue_order_accounting_values(row)
        self.assertAlmostEqual(values["price_usd"], 1.0, places=12)
        self.assertAlmostEqual(values["fee_usd"], 0.1, places=12)
        self.assertAlmostEqual(values["qty"] * values["price_usd"] - values["fee_usd"], 9.9, places=12)

    def test_sell_fifo_does_not_consume_future_lot(self):
        engine = create_engine("sqlite:///:memory:")
        BasisLot.__table__.create(engine)
        sell_time = datetime(2026, 1, 2)
        with Session(engine) as db:
            db.add(BasisLot(
                venue="cexius", wallet_id="default", asset="DOGE",
                acquired_at=sell_time + timedelta(days=1), qty_total=10.0, qty_remaining=10.0,
                total_basis_usd=10.0, basis_is_missing=False, basis_source="FILL",
                origin_type="BUY_FILL", origin_ref="future-buy",
            ))
            db.commit()
            with self.assertRaises(ValueError):
                fifo_consume_sell_fifo(
                    db, venue="cexius", wallet_id="default", asset="DOGE", qty_sold=1.0,
                    price_usd=2.0, fee_usd=0.0, as_of=sell_time, allow_partial=False,
                )

    def test_deposit_history_resolves_quantity_but_not_basis(self):
        rows = _fetch_cexius_deposit_records(_FakeCexiusAdapter())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["asset"], "DOGE")
        self.assertEqual(rows[0]["qty"], 5.0)
        self.assertEqual(rows[0]["source"], "CEXIUS_API")
        self.assertNotIn("address", rows[0]["raw"])

    def test_replay_keeps_tax_unknown_when_older_deposit_basis_is_missing(self):
        t0 = datetime(2026, 1, 1)
        deposit = {"asset": "DOGE", "qty": 5.0, "deposit_time": t0, "txid_sha256": "a" * 64}
        buy = SimpleNamespace(
            id="buy-1", venue="cexius", side="buy", status="filled",
            symbol_canon="DOGE-USDT", symbol_venue="DOGE-USDT", filled_qty=10.0, qty=10.0,
            avg_fill_price=None, fee=0.01, fee_asset="DOGE", total_after_fee=10.0,
            updated_at=t0 + timedelta(hours=1), created_at=None, captured_at=None,
        )
        sell = SimpleNamespace(
            id="sell-1", venue="cexius", side="sell", status="filled",
            symbol_canon="DOGE-USDT", symbol_venue="DOGE-USDT", filled_qty=6.0, qty=6.0,
            avg_fill_price=None, fee=0.006, fee_asset="USDT", total_after_fee=5.994,
            updated_at=t0 + timedelta(hours=2), created_at=None, captured_at=None,
        )
        replay = _simulate_cexius_replay([buy, sell], [deposit])
        self.assertEqual(replay["sell_total"], 1)
        self.assertEqual(replay["insufficient"], 0)
        self.assertEqual(replay["basis_missing"], 1)
        self.assertEqual(replay["realized_complete"], 0)

    def test_frontend_usdt_tax_gate_and_basis_gap_guard_are_present(self):
        widget = Path(__file__).resolve().parents[2] / "frontend" / "src" / "TerminalTablesWidget.jsx"
        text = widget.read_text(encoding="utf-8")
        self.assertIn('new Set(["USD", "USDC", "USDT", "USDG"])', text)
        self.assertIn("function hasAppliedRealizedBasisGap", text)
        self.assertIn("!realizedBasisGap", text)
        self.assertIn("isRobinhoodChainExecutionRow || realizedBasisGap", text)
        self.assertIn("isCanceled || realizedBasisGap", text)
        self.assertIn('"Cost basis missing; realized tax cannot be computed"', text)


if __name__ == "__main__":
    unittest.main()
