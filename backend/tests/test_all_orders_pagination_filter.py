from __future__ import annotations

import os
import unittest
from datetime import datetime, timedelta

from sqlalchemy import create_engine
from sqlalchemy.schema import CreateTable
from sqlalchemy.orm import sessionmaker

from app.models import Order, OrderView, RuntimeSetting, VenueOrderRow
from app.models_lot_journal import LotJournal
from app.services.all_orders import list_all_orders


class AllOrdersPaginationFilterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with self.engine.begin() as connection:
            connection.execute(CreateTable(Order.__table__))
        for table in (VenueOrderRow.__table__, OrderView.__table__, RuntimeSetting.__table__, LotJournal.__table__):
            table.create(self.engine)
        self.Session = sessionmaker(bind=self.engine, expire_on_commit=False, future=True)
        self.db = self.Session()
        self._old_realized = os.environ.get("UTT_REALIZED_FIELDS_V1")
        os.environ["UTT_REALIZED_FIELDS_V1"] = "0"

        now = datetime(2026, 8, 8, 12, 0, 0)
        rows = []
        # Put hidden rows newest so the historical client-side filter would
        # visibly shrink page 1 after raw pagination.
        statuses = (["filled"] * 80) + (["canceled"] * 40) + (["rejected"] * 20)
        for index, status in enumerate(statuses):
            rows.append(
                Order(
                    client_order_id=f"ao-filter-{index}",
                    venue="testvenue",
                    symbol_canon="AAA-USD",
                    symbol_venue="AAAUSD",
                    side="buy",
                    type="limit",
                    qty=1.0,
                    limit_price=1.0,
                    status=status,
                    filled_qty=1.0 if status == "filled" else 0.0,
                    avg_fill_price=1.0 if status == "filled" else None,
                    created_at=now + timedelta(seconds=index),
                    updated_at=now + timedelta(seconds=index),
                )
            )
        self.db.add_all(rows)
        self.db.commit()

    def tearDown(self) -> None:
        self.db.close()
        self.engine.dispose()
        if self._old_realized is None:
            os.environ.pop("UTT_REALIZED_FIELDS_V1", None)
        else:
            os.environ["UTT_REALIZED_FIELDS_V1"] = self._old_realized

    def _list(self, *, status=None, exclude=False, page=1, page_size=50):
        return list_all_orders(
            self.db,
            source=None,
            scope="LOCAL",
            venue=None,
            status=status,
            status_bucket="terminal",
            symbol=None,
            dt_from=None,
            dt_to=None,
            sort_field="created_at",
            sort_dir="desc",
            page=page,
            page_size=page_size,
            exclude_canceled_rejected=exclude,
        )

    def test_hide_filter_applies_before_total_and_page_slice(self) -> None:
        page, total = self._list(exclude=True)
        self.assertEqual(total, 80)
        self.assertEqual(len(page), 50)
        self.assertTrue(all(str(row.get("status") or "").lower() == "filled" for row in page))

    def test_hide_filter_off_preserves_raw_population(self) -> None:
        page, total = self._list(exclude=False)
        self.assertEqual(total, 140)
        self.assertEqual(len(page), 50)
        self.assertTrue(any(str(row.get("status") or "").lower() in {"canceled", "rejected"} for row in page))

    def test_explicit_hidden_status_overrides_hide_filter(self) -> None:
        page, total = self._list(status="rejected", exclude=True)
        self.assertEqual(total, 20)
        self.assertEqual(len(page), 20)
        self.assertTrue(all(str(row.get("status") or "").lower() == "rejected" for row in page))


if __name__ == "__main__":
    unittest.main()
