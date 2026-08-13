from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db import Base
from app.models import (
    RobinhoodChainWalletCheckpoint,
    RobinhoodChainWalletEvent,
    WalletAddress,
)
from app.services import robinhood_chain_wallet_ingest as ingest_mod


class _FakeHistory:
    async def scan_address_history(self, address, **kwargs):
        return {
            "ok": True,
            "items": [
                {
                    "id": "0x" + "1" * 64 + ":erc20:7",
                    "transaction_hash": "0x" + "1" * 64,
                    "timestamp": "2026-08-01T12:00:00Z",
                    "status": "ok",
                    "classification": "erc20_transfer",
                    "direction": "in",
                    "asset": "TEST",
                    "amount_atomic": "123",
                    "decimals": 6,
                    "fee_wei": "0",
                    "block_number": 100,
                    "contract_address": "0x" + "2" * 40,
                    "registry_id": 9,
                    "registered": True,
                    "from_address": "0x" + "3" * 40,
                    "to_address": address,
                    "source": "blockscout_v2",
                    "provider_raw": {"log_index": 7},
                },
                {
                    "id": "0x" + "1" * 64 + ":transaction",
                    "transaction_hash": "0x" + "1" * 64,
                    "timestamp": "2026-08-01T12:00:00Z",
                    "status": "ok",
                    "classification": "contract_call",
                    "direction": "out",
                    "asset": None,
                    "amount_atomic": "0",
                    "decimals": None,
                    "fee_wei": "1000",
                    "block_number": 100,
                    "contract_address": None,
                    "registry_id": None,
                    "registered": False,
                    "from_address": address,
                    "to_address": "0x" + "4" * 40,
                    "source": "blockscout_v2",
                    "provider_raw": {},
                },
            ],
            "pages_scanned": 2,
            "max_pages": 200,
            "provider_counts": {
                "transactions": 1,
                "token_transfers": 1,
                "internal_transactions": 0,
            },
            "provider_errors": [],
            "partial": False,
            "provider_exhausted": True,
            "reached_checkpoint": False,
            "truncated": False,
            "newest_block_number": 100,
            "oldest_block_number": 100,
        }


class _FakePartialHistory(_FakeHistory):
    async def scan_address_history(self, address, **kwargs):
        payload = await super().scan_address_history(address, **kwargs)
        payload["provider_errors"] = [
            {
                "source": "token_transfers",
                "page": 8,
                "error": "ReadTimeout",
                "error_type": "ReadTimeout",
                "attempts": 3,
                "retryable": True,
            }
        ]
        payload["partial"] = True
        return payload


def _session():
    engine = create_engine("sqlite:///:memory:")
    WalletAddress.__table__.create(bind=engine)
    RobinhoodChainWalletEvent.__table__.create(bind=engine)
    RobinhoodChainWalletCheckpoint.__table__.create(bind=engine)
    # Mirror app.db.SessionLocal: production explicitly disables autoflush.
    Session = sessionmaker(bind=engine, autoflush=False)
    return Session()


def _wallet(db):
    row = WalletAddress(
        asset="ALL",
        network="robinhood_chain",
        wallet_id="robinhood_chain",
        address="0x" + "a" * 40,
        owner_scope="user",
    )
    db.add(row)
    db.commit()
    return row


class RobinhoodChainWalletIngestTests(unittest.TestCase):
    def test_wallet_ingest_event_identity_and_checkpoint_are_idempotent(self):
        db = _session()
        self.addCleanup(db.close)
        wallet = _wallet(db)

        with (
            patch.object(
                ingest_mod,
                "get_robinhood_chain_history_service",
                return_value=_FakeHistory(),
            ),
            patch.object(
                ingest_mod,
                "_registered_history_token_map",
                return_value={},
            ),
            patch.object(
                ingest_mod,
                "_existing_utt_tx_hashes",
                return_value=set(),
            ),
        ):
            first = asyncio.run(
                ingest_mod.ingest_robinhood_chain_wallet_history(
                    db,
                    wallet_address_id=wallet.id,
                    force_full=True,
                    force_refresh=False,
                )
            )
            db.commit()

            self.assertIs(first["ok"], True)
            self.assertEqual(first["created_events"], 2)
            self.assertEqual(db.query(RobinhoodChainWalletEvent).count(), 2)

            checkpoint = db.query(RobinhoodChainWalletCheckpoint).one()
            self.assertIs(checkpoint.fully_backfilled, True)
            self.assertEqual(checkpoint.event_count, 2)
            self.assertEqual(checkpoint.transaction_count, 1)

            second = asyncio.run(
                ingest_mod.ingest_robinhood_chain_wallet_history(
                    db,
                    wallet_address_id=wallet.id,
                    force_full=True,
                    force_refresh=False,
                )
            )
            db.commit()

            self.assertIs(second["ok"], True)
            self.assertEqual(second["created_events"], 0)
            self.assertEqual(db.query(RobinhoodChainWalletEvent).count(), 2)

    def test_wallet_ingest_requires_all_scope_robinhood_chain_wallet(self):
        db = _session()
        self.addCleanup(db.close)
        row = WalletAddress(
            asset="ETH",
            network="robinhood_chain",
            wallet_id="robinhood_chain",
            address="0x" + "b" * 40,
            owner_scope="user",
        )
        db.add(row)
        db.commit()

        with self.assertRaisesRegex(ValueError, "all_scope_required"):
            ingest_mod._wallet_row(db, row.id)

    def test_internal_native_event_gets_stable_event_identity(self):
        from app.services.robinhood_chain_history import _normalize_internal_transaction

        wallet = "0x" + "a" * 40
        tx_hash = "0x" + "9" * 64
        row = _normalize_internal_transaction(
            {
                "transaction_hash": tx_hash,
                "index": 3,
                "from": {"hash": "0x" + "b" * 40},
                "to": {"hash": wallet},
                "value": "1000000000000000",
                "block_number": 123,
                "timestamp": "2026-08-01T12:00:00Z",
                "success": True,
                "type": "call",
            },
            owner=wallet,
        )

        self.assertIsNotNone(row)
        self.assertEqual(row["id"], f"{tx_hash}:internal:3")
        self.assertEqual(row["direction"], "in")
        self.assertEqual(row["asset"], "ETH")
        self.assertEqual(row["fee_wei"], "0")


    def test_incomplete_scan_refuses_event_and_checkpoint_persistence(self):
        db = _session()
        self.addCleanup(db.close)
        wallet = _wallet(db)

        with (
            patch.object(
                ingest_mod,
                "get_robinhood_chain_history_service",
                return_value=_FakePartialHistory(),
            ),
            patch.object(
                ingest_mod,
                "_registered_history_token_map",
                return_value={},
            ),
            patch.object(
                ingest_mod,
                "_existing_utt_tx_hashes",
                return_value=set(),
            ),
        ):
            result = asyncio.run(
                ingest_mod.ingest_robinhood_chain_wallet_history(
                    db,
                    wallet_address_id=wallet.id,
                    force_full=True,
                    force_refresh=False,
                )
            )

        self.assertIs(result["ok"], False)
        self.assertEqual(result["error"], "robinhood_chain_wallet_ingest_incomplete_scan")
        self.assertIs(result["will_mutate"], False)
        self.assertEqual(db.query(RobinhoodChainWalletEvent).count(), 0)
        self.assertEqual(db.query(RobinhoodChainWalletCheckpoint).count(), 0)

    def test_scan_provider_timeout_retries_and_returns_nonempty_error_type(self):
        from app.services.robinhood_chain_history import RobinhoodChainHistoryService

        service = RobinhoodChainHistoryService(
            api_base="https://example.invalid/api/v2",
            timeout_s=2.0,
            cache_ttl_s=0.0,
            error_backoff_s=0.0,
            max_pages=5,
            page_size=100,
            max_concurrent=1,
        )

        success = {"items": [], "next_page_params": None}
        with (
            patch.object(
                service,
                "_get_json",
                new=AsyncMock(side_effect=[httpx.ReadTimeout(""), success]),
            ) as mocked_get,
            patch(
                "app.services.robinhood_chain_history.asyncio.sleep",
                new=AsyncMock(),
            ),
        ):
            payload = asyncio.run(
                service._scan_get_json(
                    "addresses/0x" + "a" * 40 + "/token-transfers",
                    {"type": "ERC-20"},
                )
            )

        self.assertEqual(payload, success)
        self.assertEqual(mocked_get.await_count, 2)

    def test_partial_provider_failure_is_not_reported_as_exhausted_or_truncated(self):
        from app.services.robinhood_chain_history import RobinhoodChainHistoryService

        class _FakeChainClient:
            async def verify_expected_chain(self, *, force_refresh):
                return {"ok": True}

        service = RobinhoodChainHistoryService(
            api_base="https://example.invalid/api/v2",
            timeout_s=2.0,
            cache_ttl_s=0.0,
            error_backoff_s=0.0,
            max_pages=5,
            page_size=100,
            max_concurrent=1,
        )

        empty = {"items": [], "next_page_params": None}
        with (
            patch(
                "app.services.robinhood_chain_history.get_robinhood_chain_client",
                return_value=_FakeChainClient(),
            ),
            patch.object(
                service,
                "_scan_get_json",
                new=AsyncMock(side_effect=[empty, httpx.ReadTimeout(""), empty]),
            ),
        ):
            result = asyncio.run(
                service.scan_address_history(
                    "0x" + "a" * 40,
                    force_refresh=True,
                    registry_tokens={},
                    max_pages=10,
                )
            )

        self.assertIs(result["ok"], True)
        self.assertIs(result["partial"], True)
        self.assertIs(result["provider_exhausted"], False)
        self.assertIs(result["truncated"], False)
        self.assertEqual(result["provider_errors"][0]["source"], "token_transfers")
        self.assertEqual(result["provider_errors"][0]["error"], "ReadTimeout")

    def test_scan_provider_timeout_exhaustion_reports_type_and_attempts(self):
        from app.services.robinhood_chain_history import RobinhoodChainHistoryService

        service = RobinhoodChainHistoryService(
            api_base="https://example.invalid/api/v2",
            timeout_s=2.0,
            cache_ttl_s=0.0,
            error_backoff_s=0.0,
            max_pages=5,
            page_size=100,
            max_concurrent=1,
        )

        with (
            patch.object(
                service,
                "_get_json",
                new=AsyncMock(side_effect=[httpx.ReadTimeout(""), httpx.ReadTimeout(""), httpx.ReadTimeout("")]),
            ),
            patch(
                "app.services.robinhood_chain_history.asyncio.sleep",
                new=AsyncMock(),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "ReadTimeout") as raised:
                asyncio.run(
                    service._scan_get_json(
                        "addresses/0x" + "a" * 40 + "/token-transfers",
                        {"type": "ERC-20"},
                    )
                )

        self.assertEqual(getattr(raised.exception, "error_type", None), "ReadTimeout")
        self.assertEqual(getattr(raised.exception, "attempts", None), 3)
        self.assertIs(getattr(raised.exception, "retryable", None), True)


if __name__ == "__main__":
    unittest.main()
