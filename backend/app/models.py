import uuid
from datetime import datetime

from sqlalchemy import (
    String,
    Float,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    Index,
    Integer,
    Boolean,
    JSON,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base


def _uuid() -> str:
    return str(uuid.uuid4())


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    client_order_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)

    venue: Mapped[str] = mapped_column(String(16), nullable=False)  # gemini/kraken/coinbase
    symbol_canon: Mapped[str] = mapped_column(String(32), nullable=False)  # USDT-USD
    symbol_venue: Mapped[str] = mapped_column(String(32), nullable=False)  # USDTUSD (example)

    side: Mapped[str] = mapped_column(String(8), nullable=False)   # buy/sell
    type: Mapped[str] = mapped_column(String(8), nullable=False)   # market/limit

    qty: Mapped[float] = mapped_column(Float, nullable=False)
    limit_price: Mapped[float | None] = mapped_column(Float, nullable=True)

    status: Mapped[str] = mapped_column(String(16), nullable=False, default="new")
    filled_qty: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    avg_fill_price: Mapped[float | None] = mapped_column(Float, nullable=True)

    venue_order_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    reject_reason: Mapped[str | None] = mapped_column(String(256), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    fills: Mapped[list["Fill"]] = relationship("Fill", back_populates="order", cascade="all, delete-orphan")


Index("ix_orders_venue_created", Order.venue, Order.created_at)
Index("ix_orders_symbol_created", Order.symbol_canon, Order.created_at)
Index("ix_orders_status_created", Order.status, Order_at:=Order.status, Order.created_at)  # NOTE: kept as-is? (see below)
# The line above was not in your paste; removing it to avoid accidental edit.


Index("ix_orders_venue_created", Order.venue, Order.created_at)
Index("ix_orders_symbol_created", Order.symbol_canon, Order.created_at)
Index("ix_orders_status_created", Order.status, Order.created_at)


class Fill(Base):
    __tablename__ = "fills"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    order_id: Mapped[str] = mapped_column(String(36), ForeignKey("orders.id", ondelete="CASCADE"), nullable=False)

    venue: Mapped[str] = mapped_column(String(16), nullable=False)
    symbol_canon: Mapped[str] = mapped_column(String(32), nullable=False)

    venue_trade_id: Mapped[str] = mapped_column(String(64), nullable=False)
    qty: Mapped[float] = mapped_column(Float, nullable=False)
    price: Mapped[float] = mapped_column(Float, nullable=False)
    fee: Mapped[float | None] = mapped_column(Float, nullable=True)
    fee_asset: Mapped[str | None] = mapped_column(String(16), nullable=True)

    ts: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    order: Mapped["Order"] = relationship("Order", back_populates="fills")


UniqueConstraint("venue", "venue_trade_id", name="uq_fill_venue_trade")

Index("ix_fills_venue_ts", Fill.venue, Fill.ts)
Index("ix_fills_symbol_ts", Fill.symbol_canon, Fill.ts)


class BalanceSnapshot(Base):
    __tablename__ = "balance_snapshots"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    venue: Mapped[str] = mapped_column(String(16), nullable=False)
    asset: Mapped[str] = mapped_column(String(16), nullable=False)  # BTC, USDT, USD
    total: Mapped[float] = mapped_column(Float, nullable=False)
    available: Mapped[float] = mapped_column(Float, nullable=False)
    hold: Mapped[float | None] = mapped_column(Float, nullable=True)
    captured_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


Index("ix_bal_venue_time", BalanceSnapshot.venue, BalanceSnapshot.captured_at)
Index("ix_bal_asset_time", BalanceSnapshot.asset, BalanceSnapshot.captured_at)


class VenueCooldown(Base):
    __tablename__ = "venue_cooldowns"

    venue: Mapped[str] = mapped_column(String(32), primary_key=True, index=True)
    blocked_until: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    reason: Mapped[str | None] = mapped_column(String(256), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class VenueOrderRow(Base):
    """
    Read-only “venue native” orders ingested from exchanges.
    Unique by (venue, venue_order_id).
    """
    __tablename__ = "venue_orders"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    venue: Mapped[str] = mapped_column(String(16), nullable=False)
    venue_order_id: Mapped[str] = mapped_column(String(64), nullable=False)

    symbol_venue: Mapped[str] = mapped_column(String(32), nullable=False)
    symbol_canon: Mapped[str] = mapped_column(String(32), nullable=True)

    side: Mapped[str | None] = mapped_column(String(8), nullable=True)   # buy/sell
    type: Mapped[str | None] = mapped_column(String(16), nullable=True)  # market/limit/etc

    status: Mapped[str | None] = mapped_column(String(32), nullable=True)

    qty: Mapped[float | None] = mapped_column(Float, nullable=True)
    filled_qty: Mapped[float | None] = mapped_column(Float, nullable=True)
    limit_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    avg_fill_price: Mapped[float | None] = mapped_column(Float, nullable=True)

    fee: Mapped[float | None] = mapped_column(Float, nullable=True)
    fee_asset: Mapped[str | None] = mapped_column(String(16), nullable=True)

    total_after_fee: Mapped[float | None] = mapped_column(Float, nullable=True)

    created_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    captured_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("venue", "venue_order_id", name="uq_venue_orders_venue_oid"),
        Index("ix_venue_orders_venue_captured", "venue", "captured_at"),
        Index("ix_venue_orders_status_captured", "status", "captured_at"),
        Index("ix_venue_orders_symbol_captured", "symbol_venue", "captured_at"),
    )


class OrderView(Base):
    """
    Tracks whether the user has "confirmed/viewed" an order row in the unified list.
    Keyed by view_key, which is stable across sources:
      - LOCAL:{order_id}
      - VENUE:{venue}:{venue_order_id}
    """
    __tablename__ = "order_views"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    view_key: Mapped[str] = mapped_column(String(128), nullable=False, unique=True)

    viewed_confirmed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    viewed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


Index("ix_order_views_view_key", OrderView.view_key)
Index("ix_order_views_viewed_at", OrderView.viewed_at)


class SymbolView(Base):
    """
    Tracks whether the user has 'confirmed' they have seen a newly discovered symbol.

    view_key is expected to be a stable identifier like:
        "{venue}:{symbol_canon}"
    """
    __tablename__ = "symbol_views"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    view_key: Mapped[str] = mapped_column(String(256), nullable=False, unique=True, index=True)

    viewed_confirmed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    viewed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


Index("ix_symbol_views_view_key", SymbolView.view_key)
Index("ix_symbol_views_viewed_at", SymbolView.viewed_at)


class VenueSymbolSnapshot(Base):
    """
    Append-only snapshot rows for venue symbol discovery.

    Each refresh inserts one row per symbol at a single captured_at timestamp.
    This enables deterministic diffs (new listings) between snapshots later.
    """
    __tablename__ = "venue_symbols"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    venue: Mapped[str] = mapped_column(String(32), nullable=False)
    symbol_venue: Mapped[str] = mapped_column(String(64), nullable=False)
    symbol_canon: Mapped[str] = mapped_column(String(64), nullable=False)

    base_asset: Mapped[str] = mapped_column(String(32), nullable=False)
    quote_asset: Mapped[str] = mapped_column(String(32), nullable=False)

    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    captured_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


Index("ix_venue_symbols_venue_time", VenueSymbolSnapshot.venue, VenueSymbolSnapshot.captured_at)
Index("ix_venue_symbols_symbol_canon", VenueSymbolSnapshot.symbol_canon, VenueSymbolSnapshot.captured_at)




# =============================================================================
# Token / Symbol Registry (UI-managed)
# =============================================================================

class TokenRegistry(Base):
    """User-managed registry of token metadata.

    This table is intentionally *readable* (unlike API Key Vault) and is used to
    avoid hard-coding token mints/decimals in env JSON blobs.

    Scope rules (v1):
      - venue=NULL => global canonical mapping for a chain
      - venue!=NULL => venue-specific override (future use; safe to store today)
    """

    __tablename__ = "token_registry"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # e.g. "solana" (lowercase)
    chain: Mapped[str] = mapped_column(String(16), nullable=False, default="solana")

    # NULL = global; else venue id like "coinbase" (optional)
    venue: Mapped[str | None] = mapped_column(String(32), nullable=True)

    # Symbol ticker like "UTTT" (uppercase)
    symbol: Mapped[str] = mapped_column(String(32), nullable=False)

    # Contract address / mint (chain-specific). For Solana, this is the mint address.
    address: Mapped[str | None] = mapped_column(String(128), nullable=True)

    decimals: Mapped[int] = mapped_column(Integer, nullable=False)

    label: Mapped[str | None] = mapped_column(String(64), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("chain", "venue", "symbol", name="uq_token_registry_chain_venue_symbol"),
        Index("ix_token_registry_chain_symbol", "chain", "symbol"),
        Index("ix_token_registry_chain_venue", "chain", "venue"),
        Index("ix_token_registry_chain_address", "chain", "address"),
    )


# =============================================================================
# NEW (additive): Deposits + Basis Lots
# =============================================================================

class AssetDeposit(Base):
    """
    Captures inbound crypto deposits (or opening inventory entries) per venue/wallet.

    NOTE: For now, wallet_id defaults to "default" because the current UTT schema
    does not yet expose a stable per-account identifier on balances/fills.
    We will evolve wallet_id later without breaking existing behavior.
    """
    __tablename__ = "asset_deposits"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    venue: Mapped[str] = mapped_column(String(16), nullable=False)
    wallet_id: Mapped[str] = mapped_column(String(64), nullable=False, default="default")

    asset: Mapped[str] = mapped_column(String(16), nullable=False)
    qty: Mapped[float] = mapped_column(Float, nullable=False)

    deposit_time: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    txid: Mapped[str | None] = mapped_column(String(128), nullable=True)
    network: Mapped[str | None] = mapped_column(String(32), nullable=True)

    # DETECTED / CONFIRMED / MANUAL / IGNORED
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="MANUAL")

    # API / UI_MANUAL / MANUAL_IMPORT
    source: Mapped[str] = mapped_column(String(16), nullable=False, default="UI_MANUAL")

    note: Mapped[str | None] = mapped_column(String(256), nullable=True)

    # Transfer-linking (match-only path) — persisted column for D2 acceptance.
    transfer_withdrawal_id: Mapped[str | None] = mapped_column(String(36), nullable=True)

    # Raw JSON payload for forward-compat / schema drift hardening (stores transfer ids, slices, etc.)
    raw: Mapped[dict | None] = mapped_column(JSON, nullable=True)


    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        # txid can be null; SQLite allows multiple NULLs even under UNIQUE.
        UniqueConstraint("venue", "wallet_id", "txid", name="uq_deposits_venue_wallet_txid"),
        Index("ix_deposits_venue_wallet_time", "venue", "wallet_id", "deposit_time"),
        Index("ix_deposits_asset_time", "asset", "deposit_time"),
        Index("ix_deposits_status_time", "status", "deposit_time"),
    )


class BasisLot(Base):
    """
    Lots used by FIFO cost-basis / realized P&L engine.

    A deposit creates a lot (basis may be missing).
    A buy fill will also create a lot later (basis known).
    """
    __tablename__ = "basis_lots"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    venue: Mapped[str] = mapped_column(String(16), nullable=False)
    wallet_id: Mapped[str] = mapped_column(String(64), nullable=False, default="default")
    asset: Mapped[str] = mapped_column(String(16), nullable=False)

    acquired_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    qty_total: Mapped[float] = mapped_column(Float, nullable=False)
    qty_remaining: Mapped[float] = mapped_column(Float, nullable=False)

    total_basis_usd: Mapped[float | None] = mapped_column(Float, nullable=True)

    basis_is_missing: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    # BUY_FILL / DEPOSIT / MANUAL_EDIT / IMPLIED_ZERO
    basis_source: Mapped[str] = mapped_column(String(16), nullable=False, default="DEPOSIT")

    # BUY / DEPOSIT / ADJUSTMENT
    origin_type: Mapped[str] = mapped_column(String(16), nullable=False, default="DEPOSIT")

    # deposit_id (uuid) or fill_id later
    origin_ref: Mapped[str | None] = mapped_column(String(36), nullable=True)

    note: Mapped[str | None] = mapped_column(String(256), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_lots_venue_wallet_asset_acq", "venue", "wallet_id", "asset", "acquired_at"),
        Index("ix_lots_missing", "venue", "wallet_id", "basis_is_missing", "acquired_at"),
        Index("ix_lots_origin", "origin_type", "origin_ref"),
    )


# =============================================================================
# NEW (additive): Withdrawals
# =============================================================================

class AssetWithdrawal(Base):
    """
    Captures outbound crypto withdrawals (asset movements out of a venue/wallet).

    This is not a taxable sale. Later we will optionally consume FIFO lots as TRANSFER_OUT.
    """
    __tablename__ = "asset_withdrawals"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    venue: Mapped[str] = mapped_column(String(16), nullable=False)
    wallet_id: Mapped[str] = mapped_column(String(64), nullable=False, default="default")

    asset: Mapped[str] = mapped_column(String(16), nullable=False)
    qty: Mapped[float] = mapped_column(Float, nullable=False)

    withdraw_time: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    txid: Mapped[str | None] = mapped_column(String(128), nullable=True)
    chain: Mapped[str | None] = mapped_column(String(32), nullable=True)
    network: Mapped[str | None] = mapped_column(String(32), nullable=True)

    # DETECTED / CONFIRMED / MANUAL / IGNORED
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="MANUAL")

    # API / UI_MANUAL / MANUAL_IMPORT
    source: Mapped[str] = mapped_column(String(16), nullable=False, default="UI_MANUAL")

    destination: Mapped[str | None] = mapped_column(String(256), nullable=True)
    note: Mapped[str | None] = mapped_column(String(256), nullable=True)

    raw: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        # txid can be null; SQLite allows multiple NULLs even under UNIQUE.
        UniqueConstraint("venue", "wallet_id", "txid", name="uq_withdrawals_venue_wallet_txid"),
        Index("ix_withdrawals_venue_wallet_time", "venue", "wallet_id", "withdraw_time"),
        Index("ix_withdrawals_asset_time", "asset", "withdraw_time"),
        Index("ix_withdrawals_status_time", "status", "withdraw_time"),
    )


# ---------------------------------------------------------------------------
# Custom on-chain wallet addresses + snapshots (Track 5)
# ---------------------------------------------------------------------------

class WalletAddress(Base):
    __tablename__ = "wallet_addresses"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    # e.g., BTC, DOGE, DOT
    asset: Mapped[str] = mapped_column(String(16), nullable=False, index=True)

    # keep flexible: BTC, DOGE, DOT (later: BTC-ORD, DOGE-DOGINALS, DOT-AH, etc.)
    network: Mapped[str] = mapped_column(String(32), nullable=False, index=True)

    # Optional venue/account grouping for this address (e.g. "robinhood", "dex-trade").
    # When NULL => treat as self-custody.
    wallet_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)

    address: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    label: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # tenant / user scope
    owner_scope: Mapped[str] = mapped_column(String(64), nullable=False, default="default", index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("owner_scope", "network", "address", name="uq_wallet_addr_scope_network_address"),
        Index("ix_wallet_addresses_asset_network", "asset", "network"),
    )


class WalletAddressSnapshot(Base):
    __tablename__ = "wallet_address_snapshots"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    wallet_address_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("wallet_addresses.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    asset: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    network: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    address: Mapped[str] = mapped_column(String(256), nullable=False, index=True)

    # v1: Float is fine; if you want exact later we can migrate to Decimal/Numeric
    balance_qty: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    # raw provider response (JSON)
    balance_raw: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    source: Mapped[str] = mapped_column(String(32), nullable=False, default="explorer")
    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    wallet_address: Mapped["WalletAddress"] = relationship("WalletAddress")

    __table_args__ = (
        Index("ix_wallet_addr_snap_latest", "wallet_address_id", "fetched_at"),
        Index("ix_wallet_addr_snap_addr_time", "network", "address", "fetched_at"),
    )



class WalletAddressTx(Base):
    """Cached on-chain tx rows discovered for a wallet address.

    Idempotency anchor: (wallet_address_id, txid, direction).
    """
    __tablename__ = "wallet_address_txs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    wallet_address_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("wallet_addresses.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    asset: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    network: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    address: Mapped[str] = mapped_column(String(256), nullable=False, index=True)

    txid: Mapped[str] = mapped_column(String(128), nullable=False)
    direction: Mapped[str] = mapped_column(String(8), nullable=False)  # in|out

    amount: Mapped[float] = mapped_column(Float, nullable=False)
    fee: Mapped[float | None] = mapped_column(Float, nullable=True)

    tx_time: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    counterparty: Mapped[str | None] = mapped_column(String(256), nullable=True)

    raw: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    ingested_to_ledger_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    deposit_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    withdrawal_id: Mapped[str | None] = mapped_column(String(36), nullable=True)

    wallet_address: Mapped["WalletAddress"] = relationship("WalletAddress")

    __table_args__ = (
        UniqueConstraint("wallet_address_id", "txid", "direction", name="uq_wallet_addr_tx"),
        Index("ix_wallet_addr_tx_time", "wallet_address_id", "tx_time"),
        Index("ix_wallet_addr_tx_txid", "txid"),
    )


class RuntimeSetting(Base):
    __tablename__ = "runtime_settings"

    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    value_json: Mapped[dict | list | str | int | float | bool | None] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


Index("ix_runtime_settings_updated_at", RuntimeSetting.updated_at)

# =============================================================================
# Bridge / Transfer Records (planning-only linkage)
# =============================================================================

class BridgeTransferRecord(Base):
    """
    Planning/linkage record for intentional cross-chain asset movement.

    This table is intentionally diagnostic-first:
      - it does not execute bridge transactions
      - it does not mutate FIFO lots by itself
      - it links source-side outflow evidence to destination-side inflow evidence
        so a later reconciliation pass can preserve basis across chains.
    """
    __tablename__ = "bridge_transfer_records"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    asset: Mapped[str] = mapped_column(String(16), nullable=False, default="UTTT")
    amount: Mapped[float] = mapped_column(Float, nullable=False)

    source_chain: Mapped[str] = mapped_column(String(32), nullable=False)
    destination_chain: Mapped[str] = mapped_column(String(32), nullable=False)

    source_wallet_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    destination_wallet_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    source_address: Mapped[str | None] = mapped_column(String(256), nullable=True)
    destination_address: Mapped[str | None] = mapped_column(String(256), nullable=True)

    source_txid: Mapped[str | None] = mapped_column(String(128), nullable=True)
    destination_txid: Mapped[str | None] = mapped_column(String(128), nullable=True)

    # PLANNED / SOURCE_SENT / DESTINATION_RECEIVED / LINKED / RECONCILED / CANCELLED
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="PLANNED")

    # manual / treasury_mediated / burn_mint / lock_release / xcm_transfer / external_bridge
    bridge_mechanism: Mapped[str] = mapped_column(String(32), nullable=False, default="manual")

    source_withdrawal_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    destination_deposit_id: Mapped[str | None] = mapped_column(String(36), nullable=True)

    note: Mapped[str | None] = mapped_column(String(256), nullable=True)
    raw: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_bridge_transfer_asset_status", "asset", "status", "created_at"),
        Index("ix_bridge_transfer_source", "source_chain", "source_txid"),
        Index("ix_bridge_transfer_destination", "destination_chain", "destination_txid"),
        Index("ix_bridge_transfer_link_refs", "source_withdrawal_id", "destination_deposit_id"),
    )


# =============================================================================
# Robinhood Chain execution lifecycle (dedicated EVM swap records)
# =============================================================================

class RobinhoodChainExecution(Base):
    """Dedicated lifecycle record for explicit MetaMask-submitted RH Chain swaps.

    This table is intentionally separate from Order and VenueOrderRow because a
    full EVM transaction hash is 66 characters with the 0x prefix and because
    wallet-submitted swaps have a distinct prepared/claimed/pending lifecycle.
    It does not mutate ledger, FIFO, or basis state.
    """
    __tablename__ = "robinhood_chain_executions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)

    chain_id: Mapped[int] = mapped_column(Integer, nullable=False, default=4663)
    wallet_address: Mapped[str] = mapped_column(String(42), nullable=False, index=True)

    symbol: Mapped[str] = mapped_column(String(32), nullable=False, default="ETH-USDG")
    side: Mapped[str] = mapped_column(String(8), nullable=False, default="sell")
    input_asset: Mapped[str] = mapped_column(String(16), nullable=False, default="ETH")
    input_amount: Mapped[str] = mapped_column(String(64), nullable=False)
    input_amount_atomic: Mapped[str] = mapped_column(String(80), nullable=False)

    expected_output_asset: Mapped[str] = mapped_column(String(16), nullable=False, default="USDG")
    expected_output_amount: Mapped[str] = mapped_column(String(64), nullable=False)
    minimum_output_amount: Mapped[str] = mapped_column(String(64), nullable=False)
    slippage_bps: Mapped[int] = mapped_column(Integer, nullable=False)

    quote_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    plan_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    plan_fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    plan_expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    transaction_to: Mapped[str] = mapped_column(String(42), nullable=False)
    transaction_value_wei: Mapped[str] = mapped_column(String(80), nullable=False)
    calldata_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    calldata_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    gas_limit: Mapped[str] = mapped_column(String(80), nullable=False)
    gas_price_wei: Mapped[str] = mapped_column(String(80), nullable=False)
    route: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # prepared -> send_claimed -> pending -> confirmed/reverted
    # Explicit pre-submission failures terminate as wallet_rejected/submission_failed.
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="prepared", index=True)
    send_claim_id: Mapped[str | None] = mapped_column(String(64), nullable=True, unique=True)
    send_claimed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    submission_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    submission_failure_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    tx_hash: Mapped[str | None] = mapped_column(String(66), nullable=True, unique=True)
    submitted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_receipt_check_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    reverted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    block_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    gas_used: Mapped[str | None] = mapped_column(String(80), nullable=True)
    effective_gas_price_wei: Mapped[str | None] = mapped_column(String(80), nullable=True)
    receipt_status: Mapped[int | None] = mapped_column(Integer, nullable=True)

    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(512), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_rh_chain_exec_status_created", "status", "created_at"),
        Index("ix_rh_chain_exec_wallet_created", "wallet_address", "created_at"),
        Index("ix_rh_chain_exec_tx_hash", "tx_hash"),
    )


# =============================================================================
# Robinhood Chain exact-output BUY lifecycle (bounded approval + swap)
# =============================================================================

class RobinhoodChainBuyExecution(Base):
    """Dedicated two-transaction lifecycle for RH-CHAIN.10D.2 exact-output BUY.

    Approval and swap hashes are retained separately. The browser wallet remains
    the only transaction sender; this record does not mutate ledger, FIFO, or
    basis state.
    """
    __tablename__ = "robinhood_chain_buy_executions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    chain_id: Mapped[int] = mapped_column(Integer, nullable=False, default=4663)
    wallet_address: Mapped[str] = mapped_column(String(42), nullable=False, index=True)

    symbol: Mapped[str] = mapped_column(String(32), nullable=False, default="ETH-USDG")
    side: Mapped[str] = mapped_column(String(8), nullable=False, default="buy")
    exact_output_asset: Mapped[str] = mapped_column(String(16), nullable=False, default="ETH")
    exact_output_amount: Mapped[str] = mapped_column(String(64), nullable=False, default="0.001")
    exact_output_amount_atomic: Mapped[str] = mapped_column(String(80), nullable=False, default="1000000000000000")
    maximum_input_asset: Mapped[str] = mapped_column(String(16), nullable=False, default="USDG")
    maximum_input_amount: Mapped[str] = mapped_column(String(64), nullable=False, default="2")
    maximum_input_amount_atomic: Mapped[str] = mapped_column(String(80), nullable=False, default="2000000")
    slippage_bps: Mapped[int] = mapped_column(Integer, nullable=False, default=100)

    approval_token_address: Mapped[str] = mapped_column(String(42), nullable=False)
    approval_spender: Mapped[str] = mapped_column(String(42), nullable=False)
    approval_amount: Mapped[str] = mapped_column(String(64), nullable=False, default="2")
    approval_amount_atomic: Mapped[str] = mapped_column(String(80), nullable=False, default="2000000")
    allowance_before_atomic: Mapped[str] = mapped_column(String(80), nullable=False, default="0")
    allowance_confirmed_atomic: Mapped[str | None] = mapped_column(String(80), nullable=True)
    allowance_confirmed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    approval_plan_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    approval_transaction_to: Mapped[str] = mapped_column(String(42), nullable=False)
    approval_transaction_value_wei: Mapped[str] = mapped_column(String(80), nullable=False, default="0")
    approval_calldata_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    approval_calldata_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    approval_gas_limit: Mapped[str] = mapped_column(String(80), nullable=False)
    approval_gas_price_wei: Mapped[str] = mapped_column(String(80), nullable=False)

    status: Mapped[str] = mapped_column(String(40), nullable=False, default="approval_prepared", index=True)
    approval_send_claim_id: Mapped[str | None] = mapped_column(String(64), nullable=True, unique=True)
    approval_send_claimed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    approval_submission_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    approval_submission_failure_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    approval_tx_hash: Mapped[str | None] = mapped_column(String(66), nullable=True, unique=True)
    approval_submitted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    approval_last_receipt_check_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    approval_confirmed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    approval_reverted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    approval_block_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    approval_gas_used: Mapped[str | None] = mapped_column(String(80), nullable=True)
    approval_effective_gas_price_wei: Mapped[str | None] = mapped_column(String(80), nullable=True)
    approval_receipt_status: Mapped[int | None] = mapped_column(Integer, nullable=True)

    swap_quote_id: Mapped[str | None] = mapped_column(String(64), nullable=True, unique=True)
    expected_input_amount: Mapped[str | None] = mapped_column(String(64), nullable=True)
    expected_input_amount_atomic: Mapped[str | None] = mapped_column(String(80), nullable=True)
    swap_plan_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    swap_plan_fetched_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    swap_plan_expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    swap_transaction_to: Mapped[str | None] = mapped_column(String(42), nullable=True)
    swap_transaction_value_wei: Mapped[str | None] = mapped_column(String(80), nullable=True)
    swap_calldata_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    swap_calldata_bytes: Mapped[int | None] = mapped_column(Integer, nullable=True)
    swap_gas_limit: Mapped[str | None] = mapped_column(String(80), nullable=True)
    swap_gas_price_wei: Mapped[str | None] = mapped_column(String(80), nullable=True)
    route: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    swap_send_claim_id: Mapped[str | None] = mapped_column(String(64), nullable=True, unique=True)
    swap_send_claimed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    swap_submission_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    swap_submission_failure_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    swap_tx_hash: Mapped[str | None] = mapped_column(String(66), nullable=True, unique=True)
    swap_submitted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    swap_last_receipt_check_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    swap_confirmed_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    swap_reverted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    swap_block_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    swap_gas_used: Mapped[str | None] = mapped_column(String(80), nullable=True)
    swap_effective_gas_price_wei: Mapped[str | None] = mapped_column(String(80), nullable=True)
    swap_receipt_status: Mapped[int | None] = mapped_column(Integer, nullable=True)

    error_code: Mapped[str | None] = mapped_column(String(80), nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_rh_chain_buy_status_created", "status", "created_at"),
        Index("ix_rh_chain_buy_wallet_created", "wallet_address", "created_at"),
        Index("ix_rh_chain_buy_approval_hash", "approval_tx_hash"),
        Index("ix_rh_chain_buy_swap_hash", "swap_tx_hash"),
    )


# =============================================================================
# Robinhood Chain generalized exact-spend lifecycle (review-only R5A)
# =============================================================================

class RobinhoodChainSwapExecution(Base):
    """Plan-bound review record for generalized Robinhood Chain exact-spend swaps.

    R5A persists the finite approval and unsigned swap identities without
    claiming, signing, broadcasting, or mutating ledger/FIFO/basis state.
    """
    __tablename__ = "robinhood_chain_swap_executions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    chain_id: Mapped[int] = mapped_column(Integer, nullable=False, default=4663)
    wallet_address: Mapped[str] = mapped_column(String(42), nullable=False, index=True)
    provider: Mapped[str] = mapped_column(String(16), nullable=False, default="0x")
    symbol: Mapped[str] = mapped_column(String(32), nullable=False, default="ETH-USDG")
    side: Mapped[str] = mapped_column(String(8), nullable=False, default="buy")

    from_asset: Mapped[str] = mapped_column(String(32), nullable=False)
    from_contract_address: Mapped[str] = mapped_column(String(42), nullable=False)
    from_decimals: Mapped[int] = mapped_column(Integer, nullable=False)
    from_native: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    to_asset: Mapped[str] = mapped_column(String(32), nullable=False)
    to_contract_address: Mapped[str] = mapped_column(String(42), nullable=False)
    to_decimals: Mapped[int] = mapped_column(Integer, nullable=False)
    to_native: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    amount_mode: Mapped[str] = mapped_column(String(24), nullable=False, default="exact_input")
    exact_input_amount: Mapped[str] = mapped_column(String(64), nullable=False)
    exact_input_amount_atomic: Mapped[str] = mapped_column(String(80), nullable=False)
    expected_output_amount: Mapped[str] = mapped_column(String(64), nullable=False)
    expected_output_amount_atomic: Mapped[str] = mapped_column(String(80), nullable=False)
    minimum_output_amount: Mapped[str] = mapped_column(String(64), nullable=False)
    minimum_output_amount_atomic: Mapped[str] = mapped_column(String(80), nullable=False)
    slippage_bps: Mapped[int] = mapped_column(Integer, nullable=False, default=100)

    quote_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    plan_fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    plan_expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    allowance_read_method: Mapped[str] = mapped_column(String(24), nullable=False, default="eth_call")
    allowance_token_address: Mapped[str] = mapped_column(String(42), nullable=False)
    allowance_spender: Mapped[str] = mapped_column(String(42), nullable=False)
    allowance_current_atomic: Mapped[str] = mapped_column(String(80), nullable=False, default="0")
    allowance_required_atomic: Mapped[str] = mapped_column(String(80), nullable=False)
    allowance_shortfall_atomic: Mapped[str] = mapped_column(String(80), nullable=False, default="0")
    approval_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    approval_amount: Mapped[str] = mapped_column(String(64), nullable=False)
    approval_amount_atomic: Mapped[str] = mapped_column(String(80), nullable=False)
    approval_plan_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    approval_transaction_to: Mapped[str] = mapped_column(String(42), nullable=False)
    approval_transaction_value_wei: Mapped[str] = mapped_column(String(80), nullable=False, default="0")
    approval_calldata_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    approval_calldata_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    approval_gas_limit: Mapped[str] = mapped_column(String(80), nullable=False)
    approval_gas_price_wei: Mapped[str] = mapped_column(String(80), nullable=False)
    approval_status: Mapped[str] = mapped_column(String(32), nullable=False, default="prepared")
    approval_tx_hash: Mapped[str | None] = mapped_column(String(66), nullable=True, unique=True)

    swap_plan_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    swap_transaction_to: Mapped[str] = mapped_column(String(42), nullable=False)
    swap_transaction_value_wei: Mapped[str] = mapped_column(String(80), nullable=False, default="0")
    swap_calldata_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    swap_calldata_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    swap_gas_limit: Mapped[str] = mapped_column(String(80), nullable=False)
    swap_gas_price_wei: Mapped[str] = mapped_column(String(80), nullable=False)
    swap_status: Mapped[str] = mapped_column(String(32), nullable=False, default="review_only")
    swap_tx_hash: Mapped[str | None] = mapped_column(String(66), nullable=True, unique=True)
    route: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    status: Mapped[str] = mapped_column(String(40), nullable=False, default="approval_prepared", index=True)
    error_code: Mapped[str | None] = mapped_column(String(80), nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_rh_chain_swap_status_created", "status", "created_at"),
        Index("ix_rh_chain_swap_wallet_created", "wallet_address", "created_at"),
        Index("ix_rh_chain_swap_pair_mode", "from_asset", "to_asset", "amount_mode"),
        Index("ix_rh_chain_swap_approval_hash", "approval_tx_hash"),
        Index("ix_rh_chain_swap_tx_hash", "swap_tx_hash"),
    )
