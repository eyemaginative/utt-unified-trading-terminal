import asyncio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import Response

from .config import settings
from .db import init_db

# IMPORTANT: ensure model modules are imported so tables register on Base.metadata
from . import models  # noqa: F401

# NEW: lot journal model (so lot_journal table gets created by create_all)
from . import models_lot_journal  # noqa: F401

# IMPORTANT: discovery models must also be imported or their tables won't be created by create_all()
# This must happen before Base.metadata.create_all(bind=engine)
from . import discovery_models  # noqa: F401

from .routers.health import router as health_router
from .routers.orders import router as orders_router
from .routers.balances import router as balances_router
from .routers.symbols import router as symbols_router
from .routers.market import router as market_router
from .routers.arm import router as arm_router
from .routers.venue_orders import router as venue_orders_router
from .routers.all_orders import router as all_orders_router
from .routers.order_views import router as order_views_router
from .routers.trade import router as trade_router
from .routers.venues import router as venues_router
from .routers import scanners
from .routers.market_intel import router as market_intel_router

# NEW: rules router (/api/rules/order)
from .routers.rules import router as rules_router

# IMPORTANT: keep deposits + withdrawals routers both included
from .routers.deposits import router as deposits_router
from .routers.withdrawals import router as withdrawals_router

# NEW: ledger router (/api/ledger/sync)
from .routers.ledger import router as ledger_router

# NEW: on-chain wallet addresses + snapshots (Track 5)
try:
    from .routers.wallet_addresses import router as wallet_addresses_router
except Exception as e:
    print("wallet_addresses router import failed:", repr(e))
    wallet_addresses_router = None

# NEW: solana dex router (/api/solana_dex/*)
try:
    from .routers.solana_dex import router as solana_dex_router
except Exception as e:
    print("solana_dex router import failed:", repr(e))
    solana_dex_router = None

# Optional: symbol view confirmation router (only if file exists)
try:
    from .routers.symbol_views import router as symbol_views_router
except Exception:
    symbol_views_router = None


def create_app() -> FastAPI:
    app = FastAPI(title="Unified Trading Terminal (Local)")

    # Treat request cancellations (e.g., Ctrl+C during active requests) as "Client Closed Request"
    # Use middleware (not @exception_handler) to avoid Starlette's Exception-subclass assertion.
    @app.middleware("http")
    async def cancelled_to_499(request, call_next):
        try:
            return await call_next(request)
        except asyncio.CancelledError:
            # 499 is a common convention (nginx) for client disconnect/cancel.
            return Response(status_code=499)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_list(),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Create DB tables on startup (MVP convenience)
    # NOTE: init_db() imports any required model modules and calls Base.metadata.create_all().
    init_db()

    app.include_router(health_router)
    app.include_router(symbols_router)
    app.include_router(balances_router)

    # Track 5 (wallet addresses)
    if wallet_addresses_router is not None:
        app.include_router(wallet_addresses_router)

    # Solana DEX (reads first)
    if solana_dex_router is not None:
        app.include_router(solana_dex_router)

    app.include_router(orders_router)
    app.include_router(market_router)
    app.include_router(arm_router)
    app.include_router(venue_orders_router)
    app.include_router(all_orders_router)
    app.include_router(order_views_router)
    app.include_router(trade_router)
    app.include_router(venues_router)
    app.include_router(scanners.router)
    app.include_router(market_intel_router)

    # Rules
    app.include_router(rules_router)

    # Deposits / Withdrawals
    app.include_router(deposits_router)
    app.include_router(withdrawals_router)

    # Manual ledger sync (3.1/3.2)
    app.include_router(ledger_router)

    if symbol_views_router is not None:
        app.include_router(symbol_views_router)

    return app


app = create_app()
