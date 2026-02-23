import os

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, DeclarativeBase

from .config import settings


class Base(DeclarativeBase):
    pass


def _sqlite_url() -> str:
    # Ensure directory exists
    path = settings.sqlite_path
    # path is relative to backend/ (recommended)
    dir_ = os.path.dirname(path)
    if dir_:
        os.makedirs(dir_, exist_ok=True)
    return f"sqlite:///{path}"


engine = create_engine(
    _sqlite_url(),
    connect_args={
        "check_same_thread": False,
        # Driver-level busy timeout (seconds). This is helpful, but NOT sufficient by itself.
        # We also set PRAGMA busy_timeout below (milliseconds) per-connection.
        "timeout": 30,
    },
    # Pool hardening: default (5 + 10 overflow) is too small once you have multiple windows/tabs + enrichment.
    pool_size=20,
    max_overflow=40,
    pool_timeout=10,
    pool_pre_ping=True,
    future=True,
)


@event.listens_for(engine, "connect")
def _set_sqlite_pragmas(dbapi_connection, _connection_record):
    """
    Improve SQLite concurrency characteristics for a UI that has multiple polling endpoints.
    These are safe no-ops if unsupported by the driver/environment.
    """
    try:
        cursor = dbapi_connection.cursor()

        # Critical for reducing writer contention.
        cursor.execute("PRAGMA journal_mode=WAL;")

        # Reasonable durability/perf tradeoff for dev.
        cursor.execute("PRAGMA synchronous=NORMAL;")

        # Critical: wait for locks instead of failing immediately.
        # (milliseconds; this is the PRAGMA-side busy timeout, distinct from connect_args["timeout"])
        cursor.execute("PRAGMA busy_timeout=30000;")

        # Enforce FK constraints.
        cursor.execute("PRAGMA foreign_keys=ON;")

        # Optional/mostly harmless dev ergonomics:
        # cursor.execute("PRAGMA temp_store=MEMORY;")
        # cursor.execute("PRAGMA wal_autocheckpoint=1000;")

        cursor.close()
    except Exception:
        # Do not fail startup if pragmas cannot be applied.
        pass


SessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
    # Critical: prevents ORM instances from expiring after commit/rollback, which can otherwise
    # trigger re-fetches (and re-checkout connections) during later serialization/enrichment.
    expire_on_commit=False,
    future=True,
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    """
    Ensure all SQLAlchemy models are imported (registered on Base.metadata),
    then create tables (SQLite MVP convenience).
    """
    # Import model modules so their tables register on Base.metadata.
    # NOTE: app/models.py is a module (not a package), so do NOT import app.models.<submodule>.
    from . import models  # noqa: F401
    from . import discovery_models  # noqa: F401

    Base.metadata.create_all(bind=engine)
