from sqlalchemy import text
from app.db import engine

# SQLite: ALTER TABLE ADD COLUMN cannot use IF NOT EXISTS, so we try and ignore failures.
COLS = [
    ("source", "TEXT", "'local'"),
    ("source_name", "TEXT", "'LOCAL'"),
    ("external_order_id", "TEXT", "NULL"),
    ("raw_status", "TEXT", "NULL"),
    ("last_seen_at", "DATETIME", "NULL"),

    ("viewed_confirmed", "INTEGER", "0"),

    # Cached sortable execution summaries
    ("fee_total", "REAL", "NULL"),
    ("fee_asset", "TEXT", "NULL"),
    ("gross_total", "REAL", "NULL"),
    ("net_total_after_fee", "REAL", "NULL"),
]

INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS ix_orders_source_created ON orders(source_name, created_at)",
    "CREATE INDEX IF NOT EXISTS ix_orders_viewed_status ON orders(viewed_confirmed, status)",
    "CREATE INDEX IF NOT EXISTS ix_orders_external_id ON orders(external_order_id)",
]

def _try(sql: str) -> None:
    with engine.begin() as conn:
        try:
            conn.execute(text(sql))
        except Exception as e:
            # Ignore "duplicate column name" and similar, so reruns are safe.
            msg = str(e).lower()
            if "duplicate column name" in msg or "already exists" in msg:
                return
            raise

def main():
    # Add columns
    for name, typ, default in COLS:
        _try(f"ALTER TABLE orders ADD COLUMN {name} {typ} DEFAULT {default}")

    # Indexes
    for sql in INDEX_SQL:
        _try(sql)

    print("migrate_001_orders_expand: OK")

if __name__ == "__main__":
    main()
