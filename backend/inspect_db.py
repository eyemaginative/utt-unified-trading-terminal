import sqlite3, pathlib

p = pathlib.Path("data/app.db")
conn = sqlite3.connect(p)
cur = conn.cursor()

print("DB:", p.resolve())

def list_tables():
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    return [r[0] for r in cur.fetchall()]

def table_columns(tbl: str):
    cur.execute(f"PRAGMA table_info({tbl})")
    # rows: cid, name, type, notnull, dflt_value, pk
    return [r[1] for r in cur.fetchall()]

def print_table_sample(tbl: str, cols_wanted):
    cols = table_columns(tbl)
    use_cols = [c for c in cols_wanted if c in cols]
    if not use_cols:
        print(f"\n== {tbl} == (no matching columns; has: {cols})")
        return

    # Build ORDER BY using the best available timestamp columns
    order_cols = [c for c in ["captured_at", "updated_at", "created_at"] if c in cols]
    if order_cols:
        order_expr = "COALESCE(" + ", ".join(order_cols) + ")"
    else:
        order_expr = use_cols[0]

    q = f"SELECT {', '.join(use_cols)} FROM {tbl} ORDER BY {order_expr} DESC LIMIT 8"
    print(f"\n== {tbl} ==")
    print("cols:", use_cols)
    cur.execute(q)
    for r in cur.fetchall():
        print(r)

tables = list_tables()
print("\nTables:", tables)

# Print local orders sample (schema-aware)
if "orders" in tables:
    print_table_sample(
        "orders",
        ["id", "venue", "venue_order_id", "status", "created_at", "updated_at", "captured_at"],
    )

# Attempt to locate the venue ingestion table by column signatures
# We look for a table that has venue + venue_order_id + captured_at (or at least created_at/updated_at).
candidate_tables = []
for t in tables:
    cols = set(table_columns(t))
    if {"venue", "venue_order_id"}.issubset(cols) and (("captured_at" in cols) or ("updated_at" in cols) or ("created_at" in cols)):
        candidate_tables.append(t)

print("\nVenue-table candidates:", candidate_tables)

# Print sample rows for each candidate
for t in candidate_tables:
    print_table_sample(
        t,
        ["id", "venue", "venue_order_id", "status", "created_at", "updated_at", "captured_at"],
    )

conn.close()
