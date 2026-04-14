"""
OpenDuck Python client examples.

Prerequisites:
    pip install -e clients/python
    export OPENDUCK_TOKEN=your-token

    # Start the OpenDuck service:
    cargo run -p openduck -- serve -d mydb -t your-token

    # Then run:
    python examples/client_python.py
"""

import openduck


# ── 1. Simple connect ────────────────────────────────────────────────────────
# Uses OPENDUCK_TOKEN and OPENDUCK_ENDPOINT env vars.

con = openduck.connect("mydb")
con.sql("SELECT * FROM users LIMIT 10").show()


# ── 2. URI shorthand ─────────────────────────────────────────────────────────

con = openduck.connect("od:mydb")
con.sql("SELECT count(*) FROM events").show()


# ── 3. Explicit token + endpoint ─────────────────────────────────────────────

con = openduck.connect("analytics", token="my-token", endpoint="http://localhost:7878")
con.sql("SHOW TABLES").show()


# ── 4. Full URI with inline credentials ──────────────────────────────────────

con = openduck.connect(
    "openduck:staging_db?endpoint=http://localhost:7878&token=dev-token"
)
con.sql("SELECT * FROM logs LIMIT 5").show()


# ── 5. Fetch results ─────────────────────────────────────────────────────────

con = openduck.connect("mydb")

# As a list of tuples
rows = con.sql("SELECT id, name FROM users LIMIT 5").fetchall()
for row in rows:
    print(f"  id={row[0]}, name={row[1]}")

# As a pandas DataFrame
df = con.sql("SELECT * FROM events WHERE ts > '2025-01-01'").fetchdf()
print(df.head())

# As an Apache Arrow table
arrow_table = con.sql("SELECT * FROM events LIMIT 1000").arrow()
print(f"  Arrow schema: {arrow_table.schema}")


# ── 6. Aggregation + filtering ───────────────────────────────────────────────

con = openduck.connect("analytics")
con.sql("""
    SELECT
        department,
        count(*) AS headcount,
        avg(salary) AS avg_salary
    FROM employees
    WHERE active = true
    GROUP BY department
    ORDER BY headcount DESC
""").show()


# ── 7. Hybrid: local CSV + remote table ─────────────────────────────────────

con = openduck.connect("warehouse")

# Load a local CSV into a temporary table
con.raw.execute("CREATE TABLE local_csv AS SELECT * FROM read_csv('data/orders.csv');")

# Join with a remote table
con.raw.sql("""
    SELECT o.order_id, o.amount, c.name, c.email
    FROM local_csv o
    JOIN cloud.customers c ON o.customer_id = c.id
    ORDER BY o.amount DESC
    LIMIT 20
""").show()


# ── 8. Hybrid: local DataFrame + remote table ───────────────────────────────

import pandas as pd

con = openduck.connect("warehouse")

local_df = pd.DataFrame({
    "product_id": [1, 2, 3, 4, 5],
    "product_name": ["Widget", "Gadget", "Doohickey", "Thingamajig", "Whatsit"],
})

# Register the DataFrame as a DuckDB table
con.raw.execute("CREATE TABLE local_products AS SELECT * FROM local_df")

# Join local DataFrame with remote sales data
con.raw.sql("""
    SELECT
        p.product_name,
        sum(s.quantity) AS total_sold,
        sum(s.revenue) AS total_revenue
    FROM local_products p
    JOIN cloud.sales s ON p.product_id = s.product_id
    GROUP BY p.product_name
    ORDER BY total_revenue DESC
""").show()


# ── 9. Direct DuckDB — no wrapper needed ────────────────────────────────────

import duckdb

raw = duckdb.connect(config={"allow_unsigned_extensions": "true"})
raw.execute("LOAD 'openduck';")
raw.execute(
    "ATTACH 'openduck:mydb?endpoint=http://localhost:7878&token=xxx' AS cloud;"
)
raw.sql("SELECT * FROM cloud.users LIMIT 10").show()


# ── 10. Context manager ─────────────────────────────────────────────────────

with openduck.connect("warehouse") as con:
    con.sql("SELECT * FROM events LIMIT 10").show()
# connection closed automatically


# ── 11. Multiple remote databases ───────────────────────────────────────────

prod = openduck.connect(
    "production",
    token="prod-token",
    endpoint="http://prod:7878",
    alias="prod",
)
staging = openduck.connect(
    "staging",
    token="staging-token",
    endpoint="http://staging:7878",
    alias="staging",
)

prod_count = prod.sql("SELECT count(*) FROM users").fetchone()[0]
staging_count = staging.sql("SELECT count(*) FROM users").fetchone()[0]
print(f"  Production users: {prod_count}, Staging users: {staging_count}")

prod.close()
staging.close()


print("\nAll examples complete.")
