# OpenDuck Python Client

A thin wrapper around DuckDB's Python package that loads the OpenDuck extension and attaches a remote database in one call. Tables resolve transparently — `SELECT * FROM users` just works.

## Install

```bash
pip install -e clients/python
```

## Quick start

```python
import openduck

con = openduck.connect("mydb", token="my-token")
con.sql("SELECT * FROM users").show()
```

## Connection methods

### Environment variables (recommended for production)

```bash
export OPENDUCK_TOKEN=my-token
export OPENDUCK_ENDPOINT=http://my-service:7878
```

```python
import openduck

con = openduck.connect("mydb")
con.sql("SELECT * FROM users LIMIT 10").show()
```

### URI with inline credentials

```python
con = openduck.connect("openduck:mydb?endpoint=http://localhost:7878&token=my-token")
```

### Short alias

```python
con = openduck.connect("od:mydb")
```

### Explicit keyword arguments

```python
con = openduck.connect(
    "analytics",
    token="my-token",
    endpoint="http://localhost:7878",
)
```

## Querying

The default database is set to the attached remote, so table names resolve transparently:

```python
con = openduck.connect("mydb", token="my-token")

# .sql() returns a DuckDB relation — call .show(), .fetchall(), .fetchdf(), etc.
con.sql("SELECT * FROM users WHERE active = true").show()
con.sql("SELECT department, count(*) FROM employees GROUP BY 1").fetchdf()

# .execute() for statements, then fetch
con.execute("SELECT sum(revenue) FROM sales")
total = con.fetchone()[0]
```

## Hybrid queries (local + remote)

Use `con.raw` to access the underlying DuckDB connection. Mix local and remote data in a single query:

```python
con = openduck.connect("warehouse", token="my-token")

# Create a local table
con.raw.execute("CREATE TABLE local_products (id INT, name VARCHAR);")
con.raw.execute("INSERT INTO local_products VALUES (1, 'Widget'), (2, 'Gadget');")

# Join local data with remote data
con.raw.sql("""
    SELECT p.name, s.revenue
    FROM local_products p
    JOIN cloud.sales s ON p.id = s.product_id
""").show()
```

## Loading local files alongside remote tables

```python
con = openduck.connect("mydb", token="my-token")

# Read a local CSV
con.raw.sql("CREATE TABLE local_csv AS SELECT * FROM read_csv('data/input.csv');")

# Join local CSV with a remote table
con.raw.sql("""
    SELECT c.*, u.email
    FROM local_csv c
    JOIN cloud.users u ON c.user_id = u.id
""").show()
```

## Pandas / Arrow integration

```python
import pandas as pd

con = openduck.connect("mydb", token="my-token")

# Remote query → pandas DataFrame
df = con.sql("SELECT * FROM events WHERE ts > '2025-01-01'").fetchdf()

# Remote query → Apache Arrow table
arrow_table = con.sql("SELECT * FROM events LIMIT 1000").arrow()

# Write a pandas DataFrame to a local table, then join with remote
con.raw.execute("CREATE TABLE local_df AS SELECT * FROM df")
con.raw.sql("""
    SELECT d.*, u.name
    FROM local_df d
    JOIN cloud.users u ON d.user_id = u.id
""").show()
```

## Multiple remote databases

```python
import openduck

prod = openduck.connect("production", token="prod-token", endpoint="http://prod:7878", alias="prod")
staging = openduck.connect("staging", token="staging-token", endpoint="http://staging:7878", alias="staging")

# Each connection has its own attached remote
prod.sql("SELECT count(*) FROM users").show()
staging.sql("SELECT count(*) FROM users").show()
```

## Context manager

```python
with openduck.connect("mydb", token="my-token") as con:
    con.sql("SELECT * FROM users").show()
# connection closed automatically
```

## Direct DuckDB (no wrapper)

You don't need the Python package at all — the extension works with plain `duckdb`:

```python
import duckdb

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute("LOAD 'openduck';")
con.execute("ATTACH 'openduck:mydb?endpoint=http://localhost:7878&token=xxx' AS cloud;")

con.sql("SELECT * FROM cloud.users").show()
```

## API reference

### `openduck.connect()`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `database` | `str` | `"default"` | Database name or URI (`openduck:...` / `od:...`) |
| `token` | `str` | `OPENDUCK_TOKEN` env | Access token |
| `endpoint` | `str` | `OPENDUCK_ENDPOINT` env or `http://127.0.0.1:7878` | Gateway endpoint |
| `extension_path` | `str` | auto-detected | Path to `.duckdb_extension` file |
| `alias` | `str` | `"cloud"` | SQL alias for the ATTACHed database |
| `**duckdb_config` | | | Extra config passed to `duckdb.connect()` |

### `OpenDuckConnection`

| Method | Returns | Description |
|--------|---------|-------------|
| `.sql(query)` | `DuckDBPyRelation` | Run SQL, returns a relation (`.show()`, `.fetchdf()`, `.arrow()`, etc.) |
| `.execute(query)` | `DuckDBPyConnection` | Execute SQL statement |
| `.fetchall()` | `list` | Fetch all rows from last query |
| `.fetchone()` | `tuple` | Fetch one row |
| `.fetchdf()` | `DataFrame` | Fetch as pandas DataFrame |
| `.close()` | `None` | Close the connection |
| `.raw` | `DuckDBPyConnection` | Access the underlying DuckDB connection |
| `.alias` | `str` | The SQL alias for the remote database |
| `.database` | `str` | The remote database name |
