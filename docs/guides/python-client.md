# Python client

The `openduck` Python package is a thin wrapper around DuckDB's Python bindings. It loads the OpenDuck extension, attaches a remote database, and gets out of your way. Tables resolve transparently — `con.sql("SELECT * FROM users")` just works.

You can absolutely use plain `duckdb` instead; the wrapper just removes a few lines of boilerplate.

## Install

```bash
pip install -e clients/python
```

The package depends on `duckdb >= 1.0`. The OpenDuck extension itself is **not** bundled — you point at a built `.duckdb_extension` file via `OPENDUCK_EXTENSION_PATH`, the wrapper auto-detects a build under `extensions/openduck/build/`, or you pass `extension_path=` explicitly.

## Quick start

```python
import openduck

con = openduck.connect("mydb", token="my-token")
con.sql("SELECT * FROM users").show()
```

## Connection methods

The wrapper accepts a database name plus a few overrides. Most users set `OPENDUCK_TOKEN` and `OPENDUCK_ENDPOINT` once and pass just the database name.

### Environment variables (recommended)

```bash
export OPENDUCK_TOKEN=my-token
export OPENDUCK_ENDPOINT=http://gateway:7878
```

```python
import openduck

con = openduck.connect("mydb")
con.sql("SELECT * FROM users LIMIT 10").show()
```

### Explicit keyword arguments

```python
con = openduck.connect(
    "analytics",
    token="my-token",
    endpoint="http://localhost:7878",
)
```

### URI form

```python
# Canonical
con = openduck.connect("openduck:mydb?endpoint=http://localhost:7878&token=my-token")

# Short alias
con = openduck.connect("od:mydb")
```

The URI is the same one used in raw `ATTACH 'openduck:...'` statements. See [DuckDB extension → URI format](duckdb-extension.md#uri-format).

## API reference

### `openduck.connect(...)`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `database` | `str` | `"default"` | Database name or URI (`openduck:...` / `od:...`). |
| `token` | `str` | `OPENDUCK_TOKEN` env | Access token. |
| `endpoint` | `str` | `OPENDUCK_ENDPOINT` env or `http://127.0.0.1:7878` | Gateway endpoint. |
| `extension_path` | `str` | auto-detected | Path to `.duckdb_extension` file. |
| `alias` | `str` | `"cloud"` | SQL alias used in the generated `ATTACH`. |
| `**duckdb_config` | | | Forwarded to `duckdb.connect()`. |

Returns an `OpenDuckConnection`.

### `OpenDuckConnection`

| Method | Returns | Description |
|--------|---------|-------------|
| `.sql(query)` | `DuckDBPyRelation` | Run SQL via the attached remote — call `.show()`, `.fetchdf()`, `.arrow()`, etc. |
| `.execute(query)` | `DuckDBPyConnection` | Execute a statement; chain `.fetchall()` / `.fetchone()` / `.fetchdf()`. |
| `.fetchall()` | `list` | Fetch all rows from the last `.execute()`. |
| `.fetchone()` | `tuple` | Fetch a single row. |
| `.fetchdf()` | `pandas.DataFrame` | Fetch as a DataFrame. |
| `.close()` | `None` | Close the connection (also a context manager). |
| `.raw` | `DuckDBPyConnection` | Underlying DuckDB connection — use for local tables, JOINs, file IO. |
| `.alias` | `str` | The SQL alias for the remote database. |
| `.database` | `str` | The remote database name. |

## Patterns

### `con.sql` vs `con.raw`

- **`con.sql(...)`** routes through the attached remote (the default schema). Use it for plain remote queries.
- **`con.raw`** is the underlying DuckDB connection. Use it for local tables, file IO, hybrid joins, and anything else DuckDB can do.

```python
con = openduck.connect("mydb", token="my-token")

# remote
con.sql("SELECT * FROM users WHERE active").show()

# local + hybrid
con.raw.execute("CREATE TABLE local.scores(id INT, pts INT);")
con.raw.execute("INSERT INTO local.scores VALUES (1, 10), (2, 20);")
con.raw.sql("""
    SELECT u.name, s.pts
    FROM cloud.users u
    JOIN local.scores s ON s.id = u.id
""").show()
```

### Pandas / Arrow

```python
import pandas as pd

con = openduck.connect("mydb", token="my-token")

df = con.sql("SELECT * FROM events WHERE ts > '2025-01-01'").fetchdf()
arrow_table = con.sql("SELECT * FROM events LIMIT 1000").arrow()

# Push a DataFrame into local DuckDB and join with remote data
con.raw.execute("CREATE TABLE local_df AS SELECT * FROM df")
con.raw.sql("""
    SELECT d.*, u.name
    FROM local_df d
    JOIN cloud.users u ON d.user_id = u.id
""").show()
```

### Loading a CSV alongside a remote table

```python
con = openduck.connect("mydb", token="my-token")
con.raw.sql("CREATE TABLE local_csv AS SELECT * FROM read_csv('data/input.csv');")
con.raw.sql("""
    SELECT c.*, u.email
    FROM local_csv c
    JOIN cloud.users u ON c.user_id = u.id
""").show()
```

### Multiple remote databases

```python
prod = openduck.connect("production", token="prod-token", endpoint="http://prod:7878", alias="prod")
staging = openduck.connect("staging", token="staging-token", endpoint="http://staging:7878", alias="staging")

prod.sql("SELECT count(*) FROM users").show()
staging.sql("SELECT count(*) FROM users").show()
```

Each `connect()` returns a separate DuckDB connection with its own attached remote.

### Context manager

```python
with openduck.connect("mydb") as con:
    con.sql("SELECT count(*) FROM events").show()
# connection closed automatically
```

### Plain DuckDB (no wrapper)

You don't actually need the package — the extension works with vanilla `duckdb`:

```python
import duckdb

con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute("LOAD 'extensions/openduck/build/release/extension/openduck/openduck.duckdb_extension';")
con.execute("ATTACH 'openduck:mydb?endpoint=http://localhost:7878&token=xxx' AS cloud;")

con.sql("SELECT * FROM cloud.users").show()
```

## DuckLake interop

The same connection can attach a local DuckLake catalog and a remote OpenDuck database, and join across them. See [`examples/python/duckdb_sdk_ducklake.py`](../../examples/python/duckdb_sdk_ducklake.py) — five self-contained sections (`remote`, `local`, `hybrid`, `wrapper`, `worker`) covering everything from a server-less local lake to a server-backed hybrid.

## Security

`connect()` validates and escapes everything it interpolates into SQL:

| Parameter | Validation |
|-----------|------------|
| `alias` | Strict identifier — alphanumeric + underscores; double-quoted in SQL. Invalid input raises `ValueError`. |
| `db_name` | Single-quote escaped (`'` → `''`). |
| `token` | Single-quote escaped. |
| `endpoint` | Single-quote escaped. |
| `extension_path` | Single-quote escaped. |

This prevents SQL injection through any of the wrapper's parameters.

## Where to next

- [Hybrid execution](hybrid-execution.md) — `openduck_run` hints and how plans split.
- [DuckDB extension](duckdb-extension.md) — what's happening underneath `connect()`.
- [Troubleshooting](troubleshooting.md) — common errors and fixes.
