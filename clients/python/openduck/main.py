"""OpenDuck connection wrapper around DuckDB."""

from __future__ import annotations

import os
from typing import Any, Optional

import duckdb

from openduck.utils import find_extension, parse_uri, URI_RE, EXTENSION_SEARCH_PATHS

def connect(
    database: str = "default",
    *,
    token: Optional[str] = None,
    endpoint: Optional[str] = None,
    extension_path: Optional[str] = None,
    alias: str = "cloud",
    **duckdb_config: Any,
) -> OpenDuckConnection:
    """Connect to an OpenDuck-compatible remote database.

    Works with any service implementing the OpenDuck gRPC protocol.

    Supports both plain names and URI-style strings::

        openduck.connect("mydb")
        openduck.connect("od:mydb")
        openduck.connect("openduck:mydb?endpoint=http://localhost:7878")

    Args:
        database: Database name or full URI (``openduck:...`` / ``od:...``).
        token: Access token. Falls back to ``OPENDUCK_TOKEN`` env var.
        endpoint: Gateway endpoint. Falls back to ``OPENDUCK_ENDPOINT``
            or ``http://127.0.0.1:7878``.
        extension_path: Path to the ``.duckdb_extension`` file.
            Auto-detected from the build tree if omitted.
        alias: SQL alias for the ATTACHed remote database (default ``"cloud"``).
        **duckdb_config: Extra config passed to ``duckdb.connect()``.

    Returns:
        An :class:`OpenDuckConnection` wrapping a DuckDB connection with
        the remote database ATTACHed.
    """
    if URI_RE.match(database):
        db_name, params = parse_uri(database)
        token = token or params.get("token")
        endpoint = endpoint or params.get("endpoint")
    else:
        db_name = database or "default"

    token = token or os.environ.get("OPENDUCK_TOKEN", "")
    endpoint = endpoint or os.environ.get(
        "OPENDUCK_ENDPOINT", "http://127.0.0.1:7878"
    )

    if not token:
        raise ValueError(
            "No token provided. Pass token= or set the OPENDUCK_TOKEN env var."
        )

    duckdb_config.setdefault("allow_unsigned_extensions", "true")
    con = duckdb.connect(":memory:", config=duckdb_config)

    ext = extension_path or find_extension()
    if ext:
        con.execute(f"LOAD '{ext}';")
    else:
        con.execute("INSTALL openduck;")
        con.execute("LOAD openduck;")

    query_params = f"?endpoint={endpoint}&token={token}"
    con.execute(f"ATTACH 'openduck:{db_name}{query_params}' AS {alias};")
    con.execute(f"USE {alias};")

    return OpenDuckConnection(con, alias=alias, database=db_name)


class OpenDuckConnection:
    """A DuckDB connection with an ATTACHed OpenDuck remote database.

    Queries run through ``sql()`` and ``execute()`` are sent directly
    to the remote — the catalog resolves table names transparently
    via gRPC, so ``SELECT * FROM users`` just works.

    Use ``con.raw`` to access the underlying ``duckdb.DuckDBPyConnection``
    for local queries, hybrid JOINs, or any DuckDB feature.
    """

    def __init__(self, con: duckdb.DuckDBPyConnection, *, alias: str, database: str):
        self._con = con
        self.alias = alias
        self.database = database

    def sql(self, query: str) -> duckdb.DuckDBPyRelation:
        """Run a SQL query on the remote database.

        Table names resolve transparently through the remote catalog::

            con.sql("SELECT * FROM users").show()
            con.sql("SELECT count(*) FROM events WHERE ts > '2025-01-01'").fetchone()
        """
        return self._con.sql(query)

    def execute(
        self, query: str, parameters: Any = None
    ) -> duckdb.DuckDBPyConnection:
        """Execute a SQL statement on the remote database."""
        return self._con.execute(query, parameters)

    def fetchall(self) -> list:
        """Fetch all rows from the last executed query."""
        return self._con.fetchall()

    def fetchone(self) -> Any:
        """Fetch one row from the last executed query."""
        return self._con.fetchone()

    def fetchdf(self):
        """Fetch the result as a pandas DataFrame."""
        return self._con.fetchdf()

    def close(self) -> None:
        """Close the connection."""
        self._con.close()

    @property
    def raw(self) -> duckdb.DuckDBPyConnection:
        """Access the underlying DuckDB connection for local queries, JOINs, etc."""
        return self._con

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __repr__(self):
        return f"OpenDuckConnection(database={self.database!r}, alias={self.alias!r})"
