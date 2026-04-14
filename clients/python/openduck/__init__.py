"""OpenDuck — distributed DuckDB client.

Connects to any service implementing the OpenDuck Protocol (gRPC + Arrow IPC).

Usage::

    import openduck

    con = openduck.connect("mydb", token="my-token")
    con.sql("SELECT * FROM users").show()
"""

from openduck.main import connect, OpenDuckConnection

__all__ = ["connect", "OpenDuckConnection"]
__version__ = "0.1.0"
