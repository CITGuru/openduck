# OpenDuck Documentation

Welcome to the OpenDuck docs. OpenDuck is an open-source implementation of differential storage, hybrid (dual) execution, and transparent remote databases for DuckDB — a self-hostable, protocol-open take on the architecture pioneered by [MotherDuck](https://motherduck.com).

```python
import openduck

con = openduck.connect("mydb")
con.sql("SELECT * FROM users").show()                 # remote, transparent
con.sql("SELECT * FROM local.t JOIN cloud.t2 ...")    # hybrid, one query
```

## Read this first

- [**Overview**](overview.md) — what OpenDuck is, the problems it solves, and how it compares to MotherDuck, Arrow Flight SQL, and DuckLake.
- [**Architecture**](architecture.md) — components, data flow, the protocol, and how a query becomes Arrow batches.
- [**Configuration**](configuration.md) — every CLI flag, environment variable, TOML key, and DuckDB secret OpenDuck understands.

## Guides

- [**Getting started**](guides/getting-started.md) — build the backend and the extension, start the service, run your first query.
- [**Python client**](guides/python-client.md) — the `openduck` Python package: connections, hybrid queries, pandas/Arrow integration.
- [**DuckDB extension**](guides/duckdb-extension.md) — `LOAD`, `ATTACH`, URI format, secrets, table functions.
- [**Differential storage**](guides/differential-storage.md) — append-only layers, snapshots, the three storage modes (Direct, FUSE, In-Process).
- [**Hybrid execution**](guides/hybrid-execution.md) — how to enable it, how plans split, the `openduck_run` hint.
- [**Snapshots and garbage collection**](guides/snapshots-and-gc.md) — sealing, point-in-time reads, `openduck gc`.
- [**Deployment**](guides/deployment.md) — Docker, multi-worker, DuckLake, S3 tiering, observability.
- [**Troubleshooting**](guides/troubleshooting.md) — common errors and how to fix them.

## Reference

- [Protocol definition](../proto/openduck/v1/execution.proto) — the wire format (gRPC + Arrow IPC).
- [Examples](../examples/README.md) — runnable Rust and Python examples for every major feature.
- [Python client README](../clients/python/README.md) — full API for the `openduck` package.

## Contributing

Source layout, build instructions, and CI are in the [top-level README](../README.md).

## License

MIT.
