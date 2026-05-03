# Hybrid execution

Sometimes the data is remote but a small filter / projection / join belongs on the client. Sometimes a heavy aggregation belongs on a beefy worker but you want the result in your local notebook.

Hybrid execution lets you write a single SQL query whose operators run partly on your local DuckDB and partly on a remote worker. The gateway splits the plan, runs each part where it belongs, ships intermediate Arrow IPC batches across the wire, and stitches the result together.

This is the same pattern MotherDuck calls **dual execution** — OpenDuck implements it as an open protocol.

## When you want it

- A small local table (filter list, dimension table, in-flight DataFrame) joined against a large remote table.
- A heavy `SUM(...)` / `COUNT(*) GROUP BY ...` you want to run on the worker, with the (small) result joined locally.
- Mixing data from a local file (Parquet, CSV, in-memory dict) with a remote OpenDuck database.

If your query is fully local or fully remote, you don't need this — it just runs.

## Enabling it

Hybrid is **off** by default. Turn it on at the gateway:

```bash
openduck -d mydb -p $OPENDUCK_TOKEN --hybrid
# or:
OPENDUCK_HYBRID=1 openduck -d mydb -p $OPENDUCK_TOKEN
```

When the gateway sees `--hybrid` (or `OPENDUCK_HYBRID=1`), it parses incoming SQL for `openduck_run(...)` hints and splits the query plan accordingly.

## A first hybrid query

```python
import openduck

with openduck.connect("mydb") as con:
    con.raw.execute("CREATE TABLE local.products(id INT, label VARCHAR);")
    con.raw.execute("INSERT INTO local.products VALUES (1, 'Widget'), (2, 'Gadget');")

    con.raw.sql("""
        SELECT p.label, u.name
        FROM local.products p
        JOIN cloud.users u ON p.id = u.id
    """).show()
```

The local table lives in the client's in-memory DuckDB. `cloud.users` lives on a worker. The gateway pulls just `users` rows back as Arrow IPC and the join runs locally.

## Explicit placement: `openduck_run`

For more control, wrap fragments in `openduck_run('LOCAL'|'REMOTE'|'AUTO', '<sql>')`. This mirrors MotherDuck's `md_run` pattern.

```sql
-- Mark a subquery as remote
openduck_run('REMOTE', 'SELECT sum(x) FROM big_table')

-- Compound: local outer query, remote subquery
SELECT l.*
FROM local_t l
JOIN openduck_run('REMOTE', 'SELECT * FROM remote_t') r
  ON l.id = r.id
```

| Placement | Where it runs |
|-----------|---------------|
| `LOCAL` | Client-side DuckDB process. |
| `REMOTE` | A worker via gRPC. |
| `AUTO` | Resolved from catalog metadata: `Scan` nodes against a known remote table become `REMOTE`; otherwise inherit from the majority of children; default `LOCAL`. |

## What the planner does

The hybrid planner builds a `PlanNode` tree, then runs three passes:

1. **`resolve_auto()`** — replaces `Auto` placement with `Local` or `Remote` based on catalog metadata and child majority.
2. **`insert_bridges()`** — inserts a `Bridge` operator at every boundary where a child's placement differs from its parent's.
3. **`explain_annotated()`** — produces an EXPLAIN-style tree with each operator labeled `[LOCAL]` or `[REMOTE]`.

```
[LOCAL]  HashJoin(l.id = r.id)
  [LOCAL]  Scan(orders)
  [LOCAL]  Bridge(R→L)              ← inserted automatically
    [REMOTE] Scan(lineitem)
```

`Bridge(R→L)` means data crosses from a remote fragment into the local fragment via Arrow IPC. The reverse direction (`L→R`) is defined in the model but not exercised by the current planner.

## Runtime: what actually happens

1. Client sends the SQL to the gateway.
2. Gateway parses for `openduck_run(...)`, builds the plan tree, resolves AUTO, inserts bridges.
3. For each remote fragment:
   - `ExecuteFragment` to a worker selected via [affinity routing](../architecture.md#worker-registration--routing).
   - Worker streams Arrow IPC batches back.
   - Gateway materializes them into a temp table on the client (`__remote`).
4. The local SQL runs against `__remote` alongside any local data.
5. Result returns to the client.

Output is **guaranteed** to match a single-process DuckDB baseline — the `hybrid_join_matches_baseline` parity test enforces this.

Key implementation: `hybrid::execute_hybrid_join()` in [`crates/exec-gateway/src/hybrid.rs`](../../crates/exec-gateway/src/hybrid.rs).

## Worker affinity and routing

Workers self-register with capabilities (databases, tables, compute context, max concurrency). The gateway picks a worker for each remote fragment using a tiered preference:

1. **Database affinity** — workers that declared the requested database.
2. **Table co-location** — workers authoritative for the tables in the query (`TableSourceRegistry`).
3. **Compute context** — match `ExecuteFragmentRequest.compute_context` against worker `compute_context` (e.g. `region=us-east-1`).
4. **Fallback** — round-robin across all healthy workers.

For multi-worker setups, declare table ownership when registering — the planner can then push filters and projections to the worker that owns the data:

```rust
WorkerRegistration {
    worker_id: "w-east-1",
    endpoint: "http://10.0.1.5:9898",
    databases: vec!["analytics".into()],
    tables: vec!["sales".into(), "lineitem".into()],
    compute_context: "region=us-east-1".into(),
    max_concurrency: 8,
    access_token: token,
}
```

See `examples/rust/worker_registration.rs`, `examples/rust/federation_provider.rs`, and `examples/rust/compute_pushdown.rs`.

## EXPLAIN

You can ask the gateway for the annotated plan without executing:

```sql
SELECT * FROM openduck_query('cloud',
    'EXPLAIN openduck_run(''REMOTE'', ''SELECT count(*) FROM big'')'
);
```

(The `examples/rust/hybrid_plan` example builds plans, inserts bridges, resolves AUTO, and prints the same EXPLAIN-style output without any servers.)

## Cancellation

While a hybrid query is running, you can cancel it via execution ID:

```bash
openduck cancel <EXECUTION_ID> --endpoint http://127.0.0.1:7878 --token $OPENDUCK_TOKEN
```

The gateway broadcasts the cancel to every worker that has an in-flight fragment for that execution.

## Limits to be aware of

- **Single remote fragment** — the gateway handles one remote subquery per hybrid query. Deeply nested multi-remote trees need manual decomposition.
- **Type mapping** — Arrow → DuckDB conversion covers the common types (INT, BIGINT, FLOAT, DOUBLE, VARCHAR, BOOLEAN, DATE, TIMESTAMP). Complex / nested types fall back to `VARCHAR`.
- **Schema inference** — the remote batch schema is inferred from the first Arrow batch.
- **No cost-based splitting** — `AUTO` resolution uses table membership, not statistics.
- **In-flight cap** — bound by `OPENDUCK_MAX_IN_FLIGHT` (default 64) per gateway.
- **Message size** — gRPC encode/decode capped at 64 MiB; large remote results stream as multiple Arrow IPC batches.

## Related examples

- `examples/rust/hybrid_plan` — build a plan, insert bridges, resolve AUTO, print EXPLAIN.
- `examples/rust/hybrid_execution` — full end-to-end LOCAL+REMOTE join via real gRPC.
- `examples/rust/compute_pushdown` — federation / `TableSourceRegistry` plan optimization.
- `examples/rust/worker_registration` — affinity routing + `max_concurrency`.
- `examples/rust/federation_provider` — `FederationProvider`, `compute_context` tiers.

## Where to next

- [Architecture → Hybrid execution](../architecture.md#hybrid-execution) for internals.
- [Deployment](deployment.md) for multi-worker setups where affinity routing matters.
