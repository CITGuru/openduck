#pragma once

namespace duckdb {
class LogicalOperator;
} // namespace duckdb

namespace openduck {

class OpenDuckCatalog;

// ═══════════════════════════════════════════════════════════════════════════
// HasNonRemoteReference
// ═══════════════════════════════════════════════════════════════════════════
//
// Walks a bound `LogicalOperator` tree and returns true if ANY bound
// reference resolves to a target outside of `remote_catalog`:
//
//   * `LogicalGet` on a TableCatalogEntry in a different catalog
//   * `LogicalGet` on any local table function (read_csv, read_parquet,
//     generate_series, or user-defined table functions)
//   * `BoundFunctionExpression` / `BoundAggregateExpression` /
//     `BoundWindowExpression` whose FunctionCatalogEntry lives outside
//     `remote_catalog`
//   * `LogicalCTERef` whose body (transitively) contains any of the
//     above
//
// Used by `OpenDuckCatalog::Plan{Insert,Update,Delete,CreateTableAs,
// Copy}` to decide between the pure-remote path
// (`PhysicalOpenDuckMutate`) and the cross-catalog path
// (`PhysicalOpenDuckIngestAndMutate`).
bool HasNonRemoteReference(duckdb::LogicalOperator &op, OpenDuckCatalog &remote_catalog);

} // namespace openduck
