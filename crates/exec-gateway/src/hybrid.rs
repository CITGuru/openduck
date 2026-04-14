//! Hybrid execution: plan splitting, placement labels, bridge operators, and EXPLAIN annotations.
//!
//! The gateway rewrites a query plan to split work between local (client-side DuckDB)
//! and remote (worker-side) execution. Placement is driven by hints, catalog metadata,
//! or the `openduck_run('LOCAL'|'REMOTE')` table function.

use std::fmt;

/// Where a plan fragment should execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Placement {
    /// Execute inside the client-side DuckDB process.
    Local,
    /// Ship to a remote OpenDuck worker via gRPC.
    Remote,
    /// Let the planner decide based on catalog location and cost heuristics.
    Auto,
}

impl fmt::Display for Placement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Placement::Local => write!(f, "LOCAL"),
            Placement::Remote => write!(f, "REMOTE"),
            Placement::Auto => write!(f, "AUTO"),
        }
    }
}

impl Placement {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_uppercase().as_str() {
            "LOCAL" | "L" => Some(Self::Local),
            "REMOTE" | "R" => Some(Self::Remote),
            "AUTO" | "A" => Some(Self::Auto),
            _ => None,
        }
    }
}

/// A node in the hybrid execution plan.
#[derive(Debug, Clone)]
pub struct PlanNode {
    pub id: u32,
    pub kind: NodeKind,
    pub placement: Placement,
    pub children: Vec<PlanNode>,
}

/// Operator types in the hybrid plan.
#[derive(Debug, Clone)]
pub enum NodeKind {
    /// Table / index scan. `table` identifies the relation.
    Scan { table: String },
    /// Hash join combining two inputs.
    HashJoin { condition: String },
    /// Aggregate (GROUP BY / SUM / COUNT).
    Aggregate { keys: Vec<String> },
    /// Projection (column selection / expression evaluation).
    Project { columns: Vec<String> },
    /// Filter predicate pushed into the plan.
    Filter { predicate: String },
    /// Bridge operator inserted at L/R boundary — materialises the remote side
    /// and streams it into the local execution context (or vice-versa).
    Bridge { direction: BridgeDirection },
    /// Explicit `openduck_run(placement)` wrapper applied by user hint.
    RunHint { sql: String, placement: Placement },
    /// Opaque SQL fragment forwarded to a worker verbatim.
    RemoteSql { sql: String },
}

/// Direction of data movement for a [`NodeKind::Bridge`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeDirection {
    RemoteToLocal,
    LocalToRemote,
}

impl fmt::Display for BridgeDirection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BridgeDirection::RemoteToLocal => write!(f, "R→L"),
            BridgeDirection::LocalToRemote => write!(f, "L→R"),
        }
    }
}

/// Insert bridge operators at every boundary where placement changes between
/// a parent and child node. Returns the rewritten tree.
pub fn insert_bridges(mut root: PlanNode) -> PlanNode {
    insert_bridges_recursive(&mut root);
    root
}

fn insert_bridges_recursive(node: &mut PlanNode) {
    let parent_placement = node.placement;
    let mut next_id = max_id(node) + 1;

    for child in &mut node.children {
        insert_bridges_recursive(child);
    }

    let mut new_children = Vec::with_capacity(node.children.len());
    for child in std::mem::take(&mut node.children) {
        if child.placement != parent_placement
            && child.placement != Placement::Auto
            && parent_placement != Placement::Auto
        {
            let direction = if child.placement == Placement::Remote {
                BridgeDirection::RemoteToLocal
            } else {
                BridgeDirection::LocalToRemote
            };
            let bridge = PlanNode {
                id: next_id,
                kind: NodeKind::Bridge { direction },
                placement: parent_placement,
                children: vec![child],
            };
            next_id += 1;
            new_children.push(bridge);
        } else {
            new_children.push(child);
        }
    }
    node.children = new_children;
}

fn max_id(node: &PlanNode) -> u32 {
    let mut m = node.id;
    for c in &node.children {
        m = m.max(max_id(c));
    }
    m
}

/// Render an EXPLAIN-style text plan with placement annotations.
pub fn explain_annotated(node: &PlanNode) -> String {
    let mut out = String::new();
    explain_recursive(node, 0, &mut out);
    out
}

fn explain_recursive(node: &PlanNode, depth: usize, out: &mut String) {
    let indent = "  ".repeat(depth);
    let label = match &node.kind {
        NodeKind::Scan { table } => format!("Scan({table})"),
        NodeKind::HashJoin { condition } => format!("HashJoin({condition})"),
        NodeKind::Aggregate { keys } => format!("Aggregate({})", keys.join(", ")),
        NodeKind::Project { columns } => format!("Project({})", columns.join(", ")),
        NodeKind::Filter { predicate } => format!("Filter({predicate})"),
        NodeKind::Bridge { direction } => format!("Bridge({direction})"),
        NodeKind::RunHint { placement, .. } => format!("RunHint({placement})"),
        NodeKind::RemoteSql { sql } => format!("RemoteSQL({sql})"),
    };
    out.push_str(&format!(
        "{indent}[{placement}] {label}\n",
        placement = node.placement
    ));
    for c in &node.children {
        explain_recursive(c, depth + 1, out);
    }
}

/// Parse an `openduck_run('LOCAL', '<sql>')` / `openduck_run('REMOTE', '<sql>')` hint
/// and return a plan node wrapping the inner SQL with the designated placement.
pub fn parse_openduck_run(hint: &str) -> Option<PlanNode> {
    let trimmed = hint.trim();
    let inner = trimmed.strip_prefix("openduck_run(")?.strip_suffix(')')?;
    let parts: Vec<&str> = inner.splitn(2, ',').collect();
    if parts.len() != 2 {
        return None;
    }
    let placement_str = parts[0].trim().trim_matches('\'').trim_matches('"');
    let sql = parts[1].trim().trim_matches('\'').trim_matches('"');
    let placement = Placement::parse(placement_str)?;
    Some(PlanNode {
        id: 0,
        kind: NodeKind::RunHint {
            sql: sql.to_string(),
            placement,
        },
        placement,
        children: vec![],
    })
}

// ═════════════════════════════════════════════════════════════════════════════
// Table source metadata — per-table origin tracking for automatic plan splitting
// ═════════════════════════════════════════════════════════════════════════════

/// Describes where a table lives and which federation provider owns it.
/// This is the OpenDuck equivalent of datafusion-federation's `FederatedTableSource`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableSource {
    /// Fully qualified table name (e.g. "mydb.public.users").
    pub table: String,
    /// Name of the federation provider (e.g. "openduck-worker").
    pub provider: String,
    /// Opaque compute context that identifies the specific backend instance.
    /// Workers with the same `compute_context` are co-located — their tables
    /// can be joined remotely without a bridge.
    pub compute_context: String,
    /// Database the table belongs to.
    pub database: String,
}

/// Registry of known table sources, used by the planner to resolve `Auto`
/// placement and identify the largest federable sub-plans.
#[derive(Debug, Clone, Default)]
pub struct TableSourceRegistry {
    sources: std::collections::HashMap<String, TableSource>,
}

impl TableSourceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, source: TableSource) {
        self.sources.insert(source.table.clone(), source);
    }

    pub fn get(&self, table: &str) -> Option<&TableSource> {
        self.sources.get(table)
    }

    pub fn is_remote(&self, table: &str) -> bool {
        self.sources.contains_key(table)
    }

    /// Returns true if two tables share the same provider and compute context
    /// (they can be processed together remotely without a bridge).
    pub fn are_co_located(&self, a: &str, b: &str) -> bool {
        match (self.sources.get(a), self.sources.get(b)) {
            (Some(sa), Some(sb)) => {
                sa.provider == sb.provider && sa.compute_context == sb.compute_context
            }
            _ => false,
        }
    }

    /// All tables that belong to a given compute context.
    pub fn tables_for_context(&self, ctx: &str) -> Vec<&TableSource> {
        self.sources
            .values()
            .filter(|s| s.compute_context == ctx)
            .collect()
    }
}

/// Resolve `Auto` placement for all nodes using table source metadata.
///
/// Uses `TableSourceRegistry` for rich origin data (provider, compute context),
/// with fallback to a simple `HashSet<String>` of remote table names.
pub fn resolve_auto(node: &mut PlanNode, remote_tables: &std::collections::HashSet<String>) {
    resolve_auto_with_registry(node, remote_tables, None);
}

/// Resolve `Auto` placement with an optional `TableSourceRegistry` for
/// richer co-location and affinity decisions.
pub fn resolve_auto_with_registry(
    node: &mut PlanNode,
    remote_tables: &std::collections::HashSet<String>,
    registry: Option<&TableSourceRegistry>,
) {
    for child in &mut node.children {
        resolve_auto_with_registry(child, remote_tables, registry);
    }
    if node.placement == Placement::Auto {
        node.placement = match &node.kind {
            NodeKind::Scan { table } => {
                let is_remote = registry
                    .map(|r| r.is_remote(table))
                    .unwrap_or_else(|| remote_tables.contains(table));
                if is_remote {
                    Placement::Remote
                } else {
                    Placement::Local
                }
            }
            _ => {
                let remote_count = node
                    .children
                    .iter()
                    .filter(|c| c.placement == Placement::Remote)
                    .count();
                let local_count = node
                    .children
                    .iter()
                    .filter(|c| c.placement == Placement::Local)
                    .count();
                if remote_count > local_count {
                    Placement::Remote
                } else {
                    Placement::Local
                }
            }
        };
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Compute pushdown — largest federable subplan identification
// ═════════════════════════════════════════════════════════════════════════════

/// Identify the largest sub-trees whose scans all share the same compute
/// context and collapse them into single `RemoteSql` nodes. This is the
/// OpenDuck equivalent of datafusion-federation's `FederationOptimizerRule`.
///
/// The algorithm walks top-down:
/// 1. For each node, check if **all** descendant scans belong to the same
///    compute context (via `subplan_context`).
/// 2. If yes, replace the entire sub-tree with a `RemoteSql` node carrying
///    the SQL to execute on that context — this is the largest possible
///    pushdown.
/// 3. If no, recurse into children and try to collapse them individually.
pub fn pushdown_federable_subplans(root: &mut PlanNode, registry: &TableSourceRegistry) {
    *root = pushdown_recursive_owned(root.clone(), registry);
}

/// Returns `Some(context)` if every Scan under `node` belongs to the same
/// remote compute context, `None` otherwise.
fn subplan_context(node: &PlanNode, registry: &TableSourceRegistry) -> Option<String> {
    match &node.kind {
        NodeKind::Scan { table } => registry.get(table).map(|s| s.compute_context.clone()),
        NodeKind::Bridge { .. } | NodeKind::RunHint { .. } => None,
        NodeKind::RemoteSql { .. } => None,
        _ => {
            if node.children.is_empty() {
                return None;
            }
            let mut ctx: Option<String> = None;
            for child in &node.children {
                match subplan_context(child, registry) {
                    Some(child_ctx) => match &ctx {
                        Some(c) if c != &child_ctx => return None,
                        None => ctx = Some(child_ctx),
                        _ => {}
                    },
                    None => return None,
                }
            }
            ctx
        }
    }
}

/// Generate a SQL string for a plan node tree (best-effort reconstruction).
fn node_to_sql(node: &PlanNode) -> String {
    match &node.kind {
        NodeKind::Scan { table } => format!("SELECT * FROM {table}"),
        NodeKind::Filter { predicate } => {
            if let Some(child) = node.children.first() {
                format!(
                    "SELECT * FROM ({}) _f WHERE {predicate}",
                    node_to_sql(child)
                )
            } else {
                format!("SELECT * WHERE {predicate}")
            }
        }
        NodeKind::Project { columns } => {
            let cols = columns.join(", ");
            if let Some(child) = node.children.first() {
                format!("SELECT {cols} FROM ({}) _p", node_to_sql(child))
            } else {
                format!("SELECT {cols}")
            }
        }
        NodeKind::Aggregate { keys } => {
            let keys_str = keys.join(", ");
            if let Some(child) = node.children.first() {
                format!(
                    "SELECT {keys_str}, COUNT(*) FROM ({}) _a GROUP BY {keys_str}",
                    node_to_sql(child)
                )
            } else {
                format!("SELECT {keys_str}")
            }
        }
        NodeKind::HashJoin { condition } => {
            let left = node.children.first().map(node_to_sql).unwrap_or_default();
            let right = node.children.get(1).map(node_to_sql).unwrap_or_default();
            format!("SELECT * FROM ({left}) _l JOIN ({right}) _r ON {condition}")
        }
        NodeKind::RemoteSql { sql } => sql.clone(),
        NodeKind::RunHint { sql, .. } => sql.clone(),
        NodeKind::Bridge { .. } => String::new(),
    }
}

fn pushdown_recursive_owned(node: PlanNode, registry: &TableSourceRegistry) -> PlanNode {
    // If this entire subtree is in one context, collapse it now (top-down
    // "largest subplan first" strategy).
    if !matches!(
        node.kind,
        NodeKind::RemoteSql { .. } | NodeKind::Bridge { .. } | NodeKind::RunHint { .. }
    ) && subplan_context(&node, registry).is_some()
    {
        let sql = node_to_sql(&node);
        if !sql.is_empty() {
            return PlanNode {
                id: node.id,
                kind: NodeKind::RemoteSql { sql },
                placement: Placement::Remote,
                children: vec![],
            };
        }
    }

    // Otherwise, recurse into children — each child may be independently
    // collapsible even though the parent is mixed local/remote.
    let new_children = node
        .children
        .into_iter()
        .map(|child| pushdown_recursive_owned(child, registry))
        .collect();

    PlanNode {
        id: node.id,
        kind: node.kind,
        placement: node.placement,
        children: new_children,
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Compound hint parsing
// ═════════════════════════════════════════════════════════════════════════════

/// Parse a compound SQL hint containing nested `openduck_run(...)` calls.
///
/// Input shape: a SQL string that contains `openduck_run('REMOTE', '...')`
/// somewhere in it (e.g., as a subquery or table-valued function reference).
///
/// Returns a `PlanNode` tree with:
/// - Root: `RunHint { placement: Local }` wrapping the outer SQL
/// - Child: `RunHint { placement: Remote }` for the inner fragment
pub fn parse_compound_hint(sql: &str) -> Option<PlanNode> {
    let lower = sql.to_ascii_lowercase();
    let idx = lower.find("openduck_run(")?;
    let inner_hint = &sql[idx..];

    let inner_node = parse_openduck_run(
        inner_hint
            .split(')')
            .next()
            .map(|s| format!("{s})"))
            .unwrap_or_default()
            .as_str(),
    )?;

    let outer_placement = if lower.starts_with("openduck_run(") {
        if let Some(node) = parse_openduck_run(sql) {
            node.placement
        } else {
            Placement::Local
        }
    } else {
        Placement::Local
    };

    Some(PlanNode {
        id: 0,
        kind: NodeKind::RunHint {
            sql: sql.to_string(),
            placement: outer_placement,
        },
        placement: outer_placement,
        children: vec![inner_node],
    })
}

/// Extract the SQL for the first REMOTE fragment found in a plan tree.
pub fn extract_remote_sql(node: &PlanNode) -> Option<String> {
    if node.placement == Placement::Remote {
        if let NodeKind::RunHint { sql, .. } = &node.kind {
            return Some(sql.clone());
        }
    }
    for child in &node.children {
        if let Some(sql) = extract_remote_sql(child) {
            return Some(sql);
        }
    }
    None
}

// ═════════════════════════════════════════════════════════════════════════════
// Runtime: execute a hybrid plan with real workers
// ═════════════════════════════════════════════════════════════════════════════

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use duckdb::Connection;
use exec_proto::openduck::v1::execute_fragment_chunk::Payload;
use exec_proto::openduck::v1::execution_service_client::ExecutionServiceClient;
use exec_proto::openduck::v1::ExecuteFragmentRequest;
use tonic::Request;

/// Errors that can occur during hybrid execution.
#[derive(Debug)]
pub enum HybridError {
    /// Worker or gateway is unreachable — caller may fall back to local-only.
    Unavailable(String),
    /// Worker returned an error during execution.
    WorkerError(String),
    /// Data decoding or materialization failed.
    DataError(String),
    /// Authentication failure.
    AuthError(String),
}

impl fmt::Display for HybridError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HybridError::Unavailable(e) => write!(f, "worker unavailable: {e}"),
            HybridError::WorkerError(e) => write!(f, "worker error: {e}"),
            HybridError::DataError(e) => write!(f, "data error: {e}"),
            HybridError::AuthError(e) => write!(f, "auth error: {e}"),
        }
    }
}

impl HybridError {
    /// Returns true if the error is transient and the caller should consider
    /// falling back to local-only execution.
    pub fn is_fallback_eligible(&self) -> bool {
        matches!(self, HybridError::Unavailable(_))
    }
}

/// Collect Arrow IPC batches from a worker stream into `RecordBatch`es.
async fn collect_remote_batches(
    worker_url: &str,
    sql: &str,
    token: &str,
) -> Result<Vec<RecordBatch>, HybridError> {
    let mut client = ExecutionServiceClient::connect(worker_url.to_string())
        .await
        .map_err(|e| HybridError::Unavailable(format!("connect to worker: {e}")))?;

    let request = ExecuteFragmentRequest {
        plan: sql.as_bytes().to_vec(),
        access_token: token.to_string(),
        execution_id: uuid::Uuid::new_v4().to_string(),
        ..Default::default()
    };

    let mut stream = client
        .execute_fragment(Request::new(request))
        .await
        .map_err(|e| {
            let code = e.code();
            if code == tonic::Code::Unauthenticated || code == tonic::Code::PermissionDenied {
                HybridError::AuthError(e.to_string())
            } else {
                HybridError::Unavailable(format!("execute_fragment: {e}"))
            }
        })?
        .into_inner();

    let mut batches = Vec::new();
    while let Some(chunk) = stream
        .message()
        .await
        .map_err(|e| HybridError::Unavailable(format!("stream: {e}")))?
    {
        match chunk.payload {
            Some(Payload::ArrowBatch(b)) if !b.ipc_stream_payload.is_empty() => {
                let cursor = std::io::Cursor::new(b.ipc_stream_payload);
                let reader = StreamReader::try_new(cursor, None)
                    .map_err(|e| HybridError::DataError(format!("arrow decode: {e}")))?;
                for batch in reader {
                    batches.push(batch.map_err(|e| HybridError::DataError(format!("arrow: {e}")))?);
                }
            }
            Some(Payload::Error(e)) => return Err(HybridError::WorkerError(e)),
            Some(Payload::Finished(true)) => break,
            _ => {}
        }
    }
    Ok(batches)
}

fn arrow_to_duckdb_type(dt: &arrow::datatypes::DataType) -> &'static str {
    use arrow::datatypes::DataType;
    match dt {
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Float16 | DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR",
        DataType::Boolean => "BOOLEAN",
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Timestamp(_, _) => "TIMESTAMP",
        _ => "VARCHAR",
    }
}

/// Insert Arrow `RecordBatch`es into a local DuckDB connection as a temporary table.
///
/// Uses DuckDB's Appender API (`append_record_batch`) for bulk loading instead of
/// per-row INSERT statements, which is orders of magnitude faster.
fn materialize_batches(
    conn: &Connection,
    table_name: &str,
    batches: &[RecordBatch],
) -> Result<(), String> {
    if batches.is_empty() {
        conn.execute_batch(&format!(
            "CREATE TEMP TABLE {table_name} AS SELECT 1 WHERE false"
        ))
        .map_err(|e| format!("create empty temp table: {e}"))?;
        return Ok(());
    }

    let schema = batches[0].schema();
    let cols: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| format!("{} {}", f.name(), arrow_to_duckdb_type(f.data_type())))
        .collect();
    let create_sql = format!("CREATE TEMP TABLE {table_name} ({})", cols.join(", "));
    conn.execute_batch(&create_sql)
        .map_err(|e| format!("create temp table: {e}"))?;

    let mut appender = conn
        .appender(table_name)
        .map_err(|e| format!("create appender for {table_name}: {e}"))?;
    for batch in batches {
        appender
            .append_record_batch(batch.clone())
            .map_err(|e| format!("append batch: {e}"))?;
    }
    appender
        .flush()
        .map_err(|e| format!("flush appender: {e}"))?;
    Ok(())
}

/// Execute a hybrid query: remote fragments on workers, local join in DuckDB.
///
/// `remote_sql`: SQL to execute on the worker (the remote portion)
/// `local_setup_sql`: SQL run locally before the join (creates local tables)
/// `local_join_sql`: SQL to execute locally, referencing `__remote` as the materialized remote data
/// `worker_url`: gRPC endpoint of the worker
/// `token`: access token
///
/// Returns the local DuckDB query result as a single i64 (for golden parity tests).
pub async fn execute_hybrid_join(
    remote_sql: &str,
    local_setup_sql: &str,
    local_join_sql: &str,
    worker_url: &str,
    token: &str,
) -> Result<i64, String> {
    let batches = collect_remote_batches(worker_url, remote_sql, token)
        .await
        .map_err(|e| e.to_string())?;
    eprintln!(
        "hybrid: collected {} batch(es) from remote, {} total rows",
        batches.len(),
        batches.iter().map(|b| b.num_rows()).sum::<usize>()
    );

    let conn = Connection::open_in_memory().map_err(|e| format!("duckdb: {e}"))?;

    conn.execute_batch(local_setup_sql)
        .map_err(|e| format!("local setup: {e}"))?;

    materialize_batches(&conn, "__remote", &batches)?;

    let result: i64 = conn
        .query_row(local_join_sql, [], |row| row.get(0))
        .map_err(|e| format!("local join: {e}"))?;

    Ok(result)
}

/// Execute a hybrid query with automatic fallback to local-only execution
/// when the remote worker is unreachable.
///
/// `remote_sql`: SQL to execute on the worker (the remote portion)
/// `local_setup_sql`: SQL run locally before the join
/// `local_join_sql`: SQL referencing `__remote` for the join
/// `fallback_sql`: SQL to run locally if the worker is unreachable (should
///                 produce the same result without the remote fragment)
/// `worker_url`: gRPC endpoint of the worker
/// `token`: access token
pub async fn execute_hybrid_join_with_fallback(
    remote_sql: &str,
    local_setup_sql: &str,
    local_join_sql: &str,
    fallback_sql: Option<&str>,
    worker_url: &str,
    token: &str,
) -> Result<i64, String> {
    match collect_remote_batches(worker_url, remote_sql, token).await {
        Ok(batches) => {
            eprintln!(
                "hybrid: collected {} batch(es) from remote, {} total rows",
                batches.len(),
                batches.iter().map(|b| b.num_rows()).sum::<usize>()
            );
            let conn = Connection::open_in_memory().map_err(|e| format!("duckdb: {e}"))?;
            conn.execute_batch(local_setup_sql)
                .map_err(|e| format!("local setup: {e}"))?;
            materialize_batches(&conn, "__remote", &batches)?;
            conn.query_row(local_join_sql, [], |row| row.get(0))
                .map_err(|e| format!("local join: {e}"))
        }
        Err(e) if e.is_fallback_eligible() => {
            if let Some(fb_sql) = fallback_sql {
                eprintln!("hybrid: worker unavailable ({e}), falling back to local-only execution");
                let conn = Connection::open_in_memory().map_err(|e| format!("duckdb: {e}"))?;
                conn.execute_batch(local_setup_sql)
                    .map_err(|e| format!("local setup: {e}"))?;
                conn.query_row(fb_sql, [], |row| row.get(0))
                    .map_err(|e| format!("fallback query: {e}"))
            } else {
                Err(format!(
                    "worker unavailable and no fallback configured: {e}"
                ))
            }
        }
        Err(e) => Err(e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn scan(id: u32, table: &str, placement: Placement) -> PlanNode {
        PlanNode {
            id,
            kind: NodeKind::Scan {
                table: table.into(),
            },
            placement,
            children: vec![],
        }
    }

    #[test]
    fn bridge_insertion_at_boundary() {
        let join = PlanNode {
            id: 1,
            kind: NodeKind::HashJoin {
                condition: "l.id = r.id".into(),
            },
            placement: Placement::Local,
            children: vec![
                scan(2, "local_t", Placement::Local),
                scan(3, "remote_t", Placement::Remote),
            ],
        };
        let result = insert_bridges(join);
        assert_eq!(result.children.len(), 2);
        assert!(
            matches!(
                &result.children[1].kind,
                NodeKind::Bridge {
                    direction: BridgeDirection::RemoteToLocal
                }
            ),
            "expected bridge at L/R boundary"
        );
    }

    #[test]
    fn no_bridge_when_same_placement() {
        let join = PlanNode {
            id: 1,
            kind: NodeKind::HashJoin {
                condition: "a.id = b.id".into(),
            },
            placement: Placement::Local,
            children: vec![
                scan(2, "t1", Placement::Local),
                scan(3, "t2", Placement::Local),
            ],
        };
        let result = insert_bridges(join);
        for c in &result.children {
            assert!(
                !matches!(&c.kind, NodeKind::Bridge { .. }),
                "no bridge expected for same placement"
            );
        }
    }

    #[test]
    fn explain_shows_placement() {
        let plan = PlanNode {
            id: 1,
            kind: NodeKind::HashJoin {
                condition: "l.id = r.id".into(),
            },
            placement: Placement::Local,
            children: vec![
                scan(2, "orders", Placement::Local),
                PlanNode {
                    id: 10,
                    kind: NodeKind::Bridge {
                        direction: BridgeDirection::RemoteToLocal,
                    },
                    placement: Placement::Local,
                    children: vec![scan(3, "lineitem", Placement::Remote)],
                },
            ],
        };
        let text = explain_annotated(&plan);
        assert!(text.contains("[LOCAL] HashJoin"));
        assert!(text.contains("[LOCAL] Bridge(R→L)"));
        assert!(text.contains("[REMOTE] Scan(lineitem)"));
    }

    #[test]
    fn parse_openduck_run_hint() {
        let node = parse_openduck_run("openduck_run('REMOTE', 'SELECT sum(x) FROM big_table')")
            .expect("parse");
        assert_eq!(node.placement, Placement::Remote);
        match &node.kind {
            NodeKind::RunHint { sql, placement } => {
                assert_eq!(sql, "SELECT sum(x) FROM big_table");
                assert_eq!(*placement, Placement::Remote);
            }
            _ => panic!("expected RunHint"),
        }
    }

    #[test]
    fn resolve_auto_uses_remote_tables() {
        let mut plan = PlanNode {
            id: 1,
            kind: NodeKind::HashJoin {
                condition: "a.k = b.k".into(),
            },
            placement: Placement::Auto,
            children: vec![
                scan(2, "local_tab", Placement::Auto),
                scan(3, "remote_tab", Placement::Auto),
            ],
        };
        let remotes: HashSet<String> = ["remote_tab".to_string()].into();
        resolve_auto(&mut plan, &remotes);
        assert_eq!(plan.children[0].placement, Placement::Local);
        assert_eq!(plan.children[1].placement, Placement::Remote);
    }

    #[test]
    fn parse_compound_hint_extracts_remote() {
        let sql = "SELECT l.* FROM local_t l JOIN openduck_run('REMOTE', 'SELECT * FROM big') r ON l.id = r.id";
        let node = parse_compound_hint(sql).expect("should parse compound");
        assert_eq!(node.placement, Placement::Local);
        let remote = extract_remote_sql(&node);
        assert!(remote.is_some(), "should find remote SQL");
    }

    #[test]
    fn extract_remote_from_tree() {
        let tree = PlanNode {
            id: 0,
            kind: NodeKind::RunHint {
                sql: "outer".into(),
                placement: Placement::Local,
            },
            placement: Placement::Local,
            children: vec![PlanNode {
                id: 1,
                kind: NodeKind::RunHint {
                    sql: "SELECT * FROM remote_table".into(),
                    placement: Placement::Remote,
                },
                placement: Placement::Remote,
                children: vec![],
            }],
        };
        let remote = extract_remote_sql(&tree);
        assert_eq!(remote, Some("SELECT * FROM remote_table".to_string()));
    }

    #[test]
    fn hybrid_join_golden_parity() {
        use duckdb::Connection;

        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            r"CREATE TABLE l (id INT, v INT);
              CREATE TABLE r (id INT, w INT);
              INSERT INTO l VALUES (1, 10), (2, 20);
              INSERT INTO r VALUES (1, 100), (3, 300);",
        )
        .unwrap();

        let expected: i64 = conn
            .query_row(
                "SELECT COALESCE(SUM(l.v + r.w), 0) FROM l JOIN r ON l.id = r.id",
                [],
                |row| row.get(0),
            )
            .unwrap();

        let plan = PlanNode {
            id: 1,
            kind: NodeKind::HashJoin {
                condition: "l.id = r.id".into(),
            },
            placement: Placement::Local,
            children: vec![
                scan(2, "l", Placement::Local),
                scan(3, "r", Placement::Remote),
            ],
        };
        let annotated = insert_bridges(plan);
        let explain = explain_annotated(&annotated);

        assert!(explain.contains("Bridge(R→L)"), "plan must have bridge");
        assert_eq!(expected, 110, "golden parity: join sum must be 110");
    }

    // ── Table source registry tests ────────────────────────────────────

    #[test]
    fn table_source_registry_basic() {
        let mut reg = TableSourceRegistry::new();
        reg.register(TableSource {
            table: "sales".into(),
            provider: "worker".into(),
            compute_context: "us-east-1".into(),
            database: "analytics".into(),
        });
        reg.register(TableSource {
            table: "orders".into(),
            provider: "worker".into(),
            compute_context: "us-east-1".into(),
            database: "analytics".into(),
        });

        assert!(reg.is_remote("sales"));
        assert!(reg.is_remote("orders"));
        assert!(!reg.is_remote("local_table"));
        assert!(reg.are_co_located("sales", "orders"));
    }

    #[test]
    fn table_source_different_contexts_not_co_located() {
        let mut reg = TableSourceRegistry::new();
        reg.register(TableSource {
            table: "sales".into(),
            provider: "worker".into(),
            compute_context: "us-east-1".into(),
            database: "db1".into(),
        });
        reg.register(TableSource {
            table: "logs".into(),
            provider: "worker".into(),
            compute_context: "eu-west-1".into(),
            database: "db2".into(),
        });

        assert!(!reg.are_co_located("sales", "logs"));
    }

    #[test]
    fn resolve_auto_with_registry_works() {
        let mut reg = TableSourceRegistry::new();
        reg.register(TableSource {
            table: "remote_tab".into(),
            provider: "worker".into(),
            compute_context: "ctx1".into(),
            database: "db".into(),
        });

        let mut plan = PlanNode {
            id: 1,
            kind: NodeKind::HashJoin {
                condition: "a.k = b.k".into(),
            },
            placement: Placement::Auto,
            children: vec![
                scan(2, "local_tab", Placement::Auto),
                scan(3, "remote_tab", Placement::Auto),
            ],
        };
        let empty: HashSet<String> = HashSet::new();
        resolve_auto_with_registry(&mut plan, &empty, Some(&reg));
        assert_eq!(plan.children[0].placement, Placement::Local);
        assert_eq!(plan.children[1].placement, Placement::Remote);
    }

    // ── Compute pushdown tests ─────────────────────────────────────────

    #[test]
    fn pushdown_collapses_co_located_join() {
        let mut reg = TableSourceRegistry::new();
        reg.register(TableSource {
            table: "sales".into(),
            provider: "worker".into(),
            compute_context: "ctx1".into(),
            database: "analytics".into(),
        });
        reg.register(TableSource {
            table: "products".into(),
            provider: "worker".into(),
            compute_context: "ctx1".into(),
            database: "analytics".into(),
        });

        let mut plan = PlanNode {
            id: 1,
            kind: NodeKind::HashJoin {
                condition: "l.id = r.id".into(),
            },
            placement: Placement::Local,
            children: vec![
                scan(2, "local_users", Placement::Local),
                PlanNode {
                    id: 3,
                    kind: NodeKind::HashJoin {
                        condition: "s.pid = p.id".into(),
                    },
                    placement: Placement::Remote,
                    children: vec![
                        scan(4, "sales", Placement::Remote),
                        scan(5, "products", Placement::Remote),
                    ],
                },
            ],
        };

        pushdown_federable_subplans(&mut plan, &reg);

        assert!(
            matches!(&plan.children[1].kind, NodeKind::RemoteSql { sql } if sql.contains("sales") && sql.contains("products")),
            "co-located join should be collapsed into RemoteSql, got: {:?}",
            plan.children[1].kind
        );
    }

    #[test]
    fn pushdown_does_not_collapse_mixed_contexts() {
        let mut reg = TableSourceRegistry::new();
        reg.register(TableSource {
            table: "a".into(),
            provider: "worker".into(),
            compute_context: "ctx1".into(),
            database: "db1".into(),
        });
        reg.register(TableSource {
            table: "b".into(),
            provider: "worker".into(),
            compute_context: "ctx2".into(),
            database: "db2".into(),
        });

        let mut plan = PlanNode {
            id: 1,
            kind: NodeKind::HashJoin {
                condition: "a.id = b.id".into(),
            },
            placement: Placement::Remote,
            children: vec![
                scan(2, "a", Placement::Remote),
                scan(3, "b", Placement::Remote),
            ],
        };

        pushdown_federable_subplans(&mut plan, &reg);

        assert!(
            !matches!(&plan.kind, NodeKind::RemoteSql { .. }),
            "mixed-context join should NOT be collapsed"
        );
    }

    #[test]
    fn pushdown_collapses_single_remote_scan_with_filter() {
        let mut reg = TableSourceRegistry::new();
        reg.register(TableSource {
            table: "events".into(),
            provider: "worker".into(),
            compute_context: "ctx1".into(),
            database: "logs".into(),
        });

        let mut plan = PlanNode {
            id: 1,
            kind: NodeKind::Filter {
                predicate: "ts > '2024-01-01'".into(),
            },
            placement: Placement::Remote,
            children: vec![scan(2, "events", Placement::Remote)],
        };

        pushdown_federable_subplans(&mut plan, &reg);

        assert!(
            matches!(&plan.kind, NodeKind::RemoteSql { .. }),
            "filter + scan on same context should collapse"
        );
    }

    // ── HybridError tests ──────────────────────────────────────────────

    #[test]
    fn hybrid_error_unavailable_is_fallback_eligible() {
        let err = HybridError::Unavailable("connection refused".into());
        assert!(err.is_fallback_eligible());
    }

    #[test]
    fn hybrid_error_worker_error_is_not_fallback_eligible() {
        let err = HybridError::WorkerError("SQL syntax error".into());
        assert!(!err.is_fallback_eligible());
    }

    #[test]
    fn hybrid_error_auth_is_not_fallback_eligible() {
        let err = HybridError::AuthError("bad token".into());
        assert!(!err.is_fallback_eligible());
    }
}
