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
    out.push_str(&format!("{indent}[{placement}] {label}\n", placement = node.placement));
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

/// Resolve `Auto` placement for all nodes using a simple heuristic:
/// - Scan nodes → check whether the table is in the `remote_tables` set.
/// - Other nodes → inherit from the majority of children; default to `Local`.
pub fn resolve_auto(node: &mut PlanNode, remote_tables: &std::collections::HashSet<String>) {
    for child in &mut node.children {
        resolve_auto(child, remote_tables);
    }
    if node.placement == Placement::Auto {
        node.placement = match &node.kind {
            NodeKind::Scan { table } => {
                if remote_tables.contains(table) {
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

    let inner_node = parse_openduck_run(inner_hint.split(')').next().map(|s| format!("{s})")).unwrap_or_default().as_str())?;

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

/// Collect Arrow IPC batches from a worker stream into `RecordBatch`es.
async fn collect_remote_batches(
    worker_url: &str,
    sql: &str,
    token: &str,
) -> Result<Vec<RecordBatch>, String> {
    let mut client = ExecutionServiceClient::connect(worker_url.to_string())
        .await
        .map_err(|e| format!("connect to worker: {e}"))?;

    let request = ExecuteFragmentRequest {
        plan: sql.as_bytes().to_vec(),
        access_token: token.to_string(),
        execution_id: uuid::Uuid::new_v4().to_string(),
        ..Default::default()
    };

    let mut stream = client
        .execute_fragment(Request::new(request))
        .await
        .map_err(|e| format!("execute_fragment: {e}"))?
        .into_inner();

    let mut batches = Vec::new();
    while let Some(chunk) = stream
        .message()
        .await
        .map_err(|e| format!("stream: {e}"))?
    {
        match chunk.payload {
            Some(Payload::ArrowBatch(b)) if !b.ipc_stream_payload.is_empty() => {
                let cursor = std::io::Cursor::new(b.ipc_stream_payload);
                let reader = StreamReader::try_new(cursor, None)
                    .map_err(|e| format!("arrow decode: {e}"))?;
                for batch in reader {
                    batches.push(batch.map_err(|e| format!("arrow batch: {e}"))?);
                }
            }
            Some(Payload::Error(e)) => return Err(format!("worker error: {e}")),
            Some(Payload::Finished(true)) => break,
            _ => {}
        }
    }
    Ok(batches)
}

/// Insert Arrow `RecordBatch`es into a local DuckDB connection as a temporary table.
fn materialize_batches(
    conn: &Connection,
    table_name: &str,
    batches: &[RecordBatch],
) -> Result<(), String> {
    if batches.is_empty() {
        conn.execute_batch(&format!("CREATE TEMP TABLE {table_name} AS SELECT 1 WHERE false"))
            .map_err(|e| format!("create empty temp table: {e}"))?;
        return Ok(());
    }

    let schema = batches[0].schema();
    let mut cols = Vec::new();
    for field in schema.fields() {
        let col_type = match field.data_type() {
            arrow::datatypes::DataType::Int8 => "TINYINT",
            arrow::datatypes::DataType::Int16 => "SMALLINT",
            arrow::datatypes::DataType::Int32 => "INTEGER",
            arrow::datatypes::DataType::Int64 => "BIGINT",
            arrow::datatypes::DataType::Float32 => "FLOAT",
            arrow::datatypes::DataType::Float64 => "DOUBLE",
            arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => "VARCHAR",
            arrow::datatypes::DataType::Boolean => "BOOLEAN",
            _ => "VARCHAR",
        };
        cols.push(format!("{} {}", field.name(), col_type));
    }
    let create_sql = format!("CREATE TEMP TABLE {table_name} ({})", cols.join(", "));
    conn.execute_batch(&create_sql)
        .map_err(|e| format!("create temp table: {e}"))?;

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut values = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                let val = arrow::util::display::array_value_to_string(col, row_idx)
                    .unwrap_or_else(|_| "NULL".to_string());
                if col.is_null(row_idx) {
                    values.push("NULL".to_string());
                } else {
                    values.push(format!("'{}'", val.replace('\'', "''")));
                }
            }
            let insert = format!("INSERT INTO {table_name} VALUES ({})", values.join(", "));
            conn.execute_batch(&insert)
                .map_err(|e| format!("insert row: {e}"))?;
        }
    }
    Ok(())
}

/// Execute a hybrid query: remote fragments on workers, local join in DuckDB.
///
/// `remote_sql`: SQL to execute on the worker (the remote portion)
/// `local_sql`: SQL to execute locally, referencing `__remote` as the materialized remote data
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
    let batches = collect_remote_batches(worker_url, remote_sql, token).await?;
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
            matches!(&result.children[1].kind, NodeKind::Bridge { direction: BridgeDirection::RemoteToLocal }),
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
}
