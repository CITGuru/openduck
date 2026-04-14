//! # Hybrid Plan Example
//!
//! Builds a query plan with LOCAL and REMOTE scans, automatically inserts
//! bridge operators at placement boundaries, resolves AUTO placement,
//! and prints an annotated EXPLAIN.
//!
//! ```bash
//! cargo run --example hybrid_plan
//! ```

use std::collections::HashSet;

use exec_gateway::hybrid::*;

fn main() {
    println!("=== OpenDuck Hybrid Plan Example ===\n");

    // 1. Build a plan with mixed placement.
    let plan = PlanNode {
        id: 1,
        kind: NodeKind::HashJoin {
            condition: "orders.customer_id = customers.id".into(),
        },
        placement: Placement::Local,
        children: vec![
            PlanNode {
                id: 2,
                kind: NodeKind::Scan {
                    table: "customers".into(),
                },
                placement: Placement::Local,
                children: vec![],
            },
            PlanNode {
                id: 3,
                kind: NodeKind::Aggregate {
                    keys: vec!["customer_id".into()],
                },
                placement: Placement::Remote,
                children: vec![PlanNode {
                    id: 4,
                    kind: NodeKind::Filter {
                        predicate: "amount > 100".into(),
                    },
                    placement: Placement::Remote,
                    children: vec![PlanNode {
                        id: 5,
                        kind: NodeKind::Scan {
                            table: "orders".into(),
                        },
                        placement: Placement::Remote,
                        children: vec![],
                    }],
                }],
            },
        ],
    };

    println!("--- Before bridge insertion ---");
    println!("{}", explain_annotated(&plan));

    // 2. Insert bridges at L/R boundaries.
    let bridged = insert_bridges(plan);
    println!("--- After bridge insertion ---");
    println!("{}", explain_annotated(&bridged));

    // 3. Demonstrate AUTO resolution.
    let mut auto_plan = PlanNode {
        id: 10,
        kind: NodeKind::HashJoin {
            condition: "a.id = b.id".into(),
        },
        placement: Placement::Auto,
        children: vec![
            PlanNode {
                id: 11,
                kind: NodeKind::Scan {
                    table: "local_cache".into(),
                },
                placement: Placement::Auto,
                children: vec![],
            },
            PlanNode {
                id: 12,
                kind: NodeKind::Scan {
                    table: "warehouse_events".into(),
                },
                placement: Placement::Auto,
                children: vec![],
            },
        ],
    };

    let remote_tables: HashSet<String> = ["warehouse_events".to_string()].into();
    resolve_auto(&mut auto_plan, &remote_tables);

    println!("--- AUTO resolved ---");
    let resolved_bridged = insert_bridges(auto_plan);
    println!("{}", explain_annotated(&resolved_bridged));

    // 4. Parse an openduck_run hint.
    let hint = "openduck_run('REMOTE', 'SELECT sum(revenue) FROM sales')";
    println!("--- openduck_run hint ---");
    if let Some(node) = parse_openduck_run(hint) {
        println!("{}", explain_annotated(&node));
    }
}
