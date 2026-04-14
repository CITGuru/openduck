//! # Compute Pushdown Example
//!
//! Demonstrates how the federation optimizer identifies co-located remote
//! tables, collapses their sub-plans into a single `RemoteSql` node, and
//! resolves `Auto` placement using `TableSourceRegistry`.
//!
//! No servers needed — this is pure plan manipulation.
//!
//! ```bash
//! cargo run --example compute_pushdown
//! ```

use std::collections::HashSet;

use exec_gateway::hybrid::*;

fn main() {
    println!("=== OpenDuck Compute Pushdown Example ===\n");

    // ── 1. Register remote tables with origin metadata ───────────────────
    println!("── 1. Table source registry ──\n");

    let mut registry = TableSourceRegistry::new();
    registry.register(TableSource {
        table: "sales".into(),
        provider: "openduck-worker".into(),
        compute_context: "us-east-1-analytics".into(),
        database: "analytics".into(),
    });
    registry.register(TableSource {
        table: "products".into(),
        provider: "openduck-worker".into(),
        compute_context: "us-east-1-analytics".into(),
        database: "analytics".into(),
    });
    registry.register(TableSource {
        table: "eu_logs".into(),
        provider: "openduck-worker".into(),
        compute_context: "eu-west-1-logs".into(),
        database: "logging".into(),
    });

    println!("   Registered tables:");
    println!("     sales    → us-east-1-analytics");
    println!("     products → us-east-1-analytics");
    println!("     eu_logs  → eu-west-1-logs");
    println!();

    println!(
        "   sales & products co-located? {}",
        registry.are_co_located("sales", "products")
    );
    println!(
        "   sales & eu_logs co-located?  {}",
        registry.are_co_located("sales", "eu_logs")
    );
    println!();

    // ── 2. AUTO placement with registry ──────────────────────────────────
    println!("── 2. AUTO placement via TableSourceRegistry ──\n");

    let mut plan = PlanNode {
        id: 1,
        kind: NodeKind::HashJoin {
            condition: "u.id = s.customer_id".into(),
        },
        placement: Placement::Auto,
        children: vec![
            PlanNode {
                id: 2,
                kind: NodeKind::Scan {
                    table: "local_users".into(),
                },
                placement: Placement::Auto,
                children: vec![],
            },
            PlanNode {
                id: 3,
                kind: NodeKind::Scan {
                    table: "sales".into(),
                },
                placement: Placement::Auto,
                children: vec![],
            },
        ],
    };

    println!("   Before resolve_auto_with_registry:");
    println!("{}", indent_explain(&plan));

    let empty: HashSet<String> = HashSet::new();
    resolve_auto_with_registry(&mut plan, &empty, Some(&registry));

    println!("   After resolve_auto_with_registry:");
    println!("{}", indent_explain(&plan));

    // ── 3. Compute pushdown: co-located remote join ──────────────────────
    println!("── 3. Compute pushdown (co-located tables) ──\n");

    let mut mixed_plan = PlanNode {
        id: 1,
        kind: NodeKind::HashJoin {
            condition: "u.id = agg.customer_id".into(),
        },
        placement: Placement::Local,
        children: vec![
            PlanNode {
                id: 2,
                kind: NodeKind::Scan {
                    table: "local_users".into(),
                },
                placement: Placement::Local,
                children: vec![],
            },
            PlanNode {
                id: 3,
                kind: NodeKind::HashJoin {
                    condition: "s.product_id = p.id".into(),
                },
                placement: Placement::Remote,
                children: vec![
                    PlanNode {
                        id: 4,
                        kind: NodeKind::Filter {
                            predicate: "s.amount > 50".into(),
                        },
                        placement: Placement::Remote,
                        children: vec![PlanNode {
                            id: 5,
                            kind: NodeKind::Scan {
                                table: "sales".into(),
                            },
                            placement: Placement::Remote,
                            children: vec![],
                        }],
                    },
                    PlanNode {
                        id: 6,
                        kind: NodeKind::Scan {
                            table: "products".into(),
                        },
                        placement: Placement::Remote,
                        children: vec![],
                    },
                ],
            },
        ],
    };

    println!("   Before pushdown (5 nodes, 3 remote):");
    println!("{}", indent_explain(&mixed_plan));

    pushdown_federable_subplans(&mut mixed_plan, &registry);

    println!("   After pushdown (remote join collapsed into RemoteSql):");
    println!("{}", indent_explain(&mixed_plan));

    // ── 4. Mixed contexts: pushdown does NOT collapse ────────────────────
    println!("── 4. Mixed contexts — no collapse ──\n");

    let mut cross_region = PlanNode {
        id: 1,
        kind: NodeKind::HashJoin {
            condition: "s.id = l.sale_id".into(),
        },
        placement: Placement::Remote,
        children: vec![
            PlanNode {
                id: 2,
                kind: NodeKind::Scan {
                    table: "sales".into(),
                },
                placement: Placement::Remote,
                children: vec![],
            },
            PlanNode {
                id: 3,
                kind: NodeKind::Scan {
                    table: "eu_logs".into(),
                },
                placement: Placement::Remote,
                children: vec![],
            },
        ],
    };

    println!("   Before pushdown (sales=us-east-1, eu_logs=eu-west-1):");
    println!("{}", indent_explain(&cross_region));

    pushdown_federable_subplans(&mut cross_region, &registry);

    println!("   After pushdown (individual scans collapsed, join stays):");
    println!("{}", indent_explain(&cross_region));

    // ── 5. Full pipeline: resolve → pushdown → bridges ───────────────────
    println!("── 5. Full pipeline: AUTO → pushdown → bridges ──\n");

    let mut full_plan = PlanNode {
        id: 1,
        kind: NodeKind::HashJoin {
            condition: "u.id = r.customer_id".into(),
        },
        placement: Placement::Auto,
        children: vec![
            PlanNode {
                id: 2,
                kind: NodeKind::Scan {
                    table: "local_users".into(),
                },
                placement: Placement::Auto,
                children: vec![],
            },
            PlanNode {
                id: 3,
                kind: NodeKind::Aggregate {
                    keys: vec!["customer_id".into(), "SUM(amount)".into()],
                },
                placement: Placement::Auto,
                children: vec![PlanNode {
                    id: 4,
                    kind: NodeKind::Scan {
                        table: "sales".into(),
                    },
                    placement: Placement::Auto,
                    children: vec![],
                }],
            },
        ],
    };

    println!("   Step 1 — raw plan (all AUTO):");
    println!("{}", indent_explain(&full_plan));

    resolve_auto_with_registry(&mut full_plan, &empty, Some(&registry));
    println!("   Step 2 — after AUTO resolution:");
    println!("{}", indent_explain(&full_plan));

    pushdown_federable_subplans(&mut full_plan, &registry);
    println!("   Step 3 — after compute pushdown:");
    println!("{}", indent_explain(&full_plan));

    let final_plan = insert_bridges(full_plan);
    println!("   Step 4 — after bridge insertion:");
    println!("{}", indent_explain(&final_plan));

    println!("=== Done ===");
}

fn indent_explain(node: &PlanNode) -> String {
    explain_annotated(node)
        .lines()
        .map(|l| format!("     {l}"))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}
