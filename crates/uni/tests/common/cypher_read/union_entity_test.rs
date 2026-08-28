// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `UNION` over a whole node or relationship (#190).
//!
//! `MATCH (a:P)-[:KNOWS]->(b:P) RETURN b AS n` returned a node. The same query
//! `UNION ALL`-ed **with itself** returned `[List(["P"]), Null]` — the node's
//! `_labels` list, then a null. No error, and both values well-formed enough to
//! flow onward.
//!
//! The issue guessed at positional column misalignment inside the union. It was
//! not that, and the batches were never wrong: dumping them at the read boundary
//! showed column `n` holding a correct struct on both sides. The defect was one
//! layer further out, in *naming* the result's columns.
//!
//! `extract_projection_order` had no `Union` arm and no `Distinct` arm, so both
//! shapes fell through its catch-all to `columns_for_results`, which "falls back
//! to the first row's keys, sorted". A traversal's rows carry internal helper
//! columns beside the projected one, so the sorted keys were
//! `["b._labels", "b._vid", "b.name", "n"]` and column 0 — the only one the
//! caller reads — was `b._labels`. The second row came from the other branch,
//! whose keys are `d.*`, so it had no `b._labels` at all and rendered as `Null`.
//!
//! Two things follow, and both are pinned below.
//!
//! **The union of two scans passed by luck, not by correctness.** Its helper
//! prefix is `z`, which sorts *after* `n`, so the guess landed on the right
//! column. Rename the variable and the same query breaks. A test written only
//! against that shape would have reported the feature working.
//!
//! **`UNION` was never the requirement.** `RETURN DISTINCT b AS n` reaches the
//! same catch-all with no union anywhere, and returned the same labels list.
//! That is the more common query of the two.
//!
//! The fix is one canonical `projection_columns` in the planner, with an
//! exhaustive match over `LogicalPlan`. A second, independent copy of this
//! logic already existed there and *did* handle `Union` and `Distinct`; the
//! two disagreed, and the disagreement was the bug.

use uni_db::{Uni, Value};

/// `a` -[:KNOWS]-> `b`, plus an isolated `c` so scans and traversals return
/// different row counts and a test cannot pass by conflating them.
async fn fixture() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE EDGE TYPE KNOWS FROM P TO P")
        .await
        .unwrap();
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'}), (:P {name:'c'})")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {name:'a'}), (y:P {name:'b'}) CREATE (x)-[:KNOWS]->(y)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

/// Assert every value is a node named `name`, and that the result advertises
/// exactly the columns the query asked for.
fn assert_all_nodes_named(rows: &[Value], name: &str) {
    for v in rows {
        match v {
            Value::Node(n) => assert_eq!(
                n.properties.get("name"),
                Some(&Value::String(name.to_string())),
                "node came back without its properties: {n:?}"
            ),
            other => panic!("expected a Node, got {other:?}"),
        }
    }
}

fn column_zero(r: &uni_db::QueryResult) -> Vec<Value> {
    r.rows().iter().map(|row| row.values()[0].clone()).collect()
}

/// The reported repro. Asserted on properties, not on `id()` alone — a node
/// stripped of its properties would still satisfy an id-only assertion.
#[tokio::test]
async fn a_traversal_bound_node_survives_union_with_itself() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (a:P)-[:KNOWS]->(b:P) RETURN b AS n \
             UNION ALL MATCH (c:P)-[:KNOWS]->(d:P) RETURN d AS n",
        )
        .await
        .unwrap();
    assert_eq!(r.columns(), &["n".to_string()]);
    let vals = column_zero(&r);
    assert_eq!(vals.len(), 2);
    assert_all_nodes_named(&vals, "b");
}

/// The relationship form, which lost the value entirely rather than swapping it.
#[tokio::test]
async fn a_traversal_bound_relationship_survives_union_with_itself() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH ()-[e:KNOWS]->() RETURN e AS r \
             UNION ALL MATCH ()-[f:KNOWS]->() RETURN f AS r",
        )
        .await
        .unwrap();
    assert_eq!(r.columns(), &["r".to_string()]);
    let vals = column_zero(&r);
    assert_eq!(vals.len(), 2);
    for v in &vals {
        match v {
            Value::Edge(e) => assert_eq!(e.edge_type, "KNOWS"),
            other => panic!("expected an Edge, got {other:?}"),
        }
    }
}

/// `UNION` (deduplicating) is a different physical path from `UNION ALL` — it
/// coalesces and groups on every column — so it needs its own case.
#[tokio::test]
async fn the_deduplicating_union_also_returns_nodes() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (a:P)-[:KNOWS]->(b:P) RETURN b AS n \
             UNION MATCH (c:P)-[:KNOWS]->(d:P) RETURN d AS n",
        )
        .await
        .unwrap();
    let vals = column_zero(&r);
    assert_eq!(vals.len(), 1, "both branches yield the same node");
    assert_all_nodes_named(&vals, "b");
}

/// No union in this query at all. `RETURN DISTINCT <node>` reached the same
/// catch-all and returned the same wrong value, which makes the union framing
/// of #190 too narrow.
#[tokio::test]
async fn a_distinct_projection_of_a_traversal_bound_node_returns_the_node() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH (a:P)-[:KNOWS]->(b:P) RETURN DISTINCT b AS n")
        .await
        .unwrap();
    assert_eq!(r.columns(), &["n".to_string()]);
    assert_all_nodes_named(&column_zero(&r), "b");
}

/// The union of two *scans* returned the right answer before the fix, but only
/// because its helper columns are prefixed `z`, which sorts after `n`. Binding
/// the same scan to `a` puts the helpers first and reproduces the original
/// failure — so this pins the luck, not just the behaviour.
#[tokio::test]
async fn a_scan_bound_node_survives_union_under_a_variable_that_sorts_first() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH (a:P) RETURN a AS n UNION ALL MATCH (a2:P) RETURN a2 AS n")
        .await
        .unwrap();
    assert_eq!(r.columns(), &["n".to_string()]);
    let vals = column_zero(&r);
    assert_eq!(vals.len(), 6, "3 nodes from each branch");
    for v in &vals {
        assert!(matches!(v, Value::Node(_)), "expected a Node, got {v:?}");
    }
}

/// Control: a property through the same union worked before the fix and must
/// keep working, so a later regression cannot be misread as this bug returning.
#[tokio::test]
async fn a_property_through_a_union_is_unaffected() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (a:P)-[:KNOWS]->(b:P) RETURN b.name AS n \
             UNION ALL MATCH (c:P)-[:KNOWS]->(d:P) RETURN d.name AS n",
        )
        .await
        .unwrap();
    assert_eq!(
        column_zero(&r),
        vec![
            Value::String("b".to_string()),
            Value::String("b".to_string())
        ]
    );
}

/// Control: the same node without a union. If this ever fails, the defect is
/// upstream of anything #190 touched.
#[tokio::test]
async fn a_traversal_bound_node_without_a_union_is_unaffected() {
    let db = fixture().await;
    let r = db
        .session()
        .query("MATCH (a:P)-[:KNOWS]->(b:P) RETURN b AS n")
        .await
        .unwrap();
    assert_eq!(r.columns(), &["n".to_string()]);
    assert_all_nodes_named(&column_zero(&r), "b");
}

/// Multiple columns must keep their **query order**, not sorted order — the
/// old fallback sorted, so a two-column union could also transpose values.
#[tokio::test]
async fn union_preserves_declared_column_order() {
    let db = fixture().await;
    let r = db
        .session()
        .query(
            "MATCH (a:P)-[:KNOWS]->(b:P) RETURN b.name AS zeta, a.name AS alpha \
             UNION ALL MATCH (c:P)-[:KNOWS]->(d:P) RETURN d.name AS zeta, c.name AS alpha",
        )
        .await
        .unwrap();
    assert_eq!(r.columns(), &["zeta".to_string(), "alpha".to_string()]);
    assert_eq!(r.rows()[0].values()[0], Value::String("b".to_string()));
    assert_eq!(r.rows()[0].values()[1], Value::String("a".to_string()));
}
