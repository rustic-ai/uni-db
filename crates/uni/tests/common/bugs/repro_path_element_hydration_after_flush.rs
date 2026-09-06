// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A path's elements lose their type and labels once the data is flushed.
//!
//! `MATCH p = (a)-[:T]->(b) RETURN p` returns a path whose relationship has an
//! empty `edge_type` and whose nodes have no labels, as soon as the edge and
//! the vertices have left the L0 write buffers. The same query answers
//! correctly in the session that wrote them, which is why it survived: every
//! test that builds its fixture and queries it in one go passes.
//!
//! Both halves are silent. An empty label list and an empty type name are
//! representable values, so nothing at the call site distinguishes them from a
//! correct answer — and a node returned this way compares unequal to the same
//! node returned by an ordinary `MATCH`.
//!
//! ## Why each half happened
//!
//! **The relationship type.** `BindFixedPathExec` read it from the operator's
//! `{var}._type` column. That column exists only for a *named* relationship: an
//! anonymous one is carried by a bare `__eid_to_<target>` column with no
//! sibling `_type`. The only remaining source was the L0 visibility chain,
//! which answers for a resident edge and nothing else. Two sources were
//! available and unused — the adjacency probe already runs to recover the
//! edge's stored orientation and identifies its type as a by-product
//! (`resolve_stored_edge` reports it; `resolve_stored_edge_endpoints`, which
//! the operator called, drops it), and the pattern itself names the type.
//!
//! **The node labels.** `append_node_to_struct_with` read labels from the L0
//! chain only, with no storage fallback, while properties beside them had one
//! (`EntityPropertyCache`). The operator's `{var}._labels` column was right
//! there in the batch and was not consulted.
//!
//! Found while fixing the schemaless registry defect
//! (rustic-ai/uni-db#253), by probing how far that symptom reached. It is a
//! separate defect: it needs neither an undeclared type nor a reopen, only a
//! flush, and it predates that fix.

use uni_db::{Uni, Value};

/// `a -[:KNOWS]-> b` with `KNOWS` declared, and `a -[:LIKES]-> b` without.
///
/// Both are present so the assertions cover a declared and an undeclared type
/// in one fixture — the two are planned by different operators, and only one of
/// them warms the adjacency the probe consults.
async fn write_fixture(db: &Uni) {
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE EDGE TYPE KNOWS FROM P TO P")
        .await
        .unwrap();
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'})")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {name:'a'}), (y:P {name:'b'}) CREATE (x)-[:LIKES]->(y)")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {name:'a'}), (y:P {name:'b'}) CREATE (x)-[:KNOWS]->(y)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

/// The relationship types and node labels the query's paths carry.
async fn path_shapes(db: &Uni, query: &str) -> (Vec<String>, Vec<Vec<String>>) {
    let result = db.session().query(query).await.unwrap();
    let mut types = Vec::new();
    let mut labels = Vec::new();
    for row in result.rows() {
        match &row.values()[0] {
            Value::Path(p) => {
                types.extend(p.edges.iter().map(|e| e.edge_type.clone()));
                labels.extend(p.nodes.iter().map(|n| n.labels.clone()));
            }
            other => panic!("expected a Path from `{query}`, got {other:?}"),
        }
    }
    (types, labels)
}

/// Every way of writing the relationship reports the same path.
///
/// The four cases are the two axes that mattered: whether the pattern names the
/// relationship variable (which decides if a `_type` column exists at all) and
/// whether the type is declared (which decides which operator plans the hop).
/// Before the fix, the anonymous rows returned `""` for the type; all four
/// returned no labels.
async fn assert_every_form_agrees(db: &Uni, when: &str) {
    for (query, expected) in [
        ("MATCH p = (a:P {name:'a'})-[:KNOWS]->() RETURN p", "KNOWS"),
        ("MATCH p = (a:P {name:'a'})-[r:KNOWS]->() RETURN p", "KNOWS"),
        ("MATCH p = (a:P {name:'a'})-[:LIKES]->() RETURN p", "LIKES"),
        ("MATCH p = (a:P {name:'a'})-[r:LIKES]->() RETURN p", "LIKES"),
    ] {
        let (types, labels) = path_shapes(db, query).await;
        assert_eq!(types, vec![expected.to_string()], "{when}: `{query}`");
        assert_eq!(
            labels,
            vec![vec!["P".to_string()], vec!["P".to_string()]],
            "{when}: `{query}`"
        );
    }

    // Untyped, which matches both edges and so pins that the type reported is
    // the edge's own rather than the one the pattern happened to name.
    let (mut types, labels) = path_shapes(db, "MATCH p = (a:P {name:'a'})-[]->() RETURN p").await;
    types.sort();
    assert_eq!(
        types,
        vec!["KNOWS".to_string(), "LIKES".to_string()],
        "{when}: untyped"
    );
    assert!(
        labels.iter().all(|l| l == &vec!["P".to_string()]),
        "{when}: untyped labels were {labels:?}"
    );
}

/// Control: correct while the data is still resident in L0.
///
/// This is what made the defect invisible — it is the shape almost every test
/// uses, and it passed throughout.
#[tokio::test]
async fn path_elements_are_hydrated_before_a_flush() {
    let db = Uni::in_memory().build().await.unwrap();
    write_fixture(&db).await;
    assert_every_form_agrees(&db, "before flush").await;
}

/// A flush alone is enough to lose them; no reopen is required.
#[tokio::test]
async fn path_elements_survive_a_flush() {
    let db = Uni::in_memory().build().await.unwrap();
    write_fixture(&db).await;
    db.flush().await.unwrap();
    assert_every_form_agrees(&db, "after flush").await;
}

/// And they survive the process exiting.
#[tokio::test]
async fn path_elements_survive_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let store = dir.path().join("store");
    let path = store.to_str().unwrap();

    {
        let db = Uni::open(path).build().await.unwrap();
        write_fixture(&db).await;
        db.shutdown().await.unwrap();
    }

    let db = Uni::open(path).build().await.unwrap();
    assert_every_form_agrees(&db, "after reopen").await;
}

/// Every constructor that builds a node value reports its labels after a flush.
///
/// The label read was `l0_visibility::get_vertex_labels`, which answers only
/// for a resident vertex and returns an empty list otherwise. Six constructors
/// shared it — fixed-length paths, zero-length paths, `shortestPath`,
/// variable-length paths, quantified patterns, and pattern comprehensions —
/// so a flushed node came back unlabelled from all of them while an ordinary
/// `MATCH` on the same vertex was correct. The last row below is that control:
/// it passed throughout and is what makes the others a defect rather than a
/// missing feature.
///
/// `GraphExecutionContext::resolve_vertex_labels` (L0 chain, then the in-memory
/// `VidLabelsIndex`) already existed and answered correctly; none of the six
/// called it.
#[tokio::test]
async fn every_node_constructor_reports_labels_after_a_flush() {
    let db = Uni::in_memory().build().await.unwrap();
    write_fixture(&db).await;
    db.flush().await.unwrap();

    // Each query returns node values by a different construction path.
    for query in [
        // Pattern comprehension projecting a whole node.
        "MATCH (a:P {name:'a'}) RETURN [ (a)-[:KNOWS]->(x) | x ] AS xs",
        // Zero-length path.
        "MATCH p = (a:P {name:'a'}) RETURN p",
        // shortestPath.
        "MATCH (a:P {name:'a'}), (b:P {name:'b'}) \
         MATCH p = shortestPath((a)-[*..3]-(b)) RETURN p",
        // Variable-length path.
        "MATCH p = (a:P {name:'a'})-[:KNOWS*1..2]->() RETURN p",
        // Fixed-length path.
        "MATCH p = (a:P {name:'a'})-[:KNOWS]->() RETURN p",
        // Control: the ordinary projection, correct before and after.
        "MATCH (a:P {name:'a'}) RETURN a",
    ] {
        let result = db.session().query(query).await.unwrap();
        let mut seen = 0usize;
        for row in result.rows() {
            for value in row.values() {
                for node in collect_nodes(value) {
                    seen += 1;
                    assert_eq!(
                        node,
                        vec!["P".to_string()],
                        "`{query}` returned a node with labels {node:?}"
                    );
                }
            }
        }
        assert!(seen > 0, "`{query}` returned no nodes to check");
    }
}

/// The labels of every node reachable inside `value`, however it is wrapped.
fn collect_nodes(value: &Value) -> Vec<Vec<String>> {
    match value {
        Value::Node(n) => vec![n.labels.clone()],
        Value::Path(p) => p.nodes.iter().map(|n| n.labels.clone()).collect(),
        Value::List(items) => items.iter().flat_map(collect_nodes).collect(),
        _ => Vec::new(),
    }
}
