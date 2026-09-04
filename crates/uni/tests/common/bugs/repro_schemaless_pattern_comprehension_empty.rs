// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! A pattern comprehension over an undeclared edge type yields nothing after the
//! store is reopened.
//!
//! `[ (a)-[e:T]-(x) | e ]` returns `[]` where `T` was never declared with
//! `CREATE EDGE TYPE`, once the store has been closed and reopened. The plain
//! traversal `MATCH (a)-[e:T]-(x)` finds the edge in the same session, so the
//! edge is present and reachable — the comprehension's own expansion is what
//! comes back empty.
//!
//! The bug is invisible in the obvious test: in a single session, before any
//! reopen, the comprehension works. It reproduces through the CLI because every
//! invocation is its own process, so the query always runs against a reopened
//! store.
//!
//! This is a silent wrong answer. An empty list is a legitimate result for a
//! node with no matching edges, so nothing at the call site distinguishes it
//! from the correct answer.
//!
//! Found while fixing #193. Unrelated to it, and present before that fix —
//! confirmed by reverting the #193 planner change and reproducing.

use uni_db::{Uni, Value};

/// `a` -[:LIKES]-> `b`, with no `CREATE EDGE TYPE`.
async fn write_fixture(db: &Uni) {
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'})")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {name:'a'}), (y:P {name:'b'}) CREATE (x)-[:LIKES]->(y)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

/// How many items the comprehension yields, and how many rows the equivalent
/// traversal yields. They must agree.
async fn traversal_and_comprehension(db: &Uni) -> (usize, usize) {
    let t = db
        .session()
        .query("MATCH (a:P {name:'b'})-[e:LIKES]-(x) RETURN e")
        .await
        .unwrap();
    let c = db
        .session()
        .query("MATCH (a:P {name:'b'}) RETURN [ (a)-[e:LIKES]-(x) | e ] AS es")
        .await
        .unwrap();
    let clen = match &c.rows()[0].values()[0] {
        Value::List(items) => items.len(),
        other => panic!("expected a List, got {other:?}"),
    };
    (t.rows().len(), clen)
}

/// Control: within one session the two agree, even across a flush.
///
/// This is what makes the failure below a finding rather than a broken fixture —
/// the same query pair passes here, so neither the data nor the comprehension
/// syntax is at fault.
#[tokio::test]
async fn schemaless_comprehension_agrees_with_traversal_in_one_session() {
    let db = Uni::in_memory().build().await.unwrap();
    write_fixture(&db).await;
    assert_eq!(traversal_and_comprehension(&db).await, (1, 1));
    db.flush().await.unwrap();
    assert_eq!(
        traversal_and_comprehension(&db).await,
        (1, 1),
        "a flush alone does not break it"
    );
}

/// After a close and reopen, the comprehension yields nothing.
#[tokio::test]
#[ignore = "open defect: schemaless pattern comprehension is empty after reopen"]
async fn schemaless_comprehension_survives_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store");
    let path = path.to_str().unwrap();

    {
        let db = Uni::open(path).build().await.unwrap();
        write_fixture(&db).await;
        db.shutdown().await.unwrap();
    }

    let db = Uni::open(path).build().await.unwrap();
    assert_eq!(
        traversal_and_comprehension(&db).await,
        (1, 1),
        "the traversal still finds the edge; the comprehension must too"
    );
}
