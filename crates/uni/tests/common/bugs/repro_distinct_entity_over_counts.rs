// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `RETURN DISTINCT n` and `count(DISTINCT n)` count one node twice.
//!
//! When the same vertex reaches an `UNWIND` from two producers — a `collect()`
//! over a traversal and a pattern comprehension — both planner-side DISTINCT
//! paths report two, while `collect(DISTINCT n)` reports one. Same query, same
//! row set, three dedup mechanisms, two answers.
//!
//! The decoded rows are identical: same vid, same labels, same properties. What
//! differs is a *hidden* column. `RETURN DISTINCT n._vid` returns `Int(1)` and
//! `Null` — so one producer's entity yields no `_vid` at all, and the group-by
//! that backs DISTINCT is separating rows on that NULL rather than on anything
//! visible in the result.
//!
//! Recorded rather than fixed, and the distinction matters: the planner's
//! group-by is behaving correctly *given its input*. The defect is upstream, in
//! whatever emits a NULL `_vid` for a natively-encoded entity. Rewriting the
//! DISTINCT operator to group on identity would hide that NULL rather than fix
//! it, and the same NULL is presumably visible to anything else reading that
//! column. Pin first, attribute second (#234, #235).

use uni_db::{Uni, Value};

async fn fixture() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let tx = db.session().tx().await.unwrap();
    tx.execute("CREATE LABEL P (name STRING)").await.unwrap();
    tx.execute("CREATE EDGE TYPE KNOWS FROM P TO P")
        .await
        .unwrap();
    tx.execute("CREATE (:P {name:'a'}), (:P {name:'b'})")
        .await
        .unwrap();
    tx.execute("MATCH (x:P {name:'a'}), (y:P {name:'b'}) CREATE (x)-[:KNOWS]->(y)")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db
}

/// One vertex reached twice, via `collect()` and via a pattern comprehension.
const TWO_PRODUCERS: &str = "MATCH (a:P {name:'a'})-[:KNOWS]->(x) \
     WITH a, collect(x) AS trav \
     WITH trav + [(a)-[:KNOWS]->(y) | y] AS both \
     UNWIND both AS n ";

/// Control: the list really does hold two references to the one vertex, and
/// `collect(DISTINCT n)` collapses them. Without this the failures below could
/// be a fixture that never produced a duplicate.
#[tokio::test]
async fn collect_distinct_collapses_the_duplicate() {
    let db = fixture().await;
    let r = db
        .session()
        .query(&format!(
            "{TWO_PRODUCERS}RETURN count(n) AS total, size(collect(DISTINCT n)) AS uniq"
        ))
        .await
        .unwrap();
    let vals = r.rows()[0].values();
    assert_eq!(vals[0], Value::Int(2), "two references to the one vertex");
    assert_eq!(
        vals[1],
        Value::Int(1),
        "collect(DISTINCT) already collapses them"
    );
}

/// `RETURN DISTINCT n` must yield the vertex once.
#[tokio::test]
#[ignore = "open defect: DISTINCT separates rows on a hidden NULL _vid column"]
async fn return_distinct_yields_one_row_per_vertex() {
    let db = fixture().await;
    let r = db
        .session()
        .query(&format!("{TWO_PRODUCERS}RETURN DISTINCT n"))
        .await
        .unwrap();
    assert_eq!(r.rows().len(), 1, "one vertex, one row");
}

/// `count(DISTINCT n)` must agree with `collect(DISTINCT n)`.
#[tokio::test]
#[ignore = "open defect: count(DISTINCT) falls back to byte comparison for an UNWIND-rebound entity"]
async fn count_distinct_agrees_with_collect_distinct() {
    let db = fixture().await;
    let r = db
        .session()
        .query(&format!(
            "{TWO_PRODUCERS}RETURN count(DISTINCT n) AS c, size(collect(DISTINCT n)) AS k"
        ))
        .await
        .unwrap();
    let vals = r.rows()[0].values();
    assert_eq!(vals[0], vals[1], "two DISTINCT mechanisms, one answer");
}

/// The upstream defect, pinned on its own: `n._vid` is NULL for one producer.
///
/// This is the thing to fix. Both rows decode to the identical vertex, so a
/// system field of that vertex must not read as NULL for either of them.
#[tokio::test]
#[ignore = "open defect: _vid reads NULL for a natively-encoded entity"]
async fn the_vid_of_a_vertex_is_never_null() {
    let db = fixture().await;
    let r = db
        .session()
        .query(&format!("{TWO_PRODUCERS}RETURN n._vid AS v"))
        .await
        .unwrap();
    for (i, row) in r.rows().iter().enumerate() {
        assert_ne!(
            row.values()[0],
            Value::Null,
            "row {i}: the vertex is present, so its _vid must be too"
        );
    }
}
