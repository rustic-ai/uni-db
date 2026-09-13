// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #268 — an equality predicate must anchor a pattern from either end.
//!
//! `MATCH (o:Entity)-[:OWNS]->(e:Entity) WHERE e.uid = $u` and
//! `MATCH (e:Entity)<-[:OWNS]-(o:Entity) WHERE e.uid = $u` are the same
//! question. Before the fix the first walked every `OWNS` edge and reapplied
//! `e.uid` as a filter above the traversal, because anchor selection only asked
//! whether a variable was already in scope — and a `WHERE` is planned after the
//! pattern it constrains. "Which entities own this one" is the reverse
//! direction, so for an ownership graph the natural spelling was the unusable
//! one.
//!
//! # Why this asserts on a counter
//!
//! Both spellings return identical rows, so no correctness assertion can
//! separate them; only cost can. `QueryMetrics::rows_scanned` is "rows examined
//! by scans, before filtering and projection", which reads the plan's choice:
//! an anchored walk examines the matching rows, a scan-and-filter examines the
//! table.
//!
//! Wall-clock is deliberately not the observable — it cannot tell a full scan
//! from a slow machine, and this repository has repeatedly attributed a cost to
//! a mechanism the query did not use.
//!
//! # The control is load-bearing
//!
//! Each test pairs the spelling under test against the spelling that always
//! worked, over the same target. A bare `rows_scanned < N` threshold would also
//! hold if the counter broke and both arms reported zero, so the working
//! spelling is measured in the same test rather than assumed. Row equality is
//! asserted alongside, so an arm that got cheap by answering a different
//! question fails instead of passing.

use std::collections::HashMap;

use uni_db::{DataType, IndexType, ScalarType, Uni, Value};

/// Vertices, and edges. Large enough that a full walk is unmistakable against
/// a single anchored lookup, small enough to stay a unit test.
const N_NODES: usize = 4_000;
const N_EDGES: usize = 8_000;

/// The node every arm asks about. Given a high in-degree below so the answer is
/// a comfortable number of rows rather than one — a single-row answer could be
/// produced by an accidental limit and still look correct.
const TARGET: &str = "e7";

/// `Entity -[:OWNS]-> Entity`, `uid` BTree-indexed, with `TARGET` given a high
/// in-degree.
async fn fixture() -> Uni {
    let db = Uni::temporary().build().await.unwrap();
    db.schema()
        .label("Entity")
        .property("uid", DataType::String)
        .property("name", DataType::String)
        .done()
        .edge_type("OWNS", &["Entity"], &["Entity"])
        .done()
        .apply()
        .await
        .unwrap();

    let session = db.session();
    let tx = session.tx().await.unwrap();
    let mut bulk = tx.bulk_writer().build().unwrap();
    let vertices: Vec<HashMap<String, Value>> = (0..N_NODES)
        .map(|i| {
            HashMap::from([
                ("uid".to_string(), Value::String(format!("e{i}"))),
                ("name".to_string(), Value::String(format!("Entity {i}"))),
            ])
        })
        .collect();
    let vids = bulk.insert_vertices("Entity", vertices).await.unwrap();

    // Every fourth edge points at TARGET, the rest are spread deterministically.
    // A skewed in-degree is what makes the reverse direction the interesting one.
    let target_index = 7;
    let edges = (0..N_EDGES)
        .map(|i| {
            let src = (i * 7 + 1) % N_NODES;
            let dst = if i % 4 == 0 {
                target_index
            } else {
                (i * 13 + 3) % N_NODES
            };
            uni_bulk::EdgeData::new(vids[src], vids[dst], HashMap::new())
        })
        .filter(|e| e.src_vid != e.dst_vid)
        .collect();
    bulk.insert_edges("OWNS", edges).await.unwrap();
    bulk.commit().await.unwrap();
    tx.commit().await.unwrap();
    db.flush().await.unwrap();

    db.schema()
        .label("Entity")
        .index("uid", IndexType::Scalar(ScalarType::BTree))
        .apply()
        .await
        .unwrap();
    db
}

/// Rows returned and rows examined, for one spelling.
async fn measure(db: &Uni, query: &str) -> (usize, usize) {
    let result = db
        .session()
        .query_with(query)
        .param("u", Value::String(TARGET.to_string()))
        .fetch_all()
        .await
        .unwrap();
    let scanned = result.metrics().rows_scanned;
    (result.rows().len(), scanned)
}

/// The spelling that always worked: the filtered node is written first.
const CONTROL: &str = "MATCH (e:Entity)<-[:OWNS]-(o:Entity) WHERE e.uid = $u RETURN o.uid AS x";

/// Assert `subject` costs about what `CONTROL` costs, and answers the same.
async fn assert_matches_control(db: &Uni, subject: &str, what: &str) {
    let (control_rows, control_scanned) = measure(db, CONTROL).await;
    let (rows, scanned) = measure(db, subject).await;

    assert!(
        control_rows > 1,
        "the control must return several rows, or a spelling that accidentally \
         returns one could pass by looking identical; got {control_rows}"
    );
    assert_eq!(
        rows, control_rows,
        "{what} must answer the same question as the control, not a cheaper one"
    );
    assert!(
        scanned <= control_scanned * 4,
        "{what} examined {scanned} rows against the control's {control_scanned}: \
         the pattern was walked from its unconstrained end (#268)"
    );
}

/// The predicate written in the clause's WHERE anchors the last node.
#[tokio::test]
async fn a_where_equality_anchors_the_pattern_written_left_to_right() {
    let db = fixture().await;
    assert_matches_control(
        &db,
        "MATCH (o:Entity)-[:OWNS]->(e:Entity) WHERE e.uid = $u RETURN o.uid AS x",
        "a WHERE equality on the last node",
    )
    .await;
}

/// The inline map spelling anchors identically.
#[tokio::test]
async fn an_inline_equality_anchors_the_pattern_written_left_to_right() {
    let db = fixture().await;
    assert_matches_control(
        &db,
        "MATCH (o:Entity)-[:OWNS]->(e:Entity {uid: $u}) RETURN o.uid AS x",
        "an inline property map on the last node",
    )
    .await;
}

/// Splitting the binding across two MATCH clauses anchors too.
///
/// This one was already fixed by #219/#224 — it is here as the third spelling a
/// user reaches for, so a regression in any of the three is visible together.
#[tokio::test]
async fn a_binding_split_across_two_match_clauses_anchors() {
    let db = fixture().await;
    assert_matches_control(
        &db,
        "MATCH (e:Entity) WHERE e.uid = $u \
         MATCH (o:Entity)-[:OWNS]->(e) RETURN o.uid AS x",
        "a binding from a previous clause",
    )
    .await;
}

/// Anchoring did not change the answer.
///
/// The cost assertions above would all be satisfied by a rewrite that returned
/// the wrong rows cheaply. Reversal is only sound because `source_variable`
/// names the traversal start rather than the arrow's tail, so this pins the
/// three spellings to one another as sets, not merely as counts.
#[tokio::test]
async fn every_spelling_returns_the_same_owners() {
    let db = fixture().await;

    let mut answers = Vec::new();
    for query in [
        CONTROL,
        "MATCH (o:Entity)-[:OWNS]->(e:Entity) WHERE e.uid = $u RETURN o.uid AS x",
        "MATCH (o:Entity)-[:OWNS]->(e:Entity {uid: $u}) RETURN o.uid AS x",
        "MATCH (e:Entity) WHERE e.uid = $u MATCH (o:Entity)-[:OWNS]->(e) RETURN o.uid AS x",
    ] {
        let rows = db
            .session()
            .query_with(query)
            .param("u", Value::String(TARGET.to_string()))
            .fetch_all()
            .await
            .unwrap();
        let mut owners: Vec<String> = rows
            .rows()
            .iter()
            .map(|r| r.get::<String>("x").unwrap())
            .collect();
        owners.sort();
        answers.push(owners);
    }

    assert!(
        !answers[0].is_empty(),
        "the fixture must give the target some owners, or every arm agrees vacuously"
    );
    for (i, answer) in answers.iter().enumerate().skip(1) {
        assert_eq!(
            answer, &answers[0],
            "spelling {i} returned a different owner set than the control"
        );
    }
}
