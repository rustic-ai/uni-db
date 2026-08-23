// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! #181: `flush()` resurrected an edge that `DETACH DELETE` had removed.
//!
//! The traversal was correct before the flush and wrong after it, with no error
//! raised — a silent wrong answer, and the surviving edge pointed at a deleted
//! vertex, so its endpoint read back as NULL.
//!
//! Fix site: `MainEdgeDataset::find_edges_by_type_names`
//! (`uni-store/src/storage/main_edge.rs`). The main edges table is append-only,
//! so a deleted edge has both a live row and a tombstone at a higher
//! `_version`; the scan filtered `_deleted = false` with no version ranking and
//! therefore selected the stale live row and discarded its own tombstone.
//!
//! Before the flush the defect was masked by a tombstone filter in
//! `build_edge_adjacency_map` (`uni-query/.../traverse.rs`) that consults the L0
//! buffers only. After the flush that buffer is rotated and the filter finds
//! nothing, so the stale row surfaced.
//!
//! **The queries here are deliberately unanchored.** The two existing
//! resurrection repros (`repro_11_compact_adjacency_empty_resurrect`,
//! `bug_rc6_get_edges_post_flush_step`) count neighbours of a *known* src vid,
//! so an edge whose destination is gone cannot appear in their probe at all.
//! Binding neither endpoint is what makes a stranded edge observable.
//!
//! Schemaless on purpose: a declared edge type routes the traversal away from
//! `build_edge_adjacency_map`.

// Rust guideline compliant

use uni_db::{Result, Uni};

/// `(a {uid:1})-[:EDGE_P]->(b {uid:2})`, flushed, so both rows are in L1.
async fn flushed_pair() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (a {uid: 1}), (b {uid: 2}), (a)-[:EDGE_P]->(b)")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

/// Rows of `(src uid, dst uid)` for every `EDGE_P`, both endpoints unbound.
async fn edges(db: &Uni) -> Result<Vec<(Option<i64>, Option<i64>)>> {
    let r = db
        .session()
        .query("MATCH (a)-[r:EDGE_P]->(b) RETURN a.uid AS s, b.uid AS d")
        .await?;
    Ok(r.rows()
        .iter()
        .map(|row| {
            // `get::<i64>` errors on a NULL, which is exactly the symptom under
            // test — so a failed read maps to `None` rather than aborting.
            (row.get::<i64>("s").ok(), row.get::<i64>("d").ok())
        })
        .collect())
}

/// The headline: deleting the destination must survive a flush.
#[tokio::test]
async fn detach_delete_survives_a_flush() -> Result<()> {
    let db = flushed_pair().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n {uid: 2}) DETACH DELETE n").await?;
    tx.commit().await?;

    // Correct before the flush — this half passed even with the bug, because
    // the L0 tombstone filter was still masking it.
    assert!(
        edges(&db).await?.is_empty(),
        "the edge is gone before the flush"
    );

    db.flush().await?;

    let after = edges(&db).await?;
    assert!(
        after.is_empty(),
        "flush resurrected the detached edge: {after:?}"
    );
    // Named explicitly: the symptom that makes this more than a count being
    // wrong. A resurrected row keeps its dst_vid, and hydrating a deleted
    // vertex yields nothing, so the endpoint comes back NULL.
    assert!(
        after.iter().all(|(_, d)| d.is_some()),
        "an edge survived with a null endpoint: {after:?}"
    );

    db.shutdown().await?;
    Ok(())
}

/// The same hazard without the vertex cascade: delete only the relationship.
///
/// Isolates the append-only tombstone problem from `DETACH`, and stays
/// meaningful if the null-endpoint symptom is ever masked elsewhere.
#[tokio::test]
async fn relationship_delete_survives_a_flush() -> Result<()> {
    let db = flushed_pair().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH ()-[r:EDGE_P]->() DELETE r").await?;
    tx.commit().await?;
    db.flush().await?;

    let after = edges(&db).await?;
    assert!(
        after.is_empty(),
        "flush resurrected the deleted relationship: {after:?}"
    );

    db.shutdown().await?;
    Ok(())
}

/// The guard against the fix over-filtering.
///
/// Version-ranking drops an eid whose winner is a tombstone; if it dropped too
/// much, the other two tests here would still pass while every edge vanished.
#[tokio::test]
async fn surviving_edges_of_the_same_type_are_untouched() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a {uid: 1}), (b {uid: 2}), (c {uid: 3}), \
         (a)-[:EDGE_P]->(b), (a)-[:EDGE_P]->(c)",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    tx.execute("MATCH (n {uid: 2}) DETACH DELETE n").await?;
    tx.commit().await?;
    db.flush().await?;

    let after = edges(&db).await?;
    assert_eq!(
        after,
        vec![(Some(1), Some(3))],
        "exactly the untouched edge must survive, with a live endpoint"
    );

    db.shutdown().await?;
    Ok(())
}
