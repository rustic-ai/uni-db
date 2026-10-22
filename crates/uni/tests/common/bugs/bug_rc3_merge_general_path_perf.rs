// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! RC3 (uniko `UNI_DB_WORKAROUNDS.md`) — relationship-`MERGE` bulk fast-path.
//! FIXED. The single-node `MERGE` shape had a bulk fast-path (issue #69), but the
//! relationship shape `(a)-[:R]->(b)` rebuilt and ran a per-row traversal
//! `LogicalPlan` just to check edge existence — measured ~**19x** the bulk
//! `CREATE` of the same edges (clean isolation: same edge writes, the only
//! difference is MERGE's missing fast-path).
//!
//! `execute_merge` (`uni-query/src/query/executor/write.rs`) now detects the
//! bound-endpoints, anonymous-edge shape and resolves existence with one
//! MVCC-correct adjacency probe (`GraphExecutionContext::get_neighbors`, which
//! merges CSR + all L0 buffers incl. the transaction's own writes — so
//! intra-batch edges are seen), reusing the general create / ON CREATE path
//! unchanged. Shapes it does not cover (edge variable/properties, ON MATCH SET,
//! variable-length, unbound endpoints) fall through to the general path. After
//! the fix the batched relationship MERGE is ~**1x** the bulk CREATE.
//!
//! Guards below: the perf guard pins the ratio under the bulk-CREATE bar, and the
//! correctness guards pin edge-MERGE idempotency across the fast-path (no
//! duplicate edges within a batch, across statements, or after a flush).
//!
//! Run with:
//!   cargo nextest run -p uni-db --test integration bug_rc3_merge_general_path_perf

use std::time::Instant;

use anyhow::Result;
use uni_db::{DataType, IndexType, ScalarType, Uni, Value};

/// Builds a DB with `M` source `:S` and `M` target `:T` nodes, keyed `0..M`.
async fn endpoints_db(m: i64) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("S")
        .property("k", DataType::Int)
        .index("k", IndexType::Scalar(ScalarType::Hash))
        .done()
        .label("T")
        .property("k", DataType::Int)
        .index("k", IndexType::Scalar(ScalarType::Hash))
        .done()
        .edge_type("REL", &["S"], &["T"])
        .done()
        .apply()
        .await?;

    let session = db.session();
    let tx = session.tx().await?;
    tx.execute_with("UNWIND range(0, $hi) AS i CREATE (:S {k: i}) CREATE (:T {k: i})")
        .param("hi", Value::Int(m - 1))
        .run()
        .await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

/// VERIFIED: a batched relationship `MERGE` is within a small factor of writing
/// the same edges via the bulk-fast-path `CREATE` (MERGE adds only an existence
/// check). Was ~19x before the fast path; ~1x after. The generous 8x bar guards
/// against a regression to per-row planning while tolerating timing noise.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn relationship_merge_should_match_bulk_create_fastpath() -> Result<()> {
    const M: i64 = 150;

    // ── (A) one MERGE statement per edge ────────────────────────────────────
    let db = endpoints_db(M).await?;
    let session = db.session();
    let tx = session.tx().await?;
    let start = Instant::now();
    for i in 0..M {
        tx.execute_with("MATCH (a:S {k: $k}), (b:T {k: $k}) MERGE (a)-[:REL]->(b)")
            .param("k", Value::Int(i))
            .run()
            .await?;
    }
    tx.commit().await?;
    let per_stmt_us = start.elapsed().as_micros() / M as u128;

    // ── (B) one batched UNWIND … MERGE statement (general/edge path) ────────
    let db = endpoints_db(M).await?;
    let session = db.session();
    let tx = session.tx().await?;
    let start = Instant::now();
    let merged = tx
        .execute_with(
            "UNWIND range(0, $hi) AS k \
             MATCH (a:S {k: k}), (b:T {k: k}) MERGE (a)-[:REL]->(b)",
        )
        .param("hi", Value::Int(M - 1))
        .run()
        .await?;
    tx.commit().await?;
    let batched_rel_us = start.elapsed().as_micros() / M as u128;
    let merge_plans = merged.metrics().physical_plans;
    assert_eq!(merged.relationships_created(), M as usize);

    // ── (C) one batched relationship CREATE (the edge bulk fast-path) ───────
    // Identical edge writes to (B), but CREATE has a bulk fast-path while MERGE
    // takes the general per-row path — so this isolates MERGE's missing fast-path
    // from the inherent cost of writing the edges.
    let db = endpoints_db(M).await?;
    let session = db.session();
    let tx = session.tx().await?;
    let start = Instant::now();
    tx.execute_with(
        "UNWIND range(0, $hi) AS k \
         MATCH (a:S {k: k}), (b:T {k: k}) CREATE (a)-[:REL]->(b)",
    )
    .param("hi", Value::Int(M - 1))
    .run()
    .await?;
    tx.commit().await?;
    let batched_create_us = start.elapsed().as_micros() / M as u128;

    // ── (D) control: the same batch forced onto the general path ────────────
    // `ON MATCH SET` is outside the fast path's shape, so every row plans its
    // own existence check. This is what the plan count below must tell apart
    // from (B); without it a count that never moves would pass for free.
    let db = endpoints_db(M).await?;
    let session = db.session();
    let tx = session.tx().await?;
    let general = tx
        .execute_with(
            "UNWIND range(0, $hi) AS k \
             MATCH (a:S {k: k}), (b:T {k: k}) \
             MERGE (a)-[r:REL]->(b) ON MATCH SET r.seen = true",
        )
        .param("hi", Value::Int(M - 1))
        .run()
        .await?;
    tx.commit().await?;
    let general_plans = general.metrics().physical_plans;

    let timing = format!(
        "RC3 edge-write per-row: merge-per-statement={per_stmt_us}us, \
         merge-batched={batched_rel_us}us, create-batched-fastpath={batched_create_us}us, \
         merge/create={:.1}x; physical plans: merge-batched={merge_plans}, \
         general-path control={general_plans} (M={M})",
        batched_rel_us as f64 / batched_create_us.max(1) as f64
    );
    eprintln!("{timing}");

    // The path taken, independent of timing: the general path builds a plan
    // per row, the fast path builds none per row.
    assert!(
        general_plans >= M as u64,
        "control: the general path must plan per row, or the plan count cannot \
         discriminate the paths\n{timing}"
    );
    assert!(
        merge_plans < (M as u64) / 4,
        "the batched relationship MERGE planned per row: it took the general path, \
         not the RC3 fast path\n{timing}"
    );

    // A batched relationship MERGE should be within a small factor of the same
    // edges written via the bulk-fast-path CREATE (MERGE adds only an existence
    // check). The 8x bar guards against a regression to per-row planning.
    assert!(
        batched_rel_us < batched_create_us * 8,
        "batched relationship MERGE ({batched_rel_us}us/row) is >8x the bulk-fast-path \
         CREATE of the same edges ({batched_create_us}us/row) — relationship MERGE \
         fast path appears to have regressed to per-row planning (RC3)\n{timing}"
    );
    Ok(())
}

/// Counts `:REL` edges.
async fn rel_count(db: &Uni) -> Result<i64> {
    let r = db
        .session()
        .query("MATCH ()-[e:REL]->() RETURN count(e) AS c")
        .await?;
    Ok(r.rows()[0].get::<i64>("c")?)
}

/// Correctness: the relationship-MERGE fast path must be idempotent — no
/// duplicate edges within a single UNWIND batch (the fast path's existence probe
/// must see an edge a prior row of the same transaction just created), across
/// statements, or after a flush moves the edge into the CSR.
#[tokio::test]
async fn relationship_merge_fastpath_is_idempotent() -> Result<()> {
    let db = endpoints_db(3).await?; // S/T keyed 0..2

    let session = db.session();

    // Intra-batch dedup: the duplicate (0,0) pair must produce ONE edge — the
    // second row's existence probe must see the edge the first row created in
    // the same transaction (tx-L0 visibility through get_neighbors).
    let tx = session.tx().await?;
    tx.execute(
        "UNWIND [{a: 0, b: 0}, {a: 0, b: 0}, {a: 1, b: 1}] AS r \
         MATCH (a:S {k: r.a}), (b:T {k: r.b}) MERGE (a)-[:REL]->(b)",
    )
    .await?;
    tx.commit().await?;
    assert_eq!(
        rel_count(&db).await?,
        2,
        "intra-batch dedup: two distinct edges"
    );

    // Cross-statement idempotency: re-MERGE the same pairs (now committed in L0)
    // → still no new edges.
    let tx = session.tx().await?;
    tx.execute(
        "UNWIND [{a: 0, b: 0}, {a: 1, b: 1}] AS r \
         MATCH (a:S {k: r.a}), (b:T {k: r.b}) MERGE (a)-[:REL]->(b)",
    )
    .await?;
    tx.commit().await?;
    assert_eq!(
        rel_count(&db).await?,
        2,
        "re-MERGE of L0 edges: no duplicates"
    );

    // Post-flush idempotency: after a flush the edges live in the CSR; the probe
    // (CSR + L0 merged) must still match them.
    db.flush().await?;
    let tx = session.tx().await?;
    tx.execute(
        "UNWIND [{a: 0, b: 0}, {a: 1, b: 1}] AS r \
         MATCH (a:S {k: r.a}), (b:T {k: r.b}) MERGE (a)-[:REL]->(b)",
    )
    .await?;
    tx.commit().await?;
    assert_eq!(
        rel_count(&db).await?,
        2,
        "re-MERGE of flushed edges: no duplicates"
    );

    // And a genuinely new pair still creates.
    let tx = session.tx().await?;
    tx.execute("MATCH (a:S {k: 2}), (b:T {k: 2}) MERGE (a)-[:REL]->(b)")
        .await?;
    tx.commit().await?;
    assert_eq!(rel_count(&db).await?, 3, "new pair creates a third edge");

    Ok(())
}
