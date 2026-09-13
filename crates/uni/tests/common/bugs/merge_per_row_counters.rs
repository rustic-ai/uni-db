// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Execution counters never reached MERGE's per-row plans, so `rows_scanned`
//! read the same for a sixteen-second MERGE as for a sub-second one.

use std::collections::HashMap;

use anyhow::Result;
use uni_db::{Uni, Value};

const N: usize = 4_000;

async fn graph() -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL Entity (uid STRING)").await?;
    tx.execute("CREATE EDGE TYPE OWNS FROM Entity TO Entity")
        .await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let vertices: Vec<HashMap<String, Value>> = (0..N)
        .map(|i| HashMap::from([("uid".to_string(), Value::String(format!("e{i}")))]))
        .collect();
    bulk.insert_vertices("Entity", vertices).await?;
    bulk.commit().await?;
    tx.commit().await?;
    Ok(db)
}

/// A MERGE whose per-row plans scan a label must report those scans.
///
/// The counters live on an `Arc<QueryCounters>` that `Executor::clone`
/// deliberately makes fresh — the write path clones a cached template, and a
/// shared handle would spill one query's counts into the next one's result. But
/// `MutationContext` also holds a clone, so every scan a mutation performed
/// counted into a set nobody harvests.
///
/// The shape below is the one #225 measured: an unbound, unlabelled-key first
/// element forces a per-row scan of the whole label, which cannot be anchored
/// away. With `N` rows over a label of `N`, the per-row scans dominate the outer
/// MATCH by orders of magnitude — so a counter that only sees the outer MATCH is
/// unmistakable.
#[tokio::test]
async fn merge_reports_the_rows_its_per_row_plans_scanned() -> Result<()> {
    let db = graph().await?;
    let batch = Value::List(
        (0..20)
            .map(|i| {
                Value::Map(HashMap::from([(
                    "dst".to_string(),
                    Value::String(format!("e{i}")),
                )]))
            })
            .collect(),
    );

    let tx = db.session().tx().await?;
    let res = tx
        .execute_with(
            "UNWIND $batch AS r \
             MATCH (b:Entity {uid: r.dst}) \
             MERGE (a:Entity)-[:OWNS]->(b)",
        )
        .param("batch", batch)
        .run()
        .await?;
    let scanned = res.metrics().rows_scanned;
    tx.rollback();

    eprintln!("rows_scanned = {scanned} over a label of {N} with 20 rows");
    assert!(
        scanned > N,
        "rows_scanned = {scanned}, which is below the {N}-row label this MERGE \
         rescans once per input row. The counters are not reaching the \
         mutation executor, so a MERGE's own scans are invisible and any \
         assertion on this counter passes for free."
    );
    Ok(())
}

/// A keyed-node MERGE that must scan reports the rows it scanned.
///
/// `merge_lookup_persisted_batch` resolves the batch's keys with a filtered
/// scan of the label's table, and used the uncounted entry point — so a
/// `MERGE (n:L {k: v})` answered without ever appearing to look at anything.
///
/// The key here is deliberately **unindexed**. With an index the fast path does
/// a point lookup and examining no scan rows is the correct answer, so an
/// indexed key cannot tell a fixed counter from a broken one.
#[tokio::test]
async fn a_scanning_merge_reports_the_rows_it_scanned() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    // No index on `tag`, which is what the MERGE keys on.
    tx.execute("CREATE LABEL Thing (tag STRING)").await?;
    tx.commit().await?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let vertices: Vec<HashMap<String, Value>> = (0..N)
        .map(|i| HashMap::from([("tag".to_string(), Value::String(format!("t{i}")))]))
        .collect();
    bulk.insert_vertices("Thing", vertices).await?;
    bulk.commit().await?;
    tx.commit().await?;
    db.flush().await?;

    let tx = db.session().tx().await?;
    let res = tx
        .query("MERGE (n:Thing {tag: 't5'}) RETURN n.tag AS t")
        .await?;
    let m = res.metrics().clone();
    let scanned = m.rows_scanned;
    let rows = res.rows().len();
    tx.rollback();

    eprintln!(
        "unindexed keyed MERGE: rows_scanned={scanned} rows={rows} \
         scans_reported={} index_scans={} lance_iops={}",
        m.scans_reported, m.index_scans, m.lance_iops
    );
    assert_eq!(rows, 1, "the MERGE should have matched the existing node");
    // `rows_scanned` counts what the storage scan handed back — that is how
    // `columnar_scan` computes it too (`lance_rows + l0_rows`), so a pushed-down
    // predicate lowers it on every path, not just this one. The assertion is
    // therefore "> 0", not a row count: it distinguishes a scan that is
    // observable from one that is invisible, which is the defect.
    assert!(
        scanned > 0,
        "the MERGE fast path's key-resolution scan reported no rows \
         (rows_scanned=0, scans_reported={}). It scans the label to resolve an \
         unindexed key, so a statement can do that work while appearing to do \
         none.",
        m.scans_reported
    );
    Ok(())
}
