// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Bulk-writer and appender loads emit trigger and CDC events (#5c part 2).
//!
//! A load is written straight to storage, outside any transaction buffer, so
//! the commit path's event extraction never saw it: no after-commit trigger
//! fired and CDC got no batch. A load now announces itself once it is visible
//! and streams its rows' create events, read back from storage, to after-phase
//! triggers and CDC. A synchronous before-trigger cannot veto a load that is
//! already visible, so a load it would see is refused unless the caller opts
//! in to skipping it.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::Result;
use smol_str::SmolStr;
use uni_db::{DataType, Uni, UniError, Value};
use uni_plugin::traits::trigger::{TriggerEventMask, TriggerPhase};

use super::event_support::{
    RecordingTrigger, attach_cdc, event_kinds, register_trigger, wait_for_batches,
};

/// Rows per load: more than one event chunk (`BULK_EVENT_CHUNK_ROWS`), so a
/// load whose events were capped at one chunk would fail the row counts.
const ROWS: usize = 10_000;

const NODE_CREATE: u8 = 1;
const EDGE_CREATE: u8 = 4;

async fn db() -> Result<Arc<Uni>> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("A")
        .property("x", DataType::Int64)
        .label("B")
        .property("x", DataType::Int64)
        .edge_type("R", &["A"], &["A"])
        .property("w", DataType::Int64)
        .apply()
        .await?;
    Ok(Arc::new(db))
}

fn rows(n: usize) -> Vec<HashMap<String, Value>> {
    (0..n)
        .map(|i| HashMap::from([("x".to_string(), Value::Int(i as i64))]))
        .collect()
}

async fn count(db: &Uni, cypher: &str) -> Result<i64> {
    Ok(db.session().query(cypher).await?.rows()[0].get::<i64>("n")?)
}

fn after_trigger(events: TriggerEventMask, labels: Option<&[&str]>) -> Arc<RecordingTrigger> {
    RecordingTrigger::new(
        TriggerPhase::AfterCommit,
        events,
        labels.map(|ls| ls.iter().map(|l| SmolStr::new(l)).collect()),
    )
}

/// Every row of a load larger than one event chunk fires `NODE_CREATE` once,
/// for its own vertex.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_insert_emits_node_create_for_every_row() -> Result<()> {
    let db = db().await?;
    let t = after_trigger(TriggerEventMask::NODE_CREATE, Some(&["A"]));
    register_trigger(&db, "bulk-node-create", t.clone())?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let vids = bulk.insert_vertices("A", rows(ROWS)).await?;
    bulk.commit().await?;
    tx.commit().await?;

    let kinds = t.kinds.lock().clone();
    let ids: HashSet<i64> = t.ids.lock().iter().copied().collect();
    let expected: HashSet<i64> = vids.iter().map(|v| v.as_u64() as i64).collect();
    assert_eq!(
        kinds.len(),
        ROWS,
        "a {ROWS}-row bulk load fired {} NODE_CREATE rows over {} fires",
        kinds.len(),
        t.fires.load(Ordering::SeqCst)
    );
    assert!(kinds.iter().all(|k| *k == NODE_CREATE), "kinds: {kinds:?}");
    assert_eq!(ids, expected, "the fired rows are not the loaded vertices");
    assert!(
        t.fires.load(Ordering::SeqCst) >= 2,
        "a load larger than one chunk must be delivered in more than one fire"
    );
    Ok(())
}

/// The appender path emits too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn appender_emits_node_create_for_every_row() -> Result<()> {
    let db = db().await?;
    let t = after_trigger(TriggerEventMask::NODE_CREATE, None);
    register_trigger(&db, "appender-node-create", t.clone())?;

    let tx = db.session().tx().await?;
    let mut app = tx.appender("A").batch_size(1000).build()?;
    for row in rows(2500) {
        app.append(row).await?;
    }
    app.finish().await?;
    tx.commit().await?;

    assert_eq!(t.rows(), 2500, "appender load fired {} rows", t.rows());
    Ok(())
}

/// Bulk-inserted edges fire `EDGE_CREATE`, one row per edge, for the edges'
/// own ids.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_edges_emit_edge_create_rows() -> Result<()> {
    let db = db().await?;
    let t = after_trigger(TriggerEventMask::EDGE_CREATE, None);
    register_trigger(&db, "bulk-edge-create", t.clone())?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let vids = bulk.insert_vertices("A", rows(100)).await?;
    let edges: Vec<uni_bulk::EdgeData> = (0..ROWS)
        .map(|i| {
            uni_bulk::EdgeData::new(
                vids[i % 100],
                vids[(i * 7 + 3) % 100],
                HashMap::from([("w".to_string(), Value::Int(i as i64))]),
            )
        })
        .collect();
    let eids = bulk.insert_edges("R", edges).await?;
    bulk.commit().await?;
    tx.commit().await?;

    let kinds = t.kinds.lock().clone();
    assert_eq!(
        kinds.len(),
        ROWS,
        "{ROWS} bulk edges fired {} rows",
        kinds.len()
    );
    assert!(kinds.iter().all(|k| *k == EDGE_CREATE), "kinds: {kinds:?}");
    let ids: HashSet<i64> = t.ids.lock().iter().copied().collect();
    let expected: HashSet<i64> = eids.iter().map(|e| e.as_u64() as i64).collect();
    assert_eq!(ids, expected, "the fired rows are not the loaded edges");
    Ok(())
}

/// CDC: one announcement per load, whose batches together carry every row,
/// all with the load's LSN range; the checkpoint moves once, to the load's
/// `lsn_end`, and never goes backwards across the commits around it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_load_reaches_cdc_with_monotonic_lsns() -> Result<()> {
    let db = db().await?;
    let cdc = attach_cdc(&db, "CREATE (:B {x: -1})").await?;
    let mut watch = db.session().watch();

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:B {x: 1})").await?;
    let before = tx.commit().await?.version;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let vids = bulk.insert_vertices("A", rows(ROWS)).await?;
    let edges: Vec<uni_bulk::EdgeData> = (0..50)
        .map(|i| {
            uni_bulk::EdgeData::new(
                vids[i],
                vids[i + 1],
                HashMap::from([("w".to_string(), Value::Int(i as i64))]),
            )
        })
        .collect();
    bulk.insert_edges("R", edges).await?;
    let load_id = bulk.load_id().to_owned();
    bulk.commit().await?;
    // The enclosing transaction wrote nothing itself; its commit is announced
    // like any other.
    let enclosing = tx.commit().await?.version;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:B {x: 2})").await?;
    let after = tx.commit().await?.version;

    // Four announcements, in commit order: the commit before, the load, the
    // enclosing transaction's commit, the commit after.
    let mut announced = Vec::new();
    for _ in 0..4 {
        let n = tokio::time::timeout(Duration::from_secs(10), watch.next())
            .await?
            .expect("watch open");
        announced.push((n.tx_id, n.causal_version, n.version, n.mutation_count));
    }
    let versions: Vec<u64> = announced.iter().map(|a| a.2).collect();
    assert_eq!(announced[1].0, load_id, "announcements: {announced:?}");
    let (_, load_start, load_end, load_rows) = announced[1].clone();
    assert_eq!(load_rows, ROWS + 50, "announcements: {announced:?}");
    assert_eq!(
        [versions[0], versions[2], versions[3]],
        [before, enclosing, after],
        "announcements: {announced:?}"
    );
    assert!(
        versions.windows(2).all(|w| w[0] <= w[1]) && load_start <= load_end,
        "announced versions must not decrease: {announced:?}"
    );

    // Wait for the trailing commit, which is delivered after every batch of the
    // load.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline
        && !cdc.delivered.lock().iter().any(|b| b.lsn_end.0 == after)
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let batches = cdc.delivered.lock().clone();
    let ranges: Vec<(u64, u64)> = batches
        .iter()
        .map(|b| (b.lsn_start.0, b.lsn_end.0))
        .collect();
    let load: Vec<_> = batches
        .iter()
        .filter(|b| (b.lsn_start.0, b.lsn_end.0) == (load_start, load_end))
        .collect();
    assert!(
        load.len() >= 2,
        "a load larger than one chunk arrives as several batches: {ranges:?}"
    );
    // Batches arrive in announcement order: the load's are contiguous, after
    // the commit before it and ahead of the enclosing commit.
    assert_eq!(ranges.len(), load.len() + 3, "{ranges:?}");
    assert_eq!(ranges[0].1, before, "{ranges:?}");
    assert!(
        ranges[1..=load.len()]
            .iter()
            .all(|r| *r == (load_start, load_end)),
        "{ranges:?}"
    );
    assert_eq!(
        [ranges[load.len() + 1].1, ranges[load.len() + 2].1],
        [enclosing, after],
        "{ranges:?}"
    );
    let mut kinds = Vec::new();
    for b in &load {
        assert!(b.mutations.num_rows() <= uni_bulk::BULK_EVENT_CHUNK_ROWS);
        kinds.extend(event_kinds(&b.mutations));
    }
    assert_eq!(
        kinds.iter().filter(|k| **k == NODE_CREATE).count(),
        ROWS,
        "CDC node rows"
    );
    assert_eq!(
        kinds.iter().filter(|k| **k == EDGE_CREATE).count(),
        50,
        "CDC edge rows"
    );

    let checkpoints: Vec<u64> = cdc.checkpoints.lock().iter().map(|c| c.0).collect();
    assert_eq!(
        checkpoints,
        [before, load_end, enclosing, after],
        "one checkpoint per announcement (the load's after its last batch), never regressing"
    );
    Ok(())
}

/// A synchronous `BeforeCommit` trigger on the loaded label refuses the load
/// with a typed error, before anything is written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_load_seen_by_a_before_trigger_is_refused() -> Result<()> {
    let db = db().await?;
    let before = RecordingTrigger::new(
        TriggerPhase::BeforeCommit,
        TriggerEventMask::NODE_CREATE,
        Some(vec![SmolStr::new("A")]),
    );
    register_trigger(&db, "veto-a", before.clone())?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let refused = bulk.insert_vertices("A", rows(10)).await;
    let err = refused.expect_err("a load a before-trigger subscribes to must be refused");
    assert!(
        matches!(
            err.downcast_ref::<UniError>(),
            Some(UniError::BulkLoadBlockedByTrigger { .. })
        ),
        "expected BulkLoadBlockedByTrigger, got {err:?}"
    );
    // A label the trigger does not watch still loads.
    bulk.insert_vertices("B", rows(3)).await?;
    bulk.commit().await?;
    tx.commit().await?;

    assert_eq!(count(&db, "MATCH (n:A) RETURN count(n) AS n").await?, 0);
    assert_eq!(count(&db, "MATCH (n:B) RETURN count(n) AS n").await?, 3);
    assert_eq!(before.rows(), 0, "the before-trigger must not fire");
    Ok(())
}

/// A before-trigger registered after the rows were staged is caught at
/// commit: the load is refused and nothing of it becomes visible.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn before_trigger_registered_mid_load_refuses_at_commit() -> Result<()> {
    let db = db().await?;
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    bulk.insert_vertices("A", rows(10)).await?;

    let before = RecordingTrigger::new(
        TriggerPhase::BeforeMutation,
        TriggerEventMask::NODE_CREATE,
        None,
    );
    register_trigger(&db, "veto-late", before)?;

    let err = bulk.commit().await.expect_err("commit must be refused");
    assert!(
        matches!(
            err.downcast_ref::<UniError>(),
            Some(UniError::BulkLoadBlockedByTrigger { .. })
        ),
        "expected BulkLoadBlockedByTrigger, got {err:?}"
    );
    tx.commit().await?;
    assert_eq!(count(&db, "MATCH (n:A) RETURN count(n) AS n").await?, 0);
    Ok(())
}

/// Opting in loads past the before-trigger without running it; after-phase
/// triggers still fire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn skip_before_triggers_loads_and_still_fires_after_triggers() -> Result<()> {
    let db = db().await?;
    let before = RecordingTrigger::new(
        TriggerPhase::BeforeCommit,
        TriggerEventMask::NODE_CREATE,
        None,
    );
    register_trigger(&db, "veto-all", before.clone())?;
    let after = after_trigger(TriggerEventMask::NODE_CREATE, None);
    register_trigger(&db, "after-all", after.clone())?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().skip_before_triggers(true).build()?;
    bulk.insert_vertices("A", rows(10)).await?;
    bulk.commit().await?;
    tx.commit().await?;

    assert_eq!(count(&db, "MATCH (n:A) RETURN count(n) AS n").await?, 10);
    assert_eq!(before.rows(), 0, "a skipped before-trigger must not run");
    assert_eq!(after.rows(), 10);
    Ok(())
}

/// A committed bulk load is not undone by rolling back the transaction it was
/// built from: it commits on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bulk_load_survives_rollback_of_its_transaction() -> Result<()> {
    let db = db().await?;
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    bulk.insert_vertices("A", rows(5)).await?;
    bulk.commit().await?;
    tx.rollback();
    assert_eq!(count(&db, "MATCH (n:A) RETURN count(n) AS n").await?, 5);
    Ok(())
}

/// A load whose publish `commit` could not finish (`BulkCommitIncomplete`)
/// emits its events from the background re-drive, once that makes it visible
/// -- and not before.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redriven_load_emits_when_it_becomes_visible_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let db = Uni::open(dir.path().to_str().unwrap()).build().await?;
    db.schema()
        .label("A")
        .property("x", DataType::Int64)
        .apply()
        .await?;
    let t = after_trigger(TriggerEventMask::NODE_CREATE, Some(&["A"]));
    register_trigger(&db, "redrive-node-create", t.clone())?;

    fail::cfg("bulk::commit::publish-table", "1*off->1*return->off").unwrap();
    fail::cfg("bulk::commit::complete-publish", "return").unwrap();
    fail::cfg("bulk::redrive::attempt", "return").unwrap();
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    bulk.insert_vertices("A", rows(50)).await?;
    let outcome = bulk.commit().await;
    tx.commit().await?;
    fail::remove("bulk::commit::publish-table");
    fail::remove("bulk::commit::complete-publish");
    let err = outcome.expect_err("premise: the publish must not finish in commit");
    assert!(
        matches!(
            err.downcast_ref::<UniError>(),
            Some(UniError::BulkCommitIncomplete { .. })
        ),
        "premise: {err:?}"
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(t.rows(), 0, "no event while the load is still hidden");

    fail::remove("bulk::redrive::attempt");
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while t.rows() < 50 && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(t.rows(), 50, "the re-driven load's rows");
    assert_eq!(count(&db, "MATCH (n:A) RETURN count(n) AS n").await?, 50);
    Ok(())
}
