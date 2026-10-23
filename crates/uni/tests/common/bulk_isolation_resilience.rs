// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Isolation of an in-progress bulk load, and a clean abort, under compaction.
//!
//! The bulk writer used to append every flush straight into the live tables and
//! undo an abort by restoring each table's pre-load Lance version. Measured
//! against that design:
//!
//! - another session saw the uncommitted vertices and edges, with or without a
//!   compaction pass (a cold adjacency CSR reads the delta tables directly);
//! - a background pass folded the uncommitted deltas into L2 adjacency, which
//!   abort does not restore, so after abort every bulk edge was still
//!   traversable in both directions with no `main_edges` row — properties
//!   `NULL` — and it stayed that way after a flush and after reopen;
//! - abort restored `main_edges`, the delta tables and the vertex tables to
//!   their pre-load versions, discarding an edge and a vertex another session
//!   had committed and flushed in the meantime.
//!
//! The writer now stages its rows as data files no table version references and
//! publishes them at commit, so there is nothing to see before commit and
//! nothing to undo on abort. Background compaction is driven for real (enabled,
//! 50 ms tick, a 1-byte size trigger) and the tests wait for a completed pass.

#![cfg(feature = "failpoints")]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use anyhow::Result;
use futures::FutureExt;
use uni_db::{Uni, UniConfig, Value, Vid};
use uni_store::backend::table_names;
use uni_store::storage::compaction::Compactor;

// Rust guideline compliant

/// Edges the bulk load writes, all between pre-existing (committed) vertices so
/// a leftover adjacency entry is reachable from a live endpoint.
const BULK_EDGES: usize = 30;
/// Committed vertices the bulk edges connect.
const SEED: usize = 10;
/// Vertices the bulk load inserts (`k` in `100..120`).
const BULK_VERTICES: i64 = 20;
/// `w` / `k` of the edge and vertex another session commits mid-load.
const CONCURRENT: i64 = 999;

fn config(background_compaction: bool) -> UniConfig {
    let mut cfg = UniConfig {
        async_flush_enabled: false,
        ..Default::default()
    };
    cfg.compaction.enabled = background_compaction;
    cfg.compaction.check_interval = Duration::from_millis(50);
    // Any delta row triggers a pass, so a pass runs whenever there is
    // committed L1 data to fold.
    cfg.compaction.max_l1_size_bytes = 1;
    cfg
}

/// Ten committed `P` vertices and one committed edge (`w = -1`), flushed.
async fn seed(db: &Uni) -> Result<Vec<Vid>> {
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL P (k INT)").await?;
    tx.execute("CREATE EDGE TYPE E (w INT) FROM P TO P").await?;
    tx.commit().await?;
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "UNWIND range(0, {}) AS i CREATE (:P {{k: i}})",
        SEED - 1
    ))
    .await?;
    tx.execute("MATCH (a:P {k: 0}), (b:P {k: 1}) CREATE (a)-[:E {w: -1}]->(b)")
        .await?;
    tx.commit().await?;
    db.flush().await?;
    seed_vids(db).await
}

async fn seed_vids(db: &Uni) -> Result<Vec<Vid>> {
    let r = db
        .session()
        .query(&format!(
            "MATCH (n:P) WHERE n.k < {SEED} RETURN id(n) AS id ORDER BY n.k"
        ))
        .await?;
    r.rows()
        .iter()
        .map(|row| Ok(Vid::from(row.get::<i64>("id")? as u64)))
        .collect()
}

fn bulk_edges(seed: &[Vid]) -> Vec<uni_bulk::EdgeData> {
    (0..BULK_EDGES)
        .map(|i| {
            uni_bulk::EdgeData::new(
                seed[i % SEED],
                seed[(i * 7 + 3) % SEED],
                HashMap::from([("w".to_string(), Value::Int(i as i64))]),
            )
        })
        .collect()
}

fn bulk_vertices() -> Vec<HashMap<String, Value>> {
    (0..BULK_VERTICES)
        .map(|i| HashMap::from([("k".to_string(), Value::Int(100 + i))]))
        .collect()
}

async fn count(db: &Uni, q: &str) -> Result<i64> {
    let r = db.session().query(q).await?;
    Ok(r.rows()[0].get::<i64>("c")?)
}

/// What a reader sees of `E` and `P`.
#[derive(Debug, PartialEq, Eq)]
struct View {
    /// `MATCH ()-[r:E]->()`.
    edges: i64,
    /// Traversal from the source side.
    fwd: i64,
    /// Traversal from the destination side.
    bwd: i64,
    /// Edges whose property is gone: an adjacency entry with no `main_edges` row.
    dangling: i64,
    /// Edges carrying a bulk `w` (`0..BULK_EDGES`).
    bulk_edges: i64,
    /// Vertices the bulk load inserted.
    bulk_vertices: i64,
}

async fn view(db: &Uni) -> Result<View> {
    Ok(View {
        edges: count(db, "MATCH ()-[r:E]->() RETURN count(r) AS c").await?,
        fwd: count(db, "MATCH (a:P)-[r:E]->(b:P) RETURN count(r) AS c").await?,
        bwd: count(db, "MATCH (b:P)<-[r:E]-(a:P) RETURN count(r) AS c").await?,
        dangling: count(
            db,
            "MATCH (a:P)-[r:E]->(b:P) WHERE r.w IS NULL RETURN count(r) AS c",
        )
        .await?,
        bulk_edges: count(
            db,
            &format!(
                "MATCH (a:P)-[r:E]->(b:P) WHERE r.w >= 0 AND r.w < {BULK_EDGES} \
                 RETURN count(r) AS c"
            ),
        )
        .await?,
        bulk_vertices: count(
            db,
            &format!(
                "MATCH (n:P) WHERE n.k >= 100 AND n.k < {} RETURN count(n) AS c",
                100 + BULK_VERTICES
            ),
        )
        .await?,
    })
}

/// `n` committed edges, nothing from the bulk load, nothing dangling.
fn committed_only(n: i64) -> View {
    View {
        edges: n,
        fwd: n,
        bwd: n,
        dangling: 0,
        bulk_edges: 0,
        bulk_vertices: 0,
    }
}

/// Waits until a background pass that started after `after` has completed.
async fn wait_for_pass(db: &Uni, after: u64) -> Result<u64> {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let status = db.storage().compaction_status()?;
        if status.total_compactions > after && !status.compaction_in_progress {
            return Ok(status.total_compactions);
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "no background compaction pass completed after pass {after}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn row_count(db: &Uni, table: &str) -> Result<usize> {
    let storage = db.storage();
    let backend = storage.backend();
    Ok(if backend.table_exists(table).await? {
        backend.count_rows(table, None).await?
    } else {
        0
    })
}

/// Another session commits an edge and a vertex and flushes them.
async fn commit_concurrent_writes(db: &Uni) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(&format!(
        "MATCH (a:P {{k: 2}}), (b:P {{k: 3}}) CREATE (a)-[:E {{w: {CONCURRENT}}}]->(b)"
    ))
    .await?;
    tx.execute(&format!("CREATE (:P {{k: {CONCURRENT}}})"))
        .await?;
    tx.commit().await?;
    db.flush().await?;
    Ok(())
}

/// How many of the concurrent session's edge and vertex a reader sees.
async fn concurrent_writes(db: &Uni) -> Result<(i64, i64)> {
    Ok((
        count(
            db,
            &format!("MATCH ()-[r:E {{w: {CONCURRENT}}}]->() RETURN count(r) AS c"),
        )
        .await?,
        count(
            db,
            &format!("MATCH (n:P {{k: {CONCURRENT}}}) RETURN count(n) AS c"),
        )
        .await?,
    ))
}

/// A background pass runs while a bulk load is open. Readers must not see the
/// load before that pass (cold CSR straight from the delta tables) nor after it
/// (CSR re-warmed from the rewritten L2), and the pass must not fold the load
/// into L2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uncommitted_bulk_load_invisible_across_compaction_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    {
        let db = Uni::open(&path).config(config(false)).build().await?;
        seed(&db).await?;
    }
    // Reopen so the first read of `E` below builds its CSR from storage.
    let db = Uni::open(&path).config(config(true)).build().await?;
    let seed = seed_vids(&db).await?;
    let passes = wait_for_pass(&db, 0).await?;

    // The load outlives its host transaction, which ends here: an open
    // transaction pins edge compaction at its snapshot, so while it lived no
    // pass could fold the concurrent writes below and the pass this test needs
    // could not run.
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(10).build()?;
    tx.rollback();
    bulk.insert_vertices("P", bulk_vertices()).await?;
    bulk.insert_edges("E", bulk_edges(&seed)).await?;

    assert_eq!(
        view(&db).await?,
        committed_only(1),
        "another session sees the uncommitted bulk load before any compaction pass"
    );

    // Give the background pass committed delta rows to fold, and wait for it.
    commit_concurrent_writes(&db).await?;
    wait_for_pass(&db, passes).await?;
    assert_eq!(
        row_count(&db, &table_names::delta_table_name("E", "fwd")).await?,
        0,
        "premise: the pass must have folded and cleared the committed deltas, \
         or it never ran inside the bulk load's window"
    );
    assert_eq!(
        view(&db).await?,
        committed_only(2),
        "another session sees the uncommitted bulk load after a compaction pass"
    );

    bulk.commit().await?;
    let committed = View {
        bulk_edges: BULK_EDGES as i64,
        bulk_vertices: BULK_VERTICES,
        ..committed_only(2 + BULK_EDGES as i64)
    };
    assert_eq!(view(&db).await?, committed, "the committed load");
    drop(db);
    let db = Uni::open(&path).config(config(false)).build().await?;
    assert_eq!(
        view(&db).await?,
        committed,
        "the committed load after reopen"
    );
    Compactor::new(db.storage()).compact_all().await?;
    drop(db);
    let db = Uni::open(&path).config(config(false)).build().await?;
    assert_eq!(
        view(&db).await?,
        committed,
        "the committed load after compaction and reopen"
    );
    Ok(())
}

/// Abort after a background pass ran over the open load: no bulk edge may stay
/// reachable in either direction — flushed, and after reopen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_abort_after_compaction_leaves_no_dangling_edges_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config(true)).build().await?;
    let seed = seed(&db).await?;
    let passes = wait_for_pass(&db, 0).await?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(10).build()?;
    bulk.insert_vertices("P", bulk_vertices()).await?;
    bulk.insert_edges("E", bulk_edges(&seed)).await?;
    commit_concurrent_writes(&db).await?;
    wait_for_pass(&db, passes).await?;

    bulk.abort().await?;
    drop(tx);

    let expected = committed_only(2);
    assert_eq!(view(&db).await?, expected, "after abort");
    db.flush().await?;
    assert_eq!(view(&db).await?, expected, "after abort and flush");
    drop(db);
    let db = Uni::open(&path).config(config(false)).build().await?;
    assert_eq!(view(&db).await?, expected, "after abort and reopen");
    Ok(())
}

/// Abort must not discard what another session committed to the same tables
/// while the load was open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_abort_keeps_concurrent_committed_writes_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config(false)).build().await?;
    let seed = seed(&db).await?;

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(10).build()?;
    bulk.insert_edges("E", bulk_edges(&seed)).await?;
    bulk.insert_vertices("P", bulk_vertices()).await?;
    commit_concurrent_writes(&db).await?;
    bulk.abort().await?;
    drop(tx);

    assert_eq!(
        concurrent_writes(&db).await?,
        (1, 1),
        "abort discarded an edge and a vertex another session had committed"
    );
    assert_eq!(view(&db).await?, committed_only(2));
    drop(db);
    let db = Uni::open(&path).config(config(false)).build().await?;
    assert_eq!(concurrent_writes(&db).await?, (1, 1), "after reopen");
    assert_eq!(view(&db).await?, committed_only(2), "after reopen");
    Ok(())
}

/// A crash part way through publishing, then a compaction pass that folds the
/// already-published forward deltas into L2 (and clears them from the delta
/// table): reopen completes the publish, and must recognize the forward
/// deltas as already published although no current fragment holds them, or
/// the forward direction counts every bulk edge twice. Both directions must
/// end up agreeing with `main_edges`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_publish_crash_after_compaction_completes_consistently_resilience() -> Result<()> {
    const SEAM: &str = "delta::before-write-run";
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Arc::new(Uni::open(&path).config(config(false)).build().await?);
    let seed = seed(&db).await?;
    Compactor::new(db.storage()).compact_all().await?;
    let adj_fwd = table_names::adjacency_table_name("E", "fwd");
    let adj_rows_before = row_count(&db, &adj_fwd).await?;

    // Publish hit 1 is the fwd delta, hit 2 the bwd delta; hit 2 crashes. No
    // pass can run during the publish itself (it holds the bulk gate, which a
    // pass must take exclusively), so the pass below runs where it can in
    // production: after the crash, over the half-published load, before the
    // reopen that completes it.
    let hits = Arc::new(AtomicUsize::new(0));
    {
        let hits = Arc::clone(&hits);
        fail::cfg_callback(SEAM, move || {
            if hits.fetch_add(1, Ordering::SeqCst) == 1 {
                panic!("injected crash while publishing the bwd delta");
            }
        })
        .unwrap();
    }

    // The host transaction ends before the publish: an open transaction pins
    // edge compaction at its snapshot, so while it lived the pass could not
    // fold the half-published load.
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(10).build()?;
    tx.rollback();
    bulk.insert_edges("E", bulk_edges(&seed)).await?;
    let crashed = std::panic::AssertUnwindSafe(bulk.commit())
        .catch_unwind()
        .await
        .is_err();
    fail::remove(SEAM);
    assert_eq!(
        hits.load(Ordering::SeqCst),
        2,
        "premise: the publish must reach the bwd delta"
    );
    Compactor::new(db.storage()).compact_all().await?;
    let adj_rows_mid = row_count(&db, &adj_fwd).await?;
    assert!(
        crashed,
        "premise: the injected crash must interrupt the commit"
    );
    assert!(
        adj_rows_mid > adj_rows_before,
        "premise: the pass must have folded the published fwd deltas into L2 \
         ({adj_rows_before} -> {adj_rows_mid} rows), or the test proves nothing"
    );
    drop(db);

    let db = Uni::open(&path).config(config(false)).build().await?;
    let completed = View {
        bulk_edges: BULK_EDGES as i64,
        bulk_vertices: 0,
        ..committed_only(1 + BULK_EDGES as i64)
    };
    assert_eq!(
        view(&db).await?,
        completed,
        "reopen after a crash mid-publish must complete the load exactly once"
    );
    drop(db);
    let db = Uni::open(&path).config(config(false)).build().await?;
    assert_eq!(
        view(&db).await?,
        completed,
        "a second reopen changes nothing"
    );
    Compactor::new(db.storage()).compact_all().await?;
    assert_eq!(view(&db).await?, completed, "after compaction");
    Ok(())
}

// ---------------------------------------------------------------------------
// A crash mid-publish must not cost any other writer its committed data, and
// every interrupted publish must be reconciled, not only the last one.
// ---------------------------------------------------------------------------

/// Rows of one bulk load as a reader sees them.
#[derive(Debug, PartialEq, Eq)]
struct LoadView {
    /// Vertices of the load (`k` in its range).
    vertices: i64,
    /// Edges of the load (`w` in its range), traversed from the source side.
    fwd: i64,
    /// The same edges, traversed from the destination side.
    bwd: i64,
    /// The same edges, matched without endpoint labels.
    edges: i64,
}

/// A bulk load's vertex `k` range and edge `w` range.
#[derive(Debug, Clone, Copy)]
struct Load {
    k_from: i64,
    w_from: i64,
}

/// The load the earlier tests use: `k` in `100..120`, `w` in `0..30`.
const LOAD_A: Load = Load {
    k_from: 100,
    w_from: 0,
};
/// A second, disjoint load: `k` in `200..220`, `w` in `1000..1030`.
const LOAD_B: Load = Load {
    k_from: 200,
    w_from: 1000,
};

impl Load {
    fn vertices(self) -> Vec<HashMap<String, Value>> {
        (0..BULK_VERTICES)
            .map(|i| HashMap::from([("k".to_string(), Value::Int(self.k_from + i))]))
            .collect()
    }

    fn edges(self, seed: &[Vid]) -> Vec<uni_bulk::EdgeData> {
        (0..BULK_EDGES)
            .map(|i| {
                uni_bulk::EdgeData::new(
                    seed[i % SEED],
                    seed[(i * 7 + 3) % SEED],
                    HashMap::from([("w".to_string(), Value::Int(self.w_from + i as i64))]),
                )
            })
            .collect()
    }

    async fn view(self, db: &Uni) -> Result<LoadView> {
        let w = format!(
            "r.w >= {} AND r.w < {}",
            self.w_from,
            self.w_from + BULK_EDGES as i64
        );
        Ok(LoadView {
            vertices: count(
                db,
                &format!(
                    "MATCH (n:P) WHERE n.k >= {} AND n.k < {} RETURN count(n) AS c",
                    self.k_from,
                    self.k_from + BULK_VERTICES
                ),
            )
            .await?,
            fwd: count(
                db,
                &format!("MATCH (a:P)-[r:E]->(b:P) WHERE {w} RETURN count(r) AS c"),
            )
            .await?,
            bwd: count(
                db,
                &format!("MATCH (b:P)<-[r:E]-(a:P) WHERE {w} RETURN count(r) AS c"),
            )
            .await?,
            edges: count(
                db,
                &format!("MATCH ()-[r:E]->() WHERE {w} RETURN count(r) AS c"),
            )
            .await?,
        })
    }

    /// Asserts the load is entirely present. An interrupted load whose
    /// publish had begun is completed at the next open, so this is also what
    /// recovery must leave: all of it, each row once.
    async fn assert_all(self, db: &Uni, what: &str) -> Result<()> {
        let seen = self.view(db).await?;
        assert_eq!(
            seen,
            LoadView {
                vertices: BULK_VERTICES,
                fwd: BULK_EDGES as i64,
                bwd: BULK_EDGES as i64,
                edges: BULK_EDGES as i64,
            },
            "{what}: the load must be entirely present, each row once"
        );
        Ok(())
    }
}

/// The seed's ten vertices and one edge.
async fn assert_seed_intact(db: &Uni, what: &str) -> Result<()> {
    assert_eq!(
        count(
            db,
            &format!("MATCH (n:P) WHERE n.k < {SEED} RETURN count(n) AS c")
        )
        .await?,
        SEED as i64,
        "{what}: seed vertices"
    );
    assert_eq!(
        count(db, "MATCH (:P)-[r:E {w: -1}]->(:P) RETURN count(r) AS c").await?,
        1,
        "{what}: seed edge"
    );
    Ok(())
}

/// Publish seam hits of one load with one edge type: 0 = fwd delta, 1 = bwd.
const PUBLISH_SEAM: &str = "delta::before-write-run";

/// Runs `load`'s commit with a crash injected at its bwd delta publish — after
/// its vertex tables, `main_edges` and fwd delta are already published.
/// Returns once the panic has unwound out of the commit.
async fn crash_mid_publish(db: &Uni, load: Load, seed: &[Vid]) -> Result<()> {
    crash_at_publish_hit(db, load, seed, 1).await
}

/// Runs `load`'s commit with a crash injected at publish seam hit `crash_hit`
/// (0 = the fwd delta, 1 = the bwd delta).
async fn crash_at_publish_hit(db: &Uni, load: Load, seed: &[Vid], crash_hit: usize) -> Result<()> {
    let hits = Arc::new(AtomicUsize::new(0));
    {
        let hits = Arc::clone(&hits);
        fail::cfg_callback(PUBLISH_SEAM, move || {
            if hits.fetch_add(1, Ordering::SeqCst) == crash_hit {
                panic!("injected crash while publishing a delta table");
            }
        })
        .unwrap();
    }
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(10).build()?;
    bulk.insert_vertices("P", load.vertices()).await?;
    bulk.insert_edges("E", load.edges(seed)).await?;
    let crashed = std::panic::AssertUnwindSafe(bulk.commit())
        .catch_unwind()
        .await
        .is_err();
    fail::remove(PUBLISH_SEAM);
    assert!(
        crashed,
        "premise: the injected crash must interrupt the commit"
    );
    assert_eq!(
        hits.load(Ordering::SeqCst),
        crash_hit + 1,
        "premise: the crash must come at publish seam hit {crash_hit}"
    );
    drop(tx);
    Ok(())
}

/// Another session commits and flushes while a load's publish is part way
/// through; the process then dies in that publish. Reopen must keep the other
/// session's committed vertex and edge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_publish_crash_keeps_commits_made_inside_the_publish_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Arc::new(Uni::open(&path).config(config(false)).build().await?);
    let seed = seed(&db).await?;

    // Publish hit 1 (the bwd delta) parks until the other session has
    // committed and flushed into the same tables, then crashes. The other
    // session's flush passes the same seam; only hit 1 parks.
    let (reached_tx, reached_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(release_rx);
    let hits = Arc::new(AtomicUsize::new(0));
    {
        let hits = Arc::clone(&hits);
        fail::cfg_callback(PUBLISH_SEAM, move || {
            if hits.fetch_add(1, Ordering::SeqCst) == 1 {
                reached_tx.send(()).ok();
                release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(30))
                    .ok();
                panic!("injected crash while publishing the bwd delta");
            }
        })
        .unwrap();
    }
    let writer = tokio::spawn({
        let db = Arc::clone(&db);
        async move {
            let reached = tokio::task::spawn_blocking(move || {
                reached_rx.recv_timeout(Duration::from_secs(30))
            })
            .await?;
            let result = match reached {
                Ok(()) => commit_concurrent_writes(&db).await.map(|()| true),
                Err(_) => Ok(false),
            };
            release_tx.send(()).ok();
            result
        }
    });

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(10).build()?;
    bulk.insert_vertices("P", LOAD_A.vertices()).await?;
    bulk.insert_edges("E", LOAD_A.edges(&seed)).await?;
    let crashed = std::panic::AssertUnwindSafe(bulk.commit())
        .catch_unwind()
        .await
        .is_err();
    fail::remove(PUBLISH_SEAM);
    assert!(
        writer.await??,
        "premise: the other session must commit inside the publish"
    );
    assert!(
        crashed,
        "premise: the injected crash must interrupt the commit"
    );
    assert_eq!(
        concurrent_writes(&db).await?,
        (1, 1),
        "premise: the other session's writes are committed before the crash"
    );
    drop(tx);
    drop(db);

    let db = Uni::open(&path).config(config(false)).build().await?;
    assert_eq!(
        concurrent_writes(&db).await?,
        (1, 1),
        "reopen after a crash mid-publish discarded an edge and a vertex another \
         session committed and flushed during the publish"
    );
    assert_seed_intact(&db, "after reopen").await?;
    LOAD_A.assert_all(&db, "after reopen").await?;
    Ok(())
}

/// A publish fails part way and the process carries on: another session
/// commits and flushes afterwards. The next open must keep those commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_publish_failure_keeps_later_commits_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config(false)).build().await?;
    let seed = seed(&db).await?;

    crash_mid_publish(&db, LOAD_A, &seed).await?;
    commit_concurrent_writes(&db).await?;
    drop(db);

    let db = Uni::open(&path).config(config(false)).build().await?;
    assert_eq!(
        concurrent_writes(&db).await?,
        (1, 1),
        "reopen discarded an edge and a vertex committed after a failed publish"
    );
    assert_seed_intact(&db, "after reopen").await?;
    LOAD_A.assert_all(&db, "after reopen").await?;
    Ok(())
}

/// Load A's publish fails part way; load B then commits in full. The database
/// must still reconcile A at the next open, and keep all of B.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_publish_failure_reconciled_after_another_load_commits_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config(false)).build().await?;
    let seed = seed(&db).await?;

    crash_mid_publish(&db, LOAD_A, &seed).await?;
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(10).build()?;
    bulk.insert_vertices("P", LOAD_B.vertices()).await?;
    bulk.insert_edges("E", LOAD_B.edges(&seed)).await?;
    bulk.commit().await?;
    tx.commit().await?;
    LOAD_B.assert_all(&db, "before reopen").await?;
    drop(db);

    let db = Uni::open(&path).config(config(false)).build().await?;
    assert_seed_intact(&db, "after reopen").await?;
    LOAD_B.assert_all(&db, "after reopen").await?;
    LOAD_A.assert_all(&db, "after reopen").await?;
    Ok(())
}

/// Two publishes are interrupted before the process dies. Both must be
/// reconciled at the next open, not only the one that wrote last.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_two_interrupted_publishes_both_reconciled_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config(false)).build().await?;
    let seed = seed(&db).await?;

    crash_mid_publish(&db, LOAD_A, &seed).await?;
    crash_mid_publish(&db, LOAD_B, &seed).await?;
    drop(db);

    let db = Uni::open(&path).config(config(false)).build().await?;
    assert_seed_intact(&db, "after reopen").await?;
    LOAD_A.assert_all(&db, "load A after reopen").await?;
    LOAD_B.assert_all(&db, "load B after reopen").await?;
    Ok(())
}

/// Recovery itself is interrupted after publishing one of the tables the
/// crashed publish had not reached. The next open must finish the rest and
/// publish nothing twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_recovery_interrupted_completes_exactly_once_resilience() -> Result<()> {
    const RECOVERY_SEAM: &str = "bulk::recover::after-table";
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config(false)).build().await?;
    let seed = seed(&db).await?;
    // Crash at the fwd delta: both deltas are left for recovery.
    crash_at_publish_hit(&db, LOAD_A, &seed, 0).await?;
    drop(db);

    fail::cfg(RECOVERY_SEAM, "1*return").unwrap();
    let interrupted = Uni::open(&path).config(config(false)).build().await;
    fail::remove(RECOVERY_SEAM);
    assert!(
        interrupted.is_err(),
        "premise: the injected failure must interrupt recovery"
    );
    drop(interrupted);

    let db = Uni::open(&path).config(config(false)).build().await?;
    assert_seed_intact(&db, "after the second recovery").await?;
    LOAD_A.assert_all(&db, "after the second recovery").await?;
    drop(db);
    let db = Uni::open(&path).config(config(false)).build().await?;
    LOAD_A.assert_all(&db, "after a third open").await?;
    Ok(())
}

/// A crash after the load's `Committed` marker, before it moved the latest
/// snapshot pointer; another session then commits and flushes, moving the
/// pointer on. Recovery must not move it back to the load's older snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_committed_recovery_keeps_a_later_snapshot_resilience() -> Result<()> {
    const SEAM: &str = "bulk::commit::after-committed-marker";
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config(false)).build().await?;
    let seed = seed(&db).await?;

    fail::cfg(SEAM, "panic").unwrap();
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(10).build()?;
    bulk.insert_vertices("P", LOAD_A.vertices()).await?;
    bulk.insert_edges("E", LOAD_A.edges(&seed)).await?;
    let crashed = std::panic::AssertUnwindSafe(bulk.commit())
        .catch_unwind()
        .await
        .is_err();
    fail::remove(SEAM);
    assert!(
        crashed,
        "premise: the injected crash must interrupt the commit"
    );
    drop(tx);

    commit_concurrent_writes(&db).await?;
    let latest = db
        .storage()
        .snapshot_manager()
        .load_latest_snapshot()
        .await?
        .ok_or_else(|| anyhow::anyhow!("premise: a flush must leave a latest snapshot"))?;
    drop(db);

    let db = Uni::open(&path).config(config(false)).build().await?;
    let reopened = db
        .storage()
        .snapshot_manager()
        .load_latest_snapshot()
        .await?
        .ok_or_else(|| anyhow::anyhow!("no latest snapshot after reopen"))?;
    assert_eq!(
        reopened.snapshot_id, latest.snapshot_id,
        "recovery moved the latest snapshot pointer back to the bulk load's"
    );
    assert_eq!(concurrent_writes(&db).await?, (1, 1), "after reopen");
    assert_seed_intact(&db, "after reopen").await?;
    LOAD_A.assert_all(&db, "after reopen").await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// The commit point. Once the publish marker is durable the load is decided:
// `commit` must finish it in-process, or say it is decided, never answer a
// plain error that invites a retry.
// ---------------------------------------------------------------------------

/// Fails publish seam calls `1..=n` after `skip` successful ones.
const COMMIT_PUBLISH_SEAM: &str = "bulk::commit::publish-table";

/// Runs one load of `load`'s vertices and edges; if `commit` fails, retries it
/// once with a fresh writer, as a caller told only "it failed" would. Returns
/// whether the first commit failed.
async fn commit_retrying_on_error(db: &Uni, load: Load, seed: &[Vid]) -> Result<bool> {
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(10).build()?;
    bulk.insert_vertices("P", load.vertices()).await?;
    bulk.insert_edges("E", load.edges(seed)).await?;
    let first = bulk.commit().await;
    for name in [COMMIT_PUBLISH_SEAM, "bulk::commit::save-snapshot"] {
        fail::remove(name);
    }
    let failed = first.is_err();
    if failed {
        let mut bulk = tx.bulk_writer().batch_size(10).build()?;
        bulk.insert_vertices("P", load.vertices()).await?;
        bulk.insert_edges("E", load.edges(seed)).await?;
        bulk.commit().await?;
    }
    tx.commit().await?;
    Ok(failed)
}

/// H1: a publish that fails after the per-label table but before `vertices`.
/// The marker already decided the load, so the next open completes it; a caller
/// that retried an `Err` then held the load twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_commit_mid_publish_failure_never_invites_a_double_load_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config(false)).build().await?;
    let seed = seed(&db).await?;

    // Publish order: vertices_P, vertices, main_edges, deltas. Hit 2 fails.
    fail::cfg(COMMIT_PUBLISH_SEAM, "1*off->1*return->off").unwrap();
    let first_failed = commit_retrying_on_error(&db, LOAD_A, &seed).await?;
    let live = LOAD_A.view(&db).await?;
    drop(db);
    let db = Uni::open(&path).config(config(false)).build().await?;
    LOAD_A
        .assert_all(
            &db,
            &format!("after reopen (first commit failed: {first_failed}, live view: {live:?})"),
        )
        .await?;
    assert_seed_intact(&db, "after reopen").await?;
    assert!(
        !first_failed,
        "a publish failure after the commit point must be completed in-process, not \
         returned as a retryable error"
    );
    Ok(())
}

/// H2: a failure after the whole load is published (the snapshot manifest).
/// The load is fully visible already; an `Err` there invited a retry that
/// doubled it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_commit_failure_after_publish_never_invites_a_double_load_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config(false)).build().await?;
    let seed = seed(&db).await?;

    fail::cfg("bulk::commit::save-snapshot", "1*return->off").unwrap();
    let first_failed = commit_retrying_on_error(&db, LOAD_A, &seed).await?;
    let live = LOAD_A.view(&db).await?;
    drop(db);
    let db = Uni::open(&path).config(config(false)).build().await?;
    LOAD_A
        .assert_all(
            &db,
            &format!("after reopen (first commit failed: {first_failed}, live view: {live:?})"),
        )
        .await?;
    assert!(
        !first_failed,
        "a best-effort step failing after the load is published must not fail the commit"
    );
    Ok(())
}

/// H3: after a publish failure part way, live readers saw the per-label table
/// without `vertices` (label scan and label-less scan disagreed) until the next
/// open. `commit` returning must leave every table published.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_commit_mid_publish_failure_leaves_no_partial_load_live_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config(false)).build().await?;
    let seed = seed(&db).await?;

    fail::cfg(COMMIT_PUBLISH_SEAM, "1*off->1*return->off").unwrap();
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(10).build()?;
    bulk.insert_vertices("P", LOAD_A.vertices()).await?;
    bulk.insert_edges("E", LOAD_A.edges(&seed)).await?;
    let outcome = bulk.commit().await;
    fail::remove(COMMIT_PUBLISH_SEAM);
    drop(tx);

    let range = format!(
        "n.k >= {} AND n.k < {}",
        LOAD_A.k_from,
        LOAD_A.k_from + BULK_VERTICES
    );
    let by_label = count(
        &db,
        &format!("MATCH (n:P) WHERE {range} RETURN count(n) AS c"),
    )
    .await?;
    let unlabelled = count(
        &db,
        &format!("MATCH (n) WHERE {range} RETURN count(n) AS c"),
    )
    .await?;
    assert_eq!(
        (by_label, unlabelled),
        (BULK_VERTICES, BULK_VERTICES),
        "live readers after commit returned ({outcome:?}): the label scan and the \
         label-less scan must both see the whole load"
    );
    assert!(outcome.is_ok(), "commit outcome: {outcome:?}");
    LOAD_A.assert_all(&db, "live").await?;
    Ok(())
}

/// Data files under every Lance dataset's `data/` directory in the database.
fn data_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    fn walk(dir: &std::path::Path, in_data: bool, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let data = in_data || path.file_name().is_some_and(|n| n == "data");
                walk(&path, data, out);
            } else if in_data {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(root, false, &mut out);
    out.sort();
    out
}

/// The typed bulk-commit error inside `e`, if any.
fn uni_error(e: &anyhow::Error) -> Option<&uni_db::UniError> {
    e.downcast_ref::<uni_db::UniError>()
}

/// Starts load `load` and commits it; returns the load id and the outcome.
async fn commit_load(
    db: &Uni,
    load: Load,
    seed: &[Vid],
) -> Result<(String, anyhow::Result<uni_bulk::BulkStats>)> {
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(10).build()?;
    let load_id = bulk.load_id().to_string();
    bulk.insert_vertices("P", load.vertices()).await?;
    bulk.insert_edges("E", load.edges(seed)).await?;
    let outcome = bulk.commit().await;
    drop(tx);
    Ok((load_id, outcome))
}

/// Asserts nothing of `load` is visible.
async fn assert_none(db: &Uni, load: Load, what: &str) -> Result<()> {
    assert_eq!(
        load.view(db).await?,
        LoadView {
            vertices: 0,
            fwd: 0,
            bwd: 0,
            edges: 0
        },
        "{what}: nothing of the load may be visible"
    );
    Ok(())
}

/// A failure before the commit point is a plain error that means "not
/// applied": nothing visible, live or after a reopen, the staged files
/// deleted, and the load reported not applied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_commit_failure_before_commit_point_applies_nothing_resilience() -> Result<()> {
    for (seam, action) in [
        ("bulk::commit::before-commit-point", "return"),
        ("bulk::commit::marker-write", "return(terminal)"),
    ] {
        let dir = tempfile::TempDir::new()?;
        let path = dir.path().to_str().unwrap().to_string();
        let db = Uni::open(&path).config(config(false)).build().await?;
        let seed = seed(&db).await?;
        let files_before = data_files(dir.path());

        fail::cfg(seam, action).unwrap();
        let (load_id, outcome) = commit_load(&db, LOAD_A, &seed).await?;
        fail::remove(seam);
        let err = outcome.expect_err("premise: the injected failure must fail the commit");
        assert!(
            !matches!(
                uni_error(&err),
                Some(
                    uni_db::UniError::BulkCommitIncomplete { .. }
                        | uni_db::UniError::BulkCommitOutcomeUnknown { .. }
                )
            ),
            "{seam}: a failure before the commit point is not a committed outcome: {err:#}"
        );
        assert_none(&db, LOAD_A, &format!("{seam}: live")).await?;
        assert!(!db.bulk_load_applied(&load_id).await?, "{seam}: live");
        assert_eq!(
            data_files(dir.path()),
            files_before,
            "{seam}: the load's staged files must be deleted"
        );
        drop(db);

        let db = Uni::open(&path).config(config(false)).build().await?;
        assert_none(&db, LOAD_A, &format!("{seam}: after reopen")).await?;
        assert_seed_intact(&db, seam).await?;
        assert!(
            !db.bulk_load_applied(&load_id).await?,
            "{seam}: after reopen"
        );
    }
    Ok(())
}

/// A failure after the commit point that the in-process completion recovers
/// from: `Ok`, with the failed attempt in `warnings`, the load exactly once
/// live and after a reopen, and reported applied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_commit_recovered_after_commit_point_returns_ok_with_warnings_resilience() -> Result<()>
{
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config(false)).build().await?;
    let seed = seed(&db).await?;

    // The second table's publish fails, and so does the first completion
    // attempt; the second completion attempt succeeds.
    fail::cfg(COMMIT_PUBLISH_SEAM, "1*off->1*return->off").unwrap();
    fail::cfg("bulk::commit::complete-publish", "1*return->off").unwrap();
    let (load_id, outcome) = commit_load(&db, LOAD_A, &seed).await?;
    fail::remove(COMMIT_PUBLISH_SEAM);
    fail::remove("bulk::commit::complete-publish");
    let stats = outcome?;
    assert_eq!(stats.load_id, load_id);
    assert_eq!(stats.vertices_inserted, BULK_VERTICES as usize);
    assert!(
        stats
            .warnings
            .iter()
            .any(|w| w.contains("injected failure publishing 'vertices'") && w.contains("retry 2")),
        "the failed attempts must be reported: {:?}",
        stats.warnings
    );
    LOAD_A.assert_all(&db, "live").await?;
    assert!(db.bulk_load_applied(&load_id).await?, "live");
    drop(db);

    let db = Uni::open(&path).config(config(false)).build().await?;
    LOAD_A.assert_all(&db, "after reopen").await?;
    assert_seed_intact(&db, "after reopen").await?;
    assert!(db.bulk_load_applied(&load_id).await?, "after reopen");
    Ok(())
}

/// The publish keeps failing after the commit point: `BulkCommitIncomplete`
/// naming the load, which is reported applied; the next open completes it
/// exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_commit_persistent_failure_after_commit_point_is_incomplete_resilience() -> Result<()>
{
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config(false)).build().await?;
    let seed = seed(&db).await?;

    fail::cfg(COMMIT_PUBLISH_SEAM, "1*off->1*return->off").unwrap();
    fail::cfg("bulk::commit::complete-publish", "return").unwrap();
    let (load_id, outcome) = commit_load(&db, LOAD_A, &seed).await?;
    fail::remove(COMMIT_PUBLISH_SEAM);
    fail::remove("bulk::commit::complete-publish");
    let err = outcome.expect_err("premise: completion must keep failing");
    match uni_error(&err) {
        Some(uni_db::UniError::BulkCommitIncomplete { load_id: id, .. }) => {
            assert_eq!(id, &load_id);
        }
        other => panic!("expected BulkCommitIncomplete, got {other:?} ({err:#})"),
    }
    assert!(
        db.bulk_load_applied(&load_id).await?,
        "a committed load is applied although its publish is unfinished"
    );
    drop(db);

    let db = Uni::open(&path).config(config(false)).build().await?;
    LOAD_A.assert_all(&db, "after reopen").await?;
    assert_seed_intact(&db, "after reopen").await?;
    assert!(db.bulk_load_applied(&load_id).await?, "after reopen");
    drop(db);
    let db = Uni::open(&path).config(config(false)).build().await?;
    LOAD_A.assert_all(&db, "after a second reopen").await?;
    Ok(())
}

/// A committed load left to the next open must survive version cleanup run
/// meanwhile, however old its staged files are. Lance deletes a data file no
/// version references once it is 7 days old; recovery read "every staged file
/// gone" as "published and since rewritten" and skipped the tables silently.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_incomplete_load_survives_cleanup_of_aged_staged_files_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config(false)).build().await?;
    let seed = seed(&db).await?;

    fail::cfg(COMMIT_PUBLISH_SEAM, "1*off->1*return->off").unwrap();
    fail::cfg("bulk::commit::complete-publish", "return").unwrap();
    let (_, outcome) = commit_load(&db, LOAD_A, &seed).await?;
    fail::remove(COMMIT_PUBLISH_SEAM);
    fail::remove("bulk::commit::complete-publish");
    assert!(
        matches!(
            outcome.as_ref().err().and_then(uni_error),
            Some(uni_db::UniError::BulkCommitIncomplete { .. })
        ),
        "premise: {outcome:?}"
    );

    // Age every data file past Lance's grace period, then run cleanup on every
    // table the load staged into, as a compaction pass in this process would.
    let aged = std::time::SystemTime::now() - Duration::from_secs(8 * 24 * 3600);
    for file in data_files(dir.path()) {
        std::fs::File::options()
            .write(true)
            .open(&file)?
            .set_modified(aged)?;
    }
    let storage = db.storage();
    for table in [
        table_names::vertex_table_name("P"),
        table_names::main_vertex_table_name().to_string(),
        table_names::main_edge_table_name().to_string(),
        table_names::delta_table_name("E", "fwd"),
        table_names::delta_table_name("E", "bwd"),
    ] {
        storage
            .backend()
            .optimize_table(&table, Duration::ZERO)
            .await?;
    }
    drop(storage);
    drop(db);

    let db = Uni::open(&path).config(config(false)).build().await?;
    LOAD_A.assert_all(&db, "after cleanup and reopen").await?;
    assert_seed_intact(&db, "after cleanup and reopen").await?;
    Ok(())
}

/// The commit marker write reports a failure. Read back, the marker decides:
/// present means committed (`Ok`, exactly once); absent after a transport
/// failure, or unreadable, is `BulkCommitOutcomeUnknown` -- the write may
/// still land -- and the staged files are kept so a landed marker can be
/// completed at the next open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_commit_marker_write_failure_is_decided_by_read_back_resilience() -> Result<()> {
    // (marker-write action, fail the read-back, expected applied)
    for (action, fail_read, applied) in [
        ("return(landed)", false, true),
        ("return(transient)", false, false),
        ("return(landed)", true, true),
    ] {
        let dir = tempfile::TempDir::new()?;
        let path = dir.path().to_str().unwrap().to_string();
        let db = Uni::open(&path).config(config(false)).build().await?;
        let seed = seed(&db).await?;
        let files_before = data_files(dir.path());

        fail::cfg("bulk::commit::marker-write", action).unwrap();
        if fail_read {
            fail::cfg("bulk::commit::marker-read", "return").unwrap();
        }
        let (load_id, outcome) = commit_load(&db, LOAD_A, &seed).await?;
        fail::remove("bulk::commit::marker-write");
        fail::remove("bulk::commit::marker-read");
        let case = format!("{action}, read-back fails: {fail_read}");

        if action == "return(landed)" && !fail_read {
            let stats = outcome.map_err(|e| anyhow::anyhow!("{case}: {e:#}"))?;
            assert_eq!(stats.load_id, load_id);
            LOAD_A.assert_all(&db, &case).await?;
        } else {
            let err = outcome.expect_err("premise");
            match uni_error(&err) {
                Some(uni_db::UniError::BulkCommitOutcomeUnknown { load_id: id, .. }) => {
                    assert_eq!(id, &load_id, "{case}");
                }
                other => panic!("{case}: expected BulkCommitOutcomeUnknown, got {other:?}"),
            }
            assert_none(&db, LOAD_A, &format!("{case}: live")).await?;
            assert!(
                data_files(dir.path()).len() > files_before.len(),
                "{case}: the staged files must be kept while the outcome is unknown"
            );
        }
        drop(db);

        let db = Uni::open(&path).config(config(false)).build().await?;
        assert_eq!(
            db.bulk_load_applied(&load_id).await?,
            applied,
            "{case}: applied after reopen"
        );
        if applied {
            LOAD_A
                .assert_all(&db, &format!("{case}: after reopen"))
                .await?;
        } else {
            assert_none(&db, LOAD_A, &format!("{case}: after reopen")).await?;
        }
        assert_seed_intact(&db, &case).await?;
    }
    Ok(())
}

/// The index-status schema save after publish fails: `Ok` with the failure
/// in `warnings`, the commit marker kept, and the next open finishing it --
/// the load exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_commit_schema_save_failure_after_publish_is_a_warning_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config(false)).build().await?;
    let seed = seed(&db).await?;

    fail::cfg("bulk::commit::schema-save", "return").unwrap();
    let (load_id, outcome) = commit_load(&db, LOAD_A, &seed).await?;
    fail::remove("bulk::commit::schema-save");
    let stats = outcome?;
    assert!(
        stats.warnings.iter().any(|w| w.starts_with("schema save"))
            && stats.warnings.iter().any(|w| w.contains("marker kept")),
        "{:?}",
        stats.warnings
    );
    LOAD_A.assert_all(&db, "live").await?;
    assert!(db.bulk_load_applied(&load_id).await?, "live, marker kept");
    drop(db);

    let db = Uni::open(&path).config(config(false)).build().await?;
    LOAD_A.assert_all(&db, "after reopen").await?;
    assert!(db.bulk_load_applied(&load_id).await?, "after reopen");
    Ok(())
}

// ---------------------------------------------------------------------------
// Wave P1: one load becomes visible at once (B1), a load left incomplete is
// re-driven in the background (B2), fork loads keep markers (B3), and a
// compaction pass keeps the flush generations that landed during it (T2).
// ---------------------------------------------------------------------------

/// `load`'s vertices seen by a label-less scan (the shared `vertices` table).
async fn unlabelled(db: &Uni, load: Load) -> Result<i64> {
    count(
        db,
        &format!(
            "MATCH (n) WHERE n.k >= {} AND n.k < {} RETURN count(n) AS c",
            load.k_from,
            load.k_from + BULK_VERTICES
        ),
    )
    .await
}

/// Parks every hit of `seam` until the returned sender releases it, and
/// reports each hit on the returned receiver. Remove the seam when done.
fn park_at(seam: &str) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
    let (reached_tx, reached_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let go_rx = std::sync::Mutex::new(go_rx);
    fail::cfg_callback(seam, move || {
        let _ = reached_tx.send(());
        let _ = go_rx.lock().unwrap().recv_timeout(Duration::from_secs(60));
    })
    .unwrap();
    (reached_rx, go_tx)
}

/// Commits `load` with the publish parked before each of its five tables
/// (`vertices_P`, `vertices`, `main_edges`, both deltas); at every park a
/// reader on another task records the load's view and its label-less count.
async fn views_during_publish(
    db: &Arc<Uni>,
    load: Load,
    seed: &[Vid],
) -> Result<Vec<(LoadView, i64)>> {
    const TABLES: usize = 5;
    let (reached, go) = park_at(COMMIT_PUBLISH_SEAM);
    let observer = {
        let db = Arc::clone(db);
        tokio::spawn(async move {
            let mut reached = Some(reached);
            let mut seen = Vec::new();
            for _ in 0..TABLES {
                let rx = reached.take().unwrap();
                let (rx, hit) = tokio::task::spawn_blocking(move || {
                    let hit = rx.recv_timeout(Duration::from_secs(60)).is_ok();
                    (rx, hit)
                })
                .await?;
                reached = Some(rx);
                anyhow::ensure!(hit, "the publish never reached the next table");
                seen.push((load.view(&db).await?, unlabelled(&db, load).await?));
                go.send(())?;
            }
            Ok::<_, anyhow::Error>(seen)
        })
    };
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(10).build()?;
    bulk.insert_vertices("P", load.vertices()).await?;
    bulk.insert_edges("E", load.edges(seed)).await?;
    let outcome = bulk.commit().await;
    fail::remove(COMMIT_PUBLISH_SEAM);
    let seen = observer.await??;
    outcome?;
    tx.commit().await?;
    Ok(seen)
}

/// B1: between two table publishes of a successful commit, a reader saw part
/// of the load. Measured on df777cb3e with a warm adjacency CSR: label scan 20
/// with the label-less scan 0, then every vertex with none of its edges; with
/// a cold CSR, fwd 30 with bwd 0. Every park must now show none of it, and the
/// commit all of it -- with background compaction ticking throughout, which
/// must not fold the hidden deltas into L2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_commit_is_invisible_until_every_table_is_published_resilience() -> Result<()> {
    for warm in [true, false] {
        let dir = tempfile::TempDir::new()?;
        let path = dir.path().to_str().unwrap().to_string();
        {
            let db = Uni::open(&path).config(config(false)).build().await?;
            seed(&db).await?;
        }
        let db = Arc::new(Uni::open(&path).config(config(warm)).build().await?);
        let seed = seed_vids(&db).await?;
        if warm {
            // Builds both directions' CSRs before the load.
            assert_none(&db, LOAD_A, "premise: before the load").await?;
        }
        let seen = views_during_publish(&db, LOAD_A, &seed).await?;
        for (i, (view, label_less)) in seen.iter().enumerate() {
            assert_eq!(
                (view, *label_less),
                (
                    &LoadView {
                        vertices: 0,
                        fwd: 0,
                        bwd: 0,
                        edges: 0
                    },
                    0
                ),
                "warm CSR {warm}: parked before table #{i}, nothing of the load may be \
                 visible: {seen:?}"
            );
        }
        LOAD_A
            .assert_all(&db, &format!("warm CSR {warm}: after commit"))
            .await?;
        assert_eq!(unlabelled(&db, LOAD_A).await?, BULK_VERTICES);
        assert_seed_intact(&db, "after commit").await?;
        drop(db);
        let db = Uni::open(&path).config(config(false)).build().await?;
        LOAD_A
            .assert_all(&db, &format!("warm CSR {warm}: after reopen"))
            .await?;
    }
    Ok(())
}

/// The marker files of the database's in-flight bulk loads.
fn markers(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(root.join("storage/catalog/bulk_flush_intents"))
        .or_else(|_| std::fs::read_dir(root.join("catalog/bulk_flush_intents")))
        .map(|d| d.flatten().map(|e| e.path()).collect())
        .unwrap_or_default()
}

/// Waits until `load` is entirely visible, or fails after 20 s.
async fn wait_for_all(db: &Uni, load: Load) -> Result<()> {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if load.view(db).await?
            == (LoadView {
                vertices: BULK_VERTICES,
                fwd: BULK_EDGES as i64,
                bwd: BULK_EDGES as i64,
                edges: BULK_EDGES as i64,
            })
        {
            return Ok(());
        }
        anyhow::ensure!(
            std::time::Instant::now() < deadline,
            "the load was never completed in the background: {:?}",
            load.view(db).await?
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// B2: after `BulkCommitIncomplete` nothing completed the load until reopen,
/// and readers saw the part already published (measured on df777cb3e: 20
/// per-label vertices, nothing else, until reopen). The load must now stay
/// hidden while a background task retries it, then appear entirely, with its
/// receipt written and its marker gone, exactly once after a reopen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_incomplete_load_is_redriven_in_the_background_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config(false)).build().await?;
    let seed = seed(&db).await?;

    fail::cfg(COMMIT_PUBLISH_SEAM, "1*off->1*return->off").unwrap();
    fail::cfg("bulk::commit::complete-publish", "return").unwrap();
    fail::cfg("bulk::redrive::attempt", "return").unwrap();
    let (load_id, outcome) = commit_load(&db, LOAD_A, &seed).await?;
    fail::remove(COMMIT_PUBLISH_SEAM);
    fail::remove("bulk::commit::complete-publish");
    assert!(
        matches!(
            outcome.as_ref().err().and_then(uni_error),
            Some(uni_db::UniError::BulkCommitIncomplete { .. })
        ),
        "premise: {outcome:?}"
    );
    // The re-drive keeps failing: the published part stays hidden.
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_none(&db, LOAD_A, "while the re-drive keeps failing").await?;
    assert_eq!(unlabelled(&db, LOAD_A).await?, 0);
    assert!(db.bulk_load_applied(&load_id).await?);

    fail::remove("bulk::redrive::attempt");
    wait_for_all(&db, LOAD_A).await?;
    assert_eq!(unlabelled(&db, LOAD_A).await?, BULK_VERTICES);
    assert!(db.bulk_load_applied(&load_id).await?, "from the receipt");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !markers(dir.path()).is_empty() && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        markers(dir.path()).is_empty(),
        "the re-drive must delete the marker once the receipt is written"
    );
    drop(db);

    let db = Uni::open(&path).config(config(false)).build().await?;
    LOAD_A.assert_all(&db, "after reopen").await?;
    assert_seed_intact(&db, "after reopen").await?;
    assert!(db.bulk_load_applied(&load_id).await?, "after reopen");
    Ok(())
}

/// B2: the re-drive stops with the database -- no task survives shutdown and
/// nothing is published after it -- and the next open completes the load.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bulk_redrive_stops_with_the_database_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config(false)).build().await?;
    let seed = seed(&db).await?;

    let baseline = db.background_task_count();
    fail::cfg(COMMIT_PUBLISH_SEAM, "1*off->1*return->off").unwrap();
    fail::cfg("bulk::commit::complete-publish", "return").unwrap();
    fail::cfg("bulk::redrive::attempt", "return").unwrap();
    let (load_id, outcome) = commit_load(&db, LOAD_A, &seed).await?;
    fail::remove(COMMIT_PUBLISH_SEAM);
    fail::remove("bulk::commit::complete-publish");
    assert!(outcome.is_err(), "premise");
    assert_eq!(
        db.background_task_count(),
        baseline + 1,
        "premise: the incomplete load started one re-drive task"
    );

    let started = std::time::Instant::now();
    db.shutdown_in_place().await?;
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "shutdown must not wait out the re-drive's backoff: {:?}",
        started.elapsed()
    );
    assert_eq!(
        db.background_task_count(),
        0,
        "no background task may outlive the database's shutdown"
    );
    // With the fault gone, a task that survived would complete the load and
    // delete the marker; none may.
    fail::remove("bulk::redrive::attempt");
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(
        markers(dir.path()).len(),
        1,
        "nothing may run after shutdown"
    );
    drop(db);

    let db = Uni::open(&path).config(config(false)).build().await?;
    LOAD_A.assert_all(&db, "after reopen").await?;
    assert!(db.bulk_load_applied(&load_id).await?);
    Ok(())
}

/// T2: a pass reset `l1_runs` to zero, erasing the flushes that landed while
/// it ran. It must subtract only the runs it saw at its start.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_keeps_flush_generations_that_land_during_the_pass_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config(false)).build().await?;
    seed(&db).await?;
    commit_concurrent_writes(&db).await?;
    let at_start = db.metrics().l1_run_count;
    assert!(at_start >= 2, "premise: {at_start} runs");

    const SEAM: &str = "compaction::safe-point::before-label";
    let (reached, go) = park_at(SEAM);
    let storage = db.storage();
    let pass = tokio::spawn(async move { storage.compact().await });
    let reached =
        tokio::task::spawn_blocking(move || reached.recv_timeout(Duration::from_secs(60)).is_ok())
            .await?;
    assert!(reached, "premise: the pass must park");
    // A flush generation lands while the pass runs.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:P {k: 5000})").await?;
    tx.commit().await?;
    db.flush().await?;
    assert_eq!(db.metrics().l1_run_count, at_start + 1);
    fail::remove(SEAM);
    go.send(())?;
    pass.await??;
    assert_eq!(
        db.metrics().l1_run_count,
        1,
        "the pass folded the {at_start} runs it saw; the run that landed during it remains"
    );
    Ok(())
}

/// B3: a fork load that fails part way through publishing returned
/// `Internal` and left the fork's per-label table published, surviving
/// reopen; `bulk_load_applied` answered `false` even for a successful fork
/// load. A fork load now returns the primary's typed outcomes, stays hidden
/// while incomplete, is completed when the fork is next opened, and is
/// answered by the fork session's `bulk_load_applied`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_bulk_load_incomplete_is_completed_at_fork_reopen_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let load_id = {
        let db = Uni::open(&path).config(config(false)).build().await?;
        let seed = seed(&db).await?;
        let s = db.session();
        let fork = s.fork("bulk_b3").await?;

        fail::cfg(COMMIT_PUBLISH_SEAM, "1*off->1*return->off").unwrap();
        fail::cfg("bulk::commit::complete-publish", "return").unwrap();
        fail::cfg("bulk::redrive::attempt", "return").unwrap();
        let tx = fork.tx().await?;
        let mut bulk = tx.bulk_writer().batch_size(10).build()?;
        let load_id = bulk.load_id().to_string();
        bulk.insert_vertices("P", LOAD_A.vertices()).await?;
        bulk.insert_edges("E", LOAD_A.edges(&seed)).await?;
        let outcome = bulk.commit().await;
        fail::remove(COMMIT_PUBLISH_SEAM);
        fail::remove("bulk::commit::complete-publish");
        drop(tx);
        match outcome.as_ref().err().and_then(uni_error) {
            Some(uni_db::UniError::BulkCommitIncomplete { load_id: id, .. }) => {
                assert_eq!(id, &load_id);
            }
            _ => panic!("expected BulkCommitIncomplete on a fork, got {outcome:?}"),
        }
        let hidden = count_on(&fork, LOAD_A).await?;
        assert_eq!(hidden, (0, 0), "the fork's partial load stays hidden");
        assert!(fork.bulk_load_applied(&load_id).await?, "fork: applied");
        assert!(
            !db.bulk_load_applied(&load_id).await?,
            "primary knows nothing of the fork's load"
        );
        drop(fork);
        db.shutdown().await?;
        fail::remove("bulk::redrive::attempt");
        load_id
    };

    let db = Uni::open(&path).config(config(false)).build().await?;
    let s = db.session();
    let fork = s.fork("bulk_b3").await?;
    assert_eq!(
        count_on(&fork, LOAD_A).await?,
        (BULK_VERTICES, BULK_VERTICES),
        "the fork's reopen completes the load"
    );
    assert_eq!(
        fork.query(&format!(
            "MATCH (b:P)<-[r:E]-(a:P) WHERE r.w >= {} AND r.w < {} RETURN count(r) AS c",
            LOAD_A.w_from,
            LOAD_A.w_from + BULK_EDGES as i64
        ))
        .await?
        .rows()[0]
            .get::<i64>("c")?,
        BULK_EDGES as i64
    );
    assert!(
        fork.bulk_load_applied(&load_id).await?,
        "fork: after reopen"
    );
    assert_eq!(
        count(&db, "MATCH (n:P) WHERE n.k >= 100 RETURN count(n) AS c").await?,
        0,
        "primary"
    );
    Ok(())
}

/// The load's vertices on `session` by label and label-less.
async fn count_on(session: &uni_db::Session, load: Load) -> Result<(i64, i64)> {
    let range = format!(
        "n.k >= {} AND n.k < {}",
        load.k_from,
        load.k_from + BULK_VERTICES
    );
    let by_label = session
        .query(&format!("MATCH (n:P) WHERE {range} RETURN count(n) AS c"))
        .await?
        .rows()[0]
        .get::<i64>("c")?;
    let label_less = session
        .query(&format!("MATCH (n) WHERE {range} RETURN count(n) AS c"))
        .await?
        .rows()[0]
        .get::<i64>("c")?;
    Ok((by_label, label_less))
}

// ---------------------------------------------------------------------------
// Wave Q: a fork created while a bulk load is being published.
// ---------------------------------------------------------------------------

/// `load` as a reader of `session` sees it.
async fn session_view(session: &uni_db::Session, load: Load) -> Result<LoadView> {
    let c = |q: String| async move {
        Ok::<_, anyhow::Error>(session.query(&q).await?.rows()[0].get::<i64>("c")?)
    };
    let w = format!(
        "r.w >= {} AND r.w < {}",
        load.w_from,
        load.w_from + BULK_EDGES as i64
    );
    Ok(LoadView {
        vertices: c(format!(
            "MATCH (n:P) WHERE n.k >= {} AND n.k < {} RETURN count(n) AS c",
            load.k_from,
            load.k_from + BULK_VERTICES
        ))
        .await?,
        fwd: c(format!(
            "MATCH (a:P)-[r:E]->(b:P) WHERE {w} RETURN count(r) AS c"
        ))
        .await?,
        bwd: c(format!(
            "MATCH (b:P)<-[r:E]-(a:P) WHERE {w} RETURN count(r) AS c"
        ))
        .await?,
        edges: c(format!("MATCH ()-[r:E]->() WHERE {w} RETURN count(r) AS c")).await?,
    })
}

const ALL_OF_LOAD: LoadView = LoadView {
    vertices: BULK_VERTICES,
    fwd: BULK_EDGES as i64,
    bwd: BULK_EDGES as i64,
    edges: BULK_EDGES as i64,
};

/// Receives one park of a `park_at` seam, or fails after 60 s.
async fn reached(rx: mpsc::Receiver<()>) -> Result<mpsc::Receiver<()>> {
    let (rx, hit) = tokio::task::spawn_blocking(move || {
        let hit = rx.recv_timeout(Duration::from_secs(60)).is_ok();
        (rx, hit)
    })
    .await?;
    anyhow::ensure!(hit, "the seam was never reached");
    Ok(rx)
}

/// Starts committing `load` on another task, parked before its third table
/// (`vertices_P` and `vertices` are published, `main_edges` and the deltas
/// are not). Returns the commit task and the seam's controls.
async fn commit_parked_mid_publish(
    db: &Arc<Uni>,
    load: Load,
    seed: &[Vid],
) -> Result<(
    tokio::task::JoinHandle<Result<()>>,
    mpsc::Receiver<()>,
    mpsc::Sender<()>,
)> {
    let (rx, go) = park_at(COMMIT_PUBLISH_SEAM);
    let commit = {
        let db = Arc::clone(db);
        let seed = seed.to_vec();
        tokio::spawn(async move {
            let tx = db.session().tx().await?;
            let mut bulk = tx.bulk_writer().batch_size(10).build()?;
            bulk.insert_vertices("P", load.vertices()).await?;
            bulk.insert_edges("E", load.edges(&seed)).await?;
            bulk.commit().await?;
            tx.commit().await?;
            Ok(())
        })
    };
    let mut rx = reached(rx).await?;
    for _ in 0..2 {
        go.send(())?;
        rx = reached(rx).await?;
    }
    Ok((commit, rx, go))
}

/// Releases a parked publish for good and waits for its commit.
async fn finish_publish(
    commit: tokio::task::JoinHandle<Result<()>>,
    go: mpsc::Sender<()>,
) -> Result<()> {
    fail::remove(COMMIT_PUBLISH_SEAM);
    // One release per table still parked or about to park.
    for _ in 0..5 {
        let _ = go.send(());
    }
    commit.await?
}

/// A fork created while a bulk load was being published branched off the
/// tables part way: measured on 7b5856437, the fork saw the load's 20 vertices
/// by label with none of its 30 edges, for good, since its branches were cut
/// mid-publish and it reads with a gate of its own. Fork creation must now
/// wait for the publish and branch off the whole load.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_created_mid_publish_waits_for_the_whole_load_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Arc::new(Uni::open(&path).config(config(false)).build().await?);
    let seed = seed(&db).await?;

    let (commit, _rx, go) = commit_parked_mid_publish(&db, LOAD_A, &seed).await?;
    let fork = {
        let db = Arc::clone(&db);
        tokio::spawn(async move { db.session().fork("mid_publish").await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    let waited = !fork.is_finished();
    finish_publish(commit, go).await?;
    let fork = fork.await??;

    assert_eq!(
        session_view(&fork, LOAD_A).await?,
        ALL_OF_LOAD,
        "the fork must branch off the whole load, not the tables published so far"
    );
    assert!(
        waited,
        "fork creation must wait while the publish is in flight"
    );
    LOAD_A.assert_all(&db, "primary").await?;
    Ok(())
}

/// The gate was open when fork creation looked, and a publish began before
/// the fork point was captured: the capture saw part of the load. The epoch
/// read with the open gate must expose that, and the capture be retried once
/// the publish is done.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_capture_racing_a_publish_start_is_retried_resilience() -> Result<()> {
    const CAPTURE_SEAM: &str = "fork::create-before-capture";
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Arc::new(Uni::open(&path).config(config(false)).build().await?);
    let seed = seed(&db).await?;

    // Park the fork after it found the gate open, before its capture.
    let (fork_rx, fork_go) = park_at(CAPTURE_SEAM);
    let fork = {
        let db = Arc::clone(&db);
        tokio::spawn(async move { db.session().fork("racing").await })
    };
    let _fork_rx = reached(fork_rx).await?;
    // A publish begins and parks part way.
    let (commit, _rx, go) = commit_parked_mid_publish(&db, LOAD_A, &seed).await?;
    // The fork captures now, mid-publish.
    fail::remove(CAPTURE_SEAM);
    fork_go.send(())?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let waited = !fork.is_finished();
    finish_publish(commit, go).await?;
    let fork = fork.await??;

    assert_eq!(
        session_view(&fork, LOAD_A).await?,
        ALL_OF_LOAD,
        "a capture that overlapped a publish must be retried after it"
    );
    assert!(waited, "the retried capture must wait for the publish");
    Ok(())
}

/// A load left incomplete keeps its version hidden while its re-drive keeps
/// failing. Fork creation must not wait for it indefinitely nor branch off
/// the partial publish: after its bounded wait it refuses with a retryable
/// `ForkLifecycle` error and creates nothing, and once the load is completed
/// the same fork creation succeeds with all of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fork_refused_while_a_publish_stays_incomplete_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(config(false)).build().await?;
    let seed = seed(&db).await?;

    fail::cfg(COMMIT_PUBLISH_SEAM, "1*off->1*off->1*return->off").unwrap();
    fail::cfg("bulk::commit::complete-publish", "return").unwrap();
    fail::cfg("bulk::redrive::attempt", "return").unwrap();
    let (_load_id, outcome) = commit_load(&db, LOAD_A, &seed).await?;
    fail::remove(COMMIT_PUBLISH_SEAM);
    fail::remove("bulk::commit::complete-publish");
    assert!(
        matches!(
            outcome.as_ref().err().and_then(uni_error),
            Some(uni_db::UniError::BulkCommitIncomplete { .. })
        ),
        "premise: {outcome:?}"
    );

    fail::cfg("fork::create-bulk-publish-wait", "return(200)").unwrap();
    let refused = db.session().fork("during_incomplete").await;
    match refused.as_ref().err() {
        Some(uni_db::UniError::ForkLifecycle { stage, .. }) => {
            assert_eq!(*stage, "bulk_publish");
        }
        other => panic!("expected a bulk_publish ForkLifecycle refusal, got {other:?}"),
    }
    assert!(
        db.list_forks().await.is_empty(),
        "a refused fork creation leaves nothing registered"
    );

    fail::remove("bulk::redrive::attempt");
    fail::remove("fork::create-bulk-publish-wait");
    wait_for_all(&db, LOAD_A).await?;
    let fork = db.session().fork("during_incomplete").await?;
    assert_eq!(session_view(&fork, LOAD_A).await?, ALL_OF_LOAD);
    Ok(())
}

// ---------------------------------------------------------------------------
// Wave Q: declared endpoints re-checked when a committed load is published
// later -- by the background re-drive, or by recovery at the next open.
// ---------------------------------------------------------------------------

/// `P`, `Q`, `R: P -> Q` and one flushed `Q {k: 1}`; returns its VID.
async fn seed_declared(db: &Uni) -> Result<Vid> {
    let tx = db.session().tx().await?;
    tx.execute("CREATE LABEL P (k INT)").await?;
    tx.execute("CREATE LABEL Q (k INT)").await?;
    tx.execute("CREATE EDGE TYPE R FROM P TO Q").await?;
    tx.commit().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Q {k: 1})").await?;
    tx.commit().await?;
    db.flush().await?;
    let id = db
        .session()
        .query("MATCH (n:Q {k: 1}) RETURN id(n) AS id")
        .await?
        .rows()[0]
        .get::<i64>("id")?;
    Ok(Vid::from(id as u64))
}

fn r_conformance(db: &Uni) -> uni_common::core::schema::EndpointConformance {
    db.storage().schema_manager().schema().edge_types["R"]
        .endpoint_conformance
        .clone()
}

/// Commits a load of one `P` and an `R` edge to `q` whose publish cannot be
/// finished in-process (the re-drive keeps failing), then removes `Q` from
/// `q` while the edge is still hidden.
async fn incomplete_load_then_relabel(db: &Uni, q: Vid) -> Result<String> {
    fail::cfg(COMMIT_PUBLISH_SEAM, "1*off->1*return->off").unwrap();
    fail::cfg("bulk::commit::complete-publish", "return").unwrap();
    fail::cfg("bulk::redrive::attempt", "return").unwrap();
    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().build()?;
    let load_id = bulk.load_id().to_string();
    let p = bulk
        .insert_vertices("P", vec![HashMap::from([("k".to_string(), Value::Int(0))])])
        .await?;
    bulk.insert_edges("R", vec![uni_bulk::EdgeData::new(p[0], q, HashMap::new())])
        .await?;
    let outcome = bulk.commit().await;
    drop(tx);
    fail::remove(COMMIT_PUBLISH_SEAM);
    fail::remove("bulk::commit::complete-publish");
    assert!(
        matches!(
            outcome.as_ref().err().and_then(uni_error),
            Some(uni_db::UniError::BulkCommitIncomplete { .. })
        ),
        "premise: {outcome:?}"
    );
    let tx = db.session().tx().await?;
    tx.execute("MATCH (n:Q {k: 1}) REMOVE n:Q").await?;
    tx.commit().await?;
    Ok(load_id)
}

fn no_flush_config() -> UniConfig {
    UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..config(false)
    }
}

/// The re-drive published a committed load without re-checking its edges'
/// declared endpoints: `R` stayed enforced over an edge to a vertex that lost
/// `Q` while the edge was hidden, and `()-[r:R]->()` found nothing. The
/// re-drive must record `R` unverified, so the edge is found.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redrive_publish_rechecks_declared_endpoints_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Uni::open(&path).config(no_flush_config()).build().await?;
    let q = seed_declared(&db).await?;
    let load_id = incomplete_load_then_relabel(&db, q).await?;

    fail::remove("bulk::redrive::attempt");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while !markers(dir.path()).is_empty() && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        markers(dir.path()).is_empty(),
        "the re-drive finished the load"
    );
    assert!(db.bulk_load_applied(&load_id).await?);
    assert_eq!(
        r_conformance(&db),
        uni_common::core::schema::EndpointConformance::Unverified
    );
    assert_eq!(
        count(&db, "MATCH ()-[r:R]->() RETURN count(r) AS c").await?,
        1
    );
    Ok(())
}

/// Recovery at open published a committed load before the WAL was replayed
/// and never re-checked its edges. Here the label removal is only in the WAL
/// when the process dies, so a check against storage at recovery time would
/// still see `Q`: it must run once the WAL is replayed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_rechecks_declared_endpoints_after_wal_replay_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let load_id = {
        let db = Uni::open(&path).config(no_flush_config()).build().await?;
        let q = seed_declared(&db).await?;
        let load_id = incomplete_load_then_relabel(&db, q).await?;
        // Dies with the removal in the WAL only (dropping does not flush).
        drop(db);
        fail::remove("bulk::redrive::attempt");
        load_id
    };
    let db = Uni::open(&path).config(no_flush_config()).build().await?;
    assert!(markers(dir.path()).is_empty(), "the open finished the load");
    assert!(db.bulk_load_applied(&load_id).await?);
    assert_eq!(
        count(&db, "MATCH (n:Q) RETURN count(n) AS c").await?,
        0,
        "premise: the replayed WAL removed the label"
    );
    assert_eq!(
        r_conformance(&db),
        uni_common::core::schema::EndpointConformance::Unverified
    );
    assert_eq!(
        count(&db, "MATCH ()-[r:R]->() RETURN count(r) AS c").await?,
        1
    );
    drop(db);
    let db = Uni::open(&path).config(no_flush_config()).build().await?;
    assert_eq!(
        r_conformance(&db),
        uni_common::core::schema::EndpointConformance::Unverified,
        "the downgrade is saved"
    );
    Ok(())
}

/// What `session` reads of the committed seed and of the load: `(P vertices,
/// E edges, bulk vertices, bulk edges)`.
async fn pinned_view(session: &uni_db::Session) -> Result<(i64, i64, i64, i64)> {
    let count = |q: String| async move {
        let r = session.query(&q).await?;
        anyhow::Ok(r.rows()[0].get::<i64>("c")?)
    };
    Ok((
        count("MATCH (n:P) RETURN count(n) AS c".to_string()).await?,
        count("MATCH ()-[r:E]->() RETURN count(r) AS c".to_string()).await?,
        count(format!(
            "MATCH (n) WHERE n.k >= 100 AND n.k < {} RETURN count(n) AS c",
            100 + BULK_VERTICES
        ))
        .await?,
        count(format!(
            "MATCH (a:P)-[r:E]->(b:P) WHERE r.w >= 0 AND r.w < {BULK_EDGES} \
             RETURN count(r) AS c"
        ))
        .await?,
    ))
}

/// Pins `snapshot` and the flush manifest cut at `flushed_at`; each must read
/// `committed` ([`pinned_view`]).
async fn check_mid_publish_pins(
    db: &Uni,
    snapshot: &str,
    flushed_at: chrono::DateTime<chrono::Utc>,
    committed: (i64, i64, i64, i64),
    phase: &str,
) -> Result<()> {
    let mut pinned = db.session();
    pinned.pin_to_version(snapshot).await?;
    assert_eq!(
        pinned_view(&pinned).await?,
        committed,
        "{phase}: the snapshot taken mid-publish"
    );
    let mut pinned = db.session();
    pinned.pin_to_timestamp(flushed_at).await?;
    assert_eq!(
        pinned_view(&pinned).await?,
        committed,
        "{phase}: the flush manifest cut mid-publish"
    );
    Ok(())
}

/// A flush and a snapshot taken while a load's publish is parked part way
/// through (its vertex tables and `main_edges` published, its delta tables
/// not) exclude the load for good.
///
/// Both capture a version ceiling above the load's version, and the snapshot
/// records each table's version with the load's rows half in. Pinned after
/// the load completes, each must read the committed state only, before and
/// after a reopen; restored, the snapshot must not bring back half the load.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ceilings_captured_mid_publish_exclude_the_load_resilience() -> Result<()> {
    let dir = tempfile::TempDir::new()?;
    let path = dir.path().to_str().unwrap().to_string();
    let db = Arc::new(Uni::open(&path).config(config(false)).build().await?);
    let seed = seed(&db).await?;

    let (reached_tx, reached_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(release_rx);
    let hits = Arc::new(AtomicUsize::new(0));
    {
        let hits = Arc::clone(&hits);
        fail::cfg_callback(PUBLISH_SEAM, move || {
            if hits.fetch_add(1, Ordering::SeqCst) == 0 {
                reached_tx.send(()).ok();
                release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(30))
                    .ok();
            }
        })
        .unwrap();
    }
    let capture = tokio::spawn({
        let db = Arc::clone(&db);
        async move {
            let reached = tokio::task::spawn_blocking(move || {
                reached_rx.recv_timeout(Duration::from_secs(30))
            })
            .await?;
            let result = match reached {
                Ok(()) => {
                    async {
                        db.flush().await?;
                        let flushed_at = chrono::Utc::now();
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        let snapshot = db.create_snapshot("mid_publish").await?;
                        anyhow::Ok(Some((flushed_at, snapshot)))
                    }
                    .await
                }
                Err(_) => Ok(None),
            };
            release_tx.send(()).ok();
            result
        }
    });

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(10).build()?;
    bulk.insert_vertices("P", bulk_vertices()).await?;
    bulk.insert_edges("E", bulk_edges(&seed)).await?;
    bulk.commit().await?;
    tx.commit().await?;
    fail::remove(PUBLISH_SEAM);
    let (flushed_at, snapshot) = capture
        .await??
        .expect("premise: the publish parks and the flush and snapshot run inside it");

    let committed = (SEED as i64, 1, 0, 0);
    let loaded = (
        SEED as i64 + BULK_VERTICES,
        1 + BULK_EDGES as i64,
        BULK_VERTICES,
        BULK_EDGES as i64,
    );
    assert_eq!(pinned_view(&db.session()).await?, loaded, "live");
    check_mid_publish_pins(&db, &snapshot, flushed_at, committed, "same process").await?;
    drop(db);
    let db = Uni::open(&path).config(config(false)).build().await?;
    assert_eq!(
        pinned_view(&db.session()).await?,
        loaded,
        "live after reopen"
    );
    check_mid_publish_pins(&db, &snapshot, flushed_at, committed, "after reopen").await?;

    // Restored, the store holds what the snapshot reads: none of the load.
    db.restore_snapshot(&snapshot).await?;
    assert_eq!(pinned_view(&db.session()).await?, committed, "restored");
    drop(db);
    let db = Uni::open(&path).config(config(false)).build().await?;
    assert_eq!(
        pinned_view(&db.session()).await?,
        committed,
        "restored, after reopen"
    );
    Ok(())
}
