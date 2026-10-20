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

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(10).build()?;
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
    tx.commit().await?;
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

/// A crash part way through publishing, after a compaction pass folded the
/// already-published forward deltas into L2 (and cleared them from the delta
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

    // Publish hit 1 is the fwd delta, hit 2 the bwd delta. Hit 2 parks until a
    // compaction pass has run over the half-published load, then crashes.
    let (reached_tx, reached_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let release_rx = std::sync::Mutex::new(release_rx);
    let hits = Arc::new(AtomicUsize::new(0));
    {
        let hits = Arc::clone(&hits);
        fail::cfg_callback(SEAM, move || {
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

    // The bulk writer's future is not `Send`, so it runs on this task and the
    // compaction on a worker; the seam blocks the loading thread only.
    let compactor = tokio::spawn({
        let db = Arc::clone(&db);
        let adj_fwd = adj_fwd.clone();
        async move {
            let reached = tokio::task::spawn_blocking(move || {
                reached_rx.recv_timeout(Duration::from_secs(30))
            })
            .await?;
            let result = match reached {
                Ok(()) => {
                    let pass = Compactor::new(db.storage()).compact_all().await;
                    let rows = row_count(&db, &adj_fwd).await;
                    release_tx.send(()).ok();
                    pass?;
                    Some(rows?)
                }
                Err(_) => {
                    release_tx.send(()).ok();
                    None
                }
            };
            anyhow::Ok(result)
        }
    });

    let tx = db.session().tx().await?;
    let mut bulk = tx.bulk_writer().batch_size(10).build()?;
    bulk.insert_edges("E", bulk_edges(&seed)).await?;
    let crashed = std::panic::AssertUnwindSafe(bulk.commit())
        .catch_unwind()
        .await
        .is_err();
    fail::remove(SEAM);
    let adj_rows_mid = compactor
        .await??
        .ok_or_else(|| anyhow::anyhow!("publish never reached the bwd delta"))?;
    assert!(
        crashed,
        "premise: the injected crash must interrupt the commit"
    );
    assert!(
        adj_rows_mid > adj_rows_before,
        "premise: the pass must have folded the published fwd deltas into L2 \
         ({adj_rows_before} -> {adj_rows_mid} rows), or the test proves nothing"
    );
    drop(tx);
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
