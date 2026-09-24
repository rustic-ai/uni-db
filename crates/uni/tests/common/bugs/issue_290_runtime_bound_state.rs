// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #290 — state bound to a tokio runtime that no longer exists.
//!
//! The reported symptom: two `similar_to` full-text calls with different query
//! text, and the second never returns. The cause is wider than `similar_to`.
//! Lance's `ScanScheduler` spawns its I/O loop onto whichever runtime is current
//! when it is built, and the inverted index caches its readers — and so that
//! loop — in the database's shared `lance::Session`. Posting lists load lazily,
//! so a later query for an uncached term submits I/O to the loop. When the
//! runtime that first loaded the index has been dropped, that I/O is never
//! serviced and the query waits forever with every thread idle.
//!
//! Anything that runs storage work on a runtime that dies before the database
//! does can be that first loader, and the damage is shared: once the index is
//! poisoned, *every* later full-text query hangs, whichever path it takes.
//! These tests cover each entry point that was shown to do it, plus two
//! siblings of the same class in the write path.
//!
//! # Why the tests run on their own threads
//!
//! A hang is the failure mode, and a hang inside a nested runtime blocks the
//! tokio worker that would service `tokio::time::timeout`. Each scenario runs
//! on a plain OS thread and the test waits with `recv_timeout`, so a regression
//! fails the test instead of wedging the run. Under nextest each test is its
//! own process, so the stuck thread dies with it.

use std::path::Path;
use std::sync::mpsc;
use std::time::Duration;

use uni_db::{DataType, IndexType, Uni, UniConfig};

/// Generous against a slow CI machine; a healthy scenario takes well under 1 s.
const SCENARIO_LIMIT: Duration = Duration::from_secs(90);

/// Bounds a query that would otherwise sit on a dead I/O loop, so a caller-
/// runtime hang surfaces as an error long before `SCENARIO_LIMIT`.
const QUERY_TIMEOUT: Duration = Duration::from_secs(20);

const TEXT_A: &str = "lattice tower paris";
const TEXT_B: &str = "dog tennis ball field";

const SIMILAR_TO: &str = "MATCH (d:Doc) RETURN id(d) AS nid, similar_to(d.text, $q) AS score \
                          ORDER BY score DESC LIMIT 10";
const FTS: &str = "CALL uni.fts.query('Doc', 'text', $q, 10) YIELD node, score \
                   RETURN id(node) AS nid, score";

/// Runs `scenario` on its own OS thread and fails the test if it does not
/// finish within [`SCENARIO_LIMIT`].
fn within<R: Send + 'static>(what: &str, scenario: impl FnOnce() -> R + Send + 'static) -> R {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(scenario());
    });
    match rx.recv_timeout(SCENARIO_LIMIT) {
        Ok(r) => r,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("{what}: no result after {SCENARIO_LIMIT:?} — hung (#290)")
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => panic!("{what}: scenario thread panicked"),
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

/// Counts files whose name contains `needle` anywhere under `root`.
fn count_files(root: &Path, needle: &str) -> usize {
    let Ok(rd) = std::fs::read_dir(root) else {
        return 0;
    };
    rd.flatten()
        .map(|e| {
            let p = e.path();
            if p.is_dir() {
                count_files(&p, needle)
            } else {
                usize::from(p.to_string_lossy().contains(needle))
            }
        })
        .sum()
}

/// Opens a database with `Doc` rows and a built full-text index on `text`.
///
/// The rows are flushed before the index is declared, so declaring it builds
/// the index over them. The build does not leave the index loaded in the
/// session — the first *query* does — so the runtime that runs this fixture
/// is not what binds the index.
async fn fts_db(path: &Path) -> Uni {
    let db = Uni::open(path.to_string_lossy()).build().await.unwrap();
    db.schema()
        .label("Doc")
        .property("doc_id", DataType::String)
        .property("text", DataType::String)
        .done()
        .apply()
        .await
        .unwrap();
    let topics = [
        "the lattice tower in paris was built of wrought iron",
        "a dog ran across the field chasing a tennis ball",
        "she published the first algorithm for a mechanical engine",
    ];
    let tx = db.session().tx().await.unwrap();
    for i in 0..300 {
        tx.query_with("CREATE (d:Doc {doc_id: $id, text: $text})")
            .param("id", format!("d{i}"))
            .param("text", format!("{} (row {i})", topics[i % topics.len()]))
            .fetch_all()
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    db.flush().await.unwrap();
    db.schema()
        .label("Doc")
        .property("doc_id", DataType::String)
        .property("text", DataType::String)
        .index("text", IndexType::FullText)
        .done()
        .apply()
        .await
        .unwrap();
    // Without an on-disk inverted index every query below is served some
    // other way and the tests would pass without exercising anything.
    assert!(
        count_files(path, "invert") > 0,
        "fixture must materialize a Lance inverted index"
    );
    db
}

async fn row_count(db: &Uni, cypher: &str, q: &str) -> Result<usize, String> {
    db.session()
        .query_with(cypher)
        .param("q", q)
        .timeout(QUERY_TIMEOUT)
        .fetch_all()
        .await
        .map(|r| r.rows().len())
        .map_err(|e| e.to_string())
}

/// The reported case: consecutive `similar_to` calls with different text.
#[test]
fn similar_to_with_changing_query_text_returns() {
    within("similar_to A then B", || {
        let dir = tempfile::tempdir().unwrap();
        let rt = runtime();
        rt.block_on(async {
            let db = fts_db(dir.path()).await;
            for (i, q) in [TEXT_A, TEXT_B, TEXT_A, TEXT_B].into_iter().enumerate() {
                let n = row_count(&db, SIMILAR_TO, q).await;
                assert_eq!(n, Ok(10), "similar_to call {i} ({q:?})");
            }
        });
    });
}

/// A `similar_to` call must not poison the index for the ordinary FTS path.
#[test]
fn similar_to_first_does_not_poison_fts_procedure() {
    within("similar_to A then uni.fts.query B", || {
        let dir = tempfile::tempdir().unwrap();
        let rt = runtime();
        rt.block_on(async {
            let db = fts_db(dir.path()).await;
            assert_eq!(row_count(&db, SIMILAR_TO, TEXT_A).await, Ok(10));
            assert_eq!(row_count(&db, FTS, TEXT_B).await, Ok(10));
        });
    });
}

/// EXISTS and COUNT subqueries drive their sub-plans synchronously too; a
/// full-text search first run inside one must not poison the index.
#[test]
fn subquery_first_touch_does_not_poison_fts() {
    let subqueries = [
        (
            "EXISTS",
            "MATCH (d:Doc) WHERE d.doc_id = 'd0' AND EXISTS { \
             CALL uni.fts.query('Doc', 'text', $q, 10) YIELD node RETURN node } \
             RETURN id(d) AS nid",
        ),
        (
            "COUNT",
            "MATCH (d:Doc) WHERE d.doc_id = 'd0' RETURN COUNT { \
             CALL uni.fts.query('Doc', 'text', $q, 10) YIELD node RETURN node } AS c",
        ),
    ];
    for (name, first) in subqueries {
        within(name, move || {
            let dir = tempfile::tempdir().unwrap();
            let rt = runtime();
            rt.block_on(async {
                let db = fts_db(dir.path()).await;
                assert_eq!(row_count(&db, first, TEXT_A).await, Ok(1), "{name} A");
                // Same shape again with new text: the nested path itself.
                assert_eq!(row_count(&db, first, TEXT_B).await, Ok(1), "{name} B");
                // And the ordinary path, with text neither call has seen.
                let fresh = "mechanical engine algorithm";
                assert_eq!(row_count(&db, FTS, fresh).await, Ok(10), "{name} then FTS");
            });
        });
    }
}

/// No nested runtime at all: the caller's own runtime loads the index and is
/// then dropped while the database lives on.
#[test]
fn fts_survives_the_runtime_that_first_queried_it() {
    within("FTS on rt1, rt1 dropped, FTS on rt2", || {
        let dir = tempfile::tempdir().unwrap();
        // rt0 opens the database and stays alive, so its background tasks do.
        let rt0 = runtime();
        let db = rt0.block_on(fts_db(dir.path()));

        let rt1 = runtime();
        assert_eq!(rt1.block_on(row_count(&db, FTS, TEXT_A)), Ok(10));
        drop(rt1);

        let rt2 = runtime();
        assert_eq!(rt2.block_on(row_count(&db, FTS, TEXT_B)), Ok(10));
        drop(rt2);
        drop(rt0);
    });
}

/// `build_sync` opens on a runtime of its own. The background tasks spawned
/// during open must outlive the call, or auto-flush silently never runs.
#[test]
fn build_sync_keeps_background_tasks_alive() {
    fn flushed_after_idle(open: impl FnOnce(&Path, UniConfig) -> Uni) -> usize {
        let dir = tempfile::tempdir().unwrap();
        let config = UniConfig {
            auto_flush_interval: Some(Duration::from_millis(200)),
            ..Default::default()
        };
        let db = open(dir.path(), config);
        let rt = runtime();
        rt.block_on(async {
            db.schema()
                .label("Doc")
                .property("doc_id", DataType::String)
                .done()
                .apply()
                .await
                .unwrap();
            let tx = db.session().tx().await.unwrap();
            for i in 0..20 {
                tx.query_with("CREATE (d:Doc {doc_id: $id})")
                    .param("id", format!("d{i}"))
                    .fetch_all()
                    .await
                    .unwrap();
            }
            tx.commit().await.unwrap();
            // No explicit flush: only the auto-flush task can write L1.
            for _ in 0..50 {
                if count_files(dir.path(), "vertices_Doc.lance/data/") > 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
        count_files(dir.path(), "vertices_Doc.lance/data/")
    }

    within("build_sync auto-flush", || {
        // Control: the same workload opened on a live runtime. It is what shows
        // the observable works; without it a broken probe would read as 0 in
        // both arms and hide the bug.
        let live = runtime();
        let control = flushed_after_idle(|p, c| {
            live.block_on(Uni::open(p.to_string_lossy()).config(c).build())
                .unwrap()
        });
        assert!(
            control > 0,
            "control: auto-flush on a live runtime wrote no L1 data"
        );

        let synced = flushed_after_idle(|p, c| {
            Uni::open(p.to_string_lossy())
                .config(c)
                .build_sync()
                .unwrap()
        });
        assert!(synced > 0, "build_sync: auto-flush never wrote L1 data");
    });
}

/// A flush task spawned by a commit on a runtime that is dropped before the
/// task first runs must still release its sequence number. Otherwise the
/// finalizer, which completes flushes strictly in order, waits for it forever
/// and every later flush hangs.
#[test]
fn flush_spawned_on_a_dropped_runtime_does_not_wedge_later_flushes() {
    within("flush after a dropped-runtime commit", || {
        let dir = tempfile::tempdir().unwrap();
        let rt0 = runtime();
        let config = UniConfig {
            auto_flush_threshold: 10,
            auto_flush_interval: None,
            ..Default::default()
        };
        let db = rt0.block_on(async {
            let db = Uni::open(dir.path().to_string_lossy())
                .config(config)
                .build()
                .await
                .unwrap();
            db.schema()
                .label("Doc")
                .property("doc_id", DataType::String)
                .done()
                .apply()
                .await
                .unwrap();
            db
        });

        // A current-thread runtime only polls spawned tasks while its
        // `block_on` is running. The commit crosses the auto-flush threshold
        // and spawns the flush stream; `block_on` then returns, and dropping
        // the runtime discards that task before its first poll.
        let rt1 = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt1.block_on(async {
            let tx = db.session().tx().await.unwrap();
            for i in 0..20 {
                tx.query_with("CREATE (d:Doc {doc_id: $id})")
                    .param("id", format!("d{i}"))
                    .fetch_all()
                    .await
                    .unwrap();
            }
            tx.commit().await.unwrap();
        });
        drop(rt1);

        rt0.block_on(async {
            let flushed = tokio::time::timeout(QUERY_TIMEOUT, db.flush()).await;
            assert!(
                matches!(flushed, Ok(Ok(_))),
                "flush after a dropped-runtime commit: {flushed:?}"
            );
            let n = db
                .session()
                .query("MATCH (d:Doc) RETURN count(d) AS c")
                .await
                .unwrap();
            assert_eq!(n.rows()[0].get::<i64>("c").unwrap(), 20);
        });
    });
}
