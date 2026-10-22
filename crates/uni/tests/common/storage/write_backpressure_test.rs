// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Soft write backpressure through the public API.
//!
//! The soft band (`soft_limit <= l1_runs < hard_limit`) used to be one
//! uninterruptible sleep of `base_delay × 2^(l1_runs − soft_limit)`; at the
//! defaults that is 40.96 s at 28 runs, longer than the 30 s statement
//! timeout. It is now a re-checking wait capped at one 100 ms hard-limit poll
//! period per run of the soft band. These tests pin the cap and the
//! interaction with a statement's own deadline.

// Rust guideline compliant

use std::time::{Duration, Instant};

use uni_common::config::WriteThrottleConfig;
use uni_db::{DataType, Uni, UniConfig, UniError};

/// Open a database whose every write is in the soft band: `soft_limit = 1`
/// and one flush generation on disk, with background compaction enabled but
/// out of reach so nothing resets the count. (With compaction disabled the
/// soft band does not wait at all, since nothing would relieve it.)
/// `hard_limit = 11` makes the soft budget 100 ms × 10 = 1 s, while
/// `base_delay = 10 s` puts the uncapped curve well past it.
async fn db_in_soft_band(path: &str) -> Uni {
    let mut config = UniConfig::default();
    idle_compaction(&mut config);
    config.throttle = WriteThrottleConfig {
        soft_limit: 1,
        hard_limit: 11,
        base_delay: Duration::from_secs(10),
    };
    let db = Uni::open(path).config(config).build().await.unwrap();
    db.schema()
        .label("Item")
        .property("n", DataType::Int64)
        .apply()
        .await
        .unwrap();

    let session = db.session();
    let tx = session.tx().await.unwrap();
    tx.execute("CREATE (:Item {n: 0})").await.unwrap();
    tx.commit().await.unwrap();
    db.flush().await.unwrap();
    assert_eq!(
        db.metrics().l1_run_count,
        1,
        "setup must leave exactly one flush generation, putting writes in the soft band"
    );
    db
}

/// An unbounded write in the soft band waits the budget (1 s), not the
/// 10 s the uncapped curve gives.
#[tokio::test]
async fn soft_band_write_waits_the_budget_not_the_curve() {
    let dir = tempfile::tempdir().unwrap();
    let db = db_in_soft_band(dir.path().to_str().unwrap()).await;

    let session = db.session();
    let tx = session.tx().await.unwrap();
    let started = Instant::now();
    tx.execute("CREATE (:Item {n: 1})").await.unwrap();
    let waited = started.elapsed();
    tx.commit().await.unwrap();

    assert!(
        waited >= Duration::from_millis(1000),
        "the soft band must still throttle for its 1 s budget; waited {waited:?}"
    );
    assert!(
        waited < Duration::from_secs(3),
        "the soft wait must be capped at its 1 s budget, not the 10 s curve; \
         waited {waited:?}"
    );
    db.shutdown().await.unwrap();
}

/// A statement deadline shorter than the soft wait surfaces as the typed
/// `Timeout` at the deadline, not after the throttle finishes.
#[tokio::test]
async fn statement_deadline_ends_a_soft_throttle_wait() {
    let dir = tempfile::tempdir().unwrap();
    let db = db_in_soft_band(dir.path().to_str().unwrap()).await;

    let session = db.session();
    let tx = session.tx().await.unwrap();
    let deadline = Duration::from_millis(150);
    let started = Instant::now();
    let err = tx
        .execute_with("CREATE (:Item {n: 2})")
        .timeout(deadline)
        .run()
        .await
        .expect_err("a write throttled past its deadline must time out");
    let waited = started.elapsed();

    assert!(
        matches!(err, UniError::Timeout { timeout_ms: 150 }),
        "expected the typed Timeout for the 150 ms deadline, got {err:?}"
    );
    assert!(
        waited < Duration::from_millis(900),
        "the deadline must end the throttle wait promptly, well inside the 1 s \
         budget; waited {waited:?}"
    );
    drop(tx);
    db.shutdown().await.unwrap();
}

// ── Wave P1: per-statement charge, metrics, deadlines, compaction off ──

/// Background compaction that is enabled but never runs in a test's
/// lifetime: thresholds and the check interval are out of reach, so the
/// flush-generation count only moves when the test flushes.
fn idle_compaction(config: &mut UniConfig) {
    config.compaction.enabled = true;
    config.compaction.max_l1_runs = usize::MAX;
    config.compaction.max_l1_size_bytes = u64::MAX;
    config.compaction.max_l1_age = Duration::from_secs(365 * 24 * 3600);
    config.compaction.check_interval = Duration::from_secs(3600);
}

/// A database with `runs` flush generations on disk under `throttle`, with
/// `Item` and `R` declared.
async fn db_with_runs(
    path: &str,
    throttle: WriteThrottleConfig,
    compaction_enabled: bool,
    query_timeout: Option<Duration>,
    runs: usize,
) -> Uni {
    let mut config = UniConfig::default();
    if compaction_enabled {
        idle_compaction(&mut config);
    } else {
        config.compaction.enabled = false;
    }
    if let Some(t) = query_timeout {
        config.query_timeout = t;
    }
    config.throttle = throttle;
    let db = Uni::open(path).config(config).build().await.unwrap();
    db.schema()
        .label("Item")
        .property("n", DataType::Int64)
        .edge_type("R", &["Item"], &["Item"])
        .apply()
        .await
        .unwrap();
    // Seed with no throttle in effect: the count starts at zero.
    for i in 0..runs {
        let session = db.session();
        let tx = session.tx().await.unwrap();
        tx.execute(&format!("CREATE (:Item {{n: {}}})", 1000 + i))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        db.flush().await.unwrap();
    }
    assert_eq!(db.metrics().l1_run_count, runs, "setup flush generations");
    db
}

/// Soft band whose budget is 1 s: `soft_limit = 1`, `hard_limit = 11`,
/// `base_delay = 10 s`, one flush generation.
const SOFT: WriteThrottleConfig = WriteThrottleConfig {
    soft_limit: 1,
    hard_limit: 11,
    base_delay: Duration::from_secs(10),
};

async fn timed<T>(fut: impl std::future::Future<Output = T>) -> (T, Duration) {
    let started = Instant::now();
    let out = fut.await;
    (out, started.elapsed())
}

/// The soft-band budget is charged once per statement or batch, not once
/// per row: a four-row `UNWIND … CREATE`, a two-node one-edge `CREATE`, and
/// bulk inserts of four vertices and three edges each wait one budget.
#[tokio::test]
async fn soft_band_charges_once_per_statement_or_batch() {
    let dir = tempfile::tempdir().unwrap();
    let db = db_with_runs(dir.path().to_str().unwrap(), SOFT, true, None, 1).await;
    let session = db.session();
    let one = Duration::from_millis(1000);
    let two = Duration::from_millis(1900);

    let tx = session.tx().await.unwrap();
    let (r, waited) = timed(tx.execute("UNWIND range(1, 4) AS i CREATE (:Item {n: i})")).await;
    r.unwrap();
    assert!(
        waited >= one && waited < two,
        "UNWIND of 4 CREATEs: one 1 s budget, waited {waited:?}"
    );

    let (r, waited) = timed(tx.execute("CREATE (:Item {n: 10})-[:R]->(:Item {n: 11})")).await;
    r.unwrap();
    assert!(
        waited >= one && waited < two,
        "CREATE of 2 nodes + 1 edge: one budget, waited {waited:?}"
    );

    let props = |n: i64| -> uni_common::Properties {
        [("n".to_owned(), uni_common::Value::Int(n))]
            .into_iter()
            .collect()
    };
    let (vids, waited) =
        timed(tx.bulk_insert_vertices("Item", (20..24).map(props).collect())).await;
    let vids = vids.unwrap();
    assert!(
        waited >= one && waited < two,
        "bulk_insert_vertices of 4: one budget, waited {waited:?}"
    );

    let edges = vec![
        (vids[0], vids[1], uni_common::Properties::new()),
        (vids[1], vids[2], uni_common::Properties::new()),
        (vids[2], vids[3], uni_common::Properties::new()),
    ];
    let (r, waited) = timed(tx.bulk_insert_edges("R", edges)).await;
    r.unwrap();
    assert!(
        waited >= one && waited < two,
        "bulk_insert_edges of 3: one budget, waited {waited:?}"
    );

    // A read in the same transaction is never throttled.
    let (r, waited) = timed(tx.query("MATCH (n:Item) RETURN count(n) AS c")).await;
    r.unwrap();
    assert!(
        waited < Duration::from_millis(500),
        "read waited {waited:?}"
    );
    tx.commit().await.unwrap();

    let m = db.metrics();
    assert_eq!(
        m.write_throttle_waits, 4,
        "one throttled wait per statement or batch"
    );
    assert!(
        m.write_throttle_wait_time >= Duration::from_millis(4000),
        "wait time accumulates every wait: {:?}",
        m.write_throttle_wait_time
    );
    db.shutdown().await.unwrap();
}

/// `write_throttle_pressure` reflects where the flush-generation count sits
/// between the soft and the hard limit.
#[tokio::test]
async fn write_throttle_pressure_tracks_the_band() {
    let throttle = WriteThrottleConfig {
        soft_limit: 2,
        hard_limit: 4,
        base_delay: Duration::from_millis(1),
    };
    let dir = tempfile::tempdir().unwrap();
    let db = db_with_runs(dir.path().to_str().unwrap(), throttle, true, None, 1).await;
    let m = db.metrics();
    assert_eq!(
        m.write_throttle_pressure.value(),
        0.0,
        "below the soft limit"
    );
    assert!(!m.write_throttle_pressure.is_throttled());
    assert_eq!(m.write_throttle_waits, 0);
    assert_eq!(m.writes_stalled, 0);

    let s = db.session();
    let tx = s.tx().await.unwrap();
    tx.execute("CREATE (:Item {n: 1})").await.unwrap();
    tx.commit().await.unwrap();
    db.flush().await.unwrap(); // 2 runs: the soft limit
    let p2 = db.metrics().write_throttle_pressure.value();
    let tx = s.tx().await.unwrap();
    tx.execute("CREATE (:Item {n: 2})").await.unwrap();
    tx.commit().await.unwrap();
    db.flush().await.unwrap(); // 3 runs
    let p3 = db.metrics().write_throttle_pressure.value();
    assert!(p2 > 0.0 && p2 < p3 && p3 < 1.0, "p2={p2} p3={p3}");
    db.shutdown().await.unwrap();
}

/// Hard band with a 1 s statement timeout: a bulk insert ends with the
/// typed `Timeout` at the deadline, as a Cypher write does, instead of
/// stalling until compaction runs.
#[tokio::test]
async fn bulk_insert_in_the_hard_band_times_out_like_a_statement() {
    let throttle = WriteThrottleConfig {
        soft_limit: 1,
        hard_limit: 2,
        base_delay: Duration::from_millis(1),
    };
    let dir = tempfile::tempdir().unwrap();
    let db = db_with_runs(
        dir.path().to_str().unwrap(),
        throttle,
        true,
        Some(Duration::from_secs(1)),
        2,
    )
    .await;
    let s = db.session();
    let props: uni_common::Properties = [("n".to_owned(), uni_common::Value::Int(1))]
        .into_iter()
        .collect();

    let tx = s.tx().await.unwrap();
    let (r, waited) = timed(tokio::time::timeout(
        Duration::from_secs(5),
        tx.execute("CREATE (:Item {n: 1})"),
    ))
    .await;
    let err = r.expect("outer guard").expect_err("stalled CREATE");
    assert!(matches!(err, UniError::Timeout { .. }), "{err:?}");
    assert!(waited < Duration::from_secs(3), "CREATE waited {waited:?}");
    drop(tx);

    let tx = s.tx().await.unwrap();
    let (r, waited) = timed(tokio::time::timeout(
        Duration::from_secs(5),
        tx.bulk_insert_vertices("Item", vec![props.clone()]),
    ))
    .await;
    let err = r
        .expect("bulk_insert_vertices must end at the statement timeout, not stall")
        .expect_err("stalled bulk insert");
    assert!(matches!(err, UniError::Timeout { .. }), "{err:?}");
    assert!(
        waited < Duration::from_secs(3),
        "bulk insert waited {waited:?}"
    );
    assert!(
        tx.commit().await.is_err(),
        "a timed-out batch leaves the transaction rollback-only"
    );

    let tx = s.tx().await.unwrap();
    let vids: Vec<_> = {
        let r = s
            .query("MATCH (n:Item) RETURN id(n) AS v ORDER BY v LIMIT 2")
            .await
            .unwrap();
        r.rows()
            .iter()
            .map(|row| uni_common::core::id::Vid::new(row.get::<i64>("v").unwrap() as u64))
            .collect()
    };
    let (r, waited) = timed(tokio::time::timeout(
        Duration::from_secs(5),
        tx.bulk_insert_edges("R", vec![(vids[0], vids[1], uni_common::Properties::new())]),
    ))
    .await;
    let err = r
        .expect("bulk_insert_edges must end at the statement timeout, not stall")
        .expect_err("stalled bulk edge insert");
    assert!(matches!(err, UniError::Timeout { .. }), "{err:?}");
    assert!(
        waited < Duration::from_secs(3),
        "bulk edges waited {waited:?}"
    );
    drop(tx);
    db.shutdown().await.unwrap();
}

/// With compaction disabled nothing will lower the flush-generation count,
/// so a write at the hard limit fails at once with `WriteStalled`, naming
/// the manual compaction, and the soft band does not wait at all.
#[tokio::test]
async fn compaction_disabled_hard_limit_fails_fast_with_write_stalled() {
    let throttle = WriteThrottleConfig {
        soft_limit: 1,
        hard_limit: 2,
        base_delay: Duration::from_secs(10),
    };
    let dir = tempfile::tempdir().unwrap();
    let db = db_with_runs(dir.path().to_str().unwrap(), throttle, false, None, 1).await;
    let s = db.session();

    // Soft band, compaction off: no wait.
    let tx = s.tx().await.unwrap();
    let (r, waited) = timed(tx.execute("CREATE (:Item {n: 1})")).await;
    r.unwrap();
    tx.commit().await.unwrap();
    assert!(
        waited < Duration::from_millis(500),
        "soft band with compaction off must not wait; waited {waited:?}"
    );
    db.flush().await.unwrap(); // 2 runs: the hard limit
    assert_eq!(db.metrics().write_throttle_pressure.value(), 1.0);

    let tx = s.tx().await.unwrap();
    let (r, waited) = timed(tx.execute("CREATE (:Item {n: 2})")).await;
    let err = r.expect_err("a write at the hard limit with compaction off");
    assert!(
        matches!(
            err,
            UniError::WriteStalled {
                l1_runs: 2,
                hard_limit: 2
            }
        ),
        "{err:?}"
    );
    assert!(
        err.to_string().contains("uni.admin.compact()"),
        "the message names the way out: {err}"
    );
    assert!(
        waited < Duration::from_millis(500),
        "fail fast; waited {waited:?}"
    );
    drop(tx);

    let tx = s.tx().await.unwrap();
    let props: uni_common::Properties = [("n".to_owned(), uni_common::Value::Int(3))]
        .into_iter()
        .collect();
    let err = tx
        .bulk_insert_vertices("Item", vec![props])
        .await
        .expect_err("bulk insert at the hard limit with compaction off");
    assert!(matches!(err, UniError::WriteStalled { .. }), "{err:?}");
    drop(tx);
    assert!(db.metrics().writes_stalled >= 2);

    // Reads still work.
    let c = s
        .query("MATCH (n:Item) RETURN count(n) AS c")
        .await
        .unwrap();
    assert_eq!(c.rows()[0].get::<i64>("c").unwrap(), 2);
    db.shutdown().await.unwrap();
}
