//! Phase 1 — per-query counter tests.
//!
//! Every counter gets a **positive and a negative** case. That pairing is the
//! whole point: a counter that is always non-zero is exactly as useless as one
//! that is always zero, and both look like success from the outside.
//!
//! Phase 0A found five `QueryMetrics` fields that had existed for a long time
//! and always read zero (`docs/perf/dqp-feasibility-2026-08-12.md` §4). Nothing
//! caught it because nothing asserted on them. These tests are the assertion.

use uni_db::{DataType, Uni};

/// A small flushed fixture: `n` `Person` rows in L1, nothing dirty.
async fn flushed_db(n: usize) -> anyhow::Result<Uni> {
    let db = Uni::temporary().build().await?;
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .property_nullable("age", DataType::Int)
        .done()
        .label("Ghost")
        .property("name", DataType::String)
        .done()
        .apply()
        .await?;
    let session = db.session();
    let tx = session.tx().await?;
    for i in 0..n {
        tx.execute(&format!(
            "CREATE (:Person {{name: 'p{i}', age: {}}})",
            i % 60
        ))
        .await?;
    }
    tx.commit().await?;
    db.flush().await?;
    Ok(db)
}

const Q: &str = "MATCH (p:Person) RETURN p.name AS c0";

// ── L0 vs storage ───────────────────────────────────────────────────────────

/// Positive **and** negative in one test, because the pair is the claim: a
/// flushed read touches no L0, and the same read after an uncommitted-to-L1
/// write touches exactly the rows that were added.
#[tokio::test]
async fn l0_reads_zero_when_flushed_and_nonzero_when_dirty() -> anyhow::Result<()> {
    let db = flushed_db(50).await?;
    let session = db.session();

    let clean = session.query(Q).await?;
    assert_eq!(
        clean.metrics().l0_reads,
        0,
        "a fully flushed read must serve nothing from L0"
    );
    assert!(
        clean.metrics().storage_reads > 0,
        "...and must serve its rows from storage instead (got {})",
        clean.metrics().storage_reads
    );

    // Add rows without flushing: they live in L0 only.
    let tx = session.tx().await?;
    for i in 0..7 {
        tx.execute(&format!("CREATE (:Person {{name: 'dirty{i}', age: 41}})"))
            .await?;
    }
    tx.commit().await?;

    let dirty = session.query(Q).await?;
    assert_eq!(
        dirty.metrics().l0_reads,
        7,
        "exactly the 7 unflushed rows must be counted as served from L0"
    );
    assert_eq!(
        dirty.metrics().storage_reads,
        clean.metrics().storage_reads,
        "the flushed corpus did not change, so storage_reads must not move"
    );
    Ok(())
}

/// `rows_scanned` must exceed `rows_returned` when a filter discards rows —
/// otherwise it is just `rows_returned` under another name.
#[tokio::test]
async fn rows_scanned_exceeds_rows_returned_under_a_filter() -> anyhow::Result<()> {
    let db = flushed_db(60).await?;
    let r = db
        .session()
        .query("MATCH (p:Person) WHERE p.age = 1 RETURN p.name AS c0")
        .await?;
    let m = r.metrics();
    assert!(
        m.rows_returned < m.rows_scanned,
        "a selective filter must scan more than it returns (returned {}, scanned {})",
        m.rows_returned,
        m.rows_scanned
    );
    assert!(m.rows_scanned >= 60, "the whole label was scanned");
    Ok(())
}

/// The session-level rollup was never incremented before Phase 1.
#[tokio::test]
async fn session_total_rows_scanned_accumulates() -> anyhow::Result<()> {
    let db = flushed_db(30).await?;
    let session = db.session();
    assert_eq!(session.metrics().total_rows_scanned, 0);
    session.query(Q).await?;
    let after_one = session.metrics().total_rows_scanned;
    assert!(after_one > 0, "one query must contribute to the rollup");
    session.query(Q).await?;
    assert!(
        session.metrics().total_rows_scanned > after_one,
        "a second query must add to it"
    );
    Ok(())
}

// ── fork branch scans ───────────────────────────────────────────────────────

/// A **pristine** fork — one that has written nothing — still executes branch
/// scans, and that is correct.
///
/// `create_fork_2pc` materializes one Lance branch per dataset that exists at
/// fork creation, and `ForkScope::branch_for` (`fork/scope.rs:292`) resolves
/// from that map. So fork scope alone is enough to take the branch path; no
/// fork-local write is required.
///
/// This matters beyond the counter: the DQP oracle's highest-value lever is
/// "primary vs **pristine** fork", and it needs this witness to fire on a fork
/// with zero writes. It does.
#[tokio::test(flavor = "multi_thread")]
async fn branch_scans_fire_on_a_pristine_fork_and_never_on_primary() -> anyhow::Result<()> {
    let db = flushed_db(20).await?;

    let primary = db.session().query(Q).await?;
    assert_eq!(
        primary.metrics().branch_scans,
        0,
        "a primary session never executes a branch scan"
    );

    let fork = db.session().fork("dqp_pristine").await?;
    let pristine = fork.query(Q).await?;
    assert!(
        pristine.metrics().branch_scans > 0,
        "a pristine fork reads through the branch created for Person at fork \
         time (branch_scans = {})",
        pristine.metrics().branch_scans
    );
    assert!(
        !pristine.is_empty(),
        "and still sees the parent's rows through the branch's base_paths chain"
    );

    // The fork's branch scan must not leak into the parent's metrics.
    let after = db.session().query(Q).await?;
    assert_eq!(
        after.metrics().branch_scans,
        0,
        "counters are per-query: the fork's scan must not appear on primary"
    );
    Ok(())
}

/// The negative case that separates an execution witness from a config read.
///
/// `BranchedBackend` resolves a branch **per table**. A dataset that did not
/// exist when the fork was created has no entry in the fork's dataset map, so
/// `branch_for` returns `None` and the read executes an ordinary primary scan —
/// with fork scope fully active the whole time.
///
/// A counter incremented where the branch is *selected*, or off "this session is
/// forked", would report a fork read here. One incremented where the branch scan
/// *executes* reports zero. That is the entire distinction Phase 1 exists to
/// get right.
#[tokio::test(flavor = "multi_thread")]
async fn branch_scans_zero_for_a_table_the_fork_has_no_branch_for() -> anyhow::Result<()> {
    let db = flushed_db(20).await?;

    // `Ghost` is declared in the schema but never written, so no dataset exists
    // for it at fork time and the fork gets no branch for it.
    let fork = db.session().fork("dqp_no_branch").await?;

    // Now create the dataset on primary, after the fork point.
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Ghost {name: 'late'})").await?;
    tx.commit().await?;
    db.flush().await?;

    let r = fork.query("MATCH (g:Ghost) RETURN g.name AS c0").await?;
    assert_eq!(
        r.metrics().branch_scans,
        0,
        "the fork has no branch for Ghost, so this scan ran against primary — a \
         non-zero count here would mean the counter reports configuration \
         (\"this session is forked\") rather than execution"
    );
    Ok(())
}

// ── pinned snapshot reads ───────────────────────────────────────────────────

/// Positive and negative for the pinned-read witness.
///
/// Deliberately not asserted off `Session::is_pinned`: that is configuration.
/// The counter fires where the version ceiling is applied to a scan.
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_reads_zero_when_live_and_nonzero_when_pinned() -> anyhow::Result<()> {
    let db = flushed_db(20).await?;

    let live = db.session().query(Q).await?;
    assert_eq!(
        live.metrics().snapshot_reads,
        0,
        "a live read applies no snapshot version ceiling"
    );

    let snap = db.create_snapshot("dqp_pin").await?;
    let mut pinned = db.session();
    pinned.pin_to_version(&snap).await?;
    let r = pinned.query(Q).await?;
    assert!(
        r.metrics().snapshot_reads > 0,
        "a pinned read must apply the snapshot ceiling (snapshot_reads = {})",
        r.metrics().snapshot_reads
    );
    Ok(())
}

/// An ordinary transaction also carries a version pin internally. It must **not**
/// count as a snapshot read, or the pinned-vs-live witness fires on every
/// transactional query and means nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_plain_transaction_is_not_counted_as_a_snapshot_read() -> anyhow::Result<()> {
    let db = flushed_db(20).await?;
    let session = db.session();
    let tx = session.tx().await?;
    let r = tx.query(Q).await?;
    assert_eq!(
        r.metrics().snapshot_reads,
        0,
        "a read-write transaction's version pin is not a time-travel snapshot"
    );
    tx.commit().await?;
    Ok(())
}

// ── counter isolation ───────────────────────────────────────────────────────

/// Counters must not accumulate across queries on one session.
///
/// The executor is cloned from a cached template on the write path, so a shared
/// counter handle would fold one query's counts into the next one's result —
/// the same trap the `warnings` collector documents.
#[tokio::test]
async fn counters_do_not_accumulate_across_queries() -> anyhow::Result<()> {
    let db = flushed_db(40).await?;
    let session = db.session();
    let first = session.query(Q).await?;
    let second = session.query(Q).await?;
    assert_eq!(
        first.metrics().storage_reads,
        second.metrics().storage_reads,
        "identical queries must report identical counts, not a running total"
    );
    assert_eq!(
        first.metrics().rows_scanned,
        second.metrics().rows_scanned,
        "rows_scanned must be per-query, not cumulative"
    );
    Ok(())
}

/// PROFILE used to return a result whose every metric read zero — on the one
/// query form a user runs *specifically* to see metrics.
#[tokio::test]
async fn profile_returns_real_metrics() -> anyhow::Result<()> {
    let db = flushed_db(25).await?;
    let (result, _profile) = db.session().query_with(Q).profile().await?;
    assert!(
        result.metrics().rows_returned > 0,
        "PROFILE must report rows_returned"
    );
    assert!(
        result.metrics().rows_scanned > 0,
        "PROFILE must report scan counters, not Default::default()"
    );
    Ok(())
}

/// The cached read path parsed and planned, then threw both measurements away.
#[tokio::test]
async fn cache_miss_reports_real_parse_and_plan_time() -> anyhow::Result<()> {
    let db = flushed_db(10).await?;
    let session = db.session();
    // First execution of this text: a cache miss, so parse and plan both ran.
    let miss = session
        .query("MATCH (p:Person) WHERE p.age >= 0 RETURN p.name AS c0")
        .await?;
    assert!(
        !miss.metrics().parse_time.is_zero(),
        "a cache miss parses, so parse_time must be non-zero"
    );
    assert!(
        !miss.metrics().plan_time.is_zero(),
        "a cache miss plans, so plan_time must be non-zero"
    );
    Ok(())
}
