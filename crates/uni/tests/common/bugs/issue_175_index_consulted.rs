// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #175 — an observable for whether a query consulted a scalar index.
//!
//! Before this, index-present and index-absent execution were indistinguishable
//! from any layer above storage. The two things that looked like observables
//! were not:
//!
//! * `assert!(rows <= 1)` in the #57 tests is satisfied by `extra_runtime_filter`
//!   — the Arrow-side predicate applied in-process to the merged Lance+L0 batch
//!   (`df_graph/scan.rs`). It holds whether Lance used an index, scanned
//!   sequentially, or had no index at all.
//! * `ExplainOutput::index_usage` reports a *planner prediction*
//!   (`collect_index_usage` hardcodes `used: true`); nothing confirms it ran.
//!
//! `QueryMetrics::index_scans` comes from Lance's execution-stats callback,
//! which walks the metrics of the plan that actually executed.
//!
//! Every negative here asserts `scans_reported > 0` alongside
//! `index_scans == 0`. Without that pairing, deleting the callback would make
//! all of them pass.

use uni_db::{DataType, IndexType, ScalarType, Uni, Value};

/// Large enough that Lance does not brute-force past the index.
const N: i64 = 3000;

/// A key that exists. A missing key can legitimately report zero comparisons
/// when it falls outside every BTree page range, so the positive test must not
/// depend on one.
const PRESENT_KEY: &str = "name-42";

async fn fixture() -> Uni {
    let db = Uni::temporary().build().await.unwrap();
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .property("age", DataType::Int)
        .done()
        .apply()
        .await
        .unwrap();

    let s = db.session();
    let tx = s.tx().await.unwrap();
    tx.query_with(
        "UNWIND range(0, $n - 1) AS i \
         CREATE (:Item {name: 'name-' + toString(i), age: i % 90})",
    )
    .param("n", Value::Int(N))
    .fetch_all()
    .await
    .unwrap();
    tx.commit().await.unwrap();
    db.flush().await.unwrap();

    db.schema()
        .label("Item")
        .index("name", IndexType::Scalar(ScalarType::Hash))
        .apply()
        .await
        .unwrap();
    db
}

const EQ_QUERY: &str = "MATCH (n:Item) WHERE n.name = 'name-42' RETURN n.age AS a";

/// An equality predicate on an indexed column consults the index.
#[tokio::test]
async fn a_hash_equality_lookup_consults_the_index() {
    let db = fixture().await;
    let m = db
        .session()
        .query(EQ_QUERY)
        .await
        .unwrap()
        .metrics()
        .clone();

    assert!(
        m.scans_reported > 0,
        "no Lance scan reported at all, so this test measured nothing"
    );
    assert!(
        m.index_scans >= 1,
        "an `=` on a Hash-indexed column did not consult the index \
         (index_scans={}, comparisons={}, scans_reported={})",
        m.index_scans,
        m.index_comparisons,
        m.scans_reported
    );
    assert!(
        m.index_comparisons > 0,
        "the index was opened but performed no comparisons"
    );
}

/// A range predicate does not: `build_indexed_property_pushdown` collects only
/// Hash-indexed `=`/`IN`, so Lance is handed no predicate to serve from an index.
///
/// This is the shape the original tripwires used to conclude that index usage
/// was unobservable. It is a true negative, not an absent observable.
#[tokio::test]
async fn a_range_predicate_does_not_consult_the_index() {
    let db = fixture().await;
    let m = db
        .session()
        .query("MATCH (n:Item) WHERE n.age > 30 RETURN n.age AS a")
        .await
        .unwrap()
        .metrics()
        .clone();

    assert!(
        m.scans_reported > 0,
        "the callback never fired, so index_scans == 0 proves nothing"
    );
    assert_eq!(
        m.index_scans, 0,
        "a range predicate now consults an index — `build_indexed_property_pushdown` \
         grew a BTree route and #175's semantics need revisiting"
    );
}

/// An unfiltered scan consults nothing, even though the table has an index.
#[tokio::test]
async fn an_unfiltered_scan_consults_no_index() {
    let db = fixture().await;
    let m = db
        .session()
        .query("MATCH (n:Item) RETURN n.age AS a")
        .await
        .unwrap()
        .metrics()
        .clone();

    assert!(m.scans_reported > 0, "the callback never fired");
    assert_eq!(m.index_scans, 0, "an unfiltered scan consulted an index");
}

/// The counter tracks index *use*, not filter *pushdown*.
///
/// A fork runs the identical query against the identical rows with the identical
/// pushed filter; the only difference is `use_scalar_index(false)` on branch
/// scans (#106). A counter wired to "a filter was pushed" would report the same
/// on both sides. This also pins that `use_scalar_index(false)`, which was
/// previously asserted only by a comment.
#[tokio::test]
async fn a_branch_scan_consults_no_index_but_returns_the_same_rows() {
    let db = fixture().await;

    let primary = db.session().query(EQ_QUERY).await.unwrap();
    let primary_rows = primary.rows().len();
    let pm = primary.metrics().clone();

    let fork = db.session().fork("idx_175").await.unwrap();
    let forked = fork.query(EQ_QUERY).await.unwrap();
    let fm = forked.metrics().clone();

    assert_eq!(
        forked.rows().len(),
        primary_rows,
        "the fork returned a different row count, so the two sides are not comparable"
    );
    assert!(
        pm.index_scans >= 1,
        "precondition: primary must consult the index"
    );
    assert!(
        fm.branch_scans > 0,
        "precondition: the fork must have read its branch"
    );
    assert!(
        fm.scans_reported > 0,
        "the callback never fired on the branch path"
    );
    assert_eq!(
        fm.index_scans, 0,
        "a branch scan consulted a scalar index — `lance.rs` disables scalar-index \
         pushdown on branches for #106, so either that changed or the counter is \
         tracking filter pushdown rather than index use"
    );
}

/// The counter is not a constant, proven by flipping exactly one bit.
///
/// A hardcoded 0 fails the first assertion, a hardcoded 1 the second.
#[tokio::test]
async fn the_index_counter_is_not_a_constant() {
    let db = fixture().await;

    let with_index = db
        .session()
        .query(EQ_QUERY)
        .await
        .unwrap()
        .metrics()
        .clone();
    let without = db
        .session()
        .query("MATCH (n:Item) RETURN n.age AS a")
        .await
        .unwrap()
        .metrics()
        .clone();

    assert!(with_index.index_scans > 0, "counter never rises");
    assert_eq!(without.index_scans, 0, "counter never falls");
    assert!(with_index.scans_reported > 0 && without.scans_reported > 0);
}

/// Data still in L0 never reaches Lance, so nothing is reported at all.
///
/// This separates "a scan ran and consulted no index" from "no scan ran", which
/// is the distinction `scans_reported` exists to make.
#[tokio::test]
async fn an_unflushed_query_reports_no_lance_scan() {
    let db = Uni::temporary().build().await.unwrap();
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .done()
        .apply()
        .await
        .unwrap();

    let s = db.session();
    let tx = s.tx().await.unwrap();
    tx.query("CREATE (:Item {name: 'only'})").await.unwrap();
    tx.commit().await.unwrap();
    // deliberately no flush

    let m = s
        .query("MATCH (n:Item) WHERE n.name = 'only' RETURN n.name AS a")
        .await
        .unwrap()
        .metrics()
        .clone();

    assert!(
        m.l0_reads > 0,
        "precondition: the row must be served from L0"
    );
    assert_eq!(m.index_scans, 0, "an L0-only read consulted an index");
    assert_eq!(
        m.scans_reported, 0,
        "a Lance scan was reported for a query served entirely from L0"
    );
}

/// The signal survives a repeat of the identical query.
///
/// This is a tripwire for a change nobody has made yet. `indices_loaded` and
/// `parts_loaded` are cache-*miss* counters — Lance's `MetricsCollector`
/// documents that an index already in memory must not be counted as loaded —
/// and they fire today only because a fresh `Dataset` is opened per scan, so
/// the cache is always cold. Introduce a dataset or index cache and both drop
/// to zero on the second call while the index is still being used exactly as
/// before.
///
/// `index_comparisons` is recorded on the search path and is indifferent to
/// caching, which is why `attach_scan_stats` ORs it in. This test is what
/// notices if that stops being true: if a cache lands *and* the comparison term
/// is lost, the second run reports zero and this fails at the point of the
/// change rather than silently zeroing an observable that other tests depend on.
#[tokio::test]
async fn the_index_signal_survives_a_second_identical_query() {
    let db = fixture().await;
    let s = db.session();

    let first = s.query(EQ_QUERY).await.unwrap().metrics().clone();
    let second = s.query(EQ_QUERY).await.unwrap().metrics().clone();

    assert!(
        first.index_scans > 0,
        "precondition: the first run must consult the index"
    );
    assert!(
        second.index_scans > 0,
        "the second run of an identical query reported no index consultation \
         (first={}, second={}). If a dataset/index cache was added, `indices_loaded` \
         is now a cache-miss signal and `index_comparisons` must carry this \
         predicate on its own — see `attach_scan_stats`.",
        first.index_scans,
        second.index_scans
    );
    assert!(
        second.index_comparisons > 0,
        "the cache-insensitive term reported zero on a warm run, so `index_scans` \
         now depends entirely on cache-miss counters"
    );
}

/// `OperatorStats::index_hits` reports per operator, and only where it means
/// something (#173).
///
/// The field was `Option<usize>` hardcoded to `None` at every construction
/// site, while shipping in `PROFILE` output as though it reported something.
/// `None` there was indistinguishable from "no index was used".
///
/// It is filled from a DataFusion metric that `GraphScanExec` registers by
/// name, deliberately *not* from the query-level `index_scans`: copying one
/// total onto every node would print the same number on a projection as on the
/// scan that did the work — a number that looks like data and is not. So the
/// assertion has two halves, and the second is the important one.
#[tokio::test]
async fn index_hits_is_attributed_to_the_scan_and_not_to_every_operator() {
    let db = fixture().await;
    let (_r, profile) = db.session().query_with(EQ_QUERY).profile().await.unwrap();

    let scan = profile
        .runtime_stats
        .iter()
        .find(|s| s.operator == "GraphScanExec")
        .expect("no GraphScanExec in the profiled plan");
    assert_eq!(
        scan.index_hits,
        Some(1),
        "the scan that consulted the index did not report it (stats: {:?})",
        profile.runtime_stats
    );

    // Every other operator must abstain rather than inherit the scan's number.
    for s in profile
        .runtime_stats
        .iter()
        .filter(|s| s.operator != "GraphScanExec")
    {
        assert_eq!(
            s.index_hits, None,
            "{} reported index_hits={:?}; a query-level total has leaked onto an \
             operator that did no index work",
            s.operator, s.index_hits
        );
    }
}

/// A scan that consults nothing reports `Some(0)`, not `None`.
///
/// The distinction is the point: `None` means "this operator has no index
/// opinion", `Some(0)` means "this scan looked and used none". Collapsing them
/// would put the field back where it started.
#[tokio::test]
async fn a_scan_without_an_index_reports_zero_rather_than_unknown() {
    let db = fixture().await;
    let (_r, profile) = db
        .session()
        .query_with("MATCH (n:Item) RETURN n.age AS a")
        .profile()
        .await
        .unwrap();

    let scan = profile
        .runtime_stats
        .iter()
        .find(|s| s.operator == "GraphScanExec")
        .expect("no GraphScanExec in the profiled plan");
    assert_eq!(
        scan.index_hits,
        Some(0),
        "an unfiltered scan reported {:?} instead of Some(0) — the metric is not \
         being registered when it does not fire, so zero is indistinguishable \
         from absent",
        scan.index_hits
    );
}

// ---------------------------------------------------------------------------
// `scans_reported` has to cover the scans a query actually performs
// ---------------------------------------------------------------------------
//
// `scans_reported` is the denominator that lets `index_scans == 0` be told
// apart from silence. It only counts a scan whose `ScanRequest` carries the
// query's counters, and exactly one producer set them: the labelled vertex
// scan. A schemaless vertex scan and every edge scan passed `None`, so a query
// made entirely of those reported `scans_reported = 0` — and a zero denominator
// makes the numerator unreadable, which is the failure this counter pair was
// introduced to prevent.
//
// `docs/perf/index-scan-counter-2026-08-27.md` has the full audit, including
// the two thirds of the gap that are *not* a wiring omission and are
// deliberately left alone: Lance's `nearest()` path, which builds no
// `ScanRequest`, and the traversal, which serves from the in-memory adjacency
// and issues no Lance scan to count.

/// A schemaless scan is a real Lance scan and must be reported.
#[tokio::test]
async fn a_schemaless_scan_reaches_the_denominator() -> anyhow::Result<()> {
    let db = uni_db::Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Loose {k: 1}), (:Loose {k: 2})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    let m = db
        .session()
        .query("MATCH (n:Loose) RETURN count(n) AS c")
        .await?
        .metrics()
        .clone();

    assert!(
        m.scans_reported > 0,
        "a schemaless vertex scan reported no scan at all, so `index_scans` \
         has no denominator: scans_reported={}, index_scans={}",
        m.scans_reported,
        m.index_scans
    );
    Ok(())
}
