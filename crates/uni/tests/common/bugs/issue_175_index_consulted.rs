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
