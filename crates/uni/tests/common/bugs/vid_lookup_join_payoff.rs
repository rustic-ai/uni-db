// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Does the `VidLookupJoinExec` optimization actually pay?
//!
//! The operator exists to turn a full probe-table scan into N indexed `_vid`
//! lookups: materialize the build side, collect its distinct VIDs, push them
//! down as `_vid IN (...)`. On a small selective build against a large target
//! table that should be a large win.
//!
//! It has never fired for that shape. The build anchor must be Arrow `UInt64`
//! (`df_planner.rs:4387`, protecting a bare `downcast_ref::<UInt64Array>` at
//! `vid_lookup_join.rs:383`), but the canonical build key is a *user property*
//! — `a.linked_vid` — and user properties are `Int64` because Cypher has no
//! unsigned integer. So the only shape that clears the guard is `id(a) = id(b)`,
//! where both keys are already `_vid`s and there is nothing to look up.
//!
//! This measures what is being left on the table, so the keep-or-delete call
//! rests on a number rather than on the design's promise.
//!
//! Ignored by default: it builds a 20k-row fixture. Run explicitly:
//! `cargo nextest run -p uni-db --run-ignored all -E 'test(vid_lookup_join_payoff)'`

use std::time::Instant;

use uni_db::{DataType, Uni, Value};

use crate::plan_shape::plan_ops;

/// Targets in the probe table — large enough that a full scan is clearly worse
/// than a handful of indexed lookups.
const TARGETS: i64 = 20_000;
/// Sources in the build side — small and selective, the regime the operator was
/// written for.
const SOURCES: i64 = 50;

async fn fixture() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Target")
        .property("name", DataType::String)
        .done()
        .label("Source")
        .property("name", DataType::String)
        .property_nullable("linked_vid", DataType::Int64)
        .done()
        .apply()
        .await
        .unwrap();

    let s = db.session();
    let tx = s.tx().await.unwrap();
    tx.query_with("UNWIND range(0, $n - 1) AS i CREATE (:Target {name: 't' + toString(i)})")
        .param("n", Value::Int(TARGETS))
        .fetch_all()
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Link each Source at a distinct, widely-spaced Target vid so the build set
    // is small and scattered — the worst case for a scan, the best for lookups.
    let tx = s.tx().await.unwrap();
    for i in 0..SOURCES {
        tx.query_with("CREATE (:Source {name: 's' + toString($i), linked_vid: $v})")
            .param("i", Value::Int(i))
            .param("v", Value::Int(i * (TARGETS / SOURCES)))
            .fetch_all()
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    db.flush().await.unwrap();
    db
}

/// Reports the operator chosen, the rows each operator moved, and wall-clock.
///
/// Caveat worth stating: when `VidLookupJoinExec` *does* fire, its probe scan is
/// invisible here — `children()` returns only the build side, so the probe
/// `GraphScanExec` never reaches `runtime_stats`. The operator's own
/// observability defect hides the very number that would prove its win, which is
/// why wall-clock is reported alongside.
#[tokio::test]
#[ignore = "payoff measurement: builds a 20k-row fixture"]
async fn measure_vid_lookup_join_payoff() {
    let db = fixture().await;
    let s = db.session();

    let q = "MATCH (a:Source) MATCH (b:Target) WHERE id(b) = a.linked_vid \
             RETURN a.name AS an, b.name AS bn";

    let ops = plan_ops(&s, q).await;
    eprintln!("[payoff] operator = {ops:?}");

    let (_r, profile) = s.query_with(q).profile().await.unwrap();
    for st in &profile.runtime_stats {
        eprintln!(
            "[payoff]   {:<22} rows={:<8} {:.2}ms",
            st.operator, st.actual_rows, st.time_ms
        );
    }

    // Wall-clock over repeats, since a single run is dominated by setup.
    const REPS: u32 = 5;
    let t0 = Instant::now();
    let mut rows = 0usize;
    for _ in 0..REPS {
        rows = s.query(q).await.unwrap().rows().len();
    }
    let ms = t0.elapsed().as_secs_f64() * 1000.0 / f64::from(REPS);
    eprintln!("[payoff] {TARGETS} targets x {SOURCES} sources -> {rows} rows, {ms:.2} ms/query");

    assert_eq!(
        rows as i64, SOURCES,
        "each Source must match exactly one Target"
    );
}

/// The same measurement for the **LEFT outer** path.
///
/// `VidJoinKind::Left` had never executed: for LEFT the probe is necessarily
/// the optional side, and `wrap_optional` always wraps that in
/// `NestedLoopJoinExec(PlaceholderRowExec, GraphScanExec)`, which the
/// bare-scan guard rejected. This measures what unwrapping that redundant
/// wrapper buys, on the same fixture as the INNER case so the two are
/// comparable.
#[tokio::test]
#[ignore = "payoff measurement: builds a 20k-row fixture"]
async fn measure_left_outer_payoff() {
    let db = fixture().await;
    let s = db.session();

    let q = "MATCH (a:Source) OPTIONAL MATCH (b:Target) WHERE id(b) = a.linked_vid \
             RETURN a.name AS an, b.name AS bn";

    let ops = plan_ops(&s, q).await;
    eprintln!("[payoff-left] operator = {ops:?}");

    let (_r, profile) = s.query_with(q).profile().await.unwrap();
    for st in &profile.runtime_stats {
        eprintln!(
            "[payoff-left]   {:<22} rows={:<8} {:.2}ms",
            st.operator, st.actual_rows, st.time_ms
        );
    }

    const REPS: u32 = 5;
    let t0 = Instant::now();
    let mut rows = 0usize;
    for _ in 0..REPS {
        rows = s.query(q).await.unwrap().rows().len();
    }
    let ms = t0.elapsed().as_secs_f64() * 1000.0 / f64::from(REPS);
    eprintln!(
        "[payoff-left] {TARGETS} targets x {SOURCES} sources -> {rows} rows, {ms:.2} ms/query"
    );

    // Every Source survives the outer join; each happens to match here.
    assert_eq!(
        rows as i64, SOURCES,
        "OPTIONAL MATCH must preserve every Source"
    );
}
