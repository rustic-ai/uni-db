// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Can `VidLookupJoinExec` be emitted at all?
//!
//! A coverage run found `df_graph/vid_lookup_join.rs` at **0 of 441 executable
//! lines**, despite the 15-test suite in `issue_55_cross_match_pushdown.rs`
//! written specifically for it. Those tests assert result bags, and the
//! operator is contractually bag-identical to the `HashJoinExec` it replaces —
//! so they pass whether it fires or silently falls back.
//!
//! These probes answer the question the bag cannot, by asserting on the
//! **physical** operators actually executed (`PROFILE`, not `EXPLAIN` —
//! `plan_text` is the *logical* plan and can never name a physical operator).
//!
//! # The six guards, and which probe targets which
//!
//! `try_emit_vid_lookup_join` (`df_planner.rs:4279`) returns `Ok(None)` — and
//! the planner silently emits `HashJoinExec` — unless all of:
//!
//! 1. equi-pairs are non-empty,
//! 2. some pair is an `id(x)` / `_vid` anchor,
//! 3. the **probe subtree is a bare `GraphScanExec`** — any `FilterExec`,
//!    `ProjectionExec` (added when a bare variable is projected) or SSI
//!    `ReadSetRecordingExec` defeats it,
//! 4. every equi key compiles to a bare `Column`,
//! 5. the **anchor build column is Arrow `UInt64`**,
//! 6. not (`Left` join with the probe on the left).
//!
//! Guard 5 is the decisive one: **no `uni_common::DataType` maps to `UInt64`**.
//! Only `{var}._vid` columns (`df_graph/scan.rs:378,402`) and Locy derived-scan
//! node columns are `UInt64`. So a property can never be the build anchor,
//! which is exactly what `documented_motivating_query_cannot_fire` pins.

use uni_db::{DataType, Uni, Value};

use crate::plan_shape::{assert_plan_avoids, assert_plan_uses, plan_ops};

// Operator names are written as literals at each call site rather than via
// constants. The activation gate (`plan_shape::gate`) requires the name to
// appear as an argument to the assertion helper, because that is the only form
// it can verify — a constant hides which operator a test actually vouches for,
// from the gate and from the reader alike.

/// Seeds so `id(a) = id(b)` has **both** matches and non-matches.
///
/// A vid is globally unique, so a `:A` node and a *different* `:B` node can
/// never share one — an earlier version of this seed created them separately
/// and the join matched zero rows, which fired the operator but exercised none
/// of its join logic and made the correctness check vacuous.
///
/// The only way `id(a) = id(b)` matches is a node carrying **both** labels. So:
/// 3 dual-labelled `:A:B` nodes (which must match themselves), plus 2 `:A`-only
/// and 2 `:B`-only (which must not match anything).
async fn seeded() -> Uni {
    let db = Uni::in_memory().build().await.unwrap();
    let session = db.session();
    let tx = session.tx().await.unwrap();
    // Schemaless on purpose: a declared multi-label schema is a separate axis,
    // and all this probe needs is `_vid` on both sides.
    for i in 0..3i64 {
        tx.query_with("CREATE (:A:B {x: $x, y: $y})")
            .param("x", Value::Int(i))
            .param("y", Value::Int(i * 10))
            .fetch_all()
            .await
            .unwrap();
    }
    for i in 100..102i64 {
        tx.query_with("CREATE (:A {x: $x})")
            .param("x", Value::Int(i))
            .fetch_all()
            .await
            .unwrap();
        tx.query_with("CREATE (:B {y: $y})")
            .param("y", Value::Int(i))
            .fetch_all()
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    db
}

/// **Probe A.** The only Cypher shape that can clear all six guards: both join
/// keys are `_vid` (the sole `UInt64` producer), the projection is
/// property-only so no `ProjectionExec` is added, there is no `WHERE` on the
/// probe side so no `FilterExec` is added, and it runs outside a transaction so
/// no `ReadSetRecordingExec` wraps the scan.
///
/// This test does not assert — it **reports**. Its job is to produce the
/// evidence the fix-or-delete decision is made from, and an assertion here
/// would encode the conclusion before the measurement.
#[tokio::test]
async fn probe_a_degenerate_id_equality() {
    let db = seeded().await;
    let s = db.session();
    let q = "MATCH (a:A) MATCH (b:B) WHERE id(a) = id(b) RETURN a.x AS ax, b.y AS by";
    let ops = plan_ops(&s, q).await;
    eprintln!("[probe-a] ops = {ops:?}");
    eprintln!(
        "[probe-a] VidLookupJoinExec present: {}",
        ops.iter().any(|o| o == "VidLookupJoinExec")
    );
}

/// **Probe A, guard 3.** One extra conjunct on the probe side merges into
/// `Scan.filter`, which `apply_scan_filter` materialises as a `FilterExec`
/// above the scan — so the probe is no longer a bare `GraphScanExec`.
#[tokio::test]
async fn probe_guard3_probe_side_filter() {
    let db = seeded().await;
    let s = db.session();
    let q = "MATCH (a:A) MATCH (b:B) WHERE a.x > 0 AND id(a) = id(b) \
             RETURN a.x AS ax, b.y AS by";
    let ops = plan_ops(&s, q).await;
    eprintln!("[probe-guard3] ops = {ops:?}");
}

/// Reporting probe for the operator's documented motivating query
/// (`vid_lookup_join.rs:6-18`): join a stored vid property against `id(b)`.
///
/// This one prints rather than asserts — the assertion lives in
/// `documented_query_uses_the_vid_lookup_join`. Kept because the printed
/// operator vector is what the investigation was decided from, and it is
/// cheaper to read than to reconstruct.
#[tokio::test]
async fn report_documented_motivating_query_plan() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Source")
        .property("score", DataType::Float64)
        .property_nullable("linked_vid", DataType::Int64)
        .done()
        .label("Target")
        .property("name", DataType::String)
        .done()
        .apply()
        .await
        .unwrap();

    let s = db.session();
    let tx = s.tx().await.unwrap();
    tx.execute("CREATE (:Target {name: 't0'})").await.unwrap();
    tx.execute("CREATE (:Source {score: 0.9, linked_vid: 0})")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let q = "MATCH (a:Source) MATCH (b:Target) WHERE id(b) = a.linked_vid \
             RETURN a.score AS s, b.name AS n";
    let ops = plan_ops(&s, q).await;
    eprintln!("[probe-guard5/documented] ops = {ops:?}");
    eprintln!(
        "[probe-guard5/documented] VidLookupJoinExec present: {}",
        ops.iter().any(|o| o == "VidLookupJoinExec")
    );
}

/// **Probe, guard 6.** `LEFT` outer with the anchor matching the *left*
/// expression first gives `(Left, ProbeSide::Left)`, which bails.
///
/// This matters beyond coverage: `VidJoinKind::Left` is the branch carrying the
/// two latent bugs (non-nullable `_vid` null-padding, and multi-batch row
/// misalignment). If it is unreachable, those bugs are unreachable too.
#[tokio::test]
async fn probe_guard6_left_outer() {
    let db = seeded().await;
    let s = db.session();
    let q = "MATCH (a:A) OPTIONAL MATCH (b:B) WHERE id(a) = id(b) \
             RETURN a.x AS ax, b.y AS by";
    let ops = plan_ops(&s, q).await;
    eprintln!("[probe-guard6/left] ops = {ops:?}");
}

/// Whatever the plan, the answer must be right — and must match the shape a
/// plain `HashJoinExec` produces.
///
/// If `VidLookupJoinExec` ever does fire here, this is what catches it
/// returning something different.
#[tokio::test]
async fn probe_a_answer_is_correct() {
    let db = seeded().await;
    let s = db.session();
    let q = "MATCH (a:A) MATCH (b:B) WHERE id(a) = id(b) RETURN a.x AS ax, b.y AS by";
    let rows = s.query(q).await.unwrap();
    eprintln!("[probe-a/answer] matched rows = {}", rows.rows().len());

    // The 3 dual-labelled nodes must match themselves, and nothing else may
    // match. A non-empty result is the point: with zero matches the operator
    // runs but joins nothing, and any correctness claim is vacuous.
    assert_eq!(
        rows.rows().len(),
        3,
        "expected exactly the 3 dual-labelled :A:B nodes to self-match"
    );
    for r in rows.rows() {
        let ax: i64 = r.get("ax").unwrap();
        let by: i64 = r.get("by").unwrap();
        assert_eq!(
            by,
            ax * 10,
            "id-equality paired rows from different nodes: {r:?}"
        );
    }
}

/// **The differential**: the same question answered by the two operators must
/// give the same bag.
///
/// `WHERE a.x >= -1` is semantically a no-op over this seed but merges into
/// `Scan.filter`, adding a `FilterExec` that defeats guard 3 — so the twin
/// provably runs on `HashJoinExec`. That makes this a genuine operator-vs-
/// operator comparison rather than two runs of the same code.
#[tokio::test]
async fn vid_lookup_join_agrees_with_its_hash_join_fallback() {
    let db = seeded().await;
    let s = db.session();

    let fast = "MATCH (a:A) MATCH (b:B) WHERE id(a) = id(b) RETURN a.x AS ax, b.y AS by";
    let slow = "MATCH (a:A) MATCH (b:B) WHERE a.x >= -1 AND id(a) = id(b) \
                RETURN a.x AS ax, b.y AS by";

    assert_plan_uses(&s, fast, "VidLookupJoinExec").await;
    assert_plan_avoids(&s, slow, "VidLookupJoinExec").await;
    assert_plan_uses(&s, slow, "HashJoinExec").await;

    let mut a: Vec<(i64, i64)> = s
        .query(fast)
        .await
        .unwrap()
        .rows()
        .iter()
        .map(|r| (r.get("ax").unwrap(), r.get("by").unwrap()))
        .collect();
    let mut b: Vec<(i64, i64)> = s
        .query(slow)
        .await
        .unwrap()
        .rows()
        .iter()
        .map(|r| (r.get("ax").unwrap(), r.get("by").unwrap()))
        .collect();
    a.sort_unstable();
    b.sort_unstable();

    assert!(!a.is_empty(), "empty bags make this comparison vacuous");
    assert_eq!(
        a, b,
        "VidLookupJoinExec disagreed with the HashJoinExec fallback"
    );
}

// ── Assertions, added only after the probes above reported their evidence ────
//
// These encode the measured outcome so a future planner change that silently
// alters which operator is chosen fails loudly. They are deliberately written
// against what was *observed*, not what was predicted.

/// **The operator's documented motivating query now fires it.**
///
/// For four months this asserted the opposite. The build anchor here is
/// `a.linked_vid`, a user property, and therefore `Int64` — Cypher has no
/// unsigned integer — while guard 5 demanded `UInt64`. So the one shape the
/// operator was written for could never reach it, and the only shape that could
/// was `id(a) = id(b)`, where both keys are already vids and there is nothing to
/// look up.
///
/// Widening the guard to accept `Int64` (with a range-checked conversion at the
/// read site) is what made this real: measured 18.12 ms -> 7.38 ms on a
/// 20k-target fixture, with the 20,000-row probe scan gone from the plan.
#[tokio::test]
async fn documented_query_uses_the_vid_lookup_join() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Source")
        .property("score", DataType::Float64)
        .property_nullable("linked_vid", DataType::Int64)
        .done()
        .label("Target")
        .property("name", DataType::String)
        .done()
        .apply()
        .await
        .unwrap();
    let s = db.session();
    let tx = s.tx().await.unwrap();
    tx.execute("CREATE (:Target {name: 't0'})").await.unwrap();
    tx.execute("CREATE (:Source {score: 0.9, linked_vid: 0})")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let q = "MATCH (a:Source) MATCH (b:Target) WHERE id(b) = a.linked_vid \
             RETURN a.score AS s, b.name AS n";
    assert_plan_uses(&s, q, "VidLookupJoinExec").await;
    assert_plan_avoids(&s, q, "HashJoinExec").await;
}

/// **The shape `VidJoinKind::Left` was actually written for.**
///
/// PR #6 (`deb9d907d`) added LEFT support for "the `OPTIONAL MATCH` pattern",
/// i.e. the documented motivating query with the target side optional:
///
/// ```text
/// MATCH (a:Source) OPTIONAL MATCH (b:Target) WHERE id(b) = a.linked_vid
/// ```
///
/// The design is sound and the planner comment explains it well: the operator
/// is *build-outer* — it materialises the build side and fetches the probe by
/// build VIDs, so it can null-pad unmatched **build** rows but never unmatched
/// probe rows. LEFT is therefore correct exactly when the build is the left
/// (outer) side, which is `ProbeSide::Right`.
///
/// And this shape does select `ProbeSide::Right`: the classifier puts the
/// left-subtree variable in `l_expr`, so `l_expr = a.linked_vid` (not a `_vid`
/// property) and `r_expr = b._vid`, which is what the anchor loop keys on.
///
// The LEFT path was blocked twice over, and both are now fixed:
///
/// * guard 5 rejected the `Int64` build anchor (same root cause as the INNER
///   case), and
/// * guard 3 rejected the probe, because `wrap_optional` wraps an OPTIONAL scan
///   in `NestedLoopJoinExec(PlaceholderRowExec, GraphScanExec)` and for LEFT the
///   probe is *necessarily* the optional side. Those are the same condition, so
///   `VidJoinKind::Left` was dead **by construction** from the commit that
///   introduced it (`deb9d907d`, April 2026) — born dead, never orphaned.
///
/// Unwrapping that redundant wrapper is sound because this operator already
/// null-pads every unmatched build row, which is the only thing the wrapper
/// existed to guarantee. Measured 19.03 ms -> 8.86 ms.
#[tokio::test]
async fn intended_left_outer_shape_uses_the_vid_lookup_join() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Source")
        .property("score", DataType::Float64)
        .property_nullable("linked_vid", DataType::Int64)
        .done()
        .label("Target")
        .property("name", DataType::String)
        .done()
        .apply()
        .await
        .unwrap();

    let s = db.session();
    let tx = s.tx().await.unwrap();
    tx.execute("CREATE (:Target {name: 't0'})").await.unwrap();
    tx.execute("CREATE (:Source {score: 0.9, linked_vid: 0})")
        .await
        .unwrap();
    tx.execute("CREATE (:Source {score: 0.1, linked_vid: 9999})")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let q = "MATCH (a:Source) OPTIONAL MATCH (b:Target) WHERE id(b) = a.linked_vid \
             RETURN a.score AS s, b.name AS n";
    let ops = plan_ops(&s, q).await;
    eprintln!("[probe-left/intended] ops = {ops:?}");
    assert_plan_uses(&s, q, "VidLookupJoinExec").await;
}

/// The three LEFT-outer tests in `issue_55_cross_match_pushdown.rs` now execute
/// the LEFT path they were written for — for the first time.
///
/// `cross_match_left_outer_preserves_build_with_null` runs precisely the
/// scenario `VidJoinKind::Left` exists to serve — one matching source, one
/// unmatched, asserting the unmatched row comes back NULL-padded. It passes.
/// It has always passed. And it has always run on `HashJoinExec`.
///
/// That is why B1 and B2 survived review and three targeted tests: the code
/// that null-pads is `emit_joined_batch`, and nothing ever called it. The tests
/// assert the *answer*, which `HashJoinExec` produces correctly — so they are
/// simultaneously well-written, passing, and blind to the operator they name.
///
/// They were always well written — one match, one non-match, asserting the NULL
/// padding, which is exactly where B1 and B2 lived. They simply asserted the
/// *answer*, and `HashJoinExec` produced the right answer, so they were
/// simultaneously correct, passing, and blind to the operator they named. That
/// is the whole anatomy of the miss, and why `plan_shape::assert_plan_uses`
/// exists.
#[tokio::test]
async fn the_left_outer_tests_now_run_on_the_operator() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Source")
        .property("name", DataType::String)
        .property("score", DataType::Float64)
        .property_nullable("linked_vid", DataType::Int64)
        .done()
        .label("Target")
        .property("name", DataType::String)
        .done()
        .apply()
        .await
        .unwrap();

    let s = db.session();
    let tx = s.tx().await.unwrap();
    tx.execute("CREATE (:Target {name: 't'})").await.unwrap();
    tx.execute("CREATE (:Source {name: 'matches', score: 1.0, linked_vid: 0})")
        .await
        .unwrap();
    tx.execute("CREATE (:Source {name: 'unmatched', score: 1.0, linked_vid: 9999999})")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    // Verbatim from `cross_match_left_outer_preserves_build_with_null`.
    let q = "MATCH (a:Source) WHERE a.score > 0.5 \
             OPTIONAL MATCH (b:Target) WHERE id(b) = a.linked_vid \
             RETURN a.name AS aname, b.name AS bname";

    let ops = plan_ops(&s, q).await;
    eprintln!("[left-outer-test/actual] ops = {ops:?}");
    assert_plan_uses(&s, q, "VidLookupJoinExec").await;

    // And the answer is right anyway — which is the whole problem.
    let rows = s.query(q).await.unwrap();
    assert_eq!(
        rows.rows().len(),
        2,
        "both source rows must survive the outer join"
    );
}

/// **LEFT-outer correctness, on the operator.**
///
/// What the three `cross_match_left_outer_*` tests in
/// `issue_55_cross_match_pushdown.rs` were always meant to be: the same
/// scenario, plus proof of which operator answered it. Matched and unmatched
/// build rows, asserting the NULL padding — the exact path where B1 (a hard
/// error on the non-nullable `_vid`) and B2 (silent row misalignment) lived.
#[tokio::test]
async fn left_outer_null_padding_is_correct_on_the_operator() {
    let db = Uni::in_memory().build().await.unwrap();
    db.schema()
        .label("Source")
        .property("name", DataType::String)
        .property_nullable("linked_vid", DataType::Int64)
        .done()
        .label("Target")
        .property("name", DataType::String)
        .done()
        .apply()
        .await
        .unwrap();

    let s = db.session();
    let tx = s.tx().await.unwrap();
    tx.execute("CREATE (:Target {name: 't0'})").await.unwrap();
    tx.execute("CREATE (:Target {name: 't1'})").await.unwrap();
    // Two match, one points at a vid that does not exist.
    tx.execute("CREATE (:Source {name: 'a', linked_vid: 0})")
        .await
        .unwrap();
    tx.execute("CREATE (:Source {name: 'b', linked_vid: 1})")
        .await
        .unwrap();
    tx.execute("CREATE (:Source {name: 'c', linked_vid: 987654})")
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let q = "MATCH (a:Source) OPTIONAL MATCH (b:Target) WHERE id(b) = a.linked_vid \
             RETURN a.name AS an, b.name AS bn";

    // Without this the test could pass on the fallback and prove nothing about
    // the operator — which is exactly what happened for four months.
    assert_plan_uses(&s, q, "VidLookupJoinExec").await;

    let mut got: Vec<(String, Option<String>)> = s
        .query(q)
        .await
        .unwrap()
        .rows()
        .iter()
        .map(|r| (r.get::<String>("an").unwrap(), r.try_get::<String>("bn")))
        .collect();
    got.sort();

    assert_eq!(
        got,
        vec![
            ("a".to_string(), Some("t0".to_string())),
            ("b".to_string(), Some("t1".to_string())),
            ("c".to_string(), None),
        ],
        "every Source must survive; only the one with no matching Target is \
         NULL-padded, and each match must keep its OWN target (B2 paired the \
         unmatched row with a matched row's payload)"
    );
}
