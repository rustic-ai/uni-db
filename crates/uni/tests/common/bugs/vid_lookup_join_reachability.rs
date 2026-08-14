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

/// **Probe A, guard 5 — the decisive one.**
///
/// This is the operator's own documented motivating query
/// (`vid_lookup_join.rs:6-18`): join a stored vid property against `id(b)`.
/// `linked_vid` is `Int64`, as it must be — Cypher has no unsigned integer —
/// and the build anchor must be `UInt64`. If this reports `HashJoinExec`, the
/// operator cannot serve the purpose it was written for.
#[tokio::test]
async fn documented_motivating_query_cannot_fire() {
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

/// Pins that the documented motivating query takes the fallback.
///
/// If this ever fails, guard 5 changed — someone introduced a `UInt64`-typed
/// property path — and the operator's whole reachability analysis must be redone.
#[tokio::test]
async fn documented_query_takes_the_hash_join_fallback() {
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
    assert_plan_avoids(&s, q, "VidLookupJoinExec").await;
    assert_plan_uses(&s, q, "HashJoinExec").await;
}
