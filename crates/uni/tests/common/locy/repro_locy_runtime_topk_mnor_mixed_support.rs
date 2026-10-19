// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end runtime coverage for uni-query finding [24]
//! (`crates/uni-query/src/query/df_graph/locy_fold.rs:720`).
//!
//! # The defect
//!
//! Under the `TopKProofs` semiring an `MNOR` fold builds a `Proof` per pre-fold
//! row (`topk_dnf_disjunction`) and computes the group probability from a
//! `DependencyDnf`. A row's proof carries IS-ref lineage `base_rvs` only when
//! its body-row hash is present in `body_support_map`; a row with no IS-ref
//! support contributes a proof with an EMPTY `base_rvs` clause. The defect: when
//! a group has at least one supported row (`base_weights` non-empty) so the DNF
//! branch is taken, an unsupported row's EMPTY clause is treated by
//! `DependencyDnf::weight` as trivially TRUE (probability `1.0`), which would
//! drive the whole disjunction to `1.0`, discarding every row's real weight.
//!
//! # Reproducibility through the public API
//!
//! Reaching the buggy branch requires a SINGLE KEY group that mixes a supported
//! and an unsupported row. That precondition turns out to be unreachable through
//! the public Locy FOLD surface, and the reason is structural:
//!
//! `body_support_map` is populated by `collect_is_ref_inputs_for_body_row`,
//! which resolves a row's IS-ref support by looking up the value of the IS-ref
//! subject's *body column* against the referenced rule
//! (`collect_is_ref_inputs`, `locy_fixpoint.rs:2039`). For that column to exist
//! in the pre-fold facts it must be a YIELD column, and for a FOLD it is
//! effectively a KEY column — which is **group-invariant**. So within one KEY
//! group every row resolves support identically: the group is either ALL
//! supported or ALL unsupported, never mixed. Empirically confirmed with three
//! distinct shapes (KEY `i` only → all-unsupported `0.88`; KEY `(h,i)` shared →
//! all-supported DNF collapse `0.7`; subject as a value alias `src` → support
//! unresolved `0.88`).
//!
//! What the passing test below DOES exercise is the `topk_dnf_disjunction` DNF
//! branch itself (the function that houses the defect): a shared-base group
//! collapses to `0.7` — provably distinct from the `0.88` independence value —
//! proving the proof/DNF machinery runs end-to-end.
//!
//! # Fixed
//!
//! `topk_dnf_disjunction` keeps unsupported rows out of the DNF and folds them
//! in as independent noisy-OR factors. A mixed group, which the public surface
//! cannot produce, is built directly in the unit test
//! `locy_fold::tests::topk_mixed_support_group_folds_unsupported_rows_independently`.

use anyhow::Result;
use uni_db::locy::LocyConfig;
use uni_db::{DataType, Uni, Value};
use uni_locy::SemiringKind;

/// A `risk` rule keyed by `(h, i)` where the IS-ref subject `h` is a YIELD KEY
/// (so its base-fact support IS tracked). Two edges from the same hub `h` into
/// item `i` both resolve support `{hub_score(h)}`, so the DNF collapses their
/// shared base — a value that differs from the independence noisy-OR, proving
/// the TopKProofs proof/DNF path (the [24] code) executes.
const RISK_SHARED: &str = "CREATE RULE hub_score AS \
     MATCH (h:Hub) YIELD KEY h \n\
     CREATE RULE risk AS \
     MATCH (h:Hub)-[e:CAUSE]->(i:Item) WHERE h IS hub_score \
     FOLD p = MNOR(e.prob) YIELD KEY h, KEY i, p \n\
     CREATE RULE risk AS \
     MATCH (h:Hub)-[e:PLAIN]->(i:Item) WHERE h IS hub_score \
     FOLD p = MNOR(e.prob) YIELD KEY h, KEY i, p \n\
     QUERY risk RETURN i.name AS item, p";

/// TopKProofs semiring threads `topk_k = Some(k)` into the fold and engages the
/// per-row proof / DNF math (`topk_dnf_disjunction`).
fn topk_config() -> LocyConfig {
    LocyConfig {
        max_iterations: 1000,
        semiring: SemiringKind::TopKProofs { k: 8 },
        ..Default::default()
    }
}

/// Seed a Hub with two edges into Item `i1`:
///   h -CAUSE 0.7-> i1  (IS-ref supported via hub_score)
///   h -PLAIN 0.6-> i1  (IS-ref supported via hub_score — same base fact)
async fn seed(db: &Uni) -> Result<()> {
    db.schema()
        .label("Hub")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .label("Item")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .edge_type("CAUSE", &["Hub"], &["Item"])
        .property("prob", DataType::Float64)
        .apply()
        .await?;
    db.schema()
        .edge_type("PLAIN", &["Hub"], &["Item"])
        .property("prob", DataType::Float64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Hub {name: 'h'}), (:Item {name: 'i1'})")
        .await?;
    tx.execute(
        "MATCH (h:Hub {name:'h'}), (i:Item {name:'i1'}) CREATE (h)-[:CAUSE {prob: 0.7}]->(i)",
    )
    .await?;
    tx.execute(
        "MATCH (h:Hub {name:'h'}), (i:Item {name:'i1'}) CREATE (h)-[:PLAIN {prob: 0.6}]->(i)",
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

fn prob_of(result: &uni_locy::LocyResult, item: &str) -> Option<f64> {
    let rows = result.rows()?;
    rows.iter()
        .find(|r| r.get("item").and_then(Value::as_str) == Some(item))
        .and_then(|r| r.get("p"))
        .and_then(Value::as_f64)
}

/// Proves the TopKProofs DNF branch (the function that contains the [24] defect)
/// runs end-to-end: a shared-base MNOR group collapses via DNF inclusion-
/// exclusion to `0.7`, which is provably NOT the `0.88` independence noisy-OR of
/// its two edge weights — so the proof/DNF machinery is genuinely engaged.
#[tokio::test]
async fn topk_mnor_shared_base_dnf_branch_engages() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed(&db).await?;

    let result = db
        .session()
        .locy_with(RISK_SHARED)
        .with_config(topk_config())
        .run()
        .await?;

    let p = prob_of(&result, "i1");
    println!(
        "[24] TopKProofs MNOR shared-base risk(i1) prob -> {p:?} (independence would be 0.88)"
    );
    assert_eq!(
        p,
        Some(0.7),
        "shared-base DNF inclusion-exclusion should collapse the two proofs onto their common base (0.7), \
         proving topk_dnf_disjunction runs; the independence value 0.88 would mean the DNF branch was skipped"
    );
    Ok(())
}

// The ignored `topk_mnor_mixed_support_collapses_to_one` that stood here
// asserted the DEFECT's value (`1.0`) on a group that is not mixed, so it could
// pass neither before nor after the fix. The mixed-group behavior is covered by
// the unit test named in the module docs.
