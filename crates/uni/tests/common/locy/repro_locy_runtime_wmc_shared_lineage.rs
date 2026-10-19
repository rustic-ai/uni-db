// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end runtime coverage for uni-query finding [13]
//! (`crates/uni-query/src/query/df_graph/locy_fixpoint.rs:2537`).
//!
//! # The defect
//!
//! When `exact_probability` is enabled and a probabilistic (`MNOR`/`MPROD`) rule
//! produces derived facts that share lineage, `apply_exact_wmc` replaces each
//! shared KEY group with a single weighted-model-count row. It reads the
//! probability column and the group KEY using raw YIELD-schema positions
//! (`prob_fold.input_col_index`, `rule.key_column_indices`) and hashes rows over
//! the full yield-schema index set — but it applies those positions against the
//! `pre_fold_facts` batches. The surrounding FOLD path (`locy_fixpoint.rs:5192`)
//! deliberately *reconciles* key indices by NAME because "the actual batch may
//! have different column ordering after schema reconciliation during fixpoint
//! iteration"; `detect_shared_lineage` (`:2222`) and `apply_exact_wmc` (`:2555`)
//! do NOT reconcile — they use raw yield positions. When the pre-fold batch
//! column order diverges from the yield schema, the KEY grouping and the PROB
//! column read/overwrite land on the wrong columns.
//!
//! # Reproducibility through the public API
//!
//! The canonical shared-proof shape is the reachability diamond
//!   s -0.3-> m -0.5-> t ,  s -0.7-> t
//! evaluated by a recursive `reach` rule. With `exact_probability` this fires
//! `detect_shared_lineage` (a `SharedProbabilisticDependency` warning is
//! emitted) and runs `apply_exact_wmc`. The passing test below asserts BOTH
//! facts, proving the exact-WMC path executes end-to-end. Its computed
//! probability for `reach(s,t)` is `0.79` — the value the Locy TCK
//! (`monotonic/ExactProbability.feature`, scenario "BDD mode produces numeric
//! probability for MNOR diamond") codifies as correct for exact mode
//! (`MNOR(0.7, 0.3)` over the two fold-input edges).
//!
//! In every shape reachable from surface Locy, the pre-fold batch column order
//! coincides with the yield schema, so the positional reads were
//! *coincidentally* correct and the output matches the codified expectation.
//!
//! # Fixed
//!
//! `detect_shared_lineage`, `apply_exact_wmc` and both provenance-recording
//! paths now resolve the yield and KEY columns by name
//! (`locy_fixpoint.rs::yield_positions_in`) and fail closed on a missing one.
//! The precondition (a pre-fold batch whose column order differs from the yield
//! schema) is built directly in the unit tests
//! `locy_fixpoint::tests::exact_wmc_resolves_key_and_hash_columns_by_name` and
//! `exact_wmc_fails_closed_on_missing_yield_column`, since the public surface
//! cannot produce it.

use anyhow::Result;
use uni_db::locy::LocyConfig;
use uni_db::{DataType, Uni, Value};

const REACH_MNOR: &str = "CREATE RULE reach AS \
     MATCH (a:N)-[e:E]->(b:N) \
     FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob \n\
     CREATE RULE reach AS \
     MATCH (a:N)-[e:E]->(mid:N) WHERE mid IS reach TO b \
     FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob \n\
     QUERY reach RETURN a.name AS src, b.name AS dst, prob";

/// Exact-probability (BDD/WMC) path: `exact_probability = true` promotes the
/// default AddMultProb semiring to BddExact and runs `apply_exact_wmc` on
/// shared-lineage groups.
fn exact_config() -> LocyConfig {
    LocyConfig {
        max_iterations: 1000,
        exact_probability: true,
        ..Default::default()
    }
}

async fn seed_diamond(db: &Uni) -> Result<()> {
    db.schema()
        .label("N")
        .property("name", DataType::String)
        .apply()
        .await?;
    db.schema()
        .edge_type("E", &["N"], &["N"])
        .property("p", DataType::Float64)
        .apply()
        .await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {name: 's'}), (:N {name: 'm'}), (:N {name: 't'})")
        .await?;
    tx.execute("MATCH (s:N {name:'s'}), (t:N {name:'t'}) CREATE (s)-[:E {p: 0.7}]->(t)")
        .await?;
    tx.execute("MATCH (s:N {name:'s'}), (m:N {name:'m'}) CREATE (s)-[:E {p: 0.3}]->(m)")
        .await?;
    tx.execute("MATCH (m:N {name:'m'}), (t:N {name:'t'}) CREATE (m)-[:E {p: 0.5}]->(t)")
        .await?;
    tx.commit().await?;
    Ok(())
}

fn prob_of(result: &uni_locy::LocyResult, src: &str, dst: &str) -> Option<f64> {
    let rows = result.rows()?;
    rows.iter()
        .find(|r| {
            r.get("src").and_then(Value::as_str) == Some(src)
                && r.get("dst").and_then(Value::as_str) == Some(dst)
        })
        .and_then(|r| r.get("prob"))
        .and_then(Value::as_f64)
}

fn has_warning(result: &uni_locy::LocyResult, code_substr: &str) -> bool {
    result
        .warnings()
        .iter()
        .any(|w| format!("{:?}", w.code).contains(code_substr))
}

/// Proves the exact-WMC path (`detect_shared_lineage` + `apply_exact_wmc`, which
/// contain the [13] positional-index code) runs end-to-end: the shared-proof
/// diamond emits a `SharedProbabilisticDependency` warning and produces the
/// TCK-codified exact-mode probability `0.79` for `reach(s,t)`.
#[tokio::test]
async fn wmc_shared_lineage_exact_path_engages() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed_diamond(&db).await?;

    let result = db
        .session()
        .locy_with(REACH_MNOR)
        .with_config(exact_config())
        .run()
        .await?;

    let p_st = prob_of(&result, "s", "t");
    let shared = has_warning(&result, "SharedProbabilisticDependency");
    println!("[13] exact-WMC reach(s,t) prob -> {p_st:?}, shared-dependency warning = {shared}");
    assert!(
        shared,
        "the shared-proof diamond must emit SharedProbabilisticDependency, proving detect_shared_lineage/apply_exact_wmc ran"
    );
    let p_st = p_st.expect("reach(s,t) should exist");
    assert!(
        (p_st - 0.79).abs() < 1e-9,
        "exact-mode MNOR diamond is codified as 0.79 by the TCK; the pre-fold column order coincides with the yield schema so the positional read at :2537 is coincidentally correct; got {p_st}"
    );
    Ok(())
}

/// The diamond with the FOLD output renamed in YIELD (`prob AS pr`).
///
/// The pre-fold facts carry the PROB column under the fold variable (`prob`),
/// but exact WMC looked it up under the YIELD alias (`pr`). No column matched,
/// so the exact probability was never written: the shared group collapsed to
/// its first row and kept that row's own weight.
#[tokio::test]
async fn wmc_shared_lineage_exact_path_with_renamed_fold_output() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    seed_diamond(&db).await?;

    let program = "CREATE RULE reach AS \
         MATCH (a:N)-[e:E]->(b:N) \
         FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob AS pr \n\
         CREATE RULE reach AS \
         MATCH (a:N)-[e:E]->(mid:N) WHERE mid IS reach TO b \
         FOLD prob = MNOR(e.p) YIELD KEY a, KEY b, prob AS pr \n\
         QUERY reach RETURN a.name AS src, b.name AS dst, pr AS prob";
    let result = db
        .session()
        .locy_with(program)
        .with_config(exact_config())
        .run()
        .await?;

    assert!(
        has_warning(&result, "SharedProbabilisticDependency"),
        "the renamed diamond must still take the exact-WMC path"
    );
    let p_st = prob_of(&result, "s", "t").expect("reach(s,t) should exist");
    assert!(
        (p_st - 0.79).abs() < 1e-9,
        "renaming the FOLD output must not change the exact-mode answer 0.79; got {p_st}"
    );
    Ok(())
}

// The ignored `wmc_shared_lineage_positional_misgroup_would_diverge` that
// stood here asserted `0.745` (full inclusion-exclusion `X ∨ (Y∧Z)`, counting
// the probability of the `IS reach` dependency) for this diamond. That is not
// what the positional-read defect would produce, and it contradicts the
// exact-mode `0.79` the TCK codifies (`MNOR` folds the edge weights only; the
// IS-reference contributes no probability), so it could never pass. The
// defect itself is now covered by the unit tests named in the module docs.
