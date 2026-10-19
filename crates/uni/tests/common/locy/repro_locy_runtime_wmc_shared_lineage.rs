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
//! The canonical shape is the reachability diamond
//!   s -0.3-> m -0.5-> t ,  s -0.7-> t
//! evaluated by a recursive `reach` rule with `exact_probability`. A positive
//! `IS` reference to a PROB rule is a probabilistic condition, so `reach(s,t)`
//! is the possible-worlds probability of `X ∨ (Y ∧ Z)`:
//! 1 − (1 − 0.7)(1 − 0.3 · 0.5) = 0.745. (It was 0.79 = P(X ∨ Y) while the
//! reference was a Boolean semi-join, which dropped the conjunct Z.) The two
//! proofs share no edge, so no `SharedProbabilisticDependency` warning is
//! raised; `locy_prob_is_ref_semantics` covers a genuinely shared case.
//!
//! # Fixed
//!
//! Exact inference resolves the KEY, PROB and lineage columns by name and
//! fails closed on a missing one (`locy_lineage::tests`), since the public
//! surface cannot produce a pre-fold batch whose column order differs from
//! the yield schema.

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

/// The exact path runs end-to-end on the diamond and gives the
/// possible-worlds probability `0.745` for `reach(s,t)`, with no
/// shared-lineage warning: the two proofs share no edge.
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
    assert!(
        !shared,
        "the diamond's two proofs share no edge, so no shared-lineage warning"
    );
    let p_st = p_st.expect("reach(s,t) should exist");
    assert!(
        (p_st - 0.745).abs() < 1e-9,
        "exact-mode MNOR diamond: 1 - (1 - 0.7)(1 - 0.3 * 0.5) = 0.745; got {p_st}"
    );
    Ok(())
}

/// The diamond with the FOLD output renamed in YIELD (`prob AS pr`).
///
/// The pre-fold facts carry the PROB column under the fold variable (`prob`),
/// not the YIELD alias (`pr`). Exact inference once looked it up under the
/// alias, matched no column, and the shared group kept its first row's own
/// weight.
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

    let p_st = prob_of(&result, "s", "t").expect("reach(s,t) should exist");
    assert!(
        (p_st - 0.745).abs() < 1e-9,
        "renaming the FOLD output must not change the exact-mode answer 0.745; got {p_st}"
    );
    Ok(())
}

// The ignored `wmc_shared_lineage_positional_misgroup_would_diverge` that
// stood here asserted `0.745` for this diamond. That is the possible-worlds
// value, which `wmc_shared_lineage_exact_path_engages` now asserts; the
// positional-read defect it was named for is covered by the unit tests named
// in the module docs.
