// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Scaling guard for the issue #162 per-iteration folded view.
//!
//! `recompute_folded` re-folds the rule's *entire* accumulated fact set once
//! per iteration — O(facts), not O(delta). On a fixed-depth graph that is a
//! constant factor (the depth); on a deep chain, iterations grow with depth and
//! the total fold work grows with depth × facts.
//!
//! This pins the shape so a future change cannot quietly make it worse, using
//! the `profile()`-based assertion style of
//! `bugs::issue_131_locy_iter_cross_join` rather than a Criterion bench (the
//! bench tier is nightly, artifact-only, and not a regression gate).

use std::time::Duration;

use anyhow::Result;
use uni_db::Uni;
use uni_db::locy::LocyConfig;

const PROGRAM: &str = "\
CREATE RULE avail AS \
  MATCH (p:P)<-[s:SUPPLIES]-(v:V) \
  FOLD a = MNOR(s.r) \
  YIELD KEY p, a PROB \n\
CREATE RULE assembly AS \
  MATCH (p:P)-[:CONTAINS]->(:P) \
  YIELD KEY p \n\
CREATE RULE build AS \
  MATCH (p:P) \
  WHERE p IS avail, p IS NOT assembly \
  YIELD KEY p, a AS b PROB \n\
CREATE RULE build AS \
  MATCH (p:P)-[:CONTAINS]->(c:P) \
  WHERE c IS build \
  FOLD b = MPROD(b) \
  YIELD KEY p, b PROB";

/// A chain of `depth` assemblies, `n0 → n1 → … → n{depth-1}`, whose last link
/// contains two supplied leaves. Depth drives the iteration count; the two
/// leaves keep the fold non-trivial at the bottom.
async fn build_chain(depth: usize) -> Result<Uni> {
    let db = Uni::in_memory().build().await?;
    let mut cypher = String::from("CREATE (v:V {n: 'v'})");
    for i in 0..depth {
        cypher.push_str(&format!(", (n{i}:P {{s: 'n{i}'}})"));
    }
    cypher.push_str(", (la:P {s: 'la'}), (lb:P {s: 'lb'})");
    for i in 0..depth - 1 {
        cypher.push_str(&format!(", (n{i})-[:CONTAINS]->(n{})", i + 1));
    }
    let last = depth - 1;
    cypher.push_str(&format!(
        ", (n{last})-[:CONTAINS]->(la), (n{last})-[:CONTAINS]->(lb), \
           (v)-[:SUPPLIES {{r: 0.9}}]->(la), (v)-[:SUPPLIES {{r: 0.9}}]->(lb)"
    ));

    let tx = db.session().tx().await?;
    tx.execute(&cypher).await?;
    tx.commit().await?;
    Ok(db)
}

/// Returns (fold work, iterations, `build` fact count).
///
/// "Fold work" is the number of rows `recompute_folded` passes over across the
/// whole run: at each iteration it folds every accumulated fact, so the total
/// is `Σᵢ (facts after iteration i)`. That is reconstructed from the profile's
/// per-iteration `delta_facts`, since the per-iteration `FoldExec` is built
/// outside the profiled clause-body plans.
async fn measure(depth: usize) -> Result<(usize, usize, usize)> {
    let db = build_chain(depth).await?;
    let (result, profile) = db
        .session()
        .locy_with(PROGRAM)
        .with_config(LocyConfig {
            max_iterations: 1000,
            timeout: Some(Duration::from_secs(120)),
            ..Default::default()
        })
        .profile()
        .await?;

    let mut fold_work = 0usize;
    let mut iterations = 0usize;
    for stratum in &profile.profile.strata {
        for rule in &stratum.rules {
            if rule.name != "build" {
                continue;
            }
            let mut running = 0usize;
            for it in &rule.iterations {
                running += it.delta_facts;
                fold_work += running;
                iterations += 1;
            }
        }
    }
    let facts = result.derived_facts("build").map(|v| v.len()).unwrap_or(0);
    Ok((fold_work, iterations, facts))
}

#[tokio::test]
async fn issue_162_fold_view_work_stays_within_its_documented_shape() -> Result<()> {
    let (w10, i10, f10) = measure(10).await?;
    let (w20, i20, f20) = measure(20).await?;
    let (w40, i40, f40) = measure(40).await?;

    eprintln!("depth=10  fold_work={w10:>7}  iterations={i10:>3}  facts={f10}");
    eprintln!("depth=20  fold_work={w20:>7}  iterations={i20:>3}  facts={f20}");
    eprintln!("depth=40  fold_work={w40:>7}  iterations={i40:>3}  facts={f40}");

    // Correctness first — a cheap wrong answer is not a win. Every assembly
    // rolls up MPROD over the two 0.9 leaves.
    assert_eq!(f10, 10 + 2, "depth=10 should derive one fact per part");
    assert_eq!(f20, 20 + 2, "depth=20 should derive one fact per part");
    assert_eq!(f40, 40 + 2, "depth=40 should derive one fact per part");

    // Iterations track depth and facts grow linearly with it, so total fold
    // work is Σᵢ factsᵢ ≈ D²/2 — quadratic in depth, and the documented cost of
    // the O(facts) snapshot. A 4× depth increase therefore approaches 16×
    // asymptotically (measured 9.85× at these sizes, where the constant offset
    // still dominates). The guard sits 1.5× above the asymptote: enough
    // headroom not to be flaky, tight enough that a regression to cubic — an
    // accidental re-fold per *clause* rather than per iteration, or a lost
    // dedup letting facts grow super-linearly — trips it.
    let quadratic = (40.0 / 10.0_f64).powi(2); // 16×
    let observed = w40 as f64 / (w10.max(1)) as f64;
    assert!(
        observed < quadratic * 1.5,
        "fold work grew {observed:.1}× for a 4× depth increase, past the \
         {:.0}× guard — the per-iteration snapshot is no longer O(facts) \
         (w10={w10}, w40={w40})",
        quadratic * 1.5
    );
    Ok(())
}
