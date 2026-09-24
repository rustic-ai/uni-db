// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Regression tests for architecture review finding §2.2 (non-linear
//! recursion / multiple positive IS-refs per clause), fixed 2026-06-10.
//!
//! Two distinct bugs used to make any clause with two positive IS-refs
//! silently derive zero rows:
//!
//! 1. **Column collision:** each positive IS-ref cross-joins a
//!    `LocyDerivedScan` whose columns keep the target rule's yield names,
//!    so the second ref's unqualified join predicate resolved against the
//!    FIRST scan's columns — contradictory predicates, empty result, no
//!    error. Fixed by aliasing the second and later scans' columns with a
//!    per-occurrence `__isref{n}_` prefix (`locy_planner.rs` Step 3) and
//!    re-stamping batches with the exec's schema in `DerivedScanExec`.
//!    Chained refs (`a IS r TO mid, mid IS r TO z`) additionally needed
//!    `TO`-bound targets registered as node variables for later subjects.
//! 2. **Semi-naive incompleteness:** self-ref scans received only the
//!    latest delta, so non-linear recursion joined Δ×Δ and missed Δ×F_old
//!    (8/10 facts on the 5-chain). Fixed by `FixpointRulePlan::non_linear`:
//!    rules with a clause holding ≥2 positive same-stratum IS-refs get full
//!    facts on their self-ref scans (naive evaluation; dedup keeps it
//!    convergent).
//!
//! The tests compute the transitive closure of the 5-chain A→B→C→D→E,
//! which has exactly 10 reachable pairs. The linear formulation is the
//! ground truth; the non-linear formulation must produce the same relation.

use std::time::Duration;

use anyhow::Result;
use uni_db::Uni;
use uni_db::locy::LocyConfig;

fn default_config() -> LocyConfig {
    LocyConfig {
        max_iterations: 1000,
        timeout: Some(Duration::from_secs(60)),
        ..Default::default()
    }
}

/// Create the 5-node chain A→B→C→D→E.
async fn build_chain(db: &Uni) -> Result<()> {
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:N {name: 'A'})-[:E]->(:N {name: 'B'})-[:E]->(:N {name: 'C'})\
         -[:E]->(:N {name: 'D'})-[:E]->(:N {name: 'E'})",
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Ground truth: linear transitive closure (single self-ref per clause)
/// derives all 10 pairs of the 5-chain. Validates the harness and the
/// expected count used by the non-linear reproducer below.
#[tokio::test]
async fn linear_tc_ground_truth() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    build_chain(&db).await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE reachable AS \
             MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b \n\
             CREATE RULE reachable AS \
             MATCH (a:N)-[:E]->(mid:N) WHERE mid IS reachable TO b \
             YIELD KEY a, b",
        )
        .with_config(default_config())
        .run()
        .await?;

    let reachable = result
        .derived
        .get("reachable")
        .expect("rule 'reachable' missing");
    assert_eq!(
        reachable.len(),
        10,
        "linear TC on a 5-chain must derive 10 pairs, got {}: {:?}",
        reachable.len(),
        reachable
    );
    Ok(())
}

/// Chained IS-refs across two *different* rules (`a IS hop TO mid,
/// mid IS hop TO b`): covers the column-aliasing fix plus the chained case
/// where the second ref's subject is bound by the first ref's `TO` target
/// (requires the target to count as a node variable for later predicates).
#[tokio::test]
async fn chained_is_refs_cross_rule_probe() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    build_chain(&db).await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE hop AS \
             MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b \n\
             CREATE RULE two_hop AS \
             MATCH (a:N) WHERE a IS hop TO mid, mid IS hop TO b \
             YIELD KEY a, b",
        )
        .with_config(default_config())
        .run()
        .await?;

    let two_hop = result
        .derived
        .get("two_hop")
        .expect("rule 'two_hop' missing");
    assert_eq!(
        two_hop.len(),
        3,
        "two chained cross-rule IS refs must derive the 3 two-hop pairs \
         (A→C, B→D, C→E), got {} facts: {:?}",
        two_hop.len(),
        two_hop
    );
    Ok(())
}

/// Control probe: a SINGLE cross-rule IS-ref in the same clause shape
/// (mirrors the linear-recursion pattern that passes). Expected to work;
/// anchors the finding that exactly the second IS-ref breaks the clause.
#[tokio::test]
async fn single_is_ref_control_probe() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    build_chain(&db).await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE hop AS \
             MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b \n\
             CREATE RULE two_hop AS \
             MATCH (a:N)-[:E]->(mid:N) WHERE mid IS hop TO b \
             YIELD KEY a, b",
        )
        .with_config(default_config())
        .run()
        .await?;

    let two_hop = result
        .derived
        .get("two_hop")
        .expect("rule 'two_hop' missing");
    assert_eq!(
        two_hop.len(),
        3,
        "single cross-rule IS ref must derive the 3 two-hop pairs, \
         got {} facts: {:?}",
        two_hop.len(),
        two_hop
    );
    Ok(())
}

/// Two IS-refs in one clause where BOTH subjects are MATCH-bound (no
/// IS-bound variable feeds another IS-ref). On the chain, `a IS hop TO x`
/// (x = the node after a) and `mid IS hop TO b` (b = the node after mid)
/// over edge (a)-[:E]->(mid) yields the 3 two-hop pairs. Covers the
/// column-aliasing fix in isolation.
#[tokio::test]
async fn two_is_refs_match_bound_subjects_probe() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    build_chain(&db).await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE hop AS \
             MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b \n\
             CREATE RULE two_hop AS \
             MATCH (a:N)-[:E]->(mid:N) WHERE a IS hop TO x, mid IS hop TO b \
             YIELD KEY a, b",
        )
        .with_config(default_config())
        .run()
        .await?;

    let two_hop = result
        .derived
        .get("two_hop")
        .expect("rule 'two_hop' missing");
    assert_eq!(
        two_hop.len(),
        3,
        "two MATCH-bound-subject IS refs must derive the 3 two-hop pairs, \
         got {} facts: {:?}",
        two_hop.len(),
        two_hop
    );
    Ok(())
}

/// Non-linear transitive closure (two self-refs in one clause,
/// `reachable(a,b) :- reachable(a,mid), reachable(mid,b)`) must derive the
/// same 10 pairs as the linear formulation. Exercises both fixes: column
/// aliasing (without it: 4 facts — recursive clause inert) and full-facts
/// injection for non-linear rules (without it: 8 facts — the length-3
/// paths A→D and B→E need joining a new fact with an older one).
#[tokio::test]
async fn nonlinear_tc_semi_naive_completeness() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    build_chain(&db).await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE reachable AS \
             MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b \n\
             CREATE RULE reachable AS \
             MATCH (a:N) WHERE a IS reachable TO mid, mid IS reachable TO b \
             YIELD KEY a, b",
        )
        .with_config(default_config())
        .run()
        .await?;

    let reachable = result
        .derived
        .get("reachable")
        .expect("rule 'reachable' missing");
    assert_eq!(
        reachable.len(),
        10,
        "non-linear TC on a 5-chain must derive the same 10 pairs as the \
         linear formulation, got {} facts: {:?}",
        reachable.len(),
        reachable
    );
    Ok(())
}
