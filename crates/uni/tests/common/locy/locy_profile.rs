// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for the Locy `profile()` API.
//!
//! `profile()` is the Locy analog of Cypher's `query.profile()`: it runs the
//! program and returns a structured execution profile — a stratum → rule →
//! fixpoint-iteration tree, each iteration carrying the same per-operator
//! metrics (`OperatorStats`) Cypher reports.

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

/// A recursive program (transitive closure) profiles into a recursive stratum
/// with multiple fixpoint iterations, each carrying a non-empty operator tree.
#[tokio::test]
async fn profile_recursive_transitive_closure() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {name: 'A'})-[:E]->(:N {name: 'B'})-[:E]->(:N {name: 'C'})")
        .await?;
    tx.commit().await?;

    let (result, profile) = db
        .session()
        .locy_with(
            "CREATE RULE reachable AS \
             MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b \n\
             CREATE RULE reachable AS \
             MATCH (a:N)-[:E]->(mid:N) WHERE mid IS reachable TO b \
             YIELD KEY a, b",
        )
        .with_config(default_config())
        .profile()
        .await?;

    // The result is identical to `run()`: A→B, B→C, A→C.
    let reachable = result
        .derived
        .get("reachable")
        .expect("rule 'reachable' missing");
    assert_eq!(reachable.len(), 3, "expected 3 reachable facts");

    // evaluation_time is populated (it always is, profiling or not).
    assert!(
        result.stats.evaluation_time > Duration::ZERO,
        "evaluation_time should be populated"
    );

    // The profile has at least one stratum, and a recursive one.
    assert!(!profile.profile.strata.is_empty(), "no strata in profile");
    let rec = profile
        .profile
        .strata
        .iter()
        .find(|s| s.recursive)
        .expect("expected a recursive stratum");

    // Transitive closure over A→B→C needs more than one fixpoint pass.
    assert!(
        rec.iterations >= 2,
        "recursive stratum should run multiple iterations, got {}",
        rec.iterations
    );
    assert!(!rec.rules.is_empty(), "recursive stratum has no rules");

    // Every recorded iteration carries the clause-body operator tree (the same
    // per-operator metrics Cypher's profile produces).
    let rule = &rec.rules[0];
    assert_eq!(
        rule.iterations.len(),
        rec.iterations,
        "per-rule iteration count should match the stratum's"
    );
    let total_ops: usize = rule.iterations.iter().map(|it| it.operators.len()).sum();
    assert!(
        total_ops > 0,
        "expected per-operator metrics in the recursive iterations"
    );
    // At least one operator should be a real physical operator with rows.
    let saw_rows = rule
        .iterations
        .iter()
        .flat_map(|it| it.operators.iter())
        .any(|op| !op.operator.is_empty());
    assert!(saw_rows, "operator entries should carry an operator name");

    // Display renders without panicking and mentions the header.
    let rendered = format!("{profile}");
    assert!(
        rendered.contains("Locy Profile"),
        "Display output missing header: {rendered}"
    );

    Ok(())
}

/// A non-recursive program profiles into a single-iteration stratum.
#[tokio::test]
async fn profile_non_recursive_single_iteration() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {name: 'A'})-[:E]->(:N {name: 'B'})")
        .await?;
    tx.commit().await?;

    let (result, profile) = db
        .session()
        .locy_with("CREATE RULE edge AS MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b")
        .with_config(default_config())
        .profile()
        .await?;

    assert_eq!(result.derived.get("edge").map(|r| r.len()), Some(1));

    let stratum = profile
        .profile
        .strata
        .iter()
        .find(|s| s.rules.iter().any(|r| r.name == "edge"))
        .expect("stratum for rule 'edge' missing");
    assert!(!stratum.recursive, "single-pass rule is non-recursive");
    assert_eq!(stratum.iterations, 1, "non-recursive stratum = 1 iteration");
    let rule = stratum
        .rules
        .iter()
        .find(|r| r.name == "edge")
        .expect("rule 'edge' missing");
    assert_eq!(rule.iterations.len(), 1);
    assert_eq!(rule.facts, 1, "edge rule derived one fact");

    Ok(())
}
