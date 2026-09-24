// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end integration tests for the **native** Locy execution path.
//!
//! These tests exercise the full pipeline:
//! `compile → LocyPlanBuilder → HybridPhysicalPlanner → LocyProgramExec → DerivedStore → LocyResult`
//!
//! Separate from `locy_integration.rs` (which tests the old orchestrator path).

use std::time::Duration;

use anyhow::Result;
use uni_db::Uni;
use uni_db::locy::LocyConfig;

// ── Helpers ──────────────────────────────────────────────────────────────────

fn default_config() -> LocyConfig {
    LocyConfig {
        max_iterations: 1000,
        timeout: Some(Duration::from_secs(60)),
        ..Default::default()
    }
}

fn tight_config(max_iter: usize) -> LocyConfig {
    LocyConfig {
        max_iterations: max_iter,
        timeout: Some(Duration::from_secs(60)),
        ..Default::default()
    }
}

fn timeout_config() -> LocyConfig {
    LocyConfig {
        max_iterations: 1000,
        timeout: Some(Duration::from_nanos(1)),
        ..Default::default()
    }
}

// ── TC1: Basic recursion (transitive closure) ────────────────────────────────

#[tokio::test]
async fn test_native_transitive_closure() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {name: 'A'})-[:E]->(:N {name: 'B'})-[:E]->(:N {name: 'C'})")
        .await?;
    tx.commit().await?;

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
    // A→B, B→C, A→C (transitive)
    assert_eq!(
        reachable.len(),
        3,
        "expected 3 reachable facts, got {}",
        reachable.len()
    );
    Ok(())
}

// ── TC2: FOLD SUM aggregation ────────────────────────────────────────────────

#[tokio::test]
async fn test_native_fold_sum() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), \
         (i1:Invoice {id: 1}), (i2:Invoice {id: 2}), (i3:Invoice {id: 3}), \
         (a)-[:PAID {amount: 100}]->(i1), \
         (a)-[:PAID {amount: 200}]->(i2), \
         (b)-[:PAID {amount: 50}]->(i3)",
    )
    .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE spending AS \
             MATCH (p:Person)-[r:PAID]->(i:Invoice) \
             FOLD total = SUM(r.amount) \
             YIELD KEY p, total",
        )
        .with_config(default_config())
        .run()
        .await?;

    let spending = result
        .derived
        .get("spending")
        .expect("rule 'spending' missing");
    assert_eq!(
        spending.len(),
        2,
        "expected 2 spending facts (one per person), got {}",
        spending.len()
    );
    Ok(())
}

// ── TC3: BEST BY selection ───────────────────────────────────────────────────

#[tokio::test]
async fn test_native_best_by() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:Node {name: 'A'}), \
         (b:Node {name: 'B'}), (c:Node {name: 'C'}), (d:Node {name: 'D'}), \
         (a)-[:EDGE {cost: 5}]->(b), \
         (a)-[:EDGE {cost: 3}]->(c), \
         (a)-[:EDGE {cost: 7}]->(d)",
    )
    .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE cheapest AS \
             MATCH (a:Node)-[e:EDGE]->(b:Node) \
             BEST BY e.cost ASC \
             YIELD KEY a, KEY b, e.cost AS cost",
        )
        .with_config(default_config())
        .run()
        .await?;

    let cheapest = result
        .derived
        .get("cheapest")
        .expect("rule 'cheapest' missing");
    // BEST BY groups by KEY columns (a, b); each (a,b) pair is unique,
    // so BEST BY selects the single row per (a,b) group = 3 rows.
    assert_eq!(
        cheapest.len(),
        3,
        "expected 3 cheapest facts (one per a→b pair), got {}",
        cheapest.len()
    );
    Ok(())
}

// ── TC4: PRIORITY clause selection ───────────────────────────────────────────

#[tokio::test]
async fn test_native_priority() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:Item {name: 'A', risk: 0.8}), \
         (:Item {name: 'B', risk: 0.2})",
    )
    .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE classify PRIORITY 1 AS \
             MATCH (n:Item) YIELD KEY n, 'low' AS label \n\
             CREATE RULE classify PRIORITY 2 AS \
             MATCH (n:Item) WHERE n.risk > 0.5 YIELD KEY n, 'high' AS label",
        )
        .with_config(default_config())
        .run()
        .await?;

    let classify = result
        .derived
        .get("classify")
        .expect("rule 'classify' missing");
    // A (risk=0.8) gets 'high' via P2, B (risk=0.2) gets 'low' via P1
    assert_eq!(
        classify.len(),
        2,
        "expected 2 classify facts, got {}",
        classify.len()
    );
    Ok(())
}

// ── TC5: Multi-stratum (cross-stratum fact injection) ────────────────────────

#[tokio::test]
async fn test_native_multi_stratum() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {name: 'A'})-[:E]->(:N {name: 'B'})-[:E]->(:N {name: 'C'})")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE base AS \
             MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b \n\
             CREATE RULE extended AS \
             MATCH (a:N) WHERE a IS base TO b YIELD KEY a, b \n\
             CREATE RULE extended AS \
             MATCH (a:N)-[:E]->(mid:N) WHERE mid IS extended TO b \
             YIELD KEY a, b",
        )
        .with_config(default_config())
        .run()
        .await?;

    let base = result.derived.get("base").expect("rule 'base' missing");
    // base: direct edges only → A→B, B→C
    assert_eq!(base.len(), 2, "expected 2 base facts, got {}", base.len());

    let extended = result
        .derived
        .get("extended")
        .expect("rule 'extended' missing");
    // extended: same as transitive closure via IS base → A→B, B→C, A→C
    assert!(
        extended.len() >= 3,
        "expected at least 3 extended facts, got {}",
        extended.len()
    );
    Ok(())
}

// ── TC6: ALONG path-carried value accumulation ───────────────────────────────

#[tokio::test]
async fn test_native_along() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:Node {name: 'A'}), (b:Node {name: 'B'}), (c:Node {name: 'C'}), \
         (a)-[:EDGE {weight: 5.0}]->(b), \
         (b)-[:EDGE {weight: 3.0}]->(c), \
         (a)-[:EDGE {weight: 20.0}]->(c)",
    )
    .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE shortest AS \
             MATCH (a:Node)-[e:EDGE]->(b:Node) \
             ALONG cost = e.weight \
             BEST BY cost ASC \
             YIELD KEY a, KEY b, cost \n\
             CREATE RULE shortest AS \
             MATCH (a:Node)-[e:EDGE]->(mid:Node) \
             WHERE mid IS shortest TO b \
             ALONG cost = prev.cost + e.weight \
             BEST BY cost ASC \
             YIELD KEY a, KEY b, cost",
        )
        .with_config(default_config())
        .run()
        .await?;

    let shortest = result
        .derived
        .get("shortest")
        .expect("rule 'shortest' missing");
    // A→B (cost=5), B→C (cost=3), A→C (cost=8, via A→B→C, not direct 20)
    assert_eq!(
        shortest.len(),
        3,
        "expected 3 shortest facts, got {}",
        shortest.len()
    );

    // Verify A→C has cost 8.0 (via A→B→C) not 20.0 (direct)
    for fact in shortest {
        if let (Some(uni_common::Value::Float(c)), true) = (
            fact.get("cost"),
            // Check if this is the A→C pair by looking for a node with name 'A' and 'C'
            fact.values()
                .any(|v| format!("{v:?}").contains("'A'") || format!("{v:?}").contains("\"A\""))
                && fact.values().any(|v| {
                    format!("{v:?}").contains("'C'") || format!("{v:?}").contains("\"C\"")
                }),
        ) {
            assert!((*c - 8.0).abs() < 0.01, "A→C cost should be 8.0, got {c}");
        }
    }
    Ok(())
}

// ── TC7: Error — max iterations exceeded ─────────────────────────────────────

#[tokio::test]
async fn test_native_error_max_iterations() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {name: 'A'})-[:E]->(:N {name: 'B'})-[:E]->(:N {name: 'C'})")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE reachable AS \
             MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b \n\
             CREATE RULE reachable AS \
             MATCH (a:N)-[:E]->(mid:N) WHERE mid IS reachable TO b \
             YIELD KEY a, b",
        )
        .with_config(tight_config(1))
        .run()
        .await;

    // Hitting max_iterations is now a hard error by default (non-convergence),
    // reported distinctly from a wall-clock timeout.
    let err = result.expect_err("max_iterations=1 should error by default, not return partial");
    match err {
        uni_db::UniError::LocyIncomplete { detail } => assert_eq!(
            detail.reason,
            uni_db::LocyIncompleteReason::IterationLimit,
            "max_iterations exhaustion should report IterationLimit"
        ),
        other => panic!("expected UniError::LocyIncomplete, got {other:?}"),
    }
    Ok(())
}

// ── TC8: Error — timeout ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_native_error_timeout() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {name: 'A'})-[:E]->(:N {name: 'B'})-[:E]->(:N {name: 'C'})")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE reachable AS \
             MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b \n\
             CREATE RULE reachable AS \
             MATCH (a:N)-[:E]->(mid:N) WHERE mid IS reachable TO b \
             YIELD KEY a, b",
        )
        .with_config(timeout_config())
        .run()
        .await;

    // An expired wall-clock timeout is now a hard error by default.
    let err = result.expect_err("an expired timeout should error by default, not return partial");
    match err {
        uni_db::UniError::LocyIncomplete { detail } => assert_eq!(
            detail.reason,
            uni_db::LocyIncompleteReason::Timeout,
            "an expired wall-clock timeout should report Timeout"
        ),
        other => panic!("expected UniError::LocyIncomplete, got {other:?}"),
    }
    Ok(())
}

// ── Command dispatch tests (Phase 6) ─────────────────────────────────────────

// ── TC9: GoalQuery command ────────────────────────────────────────────────────

#[tokio::test]
async fn test_native_goal_query_command() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})-[:KNOWS]->(:Person {name: 'Carol'})")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE reachable AS \
             MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD KEY a, b \n\
             CREATE RULE reachable AS \
             MATCH (a:Person)-[:KNOWS]->(mid:Person) WHERE mid IS reachable TO b \
             YIELD KEY a, b \n\
             QUERY reachable WHERE a.name = 'Alice'",
        )
        .with_config(default_config())
        .run()
        .await?;

    // GoalQuery dispatches via SLG resolution → CommandResult::Query
    assert_eq!(
        result.command_results.len(),
        1,
        "expected 1 command result from QUERY, got {}",
        result.command_results.len()
    );

    assert!(
        matches!(
            result.command_results[0],
            uni_db::locy::CommandResult::Query(_)
        ),
        "QUERY command should produce CommandResult::Query, got {:?}",
        result.command_results[0]
    );

    Ok(())
}

// ── TC10: Cypher pass-through command ─────────────────────────────────────────
// Locy programs accept raw Cypher statements (no keyword prefix) as pass-through commands.

#[tokio::test]
async fn test_native_cypher_command() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Node {name: 'X'}), (:Node {name: 'Y'}), (:Node {name: 'Z'})")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE connected AS \
             MATCH (n:Node) YIELD KEY n \n\
             MATCH (n:Node) RETURN n.name AS name ORDER BY n.name",
        )
        .with_config(default_config())
        .run()
        .await?;

    assert_eq!(
        result.command_results.len(),
        1,
        "expected 1 command result from raw Cypher, got {}",
        result.command_results.len()
    );

    match &result.command_results[0] {
        uni_db::locy::CommandResult::Cypher(rows) => {
            assert_eq!(rows.len(), 3, "expected 3 nodes from Cypher pass-through");
        }
        other => panic!("expected CommandResult::Cypher, got {other:?}"),
    }

    Ok(())
}

// ── TC11: ExplainRule command ─────────────────────────────────────────────────

#[tokio::test]
async fn test_native_explain_rule_command() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE knows AS \
             MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD KEY a, b \n\
             EXPLAIN RULE knows WHERE a.name = 'Alice'",
        )
        .with_config(default_config())
        .run()
        .await?;

    assert_eq!(
        result.command_results.len(),
        1,
        "expected 1 command result from EXPLAIN RULE, got {}",
        result.command_results.len()
    );

    assert!(
        matches!(
            result.command_results[0],
            uni_db::locy::CommandResult::Explain(_)
        ),
        "EXPLAIN RULE command should produce CommandResult::Explain, got {:?}",
        result.command_results[0]
    );

    Ok(())
}

// ── TC12: Multiple commands in sequence ──────────────────────────────────────

#[tokio::test]
async fn test_native_multiple_commands() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (:City {name: 'A'})-[:ROAD]->(:City {name: 'B'})-[:ROAD]->(:City {name: 'C'})",
    )
    .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE reachable AS \
             MATCH (a:City)-[:ROAD]->(b:City) YIELD KEY a, b \n\
             CREATE RULE reachable AS \
             MATCH (a:City)-[:ROAD]->(mid:City) WHERE mid IS reachable TO b \
             YIELD KEY a, b \n\
             QUERY reachable WHERE a.name = 'A' \n\
             MATCH (c:City) RETURN c.name AS name",
        )
        .with_config(default_config())
        .run()
        .await?;

    // Two commands: QUERY + raw Cypher
    assert_eq!(
        result.command_results.len(),
        2,
        "expected 2 command results, got {}",
        result.command_results.len()
    );

    assert!(
        matches!(
            result.command_results[0],
            uni_db::locy::CommandResult::Query(_)
        ),
        "first result should be Query"
    );
    assert!(
        matches!(
            result.command_results[1],
            uni_db::locy::CommandResult::Cypher(_)
        ),
        "second result should be Cypher"
    );

    Ok(())
}
