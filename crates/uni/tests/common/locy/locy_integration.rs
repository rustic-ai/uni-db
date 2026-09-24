// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for the Locy engine wired to a real database.

use anyhow::Result;
use uni_db::Uni;

// ── Step 1: Skeleton ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_locy_engine_exists() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let _session = db.session();
    Ok(())
}

// ── Step 2: compile_only ───────────────────────────────────────────────────

#[tokio::test]
async fn test_compile_only_valid() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let compiled = db
        .session()
        .compile_locy("CREATE RULE r AS MATCH (a)-[:K]->(b) YIELD KEY a, b")?;
    assert_eq!(compiled.strata.len(), 1);
    assert!(compiled.rule_catalog.contains_key("r"));
    Ok(())
}

// ── Step 3: Parse error ────────────────────────────────────────────────────

#[tokio::test]
async fn test_parse_error() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let err = db
        .session()
        .compile_locy("THIS IS NOT VALID LOCY")
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("LocyParseError"),
        "expected LocyParseError, got: {msg}"
    );
    assert!(matches!(err, uni_db::UniError::Parse { .. }));
    Ok(())
}

// ── Step 4: Compile error ──────────────────────────────────────────────────

#[tokio::test]
async fn test_compile_error_cyclic_negation() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    // a IS NOT b and b IS NOT a → cyclic negation
    let program = "CREATE RULE a AS MATCH (x) WHERE x IS NOT b YIELD KEY x \n\
                    CREATE RULE b AS MATCH (x) WHERE x IS NOT a YIELD KEY x";
    let err = db.session().compile_locy(program).unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("LocyCompileError"),
        "expected LocyCompileError, got: {msg}"
    );
    assert!(matches!(err, uni_db::UniError::Query { .. }));
    Ok(())
}

// ── Step 5: Non-recursive evaluation ───────────────────────────────────────

#[tokio::test]
async fn test_evaluate_non_recursive() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy(
            "CREATE RULE friends AS \
             MATCH (a:Person)-[:KNOWS]->(b:Person) \
             YIELD KEY a, b",
        )
        .await?;

    let friends = result
        .derived
        .get("friends")
        .expect("rule 'friends' missing");
    assert_eq!(friends.len(), 1);
    // Keys are the node variables themselves
    assert!(friends[0].contains_key("a"));
    assert!(friends[0].contains_key("b"));
    Ok(())
}

// ── Step 6: Recursive transitive closure ───────────────────────────────────

#[tokio::test]
async fn test_evaluate_recursive_transitive_closure() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {name: 'A'})-[:E]->(:N {name: 'B'})-[:E]->(:N {name: 'C'})")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy(
            "CREATE RULE reachable AS \
             MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b \n\
             CREATE RULE reachable AS \
             MATCH (a:N)-[:E]->(mid:N) WHERE mid IS reachable TO b \
             YIELD KEY a, b",
        )
        .await?;

    let reachable = result
        .derived
        .get("reachable")
        .expect("rule 'reachable' missing");
    // A→B, B→C, A→C (transitive)
    assert!(
        reachable.len() >= 3,
        "expected at least 3 reachable facts, got {}",
        reachable.len()
    );
    Ok(())
}

// ── Step 7: DERIVE creates edges ───────────────────────────────────────────

#[tokio::test]
async fn test_derive_creates_edges() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})")
        .await?;
    tx.commit().await?;

    // Session-level DERIVE now collects facts (no auto-apply).
    // Use tx.apply(derived) to materialize.
    let locy_result = session
        .locy(
            "CREATE RULE inferred AS \
             MATCH (a:Person)-[:KNOWS]->(b:Person) \
             DERIVE (a)-[:INFERRED]->(b) \
             DERIVE inferred",
        )
        .await?;

    // DerivedFactSet should be present
    let derived = locy_result
        .derived_fact_set
        .clone()
        .expect("session DERIVE should produce DerivedFactSet");
    assert!(!derived.is_empty(), "should have derived facts");

    // Apply to a transaction
    let tx = session.tx().await?;
    let apply_result = tx.apply(derived).await?;
    assert!(apply_result.facts_applied > 0);
    tx.commit().await?;

    // Query the newly created edges
    let result = session
        .query("MATCH (a)-[:INFERRED]->(b) RETURN a.name AS src, b.name AS dst")
        .await?;
    assert_eq!(result.len(), 1);
    Ok(())
}

// ── Step 8: ASSUME with savepoint rollback ─────────────────────────────────

#[tokio::test]
async fn test_assume_rollback() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})")
        .await?;
    tx.commit().await?;

    // ASSUME creates a temporary node, THEN re-evaluates rule in mutated state
    let result = db
        .session()
        .locy(
            "CREATE RULE base AS \
             MATCH (p:Person) YIELD KEY p \n\
             ASSUME { CREATE (:Person {name: 'Temp'}) } \
             THEN QUERY base WHERE p = p",
        )
        .await?;

    // The ASSUME command result should show the re-evaluated state
    assert!(!result.command_results.is_empty());

    // After rollback, the temporary node should NOT exist in the real DB
    let check = db
        .session()
        .query("MATCH (p:Person {name: 'Temp'}) RETURN p")
        .await?;
    assert_eq!(
        check.len(),
        0,
        "ASSUME should have rolled back the temporary node"
    );
    Ok(())
}

// ── Step 9: Runtime error ──────────────────────────────────────────────────

#[tokio::test]
async fn test_runtime_error_max_iterations() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:N {name: 'A'})-[:E]->(:N {name: 'B'})-[:E]->(:N {name: 'C'})")
        .await?;
    tx.commit().await?;

    let config = uni_db::locy::LocyConfig {
        max_iterations: 1,
        timeout: Some(std::time::Duration::from_secs(30)),
        ..Default::default()
    };

    let result = db
        .session()
        .locy_with(
            "CREATE RULE reachable AS \
             MATCH (a:N)-[:E]->(b:N) YIELD KEY a, b \n\
             CREATE RULE reachable AS \
             MATCH (a:N)-[:E]->(mid:N) WHERE mid IS reachable TO b \
             YIELD KEY a, b",
        )
        .with_config(config)
        .run()
        .await;

    // max_iterations=1 is now a hard error by default (non-convergence).
    let err = result.expect_err("max_iterations=1 should error by default, not return partial");
    assert!(
        matches!(
            err,
            uni_db::UniError::LocyIncomplete { ref detail }
                if detail.reason == uni_db::LocyIncompleteReason::IterationLimit
        ),
        "expected LocyIncomplete(IterationLimit), got {err:?}"
    );
    Ok(())
}

// ── Step 10: End-to-end smoke test ─────────────────────────────────────────

#[tokio::test]
async fn test_end_to_end_smoke() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create a small social network
    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}), \
         (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)",
    )
    .await?;
    tx.commit().await?;

    // Compute transitive reachability
    let result = db
        .session()
        .locy(
            "CREATE RULE reachable AS \
             MATCH (a:Person)-[:KNOWS]->(b:Person) YIELD KEY a, b \n\
             CREATE RULE reachable AS \
             MATCH (a:Person)-[:KNOWS]->(mid:Person) WHERE mid IS reachable TO b \
             YIELD KEY a, b",
        )
        .await?;

    let reachable = result
        .derived
        .get("reachable")
        .expect("rule 'reachable' missing");

    // Expected: Alice→Bob, Bob→Carol, Alice→Carol (transitive)
    assert!(
        reachable.len() >= 3,
        "expected at least 3 reachable pairs, got {}",
        reachable.len()
    );

    // Verify stats
    assert!(result.stats.total_iterations > 0);
    Ok(())
}

/// Regression: a purely non-recursive program records its single evaluation
/// pass, so `total_iterations` is >= 1. Before the fix, non-recursive strata
/// never wrote the iteration-count slot, so the stat read 0 — making it look
/// like nothing was evaluated.
#[tokio::test]
async fn test_non_recursive_reports_iterations() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice', age: 30}), (:Person {name: 'Bob', age: 16})")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy("CREATE RULE adult AS MATCH (p:Person) WHERE p.age >= 18 YIELD KEY p")
        .await?;

    assert!(
        result.stats.total_iterations >= 1,
        "non-recursive program must report >= 1 iteration, got {}",
        result.stats.total_iterations
    );
    Ok(())
}

// ── LocyBuilder (evaluate_with) ────────────────────────────────────────────

#[tokio::test]
async fn test_evaluate_with_param_builder() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Episode {agent_id: 'a1', label: 'alpha'})")
        .await?;
    tx.execute("CREATE (:Episode {agent_id: 'a2', label: 'beta'})")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE ep AS MATCH (e:Episode) WHERE e.agent_id = $aid \
             YIELD KEY e, e.label AS lbl \
             QUERY ep RETURN lbl",
        )
        .param("aid", "a1")
        .run()
        .await?;

    let rows = match &result.command_results[0] {
        uni_db::locy::CommandResult::Query(rows) => rows,
        other => panic!("expected Query, got {other:?}"),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("lbl"),
        Some(&uni_db::Value::String("alpha".to_string()))
    );
    Ok(())
}

#[tokio::test]
async fn test_evaluate_with_multiple_params() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Score {name: 'low',  val: 10})")
        .await?;
    tx.execute("CREATE (:Score {name: 'mid',  val: 50})")
        .await?;
    tx.execute("CREATE (:Score {name: 'high', val: 90})")
        .await?;
    tx.commit().await?;

    let result = db
        .session()
        .locy_with(
            "CREATE RULE sc AS MATCH (s:Score) YIELD KEY s, s.val AS v, s.name AS nm \
             QUERY sc WHERE v > $lo AND v < $hi RETURN nm",
        )
        .param("lo", uni_db::Value::Int(20))
        .param("hi", uni_db::Value::Int(80))
        .run()
        .await?;

    let rows = match &result.command_results[0] {
        uni_db::locy::CommandResult::Query(rows) => rows,
        other => panic!("expected Query, got {other:?}"),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get("nm"),
        Some(&uni_db::Value::String("mid".to_string()))
    );
    Ok(())
}

// ── DerivedFactSet / tx.apply() / apply_with() tests ──────────────────────

/// Session-level DERIVE returns a DerivedFactSet (no auto-apply).
#[tokio::test]
async fn test_session_derive_returns_derived_fact_set() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let tx = session.tx().await?;
    tx.execute("CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})")
        .await?;
    tx.commit().await?;

    let result = session
        .locy(
            "CREATE RULE inferred AS \
             MATCH (a:Person)-[:KNOWS]->(b:Person) \
             DERIVE (a)-[:INFERRED]->(b) \
             DERIVE inferred",
        )
        .await?;

    let derived = result
        .derived_fact_set
        .as_ref()
        .expect("session DERIVE should produce DerivedFactSet");
    assert!(
        derived.fact_count() > 0,
        "should have derived facts, got {}",
        derived.fact_count()
    );
    assert!(!derived.is_empty());
    assert!(!derived.mutation_queries.is_empty());

    // Verify no auto-apply: the graph should NOT have INFERRED edges yet
    let check = session
        .query("MATCH ()-[:INFERRED]->() RETURN count(*) AS cnt")
        .await?;
    let cnt = check.rows()[0].get::<i64>("cnt")?;
    assert_eq!(cnt, 0, "session DERIVE should NOT auto-apply mutations");

    Ok(())
}

/// Transaction-level DERIVE auto-applies (unchanged behavior).
#[tokio::test]
async fn test_tx_derive_auto_applies() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let setup_tx = session.tx().await?;
    setup_tx
        .execute("CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})")
        .await?;
    setup_tx.commit().await?;

    let tx = session.tx().await?;
    let result = tx
        .locy(
            "CREATE RULE inferred AS \
             MATCH (a:Person)-[:KNOWS]->(b:Person) \
             DERIVE (a)-[:INFERRED]->(b) \
             DERIVE inferred",
        )
        .await?;

    // Transaction path: derived_fact_set should be None
    assert!(
        result.derived_fact_set.is_none(),
        "tx DERIVE should NOT produce DerivedFactSet"
    );

    // Mutations should be visible within the transaction
    let check = tx
        .query("MATCH ()-[:INFERRED]->() RETURN count(*) AS cnt")
        .await?;
    let cnt = check.rows()[0].get::<i64>("cnt")?;
    assert!(cnt > 0, "tx DERIVE should auto-apply mutations");

    tx.commit().await?;
    Ok(())
}

/// tx.apply(derived) writes collected facts to the transaction.
#[tokio::test]
async fn test_tx_apply_writes_facts() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let setup_tx = session.tx().await?;
    setup_tx
        .execute("CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})")
        .await?;
    setup_tx.commit().await?;

    // Session DERIVE: collect facts
    let result = session
        .locy(
            "CREATE RULE inferred AS \
             MATCH (a:Person)-[:KNOWS]->(b:Person) \
             DERIVE (a)-[:INFERRED]->(b) \
             DERIVE inferred",
        )
        .await?;
    let derived = result.derived_fact_set.clone().unwrap();

    // Apply to transaction and commit
    let tx = session.tx().await?;
    let apply_result = tx.apply(derived).await?;
    assert!(apply_result.facts_applied > 0);
    tx.commit().await?;

    // Verify facts are visible after commit
    let check = session
        .query("MATCH (a)-[:INFERRED]->(b) RETURN a.name AS src, b.name AS dst")
        .await?;
    assert_eq!(check.len(), 1);
    Ok(())
}

/// tx.apply_with(derived).require_fresh() rejects stale DerivedFactSets.
#[tokio::test]
async fn test_tx_apply_with_require_fresh() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let setup_tx = session.tx().await?;
    setup_tx
        .execute("CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})")
        .await?;
    setup_tx.commit().await?;

    // Derive facts at current version
    let result = session
        .locy(
            "CREATE RULE inferred AS \
             MATCH (a:Person)-[:KNOWS]->(b:Person) \
             DERIVE (a)-[:INFERRED]->(b) \
             DERIVE inferred",
        )
        .await?;
    let derived = result.derived_fact_set.clone().unwrap();

    // Create a concurrent write to advance the version
    let advance_tx = session.tx().await?;
    advance_tx
        .execute("CREATE (:Person {name: 'Carol'})")
        .await?;
    advance_tx.commit().await?;

    // Now apply with require_fresh — should fail because version advanced
    let tx = session.tx().await?;
    let err = tx.apply_with(derived).require_fresh().run().await;

    match err {
        Err(uni_db::UniError::StaleDerivedFacts { version_gap }) => {
            assert!(version_gap > 0, "version gap should be > 0");
        }
        other => panic!("expected StaleDerivedFacts error, got {other:?}"),
    }
    tx.rollback();
    Ok(())
}

/// tx.apply_with(derived).max_version_gap(n) controls staleness threshold.
#[tokio::test]
async fn test_tx_apply_with_max_version_gap() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let setup_tx = session.tx().await?;
    setup_tx
        .execute("CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})")
        .await?;
    setup_tx.commit().await?;

    // Derive facts
    let result = session
        .locy(
            "CREATE RULE inferred AS \
             MATCH (a:Person)-[:KNOWS]->(b:Person) \
             DERIVE (a)-[:INFERRED]->(b) \
             DERIVE inferred",
        )
        .await?;
    let derived = result.derived_fact_set.clone().unwrap();

    // Advance version once
    let advance_tx = session.tx().await?;
    advance_tx
        .execute("CREATE (:Person {name: 'Carol'})")
        .await?;
    advance_tx.commit().await?;

    // max_version_gap(0) should reject (gap = 1)
    let tx = session.tx().await?;
    let err = tx
        .apply_with(derived.clone())
        .max_version_gap(0)
        .run()
        .await;
    assert!(
        matches!(err, Err(uni_db::UniError::StaleDerivedFacts { .. })),
        "gap=1 with max=0 should fail"
    );

    // max_version_gap(5) should succeed (gap = 1 <= 5)
    let result = tx.apply_with(derived).max_version_gap(5).run().await;
    assert!(result.is_ok(), "gap=1 with max=5 should succeed");
    let apply_result = result.unwrap();
    assert!(apply_result.version_gap > 0);
    assert!(apply_result.facts_applied > 0);
    tx.commit().await?;
    Ok(())
}

/// DerivedFactSet inspection: vertices, edges, fact_count(), is_empty().
#[tokio::test]
async fn test_derived_fact_set_inspection() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    let session = db.session();
    let setup_tx = session.tx().await?;
    setup_tx
        .execute("CREATE (:Person {name: 'Alice'})-[:KNOWS]->(:Person {name: 'Bob'})")
        .await?;
    setup_tx.commit().await?;

    let result = session
        .locy(
            "CREATE RULE inferred AS \
             MATCH (a:Person)-[:KNOWS]->(b:Person) \
             DERIVE (a)-[:INFERRED]->(b) \
             DERIVE inferred",
        )
        .await?;

    let derived = result.derived_fact_set.clone().unwrap();

    // Should have edges (INFERRED)
    assert!(!derived.edges.is_empty(), "should have derived edges");
    assert_eq!(derived.edges[0].edge_type, "INFERRED");

    // fact_count should be consistent
    let expected = derived.vertices.values().map(|v| v.len()).sum::<usize>() + derived.edges.len();
    assert_eq!(derived.fact_count(), expected);
    assert!(!derived.is_empty());

    // evaluated_at_version should be > 0 (we created data before deriving)
    assert!(
        derived.evaluated_at_version > 0,
        "evaluated_at_version should be > 0"
    );
    Ok(())
}

// ── Parse-error hint for bare registered-rule names ─────────────────────────

#[tokio::test]
async fn test_bare_registered_rule_name_yields_query_hint() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Node")
        .edge_type("EDGE", &["Node"], &["Node"])
        .done()
        .apply()
        .await?;
    db.rules()
        .register("CREATE RULE my_rule AS MATCH (a:Node)-[:EDGE]->(b:Node) YIELD KEY a, KEY b")
        .await?;

    // Invoking the rule by its bare name fails to parse, but the error should
    // point at the QUERY goal-query form.
    let err = db
        .session()
        .locy("my_rule")
        .await
        .expect_err("bare name should not parse");
    let msg = err.to_string();
    assert!(matches!(err, uni_db::UniError::Parse { .. }), "got: {msg}");
    assert!(
        msg.contains("QUERY my_rule"),
        "error should hint the goal-query form, got: {msg}"
    );
    Ok(())
}

#[tokio::test]
async fn test_bare_unknown_name_has_no_hint() -> Result<()> {
    let db = Uni::in_memory().build().await?;
    // A bare identifier that matches no registered rule gets a plain parse
    // error with no goal-query hint.
    let err = db
        .session()
        .locy("not_a_rule")
        .await
        .expect_err("should not parse");
    assert!(
        !err.to_string().contains("QUERY"),
        "no hint expected for an unregistered name, got: {err}"
    );
    Ok(())
}
