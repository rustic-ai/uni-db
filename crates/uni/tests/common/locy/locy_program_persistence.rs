// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for L-12: Locy program persistence across evaluate calls.

use uni_db::Uni;

/// Rules registered via `locy().register()` should be available in
/// subsequent `evaluate()` calls without redeclaring them.
#[tokio::test]
async fn test_locy_register_persists_rules() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Node")
        .property("name", uni_common::core::schema::DataType::String)
        .property_nullable("val", uni_common::core::schema::DataType::Int64)
        .edge_type("EDGE", &["Node"], &["Node"])
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE (a:Node {name: 'A', val: 1})-[:EDGE]->(b:Node {name: 'B', val: 2})-[:EDGE]->(c:Node {name: 'C', val: 3})",
    )
    .await?;
    tx.commit().await?;
    db.flush().await?;

    // Step 1: Register rules globally (no execution).
    db.rules()
        .register(
            "CREATE RULE reach AS
           MATCH (a:Node)-[:EDGE]->(b:Node)
           YIELD KEY a, KEY b",
        )
        .await?;

    // Step 2: Evaluate a QUERY that references the registered rule.
    let result = db.session().locy("QUERY reach WHERE a.name = 'A'").await?;

    // The QUERY should find A→B and A→C (via the registered rule).
    assert!(
        result.derived_rules().await.unwrap().contains_key("reach"),
        "Should have 'reach' derived relation"
    );
    let facts = &result.derived_rules().await.unwrap()["reach"];
    assert!(
        facts.len() >= 2,
        "Should find at least 2 facts (A→B, A→C), got {}",
        facts.len()
    );

    Ok(())
}

/// `locy_clear()` should remove all registered rules.
#[tokio::test]
async fn test_locy_clear_removes_registered_rules() -> anyhow::Result<()> {
    let db = Uni::in_memory().build().await?;
    db.schema()
        .label("Node")
        .property("name", uni_common::core::schema::DataType::String)
        .edge_type("EDGE", &["Node"], &["Node"])
        .done()
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Node {name: 'A'})-[:EDGE]->(:Node {name: 'B'})")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Register a rule globally.
    db.rules()
        .register(
            "CREATE RULE reach AS
           MATCH (a:Node)-[:EDGE]->(b:Node)
           YIELD KEY a, KEY b",
        )
        .await?;

    // Clear the global registry.
    db.rules().clear().await?;

    // Evaluating a QUERY that references the cleared rule should fail or
    // return empty results (rule is no longer available).
    let result = db.session().locy("QUERY reach WHERE a.name = 'A'").await;
    // The query should fail because 'reach' is not defined.
    assert!(result.is_err(), "Should fail when querying a cleared rule");

    Ok(())
}
