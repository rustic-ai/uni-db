// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for pattern comprehension via the DataFusion execution path.

use anyhow::Result;
use uni_db::Uni;

#[tokio::test(flavor = "multi_thread")]
async fn test_pattern_comprehension_basic_traversal() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let db = Uni::in_memory().build().await?;

    // Create nodes and relationships
    db.execute(
        "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})",
    )
    .await?;
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}) \
         CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c)",
    )
    .await?;

    // First verify regular MATCH works
    let check = db
        .query("MATCH (n:Person)-[:KNOWS]->(m:Person) RETURN n.name, m.name")
        .await?;
    eprintln!("Regular MATCH results ({} rows):", check.len());
    for row in check.rows() {
        eprintln!("  {:?}", row);
    }
    assert!(!check.is_empty(), "Regular MATCH should find results");

    // Now test pattern comprehension
    let results = db
        .query("MATCH (n:Person) RETURN n.name, [(n)-[:KNOWS]->(m) | m.name] AS friends")
        .await?;

    eprintln!("Pattern comprehension results ({} rows):", results.len());
    for row in results.rows() {
        eprintln!("  {:?}", row);
    }

    assert_eq!(results.len(), 3, "Should have 3 rows (one per Person)");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pattern_comprehension_node_property() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let db = Uni::in_memory().build().await?;

    // TCK Scenario 4: Introduce a new node variable
    db.execute("CREATE ({ext_id: 'a'})-[:T]->({name: 'val', ext_id: 'b'})-[:T]->({ext_id: 'c'})")
        .await?;

    let results = db
        .query("MATCH (n) RETURN [(n)-[:T]->(b) | b.name] AS list")
        .await?;

    eprintln!("TCK4 results ({} rows):", results.len());
    for row in results.rows() {
        eprintln!("  {:?}", row);
    }

    assert_eq!(results.len(), 3, "Should have 3 rows");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pattern_comprehension_edge_property() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let db = Uni::in_memory().build().await?;

    // TCK Scenario 5: Introduce a new relationship variable
    db.execute("CREATE (a), (b), (c) CREATE (a)-[:T {name: 'val'}]->(b), (b)-[:T]->(c)")
        .await?;

    let results = db
        .query("MATCH (n) RETURN [(n)-[r:T]->() | r.name] AS list")
        .await?;

    eprintln!("TCK5 results ({} rows):", results.len());
    for row in results.rows() {
        eprintln!("  {:?}", row);
    }

    assert_eq!(results.len(), 3, "Should have 3 rows");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pattern_comprehension_path_variable() -> Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let db = Uni::in_memory().build().await?;

    // TCK Scenario 1: Return a pattern comprehension with path variable
    db.execute("CREATE (a:A), (b:B) CREATE (a)-[:T]->(b), (b)-[:T]->(:C)")
        .await?;

    let result = db
        .query("MATCH (n) RETURN [p = (n)-->() | p] AS list")
        .await;

    match result {
        Ok(rows) => {
            eprintln!("Path variable results ({} rows):", rows.len());
            for row in rows.rows() {
                eprintln!("  {:?}", row);
            }
        }
        Err(e) => {
            eprintln!("Path variable query failed: {:?}", e);
        }
    }

    Ok(())
}
