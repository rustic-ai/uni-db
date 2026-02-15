// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
// Rust guideline compliant

//! Test to verify overflow_json column is not leaked into query results.

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_edge_properties_no_overflow_json() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create nodes and edge with properties
    db.execute("CREATE (a:Person {name: 'Alice'})-[r:KNOWS {since: 2020, strength: 0.8}]->(b:Person {name: 'Bob'})")
        .await?;

    // Query that returns edge with dotted properties
    let result = db
        .query("MATCH (a)-[r:KNOWS]->(b) RETURN r.since, r.strength")
        .await?;

    // Check that overflow_json is not in the column names
    let column_names = result
        .columns
        .iter()
        .map(|c| c.as_str())
        .collect::<Vec<_>>();
    assert!(
        !column_names.iter().any(|c| c.contains("overflow_json")),
        "Results should not contain overflow_json column. Columns: {:?}",
        column_names
    );

    assert_eq!(result.len(), 1);

    Ok(())
}

#[tokio::test]
async fn test_edge_struct_no_overflow_json() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create nodes and edge
    db.execute(
        "CREATE (a:Person {name: 'Alice'})-[r:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})",
    )
    .await?;

    // Query that returns full edge struct
    let result = db.query("MATCH (a)-[r:KNOWS]->(b) RETURN r").await?;

    // The struct form should have a single "r" column, not r.overflow_json
    let column_names = result
        .columns
        .iter()
        .map(|c| c.as_str())
        .collect::<Vec<_>>();
    assert!(
        !column_names.iter().any(|c| c.contains("overflow_json")),
        "Results should not contain overflow_json. Columns: {:?}",
        column_names
    );

    Ok(())
}
