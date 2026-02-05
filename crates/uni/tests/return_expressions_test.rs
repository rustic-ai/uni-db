// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_return_property_access() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schema and data
    db.execute("CREATE LABEL Person (name STRING, age INT)")
        .await?;
    db.execute("CREATE (:Person {name: 'Alice', age: 25})")
        .await?;
    db.execute("CREATE (:Person {name: 'Bob', age: 30})")
        .await?;

    // Test RETURN with property access
    let result = db
        .query("MATCH (n:Person) RETURN n.name, n.age ORDER BY n.name")
        .await?;

    assert_eq!(result.len(), 2, "Should return 2 rows");
    assert_eq!(result.rows()[0].get::<String>("n.name")?, "Alice");
    assert_eq!(result.rows()[0].get::<i64>("n.age")?, 25);
    assert_eq!(result.rows()[1].get::<String>("n.name")?, "Bob");
    assert_eq!(result.rows()[1].get::<i64>("n.age")?, 30);

    Ok(())
}

#[tokio::test]
async fn test_return_graph_introspection_functions() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schema
    db.execute("CREATE LABEL Person (name STRING)").await?;
    db.execute("CREATE LABEL Company (name STRING)").await?;
    db.execute("CREATE EDGE TYPE WORKS_AT FROM Person TO Company")
        .await?;

    // Create data
    db.execute("CREATE (:Person {name: 'Alice'})-[:WORKS_AT]->(:Company {name: 'Acme'})")
        .await?;

    // Test labels() function
    let result = db
        .query("MATCH (n:Person) RETURN labels(n) AS node_labels")
        .await?;
    println!(
        "labels() result: {:?}",
        result.rows()[0].value("node_labels")
    );

    // Test type() function
    let result = db
        .query("MATCH ()-[r:WORKS_AT]->() RETURN type(r) AS rel_type")
        .await?;
    println!("type() result: {:?}", result.rows()[0].value("rel_type"));

    // Test properties() function
    let result = db
        .query("MATCH (n:Person) RETURN properties(n) AS props")
        .await?;
    println!("properties() result: {:?}", result.rows()[0].value("props"));

    // Test id() function
    let result = db.query("MATCH (n:Person) RETURN id(n) AS node_id").await?;
    println!("id() result: {:?}", result.rows()[0].value("node_id"));

    Ok(())
}

#[tokio::test]
async fn test_return_arithmetic() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Test arithmetic expressions
    let result = db.query("RETURN 1 + 2 AS sum").await?;
    assert_eq!(result.len(), 1);
    println!("Arithmetic result: {:?}", result.rows()[0].value("sum"));

    Ok(())
}

#[tokio::test]
async fn test_return_full_node() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schema and data
    db.execute("CREATE LABEL Person (name STRING, age INT)")
        .await?;
    db.execute("CREATE (:Person {name: 'Alice', age: 25})")
        .await?;

    // Test returning full node
    let result = db.query("MATCH (n:Person) RETURN n").await?;

    println!("Full node result: {:?}", result.rows()[0].value("n"));
    assert_eq!(result.len(), 1, "Should return 1 row");

    Ok(())
}
