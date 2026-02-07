// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_where_property_equals() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schema and data
    db.execute("CREATE LABEL Person (name STRING, age INT)")
        .await?;
    db.execute("CREATE (:Person {name: 'Alice', age: 25})")
        .await?;
    db.execute("CREATE (:Person {name: 'Bob', age: 30})")
        .await?;
    db.execute("CREATE (:Person {name: 'Charlie', age: 35})")
        .await?;

    println!("Created 3 Person nodes");

    // Test WHERE with property equality
    let result = db
        .query("MATCH (n:Person) WHERE n.name = 'Bob' RETURN n.name, n.age")
        .await?;

    println!("Result length: {}", result.len());
    if !result.is_empty() {
        println!(
            "Found: name={}, age={}",
            result.rows()[0].get::<String>("n.name")?,
            result.rows()[0].get::<i64>("n.age")?
        );
    }

    assert_eq!(result.len(), 1, "Should match exactly one person named Bob");
    assert_eq!(result.rows()[0].get::<String>("n.name")?, "Bob");
    assert_eq!(result.rows()[0].get::<i64>("n.age")?, 30);

    Ok(())
}

#[tokio::test]
async fn test_where_property_comparison() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schema and data
    db.execute("CREATE LABEL Person (name STRING, age INT)")
        .await?;
    db.execute("CREATE (:Person {name: 'Alice', age: 25})")
        .await?;
    db.execute("CREATE (:Person {name: 'Bob', age: 30})")
        .await?;
    db.execute("CREATE (:Person {name: 'Charlie', age: 35})")
        .await?;

    println!("Created 3 Person nodes");

    // Test WHERE with comparison operator
    let result = db
        .query("MATCH (n:Person) WHERE n.age > 28 RETURN n.name ORDER BY n.name")
        .await?;

    println!("Result length: {}", result.len());
    for (i, row) in result.rows().iter().enumerate() {
        println!("Row {}: {}", i, row.get::<String>("n.name")?);
    }

    assert_eq!(result.len(), 2, "Should match Bob and Charlie (age > 28)");

    Ok(())
}

#[tokio::test]
async fn test_where_label_predicate() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schema and data
    db.execute("CREATE LABEL Person (name STRING)").await?;
    db.execute("CREATE LABEL Company (name STRING)").await?;
    db.execute("CREATE EDGE TYPE WORKS_AT FROM Person TO Company")
        .await?;

    db.execute("CREATE (:Person {name: 'Alice'})-[:WORKS_AT]->(:Company {name: 'Acme'})")
        .await?;
    db.execute("CREATE (:Person {name: 'Bob'})-[:WORKS_AT]->(:Company {name: 'BigCo'})")
        .await?;

    println!("Created graph with Person and Company nodes");

    // First, check if a simple MATCH returns labels correctly
    let simple = db.query("MATCH (a:Person) RETURN a").await?;
    println!("Simple MATCH (a:Person): {} results", simple.len());
    if !simple.is_empty() {
        println!(
            "  Node 'a' in simple query: {:?}",
            simple.rows()[0].value("a")
        );
    }

    // Check what ScanAll returns (unlabeled pattern)
    let scanall = db.query("MATCH (a) RETURN a").await?;
    println!("MATCH (a) [ScanAll]: {} results", scanall.len());
    if !scanall.is_empty() {
        println!(
            "  Node 'a' from ScanAll: {:?}",
            scanall.rows()[0].value("a")
        );
    }

    // Now test traverse - does it preserve labels?
    let without_where = db
        .query("MATCH (a)-[:WORKS_AT]->(b) RETURN a.name, b.name, a")
        .await?;
    println!("Traverse MATCH: {} results", without_where.len());
    if !without_where.is_empty() {
        println!(
            "  Node 'a' in traverse: {:?}",
            without_where.rows()[0].value("a")
        );
    }

    // Test WHERE with label predicate
    let result = db
        .query("MATCH (a)-[:WORKS_AT]->(b) WHERE a:Person RETURN a.name, b.name")
        .await?;
    println!("With WHERE a:Person: {} results", result.len());

    assert_eq!(
        result.len(),
        2,
        "Should match 2 WORKS_AT relationships with Person source"
    );

    Ok(())
}

#[tokio::test]
async fn test_where_equi_join() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schema and data
    db.execute("CREATE LABEL Person (id INT, name STRING)")
        .await?;
    db.execute("CREATE (:Person {id: 1, name: 'Alice'})")
        .await?;
    db.execute("CREATE (:Person {id: 2, name: 'Bob'})").await?;
    db.execute("CREATE (:Person {id: 1, name: 'Alice2'})")
        .await?; // Same id as first Alice

    println!("Created 3 Person nodes");

    // Test WHERE with equi-join (property equality between variables)
    let result = db.query("MATCH (a:Person), (b:Person) WHERE a.id = b.id AND a.name < b.name RETURN a.name, b.name").await?;

    println!("Result length: {}", result.len());

    // Should match pairs with same id but different names
    assert!(
        !result.is_empty(),
        "Should match at least one pair with same id"
    );

    Ok(())
}

#[tokio::test]
async fn test_where_unlabeled() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create unlabeled nodes
    db.execute("CREATE ({name: 'Alice', type: 'person'})")
        .await?;
    db.execute("CREATE ({name: 'Bob', type: 'person'})").await?;
    db.execute("CREATE ({name: 'Acme', type: 'company'})")
        .await?;

    println!("Created 3 unlabeled nodes");

    // Test WHERE on unlabeled nodes
    let result = db
        .query("MATCH (n) WHERE n.type = 'person' RETURN n.name ORDER BY n.name")
        .await?;

    println!("Result length: {}", result.len());
    for (i, row) in result.rows().iter().enumerate() {
        println!("Row {}: {}", i, row.get::<String>("n.name")?);
    }

    assert_eq!(result.len(), 2, "Should match 2 person nodes");

    Ok(())
}
