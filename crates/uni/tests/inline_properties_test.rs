// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_inline_property_simple() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schema
    db.execute("CREATE LABEL Person (name STRING, age INT)")
        .await?;

    // Create test data
    db.execute("CREATE (:Person {name: 'Alice', age: 25})")
        .await?;
    db.execute("CREATE (:Person {name: 'Bob', age: 30})")
        .await?;
    db.execute("CREATE (:Person {name: 'Charlie', age: 35})")
        .await?;

    // Test inline property matching
    let result = db
        .query("MATCH (n:Person {name: 'Bob'}) RETURN n.name, n.age")
        .await?;

    println!("Result length: {}", result.len());
    if !result.is_empty() {
        println!(
            "First row: name={}, age={}",
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
async fn test_inline_property_multiple() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schema
    db.execute("CREATE LABEL Product (name STRING, price INT, category STRING)")
        .await?;

    // Create test data
    db.execute("CREATE (:Product {name: 'Laptop', price: 1000, category: 'Electronics'})")
        .await?;
    db.execute("CREATE (:Product {name: 'Mouse', price: 25, category: 'Electronics'})")
        .await?;
    db.execute("CREATE (:Product {name: 'Desk', price: 300, category: 'Furniture'})")
        .await?;

    // Test multiple inline properties
    let result = db
        .query("MATCH (p:Product {category: 'Electronics'}) RETURN p.name ORDER BY p.name")
        .await?;

    println!("Result length: {}", result.len());
    for (i, row) in result.rows().iter().enumerate() {
        println!("Row {}: {}", i, row.get::<String>("p.name")?);
    }

    assert_eq!(result.len(), 2, "Should match 2 electronics");

    Ok(())
}
