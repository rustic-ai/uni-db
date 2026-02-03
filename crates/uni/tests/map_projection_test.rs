// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::{Uni, Value};

fn get_map(value: &Value) -> &std::collections::HashMap<String, Value> {
    match value {
        Value::Map(map) => map,
        _ => panic!("Expected Map value, got {:?}", value),
    }
}

/// Test basic property selection with map projection
#[tokio::test]
async fn test_map_projection_property_selection() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // Create schema
    db.execute("CREATE LABEL Person (name STRING, age INT32, city STRING)")
        .await?;

    // Create a person node with properties
    db.execute("CREATE (:Person {name: 'Alice', age: 30, city: 'NYC'})")
        .await?;
    db.flush().await?;

    // Test property selection
    let results = db
        .query("MATCH (p:Person) RETURN p{.name, .age} AS data")
        .await?;

    assert_eq!(results.len(), 1);
    let data = get_map(results.rows[0].value("data").unwrap());
    assert_eq!(data.get("name"), Some(&Value::String("Alice".to_string())));
    assert_eq!(data.get("age"), Some(&Value::Int(30)));
    // city should not be included
    assert!(data.get("city").is_none());

    Ok(())
}

/// Test wildcard property selection (all properties)
#[tokio::test]
async fn test_map_projection_wildcard() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.execute("CREATE LABEL Person (name STRING, age INT32, city STRING)")
        .await?;
    db.execute("CREATE (:Person {name: 'Bob', age: 25, city: 'LA'})")
        .await?;
    db.flush().await?;

    // Test wildcard selection
    let results = db.query("MATCH (p:Person) RETURN p{.*} AS data").await?;

    assert_eq!(results.len(), 1);
    let data = get_map(results.rows[0].value("data").unwrap());
    assert_eq!(data.get("name"), Some(&Value::String("Bob".to_string())));
    assert_eq!(data.get("age"), Some(&Value::Int(25)));
    assert_eq!(data.get("city"), Some(&Value::String("LA".to_string())));

    Ok(())
}

/// Test computed properties in map projection
#[tokio::test]
async fn test_map_projection_computed_properties() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.execute("CREATE LABEL Person (name STRING, age INT32)")
        .await?;
    db.execute("CREATE (:Person {name: 'Charlie', age: 35})")
        .await?;
    db.flush().await?;

    // Test computed properties
    let results = db
        .query("MATCH (p:Person) RETURN p{.name, doubled: p.age * 2} AS data")
        .await?;

    assert_eq!(results.len(), 1);
    let data = get_map(results.rows[0].value("data").unwrap());
    assert_eq!(
        data.get("name"),
        Some(&Value::String("Charlie".to_string()))
    );
    assert_eq!(data.get("doubled"), Some(&Value::Int(70)));
    // age should not be included directly
    assert!(data.get("age").is_none());

    Ok(())
}

/// Test mixed selection (wildcard + computed properties)
#[tokio::test]
async fn test_map_projection_mixed() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.execute("CREATE LABEL Person (name STRING, age INT32, city STRING)")
        .await?;
    db.execute("CREATE (:Person {name: 'Diana', age: 28, city: 'SF'})")
        .await?;
    db.flush().await?;

    // Test mixed selection
    let results = db
        .query("MATCH (p:Person) RETURN p{.*, yearsToRetirement: 65 - p.age} AS data")
        .await?;

    assert_eq!(results.len(), 1);
    let data = get_map(results.rows[0].value("data").unwrap());
    assert_eq!(data.get("name"), Some(&Value::String("Diana".to_string())));
    assert_eq!(data.get("age"), Some(&Value::Int(28)));
    assert_eq!(data.get("city"), Some(&Value::String("SF".to_string())));
    assert_eq!(data.get("yearsToRetirement"), Some(&Value::Int(37)));

    Ok(())
}

/// Test map projection in WHERE clause context
#[tokio::test]
async fn test_map_projection_with_where() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.execute("CREATE LABEL Person (name STRING, age INT32, city STRING)")
        .await?;
    db.execute("CREATE (:Person {name: 'Eve', age: 40, city: 'Boston'})")
        .await?;
    db.execute("CREATE (:Person {name: 'Frank', age: 20, city: 'Seattle'})")
        .await?;
    db.flush().await?;

    // Test map projection with WHERE clause
    let results = db
        .query("MATCH (p:Person) WHERE p.age > 30 RETURN p{.name, .city} AS data")
        .await?;

    assert_eq!(results.len(), 1);
    let data = get_map(results.rows[0].value("data").unwrap());
    assert_eq!(data.get("name"), Some(&Value::String("Eve".to_string())));
    assert_eq!(data.get("city"), Some(&Value::String("Boston".to_string())));

    Ok(())
}

/// Test map projection with ORDER BY and LIMIT
#[tokio::test]
async fn test_map_projection_with_order_limit() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.execute("CREATE LABEL Person (name STRING, age INT32)")
        .await?;
    db.execute("CREATE (:Person {name: 'Grace', age: 45})")
        .await?;
    db.execute("CREATE (:Person {name: 'Henry', age: 50})")
        .await?;
    db.execute("CREATE (:Person {name: 'Ivy', age: 42})")
        .await?;
    db.flush().await?;

    // Test with ORDER BY and LIMIT
    let results = db
        .query("MATCH (p:Person) RETURN p{.name, .age} AS data ORDER BY p.age DESC LIMIT 2")
        .await?;

    assert_eq!(results.len(), 2);
    let data0 = get_map(results.rows[0].value("data").unwrap());
    let data1 = get_map(results.rows[1].value("data").unwrap());
    assert_eq!(data0.get("name"), Some(&Value::String("Henry".to_string())));
    assert_eq!(data0.get("age"), Some(&Value::Int(50)));
    assert_eq!(data1.get("name"), Some(&Value::String("Grace".to_string())));
    assert_eq!(data1.get("age"), Some(&Value::Int(45)));

    Ok(())
}

/// Test map projection with multiple nodes
#[tokio::test]
async fn test_map_projection_multiple_nodes() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.execute("CREATE LABEL Person (name STRING, age INT32)")
        .await?;
    db.execute("CREATE (:Person {name: 'Kate', age: 29})")
        .await?;
    db.execute("CREATE (:Person {name: 'Leo', age: 31})")
        .await?;
    db.execute("CREATE (:Person {name: 'Mia', age: 27})")
        .await?;
    db.flush().await?;

    // Test with multiple nodes
    let results = db
        .query("MATCH (p:Person) RETURN p{.name} AS data ORDER BY p.name")
        .await?;

    assert_eq!(results.len(), 3);
    let data0 = get_map(results.rows[0].value("data").unwrap());
    let data1 = get_map(results.rows[1].value("data").unwrap());
    let data2 = get_map(results.rows[2].value("data").unwrap());
    assert_eq!(data0.get("name"), Some(&Value::String("Kate".to_string())));
    assert_eq!(data1.get("name"), Some(&Value::String("Leo".to_string())));
    assert_eq!(data2.get("name"), Some(&Value::String("Mia".to_string())));

    Ok(())
}

/// Test map projection with string concatenation in computed property
#[tokio::test]
async fn test_map_projection_string_concat() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.execute("CREATE LABEL Person (firstName STRING, lastName STRING)")
        .await?;
    db.execute("CREATE (:Person {firstName: 'Nora', lastName: 'Smith'})")
        .await?;
    db.flush().await?;

    // Test string concatenation in computed property
    let results = db
        .query("MATCH (p:Person) RETURN p{fullName: p.firstName + ' ' + p.lastName} AS data")
        .await?;

    assert_eq!(results.len(), 1);
    let data = get_map(results.rows[0].value("data").unwrap());
    assert_eq!(
        data.get("fullName"),
        Some(&Value::String("Nora Smith".to_string()))
    );

    Ok(())
}

/// Test map projection with edges (without properties for now)
#[tokio::test]
async fn test_map_projection_with_edges() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.execute("CREATE LABEL Person (name STRING)").await?;
    db.execute("CREATE EDGE TYPE FRIEND_OF () FROM Person TO Person")
        .await?;

    // Create nodes first
    db.execute("CREATE (a:Person {name: 'Oscar'})").await?;
    db.execute("CREATE (b:Person {name: 'Paula'})").await?;

    // Create edge
    db.execute(
        "MATCH (a:Person {name: 'Oscar'}), (b:Person {name: 'Paula'}) CREATE (a)-[:FRIEND_OF]->(b)",
    )
    .await?;
    db.flush().await?;

    // Test basic map projection syntax on relationship (even if properties are empty)
    let results = db
        .query("MATCH (a:Person)-[r:FRIEND_OF]->(b:Person) RETURN r{.*} AS relData")
        .await?;

    assert_eq!(results.len(), 1);
    // Just verify the query executes without error
    let _rel_data = results.rows[0].value("relData").unwrap();

    Ok(())
}

/// Test multiple map projections in the same query
#[tokio::test]
async fn test_map_projection_multiple() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.execute("CREATE LABEL Person (name STRING, age INT32, city STRING)")
        .await?;

    // Create two nodes
    db.execute("CREATE (a:Person {name: 'Quinn', age: 36, city: 'Portland'})")
        .await?;
    db.execute("CREATE (b:Person {name: 'Rose', age: 38, city: 'Seattle'})")
        .await?;
    db.flush().await?;

    // Test multiple map projections in the same RETURN clause
    let results = db
        .query(
            "MATCH (a:Person), (b:Person) WHERE a.name = 'Quinn' AND b.name = 'Rose' RETURN a{.name, .city} AS person1, b{.name, .age} AS person2",
        )
        .await?;

    assert_eq!(results.len(), 1);
    let person1 = get_map(results.rows[0].value("person1").unwrap());
    let person2 = get_map(results.rows[0].value("person2").unwrap());
    assert_eq!(
        person1.get("name"),
        Some(&Value::String("Quinn".to_string()))
    );
    assert_eq!(
        person1.get("city"),
        Some(&Value::String("Portland".to_string()))
    );
    assert_eq!(
        person2.get("name"),
        Some(&Value::String("Rose".to_string()))
    );
    assert_eq!(person2.get("age"), Some(&Value::Int(38)));

    Ok(())
}
