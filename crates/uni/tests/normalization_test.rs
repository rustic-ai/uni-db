// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Test that result normalization converts internal representations to proper Node/Edge types

use anyhow::Result;
use uni_db::{Uni, Value};

#[tokio::test]
async fn test_match_returns_node_type() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // Create schema and data
    db.execute("CREATE LABEL Person (name STRING, age INT32)")
        .await?;
    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .await?;
    db.execute("CREATE (:Person {name: 'Bob', age: 25})")
        .await?;
    db.flush().await?;

    // Test: MATCH should return Node types (not Map with _vid/_label)
    let results = db
        .query("MATCH (n:Person) RETURN n ORDER BY n.name")
        .await?;

    assert_eq!(results.len(), 2, "Expected 2 results");

    // Check first result
    let node1 = results.rows[0].value("n").unwrap();
    match node1 {
        Value::Node(node) => {
            assert_eq!(node.label, "Person");
            assert_eq!(
                node.properties.get("name"),
                Some(&Value::String("Alice".to_string()))
            );
            assert_eq!(node.properties.get("age"), Some(&Value::Int(30)));
            // Verify internal fields are NOT in properties
            assert!(!node.properties.contains_key("_vid"));
            assert!(!node.properties.contains_key("_label"));
        }
        other => panic!("Expected Value::Node, got {:?}", other),
    }

    // Check second result
    let node2 = results.rows[1].value("n").unwrap();
    match node2 {
        Value::Node(node) => {
            assert_eq!(node.label, "Person");
            assert_eq!(
                node.properties.get("name"),
                Some(&Value::String("Bob".to_string()))
            );
            assert_eq!(node.properties.get("age"), Some(&Value::Int(25)));
        }
        other => panic!("Expected Value::Node, got {:?}", other),
    }

    Ok(())
}

#[tokio::test]
async fn test_match_with_edge_returns_proper_types() -> Result<()> {
    let db = Uni::temporary().build().await?;

    // Create schema
    db.execute("CREATE LABEL Person (name STRING)").await?;
    db.execute("CREATE EDGE TYPE KNOWS () FROM Person TO Person")
        .await?;

    // Create nodes and edge
    db.execute("CREATE (a:Person {name: 'Alice'})").await?;
    db.execute("CREATE (b:Person {name: 'Bob'})").await?;
    db.execute(
        "MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) CREATE (a)-[:KNOWS]->(b)",
    )
    .await?;
    db.flush().await?;

    // Test: Match with edge should return Node and Edge types
    let results = db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a, r, b")
        .await?;

    assert_eq!(results.len(), 1, "Expected 1 result");

    let row = &results.rows[0];

    // Check node a
    match row.value("a").unwrap() {
        Value::Node(node) => {
            assert_eq!(node.label, "Person");
            assert_eq!(
                node.properties.get("name"),
                Some(&Value::String("Alice".to_string()))
            );
        }
        other => panic!("Expected Value::Node for 'a', got {:?}", other),
    }

    // Check edge r
    match row.value("r").unwrap() {
        Value::Edge(edge) => {
            assert_eq!(edge.edge_type, "KNOWS");
            // Verify internal fields are NOT in properties
            assert!(!edge.properties.contains_key("_eid"));
            assert!(!edge.properties.contains_key("_type"));
        }
        other => panic!("Expected Value::Edge for 'r', got {:?}", other),
    }

    // Check node b
    match row.value("b").unwrap() {
        Value::Node(node) => {
            assert_eq!(node.label, "Person");
            assert_eq!(
                node.properties.get("name"),
                Some(&Value::String("Bob".to_string()))
            );
        }
        other => panic!("Expected Value::Node for 'b', got {:?}", other),
    }

    Ok(())
}

#[tokio::test]
async fn test_property_access_still_works() -> Result<()> {
    let db = Uni::temporary().build().await?;

    db.execute("CREATE LABEL Person (name STRING, age INT32)")
        .await?;
    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .await?;
    db.flush().await?;

    // Test: Property access should still work after normalization
    let results = db
        .query("MATCH (n:Person) RETURN n.name AS name, n.age AS age")
        .await?;

    assert_eq!(results.len(), 1);

    let row = &results.rows[0];
    assert_eq!(row.value("name"), Some(&Value::String("Alice".to_string())));
    assert_eq!(row.value("age"), Some(&Value::Int(30)));

    Ok(())
}
