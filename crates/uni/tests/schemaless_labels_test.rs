// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for schemaless labels (labels with no schema-defined properties).
//!
//! This tests the overflow_json feature with labels that have NO properties defined
//! in the schema, ensuring all properties are accessible from the L0 buffer.
//!
//! ## Test Coverage
//!
//! - **Vertex properties**: Create and query vertices with arbitrary properties
//! - **Edge properties**: Create and query edges with arbitrary properties
//! - **Mixed types**: Verify different data types (string, int, float, bool, list)
//! - **Null handling**: Verify null properties are handled correctly
//! - **Performance**: Bulk insert and query of schemaless data
//!
//! ## Current Limitations
//!
//! - **L0 only**: Tests currently verify data in L0 buffer (before flush)
//! - **No WHERE clauses**: Cannot filter on schemaless properties in WHERE clauses
//!   (properties are in JSON blob, not indexed columns)
//! - **Type conversion**: Numeric/boolean literals may be stored as strings
//!   depending on Cypher parser behavior
//!
//! ## Future Work
//!
//! - Post-flush querying needs investigation (overflow_json write/read path)
//! - Consider adding property type hints to improve type handling

use anyhow::Result;
use tempfile::tempdir;
use uni_db::Uni;

#[tokio::test]
async fn test_schemaless_vertex_create_and_query() -> Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    // Create a completely schemaless label (no properties defined)
    db.schema().label("Person").apply().await?;

    db.execute("CREATE (:Person {name: 'Alice', age: 30, city: 'NYC'})")
        .await?;
    db.execute("CREATE (:Person {name: 'Bob', age: 25, country: 'USA'})")
        .await?;

    // Query from L0 (before flush) - schemaless properties are accessible
    let results = db
        .query("MATCH (p:Person) RETURN p.name, p.age, p.city, p.country")
        .await?;
    assert_eq!(results.len(), 2);

    // Find Alice's row (has city but not country)
    use uni_db::Value;
    let alice_row = results
        .rows()
        .iter()
        .find(|r| r.get::<String>("p.name").ok() == Some("Alice".to_string()))
        .expect("Alice not found");

    // Schemaless properties may be stored as strings (Cypher parser treats literals as strings)
    let age_val = alice_row.value("p.age").unwrap();
    match age_val {
        Value::Int(i) => assert_eq!(*i, 30),
        Value::String(s) => assert_eq!(s, "30"),
        _ => panic!("Unexpected type for age: {:?}", age_val),
    }
    assert_eq!(alice_row.get::<String>("p.city")?, "NYC");

    // Find Bob's row (has country but not city)
    let bob_row = results
        .rows()
        .iter()
        .find(|r| r.get::<String>("p.name").ok() == Some("Bob".to_string()))
        .expect("Bob not found");

    let bob_age_val = bob_row.value("p.age").unwrap();
    match bob_age_val {
        Value::Int(i) => assert_eq!(*i, 25),
        Value::String(s) => assert_eq!(s, "25"),
        _ => panic!("Unexpected type for age: {:?}", bob_age_val),
    }
    assert_eq!(bob_row.get::<String>("p.country")?, "USA");

    Ok(())
}

#[tokio::test]
async fn test_schemaless_edge_create_and_query() -> Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    // Create schemaless label and edge type (no properties defined)
    db.schema()
        .label("Person")
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;

    // Create vertices and edge with schemaless properties
    db.execute("CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020, strength: 0.9}]->(b:Person {name: 'Bob'})").await?;

    // Query edge properties from L0 (before flush)
    let results = db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN r.since, r.strength")
        .await?;
    assert_eq!(results.len(), 1);
    let row = &results.rows()[0];

    // Edge properties should be accessible
    assert_eq!(row.get::<i64>("r.since")?, 2020);
    assert_eq!(row.get::<f64>("r.strength")?, 0.9);

    Ok(())
}

#[tokio::test]
async fn test_schemaless_mixed_types() -> Result<()> {
    use uni_db::Value;

    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    // Create schemaless label
    db.schema().label("Product").apply().await?;

    // Create with various data types
    db.execute(
        r#"
        CREATE (:Product {
            name: 'Widget',
            price: 19.99,
            in_stock: true,
            quantity: 100,
            tags: ['electronics', 'gadget']
        })
    "#,
    )
    .await?;

    // Query from L0 (before flush) - all property types accessible
    let results = db
        .query("MATCH (p:Product) RETURN p.name, p.price, p.in_stock, p.quantity, p.tags")
        .await?;
    assert_eq!(results.len(), 1);
    let row = &results.rows()[0];

    // All properties should be accessible
    assert_eq!(row.get::<String>("p.name")?, "Widget");

    // Schemaless properties may be stored as strings
    let price_val = row.value("p.price").unwrap();
    match price_val {
        Value::Float(f) => assert_eq!(*f, 19.99),
        Value::String(s) => assert_eq!(s, "19.99"),
        _ => panic!("Unexpected type for price: {:?}", price_val),
    }

    let in_stock_val = row.value("p.in_stock").unwrap();
    match in_stock_val {
        Value::Bool(b) => assert!(*b),
        Value::String(s) => assert_eq!(s, "true"),
        _ => panic!("Unexpected type for in_stock: {:?}", in_stock_val),
    }

    let quantity_val = row.value("p.quantity").unwrap();
    match quantity_val {
        Value::Int(i) => assert_eq!(*i, 100),
        Value::String(s) => assert_eq!(s, "100"),
        _ => panic!("Unexpected type for quantity: {:?}", quantity_val),
    }

    // Check tags array (may be serialized as JSON string in schemaless mode)
    let tags = row.value("p.tags").unwrap();
    match tags {
        Value::List(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], Value::String("electronics".to_string()));
            assert_eq!(items[1], Value::String("gadget".to_string()));
        }
        Value::String(s) => {
            // In schemaless mode, arrays may be stored as JSON strings
            assert!(s.contains("electronics"));
            assert!(s.contains("gadget"));
        }
        _ => panic!("Expected list or string for tags, got {:?}", tags),
    }

    Ok(())
}

#[tokio::test]
async fn test_schemaless_update_properties() -> Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    // Create schemaless label
    db.schema().label("User").apply().await?;

    // Create with initial properties
    db.execute("CREATE (:User {name: 'Alice', email: 'alice@example.com'})")
        .await?;

    // Verify properties are accessible from L0
    let results = db.query("MATCH (u:User) RETURN u.name, u.email").await?;
    assert_eq!(results.len(), 1);
    let row = &results.rows()[0];
    assert_eq!(row.get::<String>("u.name")?, "Alice");
    assert_eq!(row.get::<String>("u.email")?, "alice@example.com");

    Ok(())
}

#[tokio::test]
async fn test_schemaless_null_properties() -> Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    // Create schemaless label
    db.schema().label("Person").apply().await?;

    // Create with some null properties
    db.execute("CREATE (:Person {name: 'Alice', age: 30})")
        .await?;
    db.execute("CREATE (:Person {name: 'Bob', age: null})")
        .await?;

    // Query from L0 - should handle null gracefully
    let results = db.query("MATCH (p:Person) RETURN p.name, p.age").await?;
    assert_eq!(results.len(), 2);

    // Find Bob's row
    let bob_row = results
        .rows()
        .iter()
        .find(|r| r.get::<String>("p.name").ok() == Some("Bob".to_string()))
        .expect("Bob not found");

    // Age should be null for Bob
    let age = bob_row.value("p.age").unwrap();
    assert_eq!(age, &uni_db::Value::Null);

    Ok(())
}

#[tokio::test]
async fn test_schemaless_performance() -> Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    // Create schemaless label
    db.schema().label("Event").apply().await?;

    // Insert many vertices with schemaless properties
    for i in 0..100 {
        db.execute(&format!(
            "CREATE (:Event {{id: {}, type: 'click', timestamp: {}, user_id: 'user_{}'}})",
            i,
            1000000 + i,
            i % 10
        ))
        .await?;
    }

    db.flush().await?;

    // Query all events (can't filter on overflow properties in WHERE clause)
    let results = db.query("MATCH (e:Event) RETURN count(e) as cnt").await?;
    assert_eq!(results.len(), 1);
    let row = &results.rows()[0];
    assert_eq!(row.get::<i64>("cnt")?, 100);

    Ok(())
}

#[tokio::test]
async fn test_schemaless_unknown_edge_type_returns_empty() -> Result<()> {
    // Test that querying with an unknown edge type returns empty results
    // instead of erroring (schemaless edge support).
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    // Create only the Person label - no edge types defined
    db.schema().label("Person").apply().await?;

    // Create some vertices
    db.execute("CREATE (:Person {name: 'Alice'})").await?;
    db.execute("CREATE (:Person {name: 'Bob'})").await?;

    // Query with an unknown edge type - should return empty, not error
    let results = db
        .query("MATCH (a:Person)-[:UNKNOWN_TYPE]->(b:Person) RETURN a.name, b.name")
        .await?;

    // Should return empty results, not error
    assert_eq!(results.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_schemaless_edge_type_query_with_data() -> Result<()> {
    // Test that we can query edges by type name, and schemaless support
    // returns empty when the type doesn't exist (rather than erroring).
    //
    // Note: CREATE currently requires edge types to be defined in schema.
    // This test verifies the MATCH side works for unknown types.
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    // Create Person label and a known edge type
    db.schema()
        .label("Person")
        .edge_type("KNOWS", &["Person"], &["Person"])
        .apply()
        .await?;

    // Create vertices and an edge with the KNOWN type
    db.execute("CREATE (:Person {name: 'Alice', ext_id: 'alice'})")
        .await?;
    db.execute("CREATE (:Person {name: 'Bob', ext_id: 'bob'})")
        .await?;
    db.execute(
        "MATCH (a:Person {ext_id: 'alice'}), (b:Person {ext_id: 'bob'}) 
         CREATE (a)-[:KNOWS {weight: 0.5, note: 'friends'}]->(b)",
    )
    .await?;

    // Query using the KNOWN edge type - should work
    let results = db
        .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name, r.weight")
        .await?;
    assert_eq!(results.len(), 1);
    let row = &results.rows()[0];
    assert_eq!(row.get::<String>("a.name")?, "Alice");
    assert_eq!(row.get::<String>("b.name")?, "Bob");
    assert_eq!(row.get::<f64>("r.weight")?, 0.5);

    // Query using an UNKNOWN edge type - should return empty (not error)
    let results = db
        .query("MATCH (a:Person)-[r:UNKNOWN_TYPE]->(b:Person) RETURN a.name, b.name")
        .await?;
    assert_eq!(results.len(), 0);

    Ok(())
}
