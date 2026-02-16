// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use std::sync::Arc;

use uni_db::query::planner::QueryPlanner;
use uni_db::{DataType, Uni};

#[tokio::test]
async fn test_valid_at_function() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Event")
        .property("id", DataType::Int64)
        .property("valid_from", DataType::Timestamp)
        .property_nullable("valid_to", DataType::Timestamp)
        .apply()
        .await?;

    // Create events with validity
    // e1: valid [2023-01-01, 2023-01-02)
    db.execute("CREATE (e:Event {id: 1, valid_from: datetime('2023-01-01T00:00:00Z'), valid_to: datetime('2023-01-02T00:00:00Z')})").await?;
    // e2: valid [2023-01-02, infinity)
    db.execute(
        "CREATE (e:Event {id: 2, valid_from: datetime('2023-01-02T00:00:00Z'), valid_to: null})",
    )
    .await?;

    // Test 1: Function call
    // Query at 2023-01-01T12:00:00Z. Should match e1.
    let results = db
        .query(
            "
        MATCH (e:Event) 
        WHERE uni.temporal.validAt(e, 'valid_from', 'valid_to', datetime('2023-01-01T12:00:00Z')) 
        RETURN e.id 
        ORDER BY e.id
    ",
        )
        .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results.rows[0].get::<i64>("e.id")?, 1);

    // Query at 2023-01-02T12:00:00Z. Should match e2.
    let results = db
        .query(
            "
        MATCH (e:Event) 
        WHERE uni.temporal.validAt(e, 'valid_from', 'valid_to', datetime('2023-01-02T12:00:00Z')) 
        RETURN e.id
    ",
        )
        .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results.rows[0].get::<i64>("e.id")?, 2);

    // Query at 2023-01-05. Should match e2.
    let results = db
        .query(
            "
        MATCH (e:Event) 
        WHERE uni.temporal.validAt(e, 'valid_from', 'valid_to', datetime('2023-01-05T00:00:00Z')) 
        RETURN e.id
    ",
        )
        .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results.rows[0].get::<i64>("e.id")?, 2);

    Ok(())
}

#[tokio::test]
async fn test_valid_at_macro() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Event")
        .property("id", DataType::Int64)
        .property("valid_from", DataType::Timestamp)
        .property_nullable("valid_to", DataType::Timestamp)
        .apply()
        .await?;

    // e1: valid [2023-01-01, 2023-01-02)
    db.execute("CREATE (e:Event {id: 1, valid_from: datetime('2023-01-01T00:00:00Z'), valid_to: datetime('2023-01-02T00:00:00Z')})").await?;
    // e2: valid [2023-01-02, infinity)
    db.execute(
        "CREATE (e:Event {id: 2, valid_from: datetime('2023-01-02T00:00:00Z'), valid_to: null})",
    )
    .await?;

    // Test 2: Macro simple
    // MATCH (e:Event) WHERE e VALID_AT datetime(...)
    let results = db
        .query(
            "
        MATCH (e:Event) 
        WHERE e VALID_AT datetime('2023-01-03T00:00:00Z') 
        RETURN e.id
    ",
        )
        .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results.rows[0].get::<i64>("e.id")?, 2);

    // Test macro with valid_from/valid_to defaults on e1 (should fail query at 2023-01-03)
    let results = db
        .query(
            "
        MATCH (e:Event) 
        WHERE e.id = 1 AND e VALID_AT datetime('2023-01-03T00:00:00Z') 
        RETURN e.id
    ",
        )
        .await?;
    assert_eq!(results.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_valid_at_macro_custom_props() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Task")
        .property("id", DataType::Int64)
        .property("start", DataType::Timestamp)
        .property("end", DataType::Timestamp)
        .apply()
        .await?;

    // Test 3: Macro with custom props
    db.execute("CREATE (t:Task {id: 3, start: datetime('2023-01-01T00:00:00Z'), end: datetime('2023-01-05T00:00:00Z')})").await?;

    // MATCH (t:Task) WHERE t VALID_AT(datetime(...), 'start', 'end')
    let results = db
        .query(
            "
        MATCH (t:Task) 
        WHERE t VALID_AT(datetime('2023-01-03T00:00:00Z'), 'start', 'end') 
        RETURN t.id
    ",
        )
        .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results.rows[0].get::<i64>("t.id")?, 3);

    // Query outside range
    let results = db
        .query(
            "
        MATCH (t:Task)
        WHERE t VALID_AT(datetime('2023-01-06T00:00:00Z'), 'start', 'end')
        RETURN t.id
    ",
        )
        .await?;
    assert_eq!(results.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_valid_at_edge_temporal() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create schema with temporal edges
    db.schema()
        .label("Person")
        .property("name", DataType::String)
        .apply()
        .await?;

    db.schema()
        .label("Company")
        .property("name", DataType::String)
        .apply()
        .await?;

    db.schema()
        .edge_type("EMPLOYED_BY", &["Person"], &["Company"])
        .property("valid_from", DataType::Timestamp)
        .property_nullable("valid_to", DataType::Timestamp)
        .property("role", DataType::String)
        .apply()
        .await?;

    // Create nodes
    db.execute("CREATE (p:Person {name: 'Alice'})").await?;
    db.execute("CREATE (c:Company {name: 'Acme Corp'})").await?;
    db.execute("CREATE (c:Company {name: 'Globex Inc'})")
        .await?;

    // Create employment history with temporal validity
    // Job 1: 2020-01-01 to 2022-06-30 at Acme Corp
    db.execute(
        "
        MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'Acme Corp'})
        CREATE (p)-[:EMPLOYED_BY {
            valid_from: datetime('2020-01-01T00:00:00Z'),
            valid_to: datetime('2022-06-30T00:00:00Z'),
            role: 'Engineer'
        }]->(c)
    ",
    )
    .await?;

    // Job 2: 2022-07-01 to present at Globex Inc
    db.execute(
        "
        MATCH (p:Person {name: 'Alice'}), (c:Company {name: 'Globex Inc'})
        CREATE (p)-[:EMPLOYED_BY {
            valid_from: datetime('2022-07-01T00:00:00Z'),
            valid_to: null,
            role: 'Senior Engineer'
        }]->(c)
    ",
    )
    .await?;

    // Flush to ensure edges are persisted
    db.flush().await?;

    // Debug: Check if edges exist without WHERE clause
    let debug_results = db
        .query("MATCH (p:Person {name: 'Alice'})-[e:EMPLOYED_BY]->(c:Company) RETURN c.name, e.role, e.valid_from, e.valid_to, e")
        .await?;
    println!("Debug: Found {} edges", debug_results.len());
    for row in &debug_results.rows {
        println!(
            "  Edge properties: company={:?}, role={:?}, valid_from={:?}, valid_to={:?}",
            row.values.first(),
            row.values.get(1),
            row.values.get(2),
            row.values.get(3)
        );
        println!("  Full edge object: {:?}", row.values.get(4));
    }

    // Test: Query where Alice worked in 2021 (should be Acme Corp)
    // Using VALID_AT macro syntax
    let results = db
        .query(
            "
        MATCH (p:Person {name: 'Alice'})-[e:EMPLOYED_BY]->(c:Company)
        WHERE e VALID_AT(datetime('2021-06-15T00:00:00Z'), 'valid_from', 'valid_to')
        RETURN c.name AS company, e.role AS role
    ",
        )
        .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results.rows[0].get::<String>("company")?, "Acme Corp");
    assert_eq!(results.rows[0].get::<String>("role")?, "Engineer");

    // Test: Query where Alice works in 2024 (should be Globex Inc)
    // Using VALID_AT macro syntax
    let results = db
        .query(
            "
        MATCH (p:Person {name: 'Alice'})-[e:EMPLOYED_BY]->(c:Company)
        WHERE e VALID_AT(datetime('2024-01-15T00:00:00Z'), 'valid_from', 'valid_to')
        RETURN c.name AS company, e.role AS role
    ",
        )
        .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results.rows[0].get::<String>("company")?, "Globex Inc");
    assert_eq!(results.rows[0].get::<String>("role")?, "Senior Engineer");

    // Test: Query exactly at transition point (2022-06-30 should still match Acme, 2022-07-01 should match Globex)
    // At 2022-06-30T00:00:00Z: [2020-01-01, 2022-06-30) does NOT include 2022-06-30
    let results = db
        .query(
            "
        MATCH (p:Person {name: 'Alice'})-[e:EMPLOYED_BY]->(c:Company)
        WHERE uni.temporal.validAt(e, 'valid_from', 'valid_to', datetime('2022-06-30T00:00:00Z'))
        RETURN c.name AS company
    ",
        )
        .await?;
    assert_eq!(
        results.len(),
        0,
        "Half-open interval should not include end date"
    );

    // At 2022-06-29T23:59:59Z: should still be at Acme
    let results = db
        .query(
            "
        MATCH (p:Person {name: 'Alice'})-[e:EMPLOYED_BY]->(c:Company)
        WHERE uni.temporal.validAt(e, 'valid_from', 'valid_to', datetime('2022-06-29T23:59:59Z'))
        RETURN c.name AS company
    ",
        )
        .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results.rows[0].get::<String>("company")?, "Acme Corp");

    Ok(())
}

#[tokio::test]
async fn test_valid_at_boundary_conditions() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Contract")
        .property("id", DataType::Int64)
        .property("valid_from", DataType::Timestamp)
        .property_nullable("valid_to", DataType::Timestamp)
        .apply()
        .await?;

    // Contract valid exactly from start to end
    db.execute("CREATE (c:Contract {id: 1, valid_from: datetime('2023-01-01T00:00:00Z'), valid_to: datetime('2023-12-31T23:59:59Z')})").await?;

    // Test: Exactly at start time (should be valid)
    let results = db
        .query(
            "
        MATCH (c:Contract)
        WHERE c VALID_AT datetime('2023-01-01T00:00:00Z')
        RETURN c.id
    ",
        )
        .await?;
    assert_eq!(results.len(), 1, "Should be valid at exact start time");

    // Test: One second before start (should be invalid)
    let results = db
        .query(
            "
        MATCH (c:Contract)
        WHERE c VALID_AT datetime('2022-12-31T23:59:59Z')
        RETURN c.id
    ",
        )
        .await?;
    assert_eq!(results.len(), 0, "Should be invalid before start time");

    // Test: Exactly at end time (should be invalid - half-open interval)
    let results = db
        .query(
            "
        MATCH (c:Contract)
        WHERE c VALID_AT datetime('2023-12-31T23:59:59Z')
        RETURN c.id
    ",
        )
        .await?;
    assert_eq!(
        results.len(),
        0,
        "Should be invalid at exact end time (half-open interval)"
    );

    // Test: One second before end (should be valid)
    let results = db
        .query(
            "
        MATCH (c:Contract)
        WHERE c VALID_AT datetime('2023-12-31T23:59:58Z')
        RETURN c.id
    ",
        )
        .await?;
    assert_eq!(
        results.len(),
        1,
        "Should be valid one second before end time"
    );

    Ok(())
}

#[tokio::test]
async fn test_valid_at_open_ended() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Subscription")
        .property("id", DataType::Int64)
        .property("valid_from", DataType::Timestamp)
        .property_nullable("valid_to", DataType::Timestamp)
        .apply()
        .await?;

    // Active subscription with no end date
    db.execute("CREATE (s:Subscription {id: 1, valid_from: datetime('2020-01-01T00:00:00Z'), valid_to: null})").await?;
    // Cancelled subscription
    db.execute("CREATE (s:Subscription {id: 2, valid_from: datetime('2020-01-01T00:00:00Z'), valid_to: datetime('2022-01-01T00:00:00Z')})").await?;

    // Test: Far future date should only match open-ended subscription
    let results = db
        .query(
            "
        MATCH (s:Subscription)
        WHERE s VALID_AT datetime('2050-01-01T00:00:00Z')
        RETURN s.id
        ORDER BY s.id
    ",
        )
        .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results.rows[0].get::<i64>("s.id")?, 1);

    // Test: Date when both were active
    let results = db
        .query(
            "
        MATCH (s:Subscription)
        WHERE s VALID_AT datetime('2021-06-15T00:00:00Z')
        RETURN s.id
        ORDER BY s.id
    ",
        )
        .await?;
    assert_eq!(results.len(), 2);

    Ok(())
}

#[tokio::test]
async fn test_valid_at_index_suggestion_function() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Event")
        .property("id", DataType::Int64)
        .property("valid_from", DataType::Timestamp)
        .property_nullable("valid_to", DataType::Timestamp)
        .apply()
        .await?;

    // Parse and plan a query with uni.temporal.validAt() - no index exists
    let query = "MATCH (e:Event) WHERE uni.temporal.validAt(e, 'valid_from', 'valid_to', datetime('2023-01-01T00:00:00Z')) RETURN e.id";
    let ast = uni_query::parse_cypher(query)?;

    let schema = db.schema_manager().schema();
    let planner = QueryPlanner::new(Arc::new(schema.clone()));
    let explain = planner.explain_plan(ast)?;

    // Should suggest an index on valid_from
    assert!(
        !explain.suggestions.is_empty(),
        "Should have index suggestions for temporal query without index"
    );

    let suggestion = &explain.suggestions[0];
    assert_eq!(suggestion.property, "valid_from");
    assert!(suggestion.index_type.contains("SCALAR"));
    assert!(suggestion.reason.contains("Temporal queries"));
    assert!(suggestion.create_statement.contains("CREATE INDEX"));

    Ok(())
}

#[tokio::test]
async fn test_valid_at_index_suggestion_macro() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Contract")
        .property("id", DataType::Int64)
        .property("valid_from", DataType::Timestamp)
        .property_nullable("valid_to", DataType::Timestamp)
        .apply()
        .await?;

    // Parse and plan a query with VALID_AT macro - no index exists
    let query = "MATCH (c:Contract) WHERE c VALID_AT datetime('2023-06-15T00:00:00Z') RETURN c.id";
    let ast = uni_query::parse_cypher(query)?;

    let schema = db.schema_manager().schema();
    let planner = QueryPlanner::new(Arc::new(schema.clone()));
    let explain = planner.explain_plan(ast)?;

    // Should suggest an index on valid_from (default property name)
    assert!(
        !explain.suggestions.is_empty(),
        "Should have index suggestions for VALID_AT macro without index"
    );

    let suggestion = &explain.suggestions[0];
    assert_eq!(suggestion.property, "valid_from");

    Ok(())
}

#[tokio::test]
async fn test_valid_at_no_suggestion_with_index() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Task")
        .property("id", DataType::Int64)
        .property("valid_from", DataType::Timestamp)
        .property_nullable("valid_to", DataType::Timestamp)
        .apply()
        .await?;

    // Create a scalar index on valid_from
    db.execute("CREATE INDEX idx_valid_from FOR (t:Task) ON (t.valid_from)")
        .await?;

    // Parse and plan a query - index now exists
    let query = "MATCH (t:Task) WHERE t VALID_AT datetime('2023-01-01T00:00:00Z') RETURN t.id";
    let ast = uni_query::parse_cypher(query)?;

    let schema = db.schema_manager().schema();
    let planner = QueryPlanner::new(Arc::new(schema.clone()));
    let explain = planner.explain_plan(ast)?;

    // Should NOT suggest an index since one exists
    let valid_from_suggestions: Vec<_> = explain
        .suggestions
        .iter()
        .filter(|s| s.property == "valid_from")
        .collect();

    assert!(
        valid_from_suggestions.is_empty(),
        "Should NOT suggest index when one already exists"
    );

    Ok(())
}

#[tokio::test]
async fn test_valid_at_custom_prop_suggestion() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.schema()
        .label("Project")
        .property("id", DataType::Int64)
        .property("start_date", DataType::Timestamp)
        .property_nullable("end_date", DataType::Timestamp)
        .apply()
        .await?;

    // Parse and plan a query with custom property names
    let query = "MATCH (p:Project) WHERE p VALID_AT(datetime('2023-01-01T00:00:00Z'), 'start_date', 'end_date') RETURN p.id";
    let ast = uni_query::parse_cypher(query)?;

    let schema = db.schema_manager().schema();
    let planner = QueryPlanner::new(Arc::new(schema.clone()));
    let explain = planner.explain_plan(ast)?;

    // Should suggest an index on start_date
    let start_date_suggestions: Vec<_> = explain
        .suggestions
        .iter()
        .filter(|s| s.property == "start_date")
        .collect();

    assert!(
        !start_date_suggestions.is_empty(),
        "Should suggest index for custom start property 'start_date'"
    );

    Ok(())
}
