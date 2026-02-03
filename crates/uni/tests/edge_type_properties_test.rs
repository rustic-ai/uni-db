// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Test CREATE EDGE TYPE with inline property definitions

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_create_edge_type_with_properties() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // Create labels first
    db.execute("CREATE LABEL Person (name STRING)").await?;
    db.execute("CREATE LABEL Company (name STRING)").await?;

    // Test 1: CREATE EDGE TYPE with single property
    db.execute("CREATE EDGE TYPE WORKS_AT (since INT) FROM Person TO Company")
        .await?;

    let schema = db.get_schema();
    assert!(schema.edge_types.contains_key("WORKS_AT"));
    let works_at_props = schema.properties.get("WORKS_AT").unwrap();
    assert!(works_at_props.contains_key("since"));
    assert_eq!(
        works_at_props.get("since").unwrap().r#type,
        uni_db::DataType::Int32
    );

    // Test 2: CREATE EDGE TYPE with multiple properties
    db.execute(
        "CREATE EDGE TYPE KNOWS (since INT, strength FLOAT, verified BOOLEAN)
         FROM Person TO Person",
    )
    .await?;

    let schema = db.get_schema();
    assert!(schema.edge_types.contains_key("KNOWS"));
    let knows_props = schema.properties.get("KNOWS").unwrap();
    assert!(knows_props.contains_key("since"));
    assert!(knows_props.contains_key("strength"));
    assert!(knows_props.contains_key("verified"));

    // Test 3: CREATE EDGE TYPE without properties (empty parentheses)
    db.execute("CREATE EDGE TYPE FOLLOWS () FROM Person TO Person")
        .await?;

    let schema = db.get_schema();
    assert!(schema.edge_types.contains_key("FOLLOWS"));
    // No properties should be defined for FOLLOWS
    let follows_props = schema.properties.get("FOLLOWS");
    assert!(follows_props.is_none() || follows_props.unwrap().is_empty());

    // Test 4: CREATE EDGE TYPE without parentheses at all
    db.execute("CREATE EDGE TYPE LIKES FROM Person TO Company")
        .await?;

    let schema = db.get_schema();
    assert!(schema.edge_types.contains_key("LIKES"));

    println!("✅ All CREATE EDGE TYPE property definition tests passed!");

    Ok(())
}

#[tokio::test]
async fn test_create_edge_type_with_property_constraints() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    db.execute("CREATE LABEL User (name STRING)").await?;

    // Test nullable and non-nullable properties
    db.execute(
        "CREATE EDGE TYPE RATED (score INT NOT NULL, comment STRING)
         FROM User TO User",
    )
    .await?;

    let schema = db.get_schema();
    let rated_props = schema.properties.get("RATED").unwrap();

    // score should be NOT NULL
    assert!(!rated_props.get("score").unwrap().nullable);

    // comment should be nullable (default)
    assert!(rated_props.get("comment").unwrap().nullable);

    println!("✅ Property constraint tests passed!");

    Ok(())
}
