// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use anyhow::Result;
use uni_db::Uni;

#[tokio::test]
async fn test_composite_index_creation() -> Result<()> {
    let db = Uni::in_memory().build().await?;

    // 1. Create label
    db.execute("CREATE LABEL Person (firstName STRING, lastName STRING, age INT)")
        .await?;

    // 2. Create composite index
    db.execute("CREATE INDEX idx_name FOR (p:Person) ON (p.lastName, p.firstName)")
        .await?;

    // 3. Verify schema
    let schema = db.get_schema();
    let index = schema
        .indexes
        .iter()
        .find(|idx| {
            if let uni_db::core::schema::IndexDefinition::Scalar(config) = idx {
                config.name == "idx_name"
            } else {
                false
            }
        })
        .expect("Index not found");

    if let uni_db::core::schema::IndexDefinition::Scalar(config) = index {
        assert_eq!(
            config.properties,
            vec!["lastName".to_string(), "firstName".to_string()]
        );
    }

    // 4. Insert data
    db.execute("CREATE (:Person {firstName: 'Alice', lastName: 'Smith', age: 30})")
        .await?;
    db.execute("CREATE (:Person {firstName: 'Bob', lastName: 'Smith', age: 40})")
        .await?;

    // 5. Query using composite predicates (Planner should push these)
    let result = db.query("MATCH (p:Person) WHERE p.lastName = 'Smith' AND p.firstName = 'Alice' RETURN p.age AS age").await?;
    assert_eq!(result.len(), 1);
    assert_eq!(result.rows()[0].get::<i64>("age")?, 30);

    // 6. Create Partial Index
    db.execute("CREATE LABEL User (email STRING, active BOOL)")
        .await?;
    db.execute("CREATE INDEX idx_active FOR (u:User) ON (u.email) WHERE u.active = true")
        .await?;

    // Verify partial index metadata
    let schema = db.get_schema();
    let partial_idx = schema
        .indexes
        .iter()
        .find(|idx| {
            if let uni_db::core::schema::IndexDefinition::Scalar(config) = idx {
                config.name == "idx_active"
            } else {
                false
            }
        })
        .expect("Partial index not found");

    if let uni_db::core::schema::IndexDefinition::Scalar(config) = partial_idx {
        assert_eq!(config.properties, vec!["email".to_string()]);
        assert!(config.where_clause.is_some());
        assert!(config.where_clause.as_ref().unwrap().contains("active"));
    }

    Ok(())
}
