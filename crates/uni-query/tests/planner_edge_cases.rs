// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::sync::Arc;
use tempfile::tempdir;
use uni_common::core::schema::{DataType, SchemaManager};

use uni_query::query::planner::QueryPlanner;

/// Test that unknown labels are handled via ScanMainByLabel (schemaless support).
#[tokio::test]
async fn test_planner_missing_label() {
    let dir = tempdir().unwrap();
    let _path = dir.path().join("schema.json");
    let schema_manager = SchemaManager::load(&dir.path().join("schema.json"))
        .await
        .unwrap(); // No labels added
    let schema = Arc::new(schema_manager.schema());
    let planner = QueryPlanner::new(schema);

    let sql = "MATCH (n:NonExistent) RETURN n";
    let ast = uni_query::parse_cypher(sql).unwrap();

    let res = planner.plan(ast);
    // Now supports unknown labels via ScanMainByLabel (schemaless)
    assert!(
        res.is_ok(),
        "Planner should handle unknown labels via ScanMainByLabel"
    );
    let plan = res.unwrap();
    // Check that the plan contains ScanMainByLabel
    let plan_str = format!("{:?}", plan);
    assert!(
        plan_str.contains("ScanMainByLabel"),
        "Plan should use ScanMainByLabel for unknown labels"
    );
}

#[tokio::test]
async fn test_planner_missing_edge_type() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schema.json");
    let schema_manager = SchemaManager::load(&path).await.unwrap();
    schema_manager.add_label("Person").unwrap();
    schema_manager.save().await.unwrap();

    let schema = Arc::new(schema_manager.schema());
    let planner = QueryPlanner::new(schema);

    let sql = "MATCH (n:Person)-[:MISSING]->(m) RETURN n";
    let ast = uni_query::parse_cypher(sql).unwrap();

    let res = planner.plan(ast);
    if let Ok(plan) = &res {
        println!("Plan: {:?}", plan);
    }
    assert!(res.is_err());
}

#[tokio::test]
async fn test_planner_create_missing_label() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schema.json");
    let schema_manager = SchemaManager::load(&path).await.unwrap();
    let schema = Arc::new(schema_manager.schema());
    let planner = QueryPlanner::new(schema);

    let sql = "CREATE (n:NewThing {id: 1})";
    let ast = uni_query::parse_cypher(sql).unwrap();

    // Planner allows creating plan with unknown label (validation is at runtime)
    let res = planner.plan(ast);
    assert!(res.is_ok(), "Planner should succeed (validation deferred)");
}

#[tokio::test]
async fn test_planner_ambiguous_merge() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schema.json");
    let schema_manager = SchemaManager::load(&path).await.unwrap();
    schema_manager.add_label("Person").unwrap();
    schema_manager.save().await.unwrap();
    let schema = Arc::new(schema_manager.schema());
    let planner = QueryPlanner::new(schema);

    // MERGE without label on node
    let sql = "MERGE (n {id: 1})";
    let ast = uni_query::parse_cypher(sql).unwrap();

    // Planner allows it (validation deferred)
    let res = planner.plan(ast);
    assert!(res.is_ok(), "Planner should succeed (validation deferred)");
}

#[tokio::test]
async fn test_planner_vector_search_validation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schema.json");
    let schema_manager = SchemaManager::load(&path).await.unwrap();
    schema_manager.add_label("Doc").unwrap();
    schema_manager
        .add_property(
            "Doc",
            "embedding",
            DataType::Vector { dimensions: 128 },
            false,
        )
        .unwrap();
    schema_manager.save().await.unwrap();

    let schema = Arc::new(schema_manager.schema());
    let planner = QueryPlanner::new(schema);

    // Vector search call with invalid label
    // NOTE: Dotted function names (uni.vector.query) are not yet supported in LALRPOP parser
    // Using simple function name instead
    let sql = "CALL vector_query('Missing', 'embedding', [1.0, 2.0], 10)";
    let ast = uni_query::parse_cypher(sql).unwrap();

    // Planner validates arguments if possible?
    // Procedure call arguments are expressions, evaluated at runtime.
    // Planner typically passes them through.
    // But LogicalPlan::VectorKnn might be generated if we had special syntax?
    // Currently we use CALL which is generic ProcedureCall in plan.
    // So Planner might NOT validate this.

    let res = planner.plan(ast);
    assert!(res.is_ok());
    // This confirms Planner doesn't validate procedure args deeply yet.
}
