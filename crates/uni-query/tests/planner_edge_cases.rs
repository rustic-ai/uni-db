// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::sync::Arc;
use tempfile::tempdir;
use uni_common::core::schema::{DataType, SchemaManager};

use uni_query::query::planner::QueryPlanner;

/// Create a SchemaManager backed by a temporary directory.
/// Returns (SchemaManager, TempDir). TempDir must be kept alive for the duration of the test.
async fn setup_schema() -> (SchemaManager, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let path = dir.path().join("schema.json");
    let schema_manager = SchemaManager::load(&path).await.unwrap();
    (schema_manager, dir)
}

/// Build a QueryPlanner from a SchemaManager.
fn planner_from(schema_manager: &SchemaManager) -> QueryPlanner {
    QueryPlanner::new(Arc::new(schema_manager.schema()))
}

/// Assert that planning the given Cypher query fails with an error containing `expected_code`.
fn assert_plan_error(planner: &QueryPlanner, cypher: &str, expected_code: &str) {
    let ast = uni_query::parse_cypher(cypher).unwrap();
    let res = planner.plan(ast);
    assert!(
        res.is_err(),
        "Expected error containing '{}' for query: {}",
        expected_code,
        cypher,
    );
    let err_msg = res.unwrap_err().to_string();
    assert!(
        err_msg.contains(expected_code),
        "Error should mention {}, got: {}",
        expected_code,
        err_msg,
    );
}

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
    // This test verifies that unknown edge types are handled gracefully
    // using schemaless support (TraverseMainByType plan) instead of erroring.
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
    // With schemaless edge support, unknown edge types should succeed
    // and generate a TraverseMainByType plan (not error)
    assert!(
        res.is_ok(),
        "Expected schemaless edge support to handle unknown type, got: {:?}",
        res.err()
    );
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

// ============================================================================
// Semantic Validation Tests
// ============================================================================

#[tokio::test]
async fn test_semantic_undefined_variable_in_delete() {
    let (schema_manager, _dir) = setup_schema().await;
    schema_manager.add_label("Person").unwrap();
    schema_manager.save().await.unwrap();
    let planner = planner_from(&schema_manager);

    assert_plan_error(&planner, "MATCH (n:Person) DELETE m", "UndefinedVariable");
}

#[tokio::test]
async fn test_semantic_invalid_argument_type_in_delete() {
    let (schema_manager, _dir) = setup_schema().await;
    schema_manager.add_label("Person").unwrap();
    schema_manager.save().await.unwrap();
    let planner = planner_from(&schema_manager);

    assert_plan_error(
        &planner,
        "UNWIND [1, 2, 3] AS x DELETE x",
        "InvalidArgumentType",
    );
}

#[tokio::test]
async fn test_semantic_invalid_argument_type_in_limit() {
    let (schema_manager, _dir) = setup_schema().await;
    schema_manager.add_label("Person").unwrap();
    schema_manager.save().await.unwrap();
    let planner = planner_from(&schema_manager);

    assert_plan_error(
        &planner,
        "MATCH (n:Person) RETURN n LIMIT 1.5",
        "InvalidArgumentType",
    );
}

#[tokio::test]
async fn test_semantic_variable_type_conflict() {
    let (schema_manager, _dir) = setup_schema().await;
    schema_manager.add_label("Person").unwrap();
    schema_manager
        .add_edge_type(
            "KNOWS",
            vec!["Person".to_string()],
            vec!["Person".to_string()],
        )
        .unwrap();
    schema_manager.save().await.unwrap();
    let planner = planner_from(&schema_manager);

    assert_plan_error(
        &planner,
        "MATCH (n:Person)-[n:KNOWS]->(m:Person) RETURN n, m",
        "VariableTypeConflict",
    );
}

#[tokio::test]
async fn test_semantic_edge_as_node_same_pattern() {
    let (schema_manager, _dir) = setup_schema().await;
    schema_manager.add_label("Person").unwrap();
    schema_manager
        .add_edge_type(
            "KNOWS",
            vec!["Person".to_string()],
            vec!["Person".to_string()],
        )
        .unwrap();
    schema_manager.save().await.unwrap();
    let planner = planner_from(&schema_manager);

    assert_plan_error(
        &planner,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person), (r) RETURN r",
        "VariableTypeConflict",
    );
}

#[tokio::test]
async fn test_semantic_variable_already_bound_in_yield() {
    let (schema_manager, _dir) = setup_schema().await;
    let planner = planner_from(&schema_manager);

    assert_plan_error(
        &planner,
        "CALL db.info() YIELD name, name AS name RETURN name",
        "VariableAlreadyBound",
    );
}

#[tokio::test]
async fn test_semantic_variable_already_bound_in_create() {
    let (schema_manager, _dir) = setup_schema().await;
    schema_manager.add_label("Person").unwrap();
    schema_manager.save().await.unwrap();
    let planner = planner_from(&schema_manager);

    assert_plan_error(
        &planner,
        "MATCH (n:Person) CREATE (n:Person)",
        "VariableAlreadyBound",
    );
}

#[tokio::test]
async fn test_semantic_undefined_variable_in_return() {
    let (schema_manager, _dir) = setup_schema().await;
    schema_manager.add_label("Person").unwrap();
    schema_manager.save().await.unwrap();
    let planner = planner_from(&schema_manager);

    assert_plan_error(&planner, "MATCH (n:Person) RETURN x", "UndefinedVariable");
}

#[tokio::test]
async fn test_semantic_undefined_variable_in_where() {
    let (schema_manager, _dir) = setup_schema().await;
    schema_manager.add_label("Person").unwrap();
    schema_manager.save().await.unwrap();
    let planner = planner_from(&schema_manager);

    assert_plan_error(
        &planner,
        "MATCH (n:Person) WHERE m.name = 'Alice' RETURN n",
        "UndefinedVariable",
    );
}

#[tokio::test]
async fn test_semantic_aggregation_in_where() {
    let (schema_manager, _dir) = setup_schema().await;
    schema_manager.add_label("Person").unwrap();
    schema_manager.save().await.unwrap();
    let planner = planner_from(&schema_manager);

    assert_plan_error(
        &planner,
        "MATCH (n:Person) WHERE count(n) > 5 RETURN n",
        "InvalidAggregation",
    );
}

#[tokio::test]
async fn test_semantic_negative_skip() {
    let (schema_manager, _dir) = setup_schema().await;
    schema_manager.add_label("Person").unwrap();
    schema_manager.save().await.unwrap();
    let planner = planner_from(&schema_manager);

    assert_plan_error(
        &planner,
        "MATCH (n:Person) RETURN n SKIP -1",
        "NegativeIntegerArgument",
    );
}

#[tokio::test]
async fn test_semantic_negative_limit() {
    let (schema_manager, _dir) = setup_schema().await;
    schema_manager.add_label("Person").unwrap();
    schema_manager.save().await.unwrap();
    let planner = planner_from(&schema_manager);

    assert_plan_error(
        &planner,
        "MATCH (n:Person) RETURN n LIMIT -1",
        "NegativeIntegerArgument",
    );
}

#[tokio::test]
async fn test_semantic_labels_on_edge() {
    let (schema_manager, _dir) = setup_schema().await;
    schema_manager.add_label("Person").unwrap();
    schema_manager
        .add_edge_type(
            "KNOWS",
            vec!["Person".to_string()],
            vec!["Person".to_string()],
        )
        .unwrap();
    schema_manager.save().await.unwrap();
    let planner = planner_from(&schema_manager);

    assert_plan_error(
        &planner,
        "MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN labels(r)",
        "InvalidArgumentType",
    );
}

#[tokio::test]
async fn test_semantic_type_on_node() {
    let (schema_manager, _dir) = setup_schema().await;
    schema_manager.add_label("Person").unwrap();
    schema_manager.save().await.unwrap();
    let planner = planner_from(&schema_manager);

    assert_plan_error(
        &planner,
        "MATCH (n:Person) RETURN type(n)",
        "InvalidArgumentType",
    );
}
