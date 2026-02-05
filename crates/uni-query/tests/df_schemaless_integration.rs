// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Integration tests for DataFusion schemaless operations (ScanAll, TraverseMainByType).

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_query::query::executor::Executor;
use uni_query::query::planner::QueryPlanner;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

async fn setup_executor(
    path: &std::path::Path,
) -> (
    Executor,
    Arc<PropertyManager>,
    Arc<SchemaManager>,
    Arc<StorageManager>,
) {
    let schema_manager = SchemaManager::load(&path.join("schema.json"))
        .await
        .unwrap();

    // Add schema elements
    schema_manager.add_label("Person").unwrap();
    schema_manager
        .add_property("Person", "name", DataType::String, true)
        .unwrap();
    schema_manager.add_label("Company").unwrap();
    schema_manager
        .add_property("Company", "name", DataType::String, true)
        .unwrap();
    schema_manager.save().await.unwrap();

    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await
        .unwrap(),
    );

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    let prop_manager = Arc::new(PropertyManager::new(
        storage.clone(),
        schema_manager.clone(),
        100,
    ));
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());

    (executor, prop_manager, schema_manager, storage)
}

#[tokio::test]
async fn test_scan_all_df_execution() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path();
    let (executor, prop_manager, schema_manager, _) = setup_executor(path).await;
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema()));

    // Create vertices with different labels
    let create_sql = r#"
        CREATE (p1:Person {name: 'Alice'}),
               (p2:Person {name: 'Bob'}),
               (c1:Company {name: 'ACME'})
        RETURN p1, p2, c1
    "#;
    let query = uni_query::parse_cypher(create_sql).unwrap();
    let plan = planner.plan(query).unwrap();
    executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await
        .unwrap();

    // Execute: MATCH (n) RETURN n
    let sql = "MATCH (n) RETURN n";
    let query = uni_query::parse_cypher(sql).unwrap();
    let plan = planner.plan(query).unwrap();

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await
        .unwrap();

    // Verify: All 3 vertices returned (2 Person + 1 Company)
    assert_eq!(results.len(), 3, "Should return all vertices");
}

#[tokio::test]
async fn test_scan_all_with_filter() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path();
    let (executor, prop_manager, schema_manager, _) = setup_executor(path).await;
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema()));

    // Create vertices
    let create_sql = r#"
        CREATE (p1:Person {name: 'Alice', age: 30}),
               (p2:Person {name: 'Bob', age: 25}),
               (p3:Person {name: 'Charlie', age: 35})
        RETURN p1, p2, p3
    "#;
    let query = uni_query::parse_cypher(create_sql).unwrap();
    let plan = planner.plan(query).unwrap();
    executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await
        .unwrap();

    // Execute: MATCH (n) WHERE n.age > 28 RETURN n
    let sql = "MATCH (n) WHERE n.age > 28 RETURN n";
    let query = uni_query::parse_cypher(sql).unwrap();
    let plan = planner.plan(query).unwrap();

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await
        .unwrap();

    // Verify: Only Alice (30) and Charlie (35) returned
    assert_eq!(results.len(), 2, "Should filter by age > 28");
}

#[tokio::test]
async fn test_scan_all_empty_graph() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path();
    let (executor, prop_manager, schema_manager, _) = setup_executor(path).await;
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema()));

    // Execute on empty graph: MATCH (n) RETURN n
    let sql = "MATCH (n) RETURN n";
    let query = uni_query::parse_cypher(sql).unwrap();
    let plan = planner.plan(query).unwrap();

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await
        .unwrap();

    // Verify: Empty result
    assert_eq!(
        results.len(),
        0,
        "Should return empty result for empty graph"
    );
}

#[tokio::test]
async fn test_traverse_main_by_type_df_execution() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path();
    let (executor, prop_manager, schema_manager, _) = setup_executor(path).await;
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema()));

    // Create graph with schemaless edge type
    let create_sql = r#"
        CREATE (p1:Person {name: 'Alice'}),
               (p2:Person {name: 'Bob'}),
               (p1)-[:CUSTOM {weight: 10}]->(p2)
        RETURN p1, p2
    "#;
    let query = uni_query::parse_cypher(create_sql).unwrap();
    let plan = planner.plan(query).unwrap();
    executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await
        .unwrap();

    // Execute: MATCH (a)-[:CUSTOM]->(b) RETURN a, b
    let sql = "MATCH (a)-[:CUSTOM]->(b) RETURN a, b";
    let query = uni_query::parse_cypher(sql).unwrap();
    let plan = planner.plan(query).unwrap();

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await
        .unwrap();

    // Verify: One traversal (Alice -> Bob)
    assert_eq!(results.len(), 1, "Should return one edge traversal");
}

#[tokio::test]
async fn test_traverse_main_by_type_direction() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path();
    let (executor, prop_manager, schema_manager, _) = setup_executor(path).await;
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema()));

    // Create directed graph
    let create_sql = r#"
        CREATE (p1:Person {name: 'Alice'}),
               (p2:Person {name: 'Bob'}),
               (p3:Person {name: 'Charlie'}),
               (p1)-[:FOLLOWS]->(p2),
               (p3)-[:FOLLOWS]->(p1)
        RETURN p1, p2, p3
    "#;
    let query = uni_query::parse_cypher(create_sql).unwrap();
    let plan = planner.plan(query).unwrap();
    executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await
        .unwrap();

    // Test outgoing traversal
    let sql = "MATCH (a:Person {name: 'Alice'})-[:FOLLOWS]->(b) RETURN b.name";
    let query = uni_query::parse_cypher(sql).unwrap();
    let plan = planner.plan(query).unwrap();
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await
        .unwrap();
    assert_eq!(results.len(), 1, "Alice follows Bob");

    // Test incoming traversal
    let sql = "MATCH (a:Person {name: 'Alice'})<-[:FOLLOWS]-(b) RETURN b.name";
    let query = uni_query::parse_cypher(sql).unwrap();
    let plan = planner.plan(query).unwrap();
    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await
        .unwrap();
    assert_eq!(results.len(), 1, "Charlie follows Alice");
}

#[tokio::test]
async fn test_optional_traverse_main_by_type() {
    let temp_dir = tempdir().unwrap();
    let path = temp_dir.path();
    let (executor, prop_manager, schema_manager, _) = setup_executor(path).await;
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema()));

    // Create vertices without edges
    let create_sql = r#"
        CREATE (p1:Person {name: 'Alice'}),
               (p2:Person {name: 'Bob'})
        RETURN p1, p2
    "#;
    let query = uni_query::parse_cypher(create_sql).unwrap();
    let plan = planner.plan(query).unwrap();
    executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await
        .unwrap();

    // Execute: OPTIONAL MATCH (a)-[:RARE]->(b) RETURN a, b
    let sql = "MATCH (a:Person) OPTIONAL MATCH (a)-[:RARE]->(b) RETURN a.name, b";
    let query = uni_query::parse_cypher(sql).unwrap();
    let plan = planner.plan(query).unwrap();

    let results = executor
        .execute(plan, &prop_manager, &HashMap::new())
        .await
        .unwrap();

    // Verify: Both persons returned with NULL for b
    assert_eq!(
        results.len(),
        2,
        "Should return both vertices with NULL target"
    );
}
