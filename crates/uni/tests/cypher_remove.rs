// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_cypher_remove() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("User")?;
    schema_manager.add_property("User", "name", DataType::String, false)?;
    schema_manager.add_property("User", "age", DataType::Int64, true)?;
    schema_manager.add_edge_type(
        "FOLLOWS",
        vec!["User".to_string()],
        vec!["User".to_string()],
    )?;
    schema_manager.add_property("FOLLOWS", "since", DataType::Int64, true)?;
    schema_manager.save().await?;
    let schema = Arc::new(schema_manager.schema().clone());
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    let prop_manager = PropertyManager::new(storage.clone(), storage.schema_manager_arc(), 1024);
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(schema);
    let params = HashMap::new();

    // 2. Create data
    let query = "CREATE (u1:User {name: 'Alice', age: 30}) CREATE (u2:User {name: 'Bob'}) CREATE (u1)-[r:FOLLOWS {since: 2020}]->(u2)";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // 3. Remove property from Vertex
    let query = "MATCH (u:User {name: 'Alice'}) REMOVE u.age RETURN u.age";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("u.age"), Some(&uni_db::Value::Null));

    // Verify persistence (fetch again)
    let query = "MATCH (u:User {name: 'Alice'}) RETURN u.age";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("u.age"), Some(&uni_db::Value::Null));

    // 4. Remove property from Relationship
    let query = "MATCH (:User {name: 'Alice'})-[r:FOLLOWS]->(:User {name: 'Bob'}) REMOVE r.since RETURN r.since";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("r.since"), Some(&uni_db::Value::Null));

    // Verify persistence
    let query = "MATCH (:User {name: 'Alice'})-[r:FOLLOWS]->(:User {name: 'Bob'}) RETURN r.since";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("r.since"), Some(&uni_db::Value::Null));

    // 5. Remove label from vertex
    let query = "MATCH (u:User {name: 'Bob'}) REMOVE u:User RETURN u";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(res.len(), 1);
    // After removing User label, the vertex should still exist but with no labels
    // Note: The vertex properties should still be there
    assert!(res[0].contains_key("u"));

    // Verify the vertex no longer matches User label query
    let query = "MATCH (u:User {name: 'Bob'}) RETURN u";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(
        res.len(),
        0,
        "Vertex should not match User label after removal"
    );

    Ok(())
}
