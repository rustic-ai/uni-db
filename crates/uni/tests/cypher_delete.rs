// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;
use uni_db::unival;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_cypher_delete() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("User")?;
    schema_manager.add_property("User", "name", DataType::String, false)?;
    schema_manager.add_edge_type(
        "FOLLOWS",
        vec!["User".to_string()],
        vec!["User".to_string()],
    )?;
    schema_manager.add_property("FOLLOWS", "since", DataType::Int64, true)?;
    schema_manager.save().await?;
    let schema = Arc::new(schema_manager.schema().clone());
    let schema_manager = Arc::new(schema_manager);

    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()).await?);

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    let prop_manager = PropertyManager::new(storage.clone(), storage.schema_manager_arc(), 1024);
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(schema);
    let params = HashMap::new();

    // 2. Create data: (Alice)-[:FOLLOWS]->(Bob)
    let query = "CREATE (u1:User {name: 'Alice'}) CREATE (u2:User {name: 'Bob'}) CREATE (u1)-[:FOLLOWS]->(u2)";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // 3. Try to delete Alice without DETACH (should fail)
    let query = "MATCH (u:User {name: 'Alice'}) DELETE u";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await;
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("still has relationships")
    );

    // 4. Delete with DETACH
    let query = "MATCH (u:User {name: 'Alice'}) DETACH DELETE u";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // 5. Verify Alice is gone
    let query = "MATCH (u:User {name: 'Alice'}) RETURN u";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(res.len(), 0);

    // 6. Verify relationship is gone
    let query = "MATCH (:User)-[r:FOLLOWS]->(:User) RETURN r";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(res.len(), 0);

    // 7. Verify Bob is still there
    let query = "MATCH (u:User {name: 'Bob'}) RETURN u.name";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("u.name"), Some(&unival!("Bob")));

    // 8. Test DELETE r
    // Create another relationship
    let query = "MATCH (u1:User {name: 'Bob'}) CREATE (u2:User {name: 'Charlie'}) CREATE (u1)-[:FOLLOWS]->(u2)";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // Verify it exists
    let query = "MATCH (u1:User {name: 'Bob'})-[r:FOLLOWS]->(u2:User {name: 'Charlie'}) RETURN r";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(res.len(), 1);

    // Delete r
    let query = "MATCH (u1:User {name: 'Bob'})-[r:FOLLOWS]->(u2:User {name: 'Charlie'}) DELETE r";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // Verify it's gone
    let query = "MATCH (u1:User {name: 'Bob'})-[r:FOLLOWS]->(u2:User {name: 'Charlie'}) RETURN r";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(res.len(), 0);

    // 9. Test SET on relationship
    // Create relationship with property
    let query = "MATCH (u1:User {name: 'Bob'}) CREATE (u2:User {name: 'David'}) CREATE (u1)-[r:FOLLOWS {since: 2020}]->(u2) RETURN r";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(res.len(), 1);

    // Update property
    let query =
        "MATCH (:User {name: 'Bob'})-[r:FOLLOWS]->(:User {name: 'David'}) SET r.since = 2021";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // Verify update
    let query = "MATCH (b:User {name: 'Bob'})-[r:FOLLOWS]->(d:User {name: 'David'}) RETURN r.since";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(res.len(), 1);
    assert_eq!(res[0].get("r.since"), Some(&unival!(2021)));

    Ok(())
}
