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
async fn test_cypher_merge_actions() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_property("Person", "created", DataType::Int64, true)?;
    schema_manager.add_property("Person", "matched", DataType::Int64, true)?;
    schema_manager.save().await?;
    let schema = Arc::new(schema_manager.schema().clone());

    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            Arc::new(schema_manager),
        )
        .await?,
    );

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), storage.schema_manager_arc(), 0)
            .await
            .unwrap(),
    ));

    let prop_manager = PropertyManager::new(storage.clone(), storage.schema_manager_arc(), 1024);
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(schema);
    let params = HashMap::new();

    // 2. MERGE (Create) with ON CREATE
    let query = "MERGE (p:Person {name: 'Alice'}) ON CREATE SET p.created = 1 ON MATCH SET p.matched = 1 RETURN p.created, p.matched";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    assert_eq!(res[0].get("p.created"), Some(&unival!(1)));
    assert_eq!(res[0].get("p.matched"), Some(&uni_db::Value::Null));

    // 3. MERGE (Match) with ON MATCH
    let query = "MERGE (p:Person {name: 'Alice'}) ON CREATE SET p.created = 2 ON MATCH SET p.matched = 2 RETURN p.created, p.matched";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    // p.created should remain 1 (from create)
    // p.matched should become 2
    assert_eq!(res[0].get("p.created"), Some(&unival!(1)));
    assert_eq!(res[0].get("p.matched"), Some(&unival!(2)));

    Ok(())
}
