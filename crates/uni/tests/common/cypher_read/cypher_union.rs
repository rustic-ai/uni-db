// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_cypher_union() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;
    let schema = schema_manager.schema();
    let schema_manager = Arc::new(schema_manager);

    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    let storage = Arc::new(StorageManager::new(storage_str, schema_manager.clone()).await?);

    let writer = Arc::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    );

    let prop_manager = PropertyManager::new(storage.clone(), storage.schema_manager_arc(), 1024);
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(schema);
    let params = HashMap::new();

    // 2. Create data: Alice, Bob
    let query = "CREATE (a:Person {name: 'Alice'}) CREATE (b:Person {name: 'Bob'})";
    let ast = uni_cypher::parse(query)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // 3. UNION ALL returns one row per branch.
    //
    // This assertion was commented out with a note that UNION ALL "only
    // returns first query results", leaving the only union test in the crate
    // asserting nothing at all. It passes, so the note was stale — and a test
    // that asserts nothing is why a union could return the wrong value for as
    // long as it did (#190). Whole-entity unions are covered separately in
    // `union_entity_test`.
    let query = "MATCH (n:Person {name: 'Alice'}) RETURN n.name UNION ALL MATCH (n:Person {name: 'Bob'}) RETURN n.name";
    let ast = uni_cypher::parse(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(res.len(), 2, "UNION ALL must return a row from each branch");

    let mut names: Vec<String> = res
        .iter()
        .filter_map(|row| match row.get("n.name") {
            Some(uni_db::Value::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect();
    names.sort();
    assert_eq!(names, vec!["Alice".to_string(), "Bob".to_string()]);

    Ok(())
}
