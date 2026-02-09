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
async fn test_cypher_set_advanced() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Item")?;
    schema_manager.add_property("Item", "name", DataType::String, false)?;
    schema_manager.add_property(
        "Item",
        "embedding",
        DataType::Vector { dimensions: 3 },
        true,
    )?;
    schema_manager.add_property("Item", "metadata", DataType::Json, true)?;

    schema_manager.add_edge_type(
        "RELATED",
        vec!["Item".to_string()],
        vec!["Item".to_string()],
    )?;
    schema_manager.add_property(
        "RELATED",
        "scores",
        DataType::Vector { dimensions: 2 },
        true,
    )?;

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

    // 2. Create node with properties
    let query = "CREATE (i:Item {name: 'A', embedding: [0.1, 0.2, 0.3], metadata: {valid: true, count: 1}})";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // 3. Verify Read
    let query = "MATCH (i:Item) RETURN i.embedding, i.metadata";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    assert_eq!(res.len(), 1);

    let emb = res[0].get("i.embedding").unwrap().as_array().unwrap();
    assert_eq!(emb.len(), 3);
    // Vector values are stored as f32, so compare with f32 precision
    let actual = emb[0].as_f64().unwrap();
    let expected = 0.1_f32 as f64;
    assert!(
        (actual - expected).abs() < 1e-6,
        "expected ~{expected} but got {actual}"
    );

    let meta = res[0].get("i.metadata").unwrap().as_object().unwrap();
    assert_eq!(meta.get("valid"), Some(&unival!(true)));

    // 4. Update Properties
    let query = "MATCH (i:Item) SET i.embedding = [0.4, 0.5, 0.6], i.metadata = {valid: false}";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    // 5. Verify Update
    let query = "MATCH (i:Item) RETURN i.embedding, i.metadata";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;

    let emb = res[0].get("i.embedding").unwrap().as_array().unwrap();
    // Vector values are stored as f32, so compare with f32 precision
    let actual = emb[0].as_f64().unwrap();
    let expected = 0.4_f32 as f64;
    assert!(
        (actual - expected).abs() < 1e-6,
        "expected ~{expected} but got {actual}"
    );

    let meta = res[0].get("i.metadata").unwrap().as_object().unwrap();
    assert_eq!(meta.get("valid"), Some(&unival!(false)));

    // 6. Test Edge Properties
    let query = "MATCH (i:Item) CREATE (i)-[r:RELATED {scores: [1.0, 2.0]}]->(i)";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    executor.execute(plan, &prop_manager, &params).await?;

    let query = "MATCH (:Item)-[r:RELATED]->(:Item) RETURN r.scores";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    let scores = res[0].get("r.scores").unwrap().as_array().unwrap();
    assert_eq!(scores[0].as_f64(), Some(1.0));

    // 7. Flush and Verify (to test Delta/Vertex build_record_batch)
    // Manually force flush?
    // The writer handles flushing if threshold met.
    // We can simulate flush by creating a new writer/storage or explicitly calling flush if exposed.
    // For now, we rely on memory test (L0).
    // To test L1, we'd need to trigger flush.

    {
        let mut w = writer.write().await;
        w.flush_to_l1(None).await?;
    }

    // Read back after flush
    let query = "MATCH (i:Item) RETURN i.embedding";
    let ast = uni_query::parse_cypher(query)?;
    let plan = planner.plan(ast)?;
    let res = executor.execute(plan, &prop_manager, &params).await?;
    let emb = res[0].get("i.embedding").unwrap().as_array().unwrap();
    // Compare with f32 precision
    let actual = emb[0].as_f64().unwrap();
    let expected = 0.4_f32 as f64;
    assert!(
        (actual - expected).abs() < 1e-6,
        "expected ~{expected} but got {actual}"
    );

    Ok(())
}
