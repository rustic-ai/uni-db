// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::{
    BooleanArray, Int32Array, LargeBinaryArray, RecordBatch, StringArray,
    TimestampMicrosecondArray, UInt64Array,
};
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::Value;
use uni_db::core::id::Vid;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_cypher_limit_order() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema & Data
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;

    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.add_property("Person", "age", DataType::Int32, false)?;

    schema_manager.save().await?;
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

    // david, 40
    let lancedb_store = storage.lancedb_store();
    let vertex_ds = storage.vertex_dataset("Person")?;
    let arrow_schema = vertex_ds.get_arrow_schema(&schema_manager.schema())?;

    // Columns: _vid, _uid, _deleted, _version, ext_id, _labels, _created_at, _updated_at, age, name, overflow_json
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(UInt64Array::from(vec![
                Vid::new(0).as_u64(),
                Vid::new(1).as_u64(),
                Vid::new(2).as_u64(),
                Vid::new(3).as_u64(),
            ])),
            Arc::new(arrow_array::FixedSizeBinaryArray::new(
                32,
                vec![0u8; 32 * 4].into(),
                None,
            )),
            Arc::new(BooleanArray::from(vec![false; 4])), // _deleted
            Arc::new(UInt64Array::from(vec![1; 4])),
            Arc::new(StringArray::from(vec![None::<&str>; 4])), // ext_id
            // _labels
            {
                let mut lb = arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new());
                for _ in 0..4 {
                    lb.values().append_value("Person");
                    lb.append(true);
                }
                Arc::new(lb.finish())
            },
            Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>; 4]).with_timezone("UTC")), // _created_at
            Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>; 4]).with_timezone("UTC")), // _updated_at
            Arc::new(Int32Array::from(vec![25, 35, 20, 40])), // age
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie", "David"])), // name
            Arc::new(LargeBinaryArray::from(vec![None::<&[u8]>; 4])), // overflow_json
        ],
    )?;
    vertex_ds
        .write_batch_lancedb(lancedb_store, batch, &schema_manager.schema())
        .await?;

    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // Test 1: LIMIT
    println!("--- Test 1: LIMIT ---");
    let sql = "MATCH (n:Person) RETURN n.name LIMIT 2";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 2);

    // Test 2: ORDER BY ASC
    println!("--- Test 2: ORDER BY ASC ---");
    let sql = "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age ASC";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 4);
    assert_eq!(
        results[0].get("n.name"),
        Some(&Value::String("Charlie".to_string()))
    ); // 20
    assert_eq!(
        results[1].get("n.name"),
        Some(&Value::String("Alice".to_string()))
    ); // 25
    assert_eq!(
        results[2].get("n.name"),
        Some(&Value::String("Bob".to_string()))
    ); // 35
    assert_eq!(
        results[3].get("n.name"),
        Some(&Value::String("David".to_string()))
    ); // 40

    // Test 3: ORDER BY DESC + LIMIT
    println!("--- Test 3: ORDER BY DESC + LIMIT ---");
    let sql = "MATCH (n:Person) RETURN n.name, n.age ORDER BY n.age DESC LIMIT 2";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].get("n.name"),
        Some(&Value::String("David".to_string()))
    ); // 40
    assert_eq!(
        results[1].get("n.name"),
        Some(&Value::String("Bob".to_string()))
    ); // 35

    // Test 4: SKIP + LIMIT
    println!("--- Test 4: SKIP + LIMIT ---");
    // Ordered by age: Charlie(20), Alice(25), Bob(35), David(40)
    // SKIP 1 LIMIT 2 -> Alice, Bob
    let sql = "MATCH (n:Person) RETURN n.name ORDER BY n.age ASC SKIP 1 LIMIT 2";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 2);
    assert_eq!(
        results[0].get("n.name"),
        Some(&Value::String("Alice".to_string()))
    );
    assert_eq!(
        results[1].get("n.name"),
        Some(&Value::String("Bob".to_string()))
    );

    Ok(())
}
