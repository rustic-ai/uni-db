// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::{
    BooleanArray, Float64Array, Int32Array, LargeBinaryArray, RecordBatch, StringArray,
    TimestampMicrosecondArray, UInt64Array,
};
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::Value;
use uni_db::core::id::Vid;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::query::executor::Executor;

use uni_db::query::planner::QueryPlanner;
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_cypher_aggregation() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "age", DataType::Int32, false)?;

    let _order_lbl = schema_manager.add_label("Order")?;
    schema_manager.add_property("Order", "amount", DataType::Float64, false)?;

    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    // Insert Persons:
    // 0: 20
    // 1: 30
    // 2: 20
    // 3: 40
    let lancedb_store = storage.lancedb_store();
    let vertex_ds = storage.vertex_dataset("Person")?;
    // Columns: _vid, _uid, _deleted, _version, ext_id, _labels, _created_at, _updated_at, age, overflow_json
    let batch = RecordBatch::try_new(
        vertex_ds.get_arrow_schema(&schema_manager.schema())?,
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
            Arc::new(BooleanArray::from(vec![false; 4])),
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
            Arc::new(Int32Array::from(vec![20, 30, 20, 40])), // age
            Arc::new(LargeBinaryArray::from(vec![None::<&[u8]>; 4])), // overflow_json
        ],
    )?;
    vertex_ds
        .write_batch_lancedb(lancedb_store, batch, &schema_manager.schema())
        .await?;

    // Insert Orders
    // 0: 10.0
    // 1: 20.0
    // 2: 30.0
    let order_ds = storage.vertex_dataset("Order")?;
    // Columns: _vid, _uid, _deleted, _version, ext_id, _labels, _created_at, _updated_at, amount, overflow_json
    let batch = RecordBatch::try_new(
        order_ds.get_arrow_schema(&schema_manager.schema())?,
        vec![
            Arc::new(UInt64Array::from(vec![
                Vid::new(0).as_u64(),
                Vid::new(1).as_u64(),
                Vid::new(2).as_u64(),
            ])),
            Arc::new(arrow_array::FixedSizeBinaryArray::new(
                32,
                vec![0u8; 32 * 3].into(),
                None,
            )),
            Arc::new(BooleanArray::from(vec![false; 3])),
            Arc::new(UInt64Array::from(vec![1; 3])),
            Arc::new(StringArray::from(vec![None::<&str>; 3])), // ext_id
            // _labels
            {
                let mut lb = arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new());
                for _ in 0..3 {
                    lb.values().append_value("Order");
                    lb.append(true);
                }
                Arc::new(lb.finish())
            },
            Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>; 3]).with_timezone("UTC")), // _created_at
            Arc::new(TimestampMicrosecondArray::from(vec![None::<i64>; 3]).with_timezone("UTC")), // _updated_at
            Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0])), // amount
            Arc::new(LargeBinaryArray::from(vec![None::<&[u8]>; 3])), // overflow_json
        ],
    )?;
    order_ds
        .write_batch_lancedb(lancedb_store, batch, &schema_manager.schema())
        .await?;

    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let executor = Executor::new(storage.clone());
    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));

    // Test 1: COUNT(*)
    println!("--- Test 1: COUNT(*) ---");
    let sql = "MATCH (n:Person) RETURN COUNT(*)";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("COUNT(*)"), Some(&Value::Int(4)));

    // Test 2: Group By Age (n.age, COUNT(*))
    println!("--- Test 2: Group By Age ---");
    let sql = "MATCH (n:Person) RETURN n.age, COUNT(*) ORDER BY n.age ASC";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;

    assert_eq!(results.len(), 3);
    // 20: 2
    assert_eq!(results[0].get("n.age"), Some(&Value::Int(20)));
    assert_eq!(results[0].get("COUNT(*)"), Some(&Value::Int(2)));
    // 30: 1
    assert_eq!(results[1].get("n.age"), Some(&Value::Int(30)));
    // 40: 1
    assert_eq!(results[2].get("n.age"), Some(&Value::Int(40)));

    // Test 3: SUM(n.amount)
    println!("--- Test 3: SUM ---");
    let sql = "MATCH (n:`Order`) RETURN SUM(n.amount)";
    let query = uni_query::parse_cypher(sql)?;
    let plan = planner.plan(query)?;
    let results = executor
        .execute(plan, &prop_mgr, &std::collections::HashMap::new())
        .await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("SUM(n.amount)"), Some(&Value::Float(60.0)));

    Ok(())
}
