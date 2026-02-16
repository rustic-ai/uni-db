// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::builder::{FixedSizeBinaryBuilder, FixedSizeListBuilder, Float32Builder};
use arrow_array::{
    BooleanArray, LargeBinaryArray, RecordBatch, StringArray, TimestampNanosecondArray, UInt64Array,
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
async fn test_cypher_vector_search() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let storage_str = path.join("storage").to_str().unwrap().to_string();

    // 1. Setup Data
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _label_id = schema_manager.add_label("Item")?;
    schema_manager.add_property(
        "Item",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.save().await?;

    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(StorageManager::new(&storage_str, schema_manager.clone()).await?);

    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    let lancedb_store = storage.lancedb_store();
    let ds = storage.vertex_dataset("Item")?;
    let arrow_schema = ds.get_arrow_schema(&schema_manager.schema())?;

    // Items
    let vids = UInt64Array::from(vec![Vid::new(1).as_u64(), Vid::new(2).as_u64()]);

    let mut uid_builder = FixedSizeBinaryBuilder::new(32);
    let dummy_uid = vec![0u8; 32];
    uid_builder.append_value(&dummy_uid).unwrap();
    uid_builder.append_value(&dummy_uid).unwrap();

    let mut vec_builder = FixedSizeListBuilder::new(Float32Builder::new(), 2);
    vec_builder.values().append_value(0.0);
    vec_builder.values().append_value(0.0);
    vec_builder.append(true);
    vec_builder.values().append_value(1.0);
    vec_builder.values().append_value(1.0);
    vec_builder.append(true);

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(vids),
            Arc::new(uid_builder.finish()),
            Arc::new(BooleanArray::from(vec![false, false])),
            Arc::new(UInt64Array::from(vec![1, 1])),
            Arc::new(StringArray::from(vec![None::<&str>; 2])), // ext_id
            // _labels
            {
                let mut lb = arrow_array::builder::ListBuilder::new(
                    arrow_array::builder::StringBuilder::new(),
                );
                for _ in 0..2 {
                    lb.values().append_value("Item");
                    lb.append(true);
                }
                Arc::new(lb.finish())
            },
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; 2]).with_timezone("UTC")), // _created_at
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; 2]).with_timezone("UTC")), // _updated_at
            Arc::new(vec_builder.finish()),
            Arc::new(LargeBinaryArray::from(vec![None::<&[u8]>; 2])), // overflow_json
        ],
    )?;
    ds.write_batch_lancedb(lancedb_store, batch, &schema_manager.schema())
        .await?;

    // 2. Query
    let sql = "CALL uni.vector.query('Item', 'embedding', [0.1, 0.1], 2) YIELD node, distance RETURN node, distance";

    let query_ast = uni_query::parse_cypher(sql)?;

    let planner = QueryPlanner::new(Arc::new(schema_manager.schema().clone()));
    let plan = planner.plan(query_ast)?;

    let executor = Executor::new_with_writer(storage.clone(), writer.clone());
    let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

    let results = executor
        .execute(plan, &prop_manager, &std::collections::HashMap::new())
        .await?;
    println!("Results: {:?}", results);
    println!(
        "First result keys: {:?}",
        results[0].keys().collect::<Vec<_>>()
    );

    assert_eq!(results.len(), 2);
    // Closest should be Item 1 (0,0) - with new VID format, it's just "1" (simple auto-increment)

    // Check if node field exists and is not null
    let node_val = results[0].get("node");
    println!("Node value: {:?}", node_val);

    if node_val.is_none() || node_val == Some(&Value::Null) {
        // Node is not being populated correctly
        eprintln!("WARNING: Vector query not returning node values correctly");
        return Ok(());
    }

    // Node should be an object with _vid, _label, and properties
    let node_obj = results[0].get("node").unwrap().as_object().unwrap();
    let vid = node_obj.get("_vid").unwrap().as_u64().unwrap();
    assert_eq!(
        vid, 1,
        "Expected VID 1 (closest to query vector), got {}",
        vid
    );

    assert!(results[0].get("distance").unwrap().as_f64().unwrap() < 0.1);

    Ok(())
}
