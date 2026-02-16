// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::{
    BooleanArray, FixedSizeBinaryArray, LargeBinaryArray, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt64Array,
};
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::Vid;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::runtime::property_manager::PropertyManager;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_compact_vertices_with_null_props() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _lbl = schema_manager.add_label("Node")?;
    schema_manager.add_property("Node", "name", DataType::String, true)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    // Insert data directly to storage to bypass L0 and force L1 writes
    let ds = storage.vertex_dataset("Node")?;
    let arrow_schema = ds.get_arrow_schema(&schema_manager.schema())?;

    // Use LanceDB to write data (canonical storage path)
    let lancedb_store = storage.lancedb_store();

    // Batch 1: VID 1, name="A"
    // Columns: _vid, _uid, _deleted, _version, ext_id, _labels, _created_at, _updated_at, name, overflow_json
    let batch1 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![Vid::new(1).as_u64()])),
            Arc::new(FixedSizeBinaryArray::new(32, vec![0u8; 32].into(), None)),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(UInt64Array::from(vec![1])),
            Arc::new(StringArray::from(vec![None::<&str>])), // ext_id
            // _labels
            {
                let mut lb = arrow_array::builder::ListBuilder::new(
                    arrow_array::builder::StringBuilder::new(),
                );
                lb.values().append_value("Node");
                lb.append(true);
                Arc::new(lb.finish())
            },
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]).with_timezone("UTC")), // _created_at
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]).with_timezone("UTC")), // _updated_at
            Arc::new(StringArray::from(vec![Some("A")])),
            Arc::new(LargeBinaryArray::from(vec![None::<&[u8]>])), // overflow_json
        ],
    )?;
    ds.write_batch_lancedb(lancedb_store, batch1, &schema_manager.schema())
        .await?;

    // Batch 2: VID 2, name=NULL
    let batch2 = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(UInt64Array::from(vec![Vid::new(2).as_u64()])),
            Arc::new(FixedSizeBinaryArray::new(32, vec![0u8; 32].into(), None)),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(UInt64Array::from(vec![2])),
            Arc::new(StringArray::from(vec![None::<&str>])), // ext_id
            // _labels
            {
                let mut lb = arrow_array::builder::ListBuilder::new(
                    arrow_array::builder::StringBuilder::new(),
                );
                lb.values().append_value("Node");
                lb.append(true);
                Arc::new(lb.finish())
            },
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]).with_timezone("UTC")), // _created_at
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>]).with_timezone("UTC")), // _updated_at
            Arc::new(StringArray::from(vec![None::<&str>])),
            Arc::new(LargeBinaryArray::from(vec![None::<&[u8]>])), // overflow_json
        ],
    )?;
    ds.write_batch_lancedb(lancedb_store, batch2, &schema_manager.schema())
        .await?;

    // Compact
    let stats = storage.compact_label("Node").await?;
    assert!(stats.files_compacted >= 1);

    // Verify
    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 10);
    let val1 = prop_mgr.get_vertex_prop(Vid::new(1), "name").await?;
    assert_eq!(val1, uni_db::Value::String("A".to_string()));

    let val2 = prop_mgr.get_vertex_prop(Vid::new(2), "name").await?;
    assert!(val2.is_null());

    Ok(())
}

#[tokio::test]
async fn test_compact_empty_dataset() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _lbl = schema_manager.add_label("Empty")?;
    schema_manager.save().await?;
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            Arc::new(schema_manager),
        )
        .await?,
    );

    // Compact empty - LanceDB creates an empty table during compaction
    let stats = storage.compact_label("Empty").await?;
    // With LanceDB, compaction creates the table structure even if empty
    assert!(stats.files_compacted <= 1);

    Ok(())
}
