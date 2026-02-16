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
use uni_db::unival;

#[tokio::test]
async fn test_property_lookup_uses_vid_filter() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    let lancedb_store = storage.lancedb_store();
    let vertex_ds = storage.vertex_dataset("Person")?;
    let arrow_schema = vertex_ds.get_arrow_schema(&schema_manager.schema())?;

    // Write rows out of VID order to ensure row index != local_offset.
    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(UInt64Array::from(vec![
                Vid::new(1).as_u64(),
                Vid::new(0).as_u64(),
            ])),
            Arc::new(FixedSizeBinaryArray::new(
                32,
                vec![0u8; 32 * 2].into(),
                None,
            )),
            Arc::new(BooleanArray::from(vec![false, false])),
            Arc::new(UInt64Array::from(vec![1, 1])),
            Arc::new(StringArray::from(vec![None::<&str>; 2])), // ext_id
            // _labels
            {
                let mut lb = arrow_array::builder::ListBuilder::new(
                    arrow_array::builder::StringBuilder::new(),
                );
                for _ in 0..2 {
                    lb.values().append_value("Person");
                    lb.append(true);
                }
                Arc::new(lb.finish())
            },
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; 2]).with_timezone("UTC")), // _created_at
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; 2]).with_timezone("UTC")), // _updated_at
            Arc::new(StringArray::from(vec!["Bob", "Alice"])),
            Arc::new(LargeBinaryArray::from(vec![None::<&[u8]>; 2])), // overflow_json
        ],
    )?;
    vertex_ds
        .write_batch_lancedb(lancedb_store, batch, &schema_manager.schema())
        .await?;

    let prop_mgr = PropertyManager::new(storage.clone(), schema_manager.clone(), 10);
    let alice_vid = Vid::new(0);
    let bob_vid = Vid::new(1);

    let alice_name = prop_mgr.get_vertex_prop(alice_vid, "name").await?;
    assert_eq!(alice_name, uni_db::Value::String("Alice".to_string()));

    let bob_props = prop_mgr.get_all_vertex_props(bob_vid).await?;
    assert_eq!(
        bob_props.get("name"),
        Some(&uni_db::Value::String("Bob".to_string()))
    );

    Ok(())
}

#[tokio::test]
async fn test_property_lookup_not_found() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_manager = Arc::new(SchemaManager::load(&path.join("schema.json")).await?);
    let _lbl = schema_manager.add_label("Node")?;
    schema_manager.save().await?;
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    let prop_mgr = PropertyManager::new(storage, schema_manager, 10);
    let vid = Vid::new(999); // Non-existent

    let val = prop_mgr.get_vertex_prop(vid, "name").await?;
    assert!(val.is_null());

    let props = prop_mgr.get_all_vertex_props(vid).await?;
    assert!(props.is_empty());

    Ok(())
}

#[tokio::test]
async fn test_list_property_storage() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _lbl = schema_manager.add_label("Node")?;
    // List<String>
    schema_manager.add_property(
        "Node",
        "tags",
        DataType::List(Box::new(DataType::String)),
        false,
    )?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    let lancedb_store = storage.lancedb_store();
    let vertex_ds = storage.vertex_dataset("Node")?;
    let arrow_schema = vertex_ds.get_arrow_schema(&schema_manager.schema())?;

    // Create List array
    let tags_builder =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new());
    let mut tags_builder = tags_builder;
    tags_builder.values().append_value("a");
    tags_builder.values().append_value("b");
    tags_builder.append(true);
    let tags_arr = Arc::new(tags_builder.finish());

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(UInt64Array::from(vec![Vid::new(0).as_u64()])),
            Arc::new(FixedSizeBinaryArray::new(32, vec![0u8; 32].into(), None)),
            Arc::new(BooleanArray::from(vec![false])),
            Arc::new(UInt64Array::from(vec![1])),
            Arc::new(StringArray::from(vec![None::<&str>; 1])), // ext_id
            // _labels
            {
                let mut lb = arrow_array::builder::ListBuilder::new(
                    arrow_array::builder::StringBuilder::new(),
                );
                lb.values().append_value("Node");
                lb.append(true);
                Arc::new(lb.finish())
            },
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; 1]).with_timezone("UTC")), // _created_at
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; 1]).with_timezone("UTC")), // _updated_at
            tags_arr,
            Arc::new(LargeBinaryArray::from(vec![None::<&[u8]>; 1])), // overflow_json
        ],
    )?;
    vertex_ds
        .write_batch_lancedb(lancedb_store, batch, &schema_manager.schema())
        .await?;

    let prop_mgr = PropertyManager::new(storage, schema_manager, 10);
    let vid = Vid::new(0);

    let val = prop_mgr.get_vertex_prop(vid, "tags").await?;
    let expected: uni_db::Value = unival!(["a", "b"]);
    assert_eq!(val, expected);

    Ok(())
}
