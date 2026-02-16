// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use arrow_array::builder::{FixedSizeBinaryBuilder, FixedSizeListBuilder, Float32Builder};
use arrow_array::{RecordBatch, StringArray, TimestampNanosecondArray, UInt64Array};
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::Vid;
use uni_db::core::schema::{DataType, SchemaManager};
use uni_db::runtime::context::QueryContext;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_vector_search() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();

    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");
    let storage_path = path.join("storage");
    let storage_str = storage_path.to_str().unwrap();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&schema_path).await?;
    let _label_id = schema_manager.add_label("Item")?;
    // Add vector property: 2 dimensions
    schema_manager.add_property(
        "Item",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage = StorageManager::new(storage_str, schema_manager.clone()).await?;
    let lancedb_store = storage.lancedb_store();

    // 2. Insert Data
    // Item 1: [0.0, 0.0] (Target)
    // Item 2: [1.0, 1.0]
    // Item 3: [10.0, 10.0]

    let vertex_ds = storage.vertex_dataset("Item")?;
    let arrow_schema = vertex_ds.get_arrow_schema(&schema_manager.schema())?;

    let vids = UInt64Array::from(vec![1, 2, 3]);
    let versions = UInt64Array::from(vec![1, 1, 1]);
    let deleted = arrow_array::BooleanArray::from(vec![false, false, false]);

    // UIDs (dummy)
    let mut uid_builder = FixedSizeBinaryBuilder::new(32);
    let dummy_uid = vec![0u8; 32];
    for _ in 0..3 {
        uid_builder.append_value(&dummy_uid).unwrap();
    }
    let uids = uid_builder.finish();

    // Vectors: [[0,0], [1,1], [10,10]]
    let mut vector_builder = FixedSizeListBuilder::new(Float32Builder::new(), 2);

    // Item 1: [0.0, 0.0]
    vector_builder.values().append_value(0.0);
    vector_builder.values().append_value(0.0);
    vector_builder.append(true);

    // Item 2: [1.0, 1.0]
    vector_builder.values().append_value(1.0);
    vector_builder.values().append_value(1.0);
    vector_builder.append(true);

    // Item 3: [10.0, 10.0]
    vector_builder.values().append_value(10.0);
    vector_builder.values().append_value(10.0);
    vector_builder.append(true);

    let vectors = vector_builder.finish();

    let batch = RecordBatch::try_new(
        arrow_schema.clone(),
        vec![
            Arc::new(vids),
            Arc::new(uids),
            Arc::new(deleted),
            Arc::new(versions),
            Arc::new(StringArray::from(vec![None::<&str>; 3])), // ext_id
            // _labels
            {
                let mut lb = arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new());
                for _ in 0..3 {
                    lb.values().append_value("Item");
                    lb.append(true);
                }
                Arc::new(lb.finish())
            },
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; 3]).with_timezone("UTC")), // _created_at
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; 3]).with_timezone("UTC")), // _updated_at
            Arc::new(vectors), // embedding
            Arc::new(arrow_array::LargeBinaryArray::from(vec![None::<&[u8]>; 3])), // overflow_json
        ],
    )?;

    vertex_ds
        .write_batch_lancedb(lancedb_store, batch, &schema_manager.schema())
        .await?;

    // 3. Search
    // Query: [0.1, 0.1]. Should match Item 1 best.
    let query = vec![0.1f32, 0.1f32];
    let results = storage
        .vector_search("Item", "embedding", &query, 2, None, None)
        .await?;

    // Expect 2 results
    assert_eq!(results.len(), 2);
    // Closest should be Item 1 (Vid 1)
    assert_eq!(results[0].0.as_u64(), 1);

    // Second closest should be Item 2 (Vid 2)
    assert_eq!(results[1].0.as_u64(), 2);

    println!("Vector Search Results: {:?}", results);

    Ok(())
}

/// Helper: create schema + storage + writer for a 2-D vector label "Item".
async fn setup_vector_env() -> anyhow::Result<(
    tempfile::TempDir,
    Arc<StorageManager>,
    Arc<SchemaManager>,
    Writer,
)> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Item")?;
    schema_manager.add_property(
        "Item",
        "embedding",
        DataType::Vector { dimensions: 2 },
        false,
    )?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    let storage_str = path.join("storage").to_str().unwrap().to_string();
    let storage = Arc::new(StorageManager::new(&storage_str, schema_manager.clone()).await?);

    let writer = Writer::new(storage.clone(), schema_manager.clone(), 0)
        .await
        .unwrap();

    Ok((temp_dir, storage, schema_manager, writer))
}

/// Helper: write a batch of (vid, embedding) pairs directly to LanceDB.
async fn write_vectors_to_lancedb(
    storage: &StorageManager,
    schema_manager: &SchemaManager,
    entries: &[(u64, [f32; 2])],
) -> anyhow::Result<()> {
    let ds = storage.vertex_dataset("Item")?;
    let arrow_schema = ds.get_arrow_schema(&schema_manager.schema())?;
    let n = entries.len();

    let vids = UInt64Array::from(entries.iter().map(|(v, _)| *v).collect::<Vec<_>>());
    let mut uid_builder = FixedSizeBinaryBuilder::new(32);
    let dummy_uid = vec![0u8; 32];
    for _ in 0..n {
        uid_builder.append_value(&dummy_uid).unwrap();
    }

    let mut vec_builder = FixedSizeListBuilder::new(Float32Builder::new(), 2);
    for (_, emb) in entries {
        vec_builder.values().append_value(emb[0]);
        vec_builder.values().append_value(emb[1]);
        vec_builder.append(true);
    }

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(vids),
            Arc::new(uid_builder.finish()),
            Arc::new(arrow_array::BooleanArray::from(vec![false; n])),
            Arc::new(UInt64Array::from(vec![1u64; n])),
            Arc::new(StringArray::from(vec![None::<&str>; n])),
            // _labels
            {
                let mut lb = arrow_array::builder::ListBuilder::new(arrow_array::builder::StringBuilder::new());
                for _ in 0..n {
                    lb.values().append_value("Item");
                    lb.append(true);
                }
                Arc::new(lb.finish())
            },
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; n]).with_timezone("UTC")),
            Arc::new(TimestampNanosecondArray::from(vec![None::<i64>; n]).with_timezone("UTC")),
            Arc::new(vec_builder.finish()),
            Arc::new(arrow_array::LargeBinaryArray::from(vec![None::<&[u8]>; n])),
        ],
    )?;

    ds.write_batch_lancedb(storage.lancedb_store(), batch, &schema_manager.schema())
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_l0_vertex_appears_in_vector_search() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let (_tmp, storage, schema_manager, mut writer) = setup_vector_env().await?;

    // Write one vertex to LanceDB (flushed).
    write_vectors_to_lancedb(&storage, &schema_manager, &[(1, [10.0, 10.0])]).await?;

    // Insert a closer vertex into L0 only (not flushed).
    let vid2 = writer.next_vid().await?;
    let mut props = HashMap::new();
    props.insert(
        "embedding".to_string(),
        serde_json::json!([0.0, 0.0]).into(),
    );
    writer
        .insert_vertex_with_labels(vid2, props, vec!["Item".to_string()])
        .await?;

    // Build QueryContext from writer's L0.
    let l0_arc = writer.l0_manager.get_current();
    let ctx = QueryContext::new(l0_arc);

    // Search near origin — L0 vertex should be closest.
    let query = vec![0.1f32, 0.1f32];
    let results = storage
        .vector_search("Item", "embedding", &query, 5, None, Some(&ctx))
        .await?;

    assert!(
        results.len() >= 2,
        "expected at least 2 results, got {}",
        results.len()
    );
    // The L0 vertex (at [0,0]) should be first.
    assert_eq!(results[0].0, vid2, "L0 vertex should be closest to query");
    // Distance to [0,0] from [0.1, 0.1] = 0.01 + 0.01 = 0.02 (L2 squared)
    assert!(
        results[0].1 < 0.1,
        "L0 vertex distance should be small, got {}",
        results[0].1
    );

    Ok(())
}

#[tokio::test]
async fn test_l0_tombstone_hides_flushed_vertex() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let (_tmp, storage, schema_manager, mut writer) = setup_vector_env().await?;

    // Write two vertices to LanceDB.
    write_vectors_to_lancedb(
        &storage,
        &schema_manager,
        &[(1, [0.0, 0.0]), (2, [1.0, 1.0])],
    )
    .await?;

    // Delete VID 1 via the writer (tombstone in L0).
    writer
        .delete_vertex(Vid::new(1), Some(vec!["Item".to_string()]))
        .await?;

    let l0_arc = writer.l0_manager.get_current();
    let ctx = QueryContext::new(l0_arc);

    let query = vec![0.0f32, 0.0f32];
    let results = storage
        .vector_search("Item", "embedding", &query, 5, None, Some(&ctx))
        .await?;

    // VID 1 should be filtered out by the tombstone.
    for (vid, _) in &results {
        assert_ne!(
            vid.as_u64(),
            1,
            "tombstoned VID 1 should not appear in results"
        );
    }
    // VID 2 should still be present.
    assert!(
        results.iter().any(|(vid, _)| vid.as_u64() == 2),
        "non-tombstoned VID 2 should remain in results"
    );

    Ok(())
}

#[tokio::test]
async fn test_l0_updated_embedding_wins() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let (_tmp, storage, schema_manager, writer) = setup_vector_env().await?;

    // Flush vertex at [10.0, 10.0] into LanceDB.
    write_vectors_to_lancedb(&storage, &schema_manager, &[(1, [10.0, 10.0])]).await?;

    // Update VID 1's embedding in L0 to [0.0, 0.0] (much closer to the query).
    let l0_arc = writer.l0_manager.get_current();
    {
        let mut l0 = l0_arc.write();
        let mut props = HashMap::new();
        props.insert(
            "embedding".to_string(),
            serde_json::json!([0.0, 0.0]).into(),
        );
        l0.vertex_properties.insert(Vid::new(1), props);
        l0.vertex_labels
            .insert(Vid::new(1), vec!["Item".to_string()]);
    }

    let ctx = QueryContext::new(l0_arc);

    let query = vec![0.1f32, 0.1f32];
    let results = storage
        .vector_search("Item", "embedding", &query, 5, None, Some(&ctx))
        .await?;

    assert!(!results.is_empty(), "should have at least one result");
    assert_eq!(results[0].0.as_u64(), 1, "VID 1 should still be top result");
    // With L0 embedding [0,0], distance = 0.02 (L2). With flushed [10,10] it would be ~198.
    assert!(
        results[0].1 < 1.0,
        "L0 updated embedding should produce small distance, got {}",
        results[0].1
    );

    Ok(())
}
