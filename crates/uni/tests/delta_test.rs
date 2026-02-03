// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::{Eid, Vid};
use uni_db::core::schema::SchemaManager;
use uni_db::storage::delta::{L1Entry, Op};
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_delta_operations() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let path = dir.path();

    // Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _ = schema_manager.add_label("Person")?;
    let _ = schema_manager.add_edge_type("KNOWS", vec!["Person".into()], vec!["Person".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);

    // Create StorageManager to access LanceDB
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );
    let lancedb_store = storage.lancedb_store();

    // Create DeltaDataset
    let delta = storage.delta_dataset("KNOWS", "fwd")?;

    // Create ops
    let vid1 = Vid::new(1);
    let vid2 = Vid::new(2);
    let eid1 = Eid::new(1);

    let op1 = L1Entry {
        src_vid: vid1,
        dst_vid: vid2,
        eid: eid1,
        op: Op::Insert,
        version: 1,
        properties: HashMap::new(),
        created_at: None,
        updated_at: None,
    };

    let op2 = L1Entry {
        src_vid: vid1,
        dst_vid: vid2,
        eid: eid1,
        op: Op::Delete,
        version: 2,
        properties: HashMap::new(),
        created_at: None,
        updated_at: None,
    };

    // Write deltas using LanceDB
    let schema = schema_manager.schema();
    let batch = delta.build_record_batch(&[op1.clone(), op2.clone()], &schema)?;
    delta.write_run_lancedb(lancedb_store, batch).await?;

    // Read deltas for vid1 using LanceDB
    let results = delta
        .read_deltas_lancedb(lancedb_store, vid1, &schema, None)
        .await?;

    assert_eq!(results.len(), 2);

    // Verify first op
    assert_eq!(results[0].src_vid, vid1);
    assert_eq!(results[0].dst_vid, vid2);
    assert_eq!(results[0].eid, eid1);
    // Lance usually returns in insertion order if not sorted.
    // Let's check versions.

    let v1 = results
        .iter()
        .find(|r| r.version == 1)
        .expect("Version 1 not found");
    assert_eq!(v1.op, Op::Insert);

    let v2 = results
        .iter()
        .find(|r| r.version == 2)
        .expect("Version 2 not found");
    assert_eq!(v2.op, Op::Delete);

    // Test with non-existent VID
    let vid3 = Vid::new(3);
    let empty_results = delta
        .read_deltas_lancedb(lancedb_store, vid3, &schema, None)
        .await?;
    assert!(empty_results.is_empty());

    Ok(())
}
