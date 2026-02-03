// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::core::id::{Eid, Vid};
use uni_db::core::schema::SchemaManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::direction::Direction;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_warm_from_delta_storage() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Node")?;
    let type_id = schema_manager.add_edge_type("REL", vec!["Node".into()], vec!["Node".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    // Write edge to storage directly via DeltaDataset
    let delta = storage.delta_dataset("REL", "fwd")?;
    let schema = schema_manager.schema();

    use uni_db::storage::delta::{L1Entry, Op};
    let op = L1Entry {
        src_vid: Vid::new(0),
        dst_vid: Vid::new(1),
        eid: Eid::new(0),
        op: Op::Insert,
        version: 1,
        properties: Default::default(),
        created_at: None,
        updated_at: None,
    };

    let batch = delta.build_record_batch(&[op], &schema)?;
    let lancedb_store = storage.lancedb_store();
    delta.write_run_lancedb(lancedb_store, batch).await?;

    // Warm AM from storage
    storage
        .warm_adjacency(type_id, Direction::Outgoing, Some(1))
        .await?;

    // Check neighbors via AM
    let am = storage.adjacency_manager();
    let neighbors = am.get_neighbors(Vid::new(0), type_id, Direction::Outgoing);
    assert_eq!(neighbors.len(), 1);
    assert_eq!(neighbors[0].0, Vid::new(1));

    Ok(())
}

#[tokio::test]
async fn test_overlay_neighbors_via_writer() -> anyhow::Result<()> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path();
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    schema_manager.add_label("Node")?;
    let type_id = schema_manager.add_edge_type("REL", vec!["Node".into()], vec!["Node".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    use uni_db::UniConfig;
    let mut writer = Writer::new_with_config(
        storage.clone(),
        schema_manager.clone(),
        0,
        UniConfig::default(),
        None,
        None,
    )
    .await?;

    let src = Vid::new(0);
    let dst = Vid::new(1);
    let eid = Eid::new(0);

    // Insert via Writer (dual-writes to L0 data + AM overlay)
    writer
        .insert_edge(src, dst, type_id, eid, HashMap::new())
        .await?;

    // Edge should be immediately visible in AM overlay
    let am = storage.adjacency_manager();
    let outgoing = am.get_neighbors(src, type_id, Direction::Outgoing);
    assert_eq!(outgoing.len(), 1);
    assert_eq!(outgoing[0], (dst, eid));

    // Also visible in Incoming direction
    let incoming = am.get_neighbors(dst, type_id, Direction::Incoming);
    assert_eq!(incoming.len(), 1);
    assert_eq!(incoming[0], (src, eid));

    Ok(())
}
