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
async fn test_adjacency_manager_lifecycle() -> anyhow::Result<()> {
    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup Schema
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _person_lbl = schema_manager.add_label("Person")?;
    let knows_type =
        schema_manager.add_edge_type("KNOWS", vec!["Person".into()], vec!["Person".into()])?;
    schema_manager.save().await?;
    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    // 2. Insert edges via Writer (dual-writes to L0 data + AM overlay)
    use uni_db::UniConfig;

    let mut writer = Writer::new_with_config(
        storage.clone(),
        schema_manager.clone(),
        0,
        UniConfig::default(),
        None,
        None,
    )
    .await
    .unwrap();

    let vid0 = Vid::new(0);
    let vid1 = Vid::new(1);
    let eid01 = Eid::new(0);

    writer
        .insert_edge(vid0, vid1, knows_type, eid01, HashMap::new())
        .await?;

    // 3. AM overlay should have the edge immediately (no flush needed)
    let am = storage.adjacency_manager();
    let neighbors = am.get_neighbors(vid0, knows_type, Direction::Outgoing);
    assert_eq!(neighbors.len(), 1);
    assert_eq!(neighbors[0].0, vid1);
    assert_eq!(neighbors[0].1, eid01);

    // 4. Flush to L1 — edges should STILL be visible via overlay
    writer.flush_to_l1(None).await?;

    let neighbors_after_flush = am.get_neighbors(vid0, knows_type, Direction::Outgoing);
    assert_eq!(neighbors_after_flush.len(), 1, "Edges must survive flush");

    // 5. Insert another edge
    let vid2 = Vid::new(2);
    let eid02 = Eid::new(1);

    writer
        .insert_edge(vid0, vid2, knows_type, eid02, HashMap::new())
        .await?;

    // Should see both neighbors immediately
    let neighbors_both = am.get_neighbors(vid0, knows_type, Direction::Outgoing);
    assert_eq!(neighbors_both.len(), 2);
    let n_vids: Vec<Vid> = neighbors_both.iter().map(|(v, _)| *v).collect();
    assert!(n_vids.contains(&vid1));
    assert!(n_vids.contains(&vid2));

    // 6. Flush again — both edges still visible
    writer.flush_to_l1(None).await?;

    let neighbors_final = am.get_neighbors(vid0, knows_type, Direction::Outgoing);
    assert_eq!(
        neighbors_final.len(),
        2,
        "Both edges must survive second flush"
    );

    // 7. Warm from storage (Main CSR) and verify
    storage
        .warm_adjacency(knows_type, Direction::Outgoing, None)
        .await?;

    let neighbors_warmed = am.get_neighbors(vid0, knows_type, Direction::Outgoing);
    assert_eq!(
        neighbors_warmed.len(),
        2,
        "Warmed Main CSR + overlay should show both edges"
    );

    Ok(())
}
