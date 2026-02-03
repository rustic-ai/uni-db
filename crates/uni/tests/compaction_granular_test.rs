// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_db::Uni;
use uni_db::core::schema::{DataType, SchemaManager};

#[tokio::test]
async fn test_granular_compaction_public_api() -> anyhow::Result<()> {
    // 1. Setup
    let temp_dir = tempfile::tempdir()?;
    let path = temp_dir.path();
    let schema_path = path.join("schema.json");

    // Create schema using SchemaManager to ensure correct format
    {
        let manager = SchemaManager::load(&schema_path).await?;
        manager.add_label("Node")?;
        manager.add_property("Node", "name", DataType::String, true)?;
        manager.add_edge_type("REL", vec!["Node".into()], vec!["Node".into()])?;
        manager.save().await?;
    }

    // Open Uni
    let db = Uni::open(path.to_str().unwrap()).build().await?;

    // 2. Write Data (Fragments)
    // Fragment 1
    db.execute("CREATE (:Node {name: 'A'})").await?;
    db.flush().await?;

    // Fragment 2
    db.execute("CREATE (:Node {name: 'B'})").await?;
    db.flush().await?;

    // 4. Compact Label (Granular)
    // We expect compaction to run because we have 2 fragments (files).
    let stats = db.compact_label("Node").await?;
    assert_eq!(
        stats.files_compacted, 1,
        "Expected 1 compaction operation on vertex label"
    );

    // 5. Test Edge Compaction
    // Fragment 1
    db.execute("MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) CREATE (a)-[:REL]->(b)")
        .await?;
    db.flush().await?;

    // Fragment 2
    db.execute("MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) CREATE (b)-[:REL]->(a)")
        .await?;
    db.flush().await?;

    // Compact Edge Type
    let stats_edge = db.compact_edge_type("REL").await?;
    assert!(
        stats_edge.files_compacted >= 1,
        "Expected at least 1 edge compaction"
    );

    // 7. Test Wait For Compaction (Should return immediately)
    let start = std::time::Instant::now();
    db.wait_for_compaction().await?;
    assert!(start.elapsed() < std::time::Duration::from_millis(500));

    Ok(())
}
