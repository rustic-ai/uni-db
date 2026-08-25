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
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Node {name: 'A'})").await?;
    tx.commit().await?;
    db.flush().await?;

    // Fragment 2
    let tx = db.session().tx().await?;
    tx.execute("CREATE (:Node {name: 'B'})").await?;
    tx.commit().await?;
    db.flush().await?;

    // 4. Compact Label (Granular)
    // We expect compaction to run because we have 2 fragments (files).
    let stats = db.compaction().compact("Node").await?;
    assert_eq!(
        stats.tables_optimized, 1,
        "one vertex table exists for this label"
    );
    assert_eq!(
        stats.semantic_passes, 0,
        "compact() runs no semantic pass, so crdt_merges is not measured here"
    );

    // The two fragments built above must actually merge — but not necessarily on
    // this first call. Compaction runs one flush behind: the pass immediately
    // after a flush reports no work and the next one does the merge, with
    // nothing happening in between. So retry until it does work, and fail loudly
    // if it never does.
    //
    // The old assertion here was `files_compacted == 1`, which the hardcoded
    // literal satisfied whether or not anything was compacted — which is why the
    // lag went unnoticed until the counts became real.
    let mut merged = stats.fragments_removed;
    for _ in 0..5 {
        if merged > 0 {
            break;
        }
        let again = db.compaction().compact("Node").await?;
        merged = again.fragments_removed;
    }
    assert!(
        merged >= 2,
        "the two flushed fragments never merged, across six compaction passes"
    );

    // 5. Test Edge Compaction
    // Fragment 1
    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) CREATE (a)-[:REL]->(b)")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Fragment 2
    let tx = db.session().tx().await?;
    tx.execute("MATCH (a:Node {name: 'A'}), (b:Node {name: 'B'}) CREATE (b)-[:REL]->(a)")
        .await?;
    tx.commit().await?;
    db.flush().await?;

    // Compact Edge Type
    let stats_edge = db.compaction().compact("REL").await?;
    assert!(
        stats_edge.tables_optimized >= 2,
        "fwd and bwd delta tables both exist after the edge flushes: {stats_edge:?}"
    );

    // 7. Test Wait For Compaction (Should return immediately)
    let start = std::time::Instant::now();
    db.compaction().wait().await?;
    assert!(start.elapsed() < std::time::Duration::from_millis(500));

    Ok(())
}

/// `bytes_reclaimed` is real, and this is the only way to see it.
///
/// Version cleanup only frees versions older than
/// `CompactionConfig::version_retention`, which defaults to seven days. Every
/// test database is seconds old, so at the default the field is always `0` —
/// real in production and indistinguishable from a constant everywhere it could
/// be checked. That is exactly the shape of the defect this whole change fixes
/// (#172), so the retention window is configurable and asserted from both sides.
#[tokio::test]
async fn bytes_reclaimed_is_measured_not_constant() -> anyhow::Result<()> {
    async fn run(retention: std::time::Duration) -> anyhow::Result<u64> {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        {
            let manager = SchemaManager::load(&path.join("schema.json")).await?;
            manager.add_label("Node")?;
            manager.add_property("Node", "name", DataType::String, true)?;
            manager.save().await?;
        }
        let mut config = uni_db::UniConfig::default();
        config.compaction.enabled = false;
        config.compaction.version_retention = retention;
        let db = Uni::open(path.to_str().unwrap())
            .config(config)
            .build()
            .await?;

        // Several versions, so cleanup has superseded ones to reclaim.
        for i in 0..4 {
            let tx = db.session().tx().await?;
            tx.execute(&format!("CREATE (:Node {{name: 'n{i}'}})"))
                .await?;
            tx.commit().await?;
            db.flush().await?;
        }

        let mut reclaimed = 0;
        for _ in 0..3 {
            reclaimed += db.compaction().compact("Node").await?.bytes_reclaimed;
        }
        Ok(reclaimed)
    }

    // Default window: nothing is old enough, so zero — and that zero is correct.
    assert_eq!(
        run(std::time::Duration::from_secs(7 * 24 * 3600)).await?,
        0,
        "a database seconds old reclaimed bytes under a seven-day window"
    );

    // Zero window: every superseded version is eligible, so bytes come back.
    let reclaimed = run(std::time::Duration::ZERO).await?;
    assert!(
        reclaimed > 0,
        "no bytes reclaimed even with the retention window at zero, across four \
         flushes and three compactions — the field is not wired to cleanup"
    );
    Ok(())
}
