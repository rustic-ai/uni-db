// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

#![recursion_limit = "256"]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tempfile::tempdir;
use tokio::sync::RwLock;
use uni_db::core::id::{Eid, Vid};
use uni_db::core::schema::SchemaManager;
use uni_db::runtime::writer::Writer;
use uni_db::storage::manager::StorageManager;

#[tokio::test]
async fn test_stress_concurrent_read_write() -> anyhow::Result<()> {
    // Stress Test: Single Writer, Multiple Readers
    // Writer inserts nodes/edges continuously.
    // Readers traverse random paths.

    let _ = env_logger::builder().is_test(true).try_init();
    let temp_dir = tempdir()?;
    let path = temp_dir.path();

    // 1. Setup
    let schema_manager = SchemaManager::load(&path.join("schema.json")).await?;
    let _l_node = schema_manager.add_label("Node")?;
    let t_link = schema_manager.add_edge_type("LINK", vec!["Node".into()], vec!["Node".into()])?;
    schema_manager.save().await?;

    let schema_manager = Arc::new(schema_manager);
    let storage = Arc::new(
        StorageManager::new(
            path.join("storage").to_str().unwrap(),
            schema_manager.clone(),
        )
        .await?,
    );

    // Writer is protected by RwLock (simulating single writer actor)
    let writer = Arc::new(RwLock::new(
        Writer::new(storage.clone(), schema_manager.clone(), 0)
            .await
            .unwrap(),
    ));

    let start = Instant::now();
    let duration = std::time::Duration::from_secs(5);

    let num_readers = 4;
    let mut set = tokio::task::JoinSet::new();

    // Shared counter for max VID
    let max_vid = Arc::new(std::sync::atomic::AtomicU64::new(0));

    // 2. Writer Task
    let writer_clone = writer.clone();
    let max_vid_clone = max_vid.clone();
    set.spawn(async move {
        let mut i = 0;

        while start.elapsed() < duration {
            {
                let mut w = writer_clone.write().await;
                // Insert a batch
                for _ in 0..100 {
                    i += 1;
                    let vid = Vid::new(i);
                    // Link to random previous node
                    if i > 1 {
                        let prev = {
                            use rand::Rng;
                            let mut rng = rand::thread_rng();
                            rng.gen_range(1..i)
                        };
                        let vid_prev = Vid::new(prev);
                        let eid = Eid::new(i); // Simplistic EID

                        w.insert_edge(vid_prev, vid, t_link, eid, HashMap::new())
                            .await
                            .unwrap();
                    }
                }
                // Periodically flush?
                if i % 1000 == 0 {
                    w.flush_to_l1(None).await.unwrap();
                }
            }
            max_vid_clone.store(i, std::sync::atomic::Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        println!("Writer finished. Total nodes: {}", i);
    });

    // 3. Reader Tasks
    for id in 0..num_readers {
        let storage_clone = storage.clone();
        let max_vid_clone = max_vid.clone();

        set.spawn(async move {
            let mut ops = 0;

            while start.elapsed() < duration {
                let max = max_vid_clone.load(std::sync::atomic::Ordering::Relaxed);
                if max > 10 {
                    let start_node = {
                        use rand::Rng;
                        let mut rng = rand::thread_rng();
                        rng.gen_range(1..max)
                    };
                    let vid = Vid::new(start_node);

                    // Traverse 1 hop
                    // L0 might be locked by writer, but load_subgraph takes &L0Buffer if passed.
                    // Here we test pure storage read (L1/L2) + L0 if exposed.
                    // Since Writer holds L0, and we don't have easy access to read-locked L0 from Writer here without refactoring,
                    // we will test reading COMMITTED data (flushed to L1).
                    // Or we acquire read lock on writer to get L0?

                    // For this test, let's just read from storage (L1/L2).
                    // This verifies that background flushing doesn't break readers.

                    let _ = storage_clone
                        .load_subgraph(
                            &[vid],
                            &[t_link],
                            1,
                            uni_db::runtime::Direction::Outgoing,
                            None, // No L0 access for now
                        )
                        .await;

                    ops += 1;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            println!("Reader {} finished. Ops: {}", id, ops);
        });
    }

    while let Some(res) = set.join_next().await {
        res?;
    }

    Ok(())
}
