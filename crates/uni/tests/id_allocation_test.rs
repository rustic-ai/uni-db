// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use std::sync::Arc;
use tempfile::tempdir;
use uni_db::runtime::id_allocator::IdAllocator;

#[tokio::test]
async fn test_id_allocation_persistence_and_restart() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let path = ObjectStorePath::from("id_allocator.json");
    let batch_size = 10;

    // --- Phase 1: First Run ---
    {
        let allocator = IdAllocator::new(store.clone(), path.clone(), batch_size).await?;

        // Allocate first VID (pure auto-increment)
        let vid1 = allocator.allocate_vid().await?;
        assert_eq!(vid1.as_u64(), 0);

        // Allocate second VID
        let vid2 = allocator.allocate_vid().await?;
        assert_eq!(vid2.as_u64(), 1);

        // Allocate first EID
        let eid1 = allocator.allocate_eid().await?;
        assert_eq!(eid1.as_u64(), 0);

        // Verify manifest file was created via object store listing?
        // Or just trust subsequent restart
    }

    // --- Phase 2: Restart (Simulate Crash/Reload) ---
    {
        // Initialize new allocator pointing to same file
        let allocator = IdAllocator::new(store.clone(), path.clone(), batch_size).await?;

        // The new allocator initializes 'current' counters from the manifest values.
        // Manifest had 10. So next VID should be 10 (skipping 2..9).
        let vid_restart = allocator.allocate_vid().await?;
        assert_eq!(
            vid_restart.as_u64(),
            10,
            "Should skip to next batch start on restart"
        );

        // Allocating again should be sequential within the new batch
        let vid_next = allocator.allocate_vid().await?;
        assert_eq!(vid_next.as_u64(), 11);

        // Check EID behavior too
        let eid_restart = allocator.allocate_eid().await?;
        assert_eq!(
            eid_restart.as_u64(),
            10,
            "Should skip to next batch start for EID"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_sequential_allocation() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let path = ObjectStorePath::from("sequential_allocator.json");
    let allocator = IdAllocator::new(store, path, 100).await?;

    // VIDs are now globally sequential (pure auto-increment)
    let v1 = allocator.allocate_vid().await?;
    assert_eq!(v1.as_u64(), 0);

    let v2 = allocator.allocate_vid().await?;
    assert_eq!(v2.as_u64(), 1);

    let v3 = allocator.allocate_vid().await?;
    assert_eq!(v3.as_u64(), 2);

    Ok(())
}
