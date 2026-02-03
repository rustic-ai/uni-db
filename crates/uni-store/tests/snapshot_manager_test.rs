// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tests for SnapshotManager functionality.
//!
//! Tests cover:
//! - Basic save/load operations
//! - Listing snapshots
//! - Latest snapshot pointer management
//! - Edge cases (corrupted manifests, missing pointers)
//! - Concurrent operations

use anyhow::Result;
use chrono::Utc;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::core::snapshot::SnapshotManifest;
use uni_store::SnapshotManager;

/// Creates a test SnapshotManifest with the given ID.
fn create_test_manifest(id: &str) -> SnapshotManifest {
    SnapshotManifest {
        snapshot_id: id.to_string(),
        name: Some(format!("Test snapshot {}", id)),
        created_at: Utc::now(),
        parent_snapshot: None,
        schema_version: 1,
        version_high_water_mark: 100,
        wal_high_water_mark: 50,
        vertices: HashMap::new(),
        edges: HashMap::new(),
    }
}

#[tokio::test]
async fn test_save_and_load_snapshot() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let manager = SnapshotManager::new(store);

    let manifest = create_test_manifest("snap-001");
    manager.save_snapshot(&manifest).await?;

    let loaded = manager.load_snapshot("snap-001").await?;
    assert_eq!(loaded.snapshot_id, "snap-001");
    assert_eq!(loaded.name, Some("Test snapshot snap-001".to_string()));
    assert_eq!(loaded.schema_version, 1);
    assert_eq!(loaded.version_high_water_mark, 100);
    assert_eq!(loaded.wal_high_water_mark, 50);

    Ok(())
}

#[tokio::test]
async fn test_list_snapshots() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let manager = SnapshotManager::new(store);

    // Initially empty
    let snapshots = manager.list_snapshots().await?;
    assert!(snapshots.is_empty());

    // Add some snapshots
    manager
        .save_snapshot(&create_test_manifest("snap-001"))
        .await?;
    manager
        .save_snapshot(&create_test_manifest("snap-002"))
        .await?;
    manager
        .save_snapshot(&create_test_manifest("snap-003"))
        .await?;

    let snapshots = manager.list_snapshots().await?;
    assert_eq!(snapshots.len(), 3);
    assert!(snapshots.contains(&"snap-001".to_string()));
    assert!(snapshots.contains(&"snap-002".to_string()));
    assert!(snapshots.contains(&"snap-003".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_latest_snapshot_pointer() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let manager = SnapshotManager::new(store);

    // Initially no latest snapshot
    let latest = manager.load_latest_snapshot().await?;
    assert!(latest.is_none());

    // Save and set latest
    let manifest = create_test_manifest("snap-001");
    manager.save_snapshot(&manifest).await?;
    manager.set_latest_snapshot("snap-001").await?;

    let latest = manager.load_latest_snapshot().await?;
    assert!(latest.is_some());
    assert_eq!(latest.unwrap().snapshot_id, "snap-001");

    // Update latest to a new snapshot
    let manifest2 = create_test_manifest("snap-002");
    manager.save_snapshot(&manifest2).await?;
    manager.set_latest_snapshot("snap-002").await?;

    let latest = manager.load_latest_snapshot().await?;
    assert!(latest.is_some());
    assert_eq!(latest.unwrap().snapshot_id, "snap-002");

    Ok(())
}

#[tokio::test]
async fn test_load_nonexistent_snapshot() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let manager = SnapshotManager::new(store);

    let result = manager.load_snapshot("nonexistent").await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_corrupted_manifest_handling() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let manager = SnapshotManager::new(store.clone());

    // Manually write a corrupted manifest
    let corrupted_path = Path::from("catalog/manifests/corrupted.json");
    store
        .put(&corrupted_path, "{ invalid json }".into())
        .await?;

    // Attempting to load should return an error
    let result = manager.load_snapshot("corrupted").await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    // Should be a JSON parse error
    assert!(
        err.contains("expected")
            || err.contains("invalid")
            || err.contains("JSON")
            || err.contains("key must be a string"),
        "Expected JSON parse error, got: {}",
        err
    );

    Ok(())
}

#[tokio::test]
async fn test_empty_latest_pointer_handling() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let manager = SnapshotManager::new(store.clone());

    // Manually write an empty latest pointer
    let latest_path = Path::from("catalog/latest");
    store.put(&latest_path, "".into()).await?;

    // Should return None for empty pointer
    let latest = manager.load_latest_snapshot().await?;
    assert!(latest.is_none());

    // Also test whitespace-only pointer
    store.put(&latest_path, "   \n  ".into()).await?;
    let latest = manager.load_latest_snapshot().await?;
    assert!(latest.is_none());

    Ok(())
}

#[tokio::test]
async fn test_latest_pointer_with_dangling_reference() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let manager = SnapshotManager::new(store.clone());

    // Set latest to a snapshot that doesn't exist
    let latest_path = Path::from("catalog/latest");
    store.put(&latest_path, "nonexistent-snap".into()).await?;

    // Should return an error (not found)
    let result = manager.load_latest_snapshot().await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_overwrite_existing_snapshot() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let manager = SnapshotManager::new(store);

    // Save initial snapshot
    let mut manifest = create_test_manifest("snap-001");
    manifest.version_high_water_mark = 100;
    manager.save_snapshot(&manifest).await?;

    // Overwrite with updated manifest
    manifest.version_high_water_mark = 200;
    manifest.name = Some("Updated snapshot".to_string());
    manager.save_snapshot(&manifest).await?;

    // Load and verify updated values
    let loaded = manager.load_snapshot("snap-001").await?;
    assert_eq!(loaded.version_high_water_mark, 200);
    assert_eq!(loaded.name, Some("Updated snapshot".to_string()));

    Ok(())
}

#[tokio::test]
async fn test_snapshot_with_vertex_and_edge_data() -> Result<()> {
    use uni_common::core::snapshot::{EdgeSnapshot, LabelSnapshot};

    let dir = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let manager = SnapshotManager::new(store);

    let mut manifest = create_test_manifest("snap-full");

    // Add vertex labels
    manifest.vertices.insert(
        "Person".to_string(),
        LabelSnapshot {
            version: 1,
            count: 1000,
            lance_version: 5,
        },
    );
    manifest.vertices.insert(
        "Company".to_string(),
        LabelSnapshot {
            version: 1,
            count: 500,
            lance_version: 3,
        },
    );

    // Add edge types
    manifest.edges.insert(
        "WORKS_AT".to_string(),
        EdgeSnapshot {
            version: 1,
            count: 800,
            lance_version: 4,
        },
    );

    manager.save_snapshot(&manifest).await?;

    let loaded = manager.load_snapshot("snap-full").await?;
    assert_eq!(loaded.vertices.len(), 2);
    assert_eq!(loaded.edges.len(), 1);

    let person = loaded.vertices.get("Person").unwrap();
    assert_eq!(person.count, 1000);
    assert_eq!(person.lance_version, 5);

    let works_at = loaded.edges.get("WORKS_AT").unwrap();
    assert_eq!(works_at.count, 800);

    Ok(())
}

#[tokio::test]
async fn test_concurrent_snapshot_saves() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let manager = Arc::new(SnapshotManager::new(store));

    // Spawn concurrent saves
    let mut handles = Vec::new();
    for i in 0..10 {
        let manager = manager.clone();
        let handle = tokio::spawn(async move {
            let manifest = create_test_manifest(&format!("concurrent-{:03}", i));
            manager.save_snapshot(&manifest).await
        });
        handles.push(handle);
    }

    // Wait for all saves to complete
    for handle in handles {
        handle.await??;
    }

    // Verify all snapshots were saved
    let snapshots = manager.list_snapshots().await?;
    assert_eq!(snapshots.len(), 10);

    Ok(())
}

#[tokio::test]
async fn test_snapshot_id_with_special_characters() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let manager = SnapshotManager::new(store);

    // Test with various ID formats
    let ids = vec![
        "snap-2024-01-15T10:30:00Z",
        "snapshot_with_underscores",
        "SNAPSHOT-UPPERCASE",
        "mixed-Case_123",
    ];

    for id in &ids {
        let manifest = create_test_manifest(id);
        manager.save_snapshot(&manifest).await?;
        let loaded = manager.load_snapshot(id).await?;
        assert_eq!(loaded.snapshot_id, *id);
    }

    Ok(())
}
