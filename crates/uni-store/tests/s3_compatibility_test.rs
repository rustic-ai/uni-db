// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Object Store Compatibility Tests
//!
//! These tests verify that Uni's storage components work correctly with
//! object_store backends. Uses InMemory store for fast, isolated testing.

use anyhow::Result;
use bytes::Bytes;
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path;
use std::sync::Arc;

/// Creates an in-memory object store for testing
fn create_memory_store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

#[tokio::test]
async fn test_object_store_basic_operations() -> Result<()> {
    let store = create_memory_store();

    // Put object
    let path = Path::from("data/test.json");
    let data = Bytes::from(r#"{"key": "value"}"#);
    store.put(&path, data.clone().into()).await?;

    // Get object
    let result = store.get(&path).await?;
    let retrieved = result.bytes().await?;
    assert_eq!(retrieved, data);

    // List objects
    let list: Vec<_> = store
        .list(Some(&Path::from("data/")))
        .filter_map(|r| async { r.ok() })
        .collect()
        .await;

    assert_eq!(list.len(), 1);
    assert_eq!(list[0].location, path);

    // Delete object
    store.delete(&path).await?;

    // Verify deletion
    let result = store.get(&path).await;
    assert!(result.is_err());

    Ok(())
}

#[tokio::test]
async fn test_id_allocator_with_object_store() -> Result<()> {
    use uni_store::runtime::id_allocator::IdAllocator;

    let store = create_memory_store();

    // Create IdAllocator
    let path = Path::from("id_allocator.json");
    let allocator = IdAllocator::new(store.clone(), path.clone(), 100).await?;

    // Allocate some VIDs (global allocation without label_id)
    let vid1 = allocator.allocate_vid().await?;
    let vid2 = allocator.allocate_vid().await?;
    let vid3 = allocator.allocate_vid().await?;

    // Verify sequential allocation
    assert_eq!(vid1.as_u64(), 0);
    assert_eq!(vid2.as_u64(), 1);
    assert_eq!(vid3.as_u64(), 2);

    // Verify persistence - manifest should exist in store
    let result = store.get(&path).await?;
    let manifest_data = result.bytes().await?;
    assert!(!manifest_data.is_empty());

    // Allocate EIDs (global allocation without type_id)
    let eid1 = allocator.allocate_eid().await?;
    let eid2 = allocator.allocate_eid().await?;

    assert_eq!(eid1.as_u64(), 0);
    assert_eq!(eid2.as_u64(), 1);

    Ok(())
}

#[tokio::test]
async fn test_wal_with_object_store() -> Result<()> {
    use uni_common::core::id::Vid;
    use uni_store::runtime::wal::{Mutation, WriteAheadLog};

    let store = create_memory_store();

    // Create WAL
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Append entries
    let entry1 = Mutation::InsertVertex {
        vid: Vid::new(100),
        properties: [(
            "name".to_string(),
            uni_common::Value::String("Alice".to_string()),
        )]
        .into_iter()
        .collect(),
    };
    wal.append(&entry1)?;

    let entry2 = Mutation::InsertVertex {
        vid: Vid::new(101),
        properties: [(
            "name".to_string(),
            uni_common::Value::String("Bob".to_string()),
        )]
        .into_iter()
        .collect(),
    };
    wal.append(&entry2)?;

    // Flush to ensure data is persisted
    wal.flush().await?;

    // Verify WAL files exist in store
    let list: Vec<_> = store
        .list(Some(&Path::from("wal/")))
        .filter_map(|r| async { r.ok() })
        .collect()
        .await;

    assert!(!list.is_empty(), "WAL files should exist in store");

    // Create new WAL instance and replay
    let wal2 = WriteAheadLog::new(store.clone(), Path::from("wal"));
    let entries = wal2.replay().await?;

    assert_eq!(entries.len(), 2, "Should replay 2 entries");

    Ok(())
}

#[tokio::test]
async fn test_concurrent_id_allocation() -> Result<()> {
    use uni_store::runtime::id_allocator::IdAllocator;

    let store = create_memory_store();

    // Create IdAllocator with small batch size to trigger more persists
    let path = Path::from("id_allocator_concurrent.json");
    let allocator = Arc::new(IdAllocator::new(store.clone(), path, 10).await?);

    // Spawn multiple concurrent allocation tasks
    let mut handles = Vec::new();
    for _ in 0..5 {
        let alloc = allocator.clone();
        handles.push(tokio::spawn(async move {
            let mut vids = Vec::new();
            for _ in 0..20 {
                let vid = alloc.allocate_vid().await.unwrap();
                vids.push(vid.as_u64());
            }
            vids
        }));
    }

    // Collect all allocated IDs
    let mut all_ids = Vec::new();
    for handle in handles {
        all_ids.extend(handle.await?);
    }

    // Verify no duplicates (all IDs should be unique)
    all_ids.sort();
    let unique_count = all_ids.len();
    all_ids.dedup();
    assert_eq!(
        all_ids.len(),
        unique_count,
        "All allocated IDs should be unique"
    );

    // Verify sequential allocation (0..100)
    assert_eq!(all_ids.len(), 100);
    for (i, &id) in all_ids.iter().enumerate() {
        assert_eq!(id, i as u64);
    }

    Ok(())
}

#[tokio::test]
async fn test_conditional_writes() -> Result<()> {
    let store = create_memory_store();

    let path = Path::from("conditional_test.txt");
    let data1 = Bytes::from("version1");

    // First write should succeed
    let result = store.put(&path, data1.clone().into()).await?;
    let etag1 = result.e_tag.clone();

    // Write with correct etag should succeed
    let data2 = Bytes::from("version2");
    if let Some(etag) = &etag1 {
        // Try conditional update
        let update_result = store
            .put_opts(
                &path,
                data2.clone().into(),
                object_store::PutMode::Update(object_store::UpdateVersion {
                    e_tag: Some(etag.clone()),
                    version: None,
                })
                .into(),
            )
            .await;

        // InMemory store supports conditional writes
        match update_result {
            Ok(_) => {
                // Verify update
                let result = store.get(&path).await?;
                let data = result.bytes().await?;
                assert_eq!(data, data2);
            }
            Err(e) => {
                // Some stores might not support this
                println!("Conditional writes not supported: {}", e);
            }
        }
    }

    Ok(())
}

#[tokio::test]
async fn test_id_allocator_recovery() -> Result<()> {
    use uni_store::runtime::id_allocator::IdAllocator;

    let store = create_memory_store();
    let path = Path::from("id_allocator_recovery.json");

    // Create allocator and allocate some IDs
    {
        let allocator = IdAllocator::new(store.clone(), path.clone(), 100).await?;
        for _ in 0..50 {
            allocator.allocate_vid().await?;
        }
    }

    // Create new allocator instance (simulating restart)
    let allocator2 = IdAllocator::new(store.clone(), path.clone(), 100).await?;

    // Next allocation should continue from where we left off
    let next_vid = allocator2.allocate_vid().await?;

    // The next ID should be >= 50 (could be higher due to batch allocation)
    assert!(
        next_vid.as_u64() >= 50,
        "Recovery should continue from persisted state"
    );

    Ok(())
}

#[tokio::test]
async fn test_global_id_allocation() -> Result<()> {
    use uni_store::runtime::id_allocator::IdAllocator;

    let store = create_memory_store();
    let path = Path::from("id_allocator_global.json");

    let allocator = IdAllocator::new(store.clone(), path, 100).await?;

    // Allocate multiple VIDs - all should be globally unique
    let vid1 = allocator.allocate_vid().await?;
    let vid2 = allocator.allocate_vid().await?;
    let vid3 = allocator.allocate_vid().await?;
    let vid4 = allocator.allocate_vid().await?;

    // Verify all IDs are unique and sequential
    assert_eq!(vid1.as_u64(), 0);
    assert_eq!(vid2.as_u64(), 1);
    assert_eq!(vid3.as_u64(), 2);
    assert_eq!(vid4.as_u64(), 3);

    // Allocate EIDs - independent sequence from VIDs
    let eid1 = allocator.allocate_eid().await?;
    let eid2 = allocator.allocate_eid().await?;

    assert_eq!(eid1.as_u64(), 0);
    assert_eq!(eid2.as_u64(), 1);

    Ok(())
}

#[tokio::test]
async fn test_wal_large_entries() -> Result<()> {
    use uni_common::core::id::Vid;
    use uni_store::runtime::wal::{Mutation, WriteAheadLog};

    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal_large"));

    // Create entry with large properties
    let large_value = "x".repeat(10000);
    let entry = Mutation::InsertVertex {
        vid: Vid::new(100),
        properties: [
            (
                "name".to_string(),
                uni_common::Value::String("Test".to_string()),
            ),
            (
                "large_field".to_string(),
                uni_common::Value::String(large_value),
            ),
        ]
        .into_iter()
        .collect(),
    };

    wal.append(&entry)?;
    wal.flush().await?;

    // Replay and verify
    let wal2 = WriteAheadLog::new(store.clone(), Path::from("wal_large"));
    let entries = wal2.replay().await?;

    assert_eq!(entries.len(), 1);
    if let Mutation::InsertVertex { properties, .. } = &entries[0] {
        let retrieved = properties.get("large_field").unwrap();
        assert_eq!(retrieved.as_str().unwrap().len(), 10000);
    } else {
        panic!("Expected InsertVertex entry");
    }

    Ok(())
}

#[tokio::test]
async fn test_wal_truncate() -> Result<()> {
    use std::collections::HashMap;
    use uni_common::core::id::Vid;
    use uni_store::runtime::wal::{Mutation, WriteAheadLog};

    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal_truncate"));

    // Append and flush multiple entries
    for i in 0..5 {
        let entry = Mutation::InsertVertex {
            vid: Vid::new(100 + i),
            properties: HashMap::new(),
        };
        wal.append(&entry)?;
    }
    wal.flush().await?;

    // Verify entries exist
    let entries = wal.replay().await?;
    assert_eq!(entries.len(), 5, "Should have 5 entries before truncate");

    // Truncate
    wal.truncate().await?;

    // Verify truncation
    let entries_after = wal.replay().await?;
    assert_eq!(
        entries_after.len(),
        0,
        "Should have 0 entries after truncate"
    );

    Ok(())
}
