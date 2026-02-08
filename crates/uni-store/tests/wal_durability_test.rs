// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! WAL durability and edge case tests.
//!
//! Tests cover:
//! - LSN ordering guarantees
//! - Replay idempotency
//! - Partial segment recovery
//! - Concurrent WAL operations
//! - Empty WAL handling

use anyhow::Result;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use object_store::path::Path;
use std::collections::HashMap;
use std::sync::Arc;
use uni_common::core::id::{Eid, Vid};
use uni_store::runtime::wal::{Mutation, WriteAheadLog};

fn create_memory_store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

#[tokio::test]
async fn test_wal_lsn_ordering() -> Result<()> {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Flush multiple segments
    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(100),
        properties: HashMap::new(),
    })?;
    let lsn1 = wal.flush().await?;

    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(101),
        properties: HashMap::new(),
    })?;
    let lsn2 = wal.flush().await?;

    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(102),
        properties: HashMap::new(),
    })?;
    let lsn3 = wal.flush().await?;

    // Verify LSNs are monotonically increasing
    assert!(lsn1 < lsn2, "LSN2 should be greater than LSN1");
    assert!(lsn2 < lsn3, "LSN3 should be greater than LSN2");

    Ok(())
}

#[tokio::test]
async fn test_wal_replay_since_high_water_mark() -> Result<()> {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Create segments with known LSNs
    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(100),
        properties: HashMap::new(),
    })?;
    let lsn1 = wal.flush().await?;

    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(101),
        properties: HashMap::new(),
    })?;
    let lsn2 = wal.flush().await?;

    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(102),
        properties: HashMap::new(),
    })?;
    wal.flush().await?;

    // Replay all mutations
    let all_mutations = wal.replay_since(0).await?;
    assert_eq!(all_mutations.len(), 3, "Should have 3 mutations total");

    // Replay only mutations since LSN1 (should get 2 mutations)
    let since_lsn1 = wal.replay_since(lsn1).await?;
    assert_eq!(since_lsn1.len(), 2, "Should have 2 mutations since LSN1");

    // Replay only mutations since LSN2 (should get 1 mutation)
    let since_lsn2 = wal.replay_since(lsn2).await?;
    assert_eq!(since_lsn2.len(), 1, "Should have 1 mutation since LSN2");

    Ok(())
}

#[tokio::test]
async fn test_wal_empty_flush() -> Result<()> {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Flush empty buffer should return flushed_lsn (0 initially)
    let lsn = wal.flush().await?;
    assert_eq!(lsn, 0, "Empty flush should return current flushed_lsn");

    // Add a mutation and flush
    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(100),
        properties: HashMap::new(),
    })?;
    let lsn1 = wal.flush().await?;
    assert!(lsn1 > 0, "LSN should be positive after flush");

    // Another empty flush should return the same LSN
    let lsn2 = wal.flush().await?;
    assert_eq!(lsn1, lsn2, "Empty flush should return same LSN");

    Ok(())
}

#[tokio::test]
async fn test_wal_truncate_before_high_water_mark() -> Result<()> {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Create multiple segments
    for i in 0..5 {
        wal.append(&Mutation::InsertVertex {
            vid: Vid::new(100 + i),
            properties: HashMap::new(),
        })?;
        wal.flush().await?;
    }

    // Replay all (should have 5 mutations)
    let all = wal.replay_since(0).await?;
    assert_eq!(all.len(), 5);

    // Get LSN after 3rd flush
    let lsn3 = 3;

    // Truncate segments with LSN <= 3
    wal.truncate_before(lsn3).await?;

    // Replay should now only have mutations from LSN > 3
    let remaining = wal.replay_since(0).await?;
    assert_eq!(
        remaining.len(),
        2,
        "Should have 2 mutations after truncating first 3"
    );

    Ok(())
}

#[tokio::test]
async fn test_wal_initialize_from_existing() -> Result<()> {
    let store = create_memory_store();
    let path = Path::from("wal");

    // Create WAL and flush some segments
    {
        let wal = WriteAheadLog::new(store.clone(), path.clone());
        for i in 0..5 {
            wal.append(&Mutation::InsertVertex {
                vid: Vid::new(100 + i),
                properties: HashMap::new(),
            })?;
            wal.flush().await?;
        }
    }

    // Create new WAL instance and initialize
    let wal2 = WriteAheadLog::new(store.clone(), path);
    let max_lsn = wal2.initialize().await?;

    // Max LSN should be 5 (5 segments flushed)
    assert_eq!(
        max_lsn, 5,
        "Max LSN should match number of segments flushed"
    );

    // Next flush should get LSN 6
    wal2.append(&Mutation::InsertVertex {
        vid: Vid::new(105),
        properties: HashMap::new(),
    })?;
    let next_lsn = wal2.flush().await?;
    assert_eq!(next_lsn, 6, "Next LSN should continue from max");

    Ok(())
}

#[tokio::test]
async fn test_wal_edge_mutations() -> Result<()> {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    let test_eid = Eid::new(1100);
    let src_vid = Vid::new(100);
    let dst_vid = Vid::new(200);

    // Insert edge
    wal.append(&Mutation::InsertEdge {
        src_vid,
        dst_vid,
        edge_type: 1,
        eid: test_eid,
        version: 1,
        properties: [("weight".to_string(), uni_common::Value::Float(1.5))]
            .into_iter()
            .collect(),
    })?;

    // Delete edge
    wal.append(&Mutation::DeleteEdge {
        eid: test_eid,
        src_vid,
        dst_vid,
        edge_type: 1,
        version: 2,
    })?;

    wal.flush().await?;

    // Replay and verify
    let mutations = wal.replay().await?;
    assert_eq!(mutations.len(), 2);

    match &mutations[0] {
        Mutation::InsertEdge {
            eid,
            edge_type,
            properties,
            ..
        } => {
            assert_eq!(eid.as_u64(), 1100);
            assert_eq!(*edge_type, 1);
            assert!(properties.contains_key("weight"));
        }
        _ => panic!("Expected InsertEdge mutation"),
    }

    match &mutations[1] {
        Mutation::DeleteEdge { eid, version, .. } => {
            assert_eq!(eid.as_u64(), 1100);
            assert_eq!(*version, 2);
        }
        _ => panic!("Expected DeleteEdge mutation"),
    }

    Ok(())
}

#[tokio::test]
async fn test_wal_delete_vertex_mutation() -> Result<()> {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    let test_vid = Vid::new(100);

    // Insert then delete vertex
    wal.append(&Mutation::InsertVertex {
        vid: test_vid,
        properties: [("name".to_string(), uni_common::Value::String("Test".to_string()))]
            .into_iter()
            .collect(),
    })?;

    wal.append(&Mutation::DeleteVertex { vid: test_vid })?;

    wal.flush().await?;

    let mutations = wal.replay().await?;
    assert_eq!(mutations.len(), 2);

    match &mutations[1] {
        Mutation::DeleteVertex { vid } => {
            assert_eq!(vid.as_u64(), 100);
        }
        _ => panic!("Expected DeleteVertex mutation"),
    }

    Ok(())
}

#[tokio::test]
async fn test_wal_concurrent_flushes() -> Result<()> {
    let store = create_memory_store();
    let wal = Arc::new(WriteAheadLog::new(store.clone(), Path::from("wal")));

    // Pre-populate with some mutations
    for i in 0..100 {
        wal.append(&Mutation::InsertVertex {
            vid: Vid::new(100 + i),
            properties: HashMap::new(),
        })?;
    }

    // Spawn concurrent flush tasks
    let mut handles = Vec::new();
    for _ in 0..10 {
        let wal_clone = wal.clone();
        handles.push(tokio::spawn(async move { wal_clone.flush().await }));
    }

    // Wait for all flushes
    let mut lsns = Vec::new();
    for handle in handles {
        let lsn = handle.await??;
        if lsn > 0 {
            lsns.push(lsn);
        }
    }

    // At least one flush should succeed (the first one with data)
    assert!(!lsns.is_empty() || wal.flushed_lsn() > 0);

    // Replay should have all 100 mutations
    let mutations = wal.replay().await?;
    assert_eq!(mutations.len(), 100);

    Ok(())
}

#[tokio::test]
async fn test_wal_flushed_lsn_tracking() -> Result<()> {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Initially flushed_lsn should be 0
    assert_eq!(wal.flushed_lsn(), 0);

    // Flush with data
    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(100),
        properties: HashMap::new(),
    })?;
    wal.flush().await?;

    assert_eq!(wal.flushed_lsn(), 1);

    // Another flush
    wal.append(&Mutation::InsertVertex {
        vid: Vid::new(101),
        properties: HashMap::new(),
    })?;
    wal.flush().await?;

    assert_eq!(wal.flushed_lsn(), 2);

    Ok(())
}

#[tokio::test]
async fn test_wal_full_truncate() -> Result<()> {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Create segments
    for i in 0..5 {
        wal.append(&Mutation::InsertVertex {
            vid: Vid::new(100 + i),
            properties: HashMap::new(),
        })?;
        wal.flush().await?;
    }

    // Full truncate
    wal.truncate().await?;

    // Replay should return empty
    let mutations = wal.replay().await?;
    assert!(mutations.is_empty());

    Ok(())
}
