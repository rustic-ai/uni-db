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
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
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
    wal.append(Mutation::InsertVertex {
        vid: Vid::new(100),
        properties: HashMap::new(),
        labels: vec![],
        created_at: None,
        updated_at: None,
    })?;
    let lsn1 = wal.flush().await?;

    wal.append(Mutation::InsertVertex {
        vid: Vid::new(101),
        properties: HashMap::new(),
        labels: vec![],
        created_at: None,
        updated_at: None,
    })?;
    let lsn2 = wal.flush().await?;

    wal.append(Mutation::InsertVertex {
        vid: Vid::new(102),
        properties: HashMap::new(),
        labels: vec![],
        created_at: None,
        updated_at: None,
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
    wal.append(Mutation::InsertVertex {
        vid: Vid::new(100),
        properties: HashMap::new(),
        labels: vec![],
        created_at: None,
        updated_at: None,
    })?;
    let lsn1 = wal.flush().await?;

    wal.append(Mutation::InsertVertex {
        vid: Vid::new(101),
        properties: HashMap::new(),
        labels: vec![],
        created_at: None,
        updated_at: None,
    })?;
    let lsn2 = wal.flush().await?;

    wal.append(Mutation::InsertVertex {
        vid: Vid::new(102),
        properties: HashMap::new(),
        labels: vec![],
        created_at: None,
        updated_at: None,
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
    wal.append(Mutation::InsertVertex {
        vid: Vid::new(100),
        properties: HashMap::new(),
        labels: vec![],
        created_at: None,
        updated_at: None,
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
        wal.append(Mutation::InsertVertex {
            vid: Vid::new(100 + i),
            properties: HashMap::new(),
            labels: vec![],
            created_at: None,
            updated_at: None,
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
            wal.append(Mutation::InsertVertex {
                vid: Vid::new(100 + i),
                properties: HashMap::new(),
                labels: vec![],
                created_at: None,
                updated_at: None,
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
    wal2.append(Mutation::InsertVertex {
        vid: Vid::new(105),
        properties: HashMap::new(),
        labels: vec![],
        created_at: None,
        updated_at: None,
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
    wal.append(Mutation::InsertEdge {
        src_vid,
        dst_vid,
        edge_type: 1,
        eid: test_eid,
        version: 1,
        properties: [("weight".to_string(), uni_common::Value::Float(1.5))]
            .into_iter()
            .collect(),
        edge_type_name: None,
        created_at: None,
        updated_at: None,
    })?;

    // Delete edge
    wal.append(Mutation::DeleteEdge {
        eid: test_eid,
        src_vid,
        dst_vid,
        edge_type: 1,
        version: 2,
        edge_type_name: None,
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
    wal.append(Mutation::InsertVertex {
        vid: test_vid,
        properties: [(
            "name".to_string(),
            uni_common::Value::String("Test".to_string()),
        )]
        .into_iter()
        .collect(),
        labels: vec![],
        created_at: None,
        updated_at: None,
    })?;

    wal.append(Mutation::DeleteVertex {
        vid: test_vid,
        labels: vec![],
    })?;

    wal.flush().await?;

    let mutations = wal.replay().await?;
    assert_eq!(mutations.len(), 2);

    match &mutations[1] {
        Mutation::DeleteVertex { vid, .. } => {
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
        wal.append(Mutation::InsertVertex {
            vid: Vid::new(100 + i),
            properties: HashMap::new(),
            labels: vec![],
            created_at: None,
            updated_at: None,
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
    assert!(!lsns.is_empty() || wal.flushed_lsn().unwrap() > 0);

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
    assert_eq!(wal.flushed_lsn().unwrap(), 0);

    // Flush with data
    wal.append(Mutation::InsertVertex {
        vid: Vid::new(100),
        properties: HashMap::new(),
        labels: vec![],
        created_at: None,
        updated_at: None,
    })?;
    wal.flush().await?;

    assert_eq!(wal.flushed_lsn().unwrap(), 1);

    // Another flush
    wal.append(Mutation::InsertVertex {
        vid: Vid::new(101),
        properties: HashMap::new(),
        labels: vec![],
        created_at: None,
        updated_at: None,
    })?;
    wal.flush().await?;

    assert_eq!(wal.flushed_lsn().unwrap(), 2);

    Ok(())
}

#[tokio::test]
async fn test_wal_full_truncate() -> Result<()> {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Create segments
    for i in 0..5 {
        wal.append(Mutation::InsertVertex {
            vid: Vid::new(100 + i),
            properties: HashMap::new(),
            labels: vec![],
            created_at: None,
            updated_at: None,
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

// ============================================================================
// Issue #76 Regression Tests: Tombstone Label Flushing
// ============================================================================

/// Test that replay restores vertex labels from InsertVertex mutations
#[tokio::test]
async fn test_replay_restores_vertex_labels() -> Result<()> {
    use uni_store::runtime::L0Buffer;

    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Create mutations with labels
    let mutations = vec![
        Mutation::InsertVertex {
            vid: Vid::new(100),
            properties: HashMap::new(),
            labels: vec!["Person".to_string(), "User".to_string()],
            created_at: None,
            updated_at: None,
        },
        Mutation::InsertVertex {
            vid: Vid::new(101),
            properties: HashMap::new(),
            labels: vec!["Person".to_string()],
            created_at: None,
            updated_at: None,
        },
    ];

    // Serialize to WAL
    for m in &mutations {
        wal.append(m.clone())?;
    }
    wal.flush().await?;

    // Replay into L0 buffer
    let replayed = wal.replay().await?;
    let mut l0 = L0Buffer::new(0, None);
    l0.replay_mutations(replayed)?;

    // Verify labels were restored
    let labels_100 = l0.vertex_labels.get(&Vid::new(100)).unwrap();
    assert_eq!(labels_100.len(), 2);
    assert!(labels_100.contains(&"Person".to_string()));
    assert!(labels_100.contains(&"User".to_string()));

    let labels_101 = l0.vertex_labels.get(&Vid::new(101)).unwrap();
    assert_eq!(labels_101.len(), 1);
    assert!(labels_101.contains(&"Person".to_string()));

    Ok(())
}

/// Test that replay of DeleteVertex preserves labels for tombstoned vertices
#[tokio::test]
async fn test_replay_delete_preserves_labels_for_flush() -> Result<()> {
    use uni_store::runtime::L0Buffer;

    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Insert then delete with labels preserved
    let mutations = vec![
        Mutation::InsertVertex {
            vid: Vid::new(100),
            properties: HashMap::new(),
            labels: vec!["Person".to_string()],
            created_at: None,
            updated_at: None,
        },
        Mutation::DeleteVertex {
            vid: Vid::new(100),
            labels: vec!["Person".to_string()],
        },
    ];

    for m in &mutations {
        wal.append(m.clone())?;
    }
    wal.flush().await?;

    // Replay into L0
    let replayed = wal.replay().await?;
    let mut l0 = L0Buffer::new(0, None);
    l0.replay_mutations(replayed)?;

    // Verify vertex is tombstoned but labels are still present in vertex_labels map
    assert!(l0.vertex_tombstones.contains(&Vid::new(100)));
    let labels = l0.vertex_labels.get(&Vid::new(100)).unwrap();
    assert_eq!(labels.len(), 1);
    assert!(labels.contains(&"Person".to_string()));

    Ok(())
}

/// Test backward compatibility: deserialize old WAL without labels field
#[tokio::test]
async fn test_wal_serde_backward_compat_missing_labels() -> Result<()> {
    // Simulate old WAL format without labels field
    let old_insert = r#"{"InsertVertex":{"vid":100,"properties":{}}}"#;
    let old_delete = r#"{"DeleteVertex":{"vid":100}}"#;

    // Should deserialize with default empty labels
    let insert: Mutation = serde_json::from_str(old_insert)?;
    match insert {
        Mutation::InsertVertex { vid, labels, .. } => {
            assert_eq!(vid.as_u64(), 100);
            assert!(labels.is_empty());
        }
        _ => panic!("Expected InsertVertex"),
    }

    let delete: Mutation = serde_json::from_str(old_delete)?;
    match delete {
        Mutation::DeleteVertex { vid, labels } => {
            assert_eq!(vid.as_u64(), 100);
            assert!(labels.is_empty());
        }
        _ => panic!("Expected DeleteVertex"),
    }

    Ok(())
}

/// A WAL segment written before `created_at`/`updated_at` existed must still
/// replay, with both timestamps unset — exactly the behaviour those segments
/// already had. `#[serde(default)]` is what makes the field additive rather
/// than a format break; this pins it, as the `labels` test above does.
#[tokio::test]
async fn test_wal_serde_backward_compat_missing_timestamps() -> Result<()> {
    use uni_store::runtime::L0Buffer;

    let old_vertex = r#"{"InsertVertex":{"vid":100,"properties":{},"labels":["Person"]}}"#;
    let old_edge = r#"{"InsertEdge":{"src_vid":100,"dst_vid":101,"edge_type":0,"eid":1,"version":1,"properties":{}}}"#;

    let vertex: Mutation = serde_json::from_str(old_vertex)?;
    match &vertex {
        Mutation::InsertVertex {
            vid,
            created_at,
            updated_at,
            ..
        } => {
            assert_eq!(vid.as_u64(), 100);
            assert!(
                created_at.is_none(),
                "pre-timestamp segment must decode to None"
            );
            assert!(
                updated_at.is_none(),
                "pre-timestamp segment must decode to None"
            );
        }
        _ => panic!("Expected InsertVertex"),
    }

    let edge: Mutation = serde_json::from_str(old_edge)?;
    match &edge {
        Mutation::InsertEdge {
            eid,
            created_at,
            updated_at,
            ..
        } => {
            assert_eq!(eid.as_u64(), 1);
            assert!(
                created_at.is_none(),
                "pre-timestamp segment must decode to None"
            );
            assert!(
                updated_at.is_none(),
                "pre-timestamp segment must decode to None"
            );
        }
        _ => panic!("Expected InsertEdge"),
    }

    // And replay must accept them rather than, say, unwrapping the Option.
    let mut l0 = L0Buffer::new(0, None);
    l0.replay_mutations(vec![vertex, edge])?;
    assert!(
        !l0.vertex_created_at.contains_key(&Vid::new(100)),
        "a timestamp-less record must leave the timestamp unset, not invent one"
    );
    assert!(
        !l0.edge_created_at.contains_key(&Eid::new(1)),
        "a timestamp-less record must leave the timestamp unset, not invent one"
    );

    Ok(())
}

/// Timestamps stamped by the live write path must survive a WAL round-trip.
/// Without this the recovered row carries `None` and the next flush bakes a
/// permanent null into L1 — silent, and user-visible via `created_at(n)`.
#[tokio::test]
async fn test_wal_timestamps_round_trip() -> Result<()> {
    use uni_store::runtime::L0Buffer;

    let vid = Vid::new(7);
    let eid = Eid::new(8);
    let created = 1_700_000_000_000_000_000_i64;
    let updated = created + 5_000_000_000;

    let mutations = [
        Mutation::InsertVertex {
            vid,
            properties: HashMap::new(),
            labels: vec!["Person".to_string()],
            created_at: Some(created),
            updated_at: Some(updated),
        },
        Mutation::InsertVertex {
            vid: Vid::new(9),
            properties: HashMap::new(),
            labels: vec!["Person".to_string()],
            created_at: Some(created),
            updated_at: Some(updated),
        },
        Mutation::InsertEdge {
            src_vid: vid,
            dst_vid: Vid::new(9),
            edge_type: 0,
            eid,
            version: 1,
            properties: HashMap::new(),
            edge_type_name: Some("KNOWS".to_string()),
            created_at: Some(created),
            updated_at: Some(updated),
        },
    ];

    // Through serde, not just in memory: the point is that the WAL carries them.
    let round_tripped: Vec<Mutation> = mutations
        .iter()
        .map(|m| serde_json::to_string(m).and_then(|s| serde_json::from_str(&s)))
        .collect::<std::result::Result<_, _>>()?;

    let mut l0 = L0Buffer::new(0, None);
    l0.replay_mutations(round_tripped)?;

    assert_eq!(l0.vertex_created_at.get(&vid).copied(), Some(created));
    assert_eq!(l0.vertex_updated_at.get(&vid).copied(), Some(updated));
    assert_eq!(l0.edge_created_at.get(&eid).copied(), Some(created));
    assert_eq!(l0.edge_updated_at.get(&eid).copied(), Some(updated));

    // `created_at` keeps the oldest value across a re-write of the same vid,
    // mirroring `apply_vertex_write`'s `or_insert`; `updated_at` takes the newest.
    let later = updated + 60_000_000_000;
    l0.replay_mutations(vec![Mutation::InsertVertex {
        vid,
        properties: HashMap::new(),
        labels: vec![],
        created_at: Some(later),
        updated_at: Some(later),
    }])?;
    assert_eq!(
        l0.vertex_created_at.get(&vid).copied(),
        Some(created),
        "a later write must not move created_at"
    );
    assert_eq!(l0.vertex_updated_at.get(&vid).copied(), Some(later));

    Ok(())
}

/// Test full WAL cycle: append DeleteVertex with labels, flush, replay
#[tokio::test]
async fn test_wal_delete_vertex_labels_round_trip() -> Result<()> {
    use uni_store::runtime::L0Buffer;

    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    // Create DeleteVertex with labels
    let delete_mutation = Mutation::DeleteVertex {
        vid: Vid::new(100),
        labels: vec!["Person".to_string(), "Admin".to_string()],
    };

    wal.append(delete_mutation)?;
    wal.flush().await?;

    // Replay in new L0
    let replayed = wal.replay().await?;
    assert_eq!(replayed.len(), 1);

    let mut l0 = L0Buffer::new(0, None);
    l0.replay_mutations(replayed)?;

    // Verify labels were preserved through the cycle
    let labels = l0.vertex_labels.get(&Vid::new(100)).unwrap();
    assert_eq!(labels.len(), 2);
    assert!(labels.contains(&"Person".to_string()));
    assert!(labels.contains(&"Admin".to_string()));

    Ok(())
}

/// `SetVertexLabels` (G7) round-trips through the WAL and REPLACES, not appends,
/// on replay — so a replayed label *removal* actually removes and the reverse
/// index is rebuilt correctly.
#[tokio::test]
async fn test_wal_set_vertex_labels_round_trip_replace() -> Result<()> {
    use uni_store::runtime::L0Buffer;

    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));
    let vid = Vid::new(100);

    // A vertex created with two labels, then a label-only change that drops one:
    // the WAL carries the FULL resolved set [Person].
    wal.append(Mutation::InsertVertex {
        vid,
        properties: HashMap::new(),
        labels: vec!["Person".to_string(), "Admin".to_string()],
        created_at: None,
        updated_at: None,
    })?;
    wal.append(Mutation::SetVertexLabels {
        vid,
        labels: vec!["Person".to_string()],
    })?;
    wal.flush().await?;

    let replayed = wal.replay().await?;
    let mut l0 = L0Buffer::new(0, None);
    l0.replay_mutations(replayed)?;

    // REPLACE semantics: only Person remains (Admin removed) and the reverse
    // index reflects it.
    assert_eq!(
        l0.vertex_labels.get(&vid).unwrap(),
        &vec!["Person".to_string()],
        "replay must REPLACE, not append"
    );
    assert!(l0.label_to_vids.get("Person").unwrap().contains(&vid));
    assert!(
        l0.label_to_vids
            .get("Admin")
            .is_none_or(|s| !s.contains(&vid)),
        "Admin must be unindexed after replace"
    );

    // The new variant round-trips through JSON unchanged.
    let json = serde_json::to_string(&Mutation::SetVertexLabels {
        vid,
        labels: vec!["X".to_string()],
    })?;
    let back: Mutation = serde_json::from_str(&json)?;
    assert!(matches!(back, Mutation::SetVertexLabels { .. }));
    Ok(())
}

// ============================================================================
// Edge Type Name WAL Tests (Issue #28, #102)
// ============================================================================

/// Test that edge type names are preserved through WAL round-trip
#[tokio::test]
async fn test_edge_type_name_wal_roundtrip() -> Result<()> {
    use uni_store::runtime::L0Buffer;

    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));

    let test_eid = Eid::new(1100);
    let src_vid = Vid::new(100);
    let dst_vid = Vid::new(200);

    // Append InsertEdge with edge_type_name
    wal.append(Mutation::InsertEdge {
        src_vid,
        dst_vid,
        edge_type: 1,
        eid: test_eid,
        version: 1,
        properties: HashMap::new(),
        edge_type_name: Some("KNOWS".to_string()),
        created_at: None,
        updated_at: None,
    })?;
    wal.flush().await?;

    // Replay into L0 buffer
    let replayed = wal.replay().await?;
    let mut l0 = L0Buffer::new(0, None);
    l0.replay_mutations(replayed)?;

    // Verify edge type name was restored
    let edge_type_name = l0.get_edge_type(test_eid);
    assert_eq!(
        edge_type_name,
        Some("KNOWS"),
        "Edge type name should be restored from WAL"
    );

    Ok(())
}

/// Test backward compatibility: deserialize old WAL without edge_type_name field
#[tokio::test]
async fn test_wal_serde_backward_compat_missing_edge_type_name() -> Result<()> {
    // Simulate old WAL format without edge_type_name field
    let old_insert_edge = r#"{"InsertEdge":{"src_vid":100,"dst_vid":200,"edge_type":1,"eid":1100,"version":1,"properties":{}}}"#;

    // Should deserialize with default None for edge_type_name
    let mutation: Mutation = serde_json::from_str(old_insert_edge)?;
    match mutation {
        Mutation::InsertEdge {
            eid,
            edge_type,
            edge_type_name,
            ..
        } => {
            assert_eq!(eid.as_u64(), 1100);
            assert_eq!(edge_type, 1);
            assert_eq!(
                edge_type_name, None,
                "edge_type_name should default to None for old WAL format"
            );
        }
        _ => panic!("Expected InsertEdge mutation"),
    }

    Ok(())
}

// ── WAL Corruption Recovery Tests ────────────────────────────────────
//
// Policy (architecture review §2.5): a corrupt segment at the TAIL of the
// log is a torn write from a crash — recovery skips it with a warning and
// keeps everything before it. A corrupt segment with valid segments AFTER
// it is real data loss and fails recovery with an error naming the file.

#[tokio::test]
async fn test_wal_truncated_tail_segment_skipped() {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));
    wal.initialize().await.unwrap();

    // Write a valid segment first
    wal.append(Mutation::InsertVertex {
        vid: Vid::new(1),
        labels: vec!["Person".to_string()],
        properties: HashMap::new(),
        created_at: None,
        updated_at: None,
    })
    .unwrap();
    wal.flush().await.unwrap();

    // Write a truncated (invalid JSON) segment directly, at the tail
    let truncated_path = Path::from("wal/00000000000000000099_bad.wal");
    store
        .put(
            &truncated_path,
            bytes::Bytes::from(b"{\"lsn\":99,\"mutations\":[{\"Inser" as &[u8]).into(),
        )
        .await
        .unwrap();

    // The corrupt tail is treated as end-of-WAL: the valid segment before
    // it replays, the torn one is skipped.
    let mutations = wal.replay_since(0).await.unwrap();
    assert_eq!(mutations.len(), 1, "valid segment before torn tail replays");
}

#[tokio::test]
async fn test_wal_corrupt_only_segment_is_tolerated_tail() {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));
    wal.initialize().await.unwrap();

    // Write random bytes as the only WAL segment — it is the tail.
    let bad_path = Path::from("wal/00000000000000000001_corrupt.wal");
    store
        .put(
            &bad_path,
            bytes::Bytes::from(vec![0xDE, 0xAD, 0xBE, 0xEF]).into(),
        )
        .await
        .unwrap();

    let mutations = wal.replay_since(0).await.unwrap();
    assert!(mutations.is_empty(), "corrupt tail yields no mutations");
}

#[tokio::test]
async fn test_wal_empty_segment_file_is_corrupt_tail() {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));
    wal.initialize().await.unwrap();

    // A zero-byte segment is a torn write: tolerated only at the tail.
    let empty_path = Path::from("wal/00000000000000000001_empty.wal");
    store
        .put(&empty_path, bytes::Bytes::new().into())
        .await
        .unwrap();

    let mutations = wal.replay_since(0).await.unwrap();
    assert!(mutations.is_empty());
}

#[tokio::test]
async fn test_wal_corrupt_middle_segment_fails_recovery() {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));
    wal.initialize().await.unwrap();

    // Valid segment at LSN 1.
    wal.append(Mutation::InsertVertex {
        vid: Vid::new(1),
        labels: vec!["Person".to_string()],
        properties: HashMap::new(),
        created_at: None,
        updated_at: None,
    })
    .unwrap();
    wal.flush().await.unwrap();

    // Corrupt segment at LSN 2 (the middle).
    let bad_path = Path::from("wal/00000000000000000002_corrupt.wal");
    store
        .put(&bad_path, bytes::Bytes::from(vec![0xDE, 0xAD]).into())
        .await
        .unwrap();

    // Valid segment at LSN 3 — written directly so its LSN is above the
    // corrupt one (the WAL instance's counter is at 2).
    wal.append(Mutation::InsertVertex {
        vid: Vid::new(3),
        labels: vec!["Person".to_string()],
        properties: HashMap::new(),
        created_at: None,
        updated_at: None,
    })
    .unwrap();
    wal.flush().await.unwrap();
    wal.append(Mutation::InsertVertex {
        vid: Vid::new(4),
        labels: vec!["Person".to_string()],
        properties: HashMap::new(),
        created_at: None,
        updated_at: None,
    })
    .unwrap();
    wal.flush().await.unwrap();

    // A corrupt segment with valid segments after it must fail recovery,
    // naming the file.
    let err = wal
        .replay_since(0)
        .await
        .expect_err("corrupt middle segment must fail recovery");
    let msg = err.to_string();
    assert!(
        msg.contains("00000000000000000002_corrupt.wal"),
        "error must name the corrupt file, got: {msg}"
    );
    assert!(
        msg.contains("refusing to skip"),
        "error must state the policy, got: {msg}"
    );
}

#[tokio::test]
async fn test_wal_checksum_mismatch_detected() {
    use futures::TryStreamExt as _;

    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));
    wal.initialize().await.unwrap();

    // Flush a valid (enveloped) segment, then flip a payload byte on disk.
    wal.append(Mutation::InsertVertex {
        vid: Vid::new(1),
        labels: vec!["Person".to_string()],
        properties: HashMap::new(),
        created_at: None,
        updated_at: None,
    })
    .unwrap();
    wal.flush().await.unwrap();

    let metas = store
        .list(Some(&Path::from("wal")))
        .map_ok(|m| m.location)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(metas.len(), 1);
    let seg_path = metas[0].clone();
    let mut bytes = store
        .get(&seg_path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec();
    // Flip the last payload byte (header stays intact → checksum mismatch).
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF;
    store
        .put(&seg_path, bytes::Bytes::from(bytes).into())
        .await
        .unwrap();

    // Single (tail) segment with bad checksum: skipped, not replayed.
    let mutations = wal.replay_since(0).await.unwrap();
    assert!(
        mutations.is_empty(),
        "checksum-mismatched segment must not replay"
    );
}

#[tokio::test]
async fn test_wal_legacy_raw_json_segment_replays() {
    let store = create_memory_store();
    let wal = WriteAheadLog::new(store.clone(), Path::from("wal"));
    wal.initialize().await.unwrap();

    // Hand-write a pre-2.0.7 segment: raw JSON, no checksum envelope.
    let legacy = serde_json::json!({
        "lsn": 5,
        "mutations": [
            { "InsertVertex": { "vid": 42, "properties": {} } }
        ]
    });
    let legacy_path = Path::from("wal/00000000000000000005_legacy.wal");
    store
        .put(
            &legacy_path,
            bytes::Bytes::from(serde_json::to_vec(&legacy).unwrap()).into(),
        )
        .await
        .unwrap();

    let mutations = wal.replay_since(0).await.unwrap();
    assert_eq!(mutations.len(), 1, "legacy segment must stay readable");
    match &mutations[0] {
        Mutation::InsertVertex { vid, .. } => assert_eq!(u64::from(*vid), 42),
        other => panic!("expected InsertVertex, got {other:?}"),
    }
}

// ============================================================================
// Bug #9 (Mechanism B): WAL recovery must rebuild the unique constraint_index
// ============================================================================

/// Regression for Bug #9 (Mechanism B): a unique constraint must still be
/// enforced against rows that were recovered from the WAL but never flushed
/// to Lance (L1).
///
/// The unique-constraint check (`Writer::check_unique_constraint_multi`)
/// consults three sources: the in-memory `constraint_index` on main L0, the
/// transaction's L0, and Lance. On crash recovery, `L0Buffer::replay_mutations`
/// restores vertices/properties/labels/edges but never calls
/// `insert_constraint_key` — its only caller is the live insert path. So after
/// reopening a database from its WAL, the recovered (WAL-resident, not-yet-
/// flushed) unique keys are invisible to all three sources, and a duplicate of
/// a recovered key can be created.
///
/// This test commits a `Person { email: "a@x" }` vertex (durable in the WAL,
/// not flushed to Lance), rebuilds the `Writer` over the same storage directory
/// so `replay_wal` runs, then inserts a SECOND `Person { email: "a@x" }`. The
/// second insert MUST fail with a constraint violation.
///
/// RED state today: step 5 SUCCEEDS (returns `Ok`) because the recovered
/// `constraint_index` is empty and the row is not in Lance.
#[tokio::test]
async fn unique_constraint_survives_wal_recovery() {
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjectStorePath;
    use uni_common::Value;
    use uni_common::config::UniConfig;
    use uni_common::core::schema::{Constraint, ConstraintTarget, ConstraintType, SchemaManager};
    use uni_store::runtime::writer::Writer;
    use uni_store::storage::manager::StorageManager;

    fn email_props(value: &str) -> HashMap<String, Value> {
        let mut props = HashMap::new();
        props.insert("email".to_string(), Value::String(value.to_string()));
        props
    }

    // A config that disables auto-flush to L1 so the inserted row stays in the
    // WAL + L0 and never reaches Lance — exactly the recovery window the bug
    // lives in.
    fn no_autoflush_config() -> UniConfig {
        UniConfig {
            auto_flush_threshold: usize::MAX,
            auto_flush_interval: None,
            ..Default::default()
        }
    }

    // Build a schema with a UNIQUE constraint on Person.email and persist it.
    async fn build_schema(store: Arc<dyn ObjectStore>) -> Arc<SchemaManager> {
        let schema_path = ObjectStorePath::from("schema.json");
        let schema = Arc::new(
            SchemaManager::load_from_store(store, &schema_path)
                .await
                .unwrap(),
        );
        schema.add_label("Person").unwrap();
        schema
            .add_constraint(Constraint {
                name: "Person_email_unique".to_string(),
                constraint_type: ConstraintType::Unique {
                    properties: vec!["email".to_string()],
                },
                target: ConstraintTarget::Label("Person".to_string()),
                enabled: true,
            })
            .unwrap();
        schema.save().await.unwrap();
        schema
    }

    let dir = tempfile::tempdir().unwrap();
    let storage_path = dir.path().join("storage");
    std::fs::create_dir_all(&storage_path).unwrap();
    let storage_path_str = storage_path.to_str().unwrap().to_string();

    // The schema store is rooted at the temp dir; the storage (and therefore the
    // WAL) is rooted at `<temp>/storage`. Both are stable across the reopen.
    let schema_store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

    // 1. Open a writer with the UNIQUE constraint and a WAL.
    {
        let schema = build_schema(schema_store.clone()).await;
        let storage = Arc::new(
            StorageManager::new_with_config(
                &storage_path_str,
                schema.clone(),
                no_autoflush_config(),
            )
            .await
            .unwrap(),
        );
        let wal = Arc::new(
            WriteAheadLog::new(storage.store(), Path::from("wal"))
                .with_local_root(storage.local_fs_root()),
        );
        wal.initialize().await.unwrap();
        let writer = Arc::new(
            Writer::new_with_config(
                storage,
                schema,
                1,
                no_autoflush_config(),
                Some(wal),
                None,
                None,
            )
            .await
            .unwrap(),
        );

        // 2. Insert Person { email: "a@x" } inside a transaction and commit so it
        //    is durable in the WAL. Do NOT flush to L1.
        let vid = writer.next_vid().await.unwrap();
        let tx = writer.create_transaction_l0();
        writer
            .insert_vertex_with_labels(vid, email_props("a@x"), &["Person".to_string()], Some(&tx))
            .await
            .unwrap();
        writer.commit_transaction_l0(tx).await.unwrap();

        // 3. Drop the writer/storage without flushing — the row lives only in
        //    the WAL + L0, never in Lance.
    }

    // 4. Reopen over the same storage directory: a fresh Writer whose main L0 is
    //    rebuilt purely by replaying the WAL.
    let schema = Arc::new(
        SchemaManager::load_from_store(schema_store, &ObjectStorePath::from("schema.json"))
            .await
            .unwrap(),
    );
    let storage = Arc::new(
        StorageManager::new_with_config(&storage_path_str, schema.clone(), no_autoflush_config())
            .await
            .unwrap(),
    );
    let wal = Arc::new(
        WriteAheadLog::new(storage.store(), Path::from("wal"))
            .with_local_root(storage.local_fs_root()),
    );
    let wal_max = wal.initialize().await.unwrap();
    let writer = Arc::new(
        Writer::new_with_config(
            storage,
            schema,
            1,
            no_autoflush_config(),
            Some(wal),
            None,
            None,
        )
        .await
        .unwrap(),
    );
    let replayed = writer.replay_wal(0).await.unwrap();
    assert!(
        replayed >= 1 && wal_max >= 1,
        "WAL recovery must restore the committed vertex (replayed={replayed}, wal_max={wal_max})"
    );

    // 5. A SECOND Person with the same email must be rejected as a duplicate.
    let vid2 = writer.next_vid().await.unwrap();
    let result = writer
        .insert_vertex_with_labels(vid2, email_props("a@x"), &["Person".to_string()], None)
        .await;
    assert!(
        result.is_err(),
        "duplicate of a WAL-recovered unique key must be rejected, but the insert succeeded \
         (Bug #9 Mechanism B: replay_wal never rebuilds constraint_index)"
    );
}

// ============================================================================
// Transaction Commit Atomicity Tests (Issue #137)
// ============================================================================
// NOTE: These tests are currently disabled because they rely on an old Writer API
// that used UniConfig::new() instead of Writer::new(storage, schema, writer_id).
// The transaction API tests should be re-implemented using the current API patterns
// once the transaction feature is fully integrated with the new storage architecture.

/// A committed transaction's mutations must appear in the WAL exactly once.
///
/// Regression test for the commit double-write: `L0Buffer::merge` used to
/// re-call the public `delete_vertex` / `insert_edge` / `delete_edge` during
/// `commit_transaction_l0`, re-appending every edge insert, edge delete, and
/// vertex delete to the WAL — the commit path had already appended all of
/// them before the durable flush (writer.rs "THIS IS THE COMMIT POINT"),
/// so edge-heavy commits paid 2x WAL volume. Only vertex inserts were
/// exempt via `insert_vertex_with_labels_impl(skip_wal=true)`.
///
/// The duplicates landed in the WAL buffer *after* the commit's flush, so
/// they were flushed inside the NEXT commit's segment — tx3 below exists to
/// surface tx2's would-be duplicates.
#[tokio::test]
async fn commit_writes_each_mutation_to_wal_exactly_once() -> Result<()> {
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjectStorePath;
    use uni_common::Value;
    use uni_common::config::UniConfig;
    use uni_common::core::schema::SchemaManager;
    use uni_store::runtime::l0::L0Buffer;
    use uni_store::runtime::writer::Writer;
    use uni_store::storage::manager::StorageManager;

    fn props() -> HashMap<String, Value> {
        let mut m = HashMap::new();
        m.insert("p".to_string(), Value::Int(1));
        m
    }

    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager =
        Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
    schema_manager.add_label("Node")?;
    schema_manager.save().await?;
    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let wal = Arc::new(WriteAheadLog::new(
        store.clone(),
        ObjectStorePath::from("wal"),
    ));
    let no_autoflush = UniConfig {
        auto_flush_threshold: usize::MAX,
        auto_flush_interval: None,
        ..Default::default()
    };
    let writer = Arc::new(
        Writer::new_with_config(
            storage,
            schema_manager.clone(),
            1,
            no_autoflush,
            Some(wal),
            None,
            None,
        )
        .await?,
    );

    let etype = schema_manager.get_or_assign_edge_type_id("REL");
    let v1 = writer.next_vid().await?;
    let v2 = writer.next_vid().await?;
    let e1 = writer.next_eid(etype).await?;
    let label = ["Node".to_string()];

    // Tx 1: two vertices + an edge between them.
    let tx1 = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(v1, props(), &label, Some(&tx1))
        .await?;
    writer
        .insert_vertex_with_labels(v2, props(), &label, Some(&tx1))
        .await?;
    writer
        .insert_edge(
            v1,
            v2,
            etype,
            e1,
            props(),
            Some("REL".to_string()),
            Some(&tx1),
        )
        .await?;
    writer.commit_transaction_l0(tx1).await?;

    // Tx 2: delete the edge and one endpoint.
    let tx2 = writer.create_transaction_l0();
    writer.delete_edge(e1, v1, v2, etype, Some(&tx2)).await?;
    writer.delete_vertex(v2, None, Some(&tx2)).await?;
    writer.commit_transaction_l0(tx2).await?;

    // Tx 3: unrelated commit whose flush would carry tx2's duplicates.
    let v3 = writer.next_vid().await?;
    let tx3 = writer.create_transaction_l0();
    writer
        .insert_vertex_with_labels(v3, props(), &label, Some(&tx3))
        .await?;
    writer.commit_transaction_l0(tx3).await?;

    // Replay the durable WAL and count entries per mutation.
    let replay_wal = WriteAheadLog::new(store, ObjectStorePath::from("wal"));
    let mutations = replay_wal.replay().await?;

    let insert_edge_e1 = mutations
        .iter()
        .filter(|m| matches!(m, Mutation::InsertEdge { eid, .. } if *eid == e1))
        .count();
    let delete_edge_e1 = mutations
        .iter()
        .filter(|m| matches!(m, Mutation::DeleteEdge { eid, .. } if *eid == e1))
        .count();
    let delete_vertex_v2 = mutations
        .iter()
        .filter(|m| matches!(m, Mutation::DeleteVertex { vid, .. } if *vid == v2))
        .count();
    assert_eq!(insert_edge_e1, 1, "edge insert WAL-logged exactly once");
    assert_eq!(delete_edge_e1, 1, "edge delete WAL-logged exactly once");
    assert_eq!(delete_vertex_v2, 1, "vertex delete WAL-logged exactly once");

    // Recovery sanity: replaying the deduplicated WAL yields the right state.
    let mut recovered = L0Buffer::new(1, None);
    recovered.replay_mutations(mutations)?;
    assert!(recovered.vertex_properties.contains_key(&v1));
    assert!(recovered.vertex_properties.contains_key(&v3));
    assert!(recovered.vertex_tombstones.contains(&v2));
    assert!(recovered.tombstones.contains_key(&e1));

    Ok(())
}
