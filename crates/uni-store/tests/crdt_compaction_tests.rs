// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Compaction CRDT merge tests.
//!
//! Tests that CRDT properties are correctly merged during storage compaction,
//! including handling of different versions and tombstones.

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_common::core::schema::{CrdtType, DataType, SchemaManager};
use uni_crdt::{Crdt, GCounter, GSet, VectorClock};
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

/// Helper to create a GCounter CRDT JSON value.
fn gcounter_json(counts: &[(&str, u64)]) -> serde_json::Value {
    let mut gc = GCounter::new();
    for (actor, count) in counts {
        gc.increment(actor, *count);
    }
    serde_json::to_value(Crdt::GCounter(gc)).expect("to_value should succeed")
}

/// Helper to create a GSet CRDT JSON value.
fn gset_json(elements: &[&str]) -> serde_json::Value {
    let mut gs = GSet::new();
    for elem in elements {
        gs.add(elem.to_string());
    }
    serde_json::to_value(Crdt::GSet(gs)).expect("to_value should succeed")
}

/// Helper to create a VectorClock CRDT JSON value.
fn vector_clock_json(clocks: &[(&str, usize)]) -> serde_json::Value {
    let mut vc = VectorClock::new();
    for (actor, count) in clocks {
        for _ in 0..*count {
            vc.increment(actor);
        }
    }
    serde_json::to_value(Crdt::VectorClock(vc)).expect("to_value should succeed")
}

// ============================================================================
// Basic Compaction Tests
// ============================================================================

mod basic_compaction {
    use super::*;

    /// Test that CRDT properties are merged across multiple flushes.
    #[tokio::test]
    async fn test_crdt_merge_across_versions() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);

        // Setup schema with CRDT property
        let _label_id = schema_manager.add_label("Counter")?;
        schema_manager.add_property(
            "Counter",
            "count",
            DataType::Crdt(CrdtType::GCounter),
            true,
        )?;
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
        let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 1)
            .await
            .unwrap();
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

        let vid = writer.next_vid().await?;

        // Version 1: actor1 = 10
        let props1 = HashMap::from([("count".to_string(), gcounter_json(&[("actor1", 10)]))]);
        let _ = writer
            .insert_vertex_with_labels(vid, props1, vec!["Counter".to_string()])
            .await?;
        writer.flush_to_l1(None).await?;

        // Version 2: actor2 = 20
        let props2 = HashMap::from([("count".to_string(), gcounter_json(&[("actor2", 20)]))]);
        let _ = writer
            .insert_vertex_with_labels(vid, props2, vec!["Counter".to_string()])
            .await?;
        writer.flush_to_l1(None).await?;

        // Read and verify merge
        let result = prop_manager.get_vertex_prop(vid, "count").await?;
        let crdt: Crdt = serde_json::from_value(result)?;

        if let Crdt::GCounter(gc) = crdt {
            assert_eq!(gc.actor_count("actor1"), 10);
            assert_eq!(gc.actor_count("actor2"), 20);
            assert_eq!(gc.value(), 30);
        } else {
            panic!("Expected GCounter");
        }

        Ok(())
    }

    /// Test that CRDT properties are merged regardless of version order.
    #[tokio::test]
    async fn test_crdt_merge_regardless_of_version() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);

        let _label_id = schema_manager.add_label("Counter")?;
        schema_manager.add_property(
            "Counter",
            "count",
            DataType::Crdt(CrdtType::GCounter),
            true,
        )?;
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
        let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 1)
            .await
            .unwrap();
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

        let vid = writer.next_vid().await?;

        // Insert with multiple actors across flushes
        for i in 0..3 {
            let actor = format!("actor{}", i);
            let props = HashMap::from([(
                "count".to_string(),
                gcounter_json(&[(&actor, (i + 1) as u64 * 10)]),
            )]);
            let _ = writer
                .insert_vertex_with_labels(vid, props, vec!["Counter".to_string()])
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Read and verify all actors are merged
        let result = prop_manager.get_vertex_prop(vid, "count").await?;
        let crdt: Crdt = serde_json::from_value(result)?;

        if let Crdt::GCounter(gc) = crdt {
            // 10 + 20 + 30 = 60
            assert_eq!(gc.value(), 60);
        } else {
            panic!("Expected GCounter");
        }

        Ok(())
    }
}

// ============================================================================
// Tombstone Tests
// ============================================================================

mod tombstone_handling {
    use super::*;

    /// Test that tombstones win over CRDT values when deletion happens after creation.
    #[tokio::test]
    async fn test_tombstone_wins_over_crdt() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);

        let _label_id = schema_manager.add_label("Counter")?;
        schema_manager.add_property(
            "Counter",
            "count",
            DataType::Crdt(CrdtType::GCounter),
            true,
        )?;
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
        let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 1)
            .await
            .unwrap();

        let vid = writer.next_vid().await?;

        // Create vertex with CRDT
        let props = HashMap::from([("count".to_string(), gcounter_json(&[("actor1", 100)]))]);
        let _ = writer
            .insert_vertex_with_labels(vid, props, vec!["Counter".to_string()])
            .await?;
        writer.flush_to_l1(None).await?;

        // Delete vertex
        writer.delete_vertex(vid, None).await?;
        writer.flush_to_l1(None).await?;

        // Vertex should be deleted
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
        let result = prop_manager.get_vertex_prop(vid, "count").await?;
        assert!(result.is_null(), "Deleted vertex should return null");

        Ok(())
    }
}

// ============================================================================
// Multiple CRDT Types Tests
// ============================================================================

mod multiple_crdt_types {
    use super::*;

    /// Test compaction with multiple CRDT types on the same vertex.
    #[tokio::test]
    async fn test_multiple_crdt_types_compaction() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);

        let _label_id = schema_manager.add_label("MultiCrdt")?;
        schema_manager.add_property(
            "MultiCrdt",
            "counter",
            DataType::Crdt(CrdtType::GCounter),
            true,
        )?;
        schema_manager.add_property("MultiCrdt", "items", DataType::Crdt(CrdtType::GSet), true)?;
        schema_manager.add_property(
            "MultiCrdt",
            "clock",
            DataType::Crdt(CrdtType::VectorClock),
            true,
        )?;
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
        let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 1)
            .await
            .unwrap();
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

        let vid = writer.next_vid().await?;

        // First flush: partial CRDTs
        let props1 = HashMap::from([
            ("counter".to_string(), gcounter_json(&[("a", 10)])),
            ("items".to_string(), gset_json(&["x", "y"])),
        ]);
        let _ = writer
            .insert_vertex_with_labels(vid, props1, vec!["MultiCrdt".to_string()])
            .await?;
        writer.flush_to_l1(None).await?;

        // Second flush: more CRDTs
        let props2 = HashMap::from([
            ("counter".to_string(), gcounter_json(&[("b", 20)])),
            ("items".to_string(), gset_json(&["z"])),
            ("clock".to_string(), vector_clock_json(&[("n1", 2)])),
        ]);
        let _ = writer
            .insert_vertex_with_labels(vid, props2, vec!["MultiCrdt".to_string()])
            .await?;
        writer.flush_to_l1(None).await?;

        // Verify all CRDTs merged correctly
        let counter = prop_manager.get_vertex_prop(vid, "counter").await?;
        let crdt: Crdt = serde_json::from_value(counter)?;
        if let Crdt::GCounter(gc) = crdt {
            assert_eq!(gc.value(), 30);
        } else {
            panic!("Expected GCounter");
        }

        let items = prop_manager.get_vertex_prop(vid, "items").await?;
        let crdt: Crdt = serde_json::from_value(items)?;
        if let Crdt::GSet(gs) = crdt {
            assert_eq!(gs.len(), 3); // {x, y, z}
        } else {
            panic!("Expected GSet");
        }

        let clock = prop_manager.get_vertex_prop(vid, "clock").await?;
        let crdt: Crdt = serde_json::from_value(clock)?;
        if let Crdt::VectorClock(vc) = crdt {
            assert_eq!(vc.get("n1"), 2);
        } else {
            panic!("Expected VectorClock");
        }

        Ok(())
    }
}

// ============================================================================
// Mixed CRDT and Non-CRDT Properties Tests
// ============================================================================

mod mixed_properties {
    use super::*;

    /// Test that CRDT properties merge while non-CRDT properties use LWW.
    #[tokio::test]
    async fn test_mixed_crdt_and_regular_properties() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);

        let _label_id = schema_manager.add_label("MixedNode")?;
        schema_manager.add_property(
            "MixedNode",
            "counter",
            DataType::Crdt(CrdtType::GCounter),
            true,
        )?;
        schema_manager.add_property("MixedNode", "name", DataType::String, true)?;
        schema_manager.add_property("MixedNode", "score", DataType::Int64, true)?;
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
        let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 1)
            .await
            .unwrap();
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

        let vid = writer.next_vid().await?;

        // First write
        let props1 = HashMap::from([
            ("counter".to_string(), gcounter_json(&[("a", 10)])),
            ("name".to_string(), json!("Alice")),
            ("score".to_string(), json!(100)),
        ]);
        let _ = writer
            .insert_vertex_with_labels(vid, props1, vec!["MixedNode".to_string()])
            .await?;
        writer.flush_to_l1(None).await?;

        // Second write
        let props2 = HashMap::from([
            ("counter".to_string(), gcounter_json(&[("b", 20)])),
            ("name".to_string(), json!("Bob")),
            ("score".to_string(), json!(200)),
        ]);
        let _ = writer
            .insert_vertex_with_labels(vid, props2, vec!["MixedNode".to_string()])
            .await?;
        writer.flush_to_l1(None).await?;

        // CRDT should be merged
        let counter = prop_manager.get_vertex_prop(vid, "counter").await?;
        let crdt: Crdt = serde_json::from_value(counter)?;
        if let Crdt::GCounter(gc) = crdt {
            assert_eq!(gc.value(), 30); // 10 + 20
        } else {
            panic!("Expected GCounter");
        }

        // Non-CRDT properties should use LWW (latest value)
        let name = prop_manager.get_vertex_prop(vid, "name").await?;
        assert_eq!(name, json!("Bob"));

        let score = prop_manager.get_vertex_prop(vid, "score").await?;
        assert_eq!(score, json!(200));

        Ok(())
    }
}

// ============================================================================
// Large Scale Tests
// ============================================================================

mod large_scale {
    use super::*;

    /// Test CRDT merge with many actors across many flushes.
    #[tokio::test]
    async fn test_many_actors_many_flushes() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);

        let _label_id = schema_manager.add_label("Counter")?;
        schema_manager.add_property(
            "Counter",
            "count",
            DataType::Crdt(CrdtType::GCounter),
            true,
        )?;
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
        let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 1)
            .await
            .unwrap();
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

        let vid = writer.next_vid().await?;

        // 10 actors, each incrementing by their index * 10
        let num_actors = 10;
        let mut expected_total: u64 = 0;

        for i in 0..num_actors {
            let actor = format!("actor{}", i);
            let value = (i + 1) as u64 * 10;
            expected_total += value;

            let props = HashMap::from([("count".to_string(), gcounter_json(&[(&actor, value)]))]);
            let _ = writer
                .insert_vertex_with_labels(vid, props, vec!["Counter".to_string()])
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Verify all actors merged
        let result = prop_manager.get_vertex_prop(vid, "count").await?;
        let crdt: Crdt = serde_json::from_value(result)?;

        if let Crdt::GCounter(gc) = crdt {
            assert_eq!(gc.value(), expected_total);
            // Verify each actor's count
            for i in 0..num_actors {
                let actor = format!("actor{}", i);
                let expected = (i + 1) as u64 * 10;
                assert_eq!(gc.actor_count(&actor), expected);
            }
        } else {
            panic!("Expected GCounter");
        }

        Ok(())
    }
}

// ============================================================================
// Edge Compaction Tests
// ============================================================================

mod edge_compaction {
    use super::*;

    /// Test that CRDT properties on edges are merged during compaction.
    #[tokio::test]
    async fn test_edge_crdt_compaction() -> anyhow::Result<()> {
        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);

        let _label_id = schema_manager.add_label("Node")?;
        let edge_type = schema_manager.add_edge_type(
            "CONNECTS",
            vec!["Node".to_string()],
            vec!["Node".to_string()],
        )?;
        schema_manager.add_property(
            "CONNECTS",
            "weight",
            DataType::Crdt(CrdtType::GCounter),
            true,
        )?;
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
        let mut writer = Writer::new(storage.clone(), schema_manager.clone(), 1)
            .await
            .unwrap();
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

        let vid_a = writer.next_vid().await?;
        let vid_b = writer.next_vid().await?;

        // Create vertices
        let _ = writer
            .insert_vertex_with_labels(vid_a, HashMap::new(), vec!["Node".to_string()])
            .await?;
        let _ = writer
            .insert_vertex_with_labels(vid_b, HashMap::new(), vec!["Node".to_string()])
            .await?;

        // Create edge with CRDT weight
        let eid = writer.next_eid(edge_type).await?;
        let props1 = HashMap::from([("weight".to_string(), gcounter_json(&[("a", 5)]))]);
        writer
            .insert_edge(vid_a, vid_b, edge_type, eid, props1)
            .await?;
        writer.flush_to_l1(None).await?;

        // Update edge weight
        let props2 = HashMap::from([("weight".to_string(), gcounter_json(&[("b", 10)]))]);
        writer
            .insert_edge(vid_a, vid_b, edge_type, eid, props2)
            .await?;
        writer.flush_to_l1(None).await?;

        // Verify edge CRDT merged
        let weight = prop_manager.get_edge_prop(eid, "weight", None).await?;
        let crdt: Crdt = serde_json::from_value(weight)?;

        if let Crdt::GCounter(gc) = crdt {
            assert_eq!(gc.value(), 15); // 5 + 10
        } else {
            panic!("Expected GCounter");
        }

        Ok(())
    }
}
