// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! End-to-end lifecycle tests for CRDT types.
//!
//! Tests the full lifecycle of CRDTs from creation through storage and retrieval,
//! verifying merge semantics at each layer.

use std::collections::HashMap;
use tempfile::tempdir;
use uni_crdt::{Crdt, GCounter, GSet, LWWMap, LWWRegister, ORSet, Rga, VCRegister, VectorClock};
use uni_db::Uni;
use uni_db::core::id::Vid;
use uni_db::core::schema::{CrdtType, DataType};
use uni_db::runtime::property_manager::PropertyManager;

/// Helper to create a test database.
async fn create_test_db() -> anyhow::Result<Uni> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path().to_str().unwrap().to_string();
    // Keep temp_dir alive by leaking it (for test purposes)
    std::mem::forget(temp_dir);
    let db = Uni::open(&path).build().await?;
    Ok(db)
}

// ============================================================================
// GCounter Lifecycle Tests
// ============================================================================

mod gcounter_lifecycle {
    use super::*;

    #[tokio::test]
    async fn test_gcounter_full_lifecycle() -> anyhow::Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let db = create_test_db().await?;

        // 1. Schema setup
        db.schema()
            .label("CounterNode")
            .property("counter", DataType::Crdt(CrdtType::GCounter))
            .done()
            .apply()
            .await?;

        let schema_manager = db.schema_manager();
        let _label_id = schema_manager
            .schema()
            .labels
            .get("CounterNode")
            .unwrap()
            .id;
        let vid = Vid::new(1);

        // 2. Create with initial CRDT value
        let mut gc1 = GCounter::new();
        gc1.increment("actor1", 10);
        let val1 = serde_json::to_value(Crdt::GCounter(gc1))?;

        let writer_lock = db.writer().unwrap();
        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("counter".to_string(), val1)]),
                    vec!["CounterNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // 3. Update (merge with new actor)
        let mut gc2 = GCounter::new();
        gc2.increment("actor2", 20);
        let val2 = serde_json::to_value(Crdt::GCounter(gc2))?;

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("counter".to_string(), val2)]),
                    vec!["CounterNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // 4. Query and verify merge
        let storage = db.storage();
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

        let result = prop_manager.get_vertex_prop(vid, "counter").await?;
        let crdt: Crdt = serde_json::from_value(result)?;

        if let Crdt::GCounter(gc) = crdt {
            assert_eq!(gc.value(), 30); // 10 + 20
            assert_eq!(gc.actor_count("actor1"), 10);
            assert_eq!(gc.actor_count("actor2"), 20);
        } else {
            panic!("Expected GCounter");
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_gcounter_many_actors() -> anyhow::Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let db = create_test_db().await?;

        db.schema()
            .label("CounterNode")
            .property("counter", DataType::Crdt(CrdtType::GCounter))
            .done()
            .apply()
            .await?;

        let schema_manager = db.schema_manager();
        let _label_id = schema_manager
            .schema()
            .labels
            .get("CounterNode")
            .unwrap()
            .id;
        let vid = Vid::new(1);

        let writer_lock = db.writer().unwrap();

        // Write from 5 different actors
        for i in 0..5 {
            let mut gc = GCounter::new();
            gc.increment(&format!("actor{}", i), (i + 1) as u64 * 10);
            let val = serde_json::to_value(Crdt::GCounter(gc))?;

            {
                let mut writer = writer_lock.write().await;
                writer
                    .insert_vertex_with_labels(
                        vid,
                        HashMap::from([("counter".to_string(), val)]),
                        vec!["CounterNode".to_string()],
                    )
                    .await?;
                writer.flush_to_l1(None).await?;
            }
        }

        let storage = db.storage();
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

        let result = prop_manager.get_vertex_prop(vid, "counter").await?;
        let crdt: Crdt = serde_json::from_value(result)?;

        if let Crdt::GCounter(gc) = crdt {
            // 10 + 20 + 30 + 40 + 50 = 150
            assert_eq!(gc.value(), 150);
        } else {
            panic!("Expected GCounter");
        }

        Ok(())
    }
}

// ============================================================================
// GSet Lifecycle Tests
// ============================================================================

mod gset_lifecycle {
    use super::*;

    #[tokio::test]
    async fn test_gset_full_lifecycle() -> anyhow::Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let db = create_test_db().await?;

        db.schema()
            .label("SetNode")
            .property("items", DataType::Crdt(CrdtType::GSet))
            .done()
            .apply()
            .await?;

        let schema_manager = db.schema_manager();
        let _label_id = schema_manager.schema().labels.get("SetNode").unwrap().id;
        let vid = Vid::new(1);

        let writer_lock = db.writer().unwrap();

        // Add first set of items
        let mut gs1 = GSet::new();
        gs1.add("apple".to_string());
        gs1.add("banana".to_string());
        let val1 = serde_json::to_value(Crdt::GSet(gs1))?;

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("items".to_string(), val1)]),
                    vec!["SetNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Add more items
        let mut gs2 = GSet::new();
        gs2.add("cherry".to_string());
        gs2.add("date".to_string());
        let val2 = serde_json::to_value(Crdt::GSet(gs2))?;

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("items".to_string(), val2)]),
                    vec!["SetNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Verify union
        let storage = db.storage();
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

        let result = prop_manager.get_vertex_prop(vid, "items").await?;
        let crdt: Crdt = serde_json::from_value(result)?;

        if let Crdt::GSet(gs) = crdt {
            assert_eq!(gs.len(), 4);
            assert!(gs.contains(&"apple".to_string()));
            assert!(gs.contains(&"banana".to_string()));
            assert!(gs.contains(&"cherry".to_string()));
            assert!(gs.contains(&"date".to_string()));
        } else {
            panic!("Expected GSet");
        }

        Ok(())
    }
}

// ============================================================================
// ORSet Lifecycle Tests
// ============================================================================

mod orset_lifecycle {
    use super::*;

    #[tokio::test]
    async fn test_orset_add_remove_add() -> anyhow::Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let db = create_test_db().await?;

        db.schema()
            .label("ORSetNode")
            .property("items", DataType::Crdt(CrdtType::ORSet))
            .done()
            .apply()
            .await?;

        let schema_manager = db.schema_manager();
        let _label_id = schema_manager.schema().labels.get("ORSetNode").unwrap().id;
        let vid = Vid::new(1);

        let writer_lock = db.writer().unwrap();

        // Add initial items
        let mut os = ORSet::new();
        os.add("item1".to_string());
        os.add("item2".to_string());
        let val1 = serde_json::to_value(Crdt::ORSet(os.clone()))?;

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("items".to_string(), val1)]),
                    vec!["ORSetNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Remove item1 and add item3
        os.remove(&"item1".to_string());
        os.add("item3".to_string());
        let val2 = serde_json::to_value(Crdt::ORSet(os))?;

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("items".to_string(), val2)]),
                    vec!["ORSetNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Verify state
        let storage = db.storage();
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

        let result = prop_manager.get_vertex_prop(vid, "items").await?;
        let crdt: Crdt = serde_json::from_value(result)?;

        if let Crdt::ORSet(os) = crdt {
            // item1 was removed, item2 and item3 should be present
            assert!(!os.contains(&"item1".to_string()));
            assert!(os.contains(&"item2".to_string()));
            assert!(os.contains(&"item3".to_string()));
        } else {
            panic!("Expected ORSet");
        }

        Ok(())
    }
}

// ============================================================================
// LWWRegister Lifecycle Tests
// ============================================================================

mod lww_register_lifecycle {
    use super::*;

    #[tokio::test]
    async fn test_lww_register_newer_wins() -> anyhow::Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let db = create_test_db().await?;

        db.schema()
            .label("RegisterNode")
            .property("value", DataType::Crdt(CrdtType::LWWRegister))
            .done()
            .apply()
            .await?;

        let schema_manager = db.schema_manager();
        let _label_id = schema_manager
            .schema()
            .labels
            .get("RegisterNode")
            .unwrap()
            .id;
        let vid = Vid::new(1);

        let writer_lock = db.writer().unwrap();

        // Write with timestamp 100
        let reg1 = LWWRegister::new(serde_json::json!("first"), 100);
        let val1 = serde_json::to_value(Crdt::LWWRegister(reg1))?;

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("value".to_string(), val1)]),
                    vec!["RegisterNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Write with timestamp 200 (newer)
        let reg2 = LWWRegister::new(serde_json::json!("second"), 200);
        let val2 = serde_json::to_value(Crdt::LWWRegister(reg2))?;

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("value".to_string(), val2)]),
                    vec!["RegisterNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Verify newer value wins
        let storage = db.storage();
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

        let result = prop_manager.get_vertex_prop(vid, "value").await?;
        let crdt: Crdt = serde_json::from_value(result)?;

        if let Crdt::LWWRegister(reg) = crdt {
            assert_eq!(reg.get(), &serde_json::json!("second"));
            assert_eq!(reg.timestamp(), 200);
        } else {
            panic!("Expected LWWRegister");
        }

        Ok(())
    }
}

// ============================================================================
// LWWMap Lifecycle Tests
// ============================================================================

mod lww_map_lifecycle {
    use super::*;

    #[tokio::test]
    async fn test_lww_map_per_key_merge() -> anyhow::Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let db = create_test_db().await?;

        db.schema()
            .label("MapNode")
            .property("data", DataType::Crdt(CrdtType::LWWMap))
            .done()
            .apply()
            .await?;

        let schema_manager = db.schema_manager();
        let _label_id = schema_manager.schema().labels.get("MapNode").unwrap().id;
        let vid = Vid::new(1);

        let writer_lock = db.writer().unwrap();

        // Write key1=value1 at timestamp 100
        let mut map1 = LWWMap::new();
        map1.put("key1".to_string(), serde_json::json!("value1"), 100);
        let val1 = serde_json::to_value(Crdt::LWWMap(map1))?;

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("data".to_string(), val1)]),
                    vec!["MapNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Write key2=value2 at timestamp 200
        let mut map2 = LWWMap::new();
        map2.put("key2".to_string(), serde_json::json!("value2"), 200);
        let val2 = serde_json::to_value(Crdt::LWWMap(map2))?;

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("data".to_string(), val2)]),
                    vec!["MapNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Verify both keys present
        let storage = db.storage();
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

        let result = prop_manager.get_vertex_prop(vid, "data").await?;
        let crdt: Crdt = serde_json::from_value(result)?;

        if let Crdt::LWWMap(map) = crdt {
            assert_eq!(
                map.get(&"key1".to_string()),
                Some(&serde_json::json!("value1"))
            );
            assert_eq!(
                map.get(&"key2".to_string()),
                Some(&serde_json::json!("value2"))
            );
        } else {
            panic!("Expected LWWMap");
        }

        Ok(())
    }
}

// ============================================================================
// Rga Lifecycle Tests
// ============================================================================

mod rga_lifecycle {
    use super::*;

    #[tokio::test]
    async fn test_rga_sequence_merge() -> anyhow::Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let db = create_test_db().await?;

        db.schema()
            .label("RgaNode")
            .property("sequence", DataType::Crdt(CrdtType::Rga))
            .done()
            .apply()
            .await?;

        let schema_manager = db.schema_manager();
        let _label_id = schema_manager.schema().labels.get("RgaNode").unwrap().id;
        let vid = Vid::new(1);

        let writer_lock = db.writer().unwrap();

        // Create initial sequence: "AB"
        let mut rga1 = Rga::new();
        let id_a = rga1.insert(None, "A".to_string(), 1);
        rga1.insert(Some(id_a), "B".to_string(), 2);
        let val1 = serde_json::to_value(Crdt::Rga(rga1.clone()))?;

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("sequence".to_string(), val1)]),
                    vec!["RgaNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Add "C" after A (concurrent insert simulation)
        let mut rga2 = rga1.clone();
        rga2.insert(Some(id_a), "C".to_string(), 3);
        let val2 = serde_json::to_value(Crdt::Rga(rga2))?;

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("sequence".to_string(), val2)]),
                    vec!["RgaNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Verify sequence
        let storage = db.storage();
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

        let result = prop_manager.get_vertex_prop(vid, "sequence").await?;
        let crdt: Crdt = serde_json::from_value(result)?;

        if let Crdt::Rga(rga) = crdt {
            let vec = rga.to_vec();
            // A, C (ts=3), B (ts=2) due to ordering by timestamp desc
            assert_eq!(vec.len(), 3);
            assert!(vec.contains(&"A".to_string()));
            assert!(vec.contains(&"B".to_string()));
            assert!(vec.contains(&"C".to_string()));
        } else {
            panic!("Expected Rga");
        }

        Ok(())
    }
}

// ============================================================================
// VectorClock Lifecycle Tests
// ============================================================================

mod vector_clock_lifecycle {
    use super::*;

    #[tokio::test]
    async fn test_vector_clock_pointwise_max() -> anyhow::Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let db = create_test_db().await?;

        db.schema()
            .label("VCNode")
            .property("clock", DataType::Crdt(CrdtType::VectorClock))
            .done()
            .apply()
            .await?;

        let schema_manager = db.schema_manager();
        let _label_id = schema_manager.schema().labels.get("VCNode").unwrap().id;
        let vid = Vid::new(1);

        let writer_lock = db.writer().unwrap();

        // Write {node1: 2, node2: 1}
        let mut vc1 = VectorClock::new();
        vc1.increment("node1");
        vc1.increment("node1");
        vc1.increment("node2");
        let val1 = serde_json::to_value(Crdt::VectorClock(vc1))?;

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("clock".to_string(), val1)]),
                    vec!["VCNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Write {node1: 1, node2: 3}
        let mut vc2 = VectorClock::new();
        vc2.increment("node1");
        vc2.increment("node2");
        vc2.increment("node2");
        vc2.increment("node2");
        let val2 = serde_json::to_value(Crdt::VectorClock(vc2))?;

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("clock".to_string(), val2)]),
                    vec!["VCNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Verify pointwise max: {node1: 2, node2: 3}
        let storage = db.storage();
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

        let result = prop_manager.get_vertex_prop(vid, "clock").await?;
        let crdt: Crdt = serde_json::from_value(result)?;

        if let Crdt::VectorClock(vc) = crdt {
            assert_eq!(vc.get("node1"), 2); // max(2, 1)
            assert_eq!(vc.get("node2"), 3); // max(1, 3)
        } else {
            panic!("Expected VectorClock");
        }

        Ok(())
    }
}

// ============================================================================
// VCRegister Lifecycle Tests
// ============================================================================

mod vc_register_lifecycle {
    use super::*;

    #[tokio::test]
    async fn test_vc_register_causal_ordering() -> anyhow::Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let db = create_test_db().await?;

        db.schema()
            .label("VCRegNode")
            .property("state", DataType::Crdt(CrdtType::VCRegister))
            .done()
            .apply()
            .await?;

        let schema_manager = db.schema_manager();
        let _label_id = schema_manager.schema().labels.get("VCRegNode").unwrap().id;
        let vid = Vid::new(1);

        let writer_lock = db.writer().unwrap();

        // Write initial value from node1
        let reg1 = VCRegister::new(serde_json::json!("initial"), "node1");
        let val1 = serde_json::to_value(Crdt::VCRegister(reg1.clone()))?;

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("state".to_string(), val1)]),
                    vec!["VCRegNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Write causally newer value
        let mut reg2 = reg1.clone();
        reg2.set(serde_json::json!("updated"), "node1");
        let val2 = serde_json::to_value(Crdt::VCRegister(reg2))?;

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("state".to_string(), val2)]),
                    vec!["VCRegNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Verify causally newer wins
        let storage = db.storage();
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

        let result = prop_manager.get_vertex_prop(vid, "state").await?;
        let crdt: Crdt = serde_json::from_value(result)?;

        if let Crdt::VCRegister(reg) = crdt {
            assert_eq!(reg.get(), &serde_json::json!("updated"));
            assert_eq!(reg.clock().get("node1"), 2);
        } else {
            panic!("Expected VCRegister");
        }

        Ok(())
    }
}

// ============================================================================
// Multi-CRDT Property Tests
// ============================================================================

mod multi_crdt {
    use super::*;

    #[tokio::test]
    async fn test_vertex_with_multiple_crdt_types() -> anyhow::Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let db = create_test_db().await?;

        db.schema()
            .label("MultiCrdtNode")
            .property_nullable("counter", DataType::Crdt(CrdtType::GCounter))
            .property_nullable("items", DataType::Crdt(CrdtType::GSet))
            .property_nullable("clock", DataType::Crdt(CrdtType::VectorClock))
            .done()
            .apply()
            .await?;

        let schema_manager = db.schema_manager();
        let _label_id = schema_manager
            .schema()
            .labels
            .get("MultiCrdtNode")
            .unwrap()
            .id;
        let vid = Vid::new(1);

        let writer_lock = db.writer().unwrap();

        // First write: partial CRDTs
        let mut gc1 = GCounter::new();
        gc1.increment("a", 10);

        let mut gs1 = GSet::new();
        gs1.add("x".to_string());

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([
                        (
                            "counter".to_string(),
                            serde_json::to_value(Crdt::GCounter(gc1))?,
                        ),
                        ("items".to_string(), serde_json::to_value(Crdt::GSet(gs1))?),
                    ]),
                    vec!["MultiCrdtNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Second write: more CRDTs
        let mut gc2 = GCounter::new();
        gc2.increment("b", 20);

        let mut gs2 = GSet::new();
        gs2.add("y".to_string());

        let mut vc = VectorClock::new();
        vc.increment("node1");

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([
                        (
                            "counter".to_string(),
                            serde_json::to_value(Crdt::GCounter(gc2))?,
                        ),
                        ("items".to_string(), serde_json::to_value(Crdt::GSet(gs2))?),
                        (
                            "clock".to_string(),
                            serde_json::to_value(Crdt::VectorClock(vc))?,
                        ),
                    ]),
                    vec!["MultiCrdtNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Verify all CRDTs merged
        let storage = db.storage();
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

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
            assert_eq!(gs.len(), 2);
        } else {
            panic!("Expected GSet");
        }

        let clock = prop_manager.get_vertex_prop(vid, "clock").await?;
        let crdt: Crdt = serde_json::from_value(clock)?;
        if let Crdt::VectorClock(vc) = crdt {
            assert_eq!(vc.get("node1"), 1);
        } else {
            panic!("Expected VectorClock");
        }

        Ok(())
    }
}

// ============================================================================
// Edge CRDT Tests
// ============================================================================

mod edge_crdt {
    use super::*;

    #[tokio::test]
    async fn test_edge_crdt_lifecycle() -> anyhow::Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let db = create_test_db().await?;

        db.schema()
            .label("Node")
            .done()
            .edge_type("CONNECTS", &["Node"], &["Node"])
            .property("weight", DataType::Crdt(CrdtType::GCounter))
            .done()
            .apply()
            .await?;

        let schema_manager = db.schema_manager();
        let _label_id = schema_manager.schema().labels.get("Node").unwrap().id;
        let edge_type = schema_manager
            .schema()
            .edge_types
            .get("CONNECTS")
            .unwrap()
            .id;

        let writer_lock = db.writer().unwrap();

        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);

        // Create vertices
        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(vid_a, HashMap::new(), vec!["Node".to_string()])
                .await?;
            writer
                .insert_vertex_with_labels(vid_b, HashMap::new(), vec!["Node".to_string()])
                .await?;

            // Create edge with initial weight
            let mut gc1 = GCounter::new();
            gc1.increment("a", 5);

            let eid = writer.next_eid(edge_type).await?;
            writer
                .insert_edge(
                    vid_a,
                    vid_b,
                    edge_type,
                    eid,
                    HashMap::from([(
                        "weight".to_string(),
                        serde_json::to_value(Crdt::GCounter(gc1))?,
                    )]),
                )
                .await?;
            writer.flush_to_l1(None).await?;

            // Update edge weight
            let mut gc2 = GCounter::new();
            gc2.increment("b", 10);
            writer
                .insert_edge(
                    vid_a,
                    vid_b,
                    edge_type,
                    eid,
                    HashMap::from([(
                        "weight".to_string(),
                        serde_json::to_value(Crdt::GCounter(gc2))?,
                    )]),
                )
                .await?;
            writer.flush_to_l1(None).await?;

            // Verify edge CRDT merged
            let storage = db.storage();
            let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

            let weight = prop_manager.get_edge_prop(eid, "weight", None).await?;
            let crdt: Crdt = serde_json::from_value(weight)?;

            if let Crdt::GCounter(gc) = crdt {
                assert_eq!(gc.value(), 15); // 5 + 10
            } else {
                panic!("Expected GCounter");
            }
        }

        Ok(())
    }
}
