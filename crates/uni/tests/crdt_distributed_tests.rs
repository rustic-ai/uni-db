// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Distributed simulation tests for CRDT convergence.
//!
//! Simulates multi-writer scenarios and verifies that CRDTs converge
//! to consistent state regardless of merge order.

use std::collections::HashMap;
use tempfile::tempdir;
use uni_crdt::{Crdt, CrdtMerge, GCounter, GSet, ORSet, VCRegister, VectorClock};
use uni_db::Uni;
use uni_db::core::id::Vid;
use uni_db::core::schema::{CrdtType, DataType};
use uni_db::runtime::property_manager::PropertyManager;

/// Helper to create a test database.
async fn create_test_db() -> anyhow::Result<Uni> {
    let temp_dir = tempdir()?;
    let path = temp_dir.path().to_str().unwrap().to_string();
    std::mem::forget(temp_dir);
    let db = Uni::open(&path).build().await?;
    Ok(db)
}

// ============================================================================
// Multi-Actor Convergence Tests
// ============================================================================

mod multi_actor_convergence {
    use super::*;

    /// Test that GCounter converges with multiple concurrent actors.
    #[tokio::test]
    async fn test_gcounter_multi_actor_convergence() -> anyhow::Result<()> {
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

        // Simulate 10 actors each incrementing concurrently
        let num_actors = 10;
        let increment_per_actor = 100u64;

        for actor_id in 0..num_actors {
            let mut gc = GCounter::new();
            gc.increment(&format!("actor{}", actor_id), increment_per_actor);
            let val = serde_json::to_value(Crdt::GCounter(gc))?;

            {
                let mut writer = writer_lock.write().await;
                writer
                    .insert_vertex_with_labels(
                        vid,
                        HashMap::from([("counter".to_string(), val.into())]),
                        vec!["CounterNode".to_string()],
                    )
                    .await?;
            }
        }

        // Flush once at the end
        {
            let mut writer = writer_lock.write().await;
            writer.flush_to_l1(None).await?;
        }

        // Verify convergence
        let storage = db.storage();
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

        let result = prop_manager.get_vertex_prop(vid, "counter").await?;
        let crdt: Crdt = serde_json::from_value(result.into())?;

        if let Crdt::GCounter(gc) = crdt {
            let expected_total = num_actors as u64 * increment_per_actor;
            assert_eq!(gc.value(), expected_total);

            // Verify each actor's count
            for actor_id in 0..num_actors {
                assert_eq!(
                    gc.actor_count(&format!("actor{}", actor_id)),
                    increment_per_actor
                );
            }
        } else {
            panic!("Expected GCounter");
        }

        Ok(())
    }

    /// Test that ORSet converges with concurrent add/remove operations.
    #[tokio::test]
    async fn test_orset_concurrent_add_remove() -> anyhow::Result<()> {
        let _ = env_logger::builder().is_test(true).try_init();
        let db = create_test_db().await?;

        db.schema()
            .label("SetNode")
            .property("items", DataType::Crdt(CrdtType::ORSet))
            .done()
            .apply()
            .await?;

        let schema_manager = db.schema_manager();
        let _label_id = schema_manager.schema().labels.get("SetNode").unwrap().id;
        let vid = Vid::new(1);

        let writer_lock = db.writer().unwrap();

        // Actor 1: Add "item"
        let mut os1 = ORSet::new();
        os1.add("item".to_string());
        let val1 = serde_json::to_value(Crdt::ORSet(os1.clone()))?;

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("items".to_string(), val1.into())]),
                    vec!["SetNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Actor 2: Concurrent remove (from same state as actor 1 started)
        // In real distributed systems, this would have seen os1's state
        // But we simulate by removing from a clone
        let mut os2 = os1.clone();
        os2.remove(&"item".to_string());
        let val2 = serde_json::to_value(Crdt::ORSet(os2))?;

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("items".to_string(), val2.into())]),
                    vec!["SetNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Actor 1: Re-add "item" (new tag)
        let mut os3 = os1.clone();
        os3.add("item".to_string()); // This creates a NEW tag
        let val3 = serde_json::to_value(Crdt::ORSet(os3))?;

        {
            let mut writer = writer_lock.write().await;
            writer
                .insert_vertex_with_labels(
                    vid,
                    HashMap::from([("items".to_string(), val3.into())]),
                    vec!["SetNode".to_string()],
                )
                .await?;
            writer.flush_to_l1(None).await?;
        }

        // Verify: Add-wins semantics should preserve the item
        let storage = db.storage();
        let prop_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);

        let result = prop_manager.get_vertex_prop(vid, "items").await?;
        let crdt: Crdt = serde_json::from_value(result.into())?;

        if let Crdt::ORSet(os) = crdt {
            // The re-add should have created a new tag, so item should be visible
            assert!(
                os.contains(&"item".to_string()),
                "Add-wins: item should be present after re-add"
            );
        } else {
            panic!("Expected ORSet");
        }

        Ok(())
    }
}

// ============================================================================
// Merge Order Independence Tests
// ============================================================================

mod merge_order_independence {
    use super::*;
    use uni_store::runtime::L0Buffer;

    /// Test that merging L0 buffers in different orders produces the same result.
    #[test]
    fn test_l0_buffer_merge_order_independent() -> anyhow::Result<()> {
        // Create 3 L0 buffers with different CRDT updates
        let vid = Vid::new(1);

        let mut buffer1 = L0Buffer::new(0, None);
        let mut gc1 = GCounter::new();
        gc1.increment("actor1", 10);
        buffer1.insert_vertex(
            vid,
            HashMap::from([(
                "counter".to_string(),
                serde_json::to_value(Crdt::GCounter(gc1))?.into(),
            )]),
        );

        let mut buffer2 = L0Buffer::new(0, None);
        let mut gc2 = GCounter::new();
        gc2.increment("actor2", 20);
        buffer2.insert_vertex(
            vid,
            HashMap::from([(
                "counter".to_string(),
                serde_json::to_value(Crdt::GCounter(gc2))?.into(),
            )]),
        );

        let mut buffer3 = L0Buffer::new(0, None);
        let mut gc3 = GCounter::new();
        gc3.increment("actor3", 30);
        buffer3.insert_vertex(
            vid,
            HashMap::from([(
                "counter".to_string(),
                serde_json::to_value(Crdt::GCounter(gc3))?.into(),
            )]),
        );

        // Merge order 1: buffer1 + buffer2 + buffer3
        let mut result1 = L0Buffer::new(0, None);
        result1.merge(&buffer1)?;
        result1.merge(&buffer2)?;
        result1.merge(&buffer3)?;

        // Merge order 2: buffer3 + buffer1 + buffer2
        let mut result2 = L0Buffer::new(0, None);
        result2.merge(&buffer3)?;
        result2.merge(&buffer1)?;
        result2.merge(&buffer2)?;

        // Merge order 3: buffer2 + buffer3 + buffer1
        let mut result3 = L0Buffer::new(0, None);
        result3.merge(&buffer2)?;
        result3.merge(&buffer3)?;
        result3.merge(&buffer1)?;

        // Extract counters and verify all produce the same result
        let extract_counter_value = |buffer: &L0Buffer| -> u64 {
            let props = buffer.vertex_properties.get(&vid).unwrap();
            let counter = props.get("counter").unwrap();
            let crdt: Crdt = serde_json::from_value(serde_json::Value::from(counter.clone())).unwrap();
            if let Crdt::GCounter(gc) = crdt {
                gc.value()
            } else {
                panic!("Expected GCounter");
            }
        };

        let val1 = extract_counter_value(&result1);
        let val2 = extract_counter_value(&result2);
        let val3 = extract_counter_value(&result3);

        assert_eq!(val1, 60); // 10 + 20 + 30
        assert_eq!(val2, 60);
        assert_eq!(val3, 60);

        Ok(())
    }

    /// Test commutativity of CRDT merges across L0 buffers.
    #[test]
    fn test_crdt_merge_commutativity() -> anyhow::Result<()> {
        let vid = Vid::new(1);

        // Helper to create GSet props
        let create_gset_props = |items: &[&str]| -> HashMap<String, uni_common::Value> {
            let mut gs = GSet::new();
            for item in items {
                gs.add(item.to_string());
            }
            HashMap::from([(
                "items".to_string(),
                serde_json::to_value(Crdt::GSet(gs)).unwrap().into(),
            )])
        };

        // Test a.merge(b): Create buffer_ab, insert a's data, then merge b's data
        let mut buffer_ab = L0Buffer::new(0, None);
        buffer_ab.insert_vertex(vid, create_gset_props(&["apple", "banana"]));

        let mut buffer_b_for_ab = L0Buffer::new(0, None);
        buffer_b_for_ab.insert_vertex(vid, create_gset_props(&["cherry", "date"]));
        buffer_ab.merge(&buffer_b_for_ab)?;

        // Test b.merge(a): Create buffer_ba, insert b's data, then merge a's data
        let mut buffer_ba = L0Buffer::new(0, None);
        buffer_ba.insert_vertex(vid, create_gset_props(&["cherry", "date"]));

        let mut buffer_a_for_ba = L0Buffer::new(0, None);
        buffer_a_for_ba.insert_vertex(vid, create_gset_props(&["apple", "banana"]));
        buffer_ba.merge(&buffer_a_for_ba)?;

        // Extract and compare
        let extract_set_len = |buffer: &L0Buffer| -> usize {
            let props = buffer.vertex_properties.get(&vid).unwrap();
            let items = props.get("items").unwrap();
            let crdt: Crdt = serde_json::from_value(serde_json::Value::from(items.clone())).unwrap();
            if let Crdt::GSet(gs) = crdt {
                gs.len()
            } else {
                panic!("Expected GSet");
            }
        };

        assert_eq!(extract_set_len(&buffer_ab), 4);
        assert_eq!(extract_set_len(&buffer_ba), 4);

        Ok(())
    }
}

// ============================================================================
// Eventual Consistency Tests
// ============================================================================

mod eventual_consistency {
    use super::*;

    /// Test that multiple replicas converge to the same state.
    #[test]
    fn test_multiple_replicas_converge() -> anyhow::Result<()> {
        // Simulate 3 replicas, each making local changes
        let mut replica1 = VectorClock::new();
        let mut replica2 = VectorClock::new();
        let mut replica3 = VectorClock::new();

        // Each replica increments its own actor
        for _ in 0..5 {
            replica1.increment("node1");
        }
        for _ in 0..3 {
            replica2.increment("node2");
        }
        for _ in 0..7 {
            replica3.increment("node3");
        }

        // Merge all replicas together (simulating gossip)
        let mut final_state = VectorClock::new();
        final_state.merge(&replica1);
        final_state.merge(&replica2);
        final_state.merge(&replica3);

        // Also try different merge orders
        let mut alt_state = VectorClock::new();
        alt_state.merge(&replica3);
        alt_state.merge(&replica1);
        alt_state.merge(&replica2);

        // Both should converge to the same state
        assert_eq!(final_state.get("node1"), 5);
        assert_eq!(final_state.get("node2"), 3);
        assert_eq!(final_state.get("node3"), 7);

        assert_eq!(alt_state.get("node1"), 5);
        assert_eq!(alt_state.get("node2"), 3);
        assert_eq!(alt_state.get("node3"), 7);

        Ok(())
    }

    /// Test GCounter convergence across simulated network partitions.
    #[test]
    fn test_network_partition_convergence() -> anyhow::Result<()> {
        // Simulate network partition:
        // Partition A: nodes 1, 2
        // Partition B: nodes 3, 4

        let mut partition_a = GCounter::new();
        let mut partition_b = GCounter::new();

        // Activity in partition A
        partition_a.increment("node1", 100);
        partition_a.increment("node2", 50);

        // Activity in partition B
        partition_b.increment("node3", 75);
        partition_b.increment("node4", 25);

        // Partition heals - merge both sides
        let mut healed = partition_a.clone();
        healed.merge(&partition_b);

        // Verify all activity is preserved
        assert_eq!(healed.actor_count("node1"), 100);
        assert_eq!(healed.actor_count("node2"), 50);
        assert_eq!(healed.actor_count("node3"), 75);
        assert_eq!(healed.actor_count("node4"), 25);
        assert_eq!(healed.value(), 250);

        Ok(())
    }
}

// ============================================================================
// Conflict Resolution Tests
// ============================================================================

mod conflict_resolution {
    use super::*;
    use uni_crdt::vc_register::MergeResult;

    /// Test VCRegister conflict resolution for concurrent updates.
    #[test]
    fn test_vc_register_concurrent_conflict() -> anyhow::Result<()> {
        // Both start from the same base state
        let base = VCRegister::new(serde_json::json!("base"), "origin");

        // Two concurrent updates from different nodes
        let mut update_a = base.clone();
        update_a.set(serde_json::json!("update_a"), "node_a");

        let mut update_b = base.clone();
        update_b.set(serde_json::json!("update_b"), "node_b");

        // These are concurrent (neither causally precedes the other)
        assert!(update_a.clock().is_concurrent(update_b.clock()));

        // Merge: tie-breaking keeps self
        let mut merged_a = update_a.clone();
        let result = merged_a.merge_register(&update_b);
        assert_eq!(result, MergeResult::Concurrent);

        // Clocks should be merged even if values have a tie-break
        assert_eq!(merged_a.clock().get("origin"), 1);
        assert_eq!(merged_a.clock().get("node_a"), 1);
        assert_eq!(merged_a.clock().get("node_b"), 1);

        Ok(())
    }

    /// Test that causally later updates always win.
    #[test]
    fn test_causal_ordering_wins() -> anyhow::Result<()> {
        let reg1 = VCRegister::new(serde_json::json!("first"), "node1");

        // reg2 is causally after reg1 (node1 incremented)
        let mut reg2 = reg1.clone();
        reg2.set(serde_json::json!("second"), "node1");

        // reg2 > reg1 causally
        assert!(reg1.clock().happened_before(reg2.clock()));

        // Merging should always take the causally newer value
        let mut result1 = reg1.clone();
        let merge_result = result1.merge_register(&reg2);
        assert_eq!(merge_result, MergeResult::TookOther);
        assert_eq!(result1.get(), &serde_json::json!("second"));

        // Order shouldn't matter
        let mut result2 = reg2.clone();
        let merge_result = result2.merge_register(&reg1);
        assert_eq!(merge_result, MergeResult::KeptSelf);
        assert_eq!(result2.get(), &serde_json::json!("second"));

        Ok(())
    }
}

// ============================================================================
// Stress Tests
// ============================================================================

mod stress {
    use super::*;

    /// Stress test: Many actors with many increments.
    #[test]
    fn test_gcounter_stress() {
        let num_actors = 100;
        let increments_per_actor = 1000u64;

        let mut gc = GCounter::new();

        for actor_id in 0..num_actors {
            gc.increment(&format!("actor{}", actor_id), increments_per_actor);
        }

        let expected = num_actors as u64 * increments_per_actor;
        assert_eq!(gc.value(), expected);
    }

    /// Stress test: Many concurrent merges.
    #[test]
    fn test_gset_stress_merge() {
        let num_sets = 50;
        let elements_per_set = 100;

        let mut result = GSet::new();

        for set_id in 0..num_sets {
            let mut gs = GSet::new();
            for elem_id in 0..elements_per_set {
                gs.add(format!("set{}_elem{}", set_id, elem_id));
            }
            result.merge(&gs);
        }

        // Each set has unique elements, so total = num_sets * elements_per_set
        let expected = num_sets * elements_per_set;
        assert_eq!(result.len(), expected);
    }

    /// Stress test: Large VectorClock.
    #[test]
    fn test_vector_clock_stress() {
        let num_actors = 500;
        let increments_per_actor = 100;

        let mut vc = VectorClock::new();

        for actor_id in 0..num_actors {
            for _ in 0..increments_per_actor {
                vc.increment(&format!("node{}", actor_id));
            }
        }

        for actor_id in 0..num_actors {
            assert_eq!(vc.get(&format!("node{}", actor_id)), increments_per_actor);
        }
    }
}
