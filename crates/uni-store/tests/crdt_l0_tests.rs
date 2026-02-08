// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! L0 buffer CRDT merge tests.
//!
//! Tests that CRDT properties are correctly merged in the L0 buffer during
//! vertex/edge insertions and buffer merges.

use serde_json::json;
use uni_common::Properties;
use uni_common::Value;
use uni_common::core::id::{Eid, Vid};
use uni_crdt::{Crdt, GCounter, GSet, VectorClock};
use uni_store::runtime::L0Buffer;

/// Helper to create a GCounter CRDT value as uni_common::Value.
fn gcounter_val(counts: &[(&str, u64)]) -> Value {
    let mut gc = GCounter::new();
    for (actor, count) in counts {
        gc.increment(actor, *count);
    }
    let json_val = serde_json::to_value(Crdt::GCounter(gc)).expect("to_value should succeed");
    json_val.into()
}

/// Helper to create a GSet CRDT value as uni_common::Value.
fn gset_val(elements: &[&str]) -> Value {
    let mut gs = GSet::new();
    for elem in elements {
        gs.add(elem.to_string());
    }
    let json_val = serde_json::to_value(Crdt::GSet(gs)).expect("to_value should succeed");
    json_val.into()
}

/// Helper to create a VectorClock CRDT value as uni_common::Value.
fn vector_clock_val(clocks: &[(&str, usize)]) -> Value {
    let mut vc = VectorClock::new();
    for (actor, count) in clocks {
        for _ in 0..*count {
            vc.increment(actor);
        }
    }
    let json_val = serde_json::to_value(Crdt::VectorClock(vc)).expect("to_value should succeed");
    json_val.into()
}

// ============================================================================
// GCounter Merge Tests
// ============================================================================

mod gcounter_merge {
    use super::*;

    #[test]
    fn test_gcounter_merge_on_vertex_insert() {
        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // First insert: actor1 = 10
        let props1: Properties = [("counter".to_string(), gcounter_val(&[("actor1", 10)]))].into();
        l0.insert_vertex(vid, props1);

        // Second insert: actor2 = 20 (should merge, not overwrite)
        let props2: Properties = [("counter".to_string(), gcounter_val(&[("actor2", 20)]))].into();
        l0.insert_vertex(vid, props2);

        // Verify merge: both actors should be present
        let stored = l0.vertex_properties.get(&vid).expect("vertex should exist");
        let counter = stored.get("counter").expect("counter should exist");

        let crdt: Crdt = serde_json::from_value(serde_json::Value::from(counter.clone()))
            .expect("should parse as CRDT");
        if let Crdt::GCounter(gc) = crdt {
            assert_eq!(gc.actor_count("actor1"), 10);
            assert_eq!(gc.actor_count("actor2"), 20);
            assert_eq!(gc.value(), 30);
        } else {
            panic!("Expected GCounter");
        }
    }

    #[test]
    fn test_gcounter_merge_overlapping_actors() {
        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // First insert: actor1 = 10
        let props1: Properties = [("counter".to_string(), gcounter_val(&[("actor1", 10)]))].into();
        l0.insert_vertex(vid, props1);

        // Second insert: actor1 = 15 (should take max)
        let props2: Properties = [("counter".to_string(), gcounter_val(&[("actor1", 15)]))].into();
        l0.insert_vertex(vid, props2);

        let stored = l0.vertex_properties.get(&vid).expect("vertex should exist");
        let counter = stored.get("counter").expect("counter should exist");

        let crdt: Crdt = serde_json::from_value(serde_json::Value::from(counter.clone()))
            .expect("should parse as CRDT");
        if let Crdt::GCounter(gc) = crdt {
            assert_eq!(gc.actor_count("actor1"), 15); // max(10, 15)
            assert_eq!(gc.value(), 15);
        } else {
            panic!("Expected GCounter");
        }
    }

    #[test]
    fn test_gcounter_merge_multiple_sequential() {
        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // Sequential inserts from multiple actors
        for i in 0..5 {
            let actor = format!("actor{}", i);
            let props: Properties = [(
                "counter".to_string(),
                gcounter_val(&[(&actor, (i + 1) as u64 * 10)]),
            )]
            .into();
            l0.insert_vertex(vid, props);
        }

        let stored = l0.vertex_properties.get(&vid).expect("vertex should exist");
        let counter = stored.get("counter").expect("counter should exist");

        let crdt: Crdt = serde_json::from_value(serde_json::Value::from(counter.clone()))
            .expect("should parse as CRDT");
        if let Crdt::GCounter(gc) = crdt {
            // 10 + 20 + 30 + 40 + 50 = 150
            assert_eq!(gc.value(), 150);
        } else {
            panic!("Expected GCounter");
        }
    }
}

// ============================================================================
// GSet Merge Tests
// ============================================================================

mod gset_merge {
    use super::*;

    #[test]
    fn test_gset_merge_on_vertex_insert() {
        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // First insert: {a, b}
        let props1: Properties = [("items".to_string(), gset_val(&["a", "b"]))].into();
        l0.insert_vertex(vid, props1);

        // Second insert: {c, d} (should merge to {a, b, c, d})
        let props2: Properties = [("items".to_string(), gset_val(&["c", "d"]))].into();
        l0.insert_vertex(vid, props2);

        let stored = l0.vertex_properties.get(&vid).expect("vertex should exist");
        let items = stored.get("items").expect("items should exist");

        let crdt: Crdt = serde_json::from_value(serde_json::Value::from(items.clone()))
            .expect("should parse as CRDT");
        if let Crdt::GSet(gs) = crdt {
            assert_eq!(gs.len(), 4);
            assert!(gs.contains(&"a".to_string()));
            assert!(gs.contains(&"b".to_string()));
            assert!(gs.contains(&"c".to_string()));
            assert!(gs.contains(&"d".to_string()));
        } else {
            panic!("Expected GSet");
        }
    }

    #[test]
    fn test_gset_merge_overlapping() {
        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // First insert: {a, b}
        let props1: Properties = [("items".to_string(), gset_val(&["a", "b"]))].into();
        l0.insert_vertex(vid, props1);

        // Second insert: {b, c} (b overlaps)
        let props2: Properties = [("items".to_string(), gset_val(&["b", "c"]))].into();
        l0.insert_vertex(vid, props2);

        let stored = l0.vertex_properties.get(&vid).expect("vertex should exist");
        let items = stored.get("items").expect("items should exist");

        let crdt: Crdt = serde_json::from_value(serde_json::Value::from(items.clone()))
            .expect("should parse as CRDT");
        if let Crdt::GSet(gs) = crdt {
            assert_eq!(gs.len(), 3); // {a, b, c}
        } else {
            panic!("Expected GSet");
        }
    }
}

// ============================================================================
// VectorClock Merge Tests
// ============================================================================

mod vector_clock_merge {
    use super::*;

    #[test]
    fn test_vector_clock_merge() {
        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // First insert: {node1: 2}
        let props1: Properties = [("clock".to_string(), vector_clock_val(&[("node1", 2)]))].into();
        l0.insert_vertex(vid, props1);

        // Second insert: {node2: 3}
        let props2: Properties = [("clock".to_string(), vector_clock_val(&[("node2", 3)]))].into();
        l0.insert_vertex(vid, props2);

        let stored = l0.vertex_properties.get(&vid).expect("vertex should exist");
        let clock = stored.get("clock").expect("clock should exist");

        let crdt: Crdt = serde_json::from_value(serde_json::Value::from(clock.clone()))
            .expect("should parse as CRDT");
        if let Crdt::VectorClock(vc) = crdt {
            assert_eq!(vc.get("node1"), 2);
            assert_eq!(vc.get("node2"), 3);
        } else {
            panic!("Expected VectorClock");
        }
    }

    #[test]
    fn test_vector_clock_merge_pointwise_max() {
        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // First insert: {node1: 5, node2: 2}
        let props1: Properties = [(
            "clock".to_string(),
            vector_clock_val(&[("node1", 5), ("node2", 2)]),
        )]
        .into();
        l0.insert_vertex(vid, props1);

        // Second insert: {node1: 3, node2: 4}
        let props2: Properties = [(
            "clock".to_string(),
            vector_clock_val(&[("node1", 3), ("node2", 4)]),
        )]
        .into();
        l0.insert_vertex(vid, props2);

        let stored = l0.vertex_properties.get(&vid).expect("vertex should exist");
        let clock = stored.get("clock").expect("clock should exist");

        let crdt: Crdt = serde_json::from_value(serde_json::Value::from(clock.clone()))
            .expect("should parse as CRDT");
        if let Crdt::VectorClock(vc) = crdt {
            assert_eq!(vc.get("node1"), 5); // max(5, 3)
            assert_eq!(vc.get("node2"), 4); // max(2, 4)
        } else {
            panic!("Expected VectorClock");
        }
    }
}

// ============================================================================
// Mixed Properties Tests
// ============================================================================

mod mixed_properties {
    use super::*;

    #[test]
    fn test_crdt_merges_regular_overwrites() {
        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // First insert: CRDT counter + regular name
        let props1: Properties = [
            ("counter".to_string(), gcounter_val(&[("a", 10)])),
            ("name".to_string(), Value::String("Alice".to_string())),
        ]
        .into();
        l0.insert_vertex(vid, props1);

        // Second insert: Different CRDT counter + different name
        let props2: Properties = [
            ("counter".to_string(), gcounter_val(&[("b", 20)])),
            ("name".to_string(), Value::String("Bob".to_string())),
        ]
        .into();
        l0.insert_vertex(vid, props2);

        let stored = l0.vertex_properties.get(&vid).expect("vertex should exist");

        // CRDT should be merged
        let counter = stored.get("counter").expect("counter should exist");
        let crdt: Crdt = serde_json::from_value(serde_json::Value::from(counter.clone()))
            .expect("should parse as CRDT");
        if let Crdt::GCounter(gc) = crdt {
            assert_eq!(gc.value(), 30); // 10 + 20 merged
        } else {
            panic!("Expected GCounter");
        }

        // Regular property should be overwritten
        let name = stored.get("name").expect("name should exist");
        assert_eq!(name, &Value::String("Bob".to_string()));
    }

    #[test]
    fn test_multiple_crdt_properties() {
        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // First insert: counter + items
        let props1: Properties = [
            ("counter".to_string(), gcounter_val(&[("a", 10)])),
            ("items".to_string(), gset_val(&["x", "y"])),
        ]
        .into();
        l0.insert_vertex(vid, props1);

        // Second insert: different counter + different items
        let props2: Properties = [
            ("counter".to_string(), gcounter_val(&[("b", 20)])),
            ("items".to_string(), gset_val(&["z"])),
        ]
        .into();
        l0.insert_vertex(vid, props2);

        let stored = l0.vertex_properties.get(&vid).expect("vertex should exist");

        // Counter should be merged
        let counter = stored.get("counter").unwrap();
        let crdt: Crdt = serde_json::from_value(serde_json::Value::from(counter.clone())).unwrap();
        if let Crdt::GCounter(gc) = crdt {
            assert_eq!(gc.value(), 30);
        } else {
            panic!("Expected GCounter");
        }

        // Items should be merged
        let items = stored.get("items").unwrap();
        let crdt: Crdt = serde_json::from_value(serde_json::Value::from(items.clone())).unwrap();
        if let Crdt::GSet(gs) = crdt {
            assert_eq!(gs.len(), 3); // {x, y, z}
        } else {
            panic!("Expected GSet");
        }
    }
}

// ============================================================================
// Type Mismatch Tests
// ============================================================================

mod type_mismatch {
    use super::*;

    #[test]
    fn test_crdt_type_mismatch_fallback_to_overwrite() {
        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // First insert: GCounter
        let props1: Properties = [("prop".to_string(), gcounter_val(&[("a", 10)]))].into();
        l0.insert_vertex(vid, props1);

        // Second insert: GSet (type mismatch, should overwrite)
        let props2: Properties = [("prop".to_string(), gset_val(&["x", "y"]))].into();
        l0.insert_vertex(vid, props2);

        let stored = l0.vertex_properties.get(&vid).expect("vertex should exist");
        let prop = stored.get("prop").expect("prop should exist");

        // Should now be a GSet (overwrite)
        let crdt: Crdt = serde_json::from_value(serde_json::Value::from(prop.clone()))
            .expect("should parse as CRDT");
        assert!(matches!(crdt, Crdt::GSet(_)));
    }

    #[test]
    fn test_non_crdt_to_crdt_overwrites() {
        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // First insert: regular value
        let props1: Properties = [("prop".to_string(), Value::Int(42))].into();
        l0.insert_vertex(vid, props1);

        // Second insert: CRDT (should overwrite since first was not CRDT)
        let props2: Properties = [("prop".to_string(), gcounter_val(&[("a", 10)]))].into();
        l0.insert_vertex(vid, props2);

        let stored = l0.vertex_properties.get(&vid).expect("vertex should exist");
        let prop = stored.get("prop").expect("prop should exist");

        let crdt: Crdt = serde_json::from_value(serde_json::Value::from(prop.clone()))
            .expect("should parse as CRDT");
        if let Crdt::GCounter(gc) = crdt {
            assert_eq!(gc.value(), 10);
        } else {
            panic!("Expected GCounter");
        }
    }
}

// ============================================================================
// L0 Buffer Merge Tests
// ============================================================================

mod buffer_merge {
    use super::*;

    #[test]
    fn test_buffer_merge_crdt_properties() -> anyhow::Result<()> {
        let mut main_l0 = L0Buffer::new(0, None);
        let mut tx_l0 = L0Buffer::new(0, None);

        let vid = Vid::new(1);

        // Main L0: actor1 = 10
        let props1: Properties = [("counter".to_string(), gcounter_val(&[("actor1", 10)]))].into();
        main_l0.insert_vertex(vid, props1);

        // Transaction L0: actor2 = 20
        let props2: Properties = [("counter".to_string(), gcounter_val(&[("actor2", 20)]))].into();
        tx_l0.insert_vertex(vid, props2);

        // Merge tx into main
        main_l0.merge(&tx_l0)?;

        let stored = main_l0
            .vertex_properties
            .get(&vid)
            .expect("vertex should exist");
        let counter = stored.get("counter").expect("counter should exist");

        let crdt: Crdt = serde_json::from_value(serde_json::Value::from(counter.clone()))
            .expect("should parse as CRDT");
        if let Crdt::GCounter(gc) = crdt {
            assert_eq!(gc.actor_count("actor1"), 10);
            assert_eq!(gc.actor_count("actor2"), 20);
            assert_eq!(gc.value(), 30);
        } else {
            panic!("Expected GCounter");
        }

        Ok(())
    }

    #[test]
    fn test_buffer_merge_multiple_vertices() -> anyhow::Result<()> {
        let mut main_l0 = L0Buffer::new(0, None);
        let mut tx_l0 = L0Buffer::new(0, None);

        let vid1 = Vid::new(1);
        let vid2 = Vid::new(2);

        // Main L0: vid1 with counter
        main_l0.insert_vertex(
            vid1,
            [("counter".to_string(), gcounter_val(&[("a", 10)]))].into(),
        );

        // Transaction L0: vid1 with different counter, vid2 new
        tx_l0.insert_vertex(
            vid1,
            [("counter".to_string(), gcounter_val(&[("b", 20)]))].into(),
        );
        tx_l0.insert_vertex(
            vid2,
            [("counter".to_string(), gcounter_val(&[("c", 30)]))].into(),
        );

        main_l0.merge(&tx_l0)?;

        // vid1 should have merged counter
        let stored1 = main_l0.vertex_properties.get(&vid1).unwrap();
        let counter1 = stored1.get("counter").unwrap();
        let crdt1: Crdt =
            serde_json::from_value(serde_json::Value::from(counter1.clone())).unwrap();
        if let Crdt::GCounter(gc) = crdt1 {
            assert_eq!(gc.value(), 30); // 10 + 20
        } else {
            panic!("Expected GCounter");
        }

        // vid2 should have new counter
        let stored2 = main_l0.vertex_properties.get(&vid2).unwrap();
        let counter2 = stored2.get("counter").unwrap();
        let crdt2: Crdt =
            serde_json::from_value(serde_json::Value::from(counter2.clone())).unwrap();
        if let Crdt::GCounter(gc) = crdt2 {
            assert_eq!(gc.value(), 30);
        } else {
            panic!("Expected GCounter");
        }

        Ok(())
    }
}

// ============================================================================
// Edge CRDT Merge Tests
// ============================================================================

mod edge_crdt {
    use super::*;

    #[test]
    fn test_edge_crdt_merge() {
        let mut l0 = L0Buffer::new(0, None);
        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        let eid = Eid::new(101);

        // First edge insert
        l0.insert_edge(
            vid_a,
            vid_b,
            1, // edge_type
            eid,
            [("weight".to_string(), gcounter_val(&[("a", 5)]))].into(),
        )
        .expect("insert should succeed");

        // Second edge insert (same edge, should merge)
        l0.insert_edge(
            vid_a,
            vid_b,
            1, // edge_type
            eid,
            [("weight".to_string(), gcounter_val(&[("b", 10)]))].into(),
        )
        .expect("insert should succeed");

        let stored = l0.edge_properties.get(&eid).expect("edge should exist");
        let weight = stored.get("weight").expect("weight should exist");

        let crdt: Crdt = serde_json::from_value(serde_json::Value::from(weight.clone()))
            .expect("should parse as CRDT");
        if let Crdt::GCounter(gc) = crdt {
            assert_eq!(gc.value(), 15); // 5 + 10
        } else {
            panic!("Expected GCounter");
        }
    }
}

// ============================================================================
// WAL Replay Tests
// ============================================================================

mod wal_replay {
    use super::*;
    use uni_store::runtime::wal::Mutation;

    #[test]
    fn test_replay_crdt_merge_semantics() -> anyhow::Result<()> {
        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // Simulate WAL replay with sequential CRDT mutations
        l0.replay_mutations(vec![Mutation::InsertVertex {
            vid,
            properties: [("counter".to_string(), gcounter_val(&[("node1", 5)]))].into(),
        }])?;

        l0.replay_mutations(vec![Mutation::InsertVertex {
            vid,
            properties: [("counter".to_string(), gcounter_val(&[("node2", 3)]))].into(),
        }])?;

        let stored = l0.vertex_properties.get(&vid).expect("vertex should exist");
        let counter = stored.get("counter").expect("counter should exist");

        // Convert back to serde_json::Value for nested access
        let counter_json: serde_json::Value = counter.clone().into();
        // Verify CRDT was merged
        let data = counter_json.get("d").unwrap();
        let counts = data.get("counts").unwrap();
        assert_eq!(counts.get("node1"), Some(&json!(5)));
        assert_eq!(counts.get("node2"), Some(&json!(3)));

        Ok(())
    }
}
