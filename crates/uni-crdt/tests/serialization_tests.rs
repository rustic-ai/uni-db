// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Serialization tests for CRDT types.
//!
//! Tests MessagePack and JSON round-trips, format verification, and error handling.

use uni_crdt::{
    Crdt, CrdtError, GCounter, GSet, LWWMap, LWWRegister, ORSet, Rga, VCRegister, VectorClock,
};

// ============================================================================
// MessagePack Round-Trip Tests
// ============================================================================

mod msgpack_roundtrip {
    use super::*;

    #[test]
    fn test_gcounter_msgpack_roundtrip() {
        let mut gc = GCounter::new();
        gc.increment("actor1", 42);
        gc.increment("actor2", 10);

        let crdt = Crdt::GCounter(gc.clone());
        let bytes = crdt.to_msgpack().expect("serialization should succeed");
        let decoded = Crdt::from_msgpack(&bytes).expect("deserialization should succeed");

        assert_eq!(crdt, decoded);
        if let Crdt::GCounter(decoded_gc) = decoded {
            assert_eq!(decoded_gc.value(), 52);
            assert_eq!(decoded_gc.actor_count("actor1"), 42);
            assert_eq!(decoded_gc.actor_count("actor2"), 10);
        } else {
            panic!("Expected GCounter");
        }
    }

    #[test]
    fn test_gset_msgpack_roundtrip() {
        let mut gs: GSet<String> = GSet::new();
        gs.add("apple".to_string());
        gs.add("banana".to_string());
        gs.add("cherry".to_string());

        let crdt = Crdt::GSet(gs);
        let bytes = crdt.to_msgpack().expect("serialization should succeed");
        let decoded = Crdt::from_msgpack(&bytes).expect("deserialization should succeed");

        assert_eq!(crdt, decoded);
        if let Crdt::GSet(decoded_gs) = decoded {
            assert_eq!(decoded_gs.len(), 3);
            assert!(decoded_gs.contains(&"apple".to_string()));
            assert!(decoded_gs.contains(&"banana".to_string()));
            assert!(decoded_gs.contains(&"cherry".to_string()));
        } else {
            panic!("Expected GSet");
        }
    }

    #[test]
    fn test_orset_msgpack_roundtrip() {
        let mut os: ORSet<String> = ORSet::new();
        os.add("item1".to_string());
        os.add("item2".to_string());
        os.remove(&"item1".to_string());

        let crdt = Crdt::ORSet(os);
        let bytes = crdt.to_msgpack().expect("serialization should succeed");
        let decoded = Crdt::from_msgpack(&bytes).expect("deserialization should succeed");

        assert_eq!(crdt, decoded);
        if let Crdt::ORSet(decoded_os) = decoded {
            // item1 removed, item2 remains
            assert!(!decoded_os.contains(&"item1".to_string()));
            assert!(decoded_os.contains(&"item2".to_string()));
        } else {
            panic!("Expected ORSet");
        }
    }

    #[test]
    fn test_lww_register_msgpack_roundtrip() {
        let reg = LWWRegister::new(serde_json::json!("hello"), 12345);

        let crdt = Crdt::LWWRegister(reg);
        let bytes = crdt.to_msgpack().expect("serialization should succeed");
        let decoded = Crdt::from_msgpack(&bytes).expect("deserialization should succeed");

        assert_eq!(crdt, decoded);
        if let Crdt::LWWRegister(decoded_reg) = decoded {
            assert_eq!(decoded_reg.get(), &serde_json::json!("hello"));
            assert_eq!(decoded_reg.timestamp(), 12345);
        } else {
            panic!("Expected LWWRegister");
        }
    }

    #[test]
    fn test_lww_map_msgpack_roundtrip() {
        let mut map: LWWMap<String, serde_json::Value> = LWWMap::new();
        map.put("key1".to_string(), serde_json::json!(42), 100);
        map.put("key2".to_string(), serde_json::json!("value"), 200);

        let crdt = Crdt::LWWMap(map);
        let bytes = crdt.to_msgpack().expect("serialization should succeed");
        let decoded = Crdt::from_msgpack(&bytes).expect("deserialization should succeed");

        assert_eq!(crdt, decoded);
        if let Crdt::LWWMap(decoded_map) = decoded {
            assert_eq!(
                decoded_map.get(&"key1".to_string()),
                Some(&serde_json::json!(42))
            );
            assert_eq!(
                decoded_map.get(&"key2".to_string()),
                Some(&serde_json::json!("value"))
            );
        } else {
            panic!("Expected LWWMap");
        }
    }

    #[test]
    fn test_rga_msgpack_roundtrip() {
        let mut rga: Rga<String> = Rga::new();
        let id1 = rga.insert(None, "H".to_string(), 1);
        let id2 = rga.insert(Some(id1), "i".to_string(), 2);
        rga.insert(Some(id2), "!".to_string(), 3);

        let crdt = Crdt::Rga(rga);
        let bytes = crdt.to_msgpack().expect("serialization should succeed");
        let decoded = Crdt::from_msgpack(&bytes).expect("deserialization should succeed");

        assert_eq!(crdt, decoded);
        if let Crdt::Rga(decoded_rga) = decoded {
            let vec = decoded_rga.to_vec();
            assert_eq!(vec, vec!["H".to_string(), "i".to_string(), "!".to_string()]);
        } else {
            panic!("Expected Rga");
        }
    }

    #[test]
    fn test_vector_clock_msgpack_roundtrip() {
        let mut vc = VectorClock::new();
        vc.increment("node1");
        vc.increment("node1");
        vc.increment("node2");

        let crdt = Crdt::VectorClock(vc);
        let bytes = crdt.to_msgpack().expect("serialization should succeed");
        let decoded = Crdt::from_msgpack(&bytes).expect("deserialization should succeed");

        assert_eq!(crdt, decoded);
        if let Crdt::VectorClock(decoded_vc) = decoded {
            assert_eq!(decoded_vc.get("node1"), 2);
            assert_eq!(decoded_vc.get("node2"), 1);
        } else {
            panic!("Expected VectorClock");
        }
    }

    #[test]
    fn test_vc_register_msgpack_roundtrip() {
        let mut reg = VCRegister::new(serde_json::json!("initial"), "actor1");
        reg.set(serde_json::json!("updated"), "actor1");

        let crdt = Crdt::VCRegister(reg);
        let bytes = crdt.to_msgpack().expect("serialization should succeed");
        let decoded = Crdt::from_msgpack(&bytes).expect("deserialization should succeed");

        assert_eq!(crdt, decoded);
        if let Crdt::VCRegister(decoded_reg) = decoded {
            assert_eq!(decoded_reg.get(), &serde_json::json!("updated"));
            assert_eq!(decoded_reg.clock().get("actor1"), 2);
        } else {
            panic!("Expected VCRegister");
        }
    }

    #[test]
    fn test_empty_crdts_roundtrip() {
        // Test that empty CRDTs round-trip correctly
        let empty_gc = Crdt::GCounter(GCounter::new());
        let empty_gs = Crdt::GSet(GSet::<String>::new());
        let empty_os = Crdt::ORSet(ORSet::<String>::new());
        let empty_lm = Crdt::LWWMap(LWWMap::<String, serde_json::Value>::new());
        let empty_rga = Crdt::Rga(Rga::<String>::new());
        let empty_vc = Crdt::VectorClock(VectorClock::new());

        for crdt in [empty_gc, empty_gs, empty_os, empty_lm, empty_rga, empty_vc] {
            let bytes = crdt.to_msgpack().expect("serialization should succeed");
            let decoded = Crdt::from_msgpack(&bytes).expect("deserialization should succeed");
            assert_eq!(crdt, decoded);
        }
    }
}

// ============================================================================
// JSON Format Tests
// ============================================================================

mod json_format {
    use super::*;

    #[test]
    fn test_gcounter_json_format() {
        let mut gc = GCounter::new();
        gc.increment("actor1", 5);

        let crdt = Crdt::GCounter(gc);
        let json = serde_json::to_value(&crdt).expect("JSON serialization should succeed");

        assert_eq!(json.get("t"), Some(&serde_json::json!("gc")));
        assert!(json.get("d").is_some());
        let data = json.get("d").unwrap();
        assert!(data.get("counts").is_some());
    }

    #[test]
    fn test_gset_json_format() {
        let mut gs = GSet::new();
        gs.add("item".to_string());

        let crdt = Crdt::GSet(gs);
        let json = serde_json::to_value(&crdt).expect("JSON serialization should succeed");

        assert_eq!(json.get("t"), Some(&serde_json::json!("gs")));
        assert!(json.get("d").is_some());
        let data = json.get("d").unwrap();
        assert!(data.get("elements").is_some());
    }

    #[test]
    fn test_orset_json_format() {
        let mut os = ORSet::new();
        os.add("item".to_string());

        let crdt = Crdt::ORSet(os);
        let json = serde_json::to_value(&crdt).expect("JSON serialization should succeed");

        assert_eq!(json.get("t"), Some(&serde_json::json!("os")));
        assert!(json.get("d").is_some());
        let data = json.get("d").unwrap();
        assert!(data.get("elements").is_some());
        assert!(data.get("tombstones").is_some());
    }

    #[test]
    fn test_lww_register_json_format() {
        let reg = LWWRegister::new(serde_json::json!("hello"), 1000);

        let crdt = Crdt::LWWRegister(reg);
        let json = serde_json::to_value(&crdt).expect("JSON serialization should succeed");

        assert_eq!(json.get("t"), Some(&serde_json::json!("lr")));
        assert!(json.get("d").is_some());
        let data = json.get("d").unwrap();
        assert!(data.get("value").is_some());
        assert!(data.get("timestamp").is_some());
    }

    #[test]
    fn test_lww_map_json_format() {
        let mut map = LWWMap::new();
        map.put("key".to_string(), serde_json::json!("value"), 100);

        let crdt = Crdt::LWWMap(map);
        let json = serde_json::to_value(&crdt).expect("JSON serialization should succeed");

        assert_eq!(json.get("t"), Some(&serde_json::json!("lm")));
        assert!(json.get("d").is_some());
        let data = json.get("d").unwrap();
        assert!(data.get("map").is_some());
    }

    #[test]
    fn test_rga_json_format() {
        let mut rga = Rga::new();
        rga.insert(None, "a".to_string(), 1);

        let crdt = Crdt::Rga(rga);
        let json = serde_json::to_value(&crdt).expect("JSON serialization should succeed");

        assert_eq!(json.get("t"), Some(&serde_json::json!("rg")));
        assert!(json.get("d").is_some());
        let data = json.get("d").unwrap();
        assert!(data.get("nodes").is_some());
    }

    #[test]
    fn test_vector_clock_json_format() {
        let mut vc = VectorClock::new();
        vc.increment("node1");

        let crdt = Crdt::VectorClock(vc);
        let json = serde_json::to_value(&crdt).expect("JSON serialization should succeed");

        assert_eq!(json.get("t"), Some(&serde_json::json!("vc")));
        assert!(json.get("d").is_some());
        let data = json.get("d").unwrap();
        assert!(data.get("clocks").is_some());
    }

    #[test]
    fn test_vc_register_json_format() {
        let reg = VCRegister::new(serde_json::json!("test"), "actor");

        let crdt = Crdt::VCRegister(reg);
        let json = serde_json::to_value(&crdt).expect("JSON serialization should succeed");

        assert_eq!(json.get("t"), Some(&serde_json::json!("vr")));
        assert!(json.get("d").is_some());
        let data = json.get("d").unwrap();
        assert!(data.get("value").is_some());
        assert!(data.get("clock").is_some());
    }

    #[test]
    fn test_json_to_crdt_roundtrip() {
        let mut gc = GCounter::new();
        gc.increment("actor1", 5);
        let original = Crdt::GCounter(gc);

        let json = serde_json::to_value(&original).expect("to_value should succeed");
        let parsed: Crdt = serde_json::from_value(json).expect("from_value should succeed");

        assert_eq!(original, parsed);
    }

    #[test]
    fn test_json_string_to_crdt() {
        let json_str = r#"{"t": "gc", "d": {"counts": {"actor1": 5}}}"#;
        let crdt: Crdt = serde_json::from_str(json_str).expect("parsing should succeed");

        if let Crdt::GCounter(gc) = crdt {
            assert_eq!(gc.actor_count("actor1"), 5);
        } else {
            panic!("Expected GCounter");
        }
    }
}

// ============================================================================
// Error Handling Tests
// ============================================================================

mod error_handling {
    use super::*;

    #[test]
    fn test_from_msgpack_invalid_bytes() {
        let invalid_bytes = vec![0xFF, 0xFE, 0xFD];
        let result = Crdt::from_msgpack(&invalid_bytes);
        assert!(result.is_err());
        if let Err(CrdtError::Serialization(msg)) = result {
            assert!(!msg.is_empty());
        } else {
            panic!("Expected Serialization error");
        }
    }

    #[test]
    fn test_from_msgpack_truncated() {
        // Create valid msgpack then truncate it
        let gc = GCounter::new();
        let crdt = Crdt::GCounter(gc);
        let bytes = crdt.to_msgpack().expect("serialization should succeed");

        // Truncate to half length
        let truncated = &bytes[..bytes.len() / 2];
        let result = Crdt::from_msgpack(truncated);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_msgpack_empty() {
        let result = Crdt::from_msgpack(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_msgpack_wrong_structure() {
        // Valid msgpack but wrong structure (a simple integer)
        let bytes = rmp_serde::to_vec(&42i32).expect("serialization should succeed");
        let result = Crdt::from_msgpack(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_json_wrong_type_tag() {
        let json_str = r#"{"t": "invalid", "d": {}}"#;
        let result: Result<Crdt, _> = serde_json::from_str(json_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_json_missing_type_tag() {
        let json_str = r#"{"d": {"counts": {}}}"#;
        let result: Result<Crdt, _> = serde_json::from_str(json_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_json_missing_data() {
        let json_str = r#"{"t": "gc"}"#;
        let result: Result<Crdt, _> = serde_json::from_str(json_str);
        assert!(result.is_err());
    }
}

// ============================================================================
// Cross-Format Tests
// ============================================================================

mod cross_format {
    use super::*;

    #[test]
    fn test_json_to_msgpack_to_json() {
        let mut gc = GCounter::new();
        gc.increment("actor1", 42);
        gc.increment("actor2", 10);

        let original = Crdt::GCounter(gc);

        // JSON -> msgpack -> JSON
        let json1 = serde_json::to_value(&original).expect("to_value should succeed");
        let bytes = original.to_msgpack().expect("to_msgpack should succeed");
        let decoded = Crdt::from_msgpack(&bytes).expect("from_msgpack should succeed");
        let json2 = serde_json::to_value(&decoded).expect("to_value should succeed");

        assert_eq!(json1, json2);
    }

    #[test]
    fn test_msgpack_preserves_all_data() {
        // Create a complex ORSet with adds and removes
        let mut os: ORSet<String> = ORSet::new();
        let tag1 = os.add("item1".to_string());
        let _tag2 = os.add("item2".to_string());
        let _tag3 = os.add("item1".to_string()); // Second add of item1
        os.remove(&"item2".to_string());

        let crdt = Crdt::ORSet(os);
        let bytes = crdt.to_msgpack().expect("serialization should succeed");
        let decoded = Crdt::from_msgpack(&bytes).expect("deserialization should succeed");

        if let (Crdt::ORSet(original), Crdt::ORSet(decoded_os)) = (&crdt, &decoded) {
            // Check that visibility is preserved
            assert_eq!(
                original.contains(&"item1".to_string()),
                decoded_os.contains(&"item1".to_string())
            );
            assert_eq!(
                original.contains(&"item2".to_string()),
                decoded_os.contains(&"item2".to_string())
            );
        } else {
            panic!("Type mismatch");
        }

        // Silence unused variable warning
        let _ = tag1;
    }

    #[test]
    fn test_all_types_msgpack_size_reasonable() {
        // Ensure serialized sizes are reasonable (not bloated)
        let gc = Crdt::GCounter(GCounter::new());
        let gs = Crdt::GSet(GSet::<String>::new());
        let os = Crdt::ORSet(ORSet::<String>::new());
        let lr = Crdt::LWWRegister(LWWRegister::new(serde_json::json!(null), 0));
        let lm = Crdt::LWWMap(LWWMap::<String, serde_json::Value>::new());
        let rga = Crdt::Rga(Rga::<String>::new());
        let vc = Crdt::VectorClock(VectorClock::new());
        let vr = Crdt::VCRegister(VCRegister::new(serde_json::json!(null), "a"));

        for crdt in [gc, gs, os, lr, lm, rga, vc, vr] {
            let bytes = crdt.to_msgpack().expect("serialization should succeed");
            // Empty CRDTs should be relatively small
            assert!(
                bytes.len() < 100,
                "Empty CRDT should be small, got {} bytes",
                bytes.len()
            );
        }
    }
}
