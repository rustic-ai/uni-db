// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Arrow encoding and decoding tests for CRDT types.
//!
//! Tests the conversion between CRDT JSON values, MessagePack binary, and Arrow arrays.

use arrow_array::builder::BinaryBuilder;
use arrow_array::{Array, BinaryArray};
use uni_common::Value;
use uni_common::core::schema::{CrdtType, DataType};
use uni_crdt::{Crdt, GCounter, GSet, LWWMap, LWWRegister, ORSet, Rga, VCRegister, VectorClock};
use uni_store::storage::arrow_convert::PropertyExtractor;
use uni_store::storage::value_codec::{CrdtDecodeMode, value_from_column};

// ============================================================================
// Encoding Tests - JSON Value to Arrow BinaryArray
// ============================================================================

mod encoding {
    use super::*;

    /// Test encoding a GCounter from a JSON string (what Cypher CREATE produces).
    /// This tests the data flow: Cypher literal -> Value::String -> build_crdt_column
    #[test]
    fn test_encode_gcounter_from_string() {
        // This is exactly what Cypher CREATE produces: a Value::String containing JSON
        let val = Value::String(r#"{"t": "gc", "d": {"counts": {"actor1": 5}}}"#.to_string());

        let extractor = PropertyExtractor::new("counter", &DataType::Crdt(CrdtType::GCounter));
        let deleted = vec![false];
        let col = extractor
            .build_column(1, &deleted, |_| Some(&val))
            .expect("build_column should succeed");

        let binary = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Should be BinaryArray");

        assert!(
            !binary.is_null(0),
            "CRDT should not be null - string parsing failed"
        );

        let decoded = Crdt::from_msgpack(binary.value(0)).expect("from_msgpack should succeed");
        if let Crdt::GCounter(gc) = decoded {
            assert_eq!(gc.value(), 5);
            assert_eq!(gc.actor_count("actor1"), 5);
        } else {
            panic!("Expected GCounter, got {:?}", decoded);
        }
    }

    /// Test encoding a VCRegister from a JSON string with correct field names.
    #[test]
    fn test_encode_vc_register_from_string() {
        // VCRegister has "value" and "clock" fields (not "vector_clock")
        // The clock field is a VectorClock which has "clocks" field
        let val = Value::String(
            r#"{"t": "vr", "d": {"value": "test", "clock": {"clocks": {"node1": 1}}}}"#.to_string(),
        );

        let extractor = PropertyExtractor::new("state", &DataType::Crdt(CrdtType::VCRegister));
        let deleted = vec![false];
        let col = extractor
            .build_column(1, &deleted, |_| Some(&val))
            .expect("build_column should succeed");

        let binary = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Should be BinaryArray");

        assert!(
            !binary.is_null(0),
            "CRDT should not be null - string parsing failed"
        );

        let decoded = Crdt::from_msgpack(binary.value(0)).expect("from_msgpack should succeed");
        if let Crdt::VCRegister(reg) = decoded {
            assert_eq!(reg.get(), &serde_json::json!("test"));
            assert_eq!(reg.clock().get("node1"), 1);
        } else {
            panic!("Expected VCRegister, got {:?}", decoded);
        }
    }

    /// Test encoding a GCounter from JSON value to Arrow BinaryArray.
    #[test]
    fn test_encode_gcounter_from_json() {
        let mut gc = GCounter::new();
        gc.increment("actor1", 10);
        gc.increment("actor2", 20);

        let crdt = Crdt::GCounter(gc);
        let val: Value = serde_json::to_value(&crdt)
            .expect("to_value should succeed")
            .into();

        let extractor = PropertyExtractor::new("counter", &DataType::Crdt(CrdtType::GCounter));
        let deleted = vec![false];
        let col = extractor
            .build_column(1, &deleted, |_| Some(&val))
            .expect("build_column should succeed");

        // Should produce a BinaryArray
        let binary = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Should be BinaryArray");

        assert!(!binary.is_null(0));
        let bytes = binary.value(0);

        // Decode and verify
        let decoded = Crdt::from_msgpack(bytes).expect("from_msgpack should succeed");
        if let Crdt::GCounter(decoded_gc) = decoded {
            assert_eq!(decoded_gc.value(), 30);
            assert_eq!(decoded_gc.actor_count("actor1"), 10);
            assert_eq!(decoded_gc.actor_count("actor2"), 20);
        } else {
            panic!("Expected GCounter");
        }
    }

    /// Test encoding a GSet from JSON value to Arrow BinaryArray.
    #[test]
    fn test_encode_gset_from_json() {
        let mut gs = GSet::new();
        gs.add("item1".to_string());
        gs.add("item2".to_string());

        let crdt = Crdt::GSet(gs);
        let val: Value = serde_json::to_value(&crdt)
            .expect("to_value should succeed")
            .into();

        let extractor = PropertyExtractor::new("items", &DataType::Crdt(CrdtType::GSet));
        let deleted = vec![false];
        let col = extractor
            .build_column(1, &deleted, |_| Some(&val))
            .expect("build_column should succeed");

        let binary = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Should be BinaryArray");

        let decoded = Crdt::from_msgpack(binary.value(0)).expect("from_msgpack should succeed");
        if let Crdt::GSet(decoded_gs) = decoded {
            assert_eq!(decoded_gs.len(), 2);
            assert!(decoded_gs.contains(&"item1".to_string()));
            assert!(decoded_gs.contains(&"item2".to_string()));
        } else {
            panic!("Expected GSet");
        }
    }

    /// Test encoding an ORSet from JSON value to Arrow BinaryArray.
    #[test]
    fn test_encode_orset_from_json() {
        let mut os = ORSet::new();
        os.add("visible".to_string());
        os.add("removed".to_string());
        os.remove(&"removed".to_string());

        let crdt = Crdt::ORSet(os);
        let val: Value = serde_json::to_value(&crdt)
            .expect("to_value should succeed")
            .into();

        let extractor = PropertyExtractor::new("items", &DataType::Crdt(CrdtType::ORSet));
        let deleted = vec![false];
        let col = extractor
            .build_column(1, &deleted, |_| Some(&val))
            .expect("build_column should succeed");

        let binary = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Should be BinaryArray");

        let decoded = Crdt::from_msgpack(binary.value(0)).expect("from_msgpack should succeed");
        if let Crdt::ORSet(decoded_os) = decoded {
            assert!(decoded_os.contains(&"visible".to_string()));
            assert!(!decoded_os.contains(&"removed".to_string()));
        } else {
            panic!("Expected ORSet");
        }
    }

    /// Test encoding an LWWRegister from JSON value to Arrow BinaryArray.
    #[test]
    fn test_encode_lww_register_from_json() {
        let reg = LWWRegister::new(serde_json::json!("hello world"), 12345);

        let crdt = Crdt::LWWRegister(reg);
        let val: Value = serde_json::to_value(&crdt)
            .expect("to_value should succeed")
            .into();

        let extractor = PropertyExtractor::new("value", &DataType::Crdt(CrdtType::LWWRegister));
        let deleted = vec![false];
        let col = extractor
            .build_column(1, &deleted, |_| Some(&val))
            .expect("build_column should succeed");

        let binary = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Should be BinaryArray");

        let decoded = Crdt::from_msgpack(binary.value(0)).expect("from_msgpack should succeed");
        if let Crdt::LWWRegister(decoded_reg) = decoded {
            assert_eq!(decoded_reg.get(), &serde_json::json!("hello world"));
            assert_eq!(decoded_reg.timestamp(), 12345);
        } else {
            panic!("Expected LWWRegister");
        }
    }

    /// Test encoding an LWWMap from JSON value to Arrow BinaryArray.
    #[test]
    fn test_encode_lww_map_from_json() {
        let mut map = LWWMap::new();
        map.put("key1".to_string(), serde_json::json!(42), 100);
        map.put("key2".to_string(), serde_json::json!("value"), 200);

        let crdt = Crdt::LWWMap(map);
        let val: Value = serde_json::to_value(&crdt)
            .expect("to_value should succeed")
            .into();

        let extractor = PropertyExtractor::new("data", &DataType::Crdt(CrdtType::LWWMap));
        let deleted = vec![false];
        let col = extractor
            .build_column(1, &deleted, |_| Some(&val))
            .expect("build_column should succeed");

        let binary = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Should be BinaryArray");

        let decoded = Crdt::from_msgpack(binary.value(0)).expect("from_msgpack should succeed");
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

    /// Test encoding an Rga from JSON value to Arrow BinaryArray.
    #[test]
    fn test_encode_rga_from_json() {
        let mut rga = Rga::new();
        let id1 = rga.insert(None, "H".to_string(), 1);
        let id2 = rga.insert(Some(id1), "i".to_string(), 2);
        rga.insert(Some(id2), "!".to_string(), 3);

        let crdt = Crdt::Rga(rga);
        let val: Value = serde_json::to_value(&crdt)
            .expect("to_value should succeed")
            .into();

        let extractor = PropertyExtractor::new("sequence", &DataType::Crdt(CrdtType::Rga));
        let deleted = vec![false];
        let col = extractor
            .build_column(1, &deleted, |_| Some(&val))
            .expect("build_column should succeed");

        let binary = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Should be BinaryArray");

        let decoded = Crdt::from_msgpack(binary.value(0)).expect("from_msgpack should succeed");
        if let Crdt::Rga(decoded_rga) = decoded {
            assert_eq!(
                decoded_rga.to_vec(),
                vec!["H".to_string(), "i".to_string(), "!".to_string()]
            );
        } else {
            panic!("Expected Rga");
        }
    }

    /// Test encoding a VectorClock from JSON value to Arrow BinaryArray.
    #[test]
    fn test_encode_vector_clock_from_json() {
        let mut vc = VectorClock::new();
        vc.increment("node1");
        vc.increment("node1");
        vc.increment("node2");

        let crdt = Crdt::VectorClock(vc);
        let val: Value = serde_json::to_value(&crdt)
            .expect("to_value should succeed")
            .into();

        let extractor = PropertyExtractor::new("clock", &DataType::Crdt(CrdtType::VectorClock));
        let deleted = vec![false];
        let col = extractor
            .build_column(1, &deleted, |_| Some(&val))
            .expect("build_column should succeed");

        let binary = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Should be BinaryArray");

        let decoded = Crdt::from_msgpack(binary.value(0)).expect("from_msgpack should succeed");
        if let Crdt::VectorClock(decoded_vc) = decoded {
            assert_eq!(decoded_vc.get("node1"), 2);
            assert_eq!(decoded_vc.get("node2"), 1);
        } else {
            panic!("Expected VectorClock");
        }
    }

    /// Test encoding a VCRegister from JSON value to Arrow BinaryArray.
    #[test]
    fn test_encode_vc_register_from_json() {
        let reg = VCRegister::new(serde_json::json!("test value"), "actor1");

        let crdt = Crdt::VCRegister(reg);
        let val: Value = serde_json::to_value(&crdt)
            .expect("to_value should succeed")
            .into();

        let extractor = PropertyExtractor::new("state", &DataType::Crdt(CrdtType::VCRegister));
        let deleted = vec![false];
        let col = extractor
            .build_column(1, &deleted, |_| Some(&val))
            .expect("build_column should succeed");

        let binary = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Should be BinaryArray");

        let decoded = Crdt::from_msgpack(binary.value(0)).expect("from_msgpack should succeed");
        if let Crdt::VCRegister(decoded_reg) = decoded {
            assert_eq!(decoded_reg.get(), &serde_json::json!("test value"));
            assert_eq!(decoded_reg.clock().get("actor1"), 1);
        } else {
            panic!("Expected VCRegister");
        }
    }

    /// Test that deleted rows produce null values.
    #[test]
    fn test_encode_crdt_null_for_deleted() {
        let mut gc = GCounter::new();
        gc.increment("actor", 10);
        let crdt = Crdt::GCounter(gc);
        let val: Value = serde_json::to_value(&crdt)
            .expect("to_value should succeed")
            .into();

        let extractor = PropertyExtractor::new("counter", &DataType::Crdt(CrdtType::GCounter));
        let deleted = vec![true, false, true];
        let col = extractor
            .build_column(3, &deleted, |i| if i == 1 { Some(&val) } else { None })
            .expect("build_column should succeed");

        let binary = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Should be BinaryArray");

        assert!(binary.is_null(0), "Deleted row 0 should be null");
        assert!(!binary.is_null(1), "Non-deleted row 1 should not be null");
        assert!(binary.is_null(2), "Deleted row 2 should be null");
    }

    /// Test that missing properties produce null values.
    #[test]
    fn test_encode_crdt_null_for_missing() {
        let extractor = PropertyExtractor::new("counter", &DataType::Crdt(CrdtType::GCounter));
        let deleted = vec![false, false];
        let col = extractor
            .build_column(2, &deleted, |_| None)
            .expect("build_column should succeed");

        let binary = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Should be BinaryArray");

        assert!(binary.is_null(0));
        assert!(binary.is_null(1));
    }
}

// ============================================================================
// Decoding Tests - Arrow BinaryArray to JSON Value
// ============================================================================

mod decoding {
    use super::*;

    /// Helper to create a BinaryArray from a CRDT.
    fn crdt_to_binary_array(crdt: &Crdt) -> BinaryArray {
        let bytes = crdt.to_msgpack().expect("to_msgpack should succeed");
        let mut builder = BinaryBuilder::new();
        builder.append_value(&bytes);
        builder.finish()
    }

    #[test]
    fn test_decode_gcounter_strict() {
        let mut gc = GCounter::new();
        gc.increment("actor1", 42);
        let crdt = Crdt::GCounter(gc);
        let array = crdt_to_binary_array(&crdt);

        let val = value_from_column(
            &array,
            &DataType::Crdt(CrdtType::GCounter),
            0,
            CrdtDecodeMode::Strict,
        )
        .expect("decode should succeed");

        let decoded: Crdt = serde_json::from_value(val).expect("from_value should succeed");
        if let Crdt::GCounter(decoded_gc) = decoded {
            assert_eq!(decoded_gc.value(), 42);
        } else {
            panic!("Expected GCounter");
        }
    }

    #[test]
    fn test_decode_gset_strict() {
        let mut gs = GSet::new();
        gs.add("a".to_string());
        gs.add("b".to_string());
        let crdt = Crdt::GSet(gs);
        let array = crdt_to_binary_array(&crdt);

        let val = value_from_column(
            &array,
            &DataType::Crdt(CrdtType::GSet),
            0,
            CrdtDecodeMode::Strict,
        )
        .expect("decode should succeed");

        let decoded: Crdt = serde_json::from_value(val).expect("from_value should succeed");
        if let Crdt::GSet(decoded_gs) = decoded {
            assert_eq!(decoded_gs.len(), 2);
        } else {
            panic!("Expected GSet");
        }
    }

    #[test]
    fn test_decode_all_types_strict() {
        let test_cases: Vec<Crdt> = vec![
            Crdt::GCounter(GCounter::new()),
            Crdt::GSet(GSet::<String>::new()),
            Crdt::ORSet(ORSet::<String>::new()),
            Crdt::LWWRegister(LWWRegister::new(serde_json::json!(null), 0)),
            Crdt::LWWMap(LWWMap::<String, serde_json::Value>::new()),
            Crdt::Rga(Rga::<String>::new()),
            Crdt::VectorClock(VectorClock::new()),
            Crdt::VCRegister(VCRegister::new(serde_json::json!(null), "a")),
        ];

        let data_types = vec![
            DataType::Crdt(CrdtType::GCounter),
            DataType::Crdt(CrdtType::GSet),
            DataType::Crdt(CrdtType::ORSet),
            DataType::Crdt(CrdtType::LWWRegister),
            DataType::Crdt(CrdtType::LWWMap),
            DataType::Crdt(CrdtType::Rga),
            DataType::Crdt(CrdtType::VectorClock),
            DataType::Crdt(CrdtType::VCRegister),
        ];

        for (crdt, dt) in test_cases.into_iter().zip(data_types.into_iter()) {
            let array = crdt_to_binary_array(&crdt);
            let val = value_from_column(&array, &dt, 0, CrdtDecodeMode::Strict)
                .expect("decode should succeed");

            // Round-trip should work
            let decoded: Crdt = serde_json::from_value(val).expect("from_value should succeed");
            assert_eq!(crdt, decoded, "Round-trip failed for {:?}", dt);
        }
    }

    #[test]
    fn test_decode_strict_mode_error() {
        let invalid_bytes = vec![0xFF, 0xFE, 0xFD];
        let mut builder = BinaryBuilder::new();
        builder.append_value(&invalid_bytes);
        let array = builder.finish();

        let result = value_from_column(
            &array,
            &DataType::Crdt(CrdtType::GCounter),
            0,
            CrdtDecodeMode::Strict,
        );

        assert!(result.is_err(), "Strict mode should error on invalid bytes");
    }

    #[test]
    fn test_decode_lenient_mode_fallback() {
        let invalid_bytes = vec![0xFF, 0xFE, 0xFD];
        let mut builder = BinaryBuilder::new();
        builder.append_value(&invalid_bytes);
        let array = builder.finish();

        let val = value_from_column(
            &array,
            &DataType::Crdt(CrdtType::GCounter),
            0,
            CrdtDecodeMode::Lenient,
        )
        .expect("Lenient mode should not error");

        // Should return a default GCounter
        let decoded: Crdt = serde_json::from_value(val).expect("from_value should succeed");
        if let Crdt::GCounter(gc) = decoded {
            assert_eq!(gc.value(), 0);
        } else {
            panic!("Expected default GCounter");
        }
    }
}

// ============================================================================
// Full Round-Trip Tests
// ============================================================================

mod roundtrip {
    use super::*;

    /// Test full round-trip: CRDT -> JSON -> Arrow -> JSON -> CRDT
    fn test_roundtrip_for_type<F>(crdt: Crdt, data_type: DataType, verify: F)
    where
        F: Fn(&Crdt),
    {
        // Step 1: CRDT to JSON value
        let json_val: Value = serde_json::to_value(&crdt)
            .expect("to_value should succeed")
            .into();

        // Step 2: JSON value to Arrow BinaryArray
        let extractor = PropertyExtractor::new("prop", &data_type);
        let deleted = vec![false];
        let col = extractor
            .build_column(1, &deleted, |_| Some(&json_val))
            .expect("build_column should succeed");

        // Step 3: Arrow BinaryArray to JSON value
        let decoded_json = value_from_column(col.as_ref(), &data_type, 0, CrdtDecodeMode::Strict)
            .expect("decode should succeed");

        // Step 4: JSON value to CRDT
        let decoded_crdt: Crdt =
            serde_json::from_value(decoded_json).expect("from_value should succeed");

        // Verify the result
        verify(&decoded_crdt);
    }

    #[test]
    fn test_roundtrip_gcounter() {
        let mut gc = GCounter::new();
        gc.increment("actor1", 100);
        gc.increment("actor2", 200);

        test_roundtrip_for_type(
            Crdt::GCounter(gc),
            DataType::Crdt(CrdtType::GCounter),
            |decoded| {
                if let Crdt::GCounter(gc) = decoded {
                    assert_eq!(gc.value(), 300);
                    assert_eq!(gc.actor_count("actor1"), 100);
                    assert_eq!(gc.actor_count("actor2"), 200);
                } else {
                    panic!("Expected GCounter");
                }
            },
        );
    }

    #[test]
    fn test_roundtrip_gset() {
        let mut gs = GSet::new();
        gs.add("apple".to_string());
        gs.add("banana".to_string());
        gs.add("cherry".to_string());

        test_roundtrip_for_type(Crdt::GSet(gs), DataType::Crdt(CrdtType::GSet), |decoded| {
            if let Crdt::GSet(gs) = decoded {
                assert_eq!(gs.len(), 3);
                assert!(gs.contains(&"apple".to_string()));
                assert!(gs.contains(&"banana".to_string()));
                assert!(gs.contains(&"cherry".to_string()));
            } else {
                panic!("Expected GSet");
            }
        });
    }

    #[test]
    fn test_roundtrip_orset_with_removes() {
        let mut os = ORSet::new();
        os.add("keep".to_string());
        os.add("remove".to_string());
        os.remove(&"remove".to_string());
        os.add("keep".to_string()); // Second add

        test_roundtrip_for_type(
            Crdt::ORSet(os),
            DataType::Crdt(CrdtType::ORSet),
            |decoded| {
                if let Crdt::ORSet(os) = decoded {
                    assert!(os.contains(&"keep".to_string()));
                    assert!(!os.contains(&"remove".to_string()));
                } else {
                    panic!("Expected ORSet");
                }
            },
        );
    }

    #[test]
    fn test_roundtrip_lww_register() {
        let reg = LWWRegister::new(serde_json::json!({"nested": "value", "count": 42}), 999999);

        test_roundtrip_for_type(
            Crdt::LWWRegister(reg),
            DataType::Crdt(CrdtType::LWWRegister),
            |decoded| {
                if let Crdt::LWWRegister(reg) = decoded {
                    assert_eq!(
                        reg.get(),
                        &serde_json::json!({"nested": "value", "count": 42})
                    );
                    assert_eq!(reg.timestamp(), 999999);
                } else {
                    panic!("Expected LWWRegister");
                }
            },
        );
    }

    #[test]
    fn test_roundtrip_lww_map() {
        let mut map = LWWMap::new();
        map.put("active".to_string(), serde_json::json!(true), 100);
        map.put("removed".to_string(), serde_json::json!("value"), 100);
        map.remove(&"removed".to_string(), 200);

        test_roundtrip_for_type(
            Crdt::LWWMap(map),
            DataType::Crdt(CrdtType::LWWMap),
            |decoded| {
                if let Crdt::LWWMap(map) = decoded {
                    assert_eq!(
                        map.get(&"active".to_string()),
                        Some(&serde_json::json!(true))
                    );
                    assert_eq!(map.get(&"removed".to_string()), None);
                } else {
                    panic!("Expected LWWMap");
                }
            },
        );
    }

    #[test]
    fn test_roundtrip_rga() {
        let mut rga = Rga::new();
        let id1 = rga.insert(None, "A".to_string(), 1);
        let id2 = rga.insert(Some(id1), "B".to_string(), 2);
        let id3 = rga.insert(Some(id2), "C".to_string(), 3);
        rga.delete(id2); // Delete "B"

        test_roundtrip_for_type(Crdt::Rga(rga), DataType::Crdt(CrdtType::Rga), |decoded| {
            if let Crdt::Rga(rga) = decoded {
                assert_eq!(rga.to_vec(), vec!["A".to_string(), "C".to_string()]);
            } else {
                panic!("Expected Rga");
            }
        });

        let _ = id3;
    }

    #[test]
    fn test_roundtrip_vector_clock() {
        let mut vc = VectorClock::new();
        for _ in 0..5 {
            vc.increment("node1");
        }
        for _ in 0..3 {
            vc.increment("node2");
        }

        test_roundtrip_for_type(
            Crdt::VectorClock(vc),
            DataType::Crdt(CrdtType::VectorClock),
            |decoded| {
                if let Crdt::VectorClock(vc) = decoded {
                    assert_eq!(vc.get("node1"), 5);
                    assert_eq!(vc.get("node2"), 3);
                } else {
                    panic!("Expected VectorClock");
                }
            },
        );
    }

    #[test]
    fn test_roundtrip_vc_register() {
        let mut reg = VCRegister::new(serde_json::json!("initial"), "actor1");
        reg.set(serde_json::json!("updated"), "actor1");
        reg.set(serde_json::json!("final"), "actor2");

        test_roundtrip_for_type(
            Crdt::VCRegister(reg),
            DataType::Crdt(CrdtType::VCRegister),
            |decoded| {
                if let Crdt::VCRegister(reg) = decoded {
                    assert_eq!(reg.get(), &serde_json::json!("final"));
                    assert_eq!(reg.clock().get("actor1"), 2);
                    assert_eq!(reg.clock().get("actor2"), 1);
                } else {
                    panic!("Expected VCRegister");
                }
            },
        );
    }
}

// ============================================================================
// Multiple Row Tests
// ============================================================================

mod multi_row {
    use super::*;

    #[test]
    fn test_encode_multiple_crdts() {
        let mut gc1 = GCounter::new();
        gc1.increment("a", 10);

        let mut gc2 = GCounter::new();
        gc2.increment("b", 20);

        let mut gc3 = GCounter::new();
        gc3.increment("c", 30);

        let crdts = [
            Crdt::GCounter(gc1),
            Crdt::GCounter(gc2),
            Crdt::GCounter(gc3),
        ];

        let vals: Vec<Value> = crdts
            .iter()
            .map(|c| Value::from(serde_json::to_value(c).unwrap()))
            .collect();

        let extractor = PropertyExtractor::new("counter", &DataType::Crdt(CrdtType::GCounter));
        let deleted = vec![false, false, false];
        let col = extractor
            .build_column(3, &deleted, |i| Some(&vals[i]))
            .expect("build_column should succeed");

        let binary = col
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("Should be BinaryArray");

        assert_eq!(binary.len(), 3);

        for (i, expected) in [10u64, 20, 30].iter().enumerate() {
            let decoded = Crdt::from_msgpack(binary.value(i)).expect("from_msgpack should succeed");
            if let Crdt::GCounter(gc) = decoded {
                assert_eq!(gc.value(), *expected);
            } else {
                panic!("Expected GCounter at row {}", i);
            }
        }
    }

    #[test]
    fn test_decode_multiple_rows_with_nulls() {
        let mut gc = GCounter::new();
        gc.increment("actor", 42);
        let crdt = Crdt::GCounter(gc);
        let bytes = crdt.to_msgpack().expect("to_msgpack should succeed");

        let mut builder = BinaryBuilder::new();
        builder.append_value(&bytes);
        builder.append_null();
        builder.append_value(&bytes);
        let array = builder.finish();

        // Row 0: valid
        let val0 = value_from_column(
            &array,
            &DataType::Crdt(CrdtType::GCounter),
            0,
            CrdtDecodeMode::Strict,
        )
        .expect("decode row 0 should succeed");
        let decoded0: Crdt = serde_json::from_value(val0).unwrap();
        assert!(matches!(decoded0, Crdt::GCounter(_)));

        // Row 1: null - should error in strict mode when trying to read null
        // Actually, value_from_column doesn't check for nulls in BinaryArray
        // The caller should check is_null first
        assert!(array.is_null(1));

        // Row 2: valid
        let val2 = value_from_column(
            &array,
            &DataType::Crdt(CrdtType::GCounter),
            2,
            CrdtDecodeMode::Strict,
        )
        .expect("decode row 2 should succeed");
        let decoded2: Crdt = serde_json::from_value(val2).unwrap();
        assert!(matches!(decoded2, Crdt::GCounter(_)));
    }
}
