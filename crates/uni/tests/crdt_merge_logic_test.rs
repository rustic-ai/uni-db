// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! # CRDT Merge Logic Integration Tests
//!
//! This test suite specifically targets the merge logic of CRDTs in the
//! Write and Read paths, ensuring that state evolves correctly under updates.
//!
//! Unlike `e2e_comprehensive_test.rs` which focuses on round-tripping,
//! these tests perform multiple updates to trigger:
//! 1. `Writer::prepare_vertex_upsert` (Merge on Write)
//! 2. `PropertyManager::get_vertex_prop` (Merge on Read)

use anyhow::Result;
use uni_db::{CrdtType, DataType, Uni};

mod test_helpers {
    use super::*;

    pub async fn create_db() -> Result<Uni> {
        let db = Uni::in_memory().build().await?;
        db.schema()
            .label("CrdtCounter")
            .property("id", DataType::Int64)
            .property("count", DataType::Crdt(CrdtType::GCounter))
            .index("id", uni_db::IndexType::Scalar(uni_db::ScalarType::BTree))
            .label("CrdtSet")
            .property("id", DataType::Int64)
            .property("items", DataType::Crdt(CrdtType::GSet))
            .index("id", uni_db::IndexType::Scalar(uni_db::ScalarType::BTree))
            .apply()
            .await?;
        Ok(db)
    }
}

#[tokio::test]
async fn test_gcounter_merge_on_write() -> Result<()> {
    let db = test_helpers::create_db().await?;

    // 1. Initial Create: Actor A = 10
    // JSON: {"t": "gc", "d": {"counts": {"A": 10}}}
    db.execute(
        r#"CREATE (c:CrdtCounter {id: 1, count: '{"t": "gc", "d": {"counts": {"A": 10}}}'})"#,
    )
    .await?;

    // Flush to ensure it's in storage/L1 for the next read
    db.flush().await?;

    // 2. Update: Actor B = 5
    // This MATCH + SET should trigger `Writer::prepare_vertex_upsert`
    // The writer will fetch the existing value (A=10) and merge with new value (B=5)
    db.execute(
        r#"MATCH (c:CrdtCounter {id: 1}) SET c.count = '{"t": "gc", "d": {"counts": {"B": 5}}}'"#,
    )
    .await?;

    // 3. Verify immediately (Read from L0 overlay)
    let result = db
        .query("MATCH (c:CrdtCounter {id: 1}) RETURN c.count")
        .await?;

    assert_eq!(result.len(), 1, "Expected 1 row");
    let count = result.rows()[0].value("c.count").unwrap();

    // The CRDT value is returned as a parsed JSON object (Map), not a string.
    // Convert to serde_json::Value for easy access.
    let val_json: serde_json::Value = count.clone().into();
    let counts = val_json["d"]["counts"].as_object().unwrap();

    // Should have both A=10 and B=5
    assert_eq!(counts["A"], 10);
    assert_eq!(counts["B"], 5);

    Ok(())
}

#[tokio::test]
async fn test_gset_merge_on_read() -> Result<()> {
    let db = test_helpers::create_db().await?;

    // 1. Create with Item "Apple"
    db.execute(r#"CREATE (s:CrdtSet {id: 1, items: '{"t": "gs", "d": {"elements": ["Apple"]}}'})"#)
        .await?;
    db.flush().await?;

    // 2. Update with Item "Banana"
    // We flush again to force both versions into storage runs (L1),
    // forcing PropertyManager to merge them on read if using L1-L1 merge
    // (though Writer usually compacts on write).
    // Alternatively, we can leave one in L0.

    db.execute(
        r#"MATCH (s:CrdtSet {id: 1}) SET s.items = '{"t": "gs", "d": {"elements": ["Banana"]}}'"#,
    )
    .await?;

    // We do NOT flush here, so "Banana" is in L0, "Apple" is in Storage.
    // `get_vertex_prop` should merge Storage("Apple") + L0("Banana")

    let result = db.query("MATCH (s:CrdtSet {id: 1}) RETURN s.items").await?;
    assert_eq!(result.len(), 1, "Expected 1 row");

    let items_val = result.rows()[0].value("s.items").unwrap();

    // The CRDT value is returned as a parsed JSON object (Map), not a string.
    let val_json: serde_json::Value = items_val.clone().into();
    let elements = val_json["d"]["elements"].as_array().unwrap();

    let elems_vec: Vec<&str> = elements.iter().map(|v| v.as_str().unwrap()).collect();

    assert!(elems_vec.contains(&"Apple"));
    assert!(elems_vec.contains(&"Banana"));
    assert_eq!(elems_vec.len(), 2);

    Ok(())
}
