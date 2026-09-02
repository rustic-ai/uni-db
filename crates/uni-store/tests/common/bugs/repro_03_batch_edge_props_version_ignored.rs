// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for property_manager.rs:735 (finding [3]).
//!
//! `PropertyManager::get_batch_edge_props` is the edge sibling of the
//! version-ignoring batch read: `_version` is projected (:693) but never read,
//! each storage row fully replaces the previous props via `result.insert`
//! (:744), and a delete row (`op == 1`) unconditionally `result.remove`s (:736)
//! — with NO `_version` ranking. So the surviving props depend on raw scan
//! order. When a delete tombstone and an earlier live row for the same eid
//! coexist in the delta table and the scan surfaces the DELETE first, the loop
//! removes nothing and then re-inserts the stale live row — resurrecting a
//! deleted edge's properties.
//!
//! We write both rows into one delta batch with the DELETE first so the
//! order-blind loop applies the live row last.
//!
//! Ignored: the manifestation depends on scan row order, which the backend does
//! not contractually guarantee.

#![cfg(feature = "lance-backend")]

use std::collections::HashMap;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::tempdir;
use uni_common::Value;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::delta::{L1Entry, Op};
use uni_store::storage::manager::StorageManager;

#[tokio::test]
async fn repro_batch_edge_props_resurrects_deleted_edge() {
    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let schema_path = ObjectStorePath::from("schema.json");

    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store, &schema_path)
            .await
            .unwrap(),
    );
    schema_manager.add_label("Person").unwrap();
    schema_manager
        .add_edge_type(
            "KNOWS",
            vec!["Person".to_string()],
            vec!["Person".to_string()],
        )
        .unwrap();
    schema_manager
        .add_property("KNOWS", "weight", DataType::Int64, true)
        .unwrap();
    schema_manager.save().await.unwrap();

    let storage = Arc::new(
        StorageManager::new(&path, schema_manager.clone())
            .await
            .unwrap(),
    );

    let src = Vid::new(1);
    let dst = Vid::new(2);
    let eid = Eid::new(10);

    // Two delta rows for the same eid in ONE batch, DELETE (v2) first, live
    // INSERT (v1) second, so the order-blind loop applies the live row last.
    let mut live_props = HashMap::new();
    live_props.insert("weight".to_string(), Value::Int(7));
    let entries = vec![
        L1Entry {
            src_vid: src,
            dst_vid: dst,
            eid,
            op: Op::Delete,
            version: 2,
            properties: HashMap::new(),
            created_at: None,
            updated_at: None,
        },
        L1Entry {
            src_vid: src,
            dst_vid: dst,
            eid,
            op: Op::Insert,
            version: 1,
            properties: live_props,
            created_at: None,
            updated_at: None,
        },
    ];

    let dds = storage.delta_dataset("KNOWS", "fwd").unwrap();
    let schema = schema_manager.schema();
    let batch = dds.build_record_batch(&entries, schema.as_ref()).unwrap();
    dds.write_run(storage.backend(), batch).await.unwrap();

    let pm = PropertyManager::new(storage.clone(), schema_manager.clone(), 0);
    // eids are keyed as Vid(eid) in the result map (see get_batch_edge_props).
    let key = Vid::from(eid.as_u64());
    let res = pm
        .get_batch_edge_props(&[eid], &["weight"], None)
        .await
        .unwrap();

    // FIXED (property_manager.rs): get_batch_edge_props now version-ranks, so the
    // v2 DELETE wins over the older v1 live row regardless of scan order — the
    // deleted edge is omitted.
    assert!(
        !res.contains_key(&key),
        "version-ranked batch read must honour the v2 delete (no resurrection); got {res:?}"
    );
}

/// A delta-tombstoned edge must not be resurrected by the main-edges fallback.
///
/// The delta scan removes a deleted eid from the result, and the fallback then
/// re-hydrates it from `main_edges` *precisely because* its entry is missing:
/// `needs_fallback` is true for an absent key. Only L0 deletes were skipped,
/// not delta-table ones.
///
/// This is review finding C2 — the vertex path guards it with a `tombstoned`
/// set, and the two edge paths had no equivalent. It only bites when the delete
/// was recorded in the delta run without a matching `_deleted` main-edges row,
/// which is exactly what this test constructs.
#[tokio::test]
async fn a_delta_tombstoned_edge_is_not_resurrected_from_main_edges() {
    use uni_store::storage::main_edge::MainEdgeDataset;

    let dir = tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let schema_path = ObjectStorePath::from("schema.json");

    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store, &schema_path)
            .await
            .unwrap(),
    );
    schema_manager.add_label("Person").unwrap();
    schema_manager
        .add_edge_type(
            "KNOWS",
            vec!["Person".to_string()],
            vec!["Person".to_string()],
        )
        .unwrap();
    schema_manager
        .add_property("KNOWS", "weight", DataType::Int64, true)
        .unwrap();
    schema_manager.save().await.unwrap();

    let storage = Arc::new(
        StorageManager::new(&path, schema_manager.clone())
            .await
            .unwrap(),
    );

    let src = Vid::new(1);
    let dst = Vid::new(2);
    let eid = Eid::new(10);

    // The durable row: a live edge in main_edges, as a flush would dual-write.
    let mut main_props = HashMap::new();
    main_props.insert("weight".to_string(), Value::Int(7));
    let main_batch = MainEdgeDataset::build_record_batch(
        &[(
            eid,
            src,
            dst,
            "KNOWS".to_string(),
            main_props,
            false, // not deleted in main_edges — the case that bites
            1,
        )],
        None,
        None,
    )
    .unwrap();
    MainEdgeDataset::write_batch(storage.backend(), main_batch)
        .await
        .unwrap();

    // The delete, recorded only in the delta run at a newer version.
    let entries = vec![L1Entry {
        src_vid: src,
        dst_vid: dst,
        eid,
        op: Op::Delete,
        version: 2,
        properties: HashMap::new(),
        created_at: None,
        updated_at: None,
    }];
    let dds = storage.delta_dataset("KNOWS", "fwd").unwrap();
    let schema = schema_manager.schema();
    let batch = dds.build_record_batch(&entries, schema.as_ref()).unwrap();
    dds.write_run(storage.backend(), batch).await.unwrap();

    let pm = PropertyManager::new(storage.clone(), schema_manager.clone(), 0);
    let key = Vid::from(eid.as_u64());

    let res = pm
        .get_batch_edge_props(&[eid], &["weight"], None)
        .await
        .unwrap();
    assert!(
        !res.contains_key(&key),
        "the delta delete must gate the main-edges fallback; got {res:?}"
    );

    // The per-type path replays `op` and has the identical hole.
    let typed = pm
        .get_batch_edge_props_for_type(&[eid], "KNOWS", None)
        .await
        .unwrap();
    assert!(
        !typed.contains_key(&eid),
        "the replayed delete must gate the fallback too; got {typed:?}"
    );
}
