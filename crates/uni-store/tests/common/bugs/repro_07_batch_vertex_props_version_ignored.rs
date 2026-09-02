// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for property_manager.rs:506 (finding [7]).
//!
//! `PropertyManager::get_batch_vertex_props` projects `_version` (columns list
//! at :465) but never reads it: rows are applied in raw scan order with a
//! full-map `result.insert` overwrite (:516) and an unconditional
//! `result.remove` on `_deleted` (:507). There is NO `_version` ranking, so the
//! materialized properties depend on which storage row the scan happens to
//! return last. When two versions of the same vid coexist in the per-label
//! vertex table (e.g. two flushes before compaction) and the scan surfaces the
//! OLDER row last, the batch reader returns the STALE value — the exact MVCC
//! defect (review C2) that the single-vid `find_props_by_vid` was fixed to
//! close.
//!
//! We write both versions into one batch with the NEWER row first, so the
//! order-blind loop applies the older row last and the stale value wins.
//!
//! Ignored: the manifestation depends on scan row order, which the backend does
//! not contractually guarantee; the assertion pins the buggy (order-dependent)
//! outcome rather than a fixed invariant.

#![cfg(feature = "lance-backend")]

use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use uni_store::runtime::Writer;

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::tempdir;
use uni_common::Value;
use uni_common::core::id::Vid;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::manager::StorageManager;

#[tokio::test]
async fn repro_batch_vertex_props_returns_stale_version() {
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
        .add_property("Person", "name", DataType::String, true)
        .unwrap();
    schema_manager.save().await.unwrap();

    let storage = Arc::new(
        StorageManager::new(&path, schema_manager.clone())
            .await
            .unwrap(),
    );

    // Write two LIVE versions of Vid(1) into a single batch, NEWER row first so
    // the order-blind loop applies the stale v1 row last.
    let vid = Vid::new(1);
    let mut newer = HashMap::new();
    newer.insert("name".to_string(), Value::String("Bob".to_string())); // v2
    let mut older = HashMap::new();
    older.insert("name".to_string(), Value::String("Alice".to_string())); // v1

    let ds = storage.vertex_dataset("Person").unwrap();
    let schema = schema_manager.schema();
    let batch = ds
        .build_record_batch(
            &[
                (vid, vec!["Person".to_string()], newer),
                (vid, vec!["Person".to_string()], older),
            ],
            &[false, false],
            &[2u64, 1u64],
            schema.as_ref(),
        )
        .unwrap();
    ds.write_batch(storage.backend(), batch, schema.as_ref())
        .await
        .unwrap();

    let pm = PropertyManager::new(storage.clone(), schema_manager.clone(), 0);
    let res = pm
        .get_batch_vertex_props(&[vid], &["name"], None)
        .await
        .unwrap();

    // FIXED (property_manager.rs): get_batch_vertex_props now version-ranks, so
    // the v2 value "Bob" wins regardless of physical scan order.
    assert_eq!(
        res.get(&vid).and_then(|p| p.get("name")),
        Some(&Value::String("Bob".to_string())),
        "version-ranked batch read must return the newest (v2) value; got {res:?}"
    );
}

/// A multi-label vertex must keep every label's properties.
///
/// Written to check a suspected defect that turned out not to exist, and kept
/// because nothing else pinned the behaviour.
///
/// `get_batch_vertex_props` scans one table per label while ranking versions in
/// a single `best_version` map shared across the whole loop, and writes each
/// label's result with `result.insert` — replacing rather than merging. That
/// reads like it must drop the other label's columns, and would if per-label
/// tables held disjoint properties.
///
/// They do not: a property not declared on a label lands in that table's
/// `overflow_json`, so every label's row reconstructs the *full* property set
/// and replacing one with another loses nothing. The second half skews the two
/// tables' versions — touching only `name`, so Person advances and Staff does
/// not — which is the case the shared ranking would actually drop, and it
/// survives for the same reason.
#[tokio::test]
async fn batch_vertex_props_keeps_every_label_s_properties() -> Result<()> {
    use uni_common::Value;
    use uni_common::core::id::Vid;

    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, true)?;
    schema_manager.add_label("Staff")?;
    schema_manager.add_property("Staff", "badge", DataType::Int64, true)?;
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(&path, schema_manager.clone()).await?);
    let writer = Writer::new(storage.clone(), schema_manager.clone(), 0).await?;

    let vid = Vid::new(1);
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Alice".to_string()));
    props.insert("badge".to_string(), Value::Int(42));
    writer
        .insert_vertex_with_labels(
            vid,
            props,
            &["Person".to_string(), "Staff".to_string()],
            None,
        )
        .await?;
    writer.flush_to_l1(None).await?;

    let pm = PropertyManager::new(storage.clone(), schema_manager.clone(), 0);
    let res = pm
        .get_batch_vertex_props(&[vid], &["name", "badge"], None)
        .await?;
    let got = res.get(&vid).expect("the vertex is visible");
    assert_eq!(
        got.get("name"),
        Some(&Value::String("Alice".to_string())),
        "Person's property must survive; got {got:?}"
    );
    assert_eq!(
        got.get("badge"),
        Some(&Value::Int(42)),
        "Staff's property must survive too; got {got:?}"
    );

    // Now skew the versions between the two label tables: touch only `name`,
    // so Person's row advances and Staff's stays behind. `best_version` is
    // shared across the label loop, so the older label's row is skipped
    // entirely — the case where a shared ranking could drop columns.
    let mut updated = HashMap::new();
    updated.insert("name".to_string(), Value::String("Alice II".to_string()));
    updated.insert("badge".to_string(), Value::Int(42));
    writer
        .insert_vertex_with_labels(
            vid,
            updated,
            &["Person".to_string(), "Staff".to_string()],
            None,
        )
        .await?;
    writer.flush_to_l1(None).await?;

    let res = pm
        .get_batch_vertex_props(&[vid], &["name", "badge"], None)
        .await?;
    let got = res.get(&vid).expect("still visible after the update");
    assert_eq!(
        got.get("name"),
        Some(&Value::String("Alice II".to_string())),
        "the newer value wins; got {got:?}"
    );
    assert_eq!(
        got.get("badge"),
        Some(&Value::Int(42)),
        "the other label's property must not be dropped by version skew; got {got:?}"
    );
    Ok(())
}

/// A partial L0 write must not hide the properties still living in storage.
///
/// `get_batch_vertex_props_for_label_projected` skips storage entirely for any
/// vid that has L0 properties. That is sound while L0 rows are complete — the
/// invariant `L0Buffer::insert_vertex_partial_full` documents, and the default
/// write path upholds by merging a full map before staging.
///
/// With `partial_lance_writes` on, `Writer::insert_vertex_partial` stages only
/// the touched keys, so the L0 row is a delta. The reader then returns just
/// those keys and the stored ones vanish. That matters most on the write path:
/// the SET prefetch reads this map, merges over it, and writes the result back,
/// so a dropped property is not merely missing from one read — it is deleted.
#[tokio::test]
async fn a_partial_l0_write_does_not_hide_stored_properties() -> Result<()> {
    use uni_common::Value;
    use uni_common::config::UniConfig;
    use uni_common::core::id::Vid;

    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, true)?;
    schema_manager.add_property("Person", "age", DataType::Int64, true)?;
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(&path, schema_manager.clone()).await?);
    let config = UniConfig {
        partial_lance_writes: true,
        ..Default::default()
    };
    let writer = Writer::new_with_config(
        storage.clone(),
        schema_manager.clone(),
        0,
        config,
        None,
        None,
    )
    .await?;

    let vid = Vid::new(1);
    let mut props = HashMap::new();
    props.insert("name".to_string(), Value::String("Alice".to_string()));
    props.insert("age".to_string(), Value::Int(30));
    writer
        .insert_vertex_with_labels(vid, props, &["Person".to_string()], None)
        .await?;
    writer.flush_to_l1(None).await?;

    // Touch only `age`; `name` stays in storage and is absent from L0.
    let mut touched = HashMap::new();
    touched.insert("age".to_string(), Value::Int(31));
    writer
        .insert_vertex_partial(vid, touched, &["Person".to_string()], None)
        .await?;

    let ctx = uni_store::runtime::context::QueryContext::new(writer.l0_manager.get_current());
    let pm = PropertyManager::new(storage.clone(), schema_manager.clone(), 0);
    let res = pm
        .get_batch_vertex_props_for_label(&[vid], "Person", Some(&ctx))
        .await?;
    let got = res.get(&vid).expect("the vertex is visible");

    assert_eq!(
        got.get("age"),
        Some(&Value::Int(31)),
        "the touched value comes from L0; got {got:?}"
    );
    assert_eq!(
        got.get("name"),
        Some(&Value::String("Alice".to_string())),
        "the untouched property still lives in storage and must survive; got {got:?}"
    );
    Ok(())
}
