// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Semantic vertex compaction must not lose data it cannot see.
//!
//! `Compactor::compact_vertices` rebuilds every vertex's property map by
//! iterating the **schema-declared** properties for the label
//! (`storage/compaction.rs`, `for (name, meta) in label_props`) and then hands
//! that map to `VertexDataset::build_record_batch`, which derives four physical
//! columns from it.
//!
//! But `ext_id` and `overflow_json` are on the reserved-property list
//! (`uni_common::core::schema`, `RESERVED_PROPS`) precisely so they cannot
//! collide with user properties — so they can never appear in `label_props`.
//! The reconstruction is structurally incapable of seeing them, and the
//! read-path fallback to the main table is gated on there being no live
//! per-label row (`runtime/property_manager.rs`), which compaction has just
//! written. So the loss is not recoverable through reads either.
//!
//! Reachable from the `VACUUM` Cypher statement and from the background
//! compaction loop at default config.
//!
//! Every test here asserts the **pre-compaction** state first. Without that
//! denominator a "still present after compaction" assertion would pass
//! vacuously on a fixture where the value was never written in the first
//! place.

#![cfg(feature = "lance-backend")]

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use arrow_array::TimestampNanosecondArray;
use arrow_array::{Array, FixedSizeBinaryArray, LargeBinaryArray, StringArray, UInt64Array};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::TempDir;
use uni_common::Value;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::writer::Writer;
use uni_store::storage::compaction::Compactor;
use uni_store::storage::manager::StorageManager;

// Rust guideline compliant

const LABEL: &str = "Person";
const EXT_ID: &str = "person-1";
const SCHEMALESS_PROP: &str = "nickname";
const SCHEMALESS_VALUE: &str = "Al";

struct Fixture {
    writer: Arc<Writer>,
    storage: Arc<StorageManager>,
    schema_manager: Arc<SchemaManager>,
    _dir: TempDir,
}

/// One `Person` with a declared property, an `ext_id`, and a schemaless
/// property, flushed to L1 so the per-label Lance table exists.
async fn seeded_person() -> Result<(Fixture, u64)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);

    schema_manager.add_label(LABEL)?;
    // `compact_vertices` errors with "Label not found" unless the label has at
    // least one declared property, so this is load-bearing, not decoration.
    schema_manager.add_property(LABEL, "name", DataType::String, true)?;
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer = Arc::new(Writer::new(storage.clone(), schema_manager.clone(), 1).await?);

    let vid = writer.next_vid().await?;
    let mut props: HashMap<String, Value> = HashMap::new();
    // A system column: never in `schema.properties`, extracted by
    // `build_record_batch` from the property map it is handed.
    props.insert("ext_id".to_string(), Value::String(EXT_ID.to_string()));
    // A declared property: survives compaction today; the control.
    props.insert("name".to_string(), Value::String("Alice".to_string()));
    // A schemaless property: lands in `overflow_json`.
    props.insert(
        SCHEMALESS_PROP.to_string(),
        Value::String(SCHEMALESS_VALUE.to_string()),
    );

    writer
        .insert_vertex_with_labels(vid, props, &[LABEL.to_string()], None)
        .await?;
    writer.flush_to_l1(None).await?;

    Ok((
        Fixture {
            writer,
            storage,
            schema_manager,
            _dir: dir,
        },
        vid.as_u64(),
    ))
}

/// Raw scan of the per-label table — deliberately underneath the read path,
/// because the read path is where this loss is currently invisible.
async fn scan_columns(storage: &StorageManager) -> Result<arrow_array::RecordBatch> {
    storage
        .scan_vertex_table(
            LABEL,
            &[
                "_vid",
                "_uid",
                "ext_id",
                "overflow_json",
                "_created_at",
                "name",
            ],
            None,
        )
        .await?
        .ok_or_else(|| anyhow::anyhow!("per-label vertex table is missing after flush"))
}

fn ext_id_at(batch: &arrow_array::RecordBatch, row: usize) -> Option<String> {
    let col = batch.column_by_name("ext_id")?;
    let arr = col.as_any().downcast_ref::<StringArray>()?;
    (!arr.is_null(row)).then(|| arr.value(row).to_string())
}

fn uid_at(batch: &arrow_array::RecordBatch, row: usize) -> Option<Vec<u8>> {
    let col = batch.column_by_name("_uid")?;
    let arr = col.as_any().downcast_ref::<FixedSizeBinaryArray>()?;
    (!arr.is_null(row)).then(|| arr.value(row).to_vec())
}

fn overflow_at(batch: &arrow_array::RecordBatch, row: usize) -> Option<Vec<u8>> {
    let col = batch.column_by_name("overflow_json")?;
    let arr = col.as_any().downcast_ref::<LargeBinaryArray>()?;
    (!arr.is_null(row)).then(|| arr.value(row).to_vec())
}

fn created_at_is_set(batch: &arrow_array::RecordBatch, row: usize) -> bool {
    let Some(col) = batch.column_by_name("_created_at") else {
        return false;
    };
    match col.as_any().downcast_ref::<TimestampNanosecondArray>() {
        Some(arr) => !arr.is_null(row),
        None => false,
    }
}

fn row_of(batch: &arrow_array::RecordBatch, vid: u64) -> Option<usize> {
    let col = batch.column_by_name("_vid")?;
    let arr = col.as_any().downcast_ref::<UInt64Array>()?;
    (0..arr.len()).find(|&i| arr.value(i) == vid)
}

#[tokio::test]
async fn compaction_preserves_ext_id() -> Result<()> {
    let (fx, vid) = seeded_person().await?;

    let before = scan_columns(&fx.storage).await?;
    let row = row_of(&before, vid).expect("seeded vertex is in the table");
    assert_eq!(
        ext_id_at(&before, row).as_deref(),
        Some(EXT_ID),
        "precondition: the flush must write ext_id, or this test proves nothing"
    );

    Compactor::new(fx.storage.clone())
        .compact_vertices(LABEL)
        .await?;

    let after = scan_columns(&fx.storage).await?;
    let row = row_of(&after, vid).expect("vertex survives compaction");
    assert_eq!(
        ext_id_at(&after, row).as_deref(),
        Some(EXT_ID),
        "ext_id was dropped by semantic compaction: it is a reserved column, so \
         compact_vertices' schema-driven property rebuild never reads it"
    );
    Ok(())
}

#[tokio::test]
async fn compaction_preserves_schemaless_properties() -> Result<()> {
    let (fx, vid) = seeded_person().await?;

    let before = scan_columns(&fx.storage).await?;
    let row = row_of(&before, vid).expect("seeded vertex is in the table");
    assert!(
        overflow_at(&before, row).is_some(),
        "precondition: the flush must write overflow_json for a schemaless property"
    );

    Compactor::new(fx.storage.clone())
        .compact_vertices(LABEL)
        .await?;

    // Through the read path, with the cache disabled so this is a real read.
    let pm = PropertyManager::new(fx.storage.clone(), fx.schema_manager.clone(), 0);
    let props = pm.get_all_vertex_props(uni_common::Vid::from(vid)).await?;
    assert_eq!(
        props.get(SCHEMALESS_PROP),
        Some(&Value::String(SCHEMALESS_VALUE.to_string())),
        "schemaless property was erased by semantic compaction, and the main-table \
         fallback does not rescue it because a live per-label row now exists"
    );

    // And underneath it, so a future caching change cannot mask the loss.
    let after = scan_columns(&fx.storage).await?;
    let row = row_of(&after, vid).expect("vertex survives compaction");
    assert!(
        overflow_at(&after, row).is_some(),
        "overflow_json is NULL after compaction"
    );
    Ok(())
}

#[tokio::test]
async fn compaction_preserves_vertex_uid() -> Result<()> {
    let (fx, vid) = seeded_person().await?;

    let before = scan_columns(&fx.storage).await?;
    let row = row_of(&before, vid).expect("seeded vertex is in the table");
    let uid_before = uid_at(&before, row).expect("precondition: the flush must write _uid");

    Compactor::new(fx.storage.clone())
        .compact_vertices(LABEL)
        .await?;

    let after = scan_columns(&fx.storage).await?;
    let row = row_of(&after, vid).expect("vertex survives compaction");
    let uid_after = uid_at(&after, row).expect("_uid is NULL after compaction");
    assert_eq!(
        uid_before, uid_after,
        "_uid changed across compaction with no property change: it is content-addressed \
         over (label, ext_id, properties), and compaction recomputes it from a map that \
         is missing ext_id and every schemaless property"
    );
    Ok(())
}

#[tokio::test]
async fn compaction_preserves_vertex_timestamps() -> Result<()> {
    let (fx, vid) = seeded_person().await?;

    let before = scan_columns(&fx.storage).await?;
    let row = row_of(&before, vid).expect("seeded vertex is in the table");
    assert!(
        created_at_is_set(&before, row),
        "precondition: the flush must write _created_at, or this test proves nothing"
    );

    Compactor::new(fx.storage.clone())
        .compact_vertices(LABEL)
        .await?;

    let after = scan_columns(&fx.storage).await?;
    let row = row_of(&after, vid).expect("vertex survives compaction");
    assert!(
        created_at_is_set(&after, row),
        "_created_at is NULL after compaction: compact_vertices calls build_record_batch, \
         which passes None for both timestamp maps"
    );
    Ok(())
}

/// Pin, not a repro. `compact_vertices` skips the replace when every vertex of
/// the label is tombstoned. That looks like the vertex analogue of the
/// adjacency bug fixed in `compact_adjacency` ("skipping the replace here would
/// leave the stale pre-delete L2 rows intact while the tombstone-clear below
/// erases the Delta L1 deletes"), but it is not: the adjacency bug was a bug
/// only because the skipped write was paired with an unconditional delete of
/// the evidence. The per-label vertex table is self-contained, so skipping the
/// rewrite leaves the pre-compaction truth — tombstones included — in place.
/// Unreclaimed space, not resurrection.
#[tokio::test]
async fn all_vertices_deleted_leaves_none_visible() -> Result<()> {
    let (fx, vid) = seeded_person().await?;
    let typed_vid = uni_common::Vid::from(vid);

    fx.writer
        .delete_vertex(typed_vid, Some(vec![LABEL.to_string()]), None)
        .await?;
    fx.writer.flush_to_l1(None).await?;

    Compactor::new(fx.storage.clone())
        .compact_vertices(LABEL)
        .await?;

    let pm = PropertyManager::new(fx.storage.clone(), fx.schema_manager.clone(), 0);
    let props = pm.get_all_vertex_props(typed_vid).await?;
    assert!(
        props.is_empty(),
        "a deleted vertex must stay invisible after compaction skips the replace"
    );

    // The usability half: a store that survives every read assertion but cannot
    // take a new write is still broken.
    let fresh = fx.writer.next_vid().await?;
    let mut props: HashMap<String, Value> = HashMap::new();
    props.insert("name".to_string(), Value::String("Bob".to_string()));
    fx.writer
        .insert_vertex_with_labels(fresh, props, &[LABEL.to_string()], None)
        .await?;
    fx.writer.flush_to_l1(None).await?;

    let after = pm.get_all_vertex_props(fresh).await?;
    assert_eq!(
        after.get("name"),
        Some(&Value::String("Bob".to_string())),
        "a fresh insert after an all-deleted compaction must be readable"
    );
    Ok(())
}
