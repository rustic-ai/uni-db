// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Tests for Issue #29: VidLabelsIndex integration
//!
//! Verifies that the VID-to-labels index provides O(1) lookups. The index is
//! always enabled (see Issue #99): the planner relies on it to resolve
//! traversal-time label predicates for vertices that live only in Lance
//! storage, so it can no longer be disabled.

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::runtime::Writer;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::storage::manager::StorageManager;

#[tokio::test]
async fn test_vid_labels_index_basic_functionality() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");

    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;

    // Create storage with index enabled (default)
    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);

    // Create a writer and insert some vertices
    let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

    // Allocate VIDs and insert 3 vertices with labels
    let vid1 = writer.next_vid().await?;
    let mut props1 = HashMap::new();
    props1.insert("name".to_string(), "Alice".into());
    writer
        .insert_vertex_with_labels(vid1, props1, &["Person".to_string()], None)
        .await?;

    let vid2 = writer.next_vid().await?;
    let mut props2 = HashMap::new();
    props2.insert("name".to_string(), "Bob".into());
    writer
        .insert_vertex_with_labels(vid2, props2, &["Person".to_string()], None)
        .await?;

    let vid3 = writer.next_vid().await?;
    let mut props3 = HashMap::new();
    props3.insert("name".to_string(), "Charlie".into());
    writer
        .insert_vertex_with_labels(vid3, props3, &["Person".to_string()], None)
        .await?;

    // Flush to storage (this should update the index)
    writer.flush_to_l1(None).await?;

    // Verify the index returns correct labels
    let labels1 = storage.get_labels_from_index(vid1);
    assert_eq!(labels1, Some(vec!["Person".to_string()]));

    let labels2 = storage.get_labels_from_index(vid2);
    assert_eq!(labels2, Some(vec!["Person".to_string()]));

    let labels3 = storage.get_labels_from_index(vid3);
    assert_eq!(labels3, Some(vec!["Person".to_string()]));

    Ok(())
}

#[tokio::test]
async fn test_vid_labels_index_delete_removes_from_index() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");

    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

    // Insert a vertex
    let vid = writer.next_vid().await?;
    let mut props = HashMap::new();
    props.insert("name".to_string(), "Alice".into());
    writer
        .insert_vertex_with_labels(vid, props, &["Person".to_string()], None)
        .await?;
    writer.flush_to_l1(None).await?;

    // Verify it's in the index
    assert!(storage.get_labels_from_index(vid).is_some());

    // Delete the vertex
    writer.delete_vertex(vid, None, None).await?;
    writer.flush_to_l1(None).await?;

    // Verify it's removed from the index
    assert_eq!(storage.get_labels_from_index(vid), None);

    Ok(())
}

#[tokio::test]
async fn test_vid_labels_index_always_enabled_after_flush() -> Result<()> {
    // The index can no longer be disabled (Issue #99). After a flush a
    // persisted vertex must resolve through the index, not just the L0
    // fallback — this is what lets a fork match a traversal-time label
    // predicate against Lance-only vertices.
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");

    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);

    let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

    // Insert a vertex
    let vid = writer.next_vid().await?;
    let mut props = HashMap::new();
    props.insert("name".to_string(), "Alice".into());
    writer
        .insert_vertex_with_labels(vid, props, &["Person".to_string()], None)
        .await?;
    writer.flush_to_l1(None).await?;

    // The index is populated at flush time and returns the real labels.
    assert_eq!(
        storage.get_labels_from_index(vid),
        Some(vec!["Person".to_string()])
    );

    // PropertyManager resolves the same labels.
    let property_manager = PropertyManager::new(storage.clone(), schema_manager.clone(), 100);
    let labels = property_manager.get_batch_labels(&[vid], None).await?;
    assert_eq!(labels.get(&vid), Some(&vec!["Person".to_string()]));

    Ok(())
}

#[tokio::test]
async fn test_vid_labels_index_rebuild_on_open() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");

    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;

    let vid1;
    let vid2;

    // Create storage and insert vertices
    {
        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
        let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

        vid1 = writer.next_vid().await?;
        let mut props1 = HashMap::new();
        props1.insert("name".to_string(), "Alice".into());
        writer
            .insert_vertex_with_labels(vid1, props1, &["Person".to_string()], None)
            .await?;

        vid2 = writer.next_vid().await?;
        let mut props2 = HashMap::new();
        props2.insert("name".to_string(), "Bob".into());
        writer
            .insert_vertex_with_labels(vid2, props2, &["Person".to_string()], None)
            .await?;

        writer.flush_to_l1(None).await?;
    }

    // Re-open storage (this should rebuild the index)
    let storage2 = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);

    // Verify the index has the correct data
    assert_eq!(
        storage2.get_labels_from_index(vid1),
        Some(vec!["Person".to_string()])
    );
    assert_eq!(
        storage2.get_labels_from_index(vid2),
        Some(vec!["Person".to_string()])
    );

    Ok(())
}

/// A derived (pinned / fork) StorageManager must get its OWN label index, not an
/// `Arc`-clone of the parent's — otherwise a fork-local relabel/delete + flush
/// mutates the parent's traversal-time label resolution (production-readiness
/// review H1/L2). `at_fork_with_schema` uses the identical deep-copy; `pinned`
/// is the cheapest constructible witness of the mechanism.
#[tokio::test]
async fn pinned_view_label_index_is_isolated_from_parent() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");

    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "name", DataType::String, false)?;
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
    let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

    let vid = writer.next_vid().await?;
    let mut props = HashMap::new();
    props.insert("name".to_string(), "Alice".into());
    writer
        .insert_vertex_with_labels(vid, props, &["Person".to_string()], None)
        .await?;
    writer.flush_to_l1(None).await?;
    assert_eq!(
        storage.get_labels_from_index(vid),
        Some(vec!["Person".to_string()])
    );

    // Derive a pinned view from a real published snapshot.
    let snapshot = storage
        .snapshot_manager()
        .load_latest_snapshot()
        .await?
        .expect("a snapshot was published by flush_to_l1");
    let pinned = storage.pinned(snapshot);
    assert_eq!(
        pinned.get_labels_from_index(vid),
        Some(vec!["Person".to_string()]),
        "pinned view inherits the parent's labels (preserves #99 inheritance)"
    );

    // Mutate the derived view's index, as a fork-local delete/relabel flush would.
    pinned.remove_from_vid_labels_index(vid);
    assert_eq!(pinned.get_labels_from_index(vid), None);

    // The parent MUST be unaffected (deep-copy isolation).
    assert_eq!(
        storage.get_labels_from_index(vid),
        Some(vec!["Person".to_string()]),
        "parent label index must not be mutated by a derived view (review H1/L2)"
    );

    Ok(())
}

/// `REMOVE n:Label` must be honoured by the batch label read.
///
/// `get_batch_labels` unioned labels across L0 buffers with `.extend` and never
/// consulted `vertex_label_overwrites`, so a label removal was a no-op and the
/// label came back on the next read. It also skipped storage entirely for any
/// vid present in L0, truncating a flushed multi-label vertex to whatever
/// labels one write happened to carry.
///
/// The columnar path already gets both right in
/// `build_labels_column_for_known_label` — union for plain writes, newest
/// overwrite replaces — so this pins the row-wise path to the same rule.
#[tokio::test]
async fn batch_labels_honour_an_l0_label_overwrite() -> Result<()> {
    use parking_lot::RwLock;
    use uni_common::core::id::Vid;
    use uni_store::runtime::context::QueryContext;
    use uni_store::runtime::l0::L0Buffer;

    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap().to_string();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
    let schema_path = ObjectStorePath::from("schema.json");
    let schema_manager = Arc::new(SchemaManager::load_from_store(store, &schema_path).await?);
    schema_manager.add_label("Person")?;
    schema_manager.add_label("Staff")?;
    schema_manager.save().await?;

    let storage = Arc::new(StorageManager::new(&path, schema_manager.clone()).await?);
    let pm = PropertyManager::new(storage.clone(), schema_manager.clone(), 0);
    let vid = Vid::new(1);

    // A plain L0 write adds labels: they union.
    let plain = Arc::new(RwLock::new(L0Buffer::new(0, None)));
    plain
        .write()
        .vertex_labels
        .insert(vid, vec!["Person".to_string(), "Staff".to_string()]);
    let ctx = QueryContext::new(plain.clone());
    let got = pm.get_batch_labels(&[vid], Some(&ctx)).await?;
    assert_eq!(
        got.get(&vid),
        Some(&vec!["Person".to_string(), "Staff".to_string()]),
        "a plain L0 write unions its labels"
    );

    // `REMOVE n:Staff` in a NEWER buffer, over the write that added it. The
    // removal has to sit in the same visibility chain as the label it removes:
    // a fresh buffer on its own would union to the right answer by accident and
    // the assertion would pass whether or not overwrites are honoured.
    let removed = Arc::new(RwLock::new(L0Buffer::new(0, None)));
    removed
        .write()
        .set_vertex_labels(vid, &["Person".to_string()]);
    let ctx = QueryContext::new_with_pending(removed.clone(), None, vec![plain.clone()]);
    let got = pm.get_batch_labels(&[vid], Some(&ctx)).await?;
    assert_eq!(
        got.get(&vid),
        Some(&vec!["Person".to_string()]),
        "an overwrite replaces the set; Staff must not survive the removal"
    );

    Ok(())
}
