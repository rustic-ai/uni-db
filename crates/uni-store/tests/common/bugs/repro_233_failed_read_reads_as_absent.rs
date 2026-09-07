// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! #233 Tier 1: a failed read must not be reported as "absent".
//!
//! Two writer paths probed storage and treated a failed probe as a negative
//! answer, so a transient object-store error produced a wrong result rather
//! than an error:
//!
//! - `check_extid_globally_unique` (and its batch twin) used
//!   `if let Ok(Some(found_vid))`, so a failed probe read as "no duplicate"
//!   and the uniqueness constraint admitted the duplicate it exists to
//!   reject. `MainVertexDataset::find_by_ext_id` already answers `Ok(None)`
//!   for an absent table, so no benign error was being absorbed.
//! - `find_vertex_labels_in_storage` ate its scan error with
//!   `unwrap_or_default()`, and `get_vertex_labels` ate the remainder with
//!   `.ok().flatten()`, so a vertex whose labels could not be read looked
//!   like a vertex with no labels.

#![cfg(feature = "lance-backend")]

use std::collections::HashMap;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::tempdir;
use uni_common::Value;
use uni_common::config::UniConfig;
use uni_common::core::schema::SchemaManager;
use uni_store::backend::lance::LanceDbBackend;
use uni_store::runtime::writer::Writer;
use uni_store::storage::manager::StorageManager;

use super::fault_backend::FaultBackend;

/// Builds a writer over a `FaultBackend` so reads can be made to fail.
async fn setup() -> (tempfile::TempDir, Writer, Arc<FaultBackend>) {
    let dir = tempdir().unwrap();
    let uri = dir.path().to_str().unwrap().to_string();

    let lance = LanceDbBackend::connect(&uri, None).await.unwrap();
    let fault = Arc::new(FaultBackend::new(Arc::new(lance)));

    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store.clone(), &ObjectStorePath::from("schema.json"))
            .await
            .unwrap(),
    );
    schema_manager.add_label("Person").unwrap();

    let storage = Arc::new(
        StorageManager::new_with_backend(
            &uri,
            store,
            fault.clone(),
            schema_manager.clone(),
            UniConfig::default(),
        )
        .await
        .unwrap(),
    );
    let writer = Writer::new(storage, schema_manager, 1).await.unwrap();
    (dir, writer, fault)
}

fn ext_id_props(ext_id: &str) -> HashMap<String, Value> {
    let mut p = HashMap::new();
    p.insert("ext_id".to_string(), Value::String(ext_id.to_string()));
    p
}

/// A failed uniqueness probe must not admit a duplicate `ext_id`.
#[tokio::test]
async fn failed_extid_probe_does_not_admit_a_duplicate() {
    let (_dir, writer, fault) = setup().await;
    let person = vec!["Person".to_string()];

    // v1 owns "dup", and is flushed so that the ONLY copy visible to a later
    // insert is the one in the main vertices table — i.e. the duplicate can
    // be found only through the storage probe under test.
    let v1 = writer.next_vid().await.unwrap();
    writer
        .insert_vertex_with_labels(v1, ext_id_props("dup"), &person, None)
        .await
        .unwrap();
    writer.flush_to_l1(None).await.unwrap();

    // Control: with a healthy backend the probe finds v1 and rejects v2.
    // Without this the test could pass on a store where nothing was written.
    let v2 = writer.next_vid().await.unwrap();
    let healthy = writer
        .insert_vertex_with_labels(v2, ext_id_props("dup"), &person, None)
        .await;
    assert!(
        healthy.is_err(),
        "control: a duplicate ext_id must be rejected when the probe can read"
    );

    // Now make the probe fail. The duplicate is still there; only our ability
    // to see it has gone.
    fault.set_fail_table_exists(true);
    let v3 = writer.next_vid().await.unwrap();
    let injected = writer
        .insert_vertex_with_labels(v3, ext_id_props("dup"), &person, None)
        .await;

    assert!(
        injected.is_err(),
        "a failed uniqueness probe must surface as an error, not admit the duplicate"
    );
}

/// A failed label read must not be reported as "this vertex has no labels".
#[tokio::test]
async fn failed_label_read_does_not_read_as_no_labels() {
    let (_dir, writer, fault) = setup().await;
    let person = vec!["Person".to_string()];

    let vid = writer.next_vid().await.unwrap();
    writer
        .insert_vertex_with_labels(vid, HashMap::new(), &person, None)
        .await
        .unwrap();
    // Flush so the labels live only in storage: the L0 arms of
    // `get_vertex_labels` must miss, leaving the storage read as the answer.
    writer.flush_to_l1(None).await.unwrap();

    // Control: the labels are readable through storage.
    let healthy = writer
        .get_vertex_labels(vid, None)
        .await
        .expect("control: a healthy label read succeeds");
    assert_eq!(
        healthy.as_deref(),
        Some(&["Person".to_string()][..]),
        "control: the flushed vertex's labels are readable from storage"
    );

    // `table_exists` still succeeds; the read itself fails. This is the inner
    // swallow (`unwrap_or_default`), which ate the error before the outer
    // `if let Ok(..)` could ever see it.
    fault.set_fail_scan(true);
    let injected = writer.get_vertex_labels(vid, None).await;

    assert!(
        injected.is_err(),
        "a failed label read must surface as an error, not as an unlabelled vertex"
    );
}
