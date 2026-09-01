// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Issue #211 — the startup rebuild of the VID→labels index must cover every
//! vertex, not the first 100 000.
//!
//! `StorageManager::rebuild_vid_labels_index` runs once when a manager opens
//! existing data, scanning the main vertex table to populate the index. It
//! carried `.with_limit(100_000)`, so on a larger graph every vertex past the cap
//! was absent from the index.
//!
//! That is not a cache miss. A traversal's target-label filter consults the index
//! and **keeps** the row when a vid does not resolve — "trust storage-level
//! filtering" (`uni-query`, `df_graph/traverse.rs`) — so an omitted vertex
//! silently passes a label predicate it does not satisfy. At LDBC SF1 (3 181 724
//! vertices) `MATCH (p:Person)<-[:HAS_CREATOR]-(post:Post)` returned 3 055 774
//! rows, `Comment`s included, against the 1 003 605 the scan-anchored form
//! returns.
//!
//! # Why the test lives here and not at the query layer
//!
//! The first attempt was an end-to-end fixture in `uni-db`: build past the cap,
//! flush, reopen, run a labelled traversal. It **passed with the cap restored** —
//! it proved nothing. `resolve_vertex_labels` tries L0 before the index, and a
//! freshly built-and-reopened database can still answer from L0, so the
//! truncation never decided the outcome.
//!
//! Asserting on the rebuild directly removes that confound: write past the cap,
//! flush, then open a *second* `StorageManager` over the same path — which is
//! what triggers the rebuild — and ask the index about a vertex written late.
//! Note this cannot be checked on the writer's own manager: `flush_to_l1` updates
//! the index incrementally as it writes (`update_vid_labels_index`), so that one
//! is populated whatever the rebuild does.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectStorePath;
use tempfile::tempdir;
use uni_common::core::schema::{DataType, SchemaManager};
use uni_store::runtime::Writer;
use uni_store::storage::manager::StorageManager;

/// Comfortably past the old 100 000-row cap.
const ROWS: usize = 110_000;

#[tokio::test]
async fn the_rebuilt_index_covers_every_vertex_not_the_first_100k() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().to_str().unwrap();
    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);

    let schema_manager = Arc::new(
        SchemaManager::load_from_store(store, &ObjectStorePath::from("schema.json")).await?,
    );
    schema_manager.add_label("Person")?;
    schema_manager.add_property("Person", "n", DataType::Int64, false)?;
    schema_manager.save().await?;

    let first_vid;
    let last_vid;
    {
        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
        let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

        let mut first = None;
        let mut last = None;
        for i in 0..ROWS {
            let vid = writer.next_vid().await?;
            let mut props = HashMap::new();
            props.insert("n".to_string(), (i as i64).into());
            writer
                .insert_vertex_with_labels(vid, props, &["Person".to_string()], None)
                .await?;
            if first.is_none() {
                first = Some(vid);
            }
            last = Some(vid);
        }
        writer.flush_to_l1(None).await?;
        first_vid = first.expect("wrote at least one vertex");
        last_vid = last.expect("wrote at least one vertex");
    }

    // A second manager over the same path: this is the constructor that rebuilds
    // the index from storage, and the only place the cap applied.
    let reopened = StorageManager::new(path, schema_manager.clone()).await?;

    assert_eq!(
        reopened.get_labels_from_index(first_vid),
        Some(vec!["Person".to_string()]),
        "the first vertex must resolve — if this fails the rebuild is broken \
         outright, not truncated"
    );
    assert_eq!(
        reopened.get_labels_from_index(last_vid),
        Some(vec!["Person".to_string()]),
        "vertex {ROWS} of {ROWS} did not resolve after a rebuild. The scan that \
         populates the index is truncating, so every vertex past its limit is \
         invisible to the traversal label filter — which keeps unresolved rows \
         rather than dropping them, and so admits vertices of the wrong label \
         (#211)"
    );

    Ok(())
}
