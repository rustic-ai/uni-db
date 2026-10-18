#![allow(dead_code, unused_imports, clippy::all)]
// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! M5 Batch 2 follow-up #4 — end-to-end planner dispatch through a
//! plugin-registered [`IndexHandle`].
//!
//! The Batch 2 commit only consulted `PluginRegistry::index_kind` for
//! diagnostics; every probe still dispatched to the native vector
//! backend. This follow-up wires `PluginRegistry::register_index_handle`
//! into the planner so a custom handle owns the retrieval step.
//!
//! Positive test: register a `CountingHandle` that returns a single
//! hand-picked vid + distance and asserts (a) the handle's `probe` ran
//! exactly once during query execution, and (b) the row count reflects
//! the handle's answer rather than the native path's. The native top-K
//! against query `[1.0, 0.0]` over `(A=[1,0], B=[0,1])` returns both rows;
//! the handle returns only `B`.
//!
//! The probe is reached through `n.embedding ~= q`, the top-K index scan.
//! A `vector_similarity(...) > t` threshold is a per-row filter and never
//! consults an index, native or plugin.
//!
//! Negative test (regression guard): do NOT register a handle; the
//! native `StorageManager::vector_search` path runs and returns both
//! rows, confirming the registry consultation is a strict opt-in.

// Rust guideline compliant

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow_array::builder::{Float32Builder, Int64Builder};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use uni_db::Uni;
use uni_db::core::schema::DataType as UniDataType;
use uni_plugin::errors::FnError;
use uni_plugin::traits::index::{IndexHandle, IndexKind};

/// Test fixture handle. Counts `probe` invocations and returns the
/// preloaded `(vid, distance)` rows verbatim.
#[derive(Debug)]
struct CountingHandle {
    /// Incremented on every `probe` call.
    probe_count: Arc<AtomicUsize>,
    /// Rows to emit on each `probe`. The schema is the trait-required
    /// `[vid: Int64, distance: Float32]`.
    rows: Vec<(i64, f32)>,
    /// Pre-built result schema (cached so `schema()` returns the same
    /// `SchemaRef` every call).
    result_schema: Arc<ArrowSchema>,
}

impl CountingHandle {
    fn new(rows: Vec<(i64, f32)>) -> Self {
        Self {
            probe_count: Arc::new(AtomicUsize::new(0)),
            rows,
            result_schema: Arc::new(ArrowSchema::new(vec![
                Field::new("vid", DataType::Int64, false),
                Field::new("distance", DataType::Float32, false),
            ])),
        }
    }

    fn probe_count(&self) -> usize {
        self.probe_count.load(Ordering::SeqCst)
    }

    fn build_result_batch(&self) -> RecordBatch {
        let mut vid_b = Int64Builder::with_capacity(self.rows.len());
        let mut dist_b = Float32Builder::with_capacity(self.rows.len());
        for (vid, dist) in &self.rows {
            vid_b.append_value(*vid);
            dist_b.append_value(*dist);
        }
        RecordBatch::try_new(
            self.result_schema.clone(),
            vec![Arc::new(vid_b.finish()), Arc::new(dist_b.finish())],
        )
        .expect("static schema must match column types")
    }
}

impl IndexHandle for CountingHandle {
    fn probe(&self, _query: &RecordBatch, _k: usize) -> Result<RecordBatch, FnError> {
        self.probe_count.fetch_add(1, Ordering::SeqCst);
        Ok(self.build_result_batch())
    }

    fn persist(&self) -> Result<Vec<u8>, FnError> {
        Ok(Vec::new())
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.result_schema.clone()
    }
}

/// Build a Uni instance with `:Item(name, embedding)` + a vector index
/// named `idx_item_embed`, insert two items, and return the db handle
/// plus the captured vids for `A` and `B`.
async fn seed_db() -> anyhow::Result<(Arc<Uni>, i64, i64)> {
    let db = Uni::temporary().build().await?;
    let db = Arc::new(db);

    db.schema()
        .label("Item")
        .property("name", UniDataType::String)
        .property("embedding", UniDataType::Vector { dimensions: 2 })
        .apply()
        .await?;

    let tx = db.session().tx().await?;
    tx.execute(
        "CREATE VECTOR INDEX idx_item_embed \
         FOR (i:Item) ON (i.embedding) \
         OPTIONS { metric: 'l2' }",
    )
    .await?;
    tx.execute("CREATE (:Item {name: 'A', embedding: [1.0, 0.0]})")
        .await?;
    tx.execute("CREATE (:Item {name: 'B', embedding: [0.0, 1.0]})")
        .await?;
    tx.commit().await?;

    // Capture vids for the two items.
    let rows = db
        .session()
        .query("MATCH (n:Item) RETURN n._vid AS vid, n.name AS name ORDER BY name")
        .await?;

    let mut vid_a = None;
    let mut vid_b = None;
    for row in rows.rows() {
        let name: String = row.get("name")?;
        // `_vid` materializes as either an i64-shaped value or a `Vid`;
        // both implement `TryFrom<&Value>` for `Vid`, which is the
        // canonical accessor.
        let vid = row.get::<uni_db::core::id::Vid>("vid")?.as_u64() as i64;
        match name.as_str() {
            "A" => vid_a = Some(vid),
            "B" => vid_b = Some(vid),
            _ => {}
        }
    }
    Ok((
        db,
        vid_a.expect("vid for A must be captured"),
        vid_b.expect("vid for B must be captured"),
    ))
}

#[tokio::test]
async fn plugin_handle_dispatched_when_registered() -> anyhow::Result<()> {
    let (db, _vid_a, vid_b) = seed_db().await?;

    // Register a handle that returns only B; the native top-K against
    // `[1.0, 0.0]` would return A *and* B.
    let handle = Arc::new(CountingHandle::new(vec![(vid_b, 0.05)]));
    let probe_counter = Arc::clone(&handle.probe_count);
    db.plugin_registry().register_index_handle(
        "idx_item_embed",
        IndexKind::new("test_counting"),
        handle.clone() as Arc<dyn IndexHandle>,
    );

    let res = db
        .session()
        .query(
            "MATCH (n:Item) \
             WHERE n.embedding ~= [1.0, 0.0] \
             RETURN n.name AS name",
        )
        .await?;

    assert_eq!(
        probe_counter.load(Ordering::SeqCst),
        1,
        "plugin IndexHandle::probe must be invoked exactly once when registered"
    );
    assert_eq!(
        res.len(),
        1,
        "plugin handle returned a single row; native L2 would have returned both"
    );
    let name: String = res.rows()[0].get("name")?;
    assert_eq!(
        name, "B",
        "the returned row must be the one the plugin emitted"
    );

    Ok(())
}

#[tokio::test]
async fn no_plugin_handle_falls_through_to_native_path() -> anyhow::Result<()> {
    let (db, _vid_a, _vid_b) = seed_db().await?;

    // Do NOT register a handle for `idx_item_embed`. The native vector
    // backend should answer the probe and return both rows (top-K of 2).
    let res = db
        .session()
        .query(
            "MATCH (n:Item) \
             WHERE n.embedding ~= [1.0, 0.0] \
             RETURN n.name AS name ORDER BY name",
        )
        .await?;

    assert_eq!(res.len(), 2, "native top-K should return both Items");

    Ok(())
}
