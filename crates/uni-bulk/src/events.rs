// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Trigger and CDC events for a bulk load.
//!
//! A load writes straight to storage, never through a transaction buffer, so
//! the commit path's event extraction cannot see it. Instead, once the load is
//! visible, the writer hands a [`VisibleLoad`] to the database's
//! [`BulkEventSink`], which announces the load and reads its rows back from
//! storage — every row of a load carries the load's one `_version` — in chunks
//! of at most [`BULK_EVENT_CHUNK_ROWS`], turning each chunk into create events
//! for after-phase triggers and CDC. A load's events are therefore never held
//! in memory all at once.
//!
//! A synchronous before-trigger could veto a transaction, but a load is
//! already visible when its events exist, so the writer asks the sink up front
//! ([`BulkEventSink::blocking_trigger`]) and refuses a load such a trigger
//! would see, unless the caller opted out
//! ([`crate::BulkWriterBuilder::skip_before_triggers`]).

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use arrow_array::{
    Array, BooleanArray, LargeBinaryArray, ListArray, RecordBatch, StringArray, UInt64Array,
};
use futures::StreamExt;
use uni_common::Properties;
use uni_common::core::id::{Eid, Vid};
use uni_plugin_host::triggers::{CreatedEdge, CreatedVertex, MutationEvents};
use uni_store::backend::table_names;
use uni_store::backend::traits::RecordBatchStream;
use uni_store::backend::types::{CmpOp, FilterExpr, Scalar, ScanRequest};
use uni_store::storage::manager::StorageManager;

/// The most rows a chunk of a load's events holds.
pub const BULK_EVENT_CHUNK_ROWS: usize = 4096;

/// What a load writes, for [`BulkEventSink::blocking_trigger`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BulkWriteKind {
    /// Vertices under a label.
    Vertex,
    /// Edges of an edge type.
    Edge,
}

/// Receives the events of the database's bulk loads.
///
/// Implemented by the database driver, which owns the trigger registry, the
/// commit broadcaster and the CDC runtime; see the module documentation.
pub trait BulkEventSink: Send + Sync {
    /// The name of a synchronous `BeforeMutation` / `BeforeCommit` trigger
    /// that would see a load's `kind` rows under `label_or_type`, if any.
    fn blocking_trigger(&self, kind: BulkWriteKind, label_or_type: &str) -> Option<String>;

    /// Announce a load that just became visible, and deliver its events.
    ///
    /// Infallible: the load is committed and visible whatever happens here;
    /// a failure to deliver is the sink's to report. Returns
    /// [`EventDelivery::Interrupted`] when the database shut down (or went
    /// away) before every event was delivered, in which case the writer keeps
    /// the load's commit marker so the next open delivers them again.
    ///
    /// The writer runs this on a task of its own, so dropping the future of
    /// `BulkWriter::commit` does not stop it.
    fn publish(
        &self,
        load: VisibleLoad,
    ) -> Pin<Box<dyn Future<Output = EventDelivery> + Send + '_>>;
}

/// Whether [`BulkEventSink::publish`] delivered a load's events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventDelivery {
    /// The load was announced and every event handed on (or the database had
    /// no consumer for them). A failure the sink logged, such as reading the
    /// load's rows back, still ends here: delivering again would fail the
    /// same way.
    Finished,
    /// The database shut down, or was dropped, before every event was
    /// delivered. The load's events are to be delivered again at the next
    /// open; consumers may then see some of them twice.
    Interrupted,
}

/// A bulk load that just became visible, handed to
/// [`BulkEventSink::publish`].
pub struct VisibleLoad {
    /// The load's id.
    pub load_id: String,
    /// The `_version` every row of the load carries.
    pub write_version: u64,
    /// Labels the load wrote vertices under.
    pub labels: Vec<String>,
    /// Edge types the load wrote edges of.
    pub edge_types: Vec<String>,
    /// Rows the load wrote (vertices plus edges).
    pub rows: usize,
    /// Reads the load's rows back as events.
    pub reader: LoadEventReader,
}

impl std::fmt::Debug for VisibleLoad {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VisibleLoad")
            .field("load_id", &self.load_id)
            .field("write_version", &self.write_version)
            .field("labels", &self.labels)
            .field("edge_types", &self.edge_types)
            .field("rows", &self.rows)
            .finish_non_exhaustive()
    }
}

/// Which table [`LoadEventReader`] reads next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    Vertices,
    Edges,
    Done,
}

/// Streams a visible load's rows back from storage as create events, at most
/// [`BULK_EVENT_CHUNK_ROWS`] at a time.
///
/// Reads the main vertex and edge tables, which hold every row of a load
/// (labels and edge type included) under the load's `_version`; no other
/// write uses that version, so the filter selects exactly the load.
pub struct LoadEventReader {
    storage: Arc<StorageManager>,
    write_version: u64,
    phase: Phase,
    has_vertices: bool,
    has_edges: bool,
    stream: Option<RecordBatchStream>,
    /// A scanned batch not yet fully emitted, and the offset of its next row.
    pending: Option<(RecordBatch, usize)>,
}

impl LoadEventReader {
    /// A reader over the rows `storage` holds at `write_version`.
    #[must_use]
    pub fn new(
        storage: Arc<StorageManager>,
        write_version: u64,
        has_vertices: bool,
        has_edges: bool,
    ) -> Self {
        Self {
            storage,
            write_version,
            phase: Phase::Vertices,
            has_vertices,
            has_edges,
            stream: None,
            pending: None,
        }
    }

    /// The next chunk of events, `None` once every row was read.
    ///
    /// Properties are decoded only when `properties_referenced` (the keys some
    /// trigger predicate reads) is non-empty, and only those keys are kept.
    ///
    /// # Errors
    ///
    /// Returns an error if a table cannot be scanned or a row decoded. The
    /// reader is then spent: the remaining rows are not delivered.
    pub async fn next_chunk(
        &mut self,
        properties_referenced: &HashSet<String>,
    ) -> Result<Option<MutationEvents>> {
        loop {
            if let Some((batch, offset)) = self.pending.take() {
                let len = (batch.num_rows() - offset).min(BULK_EVENT_CHUNK_ROWS);
                let chunk = batch.slice(offset, len);
                if offset + len < batch.num_rows() {
                    self.pending = Some((batch, offset + len));
                }
                let events = match self.phase {
                    Phase::Vertices => vertex_events(&chunk, properties_referenced)?,
                    Phase::Edges => edge_events(&chunk, properties_referenced)?,
                    Phase::Done => unreachable!("no batch is pending once done"),
                };
                if events.is_empty() {
                    continue;
                }
                return Ok(Some(events));
            }
            if let Some(stream) = self.stream.as_mut() {
                match stream.next().await {
                    Some(batch) => {
                        let batch = batch?;
                        if batch.num_rows() > 0 {
                            self.pending = Some((batch, 0));
                        }
                        continue;
                    }
                    None => {
                        self.stream = None;
                        self.phase = match self.phase {
                            Phase::Vertices => Phase::Edges,
                            Phase::Edges | Phase::Done => Phase::Done,
                        };
                    }
                }
            }
            let (table, wanted, columns) = match self.phase {
                Phase::Vertices => (
                    table_names::main_vertex_table_name(),
                    self.has_vertices,
                    ["_vid", "labels", "props_json", "_deleted"],
                ),
                Phase::Edges => (
                    table_names::main_edge_table_name(),
                    self.has_edges,
                    ["_eid", "type", "props_json", "_deleted"],
                ),
                Phase::Done => return Ok(None),
            };
            if !wanted {
                self.phase = match self.phase {
                    Phase::Vertices => Phase::Edges,
                    Phase::Edges | Phase::Done => Phase::Done,
                };
                continue;
            }
            let request = ScanRequest::all(table)
                .with_columns(columns.iter().map(|c| (*c).to_owned()).collect())
                .with_filter(FilterExpr::Compare {
                    column: "_version".to_owned(),
                    op: CmpOp::Eq,
                    value: Scalar::UInt(self.write_version),
                });
            self.stream = Some(self.storage.backend().scan_stream(request).await?);
        }
    }
}

/// Decode row `i` of a `props_json` column, or `None` when properties are
/// not wanted or the cell is null.
fn props_at(
    col: &LargeBinaryArray,
    i: usize,
    properties_referenced: &HashSet<String>,
) -> Result<Option<Properties>> {
    if properties_referenced.is_empty() || col.is_null(i) {
        return Ok(None);
    }
    match uni_common::cypher_value_codec::decode(col.value(i))
        .map_err(|e| anyhow!("bulk load events: props_json does not decode: {e}"))?
    {
        uni_common::Value::Map(map) => Ok(Some(map)),
        other => Err(anyhow!(
            "bulk load events: props_json is not a map: {other:?}"
        )),
    }
}

fn column<'a, T: 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<T>())
        .ok_or_else(|| anyhow!("bulk load events: column `{name}` missing or mistyped"))
}

fn vertex_events(
    batch: &RecordBatch,
    properties_referenced: &HashSet<String>,
) -> Result<MutationEvents> {
    let vids = column::<UInt64Array>(batch, "_vid")?;
    let labels = column::<ListArray>(batch, "labels")?;
    let props = column::<LargeBinaryArray>(batch, "props_json")?;
    let deleted = column::<BooleanArray>(batch, "_deleted")?;
    let mut vertices = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        if deleted.value(i) {
            continue;
        }
        let row_labels = if labels.is_null(i) {
            Vec::new()
        } else {
            let values = labels.value(i);
            let strings = values
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("bulk load events: labels are not strings"))?;
            strings.iter().flatten().map(str::to_owned).collect()
        };
        vertices.push(CreatedVertex {
            vid: Vid::new(vids.value(i)),
            labels: row_labels,
            properties: props_at(props, i, properties_referenced)?,
        });
    }
    Ok(MutationEvents::creates(vertices, [], properties_referenced))
}

fn edge_events(
    batch: &RecordBatch,
    properties_referenced: &HashSet<String>,
) -> Result<MutationEvents> {
    let eids = column::<UInt64Array>(batch, "_eid")?;
    let types = column::<StringArray>(batch, "type")?;
    let props = column::<LargeBinaryArray>(batch, "props_json")?;
    let deleted = column::<BooleanArray>(batch, "_deleted")?;
    let mut edges = Vec::with_capacity(batch.num_rows());
    for i in 0..batch.num_rows() {
        if deleted.value(i) {
            continue;
        }
        edges.push(CreatedEdge {
            eid: Eid::new(eids.value(i)),
            edge_type: types.value(i).to_owned(),
            properties: props_at(props, i, properties_referenced)?,
        });
    }
    Ok(MutationEvents::creates([], edges, properties_referenced))
}
