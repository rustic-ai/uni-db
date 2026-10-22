// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use crate::runtime::wal::{Mutation, WriteAheadLog};
use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{instrument, trace};
use uni_common::core::id::{Eid, Vid};
use uni_common::graph::simple_graph::{Direction, SimpleGraph};
use uni_common::{Properties, Value};
use uni_crdt::Crdt;

/// Items a read-write transaction observed during execution, used for SSI
/// read-write antidependency detection at commit.
///
/// Shared (via `Arc<Mutex<_>>`) between the read path — which records reads
/// through the transaction's `QueryContext` — and the commit path, which checks
/// it against concurrently-committed write-sets. Item-level granularity;
/// phantoms are out of scope (handled by the `FOR UPDATE` escape hatch).
#[derive(Debug, Default)]
pub struct OccReadSet {
    /// Vertices the transaction read.
    pub vertices: HashSet<Vid>,
    /// Edges the transaction read.
    pub edges: HashSet<Eid>,
}

impl OccReadSet {
    /// `true` when nothing has been read yet. Used to decide whether a `FOR
    /// UPDATE` acquisition may safely re-pin a still-fresh transaction.
    pub fn is_empty(&self) -> bool {
        self.vertices.is_empty() && self.edges.is_empty()
    }
}

/// Returns the current timestamp in nanoseconds since Unix epoch.
fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Returns the [`Crdt`] a property value encodes, or `None` if it is not one.
///
/// `Crdt` is `#[serde(tag = "t", content = "d")]`, so it deserializes only from a
/// JSON object, and only `Value::Map(_)` produces one — gating on `Map` avoids
/// allocating a JSON tree for large non-map values (e.g. embedding columns).
///
/// This is the single source of truth for "is this value CRDT-mergeable": both
/// the commit-time merge ([`L0Buffer::merge_crdt_properties`]) and the OCC
/// write-set carve-out ([`crate::runtime::occ::WriteSet::from_l0`]) consult it,
/// so the carve-out can never exclude an item the merge would actually overwrite
/// (which would silently lose an update).
pub(crate) fn try_as_crdt(v: &Value) -> Option<Crdt> {
    if !matches!(v, Value::Map(_)) {
        return None;
    }
    serde_json::from_value::<Crdt>(v.clone().into()).ok()
}

/// Serialize a constraint key for O(1) uniqueness checks.
/// Format: label + separator + sorted (prop_name, value) pairs.
pub fn serialize_constraint_key(label: &str, key_values: &[(String, Value)]) -> Vec<u8> {
    let mut buf = label.as_bytes().to_vec();
    buf.push(0); // separator
    let mut sorted = key_values.to_vec();
    sorted.sort_by(|a, b| a.0.cmp(&b.0));
    for (k, v) in &sorted {
        buf.extend(k.as_bytes());
        buf.push(0);
        // Use serde_json serialization for deterministic value encoding
        buf.extend(serde_json::to_vec(v).unwrap_or_default());
        buf.push(0);
    }
    buf
}

/// The constraint a [`serialize_constraint_key`] key belongs to: its label and
/// property names, without the values.
fn constraint_key_shape(key: &[u8]) -> Vec<u8> {
    // `label \0 (name \0 json-value \0)*`; JSON escapes control characters,
    // so a value never contains a raw NUL.
    key.split(|b| *b == 0)
        .enumerate()
        .filter(|(i, _)| *i == 0 || i % 2 == 1)
        .flat_map(|(_, part)| part.iter().copied().chain(std::iter::once(0)))
        .collect()
}

/// Per-type mutation counters accumulated by the L0 buffer.
///
/// Used to provide detailed mutation statistics (e.g., `nodes_created`,
/// `relationships_deleted`) on `ExecuteResult`. Callers snapshot before/after
/// execution and call [`diff()`](MutationStats::diff) to get the delta.
#[derive(Debug, Clone, Default)]
pub struct MutationStats {
    pub nodes_created: usize,
    pub nodes_deleted: usize,
    pub relationships_created: usize,
    pub relationships_deleted: usize,
    pub properties_set: usize,
    pub properties_removed: usize,
    pub labels_added: usize,
    pub labels_removed: usize,
}

impl MutationStats {
    /// Compute the field-wise difference `self - before`.
    pub fn diff(&self, before: &Self) -> Self {
        Self {
            nodes_created: self.nodes_created.saturating_sub(before.nodes_created),
            nodes_deleted: self.nodes_deleted.saturating_sub(before.nodes_deleted),
            relationships_created: self
                .relationships_created
                .saturating_sub(before.relationships_created),
            relationships_deleted: self
                .relationships_deleted
                .saturating_sub(before.relationships_deleted),
            properties_set: self.properties_set.saturating_sub(before.properties_set),
            properties_removed: self
                .properties_removed
                .saturating_sub(before.properties_removed),
            labels_added: self.labels_added.saturating_sub(before.labels_added),
            labels_removed: self.labels_removed.saturating_sub(before.labels_removed),
        }
    }
}

/// The entity a property side effect belongs to; see
/// [`L0Buffer::record_property_effect`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatEntity {
    /// A vertex.
    Vertex(Vid),
    /// An edge.
    Edge(Eid),
}

/// A statement (or bulk batch) running against a transaction's L0 buffer.
///
/// Opens a scope on creation and closes it on drop, including when the
/// statement's future is dropped by a timeout or a cancellation. Within it
/// the write throttle is charged once, by the first write
/// (`Writer::check_write_pressure`), rather than once per row, and the
/// entities the statement creates are remembered for its side-effect counts.
#[must_use = "the scope closes when this guard drops"]
pub struct StatementScope {
    l0: Arc<parking_lot::RwLock<L0Buffer>>,
}

impl StatementScope {
    /// Opens a statement scope on `l0`: one throttle charge, and the
    /// entities the statement creates are remembered for its side effects.
    pub fn begin(l0: Arc<parking_lot::RwLock<L0Buffer>>) -> Self {
        l0.write().begin_statement(true);
        Self { l0 }
    }

    /// Opens a bulk-batch scope on `l0`: one throttle charge for the batch.
    /// A batch reports no side effects, so it does not track what it creates.
    pub fn begin_batch(l0: Arc<parking_lot::RwLock<L0Buffer>>) -> Self {
        l0.write().begin_statement(false);
        Self { l0 }
    }
}

impl Drop for StatementScope {
    fn drop(&mut self) {
        self.l0.write().end_statement();
    }
}

impl std::fmt::Debug for StatementScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatementScope").finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct TombstoneEntry {
    pub eid: Eid,
    pub src_vid: Vid,
    pub dst_vid: Vid,
    pub edge_type: u32,
}

/// Edge identities already resolved for `Writer::update_edge`'s check that
/// a property write does not move an edge.
///
/// An edge id is never reused and an edge's endpoints and type never change,
/// so an identity once read stays true. Reading one from the adjacency
/// returns every edge of a `(source, type)` at once; memoizing them all makes
/// a `SET` over a hub's edges one adjacency read rather than one per edge,
/// which was quadratic in the hub's degree.
#[derive(Debug, Default, Clone)]
pub struct EdgeIdentityMemo {
    /// `eid -> (src, dst, type id)`.
    pub identities: HashMap<Eid, (Vid, Vid, u32)>,
    /// `(src, type id)` pairs whose outgoing adjacency is in `identities`.
    pub scanned: HashSet<(Vid, u32)>,
    /// Per type id, how many identities had to be read from storage because
    /// the type's adjacency was not loaded.
    pub storage_reads: HashMap<u32, usize>,
    /// Bytes of the memo charged to the owning buffer's `estimated_size`
    /// ([`Self::charge`]). The memo is never merged or copied with its
    /// buffer, so a merge or a fork leaves this share out.
    pub charged_bytes: usize,
}

impl EdgeIdentityMemo {
    /// Estimated bytes per memoized identity: the `Eid` key, the
    /// `(Vid, Vid, u32)` value and the map's per-entry overhead.
    pub const IDENTITY_BYTES: usize = 40;
    /// Estimated bytes per scanned `(Vid, u32)` pair, overhead included.
    pub const SCANNED_BYTES: usize = 24;

    /// The estimated bytes that growing the memo from `identities` and
    /// `scanned` entries to its current size added; recorded in
    /// [`Self::charged_bytes`]. The caller adds it to the buffer's
    /// `estimated_size`.
    pub fn charge(&mut self, identities: usize, scanned: usize) -> usize {
        let added = self.identities.len().saturating_sub(identities) * Self::IDENTITY_BYTES
            + self.scanned.len().saturating_sub(scanned) * Self::SCANNED_BYTES;
        self.charged_bytes += added;
        added
    }
}

pub struct L0Buffer {
    /// Graph topology using simple adjacency lists
    pub graph: SimpleGraph,
    /// Soft-deleted edges (tombstones for LSM-style merging)
    pub tombstones: HashMap<Eid, TombstoneEntry>,
    /// Soft-deleted vertices
    pub vertex_tombstones: HashSet<Vid>,
    /// Edge version tracking for MVCC
    pub edge_versions: HashMap<Eid, u64>,
    /// Vertex version tracking for MVCC
    pub vertex_versions: HashMap<Vid, u64>,
    /// Edge properties (stored separately from topology)
    pub edge_properties: HashMap<Eid, Properties>,
    /// Vertex properties (stored separately from topology)
    pub vertex_properties: HashMap<Vid, Properties>,
    /// Edge endpoint lookup: eid -> (src, dst, type)
    pub edge_endpoints: HashMap<Eid, (Vid, Vid, u32)>,
    /// Vertex labels (VID -> list of label names)
    /// New in storage design: vertices can have multiple labels
    pub vertex_labels: HashMap<Vid, Vec<String>>,
    /// Reverse index: label name → set of VIDs with that label. Maintained
    /// alongside `vertex_labels` for O(1) label-based vertex lookups.
    pub label_to_vids: HashMap<String, HashSet<Vid>>,
    /// Vids whose FULL label set was explicitly replaced by a label mutation
    /// (`SET n:Label` / `REMOVE n:Label`) in this buffer, via
    /// [`L0Buffer::set_vertex_labels`]. Distinguishes a deliberate label
    /// replacement from the empty `vertex_labels` entry a property-only write
    /// incidentally creates (`entry().or_default()`), so `merge` knows to REPLACE
    /// (not append) these vids' labels and `WriteSet::from_l0` knows they are
    /// conflictable writes. A transaction-buffer concept; empty on main L0.
    pub vertex_label_overwrites: HashSet<Vid>,
    /// Version of each vid's latest label mutation in this buffer.
    ///
    /// Kept apart from `vertex_versions` because the L0 read paths take a
    /// `vertex_versions` entry to mean that this buffer holds the vertex's
    /// newest *properties*; a relabel holds none, and stamping it there made
    /// the scans drop the flushed properties. Read only by the flush, which
    /// stamps a relabeled vertex's re-appended row with it (see
    /// [`L0Buffer::vertex_write_version`]).
    pub vertex_label_versions: HashMap<Vid, u64>,
    /// Edge types (EID -> type name)
    pub edge_types: HashMap<Eid, String>,
    /// Current version counter
    pub current_version: u64,
    /// Mutation count for flush decisions
    pub mutation_count: usize,
    /// Per-type mutation counters for detailed statistics.
    pub mutation_stats: MutationStats,
    /// Write-ahead log for durability
    pub wal: Option<Arc<WriteAheadLog>>,
    /// WAL LSN at the time this L0 was rotated for flush.
    /// Used to ensure WAL truncation doesn't remove entries needed by pending flushes.
    pub wal_lsn_at_flush: u64,
    /// WAL LSN at the time this L0 became active (the previous rotation point).
    ///
    /// Everything at or below this LSN is durable in L1 before this buffer's own
    /// data begins; while the buffer is pending flush its committed WAL entries
    /// live strictly ABOVE it. It is therefore the floor below which WAL
    /// truncation and a published `wal_high_water_mark` may safely advance —
    /// using `wal_lsn_at_flush` (the high watermark) there would discard a
    /// pending buffer's own not-yet-flushed entries (lost-commit on a graceful
    /// close after a failed flush).
    pub wal_lsn_at_start: u64,
    /// Bulk-load versions at or below `current_version` that were not
    /// visible when this buffer was rotated for flush. The flush's manifest
    /// records them, so a reader pinned to it never sees those loads
    /// ([`crate::runtime::l0_manager::L0Manager::reserve_bulk_version`]).
    pub unpublished_bulk_at_rotation: Vec<u64>,
    /// Vertex creation timestamps (nanoseconds since epoch)
    pub vertex_created_at: HashMap<Vid, i64>,
    /// Vertex update timestamps (nanoseconds since epoch)
    pub vertex_updated_at: HashMap<Vid, i64>,
    /// Edge creation timestamps (nanoseconds since epoch)
    pub edge_created_at: HashMap<Eid, i64>,
    /// Edge update timestamps (nanoseconds since epoch)
    pub edge_updated_at: HashMap<Eid, i64>,
    /// Estimated size in bytes for memory limit enforcement.
    /// Incremented O(1) on each mutation to avoid O(V+E) size_bytes() calls.
    pub estimated_size: usize,
    /// Per-constraint index for O(1) unique key checks.
    /// Key: constraint composite key (label + sorted property values serialized).
    /// Value: Vid that owns this key.
    pub constraint_index: HashMap<Vec<u8>, Vid>,
    /// Reverse of [`constraint_index`](Self::constraint_index): the keys each
    /// vertex holds. Lets [`insert_constraint_key`](Self::insert_constraint_key)
    /// retire a vertex's previous key for the same constraint when a `SET`
    /// moves it; without it the old key stayed indexed to the vertex and was
    /// reported as taken — at write time and again at commit.
    pub constraint_keys_by_vid: HashMap<Vid, Vec<Vec<u8>>>,
    /// Implicit MERGE-key guard for phantom-free `MERGE` *without* a declared
    /// `UNIQUE` constraint. Same key format as `constraint_index` (built by
    /// [`serialize_constraint_key`]), but populated only by a `MERGE` that
    /// *creates* a node, and re-probed at commit only against other concurrent
    /// `MERGE`-creates — so two concurrent `MERGE`s of the same key converge to
    /// one node (the loser aborts retriably) instead of silently duplicating,
    /// while a plain `CREATE` of the same properties is unaffected (it never
    /// registers a key). Tombstoned with the owning vid; transient (not rebuilt
    /// on recovery).
    pub merge_guard_index: HashMap<Vec<u8>, Vid>,
    /// Per-edge-constraint index for O(1) unique edge-key checks. Mirrors
    /// [`constraint_index`](Self::constraint_index) but keyed to a live edge's
    /// `Eid` (built by [`serialize_constraint_key`] with the edge-type name as the
    /// discriminator). Populated by the edge-insert path for declared edge-type
    /// `Unique`/`NodeKey` constraints, tombstoned in `apply_edge_deletion`, and
    /// rebuilt from live edge properties on recovery.
    pub edge_constraint_index: HashMap<Vec<u8>, Eid>,
    /// Reverse index `ext_id` → owning vid for O(1) global ext_id uniqueness
    /// checks (`Writer::check_extid_globally_unique` previously scanned every
    /// `vertex_properties` map per insert — O(n²) ingest). Maintained by the
    /// vertex insert impls (synced to the post-CRDT-merge value) and by
    /// `apply_vertex_deletion`, so merge and WAL replay keep it consistent
    /// for free.
    pub extid_index: HashMap<String, Vid>,
    /// Nesting depth of the statements (or bulk batches) running against
    /// this buffer; zero outside one. See [`StatementScope`].
    statement_depth: usize,
    /// Whether the running statement has already paid the write throttle,
    /// so its later writes do not pay it again.
    statement_write_admitted: bool,
    /// Vertices the running statement created, for its side-effect counts.
    statement_created_vertices: HashSet<Vid>,
    /// Edges the running statement created, for its side-effect counts.
    statement_created_edges: HashSet<Eid>,
    /// Whether the running scope records what it creates (a statement does;
    /// a bulk batch, which reports no side effects, does not).
    statement_tracks_created: bool,
    /// Edges created in this buffer through [`Self::insert_edge`]: each got
    /// a fresh id from the allocator, so none has committed state anywhere
    /// else. Lets the commit-time trigger/CDC pre-image probe skip them.
    pub created_edges: HashSet<Eid>,
    /// Vertices created in this buffer through
    /// [`Self::insert_vertex_with_labels`]: each got a fresh id from the
    /// allocator, so none has committed state anywhere else. Lets the
    /// commit-time trigger/CDC pre-image probe skip them, as for
    /// `created_edges`.
    pub created_vertices: HashSet<Vid>,
    /// Stored edge identities `Writer::update_edge` has already confirmed
    /// for writes into this buffer; transient, never merged or logged.
    pub edge_identity_memo: EdgeIdentityMemo,
    /// Phase B (UniConfig::defer_embeddings): VIDs whose auto-embedding
    /// was skipped at insert time and is owed at flush. Value = primary
    /// label name (the rest of the embedding config is looked up from the
    /// schema at flush time). Drained by `flush_stream_l1` before column
    /// extraction; entries are removed when the embedding lands in the
    /// vertex's L0 property map.
    pub pending_embeddings: HashMap<Vid, String>,
    /// Optimistic-concurrency read sequence (SSI). Stamped on a transaction's
    /// private L0 at creation with the Writer's commit-sequence at that moment,
    /// and consulted at commit to detect intervening conflicting commits. `0`
    /// for the main L0 and when SSI is disabled.
    pub occ_read_seq: u64,
    /// Optimistic-concurrency read-set (SSI). `Some` on a read-write
    /// transaction's private L0 when SSI tracking is active; the read path
    /// records observed ids here and commit checks them for antidependencies.
    /// `None` for the main L0 and read-only / SSI-disabled paths.
    pub occ_read_set: Option<Arc<parking_lot::Mutex<OccReadSet>>>,
    /// Optional plugin registry for registry-dispatched CRDT merges at
    /// commit-time property merge (`merge_crdt_properties`).
    ///
    /// Behavior-preserving when absent: falls back to native
    /// [`uni_crdt::Crdt::try_merge`] bit-for-bit when no provider is
    /// registered. Stamped onto every live buffer by the owning `L0Manager`.
    /// Not part of buffer identity — cloned buffers (forked ASSUME/ABDUCE L0s)
    /// inherit it, but it is never serialized or flushed.
    pub plugin_registry: Option<Arc<uni_plugin::PluginRegistry>>,
}

impl std::fmt::Debug for L0Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("L0Buffer")
            .field("vertex_count", &self.graph.vertex_count())
            .field("edge_count", &self.graph.edge_count())
            .field("tombstones", &self.tombstones.len())
            .field("vertex_tombstones", &self.vertex_tombstones.len())
            .field("current_version", &self.current_version)
            .field("mutation_count", &self.mutation_count)
            .finish()
    }
}

impl Clone for L0Buffer {
    /// Clone the L0 buffer for fork/restore (ASSUME/ABDUCE).
    ///
    /// The cloned buffer does NOT share the WAL reference — forked L0s are
    /// ephemeral and should not write to the WAL.
    fn clone(&self) -> Self {
        Self {
            graph: self.graph.clone(),
            tombstones: self.tombstones.clone(),
            vertex_tombstones: self.vertex_tombstones.clone(),
            edge_versions: self.edge_versions.clone(),
            vertex_versions: self.vertex_versions.clone(),
            edge_properties: self.edge_properties.clone(),
            vertex_properties: self.vertex_properties.clone(),
            edge_endpoints: self.edge_endpoints.clone(),
            vertex_labels: self.vertex_labels.clone(),
            label_to_vids: self.label_to_vids.clone(),
            vertex_label_overwrites: self.vertex_label_overwrites.clone(),
            vertex_label_versions: self.vertex_label_versions.clone(),
            edge_types: self.edge_types.clone(),
            current_version: self.current_version,
            mutation_count: self.mutation_count,
            mutation_stats: self.mutation_stats.clone(),
            wal: None, // Forked L0s don't share the WAL
            wal_lsn_at_flush: self.wal_lsn_at_flush,
            wal_lsn_at_start: self.wal_lsn_at_start,
            unpublished_bulk_at_rotation: self.unpublished_bulk_at_rotation.clone(),
            vertex_created_at: self.vertex_created_at.clone(),
            vertex_updated_at: self.vertex_updated_at.clone(),
            edge_created_at: self.edge_created_at.clone(),
            edge_updated_at: self.edge_updated_at.clone(),
            // The memo is not copied (below), so neither is its charge.
            estimated_size: self
                .estimated_size
                .saturating_sub(self.edge_identity_memo.charged_bytes),
            constraint_index: self.constraint_index.clone(),
            constraint_keys_by_vid: self.constraint_keys_by_vid.clone(),
            edge_constraint_index: self.edge_constraint_index.clone(),
            merge_guard_index: self.merge_guard_index.clone(),
            extid_index: self.extid_index.clone(),
            // A statement scope belongs to the buffer it was opened on.
            statement_depth: 0,
            statement_write_admitted: false,
            statement_created_vertices: HashSet::new(),
            statement_created_edges: HashSet::new(),
            statement_tracks_created: false,
            created_edges: self.created_edges.clone(),
            created_vertices: self.created_vertices.clone(),
            edge_identity_memo: EdgeIdentityMemo::default(),
            pending_embeddings: self.pending_embeddings.clone(),
            occ_read_seq: self.occ_read_seq,
            // Forked L0s (ASSUME/ABDUCE) do not participate in OCC tracking.
            occ_read_set: None,
            // Inherit the registry so forked buffers merge custom CRDTs the
            // same way the parent does.
            plugin_registry: self.plugin_registry.clone(),
        }
    }
}

impl L0Buffer {
    /// Append labels to a vec, skipping duplicates.
    fn append_unique_labels(existing: &mut Vec<String>, labels: &[String]) {
        for label in labels {
            if !existing.contains(label) {
                existing.push(label.clone());
            }
        }
    }

    /// Add a VID to the reverse label index for each of the given labels.
    fn index_labels_for_vid(&mut self, vid: Vid, labels: &[String]) {
        for label in labels {
            self.label_to_vids
                .entry(label.clone())
                .or_default()
                .insert(vid);
        }
    }

    /// Read a vertex's current string `ext_id` from a property map.
    fn extid_of(props: &Properties) -> Option<String> {
        props
            .get("ext_id")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
    }

    /// Sync `extid_index` for `vid` around a property write.
    ///
    /// `old` / `new` are the vertex's ext_id before / after the CRDT merge —
    /// the index always reflects the post-merge value, matching what the old
    /// full scan in `Writer::check_extid_globally_unique` observed. Only
    /// called when the incoming properties contain an `ext_id` key (the merge
    /// cannot change the value otherwise).
    fn sync_extid_index(&mut self, vid: Vid, old: Option<String>, new: Option<String>) {
        if old == new {
            return;
        }
        if let Some(old) = old
            && self.extid_index.get(&old) == Some(&vid)
        {
            self.extid_index.remove(&old);
        }
        if let Some(new) = new {
            self.extid_index.insert(new, vid);
        }
    }

    /// Remove a VID from all label entries in the reverse index.
    fn remove_vid_from_label_index(&mut self, vid: Vid) {
        if let Some(labels) = self.vertex_labels.get(&vid) {
            for label in labels {
                if let Some(set) = self.label_to_vids.get_mut(label) {
                    set.remove(&vid);
                }
            }
        }
    }

    /// Replaces a vertex's FULL label set — the semantics of `SET n:Label` /
    /// `REMOVE n:Label`, which resolve the new complete set before writing.
    ///
    /// Unlike [`add_vertex_labels`](Self::add_vertex_labels) (append), this clears
    /// the vid's existing labels from the reverse index, sets the new set, and
    /// re-indexes — so a removal actually removes. It marks the vid in
    /// `vertex_label_overwrites` so `merge` REPLACES (not appends) these labels at
    /// commit and `WriteSet::from_l0` treats the change as a conflictable write.
    /// Increments `mutation_count` (a label change is a real mutation; its sibling
    /// `remove_vertex_label` already does so).
    pub fn set_vertex_labels(&mut self, vid: Vid, labels: &[String]) {
        self.remove_vid_from_label_index(vid);
        self.vertex_labels.insert(vid, labels.to_vec());
        self.index_labels_for_vid(vid, labels);
        self.vertex_label_overwrites.insert(vid);
        self.current_version += 1;
        // Without a version the flush re-appended the relabeled row at version
        // 0, and the vertex's older row, which still carried the old labels,
        // won every latest-version read: the flush undid the relabel.
        self.vertex_label_versions.insert(vid, self.current_version);
        self.mutation_count += 1;
    }

    /// Version of `vid`'s latest write in this buffer, properties or labels.
    ///
    /// What the flush stamps on the vertex's rows. A relabel after a property
    /// write in the same buffer is the later write, and a pure relabel has no
    /// `vertex_versions` entry at all.
    pub fn vertex_write_version(&self, vid: Vid) -> u64 {
        let props = self.vertex_versions.get(&vid).copied().unwrap_or(0);
        let labels = self.vertex_label_versions.get(&vid).copied().unwrap_or(0);
        props.max(labels)
    }

    /// Merge CRDT properties into an existing property map.
    /// Attempts CRDT merge if both values are valid CRDTs, falls back to overwrite.
    ///
    /// When the entry is empty (new vertex insert), skips the expensive JSON
    /// round-trip and directly assigns the properties.
    ///
    /// Logs a warning when a CRDT value is overwritten by a non-CRDT scalar
    /// (limitation R1).
    fn merge_crdt_properties(
        entry: &mut Properties,
        properties: Properties,
        registry: Option<&Arc<uni_plugin::PluginRegistry>>,
    ) {
        // Fast path: new vertex with no existing properties — skip JSON round-trip
        if entry.is_empty() {
            *entry = properties;
            return;
        }

        for (k, v) in properties {
            // `try_as_crdt` performs the Map-gated CRDT probe (see its docs for the
            // wide-row perf rationale). Sharing it with `WriteSet::from_l0` keeps the
            // OCC carve-out consistent with this merge-versus-overwrite decision.
            if let Some(mut new_crdt) = try_as_crdt(&v)
                && let Some(existing_v) = entry.get(&k)
                && let Ok(existing_crdt) = serde_json::from_value::<Crdt>(existing_v.clone().into())
            {
                // Use a fallible merge to avoid panic on type mismatch.
                // Operand order: self=new (new_crdt), other=existing
                // (existing_crdt) — existing-into-new. Preserve — a custom
                // provider's merge may be non-commutative.
                let merged = match registry {
                    // #233: `.is_ok()` collapsed a PROVIDER failure
                    // (`from_persisted` / `merge` / `persist` on a registered
                    // `CrdtKindProvider`) into the same `false` as a genuine
                    // variant mismatch, so the fall-through below warned
                    // "overwriting CRDT property with a different CRDT variant"
                    // — naming the wrong cause — and discarded merged state. The
                    // two are distinguishable: only `TypeMismatch` is a variant
                    // mismatch.
                    Some(reg) => match new_crdt.merge_via_registry(&existing_crdt, reg) {
                        Ok(()) => true,
                        Err(uni_crdt::CrdtError::TypeMismatch(..)) => false,
                        Err(e) => {
                            metrics::counter!("uni_crdt_provider_merge_failures_total")
                                .increment(1);
                            tracing::error!(
                                property = %k,
                                existing_variant = existing_crdt.type_name(),
                                error = %e,
                                "registered CRDT provider failed to merge; falling back to \
                                 last-writer-wins and discarding the merged state (this is a \
                                 provider failure, NOT a variant mismatch)"
                            );
                            false
                        }
                    },
                    None => new_crdt.try_merge(&existing_crdt).is_ok(),
                };
                if merged {
                    match serde_json::to_value(new_crdt) {
                        Ok(merged_json) => {
                            entry.insert(k, uni_common::Value::from(merged_json));
                            continue;
                        }
                        Err(e) => {
                            // A merge that succeeded and then failed to serialize
                            // is a bug, not a variant mismatch, and the two used to
                            // share one `if let` and one warning — so the only
                            // signal for it was a message naming the wrong cause
                            // (#233).
                            //
                            // Still last-writer-wins. Propagating would make
                            // `merge_crdt_properties`, `apply_vertex_write`,
                            // `insert_vertex_with_labels_impl` and
                            // the three public inserts above them fallible, which is
                            // 52 non-test call sites across five crates — for a
                            // value that was deserialized as a `Crdt` moments
                            // earlier and merged successfully, so re-serializing it
                            // can realistically only fail on OOM. Kept fail-open
                            // deliberately, and made *observable* instead: a metric
                            // and an `error` log of its own, so discarded state is
                            // countable rather than only greppable.
                            metrics::counter!("uni_crdt_merge_serialize_failures_total")
                                .increment(1);
                            tracing::error!(
                                property = %k,
                                error = %e,
                                "merged CRDT failed to serialize; falling back to \
                                 last-writer-wins and discarding the merged state"
                            );
                            entry.insert(k, v);
                            continue;
                        }
                    }
                }
                // CRDT variant mismatch: fall through to a last-writer-wins
                // overwrite, discarding the existing CRDT's merged state. The OCC commit-time carve-out check
                // (`occ::crdt_carveout_overwrite`) aborts a *concurrent* writer
                // before reaching here, and write-time schema enforcement rejects a
                // wrong declared variant; this warns on any residual (e.g. a
                // single-writer variant change) so the discarded state is visible.
                tracing::warn!(
                    property = %k,
                    existing_variant = existing_crdt.type_name(),
                    "overwriting CRDT property with a different CRDT variant \
                     (last-writer-wins); merged CRDT state is discarded"
                );
            } else if try_as_crdt(&v).is_none()
                && entry.get(&k).is_some_and(|e| try_as_crdt(e).is_some())
            {
                // R1: an existing CRDT value is overwritten by a non-CRDT scalar
                // (last-writer-wins), silently discarding the CRDT's merged state
                // — a property written as BOTH a CRDT and last-writer-wins. The OCC
                // write-set carve-out lets CRDT-only writers commit without
                // conflicting, so a concurrent LWW write on the same property
                // cannot be flagged as a conflict; surface it here instead.
                tracing::warn!(
                    property = %k,
                    "overwriting CRDT property with non-CRDT value (last-writer-wins); \
                     merged CRDT state is discarded"
                );
            }
            // Fallback: Overwrite (last-writer-wins).
            entry.insert(k, v);
        }
    }

    /// Helper function to estimate property map size in bytes.
    fn estimate_properties_size(props: &Properties) -> usize {
        props.keys().map(|k| k.len() + 32).sum()
    }

    /// Returns an estimate of the buffer size in bytes.
    /// Includes all fields for accurate memory accounting.
    pub fn size_bytes(&self) -> usize {
        let mut total = 0;

        // Topology
        total += self.graph.vertex_count() * 8;
        total += self.graph.edge_count() * 24;

        // Properties (rough estimate: key string + 32 bytes for value)
        for props in self.vertex_properties.values() {
            total += Self::estimate_properties_size(props);
        }
        for props in self.edge_properties.values() {
            total += Self::estimate_properties_size(props);
        }

        // Metadata
        total += self.tombstones.len() * 64;
        total += self.vertex_tombstones.len() * 8;
        total += self.edge_versions.len() * 16;
        total += self.vertex_versions.len() * 16;
        total += self.vertex_label_versions.len() * 16;
        total += self.edge_endpoints.len() * 28; // (Vid, Vid, u32) = 8+8+4 + overhead

        // Vertex labels
        for labels in self.vertex_labels.values() {
            total += labels.iter().map(|l| l.len() + 24).sum::<usize>();
        }

        // Reverse label index (label_to_vids)
        for (label, vids) in &self.label_to_vids {
            total += label.len() + 24 + vids.len() * 8 + 48; // string + HashSet overhead
        }

        // Edge types
        for type_name in self.edge_types.values() {
            total += type_name.len() + 24;
        }

        // Timestamps (4 maps, each entry is 16 bytes: 8-byte key + 8-byte i64 value)
        total += self.vertex_created_at.len() * 16;
        total += self.vertex_updated_at.len() * 16;
        total += self.edge_created_at.len() * 16;
        total += self.edge_updated_at.len() * 16;

        // Edge identity memo (see `EdgeIdentityMemo::charge`)
        total += self.edge_identity_memo.identities.len() * EdgeIdentityMemo::IDENTITY_BYTES;
        total += self.edge_identity_memo.scanned.len() * EdgeIdentityMemo::SCANNED_BYTES;

        total
    }

    pub fn new(start_version: u64, wal: Option<Arc<WriteAheadLog>>) -> Self {
        Self {
            graph: SimpleGraph::new(),
            tombstones: HashMap::new(),
            vertex_tombstones: HashSet::new(),
            edge_versions: HashMap::new(),
            vertex_versions: HashMap::new(),
            edge_properties: HashMap::new(),
            vertex_properties: HashMap::new(),
            edge_endpoints: HashMap::new(),
            vertex_labels: HashMap::new(),
            label_to_vids: HashMap::new(),
            vertex_label_overwrites: HashSet::new(),
            vertex_label_versions: HashMap::new(),
            edge_types: HashMap::new(),
            current_version: start_version,
            mutation_count: 0,
            mutation_stats: MutationStats::default(),
            wal,
            wal_lsn_at_flush: 0,
            wal_lsn_at_start: 0,
            unpublished_bulk_at_rotation: Vec::new(),
            vertex_created_at: HashMap::new(),
            vertex_updated_at: HashMap::new(),
            edge_created_at: HashMap::new(),
            edge_updated_at: HashMap::new(),
            estimated_size: 0,
            constraint_index: HashMap::new(),
            constraint_keys_by_vid: HashMap::new(),
            edge_constraint_index: HashMap::new(),
            merge_guard_index: HashMap::new(),
            extid_index: HashMap::new(),
            statement_depth: 0,
            statement_write_admitted: false,
            statement_created_vertices: HashSet::new(),
            statement_created_edges: HashSet::new(),
            statement_tracks_created: false,
            created_edges: HashSet::new(),
            created_vertices: HashSet::new(),
            edge_identity_memo: EdgeIdentityMemo::default(),
            pending_embeddings: HashMap::new(),
            occ_read_seq: 0,
            occ_read_set: None,
            plugin_registry: None,
        }
    }

    /// Install the plugin registry used for registry-dispatched CRDT merges.
    ///
    /// Stamped by the owning `L0Manager` onto every buffer it mints so the
    /// commit-time merge (`merge_crdt_properties`) can route custom
    /// CRDT kinds through a registered provider. Absent registry preserves
    /// native [`uni_crdt::Crdt::try_merge`] behavior.
    pub fn set_plugin_registry(&mut self, registry: Arc<uni_plugin::PluginRegistry>) {
        self.plugin_registry = Some(registry);
    }

    pub fn insert_vertex(&mut self, vid: Vid, properties: Properties) {
        self.insert_vertex_with_labels(vid, properties, &[]);
    }

    /// Insert a newly created vertex with associated labels.
    ///
    /// Counts as a create in [`MutationStats`]: one node, its labels, and its
    /// non-null properties. Use [`Self::update_vertex_with_labels`] to rewrite
    /// an existing vertex's row.
    pub fn insert_vertex_with_labels(
        &mut self,
        vid: Vid,
        properties: Properties,
        labels: &[String],
    ) {
        let props_count = properties.values().filter(|v| !v.is_null()).count();
        self.insert_vertex_with_labels_impl(vid, properties, labels, false);
        self.mutation_stats.nodes_created += 1;
        self.mutation_stats.labels_added += labels.len();
        self.mutation_stats.properties_set += props_count;
        if self.created_vertices.insert(vid) {
            self.estimated_size += 16;
        }
        if self.statement_depth > 0 && self.statement_tracks_created {
            self.statement_created_vertices.insert(vid);
        }
    }

    /// Rewrite an existing vertex's full row (a `SET` / `REMOVE` / `MERGE`
    /// action, or a promote upsert).
    ///
    /// Same storage effect as [`Self::insert_vertex_with_labels`], but it is
    /// not a create: the caller records the statement's property side
    /// effects itself ([`Self::record_property_effect`]), since only it knows
    /// which values changed.
    pub fn update_vertex_with_labels(
        &mut self,
        vid: Vid,
        properties: Properties,
        labels: &[String],
    ) {
        self.insert_vertex_with_labels_impl(vid, properties, labels, false);
    }

    /// Core vertex insertion. When `skip_wal` is true, skips WAL append
    /// (used during merge where the caller already wrote to WAL).
    fn insert_vertex_with_labels_impl(
        &mut self,
        vid: Vid,
        properties: Properties,
        labels: &[String],
        skip_wal: bool,
    ) {
        self.current_version += 1;
        let version = self.current_version;
        let now = now_nanos();

        if !skip_wal && let Some(wal) = &self.wal {
            let _ = wal.append(Mutation::InsertVertex {
                vid,
                properties: properties.clone(),
                labels: labels.to_vec(),
                // Both stamped with this write's `now`; replay applies
                // `or_insert` to `created_at`, reproducing the live
                // "oldest wins" rule in `apply_vertex_write`.
                created_at: Some(now),
                updated_at: Some(now),
            });
        }

        self.apply_vertex_write(vid, properties, labels, version, now);
    }

    /// Shared body of the vertex-write paths: clear the tombstone,
    /// CRDT-merge the properties (keeping the `ext_id` index in sync), stamp
    /// version/timestamps, append the labels, and update the mutation count
    /// and size estimate. Side-effect counters ([`MutationStats`]) are left
    /// to the callers, which know whether the write is a create.
    fn apply_vertex_write(
        &mut self,
        vid: Vid,
        properties: Properties,
        labels: &[String],
        version: u64,
        now: i64,
    ) {
        self.vertex_tombstones.remove(&vid);

        // Size/count computed up front so `properties` can be moved into the
        // CRDT merge below instead of deep-cloned.
        let props_size = Self::estimate_properties_size(&properties);
        let tracks_extid = properties.contains_key("ext_id");

        let entry = self.vertex_properties.entry(vid).or_default();
        let old_extid = if tracks_extid {
            Self::extid_of(entry)
        } else {
            None
        };
        Self::merge_crdt_properties(entry, properties, self.plugin_registry.as_ref());
        if tracks_extid {
            let new_extid =
                Self::extid_of(self.vertex_properties.get(&vid).expect("just inserted"));
            self.sync_extid_index(vid, old_extid, new_extid);
        }
        self.vertex_versions.insert(vid, version);

        // Set timestamps - created_at only set if this is a new vertex
        self.vertex_created_at.entry(vid).or_insert(now);
        self.vertex_updated_at.insert(vid, now);

        // Track labels — always create an entry so unlabeled vertices are
        // distinguishable from "not in L0" when queried via get_vertex_labels.
        let labels_size: usize = labels.iter().map(|l| l.len() + 24).sum();
        let existing = self.vertex_labels.entry(vid).or_default();
        Self::append_unique_labels(existing, labels);
        self.index_labels_for_vid(vid, labels);

        self.graph.add_vertex(vid);
        self.mutation_count += 1;

        self.estimated_size += 8 + props_size + 16 + labels_size + 32;
    }

    /// Opens a statement scope on this buffer (nesting allowed).
    ///
    /// While a scope is open the buffer remembers whether the statement paid
    /// the write throttle and, when `track_created`, which entities it
    /// created. The outermost scope decides `track_created`. Prefer
    /// [`StatementScope`], which closes the scope on drop.
    pub fn begin_statement(&mut self, track_created: bool) {
        if self.statement_depth == 0 {
            self.statement_write_admitted = false;
            self.statement_tracks_created = track_created;
            self.statement_created_vertices.clear();
            self.statement_created_edges.clear();
        }
        self.statement_depth += 1;
    }

    /// Closes a scope opened by [`Self::begin_statement`].
    pub fn end_statement(&mut self) {
        self.statement_depth = self.statement_depth.saturating_sub(1);
        if self.statement_depth == 0 {
            self.statement_write_admitted = false;
            self.statement_tracks_created = false;
            self.statement_created_vertices = HashSet::new();
            self.statement_created_edges = HashSet::new();
        }
    }

    /// Whether the running statement has already paid the write throttle.
    /// Always `false` outside a statement scope.
    pub fn write_admitted(&self) -> bool {
        self.statement_depth > 0 && self.statement_write_admitted
    }

    /// Records that the running statement paid the write throttle. No-op
    /// outside a statement scope, so unscoped writes pay it every time.
    pub fn note_write_admitted(&mut self) {
        if self.statement_depth > 0 {
            self.statement_write_admitted = true;
        }
    }

    /// Records the side effect of one property changing from `old` to `new`
    /// on `entity` (`None` = absent), in the openCypher sense: a property is
    /// the triple (entity, key, value), so an addition is `+1` set, a removal
    /// `+1` removed, an overwrite with a different value both, and an equal
    /// value nothing.
    ///
    /// An entity the running statement created had no properties before the
    /// statement, and its create already counted the values it was created
    /// with: a change to it moves only `properties_set` (up for a key gained,
    /// down for a key lost), so the statement reports its net effect.
    pub fn record_property_effect(
        &mut self,
        entity: StatEntity,
        old: Option<&Value>,
        new: Option<&Value>,
    ) {
        let old = old.filter(|v| !v.is_null());
        let new = new.filter(|v| !v.is_null());
        if old == new {
            return;
        }
        let created = match entity {
            StatEntity::Vertex(vid) => self.statement_created_vertices.contains(&vid),
            StatEntity::Edge(eid) => self.statement_created_edges.contains(&eid),
        };
        let stats = &mut self.mutation_stats;
        match (old.is_some(), new.is_some(), created) {
            (_, true, true) if old.is_some() => {}
            (false, true, _) => stats.properties_set += 1,
            (true, false, true) => {
                stats.properties_set = stats.properties_set.saturating_sub(1);
            }
            (true, false, false) => stats.properties_removed += 1,
            (true, true, false) => {
                stats.properties_set += 1;
                stats.properties_removed += 1;
            }
            _ => {}
        }
    }

    /// Records `n` labels newly added to `vid`.
    pub fn record_labels_added(&mut self, _vid: Vid, n: usize) {
        self.mutation_stats.labels_added += n;
    }

    /// Records `n` labels removed from `vid`. On a vertex the running
    /// statement created, the removal cancels the create's count instead.
    pub fn record_labels_removed(&mut self, vid: Vid, n: usize) {
        if self.statement_created_vertices.contains(&vid) {
            self.mutation_stats.labels_added = self.mutation_stats.labels_added.saturating_sub(n);
        } else {
            self.mutation_stats.labels_removed += n;
        }
    }

    /// Add labels to an existing vertex.
    pub fn add_vertex_labels(&mut self, vid: Vid, labels: &[String]) {
        let existing = self.vertex_labels.entry(vid).or_default();
        Self::append_unique_labels(existing, labels);
        self.index_labels_for_vid(vid, labels);
    }

    /// Remove a label from an existing vertex.
    /// Returns true if the label was found and removed, false otherwise.
    pub fn remove_vertex_label(&mut self, vid: Vid, label: &str) -> bool {
        if let Some(labels) = self.vertex_labels.get_mut(&vid)
            && let Some(pos) = labels.iter().position(|l| l == label)
        {
            labels.remove(pos);
            if let Some(set) = self.label_to_vids.get_mut(label) {
                set.remove(&vid);
            }
            self.current_version += 1;
            self.mutation_count += 1;
            self.mutation_stats.labels_removed += 1;
            // Note: WAL logging for label mutations not yet implemented
            // Currently consistent with add_vertex_labels behavior
            return true;
        }
        false
    }

    /// Set the type for an edge.
    pub fn set_edge_type(&mut self, eid: Eid, edge_type: String) {
        self.edge_types.insert(eid, edge_type);
    }

    /// Returns the edge tombstones the cascade produced, so a caller holding
    /// the `AdjacencyManager` can mirror them (see
    /// `Writer::mirror_cascaded_tombstones`). Callers that only need the L0
    /// effect may discard the value.
    pub fn delete_vertex(&mut self, vid: Vid) -> Result<Vec<TombstoneEntry>> {
        self.delete_vertex_impl(vid, false)
    }

    /// Core vertex deletion. When `skip_wal` is true, skips WAL append
    /// (used during merge where the caller already wrote to WAL).
    fn delete_vertex_impl(&mut self, vid: Vid, skip_wal: bool) -> Result<Vec<TombstoneEntry>> {
        self.current_version += 1;

        if !skip_wal && let Some(wal) = &mut self.wal {
            let labels = self.vertex_labels.get(&vid).cloned().unwrap_or_default();
            wal.append(Mutation::DeleteVertex { vid, labels })?;
        }

        Ok(self.apply_vertex_deletion(vid))
    }

    /// Cascade-delete a vertex: tombstone all connected edges and remove the vertex.
    ///
    /// Shared between `delete_vertex` (live mutations) and `replay_mutations` (WAL recovery).
    ///
    /// Returns the edge tombstones created by the cascade. They are the L0 half
    /// of a two-place write: the adjacency overlay holds the other half, and a
    /// caller with access to the `AdjacencyManager` must mirror these or the
    /// deleted vertex's edges stay live in traversals until the next flush.
    /// Returning them is what keeps that caller from re-deriving the incident
    /// set and drifting from this one.
    fn apply_vertex_deletion(&mut self, vid: Vid) -> Vec<TombstoneEntry> {
        let version = self.current_version;

        // Collect edges to delete using O(degree) neighbors() instead of O(E) scan
        let mut edges_to_remove = HashSet::new();

        // Collect outgoing edges
        for entry in self.graph.neighbors(vid, Direction::Outgoing) {
            edges_to_remove.insert(entry.eid);
        }

        // Collect incoming edges
        for entry in self.graph.neighbors(vid, Direction::Incoming) {
            edges_to_remove.insert(entry.eid); // HashSet handles self-loop deduplication
        }

        let cascaded_edges_count = edges_to_remove.len();
        let mut cascaded = Vec::with_capacity(cascaded_edges_count);

        // Tombstone and remove all collected edges
        for eid in edges_to_remove {
            // Retrieve edge endpoints from the map to create tombstone
            if let Some((src, dst, etype)) = self.edge_endpoints.get(&eid) {
                let entry = TombstoneEntry {
                    eid,
                    src_vid: *src,
                    dst_vid: *dst,
                    edge_type: *etype,
                };
                cascaded.push(entry.clone());
                self.tombstones.insert(eid, entry);
                self.edge_versions.insert(eid, version);
                self.edge_endpoints.remove(&eid);
                self.edge_properties.remove(&eid);
                self.graph.remove_edge(eid);
                self.mutation_count += 1;
                self.mutation_stats.relationships_deleted += 1;
            }
        }

        self.remove_vid_from_label_index(vid);
        // As for edges: deleting a vertex this buffer already tombstoned
        // counts once.
        let newly_deleted = self.vertex_tombstones.insert(vid);
        // Drop the vid's ext_id index entry (O(1) via its current property
        // value, read before the property map entry is removed below).
        if let Some(props) = self.vertex_properties.get(&vid)
            && let Some(ext) = Self::extid_of(props)
            && self.extid_index.get(&ext) == Some(&vid)
        {
            self.extid_index.remove(&ext);
        }
        self.vertex_properties.remove(&vid);
        // Deletion supersedes any pending partial-update state.
        self.vertex_versions.insert(vid, version);
        self.graph.remove_vertex(vid);
        self.mutation_count += 1;
        if newly_deleted {
            self.mutation_stats.nodes_deleted += 1;
        }

        // Remove constraint index entries for this vertex
        self.constraint_index.retain(|_, v| *v != vid);
        self.constraint_keys_by_vid.remove(&vid);
        // Same for the implicit MERGE guard, so a later re-MERGE of a deleted
        // node's key does not false-conflict with the stale entry.
        self.merge_guard_index.retain(|_, v| *v != vid);

        // 64 bytes per edge tombstone + 8 for vertex tombstone
        self.estimated_size += cascaded_edges_count * 72 + 8;

        cascaded
    }

    /// Insert a newly created edge.
    ///
    /// Counts as a create in [`MutationStats`]: one relationship and its
    /// non-null properties. Use [`Self::update_edge`] to rewrite an existing
    /// edge's row.
    pub fn insert_edge(
        &mut self,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        eid: Eid,
        properties: Properties,
        edge_type_name: Option<String>,
    ) -> Result<()> {
        let props_count = properties.values().filter(|v| !v.is_null()).count();
        self.insert_edge_impl(
            src_vid,
            dst_vid,
            edge_type,
            eid,
            properties,
            edge_type_name,
            false,
        )?;
        self.mutation_stats.relationships_created += 1;
        self.mutation_stats.properties_set += props_count;
        if self.created_edges.insert(eid) {
            self.estimated_size += 16;
        }
        if self.statement_depth > 0 && self.statement_tracks_created {
            self.statement_created_edges.insert(eid);
        }
        Ok(())
    }

    /// Rewrite an existing edge's full row (a `SET` / `REMOVE` on it).
    ///
    /// Not a create; the caller records the property side effects
    /// ([`Self::record_property_effect`]).
    pub fn update_edge(
        &mut self,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        eid: Eid,
        properties: Properties,
        edge_type_name: Option<String>,
    ) -> Result<()> {
        self.insert_edge_impl(
            src_vid,
            dst_vid,
            edge_type,
            eid,
            properties,
            edge_type_name,
            false,
        )
    }

    /// Core edge insertion. When `skip_wal` is true, skips WAL append
    /// (used during merge where the caller already wrote to WAL).
    #[allow(clippy::too_many_arguments)]
    fn insert_edge_impl(
        &mut self,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        eid: Eid,
        properties: Properties,
        edge_type_name: Option<String>,
        skip_wal: bool,
    ) -> Result<()> {
        self.current_version += 1;
        let now = now_nanos();

        if !skip_wal && let Some(wal) = &mut self.wal {
            wal.append(Mutation::InsertEdge {
                src_vid,
                dst_vid,
                edge_type,
                eid,
                version: self.current_version,
                properties: properties.clone(),
                edge_type_name: edge_type_name.clone(),
                created_at: Some(now),
                updated_at: Some(now),
            })?;
        }

        self.apply_edge_insertion(src_vid, dst_vid, edge_type, eid, properties)?;

        // Store edge type name in metadata if provided
        let type_name_size = if let Some(ref name) = edge_type_name {
            let size = name.len() + 24;
            self.edge_types.insert(eid, name.clone());
            size
        } else {
            0
        };

        // Set timestamps - created_at only set if this is a new edge
        self.edge_created_at.entry(eid).or_insert(now);
        self.edge_updated_at.insert(eid, now);

        self.estimated_size += type_name_size;

        Ok(())
    }

    /// Core edge insertion logic: add vertices, add edge, merge properties, update metadata.
    ///
    /// Shared between `insert_edge` (live mutations) and `replay_mutations` (WAL recovery).
    ///
    /// # Errors
    ///
    /// Returns error if either endpoint vertex has been deleted (exists in vertex_tombstones).
    /// This prevents "ghost vertex" resurrection via edge insertion. See issue #77.
    fn apply_edge_insertion(
        &mut self,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        eid: Eid,
        properties: Properties,
    ) -> Result<()> {
        let version = self.current_version;

        // Check if either endpoint has been deleted. Inserting an edge to a deleted
        // vertex would resurrect it as a "ghost vertex" with no properties. See issue #77.
        if self.vertex_tombstones.contains(&src_vid) {
            anyhow::bail!(
                "Cannot insert edge: source vertex {} has been deleted (issue #77)",
                src_vid
            );
        }
        if self.vertex_tombstones.contains(&dst_vid) {
            anyhow::bail!(
                "Cannot insert edge: destination vertex {} has been deleted (issue #77)",
                dst_vid
            );
        }

        // Add vertices to graph topology if they don't exist.
        // IMPORTANT: Only add to graph structure, do NOT call insert_vertex.
        // insert_vertex creates a new version with empty properties, which would
        // cause MVCC to pick the empty version as "latest", losing original properties.
        if !self.graph.contains_vertex(src_vid) {
            self.graph.add_vertex(src_vid);
        }
        if !self.graph.contains_vertex(dst_vid) {
            self.graph.add_vertex(dst_vid);
        }

        self.graph.add_edge(src_vid, dst_vid, eid, edge_type);

        // Store metadata with CRDT merge logic
        let props_size = Self::estimate_properties_size(&properties);
        if !properties.is_empty() {
            let entry = self.edge_properties.entry(eid).or_default();
            Self::merge_crdt_properties(entry, properties, self.plugin_registry.as_ref());
        }

        self.edge_versions.insert(eid, version);
        self.edge_endpoints
            .insert(eid, (src_vid, dst_vid, edge_type));
        self.tombstones.remove(&eid);
        self.mutation_count += 1;

        // 24 edge + props + 16 version + 28 endpoints + 32 timestamps
        self.estimated_size += 24 + props_size + 16 + 28 + 32;

        Ok(())
    }

    pub fn delete_edge(
        &mut self,
        eid: Eid,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
    ) -> Result<()> {
        self.delete_edge_impl(eid, src_vid, dst_vid, edge_type, false)
    }

    /// Core edge deletion. When `skip_wal` is true, skips WAL append
    /// (used during merge where the caller already wrote to WAL).
    fn delete_edge_impl(
        &mut self,
        eid: Eid,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        skip_wal: bool,
    ) -> Result<()> {
        self.current_version += 1;
        let now = now_nanos();

        if !skip_wal && let Some(wal) = &mut self.wal {
            wal.append(Mutation::DeleteEdge {
                eid,
                src_vid,
                dst_vid,
                edge_type,
                version: self.current_version,
            })?;
        }

        self.apply_edge_deletion(eid, src_vid, dst_vid, edge_type);

        // Update timestamp - deletion is an update
        self.edge_updated_at.insert(eid, now);

        Ok(())
    }

    /// Core edge deletion logic: tombstone the edge, update version, remove from graph.
    ///
    /// Shared between `delete_edge` (live mutations) and `replay_mutations` (WAL recovery).
    fn apply_edge_deletion(&mut self, eid: Eid, src_vid: Vid, dst_vid: Vid, edge_type: u32) {
        let version = self.current_version;

        // A second delete of an edge this buffer already tombstoned (named
        // twice in one `DELETE`, or by name and again by `DETACH DELETE`) is
        // a no-op for the side-effect counters.
        let newly_deleted = self
            .tombstones
            .insert(
                eid,
                TombstoneEntry {
                    eid,
                    src_vid,
                    dst_vid,
                    edge_type,
                },
            )
            .is_none();
        self.edge_versions.insert(eid, version);
        // Drop any unique-constraint keys this edge owned so a later edge may
        // reuse the value (mirrors `apply_vertex_deletion`).
        self.edge_constraint_index.retain(|_, e| *e != eid);
        self.graph.remove_edge(eid);
        self.mutation_count += 1;
        if newly_deleted {
            self.mutation_stats.relationships_deleted += 1;
        }

        // 64 bytes tombstone + 16 bytes version
        self.estimated_size += 80;
    }

    /// Returns neighbors in the specified direction.
    /// O(degree) complexity - iterates only edges connected to the vertex.
    pub fn get_neighbors(
        &self,
        vid: Vid,
        edge_type: u32,
        direction: Direction,
    ) -> Vec<(Vid, Eid, u64)> {
        let edges = self.graph.neighbors(vid, direction);

        edges
            .iter()
            .filter(|e| e.edge_type == edge_type && !self.is_tombstoned(e.eid))
            .map(|e| {
                let neighbor = match direction {
                    Direction::Outgoing => e.dst_vid,
                    Direction::Incoming => e.src_vid,
                };
                let version = self.edge_versions.get(&e.eid).copied().unwrap_or(0);
                (neighbor, e.eid, version)
            })
            .collect()
    }

    pub fn is_tombstoned(&self, eid: Eid) -> bool {
        self.tombstones.contains_key(&eid)
    }

    /// Returns all VIDs in vertex_labels that match the given label name.
    /// O(1) lookup via the reverse label index.
    pub fn vids_for_label(&self, label_name: &str) -> Vec<Vid> {
        self.label_to_vids
            .get(label_name)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Returns all vertex VIDs in the L0 buffer.
    ///
    /// Used for schemaless scanning (MATCH (n) without label).
    pub fn all_vertex_vids(&self) -> Vec<Vid> {
        self.vertex_properties.keys().copied().collect()
    }

    /// Returns all VIDs in vertex_labels that match any of the given label names.
    /// Uses the reverse label index — O(sum of matching set sizes).
    pub fn vids_for_labels(&self, label_names: &[&str]) -> Vec<Vid> {
        let mut result = HashSet::new();
        for label_name in label_names {
            if let Some(set) = self.label_to_vids.get(*label_name) {
                result.extend(set.iter().copied());
            }
        }
        result.into_iter().collect()
    }

    /// Returns all VIDs that have ALL specified labels.
    /// Uses the reverse label index — intersects the per-label sets.
    pub fn vids_with_all_labels(&self, label_names: &[&str]) -> Vec<Vid> {
        if label_names.is_empty() {
            return Vec::new();
        }
        // Collect the per-label sets; if any label is missing from the index,
        // the intersection is empty.
        let sets: Vec<&HashSet<Vid>> = match label_names
            .iter()
            .map(|ln| self.label_to_vids.get(*ln))
            .collect::<Option<Vec<_>>>()
        {
            Some(s) => s,
            None => return Vec::new(),
        };
        // Start from the smallest set for efficiency.
        let smallest = sets.iter().min_by_key(|s| s.len()).unwrap();
        smallest
            .iter()
            .copied()
            .filter(|vid| sets.iter().all(|s| s.contains(vid)))
            .collect()
    }

    /// Gets the labels for a VID.
    pub fn get_vertex_labels(&self, vid: Vid) -> Option<&[String]> {
        self.vertex_labels.get(&vid).map(|v| v.as_slice())
    }

    /// Gets the edge type for an EID.
    pub fn get_edge_type(&self, eid: Eid) -> Option<&str> {
        self.edge_types.get(&eid).map(|s| s.as_str())
    }

    /// Returns all EIDs in edge_types that match the given type name.
    /// Used for L0 overlay during schemaless edge scanning.
    pub fn eids_for_type(&self, type_name: &str) -> Vec<Eid> {
        self.edge_types
            .iter()
            .filter(|(eid, etype)| *etype == type_name && !self.tombstones.contains_key(eid))
            .map(|(eid, _)| *eid)
            .collect()
    }

    /// Returns all edge EIDs in the L0 buffer (non-tombstoned).
    ///
    /// Used for schemaless scanning (`MATCH ()-[r]->()`) without type.
    pub fn all_edge_eids(&self) -> Vec<Eid> {
        self.edge_endpoints
            .keys()
            .filter(|eid| !self.tombstones.contains_key(eid))
            .copied()
            .collect()
    }

    /// Returns edge endpoint data (src_vid, dst_vid) for an EID.
    pub fn get_edge_endpoints(&self, eid: Eid) -> Option<(Vid, Vid)> {
        self.edge_endpoints
            .get(&eid)
            .map(|(src, dst, _)| (*src, *dst))
    }

    /// Returns full edge endpoint data (src_vid, dst_vid, edge_type_id) for an EID.
    pub fn get_edge_endpoint_full(&self, eid: Eid) -> Option<(Vid, Vid, u32)> {
        self.edge_endpoints.get(&eid).copied()
    }

    /// Insert a constraint key into the index for O(1) duplicate detection.
    ///
    /// A vertex holds one key per constraint, so a previous key of the same
    /// shape (label and property names) is retired first.
    pub fn insert_constraint_key(&mut self, key: Vec<u8>, vid: Vid) {
        let shape = constraint_key_shape(&key);
        let held = self.constraint_keys_by_vid.entry(vid).or_default();
        let mut retired = Vec::new();
        held.retain(|old| {
            let stale = *old != key && constraint_key_shape(old) == shape;
            if stale {
                retired.push(old.clone());
            }
            !stale
        });
        if !held.contains(&key) {
            held.push(key.clone());
        }
        for old in retired {
            if self.constraint_index.get(&old) == Some(&vid) {
                self.constraint_index.remove(&old);
            }
        }
        self.constraint_index.insert(key, vid);
    }

    /// Check if a constraint key exists in the index, excluding a specific VID.
    /// Returns true if the key exists and is owned by a different vertex.
    pub fn has_constraint_key(&self, key: &[u8], exclude_vid: Vid) -> bool {
        self.constraint_index
            .get(key)
            .is_some_and(|&v| v != exclude_vid)
    }

    /// Insert an edge unique-constraint key into the index (edge analogue of
    /// [`insert_constraint_key`](Self::insert_constraint_key)).
    pub fn insert_edge_constraint_key(&mut self, key: Vec<u8>, eid: Eid) {
        self.edge_constraint_index.insert(key, eid);
    }

    /// Check if an edge unique-constraint key exists, owned by an edge other than
    /// `exclude_eid`. Edge analogue of
    /// [`has_constraint_key`](Self::has_constraint_key).
    pub fn has_edge_constraint_key(&self, key: &[u8], exclude_eid: Eid) -> bool {
        self.edge_constraint_index
            .get(key)
            .is_some_and(|&e| e != exclude_eid)
    }

    /// Register a MERGE-create's key into the implicit phantom guard.
    pub fn insert_merge_guard_key(&mut self, key: Vec<u8>, vid: Vid) {
        self.merge_guard_index.insert(key, vid);
    }

    /// Check if a MERGE-guard key exists, owned by a different vertex than
    /// `exclude_vid` — i.e. a concurrent MERGE already created this key.
    pub fn has_merge_guard_key(&self, key: &[u8], exclude_vid: Vid) -> bool {
        self.merge_guard_index
            .get(key)
            .is_some_and(|&v| v != exclude_vid)
    }

    #[instrument(skip(self, other), level = "trace")]
    /// Validate that merging `other` into `self` will not bail on a tombstoned
    /// edge endpoint (issue #77), **without mutating** either buffer.
    ///
    /// Mirrors the endpoint-liveness guard in `apply_edge_insertion`
    /// against the tombstone state [`Self::merge`] produces: `other`'s vertex
    /// deletions are applied and its vertex inserts clear their own tombstone,
    /// so an inserted edge bails iff an endpoint is tombstoned in `self` or
    /// `other` and is not (re-)inserted by `other`.
    ///
    /// Run this under `flush_lock` *before* the durable WAL flush so an
    /// offending commit is rejected up front. After the flush the transaction
    /// is durable, and a `merge` bail would leave a ghost/partial commit whose
    /// WAL replay re-bails — rendering the database unopenable.
    ///
    /// # Errors
    ///
    /// Returns an error naming the offending edge and endpoint when the merge
    /// would bail.
    pub fn validate_merge_edge_endpoints(&self, other: &L0Buffer) -> Result<()> {
        // An endpoint is effectively deleted after the merge's vertex phase if
        // it is tombstoned in either buffer and `other` does not re-insert it
        // (an insert clears the tombstone).
        let is_deleted = |vid: &Vid| {
            (self.vertex_tombstones.contains(vid) || other.vertex_tombstones.contains(vid))
                && !other.vertex_properties.contains_key(vid)
        };
        for (eid, (src_vid, dst_vid, _etype)) in &other.edge_endpoints {
            if other.tombstones.contains_key(eid) {
                continue; // a deletion, not an insertion — never resurrects a vertex
            }
            if is_deleted(src_vid) {
                anyhow::bail!(
                    "Cannot insert edge {}: source vertex {} has been deleted (issue #77)",
                    eid,
                    src_vid
                );
            }
            if is_deleted(dst_vid) {
                anyhow::bail!(
                    "Cannot insert edge {}: destination vertex {} has been deleted (issue #77)",
                    eid,
                    dst_vid
                );
            }
        }
        Ok(())
    }

    /// Returns the cascaded edge tombstones; see [`L0Buffer::merge_take`].
    pub fn merge(&mut self, other: &L0Buffer) -> Result<Vec<TombstoneEntry>> {
        // Validate-then-apply: reject a merge that would bail on a tombstoned
        // edge endpoint before mutating anything, so a failed merge can never
        // leave a partially-applied (non-atomic) commit.
        self.validate_merge_edge_endpoints(other)?;
        self.merge_validated(
            other,
            other.vertex_properties.clone(),
            other.edge_properties.clone(),
        )
    }

    /// Commit-path variant of [`merge`](Self::merge) that consumes `other`'s
    /// vertex/edge property maps instead of deep-cloning every row.
    ///
    /// Everything else in `other` (endpoints, tombstones, versions, labels)
    /// is left intact — `commit_transaction_l0` still reads those after the
    /// merge. The caller must not rely on `other.vertex_properties` /
    /// `other.edge_properties` afterwards, which is safe on the commit path
    /// because committing consumes the transaction.
    /// Returns the edge tombstones produced by cascading deletes against
    /// `self`. These are edges that live in the main buffer and were never
    /// touched by `other`, so the caller's tx-scoped adjacency mirror cannot
    /// see them — it must mirror these separately.
    pub fn merge_take(&mut self, other: &mut L0Buffer) -> Result<Vec<TombstoneEntry>> {
        // Validate BEFORE draining: the endpoint check consults
        // `other.vertex_properties` (the "re-inserted by other" exemption).
        self.validate_merge_edge_endpoints(other)?;
        let vertex_props = std::mem::take(&mut other.vertex_properties);
        let edge_props = std::mem::take(&mut other.edge_properties);
        self.merge_validated(other, vertex_props, edge_props)
    }

    /// Shared merge body. `vertex_props` / `edge_props` are `other`'s property
    /// maps, passed by value so rows move instead of clone; the caller has
    /// already run `validate_merge_edge_endpoints`.
    fn merge_validated(
        &mut self,
        other: &L0Buffer,
        vertex_props: HashMap<Vid, Properties>,
        mut edge_props: HashMap<Eid, Properties>,
    ) -> Result<Vec<TombstoneEntry>> {
        trace!(
            other_mutation_count = other.mutation_count,
            "Merging L0 buffer"
        );
        // skip_wal=true throughout: the caller (commit_transaction_l0) already
        // wrote every one of these mutations to WAL before invoking merge —
        // re-appending here would double the WAL volume per commit.
        // Merge Vertices
        let mut cascaded = Vec::new();
        for &vid in &other.vertex_tombstones {
            cascaded.extend(self.delete_vertex_impl(vid, true)?);
        }

        for (vid, props) in vertex_props {
            let labels = other.vertex_labels.get(&vid).cloned().unwrap_or_default();
            self.insert_vertex_with_labels_impl(vid, props, &labels, true);
        }

        // Merge vertex labels that might not have properties
        for (vid, labels) in &other.vertex_labels {
            if !self.vertex_labels.contains_key(vid) {
                self.vertex_labels.insert(*vid, labels.clone());
                for label in labels {
                    self.label_to_vids
                        .entry(label.clone())
                        .or_default()
                        .insert(*vid);
                }
            }
        }

        // Label-overwrite pass: a `SET n:Label` / `REMOVE n:Label` resolved the
        // FULL new label set into `other.vertex_labels[vid]` and flagged the vid.
        // REPLACE (not append) so removals actually remove and an existing
        // vertex's label change lands — overriding any append from the property
        // loop above. Skip vids deleted in the same commit. (The append loops
        // stay correct for property-path label unions, which are NOT flagged.)
        for vid in &other.vertex_label_overwrites {
            if other.vertex_tombstones.contains(vid) {
                continue;
            }
            let labels = other.vertex_labels.get(vid).cloned().unwrap_or_default();
            self.remove_vid_from_label_index(*vid);
            self.vertex_labels.insert(*vid, labels.clone());
            self.index_labels_for_vid(*vid, &labels);
            // The relabel is a write at this buffer's next version, as in
            // `set_vertex_labels`; the flush stamps the re-appended row with it.
            self.current_version += 1;
            self.vertex_label_versions
                .insert(*vid, self.current_version);
            // Carry the overwrite flag into the persistent (main) buffer so a
            // pure relabel of a prior-window vid — which is absent from
            // `vertex_properties` — is still re-derived at flush (M8). The
            // flag is cleared when this buffer is rotated out by the flush.
            self.vertex_label_overwrites.insert(*vid);
        }

        // Merge Edges - insert all edges from edge_endpoints, using empty props if none exist
        for (eid, (src, dst, etype)) in &other.edge_endpoints {
            if other.tombstones.contains_key(eid) {
                self.delete_edge_impl(*eid, *src, *dst, *etype, true)?;
            } else {
                let props = edge_props.remove(eid).unwrap_or_default();
                let etype_name = other.edge_types.get(eid).cloned();
                self.insert_edge_impl(*src, *dst, *etype, *eid, props, etype_name, true)?;
            }
        }

        // Merge tombstones for edges that only exist in the target buffer (self),
        // not in the source buffer's edge_endpoints.  Without this, transaction
        // DELETEs of pre-existing edges are silently lost on commit.
        for (eid, tombstone) in &other.tombstones {
            if !other.edge_endpoints.contains_key(eid) {
                self.delete_edge_impl(
                    *eid,
                    tombstone.src_vid,
                    tombstone.dst_vid,
                    tombstone.edge_type,
                    true,
                )?;
            }
        }

        // Edge types are now merged inside insert_edge, so no separate loop needed

        // Merge timestamps - preserve semantics of or_insert (keep oldest created_at)
        // and insert (use latest updated_at)
        for (vid, ts) in &other.vertex_created_at {
            self.vertex_created_at.entry(*vid).or_insert(*ts); // keep oldest
        }
        for (vid, ts) in &other.vertex_updated_at {
            self.vertex_updated_at.insert(*vid, *ts); // use latest (tx wins)
        }

        for (eid, ts) in &other.edge_created_at {
            self.edge_created_at.entry(*eid).or_insert(*ts); // keep oldest
        }
        for (eid, ts) in &other.edge_updated_at {
            self.edge_updated_at.insert(*eid, *ts); // use latest (tx wins)
        }

        // Conservatively add other's estimated size (may overcount due to
        // deduplication, but that's safe for a memory limit). Not its identity
        // memo, which is not merged.
        self.estimated_size += other
            .estimated_size
            .saturating_sub(other.edge_identity_memo.charged_bytes);

        // Merge constraint index
        for (key, vid) in &other.constraint_index {
            self.insert_constraint_key(key.clone(), *vid);
        }

        // Merge the implicit MERGE-key guard so a committed MERGE-create is
        // visible to a concurrent transaction's commit-time re-probe.
        for (key, vid) in &other.merge_guard_index {
            self.merge_guard_index.insert(key.clone(), *vid);
        }

        // Merge the edge unique-constraint index (parallel to `constraint_index`).
        for (key, eid) in &other.edge_constraint_index {
            self.edge_constraint_index.insert(key.clone(), *eid);
        }

        // Carry deferred-embedding markers from the tx L0 into the main L0 so the
        // flush-time `drain_pending_embeddings` sees them (the marked vids' properties were
        // just merged above). Without this, `defer_embeddings` auto-embed silently no-ops for
        // any transactional write — a pre-existing gap that also affects single-vector
        // deferral, surfaced while wiring multi-vector auto-embed (issue #104).
        for (vid, label) in &other.pending_embeddings {
            self.pending_embeddings.insert(*vid, label.clone());
        }

        Ok(cascaded)
    }

    /// Replay mutations from WAL without re-logging them.
    /// Used during startup recovery to restore L0 state from persisted WAL.
    /// Uses CRDT merge semantics to ensure recovered state matches pre-crash state.
    #[instrument(skip(self, mutations), level = "debug")]
    pub fn replay_mutations(&mut self, mutations: Vec<Mutation>) -> Result<()> {
        trace!(count = mutations.len(), "Replaying mutations");
        for mutation in mutations {
            match mutation {
                Mutation::InsertVertex {
                    vid,
                    properties,
                    labels,
                    created_at,
                    updated_at,
                } => {
                    // Apply without WAL logging, with CRDT merge semantics.
                    //
                    // This is a hand-written twin of `apply_vertex_write`, not
                    // a call to it, and the divergence is deliberate but narrow.
                    // Everything that determines *state* is identical — and is
                    // pinned by `replay_reconstructs_what_the_live_vertex_write_produced`,
                    // because this arm has already drifted once: it dropped the
                    // timestamps, so recovered rows flushed to L1 with a null
                    // `created_at`. What it deliberately omits is accounting
                    // that describes work done *by a caller*:
                    //
                    //   * `mutation_stats.properties_set` / `labels_added` —
                    //     these report what a query did; a replayed mutation
                    //     was performed by a previous process, and counting it
                    //     here would attribute it to whoever opens the store.
                    //   * `estimated_size` — feeds the `l0_buffer_size_bytes`
                    //     gauge and the per-transaction memory cap, neither of
                    //     which applies to a recovered main buffer. (The flush
                    //     trigger reads `mutation_count`, which IS incremented
                    //     below, so recovery still flushes on schedule.)
                    //
                    // Anything beyond those two is drift, not design.
                    self.current_version += 1;
                    let version = self.current_version;

                    self.vertex_tombstones.remove(&vid);
                    let tracks_extid = properties.contains_key("ext_id");
                    let entry = self.vertex_properties.entry(vid).or_default();
                    let old_extid = if tracks_extid {
                        Self::extid_of(entry)
                    } else {
                        None
                    };
                    Self::merge_crdt_properties(entry, properties, self.plugin_registry.as_ref());
                    if tracks_extid {
                        let new_extid = Self::extid_of(
                            self.vertex_properties.get(&vid).expect("just inserted"),
                        );
                        self.sync_extid_index(vid, old_extid, new_extid);
                    }
                    self.vertex_versions.insert(vid, version);
                    // Restore the write-time timestamps. `or_insert` on
                    // `created_at` mirrors `apply_vertex_write` exactly, so
                    // replaying a create followed by an update keeps the
                    // creation time. A pre-timestamp WAL segment carries
                    // `None` and leaves both unset, as before.
                    if let Some(ts) = created_at {
                        self.vertex_created_at.entry(vid).or_insert(ts);
                    }
                    if let Some(ts) = updated_at {
                        self.vertex_updated_at.insert(vid, ts);
                    }
                    self.graph.add_vertex(vid);
                    self.mutation_count += 1;

                    // Restore vertex labels from WAL
                    let existing = self.vertex_labels.entry(vid).or_default();
                    Self::append_unique_labels(existing, &labels);
                    for label in &labels {
                        self.label_to_vids
                            .entry(label.clone())
                            .or_default()
                            .insert(vid);
                    }
                }
                Mutation::DeleteVertex { vid, labels } => {
                    self.current_version += 1;
                    // Restore labels BEFORE apply_vertex_deletion
                    if !labels.is_empty() {
                        let existing = self.vertex_labels.entry(vid).or_default();
                        Self::append_unique_labels(existing, &labels);
                        for label in &labels {
                            self.label_to_vids
                                .entry(label.clone())
                                .or_default()
                                .insert(vid);
                        }
                    }
                    self.apply_vertex_deletion(vid);
                }
                Mutation::SetVertexLabels { vid, labels } => {
                    // REPLACE the vid's full label set (a label-only mutation
                    // resolved the complete set). Replace, not append, so a
                    // replayed removal removes; clears the old reverse-index
                    // entries first.
                    self.current_version += 1;
                    self.vertex_label_versions.insert(vid, self.current_version);
                    self.remove_vid_from_label_index(vid);
                    self.vertex_labels.insert(vid, labels.clone());
                    self.index_labels_for_vid(vid, &labels);
                    // Mark this vid as a label overwrite, exactly like the live
                    // `set_vertex_labels` path. Without this marker the M8
                    // flush/merge overwrite pass skips the vid (a label-only
                    // mutation leaves no `vertex_properties` entry), so a
                    // WAL-durable SET/REMOVE label on a prior-window vertex
                    // would be silently lost at the first post-recovery flush.
                    self.vertex_label_overwrites.insert(vid);
                    self.mutation_count += 1;
                }
                Mutation::InsertEdge {
                    src_vid,
                    dst_vid,
                    edge_type,
                    eid,
                    version: _,
                    properties,
                    edge_type_name,
                    created_at,
                    updated_at,
                } => {
                    self.current_version += 1;
                    // Skip-and-warn on the issue-#77 endpoint bail: a pre-fix
                    // durable WAL may hold a ghost edge whose endpoint was
                    // tombstoned. Recovery must still open the database rather
                    // than abort, so drop the offending edge and continue.
                    match self.apply_edge_insertion(src_vid, dst_vid, edge_type, eid, properties) {
                        Ok(()) => {
                            // Restore edge type name metadata if present
                            if let Some(name) = edge_type_name {
                                self.edge_types.insert(eid, name);
                            }
                            // Restore the write-time timestamps, mirroring
                            // `insert_edge_impl`'s `or_insert`/`insert` pair.
                            // Only on the Ok path: an edge skipped for the
                            // issue-#77 endpoint bail must leave no trace.
                            if let Some(ts) = created_at {
                                self.edge_created_at.entry(eid).or_insert(ts);
                            }
                            if let Some(ts) = updated_at {
                                self.edge_updated_at.insert(eid, ts);
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                ?eid,
                                ?src_vid,
                                ?dst_vid,
                                error = %e,
                                "WAL replay: skipping edge insertion to a deleted endpoint (issue #77)"
                            );
                        }
                    }
                }
                Mutation::DeleteEdge {
                    eid,
                    src_vid,
                    dst_vid,
                    edge_type,
                    version: _,
                } => {
                    self.current_version += 1;
                    self.apply_edge_deletion(eid, src_vid, dst_vid, edge_type);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A transaction buffer's identity memo is neither merged into the main
    /// buffer nor copied by a fork, so neither carries its charge.
    #[test]
    fn merge_and_clone_leave_the_identity_memo_charge_out() {
        let mut tx = L0Buffer::new(0, None);
        for i in 0..100u64 {
            tx.edge_identity_memo
                .identities
                .insert(Eid::new(i), (Vid::new(1), Vid::new(i + 2), 0));
        }
        tx.edge_identity_memo.scanned.insert((Vid::new(1), 0));
        let added = tx.edge_identity_memo.charge(0, 0);
        tx.estimated_size += added;
        assert_eq!(
            added,
            100 * EdgeIdentityMemo::IDENTITY_BYTES + EdgeIdentityMemo::SCANNED_BYTES
        );

        let mut main = L0Buffer::new(0, None);
        main.merge(&tx).unwrap();
        assert_eq!(main.estimated_size, 0, "a merge carries no memo bytes");
        assert_eq!(tx.clone().estimated_size, 0, "a fork carries no memo bytes");
    }

    /// The `InsertVertex` replay arm is a hand-written twin of
    /// `apply_vertex_write` rather than a call to it, so it can drift — and it
    /// had: the timestamps were dropped, so a vertex recovered from the WAL
    /// came back with a null `created_at`/`updated_at` that the next flush
    /// baked into L1. This pins the fields the two paths must agree on, so the
    /// next field added to one and forgotten in the other fails here instead of
    /// silently reaching users.
    #[test]
    fn replay_reconstructs_what_the_live_vertex_write_produced() -> Result<()> {
        let vid = Vid::new(1);
        let mut props = HashMap::new();
        props.insert("name".to_string(), "ada".into());
        let labels = vec!["Person".to_string()];

        let mut live = L0Buffer::new(0, None);
        live.insert_vertex_with_labels(vid, props.clone(), &labels);

        // Fed the record the live path appends, replay must land in the same
        // place. The timestamps come from `live` because that is exactly what
        // the WAL now carries.
        let mut replayed = L0Buffer::new(0, None);
        replayed.replay_mutations(vec![Mutation::InsertVertex {
            vid,
            properties: props,
            labels,
            created_at: live.vertex_created_at.get(&vid).copied(),
            updated_at: live.vertex_updated_at.get(&vid).copied(),
        }])?;

        assert_eq!(
            replayed.vertex_properties.get(&vid),
            live.vertex_properties.get(&vid)
        );
        assert_eq!(
            replayed.vertex_labels.get(&vid),
            live.vertex_labels.get(&vid)
        );
        assert_eq!(
            replayed.vertex_versions.get(&vid),
            live.vertex_versions.get(&vid)
        );
        assert_eq!(
            replayed.vertex_created_at.get(&vid),
            live.vertex_created_at.get(&vid),
            "replay dropped created_at"
        );
        assert_eq!(
            replayed.vertex_updated_at.get(&vid),
            live.vertex_updated_at.get(&vid),
            "replay dropped updated_at"
        );
        assert_eq!(
            replayed.label_to_vids.get("Person"),
            live.label_to_vids.get("Person")
        );
        assert!(replayed.graph.contains_vertex(vid));
        Ok(())
    }

    /// Edge counterpart of the above. The edge replay arm *does* share
    /// `apply_edge_insertion` with the live path, so only what sits outside
    /// that helper can drift — which is precisely where the timestamps live.
    #[test]
    fn replay_reconstructs_what_the_live_edge_write_produced() -> Result<()> {
        let (src, dst, eid) = (Vid::new(1), Vid::new(2), Eid::new(10));

        let mut live = L0Buffer::new(0, None);
        live.insert_vertex(src, HashMap::new());
        live.insert_vertex(dst, HashMap::new());
        live.insert_edge(src, dst, 0, eid, HashMap::new(), Some("KNOWS".to_string()))?;

        let mut replayed = L0Buffer::new(0, None);
        replayed.replay_mutations(vec![
            Mutation::InsertVertex {
                vid: src,
                properties: HashMap::new(),
                labels: vec![],
                created_at: live.vertex_created_at.get(&src).copied(),
                updated_at: live.vertex_updated_at.get(&src).copied(),
            },
            Mutation::InsertVertex {
                vid: dst,
                properties: HashMap::new(),
                labels: vec![],
                created_at: live.vertex_created_at.get(&dst).copied(),
                updated_at: live.vertex_updated_at.get(&dst).copied(),
            },
            Mutation::InsertEdge {
                src_vid: src,
                dst_vid: dst,
                edge_type: 0,
                eid,
                version: live.edge_versions.get(&eid).copied().unwrap_or(0),
                properties: HashMap::new(),
                edge_type_name: Some("KNOWS".to_string()),
                created_at: live.edge_created_at.get(&eid).copied(),
                updated_at: live.edge_updated_at.get(&eid).copied(),
            },
        ])?;

        assert_eq!(
            replayed.edge_endpoints.get(&eid),
            live.edge_endpoints.get(&eid)
        );
        assert_eq!(replayed.edge_types.get(&eid), live.edge_types.get(&eid));
        assert_eq!(
            replayed.edge_created_at.get(&eid),
            live.edge_created_at.get(&eid),
            "replay dropped created_at"
        );
        assert_eq!(
            replayed.edge_updated_at.get(&eid),
            live.edge_updated_at.get(&eid),
            "replay dropped updated_at"
        );
        Ok(())
    }

    #[test]
    fn test_l0_buffer_ops() -> Result<()> {
        let mut l0 = L0Buffer::new(0, None);
        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        let eid_ab = Eid::new(101);

        l0.insert_edge(vid_a, vid_b, 1, eid_ab, HashMap::new(), None)?;

        let neighbors = l0.get_neighbors(vid_a, 1, Direction::Outgoing);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].0, vid_b);
        assert_eq!(neighbors[0].1, eid_ab);

        l0.delete_edge(eid_ab, vid_a, vid_b, 1)?;
        assert!(l0.is_tombstoned(eid_ab));

        // Verify neighbors are empty after deletion
        let neighbors_after = l0.get_neighbors(vid_a, 1, Direction::Outgoing);
        assert_eq!(neighbors_after.len(), 0);

        Ok(())
    }

    /// Regression for review #5: merging an edge whose endpoint is tombstoned
    /// in the target buffer must be rejected up front (`validate_merge_edge_endpoints`)
    /// and `merge` must be atomic — never partially applied. Before the fix this
    /// bailed only *inside* `merge`, after the durable WAL flush, leaving a ghost
    /// commit that made the database unopenable on replay.
    #[test]
    fn validate_merge_rejects_edge_to_tombstoned_endpoint() {
        let mut main = L0Buffer::new(0, None);
        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        main.insert_vertex(vid_a, HashMap::new());
        main.insert_vertex(vid_b, HashMap::new());
        main.delete_vertex(vid_b).unwrap(); // B is now tombstoned in main

        // A transaction that inserts an edge A -> B (B tombstoned in main).
        let mut tx = L0Buffer::new(0, None);
        let eid = Eid::new(101);
        tx.insert_edge(vid_a, vid_b, 1, eid, HashMap::new(), None)
            .unwrap();

        assert!(
            main.validate_merge_edge_endpoints(&tx).is_err(),
            "edge to a tombstoned endpoint must be rejected before merge"
        );
        // merge validates first, so it errors and leaves main untouched (atomic).
        assert!(
            main.merge(&tx).is_err(),
            "merge must reject, not bail mid-apply"
        );
        assert!(
            !main.edge_endpoints.contains_key(&eid),
            "a rejected merge must not have partially applied the edge"
        );
    }

    /// When the transaction re-inserts the endpoint vertex, the edge is valid
    /// (the insert clears the tombstone) and the merge succeeds.
    #[test]
    fn validate_merge_allows_edge_when_endpoint_reinserted() {
        let mut main = L0Buffer::new(0, None);
        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        main.insert_vertex(vid_a, HashMap::new());
        main.insert_vertex(vid_b, HashMap::new());
        main.delete_vertex(vid_b).unwrap();

        let mut tx = L0Buffer::new(0, None);
        tx.insert_vertex(vid_b, HashMap::new()); // re-insert B
        let eid = Eid::new(101);
        tx.insert_edge(vid_a, vid_b, 1, eid, HashMap::new(), None)
            .unwrap();

        assert!(main.validate_merge_edge_endpoints(&tx).is_ok());
        assert!(main.merge(&tx).is_ok());
        assert!(main.edge_endpoints.contains_key(&eid));
    }

    /// Edges between live endpoints merge as before — no false positives.
    #[test]
    fn validate_merge_allows_edge_to_live_endpoints() {
        let mut main = L0Buffer::new(0, None);
        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        main.insert_vertex(vid_a, HashMap::new());
        main.insert_vertex(vid_b, HashMap::new());

        let mut tx = L0Buffer::new(0, None);
        let eid = Eid::new(101);
        tx.insert_edge(vid_a, vid_b, 1, eid, HashMap::new(), None)
            .unwrap();

        assert!(main.validate_merge_edge_endpoints(&tx).is_ok());
        assert!(main.merge(&tx).is_ok());
        assert!(main.edge_endpoints.contains_key(&eid));
    }

    #[test]
    fn test_l0_buffer_multiple_edges() -> Result<()> {
        let mut l0 = L0Buffer::new(0, None);
        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        let vid_c = Vid::new(3);
        let eid_ab = Eid::new(101);
        let eid_ac = Eid::new(102);

        l0.insert_edge(vid_a, vid_b, 1, eid_ab, HashMap::new(), None)?;
        l0.insert_edge(vid_a, vid_c, 1, eid_ac, HashMap::new(), None)?;

        let neighbors = l0.get_neighbors(vid_a, 1, Direction::Outgoing);
        assert_eq!(neighbors.len(), 2);

        // Delete one edge
        l0.delete_edge(eid_ab, vid_a, vid_b, 1)?;

        // Should still have one neighbor
        let neighbors_after = l0.get_neighbors(vid_a, 1, Direction::Outgoing);
        assert_eq!(neighbors_after.len(), 1);
        assert_eq!(neighbors_after[0].0, vid_c);

        Ok(())
    }

    #[test]
    fn test_l0_buffer_edge_type_filter() -> Result<()> {
        let mut l0 = L0Buffer::new(0, None);
        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        let vid_c = Vid::new(3);
        let eid_ab = Eid::new(101);
        let eid_ac = Eid::new(201); // Different edge type

        l0.insert_edge(vid_a, vid_b, 1, eid_ab, HashMap::new(), None)?;
        l0.insert_edge(vid_a, vid_c, 2, eid_ac, HashMap::new(), None)?;

        // Filter by edge type 1
        let type1_neighbors = l0.get_neighbors(vid_a, 1, Direction::Outgoing);
        assert_eq!(type1_neighbors.len(), 1);
        assert_eq!(type1_neighbors[0].0, vid_b);

        // Filter by edge type 2
        let type2_neighbors = l0.get_neighbors(vid_a, 2, Direction::Outgoing);
        assert_eq!(type2_neighbors.len(), 1);
        assert_eq!(type2_neighbors[0].0, vid_c);

        Ok(())
    }

    #[test]
    fn test_l0_buffer_incoming_edges() -> Result<()> {
        let mut l0 = L0Buffer::new(0, None);
        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        let vid_c = Vid::new(3);
        let eid_ab = Eid::new(101);
        let eid_cb = Eid::new(102);

        // a -> b and c -> b
        l0.insert_edge(vid_a, vid_b, 1, eid_ab, HashMap::new(), None)?;
        l0.insert_edge(vid_c, vid_b, 1, eid_cb, HashMap::new(), None)?;

        // Check incoming edges to b
        let incoming = l0.get_neighbors(vid_b, 1, Direction::Incoming);
        assert_eq!(incoming.len(), 2);

        Ok(())
    }

    /// Regression test: merge should preserve edges without properties
    #[test]
    fn test_merge_empty_props_edge() -> Result<()> {
        let mut main_l0 = L0Buffer::new(0, None);
        let mut tx_l0 = L0Buffer::new(0, None);

        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        let eid_ab = Eid::new(101);

        // Insert edge with empty properties in transaction L0
        tx_l0.insert_edge(vid_a, vid_b, 1, eid_ab, HashMap::new(), None)?;

        // Verify edge exists in tx_l0
        assert!(tx_l0.edge_endpoints.contains_key(&eid_ab));
        assert!(!tx_l0.edge_properties.contains_key(&eid_ab)); // No properties entry

        // Merge into main L0
        main_l0.merge(&tx_l0)?;

        // Edge should exist in main L0 after merge
        assert!(main_l0.edge_endpoints.contains_key(&eid_ab));
        let neighbors = main_l0.get_neighbors(vid_a, 1, Direction::Outgoing);
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].0, vid_b);

        Ok(())
    }

    /// Regression test: WAL replay should use CRDT merge semantics
    #[test]
    fn test_replay_crdt_merge() -> Result<()> {
        use crate::runtime::wal::Mutation;
        use serde_json::json;
        use uni_common::Value;

        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // Create GCounter CRDT values using correct serde format:
        // {"t": "gc", "d": {"counts": {...}}}
        let counter1: Value = json!({
            "t": "gc",
            "d": {"counts": {"node1": 5}}
        })
        .into();
        let counter2: Value = json!({
            "t": "gc",
            "d": {"counts": {"node2": 3}}
        })
        .into();

        // First mutation: insert vertex with counter1
        let mut props1 = HashMap::new();
        props1.insert("counter".to_string(), counter1.clone());
        l0.replay_mutations(vec![Mutation::InsertVertex {
            vid,
            properties: props1,
            labels: vec![],
            created_at: None,
            updated_at: None,
        }])?;

        // Second mutation: insert same vertex with counter2 (should merge)
        let mut props2 = HashMap::new();
        props2.insert("counter".to_string(), counter2.clone());
        l0.replay_mutations(vec![Mutation::InsertVertex {
            vid,
            properties: props2,
            labels: vec![],
            created_at: None,
            updated_at: None,
        }])?;

        // Verify CRDT was merged (both node1 and node2 counts present)
        let stored_props = l0.vertex_properties.get(&vid).unwrap();
        let stored_counter = stored_props.get("counter").unwrap();

        // Convert back to serde_json::Value for nested access
        let stored_json: serde_json::Value = stored_counter.clone().into();
        // The merged counter should have both node1: 5 and node2: 3
        let data = stored_json.get("d").unwrap();
        let counts = data.get("counts").unwrap();
        assert_eq!(counts.get("node1"), Some(&json!(5)));
        assert_eq!(counts.get("node2"), Some(&json!(3)));

        Ok(())
    }

    #[test]
    fn test_merge_preserves_vertex_timestamps() -> Result<()> {
        let mut l0_main = L0Buffer::new(0, None);
        let mut l0_tx = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // Main buffer: insert vertex with timestamp T1
        let ts_main_created = 1000;
        let ts_main_updated = 1100;
        l0_main.insert_vertex(vid, HashMap::new());
        l0_main.vertex_created_at.insert(vid, ts_main_created);
        l0_main.vertex_updated_at.insert(vid, ts_main_updated);

        // Transaction buffer: update same vertex with timestamp T2 (later)
        let ts_tx_created = 2000; // should be ignored (main has older created_at)
        let ts_tx_updated = 2100; // should win (tx has newer updated_at)
        l0_tx.insert_vertex(vid, HashMap::new());
        l0_tx.vertex_created_at.insert(vid, ts_tx_created);
        l0_tx.vertex_updated_at.insert(vid, ts_tx_updated);

        // Merge transaction into main
        l0_main.merge(&l0_tx)?;

        // Verify created_at is oldest (from main)
        assert_eq!(
            *l0_main.vertex_created_at.get(&vid).unwrap(),
            ts_main_created,
            "created_at should preserve oldest timestamp"
        );

        // Verify updated_at is latest (from tx)
        assert_eq!(
            *l0_main.vertex_updated_at.get(&vid).unwrap(),
            ts_tx_updated,
            "updated_at should use latest timestamp"
        );

        Ok(())
    }

    #[test]
    fn test_merge_preserves_edge_timestamps() -> Result<()> {
        let mut l0_main = L0Buffer::new(0, None);
        let mut l0_tx = L0Buffer::new(0, None);
        let vid_a = Vid::new(1);
        let vid_b = Vid::new(2);
        let eid = Eid::new(100);

        // Main buffer: insert edge with timestamp T1
        let ts_main_created = 1000;
        let ts_main_updated = 1100;
        l0_main.insert_edge(vid_a, vid_b, 1, eid, HashMap::new(), None)?;
        l0_main.edge_created_at.insert(eid, ts_main_created);
        l0_main.edge_updated_at.insert(eid, ts_main_updated);

        // Transaction buffer: update same edge with timestamp T2 (later)
        let ts_tx_created = 2000; // should be ignored
        let ts_tx_updated = 2100; // should win
        l0_tx.insert_edge(vid_a, vid_b, 1, eid, HashMap::new(), None)?;
        l0_tx.edge_created_at.insert(eid, ts_tx_created);
        l0_tx.edge_updated_at.insert(eid, ts_tx_updated);

        // Merge transaction into main
        l0_main.merge(&l0_tx)?;

        // Verify created_at is oldest (from main)
        assert_eq!(
            *l0_main.edge_created_at.get(&eid).unwrap(),
            ts_main_created,
            "edge created_at should preserve oldest timestamp"
        );

        // Verify updated_at is latest (from tx)
        assert_eq!(
            *l0_main.edge_updated_at.get(&eid).unwrap(),
            ts_tx_updated,
            "edge updated_at should use latest timestamp"
        );

        Ok(())
    }

    #[test]
    fn test_merge_created_at_not_overwritten_for_existing_vertex() -> Result<()> {
        use uni_common::Value;

        let mut l0_main = L0Buffer::new(0, None);
        let mut l0_tx = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // Main buffer: vertex created at T1
        let ts_original = 1000;
        l0_main.insert_vertex(vid, HashMap::new());
        l0_main.vertex_created_at.insert(vid, ts_original);
        l0_main.vertex_updated_at.insert(vid, ts_original);

        // Transaction buffer: update vertex (created_at would be T2 if set)
        let ts_tx = 2000;
        let mut props = HashMap::new();
        props.insert("updated".to_string(), Value::String("yes".to_string()));
        l0_tx.insert_vertex(vid, props);
        l0_tx.vertex_created_at.insert(vid, ts_tx);
        l0_tx.vertex_updated_at.insert(vid, ts_tx);

        // Merge transaction into main
        l0_main.merge(&l0_tx)?;

        // Verify created_at was NOT overwritten (still T1, not T2)
        assert_eq!(
            *l0_main.vertex_created_at.get(&vid).unwrap(),
            ts_original,
            "created_at must not be overwritten for existing vertex"
        );

        // Verify updated_at WAS updated (now T2)
        assert_eq!(
            *l0_main.vertex_updated_at.get(&vid).unwrap(),
            ts_tx,
            "updated_at should reflect transaction timestamp"
        );

        // Verify properties were merged
        assert!(
            l0_main
                .vertex_properties
                .get(&vid)
                .unwrap()
                .contains_key("updated")
        );

        Ok(())
    }

    /// Test for Issue #23: Vertex labels preserved through replay_mutations
    #[test]
    fn test_replay_mutations_preserves_vertex_labels() -> Result<()> {
        use crate::runtime::wal::Mutation;

        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(42);

        // Create InsertVertex mutation with labels
        let mutations = vec![Mutation::InsertVertex {
            vid,
            properties: {
                let mut props = HashMap::new();
                props.insert(
                    "name".to_string(),
                    uni_common::Value::String("Alice".to_string()),
                );
                props
            },
            labels: vec!["Person".to_string(), "User".to_string()],
            created_at: None,
            updated_at: None,
        }];

        // Replay mutations
        l0.replay_mutations(mutations)?;

        // Verify vertex exists in L0
        assert!(l0.vertex_properties.contains_key(&vid));

        // Verify labels are preserved
        let labels = l0.get_vertex_labels(vid).expect("Labels should exist");
        assert_eq!(labels.len(), 2);
        assert!(labels.contains(&"Person".to_string()));
        assert!(labels.contains(&"User".to_string()));

        // Verify vertex is findable by label
        let person_vids = l0.vids_for_label("Person");
        assert_eq!(person_vids.len(), 1);
        assert_eq!(person_vids[0], vid);

        let user_vids = l0.vids_for_label("User");
        assert_eq!(user_vids.len(), 1);
        assert_eq!(user_vids[0], vid);

        Ok(())
    }

    /// Test for Issue #23: DeleteVertex labels preserved for tombstone flushing
    #[test]
    fn test_replay_mutations_preserves_delete_vertex_labels() -> Result<()> {
        use crate::runtime::wal::Mutation;

        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(99);

        // First insert vertex with labels
        l0.insert_vertex_with_labels(
            vid,
            HashMap::new(),
            &["Person".to_string(), "Admin".to_string()],
        );

        // Verify vertex and labels exist
        assert!(l0.vertex_properties.contains_key(&vid));
        let labels = l0.get_vertex_labels(vid).expect("Labels should exist");
        assert_eq!(labels.len(), 2);

        // Create DeleteVertex mutation with labels
        let mutations = vec![Mutation::DeleteVertex {
            vid,
            labels: vec!["Person".to_string(), "Admin".to_string()],
        }];

        // Replay deletion
        l0.replay_mutations(mutations)?;

        // Verify vertex is tombstoned
        assert!(l0.vertex_tombstones.contains(&vid));

        // Verify labels are preserved in L0 (needed for Issue #76 tombstone flushing)
        // The labels should still be accessible for the flush logic to know which tables to update
        let labels = l0.get_vertex_labels(vid);
        assert!(
            labels.is_some(),
            "Labels should be preserved even after deletion for tombstone flushing"
        );

        Ok(())
    }

    /// Test for Issue #28: Edge type name preserved through replay_mutations
    #[test]
    fn test_replay_mutations_preserves_edge_type_name() -> Result<()> {
        use crate::runtime::wal::Mutation;

        let mut l0 = L0Buffer::new(0, None);
        let src = Vid::new(1);
        let dst = Vid::new(2);
        let eid = Eid::new(500);
        let edge_type = 100;

        // Create InsertEdge mutation with edge_type_name
        let mutations = vec![Mutation::InsertEdge {
            src_vid: src,
            dst_vid: dst,
            edge_type,
            eid,
            version: 1,
            properties: {
                let mut props = HashMap::new();
                props.insert("since".to_string(), uni_common::Value::Int(2020));
                props
            },
            edge_type_name: Some("KNOWS".to_string()),
            created_at: None,
            updated_at: None,
        }];

        // Replay mutations
        l0.replay_mutations(mutations)?;

        // Verify edge exists in L0
        assert!(l0.edge_endpoints.contains_key(&eid));

        // Verify edge type name is preserved
        let type_name = l0.get_edge_type(eid).expect("Edge type name should exist");
        assert_eq!(type_name, "KNOWS");

        // Verify edge is findable by type name
        let knows_eids = l0.eids_for_type("KNOWS");
        assert_eq!(knows_eids.len(), 1);
        assert_eq!(knows_eids[0], eid);

        Ok(())
    }

    /// Test for Issue #28: Edge type mapping survives multiple replay cycles
    #[test]
    fn test_edge_type_mapping_survives_multiple_replays() -> Result<()> {
        use crate::runtime::wal::Mutation;

        let mut l0 = L0Buffer::new(0, None);

        // Replay multiple edge insertions with different types
        let mutations = vec![
            Mutation::InsertEdge {
                src_vid: Vid::new(1),
                dst_vid: Vid::new(2),
                edge_type: 100,
                eid: Eid::new(1000),
                version: 1,
                properties: HashMap::new(),
                edge_type_name: Some("KNOWS".to_string()),
                created_at: None,
                updated_at: None,
            },
            Mutation::InsertEdge {
                src_vid: Vid::new(2),
                dst_vid: Vid::new(3),
                edge_type: 101,
                eid: Eid::new(1001),
                version: 2,
                properties: HashMap::new(),
                edge_type_name: Some("LIKES".to_string()),
                created_at: None,
                updated_at: None,
            },
            Mutation::InsertEdge {
                src_vid: Vid::new(3),
                dst_vid: Vid::new(1),
                edge_type: 100,
                eid: Eid::new(1002),
                version: 3,
                properties: HashMap::new(),
                edge_type_name: Some("KNOWS".to_string()),
                created_at: None,
                updated_at: None,
            },
        ];

        l0.replay_mutations(mutations)?;

        // Verify all edge type mappings are preserved
        assert_eq!(l0.get_edge_type(Eid::new(1000)), Some("KNOWS"));
        assert_eq!(l0.get_edge_type(Eid::new(1001)), Some("LIKES"));
        assert_eq!(l0.get_edge_type(Eid::new(1002)), Some("KNOWS"));

        // Verify edges can be queried by type
        let knows_edges = l0.eids_for_type("KNOWS");
        assert_eq!(knows_edges.len(), 2);
        assert!(knows_edges.contains(&Eid::new(1000)));
        assert!(knows_edges.contains(&Eid::new(1002)));

        let likes_edges = l0.eids_for_type("LIKES");
        assert_eq!(likes_edges.len(), 1);
        assert_eq!(likes_edges[0], Eid::new(1001));

        Ok(())
    }

    /// Test for Issue #23 + #28: Combined vertex labels and edge types in replay
    #[test]
    fn test_replay_mutations_combined_labels_and_edge_types() -> Result<()> {
        use crate::runtime::wal::Mutation;

        let mut l0 = L0Buffer::new(0, None);
        let alice = Vid::new(1);
        let bob = Vid::new(2);
        let eid = Eid::new(100);

        // Simulate crash recovery scenario: replay full transaction log
        let mutations = vec![
            // Insert Alice with Person label
            Mutation::InsertVertex {
                vid: alice,
                properties: {
                    let mut props = HashMap::new();
                    props.insert(
                        "name".to_string(),
                        uni_common::Value::String("Alice".to_string()),
                    );
                    props
                },
                labels: vec!["Person".to_string()],
                created_at: None,
                updated_at: None,
            },
            // Insert Bob with Person label
            Mutation::InsertVertex {
                vid: bob,
                properties: {
                    let mut props = HashMap::new();
                    props.insert(
                        "name".to_string(),
                        uni_common::Value::String("Bob".to_string()),
                    );
                    props
                },
                labels: vec!["Person".to_string()],
                created_at: None,
                updated_at: None,
            },
            // Create KNOWS edge between them
            Mutation::InsertEdge {
                src_vid: alice,
                dst_vid: bob,
                edge_type: 1,
                eid,
                version: 3,
                properties: HashMap::new(),
                edge_type_name: Some("KNOWS".to_string()),
                created_at: None,
                updated_at: None,
            },
        ];

        // Replay all mutations
        l0.replay_mutations(mutations)?;

        // Verify vertex labels preserved
        assert_eq!(l0.get_vertex_labels(alice).unwrap().len(), 1);
        assert_eq!(l0.get_vertex_labels(bob).unwrap().len(), 1);
        assert_eq!(l0.vids_for_label("Person").len(), 2);

        // Verify edge type name preserved
        assert_eq!(l0.get_edge_type(eid).unwrap(), "KNOWS");
        assert_eq!(l0.eids_for_type("KNOWS").len(), 1);

        // Verify graph structure
        let alice_neighbors = l0.get_neighbors(alice, 1, Direction::Outgoing);
        assert_eq!(alice_neighbors.len(), 1);
        assert_eq!(alice_neighbors[0].0, bob);

        Ok(())
    }

    /// Test for Issue #23: Empty labels should deserialize correctly (backward compat)
    #[test]
    fn test_replay_mutations_backward_compat_empty_labels() -> Result<()> {
        use crate::runtime::wal::Mutation;

        let mut l0 = L0Buffer::new(0, None);
        let vid = Vid::new(1);

        // Simulate old WAL format: InsertVertex with empty labels
        // (This tests #[serde(default)] behavior)
        let mutations = vec![Mutation::InsertVertex {
            vid,
            properties: HashMap::new(),
            labels: vec![], // Empty labels (old format compatibility)
            created_at: None,
            updated_at: None,
        }];

        l0.replay_mutations(mutations)?;

        // Vertex should exist
        assert!(l0.vertex_properties.contains_key(&vid));

        // Labels should be empty but entry should exist in vertex_labels
        let labels = l0.get_vertex_labels(vid);
        assert!(labels.is_some(), "Labels entry should exist even if empty");
        assert_eq!(labels.unwrap().len(), 0);

        Ok(())
    }

    #[test]
    fn test_now_nanos_returns_nanosecond_range() {
        // Test that now_nanos() returns a value in nanosecond range
        // As of 2025, Unix timestamp in nanoseconds should be > 1.7e18
        // (2025-01-01 is approximately 1,735,689,600 seconds = 1.735e18 nanoseconds)
        let now = now_nanos();

        // Verify it's in nanosecond range (not microseconds which would be 1000x smaller)
        assert!(
            now > 1_700_000_000_000_000_000,
            "now_nanos() returned {}, expected > 1.7e18 for nanoseconds",
            now
        );

        // Sanity check: should also be less than year 2100 in nanoseconds (4.1e18)
        assert!(
            now < 4_100_000_000_000_000_000,
            "now_nanos() returned {}, expected < 4.1e18",
            now
        );
    }
}
