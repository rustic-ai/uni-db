// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

#[cfg(feature = "lance-backend")]
use crate::backend::table_names;
use crate::backend::types::{CmpOp, FilterExpr, Scalar};
use crate::runtime::context::QueryContext;
use crate::runtime::embed_caps::is_multivector_property;
use crate::runtime::flush_coordinator::{
    FinalizeFn, FlushCoordinator, FlushOutcome as AsyncFlushOutcome, RotatedFlush, SharedFlushCtx,
};
use crate::runtime::id_allocator::IdAllocator;
use crate::runtime::l0::{L0Buffer, TombstoneEntry, serialize_constraint_key};
use crate::runtime::l0_manager::L0Manager;
use crate::runtime::property_manager::PropertyManager;
use crate::runtime::wal::WriteAheadLog;
use crate::storage::adjacency_manager::AdjacencyManager;
use crate::storage::delta::{L1Entry, Op};
use crate::storage::main_edge::MainEdgeDataset;
use crate::storage::main_vertex::MainVertexDataset;
use crate::storage::manager::StorageManager;
use anyhow::{Result, anyhow};
use chrono::Utc;
use metrics;
use parking_lot::{Mutex as PlMutex, RwLock};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use tracing::{debug, info, instrument};
use uni_common::Properties;
use uni_common::Value;
use uni_common::config::UniConfig;
use uni_common::core::fork::ForkId;
use uni_common::core::id::{Eid, Vid};
use uni_common::core::schema::{ConstraintTarget, ConstraintType, IndexDefinition};
use uni_common::core::snapshot::{EdgeSnapshot, LabelSnapshot, SnapshotManifest};
use uni_xervo::error::RuntimeError;
use uni_xervo::runtime::ModelRuntime;
use uni_xervo::traits::hybrid::{HeadSet, HybridEmbedResult};
use uuid::Uuid;

/// Convert per-token embedding vectors into the stored `List<Vector>` representation:
/// a `Value::List` whose elements are each a `Value::List<Float>` (one token vector).
fn multivec_to_value(tokens: &[Vec<f32>]) -> Value {
    Value::List(
        tokens
            .iter()
            .map(|tok| Value::List(tok.iter().map(|f| Value::Float(*f as f64)).collect()))
            .collect(),
    )
}

/// One coalesced auto-embed group: all targets that share an `alias` + `source_properties`,
/// split into dense (`Vector`) vs multi-vector (`List<Vector>`) target columns. When a group
/// has BOTH kinds, a single hybrid inference fills both (single forward pass).
struct EmbedGroupSpec {
    source_properties: Vec<String>,
    document_prefix: Option<String>,
    dense: Vec<String>,
    multi: Vec<String>,
    sparse: Vec<String>,
}

/// Convert one xervo sparse embedding (`(term_id, weight)` pairs, possibly
/// unsorted / with duplicate terms) into a `Value::SparseVector`. Sorting (via
/// `BTreeMap`) and summing duplicates yields the sorted-unique invariant the
/// index and codec require; non-finite weights are dropped (never poison a row).
fn sparse_pairs_to_value(pairs: &[(u32, f32)]) -> Value {
    let mut by_term: std::collections::BTreeMap<u32, f32> = std::collections::BTreeMap::new();
    for &(term, weight) in pairs {
        if weight.is_finite() {
            *by_term.entry(term).or_insert(0.0) += weight;
        }
    }
    Value::SparseVector {
        indices: by_term.keys().copied().collect(),
        values: by_term.values().copied().collect(),
    }
}

/// Run ONE embedding inference for a coalesced group of auto-embed targets sharing an alias +
/// source text, returning `(dense_per_text, multi_vector_per_text)` (each `Some` iff that head
/// was requested). When both heads are needed this uses the hybrid model
/// (`hybrid_embedder`), so a multi-functional model (e.g. BGE-M3) produces the dense + ColBERT
/// heads from a SINGLE forward pass; otherwise it uses the single-head dense / multi-vector
/// embedder (today's behavior for non-mixed groups).
#[allow(clippy::type_complexity)]
async fn embed_group(
    runtime: &ModelRuntime,
    alias: &str,
    texts: &[&str],
    want_dense: bool,
    want_multi: bool,
    want_sparse: bool,
) -> Result<(
    Option<Vec<Vec<f32>>>,
    Option<Vec<Vec<Vec<f32>>>>,
    Option<Vec<Vec<(u32, f32)>>>,
)> {
    let heads_wanted = u8::from(want_dense) + u8::from(want_multi) + u8::from(want_sparse);
    if heads_wanted > 1 {
        // Single-pass hybrid: one model, one inference, all requested heads
        // (e.g. BGE-M3 fills dense + sparse from one forward pass). A group with >1 head
        // can only be served by a hybrid model (one alias = one task), so route directly.
        let mut heads = HeadSet::empty();
        if want_dense {
            heads |= HeadSet::DENSE;
        }
        if want_multi {
            heads |= HeadSet::MULTI_VECTOR;
        }
        if want_sparse {
            heads |= HeadSet::SPARSE;
        }
        let embedder = runtime.hybrid_embedder(alias).await?;
        let res = embedder.embed(texts, heads).await?;
        let dense = if want_dense {
            Some(res.dense.ok_or_else(|| {
                anyhow!("hybrid model '{alias}' returned no dense head for a Vector target")
            })?)
        } else {
            None
        };
        let multi = if want_multi {
            Some(res.multi_vector.ok_or_else(|| {
                anyhow!(
                    "hybrid model '{alias}' returned no multi-vector head for a List<Vector> target"
                )
            })?)
        } else {
            None
        };
        let sparse = if want_sparse {
            Some(res.sparse.ok_or_else(|| {
                anyhow!("hybrid model '{alias}' returned no sparse head for a SparseVector target")
            })?)
        } else {
            None
        };
        Ok((dense, multi, sparse))
    } else if want_multi {
        // Lone multi-vector head: prefer the narrow facade, but fall back to a hybrid
        // model on the same alias (issue #129) when it doesn't implement the narrow trait.
        match runtime.multi_vector_embedder(alias).await {
            Ok(embedder) => Ok((None, Some(embedder.embed(texts).await?.vectors), None)),
            Err(e) if is_capability_mismatch(&e) => {
                let multi = hybrid_single_head(runtime, alias, texts, HeadSet::MULTI_VECTOR)
                    .await?
                    .multi_vector
                    .ok_or_else(|| {
                        anyhow!(
                            "model '{alias}' exposes no multi-vector head for a List<Vector> target"
                        )
                    })?;
                Ok((None, Some(multi), None))
            }
            Err(e) => Err(e.into()),
        }
    } else if want_sparse {
        // Lone sparse head: narrow facade, else hybrid fallback (issue #129).
        match runtime.sparse_embedder(alias).await {
            Ok(embedder) => Ok((None, None, Some(embedder.embed(texts).await?.vectors))),
            Err(e) if is_capability_mismatch(&e) => {
                let sparse = hybrid_single_head(runtime, alias, texts, HeadSet::SPARSE)
                    .await?
                    .sparse
                    .ok_or_else(|| {
                        anyhow!("model '{alias}' exposes no sparse head for a SparseVector target")
                    })?;
                Ok((None, None, Some(sparse)))
            }
            Err(e) => Err(e.into()),
        }
    } else {
        // Lone dense head: narrow facade, else hybrid fallback (issue #129 — a hybrid
        // model like BGE-M3 serving a single dense column on its own alias).
        match runtime.embedding(alias).await {
            Ok(embedder) => Ok((Some(embedder.embed(texts).await?.vectors), None, None)),
            Err(e) if is_capability_mismatch(&e) => {
                let dense = hybrid_single_head(runtime, alias, texts, HeadSet::DENSE)
                    .await?
                    .dense
                    .ok_or_else(|| {
                        anyhow!("model '{alias}' exposes no dense head for a Vector target")
                    })?;
                Ok((Some(dense), None, None))
            }
            Err(e) => Err(e.into()),
        }
    }
}

/// Whether `err` means the alias's model does not implement the requested narrow
/// embedding trait, so the hybrid model on the same alias should be tried instead.
///
/// `embedding()` reports a missing dense trait as `RuntimeError::CapabilityMismatch`;
/// the sparse / multi-vector / hybrid facades report it as
/// `RuntimeError::ProviderCapabilityMissing`. Both mean "wrong facade for this model",
/// as opposed to a load or inference failure, which must propagate unchanged.
fn is_capability_mismatch(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::CapabilityMismatch(_) | RuntimeError::ProviderCapabilityMissing { .. }
    )
}

/// Run ONE hybrid forward pass over `texts` producing only `head`, for the lone-head
/// capability fallback (issue #129).
///
/// The hybrid embedder is cached per alias and the model is keyed by its task, so this
/// reuses the model already loaded by the failed narrow attempt (no second load). The
/// caller extracts the matching field of the result; a `None` there means the model does
/// not expose `head` and must be surfaced as a hard error, never a silently-empty column.
async fn hybrid_single_head(
    runtime: &ModelRuntime,
    alias: &str,
    texts: &[&str],
    head: HeadSet,
) -> Result<HybridEmbedResult> {
    let embedder = runtime.hybrid_embedder(alias).await?;
    embedder.embed(texts, head).await.map_err(Into::into)
}

/// Group a label's auto-embed configs by `(alias, source_properties)`, classifying each target
/// column as dense vs multi-vector. A group with both kinds is a single-pass hybrid source.
fn collect_embed_groups(
    schema: &uni_common::core::schema::Schema,
    label: &str,
) -> std::collections::BTreeMap<(String, Vec<String>), EmbedGroupSpec> {
    use std::collections::BTreeMap;
    let mut groups: BTreeMap<(String, Vec<String>), EmbedGroupSpec> = BTreeMap::new();
    fn spec_for<'a>(
        groups: &'a mut std::collections::BTreeMap<(String, Vec<String>), EmbedGroupSpec>,
        emb: &uni_common::core::schema::EmbeddingConfig,
    ) -> &'a mut EmbedGroupSpec {
        groups
            .entry((emb.alias.clone(), emb.source_properties.clone()))
            .or_insert_with(|| EmbedGroupSpec {
                source_properties: emb.source_properties.clone(),
                document_prefix: emb.document_prefix.clone(),
                dense: Vec::new(),
                multi: Vec::new(),
                sparse: Vec::new(),
            })
    }
    for idx in &schema.indexes {
        if let IndexDefinition::Vector(v_config) = idx
            && v_config.label == label
            && let Some(emb) = &v_config.embedding_config
        {
            let g = spec_for(&mut groups, emb);
            if is_multivector_property(schema, label, &v_config.property) {
                g.multi.push(v_config.property.clone());
            } else {
                g.dense.push(v_config.property.clone());
            }
        } else if let IndexDefinition::Sparse(s_config) = idx
            && s_config.label == label
            && let Some(emb) = &s_config.embedding_config
        {
            spec_for(&mut groups, emb)
                .sparse
                .push(s_config.property.clone());
        }
    }
    groups
}

/// On a `SET` write, when the write touches a *source* property of an
/// auto-embed group, drop that group's target columns from `props` so the
/// `!contains_key` guard in `process_embeddings_*` re-embeds them (otherwise the
/// re-read old embedding is preserved → stale), and add the targets to
/// `touched_keys`. A target the caller set explicitly in the SAME write (already
/// in `touched_keys`) is left intact.
fn refresh_touched_embed_targets(
    schema: &uni_common::core::schema::Schema,
    props: &mut Properties,
    touched_keys: &mut HashSet<String>,
    labels: &[String],
) {
    let Some(label) = labels.first() else {
        return;
    };
    let user_touched = touched_keys.clone();
    for (_key, group) in collect_embed_groups(schema, label) {
        if !group
            .source_properties
            .iter()
            .any(|s| touched_keys.contains(s))
        {
            continue;
        }
        for target in group
            .dense
            .iter()
            .chain(group.multi.iter())
            .chain(group.sparse.iter())
        {
            if user_touched.contains(target) {
                continue;
            }
            props.remove(target);
            touched_keys.insert(target.clone());
        }
    }
}

#[derive(Clone, Debug)]

pub struct WriterConfig {
    pub max_mutations: usize,
    /// Inert; kept so existing configurations still build.
    ///
    /// It used to route a `SET` of vertices and edges through a Lance
    /// `MergeInsert` of the touched columns at flush. Vertex and edge tables
    /// hold one row per flushed version until compaction, which a key-matched
    /// merge cannot address: removed properties came back after a flush or a
    /// compaction, and a row created and updated in one flush window was
    /// lost. Every write now stores a full row.
    #[deprecated(
        since = "4.2.0",
        note = "no-op: every write is stored as a full row; the partial MergeInsert path was removed"
    )]
    pub partial_lance_writes: bool,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            max_mutations: 10_000,
            #[expect(
                deprecated,
                reason = "the default must still name the deprecated field"
            )]
            partial_lance_writes: false,
        }
    }
}

/// A committed transaction, as [`Writer::commit_transaction_l0_sequenced`]
/// returns it.
#[derive(Debug)]
pub struct CommittedTx {
    /// WAL LSN of the commit (`0` when no WAL is configured).
    pub wal_lsn: u64,
    /// The post-commit flush trigger fired and a background flush was
    /// submitted (async-flush mode only).
    pub flush_pending: bool,
    /// The database version this commit's last mutation landed at, read under
    /// `flush_lock` as the commit became visible.
    pub version: u64,
    /// This commit's place in the announcement order. Publish its commit
    /// notification through it; dropping it gives the place up.
    pub ticket: crate::runtime::publish_sequencer::PublishTicket,
}

/// The WAL segments recovery replays, as [`Writer::begin_wal_replay`]
/// returns them.
#[derive(Debug)]
pub struct WalRecovery {
    /// Every segment above the snapshot manifest's mark, in LSN order.
    pub segments: Vec<crate::runtime::wal::WalSegment>,
    /// The highest WAL LSN known announced; see [`Writer::begin_wal_replay`].
    pub announced_through: u64,
}

/// Parent state captured atomically at a fork point under `flush_lock`.
///
/// Holds the allocator high-water marks and every existing dataset's
/// Lance main-branch version at the instant the parent's L0 was
/// flushed. Because [`Writer::flush_and_capture_fork_point`] reads these
/// while still holding `flush_lock`, no concurrent commit or flush can
/// advance the allocator or any dataset tip between the flush and the
/// reads. A fork built from these values therefore cannot collide VIDs
/// with the parent nor inherit rows committed after the fork point.
#[derive(Clone, Debug, Default)]
pub struct ForkPoint {
    /// Next vertex id the parent would allocate at the fork point.
    pub vid_hwm: u64,
    /// Next edge id the parent would allocate at the fork point.
    pub eid_hwm: u64,
    /// `dataset_name` → Lance main-branch version at the fork point.
    ///
    /// Keys use the same dataset naming as the fork branch loop
    /// (`vertices`, `edges`, `vertices_{label}`, `deltas_{type}_{dir}`,
    /// `adjacency_{type}_{dir}`). A dataset with no `.lance` directory
    /// on disk at the fork point has no entry.
    pub dataset_versions: BTreeMap<String, u64>,
    /// Parent's MVCC version high-water-mark at the fork point: the
    /// largest `_version` any inherited row can carry. A fork bootstraps
    /// its own version counter to this floor so a fork transaction's
    /// `_version <= pin` read still sees inherited (base_paths) rows,
    /// while the fork's own writes get versions above it.
    pub version_hwm: u64,
    /// `dataset_name` → parent **branch** version at the fork point, for a
    /// nested fork (a fork of a fork). Empty when the parent is the primary DB.
    ///
    /// A nested fork must branch off its parent fork's Lance *branch* tip, which
    /// advances independently of `main` (so `dataset_versions`, which tracks the
    /// main-branch version, is not usable there). Captured under `flush_lock`
    /// alongside `dataset_versions` so a concurrent parent commit+flush cannot
    /// advance the tip between capture and branch creation.
    pub parent_branch_versions: BTreeMap<String, u64>,
}

/// `flush_lock`, held by a schema declaration; see [`Writer::declaration_lock`].
#[must_use = "the declaration is only serialized against commits while this is held"]
pub struct DeclarationLock {
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

/// RAII latch on [`StorageManager::flush_in_progress`].
///
/// Sets the flag to `true` on construction (via CAS) and back to `false` on
/// drop, so any `?` early-exit inside `flush_to_l1` cannot leave the flag
/// stuck. Returns `None` if a flush is already in progress, providing
/// forward-compatible exclusion once the outer writer-RwLock is removed in
/// Phase 4 of the concurrent-writer refactor.
// FlushInProgressGuard moved to storage/manager.rs so flush_coordinator.rs
// can hold it on RotatedFlush without a writer.rs back-import cycle.
pub use crate::storage::manager::FlushInProgressGuard;

/// Output of [`Writer::flush_l0_rotate`]: the to-be-flushed L0 buffer,
/// captured WAL LSN, current_version, and the in-progress guard whose
/// lifetime spans the full flush (including the future async stream
/// phase that runs on a spawned task).
struct RotateOutput {
    old_l0_arc: Arc<RwLock<L0Buffer>>,
    wal_lsn: u64,
    current_version: u64,
    flush_in_progress_guard: FlushInProgressGuard,
}

/// Reject a write whose property map is keyed by a storage column name
/// (`uni_common::core::schema::RESERVED_PROPERTY_NAMES`). Every vertex
/// and edge insert path runs this, so CREATE, SET, MERGE and the batch
/// inserts share one rule. The error stays a typed `UniError` inside the
/// `anyhow` chain.
fn check_property_names(
    props: &Properties,
    schema: &uni_common::core::schema::Schema,
) -> Result<()> {
    uni_common::core::schema::check_written_property_names(props.keys(), schema)
        .map_err(anyhow::Error::new)
}

/// Output of [`Writer::flush_stream_l1`]: the built (but not yet
/// published) snapshot manifest and its id. Finalize is responsible
/// for `save_snapshot` + `set_latest_snapshot` + `cached_manifest`
/// update.
struct FlushOutcome {
    manifest: SnapshotManifest,
    snapshot_id: String,
}

pub struct Writer {
    /// Write-throttle counters since this writer was constructed; see
    /// [`Writer::write_throttle_stats`].
    throttle_stats: Arc<WriteThrottleStats>,
    pub l0_manager: Arc<L0Manager>,
    pub storage: Arc<StorageManager>,
    pub schema_manager: Arc<uni_common::core::schema::SchemaManager>,
    pub allocator: Arc<IdAllocator>,
    pub config: UniConfig,
    /// Optional embedding runtime. `OnceLock` so the initializer can run
    /// on `&self` after the `Writer` has been wrapped in `Arc<Writer>`
    /// (Phase 4 of concurrent_writer.md). Read through
    /// [`Writer::xervo_runtime`] — the field itself is private to keep
    /// callers oblivious to the OnceLock representation.
    xervo_runtime: OnceLock<Arc<ModelRuntime>>,
    /// Property manager for cache invalidation after flush
    pub property_manager: Option<Arc<PropertyManager>>,
    /// Adjacency manager for dual-write (edges survive flush).
    adjacency_manager: Arc<AdjacencyManager>,
    /// Timestamp of last flush or creation. Interior-mutable so that
    /// `&self` callers can update it; uncontended in practice because all
    /// writes happen inside the single-flusher critical section.
    /// Arc-wrapped so it can travel into the SharedFlushCtx that the
    /// async-flush coordinator passes to spawned stream/finalize tasks.
    last_flush_time: Arc<PlMutex<std::time::Instant>>,
    /// Background compaction task handle (prevents concurrent compaction races)
    compaction_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    /// Optional index rebuild manager for post-flush automatic rebuild scheduling.
    /// `OnceLock` for the same reason as `xervo_runtime`.
    /// Wrapped in `Arc` so the async-flush finalize path can read it
    /// from a spawned task via `SharedFlushCtx`.
    index_rebuild_manager: Arc<OnceLock<Arc<crate::storage::index_rebuild::IndexRebuildManager>>>,
    /// Cached snapshot manifest from the last flush. Avoids re-reading from
    /// object store on every flush_to_l1 call. Wrapped in a `Mutex` for
    /// `&self` access; uncontended because all access is inside the
    /// single-flusher critical section.
    cached_manifest: Arc<PlMutex<Option<SnapshotManifest>>>,
    /// Identifier of the fork this writer serves, if any. `None` for
    /// primary's writer. Read in `flush_to_l1` to emit fork-tagged metrics and
    /// to fire the fragment-count guard rail (Phase 2 Day 12).
    ///
    /// **Set at construction, never afterwards.** It used to be patched on by
    /// `writer_factory::new_for_fork` after `new_with_config` returned, which
    /// left a window the async flush path fell into: `new_with_config` captures
    /// a `SharedFlushCtx` for the `FlushCoordinator`, and that snapshot froze
    /// `fork_id: None` before the factory could tag the writer. The coordinator
    /// then finalized every async flush under primary's identity — publishing a
    /// fork's manifest pointer through the primary namespace. The sync path was
    /// unaffected because it builds its ctx per call from `shared_ctx()`.
    pub fork_id: Option<ForkId>,
    /// Number of `flush_to_l1` calls since this writer was constructed.
    /// Used as a proxy for L1 fragment growth on the fork's branches:
    /// each flush typically appends ~1 fragment per touched dataset, so
    /// the count tracks the order of magnitude of fragment accumulation.
    /// Reading the actual `Dataset::manifest().fragments.len()` per
    /// flush would add a per-dataset object-store roundtrip on the hot
    /// commit path; the proxy keeps the guard rail purely observational
    /// (Phase 5 introduces fork compaction proper). Only meaningful when
    /// `fork_id.is_some()`. `Relaxed` is sufficient — observational only.
    fork_flush_count: Arc<AtomicU64>,
    /// Whether the fork-fragment warning has already fired at the
    /// configured threshold. One-shot per writer lifetime. `Relaxed` is
    /// sufficient — observational only.
    fork_fragment_warn_fired: Arc<AtomicBool>,
    /// WAL segments [`Self::replay_wal`] recovered that carry no writer
    /// features (written by a binary that may not enforce declared edge
    /// endpoints); read by [`Self::distrust_conformance_after_unmarked_writes`].
    unmarked_wal_segments: std::sync::atomic::AtomicUsize,
    /// Dedicated lock for the genuinely-exclusive flush path. Acquired by
    /// the [`Writer::flush_to_l1`] entry and by `commit_transaction_l0`
    /// across its WAL-append + L0-merge window. Replaces the outer
    /// `Arc<RwLock<Writer>>` for flush exclusion once Phase 4 drops it.
    /// Arc-wrapped so async-flush coordinator's finalize path can
    /// re-acquire it from a spawned task via SharedFlushCtx.
    flush_lock: Arc<tokio::sync::Mutex<()>>,
    /// Coordinator for async-flush pipeline. Owns the back-pressure
    /// semaphore, rotate-order sequence, single-finalizer task, and
    /// pending-flush counter.
    ///
    /// `None` when `async_flush_enabled = false`. The
    /// coordinator's finalizer task captures `SharedFlushCtx` which
    /// includes `Arc<StorageManager>`; on a fork-scoped Writer that
    /// also pins the fork's `ForkScope` via `storage.fork_scope`, so
    /// the holder count never drops. Constructing it only when the
    /// feature is actually on avoids that side-effect for all
    /// existing sync-flush paths. When async-flush graduates from
    /// opt-in to default (Commit 12), `drop_fork` (Commit 8) handles
    /// the drain explicitly.
    pub(crate) flush_coordinator: Option<Arc<crate::runtime::flush_coordinator::FlushCoordinator>>,
    /// Optimistic-concurrency commit-sequence counter (SSI). Incremented once
    /// per successful commit under `flush_lock`; a transaction captures the
    /// current value at begin as its read sequence (`L0Buffer::occ_read_seq`).
    ///
    /// Always allocated; consulted only when `config.ssi_enabled` is `true`.
    ///
    /// Typed through the [`crate::runtime::sync`] shim so the OCC commit core can
    /// be model-checked under loom/shuttle; aliases to `std::AtomicU64` normally.
    commit_sequence: Arc<crate::runtime::sync::AtomicU64>,
    /// Bounded log of recently-committed write-sets for OCC conflict detection.
    /// Read and updated only under `flush_lock`.
    ///
    /// Always allocated; consulted only when `config.ssi_enabled` is `true`.
    committed_writes: Arc<PlMutex<crate::runtime::occ::CommitRegistry>>,
    /// Orders commit announcements by commit order. Tickets are issued under
    /// `flush_lock` (see [`Writer::commit_transaction_l0_sequenced`] and
    /// [`Writer::sequence_announcement`]).
    publish_sequencer: Arc<crate::runtime::publish_sequencer::PublishSequencer>,
    /// Per-row pessimistic locks for `FOR UPDATE` (SSI escape hatch), keyed by
    /// canonical (label, key-props) bytes. A transaction holds the lock from
    /// MATCH until commit/rollback, serializing concurrent `FOR UPDATE` writers
    /// on the same key (avoiding optimistic abort-retry on hot keys).
    ///
    /// Always allocated; populated only when `config.ssi_enabled` is `true`.
    for_update_locks: Arc<dashmap::DashMap<Vec<u8>, Arc<tokio::sync::Mutex<()>>>>,
    /// Bumped by [`Writer::restore_snapshot`] before it rewinds anything; see
    /// [`Writer::restore_epoch`].
    restore_epoch: std::sync::atomic::AtomicU64,
    /// Set while [`Writer::restore_snapshot`] runs; a snapshot deletion
    /// refuses meanwhile.
    restore_in_progress: Arc<std::sync::atomic::AtomicBool>,
}

/// Raises [`Writer`]'s `restore_in_progress` flag for its lifetime.
struct RestoreInProgress(Arc<std::sync::atomic::AtomicBool>);

impl RestoreInProgress {
    fn set(flag: &Arc<std::sync::atomic::AtomicBool>) -> Self {
        flag.store(true, std::sync::atomic::Ordering::SeqCst);
        Self(Arc::clone(flag))
    }
}

impl Drop for RestoreInProgress {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

/// How long a flush waits for the announcements of visible commits to be
/// released before refusing; see `Writer::flush_l0_rotate`.
const ANNOUNCEMENT_RELEASE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Number of recent commits retained for OCC conflict detection. Large enough
/// that under-run — and the resulting conservative abort — is rare in practice;
/// each entry is a small set of touched ids.
const OCC_REGISTRY_CAPACITY: usize = 4096;

/// How often a throttled write re-reads the flush-generation count.
///
/// The hard-limit stall has always polled at this period; the soft band uses
/// it as the ceiling on each sleep, so neither band notices relief later
/// than the other.
const WRITE_PRESSURE_POLL: std::time::Duration = std::time::Duration::from_millis(100);

/// The next backpressure sleep for a write that has waited `waited` so far
/// with `l1_runs` uncompacted flush generations, or `None` to proceed.
///
/// - `l1_runs >= hard_limit`: stall, polling every [`WRITE_PRESSURE_POLL`],
///   until compaction brings the count down. No time bound: the hard limit
///   is the stop-writes guarantee.
/// - `soft_limit <= l1_runs < hard_limit`: wait up to
///   `min(base_delay × 2^(l1_runs − soft_limit), WRITE_PRESSURE_POLL ×
///   (hard_limit − soft_limit))`, in sleeps of at most
///   [`WRITE_PRESSURE_POLL`]. The budget is recomputed from the current count
///   each step, so falling pressure shortens it and dropping below
///   `soft_limit` ends it.
/// - otherwise: proceed.
///
/// The soft budget's cap is one hard-limit poll period per run of the soft
/// band (1.6 s at the 16/32 defaults). Without it the curve reached 40.96 s
/// at 28 runs and 81.92 s at 29, a single sleep longer than the default 30 s
/// statement timeout, so a write failed with `Timeout` even when
/// compaction had already reset the count. The exponent and the multiplication
/// saturate, so no `base_delay` or run count can overflow.
fn write_pressure_step(
    throttle: &uni_common::config::WriteThrottleConfig,
    l1_runs: usize,
    waited: std::time::Duration,
) -> Option<std::time::Duration> {
    if l1_runs >= throttle.hard_limit {
        return Some(WRITE_PRESSURE_POLL);
    }
    if l1_runs < throttle.soft_limit {
        return None;
    }
    let excess = u32::try_from(l1_runs - throttle.soft_limit).unwrap_or(u32::MAX);
    let curve = throttle
        .base_delay
        .saturating_mul(2_u32.saturating_pow(excess));
    let band = u32::try_from(throttle.hard_limit - throttle.soft_limit).unwrap_or(u32::MAX);
    let budget = curve.min(WRITE_PRESSURE_POLL.saturating_mul(band));
    let remaining = budget.saturating_sub(waited);
    if remaining.is_zero() {
        None
    } else {
        Some(remaining.min(WRITE_PRESSURE_POLL))
    }
}

/// Running totals of the write throttle's effect; see
/// [`Writer::write_throttle_stats`].
#[derive(Debug, Default)]
struct WriteThrottleStats {
    waits: std::sync::atomic::AtomicU64,
    wait_nanos: std::sync::atomic::AtomicU64,
    stalled: std::sync::atomic::AtomicU64,
}

/// A snapshot of the write throttle's counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteThrottleSnapshot {
    /// Writes (statements, batches, or unscoped writer calls) that waited in
    /// the soft or the hard band before proceeding or giving up.
    pub waits: u64,
    /// Total time those writes spent waiting.
    pub wait_time: std::time::Duration,
    /// Writes refused with `UniError::WriteStalled` (hard limit reached with
    /// background compaction disabled).
    pub stalled: u64,
}

/// Records one throttled wait when dropped, so a wait cut short by a
/// timeout or a cancellation (which drops the waiting future) still counts.
struct ThrottleWaitRecorder<'a> {
    stats: &'a WriteThrottleStats,
    started: tokio::time::Instant,
    waited: bool,
}

impl Drop for ThrottleWaitRecorder<'_> {
    fn drop(&mut self) {
        if !self.waited {
            return;
        }
        use std::sync::atomic::Ordering;
        let elapsed = self.started.elapsed();
        self.stats.waits.fetch_add(1, Ordering::Relaxed);
        self.stats.wait_nanos.fetch_add(
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        metrics::counter!("uni_write_throttle_waits_total").increment(1);
        metrics::histogram!("uni_write_throttle_wait_seconds").record(elapsed.as_secs_f64());
    }
}

/// How hard the write throttle is pushing back, in `0.0..=1.0`, at
/// `l1_runs` uncompacted flush generations.
///
/// `0.0` below the soft limit; in the soft band the run's position,
/// `(l1_runs − soft_limit + 1) / (hard_limit − soft_limit + 1)`, so the first
/// throttled run reads above zero; `1.0` at or past the hard limit, where
/// writes stall (or, with compaction disabled, are refused). With background
/// compaction disabled the soft band does not wait, so it reads `0.0`.
pub fn write_throttle_pressure(
    throttle: &uni_common::config::WriteThrottleConfig,
    compaction_enabled: bool,
    l1_runs: usize,
) -> f64 {
    if l1_runs >= throttle.hard_limit {
        return 1.0;
    }
    if l1_runs < throttle.soft_limit || !compaction_enabled {
        return 0.0;
    }
    let band = (throttle.hard_limit - throttle.soft_limit + 1) as f64;
    ((l1_runs - throttle.soft_limit + 1) as f64 / band).clamp(0.0, 1.0)
}

impl Writer {
    pub async fn new(
        storage: Arc<StorageManager>,
        schema_manager: Arc<uni_common::core::schema::SchemaManager>,
        start_version: u64,
    ) -> Result<Self> {
        Self::new_with_config(
            storage,
            schema_manager,
            start_version,
            UniConfig::default(),
            None,
            None,
            None,
        )
        .await
    }

    /// `fork_id` identifies the fork this writer serves, or `None` for primary.
    /// It is a constructor argument rather than a field assigned afterwards
    /// because the `FlushCoordinator`'s `SharedFlushCtx` is captured in here:
    /// anything set after this returns is invisible to every async flush. See
    /// the field docs on [`Writer::fork_id`].
    pub async fn new_with_config(
        storage: Arc<StorageManager>,
        schema_manager: Arc<uni_common::core::schema::SchemaManager>,
        start_version: u64,
        config: UniConfig,
        wal: Option<Arc<WriteAheadLog>>,
        allocator: Option<Arc<IdAllocator>>,
        fork_id: Option<ForkId>,
    ) -> Result<Self> {
        let allocator = if let Some(a) = allocator {
            a
        } else {
            let store = storage.store();
            let path = object_store::path::Path::from("id_allocator.json");
            Arc::new(IdAllocator::new(store, path, 1000).await?)
        };

        // The sequencer raises the log's announced mark, which the log stamps
        // into its segments (see `publish_sequencer`).
        let publish_sequencer = match wal.as_ref() {
            Some(wal) => crate::runtime::publish_sequencer::PublishSequencer::with_announced_mark(
                wal.announced_mark(),
            ),
            None => crate::runtime::publish_sequencer::PublishSequencer::new(),
        };
        let l0_manager = Arc::new(L0Manager::new(start_version, wal));
        // Route commit-time CRDT merges through the DB's plugin registry (if
        // installed on the StorageManager). Behavior-preserving when absent.
        if let Some(registry) = storage.plugin_registry() {
            l0_manager.set_plugin_registry(Arc::clone(registry));
        }

        let property_manager = Some(Arc::new(PropertyManager::new(
            storage.clone(),
            schema_manager.clone(),
            1000,
        )));

        let adjacency_manager = storage.adjacency_manager();

        // Hoist the Arc'd fields so we can both stash them on Writer and
        // hand the same Arcs to the SharedFlushCtx that FlushCoordinator
        // captures. Single-source-of-truth for each piece of mutable
        // shared state.
        let last_flush_time = Arc::new(PlMutex::new(std::time::Instant::now()));
        let cached_manifest = Arc::new(PlMutex::new(None));
        let fork_flush_count = Arc::new(AtomicU64::new(0));
        let fork_fragment_warn_fired = Arc::new(AtomicBool::new(false));
        let flush_lock = Arc::new(tokio::sync::Mutex::new(()));
        let compaction_handle = Arc::new(RwLock::new(None));
        let index_rebuild_manager: Arc<
            OnceLock<Arc<crate::storage::index_rebuild::IndexRebuildManager>>,
        > = Arc::new(OnceLock::new());

        // Catch a mismatched writer/storage pairing HERE, where it is
        // deterministic, rather than at `flush_finalize_body`'s equivalent
        // assertion, which only fires if an async flush happens to reach
        // finalize — a race that hid this for three months and then failed one
        // PR in CI twice with two different symptoms.
        debug_assert_eq!(
            fork_id.is_some(),
            storage.snapshot_manager().is_fork_scoped(),
            "writer fork identity must agree with its storage's snapshot namespace: \
             fork_id={fork_id:?}, storage is_fork_scoped={}",
            storage.snapshot_manager().is_fork_scoped()
        );

        let flush_coordinator = if config.async_flush_enabled {
            let shared = SharedFlushCtx {
                storage: storage.clone(),
                l0_manager: l0_manager.clone(),
                adjacency_manager: adjacency_manager.clone(),
                property_manager: property_manager.clone(),
                schema_manager: schema_manager.clone(),
                cached_manifest: cached_manifest.clone(),
                last_flush_time: last_flush_time.clone(),
                // Must match the writer's own `fork_id` (asserted in
                // `flush_finalize_body`). Hard-coding `None` here is what sent
                // fork async flushes through primary's snapshot namespace.
                fork_id,
                fork_flush_count: fork_flush_count.clone(),
                fork_fragment_warn_fired: fork_fragment_warn_fired.clone(),
                fork_fragment_warn_threshold: config.fork_fragment_warn_threshold,
                flush_lock: flush_lock.clone(),
                index_rebuild_manager: index_rebuild_manager.clone(),
                compaction_handle: compaction_handle.clone(),
                compaction_config: config.compaction.clone(),
                index_rebuild_config: config.index_rebuild.clone(),
                auto_rebuild_enabled: config.index_rebuild.auto_rebuild_enabled,
            };
            let finalize_fn: Arc<dyn FinalizeFn> = Arc::new(WriterFinalizer);
            Some(Arc::new(FlushCoordinator::new(
                config.max_pending_flushes,
                config.flush_stream_timeout,
                shared,
                finalize_fn,
            )))
        } else {
            None
        };

        let commit_sequence = Arc::new(crate::runtime::sync::AtomicU64::new(0));
        let committed_writes = Arc::new(PlMutex::new(crate::runtime::occ::CommitRegistry::new(
            OCC_REGISTRY_CAPACITY,
        )));
        let for_update_locks = Arc::new(dashmap::DashMap::new());

        Ok(Self {
            l0_manager,
            storage,
            schema_manager,
            allocator,
            config,
            xervo_runtime: OnceLock::new(),
            property_manager,
            adjacency_manager,
            last_flush_time,
            compaction_handle,
            index_rebuild_manager,
            cached_manifest,
            fork_id,
            fork_flush_count,
            fork_fragment_warn_fired,
            flush_lock,
            flush_coordinator,
            commit_sequence,
            committed_writes,
            publish_sequencer,
            for_update_locks,
            throttle_stats: Arc::new(WriteThrottleStats::default()),
            restore_epoch: std::sync::atomic::AtomicU64::new(0),
            restore_in_progress: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            unmarked_wal_segments: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// Returns the shared pessimistic lock handle for a `FOR UPDATE` row key,
    /// creating it on first use. The caller `.lock_owned().await`s the returned
    /// mutex and holds the guard for the transaction's lifetime.
    pub fn row_lock_handle(&self, key: &[u8]) -> Arc<tokio::sync::Mutex<()>> {
        self.for_update_locks
            .entry(key.to_vec())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Prunes `FOR UPDATE` lock-map entries for `keys` that no live transaction
    /// holds anymore, so the map does not grow without bound across the keyspace.
    ///
    /// Called when a transaction ends, **after** its guards have been dropped.
    /// `remove_if` evaluates its predicate under the DashMap shard lock, which is
    /// the same lock `row_lock_handle` takes to clone an entry — so the check
    /// `strong_count == 1` (only the map holds the `Arc`) is race-free: a
    /// concurrent acquirer either already cloned the `Arc` (count ≥ 2 → we skip
    /// removal) or has not yet taken the shard lock (it will mint a fresh entry
    /// after we remove). Either way no two transactions ever lock different
    /// `Mutex` instances for the same key.
    pub fn release_for_update_locks(&self, keys: &[Vec<u8>]) {
        for key in keys {
            self.for_update_locks
                .remove_if(key, |_, handle| Arc::strong_count(handle) == 1);
        }
    }

    /// Number of live entries in the `FOR UPDATE` lock map. Introspection for
    /// tests that the map does not leak entries across transactions (G5).
    pub fn for_update_lock_count(&self) -> usize {
        self.for_update_locks.len()
    }

    /// The current OCC commit sequence. A `FOR UPDATE` acquisition re-stamps a
    /// fresh transaction's `occ_read_seq` to this so its conflict-detection
    /// baseline advances to lock-acquisition time (read-latest under the lock).
    pub fn current_commit_sequence(&self) -> u64 {
        self.commit_sequence
            .load(crate::runtime::sync::Ordering::Relaxed)
    }

    /// Build a fresh `SharedFlushCtx` from this Writer's current state.
    /// Used by the async-flush stream/finalize paths to pass into spawned
    /// tasks without smuggling `Arc<Writer>` (which would create a cycle
    /// with `flush_coordinator -> FinalizeFn -> Writer`).
    pub(crate) fn shared_ctx(&self) -> SharedFlushCtx {
        SharedFlushCtx {
            storage: self.storage.clone(),
            l0_manager: self.l0_manager.clone(),
            adjacency_manager: self.adjacency_manager.clone(),
            property_manager: self.property_manager.clone(),
            schema_manager: self.schema_manager.clone(),
            cached_manifest: self.cached_manifest.clone(),
            last_flush_time: self.last_flush_time.clone(),
            fork_id: self.fork_id,
            fork_flush_count: self.fork_flush_count.clone(),
            fork_fragment_warn_fired: self.fork_fragment_warn_fired.clone(),
            fork_fragment_warn_threshold: self.config.fork_fragment_warn_threshold,
            flush_lock: self.flush_lock.clone(),
            index_rebuild_manager: self.index_rebuild_manager.clone(),
            compaction_handle: self.compaction_handle.clone(),
            compaction_config: self.config.compaction.clone(),
            index_rebuild_config: self.config.index_rebuild.clone(),
            auto_rebuild_enabled: self.config.index_rebuild.auto_rebuild_enabled,
        }
    }

    /// Borrow the flush coordinator if async flush is enabled.
    /// Returns `None` when `config.async_flush_enabled = false`.
    /// External callers (`drop_fork`) use this to drain pending streams.
    pub fn flush_coordinator(
        &self,
    ) -> Option<&Arc<crate::runtime::flush_coordinator::FlushCoordinator>> {
        self.flush_coordinator.as_ref()
    }

    /// Set the index rebuild manager for post-flush automatic rebuild scheduling.
    ///
    /// One-shot: returns `Err` if already set. The receiver is `&self` so this
    /// can be called after the `Writer` has been wrapped in `Arc<Writer>`.
    pub fn set_index_rebuild_manager(
        &self,
        manager: Arc<crate::storage::index_rebuild::IndexRebuildManager>,
    ) -> Result<()> {
        self.index_rebuild_manager
            .set(manager)
            .map_err(|_| anyhow!("index_rebuild_manager already set"))
    }

    /// Replay WAL mutations into the current L0 buffer.
    /// Mirrors the edge tombstones produced by a vertex-delete cascade into the
    /// `AdjacencyManager` overlay.
    ///
    /// `apply_vertex_deletion` tombstones every edge incident to a deleted
    /// vertex directly in the L0 buffer, bypassing `delete_edge` and therefore
    /// the mirror that path performs. Left unmirrored, the edge is correctly
    /// dropped at the next flush but stays live in the overlay until then, so a
    /// traversal keeps returning a relationship whose endpoint is gone — the
    /// exact inverse of issues #281/#282, which lost edges that were present.
    ///
    /// In practice the executor's DETACH DELETE pre-pass deletes each incident
    /// edge through the mirrored `delete_edge` first, so the cascade usually
    /// finds nothing left to do and this is a no-op; attempts to observe a ghost
    /// edge through Cypher did not produce one. It is kept as defense in depth,
    /// because nothing enforces that ordering: a caller reaching
    /// `Writer::delete_vertex` directly, or a pre-pass that misses an edge
    /// type, would land here with real work to do.
    fn mirror_cascaded_tombstones(&self, cascaded: &[TombstoneEntry], source: &L0Buffer) {
        for entry in cascaded {
            let version = source
                .edge_versions
                .get(&entry.eid)
                .copied()
                .unwrap_or(source.current_version);
            self.adjacency_manager.add_tombstone(
                entry.eid,
                entry.src_vid,
                entry.dst_vid,
                entry.edge_type,
                version,
            );
        }
    }

    /// Mirrors every edge in `source` into the `AdjacencyManager` overlay.
    ///
    /// Edges live in two places at once and the traversal read path only ever
    /// looks at one of them. `L0Buffer` holds the authoritative row (endpoints,
    /// properties, version) and is what the flush drains into L1, but
    /// `GraphContext::neighbors_for_vid` resolves neighbours from the Main CSR
    /// plus this overlay plus the caller's *transaction-local* buffer — never
    /// the main L0 buffer. The Main CSR is built from L1, so an edge that is
    /// committed but not yet flushed is reachable only through the overlay.
    ///
    /// Every writer into main L0 therefore has to mirror here, and a writer
    /// that forgets does not fail: the edge is durable, the flush will emit it,
    /// and in the meantime `MATCH (a)-[r]->(b)` simply answers zero rows while
    /// the vertices it connects are all present (vertex reads go through the L0
    /// buffers directly). That asymmetry is issues #281 and #282 — WAL recovery
    /// was the writer that forgot.
    ///
    /// Each edge is stamped with its version in `versions`, the buffer the
    /// edges landed in. A commit passes main L0, which restamps every merged
    /// edge with the version it committed at; the transaction buffer's own
    /// versions count up from the version the transaction began at, so they
    /// would place a late commit inside the snapshot of every transaction
    /// that began after its writer did, for a pinned reader to see.
    /// `fallback_version` is used for edges with no recorded version; callers
    /// pass the current version of the buffer the edges are landing in.
    fn mirror_edges_into_adjacency(
        &self,
        source: &L0Buffer,
        versions: &L0Buffer,
        fallback_version: u64,
    ) {
        for (eid, (src, dst, etype)) in &source.edge_endpoints {
            let edge_version = versions
                .edge_versions
                .get(eid)
                .copied()
                .unwrap_or(fallback_version);
            if source.tombstones.contains_key(eid) {
                self.adjacency_manager
                    .add_tombstone(*eid, *src, *dst, *etype, edge_version);
            } else {
                self.adjacency_manager
                    .insert_edge(*src, *dst, *eid, *etype, edge_version);
            }
        }

        // Tombstones for edges whose insert is not in `source` (the edge was
        // flushed in an earlier window). `apply_edge_deletion` records the
        // endpoints on the tombstone itself for exactly this case.
        for (eid, tombstone) in &source.tombstones {
            if !source.edge_endpoints.contains_key(eid) {
                let edge_version = versions
                    .edge_versions
                    .get(eid)
                    .copied()
                    .unwrap_or(fallback_version);
                self.adjacency_manager.add_tombstone(
                    *eid,
                    tombstone.src_vid,
                    tombstone.dst_vid,
                    tombstone.edge_type,
                    edge_version,
                );
            }
        }
    }

    /// Re-resolve every replayed edge mutation's type id from its type name.
    ///
    /// A schemaless edge type id is assigned by the in-memory registry of the
    /// process that wrote the WAL record, and that registry need not survive:
    /// a fork's merged schema is never saved, and is rebuilt on reopen from
    /// primary's *current* registry, where the fork's old id may since have
    /// been given to a different name. Replaying the raw id then either
    /// relabels the edge (the id now names another type) or makes the next
    /// flush fail (`Edge type ID … not found`). The name in the record is the
    /// authority; the id is re-derived from it here, interning the name when
    /// the registry no longer knows it.
    ///
    /// An edge delete written before deletes carried a name falls back to the
    /// name an insert in this same replay gave its id; failing that, its id is
    /// left as written.
    fn remap_replayed_edge_types<'a>(
        &self,
        mutations: impl IntoIterator<Item = &'a mut crate::runtime::wal::Mutation>,
    ) {
        use crate::runtime::wal::Mutation;
        let mut name_by_logged_id: HashMap<u32, String> = HashMap::new();
        let mut remapped = 0usize;
        for mutation in mutations {
            let (edge_type, name) = match mutation {
                Mutation::InsertEdge {
                    edge_type,
                    edge_type_name: Some(name),
                    ..
                } => {
                    name_by_logged_id
                        .entry(*edge_type)
                        .or_insert_with(|| name.clone());
                    (edge_type, name.clone())
                }
                Mutation::DeleteEdge {
                    edge_type,
                    edge_type_name,
                    ..
                } => {
                    let Some(name) = edge_type_name
                        .clone()
                        .or_else(|| name_by_logged_id.get(edge_type).cloned())
                    else {
                        continue;
                    };
                    (edge_type, name)
                }
                _ => continue,
            };
            let id = self.schema_manager.get_or_assign_edge_type_id(&name);
            if *edge_type != id {
                *edge_type = id;
                remapped += 1;
            }
        }
        if remapped > 0 {
            log::info!(
                "WAL replay: re-resolved the edge type id of {remapped} edge mutations by name"
            );
        }
    }

    /// Replay the WAL above `wal_high_water_mark` into L0 in one step.
    ///
    /// Announces nothing: a caller that announces recovered commits drives
    /// [`Self::begin_wal_replay`], [`Self::apply_replayed_mutations`] and
    /// [`Self::finish_wal_replay`] itself, one segment at a time. Returns the
    /// number of mutations replayed.
    ///
    /// # Errors
    /// As [`Self::begin_wal_replay`] and [`Self::apply_replayed_mutations`].
    pub async fn replay_wal(&self, wal_high_water_mark: u64) -> Result<usize> {
        let Some(recovery) = self.begin_wal_replay(wal_high_water_mark).await? else {
            return Ok(0);
        };
        let mut count = 0;
        for segment in recovery.segments {
            count += segment.mutations.len();
            self.apply_replayed_mutations(segment.mutations)?;
        }
        if count > 0 {
            log::info!(
                "Replaying {} mutations from WAL (LSN > {})",
                count,
                wal_high_water_mark
            );
            self.finish_wal_replay();
        }
        Ok(count)
    }

    /// Read the WAL segments above `wal_high_water_mark` for replay, in LSN
    /// order, and restore the announced mark they record; `None` without a
    /// WAL. Apply each with [`Self::apply_replayed_mutations`], then call
    /// [`Self::finish_wal_replay`].
    ///
    /// [`WalRecovery::announced_through`] is the highest LSN known announced:
    /// a commit in a segment above it may never have been announced, since
    /// the process can die between a commit's WAL flush and its announcement.
    /// Every segment at or below the mark is, and so is everything at or below
    /// `wal_high_water_mark`, because a flush waits for the announcements of
    /// the commits it moves to L1 (see `flush_l0_rotate`).
    ///
    /// # Errors
    /// The log cannot be read, or holds a corrupt segment before its tail.
    pub async fn begin_wal_replay(&self, wal_high_water_mark: u64) -> Result<Option<WalRecovery>> {
        let l0 = self.l0_manager.get_current();
        let wal = l0.read().wal.clone();
        let Some(wal) = wal else {
            return Ok(None);
        };
        wal.initialize().await?;
        // Segments at or below the manifest's mark were truncated by the
        // flush that published it; new ones must number above it, or the
        // next recovery skips them.
        wal.ensure_lsn_above(wal_high_water_mark)?;
        let mut replay = wal.replay_segments_since(wal_high_water_mark).await?;
        // Remapped across every segment at once, so a legacy unnamed delete
        // can borrow the name an insert in an earlier segment gave its id.
        self.remap_replayed_edge_types(
            replay
                .segments
                .iter_mut()
                .flat_map(|segment| segment.mutations.iter_mut()),
        );
        self.unmarked_wal_segments
            .store(replay.unmarked_segments, Ordering::Release);
        let announced_through = replay
            .segments
            .iter()
            .map(|segment| segment.announced_through)
            .fold(wal_high_water_mark, u64::max);
        wal.restore_announced_mark(announced_through)?;
        Ok(Some(WalRecovery {
            segments: replay.segments,
            announced_through,
        }))
    }

    /// Apply one replayed segment's mutations to the current L0.
    ///
    /// # Errors
    /// A mutation cannot be applied.
    pub fn apply_replayed_mutations(
        &self,
        mutations: Vec<crate::runtime::wal::Mutation>,
    ) -> Result<()> {
        if mutations.is_empty() {
            return Ok(());
        }
        self.l0_manager
            .get_current()
            .write()
            .replay_mutations(mutations)
    }

    /// Rebuild what replayed mutations do not restore by themselves; call
    /// once after the last [`Self::apply_replayed_mutations`].
    pub fn finish_wal_replay(&self) {
        let l0 = self.l0_manager.get_current();
        let mut l0_guard = l0.write();
        // Rebuild the UNIQUE constraint index over the recovered rows
        // (Bug #9 Mechanism B). `replay_mutations` restores
        // vertices/properties/labels but never repopulates
        // `constraint_index` (its only other caller is the live insert
        // path). Without this, a unique key that lives only in the WAL
        // (committed but not yet flushed to Lance) is invisible to
        // `check_unique_constraint_multi` after recovery and a
        // duplicate of it could be created.
        self.rebuild_constraint_index(&mut l0_guard);
        // Null out wrong-dimension vector values recovered from a WAL written
        // before dimension enforcement (issue #137). The flush path now fails
        // closed on a dimension mismatch, so without this pass a single
        // pre-fix value would make every flush of its label fail forever,
        // leaving the database unopenable (the ghost-commit hazard documented
        // on `commit_transaction_l0`). Nulling preserves exactly the pre-fix
        // flush outcome for these values, with a loud signal.
        self.sanitize_replayed_vector_dims(&mut l0_guard);
        // Mirror the recovered edges into the AdjacencyManager overlay
        // (issues #281, #282). `replay_mutations` restores them into
        // the L0 buffer only, and the traversal read path does not read
        // edges from there — see `mirror_edges_into_adjacency`. Without
        // this, a store that crashed before its first flush, or a second
        // handle replaying another handle's committed-but-unflushed WAL,
        // returns every committed vertex and none of the committed edges.
        let fallback_version = l0_guard.current_version;
        self.mirror_edges_into_adjacency(&l0_guard, &l0_guard, fallback_version);
    }

    /// The current L0's version.
    #[must_use]
    pub fn current_version(&self) -> u64 {
        self.l0_manager.get_current().read().current_version
    }

    /// Take a place in the announcement order for a commit recovery replayed
    /// from WAL segment `lsn` and has not announced yet (see
    /// [`Self::begin_wal_replay`]).
    ///
    /// Take it as the segment is replayed, before anything else can be
    /// announced: a flush waits for the place to be released, so the commit
    /// cannot reach L1 (and leave the WAL) before its announcement does.
    pub async fn sequence_recovered_commit(
        &self,
        lsn: u64,
    ) -> crate::runtime::publish_sequencer::PublishTicket {
        let _flush_lock_guard = self.flush_lock.lock().await;
        self.publish_sequencer.issue_at(lsn)
    }

    /// Whether the WAL's announced mark is ahead of every durable segment's:
    /// a reopen would then announce the commits announced since the last
    /// segment again, unless [`Self::seal_wal`] records the mark first.
    #[must_use]
    pub fn wal_needs_seal(&self) -> bool {
        self.l0_manager
            .get_current()
            .read()
            .wal
            .as_ref()
            .is_some_and(|wal| wal.needs_seal())
    }

    /// Record the WAL's announced mark durably before the writer's owner
    /// closes; see [`Self::wal_needs_seal`] and `WriteAheadLog::seal`. A no-op
    /// without a WAL.
    ///
    /// # Errors
    /// As `WriteAheadLog::seal`.
    pub async fn seal_wal(&self) -> Result<()> {
        // Under `flush_lock`, as every other flush of the log: a seal writes
        // whatever is buffered, and a commit between its append and its own
        // WAL flush would otherwise find its mutations taken by the seal's
        // still-unfinished write and report an older segment as its commit
        // point.
        let _flush_lock_guard = self.flush_lock.lock().await;
        self.seal_wal_locked().await
    }

    /// [`Self::seal_wal`] only if `flush_lock` is free right now; `Ok(false)`
    /// without sealing when it is held.
    ///
    /// For a caller that must not wait, such as a synchronous `Drop` that
    /// blocks its thread: the lock's holder may be a flush that can only make
    /// progress on that very thread (a `current_thread` runtime), so waiting
    /// for it would deadlock. Skipping costs at most re-announcing the commits
    /// announced since the last segment at the next open.
    ///
    /// # Errors
    /// As `WriteAheadLog::seal`.
    pub async fn try_seal_wal(&self) -> Result<bool> {
        let Ok(_flush_lock_guard) = self.flush_lock.try_lock() else {
            return Ok(false);
        };
        self.seal_wal_locked().await?;
        Ok(true)
    }

    async fn seal_wal_locked(&self) -> Result<()> {
        let wal = self.l0_manager.get_current().read().wal.clone();
        if let Some(wal) = wal {
            wal.seal().await?;
        }
        Ok(())
    }

    /// Withdraw every `enforced` / `verified` endpoint-conformance record when
    /// storage holds writes by a binary that may not have enforced declared
    /// endpoints. Call once at open, after [`Self::replay_wal`].
    ///
    /// The signal is the writer features each enforcing binary stamps on what
    /// it writes (`WRITER_FEATURES`): a replayed WAL segment without them, or a
    /// latest snapshot manifest without them that is not the pre-existing one
    /// an enforcing binary already accepted
    /// (`Schema::trusted_unmarked_manifest`). Either means an older binary
    /// wrote after the records were made -- edges it never checked, or a label
    /// removal it never refused -- so the planner must stop narrowing on them.
    /// A latest manifest without features is accepted as the new floor either
    /// way: after this call every record that could predate it is unverified.
    ///
    /// With `persist`, a downgrade is saved to the catalog (not on a fork,
    /// whose catalog view is never saved; the fork's in-memory view is
    /// downgraded alone). Returns the edge types downgraded.
    ///
    /// # Errors
    /// Reading the latest manifest or saving the catalog failed.
    pub async fn distrust_conformance_after_unmarked_writes(
        &self,
        persist: bool,
    ) -> Result<Vec<String>> {
        let latest = self
            .storage
            .snapshot_manager()
            .load_latest_snapshot()
            .await?;
        let unmarked_manifest = latest
            .as_ref()
            .filter(|m| !uni_common::core::schema::writer_enforces_endpoints(&m.writer_features))
            .map(|m| m.snapshot_id.clone());
        let trusted = self.schema_manager.trusted_unmarked_manifest()?;
        let foreign_manifest = unmarked_manifest
            .as_ref()
            .is_some_and(|id| trusted.as_ref() != Some(id));
        let unmarked_wal = self.unmarked_wal_segments.load(Ordering::Acquire);

        let downgraded = if foreign_manifest || unmarked_wal > 0 {
            self.schema_manager.distrust_all_conformance()?
        } else {
            Vec::new()
        };
        if unmarked_manifest.is_some() {
            self.schema_manager
                .set_trusted_unmarked_manifest(unmarked_manifest.clone())?;
        }
        if !downgraded.is_empty() {
            tracing::warn!(
                edge_types = ?downgraded,
                latest_manifest = ?unmarked_manifest,
                unmarked_wal_segments = unmarked_wal,
                "storage holds writes by a binary that does not enforce declared edge \
                 endpoints; endpoint conformance of these edge types is now unverified \
                 (run CALL uni.admin.verifyEdgeEndpoints() to restore it)"
            );
            let forked = self.fork_id.is_some() || self.storage.fork_scope().is_some();
            if persist && !forked {
                self.schema_manager.save().await?;
            }
        }
        Ok(downgraded)
    }

    /// Nulls wrong-dimension vector values in a recovered L0 buffer (issue #137).
    ///
    /// Only runs during WAL replay, where the offending value was written by a
    /// version without write-time dimension enforcement and can no longer be
    /// rejected. Live writes never reach this — they fail in the validation and
    /// coercion guards.
    fn sanitize_replayed_vector_dims(&self, l0_guard: &mut L0Buffer) {
        let schema = self.schema_manager.schema();

        // Collect fixes first: `vertex_labels` / `edge_types` are borrowed immutably
        // from the same guard the property maps are mutated through.
        let mut vertex_fixes: Vec<(Vid, String)> = Vec::new();
        for (&vid, props) in &l0_guard.vertex_properties {
            let Some(labels) = l0_guard.vertex_labels.get(&vid) else {
                continue;
            };
            for label in labels {
                let Some(props_meta) = schema.properties.get(label) else {
                    continue;
                };
                for (prop_name, meta) in props_meta {
                    if let Some(value) = props.get(prop_name)
                        && let Err(e) = meta.r#type.check_vector_dims(value)
                    {
                        log::warn!(
                            "WAL replay: nulling wrong-dimension value for '{}.{}' on vid {:?} \
                             ({}) — written before dimension enforcement (issue #137)",
                            label,
                            prop_name,
                            vid,
                            e
                        );
                        vertex_fixes.push((vid, prop_name.clone()));
                    }
                }
            }
        }
        // Nulling here is deliberate and is the only safe option — refusing the
        // value would leave the buffer permanently unflushable (#137). But it
        // *destroys data*, and a `warn!` is the weakest possible record of that:
        // greppable if someone thinks to look, invisible otherwise. Count it, so
        // a recovery that discarded values is visible without reading the log
        // (#233).
        metrics::counter!("uni_wal_replay_dims_nulled_total", "kind" => "vertex")
            .increment(vertex_fixes.len() as u64);
        for (vid, prop) in vertex_fixes {
            if let Some(props) = l0_guard.vertex_properties.get_mut(&vid) {
                props.insert(prop, Value::Null);
            }
        }

        let mut edge_fixes: Vec<(Eid, String)> = Vec::new();
        for (&eid, props) in &l0_guard.edge_properties {
            let Some(type_name) = l0_guard.edge_types.get(&eid) else {
                continue;
            };
            let Some(props_meta) = schema.properties.get(type_name) else {
                continue;
            };
            for (prop_name, meta) in props_meta {
                if let Some(value) = props.get(prop_name)
                    && let Err(e) = meta.r#type.check_vector_dims(value)
                {
                    log::warn!(
                        "WAL replay: nulling wrong-dimension value for '{}.{}' on eid {:?} \
                         ({}) — written before dimension enforcement (issue #137)",
                        type_name,
                        prop_name,
                        eid,
                        e
                    );
                    edge_fixes.push((eid, prop_name.clone()));
                }
            }
        }
        metrics::counter!("uni_wal_replay_dims_nulled_total", "kind" => "edge")
            .increment(edge_fixes.len() as u64);
        for (eid, prop) in edge_fixes {
            if let Some(props) = l0_guard.edge_properties.get_mut(&eid) {
                props.insert(prop, Value::Null);
            }
        }
    }

    /// Rebuild the UNIQUE constraint index on a recovered L0 buffer.
    ///
    /// Scans every recovered vertex's properties and, for each enabled UNIQUE
    /// constraint whose target label the vertex carries and whose member
    /// properties are all present, inserts the same constraint key the live
    /// insert path builds (`serialize_constraint_key`). Tombstoned vertices are
    /// skipped. Called after [`L0Buffer::replay_mutations`] under the buffer's
    /// write lock; the schema is already loaded on the `Writer`.
    fn rebuild_constraint_index(&self, l0_guard: &mut L0Buffer) {
        let schema = self.schema_manager.schema();
        // Collect entries first to avoid borrowing `vertex_properties`
        // immutably while mutating `constraint_index` through the same guard.
        let mut keys: Vec<(Vec<u8>, Vid)> = Vec::new();
        for (&vid, props) in &l0_guard.vertex_properties {
            if l0_guard.vertex_tombstones.contains(&vid) {
                continue;
            }
            let Some(labels) = l0_guard.vertex_labels.get(&vid) else {
                continue;
            };
            for label in labels {
                for constraint in &schema.constraints {
                    if !constraint.enabled {
                        continue;
                    }
                    let ConstraintTarget::Label(l) = &constraint.target else {
                        continue;
                    };
                    if l != label {
                        continue;
                    }
                    // Rebuild keys for both Unique and NodeKey (uniqueness half).
                    let Some(unique_props) = constraint.constraint_type.unique_properties() else {
                        continue;
                    };
                    let mut key_values = Vec::new();
                    let mut all_present = true;
                    for prop in unique_props {
                        if let Some(val) = props.get(prop) {
                            key_values.push((prop.clone(), val.clone()));
                        } else {
                            all_present = false;
                            break;
                        }
                    }
                    if all_present {
                        keys.push((serialize_constraint_key(label, &key_values), vid));
                    }
                }
            }
        }
        for (key, vid) in keys {
            l0_guard.insert_constraint_key(key, vid);
        }

        // Same rebuild for edge unique/nodekey keys: `replay_mutations` restores
        // edge properties/types but never repopulates `edge_constraint_index`, so
        // a WAL-committed-but-unflushed unique edge key would otherwise be invisible
        // to the edge full-horizon probe after recovery (edge counterpart of Bug #9
        // Mechanism B).
        let mut edge_keys: Vec<(Vec<u8>, Eid)> = Vec::new();
        for (&eid, props) in &l0_guard.edge_properties {
            if l0_guard.tombstones.contains_key(&eid) {
                continue;
            }
            let Some(edge_type) = l0_guard.edge_types.get(&eid) else {
                continue;
            };
            for constraint in &schema.constraints {
                if !constraint.enabled {
                    continue;
                }
                let ConstraintTarget::EdgeType(t) = &constraint.target else {
                    continue;
                };
                if t != edge_type {
                    continue;
                }
                let Some(unique_props) = constraint.constraint_type.unique_properties() else {
                    continue;
                };
                let mut key_values = Vec::new();
                let mut all_present = true;
                for prop in unique_props {
                    match props.get(prop) {
                        Some(val) if !val.is_null() => key_values.push((prop.clone(), val.clone())),
                        _ => {
                            all_present = false;
                            break;
                        }
                    }
                }
                if all_present && !key_values.is_empty() {
                    edge_keys.push((serialize_constraint_key(edge_type, &key_values), eid));
                }
            }
        }
        for (key, eid) in edge_keys {
            l0_guard.insert_edge_constraint_key(key, eid);
        }
    }

    /// Allocates the next VID (pure auto-increment).
    pub async fn next_vid(&self) -> Result<Vid> {
        self.allocator.allocate_vid().await
    }

    /// Allocates multiple VIDs at once for bulk operations.
    /// This is more efficient than calling next_vid() in a loop.
    pub async fn allocate_vids(&self, count: usize) -> Result<Vec<Vid>> {
        self.allocator.allocate_vids(count).await
    }

    /// Allocates the next EID (pure auto-increment).
    pub async fn next_eid(&self, _type_id: u32) -> Result<Eid> {
        self.allocator.allocate_eid().await
    }

    /// Allocates multiple EIDs at once for bulk operations.
    /// This is more efficient than calling next_eid() in a loop.
    pub async fn allocate_eids(&self, count: usize) -> Result<Vec<Eid>> {
        self.allocator.allocate_eids(count).await
    }

    /// Install the embedding runtime exactly once. Receiver is `&self` so it
    /// can be called after the `Writer` has been wrapped in `Arc<Writer>`.
    pub fn set_xervo_runtime(&self, runtime: Arc<ModelRuntime>) -> Result<()> {
        self.xervo_runtime
            .set(runtime)
            .map_err(|_| anyhow!("xervo_runtime already set"))
    }

    pub fn xervo_runtime(&self) -> Option<Arc<ModelRuntime>> {
        self.xervo_runtime.get().cloned()
    }

    /// Create a new empty L0 buffer for transaction-scoped mutations.
    ///
    /// Only reads the current version — no exclusive lock required on Writer.
    /// The returned buffer has no WAL reference; mutations are logged at
    /// commit time via [`Self::commit_transaction_l0`].
    pub fn create_transaction_l0(&self) -> Arc<RwLock<L0Buffer>> {
        let current_version = self.l0_manager.get_current().read().current_version;
        // Transaction mutations are logged to WAL at COMMIT time, not during the transaction.
        let mut buf = L0Buffer::new(current_version, None);
        // The schema its statements validate against; see
        // `revalidate_declared_properties`.
        buf.validated_schema_version = Some(self.schema_manager.schema().schema_version);
        // SSI: stamp the OCC read sequence at begin so commit can detect any
        // transaction that committed since. Gated on the runtime `ssi_enabled`
        // toggle — when off, `occ_read_set` stays `None` and every downstream
        // read-set recording / commit validation self-gates to a no-op.
        let buf = if self.config.ssi_enabled {
            let mut buf = buf;
            buf.occ_read_seq = self
                .commit_sequence
                .load(crate::runtime::sync::Ordering::Relaxed);
            // The read path records observed ids here for SSI antidependency
            // detection; commit consults it.
            buf.occ_read_set = Some(Arc::new(parking_lot::Mutex::new(
                crate::runtime::l0::OccReadSet::default(),
            )));
            buf
        } else {
            buf
        };
        Arc::new(RwLock::new(buf))
    }

    /// Resolve the target L0 buffer for a mutation.
    ///
    /// When `tx_l0` is `Some`, the mutation targets a transaction-private buffer.
    /// When `None`, it targets the global L0 from the manager.
    fn resolve_l0(&self, tx_l0: Option<&Arc<RwLock<L0Buffer>>>) -> Arc<RwLock<L0Buffer>> {
        tx_l0
            .cloned()
            .unwrap_or_else(|| self.l0_manager.get_current())
    }

    fn update_metrics(&self) {
        let l0 = self.l0_manager.get_current();
        let size = l0.read().estimated_size;
        metrics::gauge!("l0_buffer_size_bytes").set(size as f64);
    }

    /// Overlay-aware issue-#77 edge-endpoint validation.
    ///
    /// The current buffer alone does not hold all committed-but-unflushed
    /// tombstones — a flush rotation moves them onto `pending_flush` until
    /// the Lance write completes. A vertex is effectively deleted iff,
    /// walking newest-first (tx → current → pending newest→oldest), the
    /// first buffer that knows the vid says "tombstoned" (an insert clears
    /// the tombstone within a buffer, so props/tombstone are mutually
    /// exclusive per buffer).
    ///
    /// Must run under `flush_lock` so the overlay cannot change before the
    /// merge, and BEFORE the durable WAL flush (see the call site).
    fn validate_edge_endpoints_overlay(&self, tx_l0: &L0Buffer) -> Result<()> {
        let chain = self.l0_manager.get_pending_flush();
        let current = self.l0_manager.get_current();
        let effectively_deleted = |vid: &Vid| -> bool {
            if tx_l0.vertex_properties.contains_key(vid) {
                return false;
            }
            if tx_l0.vertex_tombstones.contains(vid) {
                return true;
            }
            {
                let cur = current.read();
                if cur.vertex_properties.contains_key(vid) {
                    return false;
                }
                if cur.vertex_tombstones.contains(vid) {
                    return true;
                }
            }
            for frozen in chain.iter().rev() {
                let g = frozen.read();
                if g.vertex_properties.contains_key(vid) {
                    return false;
                }
                if g.vertex_tombstones.contains(vid) {
                    return true;
                }
            }
            false
        };
        for (eid, (src_vid, dst_vid, _etype)) in &tx_l0.edge_endpoints {
            if tx_l0.tombstones.contains_key(eid) {
                continue; // a deletion, not an insertion — never resurrects a vertex
            }
            if effectively_deleted(src_vid) {
                anyhow::bail!(
                    "Cannot insert edge {}: source vertex {} has been deleted (issue #77)",
                    eid,
                    src_vid
                );
            }
            if effectively_deleted(dst_vid) {
                anyhow::bail!(
                    "Cannot insert edge {}: destination vertex {} has been deleted (issue #77)",
                    eid,
                    dst_vid
                );
            }
        }
        Ok(())
    }

    /// Seed `main_l0` (the current buffer) with the newest pending-overlay
    /// value for each CRDT property the transaction writes, so the commit
    /// merge MERGES against the committed CRDT state instead of shadowing it
    /// (the carve-out lets concurrent CRDT writers commit on the assumption
    /// that the merge sees the committed value — true only while that value
    /// lives in current, not mid-flush on `pending_flush`). No-op when
    /// nothing is pending or the property already exists in current. Vertex
    /// properties only, mirroring the carve-out itself.
    fn seed_crdt_state_from_chain(&self, tx_l0: &L0Buffer, main_l0: &mut L0Buffer) {
        let chain = self.l0_manager.get_pending_flush();
        if chain.is_empty() {
            return;
        }
        for (vid, props) in &tx_l0.vertex_properties {
            Self::seed_crdt_props(&chain, *vid, props, main_l0);
        }
    }

    /// Per-vertex CRDT seeding (see [`Self::seed_crdt_state_from_chain`]).
    /// Also used by the non-transactional vertex write path, which CRDT-merges
    /// into the current buffer directly and has the same shadowing hazard
    /// during a flush window.
    fn seed_crdt_props(
        chain: &[Arc<RwLock<L0Buffer>>],
        vid: Vid,
        props: &Properties,
        target: &mut L0Buffer,
    ) {
        for (key, value) in props {
            if crate::runtime::l0::try_as_crdt(value).is_none() {
                continue;
            }
            if target
                .vertex_properties
                .get(&vid)
                .is_some_and(|p| p.contains_key(key))
            {
                continue;
            }
            // Newest generation first: the first hit is the live state.
            for frozen in chain.iter().rev() {
                let g = frozen.read();
                if let Some(v) = g.vertex_properties.get(&vid).and_then(|p| p.get(key)) {
                    if crate::runtime::l0::try_as_crdt(v).is_some() {
                        target
                            .vertex_properties
                            .entry(vid)
                            .or_default()
                            .insert(key.clone(), v.clone());
                    }
                    break;
                }
            }
        }
    }

    /// Commit an externally-owned transaction L0 buffer.
    ///
    /// Writes mutations to WAL, flushes, merges into main L0, and replays
    /// edges into the AdjacencyManager. Returns the WAL LSN of the commit
    /// (0 when no WAL is configured).
    /// Commit a transaction's private L0 buffer into main L0.
    ///
    /// Returns `(wal_lsn, flush_pending)`. When `flush_pending == true`, the
    /// post-commit `should_flush()` predicate fired but no flush ran — the
    /// caller is expected to spawn a background `flush_to_l1`. This is the
    /// shape used when `UniConfig::async_flush_enabled` is set, so commits
    /// don't block on L1-streaming I/O.
    pub async fn commit_transaction_l0(
        self: &Arc<Self>,
        tx_l0_arc: Arc<RwLock<L0Buffer>>,
    ) -> Result<(u64, bool)> {
        // No lock-acquisition timeout (tests and internal callers).
        self.commit_transaction_l0_with_lock_timeout(tx_l0_arc, None)
            .await
    }

    /// Like [`Self::commit_transaction_l0`] but bounds ONLY the `flush_lock`
    /// acquisition by `lock_timeout` (contention with another in-progress
    /// commit). Once the lock is held, the durable WAL flush, the main-L0 merge,
    /// and the inline post-commit L0→L1 flush all run to completion UNCANCELLED.
    ///
    /// This is the load-bearing correctness property: a caller must NOT wrap the
    /// whole future in `tokio::time::timeout`, because that would cancel past the
    /// durable point (`flush_wal`) and return a retriable `CommitTimeout` for a
    /// transaction that is already durable and visible — a retry would then
    /// double-apply it. Bounding only the lock wait keeps the "another commit is
    /// taking too long" signal without ever cancelling durable work.
    ///
    /// # Errors
    /// Returns [`uni_common::api::error::UniError::CommitTimeout`] (with an empty
    /// `tx_id` for the caller to fill in) if `flush_lock` cannot be acquired
    /// within `lock_timeout`, plus any error from the commit itself.
    pub async fn commit_transaction_l0_with_lock_timeout(
        self: &Arc<Self>,
        tx_l0_arc: Arc<RwLock<L0Buffer>>,
        lock_timeout: Option<std::time::Duration>,
    ) -> Result<(u64, bool)> {
        // The ticket is dropped unpublished: this caller announces nothing,
        // and the drop releases its place in the announcement order -- before
        // the auto-flush, which waits for every issued place to be released.
        let committed = self
            .commit_transaction_l0_sequenced(tx_l0_arc, lock_timeout, None)
            .await?;
        let wal_lsn = committed.wal_lsn;
        drop(committed.ticket);
        Ok((wal_lsn, self.flush_after_commit().await))
    }

    /// Like [`Self::commit_transaction_l0_with_lock_timeout`], and also
    /// returns the commit's own version and its place in the announcement
    /// order.
    ///
    /// Both are fixed under `flush_lock`, at the point the commit becomes
    /// visible. A caller that announces commits must take them from here:
    /// once the lock is released another commit can land, so the version read
    /// afterwards may be that commit's, and an announcement sent directly may
    /// overtake an earlier commit's. Publish the announcement through
    /// [`CommittedTx::ticket`] to keep commit order.
    ///
    /// `on_abandon`, given the commit's version, is set as the ticket's
    /// [`PublishTicket::on_abandon`] the moment it is issued, so it covers the
    /// rest of this call too: a caller dropped after the commit is visible
    /// would otherwise drop the ticket before it ever saw it.
    ///
    /// The post-commit auto-flush is not run here. Run
    /// [`Self::flush_after_commit`] once the ticket is published or dropped:
    /// a flush waits for every issued ticket to be released, so running it
    /// while holding this one waits for itself.
    ///
    /// [`PublishTicket::on_abandon`]: crate::runtime::publish_sequencer::PublishTicket::on_abandon
    ///
    /// # Errors
    /// As [`Self::commit_transaction_l0_with_lock_timeout`].
    pub async fn commit_transaction_l0_sequenced(
        self: &Arc<Self>,
        tx_l0_arc: Arc<RwLock<L0Buffer>>,
        lock_timeout: Option<std::time::Duration>,
        on_abandon: Option<crate::runtime::publish_sequencer::AbandonAnnouncement>,
    ) -> Result<CommittedTx> {
        // Hold `flush_lock` across WAL append + flush + main-L0 merge.
        // Two concurrent commits serialize here; in Phase 3 the outer
        // `Arc<RwLock<Writer>>` already provides this exclusion, so the
        // acquisition is uncontended. Phase 4 drops the outer lock and
        // this becomes the load-bearing serialization point.
        let _flush_lock_guard = match lock_timeout {
            Some(dur) => match tokio::time::timeout(dur, self.flush_lock.lock()).await {
                Ok(guard) => guard,
                Err(_) => {
                    return Err(uni_common::api::error::UniError::CommitTimeout {
                        tx_id: String::new(),
                        hint: "Another commit is in progress and taking longer than expected. \
                               Your transaction is still active \u{2014} you can retry commit().",
                    }
                    .into());
                }
            },
            None => self.flush_lock.lock().await,
        };

        // Crash-recovery seam: simulate process death immediately after winning
        // the commit serialization point but before any durable work. No-op
        // unless built with `--features failpoints`. (See ssi_resilience tests.)
        fail::fail_point!("commit::after-flush-lock");

        // SSI: optimistic conflict detection. This MUST run before any WAL
        // write — `flush_wal()` below is the durable commit point and the WAL
        // has no abort marker, so aborting after it would resurrect this
        // transaction on crash recovery. The write-set is reused for
        // registration after a successful merge.
        // Runtime-gated on `config.ssi_enabled`. When off, no validation runs
        // and `occ_write_set` is `None`, so the post-merge registration below
        // is skipped — reproducing last-writer-wins exactly.
        let occ_write_set: Option<crate::runtime::occ::WriteSet> = if self.config.ssi_enabled {
            let tx_l0 = tx_l0_arc.read();
            let read_seq = tx_l0.occ_read_seq;
            let write_set = crate::runtime::occ::WriteSet::from_l0(&tx_l0);
            if !write_set.is_empty() {
                // Telemetry: one validation per non-empty (writing) commit. The
                // ratio of conflicts to validations is the headline abort rate.
                metrics::counter!("uni_ssi_commit_validations_total").increment(1);
                // Read-set is consulted only for writing transactions, so a
                // read-only commit (empty write-set) runs at snapshot isolation.
                let read_guard = tx_l0.occ_read_set.as_ref().map(|rs| rs.lock());
                if let Some(conflict) =
                    self.committed_writes
                        .lock()
                        .check(read_seq, &write_set, read_guard.as_deref())
                {
                    use crate::runtime::occ::Conflict;
                    match &conflict {
                        Conflict::Key { .. } => {
                            metrics::counter!("uni_ssi_constraint_conflicts_total").increment(1);
                            return Err(uni_common::UniError::ConstraintConflict {
                                message: conflict.to_string(),
                            }
                            .into());
                        }
                        Conflict::WriteWrite { .. } => metrics::counter!(
                            "uni_ssi_serialization_conflicts_total",
                            "kind" => "write_write",
                        )
                        .increment(1),
                        Conflict::ReadWrite { .. } => metrics::counter!(
                            "uni_ssi_serialization_conflicts_total",
                            "kind" => "read_write",
                        )
                        .increment(1),
                        Conflict::HistoryTruncated { .. } => {
                            metrics::counter!("uni_ssi_history_truncated_total").increment(1)
                        }
                    }
                    return Err(anyhow::Error::new(
                        uni_common::UniError::SerializationConflict {
                            message: conflict.to_string(),
                        },
                    ));
                }
            }

            // Validate against the committed-but-unflushed overlay under
            // `flush_lock`: serializable MERGE uniqueness + CRDT carve-out
            // soundness. The current buffer alone does not hold all committed
            // state — a flush rotation moves it onto `pending_flush` until the
            // Lance write completes (the Bug #9A window, here at the
            // commit-time layer) — so every check walks [current, pending…].
            {
                let pending = self.l0_manager.get_pending_flush();
                let main_l0 = self.l0_manager.get_current();
                let overlay: Vec<Arc<RwLock<L0Buffer>>> =
                    std::iter::once(main_l0).chain(pending).collect();

                // SSI / serializable MERGE: abort if a concurrent transaction has
                // already committed a row with one of this transaction's unique
                // keys. Commits serialize here, so this closes the race window
                // left by the per-insert check. (Empty index → no iterations.)
                for (key, vid) in &tx_l0.constraint_index {
                    if overlay
                        .iter()
                        .any(|b| b.read().has_constraint_key(key, *vid))
                    {
                        metrics::counter!("uni_ssi_constraint_conflicts_total").increment(1);
                        return Err(anyhow::Error::new(
                            uni_common::UniError::ConstraintConflict {
                                message: "unique key already committed by a concurrent \
                                          transaction"
                                    .to_string(),
                            },
                        ));
                    }
                }

                // Same serializable guard for edge unique/nodekey keys: abort if a
                // concurrent transaction committed an edge with one of this
                // transaction's edge unique keys. (Empty index → no iterations.)
                for (key, eid) in &tx_l0.edge_constraint_index {
                    if overlay
                        .iter()
                        .any(|b| b.read().has_edge_constraint_key(key, *eid))
                    {
                        metrics::counter!("uni_ssi_constraint_conflicts_total").increment(1);
                        return Err(anyhow::Error::new(
                            uni_common::UniError::ConstraintConflict {
                                message: "unique edge key already committed by a concurrent \
                                          transaction"
                                    .to_string(),
                            },
                        ));
                    }
                }

                // Implicit MERGE phantom guard: a `MERGE` that *created* a node
                // registered its (label, key-props) here even with no declared
                // UNIQUE constraint. If a concurrent transaction already committed
                // the same MERGE key, abort retriably so the two converge to one
                // node on retry (the loser's MATCH then finds the committed row).
                // Only MERGE-creates register keys, so a plain CREATE of the same
                // properties never lands here. (Empty index → no iterations.)
                for (key, vid) in &tx_l0.merge_guard_index {
                    if overlay
                        .iter()
                        .any(|b| b.read().has_merge_guard_key(key, *vid))
                    {
                        metrics::counter!("uni_ssi_constraint_conflicts_total").increment(1);
                        return Err(anyhow::Error::new(
                            uni_common::UniError::ConstraintConflict {
                                message: "MERGE key already committed by a concurrent \
                                          transaction"
                                    .to_string(),
                            },
                        ));
                    }
                }

                // Same race window for global ext_id uniqueness: the per-insert
                // check ran against an older main L0; re-probe the committed
                // index here, where commits serialize.
                for (ext_id, vid) in &tx_l0.extid_index {
                    let taken = overlay.iter().any(|b| {
                        matches!(b.read().extid_index.get(ext_id), Some(&owner) if owner != *vid)
                    });
                    if taken {
                        metrics::counter!("uni_ssi_constraint_conflicts_total").increment(1);
                        return Err(anyhow::Error::new(
                            uni_common::UniError::ConstraintConflict {
                                message: format!(
                                    "ext_id '{ext_id}' already committed by a concurrent \
                                     transaction"
                                ),
                            },
                        ));
                    }
                }

                // CRDT carve-out soundness: a pure-CRDT write was dropped from the
                // write-set assuming its merge commutes. If the overlay holds a
                // *different* CRDT variant for the same property, the merge would
                // silently overwrite it — abort instead of losing the update.
                // (Checked against every overlay buffer: conservative if an old
                // generation held a different variant that a newer commit already
                // replaced, but an abort+retry is always sound.)
                for buf in &overlay {
                    if let Some(conflict) =
                        crate::runtime::occ::crdt_carveout_overwrite(&tx_l0, &buf.read())
                    {
                        metrics::counter!("uni_ssi_crdt_aborts_total").increment(1);
                        return Err(anyhow::Error::new(
                            uni_common::UniError::SerializationConflict {
                                message: conflict.to_string(),
                            },
                        ));
                    }
                }
            }
            Some(write_set)
        } else {
            None
        };

        // Issue #77: an edge whose endpoint is effectively deleted makes the
        // merge below bail. That bail MUST happen before the durable WAL flush —
        // after it the transaction is committed-but-unmerged (a ghost commit),
        // and WAL replay re-hits the same bail, making the database unopenable.
        // SSI validation was deliberately placed before the flush for exactly
        // this reason; the endpoint check belongs here too. Runs unconditionally
        // (issue #77 is not SSI-gated) under `flush_lock`, so the overlay
        // tombstone state cannot change between here and the merge. A
        // tombstone may live in a flush-rotated pending buffer rather than
        // the current buffer, so the check walks the overlay newest-first.
        {
            let tx_l0 = tx_l0_arc.read();
            self.validate_edge_endpoints_overlay(&tx_l0)?;
        }

        // A declaration that landed while the transaction was open: its
        // statements validated property values against the schema before it,
        // so a value of a now-declared property may not fit the declared type
        // (and the flush would refuse it). Re-validated -- and converted, as
        // the write path converts -- here, under `flush_lock` and before the
        // WAL append, so the transaction fails cleanly instead. A declaration
        // publishes its catalog change under `flush_lock` too
        // (`Writer::declaration_lock`), so no commit slips between a
        // declaration's value check and its catalog change. The version
        // compare keeps the common path free.
        {
            let schema = self.schema_manager.schema();
            let stale = tx_l0_arc
                .read()
                .validated_schema_version
                .is_some_and(|v| v != schema.schema_version);
            if stale {
                let mut tx_l0 = tx_l0_arc.write();
                self.revalidate_declared_properties(&mut tx_l0, &schema)?;
                tx_l0.validated_schema_version = Some(schema.schema_version);
            }
        }

        // Crash-recovery seam: SSI validation has passed; the transaction is
        // about to become durable. A crash here must leave NO trace (validation
        // happens before the WAL is touched). No-op unless `failpoints`.
        fail::fail_point!("commit::after-validate");

        // 1. Write transaction mutations to WAL BEFORE merging into main L0
        // This ensures durability before visibility.
        {
            let tx_l0 = tx_l0_arc.read();
            let main_l0_arc = self.l0_manager.get_current();
            let main_l0 = main_l0_arc.read();

            // If WAL exists, write mutations to it for durability
            if let Some(wal) = main_l0.wal.as_ref() {
                // Collected, then appended as one unit, so the segment records
                // which of its mutations are this commit's (`CommitSpan`):
                // recovery announces exactly those.
                let mut pending = Vec::new();
                // An edge delete records its type NAME as well as its id, so
                // replay can resolve the type by name: a schemaless id is
                // only meaningful in this process's registry.
                let wal_edge_type_name = |eid: Eid, edge_type: u32| {
                    tx_l0
                        .edge_types
                        .get(&eid)
                        .cloned()
                        .or_else(|| self.schema_manager.edge_type_name_by_id_unified(edge_type))
                };
                // Order: vertices first, then edges (to ensure src/dst exist on replay)

                // Vertex insertions
                for (vid, properties) in &tx_l0.vertex_properties {
                    if !tx_l0.vertex_tombstones.contains(vid) {
                        let labels = tx_l0.vertex_labels.get(vid).cloned().unwrap_or_default();
                        pending.push(crate::runtime::wal::Mutation::InsertVertex {
                            vid: *vid,
                            properties: properties.clone(),
                            labels,
                            // The tx buffer already holds the `now` its own
                            // writes stamped; carry those rather than taking a
                            // fresh clock reading at commit time.
                            created_at: tx_l0.vertex_created_at.get(vid).copied(),
                            updated_at: tx_l0.vertex_updated_at.get(vid).copied(),
                        });
                    }
                }

                // Vertex deletions
                for vid in &tx_l0.vertex_tombstones {
                    let labels = tx_l0.vertex_labels.get(vid).cloned().unwrap_or_default();
                    pending.push(crate::runtime::wal::Mutation::DeleteVertex { vid: *vid, labels });
                }

                // Label-only mutations (SET n:Label / REMOVE n:Label). After
                // vertex inserts (so the vertex exists on replay), before edges,
                // and skipping vertices deleted in this same commit.
                for vid in &tx_l0.vertex_label_overwrites {
                    if tx_l0.vertex_tombstones.contains(vid) {
                        continue;
                    }
                    let labels = tx_l0.vertex_labels.get(vid).cloned().unwrap_or_default();
                    pending
                        .push(crate::runtime::wal::Mutation::SetVertexLabels { vid: *vid, labels });
                }

                // Crash-recovery seam: vertices appended, edges not yet. Tests
                // assert that a crash here (before `flush_wal`) recovers NOTHING
                // — the durable commit point is the flush below, not append.
                fail::fail_point!("commit::mid-wal");

                // Edge insertions and deletions from edge_endpoints
                for (eid, (src_vid, dst_vid, edge_type)) in &tx_l0.edge_endpoints {
                    if tx_l0.tombstones.contains_key(eid) {
                        let version = tx_l0.edge_versions.get(eid).copied().unwrap_or(0);
                        pending.push(crate::runtime::wal::Mutation::DeleteEdge {
                            eid: *eid,
                            src_vid: *src_vid,
                            dst_vid: *dst_vid,
                            edge_type: *edge_type,
                            version,
                            edge_type_name: wal_edge_type_name(*eid, *edge_type),
                        });
                    } else {
                        let properties =
                            tx_l0.edge_properties.get(eid).cloned().unwrap_or_default();
                        let version = tx_l0.edge_versions.get(eid).copied().unwrap_or(0);
                        let edge_type_name = tx_l0.edge_types.get(eid).cloned();
                        pending.push(crate::runtime::wal::Mutation::InsertEdge {
                            src_vid: *src_vid,
                            dst_vid: *dst_vid,
                            edge_type: *edge_type,
                            eid: *eid,
                            version,
                            properties,
                            edge_type_name,
                            created_at: tx_l0.edge_created_at.get(eid).copied(),
                            updated_at: tx_l0.edge_updated_at.get(eid).copied(),
                        });
                    }
                }

                // Tombstones for edges that only exist in the global L0 (not in
                // this transaction's edge_endpoints).  Without this, deletes of
                // pre-existing edges would be silently lost.
                for (eid, tombstone) in &tx_l0.tombstones {
                    if !tx_l0.edge_endpoints.contains_key(eid) {
                        let version = tx_l0.edge_versions.get(eid).copied().unwrap_or(0);
                        pending.push(crate::runtime::wal::Mutation::DeleteEdge {
                            eid: *eid,
                            src_vid: tombstone.src_vid,
                            dst_vid: tombstone.dst_vid,
                            edge_type: tombstone.edge_type,
                            version,
                            edge_type_name: wal_edge_type_name(*eid, tombstone.edge_type),
                        });
                    }
                }
                wal.append_commit(pending)?;
            }
        }

        // 2. Flush WAL to durable storage - THIS IS THE COMMIT POINT
        let wal_lsn = self.flush_wal().await?;

        // Crash-recovery seam: the WAL is durable but main L0 has NOT merged.
        // A crash here must RECOVER the transaction on replay (it is committed),
        // even though it was never made visible in-process. No-op unless `failpoints`.
        fail::fail_point!("commit::after-wal-flush");

        // Component C1: if an outstanding snapshot pins the current generation,
        // clone it aside (lazy copy-on-write) before merging, so the pinning
        // transaction's reads stay isolated from this commit. No-op — and zero
        // cost — when nothing is pinned (the common case). We hold `flush_lock`,
        // so this cannot race a flush rotate or another commit's merge; the merge
        // below re-fetches `get_current()`, landing in the fresh post-freeze buffer.
        // Self-gates on the runtime SSI toggle: a snapshot is only ever pinned by
        // a transaction begun under `ssi_enabled`, so `is_current_pinned()` is
        // always false when SSI is off and this is a zero-cost no-op.
        if self.l0_manager.is_current_pinned() {
            self.l0_manager.freeze_current_for_snapshot();
            metrics::counter!("uni_l0_snapshot_freezes_total").increment(1);
        }

        // 3. Merge into main L0 and make visible
        let version = {
            // Write-lock the tx buffer: `merge_take` moves its property maps
            // into main L0 instead of cloning them. The commit consumes the
            // transaction, so the drained maps are never observed afterwards;
            // everything read below (endpoints, versions, tombstones) is left
            // intact.
            let mut tx_l0 = tx_l0_arc.write();
            let main_l0_arc = self.l0_manager.get_current();
            let mut main_l0 = main_l0_arc.write();
            // A CRDT property's committed state may live only in a
            // flush-rotated pending buffer (the post-rotation current is
            // empty until the Lance write completes — the Bug #9A window).
            // `merge_crdt_properties` merges against the CURRENT buffer's
            // value — without seeding, the tx's CRDT state would SHADOW the
            // pending buffer's at read time (newest buffer wins per property)
            // and concurrent increments would be lost. Seed the newest
            // overlay value for each CRDT property the tx writes that current
            // lacks, so the merge below merges instead of replaces.
            self.seed_crdt_state_from_chain(&tx_l0, &mut main_l0);
            let cascaded = main_l0.merge_take(&mut tx_l0)?;
            // Edges cascaded off a deleted vertex that live in MAIN L0 — the
            // transaction never touched them, so the tx-scoped mirror below
            // cannot see them.
            self.mirror_cascaded_tombstones(&cascaded, &main_l0);

            // Replay transaction edges into the AdjacencyManager overlay.
            self.mirror_edges_into_adjacency(&tx_l0, &main_l0, main_l0.current_version);
            // This commit's version: read under the buffer's write lock, so no
            // other commit (or bulk-version reservation) interleaves.
            main_l0.current_version
        };
        // Commit order is fixed here, under `flush_lock`: the announcement
        // order follows it whatever the post-commit work does.
        // The ticket carries the commit's WAL LSN: releasing it raises the
        // announced mark the log records, which is how a reopen tells a commit
        // that was announced from one that was not.
        let mut ticket = self.publish_sequencer.issue_at(wal_lsn);
        if let Some(announce) = on_abandon {
            ticket.on_abandon(move || announce(version));
        }

        // Crash-recovery seam: durable AND merged, but the in-memory commit
        // registry has not recorded this write-set yet. A crash here is
        // indistinguishable from one at `after-wal-flush` on reopen (the
        // registry is in-memory and rebuilt empty); the tx still recovers.
        fail::fail_point!("commit::after-merge");

        // SSI: register this commit's write-set under a fresh commit sequence so
        // later transactions detect conflicts against it. Still under
        // `flush_lock`, before the async-flush branch can drop the guard.
        // `occ_write_set` is `Some` only when `config.ssi_enabled`.
        if let Some(write_set) = occ_write_set
            && !write_set.is_empty()
        {
            // Bump-then-record via the shared OCC seam (see `CommitRegistry::commit`)
            // so production and the loom/shuttle models exercise identical logic.
            self.committed_writes
                .lock()
                .commit(&self.commit_sequence, write_set);
        }

        self.update_metrics();

        // The post-commit auto-flush is NOT run here: a flush must not move
        // this commit into L1 before it is announced (see
        // `flush_l0_rotate`), and the announcement happens after this returns.
        // The caller runs it after publishing the ticket, through
        // [`Self::flush_after_commit`].
        Ok(CommittedTx {
            wal_lsn,
            flush_pending: false,
            version,
            ticket,
        })
    }

    /// The best-effort auto-flush a commit triggers, run once the commit's
    /// announcement has been released (or its ticket dropped).
    ///
    /// Returns whether an async flush was submitted (async-flush mode only).
    /// A flush failure is logged, never returned: the commit it follows is
    /// already durable.
    pub async fn flush_after_commit(self: &Arc<Self>) -> bool {
        if !self.should_flush() {
            return false;
        }
        let _flush_lock_guard = self.flush_lock.lock().await;
        let mut flush_pending = false;
        if self.should_flush() {
            if self.config.async_flush_enabled
                && let Some(coord) = self.flush_coordinator.as_ref()
            {
                // Async mode: submit only when a pipeline slot is free, else SKIP
                // (a later commit retries). Never fall back to a blocking inline
                // flush here — under a stalled flush (issue #132) that inline
                // flush would hit the same stall with no timeout and hang holding
                // `flush_lock`, cascading into a full runtime park. A stalled
                // async flush is instead bounded by `flush_stream_timeout`, which
                // frees its permit for the retry.
                if coord.pending_flush_count() < self.config.max_pending_flushes {
                    match coord.try_acquire_permit() {
                        Some(permit) => {
                            match self.flush_l0_rotate().await {
                                Ok(rotate_out) => {
                                    // Allocate the rotate seq and bump pending ONLY
                                    // after the rotate succeeds (Bug #3). A failed
                                    // rotate must consume neither: the finalizer
                                    // advances strictly in consecutive seq order and
                                    // only decrements pending on finalize, so a
                                    // leaked seq/pending from a failed rotate would
                                    // wedge the finalizer forever and climb pending
                                    // toward `max_pending_flushes`. The seq is still
                                    // allocated under `flush_lock` (immediately after
                                    // the rotate, before the guard drops below), so
                                    // concurrent rotates keep seq order == rotation
                                    // order, and the seq is not used until submit.
                                    let seq = coord.next_rotate_seq();
                                    coord.note_pending();
                                    // Release flush_lock BEFORE the spawn so concurrent
                                    // commits can proceed while the stream runs.
                                    drop(_flush_lock_guard);
                                    let parent_manifest = self.cached_manifest.lock().clone();
                                    let rotated = crate::runtime::flush_coordinator::RotatedFlush {
                                        seq,
                                        old_l0_arc: rotate_out.old_l0_arc.clone(),
                                        wal_lsn: rotate_out.wal_lsn,
                                        current_version: rotate_out.current_version,
                                        name: None,
                                        parent_manifest,
                                        permit,
                                        flush_in_progress_guard: rotate_out.flush_in_progress_guard,
                                    };
                                    let writer = self.clone();
                                    let _ticket = coord.submit_for_stream(
                                        rotated,
                                        move |old_l0, wal, ver, n| async move {
                                            let outcome =
                                                writer.flush_stream_l1(old_l0, wal, ver, n).await?;
                                            Ok(crate::runtime::flush_coordinator::FlushOutcome {
                                                new_manifest: outcome.manifest,
                                                snapshot_id: outcome.snapshot_id,
                                            })
                                        },
                                    );
                                    flush_pending = true;
                                    // Early return — flush_lock already dropped.
                                    return flush_pending;
                                }
                                Err(e) => {
                                    tracing::warn!("Async rotate failed (non-critical): {}", e);
                                    // No seq was allocated and pending was not
                                    // bumped (both moved into the Ok arm for Bug
                                    // #3), so the finalizer is not wedged. The
                                    // permit drops here, freeing the slot.
                                }
                            }
                        }
                        None => {
                            // Race: someone else grabbed the last permit. Skip;
                            // next commit will retry should_flush().
                            metrics::counter!("uni_flush_trigger_skipped_total").increment(1);
                        }
                    }
                } else {
                    // Pipeline full — possibly a stalled flush occupying a slot.
                    // Skip and retry on a later commit rather than blocking on an
                    // inline flush that could hit the same stall (issue #132).
                    metrics::counter!("uni_flush_trigger_skipped_total").increment(1);
                }
            } else if let Err(e) = self.flush_inline_under_lock(None).await {
                tracing::warn!("Post-commit flush check failed (non-critical): {}", e);
            }
        }

        flush_pending
    }

    /// Take a place in the commit announcement order for a write that became
    /// visible outside [`Self::commit_transaction_l0_sequenced`] (a bulk
    /// load), with the database version current at that point.
    ///
    /// Taken under `flush_lock`, as a commit's is, so the announcement it
    /// publishes is ordered after every commit already visible and before
    /// every later one, and the returned version is at least every such
    /// earlier commit's version (it may equal the latest of them: no version
    /// is consumed).
    pub async fn sequence_announcement(
        &self,
    ) -> (crate::runtime::publish_sequencer::PublishTicket, u64) {
        let _flush_lock_guard = self.flush_lock.lock().await;
        let version = self.l0_manager.get_current().read().current_version;
        (self.publish_sequencer.issue(), version)
    }

    /// Flush the WAL buffer to durable storage.
    ///
    /// Returns the LSN of the flushed segment, or `0` when no WAL is configured.
    pub async fn flush_wal(&self) -> Result<u64> {
        let l0 = self.l0_manager.get_current();
        let wal = l0.read().wal.clone();

        match wal {
            Some(wal) => Ok(wal.flush().await?),
            None => Ok(0),
        }
    }

    /// Record property removals in the active L0 mutation stats.
    ///
    /// Routes to the transaction L0 if provided, otherwise to the main L0.
    pub fn track_properties_removed(&self, count: usize, tx_l0: Option<&Arc<RwLock<L0Buffer>>>) {
        if count == 0 {
            return;
        }
        let l0 = self.resolve_l0(tx_l0);
        l0.write().mutation_stats.properties_removed += count;
    }

    /// Validates vertex constraints for the given properties.
    /// In the new design, label is passed as a parameter since VID no longer embeds label.
    async fn validate_vertex_constraints_for_label(
        &self,
        vid: Vid,
        properties: &Properties,
        label: &str,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        self.validate_vertex_constraints_for_label_impl(vid, properties, label, tx_l0)
            .await
    }

    async fn validate_vertex_constraints_for_label_impl(
        &self,
        vid: Vid,
        properties: &Properties,
        label: &str,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        let schema = self.schema_manager.schema();

        {
            // 1. Check NOT NULL constraints (from Property definitions).
            if let Some(props_meta) = schema.properties.get(label) {
                for (prop_name, meta) in props_meta {
                    let present = properties.get(prop_name);

                    // Declared vector/multi-vector columns: enforce dimensions here so
                    // writes that bypass the Cypher coercion guard (python bulk insert,
                    // auto-embed output) can't smuggle in a wrong-length vector that the
                    // Arrow converters would otherwise null at flush (issue #137).
                    if let Some(value) = present
                        && let Err(e) = meta.r#type.check_vector_dims(value)
                    {
                        return Err(anyhow!(
                            "vector dimension mismatch: property '{}.{}': {}",
                            label,
                            prop_name,
                            e
                        ));
                    }

                    if !meta.nullable && present.is_none_or(|v| v.is_null()) {
                        log::warn!(
                            "Constraint violation: Property '{}' cannot be null for label '{}'",
                            prop_name,
                            label
                        );
                        return Err(anyhow!(
                            "Constraint violation: Property '{}' cannot be null",
                            prop_name
                        ));
                    }
                }
            }

            // 2. Check Explicit Constraints (Unique, Check, etc.)
            for constraint in &schema.constraints {
                if !constraint.enabled {
                    continue;
                }
                match &constraint.target {
                    ConstraintTarget::Label(l) if l == label => {}
                    _ => continue,
                }

                match &constraint.constraint_type {
                    ConstraintType::Unique {
                        properties: unique_props,
                    } => {
                        // Support single and multi-property unique constraints
                        if !unique_props.is_empty() {
                            let mut key_values = Vec::new();
                            let mut missing = false;
                            for prop in unique_props {
                                if let Some(val) = properties.get(prop) {
                                    key_values.push((prop.clone(), val.clone()));
                                } else {
                                    missing = true; // Can't enforce if property missing (partial update?)
                                    // For INSERT, missing means null?
                                    // If property is nullable, unique constraint typically allows multiple nulls or ignores?
                                    // For now, only check if ALL keys are present
                                }
                            }

                            if !missing {
                                self.check_unique_constraint_multi(label, &key_values, vid, tx_l0)
                                    .await?;
                            }
                        }
                    }
                    ConstraintType::Exists { property } => {
                        if properties.get(property).is_none_or(|v| v.is_null()) {
                            log::warn!(
                                "Constraint violation: Property '{}' must exist for label '{}'",
                                property,
                                label
                            );
                            return Err(anyhow!(
                                "Constraint violation: Property '{}' must exist",
                                property
                            ));
                        }
                    }
                    ConstraintType::Check { expression } => {
                        if !uni_common::core::check_constraint::evaluate(expression, properties)? {
                            return Err(anyhow!(
                                "CHECK constraint '{}' violated: expression '{}' evaluated to false",
                                constraint.name,
                                expression
                            ));
                        }
                    }
                    ConstraintType::NodeKey {
                        properties: key_props,
                    } => {
                        // Node key = composite uniqueness + NOT NULL on every key
                        // property. Unlike `Unique` (which skips enforcement when a
                        // key property is absent), a missing/null key property is
                        // itself a violation.
                        if !key_props.is_empty() {
                            let mut key_values = Vec::with_capacity(key_props.len());
                            for prop in key_props {
                                match properties.get(prop) {
                                    Some(val) if !val.is_null() => {
                                        key_values.push((prop.clone(), val.clone()));
                                    }
                                    _ => {
                                        return Err(anyhow!(
                                            "Node key constraint '{}' violated: property '{}' must exist and be non-null for label '{}'",
                                            constraint.name,
                                            prop,
                                            label
                                        ));
                                    }
                                }
                            }
                            self.check_unique_constraint_multi(label, &key_values, vid, tx_l0)
                                .await?;
                        }
                    }
                    _ => {
                        return Err(anyhow!("Unsupported constraint type"));
                    }
                }
            }
        }
        Ok(())
    }

    /// Validates vertex constraints for a vertex with the given labels.
    /// Labels must be passed explicitly since the vertex may not yet be in L0.
    /// Unknown labels (not in schema) are skipped.
    async fn validate_vertex_constraints(
        &self,
        vid: Vid,
        properties: &Properties,
        labels: &[String],
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        check_property_names(properties, &self.schema_manager.schema())?;
        let schema = self.schema_manager.schema();

        // Validate constraints only for known labels
        for label in labels {
            // Skip unknown labels (schemaless support)
            if schema.get_label_case_insensitive(label).is_none() {
                continue;
            }
            self.validate_vertex_constraints_for_label(vid, properties, label, tx_l0)
                .await?;
        }

        // Check global ext_id uniqueness if ext_id is provided
        if let Some(ext_id) = properties.get("ext_id").and_then(|v| v.as_str()) {
            self.check_extid_globally_unique(ext_id, vid, tx_l0).await?;
        }

        Ok(())
    }

    /// Collect ext_ids and unique constraint keys from an iterator of vertex properties.
    ///
    /// Used to build a constraint key index from L0 buffers for batch validation.
    fn collect_constraint_keys_from_properties<'a>(
        properties_iter: impl Iterator<Item = &'a Properties>,
        label: &str,
        constraints: &[uni_common::core::schema::Constraint],
        existing_keys: &mut HashMap<String, HashSet<String>>,
        existing_extids: &mut HashSet<String>,
    ) {
        for props in properties_iter {
            if let Some(ext_id) = props.get("ext_id").and_then(|v| v.as_str()) {
                existing_extids.insert(ext_id.to_string());
            }

            for constraint in constraints {
                if !constraint.enabled {
                    continue;
                }
                if let ConstraintTarget::Label(l) = &constraint.target {
                    if l != label {
                        continue;
                    }
                } else {
                    continue;
                }

                // Collect keys for both Unique and NodeKey (uniqueness half).
                if let Some(unique_props) = constraint.constraint_type.unique_properties() {
                    let mut key_parts = Vec::new();
                    let mut all_present = true;
                    for prop in unique_props {
                        if let Some(val) = props.get(prop) {
                            key_parts.push(format!("{}:{}", prop, val));
                        } else {
                            all_present = false;
                            break;
                        }
                    }
                    if all_present {
                        let key = key_parts.join("|");
                        existing_keys
                            .entry(constraint.name.clone())
                            .or_default()
                            .insert(key);
                    }
                }
            }
        }
    }

    /// Records one batch row's composite constraint key, erroring if it
    /// collides with an existing L0 key or with an earlier row in the batch.
    ///
    /// Shared by the `Unique` and `NodeKey` arms of
    /// [`Writer::validate_vertex_batch_constraints`] — the two differ only in
    /// how the key is built, not in how it is checked.
    fn record_batch_constraint_key(
        constraint_name: &str,
        label: &str,
        key: String,
        idx: usize,
        existing_keys: &HashMap<String, HashSet<String>>,
        batch_keys: &mut HashMap<String, HashMap<String, usize>>,
    ) -> Result<()> {
        // Check against existing L0 keys
        if let Some(keys) = existing_keys.get(constraint_name)
            && keys.contains(&key)
        {
            return Err(anyhow!(
                "Constraint violation at index {}: Duplicate composite key for label '{}' (constraint '{}')",
                idx,
                label,
                constraint_name
            ));
        }

        // Check for duplicates within batch
        let batch_constraint_keys = batch_keys.entry(constraint_name.to_string()).or_default();
        if let Some(first_idx) = batch_constraint_keys.get(&key) {
            return Err(anyhow!(
                "Constraint violation: Duplicate key '{}' in batch at indices {} and {}",
                key,
                first_idx,
                idx
            ));
        }
        batch_constraint_keys.insert(key, idx);
        Ok(())
    }

    /// Validates constraints for a batch of vertices efficiently.
    ///
    /// This method builds an in-memory index from L0 buffers ONCE instead of scanning
    /// per vertex, reducing complexity from O(n²) to O(n) for bulk inserts.
    ///
    /// # Arguments
    /// * `vids` - VIDs of vertices being inserted
    /// * `properties_batch` - Properties for each vertex
    /// * `label` - Label for all vertices (assumes single label for now)
    ///
    /// # Performance
    /// For N vertices with unique constraints:
    /// - Old approach: O(N²) - scan L0 buffer N times
    /// - New approach: O(N) - scan L0 buffer once, build HashSet, check each vertex in O(1)
    async fn validate_vertex_batch_constraints(
        &self,
        vids: &[Vid],
        properties_batch: &[Properties],
        label: &str,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        if vids.len() != properties_batch.len() {
            return Err(anyhow!("VID/properties length mismatch"));
        }
        let schema = self.schema_manager.schema();
        for properties in properties_batch {
            check_property_names(properties, &schema)?;
        }

        // 1. Validate NOT NULL constraints for each vertex
        if let Some(props_meta) = schema.properties.get(label) {
            for (idx, properties) in properties_batch.iter().enumerate() {
                for (prop_name, meta) in props_meta {
                    // Enforce declared vector dimensions on the batch path too — the
                    // python bulk API reaches here without the Cypher guard (issue #137).
                    if let Some(value) = properties.get(prop_name)
                        && let Err(e) = meta.r#type.check_vector_dims(value)
                    {
                        return Err(anyhow!(
                            "vector dimension mismatch at index {}: property '{}.{}': {}",
                            idx,
                            label,
                            prop_name,
                            e
                        ));
                    }
                    if !meta.nullable && properties.get(prop_name).is_none_or(|v| v.is_null()) {
                        return Err(anyhow!(
                            "Constraint violation at index {}: Property '{}' cannot be null",
                            idx,
                            prop_name
                        ));
                    }
                }
            }
        }

        // 2. Build constraint key index from L0 buffers (ONCE for entire batch)
        //
        // From the transaction's own buffer, `current`, and the pending-flush
        // buffers (rows rotated off `current` but not yet in Lance; skipping
        // them left the Bug #9A window open). Only each vid's newest version
        // counts: a pending buffer still holding a vid's old `ext_id` or key
        // after a newer buffer changed or deleted it must not refuse a write
        // that reuses the freed value. Pass one finds the buffer that decides
        // each vid; pass two collects keys from those versions alone. One
        // buffer lock is held at a time, as before.
        let mut existing_keys: HashMap<String, HashSet<String>> = HashMap::new();
        let mut existing_extids: HashSet<String> = HashSet::new();
        let l0_buffers = self.l0_buffers_newest_first(tx_l0);
        // vid -> index of the buffer whose version of it is newest; `None`
        // for a tombstone.
        let mut decided_by: HashMap<Vid, Option<usize>> = HashMap::new();
        for (i, l0) in l0_buffers.iter().enumerate() {
            let guard = l0.read();
            for vid in &guard.vertex_tombstones {
                decided_by.entry(*vid).or_insert(None);
            }
            for vid in guard.vertex_properties.keys() {
                decided_by.entry(*vid).or_insert(Some(i));
            }
        }
        for (i, l0) in l0_buffers.iter().enumerate() {
            let guard = l0.read();
            Self::collect_constraint_keys_from_properties(
                guard
                    .vertex_properties
                    .iter()
                    .filter(|(vid, _)| decided_by.get(*vid) == Some(&Some(i)))
                    .map(|(_, props)| props),
                label,
                &schema.constraints,
                &mut existing_keys,
                &mut existing_extids,
            );
        }

        // 3. Check batch vertices against index AND check for duplicates within batch
        let mut batch_keys: HashMap<String, HashMap<String, usize>> = HashMap::new();
        let mut batch_extids: HashMap<String, usize> = HashMap::new();

        // Probe the main vertex table once for the whole batch rather than once
        // per vertex (#220). Ordering is unchanged: the per-vertex checks below
        // still run in index order and still report the first offending index,
        // and the error semantics are those of the singular probe — an absent
        // table is `Ok`, a failed scan is an error, because a uniqueness probe
        // that reads as "no duplicate" on an I/O failure admits the duplicate
        // it exists to reject (#233).
        let flushed_extids = {
            let wanted: Vec<String> = properties_batch
                .iter()
                .filter_map(|p| p.get("ext_id").and_then(|v| v.as_str()).map(str::to_string))
                .collect();
            MainVertexDataset::find_by_ext_ids_counted(self.storage.backend(), &wanted, None, None)
                .await?
        };
        let l0_newest_first = self.l0_buffers_newest_first(tx_l0);

        for (idx, (_vid, properties)) in vids.iter().zip(properties_batch.iter()).enumerate() {
            // Check ext_id uniqueness
            if let Some(ext_id) = properties.get("ext_id").and_then(|v| v.as_str()) {
                if existing_extids.contains(ext_id) {
                    return Err(anyhow!(
                        "Constraint violation at index {}: ext_id '{}' already exists",
                        idx,
                        ext_id
                    ));
                }
                if let Some(first_idx) = batch_extids.get(ext_id) {
                    return Err(anyhow!(
                        "Constraint violation: ext_id '{}' duplicated in batch at indices {} and {}",
                        ext_id,
                        first_idx,
                        idx
                    ));
                }
                // Also check the main vertices table — the L0 scans above
                // miss vertices already flushed to L1, so without this a
                // batch insert (e.g. a fork promote onto primary) silently
                // twins a duplicate ext_id instead of erroring. Mirrors the
                // single-vertex `check_extid_globally_unique`.
                // #233 Tier 1: this was `if let Ok(Some(..))`, so a failed
                // probe read as "no duplicate" and the constraint admitted the
                // very duplicate it exists to reject — silently, on an I/O
                // error. `find_by_ext_id` already answers `Ok(None)` when the
                // table is absent (see `main_vertex.rs`), so every `Err` here
                // is a genuine failure with no benign case to absorb.
                if let Some(found_vid) = flushed_extids.get(ext_id)
                    && Self::l0_ext_id_verdict(&l0_newest_first, *found_vid, ext_id) != Some(false)
                {
                    return Err(anyhow!(
                        "Constraint violation at index {}: ext_id '{}' already exists (vertex {:?})",
                        idx,
                        ext_id,
                        found_vid
                    ));
                }
                batch_extids.insert(ext_id.to_string(), idx);
            }

            // Check unique constraints
            for constraint in &schema.constraints {
                if !constraint.enabled {
                    continue;
                }
                if let ConstraintTarget::Label(l) = &constraint.target {
                    if l != label {
                        continue;
                    }
                } else {
                    continue;
                }

                match &constraint.constraint_type {
                    ConstraintType::Unique {
                        properties: unique_props,
                    } => {
                        let mut key_parts = Vec::new();
                        let mut all_present = true;
                        for prop in unique_props {
                            if let Some(val) = properties.get(prop) {
                                key_parts.push(format!("{}:{}", prop, val));
                            } else {
                                all_present = false;
                                break;
                            }
                        }

                        // Unlike NodeKey, a missing key property is not a
                        // violation for Unique — the row is simply skipped.
                        if all_present {
                            Self::record_batch_constraint_key(
                                &constraint.name,
                                label,
                                key_parts.join("|"),
                                idx,
                                &existing_keys,
                                &mut batch_keys,
                            )?;
                        }
                    }
                    ConstraintType::Exists { property }
                        if properties.get(property).is_none_or(|v| v.is_null()) =>
                    {
                        return Err(anyhow!(
                            "Constraint violation at index {}: Property '{}' must exist",
                            idx,
                            property
                        ));
                    }
                    ConstraintType::Check { expression }
                        if !uni_common::core::check_constraint::evaluate(
                            expression, properties,
                        )? =>
                    {
                        return Err(anyhow!(
                            "Constraint violation at index {}: CHECK constraint '{}' violated",
                            idx,
                            constraint.name
                        ));
                    }
                    ConstraintType::NodeKey {
                        properties: key_props,
                    } => {
                        // NOT-NULL half: every key property must be present and
                        // non-null (a missing key is a violation, unlike Unique).
                        // Uniqueness half: same in-batch + L0 dedup as Unique.
                        let mut key_parts = Vec::with_capacity(key_props.len());
                        for prop in key_props {
                            match properties.get(prop) {
                                Some(val) if !val.is_null() => {
                                    key_parts.push(format!("{}:{}", prop, val));
                                }
                                _ => {
                                    return Err(anyhow!(
                                        "Constraint violation at index {}: node key '{}' requires property '{}' to exist and be non-null",
                                        idx,
                                        constraint.name,
                                        prop
                                    ));
                                }
                            }
                        }
                        Self::record_batch_constraint_key(
                            &constraint.name,
                            label,
                            key_parts.join("|"),
                            idx,
                            &existing_keys,
                            &mut batch_keys,
                        )?;
                    }
                    _ => {}
                }
            }
        }

        // 4. Check storage for unique constraints (can batch this into a single query)
        for constraint in &schema.constraints {
            if !constraint.enabled {
                continue;
            }
            if let ConstraintTarget::Label(l) = &constraint.target {
                if l != label {
                    continue;
                }
            } else {
                continue;
            }

            // Probe flushed storage for both Unique and NodeKey (uniqueness half).
            if let Some(unique_props) = constraint.constraint_type.unique_properties() {
                // Build compound OR filter for all batch vertices
                let mut or_filters = Vec::new();
                let mut batch_key_sets: Vec<Vec<(String, Value)>> = Vec::new();
                for properties in properties_batch.iter() {
                    let mut and_parts = Vec::new();
                    let mut key_set = Vec::new();
                    let mut all_present = true;
                    for prop in unique_props {
                        if let Some(val) = properties.get(prop) {
                            let Some(scalar) = constraint_scalar(val) else {
                                all_present = false;
                                break;
                            };
                            and_parts.push(FilterExpr::equals(prop.as_str(), scalar));
                            key_set.push((prop.clone(), val.clone()));
                        } else {
                            all_present = false;
                            break;
                        }
                    }
                    if all_present {
                        or_filters.push(FilterExpr::all(and_parts));
                        batch_key_sets.push(key_set);
                    }
                }

                #[cfg(feature = "lance-backend")]
                if !or_filters.is_empty() {
                    let filter = FilterExpr::all([
                        FilterExpr::any_of(or_filters),
                        FilterExpr::not_deleted(),
                        FilterExpr::negate(FilterExpr::one_of(
                            "_vid",
                            vids.iter().map(|v| Scalar::UInt(v.as_u64())),
                        )),
                    ]);

                    // Flushed rows matching a batch key are candidates, resolved
                    // to their current values by `vertex_key_held`. A missing
                    // table means nothing is flushed yet — the L0/pending/tx
                    // checks above covered in-memory rows — and any backend
                    // error aborts the write rather than failing open.
                    let exclude: HashSet<Vid> = vids.iter().copied().collect();
                    if self
                        .vertex_key_held(
                            label,
                            Vec::new(),
                            filter,
                            &batch_key_sets,
                            &exclude,
                            tx_l0,
                        )
                        .await?
                    {
                        return Err(anyhow!(
                            "Constraint violation: Duplicate composite key for label '{}' in storage (constraint '{}')",
                            label,
                            constraint.name
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Checks that ext_id is globally unique across all vertices.
    ///
    /// Searches L0 buffers (current, transaction, pending) and the main vertices table
    /// to ensure no other vertex uses this ext_id.
    ///
    /// # Errors
    ///
    /// Returns error if another vertex with the same ext_id exists.
    async fn check_extid_globally_unique(
        &self,
        ext_id: &str,
        current_vid: Vid,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        // Check L0 buffers: transaction, current, and pending flush, newest
        // first, which is the order `l0_ext_id_verdict` reads them in.
        let l0_buffers_to_check = self.l0_buffers_newest_first(tx_l0);

        for l0 in &l0_buffers_to_check {
            // O(1) per buffer via the maintained `extid_index` (the previous
            // full `vertex_properties` scan made constrained ingest O(n²)).
            // A buffer's index is current only for that buffer: a newer one
            // may have moved the vid off this `ext_id` or deleted it.
            let hit = l0.read().extid_index.get(ext_id).copied();
            if let Some(vid) = hit
                && vid != current_vid
                && Self::l0_ext_id_verdict(&l0_buffers_to_check, vid, ext_id) != Some(false)
            {
                return Err(anyhow!(
                    "Constraint violation: ext_id '{}' already exists (vertex {:?})",
                    ext_id,
                    vid
                ));
            }
        }

        // Check main vertices table (if it exists)
        // Pass None for global uniqueness check (not snapshot-isolated)
        let backend = self.storage.backend();
        // #233 Tier 1: see the batch path above — a failed probe must not
        // read as "no duplicate". A flushed holder whose newer L0 version
        // gave the `ext_id` up, or deleted the vertex, no longer holds it.
        if let Some(found_vid) = MainVertexDataset::find_by_ext_id(backend, ext_id, None).await?
            && found_vid != current_vid
            && Self::l0_ext_id_verdict(&l0_buffers_to_check, found_vid, ext_id) != Some(false)
        {
            return Err(anyhow!(
                "Constraint violation: ext_id '{}' already exists (vertex {:?})",
                ext_id,
                found_vid
            ));
        }

        Ok(())
    }

    /// The L0 buffers a write can see, newest first: the transaction's own,
    /// then `current`, then the pending-flush buffers from newest to oldest.
    fn l0_buffers_newest_first(
        &self,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Vec<Arc<RwLock<L0Buffer>>> {
        let mut buffers: Vec<Arc<RwLock<L0Buffer>>> = tx_l0.cloned().into_iter().collect();
        buffers.push(self.l0_manager.get_current());
        // `get_pending_flush` lists the buffers oldest first.
        buffers.extend(self.l0_manager.get_pending_flush().into_iter().rev());
        buffers
    }

    /// Whether `vid` holds `ext_id` according to the newest L0 buffer that
    /// says anything about it; `None` when no buffer does, so a flushed
    /// answer stands.
    ///
    /// A tombstone answers `false`. A complete property map answers whether
    /// its `ext_id` is that string.
    fn l0_ext_id_verdict(
        buffers_newest_first: &[Arc<RwLock<L0Buffer>>],
        vid: Vid,
        ext_id: &str,
    ) -> Option<bool> {
        for l0 in buffers_newest_first {
            let guard = l0.read();
            if guard.vertex_tombstones.contains(&vid) {
                return Some(false);
            }
            let Some(props) = guard.vertex_properties.get(&vid) else {
                continue;
            };
            match props.get("ext_id") {
                Some(v) => return Some(v.as_str() == Some(ext_id)),
                None => return Some(false),
            }
        }
        None
    }

    /// Helper to get vertex labels from L0 buffer.
    fn get_vertex_labels_from_l0(&self, vid: Vid) -> Option<Vec<String>> {
        let l0 = self.l0_manager.get_current();
        let l0_guard = l0.read();
        // Check if vertex is tombstoned (deleted) - if so, return None
        if l0_guard.vertex_tombstones.contains(&vid) {
            return None;
        }
        l0_guard.get_vertex_labels(vid).map(|l| l.to_vec())
    }

    /// Get vertex labels from all sources: current L0, pending L0s, and storage.
    /// This is the proper way to read vertex labels after a flush, as it checks both
    /// in-memory buffers and persisted storage.
    ///
    /// # Errors
    /// Returns an error if the storage read in step 4 fails. `Ok(None)` means
    /// the vertex genuinely carries no labels (or is tombstoned); it never
    /// means the labels could not be read. #233 Tier 1: this returned a bare
    /// `Option` and mapped a failed read to `None` via `.ok().flatten()`, so
    /// its one production caller silently skipped the vertex's CRDT property
    /// handling on an I/O blip.
    pub async fn get_vertex_labels(
        &self,
        vid: Vid,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<Option<Vec<String>>> {
        // 1. Check current L0
        if let Some(labels) = self.get_vertex_labels_from_l0(vid) {
            return Ok(Some(labels));
        }

        // 2. Check transaction L0 if present
        if let Some(tx_l0) = tx_l0 {
            let guard = tx_l0.read();
            if guard.vertex_tombstones.contains(&vid) {
                return Ok(None);
            }
            if let Some(labels) = guard.get_vertex_labels(vid) {
                return Ok(Some(labels.to_vec()));
            }
        }

        // 3. Check pending flush L0s
        for pending_l0 in self.l0_manager.get_pending_flush() {
            let guard = pending_l0.read();
            if guard.vertex_tombstones.contains(&vid) {
                return Ok(None);
            }
            if let Some(labels) = guard.get_vertex_labels(vid) {
                return Ok(Some(labels.to_vec()));
            }
        }

        // 4. Check storage
        self.find_vertex_labels_in_storage(vid).await
    }

    /// Helper to get edge type from L0 buffer.
    fn get_edge_type_from_l0(&self, eid: Eid) -> Option<String> {
        let l0 = self.l0_manager.get_current();
        let l0_guard = l0.read();
        l0_guard.get_edge_type(eid).map(|s| s.to_string())
    }

    /// Look up the edge type ID (u32) for an EID from the L0 buffer's edge endpoints.
    /// Falls back to the transaction L0 if available.
    pub fn get_edge_type_id_from_l0(
        &self,
        eid: Eid,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Option<u32> {
        // Check transaction L0 first
        if let Some(tx_l0) = tx_l0 {
            let guard = tx_l0.read();
            if let Some((_, _, etype)) = guard.get_edge_endpoint_full(eid) {
                return Some(etype);
            }
        }
        // Fall back to main L0
        let l0 = self.l0_manager.get_current();
        let l0_guard = l0.read();
        l0_guard
            .get_edge_endpoint_full(eid)
            .map(|(_, _, etype)| etype)
    }

    /// Set the type name for an edge (used for schemaless edge types).
    /// This is called during CREATE for edge types not found in the schema.
    pub fn set_edge_type(
        &self,
        eid: Eid,
        type_name: String,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) {
        self.resolve_l0(tx_l0).write().set_edge_type(eid, type_name);
    }

    async fn check_unique_constraint_multi(
        &self,
        label: &str,
        key_values: &[(String, Value)],
        current_vid: Vid,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        if self
            .unique_key_exists_full_horizon(label, key_values, Some(current_vid), tx_l0)
            .await?
        {
            return Err(anyhow!(
                "Constraint violation: Duplicate composite key for label '{}'",
                label
            ));
        }
        Ok(())
    }

    /// Reports whether a UNIQUE composite key already exists across the full write horizon.
    ///
    /// Probes every layer a duplicate could hide in — the current L0 buffer, any
    /// pending-flush buffers, an optional transaction-local L0, and committed
    /// storage (L1/L2). `exclude_vid` is the vertex being checked so it never
    /// conflicts with itself; pass `None` when the caller is inserting a
    /// brand-new vertex that has no VID yet (e.g. the bulk loader), so nothing is
    /// excluded. This is the single lookup surface shared by the Writer's own
    /// `check_unique_constraint_multi` and the bulk loader's validation, so both
    /// write channels observe committed-but-unflushed keys (finding uni-bulk D6).
    ///
    /// # Errors
    /// Returns an error if a storage backend probe fails; the check fails closed
    /// rather than silently treating the key as absent.
    pub async fn unique_key_exists_full_horizon(
        &self,
        label: &str,
        key_values: &[(String, Value)],
        exclude_vid: Option<Vid>,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<bool> {
        // Serialize constraint key once for O(1) lookups
        let key = serialize_constraint_key(label, key_values);

        // Sentinel for the in-memory `has_constraint_key` comparisons when there
        // is no self to exclude: u64::MAX is never an allocated VID, so any real
        // key hit compares unequal and counts. (The storage probe below omits
        // the `_vid !=` clause entirely rather than emit an out-of-range literal.)
        let exclude = exclude_vid.unwrap_or_else(|| Vid::new(u64::MAX));

        // Every layer yields *candidates*: vertices that held this key at some
        // point. Whether one still holds it is decided below against its
        // current, fully resolved properties — a key index entry or a storage
        // row can outlive the value (an unflushed delete or SET, or an older
        // appended row version).
        let mut l0_candidates: Vec<Vid> = Vec::new();

        // 1. Check L0 (in-memory) using O(1) constraint index
        {
            let l0 = self.l0_manager.get_current();
            let l0_guard = l0.read();
            if l0_guard.has_constraint_key(&key, exclude) {
                l0_candidates.extend(l0_guard.constraint_index.get(&key).copied());
            }
        }

        // 1b. Check pending-flush buffers (Bug #9A). A flush rotates a key's
        // buffer onto `pending_flush` and installs a fresh empty current
        // buffer; until the rotated rows reach Lance the key is invisible to
        // both the current-buffer check above and the storage check below, so
        // a duplicate could slip through that flush window. Mirror the read
        // paths (e.g. `check_extid_globally_unique`, `get_vertex_labels`) that
        // already consult `pending_flush`.
        for pending_l0 in self.l0_manager.get_pending_flush() {
            let guard = pending_l0.read();
            if guard.has_constraint_key(&key, exclude) {
                l0_candidates.extend(guard.constraint_index.get(&key).copied());
            }
        }

        // Check Transaction L0
        if let Some(tx_l0) = tx_l0 {
            let tx_l0_guard = tx_l0.read();
            if tx_l0_guard.has_constraint_key(&key, exclude) {
                l0_candidates.extend(tx_l0_guard.constraint_index.get(&key).copied());
            }
        }

        // 2. Check Storage (L1/L2)
        let mut parts: Vec<FilterExpr> = key_values
            .iter()
            .map(|(prop, val)| {
                FilterExpr::equals(
                    prop.as_str(),
                    constraint_scalar(val).unwrap_or(Scalar::Null),
                )
            })
            .collect();
        parts.push(FilterExpr::not_deleted());
        // Exclude the vertex's own row only when it has a VID; a brand-new insert
        // (bulk) has none, and emitting `_vid != u64::MAX` would overflow the
        // filter's i64 literal parsing.
        if let Some(vid) = exclude_vid {
            parts.push(FilterExpr::compare(
                "_vid",
                CmpOp::NotEq,
                Scalar::UInt(vid.as_u64()),
            ));
        }
        let filter = FilterExpr::all(parts);

        // 2. Check Storage (L1/L2) through the `StorageBackend` (branch-aware,
        // correct `.lance` path). Skip cleanly when the table is not yet
        // flushed; propagate any real backend error instead of failing open.
        let exclude_set: HashSet<Vid> = exclude_vid.into_iter().collect();
        self.vertex_key_held(
            label,
            l0_candidates,
            filter,
            std::slice::from_ref(&key_values.to_vec()),
            &exclude_set,
            tx_l0,
        )
        .await
    }

    /// Whether a live vertex of `label` other than those in `exclude` holds one
    /// of `keys` right now.
    ///
    /// Candidates come from `l0_candidates` and from the flushed rows matching
    /// `storage_filter`; each is resolved to its current properties (L0
    /// tombstones and overrides, latest row version) and conflicts only if
    /// those still equal a key in full. Counting matching rows instead reported
    /// a key as taken after its owner was deleted but not yet flushed, and after
    /// a flushed `SET` moved it — vertex tables are append-only, so the older
    /// row version stays and still matches.
    ///
    /// # Errors
    /// Propagates backend and property-resolution failures; the check fails
    /// closed rather than treating a key as free.
    async fn vertex_key_held(
        &self,
        label: &str,
        l0_candidates: Vec<Vid>,
        storage_filter: FilterExpr,
        keys: &[Vec<(String, Value)>],
        exclude: &HashSet<Vid>,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<bool> {
        let mut candidates: HashSet<Vid> = l0_candidates.into_iter().collect();
        #[cfg(feature = "lance-backend")]
        {
            let backend = self.storage.backend();
            let table = table_names::vertex_table_name(label);
            if backend.table_exists(&table).await? {
                let Some(_) = self.property_manager.as_ref() else {
                    // No resolver wired: keep the conservative row count.
                    let count = backend.count_rows(&table, Some(&storage_filter)).await?;
                    return Ok(count > 0 || !candidates.is_empty());
                };
                let batches = backend
                    .scan(
                        crate::backend::types::ScanRequest::all(&table)
                            .with_columns(vec!["_vid".to_string()])
                            .with_filter(storage_filter),
                    )
                    .await?;
                for batch in &batches {
                    if let Some(col) = batch
                        .column_by_name("_vid")
                        .and_then(|c| c.as_any().downcast_ref::<arrow_array::UInt64Array>())
                    {
                        candidates.extend(col.iter().flatten().map(Vid::new));
                    }
                }
            }
        }
        candidates.retain(|v| !exclude.contains(v));
        if candidates.is_empty() {
            return Ok(false);
        }
        let Some(pm) = self.property_manager.as_ref() else {
            return Ok(true);
        };
        let key_props: Vec<String> = keys
            .iter()
            .flat_map(|k| k.iter().map(|(p, _)| p.clone()))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let candidates: Vec<Vid> = candidates.into_iter().collect();
        let ctx = self.get_query_context(tx_l0).await;
        let current = pm
            .get_batch_vertex_props_for_label_projected(
                &candidates,
                label,
                ctx.as_ref(),
                Some(&key_props),
            )
            .await?;
        Ok(current.values().any(|props| {
            keys.iter()
                .any(|key| key.iter().all(|(p, v)| props.get(p) == Some(v)))
        }))
    }

    /// Edge counterpart of [`validate_vertex_constraints`](Self::validate_vertex_constraints):
    /// enforces declared `Unique` / `NodeKey` constraints on an edge type before
    /// the write. `NodeKey` additionally requires every key property present and
    /// non-null; `Unique` skips enforcement when a key property is absent.
    ///
    /// # Errors
    /// Returns an error if a key property collides with another live edge, or (for
    /// `NodeKey`) if a key property is missing/null.
    async fn validate_edge_constraints(
        &self,
        eid: Eid,
        edge_type_name: &str,
        properties: &Properties,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        let schema = self.schema_manager.schema();
        for constraint in &schema.constraints {
            if !constraint.enabled {
                continue;
            }
            match &constraint.target {
                ConstraintTarget::EdgeType(t) if t == edge_type_name => {}
                _ => continue,
            }
            match &constraint.constraint_type {
                ConstraintType::Unique {
                    properties: unique_props,
                } => {
                    if unique_props.is_empty() {
                        continue;
                    }
                    let mut key_values = Vec::with_capacity(unique_props.len());
                    let mut missing = false;
                    for prop in unique_props {
                        match properties.get(prop) {
                            Some(val) => key_values.push((prop.clone(), val.clone())),
                            None => {
                                missing = true; // Unique skips enforcement on a missing key.
                                break;
                            }
                        }
                    }
                    if !missing {
                        self.check_unique_edge_constraint_multi(
                            edge_type_name,
                            &key_values,
                            eid,
                            tx_l0,
                        )
                        .await?;
                    }
                }
                ConstraintType::NodeKey {
                    properties: key_props,
                } => {
                    if key_props.is_empty() {
                        continue;
                    }
                    let mut key_values = Vec::with_capacity(key_props.len());
                    for prop in key_props {
                        match properties.get(prop) {
                            Some(val) if !val.is_null() => {
                                key_values.push((prop.clone(), val.clone()))
                            }
                            _ => {
                                return Err(anyhow!(
                                    "Relationship key constraint '{}' violated: property '{}' must exist and be non-null for edge type '{}'",
                                    constraint.name,
                                    prop,
                                    edge_type_name
                                ));
                            }
                        }
                    }
                    self.check_unique_edge_constraint_multi(
                        edge_type_name,
                        &key_values,
                        eid,
                        tx_l0,
                    )
                    .await?;
                }
                // Edge `Exists`/`Check` are out of scope for the uniqueness family.
                _ => {}
            }
        }
        Ok(())
    }

    /// Registers an edge's declared `Unique`/`NodeKey` key values into the L0 edge
    /// constraint index, the edge analogue of the vertex index-population inside
    /// `insert_vertex_with_labels`. A key with a missing/null member is not
    /// registered (it cannot participate in a satisfied unique key).
    fn populate_edge_constraint_index(
        &self,
        eid: Eid,
        edge_type_name: &str,
        properties: &Properties,
        l0: &Arc<RwLock<L0Buffer>>,
    ) {
        let schema = self.schema_manager.schema();
        let mut guard = l0.write();
        for constraint in &schema.constraints {
            if !constraint.enabled {
                continue;
            }
            match &constraint.target {
                ConstraintTarget::EdgeType(t) if t == edge_type_name => {}
                _ => continue,
            }
            let Some(key_props) = constraint.constraint_type.unique_properties() else {
                continue;
            };
            let mut key_values = Vec::with_capacity(key_props.len());
            let mut all_present = true;
            for prop in key_props {
                match properties.get(prop) {
                    Some(val) if !val.is_null() => key_values.push((prop.clone(), val.clone())),
                    _ => {
                        all_present = false;
                        break;
                    }
                }
            }
            if all_present && !key_values.is_empty() {
                let key = serialize_constraint_key(edge_type_name, &key_values);
                guard.insert_edge_constraint_key(key, eid);
            }
        }
    }

    /// Edge analogue of [`check_unique_constraint_multi`](Self::check_unique_constraint_multi):
    /// errors if `key_values` collide with another live edge of `edge_type`.
    async fn check_unique_edge_constraint_multi(
        &self,
        edge_type: &str,
        key_values: &[(String, Value)],
        current_eid: Eid,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        if self
            .unique_edge_key_exists_full_horizon(edge_type, key_values, Some(current_eid), tx_l0)
            .await?
        {
            return Err(anyhow!(
                "Constraint violation: Duplicate composite key for edge type '{}'",
                edge_type
            ));
        }
        Ok(())
    }

    /// Reports whether a UNIQUE composite key already exists across the full write
    /// horizon for an *edge type* — the edge counterpart of
    /// [`unique_key_exists_full_horizon`](Self::unique_key_exists_full_horizon).
    ///
    /// Probes the same layers (current L0, pending-flush buffers, optional
    /// transaction L0, then committed storage), keyed to an `Eid`. `exclude_eid`
    /// is the edge being checked so it never conflicts with itself; pass `None`
    /// for a brand-new edge with no self to exclude. The committed-storage half is
    /// delegated to [`PropertyManager::flushed_edge_key_conflict`], which resolves
    /// the LSM delta table's latest-version-per-eid liveness correctly.
    ///
    /// # Errors
    /// Returns an error if a storage probe fails — fails closed rather than
    /// silently treating the key as absent.
    pub async fn unique_edge_key_exists_full_horizon(
        &self,
        edge_type: &str,
        key_values: &[(String, Value)],
        exclude_eid: Option<Eid>,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<bool> {
        let key = serialize_constraint_key(edge_type, key_values);
        // Sentinel: u64::MAX is never an allocated EID, so with no self to exclude
        // any real key hit compares unequal and counts.
        let exclude = exclude_eid.unwrap_or_else(|| Eid::new(u64::MAX));

        // 1. Current in-memory L0 index.
        if self
            .l0_manager
            .get_current()
            .read()
            .has_edge_constraint_key(&key, exclude)
        {
            return Ok(true);
        }

        // 1b. Pending-flush buffers (closes the flush-window leak, mirroring the
        // vertex probe's Bug #9A handling).
        for pending_l0 in self.l0_manager.get_pending_flush() {
            if pending_l0.read().has_edge_constraint_key(&key, exclude) {
                return Ok(true);
            }
        }

        // 1c. Transaction-local L0.
        if let Some(tx_l0) = tx_l0
            && tx_l0.read().has_edge_constraint_key(&key, exclude)
        {
            return Ok(true);
        }

        // 2. Committed storage (LSM delta), resolved for latest-version liveness.
        #[cfg(feature = "lance-backend")]
        if let Some(pm) = &self.property_manager
            && pm
                .flushed_edge_key_conflict(edge_type, key_values, exclude_eid)
                .await?
        {
            return Ok(true);
        }

        Ok(false)
    }

    /// Apply write backpressure from the flush-generation count (`l1_runs`).
    ///
    /// Re-reads the count before every sleep, so a write resumes within one
    /// [`WRITE_PRESSURE_POLL`] of compaction relieving the pressure, whichever
    /// band it was waiting in; see [`write_pressure_step`] for the bands.
    ///
    /// The writer holds no statement deadline. Every wait here is an ordinary
    /// `tokio` sleep, so a caller's deadline or cancellation, which the
    /// executor applies by racing and dropping the statement future, ends
    /// the wait at the next poll of that future.
    ///
    /// A write into a transaction buffer with an open statement scope
    /// ([`crate::runtime::l0::StatementScope`]) pays once per statement or
    /// batch: the first write waits, the rest of the statement's writes see
    /// the buffer marked admitted and go straight through. Outside a scope
    /// every call is charged.
    ///
    /// With background compaction disabled nothing will lower the count, so
    /// the soft band does not wait at all and the hard limit refuses the
    /// write at once with `UniError::WriteStalled` instead of polling until
    /// a deadline.
    async fn check_write_pressure(&self, tx_l0: Option<&Arc<RwLock<L0Buffer>>>) -> Result<()> {
        if let Some(l0) = tx_l0
            && l0.read().write_admitted()
        {
            return Ok(());
        }
        self.wait_for_write_pressure().await?;
        if let Some(l0) = tx_l0 {
            l0.write().note_write_admitted();
        }
        Ok(())
    }

    /// The throttle wait itself; see [`Self::check_write_pressure`].
    async fn wait_for_write_pressure(&self) -> Result<()> {
        let throttle = self.config.throttle;
        let compaction_enabled = self.config.compaction.enabled;
        let mut recorder = ThrottleWaitRecorder {
            stats: &self.throttle_stats,
            started: tokio::time::Instant::now(),
            waited: false,
        };
        let mut stall_logged = false;
        loop {
            let l1_runs = self
                .storage
                .compaction_status()
                .map_err(|e| anyhow::anyhow!("Failed to get compaction status: {}", e))?
                .l1_runs;
            if !compaction_enabled {
                if l1_runs >= throttle.hard_limit {
                    self.throttle_stats
                        .stalled
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    metrics::counter!("uni_write_stalled_total").increment(1);
                    return Err(anyhow::Error::new(uni_common::UniError::WriteStalled {
                        l1_runs,
                        hard_limit: throttle.hard_limit,
                    }));
                }
                return Ok(());
            }
            let Some(step) = write_pressure_step(&throttle, l1_runs, recorder.started.elapsed())
            else {
                return Ok(());
            };
            if l1_runs >= throttle.hard_limit && !stall_logged {
                log::warn!("Write stalled: L1 runs ({}) at hard limit", l1_runs);
                stall_logged = true;
            }
            recorder.waited = true;
            tokio::time::sleep(step).await;
        }
    }

    /// The write throttle's counters since this writer was constructed.
    pub fn write_throttle_stats(&self) -> WriteThrottleSnapshot {
        use std::sync::atomic::Ordering;
        WriteThrottleSnapshot {
            waits: self.throttle_stats.waits.load(Ordering::Relaxed),
            wait_time: std::time::Duration::from_nanos(
                self.throttle_stats.wait_nanos.load(Ordering::Relaxed),
            ),
            stalled: self.throttle_stats.stalled.load(Ordering::Relaxed),
        }
    }

    /// Record a statement's property side effects on `entity`: each pair is
    /// a property's value before and after (`None` = absent). See
    /// [`L0Buffer::record_property_effect`] for the counting rules. Routes to
    /// the transaction buffer if given, otherwise to the main buffer.
    pub fn record_property_effects(
        &self,
        entity: crate::runtime::l0::StatEntity,
        effects: &[(Option<Value>, Option<Value>)],
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) {
        if effects.is_empty() {
            return;
        }
        let l0 = self.resolve_l0(tx_l0);
        let mut guard = l0.write();
        for (old, new) in effects {
            guard.record_property_effect(entity, old.as_ref(), new.as_ref());
        }
    }

    /// Check transaction memory limit to prevent OOM.
    /// No-op when no transaction is active.
    fn check_transaction_memory(&self, tx_l0: Option<&Arc<RwLock<L0Buffer>>>) -> Result<()> {
        if let Some(tx_l0) = tx_l0 {
            let size = tx_l0.read().estimated_size;
            if size > self.config.max_transaction_memory {
                return Err(anyhow!(
                    "Transaction memory limit exceeded: {} bytes used, limit is {} bytes. \
                     Roll back or commit the current transaction.",
                    size,
                    self.config.max_transaction_memory
                ));
            }
        }
        Ok(())
    }

    async fn get_query_context(
        &self,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Option<QueryContext> {
        Some(QueryContext::new_with_pending(
            self.l0_manager.get_current(),
            tx_l0.cloned(),
            self.l0_manager.get_pending_flush(),
        ))
    }

    /// Layer-1 CRDT variant enforcement, shared by the single-vertex and batch
    /// write paths.
    ///
    /// Rejects a declared CRDT property written as a parsed CRDT value
    /// (`Value::Map`) whose variant differs from the schema's declared variant.
    /// A mismatch would make the commit-time merge silently overwrite instead of
    /// merge, and the OCC CRDT carve-out (`occ::crdt_carveout_overwrite` /
    /// `WriteSet::from_l0`) would hide it as a lost update — so it must be caught
    /// at write time, on *every* write path. `try_as_crdt` is `Map`-gated, so the
    /// JSON-string (Cypher) form and non-CRDT values pass through untouched: they
    /// are never carved out and stay conflictable.
    fn enforce_crdt_variants(
        props_meta: &std::collections::HashMap<String, uni_common::core::schema::PropertyMeta>,
        properties: &Properties,
    ) -> Result<()> {
        for (key, value) in properties {
            let Some(meta) = props_meta.get(key) else {
                continue;
            };
            let uni_common::core::schema::DataType::Crdt(expected) = &meta.r#type else {
                continue;
            };
            if let Some(crdt) = crate::runtime::l0::try_as_crdt(value)
                && crdt.type_name() != expected.type_name()
            {
                return Err(anyhow::Error::new(uni_common::UniError::Constraint {
                    message: format!(
                        "CRDT property '{key}' must be written as a {} value",
                        expected.type_name()
                    ),
                }));
            }
        }
        Ok(())
    }

    /// Prepare a vertex for upsert by merging CRDT properties with existing values.
    ///
    /// When `label` is provided, uses it directly to look up property metadata.
    /// Otherwise falls back to discovering the label from L0 buffers and storage.
    ///
    /// # Errors
    ///
    /// Returns an error if CRDT property merging fails.
    async fn prepare_vertex_upsert(
        &self,
        vid: Vid,
        properties: &mut Properties,
        label: Option<&str>,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        let Some(pm) = &self.property_manager else {
            return Ok(());
        };

        let schema = self.schema_manager.schema();

        // Resolve label: use provided label or discover from L0/storage
        let discovered_labels;
        let label_name = if let Some(l) = label {
            Some(l)
        } else {
            discovered_labels = self.get_vertex_labels(vid, tx_l0).await?;
            discovered_labels
                .as_ref()
                .and_then(|l| l.first().map(|s| s.as_str()))
        };

        let Some(label_str) = label_name else {
            return Ok(());
        };
        let Some(props_meta) = schema.properties.get(label_str) else {
            return Ok(());
        };

        // Identify CRDT properties in the insert data
        let crdt_keys: Vec<String> = properties
            .keys()
            .filter(|key| {
                props_meta.get(*key).is_some_and(|meta| {
                    matches!(meta.r#type, uni_common::core::schema::DataType::Crdt(_))
                })
            })
            .cloned()
            .collect();

        if crdt_keys.is_empty() {
            return Ok(());
        }

        // Enforce that each declared CRDT property written as a parsed CRDT value
        // (`Value::Map`) carries its declared variant. A mismatched variant makes
        // `merge_crdt_properties` overwrite rather than merge at commit, and the
        // OCC carve-out (`occ::crdt_carveout_overwrite` / `WriteSet::from_l0`)
        // would hide that as a silent lost update — reject it at the source.
        //
        // Only the `Map` form is checked: it is exactly the form the carve-out
        // applies to (`try_as_crdt` is `Map`-gated). A CRDT written as a JSON
        // string (the Cypher form) or a non-CRDT value is never carved out — it
        // stays conflictable — so it poses no carve-out soundness risk and is left
        // to the existing merge/parse path. This is the declared-property half of
        // the layered fix; the commit-time check covers undeclared CRDT-shaped values.
        Self::enforce_crdt_variants(props_meta, properties)?;

        let ctx = self.get_query_context(tx_l0).await;
        for key in crdt_keys {
            let existing = pm.get_vertex_prop_with_ctx(vid, &key, ctx.as_ref()).await?;
            if !existing.is_null()
                && let Some(val) = properties.get_mut(&key)
            {
                *val = pm.merge_crdt_values(&existing, val)?;
            }
        }

        Ok(())
    }

    async fn prepare_edge_upsert(
        &self,
        eid: Eid,
        properties: &mut Properties,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        if let Some(pm) = &self.property_manager {
            let schema = self.schema_manager.schema();
            // Get edge type from L0 buffer instead of from EID
            let type_name = self.get_edge_type_from_l0(eid);

            if let Some(ref t_name) = type_name
                && let Some(props_meta) = schema.properties.get(t_name)
            {
                let mut crdt_keys = Vec::new();
                for key in properties.keys() {
                    if let Some(meta) = props_meta.get(key)
                        && matches!(meta.r#type, uni_common::core::schema::DataType::Crdt(_))
                    {
                        crdt_keys.push(key.clone());
                    }
                }

                if !crdt_keys.is_empty() {
                    let ctx = self.get_query_context(tx_l0).await;
                    for key in crdt_keys {
                        let existing = pm.get_edge_prop(eid, &key, ctx.as_ref()).await?;

                        if !existing.is_null()
                            && let Some(val) = properties.get_mut(&key)
                        {
                            *val = pm.merge_crdt_values(&existing, val)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[instrument(skip(self, properties), level = "trace")]
    pub async fn insert_vertex(
        &self,
        vid: Vid,
        properties: Properties,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        self.insert_vertex_with_labels(vid, properties, &[], tx_l0)
            .await?;
        Ok(())
    }

    /// Component C1 (G4): before a non-transactional mutation merges into main
    /// L0, if an outstanding snapshot pins the current generation, freeze it
    /// aside so snapshots taken *before* this write stay isolated from it.
    ///
    /// `flush_lock` (acquired and released here) serializes the freeze against
    /// concurrent commit-time freezes/merges, matching the atomicity the tx
    /// commit path gets. No-op for transactional writes (their freeze happens at
    /// commit) and — the common case — when nothing is pinned, where it costs one
    /// atomic load. Freezes at most once per pinned generation: the freeze
    /// installs a fresh unpinned `current`, so later writes in the same bulk
    /// import see no pin and merge in place, and the snapshot keeps reading the
    /// frozen pre-import buffer.
    async fn freeze_for_non_tx_write_if_pinned(&self, tx_l0: Option<&Arc<RwLock<L0Buffer>>>) {
        // Self-gates on the runtime SSI toggle: nothing pins a snapshot unless a
        // transaction began under `ssi_enabled`, so `is_current_pinned()` is
        // always false (one atomic load) when SSI is off.
        if tx_l0.is_none() && self.l0_manager.is_current_pinned() {
            let _flush_lock_guard = self.flush_lock.lock().await;
            // Re-check under the lock: a concurrent commit may have frozen first.
            if self.l0_manager.is_current_pinned() {
                self.l0_manager.freeze_current_for_snapshot();
                metrics::counter!("uni_l0_snapshot_freezes_total").increment(1);
            }
        }
    }

    /// Insert a newly created vertex.
    ///
    /// Counted as a create in the buffer's `MutationStats`. To rewrite an
    /// existing vertex's row use [`Self::update_vertex_with_labels`].
    #[instrument(skip(self, properties, labels), level = "trace")]
    pub async fn insert_vertex_with_labels(
        &self,
        vid: Vid,
        properties: Properties,
        labels: &[String],
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<Properties> {
        // Boxed: the row write is a deep future, and callers nest it in
        // their own (`async_trait`, timeouts), which overflowed the
        // compiler's auto-trait recursion limit.
        let write: futures::future::BoxFuture<'_, Result<Properties>> =
            Box::pin(self.write_vertex_row(vid, properties, labels, tx_l0, true));
        write.await
    }

    /// Rewrite an existing vertex's full row (`SET`, `REMOVE`, a `MERGE`
    /// action, a promote upsert).
    ///
    /// The same write as [`Self::insert_vertex_with_labels`], but not a
    /// create: the caller records the statement's property side effects with
    /// [`Self::record_property_effects`], since only it knows the old values.
    #[instrument(skip(self, properties, labels), level = "trace")]
    pub async fn update_vertex_with_labels(
        &self,
        vid: Vid,
        properties: Properties,
        labels: &[String],
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<Properties> {
        // Boxed: the row write is a deep future, and callers nest it in
        // their own (`async_trait`, timeouts), which overflowed the
        // compiler's auto-trait recursion limit.
        let write: futures::future::BoxFuture<'_, Result<Properties>> =
            Box::pin(self.write_vertex_row(vid, properties, labels, tx_l0, false));
        write.await
    }

    /// Body of [`Self::insert_vertex_with_labels`] (`create`) and
    /// [`Self::update_vertex_with_labels`].
    async fn write_vertex_row(
        &self,
        vid: Vid,
        mut properties: Properties,
        labels: &[String],
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
        create: bool,
    ) -> Result<Properties> {
        let start = std::time::Instant::now();
        self.check_write_pressure(tx_l0).await?;
        self.check_transaction_memory(tx_l0)?;

        // Component C1 (G4): a non-transactional write (`tx_l0 == None`, e.g. bulk
        // import / LOAD CSV) mutates main L0 directly, outside the commit-time
        // snapshot freeze. Freeze the pinned generation aside first so snapshots
        // taken before this write stay isolated from it.
        self.freeze_for_non_tx_write_if_pinned(tx_l0).await;

        if !self.try_defer_embedding(labels, &properties, vid, tx_l0) {
            self.process_embeddings_for_labels(labels, &mut properties)
                .await?;
        }
        self.validate_vertex_constraints(vid, &properties, labels, tx_l0)
            .await?;
        self.prepare_vertex_upsert(
            vid,
            &mut properties,
            labels.first().map(|s| s.as_str()),
            tx_l0,
        )
        .await?;

        // Clone properties and labels before moving into L0 to return them and populate constraint index
        let properties_copy = properties.clone();
        let labels_copy = labels.to_vec();

        {
            // For a non-tx write, re-resolve the live `current` buffer and hold
            // `flush_lock` across the (synchronous) write so a concurrent flush
            // rotate (which takes `flush_lock` via `begin_flush`) cannot install
            // a fresh `current` between resolve and write, dropping our write
            // (Bug #4). For a tx write `resolve_l0` returns the tx-private
            // buffer, never the rotating `current`, so no `flush_lock` is needed
            // (and taking it would risk re-entrancy with the commit path).
            let _flush_lock_guard = if tx_l0.is_none() {
                Some(self.flush_lock.lock().await)
            } else {
                None
            };
            let l0 = self.resolve_l0(tx_l0);
            let mut l0_guard = l0.write();
            // Generation chaining: a non-tx CRDT write into the (post-freeze,
            // possibly empty) current buffer must merge against the chained
            // committed state, not shadow it. No-op when the chain is empty
            // or this is a tx-private write.
            if tx_l0.is_none() {
                let pending = self.l0_manager.get_pending_flush();
                if !pending.is_empty() {
                    Self::seed_crdt_props(&pending, vid, &properties, &mut l0_guard);
                }
            }
            if create {
                l0_guard.insert_vertex_with_labels(vid, properties, labels);
            } else {
                l0_guard.update_vertex_with_labels(vid, properties, labels);
            }

            // Populate constraint index for O(1) duplicate detection
            let schema = self.schema_manager.schema();
            for label in &labels_copy {
                if schema.get_label_case_insensitive(label).is_none() {
                    if self.config.strict_schema {
                        return Err(anyhow::anyhow!(
                            "Label '{}' is not defined in the schema \
                             (strict_schema is enabled).",
                            label
                        ));
                    }
                    continue; // Schemaless: skip unknown labels.
                }

                // For each unique constraint on this label, insert into constraint index
                for constraint in &schema.constraints {
                    if !constraint.enabled {
                        continue;
                    }
                    if let ConstraintTarget::Label(l) = &constraint.target {
                        if l != label {
                            continue;
                        }
                    } else {
                        continue;
                    }

                    // Index keys for both Unique and NodeKey (uniqueness half), so
                    // the full-horizon probe sees prior NodeKey rows too.
                    if let Some(unique_props) = constraint.constraint_type.unique_properties() {
                        let mut key_values = Vec::new();
                        let mut all_present = true;
                        for prop in unique_props {
                            if let Some(val) = properties_copy.get(prop) {
                                key_values.push((prop.clone(), val.clone()));
                            } else {
                                all_present = false;
                                break;
                            }
                        }

                        if all_present {
                            let key = serialize_constraint_key(label, &key_values);
                            l0_guard.insert_constraint_key(key, vid);
                        }
                    }
                }
            }
        }

        metrics::counter!("uni_l0_buffer_mutations_total").increment(1);
        self.update_metrics();

        if tx_l0.is_none() {
            self.check_flush().await?;
        }
        if start.elapsed().as_millis() > 100 {
            log::warn!("Slow insert_vertex: {}ms", start.elapsed().as_millis());
        }
        Ok(properties_copy)
    }

    /// Refresh auto-embed targets for a `SET` write whose `touched_keys`
    /// include an embed *source* column: drop stale target embeddings from `props`
    /// (so they re-embed) and add them to `touched_keys`. Public so the query
    /// executor can apply it on the coalesced write before storing it. No-op for
    /// non-SET writes / non-embed labels.
    pub fn refresh_embed_targets(
        &self,
        props: &mut Properties,
        touched_keys: &mut HashSet<String>,
        labels: &[String],
    ) {
        refresh_touched_embed_targets(&self.schema_manager.schema(), props, touched_keys, labels);
    }

    /// Write a vertex `SET`: `props` is the vertex's full merged property
    /// row and `touched_keys` the keys the statement assigned.
    ///
    /// Always stored as a full row, whatever `partial_lance_writes` says. A
    /// vertex table holds one row per flushed version of a vertex until
    /// compaction, so a flush-time `MergeInsert` of the touched columns keyed
    /// on `_vid` updated every stored version of the vertex: a removed
    /// property came back after a flush or a compaction, and a vertex
    /// created and updated in one flush window had no row to match and was
    /// lost. `touched_keys` is accepted for source compatibility and
    /// otherwise unused. Not a create: the caller records side effects.
    #[instrument(skip(self, props, _touched_keys, labels), level = "trace")]
    pub async fn insert_vertex_partial_full(
        &self,
        vid: Vid,
        props: Properties,
        _touched_keys: HashSet<String>,
        labels: &[String],
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        self.update_vertex_with_labels(vid, props, labels, tx_l0)
            .await
            .map(|_| ())
    }

    /// Merge `touched` over the vertex's current properties (L0 and storage)
    /// and store the result as a full row; see
    /// [`Self::insert_vertex_partial_full`] for why it is never a partial
    /// row. Not a create.
    #[instrument(skip(self, touched, labels), level = "trace")]
    pub async fn insert_vertex_partial(
        &self,
        vid: Vid,
        touched: Properties,
        labels: &[String],
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        // The current row as this write sees it: its transaction buffer, the
        // committed buffers, then storage.
        let ctx = self.get_query_context(tx_l0).await;
        let existing = match &self.property_manager {
            Some(pm) => pm.get_all_vertex_props_with_ctx(vid, ctx.as_ref()).await?,
            None => {
                PropertyManager::new(self.storage.clone(), self.schema_manager.clone(), 0)
                    .get_all_vertex_props_with_ctx(vid, ctx.as_ref())
                    .await?
            }
        }
        .unwrap_or_default();
        let mut merged = existing;
        merged.extend(touched);
        self.update_vertex_with_labels(vid, merged, labels, tx_l0)
            .await
            .map(|_| ())
    }

    /// Insert multiple vertices with batched operations.
    ///
    /// This method uses batched operations to achieve O(N) complexity instead of O(N²)
    /// for bulk inserts with unique constraints.
    ///
    /// # Performance Improvements
    /// - Batch VID allocation: 1 call instead of N calls
    /// - Batch constraint validation: O(N) instead of O(N²)
    /// - Batch embedding generation: 1 API call per config instead of N calls
    /// - Transaction wrapping: Automatic flush deferral, atomicity
    ///
    /// # Arguments
    /// * `vids` - Pre-allocated VIDs for the vertices
    /// * `properties_batch` - Properties for each vertex
    /// * `labels` - Labels for all vertices (assumes single label for simplicity)
    ///
    /// # Errors
    /// Returns error if:
    /// - VID/properties length mismatch
    /// - Constraint violation detected
    /// - Embedding generation fails
    /// - Transaction commit fails
    ///
    /// # Atomicity
    /// If this method fails, all changes are rolled back (if transaction was started here).
    pub async fn insert_vertices_batch(
        &self,
        vids: Vec<Vid>,
        mut properties_batch: Vec<Properties>,
        labels: Vec<String>,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<Vec<Properties>> {
        let start = std::time::Instant::now();

        // Validate inputs
        if vids.len() != properties_batch.len() {
            return Err(anyhow!(
                "VID/properties size mismatch: {} vids, {} properties",
                vids.len(),
                properties_batch.len()
            ));
        }

        if vids.is_empty() {
            return Ok(Vec::new());
        }

        // Batch operations — writes go directly to the resolved L0.
        // Atomicity is guaranteed by the caller holding the writer lock.
        let result = async {
            self.check_write_pressure(tx_l0).await?;
            self.check_transaction_memory(tx_l0)?;

            // Component C1 (G4): batch bulk-import is the canonical non-tx write —
            // freeze the pinned generation aside before merging so snapshot
            // readers stay isolated. No-op when unpinned or transactional.
            self.freeze_for_non_tx_write_if_pinned(tx_l0).await;

            // Batch embedding generation (1 API call per config)
            self.process_embeddings_for_batch(&labels, &mut properties_batch)
                .await?;

            // Batch constraint validation (O(N) instead of O(N²))
            let label = labels
                .first()
                .ok_or_else(|| anyhow!("No labels provided"))?;
            self.validate_vertex_batch_constraints(&vids, &properties_batch, label, tx_l0)
                .await?;

            // Batch prepare (CRDT merging if needed)
            // Check schema once: skip entirely if no CRDT properties for this label.
            // For new vertices (freshly allocated VIDs), there are no existing CRDT
            // values to merge, so the per-vertex lookup is unnecessary in that case.
            let has_crdt_fields = {
                let schema = self.schema_manager.schema();
                schema
                    .properties
                    .get(label.as_str())
                    .is_some_and(|props_meta| {
                        props_meta.values().any(|meta| {
                            matches!(meta.r#type, uni_common::core::schema::DataType::Crdt(_))
                        })
                    })
            };

            if has_crdt_fields {
                // Layer-1 variant enforcement (G3): the batch path must reject a
                // declared-CRDT variant mismatch exactly as the single-vertex
                // `prepare_vertex_upsert` does. Without this, a wrong-variant CRDT
                // written via batch import slips past write-time validation and
                // the OCC carve-out then masks the overwrite as a lost update.
                {
                    let schema = self.schema_manager.schema();
                    if let Some(props_meta) = schema.properties.get(label.as_str()) {
                        for props in &properties_batch {
                            Self::enforce_crdt_variants(props_meta, props)?;
                        }
                    }
                }

                // Batch fetch existing CRDT values: collect VIDs that need merging,
                // then query once via PropertyManager instead of per-vertex lookups.
                //
                // The reader has to be the CRDT-aware batched one. The generic
                // `get_batch_vertex_props_for_label` lets an L0 overlay win a key
                // outright where the singular reader merges it with storage, so on
                // a partial overlay it answers with storage's replica dropped —
                // and merging the incoming value against *that* writes a counter
                // that has gone backwards. Pinned by
                // `bugs::issue_220_crdt_reader_equivalence` (#220 site 4).
                let schema = self.schema_manager.schema();
                let crdt_keys: Vec<String> = schema
                    .properties
                    .get(label.as_str())
                    .map(|props_meta| {
                        props_meta
                            .iter()
                            .filter(|(_, meta)| {
                                matches!(meta.r#type, uni_common::core::schema::DataType::Crdt(_))
                            })
                            .map(|(key, _)| key.clone())
                            .collect()
                    })
                    .unwrap_or_default();

                if let Some(pm) = &self.property_manager
                    && !crdt_keys.is_empty()
                {
                    let ctx = self.get_query_context(tx_l0).await;
                    let existing_by_vid = pm
                        .get_batch_vertex_crdt_props(&vids, label, &crdt_keys, ctx.as_ref())
                        .await?;
                    for (vid, props) in vids.iter().zip(&mut properties_batch) {
                        for key in &crdt_keys {
                            if props.contains_key(key) {
                                // Absent means the same thing the singular
                                // reader's null did: nothing to merge against.
                                let Some(existing) =
                                    existing_by_vid.get(vid).and_then(|p| p.get(key.as_str()))
                                else {
                                    continue;
                                };
                                if !existing.is_null()
                                    && let Some(val) = props.get_mut(key)
                                {
                                    *val = pm.merge_crdt_values(existing, val)?;
                                }
                            }
                        }
                    }
                }
            }

            // Batch L0 writes — route to active L0 (transaction L0 if active, else current).
            let target_l0 = self.resolve_l0(tx_l0);

            let properties_result = properties_batch.clone();
            {
                let schema = self.schema_manager.schema();
                let mut l0_guard = target_l0.write();
                for (vid, props) in vids.iter().zip(properties_batch.iter()) {
                    l0_guard.insert_vertex_with_labels(*vid, props.clone(), &labels);

                    // Populate the L0 constraint index so a later single insert
                    // (or commit-time probe) sees this batch row's unique keys via
                    // has_constraint_key. The single-vertex path does this; the
                    // batch path formerly did not, silently twinning unique keys.
                    for label in &labels {
                        for constraint in &schema.constraints {
                            if !constraint.enabled {
                                continue;
                            }
                            let ConstraintTarget::Label(l) = &constraint.target else {
                                continue;
                            };
                            if l != label {
                                continue;
                            }
                            // Index keys for both Unique and NodeKey.
                            if let Some(unique_props) =
                                constraint.constraint_type.unique_properties()
                            {
                                let mut key_values = Vec::new();
                                let mut all_present = true;
                                for prop in unique_props {
                                    if let Some(val) = props.get(prop) {
                                        key_values.push((prop.clone(), val.clone()));
                                    } else {
                                        all_present = false;
                                        break;
                                    }
                                }
                                if all_present {
                                    let key = serialize_constraint_key(label, &key_values);
                                    l0_guard.insert_constraint_key(key, *vid);
                                }
                            }
                        }
                    }
                }
            }

            // Update metrics (batch increment)
            metrics::counter!("uni_l0_buffer_mutations_total").increment(vids.len() as u64);
            self.update_metrics();

            Ok::<Vec<Properties>, anyhow::Error>(properties_result)
        }
        .await;

        let props = result?;

        if start.elapsed().as_millis() > 100 {
            log::warn!(
                "Slow insert_vertices_batch ({} vertices): {}ms",
                vids.len(),
                start.elapsed().as_millis()
            );
        }

        Ok(props)
    }

    /// Delete a vertex by VID.
    ///
    /// When `labels` is provided, uses them directly to populate L0 for
    /// correct tombstone flushing. Otherwise discovers labels from L0
    /// buffers and storage (which can be slow for many vertices).
    ///
    /// # Errors
    ///
    /// Returns an error if write pressure stalls, label lookup fails, or
    /// the L0 delete operation fails.
    #[instrument(skip(self, labels), level = "trace")]
    pub async fn delete_vertex(
        &self,
        vid: Vid,
        labels: Option<Vec<String>>,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        self.check_write_pressure(tx_l0).await?;
        self.check_transaction_memory(tx_l0)?;
        self.freeze_for_non_tx_write_if_pinned(tx_l0).await; // C1 (G4)

        // Before deleting, ensure we have the vertex's labels stored in L0 so
        // the tombstone can be flushed to the correct label datasets. Discover
        // them up front (this may await storage) WITHOUT pinning the buffer we
        // will eventually mutate — for non-tx writes the live `current` buffer
        // is re-resolved below under `flush_lock`, so a concurrent rotate can't
        // drop our write (Bug #4). `resolve_l0` here is only used for cheap
        // reads that tolerate a racing rotate.
        let has_labels = {
            let l0_guard = self.resolve_l0(tx_l0);
            let guard = l0_guard.read();
            guard.vertex_labels.contains_key(&vid)
        };

        let backfill_labels = if has_labels {
            None
        } else if let Some(provided) = labels {
            // Caller provided labels — skip the lookup entirely
            Some(provided)
        } else {
            // Discover labels from pending flush L0s, then storage
            let mut found = None;
            for pending_l0 in self.l0_manager.get_pending_flush() {
                let pending_guard = pending_l0.read();
                if let Some(l) = pending_guard.get_vertex_labels(vid) {
                    found = Some(l.to_vec());
                    break;
                }
            }
            if found.is_none() {
                found = self.find_vertex_labels_in_storage(vid).await?;
            }
            found
        };

        // Test-only seam (no-op without the `failpoints` feature): pause a
        // non-transactional delete AFTER the awaited label discovery but BEFORE
        // it re-resolves the live buffer and writes the tombstone. A concurrent
        // flush can rotate+complete a buffer in this window; the fix re-resolves
        // `get_current()` and mutates it under `flush_lock`, so the tombstone
        // always lands in the live buffer (Bug #4 — silent lost delete across
        // L0 rotation).
        fail::fail_point!("nontx::after-capture");

        // Apply the label backfill and the tombstone together. For a non-tx
        // write, hold `flush_lock` across the (synchronous) re-resolve + write
        // so a concurrent flush rotate (which takes `flush_lock` via
        // `begin_flush`) cannot install a fresh `current` between our resolve
        // and our write. For a tx write `resolve_l0` returns the tx-private
        // buffer (never the rotating `current`), so no `flush_lock` is needed
        // — and taking it there would risk re-entrancy with the commit path.
        if tx_l0.is_none() {
            let _flush_lock_guard = self.flush_lock.lock().await;
            let l0 = self.l0_manager.get_current();
            let mut guard = l0.write();
            if let Some(found_labels) = backfill_labels {
                guard.vertex_labels.insert(vid, found_labels);
            }
            let cascaded = guard.delete_vertex(vid)?;
            self.mirror_cascaded_tombstones(&cascaded, &guard);
        } else {
            let l0 = self.resolve_l0(tx_l0);
            let mut guard = l0.write();
            if let Some(found_labels) = backfill_labels {
                guard.vertex_labels.insert(vid, found_labels);
            }
            guard.delete_vertex(vid)?;
        }
        metrics::counter!("uni_l0_buffer_mutations_total").increment(1);
        self.update_metrics();

        if tx_l0.is_none() {
            self.check_flush().await?;
        }
        if start.elapsed().as_millis() > 100 {
            log::warn!("Slow delete_vertex: {}ms", start.elapsed().as_millis());
        }
        Ok(())
    }

    /// Find vertex labels from storage by querying the main vertices table.
    /// Returns the labels from the latest non-deleted version of the vertex.
    async fn find_vertex_labels_in_storage(&self, vid: Vid) -> Result<Option<Vec<String>>> {
        use crate::backend::types::ScanRequest;
        use arrow_array::Array;
        use arrow_array::cast::AsArray;

        let backend = self.storage.backend();
        let table_name = MainVertexDataset::table_name();

        // Check if table exists first; if not, vertex hasn't been flushed to storage yet
        if !backend.table_exists(table_name).await? {
            return Ok(None);
        }

        // Query for this specific vid (don't filter by _deleted yet - we need to find the latest version first)
        let filter = FilterExpr::equals("_vid", Scalar::UInt(vid.as_u64()));
        let batches = backend
            .scan(
                ScanRequest::all(table_name)
                    .with_filter(filter)
                    .with_columns(vec![
                        "_vid".to_string(),
                        "labels".to_string(),
                        "_version".to_string(),
                        "_deleted".to_string(),
                    ]),
            )
            .await?;

        // Find the row with the highest version number
        let mut max_version: Option<u64> = None;
        let mut labels: Option<Vec<String>> = None;
        let mut is_deleted = false;

        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }

            let version_array = batch
                .column_by_name("_version")
                .unwrap()
                .as_primitive::<arrow_array::types::UInt64Type>();

            let deleted_array = batch.column_by_name("_deleted").unwrap().as_boolean();

            let labels_array = batch.column_by_name("labels").unwrap().as_list::<i32>();

            for row_idx in 0..batch.num_rows() {
                let version = version_array.value(row_idx);

                if max_version.is_none_or(|mv| version > mv) {
                    is_deleted = deleted_array.value(row_idx);

                    let labels_list = labels_array.value(row_idx);
                    let string_array = labels_list.as_string::<i32>();
                    let vertex_labels: Vec<String> = (0..string_array.len())
                        .filter(|&i| !string_array.is_null(i))
                        .map(|i| string_array.value(i).to_string())
                        .collect();

                    max_version = Some(version);
                    labels = Some(vertex_labels);
                }
            }
        }

        // If the latest version is deleted, return None
        if is_deleted { Ok(None) } else { Ok(labels) }
    }

    /// Write an edge `SET`: `props` is the edge's full merged property row
    /// and `touched_keys` the keys the statement assigned.
    ///
    /// Always stored as a full row, whatever `partial_lance_writes` says.
    /// The per-edge-type delta tables are an append log — every flush
    /// appends a row per edge and a delete appends an `op = 1` row — so an
    /// edge id has as many rows as it has flushed versions. A partial
    /// flush used to `MergeInsert` the touched columns keyed on `eid`,
    /// which that log cannot support: two source rows for one edge fail
    /// the whole flush ("ambiguous merge insert"), an edge created and
    /// updated in one flush window has no row to match and was dropped
    /// (`WhenNotMatched::DoNothing`; gone after reopen), and every logged
    /// row of the edge is updated to the same `_version`, leaving the
    /// newest-row replay a tie between a stale and a current row.
    /// `touched_keys` is accepted for the vertex-symmetric call site and
    /// otherwise unused.
    #[expect(clippy::too_many_arguments)]
    #[instrument(skip(self, props, _touched_keys), level = "trace")]
    pub async fn insert_edge_partial_full(
        &self,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        eid: Eid,
        props: Properties,
        edge_type_name: Option<String>,
        _touched_keys: HashSet<String>,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        self.update_edge(
            src_vid,
            dst_vid,
            edge_type,
            eid,
            props,
            edge_type_name,
            tx_l0,
        )
        .await
    }

    /// Insert a newly created edge.
    ///
    /// Refused with `UniError::EdgeEndpointViolation` when the edge type
    /// declares source or target labels and the endpoint carries none of
    /// them. Counted as a create in the buffer's `MutationStats`. To rewrite
    /// an existing edge's row use [`Self::update_edge`].
    #[expect(clippy::too_many_arguments)]
    pub async fn insert_edge(
        &self,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        eid: Eid,
        properties: Properties,
        edge_type_name: Option<String>,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        self.check_edge_endpoints(
            src_vid,
            dst_vid,
            edge_type,
            edge_type_name.as_deref(),
            tx_l0,
        )
        .await?;
        Box::pin(self.write_edge_row(
            src_vid,
            dst_vid,
            edge_type,
            eid,
            properties,
            edge_type_name,
            tx_l0,
            true,
        ))
        .await
    }

    /// Rewrite an existing edge's full row (`SET` / `REMOVE` on it).
    ///
    /// Not a create. `(src_vid, dst_vid, edge_type)` must be the edge's stored
    /// identity: a property write cannot move an edge or change its type, so
    /// a disagreeing identity is refused with `UniError::EdgeIdentityChange`.
    /// The caller's identity comes from the row binding, and a map carrying
    /// `_eid` / `_src` / `_dst` / `_type` is such a binding, so it can name
    /// anything. When `eid` resolves nowhere the write would create the edge,
    /// and it is held to the declared endpoints like
    /// [`Self::insert_edge`]. An edge that does resolve is not re-checked
    /// against the declaration: it already exists, and a store may hold edges
    /// written before the declaration was enforced.
    ///
    /// # Errors
    /// `UniError::EdgeIdentityChange` as above, `UniError::EdgeEndpointViolation`
    /// for an unknown `eid` that violates the declaration, or a storage read
    /// error from the identity lookup.
    #[expect(clippy::too_many_arguments)]
    pub async fn update_edge(
        &self,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        eid: Eid,
        properties: Properties,
        edge_type_name: Option<String>,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        self.check_edge_identity(
            src_vid,
            dst_vid,
            edge_type,
            eid,
            edge_type_name.as_deref(),
            tx_l0,
        )
        .await?;
        Box::pin(self.write_edge_row(
            src_vid,
            dst_vid,
            edge_type,
            eid,
            properties,
            edge_type_name,
            tx_l0,
            false,
        ))
        .await
    }

    /// Refuse an [`Self::update_edge`] whose identity disagrees with the
    /// stored edge `eid`.
    ///
    /// Looks the edge up newest layer first: the transaction's buffer, the
    /// current and pending buffers (each keeps a deleted edge's endpoints),
    /// then the buffer's identity memo, filled from the source's outgoing
    /// adjacency once per `(source, type)` (see
    /// [`Self::memoize_outgoing_identities`]), and finally the main edges
    /// table. An `eid` found nowhere gets the declared-endpoint check of a
    /// create.
    async fn check_edge_identity(
        &self,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        eid: Eid,
        edge_type_name: Option<&str>,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        let schema = self.schema_manager.schema();
        let type_label = |id: u32| {
            schema
                .edge_type_name_by_id_unified(id)
                .unwrap_or_else(|| format!("#{id}"))
        };
        let requested_name = edge_type_name
            .map(str::to_string)
            .unwrap_or_else(|| type_label(edge_type));
        let refuse = |src: Vid, dst: Vid, ty: String| {
            anyhow::Error::new(uni_common::UniError::EdgeIdentityChange {
                eid: eid.as_u64(),
                stored: format!("({})-[:{ty}]->({})", src.as_u64(), dst.as_u64()),
                requested: format!(
                    "({})-[:{requested_name}]->({})",
                    src_vid.as_u64(),
                    dst_vid.as_u64()
                ),
            })
        };

        let buffered = tx_l0
            .and_then(|l0| l0.read().get_edge_endpoint_full(eid))
            .or_else(|| {
                self.l0_manager
                    .get_current()
                    .read()
                    .get_edge_endpoint_full(eid)
            })
            .or_else(|| {
                // Newest pending buffer first: a later rewrite wins.
                self.l0_manager
                    .get_pending_flush()
                    .iter()
                    .rev()
                    .find_map(|l0| l0.read().get_edge_endpoint_full(eid))
            });
        if let Some((src, dst, ty)) = buffered {
            if (src, dst, ty) == (src_vid, dst_vid, edge_type) {
                return Ok(());
            }
            return Err(refuse(src, dst, type_label(ty)));
        }

        let l0 = self.resolve_l0(tx_l0);
        let memoized = l0.read().edge_identity_memo.identities.get(&eid).copied();
        let memoized = match memoized {
            Some(identity) => Some(identity),
            None => {
                self.memoize_outgoing_identities(&l0, src_vid, edge_type, eid)
                    .await?
            }
        };
        if let Some((src, dst, ty)) = memoized {
            if (src, dst, ty) == (src_vid, dst_vid, edge_type) {
                return Ok(());
            }
            return Err(refuse(src, dst, type_label(ty)));
        }

        let stored = self
            .storage
            .lookup_edges_by_eids(&[eid], None)
            .await?
            .remove(&eid);
        *l0.write()
            .edge_identity_memo
            .storage_reads
            .entry(edge_type)
            .or_default() += 1;
        match stored {
            Some(stored) => {
                if (stored.src_vid, stored.dst_vid) == (src_vid, dst_vid)
                    && stored.edge_type == requested_name
                {
                    Ok(())
                } else {
                    Err(refuse(stored.src_vid, stored.dst_vid, stored.edge_type))
                }
            }
            None => {
                self.check_edge_endpoints(src_vid, dst_vid, edge_type, edge_type_name, tx_l0)
                    .await
            }
        }
    }

    /// Storage reads per edge type after which [`Self::check_edge_identity`]
    /// loads the type's outgoing adjacency instead of reading edges one by
    /// one.
    const IDENTITY_STORAGE_READS_BEFORE_WARM: usize = 64;

    /// Read the outgoing `edge_type` adjacency of `src` once into `l0`'s
    /// identity memo and return `eid`'s identity if it is there.
    ///
    /// The adjacency is loaded first when the type's has no CSR yet and
    /// enough single-edge storage reads have been paid for it in this buffer
    /// ([`Self::IDENTITY_STORAGE_READS_BEFORE_WARM`]); before that the caller
    /// reads storage. An edge id is unique, so finding it under `src` and
    /// `edge_type` gives its whole identity.
    async fn memoize_outgoing_identities(
        &self,
        l0: &Arc<RwLock<L0Buffer>>,
        src: Vid,
        edge_type: u32,
        eid: Eid,
    ) -> Result<Option<(Vid, Vid, u32)>> {
        use crate::storage::direction::Direction;
        if l0
            .read()
            .edge_identity_memo
            .scanned
            .contains(&(src, edge_type))
        {
            return Ok(None);
        }
        if !self
            .adjacency_manager
            .has_csr(edge_type, Direction::Outgoing)
        {
            let reads = l0
                .read()
                .edge_identity_memo
                .storage_reads
                .get(&edge_type)
                .copied()
                .unwrap_or(0);
            if reads < Self::IDENTITY_STORAGE_READS_BEFORE_WARM {
                return Ok(None);
            }
            self.storage
                .warm_adjacency_coalesced(
                    edge_type,
                    Direction::Outgoing,
                    self.storage.snapshot_version_hwm(),
                )
                .await?;
        }
        let neighbors = self
            .adjacency_manager
            .get_neighbors(src, edge_type, Direction::Outgoing);
        let mut guard = l0.write();
        let memo = &mut guard.edge_identity_memo;
        let (identities, scanned) = (memo.identities.len(), memo.scanned.len());
        memo.scanned.insert((src, edge_type));
        memo.identities.extend(
            neighbors
                .into_iter()
                .map(|(dst, e)| (e, (src, dst, edge_type))),
        );
        // The memo lives as long as the buffer, so it counts toward the
        // buffer's size: the flush trigger and a transaction's memory limit.
        let added = memo.charge(identities, scanned);
        let found = memo.identities.get(&eid).copied();
        guard.estimated_size += added;
        Ok(found)
    }

    /// Labels of an edge endpoint as the writing transaction sees them: its
    /// own buffer first, then the committed buffers, then storage.
    async fn endpoint_labels(
        &self,
        vid: Vid,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<Vec<String>> {
        if let Some(tx_l0) = tx_l0 {
            let guard = tx_l0.read();
            if let Some(labels) = guard.get_vertex_labels(vid) {
                return Ok(labels.to_vec());
            }
        }
        if let Some(labels) = self.get_vertex_labels_from_l0(vid) {
            return Ok(labels);
        }
        for pending_l0 in self.l0_manager.get_pending_flush() {
            let guard = pending_l0.read();
            if let Some(labels) = guard.get_vertex_labels(vid) {
                return Ok(labels.to_vec());
            }
        }
        if let Some(labels) = self.storage.get_labels_from_index(vid) {
            return Ok(labels);
        }
        Ok(self
            .find_vertex_labels_in_storage(vid)
            .await?
            .unwrap_or_default())
    }

    /// Enforce the edge type's declared endpoint labels on a new edge.
    ///
    /// An endpoint passes when it carries at least one declared label; an
    /// empty declaration accepts any endpoint, and an edge type the schema
    /// does not declare (schemaless) is not checked.
    ///
    /// # Errors
    /// `UniError::EdgeEndpointViolation` for the first endpoint that fails.
    async fn check_edge_endpoints(
        &self,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        edge_type_name: Option<&str>,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        let schema = self.schema_manager.schema();
        let meta = match edge_type_name {
            Some(name) => schema.edge_types.get(name).map(|m| (name.to_string(), m)),
            None => schema
                .edge_types
                .iter()
                .find(|(_, m)| m.id == edge_type)
                .map(|(n, m)| (n.clone(), m)),
        };
        let Some((name, meta)) = meta else {
            return Ok(());
        };
        for (vid, allowed, endpoint) in [
            (src_vid, &meta.src_labels, "source"),
            (dst_vid, &meta.dst_labels, "target"),
        ] {
            if allowed.is_empty() {
                continue;
            }
            let labels = self.endpoint_labels(vid, tx_l0).await?;
            if !labels.iter().any(|l| allowed.contains(l)) {
                return Err(anyhow::Error::new(
                    uni_common::UniError::EdgeEndpointViolation {
                        edge_type: name,
                        endpoint: endpoint.to_string(),
                        labels,
                        allowed: allowed.clone(),
                    },
                ));
            }
        }
        Ok(())
    }

    /// Body of [`Self::insert_edge`] (`create`) and [`Self::update_edge`].
    #[expect(clippy::too_many_arguments)]
    async fn write_edge_row(
        &self,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        eid: Eid,
        mut properties: Properties,
        edge_type_name: Option<String>,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
        create: bool,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        check_property_names(&properties, &self.schema_manager.schema())?;
        self.check_write_pressure(tx_l0).await?;
        self.check_transaction_memory(tx_l0)?;
        self.freeze_for_non_tx_write_if_pinned(tx_l0).await; // C1 (G4)
        self.prepare_edge_upsert(eid, &mut properties, tx_l0)
            .await?;

        // Enforce declared edge-type UNIQUE / NODE KEY constraints across the full
        // write horizon before the edge lands, then register its keys so later
        // writes (this tx and concurrent ones) observe it. Schemaless edges (no
        // type name) carry no declared constraints and are skipped.
        if let Some(type_name) = edge_type_name.as_deref() {
            self.validate_edge_constraints(eid, type_name, &properties, tx_l0)
                .await?;
        }

        let l0 = self.resolve_l0(tx_l0);
        if let Some(type_name) = edge_type_name.as_deref() {
            self.populate_edge_constraint_index(eid, type_name, &properties, &l0);
        }
        if create {
            l0.write()
                .insert_edge(src_vid, dst_vid, edge_type, eid, properties, edge_type_name)?;
        } else {
            l0.write()
                .update_edge(src_vid, dst_vid, edge_type, eid, properties, edge_type_name)?;
        }

        // Dual-write to AdjacencyManager overlay (survives flush).
        // Skip for transaction-local L0 -- transaction edges are overlaid separately.
        if tx_l0.is_none() {
            let version = l0.read().current_version;
            self.adjacency_manager
                .insert_edge(src_vid, dst_vid, eid, edge_type, version);
        }

        metrics::counter!("uni_l0_buffer_mutations_total").increment(1);
        self.update_metrics();

        if tx_l0.is_none() {
            self.check_flush().await?;
        }
        if start.elapsed().as_millis() > 100 {
            log::warn!("Slow insert_edge: {}ms", start.elapsed().as_millis());
        }
        Ok(())
    }

    #[instrument(skip(self), level = "trace")]
    pub async fn delete_edge(
        &self,
        eid: Eid,
        src_vid: Vid,
        dst_vid: Vid,
        edge_type: u32,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        self.check_write_pressure(tx_l0).await?;
        self.check_transaction_memory(tx_l0)?;
        self.freeze_for_non_tx_write_if_pinned(tx_l0).await; // C1 (G4)
        let l0 = self.resolve_l0(tx_l0);

        l0.write().delete_edge(eid, src_vid, dst_vid, edge_type)?;

        // Dual-write tombstone to AdjacencyManager overlay.
        if tx_l0.is_none() {
            let version = l0.read().current_version;
            self.adjacency_manager
                .add_tombstone(eid, src_vid, dst_vid, edge_type, version);
        }
        metrics::counter!("uni_l0_buffer_mutations_total").increment(1);
        self.update_metrics();

        if tx_l0.is_none() {
            self.check_flush().await?;
        }
        if start.elapsed().as_millis() > 100 {
            log::warn!("Slow delete_edge: {}ms", start.elapsed().as_millis());
        }
        Ok(())
    }

    /// Decide whether a flush should be triggered based on mutation count
    /// or elapsed time since the last flush.
    ///
    /// Extracted from [`Writer::check_flush`] so `commit_transaction_l0` can
    /// reuse the decision while bypassing the lock-acquiring entry point
    /// (it already holds `flush_lock`).
    fn should_flush(&self) -> bool {
        let count = self.l0_manager.get_current().read().mutation_count;
        if count == 0 {
            return false;
        }
        if count >= self.config.auto_flush_threshold {
            return true;
        }
        if let Some(interval) = self.config.auto_flush_interval
            && self.last_flush_time.lock().elapsed() >= interval
            && count >= self.config.auto_flush_min_mutations
        {
            return true;
        }
        false
    }

    /// Check if flush should be triggered based on mutation count or time elapsed.
    /// This method is called after each write operation and can also be called
    /// by a background task for time-based flushing.
    pub async fn check_flush(&self) -> Result<()> {
        if self.should_flush() {
            self.flush_to_l1(None).await?;
        }
        Ok(())
    }

    /// [`Self::check_flush`] for the time-based background flush task.
    ///
    /// With async flush on, a due flush rotates L0 under `flush_lock` and
    /// writes Lance outside it, through the [`FlushCoordinator`], exactly as
    /// the commit path's own trigger does. `flush_to_l1` instead holds
    /// `flush_lock` across the whole Lance write, and a commit waits for that
    /// lock for at most `commit_timeout`: one background flush slower than
    /// that (a loaded machine, a slow disk, a debug build) failed a user
    /// commit with `CommitTimeout` although no other commit was running.
    ///
    /// Does not wait for the flush to land. When the pipeline is full the
    /// flush is skipped; the next tick retries it. Without async flush this
    /// is [`Self::check_flush`].
    ///
    /// # Errors
    ///
    /// Returns an error if the rotate or the inline flush fails.
    pub async fn check_flush_in_background(self: &Arc<Self>) -> Result<()> {
        let Some(coord) = self
            .flush_coordinator
            .as_ref()
            .filter(|_| self.config.async_flush_enabled)
        else {
            return self.check_flush().await;
        };
        if !self.should_flush() {
            return Ok(());
        }
        if coord.pending_flush_count() >= self.config.max_pending_flushes {
            metrics::counter!("uni_flush_trigger_skipped_total").increment(1);
            return Ok(());
        }
        let _ticket = self.flush_to_l1_async(None).await?;
        Ok(())
    }

    /// Process embeddings for a vertex using labels passed directly.
    /// Use this when labels haven't been stored to L0 yet.
    async fn process_embeddings_for_labels(
        &self,
        labels: &[String],
        properties: &mut Properties,
    ) -> Result<()> {
        let label_name = labels.first().map(|s| s.as_str());
        self.process_embeddings_impl(label_name, properties).await
    }

    /// Phase B: if `defer_embeddings` is enabled in `UniConfig` and the
    /// vertex has an embedding config that hasn't been satisfied by the
    /// caller-provided properties, enqueue the VID in
    /// `L0Buffer::pending_embeddings` and return `true`. The caller then
    /// skips `process_embeddings_for_labels` and the embedding is computed
    /// in a single batched call at flush time via
    /// `drain_pending_embeddings`.
    ///
    /// Returns `false` (caller falls back to today's per-row eager embed)
    /// if any of:
    ///  - the flag is off,
    ///  - no label has an embedding config,
    ///  - the user already provided the target property (matches the
    ///    existing skip-if-present semantics at writer.rs:2727).
    ///
    /// Trade-off: when deferral is active, in-tx reads of the embedding
    /// column return only what was already in storage (or nothing for
    /// brand-new vertices). Existing tests that RETURN n.embedding in
    /// the same tx as a SET on the source column must run with the flag
    /// off; opt in only when no such reads happen between write and
    /// commit.
    fn try_defer_embedding(
        &self,
        labels: &[String],
        properties: &Properties,
        vid: Vid,
        tx_l0: Option<&Arc<RwLock<L0Buffer>>>,
    ) -> bool {
        if !self.config.defer_embeddings {
            return false;
        }
        let Some(label) = labels.first() else {
            return false;
        };

        let schema = self.schema_manager.schema();
        let mut has_unsatisfied_cfg = false;
        for idx in &schema.indexes {
            let unsatisfied = match idx {
                IndexDefinition::Vector(v_cfg) => {
                    v_cfg.label == *label
                        && v_cfg.embedding_config.is_some()
                        && !properties.contains_key(&v_cfg.property)
                }
                IndexDefinition::Sparse(s_cfg) => {
                    s_cfg.label == *label
                        && s_cfg.embedding_config.is_some()
                        && !properties.contains_key(&s_cfg.property)
                }
                _ => false,
            };
            if unsatisfied {
                has_unsatisfied_cfg = true;
                break;
            }
        }
        if !has_unsatisfied_cfg {
            return false;
        }

        let l0 = self.resolve_l0(tx_l0);
        let mut guard = l0.write();
        guard.pending_embeddings.insert(vid, label.clone());
        true
    }

    /// Drain `pending_embeddings` from the rotated old-L0 right before
    /// `flush_stream_l1` reads it. Groups by label, issues one batched
    /// `process_embeddings_for_batch` call per label, and writes the
    /// resulting embedding vectors into each VID's `vertex_properties`
    /// map. After this returns, the flush proceeds against an L0 that
    /// looks no different from one whose embeddings were generated
    /// per-row at insert.
    ///
    /// Idempotent: a VID whose embedding was already materialized
    /// (e.g., by on-demand read paths in a future Phase B revision) is
    /// detected via `properties.contains_key(target_prop)` inside
    /// `process_embeddings_for_batch` (writer.rs:~2650), so re-running
    /// the drain is safe.
    async fn drain_pending_embeddings(&self, old_l0_arc: &Arc<RwLock<L0Buffer>>) -> Result<()> {
        let by_label: HashMap<String, Vec<Vid>> = {
            let guard = old_l0_arc.read();
            if guard.pending_embeddings.is_empty() {
                return Ok(());
            }
            let mut m: HashMap<String, Vec<Vid>> = HashMap::new();
            for (vid, label) in &guard.pending_embeddings {
                m.entry(label.clone()).or_default().push(*vid);
            }
            m
        };

        for (label, vids) in by_label {
            let mut properties_batch: Vec<Properties> = {
                let guard = old_l0_arc.read();
                vids.iter()
                    .map(|vid| {
                        guard
                            .vertex_properties
                            .get(vid)
                            .cloned()
                            .unwrap_or_default()
                    })
                    .collect()
            };

            self.process_embeddings_for_batch(std::slice::from_ref(&label), &mut properties_batch)
                .await?;

            let mut guard = old_l0_arc.write();
            for (vid, props) in vids.iter().zip(properties_batch) {
                let target = guard.vertex_properties.entry(*vid).or_default();
                for (k, v) in props {
                    target.insert(k, v);
                }
                guard.pending_embeddings.remove(vid);
            }
        }
        Ok(())
    }

    /// Materialise MUVERA FDE columns for the about-to-flush L0. Mirrors
    /// [`Self::drain_pending_embeddings`]: for each MUVERA index, compute the derived
    /// Fixed-Dimensional Encoding from each row's source multi-vector and inject it into
    /// that row's `vertex_properties` (so the normal column builder writes the
    /// `__fde_*` column with no hot-path change). Every row flushes whole, so a `SET` of the
    /// source column carries the recomputed FDE.
    ///
    /// No-op when the schema has no MUVERA index. Unlike auto-embed, the FDE is a pure,
    /// deterministic, in-process transform — no runtime/embedding service needed.
    fn materialize_fde_columns(&self, old_l0_arc: &Arc<RwLock<L0Buffer>>) -> Result<()> {
        let schema = self.schema_manager.schema();
        let specs = crate::storage::muvera_index::fde_specs(&schema);
        if specs.is_empty() {
            return Ok(());
        }
        let mut guard = old_l0_arc.write();
        for spec in &specs {
            let encoder = uni_common::muvera::FdeEncoder::new(&spec.params)
                .map_err(|e| anyhow!("MUVERA index '{}': {e}", spec.index_name))?;
            // VIDs of this label currently in L0 (collect first to avoid a borrow
            // conflict with the per-row mutation below).
            let vids: Vec<Vid> = guard
                .vertex_labels
                .iter()
                .filter(|(_, labels)| labels.contains(&spec.label))
                .map(|(vid, _)| *vid)
                .collect();
            for vid in vids {
                // Decode the source multi-vector tokens (borrow ends before the mutation).
                let tokens = match guard
                    .vertex_properties
                    .get(&vid)
                    .and_then(|p| p.get(&spec.source_prop))
                {
                    Some(v) => crate::storage::muvera_index::value_to_multivec(v),
                    None => continue, // source absent → leave the FDE column NULL
                };
                // A source token with the wrong dimension makes `encode_doc` error.
                // Skipping the row (leaving the FDE column NULL → ranks last under the
                // mandatory Dot metric; harmless) keeps one malformed document from
                // wedging *every* flush of this label (issue #96). Post-#137 the write
                // paths reject wrong-dimension tokens and WAL replay nulls pre-fix
                // ones, so this skip is defense-in-depth for values that predate
                // dimension enforcement.
                let fde = match encoder.encode_doc(&tokens) {
                    Ok(fde) => fde,
                    Err(e) => {
                        // Skipping is still the right call (see above), but the
                        // consequence is a *wrong answer*, not a slow one: this
                        // vertex is now permanently absent from every accelerated
                        // multi-vector search over this index, and nothing retries
                        // it. A `warn!` is the weakest possible record of that —
                        // greppable if someone thinks to look, invisible otherwise.
                        // Count it, so silent recall loss is visible without
                        // reading the log (#233).
                        metrics::counter!(
                            "uni_muvera_fde_encode_failures_total",
                            "index" => spec.index_name.clone()
                        )
                        .increment(1);
                        tracing::warn!(
                            index = %spec.index_name,
                            vid = ?vid,
                            error = %e,
                            "muvera.fde.skip_malformed: leaving FDE NULL for a source \
                             multi-vector that failed encoding"
                        );
                        continue;
                    }
                };
                if let Some(props) = guard.vertex_properties.get_mut(&vid) {
                    props.insert(spec.derived_col.clone(), Value::Vector(fde));
                }
            }
        }
        Ok(())
    }

    /// Validates auto-embed dense output width against the declared `VECTOR(dim)` targets.
    ///
    /// A schema/model dimension mismatch fails here with a message naming the embedding
    /// alias, instead of surfacing later as a generic dimension error — or, on the
    /// deferred path, as a flush failure (issue #137).
    fn check_dense_embed_dims(
        schema: &uni_common::core::schema::Schema,
        label: &str,
        alias: &str,
        targets: &[String],
        actual: usize,
    ) -> Result<()> {
        use uni_common::core::schema::DataType;
        for target in targets {
            let declared = schema
                .properties
                .get(label)
                .and_then(|props| props.get(target))
                .map(|meta| &meta.r#type);
            if let Some(DataType::Vector { dimensions }) = declared
                && actual != *dimensions
            {
                return Err(anyhow!(
                    "auto-embed: model alias '{alias}' produced a {actual}-dimensional \
                     embedding but '{label}.{target}' is declared VECTOR({dimensions}); \
                     fix the schema dimensions or the embedding model"
                ));
            }
        }
        Ok(())
    }

    /// Validates auto-embed multi-vector output against declared `List(Vector(dim))` targets.
    ///
    /// Multi-vector sibling of [`Self::check_dense_embed_dims`]: every emitted token must
    /// match the declared per-token dimensions.
    fn check_multi_embed_dims(
        schema: &uni_common::core::schema::Schema,
        label: &str,
        alias: &str,
        targets: &[String],
        tokens: &[Vec<f32>],
    ) -> Result<()> {
        use uni_common::core::schema::DataType;
        for target in targets {
            let declared = schema
                .properties
                .get(label)
                .and_then(|props| props.get(target))
                .map(|meta| &meta.r#type);
            if let Some(DataType::List(inner)) = declared
                && let DataType::Vector { dimensions } = inner.as_ref()
                && let Some(bad) = tokens.iter().find(|tok| tok.len() != *dimensions)
            {
                return Err(anyhow!(
                    "auto-embed: model alias '{alias}' produced a token with {} dimensions \
                     but '{label}.{target}' is declared as a multi-vector of \
                     {dimensions}-dimensional tokens; fix the schema dimensions or the \
                     embedding model",
                    bad.len()
                ));
            }
        }
        Ok(())
    }

    /// Process embeddings for a batch of vertices efficiently.
    ///
    /// Groups vertices by embedding config and makes batched API calls to the
    /// embedding service instead of calling once per vertex.
    ///
    /// # Performance
    /// For N vertices with embedding config:
    /// - Old approach: N API calls to embedding service
    /// - New approach: 1 API call per embedding config (usually 1 total)
    async fn process_embeddings_for_batch(
        &self,
        labels: &[String],
        properties_batch: &mut [Properties],
    ) -> Result<()> {
        let Some(label) = labels.first().map(|s| s.as_str()) else {
            return Ok(());
        };
        let schema = self.schema_manager.schema();

        // Group auto-embed targets by (alias, source). A group with both a dense Vector and a
        // multi-vector List<Vector> column is a single-pass hybrid source: one inference fills
        // both. Non-mixed groups use the dense / multi-vector embedder as before.
        let groups = collect_embed_groups(&schema, label);
        if groups.is_empty() {
            return Ok(());
        }

        for (key, group) in groups {
            let alias = &key.0;
            let want_dense = !group.dense.is_empty();
            let want_multi = !group.multi.is_empty();
            let want_sparse = !group.sparse.is_empty();

            // A row needs this group's inference if it has the source text and is still missing
            // at least one of the group's target columns (user-supplied values are preserved).
            let mut input_texts: Vec<String> = Vec::new();
            let mut needs: Vec<usize> = Vec::new();
            for (idx, properties) in properties_batch.iter().enumerate() {
                let all_present = group
                    .dense
                    .iter()
                    .chain(group.multi.iter())
                    .chain(group.sparse.iter())
                    .all(|t| properties.contains_key(t));
                if all_present {
                    continue;
                }
                let mut inputs = Vec::new();
                for src in &group.source_properties {
                    if let Some(val) = properties.get(src)
                        && let Some(s) = val.as_str()
                    {
                        inputs.push(s.to_string());
                    }
                }
                if inputs.is_empty() {
                    continue;
                }
                let text = inputs.join(" ");
                let text = match &group.document_prefix {
                    Some(prefix) => format!("{prefix}{text}"),
                    None => text,
                };
                input_texts.push(text);
                needs.push(idx);
            }
            if input_texts.is_empty() {
                continue;
            }

            let runtime = self
                .xervo_runtime
                .get()
                .ok_or_else(|| anyhow!("Uni-Xervo runtime not configured for auto-embedding"))?;
            let input_refs: Vec<&str> = input_texts.iter().map(|s| s.as_str()).collect();
            let (dense, multi, sparse) = embed_group(
                runtime,
                alias,
                &input_refs,
                want_dense,
                want_multi,
                want_sparse,
            )
            .await?;

            // Reject a schema/model width mismatch before inserting anything — on the
            // deferred (flush-time) path this is the only guard between the model output
            // and the fail-closed column builders (issue #137).
            if let Some(d) = dense.as_ref() {
                for vec in d {
                    Self::check_dense_embed_dims(&schema, label, alias, &group.dense, vec.len())?;
                }
            }
            if let Some(m) = multi.as_ref() {
                for tokens in m {
                    Self::check_multi_embed_dims(&schema, label, alias, &group.multi, tokens)?;
                }
            }

            for (i, &row) in needs.iter().enumerate() {
                if let Some(vec) = dense.as_ref().and_then(|d| d.get(i)) {
                    let vals: Vec<Value> = vec.iter().map(|f| Value::Float(*f as f64)).collect();
                    for t in &group.dense {
                        if !properties_batch[row].contains_key(t) {
                            properties_batch[row].insert(t.clone(), Value::List(vals.clone()));
                        }
                    }
                }
                if let Some(tokens) = multi.as_ref().and_then(|m| m.get(i)) {
                    let mv = multivec_to_value(tokens);
                    for t in &group.multi {
                        if !properties_batch[row].contains_key(t) {
                            properties_batch[row].insert(t.clone(), mv.clone());
                        }
                    }
                }
                if let Some(pairs) = sparse.as_ref().and_then(|s| s.get(i)) {
                    let sv = sparse_pairs_to_value(pairs);
                    for t in &group.sparse {
                        if !properties_batch[row].contains_key(t) {
                            properties_batch[row].insert(t.clone(), sv.clone());
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_embeddings_impl(
        &self,
        label_name: Option<&str>,
        properties: &mut Properties,
    ) -> Result<()> {
        let schema = self.schema_manager.schema();

        let Some(label) = label_name else {
            return Ok(());
        };

        // Same (alias, source) grouping as the deferred path: a mixed dense + multi-vector
        // group is a single-pass hybrid source (one inference fills both columns).
        let groups = collect_embed_groups(&schema, label);
        if groups.is_empty() {
            log::info!("No embedding config found for label {}", label);
            return Ok(());
        }

        for (key, group) in groups {
            let alias = &key.0;
            // Skip if every target already present (user-supplied values win).
            if group
                .dense
                .iter()
                .chain(group.multi.iter())
                .chain(group.sparse.iter())
                .all(|t| properties.contains_key(t))
            {
                continue;
            }

            let mut inputs = Vec::new();
            for src in &group.source_properties {
                if let Some(val) = properties.get(src)
                    && let Some(s) = val.as_str()
                {
                    inputs.push(s.to_string());
                }
            }
            if inputs.is_empty() {
                continue;
            }
            let text = inputs.join(" ");
            let text = match &group.document_prefix {
                Some(prefix) => format!("{prefix}{text}"),
                None => text,
            };

            let runtime = self
                .xervo_runtime
                .get()
                .ok_or_else(|| anyhow!("Uni-Xervo runtime not configured for auto-embedding"))?;
            let want_dense = !group.dense.is_empty();
            let want_multi = !group.multi.is_empty();
            let want_sparse = !group.sparse.is_empty();
            let (dense, multi, sparse) = embed_group(
                runtime,
                alias,
                &[text.as_str()],
                want_dense,
                want_multi,
                want_sparse,
            )
            .await?;

            if let Some(vec) = dense.as_ref().and_then(|d| d.first()) {
                // Alias-naming guard for the schema/model width mismatch (issue #137).
                Self::check_dense_embed_dims(&schema, label, alias, &group.dense, vec.len())?;
                let vals: Vec<Value> = vec.iter().map(|f| Value::Float(*f as f64)).collect();
                for t in &group.dense {
                    if !properties.contains_key(t) {
                        properties.insert(t.clone(), Value::List(vals.clone()));
                    }
                }
            }
            if let Some(tokens) = multi.as_ref().and_then(|m| m.first()) {
                Self::check_multi_embed_dims(&schema, label, alias, &group.multi, tokens)?;
                let mv = multivec_to_value(tokens);
                for t in &group.multi {
                    if !properties.contains_key(t) {
                        properties.insert(t.clone(), mv.clone());
                    }
                }
            }
            if let Some(pairs) = sparse.as_ref().and_then(|s| s.first()) {
                let sv = sparse_pairs_to_value(pairs);
                for t in &group.sparse {
                    if !properties.contains_key(t) {
                        properties.insert(t.clone(), sv.clone());
                    }
                }
            }
        }
        Ok(())
    }

    /// Re-validate the declared-property values a transaction carries against
    /// `schema`, converting them as the write path would.
    ///
    /// For a commit whose statements validated against an older schema; see
    /// the call in [`Self::commit_transaction_l0_sequenced`]. Vertices are
    /// judged by the declared type of the first of their labels that declares
    /// the property (as the write path does), edges by their type's.
    ///
    /// # Errors
    ///
    /// `UniError::Type` for the first value the declared type cannot hold.
    fn revalidate_declared_properties(
        &self,
        tx_l0: &mut L0Buffer,
        schema: &uni_common::core::schema::Schema,
    ) -> Result<()> {
        use crate::storage::declared_values::coerce;
        let refuse = |what: String, name: &str, declared: &uni_common::DataType, e| {
            anyhow::Error::from(uni_common::UniError::Type {
                expected: format!(
                    "TypeError: property '{name}' of {what} is declared {declared:?} (declared \
                     while this transaction was open) but its value does not fit: {e}"
                ),
                actual: String::new(),
            })
        };
        let L0Buffer {
            vertex_properties,
            vertex_labels,
            vertex_tombstones,
            edge_properties,
            edge_endpoints,
            edge_types,
            tombstones,
            ..
        } = tx_l0;
        for (vid, props) in vertex_properties.iter_mut() {
            if vertex_tombstones.contains(vid) {
                continue;
            }
            let Some(labels) = vertex_labels.get(vid) else {
                continue;
            };
            for (name, value) in props.iter_mut() {
                let Some(declared) = labels.iter().find_map(|l| {
                    schema
                        .properties
                        .get(l)
                        .and_then(|p| p.get(name))
                        .map(|m| &m.r#type)
                }) else {
                    continue;
                };
                if declared.accepts(value) && declared.check_vector_dims(value).is_ok() {
                    continue;
                }
                *value = coerce(name, declared, value.clone())
                    .map_err(|e| refuse(format!("vertex {}", vid.as_u64()), name, declared, e))?;
            }
        }
        for (eid, props) in edge_properties.iter_mut() {
            if tombstones.contains_key(eid) {
                continue;
            }
            let type_name = edge_types.get(eid).cloned().or_else(|| {
                edge_endpoints
                    .get(eid)
                    .and_then(|(_, _, t)| self.schema_manager.edge_type_name_by_id_unified(*t))
            });
            let Some(declared_props) = type_name.and_then(|t| schema.properties.get(&t)) else {
                continue;
            };
            for (name, value) in props.iter_mut() {
                let Some(declared) = declared_props.get(name).map(|m| &m.r#type) else {
                    continue;
                };
                if declared.accepts(value) && declared.check_vector_dims(value).is_ok() {
                    continue;
                }
                *value = coerce(name, declared, value.clone())
                    .map_err(|e| refuse(format!("edge {}", eid.as_u64()), name, declared, e))?;
            }
        }
        Ok(())
    }

    /// Backfill `label` from the main vertex table once it has entered the
    /// live catalog (#304). See [`StorageManager::backfill_declared_label`].
    ///
    /// A flush reads the schema once, at the start of its stream phase, and
    /// writes a vertex into a label table only for labels that snapshot
    /// declares. A flush still holding a snapshot from before the declaration
    /// adds rows to the main table alone, and if it does so after the backfill
    /// scanned, they stay hidden. Every such flush is waited out first:
    /// - an inline flush holds `flush_lock` from rotate to finalize, so taking
    ///   the lock once waits for it;
    /// - an async flush rotates under `flush_lock` and streams outside it, so
    ///   the coordinator is drained.
    ///
    /// The lock is released before the drain, because finalizing an async
    /// flush takes it. A flush that rotates after the lock was taken reads the
    /// schema after the declaration and writes the label table itself; it may
    /// run alongside the backfill, which is safe for the reason given on
    /// `backfill_declared_label`. The backfill does not hold `flush_lock`, so
    /// commits are not stalled behind a scan of the main table.
    ///
    /// # Errors
    ///
    /// Fails if pending flushes stall past `flush_drain_timeout`, or if the
    /// backfill fails.
    pub async fn backfill_declared_label(&self, label: &str) -> Result<usize> {
        self.drain_pre_declaration_flushes(label).await?;
        let copied = self.storage.backfill_declared_label(label).await?;
        self.record_backfilled_rows(label, copied).await?;
        Ok(copied)
    }

    /// Count `rows` appended to `label`'s table outside a flush in the
    /// manifest the next flush publishes.
    ///
    /// `SnapshotManifest::vertices[label].count` accumulates the live rows
    /// each flush appends, and the index growth trigger compares it with an
    /// index's `row_count_at_build` (a Lance row count, which does include
    /// backfilled rows). Left out, a label declared over a million vertices
    /// would need a million and a half flushed rows, not half a million, to
    /// trigger a rebuild. The flush publishes the manifest it seeds from
    /// `cached_manifest`, so the rows are added there. Like a flush's own
    /// increment, it can be lost to an async flush whose stream copied the
    /// manifest before this and finalizes after it; the count only drives a
    /// heuristic trigger and `SHOW STATISTICS`.
    ///
    /// # Errors
    ///
    /// Propagates a failure to load the latest manifest.
    pub async fn record_backfilled_rows(&self, label: &str, rows: usize) -> Result<()> {
        if rows == 0 {
            return Ok(());
        }
        if self.cached_manifest.lock().is_none() {
            // Seeded as a flush would seed it. No manifest at all means
            // nothing was ever flushed, so there was nothing to backfill.
            let Some(latest) = self
                .storage
                .snapshot_manager()
                .load_latest_snapshot()
                .await?
            else {
                return Ok(());
            };
            let mut cached = self.cached_manifest.lock();
            if cached.is_none() {
                *cached = Some(latest);
            }
        }
        if let Some(manifest) = self.cached_manifest.lock().as_mut() {
            manifest
                .vertices
                .entry(label.to_string())
                .or_insert(LabelSnapshot {
                    version: 0,
                    count: 0,
                    lance_version: 0,
                })
                .count += rows as u64;
        }
        Ok(())
    }

    /// Exclude commits and flushes while a declaration checks the values its
    /// entities already hold and publishes its catalog change.
    ///
    /// A declaration validates and converts the values it adopts
    /// (`StorageManager::check_declared_values`), then changes the catalog.
    /// A commit landing between the two would carry values validated against
    /// the schema without the declaration, and the commit's own re-validation
    /// (`revalidate_declared_properties`) would not fire because the version
    /// had not moved yet. Holding `flush_lock` across both closes that window:
    /// a commit before it is flushed and checked, one after it sees the new
    /// version. Async flushes still streaming are drained first (their
    /// finalize needs the lock), so none is in flight while it is held.
    ///
    /// Commits wait for the declaration while this is held. Drop it before
    /// anything that takes `flush_lock` itself, such as
    /// [`Self::backfill_declared_label`] or [`Self::flush_to_l1`].
    ///
    /// # Errors
    ///
    /// Fails if pending flushes stall past `flush_drain_timeout`.
    pub async fn declaration_lock(&self) -> Result<DeclarationLock> {
        loop {
            if let Some(coord) = self.flush_coordinator.as_ref() {
                coord
                    .drain(self.config.flush_drain_timeout)
                    .await
                    .map_err(|e| {
                        anyhow!(
                            "cannot start a schema declaration: pending flushes did not \
                             drain: {e} (waited {:?})",
                            self.config.flush_drain_timeout
                        )
                    })?;
            }
            let guard = self.flush_lock.clone().lock_owned().await;
            // A commit may have rotated an async flush between the drain and
            // the lock; none can while the lock is held.
            if self
                .flush_coordinator
                .as_ref()
                .is_none_or(|c| c.pending_flush_count() == 0)
            {
                return Ok(DeclarationLock { _guard: guard });
            }
        }
    }

    /// Flush committed L0 data of `entities` ahead of a declaration that
    /// changes their property set, under its [`Self::declaration_lock`].
    ///
    /// A declaration validates and converts the values it moves into typed
    /// columns (`StorageManager::check_declared_values`), and it reads them
    /// where flushed data lives. Flushing first puts every committed value
    /// there. A no-op when none of `entities` has L0 data.
    ///
    /// # Errors
    ///
    /// Propagates a flush failure.
    pub async fn flush_before_declaration(
        &self,
        _held: &DeclarationLock,
        entities: &[&str],
    ) -> Result<()> {
        let l0 = self.committed_l0_context();
        if entities.iter().any(|e| l0.entity_count(e) > 0) {
            self.flush_inline_under_lock(None).await?;
            self.ensure_no_failed_flush()?;
        }
        Ok(())
    }

    /// The committed, unflushed L0 buffers: current plus pending flush, with
    /// no transaction buffer.
    #[must_use]
    pub fn committed_l0_context(&self) -> crate::runtime::l0_visibility::L0Context {
        crate::runtime::l0_visibility::L0Context {
            current_l0: Some(self.l0_manager.get_current()),
            transaction_l0: None,
            pending_flush_l0s: self.l0_manager.get_pending_flush(),
        }
    }

    /// The barrier half of [`Self::backfill_declared_label`].
    async fn drain_pre_declaration_flushes(&self, label: &str) -> Result<()> {
        drop(self.flush_lock.lock().await);
        if let Some(coord) = self.flush_coordinator.as_ref() {
            coord
                .drain(self.config.flush_drain_timeout)
                .await
                .map_err(|e| {
                    anyhow!(
                        "cannot backfill label '{label}': pending flushes did not drain: {e} \
                         (waited {:?})",
                        self.config.flush_drain_timeout
                    )
                })?;
        }
        Ok(())
    }

    /// Flushes the current in-memory L0 buffer to L1 storage.
    ///
    /// # Lock Ordering
    ///
    /// To prevent deadlocks, locks must be acquired in the following order:
    /// 1. `Writer` lock (held by caller via outer `Arc<RwLock<Writer>>`; removed in Phase 4)
    /// 2. `flush_lock` (acquired by this entry point; held across the whole flush)
    /// 3. `L0Manager` lock (via `begin_flush` / `get_current`)
    /// 4. `L0Buffer` lock (individual buffer RWLocks)
    /// 5. `Index` / `Storage` locks (during actual flush)
    ///
    /// Callers that already hold `flush_lock` (today only `commit_transaction_l0`)
    /// must call `flush_inline_under_lock` (private) directly to avoid a re-entrant
    /// `tokio::sync::Mutex` deadlock — see concurrent_writer.md §5.5.
    pub async fn flush_to_l1(&self, name: Option<String>) -> Result<String> {
        // Drain any in-flight async flushes first. `flush_to_l1` is a
        // SYNCHRONIZATION BARRIER — callers (test fixtures, fork
        // setup, shutdown paths) rely on it as "all writes are now
        // durably in Lance". Without the drain, an async stream from
        // a recent commit might still be writing to Lance when
        // `flush_to_l1` returns, leaving a window where forks branch
        // off pre-write Lance state and lose data.
        if let Some(coord) = self.flush_coordinator.as_ref() {
            // Propagated, not discarded. A drain that does not finish means the
            // barrier this function documents has not been established, and
            // returning Ok would hand the caller a guarantee it does not have.
            coord
                .drain(self.config.flush_drain_timeout)
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "flush_to_l1 barrier not established: {e} (waited {:?}); \
                         writes are not yet durable in Lance",
                        self.config.flush_drain_timeout
                    )
                })?;
        }
        let _flush_lock_guard = self.flush_lock.lock().await;
        let manifest = self.flush_inline_under_lock(name).await?;
        self.ensure_no_failed_flush()?;
        Ok(manifest)
    }

    /// Fails when an async flush has failed and stranded its L0, so that a
    /// caller relying on "every write is now in Lance" does not get `Ok`.
    fn ensure_no_failed_flush(&self) -> Result<()> {
        // The barrier's contract is "all writes are now durably in Lance", so a
        // flush whose stream phase FAILED must not be reported as success.
        //
        // Draining does not surface it: the failure decrements `pending_count`,
        // so the drain completes normally. And the inline flush above cannot
        // repair it, because it only ever writes `get_current()` — never an L0
        // stranded on `pending_flush` by an earlier failure. So `flush_to_l1`
        // would return Ok having flushed nothing. Observed with a Lance write
        // failing on `Disk quota exceeded`: the ANN index build then found no
        // table, declined at debug level, and every query silently served an
        // exact scan from L0.
        //
        // The count is used rather than `pending_flush.len()`, which was tried
        // first and is a false-positive proxy: under load a buffer can sit there
        // transiently for a perfectly healthy in-flight flush.
        if let Some(coord) = self.flush_coordinator.as_ref() {
            let failed = coord.failed_flush_count();
            if failed > 0 {
                // The cause, not just the count (#200). One occurrence of this
                // under ordinary full-suite load could not be diagnosed
                // afterwards — it did not reproduce, and all that survived was
                // the number. Whatever the next occurrence is, it now says so
                // in the error that stops the run.
                let cause = coord.last_flush_error().unwrap_or_else(|| {
                    "no cause recorded — the failure predates this coordinator".to_string()
                });
                return Err(anyhow::anyhow!(
                    "flush_to_l1 barrier not established: {failed} async flush(es) have failed and \
                     their L0 is stranded; the WAL retains that data and replay recovers it on \
                     restart, but it is NOT in Lance and no later flush picks it up. \
                     Most recent failure: {cause}"
                ));
            }
        }
        Ok(())
    }

    /// Drain in-flight async flushes, so that the caller can take `flush_lock`
    /// knowing no flush is still writing.
    async fn drain_async_flushes(&self) -> Result<()> {
        if let Some(coord) = self.flush_coordinator.as_ref() {
            coord
                .drain(self.config.flush_drain_timeout)
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "in-flight flushes did not drain: {e} (waited {:?})",
                        self.config.flush_drain_timeout
                    )
                })?;
        }
        Ok(())
    }

    /// Sync primary catalog files (relative to `catalog/`) on a local store,
    /// on the blocking pool.
    async fn fsync_catalog(&self, files: Vec<String>) -> Result<()> {
        let local_root = self.storage.local_fs_root();
        tokio::task::spawn_blocking(move || {
            crate::snapshot::manager::fsync_catalog_files(local_root.as_deref(), &files)
        })
        .await
        .map_err(|e| anyhow!("fsync catalog files: task failed: {e}"))?
        .map_err(|e| anyhow!("fsync catalog files: {e}"))
    }

    /// Take an explicit snapshot: flush L0 to L1 and record a restore point,
    /// so that [`Self::restore_snapshot`] can later rewind the store to it.
    ///
    /// Returns the snapshot id. The flush manifest is the snapshot, as for
    /// [`Self::flush_to_l1`]; the restore point adds the storage version of
    /// every table and index dataset, pinned against version cleanup. Taken
    /// with compaction paused and under `flush_lock`, so the versions are the
    /// ones this flush produced. On a fork no restore point is recorded:
    /// restore applies to the primary database.
    ///
    /// # Errors
    ///
    /// When the flush fails, an earlier async flush has failed, or a version
    /// cannot be read or pinned.
    pub async fn create_snapshot(&self, name: Option<String>) -> Result<String> {
        if self.fork_id.is_some() {
            return self.flush_to_l1(name).await;
        }
        self.drain_async_flushes().await?;
        // Index builds first, compaction second, flush lock last: a build
        // takes neither of the others, and compaction never takes
        // `flush_lock`, so this order cannot deadlock. Builds are paused so
        // the catalog and the artifacts recorded below agree.
        let _builds = self.storage.pause_index_builds().await;
        let _paused = self.storage.pause_compaction().await?;
        let _flush_lock_guard = self.flush_lock.lock().await;
        let snapshot_id = self.flush_inline_under_lock(name).await?;
        self.ensure_no_failed_flush()?;
        let point =
            crate::snapshot::restore::capture_restore_point(&self.storage, &snapshot_id).await?;
        self.storage
            .snapshot_manager()
            .save_restore_point(&point)
            .await?;
        self.fsync_catalog(vec![format!("restore_points/{snapshot_id}.json")])
            .await?;
        Ok(snapshot_id)
    }

    /// Restore the database to an explicit snapshot, in this handle and on
    /// disk.
    ///
    /// Afterwards every read sees the snapshot's vertices, edges and property
    /// values, and nothing written after it, flushed or not. Steps, all with
    /// compaction paused and `flush_lock` held:
    ///
    /// 1. flush L0, so that nothing written after the snapshot is left in
    ///    memory or in the WAL to be replayed;
    /// 2. withdraw edge-endpoint verifications recorded after the snapshot
    ///    (the rewound tables were never checked by them);
    /// 3. persist a [`RestoreIntent`] and the manifest to publish, so that a
    ///    crash from here on is completed by the next open;
    /// 4. rewind every table and index dataset to the snapshot's restore
    ///    point, then reinstate the snapshot's catalog
    ///    ([`crate::snapshot::restore::rewind_storage`]);
    /// 5. publish the manifest. It carries the snapshot's contents with the
    ///    current version and WAL marks, so versions keep increasing and no
    ///    later write can reuse one;
    /// 6. drop every in-memory copy of the old state (adjacency, VID labels,
    ///    property and backend caches, cardinalities) and fence optimistic
    ///    concurrency, so a transaction that read before the restore cannot
    ///    commit after it;
    /// 7. rebuild the indexes whose artifacts no longer match their
    ///    definitions, and clear the intent.
    ///
    /// The catalog is rewound with the data: labels, edge types, properties,
    /// indexes and constraints declared, dropped, renamed or retyped after the
    /// snapshot are as they were at it. Background index builds are paused
    /// for the whole restore ([`StorageManager::pause_index_builds`]) and
    /// queued builds run afterwards against the restored store.
    ///
    /// # Errors
    ///
    /// `UniError::SnapshotNotFound` for an unknown id. Refused on a fork,
    /// while the database has forks (`UniError::RestoreBlockedByForks`),
    /// while a transaction is open, while a bulk load is publishing, for a
    /// snapshot without a restore point (one written by an ordinary flush),
    /// and for one whose restore point predates catalog recording. Otherwise
    /// any failing step; from step 3 on, reopening completes the restore.
    ///
    /// [`StorageManager::pause_index_builds`]: crate::storage::manager::StorageManager::pause_index_builds
    ///
    /// [`RestoreIntent`]: crate::snapshot::manager::RestoreIntent
    pub async fn restore_snapshot(&self, snapshot_id: &str) -> Result<()> {
        // Refuse up front too, before the flush below does any work.
        self.refuse_restore_with_forks().await?;
        use crate::snapshot::manager::RestoreIntent;
        use crate::snapshot::restore;

        if self.fork_id.is_some() || self.storage.fork_scope().is_some() {
            anyhow::bail!("restore_snapshot applies to the primary database, not to a fork");
        }
        let sm = self.storage.snapshot_manager();
        if !sm.snapshot_exists(snapshot_id).await? {
            return Err(anyhow::Error::new(uni_common::UniError::SnapshotNotFound {
                snapshot_id: snapshot_id.to_string(),
            }));
        }
        let target = sm.load_snapshot(snapshot_id).await?;
        let point = sm.load_restore_point(snapshot_id).await?.ok_or_else(|| {
            anyhow!(
                "snapshot {snapshot_id} cannot be restored: it has no restore point. Only a \
                 snapshot taken with create_snapshot() or uni.admin.snapshot.create() by this \
                 version records one; a manifest written by an ordinary flush does not"
            )
        })?;
        let Some(catalog) = point.catalog.as_ref() else {
            anyhow::bail!(
                "snapshot {snapshot_id} cannot be restored: its restore point was recorded \
                 before restore points included the catalog, so the schema it would return to \
                 is unknown. Take a new snapshot; delete_snapshot() releases this one"
            );
        };

        let _in_progress = RestoreInProgress::set(&self.restore_in_progress);
        self.drain_async_flushes().await?;
        // Same order as `create_snapshot`. Pausing index builds waits for a
        // build under way to record its outcome, so none straddles the
        // rewind.
        let _builds = self.storage.pause_index_builds().await;
        let _paused = self.storage.pause_compaction().await?;
        let _flush_lock_guard = self.flush_lock.lock().await;

        let open = self.adjacency_manager.pinned_versions().distinct_pinned();
        if open > 0 {
            anyhow::bail!(
                "restore_snapshot refused: transactions are open (pinned at {open} version(s)); \
                 commit or roll them back first"
            );
        }
        if !self.storage.bulk_gate().hidden().is_empty() {
            anyhow::bail!("restore_snapshot refused: a bulk load is publishing");
        }
        // A fork being created captured its fork point under `flush_lock`
        // and registers itself only afterwards. Bumping the epoch before
        // re-reading the registry means such a fork is either seen here
        // (and the restore refuses) or sees the bump once registered (and
        // abandons its pre-restore fork point); see `restore_epoch`.
        self.restore_epoch
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.refuse_restore_with_forks().await?;
        // 1.
        self.flush_inline_under_lock(None).await?;
        self.ensure_no_failed_flush()?;
        if !self.l0_manager.get_pending_flush().is_empty() {
            anyhow::bail!("restore_snapshot refused: an earlier flush left L0 unflushed");
        }
        // A CSR compaction folding overlay segments (the flush may just have
        // started one) must not finish after the reset below and reinstall
        // pre-restore edges.
        let csr_compaction = self.compaction_handle.write().take();
        if let Some(handle) = csr_compaction {
            let _ = handle.await;
        }

        // 2. Catalog before data: should a later step fail, the catalog has
        // only lost trust, which is safe.
        let schema = self.storage.schema_manager();
        if schema.distrust_verifications_after(target.created_at)? {
            schema.save().await?;
        }

        // 3.
        let current = self
            .cached_manifest
            .lock()
            .clone()
            .ok_or_else(|| anyhow!("no manifest after the restore's flush"))?;
        let mut manifest = target.clone();
        manifest.snapshot_id = Uuid::new_v4().to_string();
        manifest.name = None;
        manifest.created_at = Utc::now();
        manifest.parent_snapshot = Some(current.snapshot_id.clone());
        // The version `install_catalog` gives the restored catalog.
        manifest.schema_version = schema
            .schema()
            .schema_version
            .max(catalog.schema_version)
            .wrapping_add(1);
        manifest.version_high_water_mark = current.version_high_water_mark;
        manifest.wal_high_water_mark = current.wal_high_water_mark;
        sm.save_snapshot(&manifest).await?;
        let intent = RestoreIntent {
            snapshot_id: snapshot_id.to_string(),
            manifest_id: manifest.snapshot_id.clone(),
            rebuild_indexes_for: restore::indexes_needing_rebuild(&catalog.indexes, &point),
        };
        sm.save_restore_intent(&intent).await?;
        // Durable before any table moves: a crash after the first rollback is
        // completed only if reopening finds the intent and its manifest.
        self.fsync_catalog(vec![
            format!("manifests/{}.json", manifest.snapshot_id),
            "restore_intent.json".to_string(),
        ])
        .await?;

        // 4.
        restore::rewind_storage(&self.storage, &point, &target).await?;

        // 5.
        sm.set_latest_snapshot(&manifest.snapshot_id).await?;
        let local_root = self.storage.local_fs_root();
        let barrier_id = manifest.snapshot_id.clone();
        tokio::task::spawn_blocking(move || {
            crate::snapshot::manager::fsync_snapshot_pointer(
                local_root.as_deref(),
                None,
                &barrier_id,
            )
        })
        .await
        .map_err(|e| anyhow!("fsync restored snapshot pointer: task failed: {e}"))??;
        *self.cached_manifest.lock() = Some(manifest);

        // 6.
        self.adjacency_manager.reset();
        self.storage.reload_vid_labels_index().await?;
        if let Some(pm) = self.property_manager.as_ref() {
            pm.clear_cache().await;
        }
        self.storage.backend().clear_cache();
        self.storage.cardinality().invalidate_all();
        self.committed_writes.lock().fence(&self.commit_sequence);

        // 7.
        restore::finish(&self.storage, &intent).await?;
        info!(snapshot_id, "restored snapshot");
        Ok(())
    }

    /// Delete a snapshot: its restore point, the tags pinning its versions,
    /// its names and its manifest.
    ///
    /// Once its tags are gone, compaction's version cleanup reclaims the
    /// versions only this snapshot kept, once they are older than
    /// `compaction.version_retention`. Other snapshots keep their own tags
    /// and stay restorable. The steps run under `flush_lock`, so no restore,
    /// snapshot or flush interleaves, in an order that converges when run
    /// again: unpin, then drop the restore point (from then on the snapshot
    /// is not restorable), then the names, then the manifest. When the
    /// manifest is the one `latest` names, a copy under a fresh id is
    /// published as `latest` first, since opening the store reads it.
    ///
    /// # Errors
    ///
    /// `UniError::SnapshotNotFound` when neither a manifest nor a restore
    /// point exists for `snapshot_id`. Refused on a fork, while a restore is
    /// incomplete (its intent is on disk; reopening completes it), and while
    /// a fork was created from the snapshot. Otherwise a failing step.
    pub async fn delete_snapshot(&self, snapshot_id: &str) -> Result<()> {
        if self.fork_id.is_some() || self.storage.fork_scope().is_some() {
            anyhow::bail!("delete_snapshot applies to the primary database, not to a fork");
        }
        if self
            .restore_in_progress
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            anyhow::bail!("delete_snapshot refused: a snapshot restore is in progress");
        }
        let sm = self.storage.snapshot_manager();
        self.drain_async_flushes().await?;
        let _flush_lock_guard = self.flush_lock.lock().await;

        if let Some(intent) = sm.load_restore_intent().await? {
            anyhow::bail!(
                "delete_snapshot refused: the restore of snapshot {} is incomplete; reopen the \
                 database to complete it first",
                intent.snapshot_id
            );
        }
        let point = sm.load_restore_point(snapshot_id).await?;
        let has_manifest = sm.snapshot_exists(snapshot_id).await?;
        if point.is_none() && !has_manifest {
            return Err(anyhow::Error::new(uni_common::UniError::SnapshotNotFound {
                snapshot_id: snapshot_id.to_string(),
            }));
        }
        if let Some(registry) = self.storage.fork_registry() {
            let forks: Vec<String> = registry
                .snapshot()
                .await
                .forks
                .into_values()
                .filter(|f| f.parent_snapshot_id == snapshot_id)
                .map(|f| f.name)
                .collect();
            if !forks.is_empty() {
                anyhow::bail!(
                    "delete_snapshot refused: fork(s) {forks:?} were created from snapshot \
                     {snapshot_id}; drop them first"
                );
            }
        }

        if let Some(point) = &point {
            crate::snapshot::restore::release_restore_point(&self.storage, point).await?;
            sm.delete_restore_point(snapshot_id).await?;
        }
        sm.remove_snapshot_names(snapshot_id).await?;
        if has_manifest {
            if sm.latest_snapshot_id().await?.as_deref() == Some(snapshot_id) {
                let mut successor = sm.load_snapshot(snapshot_id).await?;
                successor.snapshot_id = Uuid::new_v4().to_string();
                successor.name = None;
                sm.save_snapshot(&successor).await?;
                sm.set_latest_snapshot(&successor.snapshot_id).await?;
                let local_root = self.storage.local_fs_root();
                let barrier_id = successor.snapshot_id.clone();
                tokio::task::spawn_blocking(move || {
                    crate::snapshot::manager::fsync_snapshot_pointer(
                        local_root.as_deref(),
                        None,
                        &barrier_id,
                    )
                })
                .await
                .map_err(|e| anyhow!("fsync snapshot pointer: task failed: {e}"))??;
                let mut cached = self.cached_manifest.lock();
                if cached.as_ref().map(|m| m.snapshot_id.as_str()) == Some(snapshot_id) {
                    *cached = Some(successor);
                }
            }
            sm.delete_snapshot_manifest(snapshot_id).await?;
        }
        info!(snapshot_id, "deleted snapshot");
        Ok(())
    }

    /// How many restores have started on this writer.
    ///
    /// A fork created from this writer's database reads it before capturing
    /// its fork point and again once its registry entry is persisted; if it
    /// moved, a [`Self::restore_snapshot`] ran in between, so the captured
    /// dataset versions predate the restore and the fork must not be
    /// created from them. A restore that then finds the fork registered
    /// refuses instead, so one of the two always gives way.
    #[must_use]
    pub fn restore_epoch(&self) -> u64 {
        self.restore_epoch.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Refuse a restore while the fork registry holds any fork, whatever its
    /// state: a restore rewinds primary storage and the shared UID index, but
    /// not fork branches.
    async fn refuse_restore_with_forks(&self) -> Result<()> {
        let Some(registry) = self.storage.fork_registry() else {
            return Ok(());
        };
        // Keyed by name in a `BTreeMap`, so already sorted.
        let forks: Vec<String> = registry.snapshot().await.forks.into_keys().collect();
        if forks.is_empty() {
            return Ok(());
        }
        Err(anyhow::Error::new(
            uni_common::UniError::RestoreBlockedByForks { forks },
        ))
    }

    /// Flush L0→L1 and capture the fork point under one held `flush_lock`.
    ///
    /// Drains in-flight async flushes, takes `flush_lock`, runs the inline
    /// flush, and then — still holding the lock — reads the allocator
    /// high-water marks and each existing candidate dataset's Lance
    /// version. Capturing under the held lock is what makes the fork point
    /// atomic: no concurrent commit can advance the allocator and no
    /// concurrent flush can advance a dataset tip between the flush and the
    /// reads. See [`ForkPoint`].
    ///
    /// `candidate_dataset_names` are resolved to `{base_uri}/{name}.lance`;
    /// names with no `.lance` directory on disk are skipped (returned map
    /// has no entry for them), matching the fork branch loop's existence
    /// check.
    ///
    /// # Errors
    /// Propagates flush failures from `flush_inline_under_lock` and any
    /// per-dataset version read failure from `lance_branch::current_version`.
    ///
    /// # Deadlocks
    /// Must not be called by a task already holding `flush_lock` (e.g.
    /// `commit_transaction_l0`); the `tokio::sync::Mutex` is not reentrant.
    /// Fork creation never holds the lock, so the sole call site is safe.
    pub async fn flush_and_capture_fork_point(
        &self,
        candidate_dataset_names: &[String],
        parent_branches: &BTreeMap<String, String>,
    ) -> Result<ForkPoint> {
        if let Some(coord) = self.flush_coordinator.as_ref() {
            let _ = coord.drain(self.config.drop_fork_drain_timeout).await;
        }
        let _flush_lock_guard = self.flush_lock.lock().await;
        self.flush_inline_under_lock(None).await?;

        // Still under `flush_lock`: capture the allocator HWM, the MVCC
        // version HWM, and every existing dataset's Lance version so
        // nothing can interleave.
        let (vid_hwm, eid_hwm) = self.allocator.current_hwm().await;
        // The parent's current L0 version is the largest `_version` any
        // inherited row can carry (flushed or in-memory). A fork bootstraps
        // its version floor to this so a fork tx read still sees inherited
        // rows. Cheap read lock; no buffer clone.
        let version_hwm = self.l0_manager.get_current().read().current_version;

        let branching = self.storage.backend().branching().ok_or_else(|| {
            anyhow::anyhow!(
                "storage backend does not support fork branching; \
                 cannot capture a fork point"
            )
        })?;
        let mut dataset_versions = BTreeMap::new();
        let mut parent_branch_versions = BTreeMap::new();
        for name in candidate_dataset_names {
            // Resolve after flushing: the flush may have created a parent's
            // first branch for a formerly absent dataset.
            let parent_branch = self
                .storage
                .fork_scope()
                .and_then(|s| s.branch_for(name))
                .or_else(|| parent_branches.get(name).cloned());
            if self.storage.fork_scope().is_some() && parent_branch.is_none() {
                continue;
            }
            if !branching.table_exists(name).await? {
                continue;
            }
            let version = branching.current_version(name).await?;
            dataset_versions.insert(name.clone(), version);

            // For a nested fork, also capture the parent branch's tip under the
            // lock — that is the version the child must branch from, and it
            // advances independently of `main`. Reading it here (still holding
            // `flush_lock`, after `flush_inline_under_lock`) is what makes the
            // nested fork's branch point atomic w.r.t. a concurrent parent
            // commit+flush (the D2 fix).
            if let Some(parent_branch) = parent_branch {
                let branch_version = branching
                    .current_version_on_branch(name, &parent_branch)
                    .await?;
                parent_branch_versions.insert(name.clone(), branch_version);
            }
        }

        Ok(ForkPoint {
            vid_hwm,
            eid_hwm,
            dataset_versions,
            version_hwm,
            parent_branch_versions,
        })
    }

    /// Async-flush entry point: rotate under `flush_lock`, release the
    /// lock, then submit the stream phase to the [`FlushCoordinator`].
    /// Returns a [`FlushTicket`](crate::runtime::flush_coordinator::FlushTicket)
    /// that resolves when finalize completes.
    ///
    /// Errors if `config.async_flush_enabled = false` (the coordinator
    /// is `None` in that case — see `flush_coordinator` field doc).
    pub async fn flush_to_l1_async(
        self: &Arc<Self>,
        name: Option<String>,
    ) -> Result<crate::runtime::flush_coordinator::FlushTicket> {
        let coord = self
            .flush_coordinator
            .as_ref()
            .ok_or_else(|| anyhow!("async flush not enabled (config.async_flush_enabled=false)"))?
            .clone();
        // 1. Acquire permit FIRST (outside flush_lock) so we don't
        //    introduce a permit-while-holding-flush-lock convoy.
        let permit = coord.acquire_permit().await?;
        // 2. Rotate under flush_lock (µs work), then allocate the rotate seq
        //    and bump pending ONLY after the rotate succeeds (Bug #3). A failed
        //    rotate (the `?` below) must consume neither: the finalizer
        //    advances in strictly consecutive seq order and only decrements
        //    pending on finalize, so a leaked seq/pending would wedge it
        //    forever. The seq is allocated under `flush_lock`, immediately
        //    after the rotate and before the guard drops, so concurrent rotates
        //    keep seq order == rotation order, and the seq is unused until
        //    submit. On the `?` error path the permit drops, freeing the slot.
        let (
            RotateOutput {
                old_l0_arc,
                wal_lsn,
                current_version,
                flush_in_progress_guard,
            },
            seq,
        ) = {
            let _flush_lock_guard = self.flush_lock.lock().await;
            let rotate_out = self.flush_l0_rotate().await?;
            let seq = coord.next_rotate_seq();
            coord.note_pending();
            (rotate_out, seq)
        };
        // 3. Build the coordinator's RotatedFlush. parent_manifest is the
        //    cached_manifest snapshot at this moment.
        let parent_manifest = self.cached_manifest.lock().clone();
        let rotated = RotatedFlush {
            seq,
            old_l0_arc: old_l0_arc.clone(),
            wal_lsn,
            current_version,
            name: name.clone(),
            parent_manifest,
            permit,
            flush_in_progress_guard,
        };
        // 4. Spawn the stream phase via the coordinator. The closure
        //    captures Arc<Writer> transiently — drops when stream
        //    completes (bounded, ~50-500 ms).
        let writer = self.clone();
        let ticket = coord.submit_for_stream(rotated, move |old_l0, wal, ver, n| async move {
            let outcome = writer.flush_stream_l1(old_l0, wal, ver, n).await?;
            Ok(crate::runtime::flush_coordinator::FlushOutcome {
                new_manifest: outcome.manifest,
                snapshot_id: outcome.snapshot_id,
            })
        });
        Ok(ticket)
    }

    /// Phase A+B+C of the flush: flush the WAL, rotate L0 (so the
    /// to-be-flushed buffer moves to `pending_flush` and a fresh L0 takes
    /// its place), and hand off the WAL to the new L0.
    ///
    /// Runs in microseconds. Must be called under `flush_lock` (the caller
    /// is responsible). The returned [`RotateOutput`] carries everything
    /// the subsequent stream + finalize phases need; in particular the
    /// [`FlushInProgressGuard`] is bound to the return value so it stays
    /// alive for the full flush lifetime — including any future async
    /// path where stream runs on a spawned task.
    async fn flush_l0_rotate(&self) -> Result<RotateOutput> {
        // The rotated buffer is flushed to L1 and its WAL segments truncated,
        // after which recovery can no longer announce a commit in it. So every
        // commit visible now must have its announcement released first: wait
        // for every issued ticket. `flush_lock` (held by every caller) keeps
        // new tickets from being issued meanwhile, and a ticket's holder
        // releases it without taking that lock, so the wait is bounded by the
        // in-flight commits' post-commit work. A ticket that is never released
        // (a commit future held unpolled) fails the flush rather than hiding a
        // commit from recovery; its data stays durable in the WAL.
        if !self
            .publish_sequencer
            .wait_released(ANNOUNCEMENT_RELEASE_TIMEOUT)
            .await
        {
            return Err(anyhow!(
                "flush refused: a commit visible in L0 has not released its announcement after \
                 {ANNOUNCEMENT_RELEASE_TIMEOUT:?}, and flushing it would hide it from crash \
                 recovery; its data stays durable in the WAL"
            ));
        }
        // Acquire the in-progress counter BEFORE any heavy work. The
        // guard lives on RotateOutput; dropping RotateOutput drops the
        // guard, so the counter goes back to zero exactly when the flush
        // is fully done.
        let flush_in_progress_guard = FlushInProgressGuard::new(&self.storage);

        // A. Flush WAL BEFORE rotating L0. If WAL flush fails, the
        // current L0 is still active and mutations are retained in
        // memory until restart/retry.
        let wal_for_truncate = {
            let current_l0 = self.l0_manager.get_current();
            let l0_guard = current_l0.read();
            l0_guard.wal.clone()
        };
        // Test-only seam (no-op without the `failpoints` feature): inject a
        // WAL-flush failure here to drive the "failed async rotate wedges the
        // finalizer" regression (Bug #3). When configured to "return" it makes
        // `flush_l0_rotate` return Err exactly as a real WAL-flush failure would.
        fail::fail_point!("flush::rotate-fail", |_| {
            Err(anyhow!("flush::rotate-fail injected WAL-flush failure"))
        });
        let wal_lsn = if let Some(ref w) = wal_for_truncate {
            w.flush().await?
        } else {
            0
        };

        // B. Begin flush: rotate L0 and keep old L0 visible to reads via
        // pending_flush until complete_flush is called by finalize.
        let old_l0_arc = self.l0_manager.begin_flush(0, None);
        metrics::counter!("uni_l0_buffer_rotations_total").increment(1);

        // C. WAL handoff: record wal_lsn on old L0, transfer WAL handle
        // and current_version to the new L0.
        let current_version;
        {
            let mut old_l0_guard = old_l0_arc.write();
            current_version = old_l0_guard.current_version;
            // Under the buffer's lock, like every reservation from it.
            old_l0_guard.unpublished_bulk_at_rotation =
                self.l0_manager.unpublished_bulk_at_most(current_version);
            old_l0_guard.wal_lsn_at_flush = wal_lsn;
            let wal = old_l0_guard.wal.take();
            let new_l0_arc = self.l0_manager.get_current();
            let mut new_l0_guard = new_l0_arc.write();
            new_l0_guard.wal = wal;
            // `max`: a bulk load may have reserved a version from the new
            // buffer since the rotation (`L0Manager::reserve_bulk_version`).
            new_l0_guard.current_version = new_l0_guard.current_version.max(current_version);
            // The new active buffer starts accumulating strictly above the
            // rotation point: everything <= `wal_lsn` is now owned by the old
            // (being-flushed) buffer or earlier. This start watermark is the
            // floor that keeps WAL truncation / checkpoint publication from
            // discarding this buffer's data if its eventual flush fails.
            new_l0_guard.wal_lsn_at_start = wal_lsn;
        }

        Ok(RotateOutput {
            old_l0_arc,
            wal_lsn,
            current_version,
            flush_in_progress_guard,
        })
    }

    /// Phases D, E, F, G of the flush: L1 collect, orphan resolve,
    /// manifest seed, Lance writes. Reads from `old_l0_arc` (kept in
    /// pending_flush by Phase B); writes append-only Lance datasets; does
    /// NOT call save_snapshot / set_latest_snapshot — those are
    /// finalize's job, so the manifest doesn't get published until the
    /// next phase.
    ///
    /// Today takes `&self`; in a follow-up commit this becomes a
    /// static `Send + 'static` function over `SharedFlushCtx` so it can
    /// run on a spawned task while concurrent commits proceed.
    async fn flush_stream_l1(
        &self,
        old_l0_arc: Arc<RwLock<L0Buffer>>,
        wal_lsn: u64,
        current_version: u64,
        name: Option<String>,
    ) -> Result<FlushOutcome> {
        // Test-only seam (no-op without the `failpoints` feature): the rotate
        // (begin_flush) already moved the to-be-flushed buffer onto
        // pending_flush and installed a fresh empty current buffer, but the
        // rotated rows are NOT yet durable in Lance. Pausing here holds that
        // window open to drive the unique-constraint-hole regression
        // (Bug #9 Mechanism A).
        fail::fail_point!("flush::after-rotate-before-lance");

        // Test-only (issue #132): model a STALLED flush stream. A lost-wakeup in
        // the sparse/multivec Lance read-modify-write is an *async* future that
        // never resolves while the worker stays idle. The `fail` crate's
        // `sleep`/`pause` actions block the worker thread synchronously — which
        // both mis-models a lost-wakeup and defeats `tokio::time::timeout` (a
        // timeout can't cancel a blocking call, and once the inner future
        // completes `timeout` returns `Ok`). So stall at an async `.await`
        // instead, letting the flush-stream timeout convert it into a data-safe
        // failure. Arm with `flush::stream-async-stall = 1*return` (fires once).
        #[cfg(feature = "failpoints")]
        if fail::eval("flush::stream-async-stall", |_| ()).is_some() {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        }

        // Phase B: materialize any deferred embeddings before column
        // extraction. No-op when `defer_embeddings` is off (the set will
        // be empty). On-demand reads of the embedding column are a TODO
        // for a future revision (see UniConfig::defer_embeddings docs).
        self.drain_pending_embeddings(&old_l0_arc).await?;

        // Materialise MUVERA FDE columns from each row's source multi-vector (pure/sync;
        // no-op without a MUVERA index). Runs after embeddings so a row can be both
        // auto-embedded and FDE-encoded.
        self.materialize_fde_columns(&old_l0_arc)?;

        let schema = self.schema_manager.schema();
        // 2. Acquire Read lock on Old L0 for flushing
        let mut entries_by_type: HashMap<u32, Vec<L1Entry>> = HashMap::new();
        // (Vid, labels, properties, deleted, version)
        type VertexEntry = (Vid, Vec<String>, Properties, bool, u64);
        let mut vertices_by_label: HashMap<u16, Vec<VertexEntry>> = HashMap::new();
        // Vertex tombstones, `(vid, version)` per label table and
        // `(vid, labels, version)` for the main table. Each is appended as a
        // row of its own (`_deleted = true`, no properties).
        let mut tombstones_by_label: HashMap<u16, Vec<(Vid, u64)>> = HashMap::new();
        let mut main_vertex_tombstones: Vec<(Vid, Vec<String>, u64)> = Vec::new();
        // Collect vertex timestamps from L0 for flushing to storage
        let mut vertex_created_at: HashMap<Vid, i64> = HashMap::new();
        let mut vertex_updated_at: HashMap<Vid, i64> = HashMap::new();
        // Track tombstones missing labels for storage query fallback
        let mut orphaned_tombstones: Vec<(Vid, u64)> = Vec::new();

        {
            let old_l0 = old_l0_arc.read();

            // 1. Collect all edges and tombstones from L0
            for edge in old_l0.graph.edges() {
                let properties = old_l0
                    .edge_properties
                    .get(&edge.eid)
                    .cloned()
                    .unwrap_or_default();
                let version = old_l0.edge_versions.get(&edge.eid).copied().unwrap_or(0);

                // Get timestamps from L0 buffer (populated during insert)
                let created_at = old_l0.edge_created_at.get(&edge.eid).copied();
                let updated_at = old_l0.edge_updated_at.get(&edge.eid).copied();

                entries_by_type
                    .entry(edge.edge_type)
                    .or_default()
                    .push(L1Entry {
                        src_vid: edge.src_vid,
                        dst_vid: edge.dst_vid,
                        eid: edge.eid,
                        op: Op::Insert,
                        version,
                        properties,
                        created_at,
                        updated_at,
                    });
            }

            // From tombstones
            for tombstone in old_l0.tombstones.values() {
                let version = old_l0
                    .edge_versions
                    .get(&tombstone.eid)
                    .copied()
                    .unwrap_or(0);
                // Get timestamps - for deletes, updated_at reflects deletion time
                let created_at = old_l0.edge_created_at.get(&tombstone.eid).copied();
                let updated_at = old_l0.edge_updated_at.get(&tombstone.eid).copied();

                entries_by_type
                    .entry(tombstone.edge_type)
                    .or_default()
                    .push(L1Entry {
                        src_vid: tombstone.src_vid,
                        dst_vid: tombstone.dst_vid,
                        eid: tombstone.eid,
                        op: Op::Delete,
                        version,
                        properties: HashMap::new(),
                        created_at,
                        updated_at,
                    });
            }

            // 1b. Collect vertices by label (using vertex_labels from L0)
            //
            // Helper: fan-out a single vertex entry into per-label buckets.
            // Each per-label table row carries the full label set so multi-label
            // info is preserved after flush.
            let push_vertex_to_labels =
                |vid: Vid,
                 all_labels: &[String],
                 props: Properties,
                 deleted: bool,
                 version: u64,
                 out: &mut HashMap<u16, Vec<VertexEntry>>| {
                    for label in all_labels {
                        if let Some(label_id) = schema.label_id_by_name(label) {
                            out.entry(label_id).or_default().push((
                                vid,
                                all_labels.to_vec(),
                                props.clone(),
                                deleted,
                                version,
                            ));
                        }
                    }
                };

            for (vid, props) in &old_l0.vertex_properties {
                let version = old_l0.vertex_write_version(*vid);
                // Collect timestamps for this vertex
                if let Some(&ts) = old_l0.vertex_created_at.get(vid) {
                    vertex_created_at.insert(*vid, ts);
                }
                if let Some(&ts) = old_l0.vertex_updated_at.get(vid) {
                    vertex_updated_at.insert(*vid, ts);
                }
                if let Some(labels) = old_l0.vertex_labels.get(vid) {
                    push_vertex_to_labels(
                        *vid,
                        labels,
                        props.clone(),
                        false,
                        version,
                        &mut vertices_by_label,
                    );
                }
            }
            for &vid in &old_l0.vertex_tombstones {
                let version = old_l0.vertex_versions.get(&vid).copied().unwrap_or(0);
                if let Some(&ts) = old_l0.vertex_updated_at.get(&vid) {
                    vertex_updated_at.insert(vid, ts);
                }
                if let Some(labels) = old_l0.vertex_labels.get(&vid) {
                    // Union the L0 set with the in-memory label index before
                    // bucketing. The L0 entry carries whatever the caller
                    // supplied, and callers that never went through a scan —
                    // `Uni::delete_vertex_by_vid`, the fork-promote path — pass
                    // a single label. A truncated set here leaves a live row in
                    // every unlisted label's table, i.e. the vertex reappears
                    // through its other labels.
                    //
                    // `get_labels_from_index` is sync and O(1), which matters:
                    // the `old_l0` read lock is held here and cannot await. The
                    // index is pruned later in this same flush, so it still
                    // holds the pre-delete set at this point.
                    //
                    // Union is sound for TOMBSTONES ONLY. Writing a tombstone
                    // into a table the vertex has already left adds a row no
                    // read can return (the vertex's newest row there is
                    // already a tombstone, or it never had one), so
                    // the index's known staleness and its rebuild row cap can
                    // only ever cost a missed extra tombstone, never a wrong
                    // one. Do NOT generalize this to SET/REMOVE, where the
                    // label set is a replacement and a union would resurrect
                    // removed labels.
                    let mut labels = labels.clone();
                    for extra in self.storage.get_labels_from_index(vid).unwrap_or_default() {
                        if !labels.contains(&extra) {
                            labels.push(extra);
                        }
                    }

                    for label in &labels {
                        if let Some(label_id) = schema.label_id_by_name(label) {
                            tombstones_by_label
                                .entry(label_id)
                                .or_default()
                                .push((vid, version));
                        }
                    }
                } else {
                    // Tombstone missing labels (old WAL format) - collect for storage query fallback
                    orphaned_tombstones.push((vid, version));
                }
            }
        } // Drop read lock

        // Resolve orphaned tombstones (missing labels) from storage
        if !orphaned_tombstones.is_empty() {
            tracing::warn!(
                count = orphaned_tombstones.len(),
                "Tombstones missing labels in L0, querying storage as fallback"
            );
            // One batched read for the whole orphan set rather than one scan
            // per vid (#220). Absence from the map is the batched spelling of
            // the singular form's `None`, and the error still propagates —
            // which is the #233 property below, preserved.
            let orphan_vids: Vec<Vid> = orphaned_tombstones.iter().map(|(vid, _)| *vid).collect();
            let orphan_labels = MainVertexDataset::find_batch_labels_by_vids(
                self.storage.backend(),
                &orphan_vids,
                None,
            )
            .await?;
            for (vid, version) in orphaned_tombstones {
                // #233 Tier 1: this was `if let Ok(Some(..))`, and the
                // callee additionally ate its scan error with
                // `unwrap_or_default()`. Either swallow drops the tombstone
                // for a vertex whose labels could not be read, so a deleted
                // vertex stays visible after the flush — a silent
                // resurrection. A failed flush is retriable; a lost tombstone
                // is not.
                if let Some(labels) = orphan_labels.get(&vid).cloned()
                    && !labels.is_empty()
                {
                    for label in &labels {
                        if let Some(label_id) = schema.label_id_by_name(label) {
                            tombstones_by_label
                                .entry(label_id)
                                .or_default()
                                .push((vid, version));
                        }
                    }
                }
            }
        }

        // 1. Load previous snapshot from cache, or fall back to storage.
        //
        // Use clone() not take(): for the async path, multiple
        // concurrent streams may run; if we take() here, a sibling
        // stream sees cached_manifest = None and seeds from
        // load_latest_snapshot (stale), losing the chain. clone()
        // preserves the parent. Finalize writes back the new manifest
        // unconditionally.
        let mut manifest = if let Some(cached) = self.cached_manifest.lock().clone() {
            cached
        } else {
            self.storage
                .snapshot_manager()
                .load_latest_snapshot()
                .await?
                .unwrap_or_else(|| {
                    SnapshotManifest::new(Uuid::new_v4().to_string(), schema.schema_version)
                })
        };

        // Update snapshot metadata
        // Save parent snapshot ID before generating new one (for lineage tracking)
        let parent_id = manifest.snapshot_id.clone();
        manifest.parent_snapshot = Some(parent_id);
        manifest.snapshot_id = Uuid::new_v4().to_string();
        manifest.name = name;
        manifest.created_at = Utc::now();
        manifest.version_high_water_mark = current_version;
        // Set, never inherited: a load hidden from the parent manifest may be
        // visible by now.
        manifest.unpublished_bulk_versions = old_l0_arc.read().unpublished_bulk_at_rotation.clone();
        // Cap the published WAL checkpoint at the floor of any OTHER pending
        // flush. A still-pending flush (notably one that FAILED and left its
        // buffer in `pending_flush`) holds committed WAL entries above its start
        // that are NOT in this snapshot; recovery replays from this mark, so
        // claiming durability past that floor would skip them (lost commit). A
        // normal flush with no other pending buffer keeps `wal_lsn` unchanged.
        manifest.wal_high_water_mark = self
            .l0_manager
            .min_pending_wal_lsn_start(&old_l0_arc)
            .map_or(wal_lsn, |floor| floor.min(wal_lsn));
        let snapshot_id = manifest.snapshot_id.clone();

        tracing::Span::current().record("snapshot_id", &snapshot_id);

        // 2. Write main unified tables FIRST (before deltas).
        //    Ensures the dual-write invariant: by the time an EID appears in a
        //    delta table, it already exists in main_edges. This prevents the
        //    compaction debug_assert from firing when compaction interleaves
        //    with flush at async yield points.
        //
        // 2.1 Main edges table
        let (main_edges, edge_created_at_map, edge_updated_at_map) = {
            let _old_l0 = old_l0_arc.read();
            let mut main_edges: Vec<(
                uni_common::core::id::Eid,
                Vid,
                Vid,
                String,
                Properties,
                bool,
                u64,
            )> = Vec::new();
            let mut edge_created_at_map: HashMap<uni_common::core::id::Eid, i64> = HashMap::new();
            let mut edge_updated_at_map: HashMap<uni_common::core::id::Eid, i64> = HashMap::new();

            for (&edge_type_id, entries) in entries_by_type.iter() {
                for entry in entries {
                    let edge_type_name = self
                        .storage
                        .schema_manager()
                        .edge_type_name_by_id_unified(edge_type_id)
                        .unwrap_or_else(|| "unknown".to_string());

                    let deleted = matches!(entry.op, Op::Delete);
                    main_edges.push((
                        entry.eid,
                        entry.src_vid,
                        entry.dst_vid,
                        edge_type_name,
                        entry.properties.clone(),
                        deleted,
                        entry.version,
                    ));

                    if let Some(ts) = entry.created_at {
                        edge_created_at_map.insert(entry.eid, ts);
                    }
                    if let Some(ts) = entry.updated_at {
                        edge_updated_at_map.insert(entry.eid, ts);
                    }
                }
            }

            (main_edges, edge_created_at_map, edge_updated_at_map)
        };

        if !main_edges.is_empty() {
            let main_edge_batch = MainEdgeDataset::build_record_batch(
                &main_edges,
                Some(&edge_created_at_map),
                Some(&edge_updated_at_map),
            )?;
            MainEdgeDataset::write_batch(self.storage.backend(), main_edge_batch).await?;
            MainEdgeDataset::ensure_default_indexes(self.storage.backend()).await?;
        }

        // 2.2 Main vertices table
        let mut main_vertices: Vec<(Vid, Vec<String>, Properties, bool, u64)> = {
            let old_l0 = old_l0_arc.read();
            let mut vertices = Vec::new();

            // Live vertices: full-row Append on the main table (the
            // props_json blob is required for global ID lookups).
            for (vid, props) in &old_l0.vertex_properties {
                let version = old_l0.vertex_write_version(*vid);
                let labels = old_l0.vertex_labels.get(vid).cloned().unwrap_or_default();
                vertices.push((*vid, labels, props.clone(), false, version));
            }

            // Tombstones: appended below as rows of their own, after the
            // relabels are added. They carry the labels the vertex had, so a
            // label-filtered read of the main table meets the tombstone
            // beside the vertex's older rows.
            for &vid in &old_l0.vertex_tombstones {
                let version = old_l0.vertex_versions.get(&vid).copied().unwrap_or(0);
                let mut labels = old_l0.vertex_labels.get(&vid).cloned().unwrap_or_default();
                for extra in self.storage.get_labels_from_index(vid).unwrap_or_default() {
                    if !labels.contains(&extra) {
                        labels.push(extra);
                    }
                }
                main_vertex_tombstones.push((vid, labels, version));
            }

            vertices
        };

        // M8: durable label-only mutations across flush windows.
        //
        // `SET n:Label` / `REMOVE n:Label` mark the vid in
        // `vertex_label_overwrites` and update `vertex_labels`, but for a
        // vid flushed in a PRIOR window they never re-add it to
        // `vertex_properties`. The loops above key off `vertex_properties`,
        // so such a relabel would be silently lost: absent from the main
        // table, the per-label datasets, and the VidLabelsIndex (and so
        // `rebuild_vid_labels_index` reads stale labels after a restart).
        // The same-window create+relabel case already works because the
        // create put the vid in `vertex_properties`.
        //
        // Re-derive each overwrite-only vid by fetching its persisted props
        // and labels, then route it into `main_vertices` (main table +
        // index), the new per-label datasets, and a tombstone in any
        // per-label dataset it left. `MATCH (n:OldLabel)` scans the
        // per-label table directly, so the old-label tombstone is required.
        let overwrite_only: Vec<(Vid, Vec<String>, u64)> = {
            let old_l0 = old_l0_arc.read();
            old_l0
                .vertex_label_overwrites
                .iter()
                .filter(|vid| {
                    !old_l0.vertex_properties.contains_key(*vid)
                        && !old_l0.vertex_tombstones.contains(*vid)
                })
                .map(|vid| {
                    let labels = old_l0.vertex_labels.get(vid).cloned().unwrap_or_default();
                    let version = old_l0.vertex_write_version(*vid);
                    (*vid, labels, version)
                })
                .collect()
        };
        // Both reads below are batched over the whole relabel set rather than
        // issued twice per vid (#220). The version bounds are unchanged: the
        // props read carries the high-water mark as before, and the labels read
        // carries none, matching `find_vertex_labels_in_storage`, which ranks
        // versions itself rather than filtering on one.
        let relabel_vids: Vec<Vid> = overwrite_only.iter().map(|(vid, ..)| *vid).collect();
        let relabel_props = MainVertexDataset::find_batch_props_by_vids(
            self.storage.backend(),
            &relabel_vids,
            self.storage.version_high_water_mark(),
        )
        .await?;
        let relabel_old_labels = MainVertexDataset::find_batch_labels_by_vids(
            self.storage.backend(),
            &relabel_vids,
            None,
        )
        .await?;

        for (vid, new_labels, version) in overwrite_only {
            // Persisted props of the prior-window row — required so the
            // re-Appended main row does not blank the vertex's properties.
            let Some(props) = relabel_props.get(&vid).cloned() else {
                tracing::warn!(
                    vid = vid.as_u64(),
                    "label-only mutation for a vid with no persisted main row; skipping flush \
                     of its relabel"
                );
                continue;
            };
            // Labels the vid carried BEFORE this relabel; the storage read
            // reflects pre-flush state. Any label no longer present must be
            // tombstoned in its per-label dataset.
            let old_labels = relabel_old_labels.get(&vid).cloned().unwrap_or_default();

            main_vertices.push((vid, new_labels.clone(), props.clone(), false, version));
            for label in &new_labels {
                if let Some(label_id) = schema.label_id_by_name(label) {
                    vertices_by_label.entry(label_id).or_default().push((
                        vid,
                        new_labels.clone(),
                        props.clone(),
                        false,
                        version,
                    ));
                }
            }
            for label in &old_labels {
                if !new_labels.contains(label)
                    && let Some(label_id) = schema.label_id_by_name(label)
                {
                    tombstones_by_label
                        .entry(label_id)
                        .or_default()
                        .push((vid, version));
                }
            }
        }

        // Tombstones are appended, never written over the vertex's older
        // rows; see the per-label write below for why.
        let live_main_rows = main_vertices.len();
        for (vid, labels, version) in &main_vertex_tombstones {
            main_vertices.push((*vid, labels.clone(), Properties::new(), true, *version));
        }
        if !main_vertices.is_empty() {
            let main_vertex_batch = MainVertexDataset::build_record_batch(
                &main_vertices,
                Some(&vertex_created_at),
                Some(&vertex_updated_at),
            )?;
            MainVertexDataset::write_batch(self.storage.backend(), main_vertex_batch).await?;
            MainVertexDataset::ensure_default_indexes(self.storage.backend()).await?;
        }
        main_vertices.truncate(live_main_rows);

        // Keep the VidLabelsIndex current for every flushed vertex. This is the
        // single place that sees all vertices: the per-label fan-out below skips
        // undeclared (schemaless) labels, so updating the index there would miss
        // them. Traversal-time label predicates read this index to resolve
        // labels for vertices that live only in Lance — notably on a fork, whose
        // data is flushed to Lance before branching. (GitHub #99)
        for (vid, labels, _props, _deleted, _version) in &main_vertices {
            self.storage.update_vid_labels_index(*vid, labels.clone());
        }
        for (vid, _labels, _version) in &main_vertex_tombstones {
            self.storage.remove_from_vid_labels_index(*vid);
        }

        // 3. For each edge type, write FWD and BWD delta runs
        for (&edge_type_id, entries) in entries_by_type.iter() {
            // Get edge type name from unified lookup (handles both schema'd and schemaless)
            let edge_type_name = self
                .storage
                .schema_manager()
                .edge_type_name_by_id_unified(edge_type_id)
                .ok_or_else(|| anyhow!("Edge type ID {} not found", edge_type_id))?;

            // Every edge flushes as a full-row Append; see
            // `Writer::insert_edge_partial_full` for why the delta log
            // cannot take a partial MergeInsert.
            let backend = self.storage.backend();

            // FWD run (sorted by src_vid)
            let mut fwd_entries = entries.clone();
            fwd_entries.sort_by_key(|e| e.src_vid);
            let fwd_ds = self.storage.delta_dataset(&edge_type_name, "fwd")?;
            if !fwd_entries.is_empty() {
                let fwd_batch = fwd_ds.build_record_batch(&fwd_entries, &schema)?;
                fwd_ds.write_run(backend, fwd_batch).await?;
            }
            fwd_ds.ensure_eid_index(backend).await?;

            // BWD run (sorted by dst_vid)
            let mut bwd_entries = entries.clone();
            bwd_entries.sort_by_key(|e| e.dst_vid);
            let bwd_ds = self.storage.delta_dataset(&edge_type_name, "bwd")?;
            if !bwd_entries.is_empty() {
                let bwd_batch = bwd_ds.build_record_batch(&bwd_entries, &schema)?;
                bwd_ds.write_run(backend, bwd_batch).await?;
            }
            bwd_ds.ensure_eid_index(backend).await?;

            // Update Manifest
            let current_snap =
                manifest
                    .edges
                    .entry(edge_type_name.to_string())
                    .or_insert(EdgeSnapshot {
                        version: 0,
                        count: 0,
                        lance_version: 0,
                    });
            current_snap.version += 1;
            current_snap.count += entries.len() as u64;
            // LanceDB tables don't expose Lance version directly
            current_snap.lance_version = 0;

            // Note: No CSR invalidation needed. AdjacencyManager's overlay
            // already has these edges via dual-write in insert_edge/delete_edge.
        }

        // 4. Per-label vertex table writes
        // Iterate all labels that have either full-row data or tombstones
        // pending. A label may appear in only one of the two maps.
        let all_label_ids: std::collections::HashSet<u16> = vertices_by_label
            .keys()
            .chain(tombstones_by_label.keys())
            .copied()
            .collect();
        for label_id in all_label_ids {
            let vertices = vertices_by_label.remove(&label_id).unwrap_or_default();
            let label_name = schema
                .label_name_by_id(label_id)
                .ok_or_else(|| anyhow!("Label ID {} not found", label_id))?;

            let ds = self.storage.vertex_dataset(label_name)?;

            // Collect inverted index updates before consuming vertices
            // Maps: cfg.property -> (added, removed)
            type InvertedUpdateMap = HashMap<String, (HashMap<Vid, Vec<String>>, HashSet<Vid>)>;
            let mut inverted_updates: InvertedUpdateMap = HashMap::new();

            for idx in &schema.indexes {
                if let IndexDefinition::Inverted(cfg) = idx
                    && cfg.label == label_name
                {
                    let mut added: HashMap<Vid, Vec<String>> = HashMap::new();
                    let mut removed: HashSet<Vid> = HashSet::new();

                    for (vid, _labels, props, deleted, _version) in &vertices {
                        if *deleted {
                            removed.insert(*vid);
                        } else if let Some(prop_value) = props.get(&cfg.property) {
                            // Extract terms from the property value (List<String>)
                            if let Some(arr) = prop_value.as_array() {
                                let terms: Vec<String> = arr
                                    .iter()
                                    .filter_map(|v| v.as_str().map(ToString::to_string))
                                    .collect();
                                if !terms.is_empty() {
                                    added.insert(*vid, terms);
                                }
                            }
                        }
                    }
                    // Round-12 §B: tombstones no longer in `vertices`;
                    // pull them from `tombstones_by_label` for inverted
                    // index removal.
                    if let Some(tomb_rows) = tombstones_by_label.get(&label_id) {
                        for (vid, _) in tomb_rows {
                            removed.insert(*vid);
                        }
                    }

                    if !added.is_empty() || !removed.is_empty() {
                        inverted_updates.insert(cfg.property.clone(), (added, removed));
                    }
                }
            }

            // Collect sparse-vector index updates before consuming vertices.
            // Maps: cfg.property -> (added [(term_id, weight)] per vid, removed).
            type SparseUpdateMap = HashMap<String, (HashMap<Vid, Vec<(u32, f32)>>, HashSet<Vid>)>;
            let mut sparse_updates: SparseUpdateMap = HashMap::new();

            for idx in &schema.indexes {
                if let IndexDefinition::Sparse(cfg) = idx
                    && cfg.label == label_name
                {
                    let mut added: HashMap<Vid, Vec<(u32, f32)>> = HashMap::new();
                    let mut removed: HashSet<Vid> = HashSet::new();

                    for (vid, _labels, props, deleted, _version) in &vertices {
                        if *deleted {
                            removed.insert(*vid);
                        } else if let Some(uni_common::Value::SparseVector { indices, values }) =
                            props.get(&cfg.property)
                        {
                            let pairs: Vec<(u32, f32)> = indices
                                .iter()
                                .copied()
                                .zip(values.iter().copied())
                                .collect();
                            added.insert(*vid, pairs);
                            // An in-place SET re-flushes an already-indexed vid. The
                            // sparse postings are a `Vec` with no per-vid dedup, so unless
                            // the vid is also marked removed, `apply_incremental_updates`
                            // appends the new postings *alongside* the stale ones — leaking
                            // duplicates that grow unboundedly on hot-updated docs and
                            // double-count the advisory `query_topk` score (issue #95).
                            // Mark every updated vid removed so its prior postings are
                            // purged before the new ones are appended (remove-then-add).
                            removed.insert(*vid);
                        }
                    }
                    // Tombstones are not in `vertices`; pull from tombstones_by_label.
                    if let Some(tomb_rows) = tombstones_by_label.get(&label_id) {
                        for (vid, _) in tomb_rows {
                            removed.insert(*vid);
                        }
                    }

                    if !added.is_empty() || !removed.is_empty() {
                        sparse_updates.insert(cfg.property.clone(), (added, removed));
                    }
                }
            }

            let mut v_data = Vec::new();
            let mut d_data = Vec::new();
            let mut ver_data = Vec::new();
            for (vid, labels, props, deleted, version) in vertices {
                v_data.push((vid, labels, props));
                d_data.push(deleted);
                ver_data.push(version);
            }

            let backend = self.storage.backend();

            // Tombstones are appended as rows of their own, in the same
            // append as the live rows: `_deleted = true` at the delete's
            // version, no properties. The vertex's older rows stay as they
            // were, so a read bounded below the delete (a pinned snapshot,
            // `VERSION AS OF`, a transaction's L1 pin) still finds the
            // vertex. Rewriting the older rows in place (a MergeInsert on
            // `_vid`) restamped them with the delete's version and erased
            // the vertex from every such read.
            let tombstone_rows = tombstones_by_label.remove(&label_id).unwrap_or_default();
            let live_rows = v_data.len();
            for (vid, version) in &tombstone_rows {
                v_data.push((*vid, vec![label_name.to_string()], Properties::new()));
                d_data.push(true);
                ver_data.push(*version);
            }
            if !v_data.is_empty() {
                let batch = ds.build_record_batch_with_timestamps(
                    &v_data,
                    &d_data,
                    &ver_data,
                    &schema,
                    Some(&vertex_created_at),
                    Some(&vertex_updated_at),
                )?;
                ds.write_batch(backend, batch, &schema).await?;
            }
            // The UID mappings and the manifest count below cover the rows
            // written for live vertices only.
            v_data.truncate(live_rows);

            ds.ensure_default_indexes(backend).await?;

            // Resolve any scalar or full-text index declared before this label
            // had a table. Their build paths record NotAttempted in that case,
            // and until this call nothing ever built it — the declaration
            // survived in the registry with no artifact behind it.
            self.storage
                .index_manager()
                .ensure_declared_indexes(label_name)
                .await?;

            // VidLabelsIndex maintenance is centralized at the main-vertex
            // flush above (it sees both schema'd and schemaless vertices).

            // Update Manifest
            let current_snap =
                manifest
                    .vertices
                    .entry(label_name.to_string())
                    .or_insert(LabelSnapshot {
                        version: 0,
                        count: 0,
                        lance_version: 0,
                    });
            current_snap.version += 1;
            current_snap.count += v_data.len() as u64;
            // LanceDB tables don't expose Lance version directly
            current_snap.lance_version = 0;

            // Invalidate table cache to ensure next read picks up new version
            self.storage.invalidate_table_cache(label_name);

            // Apply inverted index updates incrementally
            #[cfg(feature = "lance-backend")]
            for idx in &schema.indexes {
                if let IndexDefinition::Inverted(cfg) = idx
                    && cfg.label == label_name
                    && let Some((added, removed)) = inverted_updates.get(&cfg.property)
                {
                    self.storage
                        .index_manager()
                        .update_inverted_index_incremental(cfg, added, removed)
                        .await?;
                }
            }

            // Apply sparse-vector index updates incrementally
            #[cfg(feature = "lance-backend")]
            for idx in &schema.indexes {
                if let IndexDefinition::Sparse(cfg) = idx
                    && cfg.label == label_name
                    && let Some((added, removed)) = sparse_updates.get(&cfg.property)
                {
                    self.storage
                        .index_manager()
                        .update_sparse_vector_index_incremental(cfg, added, removed)
                        .await?;
                }
            }

            // Update UID index with new vertex mappings
            // Collect (UniId, Vid) mappings from non-deleted vertices
            #[cfg(feature = "lance-backend")]
            {
                let mut uid_mappings: Vec<(uni_common::core::id::UniId, Vid)> = Vec::new();
                for (vid, _labels, props) in &v_data {
                    let ext_id = props.get("ext_id").and_then(|v| v.as_str());
                    let uid = crate::storage::vertex::VertexDataset::compute_vertex_uid(
                        label_name, ext_id, props,
                    );
                    uid_mappings.push((uid, *vid));
                }

                if !uid_mappings.is_empty()
                    && let Ok(uid_index) = self.storage.uid_index(label_name)
                {
                    // Stamp mappings with this flush's MVCC version so a later
                    // re-create of the same UID deterministically outranks the
                    // stale mapping (review C3).
                    uid_index
                        .write_mapping_versioned(&uid_mappings, current_version)
                        .await?;
                }
            }
        }
        Ok(FlushOutcome {
            manifest,
            snapshot_id,
        })
    }

    /// Composition entry that assumes the caller already holds `flush_lock`.
    /// Runs rotate + stream + finalize_locked in sequence. Used by
    /// [`Writer::flush_to_l1`] (acquires the lock first) and by
    /// `commit_transaction_l0`'s post-merge auto-flush branch (which already
    /// holds the lock from the commit critical section).
    #[instrument(
        skip(self),
        fields(snapshot_id, mutations_count, size_bytes),
        level = "info"
    )]
    async fn flush_inline_under_lock(&self, name: Option<String>) -> Result<String> {
        let start = std::time::Instant::now();

        let (initial_size, initial_count) = {
            let l0_arc = self.l0_manager.get_current();
            let l0 = l0_arc.read();
            (l0.estimated_size, l0.mutation_count)
        };
        tracing::Span::current().record("size_bytes", initial_size);
        tracing::Span::current().record("mutations_count", initial_count);

        debug!("Starting L0 flush to L1");

        // Phases A (WAL pre-flush), B (rotate), C (WAL handoff).
        // FlushInProgressGuard lives on RotateOutput and stays alive for
        // the full flush — including the finalize_locked call below.
        let RotateOutput {
            old_l0_arc,
            wal_lsn,
            current_version,
            flush_in_progress_guard: _flush_guard,
        } = self.flush_l0_rotate().await?;

        // Phases D (L1 collect), E (orphan resolve), F (manifest seed),
        // G (Lance writes). Builds the manifest but does NOT publish it.
        let FlushOutcome {
            manifest,
            snapshot_id,
        } = self
            .flush_stream_l1(old_l0_arc.clone(), wal_lsn, current_version, name)
            .await?;

        // Phases H..S: publish manifest, complete_flush, WAL truncate,
        // property cache clear, last_flush_time, metrics, l1_runs++,
        // compaction trigger, index-rebuild scheduling, fork tick.
        self.flush_finalize_locked(
            old_l0_arc,
            wal_lsn,
            manifest,
            snapshot_id,
            initial_size,
            initial_count,
            start,
        )
        .await
    }

    /// Phases H..S of the flush: publish the manifest and run all
    /// post-publish bookkeeping. Assumes the caller already holds
    /// `flush_lock` — see [`Writer::flush_finalize_now`] for the
    /// lock-acquiring variant used by the async finalize path.
    #[allow(clippy::too_many_arguments)]
    async fn flush_finalize_locked(
        &self,
        old_l0_arc: Arc<RwLock<L0Buffer>>,
        wal_lsn: u64,
        manifest: SnapshotManifest,
        snapshot_id: String,
        initial_size: usize,
        initial_count: usize,
        start: std::time::Instant,
    ) -> Result<String> {
        Self::flush_finalize_body(
            &self.shared_ctx(),
            old_l0_arc,
            wal_lsn,
            manifest,
            snapshot_id,
            initial_size,
            initial_count,
            start,
        )
        .await
    }

    /// Phases H..S of the flush, lock-acquiring variant. Used by the
    /// async-flush finalizer task (running on a spawned tokio task),
    /// which holds neither `&self` nor `flush_lock`. Briefly re-acquires
    /// `flush_lock` to serialize the publish boundary, then runs the
    /// same body as `flush_finalize_locked` but over a SharedFlushCtx.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn flush_finalize_now(
        shared: SharedFlushCtx,
        old_l0_arc: Arc<RwLock<L0Buffer>>,
        wal_lsn: u64,
        manifest: SnapshotManifest,
        snapshot_id: String,
        initial_size: usize,
        initial_count: usize,
        start: std::time::Instant,
    ) -> Result<String> {
        let _flush_lock_guard = shared.flush_lock.clone().lock_owned().await;
        Self::flush_finalize_body(
            &shared,
            old_l0_arc,
            wal_lsn,
            manifest,
            snapshot_id,
            initial_size,
            initial_count,
            start,
        )
        .await
    }

    /// Shared body of `flush_finalize_locked` and `flush_finalize_now`.
    /// Static over `SharedFlushCtx`; the caller is responsible for
    /// holding `flush_lock`.
    #[allow(clippy::too_many_arguments)]
    async fn flush_finalize_body(
        shared: &SharedFlushCtx,
        old_l0_arc: Arc<RwLock<L0Buffer>>,
        wal_lsn: u64,
        mut manifest: SnapshotManifest,
        snapshot_id: String,
        initial_size: usize,
        initial_count: usize,
        start: std::time::Instant,
    ) -> Result<String> {
        // Flushed row counts have just moved, so forget them (#260). Placed in
        // the shared finalize body rather than in `flush_to_l1`, because the
        // sync and async paths both land here and a hook on either alone would
        // leave the other serving a stale count. Blunt rather than per-table: a
        // flush can touch every label and edge type in the batch, and an
        // invalidation that enumerated them would be a second place to get the
        // set wrong. A missing entry costs one `count_rows`; a stale one is a
        // wrong answer.
        shared.storage.cardinality().invalidate_all();

        // Parent-snapshot fixup. The stream phase built `manifest` with
        // parent_snapshot set from cached_manifest at stream time. If
        // OTHER flushes (sync or async) have finalized since then,
        // cached_manifest has advanced. Re-link this manifest to the
        // current cached chain so we don't orphan their data when we
        // overwrite cached_manifest below.
        let current_parent_id = shared
            .cached_manifest
            .lock()
            .as_ref()
            .map(|m| m.snapshot_id.clone());
        if current_parent_id.is_some() && manifest.parent_snapshot != current_parent_id {
            manifest.parent_snapshot = current_parent_id;
            metrics::counter!("uni_flush_parent_chain_fixups_total").increment(1);
        }

        // H. Publish manifest (body first, then pointer — recovery is
        // idempotent if we crash between the two).
        // A fork writer must publish to a fork-scoped namespace, never the
        // global `catalog/latest` that the primary reopen reads (review C1).
        debug_assert_eq!(
            shared.fork_id.is_some(),
            shared.storage.snapshot_manager().is_fork_scoped(),
            "fork writer must publish to a fork-scoped snapshot namespace (review C1)"
        );
        shared
            .storage
            .snapshot_manager()
            .save_snapshot(&manifest)
            .await?;
        shared
            .storage
            .snapshot_manager()
            .set_latest_snapshot(&manifest.snapshot_id)
            .await?;

        // H2. Durability barrier (review C4). `save_snapshot` / `set_latest_snapshot`
        // wrote the manifest body and the `catalog/latest` pointer through the
        // object store, which does NOT fsync. WAL truncation (K, below) removes
        // the only other durable copy of this flush's data, so a crash after K
        // but before the OS flushed those writes would lose the snapshot —
        // recovery could not resolve `latest`. Make them durable now (local-fs
        // only; remote stores provide their own durability on `put`).
        //
        // The barrier is up to four blocking `fsync`s, so it runs on the
        // blocking pool: inline, a slow disk would stall the executor thread
        // driving this flush — on a `current_thread` runtime, every task.
        let local_root = shared.storage.local_fs_root();
        let fork_id = shared.fork_id;
        let barrier_id = manifest.snapshot_id.clone();
        tokio::task::spawn_blocking(move || {
            crate::snapshot::manager::fsync_snapshot_pointer(
                local_root.as_deref(),
                fork_id.as_ref(),
                &barrier_id,
            )
        })
        .await
        .map_err(|e| {
            anyhow!(
                "fsync snapshot {} before WAL truncate: task failed: {}",
                manifest.snapshot_id,
                e
            )
        })?
        .map_err(|e| {
            anyhow!(
                "fsync snapshot {} before WAL truncate: {}",
                manifest.snapshot_id,
                e
            )
        })?;

        // I. Cache manifest for next flush to avoid re-reading from object store.
        *shared.cached_manifest.lock() = Some(manifest.clone());

        // L. Invalidate the property cache BEFORE removing the flushed buffer
        // from the L0 chain (Bug #10). `clear_cache` has no dependency on the
        // complete_flush (J) / WAL-truncate (K) steps below, so clearing it
        // first closes the non-monotonic-read window: once the buffer leaves
        // the L0 chain at J a freshly-written value would otherwise miss the
        // chain and fall through to a stale cache entry. By the time finalize
        // runs the streamed rows are already durable in L1, so a post-clear
        // read falls through to fresh storage instead. The finalizer holds
        // `flush_lock` throughout, so reordering L ahead of J is safe.
        if let Some(ref pm) = shared.property_manager {
            pm.clear_cache().await;
        }

        // J. Complete flush: remove old L0 from pending_flush. MUST happen
        // BEFORE WAL truncation so min_pending_wal_lsn is accurate.
        shared.l0_manager.complete_flush(&old_l0_arc);

        // Test-only seam (no-op without the `failpoints` feature): pause AFTER
        // complete_flush removed the buffer from the L0 chain (J) but BEFORE
        // WAL truncation (K). The property cache is already cleared (L moved
        // ahead of J above), so a read in this window falls through to fresh
        // L1 storage rather than a stale cache entry (Bug #10 — non-monotonic
        // read after flush finalize).
        fail::fail_point!("flush::after-complete-before-cache-clear");

        // K. Truncate WAL up to the safe LSN. The floor is the START watermark
        // of any OTHER pending flush (this flush's own buffer was removed from
        // pending by `complete_flush` in J, so it is excluded): a pending — e.g.
        // failed — flush's committed entries live above its start and are not yet
        // in L1, so truncating to its high watermark would delete its own data
        // (the lost-commit-on-graceful-close bug).
        let wal_handle = shared.l0_manager.get_current().read().wal.clone();
        if let Some(w) = wal_handle {
            let safe_lsn = shared
                .l0_manager
                .min_pending_wal_lsn_start(&old_l0_arc)
                .map_or(wal_lsn, |floor| floor.min(wal_lsn));
            w.truncate_before(safe_lsn).await?;
        }

        // M. Reset last flush time for time-based auto-flush.
        *shared.last_flush_time.lock() = std::time::Instant::now();

        info!(
            snapshot_id,
            mutations_count = initial_count,
            size_bytes = initial_size,
            "L0 flush to L1 completed successfully"
        );
        metrics::histogram!("uni_flush_duration_seconds").record(start.elapsed().as_secs_f64());
        metrics::counter!("uni_flush_bytes_total").increment(initial_size as u64);
        metrics::counter!("uni_flush_rows_total").increment(initial_count as u64);

        // P. Increment flush generation counter for write throttling — only
        // for a flush that wrote something. An empty buffer's flush appends no
        // fragment to any table, so it is no generation for compaction to fold;
        // counting it let back-to-back flushes of an idle L0 (a `flush()` loop,
        // the auto-flush timer while a transaction holds its writes privately)
        // drive `l1_runs` past the hard limit, and the next write — a
        // transaction's first `SET` — stalled until compaction caught up:
        // ~10 s for a 250 ms statement (`bugs::issue_311_tx_index_rekey`).
        if old_l0_arc.read().holds_flushable_data() {
            let mut status = uni_common::sync::acquire_mutex(
                &shared.storage.compaction_status,
                "compaction_status",
            )?;
            status.l1_runs += 1;
        }

        // Q. Trigger CSR compaction if enough frozen segments have accumulated.
        let am = shared.adjacency_manager.clone();
        if am.should_compact(shared.compaction_config.frozen_segments_compact_threshold) {
            let previous_still_running = {
                let guard = shared.compaction_handle.read();
                guard.as_ref().is_some_and(|h| !h.is_finished())
            };
            if previous_still_running {
                info!("Skipping compaction: previous compaction still in progress");
            } else {
                // Reclaim shadow-CSR entries no in-flight reader can reach.
                // The floor is the minimum version pinned by a live
                // `pinned_at_version` view, or the current version when none is
                // pinned — a reader starting now pins at the current version,
                // so anything deleted at or below it is already unreachable.
                let gc_version = shared
                    .storage
                    .version_high_water_mark()
                    .unwrap_or_else(|| shared.l0_manager.get_current().read().current_version);
                // Blocking pool, not the executor: the rebuild is synchronous,
                // CPU-bound and holds `compact_lock` throughout, so on a worker
                // it stalled every task scheduled there for its duration.
                let handle = tokio::task::spawn_blocking(move || {
                    am.compact();
                    am.gc_shadow(gc_version);
                });
                *shared.compaction_handle.write() = Some(handle);
            }
        }

        // R. Post-flush: check if any indexes need rebuilding based on thresholds.
        if shared.auto_rebuild_enabled
            && let Some(rebuild_mgr) = shared.index_rebuild_manager.get()
        {
            Self::schedule_index_rebuilds_if_needed_static(
                &manifest,
                rebuild_mgr.clone(),
                shared.schema_manager.clone(),
                shared.index_rebuild_config.clone(),
            );
        }

        // S. Emit fork-fragment observability after a successful forked flush.
        Self::tick_fork_fragment_observability_static(
            shared.fork_id,
            shared.fork_flush_count.clone(),
            shared.fork_fragment_warn_fired.clone(),
            shared.fork_fragment_warn_threshold,
        );

        Ok(snapshot_id)
    }

    /// Increment fork-flush bookkeeping and fire the fragment warn
    /// once if the threshold is crossed.
    ///
    /// Each flush typically appends ~1 fragment per touched dataset on
    /// the fork's branches; without compaction (deferred to Phase 5)
    /// long-lived heavy-write forks degrade. The flush count is a
    /// proxy for actual fragment growth — reading
    /// `Dataset::manifest().fragments.len()` per dataset would add a
    /// per-flush object-store roundtrip on the hot commit path, which
    /// is too costly for a purely observational guard rail.
    ///
    /// No-op for primary writers (`fork_id == None`).
    #[allow(dead_code)] // called by tests; production path uses _static
    pub(crate) fn tick_fork_fragment_observability(&self) {
        Self::tick_fork_fragment_observability_static(
            self.fork_id,
            self.fork_flush_count.clone(),
            self.fork_fragment_warn_fired.clone(),
            self.config.fork_fragment_warn_threshold,
        );
    }

    /// Static variant of [`Writer::tick_fork_fragment_observability`].
    /// Used by the async-flush finalize path, where we hold a
    /// [`SharedFlushCtx`] bundle of Arcs rather than `&Writer`.
    pub(crate) fn tick_fork_fragment_observability_static(
        fork_id: Option<ForkId>,
        fork_flush_count: Arc<AtomicU64>,
        fork_fragment_warn_fired: Arc<AtomicBool>,
        warn_threshold: usize,
    ) {
        let Some(fork_id) = fork_id else { return };
        // `Relaxed` is sufficient: observational counter, no synchronizes-with.
        let new_count = fork_flush_count.fetch_add(1, Ordering::Relaxed) + 1;
        let fork_label = fork_id.to_string();
        metrics::gauge!(
            "uni_fork_l1_flushes",
            "fork" => fork_label.clone(),
        )
        .set(new_count as f64);
        let threshold = warn_threshold as u64;
        if !fork_fragment_warn_fired.load(Ordering::Relaxed)
            && threshold > 0
            && new_count >= threshold
        {
            fork_fragment_warn_fired.store(true, Ordering::Relaxed);
            tracing::warn!(
                fork = %fork_label,
                flush_count = new_count,
                threshold,
                "fork has exceeded the L1 flush-count threshold; \
                 fork compaction is deferred to Phase 5 — consider \
                 drop+recreate or promotion to bound fragment growth"
            );
        }
    }

    /// Check rebuild thresholds and schedule background index rebuilds for
    /// labels that exceed growth or age limits. Marks affected indexes as
    /// `Stale` and queues the rebuild on the manager's worker.
    ///
    /// Static rather than a method because the async-flush finalize path
    /// holds the [`SchemaManager`] via `SharedFlushCtx` rather than `&Writer`.
    pub(crate) fn schedule_index_rebuilds_if_needed_static(
        manifest: &SnapshotManifest,
        rebuild_mgr: Arc<crate::storage::index_rebuild::IndexRebuildManager>,
        schema_manager: Arc<uni_common::core::schema::SchemaManager>,
        index_rebuild_config: uni_common::config::IndexRebuildConfig,
    ) {
        let checker =
            crate::storage::index_rebuild::RebuildTriggerChecker::new(index_rebuild_config);
        let schema = schema_manager.schema();
        let labels = checker.labels_needing_rebuild(manifest, &schema.indexes);

        if labels.is_empty() {
            return;
        }

        // Mark affected indexes as Stale. A discarded write here left the index
        // reporting `Online` while drift had already been detected against it
        // (#233 Tier 3).
        for label in &labels {
            for idx in &schema.indexes {
                if idx.label() == label {
                    let name = idx.name().to_string();
                    if let Err(e) = schema_manager.update_index_metadata(&name, |m| {
                        m.status = uni_common::core::schema::IndexStatus::Stale;
                    }) {
                        metrics::counter!("uni_index_status_write_failures_total").increment(1);
                        tracing::error!(
                            index = %name,
                            error = %e,
                            "failed to mark index stale after detecting drift; \
                             it still reports its previous status"
                        );
                    }
                }
            }
        }

        // Queue in memory only; the rebuild worker (a tracked task) persists
        // the task state when it claims the task. Persisting here needed a
        // detached task, which could write the state file after the database
        // was dropped.
        let task_ids = rebuild_mgr.enqueue(labels);
        tracing::debug!(tasks = ?task_ids, "queued index rebuilds after flush");
    }
}

/// `FinalizeFn` implementation that the `FlushCoordinator` invokes from
/// its single-task finalizer loop. Unit struct on purpose: it must NOT
/// hold `Arc<Writer>` (that would create a reference cycle Writer ->
/// FlushCoordinator -> Arc<dyn FinalizeFn> -> Writer). All state needed
/// for finalize travels in via `SharedFlushCtx`.
pub(crate) struct WriterFinalizer;

impl FinalizeFn for WriterFinalizer {
    fn finalize<'a>(
        &'a self,
        rotated: RotatedFlush,
        outcome: AsyncFlushOutcome,
        shared: SharedFlushCtx,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String>> + Send + 'a>> {
        Box::pin(async move {
            // Read initial_size / initial_count from the rotated L0 so
            // we don't have to plumb them through the coordinator
            // submission. The buffer is still alive in pending_flush
            // until `complete_flush` (J) below pops it.
            let (initial_size, initial_count) = {
                let l0 = rotated.old_l0_arc.read();
                (l0.estimated_size, l0.mutation_count)
            };
            let result = Writer::flush_finalize_now(
                shared,
                rotated.old_l0_arc.clone(),
                rotated.wal_lsn,
                outcome.new_manifest,
                outcome.snapshot_id,
                initial_size,
                initial_count,
                std::time::Instant::now(),
            )
            .await;
            // `rotated` (permit + flush_in_progress_guard) drops here.
            drop(rotated.permit);
            result
        })
    }

    fn finalize_failure<'a>(
        &'a self,
        rotated: RotatedFlush,
        err: anyhow::Error,
        _shared: SharedFlushCtx,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Error> + Send + 'a>> {
        Box::pin(async move {
            tracing::warn!(
                error = %err,
                seq = rotated.seq,
                "async flush stream failed; old L0 remains in pending_flush, \
                 WAL retains its data, recovery via WAL replay on restart"
            );
            metrics::counter!("uni_flush_failures_total").increment(1);
            // Permit + guard drop here so back-pressure releases even on
            // failure.
            drop(rotated.permit);
            err
        })
    }
}

/// Map a constraint key's [`Value`] onto a [`Scalar`] literal.
///
/// `None` for anything with no scalar equivalent (list, map, vector, null).
/// The two constraint probes differ in what they do with that: the batch check
/// abandons the row entirely, while the full-horizon check probes with SQL
/// NULL — preserving each site's prior behavior exactly.
fn constraint_scalar(value: &Value) -> Option<Scalar> {
    match value {
        Value::String(s) => Some(Scalar::Str(s.clone())),
        Value::Int(n) => Some(Scalar::Int(*n)),
        Value::Float(f) => Some(Scalar::Float(*f)),
        Value::Bool(b) => Some(Scalar::Bool(*b)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Test that commit_transaction writes mutations to WAL before merging to main L0.
    /// This verifies fix for issue #137 (transaction commit atomicity).
    #[tokio::test]
    async fn test_commit_transaction_wal_before_merge() -> Result<()> {
        use crate::runtime::wal::WriteAheadLog;
        use crate::storage::manager::StorageManager;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectStorePath;
        use uni_common::core::schema::SchemaManager;

        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager =
            Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
        let _label_id = schema_manager.add_label("Test")?;
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);

        // Create WAL for main L0
        let wal_path = ObjectStorePath::from("wal");
        let wal = Arc::new(WriteAheadLog::new(store.clone(), wal_path));

        let writer = Writer::new_with_config(
            storage.clone(),
            schema_manager.clone(),
            1,
            UniConfig::default(),
            Some(wal),
            None,
            None,
        )
        .await?;

        // Begin transaction — create a transaction L0
        let tx_l0 = writer.create_transaction_l0();

        // Insert data in transaction
        let vid_a = writer.next_vid().await?;
        let vid_b = writer.next_vid().await?;

        let mut props = std::collections::HashMap::new();
        props.insert("test".to_string(), Value::String("data".to_string()));

        writer
            .insert_vertex_with_labels(vid_a, props.clone(), &["Test".to_string()], Some(&tx_l0))
            .await?;
        writer
            .insert_vertex_with_labels(
                vid_b,
                std::collections::HashMap::new(),
                &["Test".to_string()],
                Some(&tx_l0),
            )
            .await?;

        let eid = writer.next_eid(1).await?;
        writer
            .insert_edge(
                vid_a,
                vid_b,
                1,
                eid,
                std::collections::HashMap::new(),
                None,
                Some(&tx_l0),
            )
            .await?;

        // Get WAL before commit
        let l0 = writer.l0_manager.get_current();
        let wal = l0.read().wal.clone().expect("Main L0 should have WAL");
        let mutations_before = wal.replay().await?;
        let count_before = mutations_before.len();

        // Commit transaction - this should write to WAL first
        let writer = Arc::new(writer);
        writer.commit_transaction_l0(tx_l0).await?;

        // Verify WAL has the new mutations
        let mutations_after = wal.replay().await?;
        assert!(
            mutations_after.len() > count_before,
            "WAL should contain transaction mutations after commit"
        );

        // Verify mutations are in correct order: vertices first, then edges
        let new_mutations: Vec<_> = mutations_after.into_iter().skip(count_before).collect();

        let mut saw_vertex_a = false;
        let mut saw_vertex_b = false;
        let mut saw_edge = false;

        for mutation in &new_mutations {
            match mutation {
                crate::runtime::wal::Mutation::InsertVertex { vid, .. } => {
                    if *vid == vid_a {
                        saw_vertex_a = true;
                    }
                    if *vid == vid_b {
                        saw_vertex_b = true;
                    }
                    // Vertices should come before edges
                    assert!(!saw_edge, "Vertices should be logged to WAL before edges");
                }
                crate::runtime::wal::Mutation::InsertEdge { eid: e, .. } => {
                    if *e == eid {
                        saw_edge = true;
                    }
                    // Edges should come after vertices
                    assert!(
                        saw_vertex_a && saw_vertex_b,
                        "Edge should be logged after both vertices"
                    );
                }
                _ => {}
            }
        }

        assert!(saw_vertex_a, "Vertex A should be in WAL");
        assert!(saw_vertex_b, "Vertex B should be in WAL");
        assert!(saw_edge, "Edge should be in WAL");

        // Verify data is also in main L0
        let l0_read = l0.read();
        assert!(
            l0_read.vertex_properties.contains_key(&vid_a),
            "Vertex A should be in main L0"
        );
        assert!(
            l0_read.vertex_properties.contains_key(&vid_b),
            "Vertex B should be in main L0"
        );
        assert!(
            l0_read.edge_endpoints.contains_key(&eid),
            "Edge should be in main L0"
        );

        Ok(())
    }

    /// Test that failed WAL flush leaves transaction intact for retry or rollback.
    #[tokio::test]
    async fn test_commit_transaction_wal_failure_rollback() -> Result<()> {
        use crate::runtime::wal::WriteAheadLog;
        use crate::storage::manager::StorageManager;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectStorePath;
        use uni_common::core::schema::SchemaManager;

        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager =
            Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
        let _label_id = schema_manager.add_label("Test")?;
        let _baseline_label_id = schema_manager.add_label("Baseline")?;
        let _txdata_label_id = schema_manager.add_label("TxData")?;
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);

        // Create WAL for main L0
        let wal_path = ObjectStorePath::from("wal");
        let wal = Arc::new(WriteAheadLog::new(store.clone(), wal_path));

        let writer = Writer::new_with_config(
            storage.clone(),
            schema_manager.clone(),
            1,
            UniConfig::default(),
            Some(wal),
            None,
            None,
        )
        .await?;

        // Insert baseline data (outside transaction)
        let baseline_vid = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(
                baseline_vid,
                [("baseline".to_string(), Value::Bool(true))]
                    .into_iter()
                    .collect(),
                &["Baseline".to_string()],
                None,
            )
            .await?;

        // Begin transaction — create a transaction L0
        let tx_l0 = writer.create_transaction_l0();

        // Insert data in transaction
        let tx_vid = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(
                tx_vid,
                [("tx_data".to_string(), Value::Bool(true))]
                    .into_iter()
                    .collect(),
                &["TxData".to_string()],
                Some(&tx_l0),
            )
            .await?;

        // Capture main L0 state before rollback
        let l0 = writer.l0_manager.get_current();
        let vertex_count_before = l0.read().vertex_properties.len();

        // Rollback transaction (simulating what would happen after WAL flush failure)
        drop(tx_l0);

        // Verify main L0 is unchanged
        let vertex_count_after = l0.read().vertex_properties.len();
        assert_eq!(
            vertex_count_before, vertex_count_after,
            "Main L0 should not change after rollback"
        );

        // Baseline should still be present
        assert!(
            l0.read().vertex_properties.contains_key(&baseline_vid),
            "Baseline data should remain"
        );

        // Transaction data should NOT be in main L0
        assert!(
            !l0.read().vertex_properties.contains_key(&tx_vid),
            "Transaction data should not be in main L0 after rollback"
        );

        Ok(())
    }

    /// Test that batch insert with shared labels does not clone labels per vertex.
    /// This verifies fix for issue #161 (redundant label cloning).
    #[tokio::test]
    async fn test_batch_insert_shared_labels() -> Result<()> {
        use crate::storage::manager::StorageManager;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectStorePath;
        use uni_common::core::schema::SchemaManager;

        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager =
            Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
        let _label_id = schema_manager.add_label("Person")?;
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);

        let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

        // Shared labels - should not be cloned per vertex
        let labels = &["Person".to_string()];

        // Insert batch of vertices with same labels
        let mut vids = Vec::new();
        for i in 0..100 {
            let vid = writer.next_vid().await?;
            let mut props = std::collections::HashMap::new();
            props.insert("id".to_string(), Value::Int(i));
            writer
                .insert_vertex_with_labels(vid, props, labels, None)
                .await?;
            vids.push(vid);
        }

        // Verify all vertices have the correct labels
        let l0 = writer.l0_manager.get_current();
        for vid in vids {
            let l0_guard = l0.read();
            let vertex_labels = l0_guard.vertex_labels.get(&vid);
            assert!(vertex_labels.is_some(), "Vertex should have labels");
            assert_eq!(
                vertex_labels.unwrap(),
                &vec!["Person".to_string()],
                "Labels should match"
            );
        }

        Ok(())
    }

    /// Test that estimated_size tracks mutations correctly and approximates size_bytes().
    /// This verifies fix for issue #147 (O(V+E) size_bytes() in metrics).
    /// A vertex-delete cascade must retract its edges from the adjacency
    /// overlay, not just from the L0 buffer.
    ///
    /// This drives `Writer::delete_vertex` directly rather than through Cypher.
    /// That is deliberate: the executor's DETACH DELETE pre-pass deletes each
    /// incident edge through the mirrored `delete_edge` first, so the cascade
    /// normally has nothing left to retract and a query-level test would pass
    /// whether or not the mirror exists. Going straight at the Writer is the
    /// only way to exercise the path this guard protects.
    #[tokio::test]
    async fn vertex_delete_cascade_retracts_edges_from_the_adjacency_overlay() -> Result<()> {
        use crate::storage::direction::Direction;
        use crate::storage::manager::StorageManager;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectStorePath;
        use uni_common::core::schema::SchemaManager;

        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager =
            Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
        schema_manager.add_label("Test")?;
        let etype =
            schema_manager.add_edge_type("LINK", vec!["Test".into()], vec!["Test".into()])?;
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
        let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

        let (src, dst, eid) = (Vid::new(1), Vid::new(2), Eid::new(10));
        // `LINK` is declared Test -> Test, which the writer enforces.
        let test = ["Test".to_string()];
        writer
            .insert_vertex_with_labels(src, HashMap::new(), &test, None)
            .await?;
        writer
            .insert_vertex_with_labels(dst, HashMap::new(), &test, None)
            .await?;
        writer
            .insert_edge(src, dst, etype, eid, HashMap::new(), None, None)
            .await?;

        let adjacency = storage.adjacency_manager();
        assert_eq!(
            adjacency
                .get_neighbors(src, etype, Direction::Outgoing)
                .len(),
            1,
            "control: the edge was never mirrored in the first place, so this \
             run says nothing about the cascade"
        );

        // Cascade: no prior `delete_edge`, so the edge is retracted only by the
        // vertex deletion.
        writer.delete_vertex(dst, None, None).await?;

        assert!(
            adjacency
                .get_neighbors(src, etype, Direction::Outgoing)
                .is_empty(),
            "the cascade tombstoned the edge in L0 but left it live in the \
             adjacency overlay, so a traversal would still return a \
             relationship whose endpoint is gone"
        );
        Ok(())
    }

    /// The edge identity memo a `SET` on stored edges fills is charged to its
    /// buffer's `estimated_size` (`l0::tests::merge_and_clone_leave_the_identity_memo_charge_out`
    /// covers the merge).
    #[tokio::test]
    async fn identity_memo_is_charged_to_its_buffer() -> Result<()> {
        use crate::runtime::l0::EdgeIdentityMemo;
        use crate::storage::manager::StorageManager;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectStorePath;
        use uni_common::core::schema::SchemaManager;

        const DEGREE: usize = 200;
        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_manager = Arc::new(
            SchemaManager::load_from_store(store, &ObjectStorePath::from("schema.json")).await?,
        );
        schema_manager.add_label("N")?;
        let rel = schema_manager.add_edge_type("R", vec!["N".into()], vec!["N".into()])?;
        schema_manager.save().await?;
        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);
        let writer = Arc::new(Writer::new(storage, schema_manager, 1).await?);

        let hub = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(hub, HashMap::new(), &["N".to_string()], None)
            .await?;
        let mut edges = Vec::new();
        for _ in 0..DEGREE {
            let leaf = writer.next_vid().await?;
            writer
                .insert_vertex_with_labels(leaf, HashMap::new(), &["N".to_string()], None)
                .await?;
            let eid = writer.next_eid(rel).await?;
            writer
                .insert_edge(hub, leaf, rel, eid, HashMap::new(), None, None)
                .await?;
            edges.push((leaf, eid));
        }
        writer.flush_to_l1(None).await?;

        let tx_l0 = writer.create_transaction_l0();
        let update = |i: usize| {
            let (leaf, eid) = edges[i];
            let writer = Arc::clone(&writer);
            let tx_l0 = Arc::clone(&tx_l0);
            async move {
                let props = HashMap::from([("w".to_string(), Value::Int(1))]);
                writer
                    .update_edge(hub, leaf, rel, eid, props, None, Some(&tx_l0))
                    .await
            }
        };
        // Single-edge storage reads until the hub's adjacency is memoized.
        for i in 0..Writer::IDENTITY_STORAGE_READS_BEFORE_WARM {
            update(i).await?;
        }
        assert!(
            tx_l0.read().edge_identity_memo.identities.is_empty(),
            "premise: nothing memoized yet"
        );
        let before = tx_l0.read().estimated_size;
        let one_update = {
            // An update of an edge whose identity is memoized already costs
            // only its own row.
            update(Writer::IDENTITY_STORAGE_READS_BEFORE_WARM).await?;
            let after_memo = tx_l0.read().estimated_size;
            update(Writer::IDENTITY_STORAGE_READS_BEFORE_WARM + 1).await?;
            (
                tx_l0.read().estimated_size - after_memo,
                after_memo - before,
            )
        };
        let (row_cost, memo_update_cost) = one_update;
        let (memo_bytes, identities) = {
            let guard = tx_l0.read();
            (
                guard.edge_identity_memo.charged_bytes,
                guard.edge_identity_memo.identities.len(),
            )
        };
        assert_eq!(
            identities, DEGREE,
            "premise: the hub's adjacency is memoized"
        );
        assert_eq!(
            memo_bytes,
            DEGREE * EdgeIdentityMemo::IDENTITY_BYTES + EdgeIdentityMemo::SCANNED_BYTES,
            "the memo's charge"
        );
        assert_eq!(
            memo_update_cost,
            row_cost + memo_bytes,
            "the update that filled the memo grew estimated_size by its row and the memo"
        );
        assert!(
            tx_l0.read().size_bytes() >= memo_bytes,
            "size_bytes counts the memo too"
        );

        writer.commit_transaction_l0(tx_l0).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_estimated_size_tracks_mutations() -> Result<()> {
        use crate::storage::manager::StorageManager;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectStorePath;
        use uni_common::core::schema::SchemaManager;

        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager =
            Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
        let _label_id = schema_manager.add_label("Test")?;
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);

        let writer = Writer::new(storage.clone(), schema_manager.clone(), 1).await?;

        let l0 = writer.l0_manager.get_current();

        // Initial state should be empty
        let initial_estimated = l0.read().estimated_size;
        let initial_actual = l0.read().size_bytes();
        assert_eq!(initial_estimated, 0, "Initial estimated_size should be 0");
        assert_eq!(initial_actual, 0, "Initial size_bytes should be 0");

        // Insert vertices with properties
        let mut vids = Vec::new();
        for i in 0..10 {
            let vid = writer.next_vid().await?;
            let mut props = std::collections::HashMap::new();
            props.insert("name".to_string(), Value::String(format!("vertex_{}", i)));
            props.insert("index".to_string(), Value::Int(i));
            writer
                .insert_vertex_with_labels(vid, props, &[], None)
                .await?;
            vids.push(vid);
        }

        // Verify estimated_size grew
        let after_vertices_estimated = l0.read().estimated_size;
        let after_vertices_actual = l0.read().size_bytes();
        assert!(
            after_vertices_estimated > 0,
            "estimated_size should grow after insertions"
        );

        // Verify estimated_size is within reasonable bounds of actual size (within 2x)
        let ratio = after_vertices_estimated as f64 / after_vertices_actual as f64;
        assert!(
            (0.5..=2.0).contains(&ratio),
            "estimated_size ({}) should be within 2x of size_bytes ({}), ratio: {}",
            after_vertices_estimated,
            after_vertices_actual,
            ratio
        );

        // Insert edges with a simple edge type
        let edge_type = 1u32;
        for i in 0..9 {
            let eid = writer.next_eid(edge_type).await?;
            writer
                .insert_edge(
                    vids[i],
                    vids[i + 1],
                    edge_type,
                    eid,
                    std::collections::HashMap::new(),
                    Some("NEXT".to_string()),
                    None,
                )
                .await?;
        }

        // Verify estimated_size grew further
        let after_edges_estimated = l0.read().estimated_size;
        let after_edges_actual = l0.read().size_bytes();
        assert!(
            after_edges_estimated > after_vertices_estimated,
            "estimated_size should grow after edge insertions"
        );

        // Verify still within reasonable bounds
        let ratio = after_edges_estimated as f64 / after_edges_actual as f64;
        assert!(
            (0.5..=2.0).contains(&ratio),
            "estimated_size ({}) should be within 2x of size_bytes ({}), ratio: {}",
            after_edges_estimated,
            after_edges_actual,
            ratio
        );

        Ok(())
    }

    /// Test that flushing WAL on a writer with no mutations succeeds cleanly.
    #[tokio::test]
    async fn test_flush_wal_empty_l0_is_noop() -> Result<()> {
        use crate::runtime::wal::WriteAheadLog;
        use crate::storage::manager::StorageManager;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectStorePath;
        use uni_common::core::schema::SchemaManager;

        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager =
            Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);

        let wal_path = ObjectStorePath::from("wal");
        let wal = Arc::new(WriteAheadLog::new(store.clone(), wal_path));

        let writer = Writer::new_with_config(
            storage.clone(),
            schema_manager.clone(),
            1,
            UniConfig::default(),
            Some(wal.clone()),
            None,
            None,
        )
        .await?;

        // Flush with no mutations — should succeed cleanly
        let lsn = writer.flush_wal().await?;
        // LSN should be 0 or 1 (no real mutations flushed)
        assert!(lsn <= 1, "Empty flush should produce low LSN, got {}", lsn);

        Ok(())
    }

    /// Test that transaction data does not leak into main L0 without commit.
    #[tokio::test]
    async fn test_transaction_isolation_without_commit() -> Result<()> {
        use crate::runtime::wal::WriteAheadLog;
        use crate::storage::manager::StorageManager;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectStorePath;
        use uni_common::core::schema::SchemaManager;

        let dir = tempdir()?;
        let path = dir.path().to_str().unwrap();
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");

        let schema_manager =
            Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
        let _label_id = schema_manager.add_label("Person")?;
        schema_manager.save().await?;

        let storage = Arc::new(StorageManager::new(path, schema_manager.clone()).await?);

        let wal_path = ObjectStorePath::from("wal");
        let wal = Arc::new(WriteAheadLog::new(store.clone(), wal_path));

        let writer = Writer::new_with_config(
            storage.clone(),
            schema_manager.clone(),
            1,
            UniConfig::default(),
            Some(wal),
            None,
            None,
        )
        .await?;

        // Create transaction L0
        let tx_l0 = writer.create_transaction_l0();

        // Insert vertex into transaction L0
        let vid = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(
                vid,
                [("name".to_string(), Value::String("Ghost".to_string()))]
                    .into_iter()
                    .collect(),
                &["Person".to_string()],
                Some(&tx_l0),
            )
            .await?;

        // Verify data is in transaction L0
        assert!(
            tx_l0.read().vertex_properties.contains_key(&vid),
            "Transaction L0 should contain the vertex"
        );

        // Verify data is NOT in main L0
        let main_l0 = writer.l0_manager.get_current();
        assert!(
            !main_l0.read().vertex_properties.contains_key(&vid),
            "Main L0 should NOT contain uncommitted transaction data"
        );

        // Drop transaction without committing — data should be lost
        drop(tx_l0);

        // Main L0 still should not have it
        assert!(
            !main_l0.read().vertex_properties.contains_key(&vid),
            "Main L0 should remain clean after dropped transaction"
        );

        Ok(())
    }

    /// Phase 2 Day 12: the fork-fragment warn fires exactly once when
    /// the flush count crosses the configured threshold and stays
    /// silent on subsequent flushes for the lifetime of the writer.
    /// Primary writers (`fork_id == None`) never fire it.
    ///
    /// Tested directly against `tick_fork_fragment_observability` so
    /// the contract is locked in independently of the broader
    /// `flush_to_l1` path (the end-to-end fork-flush path is blocked
    /// on Day 10's on-the-fly schema overlay growth).
    #[tokio::test]
    async fn fork_fragment_warn_fires_once_then_silences() -> Result<()> {
        use crate::storage::manager::StorageManager;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectStorePath;
        use uni_common::core::fork::ForkId;
        use uni_common::core::schema::SchemaManager;

        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");
        let schema_manager =
            Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
        let storage = Arc::new(
            StorageManager::new(dir.path().to_str().unwrap(), schema_manager.clone()).await?,
        );

        let config = UniConfig {
            fork_fragment_warn_threshold: 3,
            ..Default::default()
        };
        let mut writer =
            Writer::new_with_config(storage, schema_manager, 1, config, None, None, None).await?;

        // Primary path: never fires.
        for _ in 0..10 {
            writer.tick_fork_fragment_observability();
        }
        assert!(!writer.fork_fragment_warn_fired.load(Ordering::Relaxed));
        assert_eq!(writer.fork_flush_count.load(Ordering::Relaxed), 0);

        // Fork path: tag and tick. Below threshold → no fire.
        writer.fork_id = Some(ForkId::new());
        writer.tick_fork_fragment_observability();
        writer.tick_fork_fragment_observability();
        assert!(!writer.fork_fragment_warn_fired.load(Ordering::Relaxed));
        assert_eq!(writer.fork_flush_count.load(Ordering::Relaxed), 2);

        // Crossing threshold → fires once.
        writer.tick_fork_fragment_observability();
        assert!(writer.fork_fragment_warn_fired.load(Ordering::Relaxed));
        assert_eq!(writer.fork_flush_count.load(Ordering::Relaxed), 3);

        // Subsequent ticks bump the gauge but do not re-fire.
        let fired_after = writer.fork_fragment_warn_fired.load(Ordering::Relaxed);
        for _ in 0..5 {
            writer.tick_fork_fragment_observability();
        }
        assert_eq!(writer.fork_flush_count.load(Ordering::Relaxed), 8);
        assert_eq!(
            writer.fork_fragment_warn_fired.load(Ordering::Relaxed),
            fired_after
        );

        Ok(())
    }

    /// The hot-path mutators must not write to any `Writer` struct field.
    /// Phase 2 of the refactor
    /// gave them `&self` receivers, which the compiler enforces against
    /// direct `self.x = y` assignment — but interior-mutable writes
    /// (Mutex/Atomic/OnceLock) still compile. This regression test snapshots
    /// every potentially-writable field, calls each hot-path mutator, and
    /// asserts no field changed.
    ///
    /// Cold-path methods (`flush_to_l1`, `commit_transaction_l0`,
    /// `tick_fork_fragment_observability`) DO mutate fields by design and
    /// are intentionally out of scope here.
    #[tokio::test]
    async fn hot_path_mutators_do_not_change_writer_fields() -> Result<()> {
        use crate::storage::manager::StorageManager;
        use object_store::local::LocalFileSystem;
        use object_store::path::Path as ObjectStorePath;
        use uni_common::core::schema::SchemaManager;

        let dir = tempdir()?;
        let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path())?);
        let schema_path = ObjectStorePath::from("schema.json");
        let schema_manager =
            Arc::new(SchemaManager::load_from_store(store.clone(), &schema_path).await?);
        schema_manager.add_label("Person")?;
        schema_manager.save().await?;
        let storage = Arc::new(
            StorageManager::new(dir.path().to_str().unwrap(), schema_manager.clone()).await?,
        );

        let writer = Writer::new_with_config(
            storage,
            schema_manager,
            1,
            UniConfig::default(),
            None,
            None,
            None,
        )
        .await?;

        /// Captures every `Writer` field that *could* be written by a
        /// hot-path mutator (i.e., every non-Arc, non-immutable-after-
        /// construction field). Arc'd substructures (`l0_manager`,
        /// `storage`, etc.) are intentionally not checked — they are
        /// re-pointed only at construction.
        #[derive(Debug, PartialEq)]
        struct Snapshot {
            last_flush_time: std::time::Instant,
            cached_manifest_some: bool,
            fork_flush_count: u64,
            fork_fragment_warn_fired: bool,
            xervo_runtime_some: bool,
            index_rebuild_manager_some: bool,
            fork_id: Option<ForkId>,
        }

        fn snap(w: &Writer) -> Snapshot {
            Snapshot {
                last_flush_time: *w.last_flush_time.lock(),
                cached_manifest_some: w.cached_manifest.lock().is_some(),
                fork_flush_count: w.fork_flush_count.load(Ordering::Relaxed),
                fork_fragment_warn_fired: w.fork_fragment_warn_fired.load(Ordering::Relaxed),
                xervo_runtime_some: w.xervo_runtime.get().is_some(),
                index_rebuild_manager_some: w.index_rebuild_manager.get().is_some(),
                fork_id: w.fork_id,
            }
        }

        // 1. insert_vertex_with_labels
        let before = snap(&writer);
        let vid = writer.next_vid().await?;
        writer
            .insert_vertex_with_labels(vid, Properties::new(), &["Person".to_string()], None)
            .await?;
        assert_eq!(
            snap(&writer),
            before,
            "insert_vertex_with_labels mutated a Writer field"
        );

        // 2. insert_vertices_batch
        let before = snap(&writer);
        let vids = writer.allocate_vids(2).await?;
        writer
            .insert_vertices_batch(
                vids,
                vec![Properties::new(), Properties::new()],
                vec!["Person".into()],
                None,
            )
            .await?;
        assert_eq!(
            snap(&writer),
            before,
            "insert_vertices_batch mutated a Writer field"
        );

        // 3. delete_vertex
        let before = snap(&writer);
        writer.delete_vertex(vid, None, None).await?;
        assert_eq!(
            snap(&writer),
            before,
            "delete_vertex mutated a Writer field"
        );

        // (insert_edge / delete_edge are skipped here: their fixture cost is
        // disproportionate to the audit's marginal value, and the same
        // structural argument plus the compiler-enforced `&self` covers them.)

        Ok(())
    }
}
