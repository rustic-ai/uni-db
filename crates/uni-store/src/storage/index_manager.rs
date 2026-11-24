// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Index lifecycle management: creation, rebuild, and incremental updates for all index types.

use crate::backend::StorageBackend;
use crate::backend::table_names;
use crate::backend::types::ScanRequest;
#[cfg(feature = "lance-backend")]
use crate::storage::inverted_index::InvertedIndex;
#[cfg(feature = "lance-backend")]
use crate::storage::sparse_index::SparseVectorIndex;
use crate::storage::vertex::VertexDataset;
use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
#[cfg(feature = "lance-backend")]
use std::collections::HashSet;
use std::sync::Arc;
#[cfg(feature = "lance-backend")]
use tracing::{debug, info, instrument, warn};
use uni_common::core::id::Vid;
#[cfg(feature = "lance-backend")]
use uni_common::core::schema::IndexDefinition;
use uni_common::core::schema::SchemaManager;
#[cfg(feature = "lance-backend")]
use uni_common::core::schema::{
    DistanceMetric, FullTextIndexConfig, InvertedIndexConfig, JsonFtsIndexConfig,
    ScalarIndexConfig, ScalarIndexType, SparseVectorIndexConfig, VectorIndexConfig,
    VectorIndexType,
};
use uni_common::vector_index_opts::{AUTO_SUB_VECTORS, default_refine_factor, default_sub_vectors};

/// What happened when a physical index artifact was — or was not — materialized.
///
/// Every `create_*_index` path registers the index definition in the schema even
/// when the backend refuses to build the artifact. That tolerance is deliberate:
/// it lets `CREATE INDEX` succeed before the label has ever been flushed, and it
/// lets a degenerate column through rather than failing a DDL statement.
///
/// The cost used to be paid silently. `IndexMetadata::default()` sets
/// `status: Online` (`uni-common/src/core/schema/index_types.rs:13-17`), so
/// `Online` was what an index got for *existing*, not for being built — and six
/// call sites gate real behaviour on it: `pushdown.rs:166,237,279` and
/// `SchemaManager`'s `vector_index_for_property` / `sparse_index_for_property` /
/// `fulltext_index_for_property`. An index whose build had hard-failed still
/// advertised itself to all six.
///
/// **The tolerance is kept; what changes is that the outcome is recorded.** No
/// build path gains or loses an error return here.
///
/// The scope is deliberately narrow: only a build that was *attempted and
/// refused* changes status. A build that was never attempted stays `Online` for
/// now — see [`BuildOutcome::NotAttempted`], where demoting it was tried,
/// measured against the suite, and backed out.
///
/// # Why a failed build is `Stale` and not `Failed`
///
/// `Failed` is terminal: `index_rebuild.rs:72` skips both `Building` and
/// `Failed` when deciding which labels need a rebuild, on the reasoning that
/// they are "already being rebuilt or have permanently failed". Marking a
/// build error `Failed` would therefore convert a *retryable* failure into a
/// permanent one — and the vector path documents its failures as exactly that
/// (`build_physical_vector_index`: "Lance may clamp or defer ANN training"),
/// expecting the next flush's rebuild to materialize the index.
///
/// `Stale` denies every `Online` gate above, which is the correctness fix, while
/// leaving retry behaviour exactly as it is today. It is also the status the
/// flush-driven rebuild path already assigns (`runtime/writer.rs:6000`).
#[cfg(feature = "lance-backend")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BuildOutcome {
    /// The backend materialized the artifact.
    Built,
    /// The backend was asked and refused. The error is logged at the call site.
    Failed,
    /// No build was attempted — the table is not flushed yet, or there is no
    /// backend. The definition is registered; the artifact does not exist.
    ///
    /// **Currently mapped to `Online`, which is a known gap and not an
    /// endorsement.** Marking it `Stale` is the honest reading, but it is not a
    /// safe change today: `pushdown.rs:166` gates the hash-index point lookup on
    /// `status == Online`, and that optimization needs no physical artifact —
    /// `ScalarIndexType::Hash` has no Lance counterpart (it is mapped to BTree
    /// at `:611-616`), and the `issue_57_match_label_hash_index` tests exercise
    /// it against a label that is declared before any flush and never physically
    /// built at all, since auto-rebuild is off by default. Demoting this case
    /// turns those five tests red by disabling a working optimization.
    ///
    /// Closing the gap means separating "the user declared this index" from "an
    /// artifact exists for it" at the six `Online` gates. That is a wider change
    /// than this one and is deliberately left alone.
    NotAttempted,
    /// No artifact is required for this configuration, and the index is
    /// complete without one.
    ///
    /// The L1/Manhattan vector case: there is no ANN backend metric for L1, so
    /// the build is skipped by design and the column is searched
    /// exact/brute-force. The *definition* is the index, and `vector_search`
    /// reads the metric off it — which means this must stay `Online` or
    /// `SchemaManager::vector_index_for_property` would stop returning it and
    /// L1 vector search would silently stop finding its index.
    NotRequired,
}

/// Whether a build may rewrite the label table to fill a derived column its
/// index reads: a keyed index's key columns, a JSON full-text index's text,
/// a MUVERA index's FDE.
///
/// A rewrite is `WriteMode::Overwrite`, which drops every Lance index on the
/// table. So a build that rewrites restores the label's other indexes
/// afterwards ([`IndexManager::restore_label_indexes`]), and the builds that
/// restore them never rewrite. Neither does a build inside a flush (its
/// commit would join the flush's unit, and the catalog save naming the column
/// would run ahead of the manifest naming the table version) nor one on a fork
/// (a branch rewrite is a delete-all then an append the branch cannot take).
#[cfg(feature = "lance-backend")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fill {
    /// Rewrite the table to fill them, registered or not: a create, an
    /// upgrade, a rebuild's first phase. The rows written while no such
    /// index was declared carry none.
    Rewrite,
    /// A MUVERA create: rewrite only when this build registers the column.
    /// A registered one is kept current by the flush-time materializer.
    IfUnregistered,
    /// Filled by a rebuild's first phase just now: build over them as they
    /// are, without a rewrite.
    Filled,
    /// Never rewrite. An index that needs one to be built (its columns are
    /// unregistered over a flushed table, or it is recorded without the key
    /// its type now has) is left as it is.
    Never,
}

#[cfg(feature = "lance-backend")]
impl BuildOutcome {
    /// The metadata an index carries after this outcome.
    ///
    /// `last_built_at` is stamped only on success, which is what makes `Online`
    /// falsifiable: before this, an index registered by any of the
    /// `IndexManager` paths reported `Online` with `last_built_at: None` and
    /// there was no way to tell it apart from one that had genuinely built.
    pub(crate) fn into_metadata(
        self,
        row_count: Option<u64>,
    ) -> uni_common::core::schema::IndexMetadata {
        use uni_common::core::schema::{FTS_ANALYSIS_REVISION, IndexMetadata, IndexStatus};
        // Whatever the outcome, the index now reflects this build's analysis.
        let analysis_revision = FTS_ANALYSIS_REVISION;
        match self {
            BuildOutcome::Built => IndexMetadata {
                status: IndexStatus::Online,
                last_built_at: Some(Utc::now()),
                // Supplied by the caller (`IndexManager::built_row_count`).
                // This used to be left `None` for the rebuild manager to fill
                // in, but the rebuild manager is not on this path, so it never
                // did — and the growth trigger that reads it silently never
                // fired for anything built here.
                row_count_at_build: row_count,
                analysis_revision,
            },
            // Online, but with no `last_built_at`: nothing was built, and
            // claiming a build timestamp would be the same lie in a new field.
            // `NotRequired` is correct to be Online permanently; `NotAttempted`
            // is Online only because demoting it is not yet safe — see the
            // variant's docs.
            // No count for these: nothing was indexed, and a row count would
            // imply a build that did not happen.
            BuildOutcome::NotRequired | BuildOutcome::NotAttempted => IndexMetadata {
                status: IndexStatus::Online,
                last_built_at: None,
                row_count_at_build: None,
                analysis_revision,
            },
            BuildOutcome::Failed => IndexMetadata {
                status: IndexStatus::Stale,
                last_built_at: None,
                row_count_at_build: None,
                analysis_revision,
            },
        }
    }
}

/// Status of an index rebuild task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexRebuildStatus {
    /// Task is waiting to be processed.
    Pending,
    /// Task is currently being processed.
    InProgress,
    /// Task completed successfully.
    Completed,
    /// Task failed with an error.
    Failed,
}

/// A task representing an index rebuild operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexRebuildTask {
    /// Unique identifier for this task.
    pub id: String,
    /// The label for which indexes are being rebuilt.
    pub label: String,
    /// Current status of the task.
    pub status: IndexRebuildStatus,
    /// When the task was created.
    pub created_at: DateTime<Utc>,
    /// When the task started processing.
    pub started_at: Option<DateTime<Utc>>,
    /// When the task completed (successfully or with failure).
    pub completed_at: Option<DateTime<Utc>>,
    /// Error message if the task failed.
    pub error: Option<String>,
    /// Number of retry attempts.
    pub retry_count: u32,
}

/// Resolves the embedding dimension of a vector or multi-vector property type.
///
/// Recurses through `List(Vector{dim})` (multi-vector / ColBERT) to the inner
/// `Vector{dim}`; returns `None` for non-vector types.
fn resolve_vector_dim(t: &uni_common::DataType) -> Option<usize> {
    match t {
        uni_common::DataType::Vector { dimensions } => Some(*dimensions),
        uni_common::DataType::List(inner) => resolve_vector_dim(inner),
        _ => None,
    }
}

/// Maps a schema [`VectorIndexType`] to the backend's [`VectorIndexParams`].
///
/// The logical MUVERA type is resolved to its `inner` shape by the caller
/// (`create_vector_index_inner`) before reaching here, so a `Muvera` value is a
/// programming error. The `Option<num_partitions>` HNSW default of "auto" is
/// resolved to a single partition, matching the prior raw-`Dataset` mapping.
///
/// # Errors
/// Returns an error if a `Muvera` type reaches this physical-build mapping.
#[cfg(feature = "lance-backend")]
fn to_backend_vector_params(
    metric: DistanceMetric,
    index_type: &VectorIndexType,
) -> Result<crate::backend::types::VectorIndexParams> {
    use crate::backend::types::{VectorIndexKind, VectorIndexParams};
    // `DistanceMetric` here is the schema (uni_common) enum; the backend has its
    // own. Both are `#[non_exhaustive]`, so the catch-all arms are required.
    let backend_metric = match metric {
        DistanceMetric::L2 => crate::backend::types::DistanceMetric::L2,
        DistanceMetric::Cosine => crate::backend::types::DistanceMetric::Cosine,
        DistanceMetric::Dot => crate::backend::types::DistanceMetric::Dot,
        // L1 has no ANN backend; L1 columns are searched exact/brute-force.
        DistanceMetric::L1 => {
            return Err(anyhow!(
                "L1/Manhattan distance does not support an ANN vector index — L1 columns \
                 are searched exact/brute-force. Declare the column without a vector index \
                 (or use cosine/l2/dot if you need ANN)."
            ));
        }
        // Binary metrics apply to `BinaryVector` columns and have no ANN backend
        // here; compute exact distance with `VECTOR_DISTANCE(a, b, 'hamming'|'jaccard')`.
        DistanceMetric::Hamming | DistanceMetric::Jaccard => {
            return Err(anyhow!(
                "{metric:?} distance does not support a vector index — it applies to \
                 BinaryVector columns and is computed exactly via \
                 VECTOR_DISTANCE(a, b, 'hamming'|'jaccard'). Declare the column without a \
                 vector index."
            ));
        }
        other => return Err(anyhow!("Unsupported vector index metric: {:?}", other)),
    };
    let kind = match index_type {
        VectorIndexType::Flat => VectorIndexKind::Flat,
        VectorIndexType::IvfFlat { num_partitions } => VectorIndexKind::IvfFlat {
            num_partitions: *num_partitions,
        },
        VectorIndexType::IvfPq {
            num_partitions,
            num_sub_vectors,
            bits_per_subvector,
        } => VectorIndexKind::IvfPq {
            num_partitions: *num_partitions,
            num_sub_vectors: *num_sub_vectors,
            num_bits: *bits_per_subvector,
        },
        VectorIndexType::IvfSq { num_partitions } => VectorIndexKind::IvfSq {
            num_partitions: *num_partitions,
        },
        VectorIndexType::IvfRq {
            num_partitions,
            num_bits,
        } => VectorIndexKind::IvfRq {
            num_partitions: *num_partitions,
            num_bits: *num_bits,
        },
        VectorIndexType::HnswFlat {
            m,
            ef_construction,
            num_partitions,
        } => VectorIndexKind::HnswFlat {
            m: *m,
            ef_construction: *ef_construction,
            num_partitions: num_partitions.unwrap_or(1),
        },
        VectorIndexType::HnswSq {
            m,
            ef_construction,
            num_partitions,
        } => VectorIndexKind::HnswSq {
            m: *m,
            ef_construction: *ef_construction,
            num_partitions: num_partitions.unwrap_or(1),
        },
        VectorIndexType::HnswPq {
            m,
            ef_construction,
            num_sub_vectors,
            num_partitions,
        } => VectorIndexKind::HnswPq {
            m: *m,
            ef_construction: *ef_construction,
            num_sub_vectors: *num_sub_vectors,
            num_partitions: num_partitions.unwrap_or(1),
        },
        VectorIndexType::Muvera { .. } => {
            return Err(anyhow!(
                "MUVERA must be resolved to its inner index type before the physical build"
            ));
        }
        other => return Err(anyhow!("Unsupported vector index type: {:?}", other)),
    };
    Ok(VectorIndexParams {
        metric: backend_metric,
        kind,
    })
}

/// Manages physical and logical indexes across all vertex datasets.
pub struct IndexManager {
    base_uri: String,
    schema_manager: Arc<SchemaManager>,
    /// Storage backend, when available. Needed only for MUVERA FDE backfill (scan +
    /// `replace_table_atomic`); `None` callers (e.g. some rebuild paths) still build
    /// indexes over already-materialised columns.
    backend: Option<Arc<dyn StorageBackend>>,
    /// The store's read cut, which index datasets this manager writes record
    /// their versions in.
    read_cut: Option<Arc<crate::storage::read_cut::ReadCut>>,
    /// The fork registry, when the store has one: a key column added on
    /// primary widens every live fork branch of the label's table.
    fork_registry: Option<Arc<crate::fork::ForkRegistryHandle>>,
    /// Whether this manager serves a forked session. An index whose build
    /// rewrites the label table (a derived column's backfill) is refused
    /// there: on a fork, a table rewrite is a delete-all then an append on
    /// the branch, which a branch that lacks the new column cannot take.
    forked: bool,
}

impl std::fmt::Debug for IndexManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexManager")
            .field("base_uri", &self.base_uri)
            .finish_non_exhaustive()
    }
}

impl IndexManager {
    /// The row count to stamp on a freshly built index.
    ///
    /// `row_count_at_build` has exactly one consumer: the growth trigger in
    /// `IndexRebuildManager::labels_needing_rebuild`, which is gated on the
    /// value being present. Every `IndexManager` path used to leave it `None`
    /// and defer to the rebuild manager — but `apply()`'s synchronous rebuild
    /// and the flush-time build both come through here and never reach that
    /// manager, so the field stayed empty and the trigger could not fire.
    /// Growth is the only size-based trigger enabled by default
    /// (`growth_trigger_ratio: 0.5`; `max_index_age` is `None`), so an index
    /// built here could grow without bound and never be picked up.
    ///
    /// Counted only for `Built`, and only when a backend is available; a
    /// failure to count is not a build failure, so it warns and leaves the
    /// field empty rather than failing the build.
    #[cfg(feature = "lance-backend")]
    async fn built_row_count(&self, label: &str, outcome: BuildOutcome) -> Option<u64> {
        if outcome != BuildOutcome::Built {
            return None;
        }
        let backend = self.backend.as_ref()?;
        let table = table_names::vertex_table_name(label);
        match backend.count_rows(&table, None).await {
            Ok(count) => Some(count as u64),
            Err(e) => {
                tracing::warn!(
                    %label,
                    %table,
                    error = %e,
                    "index built, but its row count could not be read; the growth \
                     trigger stays disabled for this index"
                );
                None
            }
        }
    }

    /// Create a new `IndexManager` bound to `base_uri` and the given schema, without a
    /// storage backend (MUVERA backfill over pre-existing rows is unavailable).
    pub fn new(base_uri: &str, schema_manager: Arc<SchemaManager>) -> Self {
        Self {
            base_uri: base_uri.to_string(),
            schema_manager,
            backend: None,
            read_cut: None,
            fork_registry: None,
            forked: false,
        }
    }

    /// Mark this manager as serving a forked session (see `forked`).
    #[must_use]
    pub fn with_forked(mut self, forked: bool) -> Self {
        self.forked = forked;
        self
    }

    /// Refuses a table-rewriting index build in a forked session, before
    /// anything is written. Declare the index on primary: a live fork's
    /// branch is widened with the new column, and the fork builds nothing.
    fn refuse_table_rewrite_on_fork(&self, what: &str) -> Result<()> {
        if self.forked {
            return Err(anyhow!(
                "{what} rewrites the label table to fill a derived column, which a forked \
                 session cannot do; declare it on primary, which widens live forks"
            ));
        }
        Ok(())
    }

    /// Widen live fork branches through `registry` when this manager adds a
    /// key column (see [`Self::create_scalar_index`]).
    #[must_use]
    pub fn with_fork_registry(
        mut self,
        registry: Option<Arc<crate::fork::ForkRegistryHandle>>,
    ) -> Self {
        self.fork_registry = registry;
        self
    }

    /// Puts the index datasets this manager writes under the store's cut.
    #[must_use]
    pub fn with_read_cut(mut self, cut: Arc<crate::storage::read_cut::ReadCut>) -> Self {
        self.read_cut = Some(cut);
        self
    }

    /// Attach a storage backend, enabling MUVERA FDE backfill over already-flushed rows.
    pub fn with_backend(mut self, backend: Arc<dyn StorageBackend>) -> Self {
        self.backend = Some(backend);
        self
    }

    /// Serialize writers to a single index's postings dataset.
    ///
    /// The sparse and set-membership inverted indexes persist their postings with an
    /// unconditional `WriteMode::Overwrite`. A DDL `CREATE … INDEX` backfill and a
    /// concurrent flush incremental update both load-modify-overwrite the *same* postings
    /// path; without serialization the second overwrite clobbers the first (a silent lost
    /// update → a vid's posting vanishes from search candidates — issue #95). Keyed by the
    /// postings dataset path, reusing the backend's per-key write-lock map. Returns `None`
    /// when no backend is attached (offline rebuild paths cannot race a flush).
    #[cfg(feature = "lance-backend")]
    async fn postings_write_guard(
        &self,
        postings_path: &str,
    ) -> Option<crate::backend::traits::TableWriteGuard> {
        match self.backend.as_ref() {
            Some(backend) => Some(backend.lock_table_for_write(postings_path).await),
            None => None,
        }
    }

    /// Build and persist an inverted index for set-membership queries.
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self), level = "info")]
    pub async fn create_inverted_index(&self, config: InvertedIndexConfig) -> Result<()> {
        let label = &config.label;
        let property = &config.property;
        info!(
            "Creating Inverted Index '{}' on {}.{}",
            config.name, label, property
        );

        let schema = self.schema_manager.schema();
        if !schema.labels.contains_key(label) {
            return Err(anyhow!("Label '{}' not found", label));
        }

        // Serialize this full-rebuild overwrite against a concurrent flush incremental
        // update of the same postings dataset (issue #95).
        let postings_path = format!("{}/indexes/{}/{}_inverted", self.base_uri, label, property);
        let _postings_guard = self.postings_write_guard(&postings_path).await;

        let mut index =
            InvertedIndex::open(&self.base_uri, config.clone(), self.read_cut.clone(), false)
                .await?;

        // Backfill from the flushed vertex table via the storage backend. The
        // LanceDB-managed table is not at the raw `{base}/vertices_<label>`
        // path a `VertexDataset` open would target, and the backend read is
        // branch-aware. Mirrors the sparse-index backfill. A not-yet-flushed
        // table legitimately yields an empty index, populated on the next flush.
        let table = table_names::vertex_table_name(label);
        if let Some(backend) = self.backend.as_ref() {
            if backend.table_exists(&table).await? {
                let batches = backend.scan(ScanRequest::all(&table)).await?;
                index
                    .build_from_batches(&batches, |n| info!("Indexed {} terms", n))
                    .await?;
            } else {
                debug!(
                    "Table '{}' not flushed yet; creating empty inverted index (populated on flush)",
                    table
                );
            }
        } else {
            warn!(
                "No storage backend available; inverted index '{}' left empty (populated on flush)",
                config.name
            );
        }

        self.schema_manager
            .add_index(IndexDefinition::Inverted(config))?;
        self.schema_manager.save().await?;

        Ok(())
    }

    /// Build and persist a vector (ANN) index on an embedding column. This is the SINGLE
    /// build path every creation surface converges on (Cypher DDL, the
    /// `uni.schema.createIndex` procedure, and the Rust/Python schema builders via
    /// `rebuild`), so dense, native-multivector, and MUVERA indexes behave identically.
    ///
    /// For a `Muvera` index it first prepares the derived FDE column (`prepare_muvera_fde`:
    /// register + one-time backfill), then builds the physical single-vector ANN over that
    /// `__fde_*` column with the **Dot** metric (its inner product approximates MaxSim),
    /// while the persisted config stays the MUVERA one so query routing detects it.
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self), level = "info")]
    pub async fn create_vector_index(&self, config: VectorIndexConfig) -> Result<()> {
        let (label, name) = (config.label.clone(), config.name.clone());
        if self
            .create_vector_index_inner(config, Fill::IfUnregistered)
            .await?
        {
            self.restore_label_indexes(&label, &HashSet::new(), Some(&name), false)
                .await?;
        }
        Ok(())
    }

    /// Like [`Self::create_vector_index`]; `fill` says whether a MUVERA index's
    /// derived FDE column may be (re)materialised by a table rewrite. Returns
    /// whether the table was rewritten.
    ///
    /// [`Fill::Rewrite`] re-materialises it over ALL current rows even if it was already
    /// registered: full rebuilds ([`Self::rebuild_indexes_for_label`], hence
    /// `db.indexes().rebuild()` and the bulk loader's sync index sync, and compaction's
    /// first phase) use it. The flush-time FDE materializer
    /// (`Writer::materialize_fde_columns`) only runs on the tx write path, so after a BULK
    /// load — or any out-of-band table mutation — the newly written rows have no FDE. A
    /// plain create would hit the "already registered" guard and skip the backfill, leaving
    /// those rows invisible to the FDE ANN (silent empty results). A rebuild must therefore
    /// force a fresh backfill; `splice_fde_batch` recomputes every row's FDE deterministically
    /// and overwrites any stale column, so this is idempotent.
    #[cfg(feature = "lance-backend")]
    async fn create_vector_index_inner(
        &self,
        mut config: VectorIndexConfig,
        fill: Fill,
    ) -> Result<bool> {
        // An ANN index is built over a fixed width; without one every build
        // and every query would fail later, far from the declaration.
        match index_build_dim(&self.schema_manager.schema(), &config) {
            Some(dim) if dim > 0 => {}
            Some(_) => {
                return Err(anyhow!(
                    "vector index '{}': {}.{} is declared VECTOR with no dimension; \
                     declare it as VECTOR(N), for example VECTOR(384)",
                    config.name,
                    config.label,
                    config.property
                ));
            }
            None => {
                return Err(anyhow!(
                    "vector index '{}': {}.{} is not a declared VECTOR(N) property",
                    config.name,
                    config.label,
                    config.property
                ));
            }
        }
        let mut rewrote = false;
        let outcome = if let VectorIndexType::Muvera { inner, .. } = &config.index_type {
            // Register + backfill the derived FDE column (forced on a full rebuild).
            match self.prepare_muvera_fde(&config, fill).await? {
                Some(r) => rewrote = r,
                None => {
                    debug!(
                        index = %config.name,
                        "MUVERA index left as is: its FDE column needs a table rewrite"
                    );
                    return Ok(false);
                }
            }
            let inner_cfg = VectorIndexConfig {
                name: config.name.clone(),
                label: config.label.clone(),
                property: crate::storage::muvera_index::fde_derived_column(&config.name),
                index_type: (**inner).clone(),
                metric: DistanceMetric::Dot,
                embedding_config: None,
                metadata: config.metadata.clone(),
                default_refine_factor: config.default_refine_factor,
            };
            self.build_physical_vector_index(&inner_cfg).await?
        } else {
            self.build_physical_vector_index(&config).await?
        };
        let row_count = self.built_row_count(&config.label, outcome).await;
        config.metadata = outcome.into_metadata(row_count);
        // Turn `AUTO_SUB_VECTORS` into a real, dimension-aware value and attach a
        // refine default before the definition is persisted, so a stored schema
        // never contains the sentinel.
        {
            let schema = self.schema_manager.schema();
            let dim = index_build_dim(&schema, &config);
            let fde_dim = crate::storage::muvera_index::fde_spec_for_config(&schema, &config)
                .map(|spec| spec.params.fde_dim());
            resolve_vector_index_defaults(&mut config, dim, fde_dim);
        }
        self.schema_manager
            .add_index(IndexDefinition::Vector(config))?;
        self.schema_manager.save().await?;
        Ok(rewrote)
    }

    /// Prepare a MUVERA index's derived `__fde_*` column: register it as an internal
    /// schema property and, the FIRST time (when it was not already registered), backfill
    /// it over all already-flushed rows via a full table rewrite (scan → splice the FDE
    /// column into the `get_arrow_schema`-sorted position → `replace_table_atomic`),
    /// mirroring the inverted-index "scan all rows at create time" precedent.
    ///
    /// The "already registered" guard makes this cheap on incremental creates: a plain
    /// `create_vector_index` (e.g. when another index on the label is added, or on schema
    /// re-apply) skips the rewrite — on the tx write path the column is kept current by the
    /// flush-time materializer (`Writer::materialize_fde_columns`). [`Fill::Rewrite`]
    /// bypasses that guard for full rebuilds, where the materializer assumption does not
    /// hold (e.g. after a bulk load); see [`Self::create_vector_index_inner`]. No-op for a
    /// non-MUVERA config, an unresolved source dimension, a label with nothing flushed yet,
    /// or when no backend is attached.
    ///
    /// Returns whether the table was rewritten, or `None` when the column is unregistered
    /// over a flushed table and `fill` allows no rewrite: the index cannot be built.
    #[cfg(feature = "lance-backend")]
    async fn prepare_muvera_fde(
        &self,
        config: &VectorIndexConfig,
        fill: Fill,
    ) -> Result<Option<bool>> {
        use crate::storage::muvera_index::fde_spec_for_config;

        let schema = self.schema_manager.schema();
        let Some(spec) = fde_spec_for_config(&schema, config) else {
            return Ok(Some(false));
        };
        spec.params.validate()?;
        if matches!(fill, Fill::Filled | Fill::Never)
            && !schema
                .properties
                .get(&spec.label)
                .is_some_and(|p| p.contains_key(&spec.derived_col))
            && self.label_table_exists(&spec.label).await?
        {
            return Ok(None);
        }

        // Register the derived column. `add_internal_property` is write-lock-guarded and
        // reports whether THIS call inserted it, so two concurrent creates of the same MUVERA
        // index can't both run the (expensive) full-table backfill — only the inserter does.
        let newly_added = self.schema_manager.add_internal_property(
            &spec.label,
            &spec.derived_col,
            uni_common::DataType::Vector {
                dimensions: spec.params.fde_dim(),
            },
            true,
        )?;

        // Backfill when we just registered the column, or when a full rebuild forces it (the
        // flush-time materializer doesn't cover bulk-loaded / out-of-band rows). A plain
        // re-create that finds the column already present relies on that materializer.
        let rewrite = match fill {
            Fill::Rewrite => true,
            Fill::IfUnregistered => newly_added,
            Fill::Filled | Fill::Never => false,
        };
        if !rewrite {
            return Ok(Some(false));
        }

        // Run the backfill; if it FAILS after we just added the column, roll the registration
        // back so the in-memory schema stays consistent with disk and a retry re-adds +
        // re-backfills. Otherwise the retry would see the column registered, skip the
        // backfill, and build the index over an unpopulated FDE column.
        //
        // Crash-window note: the on-disk order is backfill (`replace_table_atomic`) THEN
        // schema save (in `create_vector_index_inner`). A crash in between leaves an orphan
        // `__fde_*` column with no persisted schema entry, which the next create's idempotent
        // rewrite overwrites — self-healing. Persisting the schema first would be worse (a
        // registered column with no data errors reads), and a `Building` marker is not
        // auto-recovered (`labels_needing_rebuild` skips `Building`/`Failed`).
        match self.backfill_fde_column(&spec).await {
            Ok(rewrote) => Ok(Some(rewrote)),
            Err(e) => {
                if newly_added {
                    let _ = self
                        .schema_manager
                        .drop_property(&spec.label, &spec.derived_col);
                }
                Err(e)
            }
        }
    }

    /// Whether `label`'s table exists; `false` with no backend attached.
    #[cfg(feature = "lance-backend")]
    async fn label_table_exists(&self, label: &str) -> Result<bool> {
        match self.backend.as_ref() {
            Some(backend) => {
                backend
                    .table_exists(&table_names::vertex_table_name(label))
                    .await
            }
            None => Ok(false),
        }
    }

    /// Materialize the MUVERA derived FDE column over all currently-flushed rows via a full
    /// table rewrite (scan → recompute each row's FDE → splice into the
    /// `get_arrow_schema`-sorted position → `replace_table_atomic`). No-op (kept registration)
    /// when no backend is attached or the label has nothing flushed yet — create-before-ingest,
    /// where the flush path materializes the column. The caller must have already registered
    /// the derived column in the schema.
    #[cfg(feature = "lance-backend")]
    async fn backfill_fde_column(
        &self,
        spec: &crate::storage::muvera_index::FdeSpec,
    ) -> Result<bool> {
        self.refuse_table_rewrite_on_fork("a multi-vector (MUVERA) index")?;
        use crate::storage::muvera_index::splice_fde_batch;

        let Some(backend) = self.backend.as_ref() else {
            return Ok(false);
        };
        let table = table_names::vertex_table_name(&spec.label);
        // Err propagates (a backend fault must not silently skip the backfill and
        // leave the FDE column NULL); Ok(false) is the create-before-ingest case
        // where the flush path will materialize the column.
        if !backend.table_exists(&table).await? {
            return Ok(false);
        }

        let schema = self.schema_manager.schema();
        let label_id = schema
            .label_id_by_name(&spec.label)
            .ok_or_else(|| anyhow!("MUVERA: label '{}' not found", spec.label))?;
        // Schema already carries the FDE column (registered by the caller) so it's in the
        // arrow schema at the position future flush appends will use.
        let target_schema =
            VertexDataset::new(&self.base_uri, &spec.label, label_id).get_arrow_schema(&schema)?;
        let source_dt = schema
            .properties
            .get(&spec.label)
            .and_then(|p| p.get(&spec.source_prop))
            .map(|m| m.r#type.clone());
        let encoder = uni_common::muvera::FdeEncoder::new(&spec.params)?;

        // Serialize the scan → splice → overwrite against concurrent flush appends.
        // `replace_table_atomic` overwrites the WHOLE vertex table, so a row a flush
        // appends between our scan and our overwrite would be silently dropped —
        // durable loss of committed data (issue #96). The flush append takes this same
        // per-table write lock inside `StorageBackend::write`, so holding it across the
        // read-modify-write makes the two mutually exclusive and guarantees we scan the
        // post-append state. The lock must wrap BOTH the scan and the replace (not just
        // the replace) to close the read→overwrite TOCTOU window.
        let _table_guard = backend.lock_table_for_write(&table).await;

        let batches = backend.scan(ScanRequest::all(&table)).await?;
        let mut new_batches = Vec::with_capacity(batches.len());
        for batch in &batches {
            new_batches.push(splice_fde_batch(
                batch,
                &target_schema,
                spec,
                &encoder,
                source_dt.as_ref(),
            )?);
        }
        backend
            .replace_table_atomic(&table, new_batches, target_schema)
            .await?;
        Ok(true)
    }

    /// Build the physical Lance ANN index described by `config` over `config.property`
    /// with `config.metric`. Does NOT persist the schema index definition — the caller
    /// does, possibly under a different logical config (see MUVERA in
    /// [`Self::create_vector_index`]).
    #[cfg(feature = "lance-backend")]
    async fn build_physical_vector_index(
        &self,
        config: &VectorIndexConfig,
    ) -> Result<BuildOutcome> {
        let label = &config.label;
        let property = &config.property;
        info!(
            "Creating vector index '{}' on {}.{}",
            config.name, label, property
        );

        let schema = self.schema_manager.schema();
        if !schema.labels.contains_key(label) {
            return Err(anyhow!("Label '{}' not found", label));
        }

        // L1/Manhattan has no ANN backend metric — an L1 column is searched
        // exact/brute-force. Skip the physical index build entirely; the caller
        // still persists the config so `vector_search` reads the L1 metric. The
        // declared ANN algorithm is intentionally ignored for L1.
        if matches!(config.metric, DistanceMetric::L1) {
            info!(
                "Vector index '{}' uses L1/Manhattan — no physical ANN index; \
                 searched exact/brute-force",
                config.name
            );
            return Ok(BuildOutcome::NotRequired);
        }

        // Fail fast on an invalid PQ configuration before touching Lance (which
        // would otherwise error opaquely at build time). The embedding dimension
        // comes from the schema property type, recursing `List(Vector{dim})` for
        // multi-vector (ColBERT) columns.
        let prop_dim = schema
            .properties
            .get(label)
            .and_then(|props| props.get(property))
            .and_then(|meta| resolve_vector_dim(&meta.r#type));
        let pq_sub = match &config.index_type {
            VectorIndexType::IvfPq {
                num_sub_vectors, ..
            }
            | VectorIndexType::HnswPq {
                num_sub_vectors, ..
            } => Some(*num_sub_vectors as usize),
            _ => None,
        };
        // Only the realistic misconfiguration (sub-vectors that don't divide a
        // dimension at least as large) is rejected up front. The degenerate
        // `sub > dim` case (e.g. the default 16 on a dim-2 column) is left to Lance,
        // which clamps/defers it — notably so an index can be declared on an empty
        // table before any rows exist.
        if let (Some(dim), Some(sub)) = (prop_dim, pq_sub)
            && sub != 0
            && dim >= sub
            && dim % sub != 0
        {
            return Err(anyhow!(
                "Vector index '{}': PQ num_sub_vectors ({}) must divide the embedding dimension ({})",
                config.name,
                sub,
                dim
            ));
        }

        let params = to_backend_vector_params(config.metric.clone(), &config.index_type)?;
        let table = table_names::vertex_table_name(label);

        let Some(backend) = self.backend.as_ref() else {
            warn!(
                "No storage backend; physical vector index '{}' deferred until a flush",
                config.name
            );
            return Ok(BuildOutcome::NotAttempted);
        };

        // Build only once the table is flushed; create-before-flush is a no-op
        // here and is materialized by the next flush's rebuild. A build failure
        // on a tiny/degenerate column is tolerated (Lance may clamp or defer ANN
        // training) — the schema definition is still persisted by the caller.
        if backend.table_exists(&table).await? {
            info!(
                "Building physical vector index '{}' on '{}'",
                config.name, table
            );
            if let Err(e) = backend
                .create_vector_index(&table, property, &config.name, params)
                .await
            {
                warn!(
                    "Failed to build physical vector index '{}' on '{}': {}",
                    config.name, table, e
                );
                Ok(BuildOutcome::Failed)
            } else {
                info!("Vector index '{}' created", config.name);
                Ok(BuildOutcome::Built)
            }
        } else {
            debug!(
                "Label '{}' not flushed yet; physical vector index '{}' deferred",
                label, config.name
            );
            Ok(BuildOutcome::NotAttempted)
        }
    }

    /// Build and persist a scalar (BTree) index for exact-match and range queries.
    ///
    /// A property whose type is indexed through a key (see
    /// [`uni_common::core::schema::IndexKey`]) is built by
    /// [`Self::create_keyed_scalar_index`].
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self), level = "info")]
    pub async fn create_scalar_index(&self, config: ScalarIndexConfig) -> Result<()> {
        let (label, name) = (config.label.clone(), config.name.clone());
        if self
            .create_scalar_index_inner(config, Fill::Rewrite)
            .await?
        {
            self.restore_label_indexes(&label, &HashSet::new(), Some(&name), false)
                .await?;
        }
        Ok(())
    }

    /// [`Self::create_scalar_index`]; `fill` says whether a keyed index's key
    /// columns may be re-derived by a table rewrite ([`Fill::Rewrite`]: a
    /// create, an upgrade, a rebuild; rows written while no such index was
    /// declared carry no key). Returns whether the table was rewritten, which
    /// dropped the label's other Lance indexes.
    #[cfg(feature = "lance-backend")]
    async fn create_scalar_index_inner(
        &self,
        mut config: ScalarIndexConfig,
        fill: Fill,
    ) -> Result<bool> {
        // Recorded without the key its type now has: an upgrade, which
        // re-derives the key whatever is registered.
        let recorded_keyed = config.key.is_some();
        info!(
            "Creating scalar index '{}' on {}.{:?}",
            config.name, config.label, config.properties
        );

        let schema = self.schema_manager.schema();
        if !schema.labels.contains_key(&config.label) {
            return Err(anyhow!("Label '{}' not found", config.label));
        }
        // The key is chosen from the declared type (an index recorded before
        // its type had a key gets one here, at its next build). A column type
        // the index kind cannot index (#356): a new index is refused before
        // anything is built; one already in the catalog is recorded `Failed`,
        // which neither the flush nor the rebuild checker retries, rather than
        // attempted (and logged) at every flush.
        if let Some(clash) = schema.scalar_index_name_clash(&config) {
            return Err(anyhow!("Cannot create index '{}': {clash}", config.name));
        }
        if let Err(reason) = schema.resolve_scalar_index(&mut config) {
            if !schema.records_scalar_index(&config) {
                return Err(anyhow!("Cannot create index '{}': {reason}", config.name));
            }
            warn!(index = %config.name, "{reason}; the index is recorded Failed");
            config.metadata = uni_common::core::schema::IndexMetadata {
                status: uni_common::core::schema::IndexStatus::Failed,
                ..Default::default()
            };
            self.schema_manager
                .add_index(IndexDefinition::Scalar(config))?;
            self.schema_manager.save().await?;
            return Ok(false);
        }
        if config.key.is_some() {
            return self
                .create_keyed_scalar_index(config, fill, !recorded_keyed)
                .await;
        }
        let label = &config.label;
        let properties = &config.properties;

        let columns: Vec<&str> = properties.iter().map(|s| s.as_str()).collect();
        // Map the schema scalar type to the backend's; anything other than the
        // explicit Bitmap/LabelList falls back to BTree (matching the prior
        // `ScalarIndexParams::default()`).
        let backend_idx_type = match config.index_type {
            ScalarIndexType::Bitmap => crate::backend::types::ScalarIndexType::Bitmap,
            ScalarIndexType::LabelList => crate::backend::types::ScalarIndexType::LabelList,
            _ => crate::backend::types::ScalarIndexType::BTree,
        };
        let table = table_names::vertex_table_name(label);

        let outcome = if let Some(backend) = self.backend.as_ref() {
            if backend.table_exists(&table).await? {
                info!(
                    "Building physical scalar index '{}' on '{}'",
                    config.name, table
                );
                if let Err(e) = backend
                    .create_scalar_index(&table, &columns, backend_idx_type, Some(&config.name))
                    .await
                {
                    // The error is reported as-is. It previously read "(table
                    // may be empty)", which guessed at a cause and guessed
                    // wrong for the reachable case — a column that does not
                    // exist, which Lance rejects with exactly that message.
                    warn!(
                        "Failed to build physical scalar index '{}' on '{}': {}",
                        config.name, table, e
                    );
                    BuildOutcome::Failed
                } else {
                    info!("Scalar index '{}' created", config.name);
                    BuildOutcome::Built
                }
            } else {
                debug!(
                    "Label '{}' not flushed yet; physical scalar index '{}' deferred",
                    label, config.name
                );
                BuildOutcome::NotAttempted
            }
        } else {
            warn!(
                "No storage backend; physical scalar index '{}' deferred until a flush",
                config.name
            );
            BuildOutcome::NotAttempted
        };
        // Test-only seam (no-op without the `failpoints` feature): the
        // artifact is committed and the catalog does not yet record it.
        fail::fail_point!("index_manager::scalar-built");
        let row_count = self.built_row_count(&config.label, outcome).await;
        config.metadata = outcome.into_metadata(row_count);

        self.schema_manager
            .add_index(IndexDefinition::Scalar(config))?;
        self.schema_manager.save().await?;

        Ok(false)
    }

    /// Whether every key column of `key` is registered on `label`.
    #[cfg(feature = "lance-backend")]
    fn key_registered(&self, label: &str, key: &uni_common::core::schema::IndexKey) -> bool {
        let schema = self.schema_manager.schema();
        let props = schema.properties.get(label);
        key.columns
            .iter()
            .all(|c| props.is_some_and(|p| p.contains_key(c)))
    }

    /// Fill a keyed index's key columns, storage first, then the catalog.
    ///
    /// 1. When `rewrite` and the label's table exists, rewrite it with the
    ///    key columns derived from every row ([`Self::backfill_key_columns`]).
    /// 2. Register each key column as an internal property, so every later
    ///    batch build fills it (`derived_rows`).
    /// 3. Widen live fork branches of the table with the key columns, all
    ///    NULL. A branch is never backfilled, and no key is pushed down on a
    ///    fork's view.
    ///
    /// Returns whether the table was rewritten.
    #[cfg(feature = "lance-backend")]
    async fn fill_key_columns(&self, config: &ScalarIndexConfig, rewrite: bool) -> Result<bool> {
        let key = config
            .key
            .clone()
            .ok_or_else(|| anyhow!("index '{}' has no key", config.name))?;
        let label = &config.label;
        let table = table_names::vertex_table_name(label);
        let registered = self.key_registered(label, &key);
        let rewrote = rewrite
            && self.label_table_exists(label).await?
            && self.backfill_key_columns(config).await?;
        let types = key.kind.column_types();
        for (column, ty) in key.columns.iter().zip(&types) {
            self.schema_manager
                .add_internal_property(label, column, ty.clone(), true)?;
        }
        if !registered && let Some(backend) = self.backend.as_ref() {
            let fields: Vec<arrow_schema::Field> = key
                .columns
                .iter()
                .zip(&types)
                .map(|(c, ty)| arrow_schema::Field::new(c, ty.to_arrow(), true))
                .collect();
            crate::storage::manager::StorageManager::widen_fork_branches(
                self.fork_registry.as_ref(),
                backend.as_ref(),
                std::slice::from_ref(&table),
                &fields,
            )
            .await;
        }
        Ok(rewrote)
    }

    /// Build a keyed scalar index: fill its key columns as `fill` allows
    /// ([`Self::fill_key_columns`]), build one BTree per key column, named
    /// `<index>__<role>`, and record the index, which publishes the cut.
    ///
    /// `upgrade` is an index recorded without the key its type now has. Its
    /// key is always re-derived, never trusted: columns registered before a
    /// downgrade were not kept current while it was unkeyed. Under
    /// [`Fill::Never`] an index that needs a rewrite (an upgrade, or key
    /// columns unregistered over a flushed table) is left as it is.
    ///
    /// Returns whether the table was rewritten.
    #[cfg(feature = "lance-backend")]
    async fn create_keyed_scalar_index(
        &self,
        mut config: ScalarIndexConfig,
        fill: Fill,
        upgrade: bool,
    ) -> Result<bool> {
        let key = config
            .key
            .clone()
            .ok_or_else(|| anyhow!("index '{}' has no key", config.name))?;
        let label = config.label.clone();
        let table = table_names::vertex_table_name(&label);
        let table_exists = self.label_table_exists(&label).await?;
        let rewrite = match fill {
            Fill::Rewrite | Fill::IfUnregistered => true,
            Fill::Filled => false,
            Fill::Never => {
                if table_exists && (upgrade || !self.key_registered(&label, &key)) {
                    debug!(
                        index = %config.name,
                        "keyed index left as is: its key needs a table rewrite, which runs \
                         at the next writable open or an explicit rebuild"
                    );
                    return Ok(false);
                }
                false
            }
        };
        let rewrote = self.fill_key_columns(&config, rewrite).await?;

        let outcome = match self.backend.as_ref() {
            Some(backend) if table_exists => {
                let mut outcome = BuildOutcome::Built;
                for (column, index_name) in key.columns.iter().zip(config.physical_index_names()) {
                    info!(
                        "Building key index '{}' on '{}.{}' for '{}'",
                        index_name, table, column, config.name
                    );
                    if let Err(e) = backend
                        .create_scalar_index(
                            &table,
                            &[column.as_str()],
                            crate::backend::types::ScalarIndexType::BTree,
                            Some(&index_name),
                        )
                        .await
                    {
                        warn!(
                            "Failed to build key index '{}' on '{}': {}",
                            index_name, table, e
                        );
                        outcome = BuildOutcome::Failed;
                        break;
                    }
                }
                outcome
            }
            Some(_) => {
                debug!(
                    "Label '{}' not flushed yet; keyed scalar index '{}' deferred",
                    label, config.name
                );
                BuildOutcome::NotAttempted
            }
            None => {
                warn!(
                    "No storage backend; keyed scalar index '{}' deferred until a flush",
                    config.name
                );
                BuildOutcome::NotAttempted
            }
        };
        fail::fail_point!("index_manager::scalar-built");
        let row_count = self.built_row_count(&config.label, outcome).await;
        config.metadata = outcome.into_metadata(row_count);
        self.schema_manager
            .add_index(IndexDefinition::Scalar(config))?;
        self.schema_manager.save().await?;
        Ok(rewrote)
    }

    /// Rewrites the label's flushed rows with a keyed index's key columns
    /// derived from each row's property value.
    ///
    /// Like [`Self::backfill_json_fts_column`]: reads the newest versions (the
    /// head backend) under the table's write lock, so a flush cannot append
    /// between the scan and the rewrite, and targets the live schema plus the
    /// key columns, which the caller registers only after this succeeds.
    #[cfg(feature = "lance-backend")]
    async fn backfill_key_columns(&self, config: &ScalarIndexConfig) -> Result<bool> {
        self.refuse_table_rewrite_on_fork(&format!("index '{}'", config.name))?;
        use arrow_array::{Array, ArrayRef};

        let (Some(backend), Some(key), [property]) = (
            self.backend.as_ref(),
            config.key.as_ref(),
            config.properties.as_slice(),
        ) else {
            return Ok(false);
        };
        let label = &config.label;
        let table = table_names::vertex_table_name(label);
        let types = key.kind.column_types();
        // The column the key is derived from (`m` for a key path `m.k`).
        let property = key.kind.source_property(property);

        let mut widened = (*self.schema_manager.schema()).clone();
        let version = widened.schema_version;
        let entry = widened.properties.entry(label.clone()).or_default();
        for (column, ty) in key.columns.iter().zip(&types) {
            entry
                .entry(column.clone())
                .or_insert_with(|| uni_common::core::schema::PropertyMeta {
                    r#type: ty.clone(),
                    nullable: true,
                    added_in: version,
                    state: uni_common::core::schema::SchemaElementState::Active,
                    generation_expression: None,
                    description: None,
                    default: None,
                });
        }
        let label_id = widened
            .label_id_by_name(label)
            .ok_or_else(|| anyhow!("keyed index: label '{label}' not found"))?;
        let target_schema =
            VertexDataset::new(&self.base_uri, label, label_id).get_arrow_schema(&widened)?;
        let source_dt = widened
            .properties
            .get(label)
            .and_then(|p| p.get(property))
            .map(|m| m.r#type.clone());

        let _table_guard = backend.lock_table_for_write(&table).await;
        let batches = backend.scan(ScanRequest::all(&table)).await?;
        let mut rewritten = Vec::with_capacity(batches.len());
        for batch in &batches {
            let mut per_column: Vec<Vec<Option<crate::storage::index_keys::KeyValue>>> =
                vec![Vec::with_capacity(batch.num_rows()); key.columns.len()];
            let source = batch.column_by_name(property);
            // A deleted row's slot holds a placeholder, not a value (a BTIC
            // tombstone is 24 zero bytes, which do not decode): its key is
            // NULL, as the batch builder leaves it.
            let deleted = batch.column_by_name("_deleted").and_then(|c| {
                c.as_any()
                    .downcast_ref::<arrow_array::BooleanArray>()
                    .cloned()
            });
            for row in 0..batch.num_rows() {
                let is_deleted = deleted
                    .as_ref()
                    .is_some_and(|d| !d.is_null(row) && d.value(row));
                let value = match source {
                    Some(src) if !is_deleted && !src.is_null(row) => {
                        Some(crate::storage::arrow_convert::arrow_to_value(
                            src.as_ref(),
                            row,
                            source_dt.as_ref(),
                        )?)
                    }
                    _ => None,
                };
                let keys = crate::storage::index_keys::derive(&key.kind, value.as_ref());
                for (slot, k) in per_column.iter_mut().zip(keys) {
                    slot.push(k);
                }
            }
            let mut key_arrays: HashMap<&str, ArrayRef> = HashMap::new();
            for ((column, ty), values) in key.columns.iter().zip(&types).zip(per_column) {
                key_arrays.insert(column.as_str(), key_column_array(ty, values)?);
            }
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());
            for field in target_schema.fields() {
                if let Some(array) = key_arrays.get(field.name().as_str()) {
                    columns.push(array.clone());
                } else if let Some(col) = batch.column_by_name(field.name()) {
                    columns.push(col.clone());
                } else {
                    columns.push(arrow_array::new_null_array(
                        field.data_type(),
                        batch.num_rows(),
                    ));
                }
            }
            rewritten.push(arrow_array::RecordBatch::try_new(
                target_schema.clone(),
                columns,
            )?);
        }
        backend
            .replace_table_atomic(&table, rewritten, target_schema)
            .await?;
        Ok(true)
    }

    /// Build any declared scalar or full-text index for `label` whose physical
    /// artifact is missing, now that the table exists.
    ///
    /// [`Self::create_scalar_index`], [`Self::create_fts_index`] and
    /// [`Self::create_json_fts_index`] record `BuildOutcome::NotAttempted` when
    /// the label has not been flushed, because there is no Lance table to build
    /// against. That left the deferral with nowhere to resolve: the flush path
    /// built only the `_vid`/`_uid`/`ext_id` defaults
    /// (`VertexDataset::ensure_default_indexes`), so an index declared before
    /// the first insert kept a registry entry and never gained an artifact. A
    /// user who declared an index up front silently got none, and only an
    /// explicit `db.indexes().rebuild(label)` ever fixed it. This is the
    /// resolution point.
    ///
    /// Idempotent, and cheap when there is nothing to do: the schema is
    /// consulted first, so a label with no declared index of these kinds costs
    /// no backend call at all. Only when one is declared do we list the
    /// physical indexes and build what is absent.
    ///
    /// Vector indexes are materialized by the flush-time FDE path, and uni's
    /// own inverted and sparse indexes are updated incrementally, so none of
    /// them is stranded this way. The Lance index behind `CREATE FULLTEXT
    /// INDEX` is not updated at flush, so it was.
    #[cfg(feature = "lance-backend")]
    pub async fn ensure_declared_indexes(&self, label: &str) -> Result<()> {
        let declared: Vec<IndexDefinition> = {
            let schema = self.schema_manager.schema();
            schema
                .indexes
                .iter()
                .filter(|idx| match idx {
                    IndexDefinition::Scalar(cfg) => cfg.label == label,
                    IndexDefinition::FullText(cfg) => cfg.label == label,
                    IndexDefinition::JsonFullText(cfg) => cfg.label == label,
                    _ => false,
                })
                .cloned()
                .collect()
        };
        if declared.is_empty() {
            return Ok(());
        }

        let Some(backend) = self.backend.as_ref() else {
            return Ok(());
        };
        let table = table_names::vertex_table_name(label);
        if !backend.table_exists(&table).await? {
            return Ok(());
        }

        let existing = backend.list_indexes(&table).await?;
        // A scalar index is matched on the covered column set, not the name:
        // the physical artifact may have been created by a path that named it
        // differently (e.g. `ensure_default_indexes` passes `None`). A
        // full-text index is matched by name, because a scalar index on the
        // same column would otherwise pass for it; the build path always names
        // it after the declaration.
        let covers = |props: &[String]| {
            existing.iter().any(|idx| {
                idx.columns.len() == props.len() && props.iter().all(|p| idx.columns.contains(p))
            })
        };
        let named = |name: &str| existing.iter().any(|idx| idx.name == name);

        let schema = self.schema_manager.schema();
        for def in declared {
            // Reuse the single build path for each kind so the outcome-to-status
            // mapping stays in one place; it re-registers the definition with
            // fresh metadata.
            match def {
                // Matched on the columns it is built on: a keyed index's key
                // columns, each with its own index. `Failed` is permanent (a
                // column type the index cannot index). An index that needs a
                // table rewrite to become keyed is left for the next
                // writable open or an explicit rebuild (`Fill::Never`).
                IndexDefinition::Scalar(config)
                    if self.scalar_needs_build(&schema, &config, &covers) =>
                {
                    info!(
                        "Resolving deferred scalar index '{}' on '{}' at flush",
                        config.name, table
                    );
                    // Inside a flush, whose rows already carry the key when
                    // its columns are registered.
                    self.create_scalar_index_inner(config, Fill::Never).await?;
                }
                IndexDefinition::FullText(config) if !named(&config.name) => {
                    info!(
                        "Resolving deferred full-text index '{}' on '{}' at flush",
                        config.name, table
                    );
                    self.create_fts_index(config).await?;
                }
                IndexDefinition::JsonFullText(config) if !named(&config.name) => {
                    info!(
                        "Resolving deferred JSON full-text index '{}' on '{}' at flush",
                        config.name, table
                    );
                    // Inside a flush, whose rows already carry the text.
                    self.create_json_fts_index_inner(config, Fill::Never)
                        .await?;
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Whether a declared scalar index lacks its physical artifact and should
    /// be built at a flush (see [`Self::ensure_declared_indexes`]).
    ///
    /// Matched on the columns it is built on (`covers`): a keyed index is
    /// built when any key column lacks an index. `Failed` is permanent (a
    /// column type the index cannot index). An index that needs a table
    /// rewrite to become keyed (recorded before its type had a key, or its
    /// key columns unregistered) is not built here, inside a flush: it is
    /// upgraded at the next writable open ([`Self::upgrade_keyed_indexes`])
    /// or by an explicit rebuild.
    fn scalar_needs_build(
        &self,
        schema: &uni_common::core::schema::Schema,
        config: &ScalarIndexConfig,
        covers: &dyn Fn(&[String]) -> bool,
    ) -> bool {
        let mut resolved = config.clone();
        let _ = schema.resolve_scalar_index(&mut resolved);
        if Self::needs_key_upgrade(schema, config) {
            debug!(
                index = %config.name,
                "keyed index needs a table rewrite; left for the next writable open"
            );
            return false;
        }
        let columns: Vec<String> = resolved
            .physical_columns()
            .into_iter()
            .map(str::to_string)
            .collect();
        let built = match &resolved.key {
            Some(_) => columns.iter().all(|c| covers(std::slice::from_ref(c))),
            None => covers(&columns),
        };
        !built && config.metadata.status != uni_common::core::schema::IndexStatus::Failed
    }

    /// Whether the recorded scalar index `config` needs a table rewrite to
    /// serve its key: its property's type resolves to a key, and it is
    /// recorded without one (an index from before its type had a key, whose
    /// key columns, even if registered, were not kept current) or its key
    /// columns are unregistered.
    fn needs_key_upgrade(
        schema: &uni_common::core::schema::Schema,
        config: &ScalarIndexConfig,
    ) -> bool {
        let mut resolved = config.clone();
        if schema.resolve_scalar_index(&mut resolved).is_err() {
            return false;
        }
        let Some(key) = &resolved.key else {
            return false;
        };
        let props = schema.properties.get(&config.label);
        config.key.is_none()
            || !key
                .columns
                .iter()
                .all(|c| props.is_some_and(|p| p.contains_key(c)))
    }

    /// Upgrade every scalar index whose key needs a table rewrite (see
    /// [`Self::needs_key_upgrade`]): re-derive the key over the label's
    /// flushed rows, build it, and restore the label's other indexes, which
    /// the rewrite dropped. Returns the names of the indexes upgraded.
    ///
    /// Runs at a writable primary open, after recovery and before the store
    /// serves, because a flush never rewrites a table (its commit would join
    /// the flush's unit) and a fork cannot. A failed upgrade is logged and
    /// leaves the index as it was; the open goes on.
    #[cfg(feature = "lance-backend")]
    pub async fn upgrade_keyed_indexes(&self) -> Result<Vec<String>> {
        if self.forked {
            return Ok(Vec::new());
        }
        let pending: Vec<ScalarIndexConfig> = {
            let schema = self.schema_manager.schema();
            schema
                .indexes
                .iter()
                .filter_map(|idx| match idx {
                    IndexDefinition::Scalar(cfg) if Self::needs_key_upgrade(&schema, cfg) => {
                        Some(cfg.clone())
                    }
                    _ => None,
                })
                .collect()
        };
        let mut upgraded = Vec::with_capacity(pending.len());
        for config in pending {
            let name = config.name.clone();
            match self.create_scalar_index(config).await {
                Ok(()) => {
                    info!(index = %name, "scalar index upgraded to a keyed index");
                    upgraded.push(name);
                }
                Err(e) => warn!(
                    index = %name,
                    error = %e,
                    "could not upgrade the index to a keyed index; it is left as it was"
                ),
            }
        }
        Ok(upgraded)
    }

    /// Build and persist a full-text search (Lance inverted) index.
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self), level = "info")]
    pub async fn create_fts_index(&self, mut config: FullTextIndexConfig) -> Result<()> {
        let label = &config.label;
        info!(
            "Creating FTS index '{}' on {}.{:?}",
            config.name, label, config.properties
        );

        let schema = self.schema_manager.schema();
        if !schema.labels.contains_key(label) {
            return Err(anyhow!("Label '{}' not found", label));
        }

        let columns: Vec<&str> = config.properties.iter().map(|s| s.as_str()).collect();
        let table = table_names::vertex_table_name(label);

        let outcome = if let Some(backend) = self.backend.as_ref() {
            if backend.table_exists(&table).await? {
                info!(
                    "Building physical FTS index '{}' on '{}'",
                    config.name, table
                );
                // Validate the tokenizer config up front so an invalid analyzer
                // is surfaced as a hard error rather than being swallowed by the
                // downgrade below.
                if let Err(e) = crate::backend::fts_analyzer::to_inverted_params(
                    &config.tokenizer,
                    config.with_positions,
                ) {
                    return Err(anyhow!(
                        "Invalid tokenizer/analyzer config for FTS index '{}': {}",
                        config.name,
                        e
                    ));
                }
                if let Err(e) = backend
                    .create_fts_index(
                        &table,
                        &columns,
                        Some(&config.name),
                        &config.tokenizer,
                        config.with_positions,
                    )
                    .await
                {
                    warn!(
                        "Failed to build physical FTS index '{}' on '{}': {}",
                        config.name, table, e
                    );
                    BuildOutcome::Failed
                } else {
                    info!("FTS index '{}' created", config.name);
                    BuildOutcome::Built
                }
            } else {
                debug!(
                    "Label '{}' not flushed yet; physical FTS index '{}' deferred",
                    label, config.name
                );
                BuildOutcome::NotAttempted
            }
        } else {
            warn!(
                "No storage backend; physical FTS index '{}' deferred until a flush",
                config.name
            );
            BuildOutcome::NotAttempted
        };
        let row_count = self.built_row_count(&config.label, outcome).await;
        config.metadata = outcome.into_metadata(row_count);

        self.schema_manager
            .add_index(IndexDefinition::FullText(config))?;
        self.schema_manager.save().await?;

        Ok(())
    }

    /// Creates a JSON full-text index on a column.
    ///
    /// The index is a Lance inverted index on the column's derived text
    /// column ([`JsonFtsIndexConfig::derived_column`]), which every write of
    /// the label's table fills from the source value
    /// (`VertexDataset::build_record_batch_with_timestamps`). Rows already
    /// flushed are backfilled first, in storage before the catalog names the
    /// column, so the catalog is never ahead of the table.
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self), level = "info")]
    pub async fn create_json_fts_index(&self, config: JsonFtsIndexConfig) -> Result<()> {
        // Always re-derived: rows written while no such index was declared
        // (before it, or after a drop) carry no text.
        let (label, name) = (config.label.clone(), config.name.clone());
        if self
            .create_json_fts_index_inner(config, Fill::Rewrite)
            .await?
        {
            self.restore_label_indexes(&label, &HashSet::new(), Some(&name), false)
                .await?;
        }
        Ok(())
    }

    /// Fill a JSON full-text index's text column, storage first: rewrite the
    /// table with it when `rewrite` and the table exists, then register it.
    /// Returns whether the table was rewritten.
    #[cfg(feature = "lance-backend")]
    async fn fill_json_fts_column(
        &self,
        config: &JsonFtsIndexConfig,
        rewrite: bool,
    ) -> Result<bool> {
        let label = &config.label;
        let rewrote = rewrite
            && self.label_table_exists(label).await?
            && self.backfill_json_fts_column(config).await?;
        self.schema_manager.add_internal_property(
            label,
            &config.derived_column(),
            uni_common::DataType::String,
            true,
        )?;
        Ok(rewrote)
    }

    /// [`Self::create_json_fts_index`] as `fill` allows; returns whether the
    /// table was rewritten. Under [`Fill::Never`] / [`Fill::Filled`] an index
    /// whose text column is unregistered over a flushed table is left as is.
    #[cfg(feature = "lance-backend")]
    async fn create_json_fts_index_inner(
        &self,
        mut config: JsonFtsIndexConfig,
        fill: Fill,
    ) -> Result<bool> {
        let label = config.label.clone();
        let derived = config.derived_column();
        let (label, derived) = (&label, &derived);
        info!(
            "Creating JSON FTS index '{}' on {}.{} (text column {})",
            config.name, label, config.column, derived
        );

        let schema = self.schema_manager.schema();
        if !schema.labels.contains_key(label) {
            return Err(anyhow!("Label '{}' not found", label));
        }
        let registered = schema
            .properties
            .get(label)
            .is_some_and(|p| p.contains_key(derived.as_str()));

        let table = table_names::vertex_table_name(label);
        let table_exists = self.label_table_exists(label).await?;
        let rewrite = match fill {
            Fill::Rewrite => true,
            Fill::IfUnregistered => !registered,
            Fill::Filled | Fill::Never => {
                if table_exists && !registered {
                    debug!(
                        index = %config.name,
                        "JSON full-text index left as is: its text column needs a table rewrite"
                    );
                    return Ok(false);
                }
                false
            }
        };
        let rewrote = self.fill_json_fts_column(&config, rewrite).await?;

        let outcome = if let Some(backend) = self.backend.as_ref() {
            if table_exists {
                info!(
                    "Building physical JSON FTS index '{}' on '{}.{}'",
                    config.name, table, derived
                );
                if let Err(e) = backend
                    .create_fts_index(
                        &table,
                        &[derived.as_str()],
                        Some(&config.name),
                        &uni_common::core::schema::TokenizerConfig::Standard,
                        config.with_positions,
                    )
                    .await
                {
                    warn!(
                        "Failed to build physical JSON FTS index '{}' on '{}': {}",
                        config.name, table, e
                    );
                    BuildOutcome::Failed
                } else {
                    info!("JSON FTS index '{}' created", config.name);
                    BuildOutcome::Built
                }
            } else {
                debug!(
                    "Label '{}' not flushed yet; physical JSON FTS index '{}' deferred",
                    label, config.name
                );
                BuildOutcome::NotAttempted
            }
        } else {
            warn!(
                "No storage backend; physical JSON FTS index '{}' deferred until a flush",
                config.name
            );
            BuildOutcome::NotAttempted
        };
        let row_count = self.built_row_count(&config.label, outcome).await;
        config.metadata = outcome.into_metadata(row_count);

        self.schema_manager
            .add_index(IndexDefinition::JsonFullText(config))?;
        self.schema_manager.save().await?;

        Ok(rewrote)
    }

    /// Rewrites the label's flushed rows with the JSON full-text index's
    /// derived text column filled from each row's source value.
    ///
    /// Reads the newest versions (`self.backend` is the head backend) and
    /// holds the table's write lock across the scan and the rewrite, so a
    /// flush cannot append between them (issue #96's rule for the MUVERA
    /// backfill). The target schema is the live one plus the derived column,
    /// which the caller adds to the catalog only after this succeeds.
    #[cfg(feature = "lance-backend")]
    async fn backfill_json_fts_column(&self, config: &JsonFtsIndexConfig) -> Result<bool> {
        self.refuse_table_rewrite_on_fork(&format!("JSON full-text index '{}'", config.name))?;
        use arrow_array::{Array, ArrayRef, StringArray};

        let Some(backend) = self.backend.as_ref() else {
            return Ok(false);
        };
        let label = &config.label;
        let derived = config.derived_column();
        let table = table_names::vertex_table_name(label);

        let mut widened = (*self.schema_manager.schema()).clone();
        widened
            .properties
            .entry(label.clone())
            .or_default()
            .entry(derived.clone())
            .or_insert_with(|| uni_common::core::schema::PropertyMeta {
                r#type: uni_common::DataType::String,
                nullable: true,
                added_in: widened.schema_version,
                state: uni_common::core::schema::SchemaElementState::Active,
                generation_expression: None,
                description: None,
                default: None,
            });
        let label_id = widened
            .label_id_by_name(label)
            .ok_or_else(|| anyhow!("JSON FTS: label '{label}' not found"))?;
        let target_schema =
            VertexDataset::new(&self.base_uri, label, label_id).get_arrow_schema(&widened)?;
        let source_dt = widened
            .properties
            .get(label)
            .and_then(|p| p.get(&config.column))
            .map(|m| m.r#type.clone());

        let _table_guard = backend.lock_table_for_write(&table).await;
        let batches = backend.scan(ScanRequest::all(&table)).await?;
        let mut rewritten = Vec::with_capacity(batches.len());
        for batch in &batches {
            let texts: Vec<Option<String>> = match batch.column_by_name(&config.column) {
                Some(src) => (0..batch.num_rows())
                    .map(|row| {
                        if src.is_null(row) {
                            return Ok(None);
                        }
                        let value = crate::storage::arrow_convert::arrow_to_value(
                            src.as_ref(),
                            row,
                            source_dt.as_ref(),
                        )?;
                        Ok(uni_common::core::schema::json_fts_text(
                            &value,
                            &config.paths,
                        ))
                    })
                    .collect::<Result<_>>()?,
                None => vec![None; batch.num_rows()],
            };
            let text_column: ArrayRef = Arc::new(StringArray::from(texts));
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(target_schema.fields().len());
            for field in target_schema.fields() {
                if field.name() == &derived {
                    columns.push(text_column.clone());
                } else if let Some(col) = batch.column_by_name(field.name()) {
                    columns.push(col.clone());
                } else {
                    columns.push(arrow_array::new_null_array(
                        field.data_type(),
                        batch.num_rows(),
                    ));
                }
            }
            rewritten.push(arrow_array::RecordBatch::try_new(
                target_schema.clone(),
                columns,
            )?);
        }
        backend
            .replace_table_atomic(&table, rewritten, target_schema)
            .await?;
        Ok(true)
    }

    /// Remove an index both physically from the Lance dataset and from the schema.
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self), level = "info")]
    pub async fn drop_index(&self, name: &str) -> Result<()> {
        info!("Dropping index '{}'", name);

        let idx_def = self
            .schema_manager
            .get_index(name)
            .ok_or_else(|| anyhow!("Index '{}' not found in schema", name))?;

        // Drop the physical index through the backend. Best-effort: the index
        // may never have been physically built (e.g. created before any flush),
        // so a failure here is non-fatal.
        let label = idx_def.label();
        let table = table_names::vertex_table_name(label);
        // A keyed index is built as one Lance index per key column.
        let physical = match &idx_def {
            IndexDefinition::Scalar(cfg) => cfg.physical_index_names(),
            _ => vec![name.to_string()],
        };
        if let Some(backend) = self.backend.as_ref() {
            for physical_name in &physical {
                if let Err(e) = backend.drop_index(&table, physical_name).await {
                    warn!(
                        "Physical index drop for '{}' returned error (non-fatal): {}",
                        physical_name, e
                    );
                } else {
                    info!(
                        "Physical index '{}' dropped from '{}'",
                        physical_name, table
                    );
                }
            }
        }

        self.schema_manager.remove_index(name)?;
        // A JSON full-text index's derived text column goes with the last
        // index on its column: left registered, it was carried, unfilled, on
        // every later row. The Lance column stays, invisible to a catalog
        // that does not name it; re-creating the index backfills it.
        if let IndexDefinition::JsonFullText(cfg) = &idx_def {
            let still_indexed = self.schema_manager.schema().indexes.iter().any(|idx| {
                matches!(idx, IndexDefinition::JsonFullText(other)
                    if other.label == cfg.label && other.column == cfg.column)
            });
            if !still_indexed {
                let derived = cfg.derived_column();
                if let Err(e) = self.schema_manager.drop_property(&cfg.label, &derived) {
                    debug!("derived column '{derived}' of '{name}' was not registered: {e}");
                }
            }
        }
        // Likewise a keyed index's key columns, with the last index that uses
        // them.
        if let IndexDefinition::Scalar(cfg) = &idx_def
            && let Some(key) = &cfg.key
        {
            let schema = self.schema_manager.schema();
            for column in &key.columns {
                let still_used = schema.indexes.iter().any(|idx| {
                    matches!(idx, IndexDefinition::Scalar(other)
                        if other.label == cfg.label
                            && other.key.as_ref().is_some_and(|k| k.columns.contains(column)))
                });
                if !still_used && let Err(e) = self.schema_manager.drop_property(&cfg.label, column)
                {
                    debug!("key column '{column}' of '{name}' was not registered: {e}");
                }
            }
        }
        self.schema_manager.save().await?;
        Ok(())
    }

    /// Rebuild all indexes registered for `label` from scratch.
    ///
    /// In three phases, because a table rewrite drops every Lance index on
    /// the table:
    ///
    /// 1. every rewrite the label's indexes need (a MUVERA FDE, a JSON
    ///    full-text index's text, a keyed index's key), each followed at once
    ///    by its column's registration, storage first
    ///    ([`Self::fill_derived_columns`]);
    /// 2. the label's default indexes (`_vid`, `_uid`, `ext_id`);
    /// 3. every declared index, with no table rewrite.
    ///
    /// An index whose key needs a rewrite to become keyed (an upgrade) is
    /// upgraded here: phase 1 re-derives its key.
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self), level = "info")]
    pub async fn rebuild_indexes_for_label(&self, label: &str) -> Result<()> {
        info!("Rebuilding all indexes for label '{}'", label);
        let filled = self.fill_derived_columns(label, false).await?;
        self.restore_label_indexes(label, &filled, None, true).await
    }

    /// Restore `label`'s indexes after a vertex compaction replaced its table.
    ///
    /// The compaction's batch already carries every registered derived column
    /// it can compute (key columns and JSON full-text text are filled at batch
    /// build), so only a MUVERA FDE is re-materialised (the flush-time
    /// materializer does not cover bulk-loaded rows); then phases 2 and 3 of
    /// [`Self::rebuild_indexes_for_label`].
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self), level = "info")]
    pub async fn restore_indexes_after_compaction(&self, label: &str) -> Result<()> {
        let filled = self.fill_derived_columns(label, true).await?;
        self.restore_label_indexes(label, &filled, None, true).await
    }

    /// Phase 1 of [`Self::rebuild_indexes_for_label`]: every table rewrite
    /// the label's declared indexes need, each followed by its column's
    /// registration. `muvera_only` limits it to MUVERA FDE columns.
    ///
    /// Returns the keyed scalar indexes whose keys it filled.
    #[cfg(feature = "lance-backend")]
    async fn fill_derived_columns(
        &self,
        label: &str,
        muvera_only: bool,
    ) -> Result<HashSet<String>> {
        let declared: Vec<IndexDefinition> = self
            .schema_manager
            .schema()
            .indexes
            .iter()
            .filter(|idx| idx.label() == label)
            .cloned()
            .collect();
        let mut filled = HashSet::new();
        for index in declared {
            match index {
                // A full rebuild must force the MUVERA FDE backfill: bulk-loaded / reopened
                // rows aren't covered by the flush-time materializer (see
                // `create_vector_index_inner`).
                IndexDefinition::Vector(cfg) => {
                    self.prepare_muvera_fde(&cfg, Fill::Rewrite).await?;
                }
                _ if muvera_only => {}
                // A rebuild re-derives the text column: bulk-loaded rows may
                // predate it.
                IndexDefinition::JsonFullText(cfg) => {
                    self.fill_json_fts_column(&cfg, true).await?;
                }
                IndexDefinition::Scalar(mut cfg) => {
                    let schema = self.schema_manager.schema();
                    if schema.resolve_scalar_index(&mut cfg).is_ok() && cfg.key.is_some() {
                        drop(schema);
                        self.fill_key_columns(&cfg, true).await?;
                        filled.insert(cfg.name);
                    }
                }
                _ => {}
            }
        }
        Ok(filled)
    }

    /// Phases 2 and 3 of [`Self::rebuild_indexes_for_label`]: the label's
    /// default indexes, then every declared index on it except `skip`, none
    /// of them rewriting the table. A keyed index named in `filled` has just
    /// had its key filled ([`Fill::Filled`]); any other build that would need
    /// a rewrite is left as it is ([`Fill::Never`]).
    ///
    /// `all_kinds` also rebuilds uni's own inverted and sparse indexes, which
    /// live outside the table; a single index's rewrite does not touch them.
    #[cfg(feature = "lance-backend")]
    async fn restore_label_indexes(
        &self,
        label: &str,
        filled: &HashSet<String>,
        skip: Option<&str>,
        all_kinds: bool,
    ) -> Result<()> {
        let (label_id, declared): (Option<u16>, Vec<IndexDefinition>) = {
            let schema = self.schema_manager.schema();
            (
                schema.label_id_by_name(label),
                schema
                    .indexes
                    .iter()
                    .filter(|idx| idx.label() == label && Some(idx.name()) != skip)
                    .cloned()
                    .collect(),
            )
        };
        if let (Some(backend), Some(label_id)) = (self.backend.as_ref(), label_id) {
            VertexDataset::new(&self.base_uri, label, label_id)
                .ensure_default_indexes(backend.as_ref())
                .await?;
        }
        for index in declared {
            match index {
                IndexDefinition::Vector(cfg) => {
                    self.create_vector_index_inner(cfg, Fill::Filled).await?;
                }
                IndexDefinition::Scalar(cfg) => {
                    let fill = if filled.contains(&cfg.name) {
                        Fill::Filled
                    } else {
                        Fill::Never
                    };
                    self.create_scalar_index_inner(cfg, fill).await?;
                }
                IndexDefinition::FullText(cfg) => self.create_fts_index(cfg).await?,
                IndexDefinition::JsonFullText(cfg) => {
                    self.create_json_fts_index_inner(cfg, Fill::Filled).await?;
                }
                IndexDefinition::Inverted(cfg) if all_kinds => {
                    self.create_inverted_index(cfg).await?
                }
                IndexDefinition::Sparse(cfg) if all_kinds => {
                    self.create_sparse_vector_index(cfg).await?
                }
                IndexDefinition::Inverted(_) | IndexDefinition::Sparse(_) => {}
                _ => warn!("Unknown index type encountered during rebuild, skipping"),
            }
        }
        Ok(())
    }

    /// Create composite index for unique constraint
    #[cfg(feature = "lance-backend")]
    pub async fn create_composite_index(&self, label: &str, properties: &[String]) -> Result<()> {
        let schema = self.schema_manager.schema();
        if !schema.labels.contains_key(label) {
            return Err(anyhow!("Label '{}' not found", label));
        }

        // Lance supports multi-column indexes.
        let index_name = format!("{}_{}_composite", label, properties.join("_"));
        let columns: Vec<&str> = properties.iter().map(|s| s.as_str()).collect();
        let table = table_names::vertex_table_name(label);

        if let Some(backend) = self.backend.as_ref() {
            if backend.table_exists(&table).await? {
                info!("Building composite index '{}' on '{}'", index_name, table);
                let outcome = if let Err(e) = backend
                    .create_scalar_index(
                        &table,
                        &columns,
                        crate::backend::types::ScalarIndexType::BTree,
                        Some(&index_name),
                    )
                    .await
                {
                    warn!(
                        "Failed to build composite index '{}' on '{}': {}",
                        index_name, table, e
                    );
                    BuildOutcome::Failed
                } else {
                    info!("Composite index '{}' created", index_name);
                    BuildOutcome::Built
                };

                let row_count = self.built_row_count(label, outcome).await;
                let config = ScalarIndexConfig {
                    name: index_name,
                    label: label.to_string(),
                    properties: properties.to_vec(),
                    index_type: uni_common::core::schema::ScalarIndexType::BTree,
                    where_clause: None,
                    metadata: outcome.into_metadata(row_count),
                    key: None,
                };
                self.schema_manager
                    .add_index(IndexDefinition::Scalar(config))?;
                self.schema_manager.save().await?;
            } else {
                debug!(
                    "Label '{}' not flushed yet; composite index for {:?} built on next flush",
                    label, properties
                );
            }
        } else {
            warn!(
                "No storage backend; composite index for {:?} deferred until a flush",
                properties
            );
        }

        Ok(())
    }

    /// Applies incremental updates to an inverted index.
    ///
    /// Instead of rebuilding the entire index, this method updates only the
    /// changed entries, making it much faster for small mutations.
    ///
    /// # Errors
    ///
    /// Returns an error if the index doesn't exist or the update fails.
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self, added, removed), level = "info", fields(
        label = %config.label,
        property = %config.property
    ))]
    pub async fn update_inverted_index_incremental(
        &self,
        config: &InvertedIndexConfig,
        added: &HashMap<Vid, Vec<String>>,
        removed: &HashSet<Vid>,
    ) -> Result<()> {
        info!(
            added = added.len(),
            removed = removed.len(),
            "Incrementally updating inverted index"
        );

        // Serialize this load-modify-overwrite against a concurrent DDL backfill of the
        // same postings dataset (issue #95).
        let postings_path = format!(
            "{}/indexes/{}/{}_inverted",
            self.base_uri, config.label, config.property
        );
        let _postings_guard = self.postings_write_guard(&postings_path).await;

        let mut index =
            InvertedIndex::open(&self.base_uri, config.clone(), self.read_cut.clone(), false)
                .await?;
        index.apply_incremental_updates(added, removed).await
    }

    /// Create (and backfill) a scored sparse-vector index. Mirrors
    /// `create_inverted_index`: build from the flushed vertex dataset if it
    /// exists, then register + persist the config.
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self), level = "info")]
    pub async fn create_sparse_vector_index(&self, config: SparseVectorIndexConfig) -> Result<()> {
        let label = &config.label;
        let property = &config.property;
        info!(
            "Creating Sparse Vector Index '{}' on {}.{}",
            config.name, label, property
        );

        let schema = self.schema_manager.schema();
        if !schema.labels.contains_key(label) {
            return Err(anyhow!("Label '{}' not found", label));
        }

        // Serialize this full-rebuild overwrite against a concurrent flush incremental
        // update of the same postings dataset (issue #95).
        let postings_path = format!("{}/indexes/{}/{}_sparse", self.base_uri, label, property);
        let _postings_guard = self.postings_write_guard(&postings_path).await;

        let mut index =
            SparseVectorIndex::open(&self.base_uri, config.clone(), self.read_cut.clone(), false)
                .await?;

        // Backfill from the flushed vertex table via the storage backend (the
        // LanceDB-managed table is not at the raw `{base}/vertices_<label>`
        // path a `VertexDataset::open` expects). Mirrors the MUVERA backfill.
        let table = table_names::vertex_table_name(label);
        if let Some(backend) = self.backend.as_ref() {
            // Err propagates (a backend fault must surface, not silently build an empty
            // index); Ok(false) is the not-yet-flushed create-before-ingest case.
            if backend.table_exists(&table).await? {
                let batches = backend.scan(ScanRequest::all(&table)).await?;
                index
                    .build_from_batches(&batches, |n| debug!("Indexed {} sparse docs", n))
                    .await?;
            } else {
                debug!(
                    "Table '{}' not flushed yet; creating empty sparse index (populated on flush)",
                    table
                );
            }
        } else {
            warn!(
                "No storage backend available; sparse index '{}' left empty (populated on flush)",
                config.name
            );
        }

        self.schema_manager
            .add_index(IndexDefinition::Sparse(config))?;
        self.schema_manager.save().await?;

        Ok(())
    }

    /// Applies incremental updates to a sparse-vector index (load-modify-write,
    /// same semantics as the set-membership inverted index).
    #[cfg(feature = "lance-backend")]
    #[instrument(skip(self, added, removed), level = "info", fields(
        label = %config.label,
        property = %config.property
    ))]
    pub async fn update_sparse_vector_index_incremental(
        &self,
        config: &SparseVectorIndexConfig,
        added: &HashMap<Vid, Vec<(u32, f32)>>,
        removed: &HashSet<Vid>,
    ) -> Result<()> {
        info!(
            added = added.len(),
            removed = removed.len(),
            "Incrementally updating sparse vector index"
        );
        // Serialize this load-modify-overwrite against a concurrent DDL backfill of the
        // same postings dataset (issue #95).
        let postings_path = format!(
            "{}/indexes/{}/{}_sparse",
            self.base_uri, config.label, config.property
        );
        let _postings_guard = self.postings_write_guard(&postings_path).await;

        let mut index =
            SparseVectorIndex::open(&self.base_uri, config.clone(), self.read_cut.clone(), false)
                .await?;
        index.apply_incremental_updates(added, removed).await
    }

    /// Open a sparse-vector index for querying, given its label + property.
    /// Errors if no `IndexDefinition::Sparse` is registered for that pair.
    #[cfg(feature = "lance-backend")]
    pub async fn sparse_vector_index(
        &self,
        label: &str,
        property: &str,
    ) -> Result<SparseVectorIndex> {
        let schema = self.schema_manager.schema();
        let config = schema
            .indexes
            .iter()
            .find_map(|idx| match idx {
                IndexDefinition::Sparse(cfg) if cfg.label == label && cfg.property == property => {
                    Some(cfg.clone())
                }
                _ => None,
            })
            .ok_or_else(|| anyhow!("No sparse vector index found for {}.{}", label, property))?;
        SparseVectorIndex::open(&self.base_uri, config, self.read_cut.clone(), true).await
    }
}

/// Resolve the `AUTO_SUB_VECTORS` sentinel and fill in `default_refine_factor`.
///
/// Called on every path that persists a vector index definition, so a stored
/// schema never contains the sentinel and every quantized index carries a refine
/// default. Idempotent: an explicit `sub_vectors` and an already-set
/// `default_refine_factor` are both left alone.
///
/// `dim` is the dimensionality the index is actually built over — for MUVERA
/// that is the derived FDE column's width, not the source column's.
pub fn resolve_vector_index_defaults(
    config: &mut VectorIndexConfig,
    dim: Option<usize>,
    fde_dim: Option<usize>,
) {
    fn resolve_type(t: &mut VectorIndexType, dim: Option<usize>, fde_dim: Option<usize>) {
        match t {
            VectorIndexType::IvfPq {
                num_sub_vectors, ..
            }
            | VectorIndexType::HnswPq {
                num_sub_vectors, ..
            } => {
                if *num_sub_vectors == AUTO_SUB_VECTORS {
                    *num_sub_vectors = default_sub_vectors(dim);
                }
            }
            // The inner ANN is built over the derived FDE column, whose width
            // the caller resolves via `fde_spec_for_config` and passes in as
            // `fde_dim` — it is not the source column's width.
            VectorIndexType::Muvera { inner, .. } => {
                // Inside MUVERA the FDE width *is* the dim the inner index sees.
                resolve_type(inner, fde_dim, fde_dim);
            }
            _ => {}
        }
    }
    resolve_type(&mut config.index_type, dim, fde_dim);
    if config.default_refine_factor.is_none() {
        let effective_dim = match &config.index_type {
            VectorIndexType::Muvera { .. } => fde_dim,
            _ => dim,
        };
        config.default_refine_factor = default_refine_factor(&config.index_type, effective_dim);
    }
}

/// Dimensionality the index is built over, from the schema's declared type.
pub fn index_build_dim(
    schema: &uni_common::core::schema::Schema,
    config: &VectorIndexConfig,
) -> Option<usize> {
    schema
        .properties
        .get(&config.label)
        .and_then(|props| props.get(&config.property))
        .and_then(|meta| crate::storage::muvera_index::resolve_vector_dim(&meta.r#type))
}

/// An Arrow column of key values of the declared key column type `ty`.
#[cfg(feature = "lance-backend")]
fn key_column_array(
    ty: &uni_common::DataType,
    values: Vec<Option<crate::storage::index_keys::KeyValue>>,
) -> Result<arrow_array::ArrayRef> {
    use crate::storage::index_keys::KeyValue;
    Ok(match ty {
        uni_common::DataType::Int64 => Arc::new(arrow_array::Int64Array::from(
            values
                .into_iter()
                .map(|v| match v {
                    Some(KeyValue::Int(i)) => Some(i),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        uni_common::DataType::Float64 => Arc::new(arrow_array::Float64Array::from(
            values
                .into_iter()
                .map(|v| match v {
                    Some(KeyValue::Float(f)) => Some(f),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        uni_common::DataType::String => Arc::new(arrow_array::StringArray::from(
            values
                .into_iter()
                .map(|v| match v {
                    Some(KeyValue::Str(s)) => Some(s),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        other => return Err(anyhow!("no key column of type {other:?}")),
    })
}
