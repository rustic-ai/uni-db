// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use uni_common::core::fork::ForkId;

/// Streaming appender, re-exported from `uni-bulk`.
///
/// Shim kept so `crate::api::appender::*` paths resolve unchanged after
/// the bulk-engine extraction.
pub mod appender {
    pub use uni_bulk::appender::*;
}
pub mod builder;
/// Bulk writer engine, re-exported from `uni-bulk`.
///
/// Shim kept so `crate::api::bulk::*` paths resolve unchanged after the
/// bulk-engine extraction.
pub mod bulk {
    pub use uni_bulk::bulk::*;
}
pub mod compaction;
pub(crate) mod for_update;
pub mod fork;
/// Fork diff/promote types and engine, re-exported from `uni-fork`.
///
/// Shim kept so `crate::api::fork_diff::*` paths resolve unchanged after the
/// fork-engine extraction. `compute_diff`/`run_promote` are generic over the
/// `uni_fork` host traits, which uni-db implements for `Session`/`Transaction`.
pub mod fork_diff {
    pub use uni_fork::diff::{compute_diff, run_promote};
    pub use uni_fork::types::*;
}
pub(crate) mod fork_admin;
pub(crate) mod fork_maintenance;
pub mod fork_schema;
pub mod functions;
/// Session/commit hooks — moved to `uni-plugin-host`; re-exported to keep the
/// `uni_db::api::hooks::*` path stable.
pub mod hooks {
    pub use uni_plugin_host::hooks::*;
}
pub(crate) mod host_executor;
pub mod impl_locy;
pub mod impl_query;
pub mod indexes;
pub mod locy_builder;
pub mod locy_result;
pub mod locy_rule_catalog;
pub mod multi_agent;
pub mod plugin_trust;
pub(crate) mod plugins;
/// Commit notifications — moved to `uni-plugin-host`; re-exported to keep the
/// `uni_db::api::notifications::*` path stable.
pub mod notifications {
    pub use uni_plugin_host::notifications::*;
}
pub mod prepared;
pub mod retry;
pub mod rule_registry;
pub mod schema;
pub mod session;
pub mod sync;
pub mod template;
pub mod transaction;
/// Trigger dispatch engine — moved to `uni-plugin-host`; re-exported to keep
/// the `uni_db::api::triggers::*` path stable.
pub mod triggers {
    pub use uni_plugin_host::triggers::*;
}
pub mod xervo;

use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use object_store::local::LocalFileSystem;
use tracing::info;
use uni_common::core::snapshot::SnapshotManifest;
use uni_common::{CloudStorageConfig, UniConfig};
use uni_common::{Result, UniError};
use uni_store::cloud::build_cloud_store;
use uni_xervo::api::ModelAliasSpec;
use uni_xervo::runtime::ModelRuntime;

use uni_common::core::schema::SchemaManager;
use uni_store::runtime::id_allocator::IdAllocator;
use uni_store::runtime::property_manager::PropertyManager;
use uni_store::runtime::wal::WriteAheadLog;
use uni_store::storage::manager::StorageManager;

use uni_store::runtime::writer::Writer;

use crate::shutdown::ShutdownHandle;

/// Re-exported so `uni_db::api::UniPluginEntry` keeps resolving after the
/// plugin surface moved into the `plugins` submodule.
pub use plugins::UniPluginEntry;
use plugins::register_builtin_plugins;
#[cfg(feature = "pyo3-plugins")]
pub(crate) use plugins::{py_plugin_err_to_uni, with_loading_registrar};

use std::collections::HashMap;

/// Shared inner state of a Uni database instance. Not intended for direct use.
#[doc(hidden)]
pub struct UniInner {
    pub(crate) storage: Arc<StorageManager>,
    pub(crate) schema: Arc<SchemaManager>,
    pub(crate) properties: Arc<PropertyManager>,
    pub(crate) writer: Option<Arc<Writer>>,
    /// The L0 tier this view reads through when it has no [`Writer`].
    ///
    /// `Some` on every live view. It is redundant when `writer` is `Some`
    /// (`Executor::get_context` prefers the writer's manager) and load-bearing
    /// on a **read-only open**, where the WAL was replayed into this manager and
    /// the writer was then dropped.
    ///
    /// `None` means *deliberately L0-free*: a pinned snapshot view
    /// ([`UniInner::at_snapshot`]), whose rows are entirely in L1.
    /// The database root as the caller named it: the path or URI passed to
    /// `open`/`create`, or the `uni_mem_*` scratch root for a temporary
    /// database. Deliberately not `storage.base_path()`, which points at the
    /// `storage/` subdirectory — a caller cleaning up after a temporary
    /// database needs the root, not a child of it.
    pub(crate) uri: String,
    pub(crate) l0_manager: Option<Arc<uni_store::runtime::l0_manager::L0Manager>>,
    /// The parent's id allocator, kept even on a read-only open so a fork can
    /// bootstrap its own allocator above the parent's HWM. See issue #169.
    pub(crate) id_allocator: Option<Arc<uni_store::runtime::id_allocator::IdAllocator>>,
    pub(crate) xervo_runtime: Option<Arc<ModelRuntime>>,
    pub(crate) config: UniConfig,
    pub(crate) procedure_registry: Arc<uni_query::ProcedureRegistry>,
    /// Framework-wide plugin registry — `BuiltinPlugin` and (optionally)
    /// `ApocCorePlugin` register here at construction time. The
    /// `procedure_registry` holds an `Arc` of this same registry so
    /// `CALL` dispatch can resolve plugin-registered procedures.
    pub(crate) plugin_registry: Arc<uni_plugin::PluginRegistry>,
    /// Per-installed-plugin lifecycle bookkeeping for M10 reload.
    ///
    /// Keyed by [`uni_plugin::PluginId`]. The value holds a clone of the
    /// installed plugin object (so `shutdown()` runs on removal), the
    /// shared [`uni_plugin::lifecycle::PluginLifecycle`] handle the
    /// `EpochFencedReload` driver advances, and the monotonic generation
    /// counter exposed through [`uni_plugin::PluginHandle::generation`].
    ///
    /// Shared by `Arc` across `at_snapshot` / `at_fork` clones because
    /// the underlying `plugin_registry` is shared too — reloading a
    /// plugin on any session must be observable to siblings.
    pub(crate) plugins: Arc<parking_lot::RwLock<HashMap<uni_plugin::PluginId, UniPluginEntry>>>,
    /// In-memory deferral queue for `TriggerOutcome::Defer` (M11 v1).
    /// Persistent backing is `TODO(M11-persist)`. The background tick
    /// task spawned in `Uni::build` drives this queue; the trigger
    /// router pushes to it on `Defer`.
    pub(crate) defer_queue: Arc<crate::api::triggers::DeferralQueue>,
    /// WS-E: per-`Uni` EventualConsistency coalescing queue. Buffers
    /// `FireMode::EventualConsistency` after-phase fires into per-trigger
    /// buckets and flushes a single coalesced `DeferredItem` per bucket on
    /// the shared deferral tick. Shared by `Arc` into each commit's
    /// (ephemeral) `TriggerRouter`, exactly like `defer_queue`; coalesced
    /// work rides `defer_queue`'s durability, so this needs no sidecar.
    pub(crate) ec_queue: Arc<crate::api::triggers::EcQueue>,
    /// M11 background-job scheduler host. Owns the
    /// [`uni_plugin::scheduler::Scheduler`] primitive that the
    /// `uni.periodic.*` procedures register jobs against. Driver task
    /// is tracked by the shared [`Self::shutdown_handle`].
    pub(crate) scheduler_host: Arc<crate::scheduler::SchedulerHost>,
    pub(crate) shutdown_handle: Arc<ShutdownHandle>,
    /// Global registry of pre-compiled Locy rules.
    ///
    /// Cloned into every new Session. Use `db.register_rules()` to add rules
    /// globally, or `session.register_rules()` for session-scoped rules.
    pub(crate) locy_rule_registry: Arc<std::sync::RwLock<impl_locy::LocyRuleRegistry>>,
    /// Durable backing for the database-level Locy rule registry.
    ///
    /// `Some` only on the primary database inner; `None` on session, fork, and
    /// snapshot inners (set in [`Self::derived_clone`]) so those registries
    /// stay ephemeral and never write the catalog.
    pub(crate) locy_rule_persister: Option<Arc<locy_rule_catalog::LocyRulePersister>>,
    /// Timestamp when this database instance was built.
    pub(crate) start_time: Instant,
    /// Broadcast channel for commit notifications.
    pub(crate) commit_tx: tokio::sync::broadcast::Sender<Arc<notifications::CommitNotification>>,
    /// Write lease configuration for multi-agent access.
    pub(crate) write_lease: Option<multi_agent::WriteLease>,
    /// Host plugin trust policy — signature enforcement + trust root.
    /// Consulted at every plugin-load site. Default: Disabled + empty root
    /// (accept everything, as before).
    pub(crate) plugin_trust: Arc<plugin_trust::PluginTrustConfig>,
    /// Number of currently active sessions.
    pub(crate) active_session_count: AtomicUsize,
    /// Total queries executed across all sessions.
    pub(crate) total_queries: AtomicU64,
    /// Total transactions committed across all sessions.
    pub(crate) total_commits: AtomicU64,
    /// Database-level registry of custom scalar functions.
    pub(crate) custom_functions: Arc<std::sync::RwLock<uni_query::CustomFunctionRegistry>>,
    /// DataFusion `SessionContext` template with all Cypher UDFs
    /// pre-registered. Cloned per query (O(1) Arc bump) when the executor
    /// has no custom UDFs installed, skipping the ~140 µs cost of building
    /// a fresh `SessionContext` and re-registering UDFs every call.
    ///
    /// **Safe to share** because: (a) no code path mutates the session via
    /// `.write()` outside of the cold-path custom-UDF branch in
    /// `create_datafusion_planner` (verified by grep); (b) custom UDFs are
    /// registered on a fresh, isolated `SessionContext` to avoid leaking
    /// into this template.
    pub(crate) df_session_template: Arc<datafusion::execution::context::SessionContext>,
    /// Pre-configured `Executor` template with all session-constant fields
    /// already populated (storage, config, xervo_runtime, procedure_registry,
    /// writer, df_session_template, prop_manager). Cloned per query
    /// (cheap Arc bumps + a fresh `warnings` Mutex via manual `Clone` impl),
    /// after which only per-query fields (transaction_l0, id_reservoir,
    /// custom_functions, cancellation_token) need to be set.
    ///
    /// Skips ~25 µs/query of `Executor::new` + repeated setter dispatches.
    pub(crate) executor_template: Arc<uni_query::Executor>,
    /// Fork registry — persists `catalog/fork_registry.json` and runs
    /// the create/drop 2PC. Built once during `Uni::open` and shared
    /// by the primary `UniInner` and every forked-session inner.
    pub(crate) fork_registry: Arc<uni_store::fork::ForkRegistryHandle>,
    /// Phase 2 Day 11 — number of `Transaction`s currently alive on
    /// this `UniInner`. A transaction increments at construction and
    /// decrements on `Drop` (whether committed, rolled back, or
    /// silently dropped). `Uni::drop_fork` peeks this counter via the
    /// `fork_inners` cache to surface uncommitted-tx state as a
    /// typed `UniError::ForkInflightTx` instead of letting the drop
    /// proceed and silently discard the work.
    pub(crate) inflight_tx_count: Arc<AtomicUsize>,
    /// Phase 2 Day 8 cache: same-fork-name `Session::fork(name)` calls
    /// share the same `Arc<UniInner>` so sibling sessions on the same
    /// fork see each other's commits without flushing through Lance
    /// (which would otherwise be the only synchronization point at the
    /// branch level). Held as `Weak` so the inner is reclaimed when
    /// the last session drops; `ForkBuilder::build` rebuilds on the
    /// next call. Initialized empty on the primary `UniInner`; each
    /// forked inner clones the same `Arc<DashMap>` so siblings see
    /// the registry from any direction.
    pub(crate) fork_inners: Arc<DashMap<ForkId, Weak<UniInner>>>,

    // ── Cached metrics (updated on commit, read by sync `metrics()`) ─────
    /// Cached L0 mutation count (updated after every commit).
    pub(crate) cached_l0_mutation_count: AtomicUsize,
    /// Cached L0 estimated size in bytes (updated after every commit).
    pub(crate) cached_l0_estimated_size: AtomicUsize,
    /// Cached WAL log sequence number (updated after every commit).
    pub(crate) cached_wal_lsn: AtomicU64,
    /// Temp directory guard — auto-deletes on drop. Only set for `Uni::temporary()`.
    /// Scratch-directory guard for `Uni::temporary()`, removed when the last
    /// holder drops.
    ///
    /// Shared as an `Arc` with every background task that can write into the
    /// directory, so removal is ordered by *ownership* rather than by timing.
    /// Previously this was a bare `TempDir` on `UniInner`: teardown removed the
    /// directory successfully and the auto-flush task, waking on the very same
    /// shutdown broadcast, then wrote its final-flush manifest and recreated
    /// the tree. See `ScratchDir`. Issue #167.
    pub(crate) _temp_dir: Option<Arc<ScratchDir>>,
    /// Transparent plan cache for the transaction write path.
    ///
    /// Caches the pre-rewrite logical plan keyed by query-text hash + schema
    /// version, so repeated `Transaction::execute` of the same statement shape
    /// (e.g. ingest `UNWIND … CREATE`) skips parse and planning. Shared db-wide
    /// via `Arc`. Forks/snapshots get a fresh empty cache in `derived_clone`
    /// because their storage layout (and thus the fork-fusion rewrite) differs.
    /// The logical-plan rewrites (`rewrite_for_fork_fusion`, `fuse_create_set`)
    /// and parameter binding still run per execution, so cached reuse is
    /// parameter-value independent.
    pub(crate) plan_cache: Arc<std::sync::Mutex<crate::api::session::PlanCache>>,
}

/// Capacity of the transaction-write-path plan cache (entries).
///
/// Matches the read-path [`crate::api::session`] cache (1000 entries, LFU
/// eviction). Large enough to retain every distinct ingest statement shape a
/// workload uses; raising it only helps when a session cycles through more than
/// this many *distinct* query texts.
pub(crate) const TX_PLAN_CACHE_CAPACITY: usize = 1000;

/// Write throttle pressure as a value in 0.0–1.0.
///
/// Indicates how much back-pressure the storage layer is exerting.
/// 0.0 means no throttling; 1.0 means fully throttled.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct ThrottlePressure(f64);

impl ThrottlePressure {
    /// Create a new throttle pressure value, clamped to 0.0–1.0.
    pub fn new(value: f64) -> Self {
        Self(value.clamp(0.0, 1.0))
    }

    /// The raw pressure value (0.0–1.0).
    pub fn value(&self) -> f64 {
        self.0
    }

    /// Returns `true` if any throttle pressure is active.
    pub fn is_throttled(&self) -> bool {
        self.0 > 0.0
    }
}

impl std::fmt::Display for ThrottlePressure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:.1}%", self.0 * 100.0)
    }
}

impl Default for ThrottlePressure {
    fn default() -> Self {
        Self(0.0)
    }
}

/// Snapshot of database-level metrics.
#[derive(Debug, Clone)]
pub struct DatabaseMetrics {
    /// Current L0 mutation count (cumulative since last flush).
    pub l0_mutation_count: usize,
    /// Estimated L0 buffer size in bytes.
    pub l0_estimated_size_bytes: usize,
    /// Schema version number.
    pub schema_version: u64,
    /// Time since the database instance was created.
    pub uptime: Duration,
    /// Number of currently active sessions.
    pub active_sessions: usize,
    /// Number of L1 compaction runs completed (0 until storage instrumentation).
    pub l1_run_count: usize,
    /// Write throttle pressure (0.0–1.0, 0 until instrumentation).
    pub write_throttle_pressure: ThrottlePressure,
    /// Current compaction status.
    pub compaction_status: uni_store::CompactionStatus,
    /// WAL size in bytes (0 until storage instrumentation).
    pub wal_size_bytes: u64,
    /// Highest WAL log sequence number that has been flushed (0 when no WAL is configured).
    pub wal_lsn: u64,
    /// Total queries executed across all sessions.
    pub total_queries: u64,
    /// Total transactions committed across all sessions.
    pub total_commits: u64,
}

/// Main entry point for Uni embedded database.
///
/// `Uni` is the lifecycle and admin handle. All data access goes through
/// [`Session`](session::Session) (reads) and [`Transaction`](transaction::Transaction) (writes).
///
/// # Examples
///
/// ```no_run
/// use uni_db::Uni;
///
/// #[tokio::main]
/// async fn main() -> Result<(), uni_db::UniError> {
///     let db = Uni::open("./my_db").build().await?;
///
///     // All data access goes through sessions
///     let session = db.session();
///     let results = session.query("MATCH (n) RETURN count(n)").await?;
///     println!("Count: {:?}", results);
///     Ok(())
/// }
/// ```
pub struct Uni {
    pub(crate) inner: Arc<UniInner>,
}

// No Deref<Target = UniInner> — Uni is an opaque handle.
// All field access goes through `self.inner.field` explicitly.

/// Build the cached `Arc<Executor>` template held on `UniInner`.
///
/// Populates every session-constant field on `Executor` so each query can
/// clone this template (cheap Arc bumps + a fresh `warnings` Mutex via the
/// manual `Clone` impl) instead of running `Executor::new` + six setters.
#[allow(clippy::too_many_arguments)]
fn build_executor_template(
    storage: Arc<StorageManager>,
    config: UniConfig,
    writer: Option<Arc<uni_store::runtime::writer::Writer>>,
    l0_manager: Option<Arc<uni_store::runtime::l0_manager::L0Manager>>,
    xervo_runtime: Option<Arc<ModelRuntime>>,
    procedure_registry: Arc<uni_query::ProcedureRegistry>,
    properties: Arc<PropertyManager>,
    df_session_template: Arc<datafusion::execution::context::SessionContext>,
) -> Arc<uni_query::Executor> {
    let mut e = uni_query::Executor::new(storage);
    e.set_config(config);
    e.set_xervo_runtime(xervo_runtime);
    e.set_procedure_registry(procedure_registry);
    if let Some(w) = writer {
        e.set_writer(w);
    }
    // Only reachable when there is no writer; `get_context` prefers the writer's
    // manager. This is what keeps a read-only open's WAL-replayed L0 visible.
    if let Some(m) = l0_manager {
        e.set_l0_manager(m);
    }
    e.set_prop_manager(properties);
    e.set_df_session_template(df_session_template);
    Arc::new(e)
}

impl UniInner {
    /// Build a [`uni_bulk::BulkBackend`] handle bundle from this inner's
    /// fields for the bulk-write driver (`bulk_writer`/`appender`).
    pub(crate) fn bulk_backend(self: &Arc<Self>) -> uni_bulk::BulkBackend {
        uni_bulk::BulkBackend {
            storage: self.storage.clone(),
            writer: self.writer.clone(),
            schema: self.schema.clone(),
            shutdown: self.shutdown_handle.clone(),
            config: self.config.clone(),
        }
    }

    /// Build a derived `UniInner` that shares most of `self`'s state but
    /// swaps in a different storage view (a pinned snapshot or a fork
    /// branch).
    ///
    /// The five arguments are the only fields that differ between a
    /// snapshot/fork inner and `self`: `storage`, `schema`, `properties`,
    /// `writer`, `locy_rule_registry`, and the `executor_template` built
    /// from them. Everything else is either cloned from `self` (registries,
    /// trust config, fork bookkeeping, …) or reset fresh per the spec's
    /// per-view isolation contract (cancellation token, broadcast channel,
    /// metrics counters). Used by both [`Self::at_snapshot`] and
    /// [`Self::at_fork`] so a new field is added in exactly one place.
    #[allow(clippy::too_many_arguments)]
    fn derived_clone(
        &self,
        storage: Arc<StorageManager>,
        schema: Arc<SchemaManager>,
        properties: Arc<PropertyManager>,
        writer: Option<Arc<Writer>>,
        l0_manager: Option<Arc<uni_store::runtime::l0_manager::L0Manager>>,
        locy_rule_registry: Arc<std::sync::RwLock<impl_locy::LocyRuleRegistry>>,
        executor_template: Arc<uni_query::Executor>,
    ) -> UniInner {
        let (commit_tx, _) = tokio::sync::broadcast::channel(256);
        UniInner {
            storage,
            schema,
            properties,
            writer,
            uri: self.uri.clone(),
            l0_manager,
            // A derived (snapshot/fork) inner keeps the parent's allocator
            // handle so a nested fork can still read its HWM (#169).
            id_allocator: self.id_allocator.clone(),
            xervo_runtime: self.xervo_runtime.clone(),
            config: self.config.clone(),
            procedure_registry: self.procedure_registry.clone(),
            plugin_registry: self.plugin_registry.clone(),
            plugins: self.plugins.clone(),
            defer_queue: self.defer_queue.clone(),
            ec_queue: self.ec_queue.clone(),
            scheduler_host: Arc::clone(&self.scheduler_host),
            shutdown_handle: Arc::new(ShutdownHandle::new(Duration::from_secs(30))),
            locy_rule_registry,
            // Fork/snapshot inners must not persist rule mutations: keep them
            // ephemeral so a fork's registrations never touch the primary
            // catalog.
            locy_rule_persister: None,
            start_time: Instant::now(),
            commit_tx,
            write_lease: None,
            plugin_trust: self.plugin_trust.clone(),
            active_session_count: AtomicUsize::new(0),
            total_queries: AtomicU64::new(0),
            total_commits: AtomicU64::new(0),
            custom_functions: self.custom_functions.clone(),
            df_session_template: self.df_session_template.clone(),
            executor_template,
            fork_registry: self.fork_registry.clone(),
            fork_inners: self.fork_inners.clone(),
            inflight_tx_count: Arc::new(AtomicUsize::new(0)),
            cached_l0_mutation_count: AtomicUsize::new(0),
            cached_l0_estimated_size: AtomicUsize::new(0),
            cached_wal_lsn: AtomicU64::new(0),
            _temp_dir: None,
            // Fork/snapshot inners read a different storage layout, so they
            // must not reuse the primary's cached (fork-fusion-shaped) plans.
            plan_cache: Arc::new(std::sync::Mutex::new(crate::api::session::PlanCache::new(
                TX_PLAN_CACHE_CAPACITY,
            ))),
        }
    }

    /// Open a point-in-time view of the database at the given snapshot.
    ///
    /// Returns a new `UniInner` that is pinned to the specified snapshot state.
    /// The returned instance is read-only.
    pub(crate) async fn at_snapshot(&self, snapshot_id: &str) -> Result<UniInner> {
        let manifest = self
            .storage
            .snapshot_manager()
            .load_snapshot(snapshot_id)
            .await
            .map_err(UniError::Internal)?;

        let pinned_storage = Arc::new(self.storage.pinned(manifest));

        let prop_manager = Arc::new(PropertyManager::with_plugin_registry(
            pinned_storage.clone(),
            self.schema.clone(),
            self.properties.cache_size(),
            self.plugin_registry.clone(),
        ));

        // Both `None`s below are load-bearing, not oversight. `create_snapshot`
        // flushes before pinning, so a pinned view's rows are entirely in L1 and
        // the live L0 holds only post-snapshot writes that MUST stay invisible
        // here. Do not "fix" these to the live L0 — see the detached-L0 guard in
        // `ProjectionBuilder::build`, which exempts exactly this case.
        let executor_template = build_executor_template(
            pinned_storage.clone(),
            self.config.clone(),
            None,
            None,
            self.xervo_runtime.clone(),
            self.procedure_registry.clone(),
            prop_manager.clone(),
            self.df_session_template.clone(),
        );
        Ok(self.derived_clone(
            pinned_storage,
            self.schema.clone(),
            prop_manager,
            None,
            None,
            Arc::new(std::sync::RwLock::new(
                impl_locy::LocyRuleRegistry::default(),
            )),
            executor_template,
        ))
    }

    /// Construct a fork-scoped clone of this `UniInner`.
    ///
    /// Mirror of [`Self::at_snapshot`] for forks: the returned inner
    /// reads through the fork's Lance branches via `base_paths`, and
    /// its schema is `primary_schema ⊕ overlay`. In Phase 1 the writer
    /// is `None` — fork-scoped writes are gated at the API layer in
    /// `Session::tx`. Phase 2 will populate `writer` once L0 routing
    /// lands.
    ///
    /// The cancellation token, broadcast channel, and metrics are all
    /// fresh per the spec §4.3–4.6 contract: a forked session has
    /// per-fork notifications, hooks, params, and metrics. The Locy
    /// rule registry is a deep clone of primary's so rule registration
    /// on a forked session does not leak to primary.
    pub(crate) async fn at_fork(&self, scope: Arc<uni_store::fork::ForkScope>) -> Result<UniInner> {
        // Phase 3 (nested forks): `self` may itself be a fork-scoped
        // UniInner, in which case `self.schema` already encodes
        // `primary ⊕ parent_overlay`. Layering the child's overlay on
        // top here gives `primary ⊕ parent_overlay ⊕ child_overlay`
        // without any explicit chain walk — `with_overlay` clones the
        // current manager's view into a fresh merged snapshot
        // (`schema.rs:929-966`), so each level produces its own frozen
        // snapshot at session-open time. Additions made on the parent
        // *after* the child was created stay isolated from the child by
        // construction, which matches the spec's fork-point snapshot
        // isolation.
        let merged_schema = self.schema.with_overlay(&scope.overlay());
        let forked_storage = Arc::new(
            self.storage
                .at_fork_with_schema(scope.clone(), merged_schema.clone()),
        );

        let prop_manager = Arc::new(PropertyManager::with_plugin_registry(
            forked_storage.clone(),
            merged_schema.clone(),
            self.properties.cache_size(),
            self.plugin_registry.clone(),
        ));

        // Deep-copy the rule registry so fork-local rule registrations
        // do not bleed into primary. Mirrors today's `Session::clone`
        // semantics for `rule_registry` (`session.rs:189`).
        let rule_registry = {
            let primary = self
                .locy_rule_registry
                .read()
                .map_err(|e| UniError::Internal(anyhow::anyhow!("rule_registry poisoned: {e}")))?;
            Arc::new(std::sync::RwLock::new(primary.clone()))
        };

        // Phase 2 Day 4: build a fork-scoped Writer so that
        // `forked.tx().commit()` can land mutations on the fork's
        // branches. The Writer uses a per-fork IdAllocator (Day 3),
        // a per-fork WAL stream (Day 5), and the fork-scoped storage's
        // BranchedBackend (Day 2). User writes are still gated at
        // `Session::tx()` until Day 7.
        let forked_writer = uni_store::fork::writer_factory::new_for_fork(
            forked_storage.clone(),
            merged_schema.clone(),
            &scope.fork_id(),
            // Bootstrap the fork's MVCC version floor to the parent's
            // fork-point HWM so in-tx fork reads see inherited rows. WAL
            // replay below advances the counter for the fork's own writes.
            scope.fork_info().fork_point_version_hwm,
            self.config.clone(),
        )
        .await
        .map_err(UniError::Internal)?;

        // Phase 2 Day 6: replay any persisted WAL entries for this
        // fork into the freshly-built L0. Without this, a process
        // restart would silently drop committed-but-not-yet-flushed
        // fork mutations.
        //
        // Gate replay on the fork's own persisted `wal_high_water_mark`
        // (review M2). The fork-scoped SnapshotManager (review C1) records
        // it at each fork flush under `catalog/forks/{fork_id}/latest`; a
        // crash between the durable branch write and complete WAL truncation
        // would otherwise replay already-flushed segments from 0 and
        // double-apply them. A fork that has never flushed (or a pre-fix
        // on-disk fork) has no per-fork snapshot, so we fall back to 0 —
        // correct, since nothing has been moved out of the WAL yet.
        let fork_wal_hwm = forked_storage
            .snapshot_manager()
            .load_latest_snapshot()
            .await
            .map_err(UniError::Internal)?
            .map(|s| s.wal_high_water_mark)
            .unwrap_or(0);
        let replayed = forked_writer
            .replay_wal(fork_wal_hwm)
            .await
            .map_err(UniError::Internal)?;
        if replayed > 0 {
            tracing::info!(
                fork_id = %scope.fork_id(),
                replayed,
                "fork WAL replay restored persisted mutations into L0"
            );
        }

        let forked_writer_arc = Arc::new(forked_writer);
        let executor_template = build_executor_template(
            forked_storage.clone(),
            self.config.clone(),
            Some(forked_writer_arc.clone()),
            Some(Arc::clone(&forked_writer_arc.l0_manager)),
            self.xervo_runtime.clone(),
            self.procedure_registry.clone(),
            prop_manager.clone(),
            self.df_session_template.clone(),
        );
        Ok(self.derived_clone(
            forked_storage,
            merged_schema,
            prop_manager,
            Some(Arc::clone(&forked_writer_arc)),
            Some(Arc::clone(&forked_writer_arc.l0_manager)),
            rule_registry,
            executor_template,
        ))
    }
}

impl Uni {
    /// Borrow this instance's background-job scheduler host.
    ///
    /// The host owns a [`uni_plugin::scheduler::Scheduler`] primitive
    /// driven by a tokio loop spawned at `Uni::build` time. The
    /// preferred Rust entry point is [`Uni::periodic_schedule`], which
    /// routes through the host's `SchedulerControl` impl so the
    /// schedule kind is captured by the durable persistence backend
    /// and survives restart:
    ///
    /// ```no_run
    /// # async fn ex(db: uni_db::Uni) {
    /// use std::time::Duration;
    /// use uni_plugin::QName;
    /// use uni_plugin::traits::background::Schedule;
    ///
    /// db.periodic_schedule(
    ///     QName::new("myorg", "nightly"),
    ///     Schedule::Periodic(Duration::from_secs(86_400)),
    /// );
    /// # }
    /// ```
    ///
    /// The job's [`BackgroundJobProvider`](
    /// uni_plugin::traits::background::BackgroundJobProvider) must
    /// have been registered into the [`uni_plugin::PluginRegistry`]
    /// (via `PluginRegistrar::background_job`) before its qname can
    /// be scheduled.
    #[must_use]
    pub fn scheduler_host(&self) -> &Arc<crate::scheduler::SchedulerHost> {
        &self.inner.scheduler_host
    }

    /// Register a background job to fire on `schedule`.
    ///
    /// This is the Rust analogue of `CALL uni.periodic.schedule(...)`
    /// — the Cypher wrapper procedure registers via this same path.
    /// The job's [`BackgroundJobProvider`](
    /// uni_plugin::traits::background::BackgroundJobProvider) must
    /// already be registered in the [`uni_plugin::PluginRegistry`]
    /// (via `PluginRegistrar::background_job` during plugin
    /// registration); otherwise the scheduler driver logs a warning
    /// on each tick that `id` is due.
    pub fn periodic_schedule(
        &self,
        id: uni_plugin::QName,
        schedule: uni_plugin::traits::background::Schedule,
    ) {
        // Route through the `SchedulerHost`'s `SchedulerControl` impl
        // (not the bare `Scheduler`) so the persistence layer captures
        // the schedule kind for restart durability.
        <crate::scheduler::SchedulerHost as uni_plugin::scheduler::SchedulerControl>::add_scheduled_job(
            &self.inner.scheduler_host,
            id,
            schedule,
        );
    }

    /// Cancel a scheduled job. Returns `true` if a job with this id
    /// was registered; `false` otherwise. Rust analogue of
    /// `CALL uni.periodic.cancel(...)`.
    pub fn periodic_cancel(&self, id: &uni_plugin::QName) -> bool {
        self.inner.scheduler_host.scheduler().cancel(id)
    }

    /// Snapshot every known job and its current lifecycle state.
    /// Rust analogue of `CALL uni.periodic.list()`.
    #[must_use]
    pub fn periodic_list(&self) -> Vec<uni_plugin::scheduler::SchedulerJobRecord> {
        self.inner.scheduler_host.scheduler().list()
    }

    /// Open or create a database at the given path.
    ///
    /// If the database does not exist, it will be created.
    ///
    /// # Arguments
    ///
    /// * `uri` - Local path or object store URI.
    ///
    /// # Returns
    ///
    /// A [`UniBuilder`] to configure and build the database instance.
    pub fn open(uri: impl Into<String>) -> UniBuilder {
        UniBuilder::new(uri.into())
    }

    /// Open an existing database at the given path. Fails if it does not exist.
    pub fn open_existing(uri: impl Into<String>) -> UniBuilder {
        let mut builder = UniBuilder::new(uri.into());
        builder.create_if_missing = false;
        builder
    }

    /// Create a new database at the given path. Fails if it already exists.
    pub fn create(uri: impl Into<String>) -> UniBuilder {
        let mut builder = UniBuilder::new(uri.into());
        builder.fail_if_exists = true;
        builder
    }

    /// Create a temporary database that is deleted when dropped.
    ///
    /// Useful for tests and short-lived processing.
    /// The underlying directory is automatically cleaned up when the `Uni` is dropped.
    pub fn temporary() -> UniBuilder {
        let temp_dir = tempfile::Builder::new()
            .prefix("uni_mem_")
            .tempdir()
            .expect("failed to create temporary directory");
        let uri = temp_dir.path().to_string_lossy().to_string();
        let mut builder = UniBuilder::new(uri);
        builder.temp_dir = Some(temp_dir);
        builder
    }

    /// Open an in-memory database (alias for temporary).
    pub fn in_memory() -> UniBuilder {
        Self::temporary()
    }

    // ── Session Factory (primary entry point for data access) ────────

    /// Create a new Session for data access.
    ///
    /// Sessions are cheap, synchronous, and infallible. All reads go through
    /// sessions, and sessions are the factory for transactions (writes).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use uni_db::Uni;
    /// # async fn example(db: &Uni) -> uni_db::Result<()> {
    /// let session = db.session();
    /// let rows = session.query("MATCH (n) RETURN n LIMIT 10").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn session(&self) -> session::Session {
        session::Session::new(self.inner.clone())
    }

    /// Open a session authenticated as the given credentials (M5i).
    ///
    /// Iterates the registered [`uni_plugin::traits::connector::AuthProvider`]s
    /// in registration order; the first provider whose `scheme()`
    /// matches the credential type is asked to `authenticate`. On
    /// success, the resulting [`uni_plugin::traits::connector::Principal`]
    /// is attached to the session and propagates into downstream
    /// authorization checks.
    ///
    /// # Errors
    ///
    /// Returns [`UniError::AuthenticationFailed`] when no registered
    /// provider matches the credential scheme or the matched
    /// provider's `authenticate` returned an error.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use uni_plugin::traits::connector::Credentials;
    /// let creds = Credentials::Basic {
    ///     username: "alice".into(),
    ///     password: "hunter2".into(),
    /// };
    /// let session = db.session_with_credentials(creds)?;
    /// ```
    pub fn session_with_credentials(
        &self,
        creds: uni_plugin::traits::connector::Credentials,
    ) -> Result<session::Session> {
        let scheme = match &creds {
            uni_plugin::traits::connector::Credentials::Basic { .. } => "basic",
            uni_plugin::traits::connector::Credentials::Bearer(_) => "bearer",
            uni_plugin::traits::connector::Credentials::MtlsCert(_) => "mtls",
        };
        let providers = self.inner.plugin_registry.auth_providers();
        // Try each matching provider in registration order; succeed on
        // the first one that authenticates. This lets a host stack its
        // own provider alongside the built-in one — either may hold the
        // credentials. `matched_any` distinguishes "no provider for this
        // scheme" from "providers were tried and all rejected".
        let mut matched_any = false;
        let mut last_error: Option<String> = None;
        for provider in providers.iter().filter(|p| p.scheme() == scheme) {
            matched_any = true;
            match provider.authenticate(&creds) {
                Ok(principal) => {
                    return Ok(self.session().with_principal(Arc::new(principal)));
                }
                Err(e) => {
                    last_error = Some(e.0);
                }
            }
        }
        if !matched_any {
            return Err(UniError::AuthenticationFailed {
                reason: format!("no AuthProvider registered for scheme `{scheme}`"),
            });
        }
        Err(UniError::AuthenticationFailed {
            reason: last_error.unwrap_or_else(|| "all matching providers rejected".to_owned()),
        })
    }

    /// Create a session template builder for pre-configured session factories.
    ///
    /// Templates pre-compile Locy rules, bind parameters, and attach hooks
    /// once, then cheaply stamp out sessions per-request.
    pub fn session_template(&self) -> template::SessionTemplateBuilder {
        template::SessionTemplateBuilder::new(self.inner.clone())
    }

    // ── Database Metrics ──────────────────────────────────────────────

    /// Snapshot the database-level metrics.
    ///
    /// This is a cheap, synchronous read of cached atomic values.
    /// L0 metrics (`l0_mutation_count`, `l0_estimated_size_bytes`, `wal_lsn`)
    /// reflect the state as of the last successful commit.
    pub fn metrics(&self) -> DatabaseMetrics {
        let schema_version = self.inner.schema.schema().schema_version as u64;
        let compaction_status = self.inner.storage.compaction_status().unwrap_or_default();
        DatabaseMetrics {
            l0_mutation_count: self.inner.cached_l0_mutation_count.load(Ordering::Relaxed),
            l0_estimated_size_bytes: self.inner.cached_l0_estimated_size.load(Ordering::Relaxed),
            schema_version,
            uptime: self.inner.start_time.elapsed(),
            active_sessions: self.inner.active_session_count.load(Ordering::Relaxed),
            l1_run_count: compaction_status.l1_runs,
            write_throttle_pressure: ThrottlePressure::default(),
            compaction_status,
            wal_size_bytes: 0u64,
            wal_lsn: self.inner.cached_wal_lsn.load(Ordering::Relaxed),
            total_queries: self.inner.total_queries.load(Ordering::Relaxed),
            total_commits: self.inner.total_commits.load(Ordering::Relaxed),
        }
    }

    /// Returns the write lease configuration, if any.
    /// Write lease enforcement is Phase 2.
    pub fn write_lease(&self) -> Option<&multi_agent::WriteLease> {
        self.inner.write_lease.as_ref()
    }

    // ── Global Locy Rule Management ───────────────────────────────────

    /// Access the global rule registry for managing pre-compiled Locy rules.
    ///
    /// Rules registered here are cloned into every new Session.
    pub fn rules(&self) -> rule_registry::RuleRegistry<'_> {
        match &self.inner.locy_rule_persister {
            Some(persister) => rule_registry::RuleRegistry::with_persister(
                &self.inner.locy_rule_registry,
                persister,
            ),
            None => rule_registry::RuleRegistry::new(&self.inner.locy_rule_registry),
        }
        .with_plugin_registry(&self.inner.plugin_registry)
    }

    // ── Configuration & Introspection ─────────────────────────────────

    /// Get configuration.
    pub fn config(&self) -> &UniConfig {
        &self.inner.config
    }

    /// Returns the procedure registry for registering test procedures.
    #[doc(hidden)]
    pub fn procedure_registry(&self) -> &Arc<uni_query::ProcedureRegistry> {
        &self.inner.procedure_registry
    }

    /// Returns the framework-wide [`uni_plugin::PluginRegistry`].
    ///
    /// Built once at `Uni::build()` time and populated with `BuiltinPlugin`
    /// (always) and `ApocCorePlugin` (when the `apoc-core` feature is on).
    /// Future user plugins added via [`Uni::add_plugin`] register into the
    /// same instance.
    pub fn plugin_registry(&self) -> &Arc<uni_plugin::PluginRegistry> {
        &self.inner.plugin_registry
    }

    /// Get schema manager.
    #[doc(hidden)]
    pub fn schema_manager(&self) -> Arc<SchemaManager> {
        self.inner.schema.clone()
    }

    #[doc(hidden)]
    pub fn writer(&self) -> Option<Arc<Writer>> {
        self.inner.writer.clone()
    }

    #[doc(hidden)]
    pub fn storage(&self) -> Arc<StorageManager> {
        self.inner.storage.clone()
    }

    /// Flush all uncommitted changes to persistent storage (L1).
    ///
    /// This forces a write of the current in-memory buffer (L0) to columnar files.
    /// It also creates a new snapshot.
    pub async fn flush(&self) -> Result<()> {
        if let Some(writer) = &self.inner.writer {
            writer
                .flush_to_l1(None)
                .await
                .map(|_| ())
                .map_err(UniError::Internal)
        } else {
            Err(UniError::ReadOnly {
                operation: "flush".to_string(),
            })
        }
    }

    /// Create a named point-in-time snapshot of the database.
    ///
    /// Flushes current changes, records the state, and persists the snapshot
    /// under the given name so it can be retrieved later.
    /// Returns the snapshot ID.
    pub async fn create_snapshot(&self, name: &str) -> Result<String> {
        if name.is_empty() {
            return Err(UniError::Internal(anyhow::anyhow!(
                "Snapshot name cannot be empty"
            )));
        }

        let snapshot_id = if let Some(writer) = &self.inner.writer {
            writer
                .flush_to_l1(Some(name.to_string()))
                .await
                .map_err(UniError::Internal)?
        } else {
            return Err(UniError::ReadOnly {
                operation: "create_snapshot".to_string(),
            });
        };

        self.inner
            .storage
            .snapshot_manager()
            .save_named_snapshot(name, &snapshot_id)
            .await
            .map_err(UniError::Internal)?;

        Ok(snapshot_id)
    }

    /// List all available snapshots.
    pub async fn list_snapshots(&self) -> Result<Vec<SnapshotManifest>> {
        let sm = self.inner.storage.snapshot_manager();
        let ids = sm.list_snapshots().await.map_err(UniError::Internal)?;
        let mut manifests = Vec::new();
        for id in ids {
            if let Ok(m) = sm.load_snapshot(&id).await {
                manifests.push(m);
            }
        }
        Ok(manifests)
    }

    /// Restore the database to a specific snapshot.
    ///
    /// **Note**: This currently requires a restart or re-opening of Uni to fully take effect
    /// as it only updates the latest pointer.
    pub async fn restore_snapshot(&self, snapshot_id: &str) -> Result<()> {
        self.inner
            .storage
            .snapshot_manager()
            .set_latest_snapshot(snapshot_id)
            .await
            .map_err(UniError::Internal)
    }

    // ── Compaction ──────────────────────────────────────────────────────

    /// Access compaction operations.
    pub fn compaction(&self) -> compaction::Compaction<'_> {
        compaction::Compaction { inner: &self.inner }
    }

    // ── Indexes ──────────────────────────────────────────────────────────

    /// Access index management operations.
    pub fn indexes(&self) -> indexes::Indexes<'_> {
        indexes::Indexes { inner: &self.inner }
    }

    // ── Custom Functions ──────────────────────────────────────────────

    /// Access custom Cypher function management.
    pub fn functions(&self) -> functions::Functions<'_> {
        functions::Functions { inner: &self.inner }
    }

    /// Shutdown the database gracefully, flushing pending data and stopping background tasks.
    ///
    /// This method flushes any pending data and waits for all background tasks to complete
    /// (with a timeout). After calling this method, the database instance should not be used.
    pub async fn shutdown(self) -> Result<()> {
        self.shutdown_in_place().await
    }

    /// Shuts the database down without consuming it.
    ///
    /// [`Self::shutdown`] takes `self`, which a shared handle (an `Arc<Uni>`
    /// behind a language binding) cannot satisfy — so the Python binding was
    /// calling `flush()` and calling it a shutdown. Real teardown then only
    /// happened at garbage-collection time through `Drop`, which *signals* the
    /// background tasks and does not wait for them; a temporary database's
    /// scratch directory was removed while writers were still finishing, and
    /// survived a few percent of the time.
    ///
    /// # Errors
    /// Propagates a failure to stop the background tasks. A flush failure is
    /// logged rather than propagated, matching the previous behaviour.
    pub async fn shutdown_in_place(&self) -> Result<()> {
        // Flush pending data.
        if let Some(writer) = &self.inner.writer {
            if let Err(e) = writer.flush_to_l1(None).await {
                tracing::error!("Error flushing during shutdown: {}", e);
            }
            // Close the async-flush coordinator's submit channel so its
            // finalizer task exits now. The finalizer's JoinHandle is
            // tracked by `shutdown_handle`, but the loop blocks on
            // `submit_rx.recv()` and never sees the shutdown broadcast — so
            // without this the `shutdown_async` below would await it for the
            // full grace period. `shutdown()` drops the sender (the
            // finalizer then receives `None` and exits) and is idempotent.
            if let Some(coord) = writer.flush_coordinator() {
                coord.shutdown().await;
            }
        }

        self.inner
            .shutdown_handle
            .shutdown_async()
            .await
            .map_err(UniError::Internal)?;

        self.reap_scratch_dir();
        Ok(())
    }

    /// Drops this handle's claim on an `in_memory()` database's scratch
    /// directory; the directory is removed once the last claim is released.
    ///
    /// A temporary database is only in-memory from the caller's side: it is
    /// backed by a `uni_mem_*` directory. Removal used to be left to `TempDir`'s
    /// `Drop`, which calls `remove_dir_all` once and **discards the error**, so
    /// a leak was silent — and the survivors never expire. A suite opening
    /// thousands of databases strands tens per run; on a tmpfs `TMPDIR` that
    /// exhausts *inodes* rather than space, which presents as unrelated
    /// "failed to create temporary directory" errors while `df -h` looks fine.
    ///
    /// Ordering is by ownership: `ScratchDir` is shared with the background
    /// tasks that can write into the directory, so the removal cannot be
    /// undone by a task that is still running. See `ScratchDir`.
    fn reap_scratch_dir(&self) {
        // Safe to remove eagerly here, and only here: the caller
        // (`shutdown_in_place`) has already awaited every tracked task via
        // `shutdown_async`, so nothing is left that could recreate the
        // directory. Deferring to `ScratchDir::drop` instead would leave the
        // directory alive for as long as the handle is — which for a Python
        // `with Uni.temporary() as db:` block means past the block's end, until
        // garbage collection. `ScratchDir::drop` remains the backstop for the
        // bare-drop path, where nothing has been awaited.
        if let Some(dir) = self.inner._temp_dir.as_ref() {
            reap_dir(dir.path());
        }
    }

    /// The filesystem location backing this database.
    ///
    /// For a `temporary()` / `in_memory()` database this is the `uni_mem_*`
    /// scratch directory. Exposed so a caller who never calls [`Uni::shutdown`]
    /// can still account for the directory — without it there is no way to
    /// discover the path short of globbing `$TMPDIR`, which is unsafe when
    /// another process owns one.
    #[must_use]
    pub fn uri(&self) -> &str {
        &self.inner.uri
    }
}

/// Removes a temporary database's scratch directory, retrying briefly.
///
/// Shared by [`Uni::shutdown_in_place`] and `UniInner`'s `Drop` so that both
/// the explicit and the implicit teardown path get the same retried removal.
fn reap_dir(path: &std::path::Path) {
    for attempt in 0..5 {
        match std::fs::remove_dir_all(path) {
            Ok(()) => return,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) if attempt == 4 => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "could not remove the temporary database directory; it will \
                     be left behind. Repeated leaks exhaust inodes on a tmpfs TMPDIR."
                );
            }
            Err(_) => std::thread::sleep(std::time::Duration::from_millis(20 * (attempt + 1))),
        }
    }
}

impl Drop for Uni {
    fn drop(&mut self) {
        self.inner.shutdown_handle.shutdown_blocking();
        self.await_scratch_claims();
        tracing::debug!("Uni dropped, shutdown signal sent");
    }
}

impl Uni {
    /// Give the background tasks a bounded chance to release their scratch-dir
    /// claims before this handle goes away.
    ///
    /// Ordering removal by ownership (see `ScratchDir`) is correct only while
    /// something still runs the tasks that release the claims. At process exit
    /// it may not: the embedder's runtime is abandoned, the tasks never wake,
    /// the `Arc` is never released, and the directory is then removed by
    /// *nothing at all* — strictly worse than the racy removal it replaced.
    /// Measured at ~15% of exits with a disk-backed `TMPDIR`; a tmpfs `/tmp`
    /// drains fast enough to hide it, which is why the first Python probe of
    /// this read 0/200.
    ///
    /// Only runs off a runtime thread — the embedder-teardown case. Inside a
    /// runtime the tasks are being driven already, and on a `current_thread`
    /// runtime blocking here would deadlock the very tasks being waited on.
    fn await_scratch_claims(&self) {
        let Some(scratch) = self.inner._temp_dir.as_ref() else {
            return;
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            return;
        }
        // The flush-coordinator finalizer blocks on its submit channel and
        // never sees the shutdown broadcast, so it would hold its claim until
        // the deadline every single time — turning this bounded wait into an
        // unconditional one. `shutdown_in_place` closes the channel for the
        // same reason; this is the synchronous half of that.
        if let Some(writer) = self.inner.writer.as_ref()
            && let Some(coord) = writer.flush_coordinator()
        {
            coord.close_submit_channel();
        }
        // One claim is `UniInner`'s own; any excess belongs to a live task.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while Arc::strong_count(scratch) > 1 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

/// Tracks a background task, holding a claim on the scratch directory until it
/// exits.
///
/// Every tracked task is a potential writer, and several of them write
/// precisely *because* shutdown was signalled — the auto-flush task performs a
/// final flush on its way out. Teardown removes the directory as soon as the
/// last claim is released, so wrapping each handle in a claim is what orders
/// the removal after the last writer instead of racing it. Issue #167.
fn track_task_with_scratch_claim(
    shutdown_handle: &ShutdownHandle,
    scratch: Option<&Arc<ScratchDir>>,
    handle: tokio::task::JoinHandle<()>,
) {
    let Some(claim) = scratch.map(Arc::clone) else {
        shutdown_handle.track_task(handle);
        return;
    };
    shutdown_handle.track_task(tokio::spawn(async move {
        let _claim = claim;
        let _ = handle.await;
    }));
}

/// Owns a temporary database's scratch directory and removes it when the last
/// holder drops.
///
/// Held as an `Arc` by `UniInner` **and** by every background task that can
/// write into the directory. That is the whole point: on the bare-drop path
/// (`db = Uni.temporary()` with no `with` block, from Python) `Drop for Uni`
/// only calls `shutdown_blocking`, which sends a broadcast and awaits nothing.
/// With the guard owned solely by `UniInner`, teardown removed the directory
/// *successfully* and the auto-flush task — waking on that very same broadcast
/// to perform its final flush — then wrote a catalog manifest and recreated the
/// tree. Measured at 39/40 drops. Sharing the guard orders the removal after
/// the last writer instead of racing it. Issue #167.
pub(crate) struct ScratchDir {
    dir: TempDir,
}

impl ScratchDir {
    pub(crate) fn new(dir: TempDir) -> Self {
        Self { dir }
    }

    pub(crate) fn path(&self) -> &std::path::Path {
        self.dir.path()
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        reap_dir(self.dir.path());
    }
}

impl Drop for UniInner {
    /// Reap the scratch directory on the implicit teardown path.
    ///
    /// A caller who never calls [`Uni::shutdown`] — `db = Uni.temporary()` with
    /// no `with` block, from Python — reached only `Drop for Uni`, whose
    /// `shutdown_blocking` sends a broadcast and awaits nothing. Cleanup then
    /// fell through to `TempDir`'s own `Drop`: one un-retried `remove_dir_all`
    /// whose error is discarded. That call walks and then unlinks, so a
    /// background flush landing mid-walk yields `ENOTEMPTY` and the directory
    /// survives silently — measured at ~39/40 drops before this ran. Issue #167.
    ///
    /// This sits on `UniInner` rather than on `Uni` deliberately: it must run
    /// when the *last* `Arc` dies, not when one handle goes out of scope while
    /// a `Session` still holds a reference to the data underneath it.
    fn drop(&mut self) {
        // Releasing the `Arc` is the whole teardown: `ScratchDir::drop` removes
        // the directory once no background task still holds a claim.
        drop(self._temp_dir.take());
    }
}

/// Builder for configuring and opening a `Uni` database instance.
#[must_use = "builders do nothing until .build() is called"]
pub struct UniBuilder {
    uri: String,
    config: UniConfig,
    schema_file: Option<PathBuf>,
    xervo_catalog: Option<Vec<ModelAliasSpec>>,
    /// Pre-built Xervo runtime (bypasses catalog-based builder when set).
    prebuilt_xervo_runtime: Option<Arc<ModelRuntime>>,
    hybrid_remote_url: Option<String>,
    cloud_config: Option<CloudStorageConfig>,
    create_if_missing: bool,
    fail_if_exists: bool,
    read_only: bool,
    write_lease: Option<multi_agent::WriteLease>,
    plugin_trust: Arc<plugin_trust::PluginTrustConfig>,
    temp_dir: Option<TempDir>,
    /// When true, persisted Locy rules that no longer compile are skipped
    /// (with a warning) on open instead of failing the open.
    skip_invalid_locy_rules: bool,
}

impl UniBuilder {
    /// Creates a new builder for the given URI.
    pub fn new(uri: String) -> Self {
        Self {
            uri,
            config: UniConfig::default(),
            schema_file: None,
            xervo_catalog: None,
            prebuilt_xervo_runtime: None,
            hybrid_remote_url: None,
            cloud_config: None,
            create_if_missing: true,
            fail_if_exists: false,
            read_only: false,
            write_lease: None,
            plugin_trust: Arc::new(plugin_trust::PluginTrustConfig::default()),
            temp_dir: None,
            skip_invalid_locy_rules: false,
        }
    }

    /// Skips persisted Locy rules that no longer compile, instead of failing.
    ///
    /// By default, opening a database whose `catalog/locy_rules.json` contains
    /// a rule that no longer compiles (for example after a grammar change)
    /// fails with an error naming the offending rule. Enabling this skips such
    /// rules with a warning and retains them in the catalog file, so a fixed
    /// binary can recover them.
    pub fn skip_invalid_locy_rules(mut self, skip: bool) -> Self {
        self.skip_invalid_locy_rules = skip;
        self
    }

    /// Load schema from JSON file on initialization.
    pub fn schema_file(mut self, path: impl AsRef<Path>) -> Self {
        self.schema_file = Some(path.as_ref().to_path_buf());
        self
    }

    /// Set Uni-Xervo catalog explicitly.
    pub fn xervo_catalog(mut self, catalog: Vec<ModelAliasSpec>) -> Self {
        self.xervo_catalog = Some(catalog);
        self
    }

    /// Set a pre-built Xervo runtime directly.
    ///
    /// This bypasses the catalog-based provider registration and uses the
    /// provided runtime as-is. Useful for testing with mock providers or
    /// for advanced scenarios where the caller controls runtime construction.
    ///
    /// Mutually exclusive with [`xervo_catalog()`](Self::xervo_catalog) —
    /// when both are set, this takes precedence.
    pub fn xervo_runtime(mut self, runtime: Arc<ModelRuntime>) -> Self {
        self.prebuilt_xervo_runtime = Some(runtime);
        self
    }

    /// Configure remote storage for data, keeping local path for WAL/IDs.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// use uni_common::CloudStorageConfig;
    ///
    /// let config = CloudStorageConfig::S3 {
    ///     bucket: "my-bucket".to_string(),
    ///     region: Some("us-east-1".to_string()),
    ///     endpoint: None,
    ///     access_key_id: None,
    ///     secret_access_key: None,
    ///     session_token: None,
    ///     virtual_hosted_style: false,
    /// };
    ///
    /// let db = Uni::open("./local_meta")
    ///     .remote_storage("s3://my-bucket/graph-data", config)
    ///     .build()
    ///     .await?;
    /// ```
    pub fn remote_storage(mut self, remote_url: &str, config: CloudStorageConfig) -> Self {
        self.hybrid_remote_url = Some(remote_url.to_string());
        self.cloud_config = Some(config);
        self
    }

    /// Open the database in read-only mode.
    ///
    /// In read-only mode, no writer is created. All write operations
    /// (`tx()`, `execute()`, `bulk_writer()`, `appender()`) will return
    /// `ReadOnly` errors. Reads work normally.
    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    /// Set the write lease strategy for multi-agent access.
    ///
    /// This configures how write access is coordinated when multiple
    /// processes share the same database.
    pub fn write_lease(mut self, lease: multi_agent::WriteLease) -> Self {
        self.write_lease = Some(lease);
        self
    }

    /// Set the host plugin trust policy (signature enforcement + trust root).
    ///
    /// Applies to externally-loaded plugins (`add_plugin` and, as the
    /// signing subsystem lands, the WASM/Extism/Rhai/Python loaders).
    /// Compile-time built-in plugins are implicitly trusted. The default
    /// is [`SignaturePolicy::Disabled`](uni_plugin::verify::SignaturePolicy)
    /// with an empty trust root — accept everything, identical to prior
    /// behavior.
    pub fn plugin_trust(mut self, cfg: plugin_trust::PluginTrustConfig) -> Self {
        self.plugin_trust = Arc::new(cfg);
        self
    }

    /// Configure database options using `UniConfig`.
    pub fn config(mut self, config: UniConfig) -> Self {
        self.config = config;
        self
    }

    /// Open the database (async).
    pub async fn build(mut self) -> Result<Uni> {
        let uri = self.uri.clone();
        // Share one guard between `UniInner` and every background task that can
        // write into the scratch directory, so removal is ordered after the
        // last writer rather than racing it. See `ScratchDir` (issue #167).
        let scratch_dir: Option<Arc<ScratchDir>> =
            self.temp_dir.take().map(|d| Arc::new(ScratchDir::new(d)));
        let is_remote_uri = uri.contains("://");
        let is_hybrid = self.hybrid_remote_url.is_some();

        if is_hybrid && is_remote_uri {
            return Err(UniError::Internal(anyhow::anyhow!(
                "Hybrid mode requires a local path as primary URI, found: {}",
                uri
            )));
        }

        let (storage_uri, data_store, local_store_opt) = if is_hybrid {
            let remote_url = self.hybrid_remote_url.as_ref().unwrap();

            // Remote Store (Data) - use explicit cloud_config if provided
            let remote_store: Arc<dyn ObjectStore> = if let Some(cloud_cfg) = &self.cloud_config {
                build_cloud_store(cloud_cfg).map_err(UniError::Internal)?
            } else {
                let url = url::Url::parse(remote_url).map_err(|e| {
                    UniError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        e.to_string(),
                    ))
                })?;
                let (os, _path) =
                    object_store::parse_url(&url).map_err(|e| UniError::Internal(e.into()))?;
                Arc::from(os)
            };

            // Local Store (WAL, IDs)
            let path = PathBuf::from(&uri);
            if path.exists() {
                if self.fail_if_exists {
                    return Err(UniError::Internal(anyhow::anyhow!(
                        "Database already exists at {}",
                        uri
                    )));
                }
            } else {
                if !self.create_if_missing {
                    return Err(UniError::NotFound { path: path.clone() });
                }
                std::fs::create_dir_all(&path).map_err(UniError::Io)?;
            }

            let local_store = Arc::new(
                LocalFileSystem::new_with_prefix(&path).map_err(|e| UniError::Io(e.into()))?,
            );

            // For hybrid, storage_uri is the remote URL (since StorageManager loads datasets from there)
            // But we must provide the correct store to other components manually.
            (
                remote_url.clone(),
                remote_store,
                Some(local_store as Arc<dyn ObjectStore>),
            )
        } else if is_remote_uri {
            // Remote Only - use explicit cloud_config if provided
            let remote_store: Arc<dyn ObjectStore> = if let Some(cloud_cfg) = &self.cloud_config {
                build_cloud_store(cloud_cfg).map_err(UniError::Internal)?
            } else {
                let url = url::Url::parse(&uri).map_err(|e| {
                    UniError::Io(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        e.to_string(),
                    ))
                })?;
                let (os, _path) =
                    object_store::parse_url(&url).map_err(|e| UniError::Internal(e.into()))?;
                Arc::from(os)
            };

            (uri.clone(), remote_store, None)
        } else {
            // Local Only
            let path = PathBuf::from(&uri);
            let storage_path = path.join("storage");

            if path.exists() {
                if self.fail_if_exists {
                    return Err(UniError::Internal(anyhow::anyhow!(
                        "Database already exists at {}",
                        uri
                    )));
                }
            } else {
                if !self.create_if_missing {
                    return Err(UniError::NotFound { path: path.clone() });
                }
                std::fs::create_dir_all(&path).map_err(UniError::Io)?;
            }

            // Ensure storage directory exists
            if !storage_path.exists() {
                std::fs::create_dir_all(&storage_path).map_err(UniError::Io)?;
            }

            let store = Arc::new(
                LocalFileSystem::new_with_prefix(&path).map_err(|e| UniError::Io(e.into()))?,
            );
            (
                storage_path.to_string_lossy().to_string(),
                store.clone() as Arc<dyn ObjectStore>,
                Some(store as Arc<dyn ObjectStore>),
            )
        };

        // Canonical schema location in metadata catalog.
        let schema_obj_path = object_store::path::Path::from("catalog/schema.json");
        // Legacy schema location used by older builds.
        let legacy_schema_obj_path = object_store::path::Path::from("schema.json");

        // Backward-compatible schema path migration:
        // if catalog/schema.json is missing but root schema.json exists,
        // copy root schema.json to catalog/schema.json.
        let has_catalog_schema = match data_store.get(&schema_obj_path).await {
            Ok(_) => true,
            Err(object_store::Error::NotFound { .. }) => false,
            Err(e) => return Err(UniError::Internal(e.into())),
        };
        if !has_catalog_schema {
            match data_store.get(&legacy_schema_obj_path).await {
                Ok(result) => {
                    let bytes = result
                        .bytes()
                        .await
                        .map_err(|e| UniError::Internal(e.into()))?;
                    data_store
                        .put(&schema_obj_path, bytes.into())
                        .await
                        .map_err(|e| UniError::Internal(e.into()))?;
                    info!(
                        legacy = %legacy_schema_obj_path,
                        target = %schema_obj_path,
                        "Migrated legacy schema path to catalog path"
                    );
                }
                Err(object_store::Error::NotFound { .. }) => {}
                Err(e) => return Err(UniError::Internal(e.into())),
            }
        }

        // Load schema (SchemaManager::load creates a default if missing)
        // Schema is always in data_store (Remote or Local)
        let schema_manager = Arc::new(
            SchemaManager::load_from_store(data_store.clone(), &schema_obj_path)
                .await
                .map_err(UniError::Internal)?,
        );

        // Hoisted above the persisted-Locy-rule load below, which needs the
        // registry to compile rules against a registry-backed monotonicity
        // oracle. Nothing between here and its previous position depended on
        // it, and it still precedes the `Arc::new(storage)` wrap that
        // `set_plugin_registry` requires.
        // Plugin registry is built early so `PropertyManager` can
        // share it for registry-dispatched CRDT merges. Built-ins are
        // registered against this same Arc below; the registry is
        // shared by-reference, so the registrations are visible to
        // every later consumer.
        //
        // Built BEFORE the StorageManager is wrapped in `Arc` so the same
        // registry can be stamped onto it via `set_plugin_registry`, wiring
        // the durable CRDT merge paths (compaction, L0 flush) to the same
        // provider set that governs `PropertyManager`. Behavior-preserving
        // when no `CrdtKindProvider` is registered (native `try_merge`).
        let plugin_registry = Arc::new(uni_plugin::PluginRegistry::new());
        // M11 A.2: pass the data directory so `SystemLabelPersistence`
        // can be wired as the meta-plugin persistence backend. Remote /
        // object-store URIs (those containing "://") have no local
        // sidecar root — for those, persistence falls back to
        // `NullPersistence`.
        let persistence_data_path: Option<std::path::PathBuf> = if is_remote_uri {
            None
        } else {
            Some(std::path::PathBuf::from(&uri))
        };
        let custom_persistence_sink =
            register_builtin_plugins(&plugin_registry, persistence_data_path.as_deref()).expect(
                "BuiltinPlugin / ApocCorePlugin registration must succeed against fresh registry",
            );

        // Load and recompile persisted Locy rules (catalog/locy_rules.json).
        // A missing file yields an empty registry; a rule that no longer
        // compiles fails the open unless `skip_invalid_locy_rules` is set.
        let locy_rules_obj_path = object_store::path::Path::from("catalog/locy_rules.json");
        let persisted_locy_sources =
            locy_rule_catalog::LocyRulePersister::load(data_store.clone(), &locy_rules_obj_path)
                .await?;
        let loaded_locy_registry = impl_locy::build_locy_registry_from_persisted(
            &persisted_locy_sources,
            self.skip_invalid_locy_rules,
            &plugin_registry,
        )?;
        let locy_rule_persister = Arc::new(locy_rule_catalog::LocyRulePersister::new(
            data_store.clone(),
            locy_rules_obj_path,
        ));

        let lancedb_storage_options = self
            .cloud_config
            .as_ref()
            .map(Self::cloud_config_to_lancedb_storage_options);

        let mut storage = if is_hybrid || is_remote_uri {
            // Preserve explicit cloud settings (endpoint, credentials, path style)
            // by reusing the constructed remote store.
            StorageManager::new_with_store_and_storage_options(
                &storage_uri,
                data_store.clone(),
                schema_manager.clone(),
                self.config.clone(),
                lancedb_storage_options.clone(),
            )
            .await
            .map_err(UniError::Internal)?
        } else {
            // Local mode keeps using a storage-path-scoped local store.
            StorageManager::new_with_config(
                &storage_uri,
                schema_manager.clone(),
                self.config.clone(),
            )
            .await
            .map_err(UniError::Internal)?
        };

        // Stamp the registry onto the owned StorageManager (before it is
        // shared) so compaction and L0 flush route custom CRDT merges through
        // it, matching the `PropertyManager` wiring below.
        storage.set_plugin_registry(Arc::clone(&plugin_registry));

        // A read-only handle does not own the writer, so its adjacency CSR is
        // not kept current by `insert_edge` and must be revalidated per read.
        // Confined to this case because revalidation costs a Lance manifest
        // read per source table per query (~0.9ms on a small traversal), and
        // the writer-owning handle provably does not need it. Issue #168.
        storage.set_requires_adjacency_revalidation(self.read_only);

        let storage = Arc::new(storage);

        // Create shutdown handle
        let shutdown_handle = Arc::new(ShutdownHandle::new(Duration::from_secs(30)));

        // Start background compaction with shutdown signal
        let compaction_handle = storage
            .clone()
            .start_background_compaction(shutdown_handle.subscribe());
        track_task_with_scratch_claim(&shutdown_handle, scratch_dir.as_ref(), compaction_handle);

        // Initialize property manager
        let prop_cache_capacity = self.config.cache_size / 1024;

        let prop_manager = Arc::new(PropertyManager::with_plugin_registry(
            storage.clone(),
            schema_manager.clone(),
            prop_cache_capacity,
            plugin_registry.clone(),
        ));

        // Setup stores for WAL and IdAllocator (needed for version recovery check)
        let id_store = local_store_opt
            .clone()
            .unwrap_or_else(|| data_store.clone());
        let wal_store = local_store_opt
            .clone()
            .unwrap_or_else(|| data_store.clone());

        // Reconcile an interrupted bulk load before reading the latest snapshot:
        // a crash between the per-label and main table commits would otherwise
        // leave them divergent. Recovery rolls an uncommitted load back, or rolls
        // a committed-but-unfinalized one forward (it may flip the latest pointer,
        // so it must run first). A no-op when no marker is present (H9).
        uni_bulk::recover_interrupted_bulk_load(&storage)
            .await
            .map_err(UniError::Internal)?;

        // Determine start version and WAL high water mark from latest snapshot.
        // Detects and recovers from a lost manifest pointer.
        let latest_snapshot = storage
            .snapshot_manager()
            .load_latest_snapshot()
            .await
            .map_err(UniError::Internal)?;

        let (start_version, wal_high_water_mark) = if let Some(ref snapshot) = latest_snapshot {
            (
                snapshot.version_high_water_mark + 1,
                snapshot.wal_high_water_mark,
            )
        } else {
            // No latest snapshot — fresh DB or lost manifest?
            let has_manifests = storage
                .snapshot_manager()
                .has_any_manifests()
                .await
                .unwrap_or(false);

            let wal_check =
                WriteAheadLog::new(wal_store.clone(), object_store::path::Path::from("wal"));
            let has_wal = wal_check.has_segments().await.unwrap_or(false);

            if has_manifests {
                // Manifests exist but latest pointer is missing — try to recover from manifests
                let snapshot_ids = storage
                    .snapshot_manager()
                    .list_snapshots()
                    .await
                    .map_err(UniError::Internal)?;
                if let Some(last_id) = snapshot_ids.last() {
                    let manifest = storage
                        .snapshot_manager()
                        .load_snapshot(last_id)
                        .await
                        .map_err(UniError::Internal)?;
                    tracing::warn!(
                        "Latest snapshot pointer missing but found manifest '{}'. \
                         Recovering version {}.",
                        last_id,
                        manifest.version_high_water_mark
                    );
                    (
                        manifest.version_high_water_mark + 1,
                        manifest.wal_high_water_mark,
                    )
                } else {
                    return Err(UniError::Internal(anyhow::anyhow!(
                        "Snapshot manifests directory exists but contains no valid manifests. \
                         Possible data corruption."
                    )));
                }
            } else if has_wal {
                // WAL exists but no manifests at all — data exists but unrecoverable version
                return Err(UniError::Internal(anyhow::anyhow!(
                    "Database has WAL segments but no snapshot manifest. \
                     Cannot safely determine version counter -- starting at 0 would cause \
                     version conflicts and data corruption. \
                     Restore the snapshot manifest or delete WAL to start fresh."
                )));
            } else {
                // Truly fresh database
                (0, 0)
            }
        };

        let allocator = Arc::new(
            IdAllocator::new(
                id_store,
                object_store::path::Path::from("id_allocator.json"),
                1000,
            )
            .await
            .map_err(UniError::Internal)?,
        );

        // When WAL is enabled the construction is identical for every
        // storage layout (remote-only, hybrid, or local): the only
        // difference is which `wal_store` was resolved above, and
        // `local_store` maps to the FS even behind the ObjectStore trait.
        // For local layouts the data directory is passed as the WAL's
        // local root, enabling fsync-on-flush (LocalFileSystem `put` does
        // not fsync; without it a power loss can drop acknowledged
        // commits). Remote layouts rely on the PUT ack.
        let wal = if self.config.wal_enabled {
            Some(Arc::new(
                WriteAheadLog::new(wal_store, object_store::path::Path::from("wal"))
                    .with_local_root(persistence_data_path.clone()),
            ))
        } else {
            None
        };

        let writer = Arc::new(
            Writer::new_with_config(
                storage.clone(),
                schema_manager.clone(),
                start_version,
                self.config.clone(),
                wal,
                Some(allocator),
            )
            .await
            .map_err(UniError::Internal)?,
        );

        // Per-alias embedding-head requirements across both Vector (dense/multi-vector)
        // and Sparse indexes that carry an auto-embed config.
        let schema_for_embed = schema_manager.schema();
        let required_embed_heads =
            uni_store::runtime::embed_caps::required_embed_heads(&schema_for_embed);

        // A prebuilt runtime (`.xervo_runtime(...)`) already carries its catalog, so it
        // satisfies the embedding-alias requirement just as a `.xervo_catalog(...)` does.
        if !required_embed_heads.is_empty()
            && self.xervo_catalog.is_none()
            && self.prebuilt_xervo_runtime.is_none()
        {
            return Err(UniError::Internal(anyhow::anyhow!(
                "Uni-Xervo catalog is required because schema has vector indexes with embedding aliases"
            )));
        }

        let xervo_runtime = if let Some(runtime) = self.prebuilt_xervo_runtime {
            // A prebuilt runtime carries its own catalog, so the alias set can be
            // verified but the per-alias capability check below cannot: that needs
            // `spec.task`, and uni-xervo keeps `lookup_spec` private. An alias bound
            // to the wrong task therefore still surfaces at first embed rather than
            // here. Presence is the half that is reachable, and it catches the likely
            // misuse — sharing a runtime into a database whose schema needs an alias
            // the runtime's catalog never had.
            for alias in required_embed_heads.keys() {
                if !runtime.contains_alias(alias).await {
                    return Err(UniError::Internal(anyhow::anyhow!(
                        "Missing Uni-Xervo alias '{}' referenced by vector index embedding config",
                        alias
                    )));
                }
            }
            Some(runtime)
        } else if let Some(catalog) = self.xervo_catalog {
            // Capability check (#129/#130): each alias's task must produce — from a text
            // source — every embedding head its bound columns require. A hybrid alias
            // (e.g. BGE-M3) covers any subset; a single-task alias covers only its own
            // head. This replaces the old blanket `task != Embed` check, which wrongly
            // rejected hybrid / multi-vector / sparse aliases at reopen, and additionally
            // validates sparse aliases (previously unchecked).
            for (alias, required) in &required_embed_heads {
                let spec = catalog.iter().find(|s| &s.alias == alias).ok_or_else(|| {
                    UniError::Internal(anyhow::anyhow!(
                        "Missing Uni-Xervo alias '{}' referenced by vector index embedding config",
                        alias
                    ))
                })?;
                let supported = uni_store::runtime::embed_caps::text_embedding_heads(spec.task);
                if !supported.contains(required.heads) {
                    let offending: Vec<&str> = required
                        .columns
                        .iter()
                        .filter(|(_, head)| !supported.contains(*head))
                        .map(|(col, _)| col.as_str())
                        .collect();
                    return Err(UniError::Internal(anyhow::anyhow!(
                        "Uni-Xervo alias '{alias}' (task {:?}) cannot produce the embedding \
                         head(s) required by column(s) {:?}: alias supports {:?}, columns \
                         require {:?}",
                        spec.task,
                        offending,
                        supported,
                        required.heads
                    )));
                }
            }

            Some(crate::api::xervo::build_model_runtime(catalog).await?)
        } else {
            None
        };

        if let Some(ref runtime) = xervo_runtime {
            writer
                .set_xervo_runtime(runtime.clone())
                .map_err(UniError::Internal)?;
        }

        // Replay the WAL to restore *committed* mutations that had not yet been
        // flushed to L1. The WAL is appended at commit time, and only entries
        // with LSN > wal_high_water_mark (the snapshot manifest's mark) are
        // replayed, so this is exactly the committed-but-unflushed suffix and
        // cannot double-apply.
        {
            let replayed = writer
                .replay_wal(wal_high_water_mark)
                .await
                .map_err(UniError::Internal)?;
            if replayed > 0 {
                info!("WAL recovery: replayed {} mutations", replayed);
            }
        }

        // Wire up IndexRebuildManager for post-flush automatic rebuild scheduling
        if !self.read_only && self.config.index_rebuild.auto_rebuild_enabled {
            let rebuild_manager = Arc::new(
                uni_store::storage::IndexRebuildManager::new(
                    storage.clone(),
                    schema_manager.clone(),
                    self.config.index_rebuild.clone(),
                )
                .await
                .map_err(UniError::Internal)?,
            );

            let handle = rebuild_manager
                .clone()
                .start_background_worker(shutdown_handle.subscribe());
            track_task_with_scratch_claim(&shutdown_handle, scratch_dir.as_ref(), handle);

            writer
                .set_index_rebuild_manager(rebuild_manager)
                .map_err(UniError::Internal)?;
        }

        // Start background flush checker for time-based auto-flush
        // A read-only open must not write L1: the auto-flush task calls
        // `flush_to_l1` on tick and on shutdown.
        if !self.read_only
            && let Some(interval) = self.config.auto_flush_interval
        {
            let writer_clone = writer.clone();
            let mut shutdown_rx = shutdown_handle.subscribe();
            let handle = tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            if let Err(e) = writer_clone.check_flush().await {
                                tracing::warn!("Background flush check failed: {}", e);
                            }
                        }
                        _ = shutdown_rx.recv() => {
                            tracing::info!("Auto-flush shutting down, performing final flush");
                            let _ = writer_clone.flush_to_l1(None).await;
                            break;
                        }
                    }
                }
            });

            track_task_with_scratch_claim(&shutdown_handle, scratch_dir.as_ref(), handle);
        }

        // Track the FlushCoordinator's single-task finalizer (if async
        // flush is enabled) so Uni::shutdown_blocking awaits its exit.
        // Without this, a graceful shutdown may proceed before the
        // finalizer drains its in-heap submissions — losing some
        // recently-streamed flushes (data is still recoverable via
        // WAL replay on next start, but we'd rather not leak fragments
        // unnecessarily).
        if let Some(coord) = writer.flush_coordinator()
            && let Some(handle) = coord.take_finalizer_handle()
        {
            track_task_with_scratch_claim(&shutdown_handle, scratch_dir.as_ref(), handle);
        }

        let (commit_tx, _) = tokio::sync::broadcast::channel(256);
        // Lift the L0 tier out BEFORE the writer is dropped on a read-only open.
        // The WAL replay above landed committed-but-unflushed mutations in it;
        // dropping the only handle would make every read on this database
        // silently miss them — a partially-flushed database would answer from
        // its flushed half alone, with no error.
        let l0_manager_field = Some(Arc::clone(&writer.l0_manager));
        // Lift the id allocator out for the same reason as the L0 tier above.
        // Forking a read-only handle still needs the parent's VID/EID
        // high-water marks to bootstrap the fork's allocator above them;
        // without them the fork started allocating at 0 and its first writes
        // were shadowed by the parent's pre-existing rows at the same VID
        // (issue #169, and exactly the collision `fork::id_alloc`'s rustdoc
        // warns about).
        let id_allocator_field = Some(Arc::clone(&writer.allocator));
        let writer_field = if self.read_only { None } else { Some(writer) };

        // Build the fork registry from the metadata store (the same
        // store the snapshot manager uses), then run recovery before
        // any session is exposed. Recovery resumes any partial fork
        // create or drop left behind by an earlier crash.
        let fork_registry = Arc::new(
            uni_store::fork::ForkRegistryHandle::load(data_store.clone())
                .await
                .map_err(|e| match e {
                    UniError::Internal(inner) => UniError::Internal(inner),
                    other => UniError::Internal(anyhow::anyhow!(other.to_string())),
                })?,
        );
        // Phase 4a: apply the configured fork budget cap.
        fork_registry.set_max_forks(self.config.max_forks).await;
        let recovery_store = storage.store();
        let recovery_branching = storage.backend().branching();
        // L3: pass the schema-derived candidate dataset names so recovery can
        // reconstruct and reclaim zombie `fork_{id}_{dataset}` branches left
        // by a create that crashed before recording them in the registry.
        let recovery_candidates =
            crate::api::fork::fork_candidate_dataset_names(&schema_manager.schema());
        let recovered = uni_store::fork::recovery::recover_forks(
            &fork_registry,
            &recovery_store,
            &recovery_candidates,
            recovery_branching.as_deref(),
        )
        .await
        .map_err(|e| match e {
            UniError::Internal(inner) => UniError::Internal(inner),
            other => UniError::Internal(anyhow::anyhow!(other.to_string())),
        })?;
        if recovered > 0 {
            tracing::info!(reconciled = recovered, "fork registry recovery completed");
        }

        // Phase 4a: capture sweeper config + a shutdown subscription
        // before the config is consumed into UniInner.
        let sweeper_interval = self.config.fork_sweeper_interval;
        let sweeper_disabled = self.config.disable_fork_sweeper;
        let sweeper_shutdown_rx = shutdown_handle.subscribe();
        // Phase 5a-impl Step 7: same for the fork index builder.
        let index_builder_interval = self.config.fork_index_builder_interval;
        let index_builder_threshold = self.config.fork_index_build_threshold;
        let index_builder_disabled = self.config.disable_fork_index_builder;
        let index_builder_shutdown_rx = shutdown_handle.subscribe();

        // Build the cached DataFusion SessionContext template once with all
        // Cypher UDFs pre-registered. Subsequent queries clone this Arc
        // instead of paying ~140 µs to construct a fresh SessionContext and
        // re-register the UDFs every call.
        let df_session_template = {
            let ctx = datafusion::execution::context::SessionContext::new();
            uni_query_functions::df_udfs::register_cypher_udfs(&ctx)
                .map_err(|e| UniError::Internal(anyhow::anyhow!(e)))?;
            Arc::new(ctx)
        };

        // (The framework-wide plugin registry was built earlier in
        // this function so `PropertyManager` could share it for
        // registry-dispatched CRDT merges. `register_builtin_plugins`
        // already ran there.)
        let procedure_registry = Arc::new(uni_query::ProcedureRegistry::new());
        procedure_registry.set_plugin_registry(Arc::clone(&plugin_registry));

        let executor_template = build_executor_template(
            storage.clone(),
            self.config.clone(),
            writer_field.clone(),
            l0_manager_field.clone(),
            xervo_runtime.clone(),
            procedure_registry.clone(),
            prop_manager.clone(),
            df_session_template.clone(),
        );

        // M11 v1 + FU-5: spawn the deferral-queue tick task. When a
        // local `data_path` is available, use the JSON-sidecar
        // persistence backend (`<data_path>/_system/deferred_triggers.json`)
        // so the queue survives restarts; otherwise fall back to the
        // in-memory queue.
        let defer_queue = match persistence_data_path.as_deref() {
            Some(p) => crate::api::triggers::DeferralQueue::with_persistence(p.to_path_buf()),
            None => crate::api::triggers::DeferralQueue::new(),
        };
        // FU-5: replay any persisted items now that triggers have been
        // re-registered by `register_builtin_plugins` + user
        // `add_plugin`s above this point.
        let _restored = defer_queue.load_from_sidecar(&plugin_registry);

        // WS-E: per-`Uni` EventualConsistency coalescing queue. Wired to
        // the shared `defer_queue` (coalesced fires + back-pressure drains
        // push there) and configured from the flush knobs. Buckets survive
        // across per-commit router rebuilds because this `Arc` lives on
        // `UniInner`.
        let ec_queue = crate::api::triggers::EcQueue::new(
            Some(Arc::clone(&defer_queue)),
            self.config.ec_flush_interval,
            self.config.ec_flush_threshold,
        );

        // FU-4: spawn the CDC runtime. Snapshots registered CDC
        // providers, resumes each from its last persisted LSN, and
        // forwards every commit notification as a `CdcBatch`.
        let _cdc_runtime = crate::cdc_runtime::CdcRuntime::spawn(
            &plugin_registry,
            commit_tx.subscribe(),
            persistence_data_path.clone(),
            &shutdown_handle,
        );
        {
            let queue = Arc::clone(&defer_queue);
            // WS-E: reuse the one 50ms deferral timer to also flush due
            // EventualConsistency coalescing buckets — no second task.
            let ec = Arc::clone(&ec_queue);
            let mut shutdown_rx = shutdown_handle.subscribe();
            let handle = tokio::spawn(async move {
                let mut ticker = tokio::time::interval(std::time::Duration::from_millis(50));
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            ec.flush_due(std::time::Instant::now());
                            queue.tick();
                        }
                        _ = shutdown_rx.recv() => { break; }
                    }
                }
            });
            track_task_with_scratch_claim(&shutdown_handle, scratch_dir.as_ref(), handle);
        }

        // M11: spawn the background-job scheduler driver. The driver
        // polls `Scheduler::tick_at(now)` every
        // `crate::scheduler::DEFAULT_TICK_INTERVAL`, looks up each due
        // job's `BackgroundJobProvider` in the plugin registry, and
        // dispatches it on `spawn_blocking`. Persistence defaults to
        // `MemoryPersistence` until the durable
        // `SystemLabelPersistence` (writes through
        // `uni_system.background_jobs` via the write-enabled
        // `execute_inner_query`) lands in `uni-query`.
        //
        // M11 A.3: the `SchedulerJobHost` is constructed with the
        // storage manager now and the `UniInner` weak ref later
        // (after the inner is wrapped in an Arc) so built-in jobs can
        // reach host services via `JobContext::host`.
        let scheduler_job_host = Arc::new(crate::scheduler::SchedulerJobHost::new(Arc::clone(
            &storage,
        )));
        // M11 A.6: pick durable scheduler persistence when a
        // local data directory is available; fall back to
        // `MemoryPersistence` for remote / in-memory instances.
        let (scheduler_persistence, scheduler_persist_sink) =
            crate::scheduler_persistence::scheduler_persistence_for_data_path(
                persistence_data_path.as_deref(),
            );
        let scheduler_host = crate::scheduler::SchedulerHost::spawn_with_job_host(
            Arc::clone(&plugin_registry),
            scheduler_persistence,
            &shutdown_handle,
            crate::scheduler::DEFAULT_TICK_INTERVAL,
            Some(Arc::clone(&scheduler_job_host)),
        );

        // M11 B.5: register `uni.periodic.{schedule,cancel,list}`
        // procedures with a `SchedulerControl` trait object pointing
        // at the live scheduler. Registration happens after
        // `SchedulerHost::spawn` so the procedures hold a handle to
        // the actual scheduler the driver loop is polling.
        {
            use uni_plugin::{
                AbiRange, Capability, CapabilitySet, Determinism, PluginId, PluginManifest,
                PluginRegistrar, ProvidedSurfaces, Scope, SideEffects as PluginSideEffects,
            };

            // M11 A.2: hand the periodic procedures a control handle
            // pointing at the host (not the bare `Scheduler` primitive)
            // so `uni.periodic.submit` / `iterate` reach
            // `JobHost::execute_write_cypher` via the
            // `SchedulerHost::submit_cypher` override.
            let scheduler_ctrl: Arc<dyn uni_plugin::scheduler::SchedulerControl> =
                Arc::clone(&scheduler_host) as Arc<dyn uni_plugin::scheduler::SchedulerControl>;
            let plugin_id = PluginId::new("uni");
            let caps =
                CapabilitySet::from_iter_of([Capability::Procedure, Capability::ProcedureWrites]);
            let manifest = PluginManifest {
                id: plugin_id.clone(),
                version: env!("CARGO_PKG_VERSION")
                    .parse()
                    .unwrap_or_else(|_| "1.0.0".parse().expect("static version parses")),
                abi: AbiRange::parse("^1").expect("manifest ABI range is valid"),
                depends_on: vec![],
                capabilities: caps.clone(),
                determinism: Determinism::Pure,
                side_effects: PluginSideEffects::Writes,
                scope: Scope::Instance,
                hash: None,
                signature: None,
                provides: ProvidedSurfaces::default(),
                docs: "uni.periodic.* procedures (M11 B.5).".to_owned(),
                metadata: std::collections::BTreeMap::new(),
            };
            // Apply the host's signature policy before activation. The
            // built-in `uni` plugin ships unsigned today; the default
            // `Disabled` policy accepts it. Embedders that opt into
            // `RequireSigned` must also sign this manifest with a key
            // in their trust root.
            uni_plugin::verify::verify_manifest_with_policy(
                &manifest,
                &uni_plugin::verify::TrustRoot::new(),
                uni_plugin::verify::SignaturePolicy::default(),
            )
            .expect("builtin uni manifest must pass the default Disabled policy");
            let mut r = PluginRegistrar::new(plugin_id, &caps, &plugin_registry);
            uni_plugin_builtin::procedures::periodic::register_into(&mut r, scheduler_ctrl)
                .expect("uni.periodic.* registration");
            r.commit_to_registry().expect("uni.periodic.* commit");
        }

        let db = Uni {
            inner: Arc::new(UniInner {
                storage,
                schema: schema_manager,
                properties: prop_manager,
                writer: writer_field,
                uri: uri.clone(),
                l0_manager: l0_manager_field,
                id_allocator: id_allocator_field,
                xervo_runtime,
                config: self.config,
                procedure_registry,
                plugin_registry,
                plugins: Arc::new(parking_lot::RwLock::new(HashMap::new())),
                defer_queue,
                ec_queue,
                scheduler_host: Arc::clone(&scheduler_host),
                shutdown_handle,
                locy_rule_registry: Arc::new(std::sync::RwLock::new(loaded_locy_registry)),
                locy_rule_persister: Some(locy_rule_persister),
                start_time: Instant::now(),
                commit_tx,
                write_lease: self.write_lease,
                plugin_trust: self.plugin_trust,
                active_session_count: AtomicUsize::new(0),
                total_queries: AtomicU64::new(0),
                total_commits: AtomicU64::new(0),
                custom_functions: Arc::new(std::sync::RwLock::new(
                    uni_query::CustomFunctionRegistry::new(),
                )),
                df_session_template,
                executor_template,
                fork_registry,
                fork_inners: Arc::new(DashMap::new()),
                inflight_tx_count: Arc::new(AtomicUsize::new(0)),
                cached_l0_mutation_count: AtomicUsize::new(0),
                cached_l0_estimated_size: AtomicUsize::new(0),
                cached_wal_lsn: AtomicU64::new(0),
                _temp_dir: scratch_dir.clone(),
                plan_cache: Arc::new(std::sync::Mutex::new(crate::api::session::PlanCache::new(
                    TX_PLAN_CACHE_CAPACITY,
                ))),
            }),
        };

        // The single `HostCypherExecutor` impl the moved plugin-host engines
        // (scheduler job host + persistence sinks) call back through for
        // write-mode Cypher (replaces the per-engine `Weak<UniInner>` they used
        // to hold directly). The executor itself only weakly references
        // `UniInner`, so the host ↔ engine cycle stays leak-free even though the
        // engines hold a strong `Arc<dyn ...>`.
        let host_cypher_exec: Arc<dyn uni_plugin_host::host::HostCypherExecutor> = Arc::new(
            host_executor::UniInnerCypherExecutor::new(Arc::downgrade(&db.inner)),
        );

        // M11 A.3: wire the host Cypher executor into the scheduler's job host
        // so built-in background jobs can reach the host for write-mode Cypher
        // (ttl_sweep, etc.).
        scheduler_job_host.set_host_executor(Arc::clone(&host_cypher_exec));

        // M11 A.7: wire the executor into the meta-plugin persistence sink so
        // subsequent `declareFunction` / `declareProcedure` calls dual-write
        // into the `_DeclaredPlugin` graph label (in addition to the JSON
        // sidecar source-of-truth).
        if let Some(sink) = &custom_persistence_sink {
            sink.set_host_executor(Arc::clone(&host_cypher_exec));
        }
        // M11 A.6: same lazy-wire pattern for the durable scheduler
        // persistence sink (`_BackgroundJob` graph nodes).
        if let Some(sink) = &scheduler_persist_sink {
            sink.set_host_executor(Arc::clone(&host_cypher_exec));
        }

        // Phase 4a: spawn the TTL sweeper (no-op when disabled).
        //
        // The host holds a `Weak<UniInner>` so the task does not extend the
        // database's lifetime; the scheduling/shutdown loop lives in
        // `uni_fork::maintenance`.
        let sweeper_host = Arc::new(fork_maintenance::ForkMaintenanceHostImpl::new(
            Arc::downgrade(&db.inner),
        ));
        if let Some(handle) = uni_fork::maintenance::spawn_sweeper(
            sweeper_host,
            sweeper_interval,
            sweeper_disabled,
            sweeper_shutdown_rx,
        ) {
            track_task_with_scratch_claim(&db.inner.shutdown_handle, scratch_dir.as_ref(), handle);
        }

        // Phase 5a-impl Step 7: spawn the fork index builder (no-op
        // when disabled).
        let index_builder_host = Arc::new(fork_maintenance::ForkMaintenanceHostImpl::new(
            Arc::downgrade(&db.inner),
        ));
        if let Some(handle) = uni_fork::maintenance::spawn_index_builder(
            index_builder_host,
            index_builder_interval,
            index_builder_threshold,
            index_builder_disabled,
            index_builder_shutdown_rx,
        ) {
            track_task_with_scratch_claim(&db.inner.shutdown_handle, scratch_dir.as_ref(), handle);
        }

        Ok(db)
    }

    /// Open the database (blocking)
    pub fn build_sync(self) -> Result<Uni> {
        let rt = tokio::runtime::Runtime::new().map_err(UniError::Io)?;
        rt.block_on(self.build())
    }

    fn cloud_config_to_lancedb_storage_options(
        config: &CloudStorageConfig,
    ) -> std::collections::HashMap<String, String> {
        let mut opts = std::collections::HashMap::new();

        match config {
            CloudStorageConfig::S3 {
                bucket,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
                session_token,
                virtual_hosted_style,
            } => {
                opts.insert("bucket".to_string(), bucket.clone());
                opts.insert(
                    "virtual_hosted_style_request".to_string(),
                    virtual_hosted_style.to_string(),
                );

                if let Some(r) = region {
                    opts.insert("region".to_string(), r.clone());
                }
                if let Some(ep) = endpoint {
                    opts.insert("endpoint".to_string(), ep.clone());
                    if ep.starts_with("http://") {
                        opts.insert("allow_http".to_string(), "true".to_string());
                    }
                }
                if let Some(v) = access_key_id {
                    opts.insert("access_key_id".to_string(), v.clone());
                }
                if let Some(v) = secret_access_key {
                    opts.insert("secret_access_key".to_string(), v.clone());
                }
                if let Some(v) = session_token {
                    opts.insert("session_token".to_string(), v.clone());
                }
            }
            CloudStorageConfig::Gcs {
                bucket,
                service_account_path,
                service_account_key,
            } => {
                opts.insert("bucket".to_string(), bucket.clone());
                if let Some(v) = service_account_path {
                    opts.insert("service_account".to_string(), v.clone());
                    opts.insert("application_credentials".to_string(), v.clone());
                }
                if let Some(v) = service_account_key {
                    opts.insert("service_account_key".to_string(), v.clone());
                }
            }
            CloudStorageConfig::Azure {
                container,
                account,
                access_key,
                sas_token,
            } => {
                opts.insert("account_name".to_string(), account.clone());
                opts.insert("container_name".to_string(), container.clone());
                if let Some(v) = access_key {
                    opts.insert("access_key".to_string(), v.clone());
                }
                if let Some(v) = sas_token {
                    opts.insert("sas_token".to_string(), v.clone());
                }
            }
        }

        opts
    }
}

#[cfg(test)]
mod fork_inner_tests {
    use super::*;
    use uni_common::core::fork::{ForkId, ForkInfo, SchemaDelta};
    use uni_store::fork::{ForkRegistryHandle, ForkScope};

    /// Smoke test for `UniInner::at_fork`: a fork-scoped inner reads
    /// through the fork's branches and writes through it are gated.
    /// Phase 1 wiring; Day 7's `Session::fork` will exercise it via
    /// the public API end-to-end.
    #[tokio::test]
    async fn at_fork_returns_inner_with_fork_scoped_storage() {
        let db = Uni::in_memory().build().await.unwrap();
        let primary_inner = db.inner.as_ref();

        // Build a registry on a fresh local store. We don't share the
        // primary's object store here — Phase 1's at_fork is a
        // structural test of UniInner construction; the registry only
        // needs to provide an Active ForkInfo to wrap into a ForkScope.
        let dir = tempfile::TempDir::new().unwrap();
        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let registry = Arc::new(ForkRegistryHandle::load(store).await.unwrap());

        let info = ForkInfo::new_pending(ForkId::new(), "smoke", "snap-1", 1);
        registry.begin_create(info).await.unwrap();
        let active = registry
            .finish_create("smoke", Default::default())
            .await
            .unwrap();

        let scope = Arc::new(ForkScope::new(
            Arc::new(active),
            SchemaDelta::empty(),
            registry,
        ));

        let forked_inner = primary_inner.at_fork(scope.clone()).await.unwrap();
        assert!(forked_inner.storage.fork_scope().is_some());
        // Phase 2 Day 4: a forked UniInner now carries its own Writer.
        // The Writer's storage is the fork-scoped clone; its allocator
        // is fork-local.
        let writer = forked_inner
            .writer
            .as_ref()
            .expect("Phase 2 fork must carry its own Writer");
        assert!(
            std::sync::Arc::ptr_eq(&writer.storage, &forked_inner.storage),
            "fork Writer's storage should be the fork-scoped storage"
        );
        // Schema is a *fresh* Arc (overlay-merged), not pointer-equal to primary's.
        assert!(!Arc::ptr_eq(&forked_inner.schema, &primary_inner.schema));

        db.shutdown().await.unwrap();
    }
}
