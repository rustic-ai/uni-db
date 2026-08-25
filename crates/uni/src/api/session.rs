// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Session — the primary read scope for all database access.
//!
//! Sessions are cheap, synchronous, and infallible to create. All reads go
//! through sessions, and sessions are the factory for transactions (writes).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::instrument;
use uuid::Uuid;

use crate::api::UniInner;
use crate::api::hooks::{HookContext, QueryType, SessionHook};
use crate::api::impl_locy::LocyRuleRegistry;
use crate::api::locy_result::LocyResult;
use crate::api::retry::RetryOptions;
use crate::api::transaction::{IsolationLevel, Transaction};
use uni_common::{Result, UniError, Value};
use uni_query::{ExecuteResult, ExplainOutput, ProfileOutput, QueryCursor, QueryResult, Row};

/// Build the [`UniError::Query`] returned when a `session.query()` /
/// `QueryBuilder::fetch_all` is rejected for containing mutation clauses.
///
/// Shared by `execute_cached` and `QueryBuilder::fetch_all` so the
/// user-facing wording lives in exactly one place.
fn read_only_violation(cypher: &str) -> UniError {
    UniError::Query {
        message: "Session.query() is read-only. Mutation clauses (CREATE, MERGE, DELETE, SET, \
             REMOVE) require a transaction. Use session.tx() to start one."
            .to_string(),
        query: Some(cypher.to_string()),
    }
}

/// Atomic counters for plan cache hits/misses, shared between Session and its
/// query execution helpers.
pub(crate) struct PlanCacheMetrics {
    pub(crate) hits: AtomicU64,
    pub(crate) misses: AtomicU64,
}

/// Describes the capabilities of a session in its current mode.
///
/// This is a snapshot — capabilities may change if the underlying database
/// configuration changes (e.g., read-only mode toggled).
#[derive(Debug, Clone)]
pub struct SessionCapabilities {
    /// Whether the session can create transactions and execute writes.
    pub can_write: bool,
    /// Whether the session supports version pinning (read-at-version).
    pub can_pin: bool,
    /// The isolation level used for transactions in this session.
    pub isolation: IsolationLevel,
    /// Whether commit notifications are available.
    pub has_notifications: bool,
    /// Write lease strategy in effect, if any.
    pub write_lease: Option<WriteLeaseSummary>,
}

/// Summary of the write lease strategy, suitable for capability snapshots.
///
/// This is a `Clone`-friendly description of the [`WriteLease`](crate::WriteLease)
/// variant without carrying the actual provider trait object.
#[derive(Debug, Clone)]
pub enum WriteLeaseSummary {
    /// Local single-process lock.
    Local,
    /// DynamoDB-based distributed lease.
    DynamoDB { table: String },
    /// Custom lease provider (opaque).
    Custom,
}

/// Internal atomic counters for session-level metrics.
pub(crate) struct SessionMetricsInner {
    pub(crate) queries_executed: AtomicU64,
    pub(crate) locy_evaluations: AtomicU64,
    pub(crate) total_query_time_us: AtomicU64,
    pub(crate) transactions_committed: AtomicU64,
    pub(crate) transactions_rolled_back: AtomicU64,
    pub(crate) total_rows_returned: AtomicU64,
    pub(crate) total_rows_scanned: AtomicU64,
}

impl SessionMetricsInner {
    fn new() -> Self {
        Self {
            queries_executed: AtomicU64::new(0),
            locy_evaluations: AtomicU64::new(0),
            total_query_time_us: AtomicU64::new(0),
            transactions_committed: AtomicU64::new(0),
            transactions_rolled_back: AtomicU64::new(0),
            total_rows_returned: AtomicU64::new(0),
            total_rows_scanned: AtomicU64::new(0),
        }
    }
}

/// Snapshot of session-level metrics.
#[derive(Debug, Clone)]
pub struct SessionMetrics {
    /// The session ID.
    pub session_id: String,
    /// When the session was created.
    pub active_since: Instant,
    /// Number of queries executed.
    pub queries_executed: u64,
    /// Number of Locy evaluations.
    pub locy_evaluations: u64,
    /// Total time spent executing queries.
    pub total_query_time: Duration,
    /// Number of transactions that were committed.
    pub transactions_committed: u64,
    /// Number of transactions that were rolled back.
    pub transactions_rolled_back: u64,
    /// Total rows returned across all queries.
    pub total_rows_returned: u64,
    /// Total rows scanned across all queries.
    ///
    /// Accumulated from each result's `QueryMetrics::rows_scanned`, so it counts
    /// scan-path reads only — a query answered entirely from an index or the
    /// plan cache contributes nothing.
    pub total_rows_scanned: u64,
    /// Number of plan cache hits.
    pub plan_cache_hits: u64,
    /// Number of plan cache misses.
    pub plan_cache_misses: u64,
    /// Current plan cache size (entries).
    pub plan_cache_size: usize,
}

/// A database session — the primary scope for reads.
///
/// All data access goes through sessions. Sessions hold scoped query parameters
/// and a private copy of the Locy rule registry. They are the factory for
/// [`Transaction`]s (write scope).
///
/// Sessions are cheap to create (sync, no I/O) and cheap to clone (Arc-based).
///
/// # Examples
///
/// ```no_run
/// # use uni_db::Uni;
/// # async fn example(db: &Uni) -> uni_db::Result<()> {
/// let session = db.session();
/// session.params().set("tenant", 42);
///
/// let rows = session.query("MATCH (n) WHERE n.tenant = $tenant RETURN n").await?;
///
/// // Transactions for writes
/// let tx = session.tx().await?;
/// tx.execute("CREATE (:Person {name: 'Alice'})").await?;
/// tx.commit().await?;
/// # Ok(())
/// # }
/// ```
pub struct Session {
    pub(crate) db: Arc<UniInner>,
    /// When pinned via `pin_to_version`/`pin_to_timestamp`, holds the original
    /// (live) db reference so `refresh()` can restore it.
    original_db: Option<Arc<UniInner>>,
    /// Fork scope when this session is forked. `None` for primary
    /// sessions. Threaded through to `StorageManager` via the swapped
    /// `UniInner`; reads route through the fork's branches automatically.
    /// `tx()` is gated when this is `Some`.
    pub(crate) fork_scope: Option<Arc<uni_store::fork::ForkScope>>,
    id: String,
    params: Arc<std::sync::RwLock<HashMap<String, Value>>>,
    rule_registry: Arc<std::sync::RwLock<LocyRuleRegistry>>,
    /// Per-session plugin registry — fresh-empty per `Session::new` /
    /// `new_forked`, shared across `Clone` (M8 follow-up F1).
    ///
    /// `Uni::load_python_plugin` registers into `db.plugin_registry`
    /// (instance scope). `Session::add_python_plugin` registers into
    /// this session-local registry. Query / procedure / Locy-aggregate
    /// resolution dual-consults both, with session entries shadowing
    /// instance entries by name — see proposal §5.4.2 for the
    /// session-scope contract.
    ///
    /// Backed by `PluginRegistry`'s wait-free internals
    /// (`DashMap` + `ArcSwap`); no outer lock needed.
    pub(crate) session_plugin_registry: Arc<uni_plugin::PluginRegistry>,
    /// Mutual exclusion for write contexts (transaction, bulk writer).
    /// Only one write context can be active per session.
    active_write_guard: Arc<AtomicBool>,
    /// Atomic session-level metrics counters.
    pub(crate) metrics_inner: Arc<SessionMetricsInner>,
    /// Timestamp when this session was created.
    created_at: Instant,
    /// Cancellation token for cooperative query cancellation.
    /// Behind `Arc<RwLock<>>` so `cancel()` can take `&self`.
    cancellation_token: Arc<std::sync::RwLock<CancellationToken>>,
    /// Transparent plan cache for parsed/planned queries (shared across clones).
    plan_cache: Arc<std::sync::Mutex<PlanCache>>,
    /// Atomic plan cache hit/miss counters.
    plan_cache_metrics: Arc<PlanCacheMetrics>,
    /// Session-level hooks for query/commit interception, keyed by name.
    pub(crate) hooks: HashMap<String, Arc<dyn SessionHook>>,
    /// Default query timeout (from template or explicit configuration).
    pub(crate) query_timeout: Option<Duration>,
    /// Default transaction timeout (from template or explicit configuration).
    pub(crate) transaction_timeout: Option<Duration>,
    /// When `true`, planner consults registered `ReplacementScanProvider`s
    /// for unknown CALL / function / label identifiers and strict-mode
    /// errors on unknown labels instead of returning empty rows. Default
    /// `false`; flip with [`Self::set_replacement_scans`]. Shared across
    /// session clones via `Arc<AtomicBool>`.
    pub(crate) replacement_scans_enabled: Arc<AtomicBool>,
    /// Authenticated principal (M5i). `None` means "anonymous" —
    /// `AuthzPolicy::check` still runs and can grant or deny, but
    /// `Principal { id: "anonymous", groups: [] }` is what it sees.
    pub(crate) principal: Option<Arc<uni_plugin::traits::connector::Principal>>,
}

impl Session {
    /// Shared base constructor for the three public-facing constructors
    /// (`new` / `new_forked` / `new_from_template`).
    ///
    /// The arguments are exactly the fields those constructors differ in;
    /// every other field is initialized identically (fresh id, fresh
    /// session-local plugin registry, fresh metrics / plan cache / write
    /// guard / cancellation token, no principal). Each caller is
    /// responsible for incrementing `db.active_session_count` before
    /// delegating here so the increment lives next to the matching
    /// `Drop`-time decrement at each call site.
    #[allow(clippy::too_many_arguments)]
    fn new_base(
        db: Arc<UniInner>,
        fork_scope: Option<Arc<uni_store::fork::ForkScope>>,
        params: HashMap<String, Value>,
        rule_registry: LocyRuleRegistry,
        cancellation_token: CancellationToken,
        hooks: HashMap<String, Arc<dyn SessionHook>>,
        query_timeout: Option<Duration>,
        transaction_timeout: Option<Duration>,
    ) -> Self {
        Self {
            db,
            original_db: None,
            fork_scope,
            id: Uuid::new_v4().to_string(),
            params: Arc::new(std::sync::RwLock::new(params)),
            rule_registry: Arc::new(std::sync::RwLock::new(rule_registry)),
            session_plugin_registry: Arc::new(uni_plugin::PluginRegistry::new()),
            active_write_guard: Arc::new(AtomicBool::new(false)),
            metrics_inner: Arc::new(SessionMetricsInner::new()),
            created_at: Instant::now(),
            cancellation_token: Arc::new(std::sync::RwLock::new(cancellation_token)),
            plan_cache: Arc::new(std::sync::Mutex::new(PlanCache::new(1000))),
            plan_cache_metrics: Arc::new(PlanCacheMetrics {
                hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
            }),
            hooks,
            query_timeout,
            transaction_timeout,
            replacement_scans_enabled: Arc::new(AtomicBool::new(false)),
            principal: None,
        }
    }

    /// Create a new session from a shared database reference.
    pub(crate) fn new(db: Arc<UniInner>) -> Self {
        // Clone the global rule registry into this session
        let session_registry = db.locy_rule_registry.read().unwrap().clone();

        db.active_session_count.fetch_add(1, Ordering::Relaxed);

        Self::new_base(
            db,
            None,
            HashMap::new(),
            session_registry,
            CancellationToken::new(),
            HashMap::new(),
            None,
            None,
        )
    }

    /// Create a forked session from a fork-scoped `UniInner`.
    ///
    /// Built by [`crate::api::fork::ForkBuilder::build`]. `db` is the
    /// new inner returned by `UniInner::at_fork`, which already wraps
    /// the fork-scoped storage and merged schema. `scope` is stored on
    /// the session so [`Session::is_forked`] can answer cheaply and
    /// `tx()` can gate writes.
    ///
    /// Per the spec §4 contract for forked sessions:
    /// - params: empty (independent from parent)
    /// - hooks: empty (no propagation)
    /// - rule_registry: deep-cloned from the new inner's registry
    ///   (which `UniInner::at_fork` already deep-cloned from primary)
    /// - plan_cache: fresh empty (storage layout differs from primary)
    /// - metrics: fresh
    /// - cancellation_token: child of the parent's token (Phase 4a,
    ///   spec §4.6). Cancelling the parent cascades to this fork;
    ///   cancelling this fork does not affect the parent.
    pub(crate) fn new_forked(
        db: Arc<UniInner>,
        scope: Arc<uni_store::fork::ForkScope>,
        parent_token: CancellationToken,
    ) -> Self {
        let session_registry = db.locy_rule_registry.read().unwrap().clone();
        db.active_session_count.fetch_add(1, Ordering::Relaxed);

        // Phase 4a: link cancellation to the parent via
        // `CancellationToken::child_token()`. The child fires when the
        // parent fires AND can be cancelled independently without
        // affecting the parent — exactly the spec §4.6 contract.
        let child_token = parent_token.child_token();

        Self::new_base(
            db,
            Some(scope),
            HashMap::new(),
            session_registry,
            child_token,
            HashMap::new(),
            None,
            None,
        )
    }

    /// Open or create a fork by name.
    ///
    /// `session.fork("scenario_1").await` opens the fork if it
    /// exists or creates it at the current primary snapshot if not.
    /// `session.fork("scenario_1").new_().await` requires creation
    /// and errors with [`uni_common::api::error::UniError::ForkAlreadyExists`] otherwise.
    ///
    /// As of Phase 2, the returned `Session` is **writable**: its
    /// `.tx().execute(...).commit()` lands mutations on the fork's
    /// Lance branches. Primary is unaffected.
    ///
    /// # Examples
    ///
    /// ```
    /// # use uni_db::Uni;
    /// # async fn example() -> uni_db::Result<()> {
    /// let db = Uni::in_memory().build().await?;
    /// let session = db.session();
    /// let forked = session.fork("scenario_1").await?;
    /// assert!(forked.is_forked());
    /// # db.shutdown().await
    /// # }
    /// ```
    #[must_use]
    pub fn fork(&self, name: impl Into<String>) -> super::fork::ForkBuilder<'_> {
        super::fork::ForkBuilder::new(self, name.into())
    }

    /// Create a new session from a template's pre-compiled state.
    pub(crate) fn new_from_template(
        db: Arc<UniInner>,
        params: HashMap<String, Value>,
        rule_registry: LocyRuleRegistry,
        hooks: HashMap<String, Arc<dyn SessionHook>>,
        query_timeout: Option<Duration>,
        transaction_timeout: Option<Duration>,
    ) -> Self {
        db.active_session_count.fetch_add(1, Ordering::Relaxed);

        Self::new_base(
            db,
            None,
            params,
            rule_registry,
            CancellationToken::new(),
            hooks,
            query_timeout,
            transaction_timeout,
        )
    }

    /// Attach an authenticated [`Principal`] to this session.
    ///
    /// Called from [`crate::api::Uni::session_with_credentials`] after
    /// a successful authentication round-trip. Authz policies will see
    /// this principal; without one they see the anonymous fallback.
    ///
    /// [`Principal`]: uni_plugin::traits::connector::Principal
    #[must_use]
    pub fn with_principal(
        mut self,
        principal: Arc<uni_plugin::traits::connector::Principal>,
    ) -> Self {
        self.principal = Some(principal);
        self
    }

    /// The principal authenticated to this session, if any. `None`
    /// means anonymous.
    #[must_use]
    pub fn principal(&self) -> Option<&Arc<uni_plugin::traits::connector::Principal>> {
        self.principal.as_ref()
    }

    /// Borrow the **session-local** plugin registry — fresh-empty
    /// per `Session::new` / `new_forked` / `new_from_template`, shared
    /// across `Clone`.
    ///
    /// Use this to add plugins that should be visible only to this
    /// session (e.g., Python UDFs registered by a notebook user).
    /// Resolution in the query path dual-consults this registry then
    /// falls back to the instance registry.
    #[must_use]
    pub fn plugin_registry(&self) -> &Arc<uni_plugin::PluginRegistry> {
        &self.session_plugin_registry
    }

    /// Borrow the **instance-level** plugin registry shared across all
    /// sessions. Plugins added through `Uni::add_plugin`,
    /// `Uni::load_rhai_plugin`, `Uni::load_python_plugin` etc. live
    /// here and are visible to every session.
    #[must_use]
    pub fn instance_plugin_registry(&self) -> &Arc<uni_plugin::PluginRegistry> {
        &self.db.plugin_registry
    }

    /// Whether a `CALL proc(...)` names a registered mutating procedure.
    ///
    /// Returns `true` only when `name` resolves (in the session-local registry
    /// or the instance registry) to a procedure whose `ProcedureMode` is not
    /// `Read`. Unknown procedure names return `false` — they are not
    /// classifiable here and fail later at dispatch if truly absent. Used by the
    /// read-only gate to reject write procedures alongside write clauses.
    pub(crate) fn procedure_is_write(&self, name: &str) -> bool {
        use uni_plugin::traits::procedure::ProcedureMode;
        let Ok(qname) = uni_plugin::QName::parse(name) else {
            return false;
        };
        let is_write = |reg: &uni_plugin::PluginRegistry| {
            reg.procedure(&qname)
                .is_some_and(|e| !matches!(e.signature.mode, ProcedureMode::Read))
        };
        is_write(&self.session_plugin_registry) || is_write(&self.db.plugin_registry)
    }

    /// Load a PyO3 (Python source) plugin into this **session**'s
    /// local plugin registry.
    ///
    /// Mirrors [`crate::Uni::load_python_plugin`] but registers
    /// session-scoped per proposal §5.4.2 default: the plugin is not
    /// visible to other sessions and is dropped when this session is
    /// dropped.
    ///
    /// The supplied `loader` carries the default plugin id used when
    /// the module doesn't call `db.set_plugin_id(...)`. `module_src` is
    /// Python source code; `module_name` is the simulated `__name__`.
    ///
    /// # Errors
    ///
    /// - [`UniError::InvalidArgument`] for plugin-side faults (parse,
    ///   manifest, unknown type name).
    /// - [`UniError::Internal`] for host-side faults.
    ///
    /// # Feature
    ///
    /// Requires the `pyo3-plugins` feature.
    #[cfg(feature = "pyo3-plugins")]
    pub fn add_python_plugin(
        &self,
        py: pyo3::Python<'_>,
        loader: &uni_plugin_pyo3::PythonPluginLoader,
        module_src: &str,
        module_name: &str,
        registrar_caps: &uni_plugin::CapabilitySet,
    ) -> Result<uni_plugin_pyo3::LoadOutcome> {
        crate::api::with_loading_registrar(
            &self.session_plugin_registry,
            "pyo3.session.loading",
            registrar_caps,
            |r| {
                loader
                    .load(py, module_src, module_name, r, registrar_caps)
                    .map_err(|e| crate::api::py_plugin_err_to_uni("module_src", e))
            },
        )
    }

    /// Commit accumulated decorator entries from a
    /// [`uni_plugin_pyo3::ManifestBuilder`] into this session's local
    /// registry.
    ///
    /// Used by the Python bindings layer to commit
    /// `@db.scalar_fn(...)`-style incremental decorations.
    ///
    /// # Errors
    ///
    /// Same shape as [`Self::add_python_plugin`].
    ///
    /// # Feature
    ///
    /// Requires the `pyo3-plugins` feature.
    #[cfg(feature = "pyo3-plugins")]
    pub fn finalize_python_plugin(
        &self,
        loader: &uni_plugin_pyo3::PythonPluginLoader,
        builder: &uni_plugin_pyo3::ManifestBuilder,
        registrar_caps: &uni_plugin::CapabilitySet,
    ) -> Result<uni_plugin_pyo3::LoadOutcome> {
        crate::api::with_loading_registrar(
            &self.session_plugin_registry,
            "pyo3.session.loading",
            registrar_caps,
            |r| {
                loader
                    .load_from_builder(builder, r, registrar_caps)
                    .map_err(|e| match e {
                        uni_plugin_pyo3::PyPluginError::ManifestInvalid(m) => {
                            UniError::InvalidArgument {
                                arg: "decorators".to_owned(),
                                message: format!("python plugin manifest: {m}"),
                            }
                        }
                        other => UniError::Internal(anyhow::anyhow!(other.to_string())),
                    })
            },
        )
    }

    /// Run every registered [`AuthzPolicy`] against the session's
    /// principal (or anonymous if none) for the given `cypher` query
    /// and `verb` action. Returns the first `Decision::Deny` as a
    /// [`UniError::AuthorizationDenied`].
    ///
    /// v1 scope: `Resource::path` is the full cypher source (used by
    /// policies that want regex / hash matching). Per-label /
    /// per-edge-type resource extraction is a v1.1 follow-up.
    ///
    /// [`AuthzPolicy`]: uni_plugin::traits::connector::AuthzPolicy
    fn authorize(&self, cypher: &str, verb: &str) -> Result<()> {
        authorize_query(&self.db, self.principal.as_deref(), cypher, verb)
    }

    /// Run the read-path guards `Session::query` applies, for the builder entry
    /// points (`fetch_all`/`cursor`/`profile`) that otherwise bypass them:
    /// authorization, before-query hooks, and read-only validation (including
    /// writes nested in `CALL { … }` and registered write procedures).
    ///
    /// Centralizing them here means a new builder method inherits the guards by
    /// calling this, rather than silently re-opening the bypass.
    ///
    /// # Errors
    /// Returns an authorization, hook, parse, or read-only-violation error.
    fn run_query_guards(&self, cypher: &str, params: &HashMap<String, Value>) -> Result<()> {
        self.authorize(cypher, "read")?;
        self.run_before_query_hooks(cypher, QueryType::Cypher, params)?;
        let ast = uni_cypher::parse(cypher).map_err(crate::api::impl_query::into_parse_error)?;
        uni_query::validate_read_only_with(&ast, &|name| self.procedure_is_write(name))
            .map_err(|_| read_only_violation(cypher))?;
        Ok(())
    }

    /// Returns whether `ReplacementScanProvider` dispatch is enabled for
    /// this session (default `false`). When true, the planner consults
    /// registered providers for unknown CALL / function / label
    /// identifiers, and label resolution becomes strict (unknown labels
    /// error instead of returning empty rows).
    #[must_use]
    pub fn replacement_scans_enabled(&self) -> bool {
        self.replacement_scans_enabled.load(Ordering::Relaxed)
    }

    /// Enable or disable `ReplacementScanProvider` dispatch for this
    /// session. The flag is shared across session clones (Arc-backed).
    pub fn set_replacement_scans(&self, enabled: bool) {
        self.replacement_scans_enabled
            .store(enabled, Ordering::Relaxed);
    }

    // ── Scoped Parameters ─────────────────────────────────────────────

    /// Access the session-scoped parameter store.
    pub fn params(&self) -> Params<'_> {
        Params {
            store: &self.params,
        }
    }

    // ── Cypher Reads ──────────────────────────────────────────────────

    /// Execute a read-only Cypher query.
    ///
    /// Uses the transparent plan cache: repeated queries with the same text
    /// skip parsing and planning. Cache entries auto-invalidate on schema
    /// changes.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn query(&self, cypher: &str) -> Result<QueryResult> {
        self.authorize(cypher, "read")?;
        let params = self.merge_params(HashMap::new());
        self.run_before_query_hooks(cypher, QueryType::Cypher, &params)?;
        let start = Instant::now();
        let result = self.execute_cached(cypher, params.clone()).await;
        self.metrics_inner
            .queries_executed
            .fetch_add(1, Ordering::Relaxed);
        self.db.total_queries.fetch_add(1, Ordering::Relaxed);
        self.metrics_inner
            .total_query_time_us
            .fetch_add(start.elapsed().as_micros() as u64, Ordering::Relaxed);
        if let Ok(ref qr) = result {
            self.metrics_inner
                .total_rows_returned
                .fetch_add(qr.len() as u64, Ordering::Relaxed);
            // Previously never incremented anywhere in the workspace, so
            // `SessionMetrics::total_rows_scanned` was permanently 0.
            self.metrics_inner
                .total_rows_scanned
                .fetch_add(qr.metrics().rows_scanned as u64, Ordering::Relaxed);
            self.run_after_query_hooks(cypher, QueryType::Cypher, &params, qr.metrics());
        }
        result
    }

    /// Run any Cypher statement, auto-committing writes; the "run anything" entry.
    ///
    /// Classifies `cypher` as read or write using the same read-only oracle
    /// [`Session::query`] enforces ([`uni_query::validate_read_only_with`], with
    /// this session's write-procedure predicate). Read statements are forwarded
    /// to [`Session::query`]. Write statements (CREATE / MERGE / DELETE / SET /
    /// REMOVE, write `CALL { … }` subqueries, write/schema/dbms procedures) are
    /// executed inside a fresh auto-commit transaction: begin, run, commit.
    ///
    /// Unlike [`Session::query`], this never rejects a write; unlike a bare
    /// [`Session::tx`] write, the transaction is committed for you. It is the
    /// convenient single-statement entry point for REPLs and one-shot CLI use
    /// where the caller does not know ahead of time whether the input mutates.
    ///
    /// Writes run through [`Transaction::query`] (not `execute`) so a write with
    /// a trailing `RETURN` still yields its rows in the returned [`QueryResult`].
    /// For multi-statement atomic units, drive a [`Session::tx`] directly.
    ///
    /// # Errors
    /// Returns a parse error for malformed Cypher, any error surfaced by the
    /// underlying read or transactional write, or a commit error.
    #[instrument(skip(self, cypher), fields(session_id = %self.id))]
    pub async fn run(&self, cypher: impl Into<String>) -> Result<QueryResult> {
        let cypher = cypher.into();
        // Reuse the read-only oracle: `Ok` ⇒ pure read, `Err` ⇒ contains a
        // write/schema/dbms clause and must go through a transaction.
        let ast = uni_cypher::parse(&cypher).map_err(crate::api::impl_query::into_parse_error)?;
        let is_read =
            uni_query::validate_read_only_with(&ast, &|name| self.procedure_is_write(name)).is_ok();

        if is_read {
            return self.query(&cypher).await;
        }

        // Write path: auto-commit transaction. `tx.query` (not `tx.execute`)
        // executes the write and preserves any `RETURN` rows. The tx read-only
        // gate only fires for time-travel, so a plain write passes.
        let tx = self.tx().await?;
        let result = tx.query(&cypher).await?;
        tx.commit().await?;
        Ok(result)
    }

    /// Execute a read-only Cypher query with a builder for parameters.
    pub fn query_with(&self, cypher: &str) -> QueryBuilder<'_> {
        QueryBuilder {
            session: self,
            cypher: cypher.to_string(),
            params: HashMap::new(),
            timeout: self.query_timeout,
            max_memory: None,
            cancellation_token: None,
        }
    }

    // ── Locy Evaluation ───────────────────────────────────────────────

    /// Evaluate a Locy program with default configuration.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn locy(&self, program: &str) -> Result<LocyResult> {
        self.run_before_query_hooks(program, QueryType::Locy, &HashMap::new())?;
        let result = self.locy_with(program).run().await;
        self.metrics_inner
            .locy_evaluations
            .fetch_add(1, Ordering::Relaxed);
        result
    }

    /// Evaluate a Locy program with parameters using a builder.
    pub fn locy_with(&self, program: &str) -> crate::api::locy_builder::LocyBuilder<'_> {
        crate::api::locy_builder::LocyBuilder::new(self, program)
    }

    // ── Rule Management ───────────────────────────────────────────────

    /// Access the session-scoped rule registry.
    pub fn rules(&self) -> super::rule_registry::RuleRegistry<'_> {
        super::rule_registry::RuleRegistry::new(&self.rule_registry)
            .with_plugin_registry(&self.db.plugin_registry)
    }

    /// Compile a Locy program without executing it, using this session's rule registry.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub fn compile_locy(&self, program: &str) -> Result<uni_locy::CompiledProgram> {
        let ast = uni_cypher::parse_locy(program).map_err(|e| UniError::Parse {
            message: format!("LocyParseError: {e}"),
            position: None,
            line: None,
            column: None,
            context: None,
        })?;
        let registry = self.rule_registry.read().unwrap();
        let external_names: Vec<String> = registry.rules.keys().cloned().collect();
        drop(registry);

        // Consult the session registry before the instance one, so a
        // session-scoped plugin aggregate is visible. This is also why the
        // oracle is passed explicitly rather than read from the task-local:
        // `LocyBuilder::run` wraps itself in `scoped_with_session_context` but
        // `explain()` and `profile()` do not, so a task-local would make
        // `explain()` reject a program `run()` accepts.
        let session_reg = &self.session_plugin_registry;
        let instance_reg = self.instance_plugin_registry();
        let oracle = |name: &str| {
            uni_query::query::df_graph::locy_fold::locy_monotonicity_verdict(session_reg, name)
                .or_else(|| {
                    uni_query::query::df_graph::locy_fold::locy_monotonicity_verdict(
                        instance_reg,
                        name,
                    )
                })
        };
        uni_locy::compile_with_oracle(&ast, &HashMap::new(), &external_names, &oracle).map_err(
            |e| UniError::Query {
                message: format!("LocyCompileError: {e}"),
                query: None,
            },
        )
    }

    // ── Transaction & Writer Factories ────────────────────────────────

    /// Create a new transaction for multi-statement writes.
    ///
    /// Only one write context (transaction or bulk writer) can be active
    /// per session at a time. Returns `ReadOnly` if the session is pinned.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn tx(&self) -> Result<Transaction> {
        // Phase 2 Day 7: writes through a forked session are no longer
        // gated. The fork's `UniInner` carries its own Writer (Day 4)
        // with a per-fork L0/WAL/IdAllocator. Commits land on the
        // fork's Lance branches via the BranchedBackend (Day 2). The
        // Phase 1 `ForkWritesNotYetSupported` gate has been removed.
        if self.is_pinned() {
            return Err(UniError::ReadOnly {
                operation: "start_transaction".to_string(),
            });
        }
        Transaction::new(self).await
    }

    /// Begin a **scratch** (ephemeral, write-isolated) transaction — the cheap
    /// per-rollout fork (G8/E2).
    ///
    /// A scratch transaction behaves like [`tx`](Self::tx) — `execute`, `query`
    /// (read-your-writes), `rollback`, and drop all work — with two differences:
    /// its writes go to a private L0 over a *pinned* read base with an in-memory
    /// id allocator, and **`commit()` is refused**, so the writes are always
    /// discarded on drop. It costs a pinned-snapshot (~1ms), not a Lance-branch
    /// fork (~10ms): no branch, no registry 2PC, no per-fork WAL, and the global
    /// `id_allocator.json` is never advanced — so thousands of speculative
    /// open-write-discard rollouts (e.g. an MCTS that *mutates* per rollout) are
    /// affordable. For a read-only rollout, prefer
    /// [`pin_to_version`](Self::pin_to_version) instead.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn scratch(&self) -> Result<Transaction> {
        if self.is_pinned() {
            return Err(UniError::ReadOnly {
                operation: "start_scratch_transaction".to_string(),
            });
        }
        Transaction::new_scratch(self).await
    }

    /// Runs `f` in a transaction, retrying on retriable conflicts.
    ///
    /// Each attempt opens a fresh transaction, passes it to `f`, and commits on
    /// `Ok`. A retriable failure ([`UniError::is_retriable`]) from either `f` or
    /// the commit rolls back and retries with jittered backoff, up to
    /// `opts.max_attempts`; the final error is returned once attempts are
    /// exhausted. Non-retriable errors return immediately. Because `commit`
    /// consumes the transaction, `f` is re-run from scratch each attempt — it
    /// must re-clone any captured input it consumes and re-read any value it
    /// derives a write from, which is exactly what makes the retry observe the
    /// winning transaction's committed state.
    ///
    /// The closure returns a boxed future borrowing the transaction; the
    /// `Box::pin(async move { … })` wrapper at the call site is required.
    ///
    /// # Examples
    /// ```no_run
    /// # async fn ex(session: &uni_db::Session) -> uni_db::Result<()> {
    /// use uni_db::RetryOptions;
    /// session
    ///     .transact_with_retry(RetryOptions::default(), |tx| {
    ///         Box::pin(async move {
    ///             tx.execute("MATCH (c:Counter {id: 'x'}) SET c.n = c.n + 1").await?;
    ///             Ok(())
    ///         })
    ///     })
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    /// Returns the last error once attempts are exhausted, or immediately for any
    /// non-retriable error raised by `f`, by `tx()`, or by `commit()`.
    pub async fn transact_with_retry<F, T>(&self, opts: RetryOptions, mut f: F) -> Result<T>
    where
        F: for<'a> FnMut(&'a Transaction) -> Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>,
        T: Send,
    {
        let mut attempt: u32 = 1;
        loop {
            let tx = self.tx().await?;
            match f(&tx).await {
                Ok(value) => match tx.commit().await {
                    Ok(_) => return Ok(value),
                    // `commit` already consumed `tx`; nothing to roll back.
                    Err(e) if e.is_retriable() && attempt < opts.max_attempts => {
                        metrics::counter!("uni_ssi_retries_total", "stage" => "commit")
                            .increment(1);
                        attempt += 1;
                        opts.backoff(attempt).await;
                    }
                    Err(e) => return Err(e),
                },
                Err(e) if e.is_retriable() && attempt < opts.max_attempts => {
                    metrics::counter!("uni_ssi_retries_total", "stage" => "body").increment(1);
                    tx.rollback();
                    attempt += 1;
                    opts.backoff(attempt).await;
                }
                Err(e) => {
                    tx.rollback();
                    return Err(e);
                }
            }
        }
    }

    /// Runs a single mutation with conflict retry, using default options.
    ///
    /// Convenience over [`Session::transact_with_retry`] for a self-contained
    /// statement such as an atomic read-modify-write `SET`.
    ///
    /// # Examples
    /// ```no_run
    /// # async fn ex(session: &uni_db::Session) -> uni_db::Result<()> {
    /// session
    ///     .execute_with_retry("MATCH (c:Counter {id: 'x'}) SET c.n = c.n + 1")
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Errors
    /// Same as [`Session::transact_with_retry`].
    pub async fn execute_with_retry(&self, cypher: &str) -> Result<ExecuteResult> {
        let cypher = cypher.to_owned();
        self.transact_with_retry(RetryOptions::default(), move |tx| {
            let cypher = cypher.clone();
            Box::pin(async move { tx.execute(&cypher).await })
        })
        .await
    }

    /// `true` when this session was returned by `Session::fork(...)`.
    pub fn is_forked(&self) -> bool {
        self.fork_scope.is_some()
    }

    /// Borrow the fork scope, if any. Used by
    /// [`crate::api::fork_schema::ForkSchemaBuilder`] to route
    /// fork-local schema additions to the right overlay; not part of
    /// the public surface.
    pub(crate) fn fork_scope(&self) -> Option<Arc<uni_store::fork::ForkScope>> {
        self.fork_scope.clone()
    }

    /// Begin a fork-local schema mutation.
    ///
    /// Only valid on a forked session. The returned builder mirrors
    /// `Uni::schema()`'s shape but routes both to the fork's
    /// in-memory `SchemaManager` and to the fork's persisted
    /// overlay file (`catalog/fork_schemas/{fork_id}.json`). Primary's
    /// schema is unaffected.
    ///
    /// Use this in strict-schema mode (`UniConfig.strict_schema =
    /// true`) to introduce labels or edge types that exist only on
    /// the fork. In schemaless mode the schema check is bypassed and
    /// `BranchedBackend` materializes the dataset on the fly without
    /// a schema entry — `fork_schema()` is harmless but unnecessary.
    ///
    /// `apply()` returns `UniError::InvalidArgument` if called on a
    /// non-forked session.
    pub fn fork_schema(&self) -> super::fork_schema::ForkSchemaBuilder<'_> {
        super::fork_schema::ForkSchemaBuilder::new(self)
    }

    /// Flush this session's writer to L1.
    ///
    /// On a forked session this flushes the fork's L0 buffer to the
    /// fork's Lance branches — required before forking a nested child
    /// if the child should see the parent fork's already-committed
    /// writes through the Lance chain. On a primary session this is
    /// equivalent to `Uni::flush()`.
    ///
    /// # Errors
    ///
    /// Returns [`UniError::ReadOnly`] when the session has no writer
    /// (e.g. snapshot-pinned sessions).
    pub async fn flush(&self) -> Result<()> {
        if let Some(writer_lock) = &self.db.writer {
            let writer: &uni_store::Writer = writer_lock.as_ref();
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

    /// Phase 5a-impl: build a fork-local index immediately on this
    /// session's fork branch. The build registers the index on the
    /// session's `ForkScope`; subsequent reads through the planner
    /// will pick `FusedIndexScan` against `(label, column)`.
    ///
    /// Bypasses the per-fork fragment-count threshold that the
    /// background scheduler honors — useful for tests and for power
    /// users who want immediate index materialization.
    ///
    /// # Errors
    ///
    /// - [`UniError::InvalidArgument`] when called on a non-forked
    ///   session.
    /// - Underlying Lance build failure (`UniError::Internal`).
    pub async fn build_fork_local_index(
        &self,
        label: &str,
        column: &str,
        kind: uni_store::fork::ForkLocalIndexKind,
    ) -> Result<()> {
        let Some(scope) = self.fork_scope() else {
            return Err(UniError::InvalidArgument {
                arg: "self".into(),
                message: "build_fork_local_index requires a forked session".into(),
            });
        };
        // Flush any pending L0 first so the build sees the current
        // tip. For VidUid this is belt-and-braces — the builder is a
        // no-op there — but for BTree/Sorted it ensures the Lance
        // index covers all rows the user has committed.
        if let Some(writer_lock) = &self.db.writer {
            let writer: &uni_store::Writer = writer_lock.as_ref();
            writer.flush_to_l1(None).await.map_err(UniError::Internal)?;
        }
        let branching =
            self.db
                .storage
                .backend()
                .branching()
                .ok_or_else(|| UniError::InvalidArgument {
                    arg: "self".into(),
                    message: "storage backend does not support fork branching".into(),
                })?;
        uni_store::fork::index_builder::build_fork_local_index(
            &scope,
            branching.as_ref(),
            label,
            column,
            kind,
        )
        .await
        .map_err(UniError::Internal)
    }

    /// Create a transaction with builder options (timeout, isolation level).
    pub fn tx_with(&self) -> TransactionBuilder<'_> {
        TransactionBuilder {
            session: self,
            timeout: self.transaction_timeout,
            isolation: IsolationLevel::default(),
        }
    }

    // ── Version Pinning ──────────────────────────────────────────────

    /// Pin this session to a specific snapshot version.
    ///
    /// All subsequent reads see data as of that version. Writes are rejected.
    ///
    /// **This is the fork-free read snapshot for read-only rollouts (G8).** A
    /// repeated read-only workload — e.g. an MCTS `graph_readonly` rollout that
    /// only *reads* the graph thousands of times — should pin a version here
    /// rather than create-and-drop a Lance-branch fork per iteration. Pinning is
    /// an in-memory `StorageManager` clone (`StorageManager::pinned`) — no
    /// branch, no registry 2PC, no per-fork WAL — so it costs ~1ms against a
    /// fork's ~10ms. A plain read session already gets MVCC snapshot isolation
    /// without any fork; use this when you additionally need a *stable, durable*
    /// version across many queries. Write-isolated speculative rollouts (mutate
    /// then discard) still need a fork.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn pin_to_version(&mut self, snapshot_id: &str) -> Result<()> {
        let pinned = self.live_db().at_snapshot(snapshot_id).await?;
        if self.original_db.is_none() {
            self.original_db = Some(self.db.clone());
        }
        self.db = Arc::new(pinned);
        Ok(())
    }

    /// Pin this session to a specific timestamp.
    ///
    /// Resolves the closest snapshot at or before the given timestamp, then
    /// pins the session to that snapshot. Writes are rejected while pinned.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn pin_to_timestamp(&mut self, ts: chrono::DateTime<chrono::Utc>) -> Result<()> {
        let snapshot_id = self.live_db().resolve_time_travel_timestamp(ts).await?;
        self.pin_to_version(&snapshot_id).await
    }

    /// Refresh: unpin the session, returning to the live database state.
    ///
    /// In single-process mode, this simply unpins the session.
    /// In multi-agent mode (Phase 2), this picks up the latest
    /// committed version from storage.
    pub async fn refresh(&mut self) -> Result<()> {
        if let Some(original) = self.original_db.take() {
            self.db = original;
        }
        Ok(())
    }

    /// Returns `true` if the session is pinned to a specific version.
    pub fn is_pinned(&self) -> bool {
        self.original_db.is_some()
    }

    /// Get the live (unpinned) db reference for resolving snapshots.
    fn live_db(&self) -> &Arc<UniInner> {
        self.original_db.as_ref().unwrap_or(&self.db)
    }

    // ── Cancellation ─────────────────────────────────────────────────

    /// Cancel all in-flight queries in this session.
    ///
    /// Queries check `is_cancelled()` at each operator boundary via
    /// `check_timeout()`. After cancellation, a fresh token is created
    /// so the session remains usable.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub fn cancel(&self) {
        let mut token = self.cancellation_token.write().unwrap();
        token.cancel();
        *token = CancellationToken::new();
    }

    /// Get a clone of this session's cancellation token.
    ///
    /// Useful for external cancellation (e.g. from a timeout task).
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.read().unwrap().clone()
    }

    /// The cancellation scope statements issued on this session run under.
    ///
    /// Every execution path takes one of these, so `cancel()` reaches work that
    /// is already in flight. Previously only `QueryBuilder` with an explicit
    /// token carried anything down, and the plain `query()` path passed `None`
    /// at each of its three call sites — which made `Session::cancel()` inert
    /// for the most common way of running a query.
    pub(crate) fn cancel_scope(
        &self,
        caller: Option<CancellationToken>,
    ) -> crate::api::impl_query::CancelScope {
        crate::api::impl_query::CancelScope::new(Some(self.cancellation_token()), caller)
    }

    // ── Prepared Statements ──────────────────────────────────────────

    /// Prepare a Cypher query for repeated execution.
    ///
    /// The query is parsed and planned once; subsequent executions skip those
    /// phases. If the schema changes, the prepared query auto-replans.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn prepare(&self, cypher: &str) -> Result<crate::api::prepared::PreparedQuery> {
        let guards = crate::api::prepared::PreparedGuards::for_session(
            self.principal.clone(),
            self.hooks.values().cloned().collect(),
            self.id.clone(),
        );
        crate::api::prepared::PreparedQuery::new(
            self.db.clone(),
            cypher,
            guards,
            self.cancel_scope(None),
        )
        .await
    }

    /// Prepare a Locy program for repeated evaluation.
    #[instrument(skip(self), fields(session_id = %self.id))]
    pub async fn prepare_locy(&self, program: &str) -> Result<crate::api::prepared::PreparedLocy> {
        crate::api::prepared::PreparedLocy::new(
            self.db.clone(),
            self.rule_registry.clone(),
            program,
        )
    }

    // ── Hooks ─────────────────────────────────────────────────────────

    /// Add a named session hook for query/commit interception.
    #[deprecated(
        since = "1.6.0",
        note = "Use `Uni::add_plugin(BuiltinHookPlugin::new(...))` instead. Registry-iterating dispatch fires both per-session hooks and plugin hooks; this method will be removed in 2.0."
    )]
    pub fn add_hook(&mut self, name: impl Into<String>, hook: impl SessionHook + 'static) {
        self.hooks.insert(name.into(), Arc::new(hook));
    }

    /// Remove a hook by name. Returns true if it existed.
    #[deprecated(
        since = "1.6.0",
        note = "Use `Uni::add_plugin(BuiltinHookPlugin::new(...))` instead. Registry-iterating dispatch fires both per-session hooks and plugin hooks; this method will be removed in 2.0."
    )]
    pub fn remove_hook(&mut self, name: &str) -> bool {
        self.hooks.remove(name).is_some()
    }

    /// List names of all registered hooks.
    #[deprecated(
        since = "1.6.0",
        note = "Use `Uni::add_plugin(BuiltinHookPlugin::new(...))` instead. Registry-iterating dispatch fires both per-session hooks and plugin hooks; this method will be removed in 2.0."
    )]
    pub fn list_hooks(&self) -> Vec<String> {
        self.hooks.keys().cloned().collect()
    }

    /// Remove all hooks.
    #[deprecated(
        since = "1.6.0",
        note = "Use `Uni::add_plugin(BuiltinHookPlugin::new(...))` instead. Registry-iterating dispatch fires both per-session hooks and plugin hooks; this method will be removed in 2.0."
    )]
    pub fn clear_hooks(&mut self) {
        self.hooks.clear();
    }

    /// Run before-query hooks. Returns `Err(HookRejected)` if any hook rejects.
    ///
    /// Iterates two dispatch tables: the legacy per-session
    /// `HashMap<String, SessionHook>` (for backward compatibility with
    /// `Session::add_hook`), and the phased
    /// `uni_plugin::SessionHook` chain in the plugin registry (M5b — any
    /// `BuiltinHookPlugin` registered via `Uni::add_plugin`). The two
    /// dispatch paths are additive: registry hooks fire even if the
    /// per-session map is empty.
    pub(crate) fn run_before_query_hooks(
        &self,
        query_text: &str,
        query_type: QueryType,
        params: &HashMap<String, Value>,
    ) -> Result<()> {
        let hooks: Vec<Arc<dyn SessionHook>> = self.hooks.values().cloned().collect();
        run_before_query_hooks_raw(&self.db, &hooks, &self.id, query_text, query_type, params)
    }

    /// Run after-query hooks. Panics in hooks are caught and logged.
    pub(crate) fn run_after_query_hooks(
        &self,
        query_text: &str,
        query_type: QueryType,
        params: &HashMap<String, Value>,
        metrics: &uni_query::QueryMetrics,
    ) {
        let legacy_ctx = HookContext {
            session_id: self.id.clone(),
            query_text: query_text.to_string(),
            query_type,
            params: params.clone(),
        };
        for hook in self.hooks.values() {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                hook.after_query(&legacy_ctx, metrics);
            }));
            if let Err(e) = result {
                tracing::error!("after_query hook panicked: {:?}", e);
            }
        }

        // Phased registry hooks (M5b): translate metrics + fire
        // `on_execute_end`. Panics in registry hooks are also caught.
        let registry_hooks = self.db.plugin_registry.hooks();
        if !registry_hooks.is_empty() {
            let exec_ctx = uni_plugin::traits::hook::ExecuteContext::new(&self.id);
            let plugin_metrics = uni_plugin::traits::hook::QueryMetrics {
                elapsed: metrics.total_time,
                rows_out: metrics.rows_returned as u64,
                bytes_read: metrics.bytes_read as u64,
            };
            for hook in registry_hooks.iter() {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    hook.on_execute_end(&exec_ctx, &plugin_metrics);
                }));
                if let Err(e) = result {
                    tracing::error!("registry on_execute_end hook panicked: {:?}", e);
                }
            }
        }
    }

    // ── Commit Notifications ─────────────────────────────────────────

    /// Watch for all commit notifications.
    pub fn watch(&self) -> crate::api::notifications::CommitStream {
        let rx = self.db.commit_tx.subscribe();
        crate::api::notifications::WatchBuilder::new(rx).build()
    }

    /// Watch for commit notifications with filters.
    pub fn watch_with(&self) -> crate::api::notifications::WatchBuilder {
        let rx = self.db.commit_tx.subscribe();
        crate::api::notifications::WatchBuilder::new(rx)
    }

    // ── Lifecycle & Observability ──────────────────────────────────────

    /// Get the session ID.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Query the capabilities of this session.
    ///
    /// Returns a snapshot of what the session can do in its current mode.
    pub fn capabilities(&self) -> SessionCapabilities {
        use crate::api::multi_agent::WriteLease;
        let write_lease = self.db.write_lease.as_ref().map(|wl| match wl {
            WriteLease::Local => WriteLeaseSummary::Local,
            WriteLease::DynamoDB { table } => WriteLeaseSummary::DynamoDB {
                table: table.clone(),
            },
            WriteLease::Custom(_) => WriteLeaseSummary::Custom,
        });
        SessionCapabilities {
            can_write: self.db.writer.is_some() && !self.is_pinned(),
            can_pin: true,
            isolation: IsolationLevel::default(),
            has_notifications: true,
            write_lease,
        }
    }

    /// Snapshot the session's accumulated metrics.
    pub fn metrics(&self) -> SessionMetrics {
        let m = &self.metrics_inner;
        SessionMetrics {
            session_id: self.id.clone(),
            active_since: self.created_at,
            queries_executed: m.queries_executed.load(Ordering::Relaxed),
            locy_evaluations: m.locy_evaluations.load(Ordering::Relaxed),
            total_query_time: Duration::from_micros(m.total_query_time_us.load(Ordering::Relaxed)),
            transactions_committed: m.transactions_committed.load(Ordering::Relaxed),
            transactions_rolled_back: m.transactions_rolled_back.load(Ordering::Relaxed),
            total_rows_returned: m.total_rows_returned.load(Ordering::Relaxed),
            total_rows_scanned: m.total_rows_scanned.load(Ordering::Relaxed),
            plan_cache_hits: self.plan_cache_metrics.hits.load(Ordering::Relaxed),
            plan_cache_misses: self.plan_cache_metrics.misses.load(Ordering::Relaxed),
            plan_cache_size: self.plan_cache.lock().map(|c| c.len()).unwrap_or(0),
        }
    }

    // ── Internal Helpers ──────────────────────────────────────────────

    /// Execute a query using the transparent plan cache.
    ///
    /// On cache hit: reuses the parsed AST and logical plan, skipping parse
    /// and planning phases entirely. On cache miss: parses, plans, caches,
    /// then executes normally. Cache entries are invalidated when the schema
    /// version changes.
    pub(crate) async fn execute_cached(
        &self,
        cypher: &str,
        params: HashMap<String, Value>,
    ) -> Result<QueryResult> {
        let schema_version = self.db.schema.schema().schema_version;
        let text_key = plan_cache_key(cypher);

        // Try cache lookup (brief lock, then release). The effective key folds
        // in the values of any LIMIT/SKIP parameters baked into the plan so a
        // `LIMIT $n` query is never served another value's plan.
        let cached = self.plan_cache.lock().ok().and_then(|mut cache| {
            let key = cache.effective_key(text_key, &params);
            cache
                .get(key, cypher, schema_version)
                .map(|entry| entry.plan.clone())
        });

        // Session-local plugin registry — threaded across the async
        // executor scope via `tokio::task_local!` so the per-query
        // UDF / procedure resolution path can consult it. See M8.6.
        // FU-1: also thread the session's authenticated principal so
        // procedure capability gates see the in-flight user.
        let session_pr = Arc::clone(&self.session_plugin_registry);
        let session_principal = self.principal.clone();

        if let Some(plan) = cached {
            // Cache hit — skip parse and plan, execute the cached plan directly.
            //
            // `parse_time`/`plan_time` stay zero on this path and that is
            // *correct*: neither phase ran. Only the miss path below has real
            // durations to report.
            // The deep clone for the owned plan happens here, off the cache mutex.
            self.plan_cache_metrics.hits.fetch_add(1, Ordering::Relaxed);
            let plan = Arc::unwrap_or_clone(plan);
            return uni_query::scoped_with_session_context(
                session_pr,
                session_principal,
                self.db.execute_plan_internal(
                    plan,
                    cypher,
                    params,
                    self.db.config.clone(),
                    self.cancel_scope(None),
                ),
            )
            .await;
        }

        // Cache miss — parse, plan, cache, execute via the normal path
        self.plan_cache_metrics
            .misses
            .fetch_add(1, Ordering::Relaxed);

        // Parse
        let parse_start = std::time::Instant::now();
        let ast = uni_cypher::parse(cypher).map_err(crate::api::impl_query::into_parse_error)?;
        let parse_time = parse_start.elapsed();

        // Enforce read-only semantics for session queries — mutations require
        // a transaction for isolation, WAL protection, and commit hooks. This
        // rejects write clauses, writes nested in `CALL { … }` subqueries, and
        // registered write/schema/dbms procedures.
        uni_query::validate_read_only_with(&ast, &|name| self.procedure_is_write(name))
            .map_err(|_| read_only_violation(cypher))?;

        // Time-travel queries bypass the cache entirely
        if matches!(ast, uni_cypher::ast::Query::TimeTravel { .. }) {
            return uni_query::scoped_with_session_context(
                Arc::clone(&session_pr),
                session_principal.clone(),
                self.db.execute_internal_with_config(
                    cypher,
                    params,
                    self.db.config.clone(),
                    self.cancel_scope(None),
                ),
            )
            .await;
        }

        // Plan
        let plan_start = std::time::Instant::now();
        let planner = uni_query::QueryPlanner::new(self.db.schema.schema().clone())
            .with_params(params.clone())
            .with_plugin_registry(Arc::clone(&self.db.plugin_registry))
            .with_replacement_scans(self.replacement_scans_enabled());
        let plan = Arc::new(
            planner
                .plan(ast)
                .map_err(|e| crate::api::impl_query::into_query_error(e, cypher))?,
        );
        let folded = planner.folded_limit_skip_params();
        let plan_time = plan_start.elapsed();

        // Cache the entry under the effective key (folding in any LIMIT/SKIP
        // parameter values the planner baked into this plan).
        if let Ok(mut cache) = self.plan_cache.lock() {
            cache.note_folded_params(text_key, folded);
            let key = cache.effective_key(text_key, &params);
            cache.insert(
                key,
                PlanCacheEntry {
                    plan: Arc::clone(&plan),
                    query: cypher.to_string(),
                    schema_version,
                    hit_count: 0,
                },
            );
        }

        // Execute the freshly planned query.
        //
        // Parse and plan happened here, above `execute_plan_internal`, which has
        // no way to observe either — so the durations are stamped onto the result
        // afterwards rather than measured and discarded.
        let mut result = uni_query::scoped_with_session_context(
            session_pr,
            session_principal,
            self.db.execute_plan_internal(
                Arc::unwrap_or_clone(plan),
                cypher,
                params,
                self.db.config.clone(),
                self.cancel_scope(None),
            ),
        )
        .await?;
        result.set_frontend_timing(parse_time, plan_time);
        Ok(result)
    }

    /// Get the database inner reference (for Transaction, LocyEngine, etc.)
    pub(crate) fn db(&self) -> &Arc<UniInner> {
        &self.db
    }

    /// Get the session's rule registry.
    pub(crate) fn rule_registry(&self) -> &Arc<std::sync::RwLock<LocyRuleRegistry>> {
        &self.rule_registry
    }

    /// Get the active write guard.
    pub(crate) fn active_write_guard(&self) -> &Arc<AtomicBool> {
        &self.active_write_guard
    }

    /// Merge session params with per-query params (per-query takes precedence).
    pub(crate) fn merge_params(
        &self,
        mut query_params: HashMap<String, Value>,
    ) -> HashMap<String, Value> {
        let session_params = self.params.read().unwrap();
        if !session_params.is_empty() {
            let session_map: HashMap<String, Value> = session_params.clone();
            if let Some(Value::Map(existing)) = query_params.get_mut("session") {
                for (k, v) in session_map {
                    existing.entry(k).or_insert(v);
                }
            } else {
                query_params.insert("session".to_string(), Value::Map(session_map));
            }
        }
        query_params
    }
}

/// Facade for session-scoped parameters.
///
/// Obtained via `session.params()`.
pub struct Params<'a> {
    store: &'a Arc<std::sync::RwLock<HashMap<String, Value>>>,
}

impl<'a> Params<'a> {
    /// Set a parameter.
    pub fn set<K: Into<String>, V: Into<Value>>(&self, key: K, value: V) {
        self.store.write().unwrap().insert(key.into(), value.into());
    }

    /// Get a parameter value by key.
    pub fn get(&self, key: &str) -> Option<Value> {
        self.store.read().unwrap().get(key).cloned()
    }

    /// Remove a parameter. Returns the previous value if it existed.
    pub fn unset(&self, key: &str) -> Option<Value> {
        self.store.write().unwrap().remove(key)
    }

    /// Get a snapshot of all parameters.
    pub fn get_all(&self) -> HashMap<String, Value> {
        self.store.read().unwrap().clone()
    }

    /// Set multiple parameters.
    pub fn set_all<I, K, V>(&self, params: I)
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<Value>,
    {
        let mut store = self.store.write().unwrap();
        for (k, v) in params {
            store.insert(k.into(), v.into());
        }
    }

    /// Clone the underlying store Arc for use in Python bindings.
    pub fn clone_store_arc(&self) -> Arc<std::sync::RwLock<HashMap<String, Value>>> {
        self.store.clone()
    }
}

/// Builder for parameterized queries within a session.
pub struct QueryBuilder<'a> {
    session: &'a Session,
    cypher: String,
    params: HashMap<String, Value>,
    timeout: Option<std::time::Duration>,
    max_memory: Option<usize>,
    cancellation_token: Option<CancellationToken>,
}

impl<'a> QueryBuilder<'a> {
    /// Bind a parameter to the query.
    pub fn param<K: Into<String>, V: Into<Value>>(mut self, key: K, value: V) -> Self {
        self.params.insert(key.into(), value.into());
        self
    }

    /// Bind multiple parameters from an iterator.
    pub fn params<'p>(mut self, params: impl IntoIterator<Item = (&'p str, Value)>) -> Self {
        for (k, v) in params {
            self.params.insert(k.to_string(), v);
        }
        self
    }

    /// Set maximum execution time for this query.
    pub fn timeout(mut self, duration: std::time::Duration) -> Self {
        self.timeout = Some(duration);
        self
    }

    /// Set maximum memory per query in bytes.
    pub fn max_memory(mut self, bytes: usize) -> Self {
        self.max_memory = Some(bytes);
        self
    }

    /// Attach a cancellation token for cooperative query cancellation.
    pub fn cancellation_token(mut self, token: CancellationToken) -> Self {
        self.cancellation_token = Some(token);
        self
    }

    /// Execute the query and fetch all results.
    ///
    /// Uses the session's transparent plan cache when no custom timeout or
    /// memory limit is set.
    pub async fn fetch_all(self) -> Result<QueryResult> {
        let params = self.session.merge_params(self.params);
        // Authorize, run before-query hooks, and enforce read-only — guards the
        // typed `Session::query` applies but the builder previously skipped,
        // letting a parameterized query bypass an `AuthzPolicy` or write
        // non-transactionally via `.profile()`/`.cursor()`.
        self.session.run_query_guards(&self.cypher, &params)?;

        let has_overrides = self.timeout.is_some()
            || self.max_memory.is_some()
            || self.cancellation_token.is_some();
        if has_overrides {
            // Custom config — bypass cache and use the config-aware path
            let mut db_config = self.session.db.config.clone();
            if let Some(t) = self.timeout {
                db_config.query_timeout = t;
            }
            if let Some(m) = self.max_memory {
                db_config.max_query_memory = m;
            }
            let session_pr = Arc::clone(&self.session.session_plugin_registry);
            let session_principal = self.session.principal.clone();
            uni_query::scoped_with_session_context(
                session_pr,
                session_principal,
                self.session.db.execute_internal_with_config_and_token(
                    &self.cypher,
                    params,
                    db_config,
                    self.session.cancel_scope(self.cancellation_token),
                ),
            )
            .await
        } else {
            // Default config — use the plan cache.
            // `execute_cached` already wraps with
            // `scoped_with_session_plugin_registry` so the session
            // registry is in scope through the cached / fresh-plan
            // branches inside.
            self.session.execute_cached(&self.cypher, params).await
        }
    }

    /// Execute the query and return the first row, or `None` if empty.
    pub async fn fetch_one(self) -> Result<Option<Row>> {
        let result = self.fetch_all().await?;
        Ok(result.into_rows().into_iter().next())
    }

    /// Execute the query and return a cursor for streaming results.
    pub async fn cursor(self) -> Result<QueryCursor> {
        let mut db_config = self.session.db.config.clone();
        if let Some(t) = self.timeout {
            db_config.query_timeout = t;
        }
        if let Some(m) = self.max_memory {
            db_config.max_query_memory = m;
        }
        let params = self.session.merge_params(self.params);
        self.session.run_query_guards(&self.cypher, &params)?;
        let session_pr = Arc::clone(&self.session.session_plugin_registry);
        let session_principal = self.session.principal.clone();
        uni_query::scoped_with_session_context(
            session_pr,
            session_principal,
            self.session.db.execute_cursor_internal_with_config(
                &self.cypher,
                params,
                db_config,
                self.session.cancel_scope(self.cancellation_token),
            ),
        )
        .await
    }

    /// Explain the query plan without executing it.
    pub async fn explain(self) -> Result<ExplainOutput> {
        // Authorize and fire before-query hooks — a parameterized `.explain()`
        // must not leak plan/cost information past an `AuthzPolicy` (the bypass
        // the other builder terminals had). Read-only validation is intentionally
        // NOT applied: EXPLAIN only plans, it never executes, so explaining a
        // write plan (e.g. to inspect a CREATE) is a legitimate read-like
        // introspection, unlike `.fetch_all()`/`.profile()` which would execute.
        let params = self.session.merge_params(self.params);
        self.session.authorize(&self.cypher, "read")?;
        self.session
            .run_before_query_hooks(&self.cypher, QueryType::Cypher, &params)?;
        self.session.db.explain_internal(&self.cypher).await
    }

    /// Profile the query execution, returning results with profiling output.
    pub async fn profile(self) -> Result<(QueryResult, ProfileOutput)> {
        let params = self.session.merge_params(self.params);
        self.session.run_query_guards(&self.cypher, &params)?;
        let session_pr = Arc::clone(&self.session.session_plugin_registry);
        let session_principal = self.session.principal.clone();
        uni_query::scoped_with_session_context(
            session_pr,
            session_principal,
            self.session.db.profile_internal(&self.cypher, params),
        )
        .await
    }
}

/// Builder for starting a transaction with options.
pub struct TransactionBuilder<'a> {
    session: &'a Session,
    timeout: Option<Duration>,
    isolation: IsolationLevel,
}

impl<'a> TransactionBuilder<'a> {
    /// Set the transaction timeout. The transaction will expire if operations
    /// are attempted after this duration.
    pub fn timeout(mut self, d: Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    /// Set the isolation level for the transaction.
    pub fn isolation(mut self, level: IsolationLevel) -> Self {
        self.isolation = level;
        self
    }

    /// Start the transaction.
    pub async fn start(self) -> Result<Transaction> {
        if self.session.is_pinned() {
            return Err(UniError::ReadOnly {
                operation: "start_transaction".to_string(),
            });
        }
        Transaction::new_with_options(self.session, self.timeout, self.isolation, false).await
    }
}

impl Clone for Session {
    /// Clone the session, sharing the plan cache with the original.
    ///
    /// The cloned session gets a fresh ID, fresh metrics counters, and a fresh
    /// cancellation token, but shares the plan cache so cache hits benefit all
    /// clones. The database's active session count is incremented.
    fn clone(&self) -> Self {
        self.db.active_session_count.fetch_add(1, Ordering::Relaxed);
        Self {
            db: self.db.clone(),
            original_db: self.original_db.clone(),
            fork_scope: self.fork_scope.clone(),
            id: Uuid::new_v4().to_string(),
            params: Arc::new(std::sync::RwLock::new(self.params.read().unwrap().clone())),
            rule_registry: Arc::new(std::sync::RwLock::new(
                self.rule_registry.read().unwrap().clone(),
            )),
            // Clones are alternate handles to the same logical
            // session — share the session-local plugin registry so
            // plugins added through one handle are visible to all
            // clones. Drop semantics are handled by `Arc` refcount.
            session_plugin_registry: Arc::clone(&self.session_plugin_registry),
            active_write_guard: Arc::new(AtomicBool::new(false)),
            metrics_inner: Arc::new(SessionMetricsInner::new()),
            created_at: Instant::now(),
            cancellation_token: Arc::new(std::sync::RwLock::new(CancellationToken::new())),
            plan_cache: self.plan_cache.clone(),
            plan_cache_metrics: self.plan_cache_metrics.clone(),
            hooks: self.hooks.clone(),
            query_timeout: self.query_timeout,
            transaction_timeout: self.transaction_timeout,
            replacement_scans_enabled: self.replacement_scans_enabled.clone(),
            principal: self.principal.clone(),
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.db.active_session_count.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Read-side host for the `uni-fork` diff/promote engine.
#[async_trait::async_trait]
impl uni_fork::ForkQueryHost for Session {
    async fn query(&self, cypher: &str) -> Result<QueryResult> {
        Session::query(self, cypher).await
    }

    fn storage(&self) -> Arc<uni_store::storage::manager::StorageManager> {
        self.db.storage.clone()
    }

    fn schema(&self) -> Arc<uni_common::core::schema::SchemaManager> {
        self.db.schema.clone()
    }
}

// ── Plan Cache (internal) ─────────────────────────────────────────────

/// Entry in the transparent plan cache.
pub(crate) struct PlanCacheEntry {
    /// `Arc` so a cache hit clones a pointer under the cache mutex; the
    /// per-execution deep clone (consumers need an owned plan for the
    /// fork-fusion rewrites) happens outside the lock via
    /// `Arc::unwrap_or_clone`.
    pub(crate) plan: std::sync::Arc<uni_query::LogicalPlan>,
    /// The exact query text this plan was built from. Compared on every
    /// lookup: the cache key is a 64-bit hash of the text, so two distinct
    /// queries can collide — without this check a collision would silently
    /// execute the other query's plan.
    pub(crate) query: String,
    pub(crate) schema_version: u32,
    pub(crate) hit_count: u64,
}

/// Transparent plan cache keyed by query text hash.
///
/// Caches parsed ASTs and logical plans to skip parsing and planning for
/// repeated queries. Entries are evicted LFU-style when the cache is full.
///
/// Shared by the read path ([`Session::execute_cached`]) and the transaction
/// write path ([`crate::api::UniInner::execute_internal_with_tx_l0`]).
pub(crate) struct PlanCache {
    entries: HashMap<u64, PlanCacheEntry>,
    /// Per-query-text record of parameter names the planner folded into a
    /// `LIMIT`/`SKIP` position. Because such plans bake the concrete value in,
    /// the effective cache key folds these parameters' values (see
    /// [`PlanCache::effective_key`]); keyed by the query-text hash so it is
    /// learned once per query shape and reused on every lookup.
    folded_params: HashMap<u64, Vec<String>>,
    max_entries: usize,
}

impl PlanCache {
    pub(crate) fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            folded_params: HashMap::new(),
            max_entries,
        }
    }

    /// Effective cache key for `cypher`, folding in the values of any parameters
    /// baked into `LIMIT`/`SKIP` during a prior planning of this query text.
    ///
    /// The base key is the query-text hash (`text_key`). When this text was
    /// previously found to fold parameters into its plan, those parameters'
    /// current values are hashed into the key so each distinct value gets its
    /// own entry — a `LIMIT $n` plan is never replayed with the wrong bound.
    /// Queries with no folded parameter keep a single, value-independent entry,
    /// so the hot parameterized-ingest path is unaffected. Collisions of the
    /// derived key are still caught by the per-entry query-text compare in
    /// [`PlanCache::get`].
    pub(crate) fn effective_key(&self, text_key: u64, params: &HashMap<String, Value>) -> u64 {
        let Some(names) = self.folded_params.get(&text_key) else {
            return text_key;
        };
        if names.is_empty() {
            return text_key;
        }
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        text_key.hash(&mut hasher);
        for name in names {
            name.hash(&mut hasher);
            match params.get(name) {
                Some(v) => {
                    1u8.hash(&mut hasher); // marker: parameter present
                    v.hash(&mut hasher);
                }
                None => 0u8.hash(&mut hasher), // marker: parameter absent
            }
        }
        hasher.finish()
    }

    /// Record that planning `text_key` folded `names` into `LIMIT`/`SKIP`.
    ///
    /// Idempotent and stable per query text. To stay bounded under a workload
    /// of ever-unique query texts (mirroring the LFU bound on `entries`), the
    /// record is cleared and re-learned lazily once it grows past a small
    /// multiple of `max_entries` — only a hint, so dropping it costs at most one
    /// extra parse on the next miss, never correctness.
    pub(crate) fn note_folded_params(&mut self, text_key: u64, names: Vec<String>) {
        if names.is_empty() {
            return;
        }
        // Headroom over `max_entries`: several param-value variants can share one
        // query text (one `folded_params` row), so allow a few× before pruning.
        const FOLDED_PARAMS_HEADROOM: usize = 8;
        if self.folded_params.len() >= self.max_entries.saturating_mul(FOLDED_PARAMS_HEADROOM) {
            self.folded_params.clear();
        }
        self.folded_params.entry(text_key).or_insert(names);
    }

    pub(crate) fn get(
        &mut self,
        key: u64,
        query: &str,
        current_schema_version: u32,
    ) -> Option<&PlanCacheEntry> {
        if let Some(entry) = self.entries.get_mut(&key) {
            // Key collision: a different query text hashed to the same key.
            // Treat as a miss but keep the entry — it belongs to a live query.
            if entry.query != query {
                return None;
            }
            if entry.schema_version == current_schema_version {
                entry.hit_count += 1;
                return self.entries.get(&key);
            }
            // Schema changed — evict stale entry
            self.entries.remove(&key);
        }
        None
    }

    pub(crate) fn insert(&mut self, key: u64, entry: PlanCacheEntry) {
        if self.entries.len() >= self.max_entries {
            // Evict entry with lowest hit_count
            if let Some((&evict_key, _)) = self.entries.iter().min_by_key(|(_, e)| e.hit_count) {
                self.entries.remove(&evict_key);
            }
        }
        self.entries.insert(key, entry);
    }

    /// Number of cached plans.
    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// Translate the legacy host [`QueryType`] enum to the plugin-framework
/// [`uni_plugin::traits::hook::QueryType`] used by phased
/// `SessionHook`s. Mirrors the inverse helper in `api::hooks`.
fn query_type_to_plugin(t: QueryType) -> uni_plugin::traits::hook::QueryType {
    use uni_plugin::traits::hook::QueryType as P;
    match t {
        QueryType::Cypher => P::Cypher,
        QueryType::Locy => P::Locy,
        QueryType::Execute => P::Execute,
    }
}

/// Consult every registered [`AuthzPolicy`] for `cypher` under `verb`.
///
/// Shared by `Session::authorize`, `Transaction::authorize`, and the prepared-
/// statement guards so all entry points enforce the same policy. An absent
/// principal is treated as anonymous; policies are responsible for rejecting it.
///
/// # Errors
/// Returns [`UniError::AuthorizationDenied`] if any policy denies or errors.
///
/// [`AuthzPolicy`]: uni_plugin::traits::connector::AuthzPolicy
/// Collect the property keys of a pattern-element map literal (`{k: v, …}`).
fn collect_map_keys(
    expr: Option<&uni_cypher::ast::Expr>,
    out: &mut std::collections::BTreeSet<String>,
) {
    if let Some(uni_cypher::ast::Expr::Map(entries)) = expr {
        for (k, _) in entries {
            out.insert(k.clone());
        }
    }
}

/// Walk one pattern element, collecting labels, relationship types, and the
/// property keys of any inline map literal. Recurses into parenthesized paths.
fn collect_pattern_element(
    el: &uni_cypher::ast::PatternElement,
    labels: &mut std::collections::BTreeSet<String>,
    rel_types: &mut std::collections::BTreeSet<String>,
    properties: &mut std::collections::BTreeSet<String>,
) {
    use uni_cypher::ast::PatternElement;
    match el {
        PatternElement::Node(n) => {
            labels.extend(n.labels.names().iter().cloned());
            collect_map_keys(n.properties.as_ref(), properties);
        }
        PatternElement::Relationship(r) => {
            rel_types.extend(r.types.names().iter().cloned());
            collect_map_keys(r.properties.as_ref(), properties);
        }
        PatternElement::Parenthesized { pattern, .. } => {
            for inner in &pattern.elements {
                collect_pattern_element(inner, labels, rel_types, properties);
            }
        }
    }
}

/// Best-effort extraction of the structured authz [`Resource`] fields —
/// labels, relationship types, touched property keys, and operations — from a
/// parsed Cypher query. Labels/types/operations are complete; property
/// extraction covers inline pattern map literals (a refinement, not a security
/// boundary on its own).
///
/// [`Resource`]: uni_plugin::traits::connector::Resource
fn extract_authz_fields(
    query: &uni_cypher::ast::Query,
) -> (Vec<String>, Vec<String>, Vec<String>, Vec<String>) {
    use std::collections::BTreeSet;

    let mut labels = BTreeSet::new();
    let mut rel_types = BTreeSet::new();
    let mut properties = BTreeSet::new();
    let mut operations = BTreeSet::new();

    walk_authz_query(
        query,
        &mut labels,
        &mut rel_types,
        &mut properties,
        &mut operations,
    );

    (
        labels.into_iter().collect(),
        rel_types.into_iter().collect(),
        properties.into_iter().collect(),
        operations.into_iter().collect(),
    )
}

/// Recurse the [`Query`](uni_cypher::ast::Query) enum (Single / Union / Explain /
/// TimeTravel / Schema), accumulating authz fields from each statement's clauses.
fn walk_authz_query(
    query: &uni_cypher::ast::Query,
    labels: &mut std::collections::BTreeSet<String>,
    rel_types: &mut std::collections::BTreeSet<String>,
    properties: &mut std::collections::BTreeSet<String>,
    operations: &mut std::collections::BTreeSet<String>,
) {
    use uni_cypher::ast::{Clause, Query};

    let walk_pattern =
        |pat: &uni_cypher::ast::Pattern,
         labels: &mut std::collections::BTreeSet<String>,
         rel_types: &mut std::collections::BTreeSet<String>,
         properties: &mut std::collections::BTreeSet<String>| {
            for path in &pat.paths {
                for el in &path.elements {
                    collect_pattern_element(el, labels, rel_types, properties);
                }
            }
        };

    match query {
        Query::Single(stmt) => {
            for clause in &stmt.clauses {
                match clause {
                    Clause::Match(m) => {
                        operations.insert("read".to_owned());
                        walk_pattern(&m.pattern, labels, rel_types, properties);
                    }
                    Clause::Create(c) => {
                        operations.insert("write".to_owned());
                        walk_pattern(&c.pattern, labels, rel_types, properties);
                    }
                    Clause::Merge(m) => {
                        operations.insert("write".to_owned());
                        walk_pattern(&m.pattern, labels, rel_types, properties);
                    }
                    Clause::Delete(_) => {
                        operations.insert("delete".to_owned());
                        operations.insert("write".to_owned());
                    }
                    Clause::Set(_) | Clause::Remove(_) => {
                        operations.insert("write".to_owned());
                    }
                    Clause::With(_)
                    | Clause::WithRecursive(_)
                    | Clause::Unwind(_)
                    | Clause::Return(_) => {
                        operations.insert("read".to_owned());
                    }
                    Clause::Call(_) => {
                        operations.insert("call".to_owned());
                    }
                }
            }
        }
        Query::Union { left, right, .. } => {
            walk_authz_query(left, labels, rel_types, properties, operations);
            walk_authz_query(right, labels, rel_types, properties, operations);
        }
        Query::Explain(inner) | Query::TimeTravel { query: inner, .. } => {
            walk_authz_query(inner, labels, rel_types, properties, operations);
        }
        Query::Schema(_) => {
            // DDL / schema commands — surfaced as a coarse "schema" operation.
            // Per-command label extraction is a follow-up.
            operations.insert("schema".to_owned());
        }
    }
}

pub(crate) fn authorize_query(
    db: &UniInner,
    principal: Option<&uni_plugin::traits::connector::Principal>,
    cypher: &str,
    verb: &str,
) -> Result<()> {
    use uni_plugin::traits::connector::{Action, Decision, Principal, Resource};

    let policies = db.plugin_registry.authz_policies();
    if policies.is_empty() {
        return Ok(());
    }
    let anon = Principal::anonymous();
    let principal = principal.unwrap_or(&anon);
    let action = Action {
        verb: verb.to_owned(),
    };
    // Parse the query (only when policies are active) to populate the structured
    // Resource. This is the same parse execution runs a moment later; a parse
    // failure leaves the structured fields empty (path-only) — the query will
    // fail to execute anyway, and a label/type-gating policy denies on empties.
    let (labels, rel_types, properties, operations) = match uni_cypher::parse(cypher) {
        Ok(query) => extract_authz_fields(&query),
        Err(_) => (Vec::new(), Vec::new(), Vec::new(), Vec::new()),
    };
    let resource = Resource {
        path: cypher.to_owned(),
        labels,
        rel_types,
        properties,
        operations,
    };
    for policy in policies.iter() {
        match policy.check(principal, &action, &resource) {
            Ok(Decision::Allow) => {}
            Ok(Decision::Deny { reason }) => return Err(UniError::AuthorizationDenied { reason }),
            Err(e) => return Err(UniError::AuthorizationDenied { reason: e.0 }),
        }
    }
    Ok(())
}

/// Fire before-query hooks (legacy session hooks + registry `on_parse`) for a
/// statement.
///
/// Shared by `Session::run_before_query_hooks` and the prepared-statement
/// guards so a prepared execution runs the same hook chain as the live path.
///
/// # Errors
/// Returns an error if a legacy hook fails or a registry hook rejects the query.
pub(crate) fn run_before_query_hooks_raw(
    db: &UniInner,
    hooks: &[Arc<dyn SessionHook>],
    session_id: &str,
    query_text: &str,
    query_type: QueryType,
    params: &HashMap<String, Value>,
) -> Result<()> {
    let legacy_ctx = HookContext {
        session_id: session_id.to_string(),
        query_text: query_text.to_string(),
        query_type,
        params: params.clone(),
    };
    for hook in hooks {
        hook.before_query(&legacy_ctx)?;
    }

    let registry_hooks = db.plugin_registry.hooks();
    if !registry_hooks.is_empty() {
        let parse_ctx = uni_plugin::traits::hook::ParseContext::new(query_text, session_id)
            .with_query_type(query_type_to_plugin(query_type));
        for hook in registry_hooks.iter() {
            use uni_plugin::errors::HookOutcome;
            match hook.on_parse(&parse_ctx) {
                HookOutcome::Continue => {}
                HookOutcome::Reject { reason } => {
                    return Err(UniError::HookRejected { message: reason });
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// Compute a hash key from a query string.
pub(crate) fn plan_cache_key(cypher: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    cypher.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod plan_cache_tests {
    use super::*;

    fn scan_plan(variable: &str) -> uni_query::LogicalPlan {
        uni_query::LogicalPlan::Scan {
            label_id: 0,
            labels: vec!["N".to_string()],
            variable: variable.to_string(),
            filter: None,
            optional: false,
        }
    }

    /// Regression test for architecture review finding §2.1: the cache key
    /// is a 64-bit hash of the query text, so two distinct queries can
    /// collide. The entry stores the query text and `get` compares it on
    /// every lookup — a colliding key with different text must MISS (and
    /// must not evict the resident entry, which belongs to a live query).
    ///
    /// A real `DefaultHasher` collision pair is a ~2^32 offline birthday
    /// search, so the collision is simulated at the cache API boundary by
    /// looking up the first query's key with the second query's text.
    #[test]
    fn colliding_keys_with_different_query_text_miss() {
        let schema_version = 7;
        let query_a = "MATCH (n:Person) RETURN n";
        let query_b = "CREATE (n:Hacker)";

        let key_a = plan_cache_key(query_a);

        let mut cache = PlanCache::new(10);
        cache.insert(
            key_a,
            PlanCacheEntry {
                plan: std::sync::Arc::new(scan_plan("plan_for_query_a")),
                query: query_a.to_string(),
                schema_version,
                hit_count: 0,
            },
        );

        // Simulate `query_b` hashing to the same key as `query_a`: same u64,
        // different text. The text comparison must turn this into a miss.
        let colliding_key = key_a; // == plan_cache_key(query_b) under collision
        assert!(
            cache.get(colliding_key, query_b, schema_version).is_none(),
            "a colliding key with different query text must miss"
        );

        // The resident entry survives the collision and still hits.
        let entry = cache
            .get(key_a, query_a, schema_version)
            .expect("original query must still hit after a collision lookup");
        match entry.plan.as_ref() {
            uni_query::LogicalPlan::Scan { variable, .. } => assert_eq!(
                variable, "plan_for_query_a",
                "query A must get its own plan back"
            ),
            _ => panic!("expected the Scan plan inserted for query A"),
        }
    }

    /// `plan_cache_key` is built on `DefaultHasher::new()` (fixed SipHash
    /// keys): the same text always yields the same key, with no per-process
    /// seed material mixed in. This is what makes an offline-crafted
    /// collision pair portable across processes and deployments.
    #[test]
    fn plan_cache_key_is_deterministic() {
        let q = "MATCH (n) RETURN n";
        assert_eq!(plan_cache_key(q), plan_cache_key(q));
        assert_ne!(plan_cache_key(q), plan_cache_key("MATCH (m) RETURN m"));
    }
}
